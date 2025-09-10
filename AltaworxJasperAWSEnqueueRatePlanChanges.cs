using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Altaworx.AWS.Core;
using Altaworx.AWS.Core.Models;
using Amazon;
using Amazon.Lambda.Core;
using Amazon.Lambda.SQSEvents;
using Amazon.SimpleEmail;
using Amazon.SimpleEmail.Model;
using Amazon.SQS;
using Amazon.SQS.Model;
using Amop.Core.Constants;
using Amop.Core.Helpers;
using Amop.Core.Logger;
using Amop.Core.Models;
using Amop.Core.Resilience;
using Microsoft.Data.SqlClient;
using MimeKit;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace AltaworxJasperAWSEnqueueRatePlanChanges
{
    public class Function : AwsFunctionBase
    {
        private const int DefaultDelaySeconds = 3;
        private string DeviceRatePlanChangeQueueUrl = Environment.GetEnvironmentVariable("DeviceRatePlanChangeQueueUrl");
        private string DeviceSyncQueueUrl = Environment.GetEnvironmentVariable("DeviceSyncQueueUrl");
        private string JasperEnqueueRatePlanChangeUrl = Environment.GetEnvironmentVariable("JasperEnqueueRatePlanChangeUrl");
        private string AWSEnv = Environment.GetEnvironmentVariable("AWSEnv");
        private int MAX_RETRY = 3;
        private const int DELAY_SECONDS_FOR_CHECK_DISCREPANCIE = 900;

        public async Task FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)
        {
            KeySysLambdaContext keysysContext = null;
            try
            {
                keysysContext = BaseFunctionHandler(context);
                if (string.IsNullOrWhiteSpace(DeviceRatePlanChangeQueueUrl))
                {
                    DeviceRatePlanChangeQueueUrl = context.ClientContext.Environment["DeviceRatePlanChangeQueueUrl"];
                    DeviceSyncQueueUrl = context.ClientContext.Environment["DeviceSyncQueueUrl"];
                    AWSEnv = context.ClientContext.Environment["AWSEnv"];
                    JasperEnqueueRatePlanChangeUrl = context.ClientContext.Environment["JasperEnqueueRatePlanChangeUrl"];
                }

                await ProcessEvent(keysysContext, sqsEvent);
            }
            catch (Exception ex)
            {
                LogInfo(keysysContext, "EXCEPTION", ex.Message);
            }

            CleanUp(keysysContext);
        }

        private async Task ProcessEvent(KeySysLambdaContext context, SQSEvent sqsEvent)
        {
            LogInfo(context, "SUB", "ProcessEvent");
            if (sqsEvent.Records.Count > 0)
            {
                if (sqsEvent.Records.Count == 1)
                {
                    await ProcessEventRecord(context, sqsEvent.Records[0]);
                }
                else
                {
                    LogInfo(context, "EXCEPTION", $"Expected a single message, received {sqsEvent.Records.Count}");
                }
            }
        }

        private async Task ProcessEventRecord(KeySysLambdaContext context, SQSEvent.SQSMessage message)
        {
            LogInfo(context, "SUB", "ProcessEventRecord");

            if (message.MessageAttributes.ContainsKey("IsSendDiscrepancieMail") && Convert.ToBoolean(message.MessageAttributes["IsSendDiscrepancieMail"].StringValue))
            {
                await ProcessSendDiscrepanciesMail(context, message);
            }
            else
            {
                if (message.MessageAttributes.ContainsKey("InstanceId"))
                {
                    string instanceIdString = message.MessageAttributes["InstanceId"].StringValue;
                    long instanceId = long.Parse(instanceIdString);

                    // get instance
                    OptimizationInstance instance = GetInstance(context, instanceId);

                    if (!instance.ServiceProviderId.HasValue)
                    {
                        LogInfo(context, "EXCEPTION", $"Missing service provider id for instance {instanceId}");
                        return;
                    }

                    var serviceProviderId = instance.ServiceProviderId.Value;
                    if (message.MessageAttributes.ContainsKey("SyncedDevices") &&
                        bool.TryParse(message.MessageAttributes["SyncedDevices"].StringValue, out var syncedDevices) &&
                        syncedDevices)
                    {
                        await ProcessInstance(context, instanceId, serviceProviderId, instance.TenantId);
                    }
                    else
                    {
                        await EnqueueDeviceSyncAsync(context, instanceId, serviceProviderId);
                    }
                }
                else
                {
                    LogInfo(context, "EXCEPTION", "No Instance Id provided in message");
                }
            }
        }

        private async Task ProcessSendDiscrepanciesMail(KeySysLambdaContext context, SQSEvent.SQSMessage message)
        {
            LogInfo(context, "INFO", "Process Discrepancies Rate Plan");
            var retryCount = 0;
            var serviceProviderId = 0;
            var instanceId = 0;
            var listWinningQueue = new List<string>();
            var outOfSyncRatePlanChanges = new List<RatePlanChange>();
            var jasperAuthentication = JasperCommon.GetJasperAuthenticationInformation(context.CentralDbConnectionString, serviceProviderId);

            if (message.MessageAttributes.ContainsKey("RetryCount"))
            {
                retryCount = Convert.ToInt32(message.MessageAttributes["RetryCount"].StringValue);
            }
            if (message.MessageAttributes.ContainsKey("ListWinningQueueId"))
            {
                listWinningQueue = message.MessageAttributes["ListWinningQueueId"].StringValue.Split(',').ToList();
            }
            if (message.MessageAttributes.ContainsKey("ServiceProviderId"))
            {
                serviceProviderId = Convert.ToInt32(message.MessageAttributes["ServiceProviderId"].StringValue);
            }
            if (message.MessageAttributes.ContainsKey("InstanceId"))
            {
                instanceId = Convert.ToInt32(message.MessageAttributes["InstanceId"].StringValue);
            }

            LogInfo(context, "INFO", $"listWinningQueue: {message.MessageAttributes["ListWinningQueueId"].StringValue}");
            LogInfo(context, "INFO", $"InstanceId: {instanceId}");
            LogInfo(context, "INFO", $"ServiceProviderId: {serviceProviderId}");
            LogInfo(context, "INFO", $"retryCount: {retryCount}");

            //check rate plan process
            var ratePlanChangeProgressCompleted = RatePlanChangeProgressCompleted(context, instanceId, serviceProviderId);
            LogInfo(context, "INFO", $"Rate Plan Change Progress Completed: {ratePlanChangeProgressCompleted.ToString()}");

            outOfSyncRatePlanChanges = GetOutOfSyncRatePlanChanges(context, listWinningQueue, serviceProviderId);


            if (jasperAuthentication != null && jasperAuthentication.WriteIsEnabled && ratePlanChangeProgressCompleted)
            {
                //in case of WriteIsEnabled is enable and process completed. Check Discrepancies Rate Pland
                await SendRatePlanDiscrepanciesMail(context, serviceProviderId, outOfSyncRatePlanChanges);
            }
            else
            {
                if (retryCount <= MAX_RETRY && outOfSyncRatePlanChanges.Count > 0)
                {
                    retryCount++;
                    await EnqueueRatePlanChangesSqs(context, instanceId, serviceProviderId, true, message.MessageAttributes["ListWinningQueueId"].StringValue, retryCount);
                }
                else
                {
                    await SendRatePlanDiscrepanciesMail(context, serviceProviderId, outOfSyncRatePlanChanges);
                }
            }
        }
        private List<RatePlanChange> GetOutOfSyncRatePlanChanges(KeySysLambdaContext context, List<string> listWinningQueue, int serviceProviderId)
        {
            var outOfSyncRatePlanChanges = new List<RatePlanChange>();
            foreach (var winningQueueId in listWinningQueue)
            {
                var ratePlanChanges = GetRatePlanChangesDb(context, long.Parse(winningQueueId), serviceProviderId);
                outOfSyncRatePlanChanges.AddRange(ratePlanChanges
                    .Where(change => change.DeviceRatePlan != change.JasperDeviceStagingRatePlan
                                     && !string.IsNullOrWhiteSpace(change.JasperDeviceStagingRatePlan))
                    .ToList());
            }
            return outOfSyncRatePlanChanges;
        }
        private bool RatePlanChangeProgressCompleted(KeySysLambdaContext context, int instanceId, int serviceProviderId)
        {
            var deviceCount = 0;
            var deviceProcessCount = 0;
            var factory = new PolicyFactory(context.logger);
            var sqlRetryPolicy = factory.GetSqlRetryPolicy(CommonConstants.NUMBER_OF_RETRIES);
            var parameters = new List<SqlParameter>()
            {
                new SqlParameter(CommonSQLParameterNames.INSTANCE_ID, instanceId),
                new SqlParameter(CommonSQLParameterNames.SERVICE_PROVIDER_ID_PASCAL_CASE, serviceProviderId)
            };

            var record = sqlRetryPolicy.Execute(() =>
                SqlQueryHelper.ExecuteStoredProcedureWithListResult(ParameterizedLog(context.logger),
                    context.CentralDbConnectionString,
                    SQLConstant.StoredProcedureName.OPTIMIZATION_RATE_PLAN_CHANGE_PROGRESS_COMPLETED,
                    (dataReader) => ReadRatePlanChangeProgressCompletedFromReader(dataReader),
                    parameters,
                    SQLConstant.ShortTimeoutSeconds)).FirstOrDefault();

            if (record != null)
            {
                deviceCount = record.DeviceQueueCount;
                deviceProcessCount = record.DeviceProcessedCount;
            }

            return deviceCount == deviceProcessCount;
        }
        private async Task ProcessInstance(KeySysLambdaContext context, long instanceId, int serviceProviderId, int tenantId)
        {
            LogInfo(context, CommonConstants.SUB, $"({instanceId},{serviceProviderId},{tenantId})");

            // get comm groups
            var commGroups = GetCommGroups(context, instanceId);
            var outOfSyncRatePlanChanges = new List<RatePlanChange>();
            var listWinningQueueId = new List<long>();
            // cleanup each comm group
            for (var index = 0; index < commGroups.Count; index++)
            {
                var commGroup = commGroups[index];
                // get winning queue for each comm group
                long winningQueueId = GetWinningQueueId(context, commGroup.Id, serviceProviderId);
                listWinningQueueId.Add(winningQueueId);
                // enqueue devices that need rate plan changes
                var ratePlanChanges = GetRatePlanChangesDb(context, winningQueueId, serviceProviderId);

                // only email real discrepancies - if a device is not in the sync results, it shouldn't have had any
                // relevant changes within the billing period
                outOfSyncRatePlanChanges.AddRange(ratePlanChanges
                    .Where(change => change.DeviceRatePlan != change.JasperDeviceStagingRatePlan
                                     && !string.IsNullOrWhiteSpace(change.JasperDeviceStagingRatePlan))
                    .ToList());

                await EnqueueRatePlanChangesAsync(context, serviceProviderId, winningQueueId, index == 0 ? instanceId : (long?)null, tenantId, ratePlanChanges);
            }

            if (outOfSyncRatePlanChanges.Any())
            {
                //trigger lambda to retry 3 times
                await EnqueueRatePlanChangesSqs(context, instanceId, serviceProviderId, true, string.Join(",", listWinningQueueId), 0);
            }
        }

        private async Task SendRatePlanDiscrepanciesMail(KeySysLambdaContext context, int serviceProviderId, List<RatePlanChange> outOfSyncRatePlanChanges)
        {
            LogInfo(context, "SUB", $"outOfSyncRatePlanChanges: {outOfSyncRatePlanChanges.Count}");
            if (outOfSyncRatePlanChanges.Any())
            {
                var serviceProvider = ServiceProviderCommon.GetServiceProvider(context.CentralDbConnectionString, serviceProviderId);
                var serviceProviderName = serviceProvider.DisplayName;

                int tenantId = context.TenantRepo.GetTenantIdByServiceProviderId(serviceProviderId);
                string tenantName = context.TenantRepo.GetTenantNameByTenantId(tenantId);

                var subject = $"Rate plan discrepancy found in AMOP for {serviceProviderName} ({tenantName})";

                if (!context.IsProduction)
                {
                    subject += $" ({AWSEnv})";
                }
                var body = BuildRatePlanDiscrepancyBody(serviceProviderName, outOfSyncRatePlanChanges, serviceProvider);
                await SendEmailAsync(context, subject, body);
            }
        }

        private static long GetWinningQueueId(KeySysLambdaContext context, long commGroupId, int serviceProviderId)
        {
            LogInfo(context, CommonConstants.SUB, $"({commGroupId})");

            var factory = new PolicyFactory(context.logger);
            var sqlRetryPolicy = factory.GetSqlRetryPolicy(CommonConstants.NUMBER_OF_RETRIES);
            var parameters = new List<SqlParameter>()
            {
                new SqlParameter(CommonSQLParameterNames.COMM_GROUP_ID, commGroupId),
                new SqlParameter(CommonSQLParameterNames.SERVICE_PROVIDER_ID_PASCAL_CASE, serviceProviderId)
            };
            var queuesToProcess = sqlRetryPolicy.Execute(() =>
                SqlQueryHelper.ExecuteStoredProcedureWithIntResult(ParameterizedLog(context.logger),
                    context.CentralDbConnectionString,
                    SQLConstant.StoredProcedureName.OPTIMIZATION_GET_WINNING_QUEUE_ID,
                    parameters,
                    SQLConstant.ShortTimeoutSeconds));

            return queuesToProcess;
        }

        public static Action<string, string> ParameterizedLog(IKeysysLogger logger)
        {
            return (type, message) => logger.LogInfo(type, message);
        }

        private async Task EnqueueRatePlanChangesAsync(KeySysLambdaContext context, int serviceProviderId, long queueId, long? instanceId,
            int tenantId, IEnumerable<RatePlanChange> ratePlanChanges)
        {
            LogInfo(context, CommonConstants.SUB, $"(..,{serviceProviderId},{queueId},{instanceId},..)");
            var ratePlanChangesToEnqueue = ratePlanChanges
                .Where(change => string.IsNullOrWhiteSpace(change.JasperDeviceStagingRatePlan) || (change.JasperDeviceStagingRatePlan != change.CarrierRatePlanCode &&
                                                                                change.JasperDeviceStagingRatePlan != change.CustomerRatePlanCode))
                .ToList();
            var serviceProvider = ServiceProviderCommon.GetServiceProvider(context.CentralDbConnectionString, serviceProviderId);
            switch (serviceProvider.IntegrationId)
            {
                case (int)IntegrationType.Telegence:
                    EnqueueMobilityRatePlanChanges(context, ratePlanChangesToEnqueue);
                    break;
                case (int)IntegrationType.Jasper:
                case (int)IntegrationType.TMobileJasper:
                case (int)IntegrationType.POD19:
                    EnqueueRatePlanChanges(context, ratePlanChangesToEnqueue);
                    break;
            }

            // get group count
            int groupCount = GetGroupCount(context, queueId, serviceProviderId);
            if (groupCount > 0)
            {
                await SendProcessMessagesToQueue(context, serviceProviderId, queueId, groupCount, instanceId, tenantId);
            }
            else
            {
                // still queue group 0 even if that's the max group count
                await EnqueueJasperAWSUpdateDeviceRatePlanSqs(context, serviceProviderId, queueId, 0, instanceId, tenantId);
            }
        }

        private static BodyBuilder BuildRatePlanDiscrepancyBody(string serviceProvider, IEnumerable<RatePlanChange> ratePlanChanges, Altaworx.AWS.Core.ServiceProvider serviceProviderDetail)
        {
            StringBuilder sb = new StringBuilder();
            var deviceColumnName = CommonColumnNames.ICCID;
            if (serviceProviderDetail.IntegrationId == (int)IntegrationType.Telegence)
            {
                deviceColumnName = CommonColumnNames.SubscriberNumberColumnName;
            }
            sb.AppendLine("<html>");
            sb.AppendLine($"<div>One or more devices has a rate plan in AMOP that differs from rate plan in carrier '{serviceProvider}'. Rate plan changes will still be queued.</div>");
            sb.AppendLine("<br/>");
            sb.AppendLine("<div>");
            sb.AppendLine(@"<table border=""0"" cellpadding=""0"" cellspacing=""0"" height=""100%"" width=""100%"">");
            sb.AppendLine(
                $@"<thead>
                    <tr>
                        <th align=""left"" valign=""top"">{deviceColumnName}</th>
                        <th align=""left"" valign=""top"">AMOP Rate Plan</th>
                        <th align=""left"" valign=""top"">Carrier Rate Plan</th>
                    </tr>
                </thead>");
            sb.AppendLine("<tbody>");
            foreach (var ratePlanChange in ratePlanChanges)
            {
                var deviceColumnValue = ratePlanChange.ICCID;
                if (serviceProviderDetail.IntegrationId == (int)IntegrationType.Telegence)
                {
                    deviceColumnValue = ratePlanChange.MSISDN;
                }
                var carrierRatePlan = !string.IsNullOrEmpty(ratePlanChange.JasperDeviceStagingRatePlan) ? ratePlanChange.JasperDeviceStagingRatePlan : "NOT AVAILABLE";
                sb.AppendLine(
                    @$"<tr>
                        <td align=""left"" valign=""top"">{deviceColumnValue}</td>
                        <td align=""left"" valign=""top"">{ratePlanChange.DeviceRatePlan}</td>
                        <td align=""left"" valign=""top"">{carrierRatePlan}</td>
                    </tr>");
            }
            sb.AppendLine("</tbody>");
            sb.AppendLine("</table>");
            sb.AppendLine("</div>");
            sb.AppendLine("</html>");

            var body = sb.ToString();

            return new BodyBuilder
            {
                HtmlBody = body,
                TextBody = body
            };
        }

        private async Task SendEmailAsync(KeySysLambdaContext context, string subject, BodyBuilder bodyBuilder)
        {
            LogInfo(context, "SUB", "SendEmailAsync()");
            using (var client = new AmazonSimpleEmailServiceClient(AwsSesCredentials(context), RegionEndpoint.USEast1))
            {
                var message = new MimeMessage();
                message.From.Add(MailboxAddress.Parse(context.OptimizationSettings.FromEmailAddress));
                var recipientAddressList = context.OptimizationSettings.ToEmailAddresses.Split(';').ToList();
                foreach (var recipientAddress in recipientAddressList)
                {
                    message.To.Add(MailboxAddress.Parse(recipientAddress));
                }

                var bccAddressList = context.OptimizationSettings.BccEmailAddresses?.Split(';').ToList() ?? new List<string>();
                foreach (var bccAddress in bccAddressList)
                {
                    message.Bcc.Add(MailboxAddress.Parse(bccAddress));
                }

                message.Subject = subject;
                message.Body = bodyBuilder.ToMessageBody();
                var stream = new System.IO.MemoryStream();
                message.WriteTo(stream);

                var sendRequest = new SendRawEmailRequest
                {
                    RawMessage = new RawMessage(stream)
                };
                try
                {
                    var response = await client.SendRawEmailAsync(sendRequest);
                    LogInfo(context, "RESPONSE STATUS", $"{response.HttpStatusCode:d} {response.HttpStatusCode:g}");
                }
                catch (Exception ex)
                {
                    LogInfo(context, "EXCEPTION", "Error Sending Email: " + ex.Message);
                }
            }
        }

        private void EnqueueRatePlanChanges(KeySysLambdaContext context, IEnumerable<RatePlanChange> ratePlanChanges)
        {
            var table = new DataTable();
            table.Columns.Add("Id");
            table.Columns.Add("OptimizationDeviceResultId");
            table.Columns.Add("IsProcessed");
            table.Columns.Add("CreatedBy");
            table.Columns.Add("CreatedDate");
            table.Columns.Add("GroupNumber");

            foreach (var ratePlanChange in ratePlanChanges)
            {
                var dr = table.NewRow();
                dr[1] = ratePlanChange.OptimizationDeviceResultId;
                dr[2] = ratePlanChange.IsProcessed;
                dr[3] = ratePlanChange.CreatedBy;
                dr[4] = ratePlanChange.CreatedDate;
                dr[5] = ratePlanChange.GroupNumber;
                table.Rows.Add(dr);
            }

            SqlBulkCopy(context, context.CentralDbConnectionString, table, "OptimizationDeviceResult_RatePlanQueue");
        }

        private void EnqueueMobilityRatePlanChanges(KeySysLambdaContext context, IEnumerable<RatePlanChange> ratePlanChanges)
        {
            var table = new DataTable();
            table.Columns.Add(CommonColumnNames.Id);
            table.Columns.Add(CommonColumnNames.OptimizationMobilityDeviceResultId);
            table.Columns.Add(CommonColumnNames.IsProcessed);
            table.Columns.Add(CommonColumnNames.CreatedBy);
            table.Columns.Add(CommonColumnNames.CreatedDate);
            table.Columns.Add(CommonColumnNames.GroupNumber);

            foreach (var ratePlanChange in ratePlanChanges)
            {
                var dr = table.NewRow();
                dr[CommonColumnNames.OptimizationMobilityDeviceResultId] = ratePlanChange.OptimizationDeviceResultId;
                dr[CommonColumnNames.IsProcessed] = ratePlanChange.IsProcessed;
                dr[CommonColumnNames.CreatedBy] = ratePlanChange.CreatedBy;
                dr[CommonColumnNames.CreatedDate] = ratePlanChange.CreatedDate;
                dr[CommonColumnNames.GroupNumber] = ratePlanChange.GroupNumber;
                table.Rows.Add(dr);
            }

            SqlBulkCopy(context, context.CentralDbConnectionString, table, DatabaseTableNames.OPTIMIZATION_MOBILITY_DEVICE_RESULT_RATE_PLAN_QUEUE);
        }

        private static ICollection<RatePlanChange> GetRatePlanChangesDb(KeySysLambdaContext context, long queueId, int serviceProviderId)
        {
            LogInfo(context, CommonConstants.SUB, $"(...,{queueId},{serviceProviderId})");
            var factory = new PolicyFactory(context.logger);
            var sqlRetryPolicy = factory.GetSqlRetryPolicy(CommonConstants.NUMBER_OF_RETRIES);
            var parameters = new List<SqlParameter>()
            {
                new SqlParameter(CommonSQLParameterNames.QUEUE_ID, queueId),
                new SqlParameter(CommonSQLParameterNames.SERVICE_PROVIDER_ID_PASCAL_CASE, serviceProviderId)
            };
            using (var jasperCn = new SqlConnection(context.GeneralProviderSettings.JasperDbConnectionString))
            {
                parameters.Add(new SqlParameter(CommonSQLParameterNames.JASPER_DB_NAME, jasperCn.Database));
            }

            var records = sqlRetryPolicy.Execute(() =>
                SqlQueryHelper.ExecuteStoredProcedureWithListResult(ParameterizedLog(context.logger),
                    context.CentralDbConnectionString,
                    SQLConstant.StoredProcedureName.OPTIMIZATION_GET_RATE_PLAN_CHANGES,
                    (dataReader) => ReadRatePlanChangesFromReader(dataReader),
                    parameters,
                    SQLConstant.ShortTimeoutSeconds));

            return records;
        }

        private static RatePlanChange ReadRatePlanChangesFromReader(SqlDataReader dataReader)
        {
            var columns = dataReader.GetColumnsFromReader();
            return new RatePlanChange()
            {
                ICCID = dataReader.StringFromReader(columns, CommonColumnNames.ICCID),
                MSISDN = dataReader.StringFromReader(columns, CommonColumnNames.MSISDN),
                OptimizationDeviceResultId = dataReader.LongFromReader(columns, CommonColumnNames.OptimizationDeviceResultId),
                IsProcessed = dataReader.BooleanFromReader(columns, CommonColumnNames.IsProcessed),
                CreatedBy = dataReader.StringFromReader(columns, CommonColumnNames.CreatedBy),
                CreatedDate = dataReader.DateTimeFromReader(columns, CommonColumnNames.CreatedDate),
                GroupNumber = dataReader.IntFromReader(columns, CommonColumnNames.GroupNumber),
                DeviceRatePlan = dataReader.StringFromReader(columns, CommonColumnNames.DeviceRatePlan),
                CarrierRatePlanCode = dataReader.StringFromReader(columns, CommonColumnNames.CarrierRatePlanCode, null),
                CustomerRatePlanCode = dataReader.StringFromReader(columns, CommonColumnNames.CustomerRatePlanCode, null),
                JasperDeviceStagingRatePlan = dataReader.StringFromReader(columns, CommonColumnNames.DeviceStagingRatePlan, null)
            };
        }

        private static RatePlanChangeProgressCompleted ReadRatePlanChangeProgressCompletedFromReader(SqlDataReader dataReader)
        {
            var columns = dataReader.GetColumnsFromReader();
            return new RatePlanChangeProgressCompleted()
            {
                DeviceQueueCount = dataReader.IntFromReader(columns, CommonColumnNames.DeviceQueueCount),
                DeviceProcessedCount = dataReader.IntFromReader(columns, CommonColumnNames.DeviceProcessedCount),
            };
        }

        private static int GetGroupCount(KeySysLambdaContext context, long queueId, int serviceProviderId)
        {
            LogInfo(context, CommonConstants.SUB, $"({queueId})");

            var factory = new PolicyFactory(context.logger);
            var sqlRetryPolicy = factory.GetSqlRetryPolicy(CommonConstants.NUMBER_OF_RETRIES);
            var parameters = new List<SqlParameter>()
            {
                new SqlParameter(CommonSQLParameterNames.QUEUE_ID, queueId),
                new SqlParameter(CommonSQLParameterNames.SERVICE_PROVIDER_ID_PASCAL_CASE, serviceProviderId)
            };
            var groupCount = sqlRetryPolicy.Execute(() =>
                SqlQueryHelper.ExecuteStoredProcedureWithIntResult(ParameterizedLog(context.logger),
                    context.CentralDbConnectionString,
                    SQLConstant.StoredProcedureName.OPTIMIZATION_GET_GROUP_COUNT,
                    parameters,
                    SQLConstant.ShortTimeoutSeconds));

            return groupCount;
        }

        private async Task SendProcessMessagesToQueue(KeySysLambdaContext context, int serviceProviderId, long queueId, int groupCount, long? instanceId, int tenantId)
        {
            LogInfo(context, "SUB", $"SendProcessMessagesToQueue({serviceProviderId},{queueId},{groupCount},{instanceId},{tenantId})");
            for (int iGroup = 0; iGroup <= groupCount - 1; iGroup++)
            {
                await EnqueueJasperAWSUpdateDeviceRatePlanSqs(context, serviceProviderId, queueId, iGroup, iGroup == 0 ? instanceId : null, tenantId);
            }
        }

        private async Task EnqueueJasperAWSUpdateDeviceRatePlanSqs(KeySysLambdaContext context, int serviceProviderId, long queueId, int groupNumber, long? instanceId, int tenantId)
        {
            LogInfo(context, "SUB", $"EnqueueRatePlanChangesSqs(...,{serviceProviderId},{queueId},{groupNumber},{instanceId},{tenantId})");
            LogInfo(context, "DeviceRatePlanChangeQueueUrl", DeviceRatePlanChangeQueueUrl);

            if (string.IsNullOrWhiteSpace(DeviceRatePlanChangeQueueUrl))
            {
                return; // to be able to skip enqueuing messages in a test
            }

            var awsCredentials = AwsCredentials(context);
            using (var client = new AmazonSQSClient(awsCredentials, RegionEndpoint.USEast1))
            {
                var requestMsgBody = $"Queue to work is {queueId}";
                var request = new SendMessageRequest
                {
                    DelaySeconds = DefaultDelaySeconds,
                    MessageAttributes = new Dictionary<string, MessageAttributeValue>
                    {
                        {
                            "QueueId", new MessageAttributeValue
                            { DataType = "String", StringValue = queueId.ToString()}
                        },
                        {
                            "GroupNumber", new MessageAttributeValue
                            { DataType = "String", StringValue = groupNumber.ToString()}
                        },
                        {
                            "ServiceProviderId", new MessageAttributeValue
                            { DataType = "String", StringValue = serviceProviderId.ToString()}
                        },
                        {
                            "TenantId", new MessageAttributeValue
                            { DataType = "String", StringValue = tenantId.ToString()}
                        }
                    },
                    MessageBody = requestMsgBody,
                    QueueUrl = DeviceRatePlanChangeQueueUrl
                };

                if (instanceId.HasValue)
                {
                    request.MessageAttributes.Add("InstanceId",
                        new MessageAttributeValue { DataType = "String", StringValue = instanceId.Value.ToString() });
                }

                var response = await client.SendMessageAsync(request);
                if (((int)response.HttpStatusCode < 200) || ((int)response.HttpStatusCode > 299))
                {
                    LogInfo(context, "EXCEPTION", $"Error enqueuing message to {DeviceRatePlanChangeQueueUrl}: {response.HttpStatusCode:d} {response.HttpStatusCode:g}");
                }
            }
        }

        private async Task EnqueueDeviceSyncAsync(KeySysLambdaContext context, long instanceId, int serviceProviderId)
        {
            LogInfo(context, "SUB", $"EnqueueDeviceSyncAsync(...,{instanceId},{serviceProviderId})");
            LogInfo(context, "DeviceSyncQueueURL", DeviceSyncQueueUrl);

            if (string.IsNullOrWhiteSpace(DeviceSyncQueueUrl))
            {
                return; // to be able to skip enqueuing messages in a test
            }

            var awsCredentials = AwsCredentials(context);
            using (var client = new AmazonSQSClient(awsCredentials, RegionEndpoint.USEast1))
            {
                var request = new SendMessageRequest
                {
                    DelaySeconds = DefaultDelaySeconds,
                    MessageAttributes = new Dictionary<string, MessageAttributeValue>
                    {
                        {
                            "PageNumber",
                            new MessageAttributeValue
                            {
                                DataType = "Number", StringValue = "1"
                            }
                        },
                        {
                            "LastSyncDate",
                            new MessageAttributeValue
                            {
                                DataType = "String", StringValue = DateTime.UtcNow.AddMonths(-1).AddDays(-1).ToString()
                            }
                        },
                        {
                            "ServiceProviderId", new MessageAttributeValue
                            {
                                DataType = "Number", StringValue = serviceProviderId.ToString()
                            }
                        },
                        {
                            "NextStep",
                            new MessageAttributeValue
                            {
                                DataType = "Number", StringValue = ((int)JasperDeviceSyncNextStep.UpdateDeviceRatePlan).ToString()
                            }
                        },
                        {
                            "OptimizationInstanceId",
                            new MessageAttributeValue
                            {
                                DataType = "Number", StringValue = instanceId.ToString()
                            }
                        }
                    },
                    MessageBody = "NOT USED",
                    QueueUrl = DeviceSyncQueueUrl
                };

                var response = await client.SendMessageAsync(request);
                if (((int)response.HttpStatusCode < 200) || ((int)response.HttpStatusCode > 299))
                {
                    LogInfo(context, "EXCEPTION", $"Error enqueuing message to {DeviceRatePlanChangeQueueUrl}: {response.HttpStatusCode:d} {response.HttpStatusCode:g}");
                }
            }
        }
        private async Task EnqueueRatePlanChangesSqs(KeySysLambdaContext context, long instanceId, int serviceProviderId, bool isSendDiscrepancieMail = false, string listWinningQueueId = "", int retryCount = 0)
        {
            LogInfo(context, "AltaworxJasperAWSEnqueueRatePlanChanges ", JasperEnqueueRatePlanChangeUrl);

            if (string.IsNullOrWhiteSpace(JasperEnqueueRatePlanChangeUrl))
            {
                return; // to be able to skip enqueuing messages in a test
            }

            var awsCredentials = AwsCredentials(context);
            using (var client = new AmazonSQSClient(awsCredentials, RegionEndpoint.USEast1))
            {
                var requestMsgBody = $"Retry AltaworxJasperAWSEnqueueRatePlanChanges : {retryCount}";
                var request = new SendMessageRequest
                {
                    DelaySeconds = DefaultDelaySeconds,
                    MessageAttributes = new Dictionary<string, MessageAttributeValue>
                    {
                        {
                            "InstanceId", new MessageAttributeValue
                            { DataType = "String", StringValue = instanceId.ToString()}
                        },
                        {
                            "ServiceProviderId", new MessageAttributeValue
                            { DataType = "String", StringValue = serviceProviderId.ToString()}
                        }
                    },
                    MessageBody = requestMsgBody,
                    QueueUrl = JasperEnqueueRatePlanChangeUrl
                };

                if (isSendDiscrepancieMail && listWinningQueueId.Length > 0)
                {
                    request.MessageAttributes.Add("IsSendDiscrepancieMail",
                        new MessageAttributeValue { DataType = "String", StringValue = isSendDiscrepancieMail.ToString() });
                    request.MessageAttributes.Add("RetryCount",
                        new MessageAttributeValue { DataType = "String", StringValue = retryCount.ToString() });
                    request.MessageAttributes.Add("ListWinningQueueId",
                       new MessageAttributeValue { DataType = "String", StringValue = listWinningQueueId });
                    request.DelaySeconds = DELAY_SECONDS_FOR_CHECK_DISCREPANCIE;
                }

                var response = await client.SendMessageAsync(request);
                if (((int)response.HttpStatusCode < 200) || ((int)response.HttpStatusCode > 299))
                {
                    LogInfo(context, "EXCEPTION", $"Error enqueuing message to {DeviceRatePlanChangeQueueUrl}: {response.HttpStatusCode:d} {response.HttpStatusCode:g}");
                }
            }
        }
    }
}
