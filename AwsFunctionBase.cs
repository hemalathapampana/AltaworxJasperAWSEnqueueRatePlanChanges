using Amazon.Lambda.Core;
using Amop.Core.Logger;
using Amop.Core.Models.Settings;
using Amop.Core.Repositories.Environment;
using Amop.Core.Services.Base64Service;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text.RegularExpressions;
using Microsoft.Data.SqlClient;
using Altaworx.AWS.Core.Models;
// for logging code information
using System.Runtime.CompilerServices;
// to use FormatLogStringObject 
using Altaworx.AWS.Core.Helpers;
using Altaworx.AWS.Core.Helpers.Constants;
using Altaworx.AWS.Core.Models.Jasper;
using Amop.Core.Constants;
using Polly;
using Renci.SshNet.Security;
using System.Linq;

namespace Altaworx.AWS.Core
{
    public class AwsFunctionBase
    {
        public static void LogInfo(KeySysLambdaContext context, string desc, object detail = null,
                        [CallerFilePath] string file = "",
                        [CallerLineNumber] int line = 0,
                        [CallerMemberName] string functionName = "")
        {
            context.LogInfo(desc, StringHelper.FormatLogStringObject(desc, detail ?? "", file, line, functionName));
        }

        private void LoadOUSettings(KeySysLambdaContext context)
        {
            context.LoadOUSettings();
        }

        public KeySysLambdaContext BaseFunctionHandler(ILambdaContext context, bool skipOUSpecificLogic = false)
        {
            // All of the existing calls to this metod as of June 5th, 2019 DO run OU specific initialization.
            KeySysLambdaContext keySysLambdaContext = new KeySysLambdaContext(context, skipOUSpecificLogic);
            return keySysLambdaContext;
        }

        public AmopLambdaContext BaseAmopFunctionHandler(ILambdaContext context, bool skipOUSpecificLogic = false)
        {
            AmopLambdaContext lambdaContext = new AmopLambdaContext(context, skipOUSpecificLogic);
            return lambdaContext;
        }

        public virtual void CleanUp(KeySysLambdaContext context)
        {
            context.CleanUp();
        }

        public string GetCustomerName(KeySysLambdaContext context, Guid customerId)
        {
            LogInfo(context, "SUB", $"GetCustomerName({customerId})");
            using (var Conn = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var Cmd = new SqlCommand("SELECT TOP 1 ISNULL(CustomerName, RevCustomerId) AS CustomerName FROM RevCustomer WHERE Id = @customerId", Conn))
                {
                    Cmd.CommandType = CommandType.Text;
                    Cmd.Parameters.AddWithValue("@customerId", customerId);
                    Conn.Open();

                    SqlDataReader rdr = Cmd.ExecuteReader();
                    while (rdr.Read())
                    {
                        return rdr[0].ToString();
                    }

                    Conn.Close();
                }
            }

            return string.Empty;
        }

        public OptimizationInstance GetInstance(KeySysLambdaContext context, long instanceId)
        {
            try
            {
                LogInfo(context, "SUB", $"GetInstance({instanceId})");
                OptimizationInstance queue = new OptimizationInstance();
                using (var Conn = new SqlConnection(context.CentralDbConnectionString))
                {
                    using (var Cmd = Conn.CreateCommand())
                    {
                        Cmd.CommandType = CommandType.StoredProcedure;
                        Cmd.CommandText = Amop.Core.Constants.SQLConstant.StoredProcedureName.GET_OPTIMIZATION_INSTANCE;
                        Cmd.Parameters.AddWithValue("@instanceId", instanceId);
                        Conn.Open();

                        SqlDataReader rdr = Cmd.ExecuteReader();
                        while (rdr.Read())
                        {
                            queue = InstanceFromReader(rdr);
                        }
                    }
                }

                return queue;
            }
            catch (SqlException ex)
            {
                LogInfo(context, LogTypeConstant.Exception, string.Format(LogCommonStrings.EXCEPTION_WHEN_EXECUTING_SQL_COMMAND, ex.Message));
            }
            catch (InvalidOperationException ex)
            {
                LogInfo(context, LogTypeConstant.Exception, string.Format(LogCommonStrings.EXCEPTION_WHEN_CONNECTING_DATABASE, ex.Message));
            }
            catch (Exception ex)
            {
                LogInfo(context, LogTypeConstant.Exception, ex.Message);
            }
            return null;
        }

        private OptimizationInstance InstanceFromReader(SqlDataReader rdr)
        {
            return new OptimizationInstance()
            {
                Id = long.Parse(rdr[CommonColumnNames.Id].ToString()),
                RunStatusId = int.Parse(rdr[CommonColumnNames.RunStatusId].ToString()),
                RunStartTime = !rdr.IsDBNull(rdr.GetOrdinal(CommonColumnNames.RunStartTime)) ? DateTime.Parse(rdr[CommonColumnNames.RunStartTime].ToString()) : new DateTime?(),
                RunEndTime = !rdr.IsDBNull(rdr.GetOrdinal(CommonColumnNames.RunEndTime)) ? DateTime.Parse(rdr[CommonColumnNames.RunEndTime].ToString()) : new DateTime?(),
                BillingPeriodStartDate = DateTime.Parse(rdr[CommonColumnNames.BillingPeriodStartDate].ToString()),
                BillingPeriodEndDate = DateTime.Parse(rdr[CommonColumnNames.BillingPeriodEndDate].ToString()),
                RevCustomerId = !rdr.IsDBNull(rdr.GetOrdinal(CommonColumnNames.RevCustomerId)) ? Guid.Parse(rdr[CommonColumnNames.RevCustomerId].ToString()) : new Guid?(),
                ServiceProviderId = !rdr.IsDBNull(rdr.GetOrdinal(CommonColumnNames.ServiceProviderId)) ? int.Parse(rdr[CommonColumnNames.ServiceProviderId].ToString()) : new int?(),
                PortalTypeId = !rdr.IsDBNull(rdr.GetOrdinal(CommonColumnNames.PortalTypeId)) ? int.Parse(rdr[CommonColumnNames.PortalTypeId].ToString()) : 0,
                TenantId = !rdr.IsDBNull(rdr.GetOrdinal(CommonColumnNames.TenantId)) ? int.Parse(rdr[CommonColumnNames.TenantId].ToString()) : 0,
                IntegrationAuthenticationId = !rdr.IsDBNull(rdr.GetOrdinal(CommonColumnNames.IntegrationAuthenticationId)) ? int.Parse(rdr[CommonColumnNames.IntegrationAuthenticationId].ToString()) : 0
            };
        }

        public OptimizationQueue GetQueue(KeySysLambdaContext context, long queueId)
        {
            LogInfo(context, "SUB", $"GetQueue({queueId})");
            OptimizationQueue queue = new OptimizationQueue();
            using (var Conn = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var Cmd = new SqlCommand("SELECT Id, InstanceId, CommPlanGroupId FROM OptimizationQueue WHERE Id = @queueId", Conn))
                {
                    Cmd.CommandType = CommandType.Text;
                    Cmd.Parameters.AddWithValue("@queueId", queueId);
                    Conn.Open();

                    SqlDataReader rdr = Cmd.ExecuteReader();
                    while (rdr.Read())
                    {
                        queue = QueueFromReader(rdr);
                    }

                    Conn.Close();
                }
            }

            return queue;
        }

        public int GetSimCardCount(KeySysLambdaContext context, string jasperDbConnectionString)
        {
            LogInfo(context, "SUB", $"GetSimCardCount()");
            int simCardCount = 0;
            using (var Conn = new SqlConnection(jasperDbConnectionString))
            {
                using (var Cmd = new SqlCommand("SELECT COUNT(1) FROM JasperDevice", Conn))
                {
                    Cmd.CommandType = CommandType.Text;
                    Conn.Open();

                    var scalarResult = Cmd.ExecuteScalar();
                    simCardCount = (int)scalarResult;

                    Conn.Close();
                }
            }

            return simCardCount;
        }

        public List<JasperDeviceBatch> GetBatchedJasperSimCardCountByServiceProviderId(KeySysLambdaContext context, int serviceProviderId, int batchSize)
        {
            try
            {
                LogInfo(context, LogTypeConstant.Sub, $"serviceProviderId: {serviceProviderId}, batchSize: {batchSize}");

                var batches = new List<JasperDeviceBatch>();
                using (var connection = new SqlConnection(context.GeneralProviderSettings.JasperDbConnectionString))
                {
                    using (var command = connection.CreateCommand())
                    {
                        command.CommandType = CommandType.StoredProcedure;
                        command.CommandText = Amop.Core.Constants.SQLConstant.StoredProcedureName.usp_GetBatchedJasperSimCardCountByServiceProviderId;
                        command.CommandTimeout = Amop.Core.Constants.SQLConstant.ShortTimeoutSeconds;
                        command.Parameters.AddWithValue("@ServiceProviderId", serviceProviderId);
                        command.Parameters.AddWithValue("@BatchSize", batchSize);
                        connection.Open();

                        using (var reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    var batch = JasperDeviceBatch.FromReader(reader);
                                    batches.Add(batch);
                                }
                            }
                            else
                            {
                                LogInfo(context, LogTypeConstant.Warning, "No device found in Jasper database.");
                            }
                        }
                    }
                }

                return batches;
            }
            catch (SqlException ex)
            {
                LogInfo(context, LogTypeConstant.Exception, $"Exception when executing stored procedure: {ex.Message}, ErrorCode:{ex.ErrorCode}-{ex.Number}, Stack Trace: {ex.StackTrace}");
                throw;
            }
            catch (InvalidOperationException ex)
            {
                LogInfo(context, LogTypeConstant.Exception, $"Exception when connecting to database: {ex.Message}, Stack Trace: {ex.StackTrace}");
                throw;
            }
            catch (Exception ex)
            {
                LogInfo(context, LogTypeConstant.Exception, $"Exception when getting devices in batches: {ex.Message}, Stack Trace: {ex.StackTrace}");
                throw;
            }
        }

        private OptimizationQueue QueueFromReader(SqlDataReader rdr)
        {
            return new OptimizationQueue()
            {
                Id = long.Parse(rdr["Id"].ToString()),
                InstanceId = long.Parse(rdr["InstanceId"].ToString()),
                CommPlanGroupId = long.Parse(rdr["CommPlanGroupId"].ToString())
            };
        }

        public List<OptimizationCommGroup> GetCommGroups(KeySysLambdaContext context, long instanceId)
        {
            LogInfo(context, "SUB", $"GetCommGroups({instanceId})");
            List<OptimizationCommGroup> commGroups = new List<OptimizationCommGroup>();
            using (var Conn = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var Cmd = new SqlCommand("SELECT Id, InstanceId FROM OptimizationCommGroup WHERE InstanceId = @instanceId", Conn))
                {
                    Cmd.CommandType = CommandType.Text;
                    Cmd.Parameters.AddWithValue("@instanceId", instanceId);
                    Conn.Open();

                    SqlDataReader rdr = Cmd.ExecuteReader();
                    while (rdr.Read())
                    {
                        var commGroup = CommGroupFromReader(rdr);
                        commGroups.Add(commGroup);
                    }

                    Conn.Close();
                }
            }

            return commGroups;
        }

        private OptimizationCommGroup CommGroupFromReader(SqlDataReader rdr)
        {
            return new OptimizationCommGroup()
            {
                Id = long.Parse(rdr["Id"].ToString()),
                InstanceId = long.Parse(rdr["InstanceId"].ToString())
            };
        }

        public static void SqlBulkCopy(KeySysLambdaContext context, string connectionString, DataTable table, string tableName, List<SqlBulkCopyColumnMapping> columnMappings = null)
        {
            SqlBulkCopy(context, connectionString, table, tableName, context.logger, columnMappings);
        }

        public static void SqlBulkCopy(KeySysLambdaContext context, string connectionString, DataTable table, string tableName, IKeysysLogger logger, List<SqlBulkCopyColumnMapping> columnMappings = null)
        {
            try
            {
                using (var conn = new SqlConnection(connectionString))
                {
                    // Open connection
                    conn.Open();

                    // Bulk copy the
                    using (SqlBulkCopy bulkCopy = new SqlBulkCopy(conn))
                    {
                        bulkCopy.DestinationTableName = tableName;
                        bulkCopy.BulkCopyTimeout = Amop.Core.Constants.SQLConstant.TimeoutSeconds;
                        bulkCopy.BatchSize = Amop.Core.Constants.SQLConstant.BatchSize;

                        if (columnMappings != null && columnMappings.Count > 0)
                        {
                            foreach (var mapping in columnMappings)
                            {
                                bulkCopy.ColumnMappings.Add(mapping);
                            }
                        }
                        bulkCopy.WriteToServer(table);
                    }
                }
            }
            catch (SqlException ex)
            {
                LogInfo(context, LogTypeConstant.Exception, $"Exception when executing SQL command: {ex.Message}, ErrorCode: {ex.ErrorCode}-{ex.Number}");
            }
            catch (InvalidOperationException ex)
            {
                LogInfo(context, LogTypeConstant.Exception, $"Exception when connecting to database: {ex.Message}");
            }
            catch (Exception ex)
            {
                LogInfo(context, LogTypeConstant.Exception, $"Exception when Bulk Copying Table: {tableName}, {ex.Message}");
            }
        }

        public Amazon.Runtime.BasicAWSCredentials AwsCredentials(KeySysLambdaContext context)
        {
            return AwsCredentials(context.Base64Service, context.GeneralProviderSettings.AWSAccesKeyID, context.GeneralProviderSettings.AWSSecretAccessKey);
        }

        public static Amazon.Runtime.BasicAWSCredentials AwsSesCredentials(KeySysLambdaContext context)
        {
            return AwsCredentials(context.Base64Service, context.GeneralProviderSettings.AWSAccesKeyID_SES, context.GeneralProviderSettings.AWSSecretAccessKey_SES);
        }

        public static Amazon.Runtime.BasicAWSCredentials AwsCredentials(IBase64Service base64Service, string awsAccessKey, string encodedSecretAccessKey)
        {
            return new Amazon.Runtime.BasicAWSCredentials(awsAccessKey, base64Service.Base64Decode(encodedSecretAccessKey));
        }

        public static Amazon.Runtime.BasicAWSCredentials AwsSesCredentials(IBase64Service base64Service, GeneralProviderSettings generalProviderSettings)
        {
            return AwsCredentials(base64Service, generalProviderSettings.AWSAccesKeyID_SES, generalProviderSettings.AWSSecretAccessKey_SES);
        }

        // For logging in generic function
        public static Action<string, string> ParameterizedLog(KeySysLambdaContext context)
        {
            return (type, message) => LogInfo(context, type, message);
        }

        public static string GetStringValueFromEnvironmentVariable(ILambdaContext context, EnvironmentRepository environmentRepo, string key)
        {
            var stringValue = environmentRepo.GetEnvironmentVariable(context, key);
            if (string.IsNullOrWhiteSpace(stringValue))
            {
                throw new InvalidOperationException(string.Format(LogCommonStrings.ENVIRONMENT_VARIABLE_NOT_CONFIGURED, key));
            }
            return stringValue;
        }

        public static long GetLongValueFromEnvironmentVariable(AmopLambdaContext lambdaContext, EnvironmentRepository environmentRepo, string variableKey, long? defaultValue = null)
        {
            var stringValue = environmentRepo.GetEnvironmentVariable(lambdaContext.Context, variableKey);
            var isParseSuccess = long.TryParse(stringValue, out long valueFromEnvironment);
            if (!isParseSuccess || valueFromEnvironment <= 0)
            {
                if (defaultValue != null)
                {
                    LogInfo(lambdaContext, CommonConstants.WARNING, string.Format(LogCommonStrings.INVALID_CONFIGURED_VALUE_FOR_VARIABLE, stringValue, defaultValue));
                    valueFromEnvironment = (long)defaultValue;
                }
                else
                {
                    throw new InvalidOperationException(string.Format(LogCommonStrings.ENVIRONMENT_VARIABLE_NOT_CONFIGURED, variableKey));
                }
            }
            return valueFromEnvironment;
        }

        public static bool GetBooleanValueFromEnvironmentVariable(KeySysLambdaContext lambdaContext, EnvironmentRepository environmentRepo, string variableKey, long? defaultValue = null)
        {
            var stringValue = environmentRepo.GetEnvironmentVariable(lambdaContext.Context, variableKey);
            var isParseSuccess = bool.TryParse(stringValue, out bool valueFromEnvironment);
            if (!isParseSuccess)
            {
                if (defaultValue != null)
                {
                    LogInfo(lambdaContext, CommonConstants.WARNING, string.Format(LogCommonStrings.INVALID_CONFIGURED_VALUE_FOR_VARIABLE, stringValue, defaultValue));
                    valueFromEnvironment = false;
                }
                else
                {
                    throw new InvalidOperationException(string.Format(LogCommonStrings.ENVIRONMENT_VARIABLE_NOT_CONFIGURED, variableKey));
                }
            }
            return valueFromEnvironment;

        }

        public static int GetIntValueFromEnvironmentVariable(AmopLambdaContext lambdaContext, EnvironmentRepository environmentRepo, string variableKey, int? defaultValue = null)
        {
            var MaxPagesPerInstanceStringValue = GetStringValueFromEnvironmentVariable(lambdaContext.Context, environmentRepo, variableKey);
            var isParseSuccess = int.TryParse(MaxPagesPerInstanceStringValue, out int valueFromEnvironment);
            if (!isParseSuccess || valueFromEnvironment <= 0)
            {
                if (defaultValue != null)
                {
                    LogInfo(lambdaContext, CommonConstants.WARNING, string.Format(LogCommonStrings.INVALID_CONFIGURED_VALUE_FOR_VARIABLE, MaxPagesPerInstanceStringValue, defaultValue));
                    valueFromEnvironment = (int)defaultValue;
                }
                else
                {
                    throw new InvalidOperationException(string.Format(LogCommonStrings.ENVIRONMENT_VARIABLE_NOT_CONFIGURED, variableKey));
                }

            }
            return valueFromEnvironment;
        }

        protected static void LogVariableValue(KeySysLambdaContext context, string variableName, object variableValue)
        {
            LogInfo(context, CommonConstants.INFO, $"{variableName}: {variableValue}");
        }
        public Dictionary<string, List<string>> GetFANFilter(KeySysLambdaContext context, int currentServiceProviderId)
        {
            var fanFilter = new Dictionary<string, List<string>>();
            fanFilter.Add("IncludedFANs", new List<string>());
            fanFilter.Add("ExcludedFANs", new List<string>());

            try
            {
                using (SqlConnection connection = new SqlConnection(context.CentralDbConnectionString))
                {
                    connection.Open();

                    using (var sqlCommand = new SqlCommand(@"
                        SELECT SettingKey, SettingValue
                            FROM ServiceProviderSetting
                            WHERE ServiceProviderId = @ServiceProviderId 
                                AND SettingKey IN ('IncludedFANs', 'ExcludedFANs')
                                AND SettingValue IS NOT NULL
                                AND IsDeleted = 0 AND IsActive = 1
                        ",
                        connection))
                    {
                        sqlCommand.Parameters.AddWithValue("@ServiceProviderId", currentServiceProviderId);

                        using (var reader = sqlCommand.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                var key = reader[0].ToString();
                                var value = reader[1].ToString();
                                if (!fanFilter.ContainsKey(key))
                                {
                                    fanFilter.Add(key, new List<string>());
                                }
                                if (!string.IsNullOrEmpty(value))
                                {
                                    var values = Regex.Split(value, @"[,;]").Select(x => x.Trim()).ToList();
                                    fanFilter[key].AddRange(values);
                                }
                            }
                        }
                    }
                }

                return fanFilter;
            }
            catch (Exception ex)
            {
                LogInfo(context, CommonConstants.EXCEPTION, ex.Message + " " + ex.StackTrace);
                return fanFilter;
            }
        }
    }
}
