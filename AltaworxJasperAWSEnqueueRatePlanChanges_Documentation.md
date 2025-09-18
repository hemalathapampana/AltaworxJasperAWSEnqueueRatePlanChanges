# AltaworxJasperAWSEnqueueRatePlanChanges Lambda Documentation

## Overview
The `AltaworxJasperAWSEnqueueRatePlanChanges` lambda function is responsible for enqueuing and managing rate plan changes for Jasper devices in the AMOP (Altaworx Mobile Optimization Platform) system. This function orchestrates the process of synchronizing device rate plans between AMOP and carrier systems, handling discrepancies, and managing retry logic.

---

## 1. Triggers & Scheduling

### Initial SQS Message Triggers
- **Primary Trigger**: The lambda is triggered by SQS messages containing device optimization instance information
- **Message Source**: Messages are typically published by CloudWatch scheduled jobs or other optimization processes
- **Scheduling Pattern**: Fixed daily/weekly schedules based on optimization cycles
- **Message Format**: SQS events with specific message attributes

### Key Message Attributes
The function expects SQS messages with the following attributes:
- `InstanceId`: Optimization instance identifier
- `ServiceProviderId`: Service provider identifier  
- `SyncedDevices`: Boolean indicating if devices have been synchronized
- `IsSendDiscrepancieMail`: Boolean flag for discrepancy email processing
- `RetryCount`: Current retry attempt number
- `ListWinningQueueId`: Comma-separated list of queue IDs for batch processing

---

## 2. Message Handling

### SQS Message Processing
```csharp
public async Task FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)
```

### Message Flow
1. **Single Message Validation**: Expects exactly one SQS message per invocation
2. **Attribute Extraction**: Extracts key attributes (InstanceId, ServiceProviderId, etc.)
3. **Processing Path Determination**:
   - **Discrepancy Mail Path**: If `IsSendDiscrepancieMail` is true
   - **Device Sync Path**: If devices need synchronization first
   - **Rate Plan Processing Path**: If devices are already synchronized

### Continuation Handling
- **Follow-up Messages**: The function enqueues additional SQS messages for continued processing
- **Queue URLs**: Uses environment variables for different queue destinations:
  - `DeviceRatePlanChangeQueueUrl`: For rate plan change processing
  - `DeviceSyncQueueUrl`: For device synchronization
  - `JasperEnqueueRatePlanChangeUrl`: For self-triggering retry logic

---

## 3. Batch & Pagination

### Batch Processing Configuration
- **Batch Size Source**: Retrieved from environment variables and database configuration
- **Group-Based Processing**: Devices are processed in groups using `GroupNumber` attribute
- **Pagination Support**: Handles API pagination through sequential message processing

### Processing Groups
```csharp
private async Task SendProcessMessagesToQueue(KeySysLambdaContext context, int serviceProviderId, long queueId, int groupCount, long? instanceId, int tenantId)
{
    for (int iGroup = 0; iGroup <= groupCount - 1; iGroup++)
    {
        await EnqueueJasperAWSUpdateDeviceRatePlanSqs(context, serviceProviderId, queueId, iGroup, iGroup == 0 ? instanceId : null, tenantId);
    }
}
```

### Batch Size Management
- **Database-Driven**: Batch sizes are determined by stored procedure results
- **Group Count Calculation**: Uses `OPTIMIZATION_GET_GROUP_COUNT` stored procedure
- **API Alignment**: Batch size aligns with carrier API page size limits

---

## 4. Integration Details

### Authentication Management
```csharp
var jasperAuthentication = JasperCommon.GetJasperAuthenticationInformation(context.CentralDbConnectionString, serviceProviderId);
```

### Authentication Sources
- **Primary Table**: `Integration_Authentication` table stores credentials
- **Service Provider Mapping**: Authentication linked via `ServiceProviderId`
- **Dynamic Retrieval**: Credentials fetched per request to ensure freshness
- **Write Permissions**: `WriteIsEnabled` flag controls modification capabilities

### Integration Types Supported
- **Jasper**: Standard Jasper integration
- **TMobileJasper**: T-Mobile specific Jasper integration  
- **POD19**: POD19 integration variant
- **Telegence**: Mobility-specific integration (uses different data structure)

---

## 5. Data Handling & Staging

### Staging Tables
The function works with several staging tables:
- **JasperDeviceUsageStaging**: Temporary device usage data
- **JasperDeviceExportStaging**: Device export staging data
- **JasperUsageByRatePlanStaging**: Rate plan usage staging data

### Data Flow Process
1. **Staging Preparation**: Staging tables are cleared at process start
2. **Data Population**: Fresh data is loaded into staging tables
3. **Rate Plan Comparison**: AMOP rate plans compared with carrier staging data
4. **Discrepancy Detection**: Identifies mismatches between systems
5. **Finalization**: Data moved to permanent AMOP tables via stored procedures

### Key Stored Procedures
- `OPTIMIZATION_GET_RATE_PLAN_CHANGES`: Retrieves rate plan changes to process
- `OPTIMIZATION_GET_WINNING_QUEUE_ID`: Gets the winning queue for processing
- `OPTIMIZATION_GET_GROUP_COUNT`: Calculates processing group count
- `OPTIMIZATION_RATE_PLAN_CHANGE_PROGRESS_COMPLETED`: Checks completion status

---

## 6. Error Handling & Retry

### Retry Configuration
```csharp
private int MAX_RETRY = 3;
private const int DELAY_SECONDS_FOR_CHECK_DISCREPANCIE = 900; // 15 minutes
```

### Polly Integration
- **SQL Retry Policy**: Uses Polly for database operation retries
- **Exponential Backoff**: Implemented through PolicyFactory
- **Retry Attempts**: 3 attempts for transient errors
- **Timeout Handling**: Different timeout configurations for various operations

### Error Recovery
```csharp
if (retryCount <= MAX_RETRY && outOfSyncRatePlanChanges.Count > 0)
{
    retryCount++;
    await EnqueueRatePlanChangesSqs(context, instanceId, serviceProviderId, true, 
        message.MessageAttributes["ListWinningQueueId"].StringValue, retryCount);
}
```

### Failure Scenarios
- **Transient Errors**: Database connection issues, temporary API failures
- **Authentication Failures**: Invalid credentials, expired tokens
- **Rate Plan Mismatches**: Discrepancies between AMOP and carrier systems
- **Processing Timeouts**: Long-running operations exceeding limits

---

## 7. Failed/Unprocessed Records

### Discrepancy Management
```csharp
outOfSyncRatePlanChanges.AddRange(ratePlanChanges
    .Where(change => change.DeviceRatePlan != change.JasperDeviceStagingRatePlan
                     && !string.IsNullOrWhiteSpace(change.JasperDeviceStagingRatePlan))
    .ToList());
```

### Record Tracking
- **Database Logging**: Failed records logged in audit tables
- **Retry Queuing**: Invalid records re-queued for next processing cycle
- **Manual Exclusion**: Capability to manually exclude problematic records
- **Status Tracking**: `IsProcessed` flag tracks processing status

### Recovery Mechanisms
- **Automatic Retry**: Up to 3 automatic retry attempts
- **Email Notifications**: Discrepancies reported via email
- **Manual Intervention**: Support for manual record processing
- **Cycle Continuation**: Next scheduled cycle attempts failed records

---

## 8. Cleanup Processes

### Record Retention
While specific retention policies aren't explicitly defined in this lambda, the system supports:
- **Staging Table Cleanup**: Staging tables cleared at process start
- **Audit Trail Maintenance**: Processing logs maintained for troubleshooting
- **Queue Management**: SQS messages automatically expire based on queue configuration

### Cleanup Operations
```csharp
// Staging table cleanup patterns
public void DeleteStagingTables(string connectionString, int serviceProviderId, IKeysysLogger logger)
public void TruncateStagingTables(string connectionString, IKeysysLogger logger)
```

### Maintenance Activities
- **Staging Data Purging**: Removes temporary processing data
- **Log Rotation**: CloudWatch logs managed by AWS retention policies
- **Queue Message Expiry**: SQS dead letter queue handling for failed messages

---

## 9. Notifications & Reporting

### Email Notifications
```csharp
private async Task SendRatePlanDiscrepanciesMail(KeySysLambdaContext context, int serviceProviderId, List<RatePlanChange> outOfSyncRatePlanChanges)
```

### Notification Content
- **Discrepancy Reports**: Detailed HTML tables showing rate plan mismatches
- **Service Provider Context**: Includes tenant and service provider information
- **Environment Indicators**: Non-production environments clearly marked
- **Device Details**: ICCID/MSISDN, AMOP rate plan, and carrier rate plan comparison

### Reporting Mechanisms
- **CloudWatch Logs**: Comprehensive logging throughout processing
- **Email Alerts**: Automated discrepancy notifications
- **Database Logging**: Processing status and error logging
- **SES Integration**: Uses Amazon SES for email delivery

### Log Categories
- **SUB**: Subroutine entry/exit logging
- **INFO**: General information logging
- **EXCEPTION**: Error and exception logging
- **RESPONSE STATUS**: HTTP response status logging

---

## 10. External Dependencies

### Required AWS Services
- **SQS**: Message queuing and processing coordination
- **SES**: Email notification delivery
- **Lambda**: Compute platform
- **CloudWatch**: Logging and monitoring

### Database Dependencies
- **SQL Server**: Central database for AMOP data
- **Jasper Database**: Carrier-specific staging data
- **Connection Strings**: Environment-based configuration

### Network Requirements
- **Carrier API Access**: Direct connectivity to Jasper/carrier APIs
- **Database Connectivity**: Secure database connections
- **AWS Service Access**: IAM permissions for AWS services

### Configuration Dependencies
- **Environment Variables**: Queue URLs, connection strings, environment identifiers
- **Database Configuration**: Service provider settings, authentication credentials
- **IAM Permissions**: Appropriate AWS service permissions
- **Network Configuration**: Security groups, VPC configuration for database access

### Critical Success Factors
1. **Valid Credentials**: Current authentication information in database
2. **Network Connectivity**: Reliable connection to all external systems
3. **Database Availability**: Central and Jasper databases accessible
4. **Queue Accessibility**: SQS queues available and properly configured
5. **File System Access**: Temporary storage for processing (if required)

---

## Environment Variables

| Variable | Purpose | Example |
|----------|---------|---------|
| `DeviceRatePlanChangeQueueUrl` | Queue for rate plan change processing | `https://sqs.us-east-1.amazonaws.com/.../device-rate-plan-changes` |
| `DeviceSyncQueueUrl` | Queue for device synchronization | `https://sqs.us-east-1.amazonaws.com/.../device-sync` |
| `JasperEnqueueRatePlanChangeUrl` | Self-triggering queue for retries | `https://sqs.us-east-1.amazonaws.com/.../jasper-enqueue-rate-plan` |
| `AWSEnv` | Environment identifier | `dev`, `test`, `prod` |

## Error Codes and Troubleshooting

### Common Issues
1. **Missing InstanceId**: Ensure SQS messages contain valid InstanceId attribute
2. **Authentication Failures**: Verify credentials in Integration_Authentication table
3. **Queue Access Errors**: Check IAM permissions and queue URLs
4. **Database Connectivity**: Validate connection strings and network access
5. **Rate Plan Discrepancies**: Review carrier API responses and staging data

### Monitoring Points
- CloudWatch metrics for lambda execution
- SQS queue depth and message age
- Database connection health
- Email delivery success rates
- Processing completion rates per service provider