# AltaworxJasperAWSEnqueueRatePlanChanges Lambda Flow Documentation

## Overview
This AWS Lambda function processes SQS events to enqueue rate plan changes for Jasper devices. It handles both regular rate plan change processing and discrepancy mail sending scenarios.

## Architecture Components
- **Main Lambda**: AltaworxJasperAWSEnqueueRatePlanChanges.cs
- **Base Class**: AwsFunctionBase.cs (provides common AWS Lambda functionality)
- **Helper Classes**: 
  - JasperCommon.cs (Jasper authentication utilities)
  - JasperDeviceStagingRepository.cs (device staging operations)
  - JasperUsageStagingRepository.cs (usage staging operations)
  - ServiceProviderRepository.cs (service provider data access)
- **Models**: JasperUsageReportResult.cs (usage report data structures)

---

## 1. HIGH-LEVEL FLOW

### Main Entry Point Flow
```
FunctionHandler(SQSEvent, ILambdaContext)
    ↓
BaseFunctionHandler() [from AwsFunctionBase]
    ↓
ProcessEvent(KeySysLambdaContext, SQSEvent)
    ↓
ProcessEventRecord(KeySysLambdaContext, SQSMessage)
    ↓
[Branch based on message attributes]
    ├── ProcessSendDiscrepanciesMail() [if IsSendDiscrepancieMail = true]
    └── ProcessInstance() [if SyncedDevices = true] OR EnqueueDeviceSyncAsync() [if SyncedDevices = false]
```

### Regular Processing Flow (SyncedDevices = true)
```
ProcessInstance()
    ↓
GetCommGroups() [from AwsFunctionBase]
    ↓
[For each CommGroup]
    ├── GetWinningQueueId()
    ├── GetRatePlanChangesDb()
    ├── EnqueueRatePlanChangesAsync()
    │   ├── EnqueueRatePlanChanges() OR EnqueueMobilityRatePlanChanges()
    │   ├── GetGroupCount()
    │   └── SendProcessMessagesToQueue()
    │       └── EnqueueJasperAWSUpdateDeviceRatePlanSqs()
    └── [If discrepancies found] EnqueueRatePlanChangesSqs()
```

### Device Sync Flow (SyncedDevices = false)
```
EnqueueDeviceSyncAsync()
    ↓
[Send message to DeviceSyncQueue with NextStep = UpdateDeviceRatePlan]
```

### Discrepancy Mail Processing Flow
```
ProcessSendDiscrepanciesMail()
    ↓
JasperCommon.GetJasperAuthenticationInformation()
    ↓
RatePlanChangeProgressCompleted()
    ↓
GetOutOfSyncRatePlanChanges()
    ↓
[Branch based on conditions]
    ├── SendRatePlanDiscrepanciesMail() [if WriteIsEnabled and process completed]
    ├── EnqueueRatePlanChangesSqs() [if retry count < MAX_RETRY]
    └── SendRatePlanDiscrepanciesMail() [if max retries reached]
```

---

## 2. LOW-LEVEL FLOW

### FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)
**Purpose**: Main entry point for the Lambda function
**Process**:
1. Initialize KeySysLambdaContext using `BaseFunctionHandler()` from AwsFunctionBase
2. Load environment variables (DeviceRatePlanChangeQueueUrl, DeviceSyncQueueUrl, etc.)
3. Call `ProcessEvent()` to handle the SQS event
4. Handle exceptions and log errors
5. Clean up resources using `CleanUp()`

### ProcessEvent(KeySysLambdaContext context, SQSEvent sqsEvent)
**Purpose**: Validates and routes SQS event processing
**Process**:
1. Check if SQS event contains records
2. Ensure only single message processing (logs exception if multiple messages)
3. Call `ProcessEventRecord()` for the single message

### ProcessEventRecord(KeySysLambdaContext context, SQSEvent.SQSMessage message)
**Purpose**: Routes message processing based on message attributes
**Process**:
1. Check message attributes for "IsSendDiscrepancieMail"
2. **Branch A - Discrepancy Mail**: If IsSendDiscrepancieMail = true → `ProcessSendDiscrepanciesMail()`
3. **Branch B - Regular Processing**: 
   - Extract InstanceId from message attributes
   - Get OptimizationInstance using `GetInstance()` from AwsFunctionBase
   - Validate ServiceProviderId exists
   - Check "SyncedDevices" attribute:
     - If true → `ProcessInstance()` (devices already synced)
     - If false → `EnqueueDeviceSyncAsync()` (need device sync first)

### ProcessInstance(KeySysLambdaContext context, long instanceId, int serviceProviderId, int tenantId)
**Purpose**: Main processing logic for rate plan changes
**Process**:
1. Get communication groups using `GetCommGroups()` from AwsFunctionBase
2. Initialize collections for tracking discrepancies and queue IDs
3. **For each CommGroup**:
   - Get winning queue ID using `GetWinningQueueId()`
   - Retrieve rate plan changes using `GetRatePlanChangesDb()`
   - Identify out-of-sync devices (where AMOP rate plan ≠ Jasper rate plan)
   - Enqueue rate plan changes using `EnqueueRatePlanChangesAsync()`
4. If discrepancies found, enqueue retry message using `EnqueueRatePlanChangesSqs()`

### EnqueueRatePlanChangesAsync(KeySysLambdaContext context, int serviceProviderId, long queueId, long? instanceId, int tenantId, IEnumerable<RatePlanChange> ratePlanChanges)
**Purpose**: Enqueues rate plan changes based on service provider integration type
**Process**:
1. Filter rate plan changes that need processing (missing or mismatched carrier/customer rate plans)
2. Get service provider details using ServiceProviderCommon.GetServiceProvider()
3. **Route by Integration Type**:
   - **Telegence**: Call `EnqueueMobilityRatePlanChanges()` → Bulk insert to OptimizationMobilityDeviceResult_RatePlanQueue
   - **Jasper/TMobileJasper/POD19**: Call `EnqueueRatePlanChanges()` → Bulk insert to OptimizationDeviceResult_RatePlanQueue
4. Get group count using `GetGroupCount()`
5. Send processing messages using `SendProcessMessagesToQueue()` or direct `EnqueueJasperAWSUpdateDeviceRatePlanSqs()`

### GetWinningQueueId(KeySysLambdaContext context, long commGroupId, int serviceProviderId)
**Purpose**: Gets the winning optimization queue for a communication group
**Process**:
1. Execute stored procedure "OPTIMIZATION_GET_WINNING_QUEUE_ID"
2. Pass commGroupId and serviceProviderId as parameters
3. Return the winning queue ID for processing

### GetRatePlanChangesDb(KeySysLambdaContext context, long queueId, int serviceProviderId)
**Purpose**: Retrieves rate plan changes from database with Jasper staging comparison
**Process**:
1. Set up SQL connection to both Central DB and Jasper DB
2. Execute stored procedure "OPTIMIZATION_GET_RATE_PLAN_CHANGES"
3. Pass queueId, serviceProviderId, and Jasper DB name as parameters
4. Map results to RatePlanChange objects using `ReadRatePlanChangesFromReader()`
5. Return collection of rate plan changes with AMOP vs Jasper rate plan comparison

### EnqueueDeviceSyncAsync(KeySysLambdaContext context, long instanceId, int serviceProviderId)
**Purpose**: Enqueues device synchronization request
**Process**:
1. Create SQS message to DeviceSyncQueueUrl
2. Set message attributes:
   - PageNumber = 1
   - LastSyncDate = 1 month + 1 day ago
   - ServiceProviderId
   - NextStep = UpdateDeviceRatePlan
   - OptimizationInstanceId
3. Send message with 3-second delay

### ProcessSendDiscrepanciesMail(KeySysLambdaContext context, SQSEvent.SQSMessage message)
**Purpose**: Handles discrepancy mail sending with retry logic
**Process**:
1. Extract retry count, winning queue IDs, service provider ID, and instance ID from message
2. Get Jasper authentication using `JasperCommon.GetJasperAuthenticationInformation()`
3. Check if rate plan change process completed using `RatePlanChangeProgressCompleted()`
4. Get out-of-sync rate plan changes using `GetOutOfSyncRatePlanChanges()`
5. **Decision Logic**:
   - If WriteIsEnabled AND process completed → Send discrepancy mail immediately
   - If retry count < MAX_RETRY AND discrepancies exist → Enqueue retry with 15-minute delay
   - Otherwise → Send discrepancy mail

### RatePlanChangeProgressCompleted(KeySysLambdaContext context, int instanceId, int serviceProviderId)
**Purpose**: Checks if all devices in the optimization instance have been processed
**Process**:
1. Execute stored procedure "OPTIMIZATION_RATE_PLAN_CHANGE_PROGRESS_COMPLETED"
2. Get device queue count and processed count
3. Return true if counts match (all devices processed)

### SendRatePlanDiscrepanciesMail(KeySysLambdaContext context, int serviceProviderId, List<RatePlanChange> outOfSyncRatePlanChanges)
**Purpose**: Sends email notification about rate plan discrepancies
**Process**:
1. Get service provider details and tenant information
2. Build email subject with environment suffix if non-production
3. Create HTML email body using `BuildRatePlanDiscrepancyBody()` showing:
   - Device identifiers (ICCID or MSISDN based on integration type)
   - AMOP rate plans vs Carrier rate plans
4. Send email using AWS SES via `SendEmailAsync()`

### EnqueueJasperAWSUpdateDeviceRatePlanSqs(KeySysLambdaContext context, int serviceProviderId, long queueId, int groupNumber, long? instanceId, int tenantId)
**Purpose**: Enqueues individual rate plan change processing messages
**Process**:
1. Create SQS message to DeviceRatePlanChangeQueueUrl
2. Set message attributes:
   - QueueId
   - GroupNumber
   - ServiceProviderId
   - TenantId
   - InstanceId (if provided, only for first group)
3. Send message with 3-second delay

### Key Database Operations
- **Bulk Insert Operations**: Uses SqlBulkCopy for efficient rate plan queue insertions
- **Stored Procedure Calls**: Leverages stored procedures for complex optimization queries
- **Retry Policies**: Implements SQL retry policies for transient error handling
- **Connection Management**: Proper disposal of SQL connections and resources

### Integration Points
- **SQS Queues**: DeviceRatePlanChangeQueueUrl, DeviceSyncQueueUrl, JasperEnqueueRatePlanChangeUrl
- **Databases**: Central DB (optimization data), Jasper DB (device staging data)
- **AWS SES**: Email notifications for discrepancies
- **Service Providers**: Supports Jasper, TMobile Jasper, POD19, and Telegence integrations

---

## Error Handling & Logging
- Comprehensive exception handling at each level
- Detailed logging using KeySysLambdaContext
- SQL retry policies for transient database errors
- Environment-specific configuration support
- Graceful degradation for missing configuration

## Environment Variables
- `DeviceRatePlanChangeQueueUrl`: Queue for device rate plan changes
- `DeviceSyncQueueUrl`: Queue for device synchronization
- `JasperEnqueueRatePlanChangeUrl`: Queue for retry processing
- `AWSEnv`: Environment identifier for email subjects