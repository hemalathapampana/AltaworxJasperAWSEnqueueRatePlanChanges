# Jasper Device Usage Export Workflow - Detailed Process Documentation

## Table of Contents
1. [Triggers & Scheduling Process](#1-triggers--scheduling-process)
2. [Message Handling Process](#2-message-handling-process)
3. [Batch & Pagination Configuration Process](#3-batch--pagination-configuration-process)
4. [Integration Details Process](#4-integration-details-process)
5. [Data Handling & Staging Process](#5-data-handling--staging-process)
6. [Error Handling & Retry Process](#6-error-handling--retry-process)
7. [Failed/Unprocessed Records Process](#7-failedunprocessed-records-process)
8. [Lambda Execution Management Process](#8-lambda-execution-management-process)

---

## 1. Triggers & Scheduling Process

### CloudWatch Scheduled Jobs Configuration

**Purpose**: Initiate Jasper device usage export workflows at predetermined intervals

#### Environment Configuration Code
```csharp
// Environment Variables Configuration Structure
public class EnvironmentConfig
{
    public string ExportDeviceUsageQueueURL { get; set; }
    public string DeviceNotificationQueueURL { get; set; }
    public int ExportBatchSize { get; set; }
    public string EnvName { get; set; }
    public bool VerboseLogging { get; set; }
    public string ConnectionString { get; set; }
    public string BaseMultiTenantConnectionString { get; set; }
}

// Environment Variables Initialization
private string ExportDeviceUsageQueueURL = Environment.GetEnvironmentVariable("ExportDeviceUsageQueueURL");
private string DeviceNotificationQueueURL = Environment.GetEnvironmentVariable("DeviceNotificationQueueURL");
private int ExportBatchSize = Convert.ToInt32(Environment.GetEnvironmentVariable("ExportBatchSize"));
```

**Low-Level Flow**:
1. **Environment Initialization**: Lambda cold start loads environment variables from AWS Lambda configuration
2. **Variable Validation**: System validates required environment variables exist and are properly formatted
3. **Configuration Storage**: Variables stored in class-level properties for function lifetime reuse
4. **Error Handling**: Missing or invalid environment variables trigger initialization exceptions

#### CloudWatch Event Configuration
```json
{
  "ScheduleExpression": "rate(1 day)",
  "Target": {
    "Arn": "arn:aws:sqs:us-east-1:130265568833:Jasper_Device_Export_TEST",
    "MessageAttributes": {
      "InitializeProcessing": "true",
      "ScheduleType": "daily", 
      "ServiceProviderId": "1",
      "IntegrationType": "Jasper"
    }
  }
}
```

**Low-Level Flow**:
1. **CloudWatch Timer**: Internal AWS scheduler evaluates cron expression
2. **Event Generation**: CloudWatch generates event with specified attributes
3. **SQS Publication**: Event published to target SQS queue with message attributes
4. **Lambda Trigger**: SQS message triggers Lambda function via event source mapping

#### Schedule Types Implementation
```csharp
public enum ScheduleType
{
    Daily,
    Weekly,
    Custom
}

// Schedule Expression Mapping
private static readonly Dictionary<ScheduleType, string> ScheduleExpressions = new()
{
    { ScheduleType.Daily, "rate(1 day)" },
    { ScheduleType.Weekly, "rate(7 days)" },
    { ScheduleType.Custom, "cron(0 2 * * ? *)" } // 2 AM daily
};
```

**Low-Level Flow**:
1. **Schedule Evaluation**: CloudWatch evaluates schedule expression syntax
2. **Trigger Calculation**: Next execution time calculated based on expression type
3. **Event Scheduling**: Event scheduled in CloudWatch internal queue
4. **Execution**: At scheduled time, event fires and triggers downstream processes

---

## 2. Message Handling Process

### SQS Message Structure & Processing

#### Core Message Attributes Definition
```csharp
public static class AttributeNames 
{
    public const string InitializeProcessing = "InitializeProcessing";
    public const string LoadStagingData = "LoadStagingData"; 
    public const string ServiceProviderId = "ServiceProviderId";
    public const string IntegrationType = "IntegrationType";
    public const string WaitCount = "WaitCount";
    public const string TotalEmailsToProcess = "TotalEmailsToProcess";
    public const string ProcessedEmailsCount = "ProcessedEmailsCount";
    public const string ExportStartDateTime = "ExportStartDateTime";
}

public class ExportDeviceUsageSqsValues
{
    public bool InitializeProcessing { get; set; }
    public bool LoadStagingData { get; set; }
    public int? ServiceProviderId { get; set; }
    public IntegrationType IntegrationType { get; set; }
    public int WaitCount { get; set; }
    public int TotalEmailsToProcess { get; set; }
    public int ProcessedEmailsCount { get; set; }
    public DateTime ExportStartDateTime { get; set; }
    
    public bool HaveAllEmailsBeenProcessed => ProcessedEmailsCount >= TotalEmailsToProcess;
}
```

**Low-Level Flow**:
1. **Message Deserialization**: SQS message body and attributes parsed into strongly-typed object
2. **Attribute Extraction**: Each message attribute converted from string to appropriate data type
3. **Validation**: Required attributes validated for presence and format
4. **Object Construction**: ExportDeviceUsageSqsValues object constructed with parsed values

#### Message Processing Logic
```csharp
private async Task ProcessEventRecordAsync(KeySysLambdaContext context, SQSEvent.SQSMessage message)
{
    ExportDeviceUsageSqsValues sqsValues = GetMessageQueueValues(context, message);
    
    LogInfo(context, "PROCESSING", $"Processing message with InitializeProcessing: {sqsValues.InitializeProcessing}, LoadStagingData: {sqsValues.LoadStagingData}");
    
    try
    {
        if (sqsValues.InitializeProcessing)
        {
            LogInfo(context, "INITIALIZE", "Starting initialization process");
            await InitializeProcessingAsync(context, sqsValues);
        }
        else if (sqsValues.LoadStagingData)
        {
            LogInfo(context, "STAGING", "Loading staging data");
            LoadStagingData(context, sqsValues.ServiceProviderId.Value, sqsValues.IntegrationType);
            await SendNotificationMessageToQueueAsync(context, sqsValues);
        }
        else
        {
            LogInfo(context, "EMAIL", "Processing email workflow");
            await HasMailToProcess(context, sqsValues, EmailPolicy);
        }
    }
    catch (Exception ex)
    {
        LogError(context, "ERROR", $"Error processing message: {ex.Message}", ex);
        throw;
    }
}

private ExportDeviceUsageSqsValues GetMessageQueueValues(KeySysLambdaContext context, SQSEvent.SQSMessage message)
{
    return new ExportDeviceUsageSqsValues(context, message);
}
```

**Low-Level Flow**:
1. **Message Routing**: Message attributes evaluated to determine processing path
2. **Initialization Path**: InitializeProcessing=true triggers Jasper API export requests
3. **Staging Path**: LoadStagingData=true triggers database staging operations
4. **Email Path**: Default path processes email polling and file downloads
5. **Error Handling**: Exceptions logged and re-thrown for SQS retry mechanism

#### Message Continuation Pattern
```csharp
private async Task SendProcessMessageToQueueAsync(KeySysLambdaContext context, bool initializeProcessing, 
    int serviceProviderId, int waitCount, IntegrationType integrationType, DateTime exportStartDateTime, 
    int totalEmailsToProcess = 0, int processedEmailsCount = 0)
{
    var request = new SendMessageRequest
    {
        QueueUrl = ExportDeviceUsageQueueURL,
        DelaySeconds = CommonConstants.DELAY_IN_SECONDS_FIVE_MINUTES, // 300 seconds
        MessageBody = "Export Device Usage Processing",
        MessageAttributes = new Dictionary<string, MessageAttributeValue>
        {
            { AttributeNames.InitializeProcessing, new MessageAttributeValue 
                { StringValue = initializeProcessing.ToString(), DataType = "String" }},
            { AttributeNames.ServiceProviderId, new MessageAttributeValue 
                { StringValue = serviceProviderId.ToString(), DataType = "Number" }},
            { AttributeNames.WaitCount, new MessageAttributeValue 
                { StringValue = waitCount.ToString(), DataType = "Number" }},
            { AttributeNames.IntegrationType, new MessageAttributeValue 
                { StringValue = ((int)integrationType).ToString(), DataType = "Number" }},
            { AttributeNames.TotalEmailsToProcess, new MessageAttributeValue 
                { StringValue = totalEmailsToProcess.ToString(), DataType = "Number" }},
            { AttributeNames.ProcessedEmailsCount, new MessageAttributeValue 
                { StringValue = processedEmailsCount.ToString(), DataType = "Number" }},
            { AttributeNames.ExportStartDateTime, new MessageAttributeValue 
                { StringValue = exportStartDateTime.ToString("O"), DataType = "String" }}
        }
    };
    
    LogInfo(context, "QUEUE", $"Sending continuation message with WaitCount: {waitCount}, DelaySeconds: {request.DelaySeconds}");
    await SqsClient.SendMessageAsync(request);
}
```

**Low-Level Flow**:
1. **Message Construction**: New SQS message built with current processing state
2. **Delay Configuration**: 5-minute delay applied to prevent rapid polling
3. **State Preservation**: All processing context preserved in message attributes
4. **Queue Submission**: Message sent to SQS for future processing
5. **Continuation**: Next Lambda invocation resumes from preserved state

---

## 3. Batch & Pagination Configuration Process

### Batch Size Constants & Configuration

#### Configuration Implementation
```csharp
public class Function : AwsFunctionBase
{
    // Jasper API limits - AT&T documentation limit is 500,000 devices
    // Using lower value for safety and performance
    private const int DEFAULT_EXPORT_BATCH_SIZE = 400000;
    
    // Environment variable configuration with fallback
    private int ExportBatchSize 
    { 
        get 
        { 
            var batchSizeStr = Environment.GetEnvironmentVariable("ExportBatchSize");
            return int.TryParse(batchSizeStr, out var size) ? size : DEFAULT_EXPORT_BATCH_SIZE;
        } 
    }
    
    // Lambda execution time management
    private const int LAMBDA_REMAINING_TIME_LIMIT_IN_SECONDS = 60;
    
    // Retry configuration constants
    private const int SQL_TRANSIENT_RETRY_BASE_SECONDS = 4;
    private const int SQL_TRANSIENT_RETRY_MAX_COUNT = 3;
    private const int HTTP_TRANSIENT_RETRY_BASE_SECONDS = 4;
    private const int HTTP_TRANSIENT_RETRY_MAX_COUNT = 3;
    private const int DEFAULT_EMAIL_WAIT_MAX_RETRIES = 17; // 18 * 5 minutes = 90 minutes total
}
```

**Low-Level Flow**:
1. **Environment Reading**: System reads ExportBatchSize from Lambda environment variables
2. **Validation**: Batch size validated against minimum/maximum acceptable values
3. **Fallback**: If invalid or missing, system uses DEFAULT_EXPORT_BATCH_SIZE (400,000)
4. **Memory Allocation**: Batch size used to allocate appropriate memory structures
5. **API Compliance**: Ensures batch size stays within Jasper API limits

#### Device Batch Processing Implementation
```csharp
public class JasperDeviceBatch
{
    public int BatchNumber { get; set; }
    public int StartRowNumber { get; set; }
    public int EndRowNumber { get; set; }
    public int DeviceCount { get; set; }
    public string BatchIdentifier { get; set; }
}

public List<JasperDeviceBatch> GetBatchedJasperSimCardCountByServiceProviderId(
    KeySysLambdaContext context, int serviceProviderId, int batchSize)
{
    var batches = new List<JasperDeviceBatch>();
    
    using (var connection = new SqlConnection(context.GeneralProviderSettings.JasperDbConnectionString))
    {
        connection.Open();
        using (var command = connection.CreateCommand())
        {
            command.CommandType = CommandType.StoredProcedure;
            command.CommandText = "usp_GetBatchedJasperSimCardCountByServiceProviderId";
            command.Parameters.AddWithValue("@ServiceProviderId", serviceProviderId);
            command.Parameters.AddWithValue("@BatchSize", batchSize);
            command.CommandTimeout = 300; // 5 minutes
            
            using (var reader = command.ExecuteReader())
            {
                int batchNumber = 1;
                while (reader.Read())
                {
                    batches.Add(new JasperDeviceBatch
                    {
                        BatchNumber = batchNumber++,
                        StartRowNumber = reader.GetInt32("StartRowNumber"),
                        EndRowNumber = reader.GetInt32("EndRowNumber"),
                        DeviceCount = reader.GetInt32("DeviceCount"),
                        BatchIdentifier = $"Batch_{batchNumber}_{DateTime.UtcNow:yyyyMMdd_HHmmss}"
                    });
                }
            }
        }
    }
    
    LogInfo(context, "BATCHING", $"Created {batches.Count} batches for ServiceProviderId: {serviceProviderId}");
    return batches;
}
```

**Low-Level Flow**:
1. **Database Connection**: Establishes connection to Jasper database using connection string
2. **Stored Procedure Call**: Executes usp_GetBatchedJasperSimCardCountByServiceProviderId with parameters
3. **Result Processing**: Reads result set and constructs JasperDeviceBatch objects
4. **Batch Numbering**: Assigns sequential batch numbers for tracking
5. **Memory Management**: Disposes database resources properly

#### Email Processing Wait Pattern
```csharp
// Email processing continuation logic
if (sqsValues.WaitCount < DEFAULT_EMAIL_WAIT_MAX_RETRIES && sqsValues.ServiceProviderId != null)
{
    LogInfo(context, "WAIT", $"Email processing wait cycle {sqsValues.WaitCount + 1} of {DEFAULT_EMAIL_WAIT_MAX_RETRIES + 1}");
    
    // Calculate next wait time with exponential backoff option
    var nextWaitCount = sqsValues.WaitCount + 1;
    var delaySeconds = CommonConstants.DELAY_IN_SECONDS_FIVE_MINUTES; // Fixed 5-minute delay
    
    // Requeue with incremented wait count
    await SendProcessMessageToQueueAsync(context, false, sqsValues.ServiceProviderId.Value, 
        nextWaitCount, sqsValues.IntegrationType, sqsValues.ExportStartDateTime, 
        sqsValues.TotalEmailsToProcess, sqsValues.ProcessedEmailsCount);
        
    LogInfo(context, "REQUEUE", $"Requeued message with WaitCount: {nextWaitCount}, Delay: {delaySeconds}s");
}
else
{
    LogError(context, "TIMEOUT", $"Max wait time exceeded. WaitCount: {sqsValues.WaitCount}, MaxRetries: {DEFAULT_EMAIL_WAIT_MAX_RETRIES}");
    
    // Max wait time exceeded - send error notification
    await SendErrorEmailNotificationAsync(context, sqsValues.IntegrationType, 
        BuildDownloadErrorEmailBody(context, "Email processing timeout exceeded"));
}
```

**Low-Level Flow**:
1. **Wait Count Evaluation**: Current wait count compared against maximum retry limit
2. **Delay Calculation**: Fixed 5-minute delay applied for email processing intervals
3. **State Preservation**: All processing state maintained across wait cycles
4. **Timeout Detection**: System detects when maximum wait time exceeded
5. **Error Escalation**: Timeout triggers error notification to operations team

---

## 4. Integration Details Process

### Jasper API Authentication Management

#### Credential Retrieval Implementation
```csharp
public class JasperProviderSettings
{
    public string JasperUIAddress { get; set; }
    public string JasperUIUsername { get; set; }
    public string JasperUIPassword { get; set; } // Base64 encoded
    public string JasperExportMailServer { get; set; }
    public int JasperExportMailPort { get; set; }
    public string JasperExportMailUsername { get; set; }
    public string JasperExportMailPassword { get; set; } // Base64 encoded
    public string JasperExportMailAlias { get; set; }
    public bool UseSSL { get; set; }
}

// Credential initialization in constructor
if (ServiceProviderId != null)
{
    LogInfo(context, "AUTH", $"Retrieving Jasper settings for ServiceProviderId: {ServiceProviderId}");
    
    JasperProviderSettings = context.SettingsRepo.GetJasperDeviceSettings(ServiceProviderId.Value);
    
    if (!string.IsNullOrWhiteSpace(JasperProviderSettings.JasperUIAddress))
    {
        JasperBaseUrl = JasperProviderSettings.JasperUIAddress.TrimEnd('/');
        LogInfo(context, "CONFIG", $"Jasper Base URL configured: {JasperBaseUrl}");
    }
    else
    {
        throw new InvalidOperationException($"JasperUIAddress not configured for ServiceProviderId: {ServiceProviderId}");
    }
}
```

**Low-Level Flow**:
1. **Settings Retrieval**: Database query retrieves provider-specific Jasper configuration
2. **URL Normalization**: Base URL trimmed of trailing slashes for consistent formatting
3. **Validation**: Required settings validated for presence and format
4. **Credential Decoding**: Base64 encoded passwords decoded when needed
5. **Error Handling**: Missing configuration triggers descriptive exceptions

#### Jasper Authentication Information Retrieval
```csharp
public static JasperAuthentication GetJasperAuthenticationInformation(string connectionString, int currentServiceProviderId)
{
    using (var conn = new SqlConnection(connectionString))
    {
        conn.Open();
        using (var cmd = new SqlCommand("usp_Jasper_Get_AuthenticationByProviderId", conn))
        {
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.Parameters.AddWithValue("@providerId", currentServiceProviderId);
            cmd.CommandTimeout = 30;
            
            using (var reader = cmd.ExecuteReader())
            {
                if (reader.Read())
                {
                    return new JasperAuthentication
                    {
                        Username = reader.GetString("Username"),
                        Password = reader.GetString("Password"), // Encrypted
                        ApiKey = reader.IsDBNull("ApiKey") ? null : reader.GetString("ApiKey"),
                        SessionToken = reader.IsDBNull("SessionToken") ? null : reader.GetString("SessionToken"),
                        TwoFactorEnabled = reader.GetBoolean("TwoFactorEnabled"),
                        LastAuthenticationTime = reader.IsDBNull("LastAuthenticationTime") ? null : reader.GetDateTime("LastAuthenticationTime")
                    };
                }
                else
                {
                    throw new InvalidOperationException($"No authentication information found for ServiceProviderId: {currentServiceProviderId}");
                }
            }
        }
    }
}
```

**Low-Level Flow**:
1. **Database Connection**: Opens connection to Integration_Authentication table
2. **Stored Procedure Execution**: Calls provider-specific authentication retrieval procedure
3. **Result Mapping**: Maps database columns to JasperAuthentication object properties
4. **Null Handling**: Properly handles optional fields like ApiKey and SessionToken
5. **Validation**: Ensures authentication record exists for the specified provider

### Jasper API Login Process Implementation

#### Complete Login Flow
```csharp
public async Task ExportUsageFromJasperAsync(KeySysLambdaContext context, ExportDeviceUsageSqsValues sqsValues, int serviceProviderId)
{
    var startTime = DateTime.UtcNow;
    LogInfo(context, "EXPORT", $"Starting Jasper export for ServiceProviderId: {serviceProviderId}");
    
    using (HttpClient client = new HttpClient(new LambdaLoggingHandler()))
    {
        client.Timeout = TimeSpan.FromMinutes(10);
        
        try
        {
            // Step 1: Get login form page to establish session
            string loginFormUrl = sqsValues.JasperBaseUrl + "/provision/jsp/login.jsp";
            LogInfo(context, "LOGIN", $"Requesting login form: {loginFormUrl}");
            
            var loginFormRequest = new HttpRequestMessage(HttpMethod.Get, loginFormUrl);
            var loginFormResponse = await client.SendAsync(loginFormRequest);
            loginFormResponse.EnsureSuccessStatusCode();
            
            // Step 2: Perform authentication
            string loginUrl = sqsValues.JasperBaseUrl + "/provision/j_acegi_security_check";
            LogInfo(context, "AUTH", "Performing authentication");
            
            var loginRequest = new HttpRequestMessage(HttpMethod.Post, loginUrl);
            var loginFormFields = new Dictionary<string, string>
            {
                {"j_username", sqsValues.JasperProviderSettings.JasperUIUsername},
                {"j_password", context.Base64Service.Base64Decode(sqsValues.JasperProviderSettings.JasperUIPassword)}
            };
            
            loginRequest.Content = new FormUrlEncodedContent(loginFormFields);
            var loginResponse = await client.SendAsync(loginRequest);
            
            // Step 3: Handle 2FA if required
            if (loginResponse.Headers.Location?.ToString().Contains("twoFactorAuth") == true)
            {
                LogInfo(context, "2FA", "Two-factor authentication required");
                await Handle2FAAsync(context, client, sqsValues);
            }
            
            // Step 4: Get CSRF token for API requests
            var csrfTokenValue = await GetCsrfToken(context, sqsValues, startTime, client, loginResponse);
            LogInfo(context, "CSRF", $"Retrieved CSRF token: {csrfTokenValue?.Substring(0, 8)}...");
            
            // Step 5: Process device batches
            var simCardBatches = GetBatchedJasperSimCardCountByServiceProviderId(context, serviceProviderId, ExportBatchSize);
            LogInfo(context, "BATCHES", $"Processing {simCardBatches.Count} device batches");
            
            foreach (var batch in simCardBatches)
            {
                await ProcessDeviceBatchAsync(context, client, sqsValues, batch, csrfTokenValue);
            }
        }
        catch (Exception ex)
        {
            LogError(context, "EXPORT_ERROR", $"Error during Jasper export: {ex.Message}", ex);
            throw;
        }
    }
}

private async Task<string> GetCsrfToken(KeySysLambdaContext context, ExportDeviceUsageSqsValues sqsValues, 
    DateTime startTime, HttpClient client, HttpResponseMessage response)
{
    string csrfUrl = sqsValues.JasperBaseUrl + "/provision/api/v1/sims";
    var csrfRequest = new HttpRequestMessage(HttpMethod.Get, csrfUrl);
    
    var csrfResponse = await client.SendAsync(csrfRequest);
    var csrfContent = await csrfResponse.Content.ReadAsStringAsync();
    
    // Extract CSRF token from response headers or content
    if (csrfResponse.Headers.TryGetValues("X-CSRF-TOKEN", out var tokenValues))
    {
        return tokenValues.FirstOrDefault();
    }
    
    // Fallback: extract from HTML content
    var tokenMatch = Regex.Match(csrfContent, @"name=""_csrf""\s+value=""([^""]+)""");
    return tokenMatch.Success ? tokenMatch.Groups[1].Value : null;
}
```

**Low-Level Flow**:
1. **Session Establishment**: GET request to login form establishes HTTP session with cookies
2. **Form Submission**: POST authentication credentials to j_acegi_security_check endpoint
3. **2FA Handling**: Detects 2FA requirement from redirect and processes verification code
4. **CSRF Token Extraction**: Retrieves CSRF token from API endpoint for subsequent requests
5. **Batch Processing**: Iterates through device batches and submits export requests

---

## 5. Data Handling & Staging Process

### Staging Process Implementation

#### Data Cleanup and Initialization
```csharp
private static void ClearStagingData(string connectionString, IKeysysLogger logger, int serviceProviderId)
{
    var stopwatch = Stopwatch.StartNew();
    logger.LogInfo("STAGING", $"Clearing staging data for ServiceProviderId: {serviceProviderId}");
    
    var usageStagingRepo = new JasperUsageStagingRepository();
    var errorMessages = new List<string>();
    
    try
    {
        var deletedRows = usageStagingRepo.DeleteStagingWithPolicy(logger, connectionString, serviceProviderId, errorMessages);
        
        stopwatch.Stop();
        logger.LogInfo("STAGING", $"Cleared {deletedRows} staging records in {stopwatch.ElapsedMilliseconds}ms");
        
        if (errorMessages.Any())
        {
            logger.LogWarning("STAGING", $"Warnings during staging cleanup: {string.Join(", ", errorMessages)}");
        }
    }
    catch (Exception ex)
    {
        logger.LogError("STAGING_ERROR", $"Error clearing staging data: {ex.Message}", ex);
        throw;
    }
}

private static void LoadStagingData(KeySysLambdaContext context, int serviceProviderId, IntegrationType integrationType)
{
    var stopwatch = Stopwatch.StartNew();
    LogInfo(context, "STAGING", $"Loading staging data for ServiceProviderId: {serviceProviderId}, IntegrationType: {integrationType}");
    
    var billingPeriod = GetBillingPeriod(context, serviceProviderId, DateTime.Now);
    LogInfo(context, "BILLING", $"Billing period: {billingPeriod.BillingPeriodStart:yyyy-MM-dd} to {billingPeriod.BillingPeriodEnd:yyyy-MM-dd}");
    
    using (var conn = new SqlConnection(context.GeneralProviderSettings.JasperDbConnectionString))
    {
        conn.Open();
        using (var cmd = new SqlCommand("usp_JasperDeviceExportStaging_Load", conn))
        {
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandTimeout = 1800; // 30 minutes for large data loads
            
            cmd.Parameters.AddWithValue("@BillYear", billingPeriod.BillingPeriodEnd.Year);
            cmd.Parameters.AddWithValue("@BillMonth", billingPeriod.BillingPeriodEnd.Month);
            cmd.Parameters.AddWithValue("@ServiceProviderid", serviceProviderId);
            cmd.Parameters.AddWithValue("@IntegrationId", (int)integrationType);
            
            var rowsAffected = cmd.ExecuteNonQuery();
            
            stopwatch.Stop();
            LogInfo(context, "STAGING", $"Loaded {rowsAffected} records to staging in {stopwatch.ElapsedMilliseconds}ms");
        }
    }
}
```

**Low-Level Flow**:
1. **Staging Cleanup**: Removes existing staging data to prevent duplicates
2. **Billing Period Calculation**: Determines current billing period for data filtering
3. **Stored Procedure Execution**: Calls usp_JasperDeviceExportStaging_Load with parameters
4. **Performance Monitoring**: Tracks execution time and record counts
5. **Error Handling**: Captures and logs any database errors during staging

### Email Processing & File Handling

#### IMAP Email Processing Implementation
```csharp
public async Task<bool> HasMailToProcess(KeySysLambdaContext context, ExportDeviceUsageSqsValues sqsValues)
{
    var stopwatch = Stopwatch.StartNew();
    bool hasProcessedAnyEmails = false;
    var processedFiles = new List<string>();
    
    LogInfo(context, "EMAIL", $"Checking for emails. WaitCount: {sqsValues.WaitCount}, ProcessedCount: {sqsValues.ProcessedEmailsCount}");
    
    using var client = new ImapClient 
    { 
        ServerCertificateValidationCallback = (s, c, h, e) => true,
        Timeout = 30000 // 30 seconds
    };
    
    try
    {
        // Connect and authenticate
        LogInfo(context, "IMAP", $"Connecting to {sqsValues.JasperProviderSettings.JasperExportMailServer}:{sqsValues.JasperProviderSettings.JasperExportMailPort}");
        
        client.Connect(sqsValues.JasperProviderSettings.JasperExportMailServer, 
                      sqsValues.JasperProviderSettings.JasperExportMailPort, 
                      sqsValues.JasperProviderSettings.UseSSL);
                      
        client.Authenticate(sqsValues.JasperProviderSettings.JasperExportMailUsername, 
                           context.Base64Service.Base64Decode(sqsValues.JasperProviderSettings.JasperExportMailPassword));
        
        client.Inbox.Open(FolderAccess.ReadWrite);
        LogInfo(context, "IMAP", $"Connected successfully. Inbox contains {client.Inbox.Count} messages");
        
        // Process messages in reverse chronological order (newest first)
        for (int i = client.Inbox.Count - 1; i >= 0; i--)
        {
            // Check Lambda timeout before processing each message
            if (context.Context.RemainingTime.TotalSeconds <= LAMBDA_REMAINING_TIME_LIMIT_IN_SECONDS)
            {
                LogInfo(context, "TIMEOUT", "Approaching Lambda timeout, stopping email processing");
                break;
            }
            
            var message = client.Inbox.GetMessage(i);
            LogInfo(context, "MESSAGE", $"Processing message {i}: Subject='{message.Subject}', From='{message.From}', Date='{message.Date}'");
            
            // Filter for Jasper export emails
            if (message.Subject == JasperConstants.EXPORT_EMAIL_SUBJECT && 
                message.To.Mailboxes.Any(m => m.Address == sqsValues.JasperProviderSettings.JasperExportMailAlias))
            {
                LogInfo(context, "MATCH", $"Found matching export email: {message.Subject}");
                
                var (isCompleted, currentFileUrl) = await ProcessMailMessageAsync(context, sqsValues, message);
                
                if (isCompleted)
                {
                    sqsValues.ProcessedEmailsCount++;
                    processedFiles.Add(currentFileUrl ?? "Unknown");
                    hasProcessedAnyEmails = true;
                    
                    // Mark email as deleted
                    client.Inbox.AddFlags(new[] { i }, MessageFlags.Deleted, true);
                    LogInfo(context, "PROCESSED", $"Successfully processed and marked email for deletion. ProcessedCount: {sqsValues.ProcessedEmailsCount}");
                }
            }
        }
        
        // Expunge deleted messages
        client.Inbox.Expunge();
        
        stopwatch.Stop();
        LogInfo(context, "EMAIL_COMPLETE", $"Email processing completed in {stopwatch.ElapsedMilliseconds}ms. Processed {processedFiles.Count} files");
        
        return hasProcessedAnyEmails;
    }
    catch (Exception ex)
    {
        LogError(context, "EMAIL_ERROR", $"Error during email processing: {ex.Message}", ex);
        throw;
    }
    finally
    {
        if (client.IsConnected)
        {
            client.Disconnect(true);
        }
    }
}
```

**Low-Level Flow**:
1. **IMAP Connection**: Establishes secure connection to email server
2. **Authentication**: Authenticates using decoded credentials
3. **Inbox Access**: Opens inbox with read/write permissions for message deletion
4. **Message Iteration**: Processes messages in reverse chronological order
5. **Content Filtering**: Filters for Jasper export emails based on subject and recipient
6. **File Processing**: Downloads and processes attachments from matching emails
7. **Cleanup**: Marks processed emails as deleted and expunges from server

### File Processing Implementation

#### Excel File Processing and Data Extraction
```csharp
public bool ProcessFile(KeySysLambdaContext context, byte[] fileBytes, int serviceProviderId, 
    IntegrationType integrationType, string fileDownloadUrl)
{
    var stopwatch = Stopwatch.StartNew();
    LogInfo(context, "FILE", $"Processing file of {fileBytes.Length} bytes for ServiceProviderId: {serviceProviderId}");
    
    try
    {
        // Load Excel file from byte array
        var dataSet = GetDataSetFromExcelFile(fileBytes);
        LogInfo(context, "EXCEL", $"Excel file loaded with {dataSet.Tables.Count} worksheets");
        
        if (dataSet.Tables.Count > 0)
        {
            var dataTable = dataSet.Tables[0]; // Use first worksheet
            LogInfo(context, "DATA", $"Worksheet contains {dataTable.Rows.Count} rows and {dataTable.Columns.Count} columns");
            
            // Validate expected column count
            if (dataTable.Columns.Count >= JasperExportDeviceUsageConfig.ExportFileNumberOfExpectedColumns)
            {
                // Transform and validate data
                DataTable exportDataTable = BuildAndPopulateExportDataTable(context, dataTable, serviceProviderId);
                LogInfo(context, "TRANSFORM", $"Transformed data contains {exportDataTable.Rows.Count} valid rows");
                
                if (exportDataTable.Rows.Count > 0)
                {
                    // Bulk load to staging table
                    var bulkCopyResult = SqlBulkCopy(context, context.GeneralProviderSettings.JasperDbConnectionString, 
                                                   exportDataTable, "JasperDeviceExportStaging");
                    
                    stopwatch.Stop();
                    LogInfo(context, "BULK_LOAD", $"Successfully bulk loaded {exportDataTable.Rows.Count} records in {stopwatch.ElapsedMilliseconds}ms");
                    
                    // Log file processing success
                    LogFileProcessingResult(context, fileDownloadUrl, serviceProviderId, integrationType, 
                                          exportDataTable.Rows.Count, true, null);
                    
                    return true;
                }
                else
                {
                    LogWarning(context, "NO_DATA", "No valid data rows found after transformation");
                }
            }
            else
            {
                LogError(context, "COLUMN_MISMATCH", 
                    $"Column count mismatch. Expected: {JasperExportDeviceUsageConfig.ExportFileNumberOfExpectedColumns}, Actual: {dataTable.Columns.Count}");
            }
        }
        else
        {
            LogError(context, "NO_WORKSHEETS", "Excel file contains no worksheets");
        }
        
        return false;
    }
    catch (Exception ex)
    {
        stopwatch.Stop();
        LogError(context, "FILE_ERROR", $"Error processing file: {ex.Message}", ex);
        
        // Log file processing failure
        LogFileProcessingResult(context, fileDownloadUrl, serviceProviderId, integrationType, 
                              0, false, ex.Message);
        
        return false;
    }
}

private DataTable BuildAndPopulateExportDataTable(KeySysLambdaContext context, DataTable sourceTable, int serviceProviderId)
{
    var exportTable = new DataTable();
    var validRowCount = 0;
    var errorRowCount = 0;
    
    // Define staging table schema
    exportTable.Columns.Add("ServiceProviderId", typeof(int));
    exportTable.Columns.Add("ICCID", typeof(string));
    exportTable.Columns.Add("MSISDN", typeof(string));
    exportTable.Columns.Add("UsageDate", typeof(DateTime));
    exportTable.Columns.Add("DataUsageMB", typeof(decimal));
    exportTable.Columns.Add("SMSUsage", typeof(int));
    exportTable.Columns.Add("VoiceUsageMinutes", typeof(decimal));
    exportTable.Columns.Add("ProcessedDateTime", typeof(DateTime));
    
    var processedDateTime = DateTime.UtcNow;
    
    // Process each row from source data
    for (int i = 1; i < sourceTable.Rows.Count; i++) // Skip header row
    {
        try
        {
            var sourceRow = sourceTable.Rows[i];
            
            // Validate and extract required fields
            var iccid = sourceRow[JasperExportDeviceUsageConfig.ICCIDColumnIndex]?.ToString()?.Trim();
            var msisdn = sourceRow[JasperExportDeviceUsageConfig.MSISDNColumnIndex]?.ToString()?.Trim();
            var usageDateStr = sourceRow[JasperExportDeviceUsageConfig.UsageDateColumnIndex]?.ToString()?.Trim();
            var dataUsageStr = sourceRow[JasperExportDeviceUsageConfig.DataUsageColumnIndex]?.ToString()?.Trim();
            
            // Validate required fields
            if (string.IsNullOrEmpty(iccid) || string.IsNullOrEmpty(usageDateStr))
            {
                errorRowCount++;
                continue;
            }
            
            // Parse and validate data types
            if (!DateTime.TryParse(usageDateStr, out var usageDate))
            {
                errorRowCount++;
                continue;
            }
            
            decimal.TryParse(dataUsageStr, out var dataUsage);
            int.TryParse(sourceRow[JasperExportDeviceUsageConfig.SMSUsageColumnIndex]?.ToString(), out var smsUsage);
            decimal.TryParse(sourceRow[JasperExportDeviceUsageConfig.VoiceUsageColumnIndex]?.ToString(), out var voiceUsage);
            
            // Create staging row
            var exportRow = exportTable.NewRow();
            exportRow["ServiceProviderId"] = serviceProviderId;
            exportRow["ICCID"] = iccid;
            exportRow["MSISDN"] = msisdn ?? string.Empty;
            exportRow["UsageDate"] = usageDate;
            exportRow["DataUsageMB"] = dataUsage;
            exportRow["SMSUsage"] = smsUsage;
            exportRow["VoiceUsageMinutes"] = voiceUsage;
            exportRow["ProcessedDateTime"] = processedDateTime;
            
            exportTable.Rows.Add(exportRow);
            validRowCount++;
        }
        catch (Exception ex)
        {
            LogWarning(context, "ROW_ERROR", $"Error processing row {i}: {ex.Message}");
            errorRowCount++;
        }
    }
    
    LogInfo(context, "VALIDATION", $"Data validation complete. Valid rows: {validRowCount}, Error rows: {errorRowCount}");
    return exportTable;
}
```

**Low-Level Flow**:
1. **File Loading**: Byte array loaded into Excel processing library (EPPlus/ExcelDataReader)
2. **Worksheet Extraction**: First worksheet extracted as DataTable
3. **Schema Validation**: Column count validated against expected Jasper export format
4. **Row Processing**: Each data row validated and transformed to staging table format
5. **Data Type Conversion**: String values parsed to appropriate data types with error handling
6. **Bulk Insert**: Valid records bulk loaded to JasperDeviceExportStaging table
7. **Result Logging**: Processing results logged for audit and monitoring

---

## 6. Error Handling & Retry Process

### Multi-Level Retry Configuration Implementation

#### Retry Policy Definitions
```csharp
// SQL-specific retry policy with exponential backoff
private static readonly RetryPolicy SqlRetryPolicy = Policy
    .Handle<SqlException>(ShouldRetry)
    .Or<TimeoutException>()
    .Or<InvalidOperationException>(ex => ex.Message.Contains("timeout"))
    .WaitAndRetryAsync(SQL_TRANSIENT_RETRY_MAX_COUNT,
        retryAttempt => TimeSpan.FromSeconds(Math.Pow(SQL_TRANSIENT_RETRY_BASE_SECONDS, retryAttempt)),
        (exception, timeSpan, retryCount, sqlContext) => 
        {
            var context = (KeySysLambdaContext)sqlContext["context"];
            LogWarning(context, "SQL_RETRY", 
                $"SQL transient error encountered - delaying for {timeSpan.TotalSeconds}s, retry {retryCount}/{SQL_TRANSIENT_RETRY_MAX_COUNT}. Error: {exception.Message}");
        });

// HTTP-specific retry policy
private static readonly AsyncRetryPolicy HttpRetryPolicy = Policy
    .Handle<HttpRequestException>()
    .Or<TaskCanceledException>()
    .Or<SocketException>()
    .WaitAndRetryAsync(HTTP_TRANSIENT_RETRY_MAX_COUNT,
        retryAttempt => TimeSpan.FromSeconds(Math.Pow(HTTP_TRANSIENT_RETRY_BASE_SECONDS, retryAttempt)),
        (exception, timeSpan, retryCount, httpContext) =>
        {
            var context = (KeySysLambdaContext)httpContext["context"];
            LogWarning(context, "HTTP_RETRY", 
                $"HTTP transient error - delaying for {timeSpan.TotalSeconds}s, retry {retryCount}/{HTTP_TRANSIENT_RETRY_MAX_COUNT}. Error: {exception.Message}");
        });

// File download retry policy with fallback
private static AsyncRetryPolicy<byte[]> CreateFileDownloadRetryPolicy(KeySysLambdaContext context, ExportDeviceUsageSqsValues sqsValues)
{
    var fallbackPolicy = Policy<byte[]>
        .Handle<Exception>()
        .FallbackAsync(async (cancellationToken) =>
        {
            LogError(context, "DOWNLOAD_FALLBACK", "All download retry attempts failed, sending error notification");
            await SendErrorEmailNotificationAsync(context, sqsValues.IntegrationType, 
                BuildDownloadErrorEmailBody(context, "File download failed after all retry attempts"));
            return null;
        });

    var downloadRetryPolicy = Policy
        .Handle<Exception>()
        .WaitAndRetryAsync(HTTP_TRANSIENT_RETRY_MAX_COUNT,
            retryAttempt => TimeSpan.FromSeconds(Math.Pow(HTTP_TRANSIENT_RETRY_BASE_SECONDS, retryAttempt)),
            (exception, timeSpan, retryCount, downloadContext) =>
            {
                LogWarning(context, "DOWNLOAD_RETRY", 
                    $"File download error - retry {retryCount}/{HTTP_TRANSIENT_RETRY_MAX_COUNT} in {timeSpan.TotalSeconds}s. Error: {exception.Message}");
            });

    return fallbackPolicy.WrapAsync(downloadRetryPolicy);
}
```

**Low-Level Flow**:
1. **Exception Classification**: Incoming exceptions categorized by type (SQL, HTTP, etc.)
2. **Retry Decision**: Policy evaluates if exception is transient and retryable
3. **Backoff Calculation**: Exponential backoff calculated based on retry attempt number
4. **Delay Implementation**: Thread sleeps for calculated delay period
5. **Retry Execution**: Operation re-attempted with fresh context
6. **Fallback Activation**: After max retries, fallback policy executes error handling

#### SQL Timeout Detection Implementation
```csharp
private static bool ShouldRetry(Exception exception)
{
    // Check for known transient SQL exceptions
    if (SqlServerTransientExceptionDetector.ShouldRetryOn(exception))
    {
        return true;
    }
    
    // Check for specific timeout conditions
    if (IsSqlTimeout(exception))
    {
        return true;
    }
    
    // Check for connection-related issues
    if (exception is SqlException sqlEx)
    {
        return sqlEx.Number switch
        {
            -2 => true,      // Timeout
            2 => true,       // Connection timeout
            53 => true,      // Network path not found
            121 => true,     // Semaphore timeout
            1205 => true,    // Deadlock victim
            1222 => true,    // Lock request timeout
            _ => false
        };
    }
    
    return false;
}

private static bool IsSqlTimeout(Exception exception)
{
    return exception is SqlException sqlException && 
           sqlException.Number == SQL_ERROR_NUMBER_DB_TIMEOUT;
}

// Enhanced SQL execution with retry
public async Task<T> ExecuteWithRetryAsync<T>(KeySysLambdaContext context, Func<Task<T>> operation, string operationName)
{
    var contextData = new Dictionary<string, object> { ["context"] = context };
    
    return await SqlRetryPolicy.ExecuteAsync(async (ctx) =>
    {
        var stopwatch = Stopwatch.StartNew();
        LogInfo(context, "SQL_EXECUTE", $"Executing {operationName}");
        
        try
        {
            var result = await operation();
            stopwatch.Stop();
            LogInfo(context, "SQL_SUCCESS", $"{operationName} completed in {stopwatch.ElapsedMilliseconds}ms");
            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            LogError(context, "SQL_ERROR", $"{operationName} failed after {stopwatch.ElapsedMilliseconds}ms: {ex.Message}", ex);
            throw;
        }
    }, contextData);
}
```

**Low-Level Flow**:
1. **Exception Analysis**: SQL exception number analyzed against known transient error codes
2. **Retry Eligibility**: Determines if error is transient and worth retrying
3. **Context Preservation**: Operation context maintained across retry attempts
4. **Performance Monitoring**: Execution time tracked for each attempt
5. **Success Logging**: Successful operations logged with performance metrics

### Error Categories & Handling Implementation

#### Comprehensive Error Classification
```csharp
public enum ErrorCategory
{
    SqlTransient,
    SqlPermanent,
    HttpTransient,
    HttpPermanent,
    EmailProcessing,
    Authentication,
    FileProcessing,
    LambdaTimeout,
    Configuration,
    BusinessLogic
}

public class ErrorHandler
{
    private readonly KeySysLambdaContext _context;
    private readonly Dictionary<ErrorCategory, IErrorHandlingStrategy> _strategies;
    
    public ErrorHandler(KeySysLambdaContext context)
    {
        _context = context;
        _strategies = new Dictionary<ErrorCategory, IErrorHandlingStrategy>
        {
            { ErrorCategory.SqlTransient, new SqlTransientErrorStrategy() },
            { ErrorCategory.HttpTransient, new HttpTransientErrorStrategy() },
            { ErrorCategory.EmailProcessing, new EmailProcessingErrorStrategy() },
            { ErrorCategory.Authentication, new AuthenticationErrorStrategy() },
            { ErrorCategory.FileProcessing, new FileProcessingErrorStrategy() },
            { ErrorCategory.LambdaTimeout, new LambdaTimeoutErrorStrategy() },
            { ErrorCategory.Configuration, new ConfigurationErrorStrategy() },
            { ErrorCategory.BusinessLogic, new BusinessLogicErrorStrategy() }
        };
    }
    
    public async Task<bool> HandleErrorAsync(Exception exception, string operationContext)
    {
        var category = ClassifyError(exception);
        var strategy = _strategies[category];
        
        LogError(_context, "ERROR_HANDLING", 
            $"Handling {category} error in {operationContext}: {exception.Message}", exception);
        
        return await strategy.HandleAsync(_context, exception, operationContext);
    }
    
    private ErrorCategory ClassifyError(Exception exception)
    {
        return exception switch
        {
            SqlException sqlEx when ShouldRetry(sqlEx) => ErrorCategory.SqlTransient,
            SqlException => ErrorCategory.SqlPermanent,
            HttpRequestException => ErrorCategory.HttpTransient,
            TaskCanceledException => ErrorCategory.HttpTransient,
            SocketException => ErrorCategory.HttpTransient,
            AuthenticationException => ErrorCategory.Authentication,
            FileFormatException => ErrorCategory.FileProcessing,
            TimeoutException when _context.Context.RemainingTime.TotalSeconds < 60 => ErrorCategory.LambdaTimeout,
            ConfigurationException => ErrorCategory.Configuration,
            BusinessRuleException => ErrorCategory.BusinessLogic,
            _ => ErrorCategory.BusinessLogic
        };
    }
}
```

**Low-Level Flow**:
1. **Error Classification**: Exception type and properties analyzed to determine category
2. **Strategy Selection**: Appropriate error handling strategy selected based on category
3. **Context Logging**: Error details logged with operation context for debugging
4. **Strategy Execution**: Selected strategy executes category-specific handling logic
5. **Recovery Decision**: Strategy returns whether operation should continue or abort

---

## 7. Failed/Unprocessed Records Process

### Email Processing Failure Handling

#### Timeout Detection and Recovery
```csharp
public class EmailProcessingTimeoutHandler
{
    private const int DEFAULT_EMAIL_WAIT_MAX_RETRIES = 17; // 90 minutes total (18 * 5 minutes)
    
    public async Task<EmailProcessingResult> HandleEmailTimeoutAsync(KeySysLambdaContext context, ExportDeviceUsageSqsValues sqsValues)
    {
        var result = new EmailProcessingResult();
        var serviceProvider = ServiceProviderCommon.GetServiceProvider(context.CentralDbConnectionString, sqsValues.ServiceProviderId.Value);
        
        LogInfo(context, "TIMEOUT_CHECK", 
            $"Checking email timeout. WaitCount: {sqsValues.WaitCount}, MaxRetries: {DEFAULT_EMAIL_WAIT_MAX_RETRIES}");
        
        // Check for processing timeout
        if (sqsValues.WaitCount >= DEFAULT_EMAIL_WAIT_MAX_RETRIES)
        {
            LogWarning(context, "EMAIL_TIMEOUT", 
                $"Email processing timeout exceeded for ServiceProvider: {serviceProvider.DisplayName}");
            
            // Get error file information
            var errorFiles = await GetUnprocessedEmailFilesAsync(context, sqsValues);
            var numberOfErrorFiles = errorFiles.Count;
            
            LogInfo(context, "ERROR_FILES", $"Found {numberOfErrorFiles} unprocessed email files");
            
            if (numberOfErrorFiles > 0)
            {
                // Log error file details
                foreach (var errorFile in errorFiles)
                {
                    LogError(context, "UNPROCESSED_FILE", 
                        $"Unprocessed file: Subject='{errorFile.Subject}', Date='{errorFile.Date}', Size={errorFile.AttachmentSize}");
                }
                
                // Send error notification with file details
                await SendErrorEmailNotificationAsync(context, sqsValues.IntegrationType, 
                    BuildIncorrectFormatFileEmailBody(context, errorFiles, serviceProvider.DisplayName));
                
                result.HasTimedOut = true;
                result.ErrorFileCount = numberOfErrorFiles;
            }
            
            // Mark as all emails processed due to timeout to prevent infinite loop
            sqsValues.ProcessedEmailsCount = sqsValues.TotalEmailsToProcess;
            result.ForceComplete = true;
            
            LogInfo(context, "TIMEOUT_RESOLUTION", 
                $"Marked processing as complete due to timeout. ProcessedCount set to: {sqsValues.ProcessedEmailsCount}");
        }
        else
        {
            result.ShouldContinueWaiting = true;
            result.NextWaitCount = sqsValues.WaitCount + 1;
            
            LogInfo(context, "CONTINUE_WAITING", 
                $"Continuing to wait for emails. Next WaitCount: {result.NextWaitCount}");
        }
        
        return result;
    }
    
    private async Task<List<UnprocessedEmailFile>> GetUnprocessedEmailFilesAsync(KeySysLambdaContext context, ExportDeviceUsageSqsValues sqsValues)
    {
        var unprocessedFiles = new List<UnprocessedEmailFile>();
        
        using var client = new ImapClient { ServerCertificateValidationCallback = (s, c, h, e) => true };
        
        try
        {
            client.Connect(sqsValues.JasperProviderSettings.JasperExportMailServer, 
                          sqsValues.JasperProviderSettings.JasperExportMailPort, 
                          sqsValues.JasperProviderSettings.UseSSL);
                          
            client.Authenticate(sqsValues.JasperProviderSettings.JasperExportMailUsername, 
                               context.Base64Service.Base64Decode(sqsValues.JasperProviderSettings.JasperExportMailPassword));
            
            client.Inbox.Open(FolderAccess.ReadOnly);
            
            for (int i = 0; i < client.Inbox.Count; i++)
            {
                var message = client.Inbox.GetMessage(i);
                
                if (message.Subject == JasperConstants.EXPORT_EMAIL_SUBJECT && 
                    message.To.Mailboxes.Any(m => m.Address == sqsValues.JasperProviderSettings.JasperExportMailAlias))
                {
                    var attachmentSize = message.Attachments.Sum(a => a.ContentObject?.ToString()?.Length ?? 0);
                    
                    unprocessedFiles.Add(new UnprocessedEmailFile
                    {
                        Subject = message.Subject,
                        Date = message.Date.DateTime,
                        From = message.From.ToString(),
                        AttachmentCount = message.Attachments.Count(),
                        AttachmentSize = attachmentSize,
                        MessageId = message.MessageId
                    });
                }
            }
        }
        catch (Exception ex)
        {
            LogError(context, "EMAIL_SCAN_ERROR", $"Error scanning for unprocessed emails: {ex.Message}", ex);
        }
        finally
        {
            if (client.IsConnected)
            {
                client.Disconnect(true);
            }
        }
        
        return unprocessedFiles;
    }
}

public class EmailProcessingResult
{
    public bool HasTimedOut { get; set; }
    public bool ForceComplete { get; set; }
    public bool ShouldContinueWaiting { get; set; }
    public int NextWaitCount { get; set; }
    public int ErrorFileCount { get; set; }
}

public class UnprocessedEmailFile
{
    public string Subject { get; set; }
    public DateTime Date { get; set; }
    public string From { get; set; }
    public int AttachmentCount { get; set; }
    public int AttachmentSize { get; set; }
    public string MessageId { get; set; }
}
```

**Low-Level Flow**:
1. **Timeout Detection**: WaitCount compared against maximum retry threshold
2. **File Scanning**: IMAP connection established to scan for unprocessed emails
3. **Error Cataloging**: Unprocessed emails catalogued with metadata for reporting
4. **Notification Generation**: Error notification email generated with file details
5. **State Resolution**: Processing state marked as complete to prevent infinite loops

### Failure Tracking Implementation

#### Processing State Management
```csharp
public class ProcessingStateManager
{
    public class EmailProcessingState
    {
        public int TotalEmailsToProcess { get; set; }
        public int ProcessedEmailsCount { get; set; }  
        public int WaitCount { get; set; }
        public DateTime ExportStartDateTime { get; set; }
        public List<ProcessedFile> ProcessedFiles { get; set; } = new List<ProcessedFile>();
        public List<FailedFile> FailedFiles { get; set; } = new List<FailedFile>();
        
        public bool IsTimedOut => WaitCount > DEFAULT_EMAIL_WAIT_MAX_RETRIES;
        public bool HaveAllEmailsBeenProcessed => ProcessedEmailsCount >= TotalEmailsToProcess;
        public TimeSpan ProcessingDuration => DateTime.UtcNow - ExportStartDateTime;
        public double CompletionPercentage => TotalEmailsToProcess > 0 ? 
            (double)ProcessedEmailsCount / TotalEmailsToProcess * 100 : 0;
    }
    
    public class ProcessedFile
    {
        public string FileName { get; set; }
        public DateTime ProcessedDateTime { get; set; }
        public int RecordCount { get; set; }
        public long FileSizeBytes { get; set; }
        public TimeSpan ProcessingTime { get; set; }
    }
    
    public class FailedFile
    {
        public string FileName { get; set; }
        public DateTime FailedDateTime { get; set; }
        public string ErrorMessage { get; set; }
        public string StackTrace { get; set; }
        public int RetryCount { get; set; }
        public FileFailureReason FailureReason { get; set; }
    }
    
    public enum FileFailureReason
    {
        DownloadFailed,
        FormatInvalid,
        DataValidationFailed,
        DatabaseInsertFailed,
        UnknownError
    }
    
    public async Task LogProcessingResultAsync(KeySysLambdaContext context, string fileName, 
        bool success, int recordCount = 0, string errorMessage = null, Exception exception = null)
    {
        var logEntry = new ProcessingLogEntry
        {
            FileName = fileName,
            ProcessedDateTime = DateTime.UtcNow,
            Success = success,
            RecordCount = recordCount,
            ErrorMessage = errorMessage,
            StackTrace = exception?.StackTrace,
            ServiceProviderId = context.ServiceProviderId,
            IntegrationType = context.IntegrationType
        };
        
        // Log to database for audit trail
        await SaveProcessingLogAsync(context, logEntry);
        
        // Log to application logs
        if (success)
        {
            LogInfo(context, "FILE_SUCCESS", 
                $"Successfully processed file: {fileName}, Records: {recordCount}");
        }
        else
        {
            LogError(context, "FILE_FAILURE", 
                $"Failed to process file: {fileName}, Error: {errorMessage}", exception);
        }
    }
    
    private async Task SaveProcessingLogAsync(KeySysLambdaContext context, ProcessingLogEntry logEntry)
    {
        using var connection = new SqlConnection(context.GeneralProviderSettings.JasperDbConnectionString);
        using var command = new SqlCommand("usp_JasperFileProcessingLog_Insert", connection);
        
        command.CommandType = CommandType.StoredProcedure;
        command.Parameters.AddWithValue("@FileName", logEntry.FileName);
        command.Parameters.AddWithValue("@ProcessedDateTime", logEntry.ProcessedDateTime);
        command.Parameters.AddWithValue("@Success", logEntry.Success);
        command.Parameters.AddWithValue("@RecordCount", logEntry.RecordCount);
        command.Parameters.AddWithValue("@ErrorMessage", logEntry.ErrorMessage ?? (object)DBNull.Value);
        command.Parameters.AddWithValue("@StackTrace", logEntry.StackTrace ?? (object)DBNull.Value);
        command.Parameters.AddWithValue("@ServiceProviderId", logEntry.ServiceProviderId);
        command.Parameters.AddWithValue("@IntegrationType", (int)logEntry.IntegrationType);
        
        await connection.OpenAsync();
        await command.ExecuteNonQueryAsync();
    }
}
```

**Low-Level Flow**:
1. **State Tracking**: Processing state continuously updated with success/failure counts
2. **File Logging**: Each file processing result logged with detailed metadata
3. **Database Persistence**: Processing logs saved to database for audit and reporting
4. **Progress Calculation**: Completion percentage calculated for monitoring dashboards
5. **Error Categorization**: Failures categorized by reason for targeted remediation

### Recovery Process Implementation

#### Automatic Recovery Mechanisms
```csharp
public class RecoveryManager
{
    public async Task<RecoveryResult> AttemptRecoveryAsync(KeySysLambdaContext context, 
        ExportDeviceUsageSqsValues sqsValues, List<FailedFile> failedFiles)
    {
        var recoveryResult = new RecoveryResult();
        LogInfo(context, "RECOVERY", $"Attempting recovery for {failedFiles.Count} failed files");
        
        foreach (var failedFile in failedFiles)
        {
            try
            {
                var recoveryStrategy = GetRecoveryStrategy(failedFile.FailureReason);
                var success = await recoveryStrategy.RecoverAsync(context, failedFile);
                
                if (success)
                {
                    recoveryResult.RecoveredFiles.Add(failedFile.FileName);
                    LogInfo(context, "RECOVERY_SUCCESS", $"Successfully recovered file: {failedFile.FileName}");
                }
                else
                {
                    recoveryResult.UnrecoverableFiles.Add(failedFile.FileName);
                    LogWarning(context, "RECOVERY_FAILED", $"Could not recover file: {failedFile.FileName}");
                }
            }
            catch (Exception ex)
            {
                recoveryResult.UnrecoverableFiles.Add(failedFile.FileName);
                LogError(context, "RECOVERY_ERROR", 
                    $"Error during recovery of file {failedFile.FileName}: {ex.Message}", ex);
            }
        }
        
        // Send recovery report
        if (recoveryResult.RecoveredFiles.Any() || recoveryResult.UnrecoverableFiles.Any())
        {
            await SendRecoveryReportAsync(context, sqsValues.IntegrationType, recoveryResult);
        }
        
        return recoveryResult;
    }
    
    private IRecoveryStrategy GetRecoveryStrategy(FileFailureReason failureReason)
    {
        return failureReason switch
        {
            FileFailureReason.DownloadFailed => new DownloadRetryStrategy(),
            FileFailureReason.FormatInvalid => new FormatValidationStrategy(),
            FileFailureReason.DataValidationFailed => new DataCleanupStrategy(),
            FileFailureReason.DatabaseInsertFailed => new DatabaseRetryStrategy(),
            _ => new ManualInterventionStrategy()
        };
    }
}

public class RecoveryResult
{
    public List<string> RecoveredFiles { get; set; } = new List<string>();
    public List<string> UnrecoverableFiles { get; set; } = new List<string>();
    public int TotalAttempted => RecoveredFiles.Count + UnrecoverableFiles.Count;
    public double SuccessRate => TotalAttempted > 0 ? (double)RecoveredFiles.Count / TotalAttempted * 100 : 0;
}
```

**Low-Level Flow**:
1. **Strategy Selection**: Recovery strategy selected based on failure reason
2. **Recovery Execution**: Strategy-specific recovery logic executed for each failed file
3. **Success Tracking**: Recovery attempts tracked with success/failure outcomes
4. **Reporting**: Recovery results compiled and reported to operations team
5. **Escalation**: Unrecoverable files escalated for manual intervention

---

## 8. Lambda Execution Management Process

### Execution Time Limits Implementation

#### Timeout Prevention and Management
```csharp
public class LambdaExecutionManager
{
    private const int LAMBDA_REMAINING_TIME_LIMIT_IN_SECONDS = 60;
    private const int LAMBDA_MAX_EXECUTION_TIME_SECONDS = 900; // 15 minutes
    private readonly Stopwatch _executionTimer;
    
    public LambdaExecutionManager()
    {
        _executionTimer = Stopwatch.StartNew();
    }
    
    public bool ShouldContinueProcessing(KeySysLambdaContext context, string operationName = "")
    {
        var remainingTime = context.Context.RemainingTime;
        var elapsedTime = _executionTimer.Elapsed;
        
        LogInfo(context, "TIMING", 
            $"Execution check for {operationName}: Elapsed={elapsedTime.TotalSeconds:F1}s, Remaining={remainingTime.TotalSeconds:F1}s");
        
        if (remainingTime.TotalSeconds <= LAMBDA_REMAINING_TIME_LIMIT_IN_SECONDS)
        {
            LogWarning(context, "TIMEOUT_WARNING", 
                $"Approaching Lambda timeout limit. Remaining: {remainingTime.TotalSeconds:F1}s, Threshold: {LAMBDA_REMAINING_TIME_LIMIT_IN_SECONDS}s");
            return false;
        }
        
        return true;
    }
    
    public async Task<bool> ExecuteWithTimeoutCheckAsync<T>(KeySysLambdaContext context, 
        Func<Task<T>> operation, string operationName, 
        Func<T, Task> onSuccess = null, Func<Exception, Task> onError = null)
    {
        if (!ShouldContinueProcessing(context, operationName))
        {
            LogInfo(context, "TIMEOUT_SKIP", $"Skipping {operationName} due to timeout constraints");
            return false;
        }
        
        var operationTimer = Stopwatch.StartNew();
        
        try
        {
            LogInfo(context, "OPERATION_START", $"Starting {operationName}");
            
            var result = await operation();
            
            operationTimer.Stop();
            LogInfo(context, "OPERATION_SUCCESS", 
                $"{operationName} completed successfully in {operationTimer.ElapsedMilliseconds}ms");
            
            if (onSuccess != null)
            {
                await onSuccess(result);
            }
            
            return true;
        }
        catch (Exception ex)
        {
            operationTimer.Stop();
            LogError(context, "OPERATION_ERROR", 
                $"{operationName} failed after {operationTimer.ElapsedMilliseconds}ms: {ex.Message}", ex);
            
            if (onError != null)
            {
                await onError(ex);
            }
            
            throw;
        }
    }
    
    public ExecutionSummary GetExecutionSummary(KeySysLambdaContext context)
    {
        _executionTimer.Stop();
        
        return new ExecutionSummary
        {
            TotalExecutionTime = _executionTimer.Elapsed,
            RemainingTime = context.Context.RemainingTime,
            MemoryUsedMB = GetMemoryUsage(),
            MaxMemoryMB = context.Context.MemoryLimitInMB,
            RequestId = context.Context.AwsRequestId,
            FunctionName = context.Context.FunctionName,
            FunctionVersion = context.Context.FunctionVersion
        };
    }
    
    private double GetMemoryUsage()
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        
        var process = Process.GetCurrentProcess();
        return process.WorkingSet64 / (1024.0 * 1024.0); // Convert to MB
    }
}

public class ExecutionSummary
{
    public TimeSpan TotalExecutionTime { get; set; }
    public TimeSpan RemainingTime { get; set; }
    public double MemoryUsedMB { get; set; }
    public int MaxMemoryMB { get; set; }
    public string RequestId { get; set; }
    public string FunctionName { get; set; }
    public string FunctionVersion { get; set; }
    
    public double ExecutionEfficiency => TotalExecutionTime.TotalSeconds / 
        (TotalExecutionTime.TotalSeconds + RemainingTime.TotalSeconds) * 100;
    public double MemoryEfficiency => MemoryUsedMB / MaxMemoryMB * 100;
}
```

**Low-Level Flow**:
1. **Execution Monitoring**: Continuous tracking of elapsed time and remaining Lambda execution time
2. **Timeout Prevention**: Operations checked against remaining time before execution
3. **Graceful Degradation**: Non-critical operations skipped when approaching timeout
4. **Resource Monitoring**: Memory usage tracked to prevent out-of-memory conditions
5. **Performance Metrics**: Execution efficiency calculated for optimization insights

### State Management Across Invocations

#### Comprehensive State Preservation
```csharp
public class StateManager
{
    public async Task<string> SerializeProcessingStateAsync(KeySysLambdaContext context, ProcessingState state)
    {
        var stateData = new
        {
            ServiceProviderId = state.ServiceProviderId,
            IntegrationType = state.IntegrationType,
            WaitCount = state.WaitCount,
            TotalEmailsToProcess = state.TotalEmailsToProcess,
            ProcessedEmailsCount = state.ProcessedEmailsCount,
            ExportStartDateTime = state.ExportStartDateTime,
            ProcessedFiles = state.ProcessedFiles,
            FailedFiles = state.FailedFiles,
            CurrentBatch = state.CurrentBatch,
            BatchProgress = state.BatchProgress,
            LastCheckpointTime = DateTime.UtcNow,
            ExecutionContext = new
            {
                RequestId = context.Context.AwsRequestId,
                FunctionVersion = context.Context.FunctionVersion,
                RemainingTime = context.Context.RemainingTime.TotalSeconds
            }
        };
        
        var json = JsonConvert.SerializeObject(stateData, Formatting.None);
        LogInfo(context, "STATE_SERIALIZE", $"Serialized processing state: {json.Length} characters");
        
        return json;
    }
    
    public ProcessingState DeserializeProcessingState(KeySysLambdaContext context, string serializedState)
    {
        try
        {
            var stateData = JsonConvert.DeserializeObject<dynamic>(serializedState);
            
            var state = new ProcessingState
            {
                ServiceProviderId = stateData.ServiceProviderId,
                IntegrationType = (IntegrationType)stateData.IntegrationType,
                WaitCount = stateData.WaitCount,
                TotalEmailsToProcess = stateData.TotalEmailsToProcess,
                ProcessedEmailsCount = stateData.ProcessedEmailsCount,
                ExportStartDateTime = stateData.ExportStartDateTime,
                ProcessedFiles = JsonConvert.DeserializeObject<List<ProcessedFile>>(stateData.ProcessedFiles?.ToString() ?? "[]"),
                FailedFiles = JsonConvert.DeserializeObject<List<FailedFile>>(stateData.FailedFiles?.ToString() ?? "[]"),
                CurrentBatch = stateData.CurrentBatch,
                BatchProgress = stateData.BatchProgress
            };
            
            LogInfo(context, "STATE_DESERIALIZE", $"Deserialized processing state: WaitCount={state.WaitCount}, ProcessedCount={state.ProcessedEmailsCount}");
            
            return state;
        }
        catch (Exception ex)
        {
            LogError(context, "STATE_DESERIALIZE_ERROR", $"Error deserializing state: {ex.Message}", ex);
            throw new InvalidOperationException("Failed to deserialize processing state", ex);
        }
    }
}

public class ProcessingState
{
    public int ServiceProviderId { get; set; }
    public IntegrationType IntegrationType { get; set; }
    public int WaitCount { get; set; }
    public int TotalEmailsToProcess { get; set; }
    public int ProcessedEmailsCount { get; set; }
    public DateTime ExportStartDateTime { get; set; }
    public List<ProcessedFile> ProcessedFiles { get; set; } = new List<ProcessedFile>();
    public List<FailedFile> FailedFiles { get; set; } = new List<FailedFile>();
    public int CurrentBatch { get; set; }
    public Dictionary<string, object> BatchProgress { get; set; } = new Dictionary<string, object>();
    
    public bool IsComplete => ProcessedEmailsCount >= TotalEmailsToProcess;
    public TimeSpan ProcessingDuration => DateTime.UtcNow - ExportStartDateTime;
    public double CompletionPercentage => TotalEmailsToProcess > 0 ? 
        (double)ProcessedEmailsCount / TotalEmailsToProcess * 100 : 0;
}
```

**Low-Level Flow**:
1. **State Serialization**: Complex processing state serialized to JSON for message attributes
2. **Context Preservation**: Lambda execution context included for debugging and monitoring
3. **State Validation**: Deserialized state validated for consistency and completeness
4. **Progress Tracking**: Processing progress maintained across Lambda invocations
5. **Error Recovery**: State corruption handled with graceful degradation

### Continuation Pattern Implementation

#### Seamless Lambda Handoff
```csharp
public class ContinuationManager
{
    public async Task<bool> ContinueProcessingAsync(KeySysLambdaContext context, ProcessingState currentState)
    {
        var executionManager = new LambdaExecutionManager();
        
        // Check if we should continue in current Lambda or hand off
        if (!executionManager.ShouldContinueProcessing(context, "ContinuationCheck"))
        {
            LogInfo(context, "HANDOFF", "Initiating handoff to new Lambda instance");
            await HandOffToNewInstanceAsync(context, currentState);
            return false; // Current instance should terminate
        }
        
        return true; // Continue processing in current instance
    }
    
    private async Task HandOffToNewInstanceAsync(KeySysLambdaContext context, ProcessingState currentState)
    {
        // Create continuation message with preserved state
        var continuationMessage = await CreateContinuationMessageAsync(context, currentState);
        
        // Send to queue with minimal delay for immediate processing
        var request = new SendMessageRequest
        {
            QueueUrl = context.ExportDeviceUsageQueueURL,
            DelaySeconds = 0, // Immediate processing
            MessageBody = "Lambda Continuation Processing",
            MessageAttributes = continuationMessage.MessageAttributes
        };
        
        LogInfo(context, "CONTINUATION", $"Sending continuation message with state: ProcessedCount={currentState.ProcessedEmailsCount}");
        
        await context.SqsClient.SendMessageAsync(request);
        
        // Log handoff metrics
        var executionSummary = new LambdaExecutionManager().GetExecutionSummary(context);
        LogInfo(context, "HANDOFF_METRICS", 
            $"Lambda handoff: ExecutionTime={executionSummary.TotalExecutionTime.TotalSeconds:F1}s, " +
            $"MemoryUsed={executionSummary.MemoryUsedMB:F1}MB, Efficiency={executionSummary.ExecutionEfficiency:F1}%");
    }
    
    private async Task<ContinuationMessage> CreateContinuationMessageAsync(KeySysLambdaContext context, ProcessingState state)
    {
        var stateManager = new StateManager();
        var serializedState = await stateManager.SerializeProcessingStateAsync(context, state);
        
        return new ContinuationMessage
        {
            MessageAttributes = new Dictionary<string, MessageAttributeValue>
            {
                { AttributeNames.InitializeProcessing, new MessageAttributeValue 
                    { StringValue = "false", DataType = "String" }},
                { AttributeNames.LoadStagingData, new MessageAttributeValue 
                    { StringValue = "false", DataType = "String" }},
                { AttributeNames.ServiceProviderId, new MessageAttributeValue 
                    { StringValue = state.ServiceProviderId.ToString(), DataType = "Number" }},
                { AttributeNames.IntegrationType, new MessageAttributeValue 
                    { StringValue = ((int)state.IntegrationType).ToString(), DataType = "Number" }},
                { AttributeNames.WaitCount, new MessageAttributeValue 
                    { StringValue = state.WaitCount.ToString(), DataType = "Number" }},
                { AttributeNames.TotalEmailsToProcess, new MessageAttributeValue 
                    { StringValue = state.TotalEmailsToProcess.ToString(), DataType = "Number" }},
                { AttributeNames.ProcessedEmailsCount, new MessageAttributeValue 
                    { StringValue = state.ProcessedEmailsCount.ToString(), DataType = "Number" }},
                { AttributeNames.ExportStartDateTime, new MessageAttributeValue 
                    { StringValue = state.ExportStartDateTime.ToString("O"), DataType = "String" }},
                { "ProcessingState", new MessageAttributeValue 
                    { StringValue = serializedState, DataType = "String" }},
                { "ContinuationTimestamp", new MessageAttributeValue 
                    { StringValue = DateTime.UtcNow.ToString("O"), DataType = "String" }}
            }
        };
    }
}

public class ContinuationMessage
{
    public Dictionary<string, MessageAttributeValue> MessageAttributes { get; set; }
}
```

**Low-Level Flow**:
1. **Timeout Detection**: Continuous monitoring of remaining Lambda execution time
2. **State Capture**: Current processing state captured and serialized
3. **Message Creation**: Continuation message created with complete state preservation
4. **Queue Submission**: Message sent to SQS queue with immediate processing (0 delay)
5. **Instance Termination**: Current Lambda instance gracefully terminates
6. **New Instance**: Fresh Lambda instance receives continuation message and resumes processing

### Progress Tracking Across Invocations

#### Comprehensive Progress Monitoring
```csharp
public class ProgressTracker
{
    public async Task UpdateProgressAsync(KeySysLambdaContext context, ProcessingState state)
    {
        var progressData = new ProgressData
        {
            ServiceProviderId = state.ServiceProviderId,
            IntegrationType = state.IntegrationType,
            ExportStartDateTime = state.ExportStartDateTime,
            TotalEmailsToProcess = state.TotalEmailsToProcess,
            ProcessedEmailsCount = state.ProcessedEmailsCount,
            CompletionPercentage = state.CompletionPercentage,
            ProcessingDuration = state.ProcessingDuration,
            CurrentLambdaRequestId = context.Context.AwsRequestId,
            LastUpdateDateTime = DateTime.UtcNow,
            EstimatedCompletionTime = CalculateEstimatedCompletion(state),
            ThroughputEmailsPerMinute = CalculateThroughput(state)
        };
        
        // Update database progress table
        await SaveProgressAsync(context, progressData);
        
        // Send progress notification if milestone reached
        if (IsProgressMilestone(state.CompletionPercentage))
        {
            await SendProgressNotificationAsync(context, progressData);
        }
        
        LogInfo(context, "PROGRESS", 
            $"Progress updated: {progressData.CompletionPercentage:F1}% complete, " +
            $"ETA: {progressData.EstimatedCompletionTime:yyyy-MM-dd HH:mm:ss}");
    }
    
    private DateTime CalculateEstimatedCompletion(ProcessingState state)
    {
        if (state.ProcessedEmailsCount == 0 || state.ProcessingDuration.TotalMinutes == 0)
            return DateTime.UtcNow.AddHours(1); // Default estimate
        
        var emailsPerMinute = state.ProcessedEmailsCount / state.ProcessingDuration.TotalMinutes;
        var remainingEmails = state.TotalEmailsToProcess - state.ProcessedEmailsCount;
        var estimatedMinutesRemaining = remainingEmails / Math.Max(emailsPerMinute, 0.1);
        
        return DateTime.UtcNow.AddMinutes(estimatedMinutesRemaining);
    }
    
    private double CalculateThroughput(ProcessingState state)
    {
        return state.ProcessingDuration.TotalMinutes > 0 ? 
            state.ProcessedEmailsCount / state.ProcessingDuration.TotalMinutes : 0;
    }
    
    private bool IsProgressMilestone(double completionPercentage)
    {
        var milestones = new[] { 25.0, 50.0, 75.0, 90.0, 100.0 };
        return milestones.Any(m => Math.Abs(completionPercentage - m) < 1.0);
    }
    
    private async Task SaveProgressAsync(KeySysLambdaContext context, ProgressData progressData)
    {
        using var connection = new SqlConnection(context.GeneralProviderSettings.JasperDbConnectionString);
        using var command = new SqlCommand("usp_JasperExportProgress_Update", connection);
        
        command.CommandType = CommandType.StoredProcedure;
        command.Parameters.AddWithValue("@ServiceProviderId", progressData.ServiceProviderId);
        command.Parameters.AddWithValue("@IntegrationType", (int)progressData.IntegrationType);
        command.Parameters.AddWithValue("@ExportStartDateTime", progressData.ExportStartDateTime);
        command.Parameters.AddWithValue("@TotalEmailsToProcess", progressData.TotalEmailsToProcess);
        command.Parameters.AddWithValue("@ProcessedEmailsCount", progressData.ProcessedEmailsCount);
        command.Parameters.AddWithValue("@CompletionPercentage", progressData.CompletionPercentage);
        command.Parameters.AddWithValue("@ProcessingDurationMinutes", progressData.ProcessingDuration.TotalMinutes);
        command.Parameters.AddWithValue("@CurrentLambdaRequestId", progressData.CurrentLambdaRequestId);
        command.Parameters.AddWithValue("@LastUpdateDateTime", progressData.LastUpdateDateTime);
        command.Parameters.AddWithValue("@EstimatedCompletionTime", progressData.EstimatedCompletionTime);
        command.Parameters.AddWithValue("@ThroughputEmailsPerMinute", progressData.ThroughputEmailsPerMinute);
        
        await connection.OpenAsync();
        await command.ExecuteNonQueryAsync();
    }
}

public class ProgressData
{
    public int ServiceProviderId { get; set; }
    public IntegrationType IntegrationType { get; set; }
    public DateTime ExportStartDateTime { get; set; }
    public int TotalEmailsToProcess { get; set; }
    public int ProcessedEmailsCount { get; set; }
    public double CompletionPercentage { get; set; }
    public TimeSpan ProcessingDuration { get; set; }
    public string CurrentLambdaRequestId { get; set; }
    public DateTime LastUpdateDateTime { get; set; }
    public DateTime EstimatedCompletionTime { get; set; }
    public double ThroughputEmailsPerMinute { get; set; }
}
```

**Low-Level Flow**:
1. **Progress Calculation**: Completion percentage and throughput metrics calculated
2. **Database Persistence**: Progress data saved to database for monitoring dashboards
3. **ETA Calculation**: Estimated completion time calculated based on current throughput
4. **Milestone Detection**: Progress milestones detected for notification triggers
5. **Performance Metrics**: Throughput and efficiency metrics tracked for optimization

---

## Summary

This comprehensive documentation provides detailed code snippets and low-level flow descriptions for each section of the Jasper Device Usage Export Workflow. The implementation demonstrates sophisticated error handling, state management, and scalability patterns using AWS Lambda, SQS, and related services.

Key architectural patterns implemented:
- **Event-driven processing** with CloudWatch and SQS
- **Retry policies** with exponential backoff
- **State preservation** across Lambda invocations
- **Timeout management** and graceful degradation
- **Comprehensive error handling** and recovery mechanisms
- **Progress tracking** and monitoring capabilities

The workflow efficiently handles large-scale device usage data exports while maintaining reliability, observability, and error recovery capabilities.