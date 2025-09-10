using System.Collections.Generic;
using System.Data;
using Amop.Core.Helpers;
using Amop.Core.Logger;
using Microsoft.Data.SqlClient;

namespace Amop.Core.Repositories.Jasper
{
    public class JasperUsageStagingRepository
    {
        public void DeleteStagingWithPolicy(IKeysysLogger logger, string connectionString, int serviceProviderId, List<string> errorMessages, int retryCount = RetryPolicyHelper.SQL_TRANSIENT_RETRY_MAX_COUNT)
        {
            var sqlTransientRetryPolicy = RetryPolicyHelper.GetSqlTransientPolicy(logger, errorMessages, retryCount);
            sqlTransientRetryPolicy.Execute(() => DeleteStagingTables(connectionString, serviceProviderId, logger));
        }

        public void DeleteStagingTables(string connectionString, int serviceProviderId, IKeysysLogger logger)
        {
            using (var conn = new SqlConnection(connectionString))
            {
                conn.Open();

                logger.LogInfo("INFO", "Start delete from JasperDeviceUsageICCICDsToProcess");
                using (var cmd = new SqlCommand("DELETE FROM [dbo].[JasperDeviceUsageICCICDsToProcess] WHERE ServiceProviderId = @serviceProviderId", conn))
                {
                    cmd.Parameters.AddWithValue("@serviceProviderId", serviceProviderId);
                    cmd.ExecuteNonQuery();
                }
                logger.LogInfo("INFO", "Complete delete from JasperDeviceUsageICCICDsToProcess");

                logger.LogInfo("INFO", "Start delete from JasperDeviceUsageStaging");
                using (var cmd = new SqlCommand("DELETE FROM [dbo].[JasperDeviceUsageStaging] WHERE ServiceProviderId = @serviceProviderId", conn))
                {
                    cmd.Parameters.AddWithValue("@serviceProviderId", serviceProviderId);
                    cmd.ExecuteNonQuery();
                }
                logger.LogInfo("INFO", "Complete delete from JasperDeviceUsageStaging");

                logger.LogInfo("INFO", "Start delete from JasperDeviceExportStaging");
                using (var cmd = new SqlCommand("DELETE FROM [dbo].[JasperDeviceExportStaging] WHERE ServiceProviderId = @serviceProviderId", conn))
                {
                    cmd.Parameters.AddWithValue("@serviceProviderId", serviceProviderId);
                    cmd.ExecuteNonQuery();
                }
                logger.LogInfo("INFO", "Complete delete from JasperDeviceExportStaging");

                logger.LogInfo("INFO", "Start delete from JasperUsageByRatePlanStaging");
                using (var cmd = new SqlCommand("DELETE FROM [dbo].[JasperUsageByRatePlanStaging] WHERE ServiceProviderId = @serviceProviderId", conn))
                {
                    cmd.Parameters.AddWithValue("@serviceProviderId", serviceProviderId);
                    cmd.ExecuteNonQuery();
                }
                logger.LogInfo("INFO", "Complete delete from JasperUsageByRatePlanStaging");
            }
        }

        public void TruncateStagingWithPolicy(IKeysysLogger logger, string connectionString, List<string> errorMessages, int retryCount = RetryPolicyHelper.SQL_TRANSIENT_RETRY_MAX_COUNT)
        {
            var sqlTransientRetryPolicy = RetryPolicyHelper.GetSqlTransientPolicy(logger, errorMessages, retryCount);
            sqlTransientRetryPolicy.Execute(() => TruncateStagingTables(connectionString, logger));
        }

        public void TruncateStagingTables(string connectionString, IKeysysLogger logger)
        {
            using (var conn = new SqlConnection(connectionString))
            {
                if (conn.State != ConnectionState.Open)
                    conn.Open();

                logger.LogInfo("INFO", "Start TRUNCATE from JasperDeviceUsageICCICDsToProcess");
                using (var cmd = new SqlCommand("TRUNCATE TABLE[dbo].[JasperDeviceUsageICCICDsToProcess]", conn))
                {
                    cmd.ExecuteNonQuery();
                }
                logger.LogInfo("INFO", "Complete TRUNCATE from JasperDeviceUsageICCICDsToProcess");

                logger.LogInfo("INFO", "Start TRUNCATE from JasperDeviceUsageStaging");
                using (var cmd = new SqlCommand("TRUNCATE TABLE[dbo].[JasperDeviceUsageStaging]", conn))
                {
                    cmd.ExecuteNonQuery();
                }
                logger.LogInfo("INFO", "Complete TRUNCATE from JasperDeviceUsageStaging");

                logger.LogInfo("INFO", "Start TRUNCATE from JasperDeviceExportStaging");
                using (var cmd = new SqlCommand("TRUNCATE TABLE [dbo].[JasperDeviceExportStaging]", conn))
                {
                    cmd.ExecuteNonQuery();
                }
                logger.LogInfo("INFO", "Complete TRUNCATE from JasperDeviceExportStaging");

                logger.LogInfo("INFO", "Start TRUNCATE from JasperUsageByRatePlanStaging");
                using (var cmd = new SqlCommand("TRUNCATE TABLE [dbo].[JasperUsageByRatePlanStaging]", conn))
                {
                    cmd.ExecuteNonQuery();
                }
                logger.LogInfo("INFO", "Complete TRUNCATE from JasperUsageByRatePlanStaging");
            }
        }
    }
}
