using System;
using System.Collections.Generic;
using System.Text;
using Amop.Core.Helpers;
using Amop.Core.Logger;
using Microsoft.Data.SqlClient;

namespace Amop.Core.Repositories.Jasper
{
    public class JasperDeviceStagingRepository
    {
        public void DeleteStagingWithPolicy(IKeysysLogger logger, string connectionString, int serviceProviderId, List<string> errorMessages, int retryCount = RetryPolicyHelper.SQL_TRANSIENT_RETRY_MAX_COUNT)
        {
            var sqlTransientRetryPolicy = RetryPolicyHelper.GetSqlTransientPolicy(logger, errorMessages, retryCount);
            sqlTransientRetryPolicy.Execute(() => DeleteStagingTables(connectionString, serviceProviderId));
        }

        public void DeleteStagingTables(string connectionString, int serviceProviderId)
        {
            using (var conn = new SqlConnection(connectionString))
            {
                conn.Open();

                using (var cmd = new SqlCommand("DELETE FROM [dbo].[JasperDeviceStaging] WHERE ServiceProviderId = @serviceProviderId", conn))
                {
                    cmd.Parameters.AddWithValue("@serviceProviderId", serviceProviderId);
                    cmd.ExecuteNonQuery();
                }

                conn.Close();
            }
        }

        public void TruncateStagingWithPolicy(IKeysysLogger logger, string connectionString, List<string> errorMessages, int retryCount = RetryPolicyHelper.SQL_TRANSIENT_RETRY_MAX_COUNT)
        {
            var sqlTransientRetryPolicy = RetryPolicyHelper.GetSqlTransientPolicy(logger, errorMessages, retryCount);
            sqlTransientRetryPolicy.Execute(() => TruncateStagingTables(connectionString));
        }

        public void TruncateStagingTables(string connectionString)
        {
            using (var conn = new SqlConnection(connectionString))
            {
                conn.Open();

                using (var cmd = new SqlCommand("TRUNCATE TABLE [dbo].[JasperDeviceStaging]", conn))
                {
                    cmd.ExecuteNonQuery();
                }

                conn.Close();
            }
        }
    }
}
