using System;
using System.Data;
using Amop.Core.Services.Jasper;
using Microsoft.Data.SqlClient;

namespace Altaworx.AWS.Core
{
    public static class JasperCommon
    {
        public static JasperAuthentication GetJasperAuthenticationInformation(string connectionString, int currentServiceProviderId)
        {
            JasperAuthentication jasperAuth = null;
            try
            {
                using (var conn = new SqlConnection(connectionString))
                {
                    using (var cmd = new SqlCommand("usp_Jasper_Get_AuthenticationByProviderId", conn))
                    {
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.Parameters.AddWithValue("@providerId", currentServiceProviderId);
                        conn.Open();

                        SqlDataReader rdr = cmd.ExecuteReader();
                        while (rdr.Read())
                        {
                            jasperAuth = new JasperAuthentication
                            {
                                JasperAuthenticationId = Convert.ToInt32(rdr["integrationAuthenticationId"]),
                                BaseUrl = rdr["baseUrl"].ToString(),
                                Username = rdr["username"].ToString(),
                                Password = rdr["password"].ToString(),
                                BillingPeriodEndDay = rdr["BillPeriodEndDay"] != DBNull.Value ? (int)rdr["BillPeriodEndDay"] : (int?)null,
                                BillingPeriodEndHour = rdr["BillPeriodEndHour"] != DBNull.Value ? (int)rdr["BillPeriodEndHour"] : (int?)null,
                                ProductionApiUrl = rdr["ProductionApiUrl"] != DBNull.Value ? rdr["ProductionApiUrl"].ToString() : null,
                                SandboxApiUrl = rdr["SandboxApiUrl"] != DBNull.Value ? rdr["SandboxApiUrl"].ToString() : null,
                                WriteIsEnabled = rdr.GetBoolean(rdr.GetOrdinal("WriteIsEnabled")),
                                ServiceProvider = rdr["ServiceProvider"].ToString()
                            };
                            break;
                        }

                        conn.Close();
                    }
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine(ex.Message);
            }

            return jasperAuth;
        }

        public static DateTime GetJasperDevicesLastSyncDate(string connectionString, int serviceProviderId)
        {
            DateTime lastSyncDate = DateTime.Now.AddMonths(-3);
            using (var conn = new SqlConnection(connectionString))
            {
                using (var cmd = new SqlCommand("usp_Jasper_Devices_Last_Sync_Date", conn))
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.AddWithValue("@ServiceProviderId", serviceProviderId);
                    conn.Open();

                    SqlDataReader rdr = cmd.ExecuteReader();
                    while (rdr.Read())
                    {
                        lastSyncDate = DateTime.Parse(rdr["LastSyncDate"].ToString());
                    }
                    conn.Close();
                }
            }

            return lastSyncDate;
        }
    }
}
