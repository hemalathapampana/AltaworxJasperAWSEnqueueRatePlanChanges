using System.Collections.Generic;
using System.Data;
using Microsoft.Data.SqlClient;
using Amazon.Lambda.Core;
using Amop.Core.Logger;
using Amop.Core.Repositories.Environment;

namespace Altaworx.AWS.Core.Repositories.ServiceProvider
{
    public class ServiceProviderRepository : IServiceProviderRepository
    {
        private readonly IKeysysLogger _logger;
        private readonly IEnvironmentRepository _environmentRepository;
        private readonly ILambdaContext _context;

        public ServiceProviderRepository(IKeysysLogger logger, IEnvironmentRepository environmentRepository,
            ILambdaContext context)
        {
            _logger = logger;
            _environmentRepository = environmentRepository;
            _context = context;
        }
        public IEnumerable<int> GetJasperServiceProviderIds()
        {
            List<int> jasperServiceProviders = new List<int>();
            using (var Conn = new SqlConnection(_environmentRepository.GetEnvironmentVariable(_context, "ConnectionString")))
            {
                using (var Cmd = new SqlCommand("usp_Jasper_Get_Active_ServiceProviders", Conn))
                {
                    Cmd.CommandType = CommandType.StoredProcedure;
                    Conn.Open();

                    SqlDataReader rdr = Cmd.ExecuteReader();
                    while (rdr.Read())
                    {
                        jasperServiceProviders.Add((int)rdr["ServiceProviderId"]);
                    }

                    Conn.Close();
                }
            }

            return jasperServiceProviders;
        }
        public int GetTenantIdByServiceProviderId(int serviceProviderId)
        {
            int tenantId = 0;
            using (var Conn = new SqlConnection(_environmentRepository.GetEnvironmentVariable(_context, "ConnectionString")))
            {
                using (var Cmd = new SqlCommand("SELECT TenantId FROM ServiceProvider WHERE IsDeleted = 0 AND IsActive = 1 AND Id = @ServiceProviderId", Conn))
                {
                    Cmd.CommandType = CommandType.Text;
                    Cmd.Parameters.AddWithValue("@ServiceProviderId", serviceProviderId);
                    Conn.Open();

                    var rdr = Cmd.ExecuteScalar();

                    tenantId = (int)rdr;
                    Conn.Close();
                }
            }
            return tenantId;
        }
    }
}
