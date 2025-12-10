using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

 
namespace LoadTests
{
    public class DbHelper
    {
        private readonly string _connStr;

        public DbHelper()
        {
            var config = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: false)
                .Build();

            _connStr = config.GetConnectionString("Default");
        }
        public async Task<int> CountRows()
        {
            using var conn = new SqlConnection(_connStr);
            using var cmd = new SqlCommand("SELECT COUNT(*) FROM SalesPayload", conn);

            await conn.OpenAsync();
            return (int)await cmd.ExecuteScalarAsync();
        }
    }
}
