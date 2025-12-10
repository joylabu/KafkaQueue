using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;
using System.Text.Json;

namespace SalesApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class SalesController : ControllerBase
    {
        private readonly IConfiguration _config;

        public SalesController(IConfiguration config)
        {
            _config = config;
        }

        [HttpPost("postsales")]
        public async Task<IActionResult> PostSales([FromBody] JsonElement payload)
        {
            string json = payload.GetRawText();

            using var conn = new SqlConnection(_config.GetConnectionString("Default"));
            using var cmd = new SqlCommand(
                "INSERT INTO SalesPayload (Payload) VALUES (@Payload)", conn);

            cmd.Parameters.AddWithValue("@Payload", json);

            await conn.OpenAsync();
            await cmd.ExecuteNonQueryAsync();

            return Ok(new { message = "Sales posted successfully" });
        }
    }
}
