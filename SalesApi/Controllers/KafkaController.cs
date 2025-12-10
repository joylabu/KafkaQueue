using Microsoft.AspNetCore.Mvc;
using SalesApi.Models;
using SalesApi.Services;
using System.Text.Json;

namespace SalesApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class KafkaController : ControllerBase
    {
        private readonly KafkaProducerService _kafkaProducer;

        public KafkaController(KafkaProducerService kafkaProducer)
        {
            _kafkaProducer = kafkaProducer;
        }


        [HttpGet("ping")]
        public IActionResult Ping()
        {
            return Ok("API OK");
        }


        // POST: api/kafka/postsales
        [HttpPost("postsales")]
        public async Task<IActionResult> PostSales([FromBody] JsonElement payload)
        {
            var dto = new KafkaPayloadDto
            {
                Data = JsonSerializer.Deserialize<object>(payload.GetRawText())
            };

            await _kafkaProducer.PublishAsync(dto);

            return Ok(new { message = "Message sent to Kafka successfully" });
        }
    }
}
