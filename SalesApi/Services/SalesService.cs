using Confluent.Kafka;
using System.Text.Json;
using SalesApi.Models;

namespace SalesApi.Services
{
    public class SalesService
    {
        private readonly IProducer<Null, string> _producer;

        public SalesService(IConfiguration config)
        {
            var kafkaConfig = new ProducerConfig
            {
                BootstrapServers = config["Kafka:BootstrapServers"]
            };

            _producer = new ProducerBuilder<Null, string>(kafkaConfig).Build();
        }

        public async Task PublishOrderAsync(SalesPayloadDto payload)
        {
            string json = JsonSerializer.Serialize(payload);

            await _producer.ProduceAsync("sales-orders",
                new Message<Null, string> { Value = json });
        }
    }
}
