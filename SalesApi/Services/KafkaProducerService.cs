using Confluent.Kafka;
using SalesApi.Models;
using System.Text.Json;

namespace SalesApi.Services
{
    public class KafkaProducerService : IDisposable
    {
        private readonly IProducer<Null, string> _producer;
        private readonly ILogger<KafkaProducerService> _logger;
        private bool _disposed = false;

        public KafkaProducerService(IConfiguration config, ILogger<KafkaProducerService> logger)
        {
            _logger = logger;
            try
            {
                var producerConfig = new ProducerConfig
                {
                    BootstrapServers = config["Kafka:BootstrapServers"]
                };
                _producer = new ProducerBuilder<Null, string>(producerConfig).Build();
                _logger.LogInformation("Kafka Producer initialized");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to initialize Kafka Producer");
                throw;
            }
        }

        public async Task PublishAsync(KafkaPayloadDto payload)
        {
            try
            {
                string json = JsonSerializer.Serialize(payload);
                await _producer.ProduceAsync("sales-orders", new Message<Null, string> { Value = json });
                _producer.Flush(TimeSpan.FromSeconds(5));
                _logger.LogInformation("Message published to Kafka");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error sending Kafka message");
                throw;
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    _producer?.Dispose();
                }
                _disposed = true;
            }
        }
    }

}
