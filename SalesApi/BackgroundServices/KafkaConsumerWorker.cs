//namespace SalesApi.BackgroundServices
//{
    public class KafkaConsumerWorker : BackgroundService
    {
        private readonly KafkaConsumerService _consumer;

        public KafkaConsumerWorker(KafkaConsumerService consumer)
        {
            _consumer = consumer;
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            return Task.Run(() => _consumer.StartConsuming(stoppingToken));
        }
    }

//}
