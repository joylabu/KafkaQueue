//namespace SalesApi.BackgroundServices
//{
    using Confluent.Kafka;

    public class KafkaConsumerService
    {
        private readonly string _bootstrapServers = "localhost:9092";
        private readonly string _topic = "orders";

        public void StartConsuming(CancellationToken token)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = _bootstrapServers,
                GroupId = "order-consumer",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
            consumer.Subscribe(_topic);

            while (!token.IsCancellationRequested)
            {
                var result = consumer.Consume(token);
                Console.WriteLine($"Consumed: {result.Message.Value}");
            }
        }
    }

//}
