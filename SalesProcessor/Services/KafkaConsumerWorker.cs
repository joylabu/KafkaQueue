using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using SalesProcessor.Data;

public class KafkaConsumerWorker : BackgroundService
{
    private readonly ILogger<KafkaConsumerWorker> _logger;
    private readonly SalesRepository _repo;
    private readonly IConsumer<Null, string> _consumer;

    public KafkaConsumerWorker(IConfiguration config,
                               ILogger<KafkaConsumerWorker> logger,
                               SalesRepository repo)
    {
        _logger = logger;
        _repo = repo;

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = config["Kafka:BootstrapServers"],
            GroupId = "salesprocessor-consumer-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true
        };

        _consumer = new ConsumerBuilder<Null, string>(consumerConfig).Build();
        _consumer.Subscribe("sales-orders");
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Batch KafkaConsumerWorker started.");

        await Task.Yield(); // allow host to start

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                // Non-blocking polling
                var cr = _consumer.Consume(TimeSpan.FromMilliseconds(200));
                if (cr == null) continue;

                _logger.LogInformation($"Received Kafka payload.");

                // Insert into DB as Pending
                await _repo.InsertPendingAsync(cr.Message.Value);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Kafka consumer error in batch.");
            }
        }
    }
}
