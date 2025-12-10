using Confluent.Kafka;
using Microsoft.Data.SqlClient;

namespace SalesApi.Services
{
    public class KafkaConsumerWorker : BackgroundService
    {
        private readonly IConfiguration _config;
        private readonly IConsumer<Null, string> _consumer;
        private readonly ILogger<KafkaConsumerWorker> _logger;
        private readonly List<string> _batch = new();   // stores messages temporarily
        private readonly object _lock = new();          // lock object for thread safety
        private readonly int _batchSize = 50;           // number of messages to insert at once


        public KafkaConsumerWorker(IConfiguration config, ILogger<KafkaConsumerWorker> logger)
        {
            _config = config;
            _logger = logger;

            try
            {
                var consumerConfig = new ConsumerConfig
                {
                    BootstrapServers = config["Kafka:BootstrapServers"],
                    GroupId = "sales-consumer-group",
                    AutoOffsetReset = AutoOffsetReset.Earliest
                };
                _consumer = new ConsumerBuilder<Null, string>(consumerConfig).Build();
                _consumer.Subscribe("sales-orders");
                _logger.LogInformation("Kafka Consumer initialized and subscribed to topic 'sales-orders'");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to initialize Kafka Consumer");
                throw;
            }
        }

        //protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        //{
        //    _logger.LogInformation("Kafka Consumer Worker started");
        //    while (!stoppingToken.IsCancellationRequested)
        //    {
        //        try
        //        {
        //            var message = _consumer.Consume(stoppingToken);
        //            _logger.LogInformation($"Received message: {message.Message.Value}");
        //            await InsertIntoDatabase(message.Message.Value);
        //        }
        //        catch (ConsumeException e)
        //        {
        //            _logger.LogError(e, "Error consuming message");
        //        }
        //        catch (Exception ex)
        //        {
        //            _logger.LogError(ex, "KAFKA CONSUMER ERROR");
        //        }
        //    }
        //}

        //individual insert
        //protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        //{
        //    _logger.LogInformation("Kafka Consumer Worker started");

        //    await Task.Yield(); // allows Web API to finish startup

        //    while (!stoppingToken.IsCancellationRequested)
        //    {
        //        try
        //        {
        //            var cr = _consumer.Consume(TimeSpan.FromMilliseconds(200));
        //            if (cr != null)
        //            {
        //                _logger.LogInformation($"Received message: {cr.Message.Value}");
        //                await InsertIntoDatabase(cr.Message.Value);
        //            }
        //        }
        //        catch (ConsumeException e)
        //        {
        //            _logger.LogError(e, "Error consuming message");
        //        }
        //        catch (Exception ex)
        //        {
        //            _logger.LogError(ex, "KAFKA CONSUMER ERROR");
        //        }
        //    }
        //}


        //private async Task InsertIntoDatabase(string json)
        //{
        //    try
        //    {
        //        using var conn = new SqlConnection(_config.GetConnectionString("Default"));
        //        using var cmd = new SqlCommand("INSERT INTO SalesPayload (Payload) VALUES (@Payload)", conn);
        //        cmd.Parameters.AddWithValue("@Payload", json);
        //        await conn.OpenAsync();
        //        await cmd.ExecuteNonQueryAsync();
        //        _logger.LogInformation("Inserted into DB via Kafka: " + json);
        //    }
        //    catch (Exception ex)
        //    {
        //        _logger.LogError(ex, "Error inserting into database");
        //        throw;
        //    }
        //}


        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Kafka Consumer Worker started");
            await Task.Yield();

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var cr = _consumer.Consume(TimeSpan.FromMilliseconds(200));
                    if (cr != null)
                    {
                        _logger.LogInformation($"Received message: {cr.Message.Value}");

                        bool shouldInsert = false;
                        List<string> batchToInsert = null;

                        lock (_lock)
                        {
                            _batch.Add(cr.Message.Value);
                            if (_batch.Count >= _batchSize)
                            {
                                batchToInsert = new List<string>(_batch);
                                _batch.Clear();
                                shouldInsert = true;
                            }
                        }

                        if (shouldInsert && batchToInsert != null)
                        {
                            await InsertBatchIntoDatabaseAsync(batchToInsert);
                        }
                    }
                }
                catch (ConsumeException e)
                {
                    _logger.LogError(e, "Error consuming message");
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "KAFKA CONSUMER ERROR");
                }
            }

            // Insert remaining messages before shutdown
            List<string> remainingBatch = null;
            lock (_lock)
            {
                if (_batch.Count > 0)
                {
                    remainingBatch = new List<string>(_batch);
                    _batch.Clear();
                }
            }
            if (remainingBatch != null)
                await InsertBatchIntoDatabaseAsync(remainingBatch);
        }


        //insert by batch
        private async Task InsertBatchIntoDatabaseAsync(List<string> batch)
        {
            if (batch.Count == 0) return;

            using var conn = new SqlConnection(_config.GetConnectionString("Default"));
            await conn.OpenAsync();

            using var transaction = conn.BeginTransaction();
            try
            {
                var cmd = conn.CreateCommand();
                cmd.Transaction = transaction;

                var valuesList = new List<string>();
                for (int i = 0; i < batch.Count; i++)
                {
                    cmd.Parameters.AddWithValue($"@Payload{i}", batch[i]);
                    valuesList.Add($"(@Payload{i})");
                }

                cmd.CommandText = $"INSERT INTO SalesPayload (Payload) VALUES {string.Join(", ", valuesList)}";
                await cmd.ExecuteNonQueryAsync();
                transaction.Commit();

                _logger.LogInformation($"Inserted batch of {batch.Count} messages into DB");
            }
            catch
            {
                transaction.Rollback();
                throw;
            }
        }

    }
}
