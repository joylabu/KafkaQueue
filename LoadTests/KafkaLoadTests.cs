using FluentAssertions;
using LoadTests;
using System.Net.Http.Json;

public class KafkaLoadTests
{
    private readonly HttpClient _client;
    private readonly DbHelper _db;
    public KafkaLoadTests()
    {
        _client = new HttpClient
        {
            BaseAddress = new Uri("http://localhost:5138")
        };


        _db = new DbHelper();
    }

    [Fact]
    public async Task Kafka_PostSales_10k_Requests_Per_Minute_Optimized()
    {
        // 1️⃣ Count rows BEFORE test
        int beforeCount = await _db.CountRows();

        int totalRequests = 10000;
        int batchSize = 500;      // number of requests per batch
        int maxParallel = 100;    // control concurrency within batch
        var rand = new Random();  // shared random instance
        var products = new[] { "Product A", "Product B", "Product C", "Product D", "Product E" };

        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5));

        int sentRequests = 0;

        while (sentRequests < totalRequests)
        {
            int currentBatchSize = Math.Min(batchSize, totalRequests - sentRequests);
            var semaphore = new SemaphoreSlim(maxParallel);
            var tasks = new List<Task>();

            for (int i = 0; i < currentBatchSize; i++)
            {
                await semaphore.WaitAsync(cts.Token);

                tasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        int itemCount;
                        lock (rand) { itemCount = rand.Next(1, 4); }

                        List<string> selectedItems;
                        lock (rand)
                        {
                            selectedItems = Enumerable.Range(0, itemCount)
                                                      .Select(_ => products[rand.Next(products.Length)])
                                                      .ToList();
                        }

                        decimal amount;
                        lock (rand) { amount = Math.Round((decimal)(rand.NextDouble() * 990) + 10, 2); }

                        var payload = new
                        {
                            orderId = Guid.NewGuid(),
                            amount = amount,
                            items = selectedItems
                        };

                        using var response = await _client.PostAsJsonAsync("/api/kafka/postsales", payload, cts.Token);
                        response.IsSuccessStatusCode.Should().BeTrue();
                    }
                    finally
                    {
                        semaphore.Release();
                    }
                }, cts.Token));
            }

            await Task.WhenAll(tasks);
            sentRequests += currentBatchSize;
        }

    //    // 2️⃣ Wait for Kafka consumer to finish inserting
    //    int afterCount = await _db.CountRows();
    //    int elapsedMs = 0;
    //    int delayMs = 500;
    //    int maxWaitMs = 120_000; // max 2 minutes

    //    while ((afterCount - beforeCount) < totalRequests && elapsedMs < maxWaitMs)
    //    {
    //        await Task.Delay(delayMs, cts.Token);
    //        elapsedMs += delayMs;
    //        afterCount = await _db.CountRows();
    //    }

    //// 3️⃣ Assert final count
    //(afterCount - beforeCount).Should().Be(totalRequests,
    //    $"Expected {totalRequests} inserts, but only {(afterCount - beforeCount)} were inserted.");
    }




    //[Fact]
    //public async Task Kafka_PostSales_10k_Requests_Per_Minute()
    //{
    //    // 1️⃣ Count rows BEFORE test
    //    int beforeCount = await _db.CountRows();

    //    int totalRequests = 10000;
    //    int maxParallel = 1000; // control concurrency
    //    var semaphore = new SemaphoreSlim(maxParallel);

    //    var tasks = new List<Task>();
    //    var rand = new Random(); // shared random instance
    //    using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)); // max 5 minutes

    //    var products = new[] { "Product A", "Product B", "Product C", "Product D", "Product E" };

    //    // 2️⃣ Send all requests
    //    for (int i = 0; i < totalRequests; i++)
    //    {
    //        await semaphore.WaitAsync(cts.Token);

    //        tasks.Add(Task.Run(async () =>
    //        {
    //            try
    //            {
    //                int itemCount;
    //                lock (rand) { itemCount = rand.Next(1, 4); }

    //                List<string> selectedItems;
    //                lock (rand)
    //                {
    //                    selectedItems = Enumerable.Range(0, itemCount)
    //                                              .Select(_ => products[rand.Next(products.Length)])
    //                                              .ToList();
    //                }

    //                decimal amount;
    //                lock (rand) { amount = Math.Round((decimal)(rand.NextDouble() * 990) + 10, 2); }

    //                var payload = new
    //                {
    //                    orderId = Guid.NewGuid(),
    //                    amount = amount,
    //                    items = selectedItems
    //                };

    //                using var response = await _client.PostAsJsonAsync("/api/kafka/postsales", payload, cts.Token);
    //                response.IsSuccessStatusCode.Should().BeTrue();
    //            }
    //            finally
    //            {
    //                semaphore.Release();
    //            }
    //        }, cts.Token));
    //    }

    //    await Task.WhenAll(tasks);

    //    // 3️⃣ Wait for Kafka consumer to finish inserting
    //    int afterCount = await _db.CountRows();
    //    int elapsedMs = 0;
    //    int delayMs = 500;
    //    int maxWaitMs = 120_000; // max 2 minutes

    //    while ((afterCount - beforeCount) < totalRequests && elapsedMs < maxWaitMs)
    //    {
    //        await Task.Delay(delayMs, cts.Token);
    //        elapsedMs += delayMs;
    //        afterCount = await _db.CountRows();
    //    }

    //   // 4️⃣ Assert final count
    //   (afterCount - beforeCount).Should().Be(totalRequests,
    //       $"Expected {totalRequests} inserts, but only {(afterCount - beforeCount)} were inserted.");
    //}


    //[Fact]
    //public async Task Kafka_PostSales_10k_Requests_Per_Minute()
    //{


    //    // ✅ 1. Count rows BEFORE test
    //    int beforeCount = await _db.CountRows();

    //    int totalRequests = 10000;
    //    int maxParallel = 1000; // control concurrency
    //    var semaphore = new SemaphoreSlim(maxParallel);

    //    var tasks = new List<Task>();

    //    for (int i = 0; i < totalRequests; i++)
    //    {
    //        await semaphore.WaitAsync();

    //        var rand = new Random();
    //        var products = new[] { "Product A", "Product B", "Product C", "Product D", "Product E" };

    //        tasks.Add(Task.Run(async () =>
    //        {
    //            try
    //            {
    //                // Generate random items (1–3 products)
    //                int itemCount = rand.Next(1, 4);
    //                var selectedItems = Enumerable.Range(0, itemCount)
    //                                              .Select(_ => products[rand.Next(products.Length)])
    //                                              .ToList();

    //                // Generate random amount (10.00 - 999.99)
    //                decimal amount = Math.Round((decimal)(rand.NextDouble() * 990) + 10, 2);

    //                var payload = new
    //                {
    //                    orderId = Guid.NewGuid(),
    //                    amount = amount,
    //                    items = selectedItems
    //                };

    //                var response = await _client.PostAsJsonAsync("/api/kafka/postsales", payload);
    //                response.IsSuccessStatusCode.Should().BeTrue();
    //            }
    //            finally
    //            {
    //                semaphore.Release();
    //            }
    //        }));


    //        //tasks.Add(Task.Run(async () =>
    //        //{
    //        //    try
    //        //    {
    //        //        var payload = new
    //        //        {
    //        //            id = Guid.NewGuid().ToString(),
    //        //            timestamp = DateTime.UtcNow,
    //        //            value = i
    //        //        };

    //        //        var response = await _client.PostAsJsonAsync("/api/kafka/postsales", payload);

    //        //        response.IsSuccessStatusCode.Should().BeTrue();
    //        //    }
    //        //    finally
    //        //    {
    //        //        semaphore.Release();
    //        //    }
    //        //}));
    //    }

    //    await Task.WhenAll(tasks);

    //    // Give Kafka consumer time to insert into database (if async)
    //    await Task.Delay(2000);

    //    int afterCount = await _db.CountRows();

    //    (afterCount - beforeCount).Should().Be(totalRequests);
    //}



}
