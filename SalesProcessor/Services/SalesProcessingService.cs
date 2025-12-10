
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using SalesProcessor.Data;

namespace SalesProcessor.Services
{
    public class SalesProcessingWorker : BackgroundService
{
    private readonly SalesRepository _repo;
    private readonly ILogger<SalesProcessingWorker> _logger;

    public SalesProcessingWorker(SalesRepository repo, ILogger<SalesProcessingWorker> logger)
    {
        _repo = repo;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("SalesProcessingWorker started.");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var records = await _repo.LoadPendingSales();

                foreach (var r in records)
                {
                    try
                    {
                        _logger.LogInformation($"Processing ID: {r.Id}");
                        await _repo.UpdateStatus(r.Id, "Processed");
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, $"Error processing ID {r.Id}");
                        await _repo.UpdateError(r.Id, ex.Message);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "General batch processing error");
            }

            await Task.Delay(1000, stoppingToken); // run every 1 second
        }
    }
}
}


//using SalesProcessor.Data;

//namespace SalesProcessor.Services
//{
//    public class SalesProcessingService
//    {
//        private readonly SalesRepository _repo;

//        public SalesProcessingService(SalesRepository repo)
//        {
//            _repo = repo;
//        }

//        public async Task ProcessPendingSales()
//        {
//            var records = await _repo.LoadPendingSales();
//            Console.WriteLine($"Found {records.Count} pending record(s).");

//            foreach (var r in records)
//            {
//                try
//                {
//                    Console.WriteLine($"Processing ID: {r.Id}");
//                    await _repo.UpdateStatus(r.Id, "Processed");
//                    Console.WriteLine($"✔ ID {r.Id} processed.");
//                }
//                catch (Exception ex)
//                {
//                    Console.WriteLine($"❌ Error on ID {r.Id}: {ex.Message}");
//                    await _repo.UpdateError(r.Id, ex.Message);
//                }
//            }
//        }
//    }
//}
