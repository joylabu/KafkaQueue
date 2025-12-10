using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Configuration;
using SalesProcessor.Services;
using SalesProcessor.Data;
using Serilog;

var builder = Host.CreateDefaultBuilder(args);

builder.ConfigureAppConfiguration(config =>
{
    config.SetBasePath(Directory.GetCurrentDirectory());
    config.AddJsonFile("appsettings.json", optional: false);
});

builder.ConfigureServices((context, services) =>
{
   string connStr = context.Configuration.GetConnectionString("Default")
        ?? throw new InvalidOperationException("Connection string 'Default' not found.");

    //// Register dependencies
    //services.AddSingleton(new SalesRepository(connStr));
    //services.AddSingleton<SalesProcessingService>();

    // Register SalesRepository with the connection string
    services.AddSingleton<SalesRepository>(new SalesRepository(connStr));
    //services.AddSingleton<SalesProcessingService>();
    services.AddHostedService<SalesProcessingWorker>();


    // Register SalesProcessingService -normal worker
    //services.AddSingleton<SalesProcessingService>();
     

    // Add hosted service if needed (future upgrade)
     

    // Kafka consumer (batch)
    services.AddHostedService<KafkaConsumerWorker>();

});

builder.UseSerilog((context, config) =>
{
    config.WriteTo.Console();
});

var host = builder.Build();

//manual call to ProcessPendingSales()
//var processor = host.Services.GetRequiredService<SalesProcessingService>();
//await processor.ProcessPendingSales();
 
await host.RunAsync();



// Keep the host alive so Kafka keeps consuming
await host.WaitForShutdownAsync();

// using Microsoft.Extensions.Configuration;
// using SalesProcessor.Services;


// var config = new ConfigurationBuilder()
//     .SetBasePath(Directory.GetCurrentDirectory())
//     .AddJsonFile("appsettings.json", optional: false)
//     .Build();

// string connStr = config.GetConnectionString("Default")
//     ?? throw new InvalidOperationException("Connection string 'Default' not found.");

// var processor = new SalesProcessingService(connStr);

// Console.WriteLine("Starting Sales Processor...");

// await processor.ProcessPendingSales();

// Console.WriteLine("Processing completed.");
