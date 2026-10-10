using Azure.Identity;
using Azure.Messaging.ServiceBus;
using Microsoft.EntityFrameworkCore;
using PedidosService.Inventario;
using PedidosService.Inventario.Inbox;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.Configure<InventarioConsumerOptions>(
    builder.Configuration.GetSection("Consumer"));

var sqlConnString = builder.Configuration.GetConnectionString("InventarioDb")
    ?? throw new InvalidOperationException("Missing ConnectionStrings:InventarioDb");

builder.Services.AddDbContext<InventarioDbContext>(opt =>
    opt.UseSqlServer(sqlConnString, sql =>
        sql.EnableRetryOnFailure(5, TimeSpan.FromSeconds(10), null)));

var sbConnString = builder.Configuration.GetConnectionString("ServiceBus");
var sbNs = builder.Configuration["ServiceBus:Namespace"];

builder.Services.AddSingleton(_ =>
{
    if (!string.IsNullOrEmpty(sbConnString))
        return new ServiceBusClient(sbConnString);

    if (!string.IsNullOrEmpty(sbNs))
        return new ServiceBusClient(sbNs, new DefaultAzureCredential());

    throw new InvalidOperationException(
        "Configurar ConnectionStrings:ServiceBus o ServiceBus:Namespace");
});

builder.Services.AddHostedService<InventarioConsumer>();

var host = builder.Build();

// Aplicar migraciones en dev
if (builder.Environment.IsDevelopment())
{
    await using var scope = host.Services.CreateAsyncScope();
    var db = scope.ServiceProvider.GetRequiredService<InventarioDbContext>();
    await db.Database.EnsureCreatedAsync();
}

await host.RunAsync();
