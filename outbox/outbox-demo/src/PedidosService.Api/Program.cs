using Azure.Identity;
using Azure.Messaging.ServiceBus;
using Microsoft.EntityFrameworkCore;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using PedidosService.Api.Application;
using PedidosService.Api.Endpoints;
using PedidosService.Api.Infrastructure;
using PedidosService.Api.Infrastructure.Outbox;

var builder = WebApplication.CreateBuilder(args);

// ===== Configuración =====
builder.Services.Configure<OutboxRelayOptions>(
    builder.Configuration.GetSection("Outbox:Relay"));
builder.Services.Configure<OutboxOrphanRescuerOptions>(
    builder.Configuration.GetSection("Outbox:OrphanRescuer"));

// ===== EF Core =====
var sqlConnString = builder.Configuration.GetConnectionString("AppDb")
    ?? throw new InvalidOperationException("Missing ConnectionStrings:AppDb");

builder.Services.AddDbContext<AppDbContext>(opt =>
    opt.UseSqlServer(sqlConnString, sql =>
    {
        sql.EnableRetryOnFailure(
            maxRetryCount: 5,
            maxRetryDelay: TimeSpan.FromSeconds(10),
            errorNumbersToAdd: null);
        sql.CommandTimeout(30);
    }));

// ===== Azure Service Bus =====
// Producción: usar Managed Identity (DefaultAzureCredential)
// Local dev: usar connection string del emulador o de un namespace de pruebas
var sbConnString = builder.Configuration.GetConnectionString("ServiceBus");
var sbFullyQualifiedNs = builder.Configuration["ServiceBus:Namespace"];

builder.Services.AddSingleton(_ =>
{
    if (!string.IsNullOrEmpty(sbConnString))
        return new ServiceBusClient(sbConnString);

    if (!string.IsNullOrEmpty(sbFullyQualifiedNs))
        return new ServiceBusClient(sbFullyQualifiedNs, new DefaultAzureCredential());

    throw new InvalidOperationException(
        "Configurar ConnectionStrings:ServiceBus o ServiceBus:Namespace");
});

// ===== Servicios de aplicación =====
builder.Services.AddScoped<CrearPedidoHandler>();
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddSingleton<OutboxMetrics>();

// ===== Hosted services del Outbox =====
builder.Services.AddHostedService<OutboxRelayService>();
builder.Services.AddHostedService<OutboxOrphanRescuerService>();

// Si querés escalar el relay sin escalar la API, podés correr múltiples
// instancias del Hosted Service en el mismo proceso:
// builder.Services.AddHostedService<OutboxRelayService>();
// builder.Services.AddHostedService<OutboxRelayService>();
// Cada una tendrá un WorkerId distinto y se repartirán los lotes vía READPAST.

// ===== OpenTelemetry =====
builder.Services
    .AddOpenTelemetry()
    .ConfigureResource(r => r.AddService("PedidosService.Api"))
    .WithMetrics(m => m
        .AddAspNetCoreInstrumentation()
        .AddRuntimeInstrumentation()
        .AddMeter(OutboxMetrics.MeterName)
        .AddOtlpExporter())
    .WithTracing(t => t
        .AddAspNetCoreInstrumentation()
        .AddSqlClientInstrumentation()
        .AddSource("PedidosService.Outbox")
        .AddOtlpExporter());

// ===== API =====
builder.Services.AddOpenApi();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddProblemDetails();

// ===== Health checks =====
builder.Services.AddHealthChecks()
    .AddDbContextCheck<AppDbContext>("sql");

var app = builder.Build();

// Aplicar migraciones automáticamente en dev (NO hacer esto en producción)
if (app.Environment.IsDevelopment())
{
    await using var scope = app.Services.CreateAsyncScope();
    var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
    await db.Database.MigrateAsync();
}

app.UseExceptionHandler();
app.UseStatusCodePages();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.MapHealthChecks("/health");
app.MapPedidos();

app.Run();

// Permite a las pruebas de integración usar WebApplicationFactory<Program>
public partial class Program;
