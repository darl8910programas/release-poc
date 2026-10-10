using System.Diagnostics.Metrics;

namespace PedidosService.Api.Infrastructure.Outbox;

/// <summary>
/// Métricas del relay del outbox usando System.Diagnostics.Metrics (OpenTelemetry-friendly).
/// Exportable a Azure Monitor, Prometheus, etc. vía OTel.
/// </summary>
public sealed class OutboxMetrics : IDisposable
{
    public const string MeterName = "PedidosService.Outbox";

    private readonly Meter _meter;
    public Counter<long> EventsPublished { get; }
    public Counter<long> EventsFailed { get; }
    public Counter<long> EventsRescued { get; }
    public Histogram<double> BatchDurationMs { get; }
    public Histogram<int> BatchSize { get; }

    public OutboxMetrics(IMeterFactory factory)
    {
        _meter = factory.Create(MeterName);

        EventsPublished = _meter.CreateCounter<long>(
            "outbox.events.published",
            unit: "{event}",
            description: "Total de eventos publicados exitosamente al broker");

        EventsFailed = _meter.CreateCounter<long>(
            "outbox.events.failed",
            unit: "{event}",
            description: "Total de eventos que fallaron al publicar");

        EventsRescued = _meter.CreateCounter<long>(
            "outbox.events.rescued",
            unit: "{event}",
            description: "Eventos huérfanos rescatados (PROCESSING > timeout)");

        BatchDurationMs = _meter.CreateHistogram<double>(
            "outbox.batch.duration",
            unit: "ms",
            description: "Duración del procesamiento de cada batch");

        BatchSize = _meter.CreateHistogram<int>(
            "outbox.batch.size",
            unit: "{event}",
            description: "Cantidad de eventos por batch");
    }

    public void Dispose() => _meter.Dispose();
}
