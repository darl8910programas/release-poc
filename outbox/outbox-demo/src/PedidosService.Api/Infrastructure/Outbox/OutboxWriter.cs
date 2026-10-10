using System.Diagnostics;
using System.Text.Json;
using PedidosService.Api.Infrastructure.Outbox;

namespace PedidosService.Api.Infrastructure.Outbox;

/// <summary>
/// Métodos de conveniencia para escribir eventos al outbox.
/// El llamador es responsable de hacer SaveChanges en la misma transacción
/// que el cambio de negocio. Esto es lo que da atomicidad.
/// </summary>
public static class OutboxWriter
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };

    public static void EnqueueEvent<TEvent>(
        this AppDbContext db,
        TEvent @event,
        string aggregateType,
        string aggregateId,
        string eventType,
        TimeProvider clock,
        IDictionary<string, string>? extraHeaders = null) where TEvent : notnull
    {
        // Headers automáticos: correlation/trace IDs desde Activity actual
        var headers = new Dictionary<string, string>();
        var activity = Activity.Current;
        if (activity is not null)
        {
            headers["traceId"]  = activity.TraceId.ToString();
            headers["spanId"]   = activity.SpanId.ToString();
        }
        if (extraHeaders is not null)
            foreach (var kv in extraHeaders) headers[kv.Key] = kv.Value;

        var message = new OutboxMessage
        {
            EventId       = Guid.CreateVersion7(),
            AggregateType = aggregateType,
            AggregateId   = aggregateId,
            EventType     = eventType,
            Payload       = JsonSerializer.Serialize(@event, JsonOptions),
            Headers       = headers.Count > 0
                              ? JsonSerializer.Serialize(headers, JsonOptions)
                              : null,
            Status        = OutboxStatus.Pending,
            CreatedAt     = clock.GetUtcNow().UtcDateTime
        };

        db.OutboxMessages.Add(message);
    }
}
