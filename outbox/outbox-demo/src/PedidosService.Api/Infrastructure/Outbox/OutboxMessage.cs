namespace PedidosService.Api.Infrastructure.Outbox;

public enum OutboxStatus : byte
{
    Pending    = 0,
    Processing = 1,
    Published  = 2,
    Failed     = 3
}

/// <summary>
/// Mensaje persistido en la tabla outbox. Una fila por evento de dominio.
/// Se inserta en la MISMA transacción que el cambio de negocio.
/// </summary>
public class OutboxMessage
{
    public Guid EventId { get; set; }
    public string AggregateType { get; set; } = default!;
    public string AggregateId { get; set; } = default!;
    public string EventType { get; set; } = default!;
    public string Payload { get; set; } = default!;
    public string? Headers { get; set; }

    public DateTime CreatedAt { get; set; }
    public DateTime? ProcessedAt { get; set; }

    public OutboxStatus Status { get; set; }
    public int Attempts { get; set; }
    public string? LastError { get; set; }

    /// <summary>Identifica qué instancia del relay reclamó el mensaje.</summary>
    public string? ClaimedBy { get; set; }
    public DateTime? ClaimedAt { get; set; }
}
