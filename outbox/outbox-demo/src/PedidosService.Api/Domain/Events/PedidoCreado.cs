namespace PedidosService.Api.Domain.Events;

/// <summary>
/// Contrato del evento publicado al broker.
/// El sufijo .v1 explicita la versión del esquema para permitir evolución.
/// </summary>
public sealed record PedidoCreado(
    Guid PedidoId,
    Guid ClienteId,
    decimal Total,
    DateTime CreadoEn)
{
    public const string EventType = "PedidoCreado.v1";
    public const string AggregateType = nameof(Pedido);
}
