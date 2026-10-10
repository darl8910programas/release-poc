namespace PedidosService.Api.Domain;

/// <summary>
/// Identificador fuertemente tipado para Pedido.
/// Usa UUIDv7 (timestamp en bits altos) para inserciones secuenciales en SQL Server,
/// lo que evita fragmentación del clustered index. Disponible desde .NET 9.
/// </summary>
public readonly record struct PedidoId(Guid Value)
{
    public static PedidoId New() => new(Guid.CreateVersion7());
    public override string ToString() => Value.ToString();
}

public readonly record struct ClienteId(Guid Value)
{
    public override string ToString() => Value.ToString();
}

public enum EstadoPedido : byte
{
    Creado = 0,
    Confirmado = 1,
    Cancelado = 2
}

public class Pedido
{
    public PedidoId Id { get; private set; }
    public ClienteId ClienteId { get; private set; }
    public decimal Total { get; private set; }
    public EstadoPedido Estado { get; private set; }
    public DateTime CreadoEn { get; private set; }

    // EF Core constructor
    private Pedido() { }

    public static Pedido Crear(ClienteId clienteId, decimal total, TimeProvider clock)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(total);

        return new Pedido
        {
            Id        = PedidoId.New(),
            ClienteId = clienteId,
            Total     = total,
            Estado    = EstadoPedido.Creado,
            CreadoEn  = clock.GetUtcNow().UtcDateTime
        };
    }

    public void Confirmar() => Estado = EstadoPedido.Confirmado;
    public void Cancelar() => Estado = EstadoPedido.Cancelado;
}
