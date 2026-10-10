using Microsoft.EntityFrameworkCore;
using PedidosService.Api.Domain;
using PedidosService.Api.Domain.Events;
using PedidosService.Api.Infrastructure;
using PedidosService.Api.Infrastructure.Outbox;

namespace PedidosService.Api.Application;

public record CrearPedidoCommand(Guid ClienteId, decimal Total);

public record CrearPedidoResult(Guid PedidoId);

/// <summary>
/// Handler de aplicación. Crea el pedido Y publica el evento en una sola transacción.
/// El punto clave del patrón Outbox está en SaveChangesAsync: ambos INSERT
/// (pedido + outbox) se commitean atómicamente.
/// </summary>
public sealed class CrearPedidoHandler(
    AppDbContext db,
    TimeProvider clock,
    ILogger<CrearPedidoHandler> logger)
{
    public async Task<CrearPedidoResult> HandleAsync(
        CrearPedidoCommand cmd,
        CancellationToken ct)
    {
        var pedido = Pedido.Crear(
            new ClienteId(cmd.ClienteId),
            cmd.Total,
            clock);

        // EF Core 10 abre transacción implícita en SaveChanges cuando hay
        // múltiples writes. Para hacerlo explícito en operaciones más complejas:
        await using var tx = await db.Database.BeginTransactionAsync(ct);

        db.Pedidos.Add(pedido);

        var evento = new PedidoCreado(
            PedidoId:  pedido.Id.Value,
            ClienteId: pedido.ClienteId.Value,
            Total:     pedido.Total,
            CreadoEn:  pedido.CreadoEn);

        db.EnqueueEvent(
            evento,
            aggregateType: PedidoCreado.AggregateType,
            aggregateId:   pedido.Id.ToString(),
            eventType:     PedidoCreado.EventType,
            clock:         clock);

        await db.SaveChangesAsync(ct);   // ← AMBOS INSERT en la misma TX
        await tx.CommitAsync(ct);

        logger.LogInformation("Pedido {PedidoId} creado y evento encolado", pedido.Id);

        return new CrearPedidoResult(pedido.Id.Value);
    }
}
