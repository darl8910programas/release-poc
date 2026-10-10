using Microsoft.EntityFrameworkCore;
using PedidosService.Api.Application;
using PedidosService.Api.Infrastructure;
using PedidosService.Api.Infrastructure.Outbox;

namespace PedidosService.Api.Endpoints;

public static class PedidosEndpoints
{
    public static IEndpointRouteBuilder MapPedidos(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/api/pedidos").WithTags("Pedidos");

        group.MapPost("/", async (
            CrearPedidoCommand cmd,
            CrearPedidoHandler handler,
            CancellationToken ct) =>
        {
            var result = await handler.HandleAsync(cmd, ct);
            return TypedResults.Created($"/api/pedidos/{result.PedidoId}", result);
        })
        .WithName("CrearPedido")
        .WithSummary("Crea un pedido y encola el evento PedidoCreado en el outbox");

        // Endpoint de inspección para ver el estado del outbox en demos
        group.MapGet("/outbox/stats", async (AppDbContext db, CancellationToken ct) =>
        {
            var stats = await db.OutboxMessages
                .GroupBy(x => x.Status)
                .Select(g => new { Status = g.Key.ToString(), Count = g.Count() })
                .ToListAsync(ct);

            var oldestPending = await db.OutboxMessages
                .Where(x => x.Status == OutboxStatus.Pending)
                .OrderBy(x => x.CreatedAt)
                .Select(x => (DateTime?)x.CreatedAt)
                .FirstOrDefaultAsync(ct);

            return TypedResults.Ok(new { stats, oldestPending });
        })
        .WithName("OutboxStats");

        return app;
    }
}
