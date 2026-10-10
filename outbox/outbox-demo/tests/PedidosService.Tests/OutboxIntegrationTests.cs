using FluentAssertions;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using PedidosService.Api.Application;
using PedidosService.Api.Infrastructure;
using PedidosService.Api.Infrastructure.Outbox;
using Testcontainers.MsSql;

namespace PedidosService.Tests;

/// <summary>
/// Pruebas de integración del patrón Outbox usando un SQL Server real
/// en Docker (Testcontainers). Cubre los dos aspectos críticos:
///   1. Atomicidad del Outbox (pedido + evento en una transacción)
///   2. Claim de lotes sin colisión entre workers concurrentes
/// </summary>
public class OutboxIntegrationTests : IAsyncLifetime
{
    private readonly MsSqlContainer _sqlContainer = new MsSqlBuilder()
        .WithImage("mcr.microsoft.com/mssql/server:2022-latest")
        .Build();

    private ServiceProvider _services = default!;

    public async ValueTask InitializeAsync()
    {
        await _sqlContainer.StartAsync();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(TimeProvider.System);

        services.AddDbContext<AppDbContext>(opt =>
            opt.UseSqlServer(_sqlContainer.GetConnectionString()));

        _services = services.BuildServiceProvider();

        await using var scope = _services.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
        await db.Database.EnsureCreatedAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await _services.DisposeAsync();
        await _sqlContainer.DisposeAsync();
    }

    [Fact]
    public async Task CrearPedido_persistePedidoYEventoEnLaMismaTransaccion()
    {
        await using var scope = _services.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
        var clock = scope.ServiceProvider.GetRequiredService<TimeProvider>();

        var handler = new CrearPedidoHandler(db, clock,
            new Microsoft.Extensions.Logging.Abstractions.NullLogger<CrearPedidoHandler>());

        var result = await handler.HandleAsync(
            new CrearPedidoCommand(Guid.NewGuid(), 150.50m),
            CancellationToken.None);

        // Ambas filas deben existir
        var pedidoExiste = await db.Pedidos.AnyAsync(p => p.Id.Value == result.PedidoId);
        pedidoExiste.Should().BeTrue();

        var outbox = await db.OutboxMessages
            .SingleOrDefaultAsync(o => o.AggregateId == result.PedidoId.ToString());
        outbox.Should().NotBeNull();
        outbox!.EventType.Should().Be("PedidoCreado.v1");
        outbox.Status.Should().Be(OutboxStatus.Pending);
        outbox.Payload.Should().Contain(result.PedidoId.ToString());
    }

    [Fact]
    public async Task ClaimAtomico_dosWorkersNoSePisan()
    {
        // Sembrar 100 eventos pending
        await using (var scope = _services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
            for (int i = 0; i < 100; i++)
            {
                db.OutboxMessages.Add(new OutboxMessage
                {
                    EventId = Guid.CreateVersion7(),
                    AggregateType = "Test",
                    AggregateId = $"agg-{i}",
                    EventType = "TestEvent",
                    Payload = "{}",
                    Status = OutboxStatus.Pending,
                    CreatedAt = DateTime.UtcNow
                });
            }
            await db.SaveChangesAsync();
        }

        // Dos workers reclaman en paralelo
        async Task<List<Guid>> ClaimBatch(string workerId)
        {
            await using var scope = _services.CreateAsyncScope();
            var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
            var claimed = await db.OutboxMessages.FromSqlInterpolated($@"
                UPDATE TOP (60) o
                SET [Status]=1, [ClaimedBy]={workerId}, [Attempts]=[Attempts]+1
                OUTPUT inserted.*
                FROM [dbo].[Outbox] o WITH (READPAST, UPDLOCK, ROWLOCK)
                WHERE o.[Status] = 0
                ORDER BY o.[CreatedAt]")
                .AsNoTracking()
                .ToListAsync();
            return claimed.Select(c => c.EventId).ToList();
        }

        var t1 = ClaimBatch("worker-A");
        var t2 = ClaimBatch("worker-B");
        await Task.WhenAll(t1, t2);

        var ids1 = await t1;
        var ids2 = await t2;

        // Cada worker reclama lo suyo, sin overlap
        ids1.Intersect(ids2).Should().BeEmpty("ningún evento debe estar en ambos workers");
        (ids1.Count + ids2.Count).Should().BeLessThanOrEqualTo(100);
        (ids1.Count + ids2.Count).Should().BeGreaterThan(50, "deben haber reclamado la mayoría");
    }
}
