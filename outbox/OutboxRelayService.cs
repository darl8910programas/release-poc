using System.Diagnostics;
using Azure.Messaging.ServiceBus;
using Microsoft.EntityFrameworkCore;
using PedidosService.Api.Infrastructure.Outbox;

namespace PedidosService.Api.Infrastructure.Outbox;

public class OutboxRelayOptions
{
    public string TopicName { get; set; } = "pedidos-events";
    public int BatchSize { get; set; } = 100;
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromMilliseconds(500);
    public TimeSpan IdleBackoff { get; set; } = TimeSpan.FromSeconds(2);
    public int MaxAttempts { get; set; } = 10;
}

/// <summary>
/// El relay del outbox. Reclama lotes de eventos PENDING, los publica al Service Bus,
/// y los marca como PUBLISHED. Tolerante a fallos (at-least-once delivery).
/// Puede correrse en múltiples instancias en paralelo sin coordinación.
/// </summary>
public sealed class OutboxRelayService(
    IServiceScopeFactory scopeFactory,
    ServiceBusClient sbClient,
    IOptions<OutboxRelayOptions> options,
    OutboxMetrics metrics,
    TimeProvider clock,
    ILogger<OutboxRelayService> logger) : BackgroundService
{
    private static readonly ActivitySource Tracer = new("PedidosService.Outbox");

    private readonly OutboxRelayOptions _opts = options.Value;
    private readonly string _workerId = BuildWorkerId();

    private ServiceBusSender? _sender;

    private static string BuildWorkerId()
    {
        var machine = Environment.MachineName;
        var pid = Environment.ProcessId;
        return $"{machine}-{pid}-{Guid.NewGuid():N}"[..Math.Min(100, 50)];
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _sender = sbClient.CreateSender(_opts.TopicName);
        logger.LogInformation(
            "OutboxRelay iniciado. WorkerId={WorkerId} Topic={Topic} BatchSize={BatchSize}",
            _workerId, _opts.TopicName, _opts.BatchSize);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var processed = await ProcessBatchAsync(stoppingToken);

                // Si no había trabajo, dormí más. Si había, vuelta inmediata.
                var delay = processed == 0 ? _opts.IdleBackoff : _opts.PollInterval;
                await Task.Delay(delay, clock, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error procesando batch del outbox. WorkerId={WorkerId}", _workerId);
                await Task.Delay(TimeSpan.FromSeconds(5), clock, stoppingToken);
            }
        }

        if (_sender is not null)
            await _sender.DisposeAsync();

        logger.LogInformation("OutboxRelay detenido. WorkerId={WorkerId}", _workerId);
    }

    private async Task<int> ProcessBatchAsync(CancellationToken ct)
    {
        using var activity = Tracer.StartActivity("outbox.process_batch", ActivityKind.Internal);
        var sw = Stopwatch.StartNew();

        await using var scope = scopeFactory.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();

        // Paso 1: reclamar el lote atómicamente con UPDATE ... OUTPUT.
        // Hints clave de SQL Server:
        //   - READPAST: salta filas bloqueadas por otros workers (≈ SKIP LOCKED de Postgres)
        //   - UPDLOCK : toma U-lock al leer; evita deadlocks
        //   - ROWLOCK : fuerza locks a nivel fila, no de página
        // OUTPUT inserted.* devuelve las filas reclamadas en el mismo round-trip.
        var batch = await db.OutboxMessages
            .FromSqlInterpolated($@"
                UPDATE TOP ({_opts.BatchSize}) o
                SET
                    [Status]    = 1,                          -- PROCESSING
                    [ClaimedBy] = {_workerId},
                    [ClaimedAt] = SYSUTCDATETIME(),
                    [Attempts]  = [Attempts] + 1
                OUTPUT
                    inserted.[EventId],
                    inserted.[AggregateType],
                    inserted.[AggregateId],
                    inserted.[EventType],
                    inserted.[Payload],
                    inserted.[Headers],
                    inserted.[CreatedAt],
                    inserted.[ProcessedAt],
                    inserted.[Status],
                    inserted.[Attempts],
                    inserted.[LastError],
                    inserted.[ClaimedBy],
                    inserted.[ClaimedAt]
                FROM [dbo].[Outbox] o WITH (READPAST, UPDLOCK, ROWLOCK)
                WHERE o.[Status] = 0                          -- PENDING
                ORDER BY o.[CreatedAt]")
            .AsNoTracking()
            .ToListAsync(ct);

        if (batch.Count == 0)
        {
            sw.Stop();
            return 0;
        }

        activity?.SetTag("outbox.batch.size", batch.Count);
        metrics.BatchSize.Record(batch.Count);

        // Paso 2: separar los que excedieron MaxAttempts → FAILED
        var publishables = batch.Where(m => m.Attempts <= _opts.MaxAttempts).ToList();
        var poisonous = batch.Where(m => m.Attempts > _opts.MaxAttempts).ToList();

        var publishedIds = new List<Guid>(publishables.Count);
        var failedIds = new List<(Guid Id, string Error)>(poisonous.Count);

        foreach (var p in poisonous)
        {
            failedIds.Add((p.EventId,
                $"Excedido MaxAttempts={_opts.MaxAttempts} (intentos={p.Attempts})"));
        }

        // Paso 3: publicar al Service Bus en batches (más eficiente que envíos sueltos)
        if (publishables.Count > 0)
        {
            await PublishBatchAsync(publishables, publishedIds, failedIds, ct);
        }

        // Paso 4: marcar resultados en la BD
        if (publishedIds.Count > 0)
        {
            // SQL Server tiene un límite de parámetros por query (2100). Si esto crece, lotear.
            await db.Database.ExecuteSqlInterpolatedAsync($@"
                UPDATE [dbo].[Outbox]
                SET [Status] = 2,                  -- PUBLISHED
                    [ProcessedAt] = SYSUTCDATETIME(),
                    [ClaimedBy] = NULL,
                    [LastError] = NULL
                WHERE [EventId] IN ({publishedIds})", ct);

            metrics.EventsPublished.Add(publishedIds.Count);
        }

        foreach (var (id, err) in failedIds)
        {
            await db.Database.ExecuteSqlInterpolatedAsync($@"
                UPDATE [dbo].[Outbox]
                SET [Status] = 3,                  -- FAILED
                    [LastError] = {err},
                    [ClaimedBy] = NULL
                WHERE [EventId] = {id}", ct);

            metrics.EventsFailed.Add(1);
        }

        sw.Stop();
        metrics.BatchDurationMs.Record(sw.Elapsed.TotalMilliseconds);

        logger.LogInformation(
            "Batch procesado. Total={Total} Publicados={Pub} Fallidos={Fail} Duración={Ms:F0}ms",
            batch.Count, publishedIds.Count, failedIds.Count, sw.Elapsed.TotalMilliseconds);

        return batch.Count;
    }

    private async Task PublishBatchAsync(
        List<OutboxMessage> messages,
        List<Guid> publishedIds,
        List<(Guid Id, string Error)> failedIds,
        CancellationToken ct)
    {
        var sbBatch = await _sender!.CreateMessageBatchAsync(ct);
        var inBatch = new List<OutboxMessage>(messages.Count);

        async Task FlushAsync()
        {
            if (sbBatch.Count == 0) return;
            try
            {
                await _sender.SendMessagesAsync(sbBatch, ct);
                publishedIds.AddRange(inBatch.Select(m => m.EventId));
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex,
                    "Falló publicación de sub-batch de {Count} mensajes. Quedarán pending.",
                    inBatch.Count);
                // No los marcamos FAILED: quedan en PROCESSING, el orphan rescuer los devuelve.
            }
            finally
            {
                sbBatch.Dispose();
                inBatch.Clear();
            }
        }

        foreach (var m in messages)
        {
            var sbMessage = BuildServiceBusMessage(m);

            if (!sbBatch.TryAddMessage(sbMessage))
            {
                // El batch actual está lleno. Enviar y empezar otro.
                if (sbBatch.Count == 0)
                {
                    // El mensaje individual es más grande que el batch máximo (1 MB típico)
                    failedIds.Add((m.EventId, "Mensaje excede tamaño máximo del Service Bus"));
                    continue;
                }

                await FlushAsync();
                sbBatch = await _sender.CreateMessageBatchAsync(ct);
                if (!sbBatch.TryAddMessage(sbMessage))
                {
                    failedIds.Add((m.EventId, "Mensaje excede tamaño máximo del Service Bus"));
                    continue;
                }
            }
            inBatch.Add(m);
        }

        await FlushAsync();
    }

    private static ServiceBusMessage BuildServiceBusMessage(OutboxMessage m)
    {
        var msg = new ServiceBusMessage(BinaryData.FromString(m.Payload))
        {
            // MessageId == EventId habilita la deduplicación nativa del Service Bus
            // (configurar RequiresDuplicateDetection=true en el topic)
            MessageId   = m.EventId.ToString(),

            // SessionId garantiza orden FIFO por aggregate
            // (configurar RequiresSession=true en las subscriptions)
            SessionId   = m.AggregateId,

            Subject     = m.EventType,
            ContentType = "application/json",
            CorrelationId = m.AggregateId
        };

        msg.ApplicationProperties["aggregateType"] = m.AggregateType;
        msg.ApplicationProperties["aggregateId"]   = m.AggregateId;
        msg.ApplicationProperties["eventType"]     = m.EventType;
        msg.ApplicationProperties["eventId"]       = m.EventId.ToString();
        msg.ApplicationProperties["createdAt"]     = m.CreatedAt.ToString("O");

        if (!string.IsNullOrEmpty(m.Headers))
            msg.ApplicationProperties["headers"] = m.Headers;

        return msg;
    }
}
