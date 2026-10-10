using System.Text.Json;
using Azure.Messaging.ServiceBus;
using Microsoft.EntityFrameworkCore;
using PedidosService.Inventario.Inbox;

namespace PedidosService.Inventario;

public class InventarioConsumerOptions
{
    public string TopicName { get; set; } = "pedidos-events";
    public string SubscriptionName { get; set; } = "inventario";
    public int MaxConcurrentSessions { get; set; } = 8;
    public int MaxConcurrentCallsPerSession { get; set; } = 1;
}

/// <summary>
/// Consumidor de eventos PedidoCreado. Reserva stock de forma idempotente.
///
/// Idempotencia:
///  - Service Bus puede entregar el mismo mensaje más de una vez (at-least-once)
///  - El productor puede haber publicado duplicados (outbox crash entre publish y UPDATE)
///  - Resolvemos ambos con el Inbox: INSERT al Inbox primero; si viola PK, ya procesado.
///
/// Orden:
///  - Usamos SessionProcessor (no ServiceBusProcessor) para procesar mensajes en orden
///    por SessionId (= aggregate_id). Mensajes del mismo pedido se procesan secuencialmente.
/// </summary>
public sealed class InventarioConsumer(
    ServiceBusClient sbClient,
    IServiceScopeFactory scopeFactory,
    IOptions<InventarioConsumerOptions> options,
    ILogger<InventarioConsumer> logger) : BackgroundService
{
    private const string ConsumerName = "inventario";
    private readonly InventarioConsumerOptions _opts = options.Value;
    private ServiceBusSessionProcessor? _processor;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _processor = sbClient.CreateSessionProcessor(
            _opts.TopicName,
            _opts.SubscriptionName,
            new ServiceBusSessionProcessorOptions
            {
                MaxConcurrentSessions = _opts.MaxConcurrentSessions,
                MaxConcurrentCallsPerSession = _opts.MaxConcurrentCallsPerSession,
                AutoCompleteMessages = false,
                ReceiveMode = ServiceBusReceiveMode.PeekLock
            });

        _processor.ProcessMessageAsync += HandleMessageAsync;
        _processor.ProcessErrorAsync += HandleErrorAsync;

        await _processor.StartProcessingAsync(stoppingToken);
        logger.LogInformation("InventarioConsumer iniciado. Sub={Sub}", _opts.SubscriptionName);

        try
        {
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException) { }

        await _processor.StopProcessingAsync(CancellationToken.None);
        await _processor.DisposeAsync();
    }

    private async Task HandleMessageAsync(ProcessSessionMessageEventArgs args)
    {
        var msg = args.Message;
        var ct = args.CancellationToken;

        // El MessageId fue seteado por el productor con el EventId del Outbox.
        // Si no parsea, dead-letter inmediatamente.
        if (!Guid.TryParse(msg.MessageId, out var eventId))
        {
            logger.LogWarning("MessageId inválido: {MessageId}. Enviando a DLQ.", msg.MessageId);
            await args.DeadLetterMessageAsync(msg,
                deadLetterReason: "InvalidMessageId",
                deadLetterErrorDescription: $"MessageId no es un Guid: {msg.MessageId}",
                cancellationToken: ct);
            return;
        }

        await using var scope = scopeFactory.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<InventarioDbContext>();

        // Transacción atómica: marcar procesado + ejecutar lógica de negocio.
        // Si la lógica falla, rollback completo y Service Bus reentregará.
        await using var tx = await db.Database.BeginTransactionAsync(ct);

        // Paso 1: intentar registrar como procesado. Si ya existe, es duplicado.
        var yaProcesado = await db.ProcessedMessages
            .AnyAsync(x => x.EventId == eventId && x.ConsumerName == ConsumerName, ct);

        if (yaProcesado)
        {
            logger.LogInformation(
                "Evento {EventId} ya procesado por {Consumer}. Completando (dedup).",
                eventId, ConsumerName);
            await args.CompleteMessageAsync(msg, ct);
            return;
        }

        // Paso 2: parsear el payload
        PedidoCreadoEventDto? evento;
        try
        {
            evento = JsonSerializer.Deserialize<PedidoCreadoEventDto>(
                msg.Body.ToString(),
                new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
        }
        catch (JsonException ex)
        {
            logger.LogError(ex, "Payload inválido para EventId={EventId}", eventId);
            await args.DeadLetterMessageAsync(msg,
                "InvalidPayload", ex.Message, ct);
            return;
        }

        if (evento is null)
        {
            await args.DeadLetterMessageAsync(msg, "EmptyPayload", "Payload nulo", ct);
            return;
        }

        // Paso 3: lógica de negocio (reservar stock)
        try
        {
            db.ProcessedMessages.Add(new ProcessedMessage
            {
                EventId = eventId,
                ConsumerName = ConsumerName,
                ProcessedAt = DateTime.UtcNow
            });

            db.ReservasStock.Add(new ReservaStock
            {
                Id = Guid.CreateVersion7(),
                PedidoId = evento.PedidoId,
                Total = evento.Total,
                CreadoEn = DateTime.UtcNow
            });

            await db.SaveChangesAsync(ct);
            await tx.CommitAsync(ct);
            await args.CompleteMessageAsync(msg, ct);

            logger.LogInformation(
                "Stock reservado para pedido {PedidoId} (eventId={EventId})",
                evento.PedidoId, eventId);
        }
        catch (DbUpdateException ex) when (IsUniqueViolation(ex))
        {
            // Race condition: dos procesadores intentaron procesar el mismo evento
            // a la vez. Uno ganó, este puede completar el mensaje sin más.
            logger.LogInformation(
                "Duplicado detectado en commit para EventId={EventId}. Completando.",
                eventId);
            await args.CompleteMessageAsync(msg, ct);
        }
    }

    private Task HandleErrorAsync(ProcessErrorEventArgs args)
    {
        logger.LogError(args.Exception,
            "Error en ServiceBusSessionProcessor. Source={Source}",
            args.ErrorSource);
        return Task.CompletedTask;
    }

    private static bool IsUniqueViolation(DbUpdateException ex)
    {
        // SQL Server: 2627 (PK violation) o 2601 (unique index violation)
        return ex.InnerException is Microsoft.Data.SqlClient.SqlException sql
            && (sql.Number is 2627 or 2601);
    }

    private sealed record PedidoCreadoEventDto(
        Guid PedidoId,
        Guid ClienteId,
        decimal Total,
        DateTime CreadoEn);
}
