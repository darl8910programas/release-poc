using Microsoft.EntityFrameworkCore;

namespace PedidosService.Api.Infrastructure.Outbox;

public class OutboxOrphanRescuerOptions
{
    /// <summary>
    /// Tiempo máximo que un mensaje puede estar en PROCESSING antes de
    /// considerarlo huérfano (worker crasheado).
    /// </summary>
    public TimeSpan ProcessingTimeout { get; set; } = TimeSpan.FromMinutes(5);

    public TimeSpan RunInterval { get; set; } = TimeSpan.FromMinutes(1);
}

/// <summary>
/// Job periódico que devuelve a PENDING los mensajes que quedaron en PROCESSING
/// por demasiado tiempo (típicamente porque el worker que los reclamó crasheó).
/// Sin esto, esos mensajes nunca se publicarían.
/// </summary>
public sealed class OutboxOrphanRescuerService(
    IServiceScopeFactory scopeFactory,
    IOptions<OutboxOrphanRescuerOptions> options,
    OutboxMetrics metrics,
    TimeProvider clock,
    ILogger<OutboxOrphanRescuerService> logger) : BackgroundService
{
    private readonly OutboxOrphanRescuerOptions _opts = options.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation(
            "OrphanRescuer iniciado. Interval={Interval} Timeout={Timeout}",
            _opts.RunInterval, _opts.ProcessingTimeout);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await RescueAsync(stoppingToken);
                await Task.Delay(_opts.RunInterval, clock, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error rescatando huérfanos del outbox");
                await Task.Delay(TimeSpan.FromSeconds(10), clock, stoppingToken);
            }
        }
    }

    private async Task RescueAsync(CancellationToken ct)
    {
        await using var scope = scopeFactory.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();

        var timeoutSeconds = (int)_opts.ProcessingTimeout.TotalSeconds;

        var affected = await db.Database.ExecuteSqlInterpolatedAsync($@"
            UPDATE [dbo].[Outbox]
            SET [Status]    = 0,                  -- back to PENDING
                [ClaimedBy] = NULL,
                [ClaimedAt] = NULL
            WHERE [Status] = 1                    -- PROCESSING
              AND [ClaimedAt] < DATEADD(SECOND, -{timeoutSeconds}, SYSUTCDATETIME())", ct);

        if (affected > 0)
        {
            metrics.EventsRescued.Add(affected);
            logger.LogWarning("Rescatados {Count} mensajes huérfanos del outbox", affected);
        }
    }
}
