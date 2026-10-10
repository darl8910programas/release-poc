using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace PedidosService.Inventario.Inbox;

/// <summary>
/// Registro de eventos procesados. La PRIMARY KEY (EventId, ConsumerName)
/// garantiza que el mismo evento no se procese dos veces por el mismo consumidor.
/// Junto con el Outbox del productor, logra el efecto "exactly-once".
/// </summary>
public class ProcessedMessage
{
    public Guid EventId { get; set; }
    public string ConsumerName { get; set; } = default!;
    public DateTime ProcessedAt { get; set; }
}

public class InventarioDbContext(DbContextOptions<InventarioDbContext> options) : DbContext(options)
{
    public DbSet<ProcessedMessage> ProcessedMessages => Set<ProcessedMessage>();
    public DbSet<ReservaStock> ReservasStock => Set<ReservaStock>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.ApplyConfiguration(new ProcessedMessageConfig());
        modelBuilder.ApplyConfiguration(new ReservaStockConfig());
    }
}

public class ProcessedMessageConfig : IEntityTypeConfiguration<ProcessedMessage>
{
    public void Configure(EntityTypeBuilder<ProcessedMessage> b)
    {
        b.ToTable("Inbox", "dbo");
        b.HasKey(x => new { x.EventId, x.ConsumerName });
        b.Property(x => x.ConsumerName).HasMaxLength(100).IsRequired();
        b.Property(x => x.ProcessedAt)
            .HasColumnType("datetime2(3)")
            .HasDefaultValueSql("SYSUTCDATETIME()");
    }
}

// Tabla de negocio del consumidor (ejemplo: reservas de stock)
public class ReservaStock
{
    public Guid Id { get; set; }
    public Guid PedidoId { get; set; }
    public decimal Total { get; set; }
    public DateTime CreadoEn { get; set; }
}

public class ReservaStockConfig : IEntityTypeConfiguration<ReservaStock>
{
    public void Configure(EntityTypeBuilder<ReservaStock> b)
    {
        b.ToTable("ReservasStock", "dbo");
        b.HasKey(x => x.Id);
        b.HasIndex(x => x.PedidoId).IsUnique();
        b.Property(x => x.Total).HasColumnType("decimal(18,2)");
        b.Property(x => x.CreadoEn).HasColumnType("datetime2(3)");
    }
}
