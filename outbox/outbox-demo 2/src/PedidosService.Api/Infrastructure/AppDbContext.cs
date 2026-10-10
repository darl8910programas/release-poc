using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using PedidosService.Api.Domain;
using PedidosService.Api.Infrastructure.Outbox;

namespace PedidosService.Api.Infrastructure;

public class AppDbContext(DbContextOptions<AppDbContext> options) : DbContext(options)
{
    public DbSet<Pedido> Pedidos => Set<Pedido>();
    public DbSet<OutboxMessage> OutboxMessages => Set<OutboxMessage>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.ApplyConfiguration(new PedidoConfig());
        modelBuilder.ApplyConfiguration(new OutboxMessageConfig());
    }
}

public class PedidoConfig : IEntityTypeConfiguration<Pedido>
{
    public void Configure(EntityTypeBuilder<Pedido> b)
    {
        b.ToTable("Pedidos", "dbo");
        b.HasKey(x => x.Id);

        // Value converters para los IDs fuertemente tipados
        b.Property(x => x.Id)
            .HasConversion(id => id.Value, v => new PedidoId(v));

        b.Property(x => x.ClienteId)
            .HasConversion(id => id.Value, v => new ClienteId(v));

        b.Property(x => x.Total).HasColumnType("decimal(18,2)");
        b.Property(x => x.Estado).HasConversion<byte>();
        b.Property(x => x.CreadoEn).HasColumnType("datetime2(3)");
    }
}

public class OutboxMessageConfig : IEntityTypeConfiguration<OutboxMessage>
{
    public void Configure(EntityTypeBuilder<OutboxMessage> b)
    {
        b.ToTable("Outbox", "dbo", t =>
        {
            // Constraint nativa: validar que Payload sea JSON válido
            t.HasCheckConstraint("CK_Outbox_Payload_IsJson", "ISJSON([Payload]) = 1");
            t.HasCheckConstraint("CK_Outbox_Headers_IsJson",
                "[Headers] IS NULL OR ISJSON([Headers]) = 1");
        });

        b.HasKey(x => x.EventId);

        b.Property(x => x.AggregateType).HasMaxLength(100).IsRequired();
        b.Property(x => x.AggregateId).HasMaxLength(100).IsRequired();
        b.Property(x => x.EventType).HasMaxLength(150).IsRequired();
        b.Property(x => x.Payload).HasColumnType("nvarchar(max)").IsRequired();
        b.Property(x => x.Headers).HasColumnType("nvarchar(max)");
        b.Property(x => x.LastError).HasMaxLength(2000);
        b.Property(x => x.ClaimedBy).HasMaxLength(100);

        b.Property(x => x.CreatedAt)
            .HasColumnType("datetime2(3)")
            .HasDefaultValueSql("SYSUTCDATETIME()");

        b.Property(x => x.ProcessedAt).HasColumnType("datetime2(3)");
        b.Property(x => x.ClaimedAt).HasColumnType("datetime2(3)");
        b.Property(x => x.Status).HasConversion<byte>().HasDefaultValue(OutboxStatus.Pending);

        // Índice filtrado: solo indexa lo PENDIENTE. Se mantiene microscópico
        // aunque la tabla histórica tenga 100M de filas.
        b.HasIndex(x => x.CreatedAt)
            .HasDatabaseName("IX_Outbox_Pending")
            .HasFilter("[Status] = 0")
            .IncludeProperties(x => new { x.AggregateId, x.EventType });

        b.HasIndex(x => new { x.AggregateId, x.CreatedAt })
            .HasDatabaseName("IX_Outbox_Aggregate");
    }
}
