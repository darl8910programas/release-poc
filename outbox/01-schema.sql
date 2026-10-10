-- ============================================================
-- Outbox Demo: Schema Setup
-- ============================================================
-- Ejecutar contra una BD vacía en SQL Server 2019+ o Azure SQL.
-- En producción usar EF Core Migrations en lugar de este script.
-- ============================================================

-- Activar snapshot isolation: reduce dramáticamente bloqueos entre lecturas
-- y escrituras, lo que ayuda al patrón Outbox (API insertando, relay leyendo).
ALTER DATABASE CURRENT SET READ_COMMITTED_SNAPSHOT ON;
GO

-- ============================================================
-- BD: PedidosDb (productor)
-- ============================================================

CREATE TABLE dbo.Pedidos
(
    Id          UNIQUEIDENTIFIER NOT NULL CONSTRAINT PK_Pedidos PRIMARY KEY,
    ClienteId   UNIQUEIDENTIFIER NOT NULL,
    Total       DECIMAL(18,2)    NOT NULL,
    Estado      TINYINT          NOT NULL,
    CreadoEn    DATETIME2(3)     NOT NULL
);
GO

CREATE TABLE dbo.Outbox
(
    EventId         UNIQUEIDENTIFIER NOT NULL CONSTRAINT PK_Outbox PRIMARY KEY,
    AggregateType   VARCHAR(100)     NOT NULL,
    AggregateId     VARCHAR(100)     NOT NULL,
    EventType       VARCHAR(150)     NOT NULL,
    Payload         NVARCHAR(MAX)    NOT NULL
                    CONSTRAINT CK_Outbox_Payload_IsJson CHECK (ISJSON(Payload) = 1),
    Headers         NVARCHAR(MAX)    NULL
                    CONSTRAINT CK_Outbox_Headers_IsJson
                    CHECK (Headers IS NULL OR ISJSON(Headers) = 1),

    CreatedAt       DATETIME2(3)     NOT NULL
                    CONSTRAINT DF_Outbox_CreatedAt DEFAULT SYSUTCDATETIME(),
    ProcessedAt     DATETIME2(3)     NULL,

    Status          TINYINT          NOT NULL
                    CONSTRAINT DF_Outbox_Status DEFAULT 0,
                    -- 0=PENDING, 1=PROCESSING, 2=PUBLISHED, 3=FAILED

    Attempts        INT              NOT NULL
                    CONSTRAINT DF_Outbox_Attempts DEFAULT 0,
    LastError       NVARCHAR(2000)   NULL,

    ClaimedBy       VARCHAR(100)     NULL,
    ClaimedAt       DATETIME2(3)     NULL
);
GO

-- Índice filtrado: solo indexa PENDING. Microscópico aunque la tabla crezca.
CREATE NONCLUSTERED INDEX IX_Outbox_Pending
    ON dbo.Outbox (CreatedAt)
    INCLUDE (AggregateId, EventType)
    WHERE Status = 0;
GO

-- Para queries por aggregate y orden
CREATE NONCLUSTERED INDEX IX_Outbox_Aggregate
    ON dbo.Outbox (AggregateId, CreatedAt);
GO

-- ============================================================
-- BD: InventarioDb (consumidor)
-- ============================================================

-- CREATE DATABASE InventarioDb;
-- USE InventarioDb;
-- ALTER DATABASE CURRENT SET READ_COMMITTED_SNAPSHOT ON;

CREATE TABLE dbo.Inbox
(
    EventId       UNIQUEIDENTIFIER NOT NULL,
    ConsumerName  VARCHAR(100)     NOT NULL,
    ProcessedAt   DATETIME2(3)     NOT NULL
                  CONSTRAINT DF_Inbox_ProcessedAt DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_Inbox PRIMARY KEY (EventId, ConsumerName)
);
GO

CREATE TABLE dbo.ReservasStock
(
    Id        UNIQUEIDENTIFIER NOT NULL CONSTRAINT PK_ReservasStock PRIMARY KEY,
    PedidoId  UNIQUEIDENTIFIER NOT NULL,
    Total     DECIMAL(18,2)    NOT NULL,
    CreadoEn  DATETIME2(3)     NOT NULL
);
GO

CREATE UNIQUE NONCLUSTERED INDEX UX_ReservasStock_PedidoId
    ON dbo.ReservasStock (PedidoId);
GO

-- ============================================================
-- Job de mantenimiento: purgar eventos publicados antiguos
-- ============================================================
-- Ejecutar como SQL Agent Job (cada hora o diario) para evitar
-- crecimiento ilimitado de la tabla outbox. Borra en lotes para
-- no bloquear.

/*
DECLARE @batchSize INT = 1000;
DECLARE @rowsDeleted INT = 1;

WHILE @rowsDeleted > 0
BEGIN
    DELETE TOP (@batchSize)
    FROM dbo.Outbox
    WHERE Status = 2                                    -- PUBLISHED
      AND ProcessedAt < DATEADD(DAY, -7, SYSUTCDATETIME());

    SET @rowsDeleted = @@ROWCOUNT;

    WAITFOR DELAY '00:00:00.100';
END
*/
