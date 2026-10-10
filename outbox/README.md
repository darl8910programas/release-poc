# Outbox Pattern Demo — .NET 10 + SQL Server + Azure Service Bus

Implementación de referencia del patrón **Transactional Outbox** sobre SQL Server y Azure Service Bus, usando .NET 10. Pensado como ejemplo educativo y como punto de partida para un template de tu IDP...

## Qué incluye

- **`PedidosService.Api`** — Productor: API REST con minimal APIs, EF Core 10, y los `BackgroundService` del relay y orphan rescuer.
- **`PedidosService.Inventario`** — Consumidor de ejemplo: lee del Service Bus con `SessionProcessor` (orden por aggregate), aplica patrón Inbox (idempotencia con tabla de eventos procesados), maneja DLQ.
- **`PedidosService.Tests`** — Tests de integración con Testcontainers (SQL Server real en Docker).
- **`db/01-schema.sql`** — Script T-SQL del schema completo.
- **`docker-compose.yml`** — SQL Server, emulador de Azure Service Bus y Aspire Dashboard para observabilidad local.

## Arquitectura

```
   ┌───────────┐      POST /api/pedidos       ┌──────────────────────────────────┐
   │  Cliente  │ ───────────────────────────▶ │  PedidosService.Api              │
   └───────────┘                              │                                  │
                                              │  ┌────────────────────────────┐  │
                                              │  │  CrearPedidoHandler        │  │
                                              │  │   ① INSERT pedidos        │  │
                                              │  │   ② INSERT outbox         │  │
                                              │  │  ────  1 sola TX  ────     │  │
                                              │  └────────────────────────────┘  │
                                              │                                  │
                                              │  ┌────────────────────────────┐  │
                                              │  │  OutboxRelayService        │  │
                                              │  │  (BackgroundService)       │  │
                                              │  │  ③ claim batch via         │  │
                                              │  │    UPDATE...OUTPUT         │  │
                                              │  │  ④ publish to ServiceBus   │  │
                                              │  │  ⑤ mark as PUBLISHED       │  │
                                              │  └────────────────────────────┘  │
                                              └────┬────────────────────────┬────┘
                                                   │                        │
                                                   ▼                        ▼
                                            ┌──────────────┐         ┌─────────────────┐
                                            │  SQL Server  │         │  Service Bus    │
                                            │  - Pedidos   │         │  Topic:         │
                                            │  - Outbox    │         │  pedidos-events │
                                            └──────────────┘         └────────┬────────┘
                                                                              │
                                                                              ▼
                                              ┌──────────────────────────────────┐
                                              │  PedidosService.Inventario       │
                                              │                                  │
                                              │  ⑥ SessionProcessor receives     │
                                              │  ⑦ check Inbox (dedup)           │
                                              │  ⑧ business logic + INSERT inbox │
                                              │     en 1 sola TX                 │
                                              └──────────────────────────────────┘
```

## Puntos clave del diseño

### Atomicidad del Outbox (`CrearPedidoHandler`)

El `SaveChangesAsync` mete ambos INSERT (pedido + outbox) en una transacción. Si commitea, ambos persistieron; si falla, ninguno. **Esto es lo que da la garantía**.

### Claim atómico con `READPAST`

El relay usa:

```sql
UPDATE TOP (N) o
SET Status = 1, ClaimedBy = @WorkerId, Attempts = Attempts + 1
OUTPUT inserted.*
FROM dbo.Outbox o WITH (READPAST, UPDLOCK, ROWLOCK)
WHERE o.Status = 0
ORDER BY o.CreatedAt
```

`READPAST` es el equivalente SQL Server del `SKIP LOCKED` de PostgreSQL. Te permite correr **N instancias del relay** sin coordinación, sin pisarse.

### Service Bus: `MessageId` + `SessionId`

- `MessageId = EventId` → habilita **duplicate detection** nativa (10 min de ventana configurada). Si el relay publica el mismo evento dos veces (porque crasheó antes de marcar PUBLISHED), Service Bus descarta el duplicado.
- `SessionId = AggregateId` → garantiza **orden FIFO por pedido**. Múltiples eventos del mismo pedido llegan en orden al consumidor.

### Inbox del consumidor (idempotencia)

El consumidor consulta `dbo.Inbox` antes de procesar. Si ya procesó ese `EventId`, completa el mensaje sin re-ejecutar la lógica. Si no, ejecuta la lógica + INSERT al Inbox en la misma TX. La PK `(EventId, ConsumerName)` impide duplicados aún si dos workers corren la lógica en paralelo.

### Orphan rescuer

`OutboxOrphanRescuerService` corre cada minuto y devuelve a PENDING los mensajes en PROCESSING > 5 min. Sin esto, mensajes reclamados por workers que crashearon se quedarían bloqueados para siempre.

### UUID v7

Tanto `Pedido.Id` como `OutboxMessage.EventId` usan `Guid.CreateVersion7()`. Los UUIDs v7 incluyen timestamp en los bits altos → inserciones secuenciales → cero fragmentación del clustered index. Crítico a escala.

### Observabilidad

- **Métricas** (`OutboxMetrics`): `outbox.events.published`, `outbox.events.failed`, `outbox.events.rescued`, `outbox.batch.duration`, `outbox.batch.size`.
- **Tracing**: `ActivitySource("PedidosService.Outbox")` por cada batch.
- Todo se exporta vía OTLP → Aspire Dashboard local o Azure Monitor en producción.

## Requisitos

- .NET 10 SDK (`dotnet --version` debe mostrar `10.0.x`)
- Docker / Docker Desktop
- (Opcional) `jq` y `uuidgen` para el smoke test

## Inicio rápido (local con Docker)

```bash
# 1. Levantar la infraestructura
docker compose up -d

# 2. Esperar a que SQL inicialice
docker compose logs -f sql-init   # Ctrl+C cuando termine

# 3. Correr el productor (API)
cd src/PedidosService.Api
dotnet run

# 4. En otra terminal, correr el consumidor
cd src/PedidosService.Inventario
dotnet run

# 5. En otra terminal, disparar pedidos
curl -X POST http://localhost:5000/api/pedidos \
  -H "Content-Type: application/json" \
  -d '{"clienteId":"11111111-1111-1111-1111-111111111111","total":42.50}'

# 6. Ver estado del Outbox
curl http://localhost:5000/api/pedidos/outbox/stats | jq .

# 7. Smoke test concurrente (requiere uuidgen y jq)
./scripts/smoke-test.sh 500
```

## Ver métricas en Aspire Dashboard

Una vez levantado `docker compose`, abrí `http://localhost:18888`. Las apps exportan métricas y traces vía OTLP al puerto 4317 del dashboard.

## Probar el patrón en acción

### Demo 1: atomicidad
1. Modificar `CrearPedidoHandler` para que lance una excepción **después** de `db.Pedidos.Add(...)` y **antes** de `SaveChangesAsync`.
2. Llamar al endpoint. Verificar que ni el pedido ni el evento existen en BD. La transacción hizo rollback.

### Demo 2: tolerancia a fallos del broker
1. Detener el emulador de Service Bus: `docker compose stop servicebus`.
2. Crear pedidos. Verificar que los eventos quedan en `Status=PENDING`.
3. Levantar Service Bus: `docker compose start servicebus`.
4. Verificar que el relay los publica al recuperarse. **Nada se perdió.**

### Demo 3: concurrencia del relay
Editá `Program.cs` y registrá el relay 3 veces:
```csharp
builder.Services.AddHostedService<OutboxRelayService>();
builder.Services.AddHostedService<OutboxRelayService>();
builder.Services.AddHostedService<OutboxRelayService>();
```
Disparar el smoke test con `./scripts/smoke-test.sh 1000`. Mirar los logs: cada worker reclamó un subset distinto.

### Demo 4: orphan rescue
1. Bajar el orphan rescuer timeout a 30 segundos en `appsettings.Development.json`.
2. Insertar a mano filas en `dbo.Outbox` con `Status=1` (PROCESSING) y `ClaimedAt` viejo.
3. Esperar 1 minuto. Verificar que vuelven a PENDING y se publican.

## Pruebas

```bash
cd tests/PedidosService.Tests
dotnet test
```

Las pruebas levantan un container de SQL Server 2022 vía Testcontainers. Cubren:
- Atomicidad del Outbox.
- Claim no-pisado con `READPAST` entre workers concurrentes.

## Despliegue en Azure (alto nivel)

Para producción reemplazá el emulador y el SQL local por:

| Componente local        | Producción en Azure                                                  |
|-------------------------|----------------------------------------------------------------------|
| SQL Server en Docker    | Azure SQL Database (S3+ para CDC) o Managed Instance                 |
| Service Bus Emulator    | Azure Service Bus **Premium** (requerido para sessions a escala)     |
| Hosted Service polling  | Container Apps con N réplicas. KEDA escala según pending count.      |
| Connection string       | Managed Identity + `DefaultAzureCredential`                          |
| Aspire Dashboard        | Application Insights / Azure Monitor                                 |

Settings críticos del Service Bus Topic en producción:
- `RequiresDuplicateDetection = true`
- `DuplicateDetectionHistoryTimeWindow = PT10M` (mínimo)
- Subscriptions con `RequiresSession = true` si necesitás orden por aggregate
- `MaxDeliveryCount = 10` y DLQ habilitada

Settings de Azure SQL:
- `ALTER DATABASE [...] SET READ_COMMITTED_SNAPSHOT ON;`
- Habilitar `EnableRetryOnFailure` en EF (ya configurado en este proyecto)
- Connection pooling correcto si tenés varias réplicas de Container Apps

## Cuándo migrar a CDC + Debezium

El polling escala perfectamente hasta unos cuantos miles de eventos por segundo. Migrá a CDC + Debezium si:
- Necesitás latencia sub-100ms entre commit y publicación.
- La carga de polling impacta tu Azure SQL DTU/vCore presupuestado.
- Tenés muchos servicios productores y querés operar una infraestructura común.

Debezium tiene un **Outbox Event Router SMT** que mapea esta misma tabla a topics de Kafka/Event Hubs automáticamente.

## Estructura del proyecto

```
outbox-demo/
├── OutboxDemo.sln
├── README.md
├── docker-compose.yml
├── db/
│   └── 01-schema.sql                         # Schema completo en T-SQL
├── scripts/
│   ├── servicebus-config.json                # Config del emulador
│   └── smoke-test.sh                         # Test de concurrencia
├── src/
│   ├── PedidosService.Api/                   # Productor
│   │   ├── Program.cs                        # Composition root
│   │   ├── Domain/
│   │   │   ├── Pedido.cs                     # Entidad + IDs tipados
│   │   │   └── Events/PedidoCreado.cs        # Contrato del evento
│   │   ├── Application/
│   │   │   └── CrearPedidoHandler.cs         # ★ El handler atómico
│   │   ├── Infrastructure/
│   │   │   ├── AppDbContext.cs               # EF Core + config
│   │   │   └── Outbox/
│   │   │       ├── OutboxMessage.cs          # Entidad outbox
│   │   │       ├── OutboxWriter.cs           # Extension methods
│   │   │       ├── OutboxRelayService.cs     # ★★ El relay
│   │   │       ├── OutboxOrphanRescuerService.cs
│   │   │       └── OutboxMetrics.cs
│   │   └── Endpoints/PedidosEndpoints.cs
│   └── PedidosService.Inventario/            # Consumidor
│       ├── Program.cs
│       ├── InventarioConsumer.cs             # ★ Session-aware + Inbox
│       └── Inbox/ProcessedMessage.cs
└── tests/
    └── PedidosService.Tests/
        └── OutboxIntegrationTests.cs         # Testcontainers
```

## Próximos pasos sugeridos para tu IDP

1. **Extraer Outbox a un paquete NuGet interno** (`Indigo.Messaging.Outbox`) — el relay, el writer, las migraciones, la config. Cada squad lo referencia y solo escribe sus eventos de dominio.
2. **Template de Container App** que ya viene con el relay configurado, Managed Identity hacia SQL y Service Bus, OpenTelemetry hacia Azure Monitor, y health checks.
3. **Schema registry interno** para validar contratos de eventos antes de mergear PRs.
4. **Dashboard común en Grafana/Azure Monitor** con paneles por servicio: pending count, oldest pending age, throughput, error rate.
5. **Convención de naming** documentada (topic per bounded context, eventType con versión semántica, etc).
