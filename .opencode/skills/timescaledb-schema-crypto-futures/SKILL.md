---
name: timescaledb-schema-crypto-futures
description: Diseña y aplica schemas TimescaleDB idempotentes para OHLCV, funding, open interest y liquidations con hypertables, continuous aggregates realtime, compression y runner de migraciones. Usar en Fase 0 y al añadir tablas.
---

# Convenciones obligatorias
- timestamptz UTC; DOUBLE PRECISION (nunca NUMERIC en series); symbol/exchange TEXT.
- PK compuesta SIEMPRE: (symbol, exchange, ts). Obligatorio si se particiona por symbol.
- Imagen fijada: timescale/timescaledb:2.x.y-pg16 (nunca latest). Verifica el tag real en Docker Hub.

# Migraciones (NO usar docker-entrypoint-initdb.d)
- initdb solo corre con volumen vacío: no sirve para evolucionar el schema.
- tsdb/migrations/NN_nombre.sql + migrate.py que:
  1. crea tabla schema_migrations(version TEXT PRIMARY KEY, applied_at timestamptz),
  2. aplica en orden los NN_*.sql pendientes, cada uno en su transacción,
  3. es idempotente (IF NOT EXISTS / DO $$ ... $$).
- Servicio compose `migrate` (profile batch) que corre tras tsdb healthy: `docker compose run --rm migrate`.
- Los caggs (CREATE MATERIALIZED VIEW ... WITH NO DATA) no pueden ir dentro de una transacción: ese archivo se marca `-- no-transaction` y migrate.py lo ejecuta con autocommit.

# DDL patrón
```sql
CREATE TABLE IF NOT EXISTS candles_1m (
  symbol TEXT NOT NULL, exchange TEXT NOT NULL, open_time TIMESTAMPTZ NOT NULL,
  open DOUBLE PRECISION NOT NULL, high DOUBLE PRECISION NOT NULL,
  low DOUBLE PRECISION NOT NULL, close DOUBLE PRECISION NOT NULL,
  volume DOUBLE PRECISION NOT NULL, quote_volume DOUBLE PRECISION,
  trades INTEGER, taker_buy_volume DOUBLE PRECISION,
  PRIMARY KEY (symbol, exchange, open_time));
SELECT create_hypertable('candles_1m','open_time',
  chunk_time_interval => INTERVAL '7 days',
  partitioning_column => 'symbol', number_partitions => 4,
  if_not_exists => TRUE);
```
Tablas: candles_1m, funding, open_interest, liquidations (todas con PK compuesta y hypertable).

# Continuous aggregates (realtime)
```sql
CREATE MATERIALIZED VIEW IF NOT EXISTS candles_1h WITH (timescaledb.continuous,
  timescaledb.materialized_only = false) AS
SELECT symbol, exchange, time_bucket('1 hour', open_time) AS bucket,
  first(open, open_time) AS open, max(high) AS high, min(low) AS low,
  last(close, open_time) AS close, sum(volume) AS volume,
  sum(quote_volume) AS quote_volume
FROM candles_1m GROUP BY 1,2,3 WITH NO DATA;
SELECT add_continuous_aggregate_policy('candles_1h',
  start_offset => INTERVAL '3 days', end_offset => INTERVAL '1 hour',
  schedule_interval => INTERVAL '15 minutes', if_not_exists => TRUE);
```
Otros caggs: funding_daily (avg/sum/count), oi_5m (avg), liq_1h (sum qty, sum notional GROUP BY side).

# Refresh tras carga histórica (OBLIGATORIO)
La policy solo refresca los últimos 3 días. Tras cualquier carga masiva:
`CALL refresh_continuous_aggregate('candles_1h', <desde>, <hasta>);` (y los demás caggs).
Debe ser un paso del loader y de su AC.

# Compression (migración separada, desactivada por defecto)
- Archivo 50_compression.sql, aplicado SOLO si ENABLE_COMPRESSION=true en .env.
- Mientras se hace el backfill histórico: ENABLE_COMPRESSION=false. Insertar con ON CONFLICT en chunks comprimidos es muy lento.
- Orden correcto: cargar histórico → refresh caggs → activar compression.
- Si hay que cargar datos viejos con compression activa: decompress_chunk() de los chunks afectados, cargar, recomprimir.
```sql
ALTER TABLE candles_1m SET (timescaledb.compress,
  timescaledb.compress_segmentby = 'symbol, exchange',
  timescaledb.compress_orderby = 'open_time DESC');
SELECT add_compression_policy('candles_1m', INTERVAL '7 days', if_not_exists => TRUE); -- funding: 30 días
```

# Retención
Nunca borrar raw Parquet. add_retention_policy solo en trades tick (ej. 180 días); OHLCV derivado se regenera del lake.

# Verificación obligatoria
- migrate.py dos veces seguidas: la segunda no aplica nada y no falla.
- 3 INSERT de prueba con ON CONFLICT DO NOTHING; refresh del cagg y comprobar que los refleja.
- hypertable_size() antes/después de compression (cuando se active).