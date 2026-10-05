-- 14_trades.sql : trades tick (lo unico que no existe en ninguna fuente historica)
-- Este es el motivo de existir del feed-daemon: ni Binance Vision ni ningun backfill dan ticks.
--
-- Grano = 1 fila por trade. La PK usa el `trade_id` NATIVO del exchange (verificado en
-- cryptofeed 3.0.1: Binance `aggTradeId`, Bybit `execId`, OKX `tradeId`, Bitget `tradeId`,
-- Hyperliquid `tid`), que es unico y monotono. Usar la PK hace idempotente el reenvio tras un
-- corte de red sin necesidad de tabla de deduplicacion.
--
-- `ts` va en la PK porque TimescaleDB lo exige: no se puede crear un indice unico sin la columna
-- de particion. Sin `ts` la migracion falla con
-- "cannot create a unique index without the column ts (used in partitioning)".
-- No se pierde deduplicacion por incluirlo: `trade_id` ya es unico, y `ts` solo aniade granularidad.
--
-- `ts` es TIMESTAMPTZ. cryptofeed entrega epoch en segundos (float) y ya normaliza por exchange
-- (`Binance.timestamp_normalize` y `Hyperliquid.timestamp_normalize` dividen entre 1000), pero el
-- writer vuelve a autodetectar la unidad por magnitud (regla 3 de AGENTS.md) en vez de confiar.
--
-- `receipt_ts` es cuando NOSOTROS recibimos el mensaje, no cuando lo emitio el exchange. Es lo
-- que permite medir el AC de "p95 trade->fila" de verdad.
CREATE TABLE IF NOT EXISTS trades (
  symbol TEXT NOT NULL,
  exchange TEXT NOT NULL,
  trade_id TEXT NOT NULL,
  ts TIMESTAMPTZ NOT NULL,
  receipt_ts TIMESTAMPTZ,
  side TEXT NOT NULL CHECK (side IN ('buy', 'sell')),
  price DOUBLE PRECISION NOT NULL,
  amount DOUBLE PRECISION NOT NULL,
  notional DOUBLE PRECISION,
  PRIMARY KEY (symbol, exchange, ts, trade_id)
);

SELECT create_hypertable('trades', 'ts',
  chunk_time_interval => INTERVAL '7 days',
  partitioning_column => 'symbol', number_partitions => 4,
  if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS trades_ts_idx ON trades (ts DESC);
CREATE INDEX IF NOT EXISTS trades_symbol_ts_idx ON trades (symbol, ts DESC);