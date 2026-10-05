-- 13_liquidations.sql : liquidaciones (forzadas)
-- Grano = 1 fila por (symbol, exchange, segundo, side). El `side` forma parte de la PK porque
-- en el mismo segundo coexisten sidelong y sidesell; así el ON CONFLICT hace idempotente el
-- insert del feed y el cagg liq_1h puede agregar por side sin perder ninguno.
-- Histórico: Binance liquidationSnapshot solo cubre ~2020-01 -> 2021-12; después, WS (~1 de cada 20).
CREATE TABLE IF NOT EXISTS liquidations (
  symbol TEXT NOT NULL,
  exchange TEXT NOT NULL,
  ts TIMESTAMPTZ NOT NULL,
  side TEXT NOT NULL CHECK (side IN ('buy', 'sell')),
  price DOUBLE PRECISION NOT NULL,
  quantity DOUBLE PRECISION NOT NULL,
  notional DOUBLE PRECISION,
  PRIMARY KEY (symbol, exchange, ts, side)
);

SELECT create_hypertable('liquidations', 'ts',
  chunk_time_interval => INTERVAL '7 days',
  partitioning_column => 'symbol', number_partitions => 4,
  if_not_exists => TRUE);