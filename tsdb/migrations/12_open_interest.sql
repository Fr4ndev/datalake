-- 12_open_interest.sql : open interest
-- Backfill histórico SOLO desde el producto `metrics` de Binance Vision (~2022 en adelante, 5m).
-- No usar REST para backfill de open interest.
CREATE TABLE IF NOT EXISTS open_interest (
  symbol TEXT NOT NULL,
  exchange TEXT NOT NULL,
  ts TIMESTAMPTZ NOT NULL,
  open_interest DOUBLE PRECISION NOT NULL,
  open_interest_value DOUBLE PRECISION,
  PRIMARY KEY (symbol, exchange, ts)
);

SELECT create_hypertable('open_interest', 'ts',
  chunk_time_interval => INTERVAL '7 days',
  partitioning_column => 'symbol', number_partitions => 4,
  if_not_exists => TRUE);