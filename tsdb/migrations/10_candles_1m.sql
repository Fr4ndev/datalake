-- 10_candles_1m.sql : velas de 1 minuto (spot de referencia, fuente Binance Vision klines)
CREATE TABLE IF NOT EXISTS candles_1m (
  symbol TEXT NOT NULL,
  exchange TEXT NOT NULL,
  open_time TIMESTAMPTZ NOT NULL,
  open DOUBLE PRECISION NOT NULL,
  high DOUBLE PRECISION NOT NULL,
  low DOUBLE PRECISION NOT NULL,
  close DOUBLE PRECISION NOT NULL,
  volume DOUBLE PRECISION NOT NULL,
  quote_volume DOUBLE PRECISION,
  trades INTEGER,
  taker_buy_volume DOUBLE PRECISION,
  PRIMARY KEY (symbol, exchange, open_time)
);

SELECT create_hypertable('candles_1m', 'open_time',
  chunk_time_interval => INTERVAL '7 days',
  partitioning_column => 'symbol', number_partitions => 4,
  if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS candles_1m_exchange_idx
  ON candles_1m (exchange, open_time DESC);