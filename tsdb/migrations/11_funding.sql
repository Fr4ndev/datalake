-- 11_funding.sql : funding rate de perpetuos
-- OJO: el intervalo real varía por símbolo (8h por defecto, pero hay 4h y 1h). No asumir 8h.
CREATE TABLE IF NOT EXISTS funding (
  symbol TEXT NOT NULL,
  exchange TEXT NOT NULL,
  funding_time TIMESTAMPTZ NOT NULL,
  funding_rate DOUBLE PRECISION NOT NULL,
  mark_price DOUBLE PRECISION,
  next_funding_time TIMESTAMPTZ,
  PRIMARY KEY (symbol, exchange, funding_time)
);

SELECT create_hypertable('funding', 'funding_time',
  chunk_time_interval => INTERVAL '30 days',
  partitioning_column => 'symbol', number_partitions => 4,
  if_not_exists => TRUE);