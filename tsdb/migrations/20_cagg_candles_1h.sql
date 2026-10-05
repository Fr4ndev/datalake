-- no-transaction
-- 20_cagg_candles_1h.sql : velas 1h derivadas de candles_1m, realtime.
-- materialized_only=false => la zona no materializada se calcula en la propia query.
CREATE MATERIALIZED VIEW IF NOT EXISTS candles_1h
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT symbol,
       exchange,
       time_bucket('1 hour', open_time) AS bucket,
       first(open, open_time)  AS open,
       max(high)                AS high,
       min(low)                 AS low,
       last(close, open_time)   AS close,
       sum(volume)              AS volume,
       sum(quote_volume)        AS quote_volume,
       count(*)                 AS n_1m
FROM candles_1m
GROUP BY 1, 2, 3
WITH NO DATA;

SELECT add_continuous_aggregate_policy('candles_1h',
  start_offset => INTERVAL '3 days',
  end_offset => INTERVAL '1 hour',
  schedule_interval => INTERVAL '15 minutes',
  if_not_exists => TRUE);