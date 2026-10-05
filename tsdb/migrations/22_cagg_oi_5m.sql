-- no-transaction
-- 22_cagg_oi_5m.sql : open interest agregado a 5m
CREATE MATERIALIZED VIEW IF NOT EXISTS oi_5m
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT symbol,
       exchange,
       time_bucket('5 minutes', ts) AS bucket,
       avg(open_interest) AS avg_oi,
       max(open_interest) AS max_oi,
       min(open_interest) AS min_oi,
       count(*)           AS n
FROM open_interest
GROUP BY 1, 2, 3
WITH NO DATA;

SELECT add_continuous_aggregate_policy('oi_5m',
  start_offset => INTERVAL '3 days',
  end_offset => INTERVAL '5 minutes',
  schedule_interval => INTERVAL '15 minutes',
  if_not_exists => TRUE);