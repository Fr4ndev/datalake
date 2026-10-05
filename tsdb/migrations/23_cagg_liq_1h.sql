-- no-transaction
-- 23_cagg_liq_1h.sql : liquidaciones agregadas por hora y lado.
-- GROUP BY side: una vela por (bucket, side). El signo del notional se usa para validar el sentido.
CREATE MATERIALIZED VIEW IF NOT EXISTS liq_1h
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT symbol,
       exchange,
       time_bucket('1 hour', ts) AS bucket,
       side,
       sum(quantity) AS qty,
       sum(notional) AS notional,
       count(*)      AS n
FROM liquidations
GROUP BY 1, 2, 3, 4
WITH NO DATA;

SELECT add_continuous_aggregate_policy('liq_1h',
  start_offset => INTERVAL '3 days',
  end_offset => INTERVAL '1 hour',
  schedule_interval => INTERVAL '15 minutes',
  if_not_exists => TRUE);