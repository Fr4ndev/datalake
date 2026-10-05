-- no-transaction
-- 21_cagg_funding_daily.sql : funding agregado por día (avg/sum/count)
CREATE MATERIALIZED VIEW IF NOT EXISTS funding_daily
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT symbol,
       exchange,
       time_bucket('1 day', funding_time) AS bucket,
       avg(funding_rate)  AS avg_rate,
       sum(funding_rate)  AS sum_rate,
       max(funding_rate)  AS max_rate,
       min(funding_rate)  AS min_rate,
       count(*)           AS n
FROM funding
GROUP BY 1, 2, 3
WITH NO DATA;

SELECT add_continuous_aggregate_policy('funding_daily',
  start_offset => INTERVAL '30 days',
  end_offset => INTERVAL '1 day',
  schedule_interval => INTERVAL '1 hour',
  if_not_exists => TRUE);