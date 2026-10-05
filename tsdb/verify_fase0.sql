-- verify_fase0.sql : comprobacion manual de la Fase 0 (AC de la skill timescaledb-schema-crypto-futures).
-- Ejecutar dos veces followed: la segunda debe dar los mismos conteos (idempotencia).
--   docker exec -i cmd_tsdb psql -U marketdata -d marketdata < tsdb/verify_fase0.sql
--
-- Usa ts del 2026-10-01: fuera de la ventana materializada de la policy (3 dias), asi que
-- cualquier fila que aparezca en candles_1h procede del refresh manual.

\set ON_ERROR_STOP on
\timing off

BEGIN;

-- 3 velas de 1m con OHLC coherente (low <= open/close <= high) y volumen > 0.
INSERT INTO candles_1m (symbol, exchange, open_time, open, high, low, close, volume, quote_volume, trades, taker_buy_volume)
VALUES
  ('BTCUSDT', 'binance', '2026-10-01 00:00:00+00', 60000.0, 60100.0, 59900.0, 60050.0, 12.5, 750000.0, 310, 6.1),
  ('BTCUSDT', 'binance', '2026-10-01 00:01:00+00', 60050.0, 60250.0, 60000.0, 60200.0, 10.0, 602000.0, 280, 5.4),
  ('BTCUSDT', 'binance', '2026-10-01 00:02:00+00', 60200.0, 60280.0, 60100.0, 60150.0, 11.5, 691500.0, 295, 6.0)
ON CONFLICT (symbol, exchange, open_time) DO NOTHING;

-- Una fila de funding y otra de open_interest para cubrir las otras dos hypertables.
INSERT INTO funding (symbol, exchange, funding_time, funding_rate, mark_price)
VALUES ('BTCUSDT', 'binance', '2026-10-01 00:00:00+00', 0.0001, 60100.0)
ON CONFLICT (symbol, exchange, funding_time) DO NOTHING;

INSERT INTO open_interest (symbol, exchange, ts, open_interest, open_interest_value)
VALUES ('BTCUSDT', 'binance', '2026-10-01 00:00:00+00', 15000.0, 901500000.0)
ON CONFLICT (symbol, exchange, ts) DO NOTHING;

INSERT INTO liquidations (symbol, exchange, ts, side, price, quantity, notional)
VALUES ('BTCUSDT', 'binance', '2026-10-01 00:01:00+00', 'sell', 60100.0, 3.0, 180300.0)
ON CONFLICT (symbol, exchange, ts, side) DO NOTHING;

COMMIT;

-- Refresh manual: obligatorio tras cualquier carga masiva (la policy solo cubre 3 dias).
CALL refresh_continuous_aggregate('candles_1h', '2026-09-30 23:00:00+00', '2026-10-01 02:00:00+00');
CALL refresh_continuous_aggregate('funding_daily', '2026-09-30 00:00:00+00', '2026-10-02 00:00:00+00');
CALL refresh_continuous_aggregate('oi_5m',      '2026-10-01 00:00:00+00', '2026-10-01 01:00:00+00');
CALL refresh_continuous_aggregate('liq_1h',     '2026-10-01 00:00:00+00', '2026-10-01 02:00:00+00');

-- ---------------------------------------------------------------------------
-- Verificacion
-- ---------------------------------------------------------------------------
\echo '--- 1) Filas en las 4 hypertables (deben ser 3/1/1/1 y NO crecer al reejecutar) ---'
SELECT 'candles_1m' AS tbl, count(*) AS rows FROM candles_1m
UNION ALL SELECT 'funding', count(*) FROM funding
UNION ALL SELECT 'open_interest', count(*) FROM open_interest
UNION ALL SELECT 'liquidations', count(*) FROM liquidations;

\echo '--- 2) Sin duplicados: count(*) = count(DISTINCT clave) ---'
SELECT count(*) AS rows,
       count(DISTINCT (symbol, exchange, open_time)) AS distinct_keys
FROM candles_1m;

\echo '--- 3) candles_1h refleja los datos: open de la 1a vela, close de la ultima, volumen summed ---'
SELECT symbol, exchange, bucket, open, high, low, close, volume, quote_volume, n_1m
FROM candles_1h
WHERE symbol = 'BTCUSDT' AND bucket = '2026-10-01 00:00:00+00'
ORDER BY bucket;

\echo '--- 4) Los otros 3 caggs tambien reflejan sus datos ---'
SELECT symbol, exchange, bucket, avg_rate, n FROM funding_daily WHERE symbol = 'BTCUSDT' ORDER BY bucket;
SELECT symbol, exchange, bucket, avg_oi, n FROM oi_5m WHERE symbol = 'BTCUSDT' ORDER BY bucket LIMIT 1;
SELECT symbol, exchange, bucket, side, qty, notional, n FROM liq_1h WHERE symbol = 'BTCUSDT' ORDER BY bucket, side;

\echo '--- 5) Cuadre cagg vs base: 1h debe sumar exactamente las 3 velas de la hora ---'
SELECT (SELECT sum(volume) FROM candles_1m
         WHERE symbol='BTCUSDT' AND open_time >= '2026-10-01 00:00:00+00'
           AND open_time <  '2026-10-01 01:00:00+00') AS base_volume,
       (SELECT volume FROM candles_1h
         WHERE symbol='BTCUSDT' AND bucket = '2026-10-01 00:00:00+00') AS cagg_volume;

\echo '--- 6) Timestamps en UTC (regla 3): el bucket debe venir con +00 ---'
SHOW timezone;