-- requires-env: ENABLE_COMPRESSION=true
-- 50_compression.sql : compression de hypertables.
-- DESACTIVADA POR DEFECTO a proposito: durante el backfill historico ENABLE_COMPRESSION=false,
-- porque insertar con ON CONFLICT sobre chunks ya comprimidos es muy lento.
-- Orden correcto: cargar historico -> refresh caggs -> ENABLE_COMPRESSION=true -> migrate.

ALTER TABLE candles_1m SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'symbol, exchange',
  timescaledb.compress_orderby = 'open_time DESC'
);
SELECT add_compression_policy('candles_1m', INTERVAL '7 days', if_not_exists => TRUE);

ALTER TABLE open_interest SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'symbol, exchange',
  timescaledb.compress_orderby = 'ts DESC'
);
SELECT add_compression_policy('open_interest', INTERVAL '7 days', if_not_exists => TRUE);

ALTER TABLE liquidations SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'symbol, exchange, side',
  timescaledb.compress_orderby = 'ts DESC'
);
SELECT add_compression_policy('liquidations', INTERVAL '7 days', if_not_exists => TRUE);

-- funding: 30 dias (mucho menos volumen)
ALTER TABLE funding SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'symbol, exchange',
  timescaledb.compress_orderby = 'funding_time DESC'
);
SELECT add_compression_policy('funding', INTERVAL '30 days', if_not_exists => TRUE);

-- Retencion: SOLO trades tick (si se crea la hypertable en el futuro). Nunca raw OHLCV.
-- El OHLCV derivado se regenera siempre desde el lake Parquet.