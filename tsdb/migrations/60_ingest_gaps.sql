-- 60_ingest_gaps.sql : ledger de huecos de ingesta + procedencia de cada trade
--
-- Motivacion (skill ingest-gap-repair): el WebSocket NO reenvia lo perdido. La garantia que
-- podemos dar no es "0 perdidas" sino "0 perdidas en silencio": todo hueco se registra, se
-- repara con la mejor fuente disponible o se declara irrecuperable con su motivo.
--
-- `ingest_gaps` es una tabla NORMAL, no hypertable: son pocas filas (decenas por semana) y se
-- consultan por estado, no por rango temporal. Una hypertable aqui solo complicaria los
-- updates de estado.
--
-- Las filas NUNCA se borran (regla 15 de AGENTS.md): un gap se cierra cambiando `status`. Asi el
-- historico de "que se perdio y por que" queda para auditoria.

CREATE TABLE IF NOT EXISTS ingest_gaps (
  id            BIGSERIAL PRIMARY KEY,
  exchange      TEXT NOT NULL,
  symbol        TEXT NOT NULL,
  dtype         TEXT NOT NULL CHECK (dtype IN ('trades','candles','funding','open_interest','liquidations')),
  gap_from      TIMESTAMPTZ NOT NULL,
  gap_to        TIMESTAMPTZ NOT NULL,
  reason        TEXT NOT NULL CHECK (reason IN ('disconnect','restart','silence','id_jump')),
  status        TEXT NOT NULL DEFAULT 'open'
                  CHECK (status IN ('open','repairing','repaired','partial','unrecoverable')),
  source        TEXT CHECK (source IN ('ws','rest','dump')),
  rows_repaired INT NOT NULL DEFAULT 0,
  attempts      INT NOT NULL DEFAULT 0,
  detected_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  note          TEXT,
  -- Un hueco con padding negativo no tiene sentido y un 'to' anterior al 'from' es un bug.
  CHECK (gap_to >= gap_from)
);

-- El worker busca por status; este es su indice hot.
CREATE INDEX IF NOT EXISTS ingest_gaps_status_idx ON ingest_gaps (status, detected_at);

-- Para el validator: "gaps abiertos de mas de 1 h" (AC 8 de la skill).
CREATE INDEX IF NOT EXISTS ingest_gaps_open_idx ON ingest_gaps (status) WHERE status IN ('open','repairing');

-- Deteccion de huecos solapados: se fusionan los del mismo (exchange, symbol, dtype).
CREATE INDEX IF NOT EXISTS ingest_gaps_range_idx ON ingest_gaps (exchange, symbol, dtype, gap_from, gap_to);


-- Procedencia de cada trade: 'ws' en vivo, 'rest' reparado por API, 'dump' reparado por volcado.
--
-- `NOT NULL DEFAULT 'ws'` con DEFAULT constante: en PG11+ es metadato only, no reescribe la tabla.
-- Las 3M+ filas de historico que ya hay en otras tablas no se ven afectadas; aqui son ~30k.
ALTER TABLE trades ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'ws';

-- Para el anti-join del volcado de Bybit.
--
-- Por que NO se puede confiar solo en la PK para eso: el volcado de Bybit trae `timestamp` en
-- SEGUNDOS con 4 decimales y el 4o decimal esta distribuido de forma uniforme entre 0 y 9
-- (medido sobre 60.000 lineas), o sea que Bybit guarda mas precision que el milisegundo que
-- expone por WS y REST. Redondear el ts del volcado puede dar un ms distinto del que ya hay en
-- la tabla, y entonces la PK `(symbol, exchange, ts, trade_id)` NO lo reconoce como duplicado y
-- lo inserta otra vez. Por eso el volcado se deduplica por `trade_id` de forma explica, con este
-- indice acotado a (exchange, symbol) y a la ventana del volcado para que Timescale pode chunks.
--
-- Es NO unico a proposito: en una hypertable un indice unico tendria que incluir `ts`, que es
-- justo la columna que no coincide.
CREATE INDEX IF NOT EXISTS trades_ex_sym_tradeid_idx ON trades (exchange, symbol, trade_id);

-- `updated_at` se mantiene desde la aplicacion (el worker), pero un trigger lo hace fiable
-- aunque alguien actualice la fila a mano.
CREATE OR REPLACE FUNCTION ingest_gaps_touch_updated_at() RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS ingest_gaps_touch ON ingest_gaps;
CREATE TRIGGER ingest_gaps_touch BEFORE UPDATE ON ingest_gaps
  FOR EACH ROW EXECUTE FUNCTION ingest_gaps_touch_updated_at();