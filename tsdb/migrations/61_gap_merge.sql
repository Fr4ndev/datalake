-- Los gaps absorbidos por una fusion NO se borran: la regla es "no borrar filas, se cierran
-- con status". Para poder expresar "esta fila se fusiono con la #N" hace falta un estado mas y una
-- columna que apunte a la fila canonica. Sin esto, fusionar obligaba a DELETE y se perdia la
-- deteccion original (que es justo lo que hay que auditar).
BEGIN;

ALTER TABLE ingest_gaps DROP CONSTRAINT IF EXISTS ingest_gaps_status_check;
ALTER TABLE ingest_gaps ADD CONSTRAINT ingest_gaps_status_check
  CHECK (status IN ('open','repairing','repaired','partial','unrecoverable','merged'));

ALTER TABLE ingest_gaps ADD COLUMN IF NOT EXISTS merged_into BIGINT
  REFERENCES ingest_gaps(id) ON DELETE RESTRICT;

CREATE INDEX IF NOT EXISTS ingest_gaps_merged_idx
  ON ingest_gaps (merged_into) WHERE merged_into IS NOT NULL;

COMMIT;