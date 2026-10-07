BEGIN;
CREATE TABLE IF NOT EXISTS bt_runs (
  run_id        TEXT PRIMARY KEY,
  family_id     TEXT NOT NULL,
  spec_hash     TEXT NOT NULL,
  spec_json     JSONB NOT NULL,
  hypothesis    TEXT NOT NULL,
  data_snapshot TEXT NOT NULL,
  git_sha       TEXT NOT NULL,
  seed          BIGINT NOT NULL,
  split         TEXT NOT NULL,
  periodo       TEXT NOT NULL,
  params        JSONB NOT NULL,
  metrics       JSONB NOT NULL,
  n_trades      INT NOT NULL,
  veredicto     TEXT NOT NULL CHECK (veredicto IN ('REJECT','INCONCLUSIVE','CANDIDATE','CONFIRMED')),
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS bt_runs_family_idx ON bt_runs(family_id);
CREATE INDEX IF NOT EXISTS bt_runs_created_idx ON bt_runs(created_at);
COMMIT;
