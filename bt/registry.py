"""Registro de ensayos en TimescaleDB. Se guardan TODOS, tambien los malos.

Conexión con TimeZone='UTC' en las opciones de sesión (regla 3.bis): si no, `created_at`
sale en zona local del cliente.
"""
from __future__ import annotations

import json
import os

import psycopg

def _dsn() -> str:
    if os.environ.get("TIMESCALE_DSN"):
        return os.environ["TIMESCALE_DSN"]
    env: dict[str, str] = {}
    ruta_env = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
    if os.path.exists(ruta_env):
        with open(ruta_env) as fh:
            for linea in fh:
                linea = linea.strip()
                if linea and not linea.startswith("#") and "=" in linea:
                    k, v = linea.split("=", 1)
                    env[k.strip()] = v.strip().strip('"').strip("'")
    user = env.get("POSTGRES_USER", "marketdata")
    pwd = env.get("POSTGRES_PASSWORD", "marketdata")
    db = env.get("POSTGRES_DB", "marketdata")
    return f"host={env.get('TIMESCALE_HOST', 'tsdb')} port=5432 dbname={db} user={user} password={pwd}"


def _con():
    return psycopg.connect(_dsn(), options="-c timezone=UTC")


def registrar(fila: dict) -> None:
    with _con() as cn, cn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO bt_runs (run_id, family_id, spec_hash, spec_json, hypothesis,
                                 data_snapshot, git_sha, seed, split, periodo, params,
                                 metrics, n_trades, veredicto)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (run_id) DO UPDATE SET
              metrics   = EXCLUDED.metrics,
              n_trades  = EXCLUDED.n_trades,
              veredicto = EXCLUDED.veredicto
            """,
            (fila["run_id"], fila["family_id"], fila["spec_hash"],
             json.dumps(fila["spec_json"]), fila["hypothesis"], fila["data_snapshot"],
             fila["git_sha"], int(fila["seed"]), fila["split"], fila["periodo"],
             json.dumps(fila["params"]), json.dumps(fila["metrics"]),
             int(fila["n_trades"]), fila["veredicto"]))
        cn.commit()


def obtener(run_id: str) -> dict | None:
    with _con() as cn, cn.cursor() as cur:
        cur.execute(
            "SELECT run_id, family_id, spec_hash, spec_json, hypothesis, data_snapshot, "
            "git_sha, seed, split, periodo, params, metrics, n_trades, veredicto "
            "FROM bt_runs WHERE run_id = %s", (run_id,))
        fila = cur.fetchone()
    if fila is None:
        return None
    claves = ("run_id", "family_id", "spec_hash", "spec_json", "hypothesis", "data_snapshot",
              "git_sha", "seed", "split", "periodo", "params", "metrics", "n_trades", "veredicto")
    out = dict(zip(claves, fila))
    # psycopg deserializa json/jsonb solo: si viene str (columna declarada como texto) se parsea
    out["spec_json"] = _json(out["spec_json"])
    out["params"] = _json(out["params"])
    out["metrics"] = _json(out["metrics"])
    return out


def _json(v):
    if isinstance(v, (str, bytes, bytearray)):
        return json.loads(v)
    if isinstance(v, dict):
        return v
    return json.loads(json.dumps(v))


def finals_registrados(spec_hash: str) -> int:
    """Numero de `--final` ya consumidos para este spec: un disparo, no mas."""
    with _con() as cn, cn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM bt_runs WHERE spec_hash=%s AND split='holdout'",
            (spec_hash,))
        n = cur.fetchone()[0]
    return int(n)


def n_trials_familia(family_id: str, exclude_run: str | None = None) -> int:
    with _con() as cn, cn.cursor() as cur:
        if exclude_run:
            cur.execute(
                "SELECT count(*) FROM bt_runs WHERE family_id=%s AND run_id<>%s",
                (family_id, exclude_run))
        else:
            cur.execute("SELECT count(*) FROM bt_runs WHERE family_id=%s", (family_id,))
        n = cur.fetchone()[0]
    return int(n)
