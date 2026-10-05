#!/usr/bin/env python3
"""Runner de migraciones de TimescaleDB (stack cripto-marketdata).

Por que existe y no docker-entrypoint-initdb.d:
initdb solo corre con el volumen vacio, asi que no sirve para evolucionar el
schema. Este runner aplica los ficheros tsdb/migrations/NN_*.sql en orden y
registra cada uno en schema_migrations, de modo que re-ejecutarlo no aplica nada.

Invariantes:
- Cada fichero se aplica en su propia transaccion (salvo los marcados `-- no-transaction`,
  que se ejecutan con autocommit porque CREATE MATERIALIZEDVIEW ... WITH NO DATA
  no puede ir dentro de una transaccion).
- Idempotente: los ficheros usan IF NOT EXISTS / if_not_exists => TRUE, y el
  registro en schema_migrations evita reaplicar lo ya aplicado.
- Los ficheros con `-- requires-env: NOMBRE=valor` se saltan (status=skipped) si
  el entorno no cumple. Asi 50_compression.sql queda desactivado mientras
  ENABLE_COMPRESSION=false durante el backfill historico.

Uso:
    python tsdb/migrate.py                # aplica pendientes
    python tsdb/migrate.py --status       # solo lista el estado
    python tsdb/migrate.py --dry-run     # muestra que se aplicaria, sin tocar la DB

Conexion: MIGRATE_DSN o DATABASE_URL; si no hay ninguna, psycopg lee las
variables estándar PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import sys
import time
from pathlib import Path

import psycopg

MIGRATIONS_DIR = Path(__file__).resolve().parent / "migrations"

DIRECTIVE_RE = re.compile(
    r"^--\s*(?:migrate:\s*)?(no-transaction|requires-env:\s*\S+)\s*$",
    re.IGNORECASE,
)
TRUTHY = {"1", "true", "yes", "on"}
ADVISORY_LOCK_KEY = "cripto-marketdata-migrate"

SCHEMA_MIGRATIONS_DDL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
  version    TEXT PRIMARY KEY,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""


def log(**fields: object) -> None:
    """Log en stdout en formato clave=valor (regla 9 de AGENTS.md)."""
    print(" ".join(f"{k}={v}" for k, v in fields.items()), flush=True)


def conninfo() -> str:
    return os.environ.get("MIGRATE_DSN") or os.environ.get("DATABASE_URL") or ""


def connect(retries: int = 30, delay: float = 2.0):
    """Conecta esperando a que la DB acepte conexiones (tsdb puede ir arrancando)."""
    last: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            return psycopg.connect(conninfo(), autocommit=True)
        except psycopg.OperationalError as exc:
            last = exc
            log(component="migrate", event="connect_retry", attempt=attempt, error=str(exc)[:120])
            time.sleep(delay)
    raise SystemExit(f"no se pudo conectar a la DB tras {retries} intentos: {last}")


def parse_directives(sql: str) -> tuple[bool, tuple[str, str] | None]:
    """Extrae las directivas de migracion del propio fichero SQL.

    Formatos aceptados (comentario de una linea):
        -- no-transaction
        -- requires-env: ENABLE_COMPRESSION=true
    """
    no_transaction = False
    required_env: tuple[str, str] | None = None
    for raw in sql.splitlines():
        match = DIRECTIVE_RE.match(raw.strip())
        if not match:
            continue
        token = match.group(1)
        if token.lower() == "no-transaction":
            no_transaction = True
        else:
            name, _, expected = token.split(":", 1)[1].strip().partition("=")
            required_env = (name.strip(), expected.strip())
    return no_transaction, required_env


def env_satisfied(required: tuple[str, str] | None) -> bool:
    if required is None:
        return True
    name, expected = required
    actual = os.environ.get(name, "")
    if expected.lower() in TRUTHY:
        return actual.strip().lower() in TRUTHY
    return actual.strip() == expected


def migration_files(directory: Path) -> list[Path]:
    files = sorted(p for p in directory.glob("*.sql") if p.is_file())
    return files


def applied_versions(conn) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(SCHEMA_MIGRATIONS_DDL)
        cur.execute("SELECT version FROM schema_migrations")
        return {row[0] for row in cur.fetchall()}


def apply_one(conn, path: Path, sql: str, no_transaction: bool) -> float:
    """Aplica un fichero y registra la version. Devuelve los segundos que tardo."""
    started = time.monotonic()
    if no_transaction:
        # Autocommit: sin bloque transaccional (caggs).
        conn.execute(sql)
    else:
        with conn.transaction():
            conn.execute(sql)
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO schema_migrations (version, applied_at) "
            "VALUES (%s, now()) ON CONFLICT (version) DO NOTHING",
            (path.name,),
        )
    return time.monotonic() - started


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Aplica las migraciones de TimescaleDB")
    parser.add_argument("--status", action="store_true", help="solo listar aplicado/pendiente")
    parser.add_argument("--dry-run", action="store_true", help="no escribir nada en la DB")
    parser.add_argument("--dir", default=str(MIGRATIONS_DIR), help="directorio de migraciones")
    args = parser.parse_args(argv)

    directory = Path(args.dir)
    files = migration_files(directory)
    if not files:
        log(component="migrate", event="no_files", dir=str(directory))
        return 0

    conn = connect()
    try:
        # Serializa ejecuciones concurrentes (idempotencia, regla 7).
        conn.execute("SELECT pg_advisory_lock(hashtext(%s))", (ADVISORY_LOCK_KEY,))
        already = applied_versions(conn)
        applied = skipped = failed = 0

        for path in files:
            sql = path.read_text(encoding="utf-8")
            no_transaction, required_env = parse_directives(sql)
            digest = hashlib.sha256(sql.encode("utf-8")).hexdigest()[:12]

            if path.name in already:
                log(component="migrate", migration=path.name, status="already_applied", sha=digest)
                continue

            if not env_satisfied(required_env):
                need = f"{required_env[0]}={required_env[1]}" if required_env else "-"
                log(
                    component="migrate",
                    migration=path.name,
                    status="skipped",
                    reason="requires_env",
                    need=need,
                )
                skipped += 1
                continue

            if args.status or args.dry_run:
                log(
                    component="migrate",
                    migration=path.name,
                    status="would_apply",
                    transaction="none" if no_transaction else "per_file",
                    sha=digest,
                )
                continue

            try:
                elapsed = apply_one(conn, path, sql, no_transaction)
            except Exception as exc:  # noqa: BLE001 - un fallo no debe tapar el resto
                log(
                    component="migrate",
                    migration=path.name,
                    status="failed",
                    error=f"{type(exc).__name__}: {exc}"[:300],
                )
                failed += 1
                continue

            applied += 1
            log(
                component="migrate",
                migration=path.name,
                status="applied",
                transaction="none" if no_transaction else "per_file",
                sha=digest,
                elapsed=f"{elapsed:.3f}",
            )
    finally:
        conn.close()

    log(
        component="migrate",
        event="summary",
        applied=applied,
        skipped=skipped,
        failed=failed,
        total=len(files),
    )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())