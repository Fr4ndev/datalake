"""Apertura de conexiones a DuckDB y Postgres con la zona horaria fijada a UTC.

Por que este modulo existe (regla 3.bis de AGENTS.md):

`year()`, `date_trunc()`, `date_part()` y cualquier bucketing sobre un `TIMESTAMP WITH TIME
ZONE` se evaluan en la **zona de la sesion**, no en UTC. Si la sesion hereda la zona del host, en
este mini PC `Europe/Madrid` (UTC+1/+2), los datos se desplazan de ano y de dia:

    -- con TimeZone='UTC'      2019-12-31 23:30 UTC  ->  year() = 2019
    -- con TimeZone='Europe/Madrid'                 ->  year() = 2020

El desplazamiento es de horas, no de meses, asi que **el total de filas no cambia** y el error no
se ve en un `count(*)`. Solo aparece al comparar por ano/dia, que es justo donde se decide si un
backfill esta bien. Por eso `gapscan` forceja UTC en su conexion y por eso hay un test de regresion
obligatorio en `tests/test_common_db.py`.
"""

from __future__ import annotations

import os

import duckdb

UTC_SESSION_SETUP = "SET TimeZone='UTC'"

#: Con `options=` el servidor fija la zona ANTES de correr la primera consulta, que es lo que
#: queremos: si se hiciera con un `SET` aparte, una consulta previa con otra zona ya habria
#:ucketado mal.
POSTGRES_UTC_OPTIONS = "-c timezone=UTC"


def duckdb_utc(database: str = ":memory:"):
    """DuckDB con `TimeZone='UTC'` fijo para toda la sesion.

    Falla ruidosamente si la sentencia no surte efecto, en vez de dejar la conexion en la zona que
    toque: un fallback silencioso aqui seria exactamente el bug que este modulo previene.
    """
    con = duckdb.connect(database)
    con.execute(UTC_SESSION_SETUP)
    actual = con.execute("SELECT current_setting('TimeZone')").fetchone()[0]
    if actual != "UTC":
        con.close()
        raise RuntimeError(
            f"no se pudo fijar TimeZone='UTC' en DuckDB (quedo en {actual!r}); "
            "los buckets por ano/dia serian incorrectos"
        )
    return con


def conninfo() -> str:
    """DSN de Postgres desde `MIGRATE_DSN`, `DATABASE_URL` o las piezas de `POSTGRES_*`."""
    dsn = os.environ.get("MIGRATE_DSN") or os.environ.get("DATABASE_URL")
    if dsn:
        return dsn
    host = os.environ.get("POSTGRES_HOST", "tsdb")
    port = os.environ.get("POSTGRES_PORT", "5432")
    user = os.environ.get("POSTGRES_USER")
    password = os.environ.get("POSTGRES_PASSWORD")
    dbname = os.environ.get("POSTGRES_DB")
    if not (user and password and dbname):
        raise RuntimeError(
            "falta DSN: define MIGRATE_DSN o DATABASE_URL, o POSTGRES_USER/POSTGRES_PASSWORD/POSTGRES_DB"
        )
    return f"host={host} port={port} user={user} password={password} dbname={dbname}"


def psycopg_utc(autocommit: bool = True, **kwargs):
    """Postgres con `timezone=UTC` aplicado por el servidor al arrancar la sesion."""
    import psycopg

    return psycopg.connect(
        conninfo(),
        autocommit=autocommit,
        options=POSTGRES_UTC_OPTIONS,
        **kwargs,
    )


def assert_session_is_utc(con) -> None:
    """Falla si la sesion no esta en UTC. Se llama tras abrir, antes de la primera consulta real."""
    if isinstance(con, duckdb.DuckDBPyConnection):
        actual = con.execute("SELECT current_setting('TimeZone')").fetchone()[0]
    else:
        actual = con.execute("SHOW TimeZone").fetchone()[0]
    if actual != "UTC":
        raise RuntimeError(
            f"la sesion de la DB no esta en UTC (TimeZone={actual!r}); "
            "los buckets por ano/dia seria incorrectos"
        )