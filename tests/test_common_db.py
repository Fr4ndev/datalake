"""Tests de conexion: el contrato es "UTC o error", nunca "UTC si de casual"."""

from __future__ import annotations

import duckdb
import pytest

from common import db


def test_duckdb_utc_fuerza_utc():
    con = db.duckdb_utc()
    assert con.execute("SELECT current_setting('TimeZone')").fetchone()[0] == "UTC"


def test_year_no_se_desplaza_entre_anos():
    """Regresion de la regla 3.bis.

    23:30 UTC del 31/12/2019 es 00:30 del 01/01/2020 en Madrid. Si la sesion hereda la zona del
    host, `year()` devuelve 2020 y la fila se contabiliza en el ano equivocado.
    """
    con = db.duckdb_utc()
    ts = "2019-12-31 23:30:00+00"
    assert con.execute(f"SELECT year(TIMESTAMPTZ '{ts}')").fetchone()[0] == 2019
    assert con.execute(f"SELECT date_trunc('day', TIMESTAMPTZ '{ts}')::DATE").fetchone()[0].isoformat() == "2019-12-31"


def test_la_conexion_sin_fijar_utc_falla_el_test():
    """Demuestra que el test anterior detecta el bug: esta conexion NO fija UTC."""
    con = duckdb.connect()
    con.execute("SET TimeZone='Europe/Madrid'")
    ts = "2019-12-31 23:30:00+00"
    # Con zona local el mismo instante cae en el ano que NO es: por eso el test de arriba importa.
    assert con.execute(f"SELECT year(TIMESTAMPTZ '{ts}')").fetchone()[0] == 2020
    with pytest.raises(RuntimeError, match="UTC"):
        db.assert_session_is_utc(con)


def test_assert_session_is_utc_pasa_con_nuestra_conexion():
    db.assert_session_is_utc(db.duckdb_utc())


def test_conninfo_acepta_las_tres_formas(monkeypatch):
    monkeypatch.setenv("MIGRATE_DSN", "host=a")
    assert db.conninfo() == "host=a"
    monkeypatch.delenv("MIGRATE_DSN")
    monkeypatch.setenv("DATABASE_URL", "host=b")
    assert db.conninfo() == "host=b"
    monkeypatch.delenv("DATABASE_URL")
    monkeypatch.setenv("POSTGRES_USER", "u")
    monkeypatch.setenv("POSTGRES_PASSWORD", "p")
    monkeypatch.setenv("POSTGRES_DB", "d")
    monkeypatch.setenv("POSTGRES_HOST", "h")
    monkeypatch.setenv("POSTGRES_PORT", "5432")
    assert db.conninfo() == "host=h port=5432 user=u password=p dbname=d"


def test_conninfo_sin_dsn_falla_con_mensaje_util(monkeypatch):
    for var in ("MIGRATE_DSN", "DATABASE_URL", "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB"):
        monkeypatch.delenv(var, raising=False)
    with pytest.raises(RuntimeError, match="falta DSN"):
        db.conninfo()


def test_postgres_utc_es_opcion_de_servidor_no_un_set():
    """`options=` debe ir en el connect(), no como SET aparte: asi aucune consulta previa puede
    ejecutarse antes con otra zona."""
    assert "timezone=UTC" in db.POSTGRES_UTC_OPTIONS


@pytest.mark.skipif(
    not __import__("os").environ.get("MIGRATE_DSN"),
    reason="requiere MIGRATE_DSN (contenedor: docker-compose run --rm migrate)",
)
def test_postgres_real_responde_en_utc():
    """Contra la DB real: `SHOW TimeZone` y el mismo caso de frontera de ano que DuckDB."""
    con = db.psycopg_utc()
    db.assert_session_is_utc(con)
    assert con.execute("SHOW TimeZone").fetchone()[0] == "UTC"
    assert con.execute(
        "SELECT extract(year FROM TIMESTAMPTZ '2019-12-31 23:30:00+00')"
    ).fetchone()[0] == 2019
    con.close()