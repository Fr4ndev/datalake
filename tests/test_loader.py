"""Tests del loader: mapeo lake->tsdb, idempotencia y, sobre todo, la regresion de UTC.

La regresion de UTC no es decorativa. Sin `TimeZone='UTC'` en la sesion de DuckDB, un filtro por
`year` o por dia sobre `TIMESTAMP WITH TIME ZONE` se evalua en `Europe/Madrid` (la zona del host en
este mini PC) y desplaza los datos de ano y de dia. El total de filas no cambia, asi que un
`count(*)` no lo detecta: lo unico que lo ve es comparar el mismo dato bucketeado de dos formas.
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from loader.loader import Loader, parquet_files
from loader.targets import CAGG_BY_TARGET, TARGETS, target_for

UTC = timezone.utc


# --------------------------------------------------------------------- mapeo
def test_cada_dtype_del_lake_tiene_tabla_destino():
    assert set(TARGETS) == {"klines", "funding", "metrics"}
    assert TARGETS["klines"].table == "candles_1m"
    assert TARGETS["funding"].table == "funding"
    assert TARGETS["metrics"].table == "open_interest"


def test_los_renombres_calc_time_y_create_time_van_a_su_columna():
    """El lake y tsdb no comparten nombres: sin renombrar, la carga no encaja."""
    funding = target_for("funding")
    assert ("calc_time", "funding_time") in funding.mapping
    assert ("last_funding_rate", "funding_rate") in funding.mapping

    metrics = target_for("metrics")
    assert ("create_time", "ts") in metrics.mapping
    assert ("sum_open_interest", "open_interest") in metrics.mapping
    assert ("sum_open_interest_value", "open_interest_value") in metrics.mapping


@pytest.mark.parametrize("dtype", sorted(TARGETS))
def test_columnas_y_tipos_estan_alineados(dtype):
    """Si mapping y pg_types se desincronizan, el INSERT escribe el precio en la columna de tiempo."""
    t = target_for(dtype)
    assert len(t.mapping) == len(t.pg_types)
    sql = t.insert_sql()
    assert sql.count("%s::") == len(t.pg_types)


def test_las_columnas_de_particion_no_se_cargan():
    """`tf` y `year` son metadatos de particionado del lake, no columnas de la serie."""
    for t in TARGETS.values():
        assert "tf" not in t.columns
        assert "year" not in t.columns


def test_open_interest_hace_upsert_y_el_resto_discard():
    """El daemon escribe OI sin `open_interest_value`; con DO NOTHING se quedaria a NULL siempre."""
    oi = target_for("metrics")
    assert "DO UPDATE" in oi.insert_sql()
    assert '"open_interest_value" = EXCLUDED."open_interest_value"' in oi.insert_sql()
    for dtype in ("klines", "funding"):
        assert "DO NOTHING" in target_for(dtype).insert_sql()
        assert "DO UPDATE" not in target_for(dtype).insert_sql()


def test_cada_tabla_tiene_cagg_asignada():
    for t in TARGETS.values():
        assert t.table in CAGG_BY_TARGET


# --------------------------------------------------------------------- lake
def test_los_ficheros_se_listan_por_anyo(tmp_path: Path):
    base = tmp_path / "binance" / "klines" / "symbol=BTCUSDT" / "tf=1m"
    for year in (2024, 2019, 2020):
        d = base / f"year={year}"
        d.mkdir(parents=True)
        (d / "part.parquet").write_bytes(b"x")
    found = parquet_files(tmp_path, "klines", "binance", "BTCUSDT")
    names = [p.parent.name for p in found]
    assert names == ["year=2019", "year=2020", "year=2024"], "deben ir en orden cronologico"


def test_sin_ficheros_devuelve_lista_vacia(tmp_path: Path):
    assert parquet_files(tmp_path, "klines", "binance", "NOPE") == []


def test_sin_ficheros_la_carga_no_falla_sino_que_lo_dice(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("LAKE_DIR", str(tmp_path))
    with Loader() as ld:
        res = ld.load("klines", symbol="NOPE")
    assert res.rows_sent == 0 and not res.errors


# --------------------------------------------------------------------- DuckDB UTC
def test_el_bucket_por_dia_y_ano_es_utc(tmp_path: Path, monkeypatch):
    """Regresion de la regla 3.bis, vista desde el loader. Esta es la que de verdad falla.

    Una fila a las 23:30 UTC del ultimo dia de 2019:
      - con `TimeZone='UTC'`            -> `year()` = 2019, `date_trunc('day')` = 2019-12-31 00:00
      - con `TimeZone='Europe/Madrid'`  -> `year()` = 2020, `date_trunc('day')` = 2020-01-01

    Es el caso que rompe el backfill diario: la fila del 31/12 se contabiliza el dia 1 de enero y
    el reparto diario queda descuadrado, sin que ningun `count(*)` lo detecte porque el total de
    filas no cambia.

    Nota sobre el filtro por rango: `calc_time < $until` compara instantes absolutos y es
    independiente de la zona de sesion, asi que **no** es lo que hay que probar aqui. Lo que hay
    que probar es el bucketing, que es donde la zona actua. Por eso este test usa `year()` y
    `date_trunc()` en vez de un simple `WHERE`.
    """
    import duckdb

    monkeypatch.setenv("LAKE_DIR", str(tmp_path))
    part = tmp_path / "binance" / "funding" / "symbol=BTCUSDT" / "tf=8h" / "year=2019"
    part.mkdir(parents=True)
    fila = (2019, 12, 31, 23, 30)
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE t(symbol VARCHAR, exchange VARCHAR, calc_time TIMESTAMPTZ, "
        "last_funding_rate DOUBLE)"
    )
    con.execute(
        "INSERT INTO t VALUES ('BTCUSDT','binance',TIMESTAMPTZ '2019-12-31 23:30:00+00', 0.1)"
    )
    con.execute("COPY t TO ? (FORMAT PARQUET)", [str(part / "part.parquet")])
    con.close()
    assert fila  # el dia es real: 2019-12-31 23:30 UTC existe

    with Loader() as ld:
        assert ld._duck.execute(
            "SELECT year(calc_time), date_trunc('day', calc_time) FROM "
            "read_parquet(?)", [str(part / "part.parquet")]
        ).fetchone() == (2019, datetime(2019, 12, 31, 0, 0, tzinfo=UTC))

        # Y el filtro por rango, que tambien tiene que respectar el rango pedido.
        res_2019 = ld.load("funding", symbol="BTCUSDT", until=datetime(2020, 1, 1, tzinfo=UTC))
        res_2020 = ld.load("funding", symbol="BTCUSDT", since=datetime(2020, 1, 1, tzinfo=UTC))

    assert res_2019.rows_read == 1, "la fila es de 2019-12-31 UTC"
    assert res_2020.rows_read == 0, "no hay nada de 2020 en adelante"


def test_la_conexion_duckdb_del_loader_es_utc():
    from common.db import duckdb_utc

    with Loader() as ld:
        zona = ld._duck.execute("SELECT current_setting('TimeZone')").fetchone()[0]
    assert zona == "UTC"


def test_la_conexion_postgres_del_loader_es_utc():
    """Si no hay DSN no se puede comprobar; el skip lo dice explicitamente."""
    if not (os.environ.get("MIGRATE_DSN") or os.environ.get("POSTGRES_PASSWORD")):
        pytest.skip("sin DSN: requiere POSTGRES_PASSWORD o MIGRATE_DSN")
    with Loader() as ld:
        # Postgres se abre perezoso (solo al escribir), asi que hay que pedirlo explicitamente.
        assert ld._ensure_pg().execute("SHOW TimeZone").fetchone()[0] == "UTC"


# --------------------------------------------------------------------- SQL
def test_el_insert_es_un_batched_insert_con_unnest_no_copy():
    """COPY no soporta ON CONFLICT. Es el mismo motivo que en el feed (ver writer.py)."""
    for dtype in TARGETS:
        sql = target_for(dtype).insert_sql()
        assert sql.startswith("INSERT INTO")
        assert "unnest(" in sql
        assert "ON CONFLICT" in sql
        assert "COPY" not in sql.upper()


def test_el_insert_proyecta_solo_las_columnas_del_mapeo():
    t = target_for("klines")
    sql = t.insert_sql()
    assert '"open_time"' in sql
    assert '"close_time"' not in sql, "close_time no va a la base: no esta en el mapeo"
    assert '"taker_buy_quote_volume"' not in sql


# --------------------------------------------------------------------- integracion
@pytest.mark.skipif(
    not os.environ.get("MIGRATE_DSN"),
    reason="integracion: requiere MIGRATE_DSN",
)
def test_carga_real_es_idempotente(tmp_path: Path):
    """Carga un dia real del lake dos veces: la segunda no debe anadir filas.

    El recuento se hace **solo dentro del rango cargado**. Con el daemon en marcha insertando
    velas del dia en curso, un `count(*)` de todo el simbolo crece por debajo y el test seria
    intermitente. Y la cuenta esperada es la del rango, no la de antes mas la insertada: el DELETE
    previo ya ha vaciado el rango, asi que al final hay exactamente un dia de velas.
    """
    from common.db import psycopg_utc
    from loader.loader import lake_dir

    symbol = "BTCUSDT"
    # `lake_dir()`, no una ruta relativa: dentro del contenedor el lake esta en `/data/lake` y
    # `Path("lake/...")` no existe, asi que el test se saltaba sin llegar a comprobar nada.
    src = lake_dir() / "binance" / "klines" / f"symbol={symbol}" / "tf=1m" / "year=2024" / "part.parquet"
    if not src.is_file():
        pytest.skip(f"el lake no tiene klines de 2024 en {lake_dir()}")
    since = datetime(2024, 1, 1, tzinfo=UTC)
    until = datetime(2024, 1, 2, tzinfo=UTC)
    rango = "symbol=%s AND open_time >= %s AND open_time < %s"

    with psycopg_utc() as pg:
        with pg.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM candles_1m WHERE {rango}", (symbol, since, until))
            antes = cur.fetchone()[0]
            cur.execute(f"DELETE FROM candles_1m WHERE {rango}", (symbol, since, until))
            pg.commit()

        with Loader() as ld:
            r1 = ld.load("klines", symbol=symbol, since=since, until=until, refresh=False)
            r2 = ld.load("klines", symbol=symbol, since=since, until=until, refresh=False)

        with pg.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM candles_1m WHERE {rango}", (symbol, since, until))
            total = cur.fetchone()[0]

    assert not r1.errors and not r2.errors
    assert antes >= 0
    # 1440 velas de 1m en un dia.
    assert r1.rows_inserted == 1440, f"esperaba 1440, insertadas {r1.rows_inserted}"
    # Este es el AC: la segunda carga del mismo rango no anade ninguna fila.
    assert r2.rows_inserted == 0, f"la recarga anadio {r2.rows_inserted} filas: no es idempotente"
    assert total == 1440, f"el rango deberia tener 1440 velas, tiene {total}"


@pytest.mark.skipif(
    not os.environ.get("MIGRATE_DSN"),
    reason="integracion: requiere MIGRATE_DSN",
)
def test_los_datos_cargados_conservan_el_instante_utc(tmp_path: Path):
    """Un ts escrito con la sesion en la zona local se desplazaria al releerlo."""
    from common.db import psycopg_utc

    symbol = "BTCUSDT"
    since = datetime(2024, 3, 1, tzinfo=UTC)
    until = datetime(2024, 3, 2, tzinfo=UTC)
    with psycopg_utc() as pg:
        with Loader() as ld:
            ld.load("klines", symbol=symbol, since=since, until=until, refresh=False)
        with pg.cursor() as cur:
            cur.execute(
                "SELECT open_time, (open_time AT TIME ZONE 'UTC') FROM candles_1m "
                "WHERE symbol=%s AND open_time >= %s AND open_time < %s "
                "ORDER BY open_time LIMIT 1",
                (symbol, since, until),
            )
            row = cur.fetchone()
    assert row is not None, "no se cargo nada del 2024-03-01"
    # Sin `ORDER BY` un `LIMIT 1` devuelve una fila arbitraria y la asercion no prueba nada.
    # Las dos columnas se comprueban por separado porque no son del mismo tipo: `open_time` es
    # TIMESTAMPTZ (aware) y `AT TIME ZONE 'UTC'` devuelve un timestamp naive con la hora de
    # pared en UTC. Compararlos entre si nunca daria True.
    assert row[0] == datetime(2024, 3, 1, 0, 0, tzinfo=UTC), "el instante no es medianoche UTC"
    assert row[1] == datetime(2024, 3, 1, 0, 0), (
        "la hora de pared en UTC no es medianoche: la fila esta desplazada"
    )