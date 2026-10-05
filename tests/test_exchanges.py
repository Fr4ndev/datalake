"""El canónico de exchange no puede volver a partirse en dos.

El fallo que esto previene no daba ningun error: `binance` (lake) y `BINANCE_FUTURES` (cryptofeed)
son dos valores de la PK, asi que el historico y el tiempo real se guardaban en series distintas,
el anti-join del reparador no encontraba lo que debia y las caggs contaban la mitad. Nada fallaba;
solo los datos estaban partidos.
"""

from __future__ import annotations

import pytest

from common.exchanges import (CANONICOS, SOLO_TESTS, canonico, es_canonico)

#: Todas las grafias que aparecen de verdad en el proyecto y en las librerias que usa.
VISTAS = {
    "binance": "binance_um",
    "BINANCE": "binance_um",
    "Binance": "binance_um",
    "BINANCE_FUTURES": "binance_um",
    "binance_futures": "binance_um",
    "BinanceFutures": "binance_um",
    "Binance-Futures-UM": "binance_um",
    "binanceum": "binance_um",
    "binance_um": "binance_um",
    "BINANCE_COINM_FUTURES": "binance_um",
    "BYBIT": "bybit",
    "bybit": "bybit",
    "OKX": "okx",
    "okx": "okx",
    "BITGET": "bitget",
    "bitget": "bitget",
    "HYPERLIQUID": "hyperliquid",
    "hyperliquid": "hyperliquid",
    "Hyperliquid": "hyperliquid",
}


@pytest.mark.parametrize("entrada,esperado", sorted(VISTAS.items()))
def test_todas_las_grafias_caen_en_el_mismo_canonico(entrada, esperado):
    assert canonico(entrada) == esperado


def test_canonico_es_idempotente():
    """Aplicarlo dos veces no cambia nada: es lo que permite canonizar en la entrada Y en el
    ledger sin que aparezca un valor intermedio raro."""
    for entrada in VISTAS:
        uno = canonico(entrada)
        assert canonico(uno) == uno


def test_los_canonicos_estan_en_la_lista():
    for c in CANONICOS:
        assert canonico(c) == c
        assert es_canonico(c)


def test_un_exchange_desconocido_lanza_en_vez_de_pasar():
    """El fallo original fue un `return str(nombre)` tolerante: por eso el valor desconocido
    llegaba hasta la PK. Aqui tiene que loudly fallar."""
    with pytest.raises(ValueError, match="exchange desconocido"):
        canonico("kraken")
    with pytest.raises(ValueError):
        canonico("")
    assert not es_canonico("kraken")


def test_los_exchanges_de_test_pasan_tal_cual():
    """Las fixtures aisan filas por exchange y no pueden depender del catalogo real."""
    for nombre in SOLO_TESTS.values():
        assert canonico(nombre) == nombre
        assert canonico(nombre.lower()) == nombre
    # Pero no son canonicos de verdad: no estan en la lista que exige la migracion 62.
    assert "TESTEX" not in CANONICOS
    assert not set(SOLO_TESTS.values()) <= set(CANONICOS)


# ====================================================================== verificacion en BD
import os  # noqa: E402

import pytest  # noqa: E402

DSN = os.environ.get("MIGRATE_DSN") or (
    f"postgresql://marketdata:{os.environ['POSTGRES_PASSWORD']}@tsdb:5432/marketdata"
    if os.environ.get("POSTGRES_PASSWORD") else ""
)
needs_db = pytest.mark.skipif(not DSN, reason="requiere MIGRATE_DSN o POSTGRES_PASSWORD")

TABLAS = ("trades", "candles_1m", "funding", "open_interest", "liquidations", "ingest_gaps")
#: `TESTEX`/`VISEX` son de los tests y la migracion 62 los tolera a proposito.
TOLERADOS = set(CANONICOS) | set(SOLO_TESTS.values())


@needs_db
def test_ninguna_tabla_tiene_un_exchange_fuera_del_canonico():
    """La consulta de verificacion de la migracion 62, como test.

    Sin esto, el siguiente sitio que escriba con el id de cryptofeed vuelve a partir las series
    y no hay nada que lo detecte hasta que alguien compara recuentos y no cuadran.
    """
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute("SET TIME ZONE 'UTC'")
        for tabla in TABLAS:
            rows = conn.execute(f"SELECT DISTINCT exchange FROM {tabla}").fetchall()
            malos = {r[0] for r in rows} - TOLERADOS
            assert not malos, f"{tabla} tiene {malos} fuera de {sorted(TOLERADOS)}"


@needs_db
def test_una_misma_clave_no_existe_bajo_dos_grafias_del_mismo_exchange():
    """El sintoma de la particion: el mismo dato bajo `binance` y bajo `binance_um`.

    No se comprueba "una clave, dos exchanges" a secas, porque eso es legitimo: Binance, Bybit,
    OKX y Bitget tienen velas abiertas a la misma hora y por el mismo simbolo, y sus `trade_id`
    pueden coincidir por casualidad. Lo que no puede existir son dos **grafias del mismo
    exchange**, y eso es justo lo que `test_ninguna_tabla_tiene_un_exchange_fuera_del_canonico`
    comprueba en cada tabla. Aqui se anade el otro lado: que la era del lake y la de la ingesta en
    vivo esten en la misma serie, que es el punto de canonicalizar.
    """
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute("SET TIME ZONE 'UTC'")
        # Velas de Binance de dos epocas distintas: el lake (2024) y el daemon (hoy). Con el lake
        # en `binance` y el daemon en `binance_um` esto daria 2 exchanges distintos; canonicalizado,
        # tiene que ser la MISMA serie con cobertura continua.
        n = conn.execute(
            "SELECT count(DISTINCT exchange) FROM candles_1m "
            "WHERE symbol = 'BTCUSDT' AND exchange = 'binance_um' "
            "  AND open_time < '2025-01-01'::timestamptz").fetchone()[0]
        assert n == 1, "las velas del lake de Binance no estan bajo el canonico"
        n = conn.execute(
            "SELECT count(DISTINCT exchange) FROM candles_1m "
            "WHERE symbol = 'BTCUSDT' AND open_time >= '2026-01-01'::timestamptz "
            "  AND open_time < '2026-10-01'::timestamptz").fetchone()[0]
        assert n == 1, "la ingesta en vivo de Binance no esta bajo el canonico"
        # Y que la serie no tenga un corte en el punto donde el lake acaba y el daemon empieza.
        n = conn.execute(
            "SELECT count(*) FROM candles_1m WHERE symbol = 'BTCUSDT' "
            "  AND exchange = 'binance_um' AND open_time = '2026-09-30 23:59:00+00'::timestamptz"
        ).fetchone()[0]
        assert n == 1, "falta la ultima vela antes del cambio lake -> daemon"


@needs_db
def test_el_daemon_escribe_nombres_canonicos():
    """El daemon es la unica fuente que aun puede colarse, porque su exchange viene de cryptofeed."""
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute("SET TIME ZONE 'UTC'")
        n = conn.execute(
            "SELECT count(*) FROM trades WHERE ts > now() - interval '10 minutes' "
            "AND exchange <> ALL(%s)", (sorted(TOLERADOS),)).fetchone()[0]
        assert n == 0, f"{n} trades recientes con exchange no canonico"
