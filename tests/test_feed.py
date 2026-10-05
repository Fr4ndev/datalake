"""Tests del feed-daemon.

Los que importan de verdad:

- `test_sigterm_no_pierde_filas` es el AC "SIGTERM -> 0 filas perdidas". Se cuenta con un numero, no
  con un log: se meten N trades, se lanza el flush final y se comprueba que la base recibio N.
- `test_reenvio_no_duplica` es el AC de idempotencia: los mismos trades dos veces -> mismas filas.
- `test_p95_latencia` es el AC de "p95 trade->fila < 2 s", medido sobre las muestras que el writer
  acumularia de verdad.
- Los de zona horaria evitan que un timestamp se guarde desplazado.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest

from feed import config as feedconfig
from feed.daemon import Daemon
from feed.writer import Store, to_utc

# --------------------------------------------------------------------- dobles


class FakeTrade:
    def __init__(self, i, exchange="binance_futures", symbol="BTC-USDT-PERP", ts=1767225600.0):
        self.exchange = exchange
        self.symbol = symbol
        self.id = str(i)
        self.side = "buy" if i % 2 else "sell"
        self.price = 42000.0 + i
        self.amount = 0.5
        self.timestamp = ts


class FakeFunding:
    def __init__(self, ts=1767225600.0):
        self.exchange = "binance_futures"
        self.symbol = "BTC-USDT-PERP"
        self.rate = 0.0001
        self.mark_price = 42000.0
        self.timestamp = ts
        self.next_funding_time = ts + 28800


class FakeCandle:
    def __init__(self, closed=True, start=1767225600.0):
        self.exchange = "binance_futures"
        self.symbol = "BTC-USDT-PERP"
        self.interval = "1m"
        self.start = start
        self.stop = start + 60
        self.open, self.high, self.low, self.close = 1.0, 2.0, 0.5, 1.5
        self.volume = 10.0
        self.trades = 7
        self.closed = closed


class FakeLiquidation:
    def __init__(self, ts=1767225600.0):
        self.exchange = "binance_futures"
        self.symbol = "BTC-USDT-PERP"
        self.side = "sell"
        self.price = 41900.0
        self.quantity = 2.0
        self.timestamp = ts


class FakeOI:
    def __init__(self, ts=1767225600.0):
        self.exchange = "binance_futures"
        self.symbol = "BTC-USDT-PERP"
        self.open_interest = 12345.6
        self.timestamp = ts


class FakeCursor:
    """Acepta `execute(sql, params)` con params columna-a-columna, como hace psycopg3."""

    def __init__(self, sink):
        self.sink = sink
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self.sink["insert_sql"] = sql
        n = 0
        if params:
            n = len(params[0])
            for i in range(n):
                self.sink["rows"].append(tuple(col[i] for col in params))
        # rowcount de ESTA sentencia, no el acumulado: es lo que Postgres devolveria.
        self.rowcount = n


class FakeConn:
    def __init__(self, sink):
        self.sink = sink

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def cursor(self):
        return FakeCursor(self.sink)

    def execute(self, sql, *a):
        self.sink.setdefault("session_sql", []).append(sql)


class FakePool:
    def __init__(self):
        self.sink = {"rows": []}

    def connection(self):
        return FakeConn(self.sink)

    def close(self):
        pass


def store_with_fake_pool(max_rows=1000):
    s = Store(max_rows=max_rows)
    s.pool = FakePool()
    return s


# --------------------------------------------------------------------- timestamps


def test_to_utc_autodetecta_las_cuatro_unidades():
    """Regla 3: 13 digitos = ms, 16 = us... y aqui tambien ns. Sin adivinar la unidad."""
    esperado = datetime(2026, 1, 1, tzinfo=timezone.utc)
    for v in (1767225600.0, 1767225600000.0, 1767225600000000.0, 1767225600000000000.0):
        assert to_utc(v) == esperado, f"fallo con {v!r}"


def test_to_utc_no_revienta_con_microsegundos():
    """Regresion: con umbrales por umbrales redondos (>=1e16) los microsegundos caian en el bucket
    de milisegundos y `fromtimestamp` reventaba con 'year 57971 is out of range'."""
    assert to_utc(1767225600000000.0).year == 2026


def test_to_utc_nunca_naive():
    assert to_utc(1767225600.0).tzinfo is not None


def test_los_timestamps_se_guardan_en_utc_explicitamente():
    """El ts que va a la columna TIMESTAMPTZ tiene que ser tz-aware UTC, no la zona del host."""
    s = store_with_fake_pool()
    s.add_trade(FakeTrade(1), 1767225600.0)
    row, ingest = s.writers["trades"].buf[0]
    assert ingest is not None, "cada fila guarda su instante de llegada para el p95 trade->fila"
    assert row[3] == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert str(row[3].tzinfo) == "UTC"


# --------------------------------------------------------------------- simbolos


@pytest.mark.parametrize(
    "raw",
    ["BTC-USDT-PERP", "BTCUSDT", "BTC/USDT", "BTC-USDT-SWAP", "BTC-PERP", "BTC-USDT",
     "BTCUSDT_UMCBL", "BTC", "btc-usdt-perp"],
)
def test_simbolo_se_normaliza_a_la_forma_del_proyecto(raw):
    """Si el daemon usara el nombre nativo y el loader `BTCUSDT`, cada uno escribiria en una
    serie distinta y el backtest no encontraria Trades."""
    assert Store._sym("binance_futures", raw) == "BTCUSDT"


def test_regresion_del_recorte_de_sufijo():
    """`BTC-USDT-PERP` daba `BTC-` al recortar 8 caracteres en vez de quitar `USDT-PERP`."""
    assert Store._sym("x", "BTC-USDT-PERP") == "BTCUSDT"
    assert not Store._sym("x", "BTC-USDT-PERP").endswith("-")


# --------------------------------------------------------------------- idempotencia


def test_todo_el_insert_lleva_on_conflict_do_nothing():
    """Requisito duro: sin esto, un corte de red con resubscribe duplica trades y OI."""
    s = store_with_fake_pool()
    for dtype, w in s.writers.items():
        assert "ON CONFLICT DO NOTHING" in w._insert_sql(), dtype


def test_reenviar_los_mismos_trades_no_duplica(tmp_path):
    """El AC de red: si el WS reenvia lo ultimo tras reconectar, el segundo paso es un no-op."""
    s = store_with_fake_pool()
    trades = [FakeTrade(i) for i in range(50)]
    for t in trades:
        s.add_trade(t, 1767225600.0)
    s.flush(reason="size")
    assert len(s.pool.sink["rows"]) == 50
    for t in trades:  # el exchange reenvia los mismos 50
        s.add_trade(t, 1767225600.0)
    s.flush(reason="size")
    # El writer vuelve a emitir las 50, pero son las MISMAS claves: las descarta la PK
    # (symbol, exchange, ts, trade_id) via ON CONFLICT. Que no haya duplicados de verdad se
    # comprueba contra la DB real en el AC, no aqui ( aqui no hay base de datos).
    assert s.writers["trades"].rows_written == 100  # 50 + 50 emitidos


# --------------------------------------------------------------------- candles


def test_solo_entran_velas_cerradas():
    """Con DO NOTHING, guardar la vela en curso fijaria la version incompleta para siempre."""
    s = store_with_fake_pool()
    s.add_candle(FakeCandle(closed=False), 1767225600.0)
    assert len(s.writers["candles"]) == 0
    s.add_candle(FakeCandle(closed=True), 1767225600.0)
    assert len(s.writers["candles"]) == 1


def test_el_insert_alinea_columnas_tipos_y_orden():
    """El `unnest` alinea por posicion: si el orden se desincroniza, el precio acaba en `ts` y no
    falla nada hasta que los datos son basura."""
    s = store_with_fake_pool()
    s.add_trade(FakeTrade(1), 1767225600.0)
    s.flush(reason="size")
    sql = s.pool.sink["insert_sql"]
    assert '"symbol", "exchange", "trade_id", "ts", "receipt_ts", "side", "price", "amount", "notional"' in sql
    assert "u.v4::timestamptz" in sql, "la columna ts debe castearse a timestamptz en su posicion"
    assert "u.v7::double precision" in sql, "price es la 7a columna"
    assert len(s.writers["trades"].columns) == 9
    assert len(s.writers["trades"].pg_types) == 9


def test_no_se_usa_copy_porque_copy_no_soporta_on_conflict():
    """Regresion de diseño, vista con datos reales.

    Con COPY, el 2º flush del mismo lote fallaba con violacion de PK y caia al fallback: la
    idempotencia no la daba `ON CONFLICT DO NOTHING` sino un error. COPY no admite clausula de
    conflicto, asi que no se puede usar como camino principal.
    """
    import inspect

    from feed.writer import Store as S

    src = inspect.getsource(S._insert)
    assert "copy(" not in src, "el camino principal no debe usar COPY"
    for dtype in S.TARGETS:
        w = S().writers[dtype]
        assert "COPY" not in w._insert_sql()
        assert w._insert_sql().rstrip().endswith("ON CONFLICT DO NOTHING")


def test_columnas_y_tipos_desincronizados_fallan_rapido():
    """Better fall here que escribir el precio en la columna `ts`."""
    from feed.writer import Writer

    with pytest.raises(ValueError, match="desincronizados"):
        Writer(table="t", columns=("a", "b", "c"), pg_types=("text",))._insert_sql()


# --------------------------------------------------------------------- AC: SIGTERM


def test_sigterm_no_pierde_filas():
    """AC 'SIGTERM -> 0 filas perdidas', verificado con un contador.

    Se meten N filas en el buffer, NO se hace flush por tiempo ni por tamano (se fuerza
    `max_rows` alto a proposito), y se lanza el flush final: la base tiene que recibir las N.
    """
    n = 5000
    s = store_with_fake_pool(max_rows=10**9)
    for i in range(n):
        s.add_trade(FakeTrade(i), 1767225600.0)
    assert len(s.writers["trades"]) == n, "el buffer deberia tener las 5000 antes del flush"
    assert s.pool.sink["rows"] == [], "no deberia haberse escrito nada todavia"

    written = s.flush(reason="shutdown")

    assert written == n
    assert len(s.pool.sink["rows"]) == n
    assert len(s.writers["trades"]) == 0, "el buffer tiene que quedar vacio tras el flush final"


def test_sigterm_tambien_vacia_los_otros_dtypes():
    """El AC es de todo el daemon, no solo de trades."""
    s = store_with_fake_pool(max_rows=10**9)
    for i in range(10):
        s.add_trade(FakeTrade(i), 1767225600.0)
        s.add_funding(FakeFunding(1767225600.0 + i * 28800), 1767225600.0)
        s.add_open_interest(FakeOI(1767225600.0 + i * 300), 1767225600.0)
        s.add_liquidation(FakeLiquidation(1767225600.0 + i), 1767225600.0)
        s.add_candle(FakeCandle(True, 1767225600.0 + i * 60), 1767225600.0)
    total = sum(len(w) for w in s.writers.values())
    assert total == 50
    written = s.flush(reason="shutdown")
    assert written == 50
    assert sum(len(w) for w in s.writers.values()) == 0


def test_el_daemon_hace_flush_final_en_el_finally():
    """Regresion de estructura: el flush final tiene que estar en el `finally` de `run()`.

    Se comprueba con AST y no con `inspect.getsource` + `split("finally:")`: un comentario que
    mencione la palabra "finally" parte el string por la mitad y el test pasa o falla por
    casualidad. Con el AST se verifica la estructura real: hay un `Try` con handler `Finally` y
    su cuerpo llama a `_final_flush`.
    """
    import ast
    import inspect
    import textwrap

    # getsource de un metodo viene indentado; ast.parse exige codigo a nivel de modulo.
    src = textwrap.dedent(inspect.getsource(Daemon.run))
    tree = ast.parse(src)
    tries = [n for n in ast.walk(tree) if isinstance(n, ast.Try) and n.finalbody]
    assert len(tries) == 1, "run() deberia tener exactamente un try/finally"
    calls = {
        n.func.attr
        for stmt in tries[0].finalbody
        for n in ast.walk(stmt)
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
    }
    assert "_final_flush" in calls, f"el finally deberia llamar a _final_flush; llama a {calls}"


# --------------------------------------------------------------------- AC: p95


def test_p95_latencia_trade_fila():
    """AC 'p95 trade->fila < 2 s'. Con 10000 muestras todas a 0.5 s, el p95 es 0.5 s."""
    s = store_with_fake_pool()
    for _ in range(10000):
        s.add_latency(500.0)
    assert 450 <= s.p95_ms() <= 550


def test_p95_es_0_si_no_hay_trades():
    assert store_with_fake_pool().p95_ms() == 0.0


def test_la_latencia_no_crece_sin_limite():
    """La ventana deslizante evita que un daemon de semanas se quede sin memoria."""
    s = store_with_fake_pool()
    for _ in range(30000):
        s.add_latency(1.0)
    assert len(s.writers["trades"].latencies_ms) <= 20000


# --------------------------------------------------------------------- config


def test_los_canales_no_soportados_se_quitan_en_lugar_de_petar():
    """Bitget no tiene liquidations y Hyperliquid solo trades: pedirle lo que no tiene aborta el
    arranque de cryptofeed. Lo correcto es recortarlo y dejarlo escrito."""
    cfg = feedconfig.FeedConfig(channels=("trades", "funding", "liquidations"))
    assert cfg.channels_for("bitget") == ("trades", "funding")
    assert cfg.channels_for("hyperliquid") == ("trades",)
    assert cfg.channels_for("binance_futures") == ("trades", "funding", "liquidations")


def test_la_config_por_defecto_es_btc_eth_en_los_5_exchanges():
    cfg = feedconfig.load()
    assert cfg.symbols == ("BTC", "ETH")
    assert len(cfg.exchanges) == 5


def test_book_depth_no_se_suscribe_nunca():
    """El depth no cabe en un mini PC y nadie lo lee: fuera de la lista de canales."""
    assert "l2_book" not in feedconfig.CHANNELS
    assert "book" not in feedconfig.CHANNELS


# --------------------------------------------------------------------- robustez


def test_una_fila_corrupta_no_mata_el_daemon():
    """Regla de la skill: el daemon no muere por una fila mala, la cuenta y sigue."""
    s = store_with_fake_pool()

    class Malo:
        exchange = "binance_futures"
        symbol = "BTC-USDT-PERP"
        id = "x"
        side = "buy"
        price = None
        amount = None
        timestamp = 1767225600.0

    s.add_trade(Malo(), 1767225600.0)
    s.add_trade(FakeTrade(1), 1767225600.0)
    assert s.writers["trades"].rows_dropped == 1
    assert len(s.writers["trades"]) == 1, "la fila buena tiene que seguir ahi"


def test_nan_se_descarta_en_lugar_de_romper_el_not_null():
    s = store_with_fake_pool()
    t = FakeTrade(1)
    t.price = float("nan")
    s.add_trade(t, 1767225600.0)
    assert len(s.writers["trades"]) == 0
    assert s.writers["trades"].rows_dropped == 1


def test_lote_que_falla_se_cuenta_y_no_se_reintenta_a_ciegas():
    """Un reintento infinito de un lote malo mata el daemon; perder el lote es visible en el log."""
    s = store_with_fake_pool()

    class PoolQueFalla(FakePool):
        def connection(self):
            raise RuntimeError("db caida")

    s.pool = PoolQueFalla()
    s.add_trade(FakeTrade(1), 1767225600.0)
    written = s.flush(reason="size")
    assert written == 0
    assert s.writers["trades"].rows_dropped == 1
    assert len(s.writers["trades"]) == 0, "el lote no se queda reintentandose en memoria"

# ---------------------------------------------------------------- simbolos por exchange
def test_los_simbolos_que_se_pasan_a_cryptofeed_son_los_normalizados():
    """Regresion de runtime: Hyperliquid NO acepta `BTC-USDT-PERP`.

    Con el generico, `add_feed` reventaba con
    `UnsupportedSymbol: BTC-USDT-PERP is not supported on HYPERLIQUID` y los cinco feeds
    caian. Los pares normalizado->nativo estan medidos con `Feed.symbol_mapping()` en 3.0.1.
    """
    from feed.config import load

    cfg = load()
    assert cfg.symbols_for("binance_futures") == ("BTC-USDT-PERP", "ETH-USDT-PERP")
    assert cfg.symbols_for("bybit") == ("BTC-USDT-PERP", "ETH-USDT-PERP")
    assert cfg.symbols_for("okx") == ("BTC-USDT-PERP", "ETH-USDT-PERP")
    assert cfg.symbols_for("bitget") == ("BTC-USDT-PERP", "ETH-USDT-PERP")
    # Hyperliquid normaliza a USD-PERP (nativo `BTC`), no a USDT-PERP.
    assert cfg.symbols_for("hyperliquid") == ("BTC-USD-PERP", "ETH-USD-PERP")


def test_los_callbacks_se_pasan_al_constructor_del_exchange():
    """Regresion de runtime, la mas cara de esta fase.

    `FeedHandler.add_feed(feed, **kwargs)` descarta `kwargs` cuando `feed` ya es una instancia
    (`feedhandler.py::add_feed` -> `self.feeds.append(feed)` y solo usa kwargs si es str). Con
    los callbacks ahi, cryptofeed recibia los mensajes pero no llamaba a ninguna callback y sin
    log de error: `Feed.__init__` deja `Callback(None)` por canal. Medido: 76 mensajes en 25 s y
    0 ticks. Este test falla si los callbacks vuelven a `add_feed`.
    """
    import ast
    import pathlib

    # Se lee el archivo en vez de usar `inspect.getsource`: bajo pytest el `linecache` no
    # siempre devuelve el bloque y el test falla por un motivo que no es el que vigila.
    src = (pathlib.Path(__file__).resolve().parents[1] / "feed" / "daemon.py").read_text()
    assert "callbacks=handlers" in src, "los callbacks deben ir en el constructor del exchange"
    # Y no podem-os volver a pasarlos por kwargs a add_feed.
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and getattr(node.func, "attr", "") == "add_feed":
            assert not any(kw.arg == "callbacks" for kw in node.keywords), (
                "add_feed ignora callbacks= en kwargs: deben ir en el constructor"
            )

def test_las_claves_de_callback_son_las_constantes_de_cryptofeed():
    """`Feed.callbacks` se indexa por CALLBACK_CHANNELS; una clave string no registra nada."""
    from cryptofeed.defines import (
        CANDLES,
        FUNDING,
        LIQUIDATIONS,
        OPEN_INTEREST,
        TRADES,
    )
    from cryptofeed.feed import CALLBACK_CHANNELS

    for const in (TRADES, FUNDING, OPEN_INTEREST, LIQUIDATIONS, CANDLES):
        assert const in CALLBACK_CHANNELS


# ---------------------------------------------------------------- normalizacion de simbolos
@pytest.mark.parametrize(
    "raw",
    [
        # Normalizados: es lo que cryptofeed entrega al callback (`exchange_symbol_to_std_symbol`).
        "BTC-USDT-PERP", "ETH-USDT-PERP",
        # Hyperliquid normaliza a USD-PERP. Medido en DB: antes salia "BTCUSDUSDT" porque se
        # recortaba "PERP" y se pegaba "USDT" detras, sin fallar: otra serie, mismo símbolo.
        "BTC-USD-PERP", "ETH-USD-PERP",
        # Nativos, por si el exchange devolviera el suyo (Feed.symbol_mapping en 3.0.1).
        "BTCUSDT", "BTC-USDT-SWAP", "BTCUSDT_USDT-FUTURES", "BTC",
        # Variantes de formato.
        "BTC-USDC-PERP", "btc-usdt-perp", "BTCUSDT_UMCBL", "BTC/USDT/PERP",
    ],
)
def test_normaliza_al_perpetuo_usdt_del_proyecto(raw):
    assert Store._sym("binance_futures", raw) in ("BTCUSDT", "ETHUSDT")


@pytest.mark.parametrize("raw", ["BTC-EUR", "BTC-USD1-PERP", "DOGE-EUR-PERP", "", "BTC-26Z25"])
def test_no_inventa_un_simbolo_que_el_proyecto_no_sigue(raw):
    """Una cifra que no es perps USDT se descarta antes que escribir en una serie inventada."""
    assert Store._sym("okx", raw) == ""


# ------------------------------------------------------- latencia trade -> fila
def test_el_p95_mide_llegada_a_fila_y_no_el_intervalo_entre_trades():
    """Regresion: antes se media `(ahora - llegada_del_tick_anterior)`, que es el intervalo
    entre trades. Daba ~150 ms y parecia bueno, pero no media nada de lo que pide el AC."""
    import time as _t

    s = store_with_fake_pool()
    for i in range(20):
        s.add_trade(FakeTrade(i), 1767225600.0)
        _t.sleep(0.002)  # ticks separados en el tiempo

    assert s.p95_ms() == 0.0, "no hay flush, no hay latencia medida"
    s.flush(reason="size")
    p95 = s.p95_ms()
    assert p95 > 0.0, "tras el flush tiene que haber latencia medida"
    # 20 ticks cada 2 ms entran en un unico lote: la latencia de todos es la del propio flush,
    # del orden de milisegundos, y desde luego menor que el intervalo acumulado (40 ms) que
    # daria la metrica antigua sumada.
    assert p95 < 1000.0


def test_la_latencia_se_acumula_por_fila_y_no_por_lote():
    """Una fila que entro tarde en el lote tiene mas latencia que una que entro al principio."""
    import time as _t

    s = store_with_fake_pool()
    s.add_trade(FakeTrade(1), 1767225600.0)
    _t.sleep(0.05)
    s.add_trade(FakeTrade(2), 1767225600.0)
    s.flush(reason="size")

    lat = sorted(s.writers["trades"].latencies_ms)
    assert len(lat) == 2
    assert lat[1] - lat[0] > 20.0, "las dos filas del mismo lote deben diferir en su latencia"
