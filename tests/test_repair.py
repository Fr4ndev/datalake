"""Tests del paquete de reparacion.

Los de red y base de datos estan marcados y corren enteros dentro del contenedor
(`docker-compose run --rm -T repair pytest`). Los de abajo, con `FakeHttp`, no tocan la red:
comprueban la **logica** (paginacion que termina, limite de OKX, granularidad de Bybit, maquina de
estados del worker) que es donde se cuelan los errores silenciosos.
"""

from __future__ import annotations

import gzip
import io
import os

import pytest

from feed.gaps import Gap
from repair.adapters.base import CandleRow, RepairResult, TradeRow, side_from_is_buyer_maker, \
    side_normalized
from repair.adapters.binance import REST_MAX_AGE_MS, BinanceFuturesAdapter
from repair.adapters.bybit import BybitAdapter, ms_desde_segundos
from repair.adapters.hyperliquid import HyperliquidAdapter
from repair.adapters.okx import OKXAdapter
from repair.worker import Worker

DSN = os.environ.get("MIGRATE_DSN") or (
    f"postgresql://marketdata:{os.environ['POSTGRES_PASSWORD']}@tsdb:5432/marketdata"
    if os.environ.get("POSTGRES_PASSWORD") else None)
needs_db = pytest.mark.skipif(not DSN, reason="requiere MIGRATE_DSN o POSTGRES_PASSWORD")

HORA = 3600_000


def gap(desde, hasta, dtype="trades", exchange="binance_um", reason="silence") -> Gap:
    return Gap(exchange=exchange, symbol="BTCUSDT", dtype=dtype,
               gap_from_ms=desde, gap_to_ms=hasta, reason=reason)


class FakeHttp:
    """Sustituto de `repair.http.Client`. `paginas` es una lista de respuestas en orden."""

    def __init__(self, paginas):
        self.paginas = list(paginas)
        self.llamadas: list[dict] = []

    def get(self, exchange, url, params=None, **kw):
        self.llamadas.append({"exchange": exchange, "url": url, "params": params or {}})
        if not self.paginas:
            return []
        return self.paginas.pop(0)

    def post(self, exchange, url, body, **kw):
        self.llamadas.append({"exchange": exchange, "url": url, "body": body})
        return self.paginas.pop(0) if self.paginas else []


# ============================================================ normalizacion
def test_side_de_los_tres_exchanges_que_no_hablan_igual():
    # Binance manda `m`; el comprador siendo maker significa que se vendo.
    assert side_from_is_buyer_maker(True) == "sell"
    assert side_from_is_buyer_maker(False) == "buy"
    # Bybit/Bitget mandan el texto; Hyperliquid `A` es *ask*, o sea venta.
    assert side_normalized("Buy") == "buy"
    assert side_normalized("Sell") == "sell"
    assert side_normalized("A") == "sell"
    assert side_normalized("B") == "buy"


def test_ts_del_volcado_bybit_floor_versus_round():
    # 0,1199 s -> el 4o decimal decide el ms, y no es ruido de formato (D-1 medido uniforme 0-9).
    assert ms_desde_segundos("1790985600.1199", "floor") == 1790985600119
    assert ms_desde_segundos("1790985600.1199", "round") == 1790985600120
    assert ms_desde_segundos("1790985600.1199", "floor") != ms_desde_segundos(
        "1790985600.1199", "round")
    assert ms_desde_segundos(1790985600.0, "floor") == 1790985600000


# ============================================================ OKX
def _trade_okx(ts, tid, side="buy"):
    return {"tradeId": tid, "px": "100.5", "sz": "0.01", "side": side, "ts": str(ts)}


def test_okx_pagina_hacia_atras_y_termina():
    """`after` devuelve lo ANTERIOR y hay que restarle 1. Sin el -1 el bucle no termina."""
    t0 = 1_700_000_000_000
    # Tres paginas de 100 ms cada una, de la mas nueva a la mas vieja.
    http = FakeHttp([
        {"data": [_trade_okx(t0 - i, f"n{i}") for i in range(100)]},
        {"data": [_trade_okx(t0 - 100 - i, f"m{i}") for i in range(100)]},
        {"data": [_trade_okx(t0 - 200 - i, f"k{i}") for i in range(100)]},
        {"data": []},
    ])
    res = OKXAdapter(http).fetch_trades(gap(t0 - 300, t0))
    cursores = [c["params"]["after"] for c in http.llamadas]
    # Cada `after` siguiente es estrictamente menor: eso es lo que guarantees la terminacion.
    assert cursores == sorted(cursores, reverse=True), cursores
    assert len(set(cursores)) == len(cursores), "se repitio pagina: falta el -1"
    assert all(c["params"]["type"] == 2 for c in http.llamadas), "type=2 es el que coincide con el WS"
    assert {r.trade_id for r in res.rows} >= {f"k{i}" for i in range(100)}


def test_okx_usa_instId_swap():
    http = FakeHttp([{"data": []}])
    OKXAdapter(http).fetch_trades(gap(0, 1000))
    assert http.llamadas[0]["params"]["instId"] == "BTCUSDT-SWAP"


# ============================================================ Hyperliquid
def test_hyperliquid_declara_los_trades_irrecuperables():
    adapter = HyperliquidAdapter(FakeHttp([]))
    puede, motivo = adapter.can_repair(gap(0, 1000))
    assert puede is False
    assert "10" in motivo and "historico" in motivo


def test_hyperliquid_repara_velas():
    t0 = 1_700_000_000_000
    velas = [{"t": str(t0 - i * 60_000), "o": "1", "h": "2", "l": "0.5", "c": "1.5",
              "v": "10", "n": 3} for i in range(5)]
    http = FakeHttp([velas, []])
    res = HyperliquidAdapter(http).fetch_candles(gap(t0 - 240_000, t0, dtype="candles"))
    assert len(res.rows) == 5
    assert res.source == "rest"


# ============================================================ Binance
def test_binance_usa_rest_para_huecos_recientes():
    ahora = 1_700_000_000_000
    http = FakeHttp([[]])
    a = BinanceFuturesAdapter(http, now_ms=ahora)
    a.fetch_trades(gap(ahora - HORA, ahora - HORA + 1000))
    assert http.llamadas[0]["url"].endswith("/fapi/v1/aggTrades")


def test_binance_cambia_a_volcado_pasados_los_48h():
    ahora = 1_700_000_000_000
    viejo = ahora - REST_MAX_AGE_MS - HORA
    http = FakeHttp([[]])
    a = BinanceFuturesAdapter(http, now_ms=ahora)
    res = a.fetch_trades(gap(viejo, viejo + 1000))
    # Sin red: no hay URL de REST registrada, solo el intento de descarga del volcado.
    assert res.source == "dump"


def test_binance_aggtrade_compartido_con_el_ws():
    """El WS de cryptofeed usa `aggTrade` y `id=str(a)`. Si esto cambia, el dedup por PK falla."""
    http = FakeHttp([[{"a": 12345, "p": "100.0", "q": "0.5", "T": 1700000000000, "m": True}]])
    res = BinanceFuturesAdapter(http, now_ms=1700000001000).fetch_trades(
        gap(1700000000000, 1700000001000))
    assert [r.trade_id for r in res.rows] == ["12345"]
    assert res.rows[0].ts_ms == 1700000000000
    assert res.rows[0].side == "sell"


def test_binance_ventana_de_30min_para_no_comerse_el_4166():
    """El REST rechaza ventanas anchas; por eso el recorrido es en trozos de 30 min."""
    ahora = 1_700_000_000_000
    http = FakeHttp([[], [], []])
    BinanceFuturesAdapter(http, now_ms=ahora).fetch_trades(
        gap(ahora - 2 * HORA, ahora - HORA + 1000))
    assert len(http.llamadas) >= 2
    for c in http.llamadas:
        if "startTime" in c["params"]:  # las de `fromId` no llevan ventana
            span = c["params"]["endTime"] - c["params"]["startTime"]
            assert span <= 30 * 60_000 + 1000, span


class _BinancePaginando:
    """Emula el `/aggTrades` de verdad: `limit` filas por pagina y `fromId` excluyente con
    `startTime`/`endTime`. Devuelve `n` trades repartidos por el hueco, uno por ms."""

    def __init__(self, desde_ms, hasta_ms, n):
        self.desde, self.hasta, self.n = desde_ms, hasta_ms, n
        self.llamadas: list[dict] = []

    def get(self, exchange, url, params=None, **kw):
        p = dict(params or {})
        self.llamadas.append(p)
        # El trade_id ES el timestamp en este fake, asi que `fromId` es un indice absoluto.
        i = (p["fromId"] if "fromId" in p else max(p["startTime"], self.desde)) - self.desde
        fin = min(i + 1000, self.n)
        if fin <= i:
            return []
        return [{"a": self.desde + j, "p": "1", "q": "1", "T": self.desde + j, "m": False}
                for j in range(i, fin)]

    @property
    def paginas(self):
        return len(self.llamadas)


def test_binance_pagina_hasta_agotar_la_ventana():
    """Una pagina LLENA no significa ventana agotada.

    Este es el fallo que produjo el gap 704 cerrado como `repaired` sin una sola fila: 4.500 trades
    en 20 min, el endpoint devuelve 1.000 y el adaptador tomaba esa respuesta como el final de la
    ventana, declaraba `covered_through` completo y el worker cerraba el hueco.
    """
    from repair.adapters.binance import LIMITE_PAGINA

    ahora = 1_700_000_000_000
    desde = ahora - 20 * 60_000
    http = _BinancePaginando(desde, ahora, 4500)
    res = BinanceFuturesAdapter(http, now_ms=ahora).fetch_trades(gap(desde, ahora))
    assert len(res.rows) == 4500, len(res.rows)
    assert http.paginas == 5, http.paginas  # 4 paginas llenas + la quinta corta
    assert any("fromId" in p for p in http.llamadas), "hubo que seguir paginando por fromId"
    assert res.limitation is None, res.limitation
    assert res.covered_through_ms >= ahora


def test_binance_no_declara_cobertura_si_no_pudo_agotar_la_ventana():
    """Si se corta por el tope de paginas, el hueco NO se cierra: se declara `limitation` y
    `covered_through=None`, y el worker lo deja en `partial`."""
    import repair.adapters.binance as mod

    ahora = 1_700_000_000_000
    desde = ahora - 20 * 60_000
    http = _BinancePaginando(desde, ahora, 9000)
    viejo = mod.MAX_PAGINAS
    mod.MAX_PAGINAS = 2  # solo 2 paginas: insuficiente para 9.000
    try:
        res = BinanceFuturesAdapter(http, now_ms=ahora).fetch_trades(gap(desde, ahora))
    finally:
        mod.MAX_PAGINAS = viejo
    assert len(res.rows) == 2000, len(res.rows)
    assert res.limitation is not None, "sin esto el hueco se cerraria como repaired incompleto"
    assert res.covered_through_ms is None
    assert "no sabe" in res.limitation or "seguia llena" in res.limitation


def test_binance_no_arrastra_trades_del_resto_por_el_fromId():
    """`fromId` ignora `endTime`: la ventana se da por agotada en cuanto la ultima fila la pasa,
    y lo que venga despues pertenece a la ventana siguiente."""
    ahora = 1_700_000_000_000
    desde = ahora - 20 * 60_000
    http = _BinancePaginando(desde, ahora + 10 * HORA, 9000)
    res = BinanceFuturesAdapter(http, now_ms=ahora).fetch_trades(gap(desde, ahora))
    assert all(desde <= r.ts_ms <= ahora for r in res.rows), "se colaron trades fuera del hueco"
    assert res.limitation is None


# ============================================================ Bybit
def test_bybit_rest_marca_limitacion_si_no_cubre_el_hueco():
    """`recent-trade` ignora `startTime`: si el trade mas viejo cae dentro del hueco, no lo cubre
    y el hueco debe quedar `partial`, nunca `repaired`."""
    t0 = 1_700_000_000_000
    http = FakeHttp([{"retCode": 0, "result": {"list": [
        {"execId": "a", "time": str(t0), "side": "Buy", "price": "1", "size": "1"},
        {"execId": "b", "time": str(t0 - 500), "side": "Sell", "price": "1", "size": "1"},
    ]}}])
    res = BybitAdapter(http).fetch_trades(gap(t0 - 60_000, t0))
    assert res.limitation is not None, "un REST de 2 min no puede cerrar un hueco de 60 s"
    assert "volcado" in res.limitation
    assert len(res.rows) == 2


def test_bybit_dedup_dentro_del_volcado():
    lineas = ["timestamp,symbol,side,size,price,trdMatchID"]
    for i in range(50):
        # 50 lineas pero solo 5 tradeId distintos: el volcado se deduplica ANTES de tocar la base.
        lineas.append(f"1790985600.{i % 10:04d},BTCUSDT,Buy,0.1,100,X{i % 5}")
    crudo = gzip.compress(("\n".join(lineas) + "\n").encode())
    from repair.adapters.bybit import _parsear_dump
    filas = _parsear_dump(crudo, "BTCUSDT", "floor")
    # 50 lineas, 5 tradeId distintos: el volcado se deduplica ANTES de tocar la base.
    assert len(filas) == 5
    assert {f.trade_id for f in filas} == {f"X{i}" for i in range(5)}
    assert all(isinstance(f.ts_ms, int) for f in filas)


def test_bybit_ventana_antijoin_acotada():
    """El anti-join debe acotarse a la ventana del dump para que Timescale pode chunks."""
    from repair.adapters.bybit import VENTANA_ANTIJOIN_MS
    assert 0 < VENTANA_ANTIJOIN_MS <= 5 * 60_000


# ============================================================ maquina de estados
class LedgerFalso:
    def __init__(self):
        self.fin: list[tuple] = []
        self.bumps: list[int] = []
        #: veces que se pidio `release(..., deshacer_intento=True)`.
        self.intentos_deshechos = 0

    def finish(self, gap_id, status, source=None, rows=0, note=None):
        self.fin.append((gap_id, status, rows, note))

    def bump_attempt(self, gap_id):
        self.bumps.append(gap_id)
        return 1

    def release(self, gap_id, deshacer_intento=False):
        self.fin.append((gap_id, "release", 0, None))
        if deshacer_intento:
            self.intentos_deshechos += 1


def _worker_con(resultado) -> Worker:
    w = Worker.__new__(Worker)
    w.ledger = LedgerFalso()
    w.conn = object()
    w._resultado = resultado
    return w


def test_estado_repaired_solo_si_la_fuente_cubre_el_hueco_entero():
    g = gap(1000, 2000)
    completo = RepairResult(rows=[TradeRow("1", 1500, "buy", 1, 1, "BTCUSDT")], source="rest",
                            covered_from_ms=1000, covered_through_ms=2000, note="ok")
    w = _worker_con(completo)
    assert w._cerrar(g, completo, 1, "insert") == "reparados"
    assert w.ledger.fin[-1][1] == "repaired"


def test_estado_partial_si_la_fuente_solo_cubre_parte():
    g = gap(1000, 60_000)
    parcial = RepairResult(rows=[TradeRow("1", 1500, "buy", 1, 1, "BTCUSDT")], source="rest",
                           covered_from_ms=30_000, covered_through_ms=60_000,
                           limitation="el REST no llega mas atras")
    w = _worker_con(parcial)
    assert w._cerrar(g, parcial, 1, "insert") == "parciales"
    assert w.ledger.fin[-1][1] == "partial"
    assert "no llega mas atras" in (w.ledger.fin[-1][3] or "")


def test_cero_filas_es_partial_y_no_irrecuperable():
    """"La fuente no devolvio filas" no es "la fuente no tiene el dato".

    OKX tiene historico de sobra y aun asi devolvio 0 filas con una paginacion sospechosa. Marcarlo
    `unrecoverable` cerraria el hueco como perdido para siempre y apagaria la pista de que el
    adaptador esta mal. Irrecuperable de verdad es solo cuando el adaptador lo declara
    (`can_repair() -> False`), y ahi el motivo va escrito.
    """
    g = gap(1000, 2000)
    vacio = RepairResult(rows=[], source="rest")
    w = _worker_con(vacio)
    assert w._cerrar(g, vacio, 0, "sin filas") == "parciales"
    assert w.ledger.fin[-1][1] == "partial"
    assert "retencion" in (w.ledger.fin[-1][3] or "")


def test_irrecuperable_solo_cuando_el_adaptador_lo_declara():
    """El unico camino honesto a `unrecoverable` es `can_repair() -> False` con su motivo."""
    g = gap(1000, 2000)
    vacio = RepairResult(rows=[], source="rest")
    w = _worker_con(vacio)
    vacio.limitation = None
    w._cerrar(g, vacio, 0, "x")
    assert w.ledger.fin[-1][1] == "partial"


def test_un_baneo_no_cierra_el_hueco():
    """418 = problema de IP. Si cerrara el hueco como fallo, un baneo de 5 min quemaria los 5
    intentos y declararia irrecuperable algo que nadie ha intentado."""
    from repair.http import Banned

    class AdapterQueFalla:
        exchange = "binance_um"

        def can_repair(self, g):
            return True, None

        def fetch_trades(self, g):
            raise Banned(120.0, "ban")

    w = Worker.__new__(Worker)
    w.ledger = LedgerFalso()
    w.conn = object()
    w.adaptadores = {"binance_um": AdapterQueFalla()}
    g = gap(1000, 2000)
    g.attempts = 2
    w.reparar(g)
    assert w.ledger.fin[-1][1] == "release", w.ledger.fin[-1]


# ============================================================ integracion (requiere DB)
@needs_db
def test_insert_trades_por_id_no_duplica_ni_sigue_la_pk():
    """El caso que motiva el anti-join: el ts del volcado puede diferir en 1 ms del que ya esta
    guardado, con lo que la PK no lo reconoce y lo insertaria dos veces."""
    import psycopg

    from repair.ingest import insert_trades, insert_trades_por_id

    exchange = "binance_um"
    g = gap(0, 10_000)
    fila = TradeRow("dup-1", 5000, "buy", 100.0, 0.5, "BTCUSDT")
    # Mismo trade_id, ts con 1 ms de diferencia: la PK`(ts, trade_id)` NO lo detecta.
    misma_id_otro_ts = TradeRow("dup-1", 5001, "buy", 100.0, 0.5, "BTCUSDT")
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute("SET TIME ZONE 'UTC'")
        conn.execute("DELETE FROM trades WHERE exchange=%s AND symbol='BTCUSDT' "
                     "AND trade_id='dup-1'", (exchange,))
        n1 = insert_trades(conn, exchange, [fila], "rest")
        n2 = insert_trades(conn, exchange, [fila], "rest")
        ins, rep = insert_trades_por_id(conn, exchange, [misma_id_otro_ts], "dump",
                                        g.gap_from_ms - 60_000, g.gap_to_ms + 60_000)
        total = conn.execute(
            "SELECT count(*) FROM trades WHERE exchange=%s AND trade_id='dup-1'",
            (exchange,)).fetchone()[0]
        conn.execute("DELETE FROM trades WHERE exchange=%s AND trade_id='dup-1'", (exchange,))
    assert n1 == 1
    assert n2 == 0, "el REST repetido debe deduplicar por PK"
    assert rep == 1, "el anti-join debe reconocer el trade_id ya presente"
    assert ins == 0, "no debe insertar el ts casi identico: seria el duplicado que motiva el dump"
    assert total == 1


@needs_db
def test_la_conexion_del_worker_conserva_lo_insertado_al_cerrar():
    """Regresion del fallo que cerro el gap 704 como `repaired` con 0 filas en la tabla.

    Con `autocommit=False`, el `SET TIME ZONE` del worker abria una transaccion implicita: los
    `with conn.transaction()` de `repair/ingest.py` eran SAVEPOINTs y todo se revertia en
    `close()`, mientras el ledger (conexion propia, autocommit) ya habia confirmado el estado.

    Este test usa `Worker.open()` de verdad, no una conexion hecha a mano: si alguien vuelve a
    poner `autocommit=False` aqui, el test falla.
    """
    import psycopg

    from repair.ingest import insert_trades

    exchange = "binance_um"
    fila = TradeRow("worker-persist-1", 1_795_000_000_000, "buy", 100.0, 0.5, "BTCUSDT")
    w = Worker(DSN, exchanges=["binance_um"])
    w.open()
    try:
        with psycopg.connect(DSN, autocommit=True) as limpio:
            limpio.execute("SET TIME ZONE 'UTC'")
            limpio.execute("DELETE FROM trades WHERE trade_id='worker-persist-1'")
        assert insert_trades(w.conn, exchange, [fila], "rest") == 1
    finally:
        w.close()  # aqui es donde se perdia todo
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute("SET TIME ZONE 'UTC'")
        total = conn.execute("SELECT count(*) FROM trades WHERE trade_id='worker-persist-1'"
                             ).fetchone()[0]
        conn.execute("DELETE FROM trades WHERE trade_id='worker-persist-1'")
    assert total == 1, "el lote se perdio al cerrar: autocommit=False en el worker"


@needs_db
def test_fusion_de_gaps_no_borra_filas():
    """Regla: no borrar, cerrar con status. La deteccion original se conserva con 'merged'."""
    from feed.gaps import GapLedger

    # pad_ms=0: este test mide la semantica de la fusion (una fila canonica, el resto conservada
    # como 'merged'), no el margen de 5 s que anade el ledger por defecto.
    lg = GapLedger(DSN, pad_ms=0)
    lg.open()
    try:
        lg._require().execute("DELETE FROM ingest_gaps WHERE exchange LIKE %s", ("TEST%",))
        base = 1_700_000_000_000
        # Dos huecos vivos que NO se solapan conviven; el tercero cae sobre los dos y absorbe al
        # segundo. Con dos huecos que ya se solapan nunca habria nada que absorber: al fusionar
        # siempre queda una sola fila viva.
        lg.record([gap(base, base + 10_000)])
        lg.record([gap(base + 20_000, base + 30_000)])
        lg.record([gap(base + 5_000, base + 25_000)])
        # No se usa `list_gaps()`: devuelve las N primeras por `id` y el ledger real ya tiene mas de
        # 50 filas, asi que los huecos de este test se caian fuera del corte y el test pasaba/fallaba
        # segun cuantos huecos tuviera el daemon. La ventana se consulta por SQL, que es lo que el
        # test afirma medir.
        filas = lg._require().execute(
            "SELECT id, status, EXTRACT(EPOCH FROM gap_from)*1000, EXTRACT(EPOCH FROM gap_to)*1000 "
            "FROM ingest_gaps WHERE exchange='binance_um' AND symbol='BTCUSDT' AND dtype='trades' "
            "  AND EXTRACT(EPOCH FROM gap_from)*1000 BETWEEN %s AND %s ORDER BY id",
            (base - 100_000, base + 200_000)).fetchall()
        vivas = [(int(f[0]), f[1], int(f[2]), int(f[3])) for f in filas]
        canonicas = [f for f in vivas if f[1] in ("open", "repairing")]
        fundidas = [f for f in vivas if f[1] == "merged"]
        assert len(canonicas) == 1, vivas
        assert canonicas[0][2] == base, canonicas[0]
        assert canonicas[0][3] == base + 30_000, "la fusion debe cubrir los dos rangos"
        assert fundidas, "la fila absorbida debe conservarse como 'merged', no borrarse"
        for f in fundidas:
            assert f[0] != canonicas[0][0]
        conn = lg._require()
        conn.execute("DELETE FROM ingest_gaps WHERE symbol='BTCUSDT' "
                     "AND EXTRACT(EPOCH FROM gap_from)*1000 BETWEEN %s AND %s",
                     (base - 200_000, base + 200_000))
        conn.commit()
    finally:
        lg.close()


@needs_db
def test_coverage_de_velopes_candles_usa_open_time():
    """`candles_1m` no tiene columna `ts`: preguntar por ella es un error, no un valor erroneo."""
    import psycopg

    from feed.gaps import GapLedger

    # Clave propia de los tests: `candles_1m` la esta rellenando el daemon en vivo y `max(open_time)`
    # se mueve entre la lectura y la comprobacion.
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute("SET TIME ZONE 'UTC'")
        conn.execute("INSERT INTO candles_1m (symbol, exchange, open_time, open, high, low, "
                     "close, volume) VALUES ('TESTBTCUSDT','TESTEX',"
                     "to_timestamp(1700000000),1,1,1,1,1) ON CONFLICT DO NOTHING")
        # Referencia independiente: el propio Postgres sobre `open_time`. Si `coverage()` preguntase
        # por otra columna, daria otro valor o reventaria.
        cur = conn.execute("SELECT EXTRACT(EPOCH FROM max(open_time))*1000 FROM candles_1m "
                           "WHERE exchange='TESTEX' AND symbol='TESTBTCUSDT'")
        esperado = cur.fetchone()[0]
    try:
        lg = GapLedger(DSN)
        lg.open()
        try:
            cov = lg.coverage()
        finally:
            lg.close()
        ms = cov.last_ms.get(("TESTEX", "TESTBTCUSDT", "candles"))
        assert ms is not None, "candles no aparece en la cobertura"
        assert abs(esperado - ms) < 1, (ms, esperado)
    finally:
        # La fila se va: `coverage()` sin claves lee TODAS las tablas y un `TESTBTCUSDT` con una
        # vela de 2023 hace que este test dependa de datos que nadie ha pedido.
        with psycopg.connect(DSN, autocommit=True) as conn:
            conn.execute("DELETE FROM candles_1m WHERE exchange='TESTEX'")

def test_binance_cubre_por_ventana_consultada_y_no_por_el_ultimo_trade():
    """`covered_through` es el fin de la ventana pedida, no el ts del ultimo trade.

    Si el ultimo trade de la ventana caeria antes de `gap_to`, medido asi el hueco salia
    `partial` con 929 filas ya insertadas y el worker lo reintentaba para siempre. Una respuesta
    vacia ES la prueba de que ahi no habia trades.
    """
    ahora = 1_700_000_000_000
    inicio = ahora - 2 * HORA
    fin_gap = ahora - HORA
    # El ultimo trade cae 30 s ANTES del fin del hueco: la ventana final esta vacia.
    http = FakeHttp([
        [{"a": 1, "p": "1", "q": "1", "T": inicio + 1000, "m": False}],
        [{"a": 2, "p": "1", "q": "1", "T": fin_gap - 30_000, "m": False}],
        [],
    ])
    res = BinanceFuturesAdapter(http, now_ms=ahora).fetch_trades(gap(inicio, fin_gap))
    assert res.covered_through_ms >= fin_gap, res.covered_through_ms
    assert res.limitation is None


class _AdaptadorConVolcado:
    """REST que no llega (como el `recent-trade` de Bybit, ~2 min) y volcado que si."""
    exchange = "BYBIT"
    llamadas: list[str] = []

    def can_repair(self, g):
        return True, None

    def fetch_trades(self, g):
        self.llamadas.append("rest")
        return RepairResult(rows=[], source="rest",
                            limitation="recent-trade solo llega a ~2 min")

    def fetch_dump_trades(self, g, dia=None):
        self.llamadas.append("dump")
        return RepairResult(rows=[TradeRow("d1", 1500, "buy", 10.0, 1.0, "BTCUSDT")],
                            source="dump", covered_from_ms=g.gap_from_ms,
                            covered_through_ms=g.gap_to_ms)


def test_el_worker_cae_al_volcado_cuando_el_rest_no_alcanza():
    """Sin esta segunda pasada, todo hueco de Bybit mas viejo de ~2 min se queda `partial` para
    siempre aunque el volcado diario tenga el dato."""
    w = Worker.__new__(Worker)
    w.ledger = LedgerFalso()
    w.conn = object()
    ad = _AdaptadorConVolcado()
    ad.llamadas = []
    w.adaptadores = {"binance_um": ad}
    w._insertar = lambda g, res: (1, "anti-join por trade_id")
    g = gap(1000, 2000)
    g.attempts = 0
    w.reparar(g)
    assert ad.llamadas == ["rest", "dump"], ad.llamadas
    assert w.ledger.fin[-1][1] == "repaired", w.ledger.fin[-1]
    assert "volcado" in w.ledger.fin[-1][3]


class _AdaptadorSoloRest:
    """Los 4 exchanges sin volcado: no debe llamarse a `fetch_dump_trades` ni fallar por eso."""
    exchange = "OKX"

    def can_repair(self, g):
        return True, None

    def fetch_trades(self, g):
        return RepairResult(rows=[TradeRow("r1", 1500, "buy", 10.0, 1.0, "BTCUSDT")],
                            source="rest", covered_from_ms=g.gap_from_ms,
                            covered_through_ms=g.gap_to_ms)


def test_sin_metodo_de_volcado_no_pasa_nada():
    w = Worker.__new__(Worker)
    w.ledger = LedgerFalso()
    w.conn = object()
    w.adaptadores = {"binance_um": _AdaptadorSoloRest()}
    w._insertar = lambda g, res: (1, "insert por PK")
    g = gap(1000, 2000)
    g.attempts = 0
    assert w.reparar(g) == "reparados"
    assert w.ledger.fin[-1][1] == "repaired"


def test_el_volcado_no_pisa_lo_que_ya_trajo_el_rest():
    """Con filas del REST, el volcado se anti-une aparte y el REST sigue cerrando el hueco: son
    las filas cuyo `ts` coincide exacto con la tabla."""
    class _Ambos(_AdaptadorConVolcado):
        def fetch_trades(self, g):
            self.llamadas.append("rest")
            return RepairResult(rows=[TradeRow("r1", 1500, "buy", 10.0, 1.0, "BTCUSDT")],
                                source="rest", covered_from_ms=g.gap_from_ms,
                                covered_through_ms=g.gap_to_ms,
                                limitation="el REST no cubre los ultimos 30 s")

    inserts: list[tuple] = []

    def _ins(g, res):
        inserts.append(res.source)
        return (5, "x")

    w = Worker.__new__(Worker)
    w.ledger = LedgerFalso()
    w.conn = object()
    ad = _Ambos()
    ad.llamadas = []
    w.adaptadores = {"binance_um": ad}
    w._insertar = _ins
    g = gap(1000, 2000)
    g.attempts = 0
    w.reparar(g)
    assert inserts == ["dump", "rest"], inserts
    assert w.ledger.fin[-1][1] == "repaired"


def test_los_limites_de_http_se_buscan_por_exchange_canonico():
    """`Client("okx")` y `Client("BYBIT")` deben caer en SU cubo, no en el de hyperliquid.

    El fallo que fijamos aqui era invisible: `EXCHANGE_KEY` era un diccionario escrito a mano
    con las claves mezcladas (canonicas de una parte, nombres de cryptofeed de otra), asi que
    `EXCHANGE_KEY.get("okx", "hyperliquid")` caia al cubo de hyperliquid y OKX quedaba con 60 de
    capacidad en vez de 40. No reventaba nada: limitaba a otro ritmo, y eso se manifesto despues
    como 429 sin explicacion.
    """
    from repair.http import LIMITS, Client, clave_exchange

    for alias, canonico_esperado in [
        ("BINANCE_FUTURES", "binance_um"),
        ("BinanceFuturesUM", "binance_um"),
        ("OKX", "okx"),
        ("BYBIT", "bybit"),
        ("BITGET", "bitget"),
        ("HYPERLIQUID", "hyperliquid"),
    ]:
        assert clave_exchange(alias) == canonico_esperado, alias

    assert set(LIMITS) == {"binance_um", "okx", "bitget", "bybit", "hyperliquid"}

    # Los limites de cada exchange son los suyos, no los de otro.
    c = Client()
    assert c.weight("okx") == LIMITS["okx"][2]
    assert c.weight("bybit") == LIMITS["bybit"][2]
    assert c.weight("BINANCE_FUTURES") == LIMITS["binance_um"][2]


def test_el_cubo_no_rhita_mas_lento_que_el_limite_del_exchange():
    """`refill_per_s` va en TOKENS, no en peticiones, y por eso se confunde.

    Binance estaba en `(20.0, 2.0, 20.0)`: 2 tokens/s con peticiones de peso 20 son **10 s por
    peticion**, cuando su limite son 2400 de peso por minuto (2 peticiones/s). No rompia nada:
    tardaba 5x mas de lo permitido. Medido en el hueco 704 (23,8 h de BTC, ~500 paginas): 50 min
    sin cerrar nada, con `attempts=1` y cero filas insertadas.

    El cubo puede ir MAS lento que el limite (es una decision, no un fallo), pero nunca mas rapido
    de lo que el endpoint documenta.
    """
    from repair.http import LIMITS

    #: exchange -> (peticiones/segundo segun la documentacion, margen minimo que hay que dejar).
    #: El margen no es opcional: la IP es del usuario y la comparten el backfill, el loader y el
    #: reconciliador. Medido: al 100 % del limite, Binance devolvio -1003 a las 212 peticiones.
    documentado = {
        "binance_um": (2400 / 60 / LIMITS["binance_um"][2], 0.7),  # peso/min / peso peticion
        "okx": (20 / 2, 1.0),                                      # 20 peticiones / 2 s
        "bitget": (10, 1.0),
        "bybit": (600 / 5, 0.5),                                   # 600 / 5 s, aqui 20/s a proposito
        "hyperliquid": (20, 1.0),
    }
    for exchange, (tope, margen) in documentado.items():
        cap, refill, peso_pet = LIMITS[exchange]
        sostenidas = refill / peso_pet
        # Nunca mas rapido que el limite...
        assert sostenidas <= tope + 1e-9, (exchange, sostenidas, "por encima del limite", tope)
        # ...y con margen, porque la IP es compartida.
        assert sostenidas <= tope * margen + 1e-9, (exchange, sostenidas, "sin margen para la IP")
        # Y tiene que ser util: por debajo de 0,5 req/s un hueco de horas no se cierra nunca.
        assert sostenidas >= 0.5, (exchange, sostenidas, "el cubo no deja avanzar la reparacion")


def test_un_429_no_quema_intentos_del_hueco():
    """Un 429 es tan poco culpa del hueco como un 418: es la IP.

    Antes caia en el mismo `except` que los errores HTTP y cerraba el hueco como `partial`
    consumiendo uno de los 5 intentos. Medido en el hueco 704: un unico 429 de Binance lo dejo
    en `partial` con `attempts=1` y 0 filas, cuando el rango es perfectamente reparable y solo
    habia que esperar 4 s.
    """
    from repair.http import RateLimited

    class AdapterQueSeLimita:
        exchange = "binance_um"

        def can_repair(self, g):
            return True, None

        def fetch_trades(self, g):
            raise RateLimited(4.0, "-1003 Too many requests")

    w = Worker.__new__(Worker)
    w.ledger = LedgerFalso()
    w.conn = object()
    w.adaptadores = {"binance_um": AdapterQueSeLimita()}
    g = gap(1000, 2000)
    g.attempts = 2
    assert w.reparar(g) == "fallidos"
    # `release` con `deshacer_intento`, no `finish`: el hueco vuelve a `open` sin gastar intento.
    assert w.ledger.fin[-1][1] == "release", w.ledger.fin[-1]
    assert len(w.ledger.bumps) == 1, "el bump ocurre una vez, antes de la llamada"
    assert w.ledger.intentos_deshechos == 1, "un 429 no debe consumir uno de los 5 intentos"


def test_un_solo_cubo_por_exchange_aunque_lleguen_dos_grafias():
    """"OKX" y "okx" no pueden tener cubos separados: seria el doble de limite para el mismo."""
    from repair.http import LIMITS, Client

    c = Client()
    b1 = c._bucket("OKX")
    b2 = c._bucket("okx")
    assert b1 is b2, "dos cubos para el mismo exchange: el limite esta partido en dos"
    assert len(c.buckets) == 1
    assert b1.capacity == LIMITS["okx"][0]


def test_los_adaptadores_declaran_el_exchange_canonico():
    """Cada adapter escribe bajo su nombre canonico; si no, la reparacion crea otra serie."""
    from repair.adapters import bitget, bybit, hyperliquid, okx

    assert bitget.BitgetAdapter.exchange == "bitget"
    assert bybit.BybitAdapter.exchange == "bybit"
    assert hyperliquid.HyperliquidAdapter.exchange == "hyperliquid"
    assert okx.OKXAdapter.exchange == "okx"
