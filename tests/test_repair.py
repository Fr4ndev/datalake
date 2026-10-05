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


def gap(desde, hasta, dtype="trades", exchange="BINANCE_FUTURES", reason="silence") -> Gap:
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
        span = c["params"]["endTime"] - c["params"]["startTime"]
        assert span <= 30 * 60_000 + 1000, span


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

    def finish(self, gap_id, status, source=None, rows=0, note=None):
        self.fin.append((gap_id, status, rows, note))

    def bump_attempt(self, gap_id):
        return 1

    def release(self, gap_id):
        self.fin.append((gap_id, "release", 0, None))


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
        exchange = "BINANCE_FUTURES"

        def can_repair(self, g):
            return True, None

        def fetch_trades(self, g):
            raise Banned(120.0, "ban")

    w = Worker.__new__(Worker)
    w.ledger = LedgerFalso()
    w.conn = object()
    w.adaptadores = {"BINANCE_FUTURES": AdapterQueFalla()}
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

    exchange = "BINANCE_FUTURES"
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
        filas = lg.list_gaps(limit=50)
        vivas = [f for f in filas if f.exchange == "BINANCE_FUTURES" and f.symbol == "BTCUSDT"
                 and f.dtype == "trades" and base <= f.gap_from_ms <= base + 100_000]
        canonicas = [f for f in vivas if f.status in ("open", "repairing")]
        fundidas = [f for f in vivas if f.status == "merged"]
        assert len(canonicas) == 1, "debe quedar una sola fila viva"
        assert canonicas[0].gap_from_ms == base
        assert canonicas[0].gap_to_ms == base + 30_000, "la fusion debe cubrir los dos rangos"
        assert fundidas, "la fila absorbida debe conservarse como 'merged', no borrarse"
        for f in fundidas:
            assert f.id != canonicas[0].id
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
                     "close, volume) VALUES ('BTCUSDT','TESTEX',"
                     "to_timestamp(1700000000),1,1,1,1,1) ON CONFLICT DO NOTHING")
        # Referencia independiente: el propio Postgres sobre `open_time`. Si `coverage()` preguntase
        # por otra columna, daria otro valor o reventaria.
        cur = conn.execute("SELECT EXTRACT(EPOCH FROM max(open_time))*1000 FROM candles_1m "
                           "WHERE exchange='TESTEX' AND symbol='BTCUSDT'")
        esperado = cur.fetchone()[0]
    lg = GapLedger(DSN)
    lg.open()
    try:
        cov = lg.coverage()
    finally:
        lg.close()
    ms = cov.last_ms.get(("TESTEX", "BTCUSDT", "candles"))
    assert ms is not None, "candles no aparece en la cobertura"
    assert abs(esperado - ms) < 1, (ms, esperado)

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
    w.adaptadores = {"BINANCE_FUTURES": ad}
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
    w.adaptadores = {"BINANCE_FUTURES": _AdaptadorSoloRest()}
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
    w.adaptadores = {"BINANCE_FUTURES": ad}
    w._insertar = _ins
    g = gap(1000, 2000)
    g.attempts = 0
    w.reparar(g)
    assert inserts == ["dump", "rest"], inserts
    assert w.ledger.fin[-1][1] == "repaired"
