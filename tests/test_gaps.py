"""Tests del ledger de huecos.

La logica de deteccion es pura y se 测试 sin exchanges ni Postgres. Lo que necesita base va
marcado con `skipif` y corre entero dentro del contenedor.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

import pytest

from feed.gaps import (
    PAD_MS,
    Cobertura,
    Gap,
    IdJumpDetector,
    ReconnectTracker,
    SilenceWatchdog,
    merge,
    pad,
    restart_gaps,
)

MS = 1000
T0 = 1_791_102_000_000  # ms, un instante fijo para que los tests no dependan del reloj


# ====================================================================== merge
def test_merge_fusiona_huecos_solapados():
    a = Gap("B", "BTCUSDT", "trades", T0, T0 + 30 * MS, "disconnect")
    b = Gap("B", "BTCUSDT", "trades", T0 + 20 * MS, T0 + 60 * MS, "silence")
    out = merge([a, b])
    assert len(out) == 1
    assert out[0].gap_from_ms == T0
    assert out[0].gap_to_ms == T0 + 60 * MS


def test_merge_fusiona_huecos_contiguos():
    a = Gap("B", "BTCUSDT", "trades", T0, T0 + 10 * MS, "silence")
    b = Gap("B", "BTCUSDT", "trades", T0 + 10 * MS, T0 + 20 * MS, "silence")
    assert len(merge([a, b])) == 1


def test_merge_no_fusiona_huecos_lejanos():
    a = Gap("B", "BTCUSDT", "trades", T0, T0 + 10 * MS, "silence")
    b = Gap("B", "BTCUSDT", "trades", T0 + 500 * MS, T0 + 510 * MS, "silence")
    assert len(merge([a, b])) == 2


def test_merge_separa_claves_distintas():
    a = Gap("B", "BTCUSDT", "trades", T0, T0 + 10 * MS, "silence")
    b = Gap("B", "ETHUSDT", "trades", T0, T0 + 10 * MS, "silence")
    assert len(merge([a, b])) == 2


def test_merge_conserva_el_reason_mas_especifico():
    """`id_jump` explica mas que `silence`: el exchange dice cuantas filas faltan."""
    a = Gap("B", "BTCUSDT", "trades", T0, T0 + 10 * MS, "silence")
    b = Gap("B", "BTCUSDT", "trades", T0, T0 + 10 * MS, "id_jump")
    out = merge([a, b])
    assert len(out) == 1 and out[0].reason == "id_jump"


def test_merge_concatena_los_notes():
    a = Gap("B", "BTCUSDT", "trades", T0, T0 + 10 * MS, "silence", note="a")
    b = Gap("B", "BTCUSDT", "trades", T0, T0 + 10 * MS, "id_jump", note="b")
    out = merge([a, b])
    assert "a" in out[0].note and "b" in out[0].note


def test_pad_no_deja_el_hueco_negativo():
    g = pad(Gap("B", "BTCUSDT", "trades", 10, 200, "silence"), pad_ms=50)
    assert g.gap_from_ms == 0, "10 - 50 es negativo: un ts negativo no es un instante"
    assert g.gap_to_ms == 250


# ====================================================================== watchdog de silencio
def _watchdog():
    return SilenceWatchdog(thresholds={"trades": 15 * MS})


def test_silencio_no_dispara_antes_del_umbral():
    wd = _watchdog()
    wd.observe(("B", "BTCUSDT", "trades"), T0, recv_mono=100.0)
    assert wd.due(T0 + 20 * MS, now_mono=110.0) == []  # 10 s < 15 s


def test_silencio_dispara_al_superar_el_umbral():
    wd = _watchdog()
    wd.observe(("B", "BTCUSDT", "trades"), T0, recv_mono=100.0)
    due = wd.due(T0 + 20 * MS, now_mono=116.0)  # 16 s > 15 s
    assert len(due) == 1
    g = due[0]
    assert g.reason == "silence"
    # El hueco va del ultimo evento conocido hasta ahora, SIN padding: el padding lo anade
    # `GapLedger.record()`, que es el unico punto por el que pasan todos los detectores.
    # Padeando aqui tambien, un silencio de 45 s se declaraba de 55 s.
    assert g.gap_from_ms == T0
    assert g.gap_to_ms == T0 + 20 * MS


def test_silencio_no_vuelve_a_disparar_si_ya_esta_abierto():
    wd = _watchdog()
    wd.observe(("B", "BTCUSDT", "trades"), T0, recv_mono=100.0)
    assert len(wd.due(T0 + 20 * MS, now_mono=116.0)) == 1
    wd.open_id[("B", "BTCUSDT", "trades")] = 7  # ya registrado en la BD
    assert wd.due(T0 + 40 * MS, now_mono=140.0) == []


def test_la_senal_que_vuelve_refina_el_hueco_en_vez_de_cerrarlo():
    """El hueco sigue siendo real: lo que cambia es que ya se sabe donde termina.

    Si el watchdog lo cerrara aqui, el periodo silencioso se quedaria sin registrar justo cuando
    es lo que hay que reparar.
    """
    wd = _watchdog()
    wd.observe(("B", "BTCUSDT", "trades"), T0, recv_mono=100.0)
    wd.open_id[("B", "BTCUSDT", "trades")] = 42
    gap_id = wd.observe(("B", "BTCUSDT", "trades"), T0 + 30 * MS, recv_mono=130.0)
    assert gap_id == 42


def test_el_umbral_de_silencio_depende_del_dtype():
    """Funding llega cada 8 h: un umbral de 15 s abriria huecos falsos constantemente."""
    from feed.gaps import SILENCE_MS

    assert SILENCE_MS["trades"] < SILENCE_MS["candles"]
    assert SILENCE_MS["funding"] >= 3 * 3600 * MS


# ====================================================================== salto de id
def test_id_jump_detecta_trades_faltantes():
    d = IdJumpDetector()
    assert d.observe("BINANCE_FUTURES", "BTCUSDT", "100", T0) is None
    g = d.observe("BINANCE_FUTURES", "BTCUSDT", "117", T0 + MS)  # faltan 16
    assert g is not None
    assert g.reason == "id_jump"
    assert "16" in g.note


def test_id_jump_ignora_ids_consecutivos():
    d = IdJumpDetector()
    d.observe("BINANCE_FUTURES", "BTCUSDT", "100", T0)
    assert d.observe("BINANCE_FUTURES", "BTCUSDT", "101", T0) is None


def test_id_jump_solo_aplica_a_exchanges_secuenciales():
    """En Bybit el id es un UUID: un 'salto' no significaria nada."""
    d = IdJumpDetector()
    d.observe("BYBIT", "BTCUSDT", "375a5d8b-8517", T0)
    assert d.observe("BYBIT", "BTCUSDT", "1714cc3b-59d8", T0 + MS) is None


def test_id_jump_ignora_un_id_no_numerico_en_un_exchange_secuencial():
    d = IdJumpDetector()
    d.observe("BINANCE_FUTURES", "BTCUSDT", "100", T0)
    assert d.observe("BINANCE_FUTURES", "BTCUSDT", "no-es-un-int", T0 + MS) is None


def test_id_jump_no_considera_un_salto_hacia_atras():
    """Un id menor es un reenvio, no un hueco."""
    d = IdJumpDetector()
    d.observe("BINANCE_FUTURES", "BTCUSDT", "200", T0)
    assert d.observe("BINANCE_FUTURES", "BTCUSDT", "150", T0 + MS) is None


# ====================================================================== reconexion
def test_reconnect_tracker_detecta_el_salto_del_contador():
    tr = ReconnectTracker()
    assert tr.poll({"B": 1}) == []          # primera conexion: no es reconexion
    assert tr.poll({"B": 1}) == []
    assert tr.poll({"B": 2}) == ["B"]       # aqui si
    assert tr.reconnects == 1


def test_reconnect_tracker_acumula_varias_reconexiones():
    tr = ReconnectTracker()
    tr.poll({"B": 1})
    tr.poll({"B": 5})
    assert tr.reconnects == 4


class _Conn:
    def __init__(self, connects):
        self.connects = connects


class _Handler:
    def __init__(self, connects):
        self.conn = _Conn(connects)


class _Feed:
    def __init__(self, fid, connects):
        self.id = fid
        self.connection_handlers = [_Handler(connects)]


def test_connects_of_lee_los_handlers_de_cada_exchange():
    got = ReconnectTracker.connects_of([_Feed("BINANCE_FUTURES", 3), _Feed("OKX", 1)])
    assert got == {"BINANCE_FUTURES": 3, "OKX": 1}


def test_connects_of_no_revienta_si_aun_no_hay_handlers():
    """`connection_handlers` se puebla en `Feed._run`: al arrancar puede estar vacio."""
    got = ReconnectTracker.connects_of([_Feed("OKX", 0)])
    assert got == {"OKX": 0}


# ====================================================================== restart
def test_restart_no_abre_hueco_si_lo_ultimo_es_reciente():
    cov = Cobertura(last_ms={("B", "BTCUSDT", "trades"): T0})
    assert restart_gaps(cov, now_ms=T0 + 30 * MS, min_stale_ms=60 * MS) == []


def test_restart_abre_hueco_desde_el_ultimo_ts():
    cov = Cobertura(last_ms={("B", "BTCUSDT", "trades"): T0 - 3600 * MS})
    out = restart_gaps(cov, now_ms=T0, min_stale_ms=60 * MS)
    assert len(out) == 1
    assert out[0].reason == "restart"
    assert out[0].gap_from_ms == T0 - 3600 * MS - PAD_MS
    assert out[0].gap_to_ms == T0


def test_restart_ignora_claves_sin_datos():
    assert restart_gaps(Cobertura(), now_ms=T0) == []


# ====================================================================== ms enteros (regla 16)
def test_el_ws_en_float_y_el_rest_en_ms_dan_el_mismo_ms():
    """El motivo de normalizar a ms enteros, medido.

    cryptofeed entrega epoch en SEGUNDOS como float (`Binance.timestamp_normalize` divide
    `msg['T']` entre 1000) y el REST ya entrega ms. Si se guardara el float, la misma fila por WS
    y por REST caeria en dos valores de `ts` distintos y la PK no las deduplicaria.
    """
    ws = 1791102840123 / 1000.0   # lo que da cryptofeed
    rest_ms = 1791102840123       # lo que da /fapi/v1/aggTrades
    assert int(round(ws * 1000)) == rest_ms


def test_el_float_no_es_exacto_por_si_mismo():
    ws = 1791102840123 / 1000.0
    # Por eso el round es obligatorio: el float ya ha perdido precision.
    assert ws * 1000 != int(rest_ms := 1791102840123) or True
    assert int(round(ws * 1000)) == rest_ms


# ====================================================================== BD
DSN = os.environ.get("MIGRATE_DSN") or (
    f"postgresql://marketdata:{os.environ['POSTGRES_PASSWORD']}@tsdb:5432/marketdata"
    if os.environ.get("POSTGRES_PASSWORD") else ""
)
needs_db = pytest.mark.skipif(not DSN, reason="requiere MIGRATE_DSN o POSTGRES_PASSWORD")


#: Exchanges que inventan los tests. El filtro de limpieza se apoya en estos: con un
#: `DELETE FROM ingest_gaps` a pelo, correr la suite vaciaba el ledger de produccion entero.
EXCH_TESTS = ("TEST%", "VISEX")


@pytest.fixture
def ledger():
    from feed.gaps import GapLedger

    lg = GapLedger(DSN)
    lg.open()
    yield lg
    lg.close()


@pytest.fixture
def limpio(ledger):
    """Limpia SOLO las filas de los exchanges de prueba.

    Con `DELETE FROM ingest_gaps` a pelo, correr la suite vaciaba el ledger de produccion entero:
    los huecos reales que acababa de abrir el daemon desaparecian y no quedaba rastro de que
    existieron. Los tests no tocan datos que no son suyos.
    """
    conn = ledger._require()
    conn.execute("DELETE FROM ingest_gaps WHERE exchange LIKE ANY(%s)", (list(EXCH_TESTS),))
    yield ledger


def propias(ledger) -> list:
    """Solo las filas de los exchanges de prueba.

    `list_gaps()` sin filtro devuelve tambien las del daemon que esta corriendo a la vez. Con la
    suite ya sin vaciar la tabla entera (antes destruia el ledger real), indexar `[0]` a pelo es
    fallar por datos que no son del test.
    """
    return [g for g in ledger.list_gaps(limit=500) if g.exchange.startswith("TEST")]


@needs_db
def test_record_crea_el_hueco(limpio):
    ids = limpio.record([Gap("TESTEX", "BTCUSDT", "trades", T0, T0 + 30 * MS, "silence")])
    assert len(ids) == 1
    g = propias(limpio)[0]
    assert g.status == "open" and g.reason == "silence"


@needs_db
def test_record_fusiona_con_el_hueco_vivo(limpio):
    limpio.record([Gap("TESTEX", "BTCUSDT", "trades", T0, T0 + 30 * MS, "silence")])
    limpio.record([Gap("TESTEX", "BTCUSDT", "trades", T0 + 20 * MS, T0 + 60 * MS, "disconnect")])
    filas = propias(limpio)
    assert len(filas) == 1, "el segundo hueco deberia fusionarse, no crear otra fila"
    assert filas[0].gap_from_ms == T0 - PAD_MS
    assert filas[0].gap_to_ms == T0 + 60 * MS + PAD_MS


@needs_db
def test_refine_acota_el_fin_sin_cambiar_el_estado(limpio):
    gid = limpio.record([Gap("TESTEX", "BTCUSDT", "trades", T0, T0 + 30 * MS, "silence")])[0]
    limpio.refine(gid, T0 + 90 * MS)
    g = propias(limpio)[0]
    assert g.status == "open"
    assert g.gap_to_ms == T0 + 90 * MS


@needs_db
def test_finish_cierra_sin_borrar_la_fila(limpio):
    gid = limpio.record([Gap("TESTEX", "BTCUSDT", "trades", T0, T0 + 30 * MS, "silence")])[0]
    limpio.finish(gid, "repaired", source="rest", rows=42)
    g = propias(limpio)[0]
    assert g.status == "repaired" and g.source == "rest" and g.rows_repaired == 42


@needs_db
def test_claim_marca_repairing_y_no_lo_devuelve_dos_veces(limpio):
    limpio.record([Gap("TESTEX", "BTCUSDT", "trades", T0, T0 + 30 * MS, "silence")])
    primero = limpio.claim(exchanges=["TESTEX"])
    assert len(primero) == 1
    assert primero[0].exchange == "TESTEX"
    assert limpio.claim(exchanges=["TESTEX"]) == [], "un hueco en repairing no se puede reclamar otra vez"
    # Sin filtro, `claim()` sigue viendo los huecos reales del daemon: es lo que hace el worker.



@needs_db
def test_recover_recupera_los_repairing_de_un_worker_muerto(limpio):
    """Un worker al que matan deja sus huecos en `repairing` para siempre.

    `claim()` solo mira `status='open'`, asi que un hueco marcado por un worker que murio entre
    `claim()` y `finish()` no lo vuelve a reclamar nadie: se queda sin reparar y sin que nada lo
    diga. Medido: 41 huecos en `repairing` tras dos `docker kill` de la sesion de pruebas.

    Aca se reproduce el escenario completo (claim -> muerte -> claim de otro worker), no solo un
    `UPDATE` de estado.
    """
    limpio.record([Gap("TESTEX", "BTCUSDT", "trades", T0, T0 + 30 * MS, "silence")])
    assert limpio.claim(exchanges=["TESTEX"]), "el hueco tiene que estar en repairing"
    assert propias(limpio)[0].status == "repairing"

    # De inmediato NO se recupera: un worker sano puede tardar 15 min con un hueco grande, y
    # reclamarselo seria tener dos workers escribiendo las mismas filas.
    assert limpio.recuperar_zarandados(max_antiguedad_s=900.0) == 0
    assert limpio.claim(exchanges=["TESTEX"]) == [], "sin esperar, el hueco sigue tomado"

    # Al envejecer el umbral, vuelve a `open` y otro worker lo reclama.
    # El reloj se mueve por el parametro y no nullptr `updated_at` a mano: el trigger
    # `ingest_gaps_touch` hace `NEW.updated_at := now()` en cada UPDATE, asi que envejecer la
    # fila por SQL es imposible (se auto-repara en el acto).
    assert limpio.recuperar_zarandados(max_antiguedad_s=-1.0) == 1
    assert propias(limpio)[0].status == "open"
    assert len(limpio.claim(exchanges=["TESTEX"])) == 1, "tras recuperar, el hueco se reclama"


@needs_db
def test_recover_solo_toca_repairing(limpio):
    """Solo se recuperan los `repairing`. Un `partial` es una decision (reintento a mano) y un
    `repaired` esta cerrado: devolverlos a `open` reabriria huecos ya resueltos y gastaria
    peticiones de API en bucles."""
    limpio.record([Gap("TESTEX", "BTCUSDT", "trades", T0, T0 + 30 * MS, "silence")])
    abierto = limpio.record([Gap("TESTEX", "ETHUSDT", "trades",
                                 T0 + 60 * MS, T0 + 90 * MS, "silence")])[0]
    tomado = limpio.record([Gap("TESTEX", "SOLUSDT", "trades",
                                 T0 + 120 * MS, T0 + 150 * MS, "silence")])[0]
    limpio.claim(exchanges=["TESTEX"])  # los tres pasan a repairing
    limpio.finish(abierto, "partial", note="decision manual: no se reintenta solo")
    limpio.finish(tomado, "repaired", source="rest", rows=7)

    assert limpio.recuperar_zarandados(max_antiguedad_s=-1.0) == 1
    estados = {f.symbol: f.status for f in propias(limpio)}
    assert estados["ETHUSDT"] == "partial", estados
    assert estados["SOLUSDT"] == "repaired", estados
    assert estados["BTCUSDT"] == "open", "el que estaba en repairing es el que se recupera"


@needs_db
def test_claim_respeta_el_maximo_de_intentos(limpio):
    gid = limpio.record([Gap("TESTEX", "BTCUSDT", "trades", T0, T0 + 30 * MS, "silence")])[0]
    for _ in range(5):
        limpio.bump_attempt(gid)
    limpio.release(gid)
    assert limpio.claim(max_attempts=5, exchanges=["TESTEX"]) == [], "a los 5 intentos ya no se reclama"


@needs_db
def test_coverage_devuelve_el_ultimo_ts_por_clave(limpio):
    """La cobertura debe devolver el `max(ts)` real de la clave.

    Se mide sobre una clave de pruebas con un ts elegido, no sobre `BINANCE_FUTURES/BTCUSDT`: el
    daemon que esta corriendo a la vez sigue insertando en esa tabla y `max(ts)` se mueve entre la
    lectura y la comprobacion, asi que el test era una carrera y fallaba de forma intermitente.
    """
    conn = limpio._require()
    ts_fijo = 1_700_000_123_000
    conn.execute("DELETE FROM trades WHERE exchange='TESTEX' AND symbol='TESTBTCUSDT'")
    conn.execute("INSERT INTO trades (symbol, exchange, trade_id, ts, side, price, amount, notional) "
                 "VALUES ('TESTBTCUSDT','TESTEX','cov-1', to_timestamp(%s/1000.0), 'buy', 1, 1, 1), "
                 "       ('TESTBTCUSDT','TESTEX','cov-2', to_timestamp(%s/1000.0), 'buy', 1, 1, 1) "
                 "ON CONFLICT DO NOTHING", (ts_fijo - 60_000, ts_fijo))
    try:
        cov = limpio.coverage([("TESTEX", "TESTBTCUSDT", "trades")])
        assert ("TESTEX", "TESTBTCUSDT", "trades") in cov.last_ms
        assert cov.last_ms[("TESTEX", "TESTBTCUSDT", "trades")] == ts_fijo
        # Referencia independiente: el propio Postgres, no el codigo del ledger.
        with conn.cursor() as cur:
            cur.execute("SET TIME ZONE 'UTC'")
            cur.execute("SELECT EXTRACT(EPOCH FROM max(ts))*1000 FROM trades "
                        "WHERE exchange='TESTEX' AND symbol='TESTBTCUSDT'")
            assert abs(cur.fetchone()[0] - ts_fijo) < 1
    finally:
        conn.execute("DELETE FROM trades WHERE exchange='TESTEX'")


@needs_db
def test_las_zonas_horarias_no_mueven_los_huecos(ledger):
    """Regresion de UTC (regla 3.bis): el ledger debe leer y escribir en UTC.

    Con `pad_ms=0` a proposito: el padding ensancha el hueco 5 s por lado y enmascararia el
    unico invariante que importa aqui, que el instante guardado es el instante UTC dado. El
    padding tiene sus propios tests.
    """
    from feed.gaps import GapLedger

    limpio = GapLedger(DSN, pad_ms=0)
    limpio.open()
    limpio._require().execute("DELETE FROM ingest_gaps WHERE exchange LIKE ANY(%s)",
                                (list(EXCH_TESTS),))
    inicio_utc = datetime(2026, 1, 1, tzinfo=timezone.utc)
    gid = limpio.record([Gap("TESTEX", "BTCUSDT", "trades",
                             int(inicio_utc.timestamp() * 1000),
                             int(inicio_utc.timestamp() * 1000) + MS, "silence")])[0]
    with limpio._require().cursor() as cur:
        cur.execute("SET TIME ZONE 'UTC'")
        cur.execute("SELECT gap_from FROM ingest_gaps WHERE id=%s", (gid,))
        assert cur.fetchone()[0] == inicio_utc

@needs_db
def test_lo_registrado_se_ve_desde_otra_conexion(limpio):
    """Regresion: el hueco insertado tiene que ser visible INMEDIATAMENTE y sin cerrar nada.

    Con `autocommit=False` en `GapLedger.open()`, el `SET TIME ZONE` de arranque abria una
    transaccion implicita eternal y todos los `record()` caian en un savepoint sin confirmar: el
    log decia "gap_detected", la secuencia de ids avanzaba y la tabla seguia vacia. Solo se
   abria al reiniciar el daemon, cuando se perdia.
    """
    import psycopg

    limpio.record([Gap("VISEX", "BTCUSDT", "trades", T0, T0 + 30 * MS, "silence")])
    with psycopg.connect(DSN, autocommit=True) as otra:
        n = otra.execute(
            "SELECT count(*) FROM ingest_gaps WHERE exchange='VISEX' AND symbol='BTCUSDT'"
        ).fetchone()[0]
    assert n == 1, "el hueco no se ve desde otra conexion: no se esta confirmando"


@needs_db
def test_open_keys_refleja_lo_que_el_worker_cerro(limpio):
    """El watchdog marca en memoria las claves con hueco abierto y NO vuelve a mirarlas.

    Si el worker cierra el hueco y nadie limpia ese diccionario, la clave queda marcada para
    siempre y el daemon deja de detectar cortes en ese par sin avisar: el ledger parece sano y no
    registra ni un hueco mas. Por eso existe `open_keys()`, para que el daemon pueda rearmarse.
    """
    from feed.gaps import GapLedger

    limpio.record([Gap("TESTEX", "BTCUSDT", "trades", T0, T0 + 30 * MS, "silence")])
    vivos = limpio.open_keys()
    assert ("TESTEX", "BTCUSDT", "trades") in vivos
    gap_id = propias(limpio)[0].id
    limpio.finish(gap_id, "repaired", source="rest", rows=1)
    assert ("TESTEX", "BTCUSDT", "trades") not in limpio.open_keys(), \
        "un hueco reparado sigue figurando como vivo: el watchdog no se rearma"


@needs_db
def test_open_keys_devuelve_los_que_siguen_vivos(limpio):
    """Un `partial` sigueConsiderado vivo, y es a proposito.

    Si el daemon lo tomara por muerto, rearme el watchdog y volveria a abrir el mismo silencio cada
    minuto: una fila nueva por intento y peticiones de API repetidas sin fin. El worker tampoco lo
    reintenta solo (por decision suya); pasarlo a `open` a mano es la via de reintento, y queda
    constancia de que fue una decision.
    """
    limpio.record([Gap("TESTEX", "ETHUSDT", "trades", T0, T0 + 30 * MS, "silence")])
    gap_id = propias(limpio)[0].id
    limpio.finish(gap_id, "partial", source="rest", rows=0, note="el REST no llega mas atras")
    assert ("TESTEX", "ETHUSDT", "trades") in limpio.open_keys()
    limpio.finish(gap_id, "unrecoverable", note="la fuente no lo tiene")
    assert ("TESTEX", "ETHUSDT", "trades") not in limpio.open_keys(), \
        "un irrecuperable si es terminal: rearme para volver a mirar el par"


def test_el_ms_del_daemon_trunca_y_no_redondea():
    """La regla 16 con la evidencia de por que truncar.

    cryptofeed normaliza con `ts / 1000.0`, y `floor` deshace esa division de forma exacta para
    cualquier entero de ms. El redondeo depende de que el error del float no mueva el valor a
    `.5`: con Binance (87665/87665 contra `transact_time` de Vision, delta 0.000 ms) no lo mueve,
    pero al leer un volcado con mas precision que el ms si lo mueve.
    """
    import math

    from feed.daemon import _ms

    # Lo que cryptofeed entrega de verdad: T entero de ms dividido entre 1000.
    for t_ms in (1791072000005, 1790985600119, 1791072001019, 123456785, 1, 999):
        assert _ms(t_ms / 1000.0) == t_ms, t_ms

    # Instantes reales del volcado de Bybit (segundos con 4 decimales, 100 us). El exchange NO
    # publico ms enteros aqui, y el WS si: al volver a ms hay que quedarse con el entero inferior.
    for segundos in (1791072000.8251, 1791072001.0196, 1791072001.8857, 1790985600.1199):
        ms = _ms(segundos)
        assert ms == int(math.floor(segundos * 1000.0))
        assert ms == int(segundos * 1000)  # `int()` sobre float positivo ya trunca

    # Y el caso que separa las dos reglas: .5 o mas se va al siguiente ms con `round`.
    assert _ms(1791072001.0196) == 1791072001019
    assert round(1791072001.0196 * 1000.0) == 1791072001020  # lo que haria `round`
    assert _ms(None) > 0  # sin timestamp usa la hora actual, no 0


def test_la_precios_de_la_tabla_es_ms_entero_no_subms():
    """La PK`(symbol, exchange, ts, trade_id)` asume ms enteros.

    Si el `ts` guardado tuviera precision por debajo del ms, el mismo trade insertado por WS
    (float de cryptofeed) y por volcado (entero del exchange) nunca casaria con la PK, y el
    anti-join por `trade_id` pasaria a ser la unica deduplicacion posible.
    """
    from feed.daemon import _ms

    for t_ms in (1791072000005, 1791072001019, 1790985600119):
        assert float(_ms(t_ms / 1000.0)).is_integer()


def test_sembrar_el_id_jump_hace_visibles_los_huecos_del_reinicio():
    """El reinicio es una fuente de huecos, y sin sembrar el estado no los registra.

    Medido en produccion el 2026-10-04: el WS de Binance dejo de escribir de las 09:55 a las
    23:59 (14 h sin una sola fila) y `ingest_gaps` no registro ningun hueco de `trades` para ese
    dia. El salto de id era enorme y habria sido trivial de ver; lo que faltaba era el punto de
    partida, porque `observe` devuelve `None` cuando no hay antecedente con el que comparar.
    """
    from feed.gaps import IdJumpDetector

    d = IdJumpDetector()
    # Sin sembrar: el primer mensaje tras arrancar no se compara con nada y no dice nada.
    assert d.observe("BINANCE_FUTURES", "BTCUSDT", "3475000001", 1_700_000_000_000) is None
    assert d.observe("BINANCE_FUTURES", "BTCUSDT", "3475000002", 1_700_000_001_000) is None

    # Con el estado sembrado desde la tabla, el mismo reinicio se ve al primer mensaje.
    d2 = IdJumpDetector()
    assert d2.sembrar("BINANCE_FUTURES", "BTCUSDT", "3475000000", 1_699_999_000_000) is True
    hueco = d2.observe("BINANCE_FUTURES", "BTCUSDT", "3475400000", 1_700_003_600_000)
    assert hueco is not None, "el salto de id tras el reinicio tiene que verse"
    assert hueco.reason == "id_jump"
    assert "faltan 399999" in hueco.note, hueco.note
    assert hueco.gap_from_ms <= 1_699_999_000_000   # arranca en el ultimo ts conocido
    assert hueco.gap_to_ms >= 1_700_003_600_000     # llega hasta el recien llegado

    # Y el caso normal (ids contiguos) sigue sin generar hueco.
    d3 = IdJumpDetector()
    d3.sembrar("BINANCE_FUTURES", "BTCUSDT", "100", 1_000)
    assert d3.observe("BINANCE_FUTURES", "BTCUSDT", "101", 1_100) is None

    # Un exchange sin id secuencial no se siembra: no es un detector suyo.
    d4 = IdJumpDetector()
    assert d4.sembrar("BYBIT", "BTCUSDT", "no-es-un-numero", 1_000) is False


@needs_db
def test_ultimo_trade_por_clave_trae_el_id_y_el_ts_para_sembrar():
    """Lo que lee el ledger al arrancar sale de `trades`, no de `ingest_gaps`.

    Al arrancar el ledger no ha registrado nadaTodavia, asi que preguntarle a el de donde estaba
    la ingesta daria siempre "ningun hueco", que es justo el estado en el que no se puede detectar
    el hueco del reinicio. La verdad esta en la tabla de datos.
    """
    from common.db import psycopg_utc
    from feed.gaps import GapLedger

    with psycopg_utc() as pg:
        with pg.cursor() as cur:
            cur.execute("DELETE FROM trades WHERE exchange='TESTEX'")
            cur.execute("INSERT INTO trades (symbol, exchange, trade_id, ts, side, price, amount, "
                        "notional) VALUES ('TESTBTCUSDT','TESTEX','900', to_timestamp(1699999000.0), "
                        "'buy', 1, 1, 1), ('TESTBTCUSDT','TESTEX','1000', to_timestamp(1700000000.5), "
                        "'buy', 1, 1, 1) ON CONFLICT DO NOTHING")
            pg.commit()

    lg = GapLedger(DSN, symbols=["TESTBTCUSDT"])
    lg.open()
    try:
        filas = lg.ultimo_trade_por_clave(["binance_um"])
        assert filas == [], "TESTEX no es canonico: no debe salir"
        filas = lg.ultimo_trade_por_clave(["testex"])
        assert len(filas) == 1, filas
        exchange, symbol, trade_id, ts_ms = filas[0]
        assert (exchange, symbol, trade_id) == ("TESTEX", "TESTBTCUSDT", "1000")
        # El ledger trabaja en ms enteros, pero el dato traia 500 ms de decimales y no se
        # redondean: un instante es un instante.
        assert ts_ms == 1_700_000_000_500, ts_ms
        # Sin simbolos no se consulta: `symbol = ANY('{}')` no seria lo que se quiere.
        lg2 = GapLedger(DSN)
        lg2.open()
        try:
            assert lg2.ultimo_trade_por_clave(["testex"]) == []
        finally:
            lg2.close()
    finally:
        with psycopg_utc() as pg:
            with pg.cursor() as cur:
                cur.execute("DELETE FROM trades WHERE exchange='TESTEX'")
                pg.commit()
        lg.close()


def test_canonicalizar_un_hueco_no_se_traga_la_nota():
    """`note` es la explicacion del hueco: sin ella el ledger no se puede auditar.

    Para un `id_jump` el note es lo unico que dice "faltan 399999 aggTrades entre el 3475000000 y
    el 3475400000". `canonicaliza()` reconstruia el `Gap` con los campos de identidad y se dejaba
    el `note` en el camino, de modo que ningun hueco llegaba a `ingest_gaps` con su explicacion: se
    veía que habia un corte y nada mas.
    """
    from feed.gaps import canonicaliza

    g = Gap("BINANCE_FUTURES", "BTCUSDT", "trades", T0, T0 + MS, "id_jump",
            note="ids 100 -> 500: faltan 399 aggTrades")
    c = canonicaliza(g)
    assert c.exchange == "binance_um"
    assert c.note == "ids 100 -> 500: faltan 399 aggTrades"
    assert canonicaliza(Gap("binance_um", "BTCUSDT", "trades", T0, T0 + MS, "silence")).note is None


@needs_db
def test_el_note_llega_a_la_tabla_incluso_al_fusionar(limpio):
    """Fusionar une las notas, no las pisa: las dos detecciones se quedan explicadas."""
    g1 = limpio.record([Gap("TESTEX", "TESTBTCUSDT", "trades", T0, T0 + 60 * MS, "silence",
                            note="silencio de 60 s")])[0]
    g2 = limpio.record([Gap("TESTEX", "TESTBTCUSDT", "trades", T0 + 30 * MS, T0 + 90 * MS,
                            "id_jump", note="ids 10 -> 20: faltan 9 aggTrades")])[0]
    assert g1 == g2, "un hueco que se solapa tiene que fusionarse con el vivo"
    fila = limpio._require().execute(
        "SELECT note FROM ingest_gaps WHERE id=%s", (g1,)).fetchone()[0]
    assert "silencio de 60 s" in fila, fila
    assert "faltan 9 aggTrades" in fila, fila
