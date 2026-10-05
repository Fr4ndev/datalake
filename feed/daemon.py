"""Feed-daemon: cryptofeed -> TimescaleDB, en vivo.

Por que esto existe y no un backfill mas: hay canales que **no se pueden recuperar**. Ni Binance
Vision ni ningun historico da ticks de trades, ni liquidaciones completas (el WS de Binance es
parcial, ~1 de cada 20 desde 2021), ni OKX/Bitget/Hyperliquid en profundidad. Cada hora sin daemon
es una hora perdida para siempre, por eso esto es lo prioritario de la Fase 2 y el loader va
despues.

Reconexion y backoff NO se reinventan: los hace `cryptofeed.FeedHandler` (ver skill
cryptofeed-timescale-adapter). Aqui solo se conectan callbacks, se vuelca a un buffer y se assures
el flush.

Firmas de callback verificadas en cryptofeed 3.0.1 (tag v3.0.1, commit 7714c10):
se invocan como `await cb(obj, receipt_timestamp)` (cryptofeed/feed.py:478).
"""

from __future__ import annotations

from common.exchanges import canonico

import asyncio
import time
from datetime import datetime, timezone

from cryptofeed import FeedHandler

from bulk.logfmt import log

from common.db import conninfo

from .config import FeedConfig, load
from .gaps import Gap, GapLedger, IdJumpDetector, ReconnectTracker, SilenceWatchdog
from .writer import Store, to_utc

#: Cada cuanto se sondean los detectores. 5 s es mucho mas corto que el umbral de trades (15 s)
#: para que un hueco se abra a tiempo, y bastante mas largo que el bucle de flush (1 s).
GAP_POLL_S = 5.0
#: Reconexion: por debajo de esto es ruido de red y no se declara hueco. Por encima, el hueco se
#: mide desde el ULTIMO evento real de esa clave, no desde "ahora - X": si el corte duro 5 s, el
#: hueco son 5 s, no 30.
RECONNECT_MIN_MS = 2_000
#: Cada cuanto se re-sincroniza el watchdog con el ledger (en sondeos, no en segundos: el bucle es
#: de 5 s). 12 sondeos = 1 min.
SYNC_CADA = 12

#: exchange (nuestro nombre) -> clase de cryptofeed.
EXCHANGE_CLASSES = {
    "binance_futures": "BinanceFutures",
    "bybit": "Bybit",
    "okx": "OKX",
    "bitget": "Bitget",
    "hyperliquid": "Hyperliquid",
}


def _ms(timestamp: float | None) -> int:
    """Timestamp de cryptofeed (float en SEGUNDOS) -> ms enteros UTC. Regla 16.

    Se trunca, no se redondea. cryptofeed normaliza dividiendo entre 1000
    (`Binance.timestamp_normalize: return ts / 1000.0`), y truncar es la operacion inversa
    exacta: `floor((T/1000)*1000) == T` para cualquier `T` entero de ms. Redondear tambien
    funciona con `T` entero, pero depende de que el error del float no empuje el valor a `.5`,
    y medido no depende: con Binance, 87665/87665 filas coinciden con `transact_time` de
    data.binance.vision, con delta 0.000 ms.

    Donde el redondeo si falla es al LEER un volcado con mas precision que el milisegundo. Bybit
    publica segundos con 4 decimales (100 us). Medido sobre el dia 2026-10-04, 91747 trades
    presentes en WS y en el volcado:

        WS == floor(volcado * 1000) : 91747/91747  (100.00%)
        WS == round(volcado * 1000) : 49412/91747  ( 53.86%)

    Es decir, con `round` el 46.14% de las filas queda 1 ms por encima del valor que trae el WS, y
    la PK`(symbol, exchange, ts, trade_id)` ya no las reconoce como el mismo trade. Una sola regla
    en todo el pipeline, la que deshace la normalizacion de cryptofeed.
    """
    if timestamp is None:
        return int(time.time() * 1000)
    return int(float(timestamp) * 1000)


class Daemon:
    def __init__(self, cfg: FeedConfig | None = None):
        self.cfg = cfg or load()
        self.store = Store(max_rows=self.cfg.buffer_size)
        self.stop = asyncio.Event()
        self.started = time.monotonic()
        #: Ultimo ts recibido por (exchange, symbol): lo usa el watchdog.
        self.last_seen: dict[tuple[str, str], float] = {}
        # Deteccion de huecos. Los detectores son puros y se prueban sin base; el ledger va contra
        # Postgres. Si la base no esta, el daemon NO se cae: sigue recogiendo datos y avisa,
        # porque la ingesta tiene prioridad sobre la deteccion (pero avisar, que callarse no).
        self.watchdog = SilenceWatchdog()
        self.id_jump = IdJumpDetector()
        self.reconnects = ReconnectTracker()
        self.ledger: GapLedger | None = None
        self.feedhandler: FeedHandler | None = None
        #: Claves (exchange, symbol, dtype) que este proceso vigila. Se rellena en `build()` y no
        #: cambia despues. NO se interroga a cryptofeed en caliente: `Feed.symbols` y
        #: `Feed.channels` son METODOS en 3.0.1, no atributos, y `getattr(feed, "symbols", [])`
        #: devuelve la funcion, que no es iterable.
        self.feeds_vigilados: list[tuple[str, str, str]] = []
        self._sync_n = 0
        #: Objetos `Feed` reales: el sondeo de reconexion tiene que leer `feed.conn.connects`, y
        #: eso solo existe en el objeto, no en la tupla de la clave.
        self.feeds: list[object] = []
        #: Refinamientos pendientes: `observe()` es sincrono y esta en el camino de la WS, asi que
        #: solo anota y los aplica `_gaps_loop`.
        self.refines: list[tuple[int, int]] = []

    # ---------------------------------------------------------------- callbacks
    async def _cb_trade(self, trade, receipt):
        self.store.add_trade(trade, receipt)
        ts_ms = _ms(trade.timestamp)
        symbol = self.store._sym(trade.exchange, trade.symbol)
        self._seen(trade.exchange, symbol, "trades", ts_ms)
        # Salto de id de Binance: `aggTrade` numera de forma correlativa, asi que un hueco en la
        # numeracion es un hueco de DATOS. En el resto de exchanges el id no es monotono y este
        # detector no aplica (SEQUENTIAL_ID_EXCHANGES solo lleva Binance).
        hueco = self.id_jump.observe(trade.exchange, symbol, str(trade.id), ts_ms)
        if hueco is not None:
            self._registrar([hueco], reason="id_jump")
        # La latencia trade->fila NO se mide aqui: la mide `Store.flush` restando el
        # `time.monotonic()` de la llegada (que `add_trade` guarda por fila) del instante del
        # volcado. Antes se restaba la llegada del tick anterior, que es el intervalo entre
        # trades: un numero que se parece al p95 pero no mide nada.

    async def _cb_funding(self, funding, receipt):
        self.store.add_funding(funding, receipt)
        self._seen(funding.exchange, self.store._sym(funding.exchange, funding.symbol),
                   "funding", _ms(funding.timestamp))

    async def _cb_open_interest(self, oi, receipt):
        self.store.add_open_interest(oi, receipt)
        self._seen(oi.exchange, self.store._sym(oi.exchange, oi.symbol),
                   "open_interest", _ms(oi.timestamp))

    async def _cb_liquidation(self, liq, receipt):
        self.store.add_liquidation(liq, receipt)
        self._seen(liq.exchange, self.store._sym(liq.exchange, liq.symbol),
                   "liquidations", _ms(liq.timestamp))

    async def _cb_candle(self, candle, receipt):
        self.store.add_candle(candle, receipt)
        self._seen(candle.exchange, self.store._sym(candle.exchange, candle.symbol),
                   "candles", _ms(candle.timestamp))

    def _seen(self, exchange: str, symbol: str, dtype: str, ts_ms: int) -> None:
        """Marca senal recibida con su instante REAL de exchange, no la hora de llegada.

        El watchdog compara contra tiempo de recepcion para el umbral y guarda el ts del exchange
        para los bordes del hueco: si no, un reloj desfasado se comeria 5 s de datos en el borde.
        """
        self.last_seen[(exchange, symbol)] = time.time()
        if not symbol:
            return
        gap_id = self.watchdog.observe((exchange, symbol, dtype), ts_ms)
        if gap_id is not None:
            self.refines.append((gap_id, ts_ms))

    # ---------------------------------------------------------------- huecos
    def _registrar(self, huecos: list[Gap], reason: str = "") -> list[int]:
        """Vuelca huecos al ledger. Nunca rompe el data path."""
        if self.ledger is None or not huecos:
            return []
        try:
            ids = self.ledger.record(huecos)
        except Exception as exc:  # noqa: BLE001
            log(component="feed", event="gap_record_error", reason=reason, error=str(exc)[:160])
            return []
        if ids:
            log(component="feed", event="gap_detected", reason=reason, count=len(ids),
                ids=",".join(str(i) for i in ids),
                exchanges=",".join(sorted({g.exchange for g in huecos})),
                dtypes=",".join(sorted({g.dtype for g in huecos})))
        return ids

    def _abrir_ledger(self) -> None:
        """Conecta el ledger y registra los huecos de arranque (`reason='restart'`).

        Un hueco de arranque es el que existe antes de que empiece este proceso: si lo ultimo que
        hay en la base era de hace 4 h, esos 4 h no los va a rellenar nunca el WS, porque el WS
        solo empieza que empieza a contarse desde ahora.
        """
        from .gaps import restart_gaps

        try:
            self.ledger = GapLedger(conninfo(), symbols=self.cfg.symbols)
            self.ledger.open()
        except Exception as exc:  # noqa: BLE001
            self.ledger = None
            log(component="feed", event="gap_ledger_off", error=str(exc)[:200])
            return
        # Solo las claves que este proceso escucha de verdad. Sin este filtro se declaran huecos
        # para filas del lake historico (exchange en minuscula, p.ej. `binance`), que ningun WS va
        # a seguir alimentando: eso es el problema de canonicalizacion, no un corte de ingesta, y el
        # worker no tendria nada que reparar.
        claves = self._claves_vigiladas()
        try:
            cov = self.ledger.coverage(keys=claves or None)
        except Exception as exc:  # noqa: BLE001
            log(component="feed", event="gap_coverage_error", error=str(exc)[:200])
            return
        huecos = restart_gaps(cov, int(time.time() * 1000))
        self._registrar(huecos, reason="restart")
        log(component="feed", event="gap_coverage", claves=len(claves),
            con_datos=len(cov.last_ms), huecos_arranque=len(huecos))

    def _claves_vigiladas(self) -> list[tuple[str, str, str]]:
        """Devuelve la lista de (exchange, symbol, dtype) que este proceso vigila.

        Se construye durante `build()` y no cambia despues, asi que es seguro y no requiere
        interrogar a cryptofeed en tiempo de ejecucion.
        """
        return self.feeds_vigilados

    def _sembrar_id_jump(self) -> None:
        """Carga en el detector de salto de id el ultimo trade ya guardado por clave.

        El reinicio es, en si mismo, una fuente de huecos: si el proceso estuvo parado un rato,
        al volver el `id` siguiente salta y ese tramo no existe en la tabla. Sin sembrar, el primer
        mensaje tras arrancar no tiene antecedente contra el que compararse y el hueco se pierde
        sin dejar rastro en `ingest_gaps`. Es el caso mas caro de perder deteccion: el ledger
        parece sano porque no tiene nada, y no tiene nada porque nadie lo miraba.
        """
        if self.ledger is None:
            return
        try:
            # `cfg.symbols` son los nombres cortos de cryptofeed (`BTC`), y en la tabla el simbolo
            # es el perpetuo normalizado (`BTCUSDT`). Se usa la misma funcion que el writer para no
            # tener dos reglas de nombres: preguntando por `BTC` no sale ninguna fila y la siembra se
            # queda vacia sin decir nada, que es el fallo que se quiere evitar.
            from .writer import Store
            simbolos = [Store._sym(e, sym) for e in self.cfg.exchanges for sym in self.cfg.symbols]
            filas = self.ledger.ultimo_trade_por_clave(self.cfg.exchanges,
                                                       simbolos=sorted(set(simbolos)))
        except Exception as exc:  # noqa: BLE001
            log(component="feed", event="id_jump_seed_error", error=str(exc)[:140])
            return
        sembradas = 0
        for exchange, symbol, trade_id, ts_ms in filas:
            if self.id_jump.sembrar(exchange, symbol, trade_id, ts_ms):
                sembradas += 1
        if sembradas:
            log(component="feed", event="id_jump_seeded", claves=sembradas,
                detalle=",".join(f"{f[0]}/{f[1]}@{f[2]}" for f in filas[:6]))

    def _sincronizar_abiertos(self) -> None:
        """Quita del watchdog las claves cuyo hueco ya no esta vivo en el ledger.

        Va cada `SYNC_CADA` sondeos (no en cada uno) porque es una consulta, y porque el worker
        no necesita que sea instantanea: mientras la clave siga marcada, el peor caso es que se
        pierda un corte muy corto que cae en la ventana de gracia.
        """
        if self.ledger is None or not self.watchdog.open_id:
            return
        self._sync_n += 1
        if self._sync_n < SYNC_CADA:
            return
        self._sync_n = 0
        try:
            vivos = self.ledger.open_keys()
        except Exception as exc:  # noqa: BLE001
            log(component="feed", event="gap_sync_error", error=str(exc)[:140])
            return
        olvidadas = [k for k in self.watchdog.open_id if k not in vivos]
        for k in olvidadas:
            del self.watchdog.open_id[k]
        if olvidadas:
            log(component="feed", event="gap_rearmed", count=len(olvidadas),
                claves=",".join(f"{k[0]}/{k[1]}/{k[2]}" for k in olvidadas[:6]))

    async def _gaps_loop(self):
        """Sondea los detectores. Los tres hacen cosas distintas y complementarias:

        - **silencio**: el dato que deberia llegar cada 15 s no llega. El mas fiable de los tres.
        - **reconexion**: `conn.connects` ha subido. cryptofeed 3.0.1 no expone ningun hook ni
          evento de reconexion (verificado leyendo el codigo), asi que hay que mirar el contador.
        - **refinamiento**: si un silencio ya estaba abierto y vuelve la senal, el hueco se acota al
          ultimo evento real y no se queda con los segundos de mas que el watchdog dio.
        """
        while not self.stop.is_set():
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=GAP_POLL_S)
            except asyncio.TimeoutError:
                pass
            ahora_ms = int(time.time() * 1000)

            # 1) refinar antes que abrir: si un hueco se puede acortar, se acorta antes de que el
            #    worker lo reclame, que si no se lleva trabajo de mas.
            if self.ledger is not None and self.refines:
                pendientes, self.refines = self.refines, []
                for gap_id, to_ms in pendientes:
                    try:
                        self.ledger.refine(gap_id, to_ms)
                    except Exception as exc:  # noqa: BLE001
                        log(component="feed", event="gap_refine_error", gap_id=gap_id,
                            error=str(exc)[:120])

            # 2) sincronizacion: soltar del watchdog las claves cuyo hueco ya se cerro. Sin esto
            #    el watchdog cree que la clave sigue abierta y no vuelve a mirar para ella.
            self._sincronizar_abiertos()

            # 3) silencio
            nuevos = self.watchdog.due(ahora_ms)
            if nuevos:
                ids = self._registrar(nuevos, reason="silence")
                for g, i in zip(nuevos, ids):
                    self.watchdog.open_id[g.key()] = i

            # 4) reconexion
            if not self.feeds:
                continue
            try:
                cambios = self.reconnects.poll(
                    ReconnectTracker.connects_of(self.feeds))
            except Exception as exc:  # noqa: BLE001
                log(component="feed", event="reconnect_poll_error", error=str(exc)[:120])
                continue
            for exch in cambios:
                huecos = []
                for (e, _sym, _dt), last_ms in self.watchdog.last_event_ms.items():
                    if e != exch or ahora_ms - last_ms < RECONNECT_MIN_MS:
                        continue
                    huecos.append(Gap(
                        exchange=e, symbol=_sym, dtype=_dt, gap_from_ms=last_ms,
                        gap_to_ms=ahora_ms, reason="disconnect",
                        note="contador de conexiones de cryptofeed incrementado"))
                if huecos:
                    self._registrar(huecos, reason="disconnect")
                log(component="feed", event="reconnect", exchange=exch,
                    huecos=len(huecos), reconnects_total=self.reconnects.reconnects)

    # ---------------------------------------------------------------- flush
    async def _flush_loop(self):
        """Flush por tiempo (1 s) o por tamano. El primero mantiene la latencia baja aunque el
        volumen sea bajo; el segundo evita micro-INSERTs cuando llega una ráfaga."""
        while not self.stop.is_set():
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=self.cfg.flush_interval)
            except asyncio.TimeoutError:
                pass
            reason = "size" if self.store.ready() else "time"
            self.store.flush(reason=reason)

# ---------------------------------------------------------------- main
    def build(self) -> FeedHandler:
        from cryptofeed.defines import (
            CANDLES,
            FUNDING,
            LIQUIDATIONS,
            OPEN_INTEREST,
            TRADES,
        )
        from cryptofeed.exchanges import (
            BinanceFutures,
            Bitget,
            Bybit,
            Hyperliquid,
            OKX,
        )

        classes = {
            "binance_futures": BinanceFutures,
            "bybit": Bybit,
            "okx": OKX,
            "bitget": Bitget,
            "hyperliquid": Hyperliquid,
        }
        # Claves de `cryptofeed.feed.CALLBACK_CHANNELS`. Son las constantes de cryptofeed, no
        # strings: `Feed.__init__` indexa `self.callbacks` por estas, y una clave inventada
        # (p.ej. "trades" en vez de TRADES) no registra nada.
        handlers = {
            TRADES: self._cb_trade,
            FUNDING: self._cb_funding,
            OPEN_INTEREST: self._cb_open_interest,
            LIQUIDATIONS: self._cb_liquidation,
            CANDLES: self._cb_candle,
        }

        # cryptofeed abre un RotatingFileHandler sobre `feedhandler.log` en el CWD por defecto
        # (`cryptofeed/config.py::_default_config`). En el contenedor el CWD es `/app`, que es de
        # root y el daemon corre como LAKE_UID -> PermissionError y el proceso no arranca. Se deja
        # el nombre vacio (falsy -> no crea fichero) y se loguea solo a stdout, que es lo que pide
        # la regla 9.
        fh = FeedHandler(
            config={"log": {"filename": "", "level": self.cfg.log_level}},
            on_feed_error="remove_feed",
        )
        for exchange in self.cfg.exchanges:
            channels = self.cfg.channels_for(exchange)
            symbols = self.cfg.symbols_for(exchange)
            try:
                # Los callbacks van en el CONSTRUCTOR del exchange, no en `add_feed`.
                # `FeedHandler.add_feed(feed, **kwargs)` solo usa `kwargs` cuando `feed` es un
                # str; si le pasas una instancia se los tira en silencio
                # (`feedhandler.py::add_feed` -> `self.feeds.append(feed)`). Con los callbacks
                # en `add_feed` la WS recibia ~76 mensajes en 25s y no llamaba a ninguna
                # callback, sin un solo error: `Feed.__init__` deja `Callback(None)` para cada
                # canal y `Feed.callback()` no encuentra a quien avisar.
                feed = classes[exchange](
                    symbols=symbols,
                    channels=list(channels),
                    callbacks=handlers,
                )
                fh.add_feed(feed)
                self.feeds.append(feed)
                # Canonico desde aqui: el id de cryptofeed es `BINANCE_FUTURES` y la tabla ya
                # guarda `binance_um`. Si la clave vigilada no coincide con lo que devuelve
                # `open_keys()`, `_sincronizar_abiertos` creeria que todos los huecos estan
                # cerrados y vaciaria `open_id` cada minuto -> el watchdog rearma sin parar.
                exch_id = canonico(classes[exchange].id)
                for sym in symbols:
                    simbolo = self.store._sym(classes[exchange].id, sym)
                    for ch in channels:
                        self.feeds_vigilados.append((exch_id, simbolo, str(ch)))
                log(component="feed", event="feed_added", exchange=exchange,
                    channels=",".join(channels), symbols=",".join(symbols),
                    claves=len(self.feeds_vigilados))
            except Exception as exc:  # noqa: BLE001
                log(component="feed", event="feed_error", exchange=exchange, error=str(exc)[:160])
        return fh

    async def run(self):
        self.store.open()
        fh = self.build()
        self.feedhandler = fh
        # Despues de `build()`: `_claves_vigiladas` lee los feeds ya construidos.
        self._abrir_ledger()
        self._sembrar_id_jump()

        loop = asyncio.get_running_loop()
        flusher = loop.create_task(self._flush_loop())
        gaps_task = loop.create_task(self._gaps_loop())
        log(component="feed", event="start", exchanges=",".join(self.cfg.exchanges),
            channels=",".join(self.cfg.channels), symbols=",".join(self.cfg.symbols))

        try:
            # `run_async` (no `run`): en cryptofeed 3.0.1 `run()` es bloqueante y crea su propio
            # `asyncio.Runner`, asi que llamarlo desde dentro de un loop ya abierto revienta con
            # "Cannot run the event loop while another loop is running".
            #
            # De las señales se encarga cryptofeed: con `install_signal_handlers=True` la PRIMERA
            # señal (SIGTERM/SIGINT) hace un apagado graceful y la segunda cancela. No se
            # reinstalan aqui porque `loop.add_signal_handler` pisa el handler anterior.
            await fh.run_async(install_signal_handlers=True)
        except asyncio.CancelledError:
            pass
        finally:
            # Flush final. El AC es "SIGTERM -> 0 filas perdidas", asi que esto es lo mas
            # importante del finally: sin el, todo lo que hubiera en el buffer se pierde.
            self.stop.set()
            flusher.cancel()
            gaps_task.cancel()
            await self._final_flush()
            self.store.close()
            if self.ledger is not None:
                try:
                    self.ledger.close()
                except Exception as exc:  # noqa: BLE001
                    log(component="feed", event="gap_ledger_close_error", error=str(exc)[:120])
            self.log_stats()

    async def _final_flush(self):
        pending = sum(len(w) for w in self.store.writers.values())
        log(component="feed", event="shutdown_flush_begin", buffered=pending,
            grace_seconds=self.cfg.shutdown_grace)
        if pending:
            loop = asyncio.get_running_loop()
            deadline = loop.time() + self.cfg.shutdown_grace
            wrote = self.store.flush(reason="shutdown")
            log(component="feed", event="shutdown_flush_done", rows=wrote,
                within_grace=loop.time() < deadline)

    def log_stats(self):
        log(component="feed", event="stats", p95_ms=f"{self.store.p95_ms():.0f}",
            elapsed=f"{time.monotonic() - self.started:.0f}",
            **{f"{d}_{k}": v for d, s in self.store.stats().items() for k, v in s.items()})


def main() -> int:
    cfg = load()
    daemon = Daemon(cfg)
    try:
        asyncio.run(daemon.run())
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())