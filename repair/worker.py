"""Worker de reparacion: toma huecos de `ingest_gaps`, los repara y los cierra.

El bucle es tonto a proposito (claim -> fetch -> insertar -> verificar -> cerrar). Toda la
inteligencia esta en los adaptadores. Lo que el worker garantiza es lo que **no** debe fallar:

- **Nunca dejar un hueco `open` para siempre.** A los 5 intentos pasa a `partial` con el error en
  el `note`. Un hueco sin cerrar es la perdida silenciosa que la regla 15 prohibe.
- **Un baneo (418) no cuenta como intento.** Es un problema de la IP, no del rango: se espera y se
  reintenta. Si contara, un baneo de cinco minutos quemaria los 5 intentos del hueco y lo
  declararia irrecuperable sin haberlo intentado de verdad.
- **Idempotente.** Reejecutar un gap no anade filas: el REST deduplica por PK y el volcado por
  anti-join de `trade_id`.
- **`repaired` solo si la fuente cubrio el hueco entero.** Si el adaptador dice que no llego, el
  estado es `partial`. Declarar `repaired` un hueco a medias es peor que no declararlo.
"""

from __future__ import annotations

import time

import psycopg

from bulk.logfmt import log
from common.db import conninfo
from feed.gaps import Gap, GapLedger

from .adapters.base import RepairResult
from .adapters.binance import BinanceFuturesAdapter
from .adapters.bitget import BitgetAdapter
from .adapters.bybit import VENTANA_ANTIJOIN_MS, BybitAdapter
from .adapters.hyperliquid import HyperliquidAdapter
from .adapters.okx import OKXAdapter
from .http import Banned, Client, HttpError, RateLimited

#: Simbolos con volcado publico en Bybit. Fijados aqui y no tomados de la configuracion del
#: daemon a proposito: el volcado es un contrato del exchange, y si el daemon anade un simbolo que
#: Bybit aun no publica el dia pasaria de "no disponible" a "faltan trades" sin que nadie lo note.
BYBIT_DUMP_SYMBOLS = ("BTCUSDT", "ETHUSDT")

MAX_INTENTOS = 5


class Worker:
    def __init__(self, dsn: str | None = None, intervalo_s: float = 30.0,
                 max_por_vuelta: int = 4, solo_dtype: str | None = None,
                 exchanges: list[str] | None = None):
        self.dsn = dsn or conninfo()
        self.intervalo_s = intervalo_s
        self.max_por_vuelta = max_por_vuelta
        self.solo_dtype = solo_dtype
        self.http = Client()
        self.ledger = GapLedger(self.dsn)
        self.conn = None
        self.adaptadores = self._construir()
        if exchanges:
            self.adaptadores = {k: v for k, v in self.adaptadores.items() if k in exchanges}

    def _construir(self) -> dict:
        bybit = BybitAdapter(self.http)
        return {
            "binance_um": BinanceFuturesAdapter(self.http),
            "okx": OKXAdapter(self.http),
            "bitget": BitgetAdapter(self.http),
            "bybit": bybit,
            "hyperliquid": HyperliquidAdapter(self.http),
        }

    def open(self) -> None:
        import psycopg

        self.ledger.open()
        # `autocommit=True` y NO `False`, y no es cosmetico. Con `autocommit=False`, el
        # `SET TIME ZONE` de abajo abre una transaccion implicita de psycopg3: los
        # `with conn.transaction()` de `repair/ingest.py` se convierten en SAVEPOINTs dentro de
        # ella, y todo se revierte al hacer `close()`. El ledger, que va en su propia conexion
        # autocommit, si habia confirmado el estado `repaired`: el hueco quedaba cerrado y sin
        # una sola fila. Medido en el hueco 704 (binance_um/BTCUSDT, ~23,8 h,
        # `rows_repaired=47994` frente a 0 filas en `trades` con `source='rest'`).
        #
        # Con autocommit, `SET TIME ZONE` se aplica a la sesion y cada `conn.transaction()` es una
        # transaccion real y autonoma: o entra el lote entero, o no entra nada.
        self.conn = psycopg.connect(self.dsn, autocommit=True)
        self.conn.execute("SET TIME ZONE 'UTC'")

    def close(self) -> None:
        if self.conn is not None:
            self.conn.close()
            self.conn = None
        self.ledger.close()

    # ------------------------------------------------------------------ una pasada
    def una_vuelta(self) -> dict[str, int]:
        contadores = {"reparados": 0, "parciales": 0, "irrecuperables": 0, "fallidos": 0}
        gaps = self.ledger.claim(limit=self.max_por_vuelta, max_attempts=MAX_INTENTOS)
        for gap in gaps:
            if self.solo_dtype and gap.dtype != self.solo_dtype:
                self.ledger.release(gap.id)
                continue
            estado = self.reparar(gap)
            contadores[estado] = contadores.get(estado, 0) + 1
        return contadores

    def reparar(self, gap: Gap) -> str:
        adapter = self.adaptadores.get(gap.exchange)
        inicio = time.monotonic()
        if adapter is None:
            self.ledger.finish(gap.id, "unrecoverable", note=f"sin adaptador para {gap.exchange}")
            return "irrecuperables"
        puede, motivo = adapter.can_repair(gap)
        if not puede:
            self.ledger.finish(gap.id, "unrecoverable", note=motivo)
            log(component="repair", event="unrecoverable", exchange=gap.exchange,
                symbol=gap.symbol, dtype=gap.dtype, reason=gap.reason, note=(motivo or "")[:160])
            return "irrecuperables"

        self.ledger.bump_attempt(gap.id)
        try:
            resultado = (adapter.fetch_candles(gap) if gap.dtype == "candles"
                         else adapter.fetch_trades(gap))
        except (Banned, RateLimited) as exc:
            # Ni un 418 ni un 429 son culpa del hueco: los dos son de la IP, y la IP es del
            # usuario, compartida con el backfill, el loader y el reconciliador. Se espera y se
            # reintenta, sin gastar uno de los 5 intentos.
            #
            # Antes el 429 caia en el `except` de abajo y cerraba el hueco como `partial`
            # consumiendo intento. Medido en el 704: un unico -1003 de Binance lo dejo en
            # `partial` con `attempts=1` y 0 filas, cuando el rango era perfectamente reparable y
            # solo habia que esperar 4 s. A los 5 huecos asi un exchange entero queda
            # "irrecuperable" sin que nadie haya intentado nada.
            #
            # `bump_attempt` ya habia corrido: se deshace con el `release`, que lo devuelve a
            # `open` y decrementa `attempts` para que el hueco no gaste de los 5.
            self.ledger.release(gap.id, deshacer_intento=True)
            log(component="repair", event="rate_limit_backoff", exchange=gap.exchange,
                symbol=gap.symbol, dtype=gap.dtype, kind=type(exc).__name__,
                seconds=round(exc.retry_after, 1))
            return "fallidos"
        except HttpError as exc:
            self.ledger.finish(gap.id, "partial", note=f"intento {gap.attempts + 1}: {exc}"[:400])
            log(component="repair", event="error", exchange=gap.exchange, symbol=gap.symbol,
                dtype=gap.dtype, gap_from=gap.gap_from_ms, gap_to=gap.gap_to_ms,
                error=str(exc)[:200])
            return "fallidos"

        # Segunda pasada por volcado: el REST solo llega a ~2 min, asi que un hueco de Bybit
        # antiguo se quedaria `partial` para siempre aunque el volcado diario tenga el dato. Con
        # esto el hueco se cierra por la misma via que el reconciliador D-1, y las dos coinciden
        # porque comparten anti-join por `trade_id`.
        detalles: list[str] = []
        insertadas = 0
        volcado: RepairResult | None = None
        #
        # El volcado solo se consulta si el REST no llego. Se decide QUE va a cerrar el hueco antes
        # de insertar, para no insertar dos veces el mismo conjunto por dos caminos distintos.
        if (resultado.limitation or not resultado.rows) and callable(
                getattr(adapter, "fetch_dump_trades", None)):
            try:
                volcado = vol = adapter.fetch_dump_trades(gap)
            except (Banned, RateLimited, HttpError) as exc:
                self.ledger.finish(gap.id, "partial", note=f"volcado: {exc}"[:400])
                return "fallidos"
            if vol.rows or vol.limitation:
                if resultado.rows:
                    # El REST trajo filas y sus `ts` coinciden con la tabla. El volcado se
                    # anti-une contra ella aparte, para rellenar lo que al REST no llego.
                    insertadas, det = self._insertar(gap, vol)
                    detalles.append(f"el REST no alcanzo del todo y el volcado aporta "
                                    f"{len(vol.rows)} filas")
                    detalles.append(det)
                else:
                    # El REST no trajo nada utilizable: el volcado es la respuesta y se inserta
                    # abajo, ya por su cuenta con el anti-join.
                    resultado = vol
                    detalles.append(f"el REST no alcanzo y el volcado aporta {len(vol.rows)} filas")

        n, det = self._insertar(gap, resultado)
        insertadas += n
        detalles.append(det)
        estado = self._cerrar(gap, resultado, insertadas, "; ".join(x for x in detalles if x),
                              respaldo=volcado)
        log(component="repair", event="gap_done", exchange=gap.exchange, symbol=gap.symbol,
            dtype=gap.dtype, gap_from=gap.gap_from_ms, gap_to=gap.gap_to_ms,
            status=estado, rows_repaired=insertadas, source=resultado.source,
            elapsed=f"{time.monotonic() - inicio:.2f}")
        return estado

    def _insertar(self, gap: Gap, res: RepairResult) -> tuple[int, str]:
        from .ingest import insert_candles, insert_trades, insert_trades_por_id

        if not res.rows:
            return 0, "la fuente no devolvio filas"
        if gap.dtype == "candles":
            n = insert_candles(self.conn, gap.exchange, res.rows)
            return n, "upsert de velas"
        if res.source == "dump":
            # Anti-join por trade_id, acotado a la ventana del volcado.
            ins, rep = insert_trades_por_id(
                self.conn, gap.exchange, res.rows, "dump",
                ventana_desde_ms=gap.gap_from_ms - VENTANA_ANTIJOIN_MS,
                ventana_hasta_ms=gap.gap_to_ms + VENTANA_ANTIJOIN_MS)
            return ins, f"{rep} ya estaban (anti-join por trade_id)"
        n = insert_trades(self.conn, gap.exchange, res.rows, res.source)
        return n, "insert por PK"

    @staticmethod
    def _cubre(gap: Gap, res) -> bool:
        """Una fuente cubre el hueco si se sabe que llego a AMBOS extremos."""
        return (res is not None
                and res.covered_from_ms is not None and res.covered_through_ms is not None
                and res.covered_from_ms <= gap.gap_from_ms
                and res.covered_through_ms >= gap.gap_to_ms)

    def _cerrar(self, gap: Gap, res: RepairResult, insertadas: int, detalle: str = "",
                respaldo: RepairResult | None = None) -> str:
        """Decide el estado. `repaired` solo si alguna fuente cubrio el hueco ENTERO.

        `respaldo` es la segunda fuente consultada (el volcado). Cuenta igual que `res` a la hora
        de decidir: con el REST limitado a 2 min y el volcado cubriendo el dia entero, el hueco SI
        esta completo. Mirar solo la limitacion del REST dejaba el hueco en `partial` para siempre
        aunque el volcado hubiera rellenado justo lo que faltaba.
        """
        completo = any(self._cubre(gap, r) and r.limitation is None
                       for r in (res, respaldo) if r is not None)
        nota = "; ".join(x for x in (res.note, detalle, res.limitation) if x) or None
        if completo:
            fuente = res.source if self._cubre(gap, res) and res.limitation is None \
                else respaldo.source
            self.ledger.finish(gap.id, "repaired", source=fuente, rows=insertadas, note=nota)
            return "reparados"
        if not res.rows and not res.limitation:
            # "La fuente no devolvio filas" NO es lo mismo que "la fuente no tiene el dato".
            # OKX tiene historico de sobra y aun asi devolvio 0 filas con una paginacion que puede
            # estar mal: declararlo irrecuperable borra la pista y el hueco deja de auditarse.
            # Irrecuperable de verdad solo es cuando el propio adaptador lo dice
            # (`can_repair() -> False`), y ahi el motivo va escrito.
            self.ledger.finish(gap.id, "partial", source=res.source, rows=0,
                               note=(nota + "; la fuente no devolvio ninguna fila: puede ser "
                                     "retencion del exchange o un fallo de paginacion, "
                                     "conviene revisarlo a mano").strip("; "))
            return "parciales"
        self.ledger.finish(gap.id, "partial", source=res.source, rows=insertadas, note=nota)
        return "parciales"

    # ------------------------------------------------------------------ bucle
    def run(self) -> None:
        self.open()
        log(component="repair", event="start", intervalo_s=self.intervalo_s,
            exchanges=",".join(self.adaptadores))
        try:
            while True:
                try:
                    contadores = self.una_vuelta()
                    if any(contadores.values()):
                        log(component="repair", event="loop", **contadores,
                            requests=self.http.peticiones, waited=f"{self.http.esperas:.1f}")
                except Exception as exc:  # noqa: BLE001 - el worker no debe morir por un hueco
                    log(component="repair", event="loop_error", error=str(exc)[:200])
                time.sleep(self.intervalo_s)
        except KeyboardInterrupt:
            pass
        finally:
            self.close()


def _cli_volcado(args) -> int:
    """Comandos del volcado D-1 de Bybit, fuera del bucle del worker.

    A proposito aparte: el worker reacts ante huecos que ya existen en el ledger; esto barre un dia
    completo sin depender de que nadie haya detectado el hueco. Un dia perdido por WebSocket que
    nadie detecto es justo el caso que el worker no puede ver.
    """
    from datetime import datetime, timedelta, timezone

    from common.db import conninfo
    from repair.reconcile import reconciliar, verificar_alineacion

    symbols = BYBIT_DUMP_SYMBOLS
    if args.exchange and args.exchange.upper() != "BYBIT":
        print(f"solo Bybit tiene volcado diario publico: {args.exchange} no aplica")
        return 2
    with psycopg.connect(conninfo(), autocommit=True) as conn:
        conn.execute("SET TIME ZONE 'UTC'")
        if args.comando == "verify-dump-alignment":
            dia = datetime.now(timezone.utc).date() - timedelta(days=args.dias)
            print(verificar_alineacion(conn, symbols[0], dia, modo=args.modo))
            return 0
        filas = reconciliar(conn, symbols, dias=args.dias, modo=args.modo,
                            dry_run=args.simular)
        total_nuevas = sum(f["nuevas"] for f in filas)
        total_fuente = sum(f["fuente"] for f in filas)
        total_repetidas = sum(f["repetidas"] for f in filas)
        no_disponibles = [f for f in filas if not f["disponibles"]]
        log(component="reconcile", event="summary", days=args.dias, modo=args.modo,
            dry_run=args.simular, filas_fuente=total_fuente, insertadas=total_nuevas,
            ya_presentes=total_repetidas, dias_sin_volcado=len(no_disponibles))
        if total_nuevas and args.simular:
            print(f"\n{total_nuevas} filas nuevas SI se insertarian. Repite sin --simular.")
    return 0


def main() -> int:
    import argparse

    ap = argparse.ArgumentParser(description="Reparador de huecos de ingesta")
    ap.add_argument("comando", choices=["run", "once", "status", "reconcile-bybit",
                                        "verify-dump-alignment"])
    ap.add_argument("--intervalo", type=float, default=30.0)
    ap.add_argument("--dtype", default=None)
    ap.add_argument("--exchange", default=None)
    ap.add_argument("--dias", type=int, default=1,
                    help="reconcile-bybit: cuantos dias atras mirar (1 = solo D-1)")
    ap.add_argument("--modo", default="floor", choices=["floor", "round"],
                    help="normalizacion del ts del volcado, en segundos con 4 decimales")
    ap.add_argument("--simular", action="store_true",
                    help="reconcile-bybit: no inserta, solo informa de lo que entraria")
    args = ap.parse_args()

    if args.comando in ("reconcile-bybit", "verify-dump-alignment"):
        return _cli_volcado(args)
    w = Worker(intervalo_s=args.intervalo, solo_dtype=args.dtype,
               exchanges=[args.exchange] if args.exchange else None)
    if args.comando == "run":
        w.run()
        return 0
    w.open()
    try:
        if args.comando == "once":
            print(w.una_vuelta())
        else:
            for g in w.ledger.list_gaps():
                print(f"{g.id:>5} {g.status:<14} {g.exchange:<17} {g.symbol:<8} {g.dtype:<13} "
                      f"{(g.note or '')[:90]}")
    finally:
        w.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())