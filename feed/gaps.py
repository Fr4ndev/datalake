"""Ledger de huecos de ingesta: detectar, registrar y refinar lo que el WebSocket pierde.

Por que existe esto (skill `ingest-gap-repair`): el WebSocket **no reenvia** lo perdido mientras
estamos desconectados. Da igual cuantas veces se resuscriba: el exchange entrega el estado actual,
no el historico. Medido en la prueba de corte real de la Fase 2: 45 s de hueco en Bybit, 38 s en
Binance y OKX, 20 s en Bitget.

La garantia que **si** se puede dar no es "0 perdidas" sino **"0 perdidas en silencio"**. Eso son
cuatro cosas:

1. **Detectar** el hueco. Cuatro detectores, todos aqui, todos con logica pura y testeable:
   `restart`, `disconnect`, `silence` e `id_jump`.
2. **Registrarlo** en `ingest_gaps`. Las filas nunca se borran (regla 15): se cierran con `status`.
3. **Repararlo** con la mejor fuente disponible (lo hace `repair/`).
4. **Declararlo irrecuperable** con su motivo, si la fuente no lo da.

Por que NO hay un "hook de reconexion" de cryptofeed
--------------------------------------------------
cryptofeed 3.0.1 **no expone ningun evento de reconexion**. `ConnectionHandler.run()`
(`cryptofeed/connection_handler.py:75`) reconecta por su cuenta en un bucle interno y no avisa a
nadie; lo unico observable desde fuera es el contador `conn.connects`, que se incrementa en cada
`connect()` (`cryptofeed/connection.py:95`). Por eso `ReconnectTracker` **sondea** ese contador en
lugar de recibir un callback, y por eso el resto de detectores no dependen de el: un corte de red se
detecta igual por silencio y por discontinuidad de timestamp.

Timestamps
----------
Todo en **milisegundos enteros**, nunca float (regla 16 de AGENTS.md). cryptofeed entrega epoch en
segundos como float (`Binance.timestamp_normalize` divide entre 1000), mientras que el REST ya
viene en ms. Con floats, la misma fila_insertada_por_WS y la misma fila_Insertada_por_REST pueden
differir en la ultima cifra y la PK `(symbol, exchange, ts, trade_id)` no las reconoce como
duplicado: la reparacion duplicaria. Con ms enteros, WS y REST coinciden exactamente (medido:
96/96 en Binance, 151/151 en Bybit, 90/90 en Bitget, 103/103 en OKX, 10/10 en Hyperliquid).
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Iterable

from common.exchanges import canonico

#: (exchange, symbol, dtype)
Key = tuple[str, str, str]

#: Padding a cada lado del hueco. El `ON CONFLICT DO NOTHING` se come el solape, asi que Goes
#: demasiado es gratis y Goes poco deja fuera los trades del borde. 5 s de margen.
PAD_MS = 5_000

#: dtype -> tabla donde mirar el `max(ts)` para la deteccion de `restart`.
#: dtype -> (tabla, columna de tiempo). `candles_1m` mide por `open_time`, el resto por `ts`.
DTYPE_TABLE = {
    "trades": ("trades", "ts"),
    "candles": ("candles_1m", "open_time"),
    "funding": ("funding", "funding_time"),
    "open_interest": ("open_interest", "ts"),
    "liquidations": ("liquidations", "ts"),
}

#: Umbral de silencio por dtype, en ms de **tiempo de recepcion** (no de reloj del exchange).
#:
#: Se mide en recepcion a proposito: lo que se quiere detectar es "la conexion esta muerta", y eso
#: se sabe por cuanto tiempo llevamos sin recibir NADA, no por lo que diga el ts del exchange. Un
#: ts antiguo puede ser un mercado parado; un silencio de recepcion es un socket caido.
#:
#: `trades` y `candles` de BTC/ETH son canales liquidos: 15 s sin nada es anomalo con seguridad.
#: `funding` llega cada 8 h, `liquidations` y `open_interest` cada pocos segundos pero no en todos
#: los pares, asi que necesitan margen. Si no, el watchdog abriria huecos falsos constantemente.
SILENCE_MS = {
    "trades": 15_000,
    # 1m **cerradas**: la ultima vela no aparece hasta que se cierra, asi que entre una vela y la
    # siguiente hay 1 min de silencio y hasta ~1 min de retraso de publicacion. Con 90 s se abrian
    # huecos falsos de velas en cada arranque; 3 min da margen sin perder sensibilidad.
    "candles": 180_000,
    "liquidations": 300_000,
    "open_interest": 300_000,
    "funding": 3 * 3600_000,
}

#: Exchanges cuyo `trade_id` es **secuencial**, y por tanto un salto de id demuestra que faltan
#: trades aunque no haya silencio. Medido en cryptofeed 3.0.1: Binance usa el stream `aggTrade` con
#: `id=str(msg['a'])`, y `a` es un contador monotono (`a`, `a+1`, `a+2`...).
#:
#: En Bybit/OKX/Bitget/Hyperliquid el id NO es una secuencia monotona comprobable (UUID, snowflake
#: con timestamp incrustado, tid derivado), asi que un "salto" no significaria nada y no se activa.
SEQUENTIAL_ID_EXCHANGES = {"binance_um"}


@dataclass
class Gap:
    """Un hueco. Todos los tiempos en ms enteros."""

    exchange: str
    symbol: str
    dtype: str
    gap_from_ms: int
    gap_to_ms: int
    reason: str
    id: int | None = None
    status: str = "open"
    source: str | None = None
    rows_repaired: int = 0
    attempts: int = 0
    note: str | None = None

    @property
    def key(self) -> Key:
        return (self.exchange, self.symbol, self.dtype)


@dataclass
class Cobertura:
    """Ultimo ts que hay en la base por clave. Lo usa la deteccion de `restart`."""

    last_ms: dict[Key, int] = field(default_factory=dict)


# ====================================================================== detectores
# Logica pura: no toca la base. Todo el estado es explicito para que los tests no necesiten
# ni Postgres ni exchanges.


class SilenceWatchdog:
    """Detecta "llevo mas de X sin recibir nada" y lo convierte en hueco.

    Dos pasos, a proposito:

    - `due()` **abre** el hueco cuando el silencio se cumple, con `gap_to` = ahora mismo.
    - `observe()` **refina** `gap_to` cuando llega el primer evento: el hueco no se cierra, se
      acota. Lo cierra `repair/`. Si el watchdog cerrara el hueco al volver la senal, el
      periodo silencioso se quedaria sin registrar justo cuando es el que hay que reparar.
    """

    def __init__(self, thresholds: dict[str, int] | None = None, pad_ms: int = PAD_MS):
        self.thresholds = dict(SILENCE_MS if thresholds is None else thresholds)
        self.pad_ms = pad_ms
        #: clave -> ts (ms) del ultimo evento recibido
        self.last_event_ms: dict[Key, int] = {}
        #: clave -> ts (ms) del ultimo evento, para el extremo izquierdo del hueco
        self.last_recv: dict[Key, float] = {}
        #: clave -> id del hueco de silencio abierto, para refinarlo en vez de duplicarlo
        self.open_id: dict[Key, int] = {}

    def observe(self, key: Key, event_ms: int, recv_mono: float | None = None) -> int | None:
        """Registra un evento. Devuelve el `gap_id` a refinar, si habia un hueco de silencio
        abierto para esa clave (el worker lo actualizara con `gap_to = event_ms + pad`)."""
        self.last_event_ms[key] = event_ms
        self.last_recv[key] = recv_mono if recv_mono is not None else time.monotonic()
        return self.open_id.get(key)

    def due(self, now_ms: int, now_mono: float | None = None) -> list[Gap]:
        """Huecos de silencio que se newly cumplen. No abre los que ya estan abiertos."""
        mono = now_mono if now_mono is not None else time.monotonic()
        out: list[Gap] = []
        # Se itera por las claves VISTAS, no por las del diccionario de umbrales: los umbrales
        # estan indexados por dtype, pero un dtype son muchas claves (exchange, symbol, dtype).
        for key, last_recv in self.last_recv.items():
            threshold = self.thresholds.get(key[2])
            if threshold is None:
                continue  # este dtype no se vigila
            if key in self.open_id:
                continue  # ya registrado; se refina al volver la senal
            if (mono - last_recv) * 1000.0 < threshold:
                continue
            # Sin padding aqui: `GapLedger.record()` lo anade una unica vez en el unico punto por
            # el que pasan todos los detectores. Padeando tambien aqui, un silencio de 45 s se
            # declaraba de 55 s: el ledger se hincha y el worker pide de mas al exchange.
            out.append(
                Gap(
                    exchange=key[0], symbol=key[1], dtype=key[2],
                    gap_from_ms=self.last_event_ms.get(key, now_ms),
                    gap_to_ms=now_ms,
                    reason="silence",
                )
            )
        return out


class IdJumpDetector:
    """Salto en un `trade_id` secuencial: prueba directa de que faltan trades.

    Es el unico detector que **acusa al exchange con su propia contabilidad**: si Binance dice que
    el siguiente aggTrade es `a+17`, han desaparecido 16. No hace falta ni sospechar del silencio.
    """

    def __init__(self, exchanges: Iterable[str] = SEQUENTIAL_ID_EXCHANGES, pad_ms: int = PAD_MS):
        # Canonicos desde aqui: quien llama trae el id de cryptofeed (`BINANCE_FUTURES`) y el
        # conjunto de origen trae el canonico (`binance_um`). Compararlos en crudo daria "este
        # exchange no es secuencial" y el detector se quedaria callado justo en Binance.
        self.exchanges = {canonico(e) for e in exchanges}
        self.pad_ms = pad_ms
        #: (exchange, symbol) -> (ultimo_id, ultimo_ts_ms)
        self.state: dict[tuple[str, str], tuple[int, int]] = {}

    def observe(self, exchange: str, symbol: str, trade_id: str, event_ms: int) -> Gap | None:
        try:
            exchange = canonico(exchange)
        except ValueError:
            return None  # exchange fuera del catalogo: este detector no aplica
        if exchange not in self.exchanges:
            return None
        try:
            tid = int(trade_id)
        except (TypeError, ValueError):
            return None  # id no numerico: este detector no aplica
        prev = self.state.get((exchange, symbol))
        self.state[(exchange, symbol)] = (tid, event_ms)
        if prev is None:
            return None
        prev_id, prev_ms = prev
        if tid <= prev_id + 1:
            return None
        return Gap(
            exchange=exchange, symbol=symbol, dtype="trades",
            # `prev_id + 1` es el primero que falta; el hueco acaba en el recien llegado.
            gap_from_ms=max(0, prev_ms - self.pad_ms),
            gap_to_ms=event_ms + self.pad_ms,
            reason="id_jump",
            note=f"ids {prev_id} -> {tid}: faltan {tid - prev_id - 1} aggTrades",
        )


class ReconnectTracker:
    """Detecta reconexiones sondeando `conn.connects` de cryptofeed.

    No es el detector principal (el silencio y el salto de id ya cubren el caso), es la
    **corroboracion**: si el numero de gaps registrados no cuadra con el numero de reconexiones,
    algo se nos esta escapando. Por eso lleva la cuenta.
    """

    def __init__(self):
        self.prev: dict[str, int] = {}
        self.reconnects = 0

    def poll(self, connects: dict[str, int]) -> list[str]:
        """Devuelve los exchanges cuyo contador de conexiones ha subido, y suma a `reconnects`."""
        out: list[str] = []
        for exchange, n in connects.items():
            before = self.prev.get(exchange)
            self.prev[exchange] = n
            if before is not None and n > before:
                # cryptofeed cuenta la primera conexion como 1, asi que `connects - 1` son
                # reconexiones. Aqui solo comparamos deltas, que es lo mismo.
                self.reconnects += n - before
                out.append(exchange)
        return out

    @staticmethod
    def connects_of(feeds: Iterable[object]) -> dict[str, int]:
        """Lee `feed.connection_handlers[*].conn.connects` de los exchanges de cryptofeed.

        `connection_handlers` se puebla en `Feed._run` justo antes de conectar, asi que puede
        estar vacio al arrancar: se trata como 0 y no como error.
        """
        out: dict[str, int] = {}
        for feed in feeds:
            handlers = getattr(feed, "connection_handlers", None) or []
            total = 0
            for h in handlers:
                conn = getattr(h, "conn", None)
                total += int(getattr(conn, "connects", 0) or 0)
            out[str(getattr(feed, "id", "?"))] = total
        return out


def restart_gaps(coverage: Cobertura, now_ms: int, min_stale_ms: int = 60_000,
                 pad_ms: int = PAD_MS) -> list[Gap]:
    """Al arrancar: si lo ultimo que hay en la base es viejo, hay un hueco desde ahi.

    `min_stale_ms` es el SUELO, no el umbral: por dtype se usa la cadencia real de `SILENCE_MS`.
    Con un umbral plano de 60 s el daemon declaraba 17 huecos falsos en cada arranque, casi todos
    de `candles` y `liquidations`: 2 min sin velas y 40 min sin liquidaciones son el intervalo
    NORMAL de esos datos, no un corte. Y un falso positivo aqui no es gratis: el worker gasta
    peticiones de REST para "reparar" datos que ya estan bien.
    """
    out: list[Gap] = []
    for key, last_ms in coverage.last_ms.items():
        umbral = max(min_stale_ms, SILENCE_MS.get(key[2], min_stale_ms))
        if now_ms - last_ms < umbral:
            continue
        out.append(
            Gap(
                exchange=key[0], symbol=key[1], dtype=key[2],
                gap_from_ms=max(0, last_ms - pad_ms),
                gap_to_ms=now_ms,
                reason="restart",
                note=f"el ultimo ts en la base era {now_ms - last_ms} ms anterior al arranque",
            )
        )
    return out


def merge(gaps: Iterable[Gap]) -> list[Gap]:
    """Fusiona huecos solapados o contiguos del mismo (exchange, symbol, dtype).

    Sin esto, un corte de 45 s con dos pausas produciria tres filas y el worker los repararia tres
    veces. Al fusionar, un hueco es "un periodo sin datos", que es lo que significa.

    El `reason` se conserva el mas especifico segun este orden, porque el mas especifico es el que
    explica mejor el hueco: un `id_jump` (el exchange dice que faltan 16 trades) explica mas que
    un `silence` (no nos chegou nada en 15 s). El `note` se concatena para no perder informacion.
    """
    prioridad = {"restart": 0, "disconnect": 1, "silence": 2, "id_jump": 3}
    por_clave: dict[Key, list[Gap]] = {}
    for g in gaps:
        por_clave.setdefault(g.key, []).append(g)
    out: list[Gap] = []
    for items in por_clave.values():
        items.sort(key=lambda g: (g.gap_from_ms, g.gap_to_ms))
        actual = items[0]
        for siguiente in items[1:]:
            if siguiente.gap_from_ms <= actual.gap_to_ms:
                actual.gap_to_ms = max(actual.gap_to_ms, siguiente.gap_to_ms)
                if prioridad.get(siguiente.reason, 0) > prioridad.get(actual.reason, 0):
                    actual.reason = siguiente.reason
                notas = [n for n in (actual.note, siguiente.note) if n]
                actual.note = " | ".join(dict.fromkeys(notas)) or None
                if siguiente.id is not None and actual.id is None:
                    actual.id = siguiente.id
            else:
                out.append(actual)
                actual = siguiente
        out.append(actual)
    return out


def canonicaliza(gap: Gap) -> Gap:
    """El `exchange` del hueco pasa a su forma canonica.

    El ledger es donde se mezclan huecos del watchdog (cryptofeed, `BINANCE_FUTURES`) y de la
    busqueda de cobertura (que lee lo que hay en la tabla, `binance_um` si ya se migro). Sin
    esto, un mismo exchange vive bajo dos claves en `open_keys` y el reparador claim()a uno de
    los dos mientras el otro se queda `open` para siempre.
    """
    return Gap(exchange=canonico(gap.exchange), symbol=gap.symbol, dtype=gap.dtype,
               gap_from_ms=gap.gap_from_ms, gap_to_ms=gap.gap_to_ms, reason=gap.reason)


def pad(gap: Gap, pad_ms: int = PAD_MS) -> Gap:
    """Anade padding a ambos lados. El `ON CONFLICT` se come el solape."""
    gap.gap_from_ms = max(0, gap.gap_from_ms - pad_ms)
    gap.gap_to_ms = gap.gap_to_ms + pad_ms
    return gap

# ====================================================================== ledger (BD)

class GapLedger:
    """Escribe y cierra huecos en `ingest_gaps`.

    Una conexion propia, con la zona fijada a UTC **antes de la primera consulta** (regla 3.bis):
    `gap_from`/`gap_to` son TIMESTAMPTZ y sin esto los comparativos se desplazarian en el bucket
    diario del host (`Europe/Madrid`).

    Las escrituras son rarity: un hueco por corte, no por fila. Se pueden hacer de forma sincrona
    sin molestar al event loop, asi que no hay pool ni `to_thread` que mantener.
    """

    #: Estados en los que un hueco sigue vivo y por tanto se fusiona con uno nuevo.
    #: Estados en los que un hueco sigue "en juego". `partial` cuenta, y es deliberado:
    # el worker NO lo reintenta solo (por decision propia, para no gastar peticiones en bucles), pero
    #: si el daemon lo tratara como muerto rearme el watchdog y volveria a abrir el MISMO silencio
    # cada minuto, con una fila nueva por intento y trabajo de API repetido al infinito. Para
    #: reintentarlo hay que pasarlo a `open` a mano, que ademas deja constancia de que fue una
    #: decision y no un olvido.
    VIVOS = ("open", "repairing", "partial")

    def __init__(self, dsn: str, pad_ms: int = PAD_MS):
        self.dsn = dsn
        self.pad_ms = pad_ms
        self._conn = None

    def open(self) -> None:
        import psycopg

        # autocommit=True, no False. Con `autocommit=False`, el `SET TIME ZONE` de arranque abria una
        # transaccion implicita que se quedaba abierta para siempre; a partir de ahi, cada
        # `with conn.transaction()` de psycopg3 crea un SAVEPOINT dentro de ella y **nunca se
        # confirma nada**. Symptoma: la secuencia de ids avanza, el log dice "gap_detected" y la
        # tabla sigue vacia para el resto del mundo (y al cerrar la conexion se pierde todo).
        self._conn = psycopg.connect(self.dsn, autocommit=True)
        self._conn.execute("SET TIME ZONE 'UTC'")

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def _require(self):
        if self._conn is None:
            raise RuntimeError("GapLedger.open() no se ha llamado")
        return self._conn

    # ------------------------------------------------------------------ lectura
    def coverage(self, keys: Iterable[Key] | None = None) -> Cobertura:
        """Ultimo ts por clave. Base del detector de `restart`."""
        cov = Cobertura()
        conn = self._require()
        for dtype, (table, col) in DTYPE_TABLE.items():
            # `candles_1m` tiene `open_time`, el resto `ts`. Preguntar por `ts` ahi revienta la
            # consulta entera, no devuelve un valor erroneo: error de columna.
            if not keys:
                sel = (f"SELECT exchange, symbol, EXTRACT(EPOCH FROM max({col}))*1000 "
                       f"FROM {table} GROUP BY 1,2")
                args: tuple = ()
            else:
                ks = [k for k in keys if k[2] == dtype]
                if not ks:
                    continue
                exch = sorted({k[0] for k in ks})
                syms = sorted({k[1] for k in ks})
                sel = (f"SELECT exchange, symbol, EXTRACT(EPOCH FROM max({col}))*1000 "
                       f"FROM {table} WHERE exchange = ANY(%s) AND symbol = ANY(%s) "
                       f"GROUP BY 1,2")
                args = (exch, syms)
            for exchange, symbol, ms in conn.execute(sel, args).fetchall():
                if ms is None:
                    continue
                cov.last_ms[(exchange, symbol, dtype)] = int(ms)
        return cov

    def list_gaps(self, status: str | None = None, limit: int = 200) -> list[Gap]:
        conn = self._require()
        if status:
            rows = conn.execute(
                "SELECT id, exchange, symbol, dtype, EXTRACT(EPOCH FROM gap_from)*1000, "
                "EXTRACT(EPOCH FROM gap_to)*1000, reason, status, source, rows_repaired, "
                "attempts, note FROM ingest_gaps WHERE status = %s ORDER BY id LIMIT %s",
                (status, limit)).fetchall()
        else:
            rows = conn.execute(
                "SELECT id, exchange, symbol, dtype, EXTRACT(EPOCH FROM gap_from)*1000, "
                "EXTRACT(EPOCH FROM gap_to)*1000, reason, status, source, rows_repaired, "
                "attempts, note FROM ingest_gaps ORDER BY id LIMIT %s", (limit,)).fetchall()
        return [Gap(exchange=r[1], symbol=r[2], dtype=r[3], gap_from_ms=int(r[4]),
                    gap_to_ms=int(r[5]), reason=r[6], id=r[0], status=r[7], source=r[8],
                    rows_repaired=r[9], attempts=r[10], note=r[11]) for r in rows]

    def open_keys(self) -> set[Key]:
        """Claves (exchange, symbol, dtype) con algun hueco vivo (`open` o `repairing`).

        El daemon lo necesita para sincronizar su memoria: el watchdog lleva en `open_id` las
        claves para las que YA abrio un hueco, y las usa para no abrir otro. Si el worker cierra ese
        hueco y nadie limpia ese diccionario, la clave queda **marcada para siempre** y el daemon
        deja de detectar cortes en ella sin decir nada. Es la forma mas sutil de perder la
        deteccion: el ledger parece sano y no registra ni un hueco mas para ese par.
        """
        conn = self._require()
        return {
            (e, s, d) for e, s, d in conn.execute(
                "SELECT DISTINCT exchange, symbol, dtype FROM ingest_gaps "
                "WHERE status = ANY(%s)", (list(self.VIVOS),)).fetchall()
        }

    # ------------------------------------------------------------------ escritura
    def record(self, gaps: Iterable[Gap]) -> list[Gap]:
        """Registra huecos, fusionando con los vivos que ya toquen. Devuelve los ids."""
        pendientes = [pad(canonicaliza(g), self.pad_ms)
                      for g in gaps if g.gap_to_ms > g.gap_from_ms]
        if not pendientes:
            return []
        conn = self._require()
        ids: list[int] = []
        with conn.transaction():
            for g in pendientes:
                ids.append(self._insert_merged(conn, g))
        return ids

    def _insert_merged(self, conn, g: Gap) -> int:
        """Inserta `g` fusionandolo con los huecos vivos que se solapen con el.

        Todo en la misma transaccion y con `FOR UPDATE`: si dos detectores ven el mismo corte a la
        vez (el watchdog y el sondeo de reconexion, por ejemplo) no se cuelan dos filas.

        Las filas absorbidas **no se borran** (regla: no borrar, cerrar con status): se marcan
        `status='merged'` apuntando con `merged_into` a la fila canonica. Se conserva asi la
        deteccion original, que es lo que hay que poder auditar.
        """
        vivos = conn.execute(
            "SELECT id, EXTRACT(EPOCH FROM gap_from)*1000, EXTRACT(EPOCH FROM gap_to)*1000, "
            "reason, note FROM ingest_gaps "
            "WHERE exchange=%s AND symbol=%s AND dtype=%s AND status = ANY(%s) "
            "  AND gap_from <= to_timestamp(%s/1000.0) "
            "  AND gap_to >= to_timestamp(%s/1000.0) FOR UPDATE",
            (g.exchange, g.symbol, g.dtype, list(self.VIVOS), g.gap_to_ms, g.gap_from_ms),
        ).fetchall()
        if not vivos:
            cur = conn.execute(
                "INSERT INTO ingest_gaps (exchange, symbol, dtype, gap_from, gap_to, reason, note) "
                "VALUES (%s,%s,%s,to_timestamp(%s/1000.0),to_timestamp(%s/1000.0),%s,%s) RETURNING id",
                (g.exchange, g.symbol, g.dtype, g.gap_from_ms, g.gap_to_ms, g.reason, g.note))
            return cur.fetchone()[0]
        # Fusionar: el mas antiguo manda, el mas reciente marca el fin. Los limites se leen ya en
        # ms desde Postgres con EXTRACT(EPOCH)*1000; convertirlos con `.timestamp()*1000` en Python
        # pasaria por float64 y perderia precision en los ultimos digitos.
        gap_from = min([g.gap_from_ms] + [int(r[1]) for r in vivos])
        gap_to = max([g.gap_to_ms] + [int(r[2]) for r in vivos])
        # Ante empate manda el motivo mas grave: `restart` (no hay datos y no los habia) explica
        # mas que un `id_jump` de un exchange que ya habiaDeliverado. El resto de motivos no se
        # pierden: quedan en el `note` unido.
        prioridad = {"restart": 0, "disconnect": 1, "silence": 2, "id_jump": 3}
        reason = g.reason
        for r in vivos:
            if prioridad.get(r[3], 9) < prioridad.get(reason, 9):
                reason = r[3]
        notas = list(dict.fromkeys([n for n in [g.note] + [r[4] for r in vivos] if n]))
        keeper = min(r[0] for r in vivos)
        otros = [r[0] for r in vivos if r[0] != keeper]
        if otros:
            conn.execute(
                "UPDATE ingest_gaps SET status='merged', merged_into=%s, "
                "note = COALESCE(note,'') || ' [fusionada en #' || %s::text || ']' "
                "WHERE id = ANY(%s)", (keeper, keeper, otros))
        conn.execute(
            "UPDATE ingest_gaps SET gap_from=to_timestamp(%s/1000.0), "
            "gap_to=to_timestamp(%s/1000.0), reason=%s, note=%s WHERE id=%s",
            (gap_from, gap_to, reason, " | ".join(notas) or None, keeper))
        return keeper

    def refine(self, gap_id: int, to_ms: int) -> None:
        """Acota `gap_to` de un hueco ya abierto sin cambiar su estado.

        Lo usa el watchdog cuando vuelve la senal: el hueco sigue siendo real (hubo silencio) pero
        ya se sabe donde termina.
        """
        conn = self._require()
        conn.execute(
            "UPDATE ingest_gaps SET gap_to = GREATEST(gap_to, to_timestamp(%s/1000.0)) WHERE id=%s",
            (to_ms, gap_id))

    def finish(self, gap_id: int, status: str, source: str | None = None,
               rows: int = 0, note: str | None = None) -> None:
        """Cierra un hueco. Nunca se borra la fila (regla 15)."""
        conn = self._require()
        conn.execute(
            "UPDATE ingest_gaps SET status=%s, source=%s, rows_repaired=%s, note=%s WHERE id=%s",
            (status, source, rows, note, gap_id))

    def bump_attempt(self, gap_id: int) -> int:
        conn = self._require()
        cur = conn.execute(
            "UPDATE ingest_gaps SET attempts = attempts + 1, status = 'repairing' WHERE id=%s "
            "RETURNING attempts", (gap_id,))
        return cur.fetchone()[0]

    def release(self, gap_id: int) -> None:
        """Devuelve a `open` un hueco que se dejo en `repairing` (worker caido)."""
        conn = self._require()
        conn.execute("UPDATE ingest_gaps SET status='open' WHERE id=%s AND status='repairing'", (gap_id,))

    # ------------------------------------------------------------------ worker
    def claim(self, limit: int = 4, max_attempts: int = 5) -> list[Gap]:
        """Toma huecos abiertos para reparar, sin que dos workers possan el mismo.

        `FOR UPDATE SKIP LOCKED`: el worker de `repair/` puede correr en paralelo con el
        reconciliador diario sin que se pisen.
        """
        conn = self._require()
        out: list[Gap] = []
        with conn.transaction():
            rows = conn.execute(
                "SELECT id, exchange, symbol, dtype, EXTRACT(EPOCH FROM gap_from)*1000, "
                "EXTRACT(EPOCH FROM gap_to)*1000, reason, attempts, note FROM ingest_gaps "
                "WHERE status='open' AND attempts < %s "
                "ORDER BY gap_from LIMIT %s FOR UPDATE SKIP LOCKED",
                (max_attempts, limit)).fetchall()
            for r in rows:
                conn.execute("UPDATE ingest_gaps SET status='repairing' WHERE id=%s", (r[0],))
                out.append(Gap(exchange=r[1], symbol=r[2], dtype=r[3], gap_from_ms=int(r[4]),
                               gap_to_ms=int(r[5]), reason=r[6], id=r[0], attempts=r[7], note=r[8]))
        return out
