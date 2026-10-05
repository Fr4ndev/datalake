"""Escritura a TimescaleDB: buffer en memoria -> COPY por lotes -> ON CONFLICT DO NOTHING.

Decisiones que NO son obvias:

1. **Por que NO se usa COPY.** COPY es lo mas rapido para meter filas, pero **no soporta
   `ON CONFLICT DO NOTHING`**: no hay clausula de conflicto que se le pueda anadir. Como la
   idempotencia es requisito duro (corte de red + resubscribe = reenvio), COPY solo se podria usar
   deduplicando antes en memoria, lo que no es posible sin perder memoria. Se usa en su lugar un
   `INSERT ... SELECT ... FROM unnest(...) ON CONFLICT DO NOTHING`: **una sola ida y vuelta por
   lote** y con la deduplicacion en la base. Es el mismo patron que usa el backend de Postgres de
   cryptofeed (`cryptofeed/backends/postgres.py::_build_insert`), que es justamente el "patron" que
   manda copiar en la skill.

2. **Por que todo lleva ON CONFLICT DO NOTHING.** Tras un corte de red cryptofeed resuscribe y
   reenvia lo ultimo. Sin DO NOTHING los reenvios duplican trades y OI. Con el, reenviar es
   idempotente por construccion y no hace falta tabla de deduplicacion.

3. **Por que `candles` solo entra si `closed`.** Una vela en curso cambia con cada tick. Con
   DO NOTHING se guardaria la primera version (incompleta) y las siguientes se ignorarian. Guardando
   solo velas cerradas, DO NOTHING sigue siendo correcto y el historico lo completa el backfill
   diario del lake.

4. **Por que las columnas van en orden fijo y declaradas aqui.** El `unnest` alinea cada columna
   por posicion, asi que el orden de `columns` tiene que coincidir con el de `pg_types` y con el de
   la fila que hace `push`. Cada `Writer` declara los tres en el mismo sitio y el `flush` los
   recorre siempre en ese orden. Si se desincronizan, el precio acaba en `ts` sin que nada falle.

5. **Como se mide el AC de p95.** `add_latency` guarda el intervalo entre la llegada del tick y su
   volcado a la base; `p95_ms` lo agrega. Se mide en el writer porque es el unico sitio donde se
   sabe que la fila ha llegado de verdad.
"""

from __future__ import annotations

import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone

from common.db import conninfo
from common.exchanges import canonico
from bulk.logfmt import log

#: cryptofeed entrega epoch en segundos, pero no esta garantizado para todos los exchanges
#: (regla 3 de AGENTS.md: autodetectar por magnitud, nunca asumir).
#:
#: Los umbrales van por DECADA, no "redondo": un epoch en segundos son ~1.8e9, en ms ~1.8e12, en us
#: ~1.8e15 y en ns ~1.8e18. Si el corte se pone en 1e16, los microsegundos (1.8e15) caen en el
#: bucket de milisegundos y `fromtimestamp` revienta con "year 57971 is out of range" en vez de
#: dar un timestamp silenciosamente equivocado.
def to_utc(value) -> datetime | None:
    """Epoch -> datetime UTC, autodetectando la unidad por magnitud."""
    if value is None:
        return None
    v = float(value)
    if v == 0:
        return None
    magnitude = abs(v)
    if magnitude >= 1e17:
        v = v / 1e9  # nanosegundos
    elif magnitude >= 1e14:
        v = v / 1e6  # microsegundos
    elif magnitude >= 1e11:
        v = v / 1e3  # milisegundos
    return datetime.fromtimestamp(v, tz=timezone.utc)


def _f(value):
    """cryptofeed devuelve Decimal para precios y tamanos; a DOUBLE PRECISION."""
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return None if f != f else f  # descarta NaN: NaN no es NOT NULL y rompe aserciones


@dataclass
class Writer:
    """Un buffer + su INSERT, para una combinacion de tabla y columnas."""

    table: str
    columns: tuple[str, ...]
    #: Tipo Postgres de cada columna, en el MISMO orden que `columns`. El `unnest` alinea por
    #: posicion, asi que una desincronizacion entre las tres listas escribiria datos en la columna
    #: equivocada sin fallar.
    pg_types: tuple[str, ...]
    max_rows: int = 1000
    buf: deque = field(default_factory=deque, repr=False)
    rows_written: int = 0
    rows_dropped: int = 0
    latencies_ms: list[float] = field(default_factory=list, repr=False)

    def __len__(self) -> int:
        return len(self.buf)

    def push(self, row: tuple, ingest_ts: float | None = None) -> None:
        """Encola una fila. `ingest_ts` es `time.monotonic()` de cuando llego el tick.

        Se guarda por fila y no por lote porque es lo que hace falta para el AC: la latencia
        trade->fila es la diferencia entre **su** llegada y el volcado. Si se midiera una sola vez
        por lote, las filas queURNaron al principio del lote aparecerian como mas rapidas de lo que
        fueron.
        """
        self.buf.append((row, ingest_ts))

    def _insert_sql(self) -> str:
        """`INSERT ... SELECT FROM unnest(...) ON CONFLICT DO NOTHING`, una ida y vuelta por lote.

        Cada columna se pasa como un array de PostgreSQL y `unnest` los re-expone en filas. Al
        haber pocas columnas y muchas filas, es lo que mas se gana frente a un VALUES clasico.
        """
        if len(self.columns) != len(self.pg_types):
            raise ValueError(
                f"{self.table}: columns y pg_types desincronizados "
                f"({len(self.columns)} vs {len(self.pg_types)})"
            )
        cols = ", ".join(f'"{c}"' for c in self.columns)
        args = ", ".join(f"%s::{t}[]" for t in self.pg_types)
        alias = ", ".join(f"v{i}" for i in range(1, len(self.columns) + 1))
        sel = ", ".join(f"u.v{i}::{t}" for i, t in enumerate(self.pg_types, 1))
        return (
            f"INSERT INTO {self.table} ({cols}) "
            f"SELECT {sel} FROM unnest({args}) AS u({alias}) "
            f"ON CONFLICT DO NOTHING"
        )


class Store:
    """Pool de conexiones + buffers. Un unico objeto para todos los dtypes."""

    #: dtype -> (tabla, columnas, tipos). Los tres en el MISMO orden: ver `Writer._insert_sql`.
    TARGETS: dict[str, tuple[str, tuple[str, ...], tuple[str, ...]]] = {
        "trades": (
            "trades",
            ("symbol", "exchange", "trade_id", "ts", "receipt_ts", "side", "price", "amount", "notional"),
            ("text", "text", "text", "timestamptz", "timestamptz", "text",
             "double precision", "double precision", "double precision"),
        ),
        "funding": (
            "funding",
            ("symbol", "exchange", "funding_time", "funding_rate", "mark_price", "next_funding_time"),
            ("text", "text", "timestamptz", "double precision", "double precision", "timestamptz"),
        ),
        "open_interest": (
            "open_interest",
            ("symbol", "exchange", "ts", "open_interest"),
            ("text", "text", "timestamptz", "double precision"),
        ),
        "liquidations": (
            "liquidations",
            ("symbol", "exchange", "ts", "side", "price", "quantity", "notional"),
            ("text", "text", "timestamptz", "text",
             "double precision", "double precision", "double precision"),
        ),
        "candles": (
            "candles_1m",
            ("symbol", "exchange", "open_time", "open", "high", "low", "close", "volume", "trades"),
            ("text", "text", "timestamptz", "double precision", "double precision",
             "double precision", "double precision", "double precision", "integer"),
        ),
    }

    def __init__(self, max_rows: int = 1000):
        self.max_rows = max_rows
        self.pool = None
        self.writers: dict[str, Writer] = {
            dtype: Writer(table=table, columns=cols, pg_types=types, max_rows=max_rows)
            for dtype, (table, cols, types) in self.TARGETS.items()
        }

    # ---------------------------------------------------------------- conexion
    def open(self) -> None:
        import psycopg_pool

        self.pool = psycopg_pool.ConnectionPool(
            conninfo(),
            min_size=1,
            max_size=4,  # la skill lo fija: daemon single-process, nunca conexion por tick
            open=False,
            kwargs={"autocommit": True, "options": "-c timezone=UTC"},
        )
        self.pool.open(wait=True, timeout=30)
        with self.pool.connection() as conn:
            conn.execute("SET TIME ZONE 'UTC'")

    def close(self) -> None:
        if self.pool is not None:
            self.pool.close()
            self.pool = None

    # ---------------------------------------------------------------- enqueue
    def add_trade(self, t, receipt) -> None:
        symbol = self._sym(t.exchange, t.symbol)
        ts = to_utc(t.timestamp)
        price, amount = _f(t.price), _f(t.amount)
        if not symbol or ts is None or price is None or amount is None:
            self.writers["trades"].rows_dropped += 1
            return
        side = t.side if t.side in ("buy", "sell") else "buy"
        self.writers["trades"].push(
            (symbol, canonico(t.exchange), str(t.id), ts, to_utc(receipt), side, price, amount, price * amount),
            ingest_ts=time.monotonic(),
        )

    def add_funding(self, f, receipt) -> None:
        symbol = self._sym(f.exchange, f.symbol)
        ts = to_utc(f.timestamp)
        rate = _f(getattr(f, "rate", None))
        if not symbol or ts is None or rate is None:
            self.writers["funding"].rows_dropped += 1
            return
        self.writers["funding"].push(
            (symbol, canonico(f.exchange), ts, rate, _f(getattr(f, "mark_price", None)), to_utc(getattr(f, "next_funding_time", None)))
        )

    def add_open_interest(self, oi, receipt) -> None:
        symbol = self._sym(oi.exchange, oi.symbol)
        ts = to_utc(oi.timestamp)
        val = _f(getattr(oi, "open_interest", None))
        if not symbol or ts is None or val is None:
            self.writers["open_interest"].rows_dropped += 1
            return
        self.writers["open_interest"].push((symbol, canonico(oi.exchange), ts, val))

    def add_liquidation(self, liq, receipt) -> None:
        symbol = self._sym(liq.exchange, liq.symbol)
        ts = to_utc(liq.timestamp)
        price, qty = _f(liq.price), _f(liq.quantity)
        if not symbol or ts is None or price is None or qty is None:
            self.writers["liquidations"].rows_dropped += 1
            return
        side = liq.side if liq.side in ("buy", "sell") else "buy"
        self.writers["liquidations"].push(
            (symbol, canonico(liq.exchange), ts, side, price, qty, price * qty)
        )

    def add_candle(self, c, receipt) -> None:
        # Solo velas cerradas: ver punto 3 del docstring.
        if not getattr(c, "closed", False):
            return
        symbol = self._sym(c.exchange, c.symbol)
        ts = to_utc(getattr(c, "start", None))
        if not symbol or ts is None:
            self.writers["candles"].rows_dropped += 1
            return
        o, h, lo, cl, v = (_f(c.open), _f(c.high), _f(c.low), _f(c.close), _f(c.volume))
        if None in (o, h, lo, cl, v):
            self.writers["candles"].rows_dropped += 1
            return
        n = getattr(c, "trades", None)
        self.writers["candles"].push(
            (symbol, canonico(c.exchange), ts, o, h, lo, cl, v, int(n) if n is not None else None)
        )

    #: Cifras que el proyecto trata como el mismo perpetuo. El lake solo tiene perps USDT, asi
    #: que USD (Hyperliquid) y USDC colapsan a USDT: son el mismo instrumento economico.
    QUOTES = ("USDT", "USDC", "USD")
    #: Sufijos de contrato USDT-marginado que aparecieron en cryptofeed 2.x (`BTCUSDT_UMCBL`).
    #: En 3.0.1 Bitget usa `BTCUSDT_USDT-FUTURES`, pero se dejan por si se vuelve a ver.
    LEGACY_USDT_QUOTES = ("UMCBL", "DMCBL", "CMCBL")
    #: Tokens que son tipo de contrato, no cifra. En un proyecto que solo sigue perps USDT,
    #: `BTC-PERP` o `BTC-FUTURES` son el perpetuo USDT: no hay otra serie a la que pertenecer.
    CONTRACT_TYPES = ("PERP", "SWAP", "FUTURES", "PERPETUAL", "SPOT")

    @staticmethod
    def _sym(exchange: str, raw: str) -> str:
        """Normaliza el simbolo de cryptofeed al perpetuo USDT del proyecto.

        El proyecto nombra los simbolos `BTCUSDT` (ver `lake/binance/klines/symbol=BTCUSDT` y el
        manifest), y si el daemon usara el nombre del exchange el loader escribiria en otra serie
        y el backtest no encontraria nada.

        cryptofeed entrega el simbolo **normalizado** (`Trade(self.id,
        self.exchange_symbol_to_std_symbol(...))`), que tiene forma `BASE-QUOTE-PERP`. Medido en
        3.0.1, lo que llega al callback es:

            binance/bybit/okx/bitget   BTC-USDT-PERP
            hyperliquid                BTC-USD-PERP

        Asi que se parsea `BASE` y `QUOTE` por separado en vez de recortar sufijos. Recortar
        "PERP" y pegar "USDT" detras daba `BTCUSDUSDT` para Hyperliquid, porque su QUOTE es USD y
        no USDT: un symbol equivocado que no revienta por ningun lado, solo escribe en otra serie.

        Tamien se aceptan los nativos por si el exchange devolviera el suyo, medidos en
        `Feed.symbol_mapping()`: `BTCUSDT` (Binance, Bybit), `BTC-USDT-SWAP` (OKX),
        `BTCUSDT_USDT-FUTURES` (Bitget), `BTC` (Hyperliquid).
        """
        parts = [p for p in raw.upper().replace("/", "-").replace("_", "-").split("-") if p]
        if not parts:
            return ""
        base = parts[0]
        quote = parts[1] if len(parts) > 1 else ""

        # `BTCUSDT_USDT-FUTURES` de Bitget parte en [BTCUSDT, USDT, FUTURES]: la base ya trae la
        # cifra pegada, asi que se recorta antes de decidir.
        for q in Store.QUOTES:
            if len(base) > len(q) and base.endswith(q):
                base = base[: -len(q)]
                quote = quote or q
                break

        if not quote:
            # Sin cifra: solo puede ser el nativo de un perps (Hyperliquid entrega `BTC`). Como
            # el proyecto es solo perps USDT, se asume USDT. Un simbolo con BASE y nada mas no
            # tiene otra serie a la que pudiera pertenecer.
            return f"{base}USDT"
        if quote in Store.LEGACY_USDT_QUOTES or quote in Store.CONTRACT_TYPES:
            return f"{base}USDT"
        if quote not in Store.QUOTES:
            # No es un perps de una cifra que el proyecto siga (p.ej. BTC-EUR). No se inventa.
            return ""
        return f"{base}USDT"

    # ---------------------------------------------------------------- flush
    def ready(self) -> bool:
        return any(len(w) >= w.max_rows for w in self.writers.values())

    def flush(self, reason: str = "size") -> int:
        total = 0
        for dtype, w in self.writers.items():
            if not w.buf:
                continue
            # Se mide antes del INSERT: el objetivo es cuanto tarda la fila en estar en la tabla.
            t_flush = time.monotonic()
            pending = list(w.buf)
            w.buf.clear()
            batch = [row for row, _ in pending]
            if dtype == "trades":
                for _, ingest in pending:
                    if ingest is not None:
                        self.add_latency((t_flush - ingest) * 1000.0)
            try:
                n = self._insert(w, batch)
                w.rows_written += n
                total += n
            except Exception as exc:  # noqa: BLE001
                # Un lote que falla NO se reintenta a ciegas: se cuenta como caido y se avisa. Perder
                # un lote es visible en el log; un bucle infinito de reintentos mata el daemon.
                w.rows_dropped += len(batch)
                log(component="feed", event="flush_error", dtype=dtype, rows=len(batch),
                    error=str(exc)[:160])
        if total or reason == "shutdown":
            p95 = self.p95_ms()
            log(component="feed", event="flush", reason=reason, rows=total,
                buffered=sum(len(w) for w in self.writers.values()), p95_ms=f"{p95:.0f}")
        return total

    def _insert(self, w: Writer, batch: list[tuple]) -> int:
        """Un INSERT batch con `unnest` + ON CONFLICT. Devuelve las filas enviadas.

        `rowcount` con `ON CONFLICT DO NOTHING` cuenta solo las insertadas, asi que se usa para
        detectar reenvios: si el exchange manda lo mismo dos veces, el segundo lote da 0.
        """
        sql = w._insert_sql()
        # Columna a columna: cada elemento es la lista de valores de esa columna en el lote.
        params = [list(col) for col in zip(*batch)]
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.rowcount if cur.rowcount and cur.rowcount > 0 else len(batch)

    # ---------------------------------------------------------------- metricas
    def p95_ms(self) -> float:
        """p95 de la latencia trade->fila de los ultimos 5 min, en ms."""
        if not self.writers["trades"].latencies_ms:
            return 0.0
        xs = sorted(self.writers["trades"].latencies_ms)[-5000:]
        return xs[min(len(xs) - 1, int(len(xs) * 0.95))]

    def add_latency(self, ms: float) -> None:
        lst = self.writers["trades"].latencies_ms
        lst.append(ms)
        if len(lst) > 20000:  # ventana deslizante: 20k muestras es de sobra para un p95
            del lst[: len(lst) - 10000]

    def stats(self) -> dict[str, dict[str, int]]:
        return {
            dtype: {"written": w.rows_written, "dropped": w.rows_dropped, "buffered": len(w)}
            for dtype, w in self.writers.items()
        }