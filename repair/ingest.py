"""Escritura de lo reparado: `INSERT ... FROM unnest(...)` con anti-join para los volcados.

Dos caminos, y la diferencia **no** es cosmetics:

1. **`insert_trades`**: `ON CONFLICT DO NOTHING`. Sirve para el REST, donde el `trade_id` y el `ts`
   coinciden con los que ya hay (medido: 96/96 en Binance, 151/151 en Bybit, 90/90 en Bitget,
   103/103 en OKX, 10/10 en Hyperliquid), asi que la PK`(symbol, exchange, ts, trade_id)`
   deduplica sola.

2. **`insert_trades_por_id`**: anti-join explicito por `trade_id`. **Solo para los volcados**, y
   solo porque hay un caso donde la PK no basta: el volcado de Bybit trae `timestamp` en segundos
   con 4 decimales, con precision sub-ms real (4o decimal uniforme 0-9 sobre 60.000 lineas), y al
   normalizarlo a ms puede dar un valor distinto del que ya esta en la tabla. La PK no lo reconoce
   como duplicado y lo insertaria otra vez. Aqui se filtra por `trade_id` ANTES de insertar.

   El anti-join va **acotado a `(exchange, symbol)` y a la ventana del volcado**: sin la cota de
   `ts`, el indice no unico `(exchange, symbol, trade_id)` recorre la hypertable entera. Con ella
   Timescale puede podar chunks.

Igual que en el writer del daemon: **nada de COPY**, porque no admite `ON CONFLICT` (D30).
"""

from __future__ import annotations

from datetime import datetime, timezone

TRADE_COLS = ("symbol", "exchange", "trade_id", "ts", "receipt_ts", "side", "price",
              "amount", "notional", "source")
TRADE_TYPES = ("text", "text", "text", "timestamptz", "timestamptz", "text",
               "double precision", "double precision", "double precision", "text")

CANDLE_COLS = ("symbol", "exchange", "open_time", "open", "high", "low", "close",
               "volume", "quote_volume", "trades", "taker_buy_volume")
CANDLE_TYPES = ("text", "text", "timestamptz", "double precision", "double precision",
                "double precision", "double precision", "double precision",
                "double precision", "integer", "double precision")


def _utc(ms: int) -> datetime:
    """ms enteros -> `datetime` UTC con zona. Regla 3: nunca naive ni zona local."""
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


def _insert_sql(table: str, cols: tuple[str, ...], types: tuple[str, ...]) -> str:
    lista = ", ".join(f'"{c}"' for c in cols)
    args = ", ".join(f"%s::{t}[]" for t in types)
    alias = ", ".join(f"v{i}" for i in range(1, len(cols) + 1))
    sel = ", ".join(f"u.v{i}::{t}" for i, t in enumerate(types, 1))
    # El alias de columna NO es opcional: `FROM unnest(...) AS u` deja las columnas sin nombre y
    # `u.v1` no existe. Es `AS u(v1, v2, ...)`.
    return (f"INSERT INTO {table} ({lista}) SELECT {sel} FROM unnest({args}) AS u({alias}) "
            f"ON CONFLICT DO NOTHING")


def _upsert_sql(table: str, cols: tuple[str, ...], types: tuple[str, ...],
                conflict: tuple[str, ...], updates: tuple[str, ...]) -> str:
    lista = ", ".join(f'"{c}"' for c in cols)
    args = ", ".join(f"%s::{t}[]" for t in types)
    alias = ", ".join(f"v{i}" for i in range(1, len(cols) + 1))
    sel = ", ".join(f"u.v{i}::{t}" for i, t in enumerate(types, 1))
    set_ = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in updates)
    return (f"INSERT INTO {table} ({lista}) SELECT {sel} FROM unnest({args}) AS u({alias}) "
            f"ON CONFLICT ({', '.join(conflict)}) DO UPDATE SET {set_}")


def filas_trade(exchange: str, rows) -> list[tuple]:
    """`(exchange, symbol, trade_id, ts, receipt, side, price, amount, notional, source)`."""
    out = []
    for r in rows:
        out.append((r.symbol, exchange, r.trade_id,
                    _utc(r.ts_ms), None,
                    r.side, float(r.price), float(r.amount),
                    float(r.price) * float(r.amount), "ws"))
    return out


def filas_trade_marcado(exchange: str, rows, source: str) -> list[tuple]:
    out = filas_trade(exchange, rows)
    return [tuple(list(f[:-1]) + [source]) for f in out]


def insert_trades(conn, exchange: str, rows, source: str = "rest") -> int:
    """Via REST. La PK deduplica sola porque WS y REST comparten `ts` y `trade_id`."""
    if not rows:
        return 0
    datos = filas_trade_marcado(exchange, rows, source)
    cols, tipos = list(TRADE_COLS), list(TRADE_TYPES)
    args = [list(f[i] for f in datos) for i in range(len(cols))]
    with conn.transaction():
        cur = conn.execute(_insert_sql("trades", tuple(cols), tuple(tipos)), args)
    return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0


def insert_trades_por_id(conn, exchange: str, rows, source: str,
                         ventana_desde_ms: int, ventana_hasta_ms: int) -> tuple[int, int]:
    """Via volcado: anti-join por `trade_id` acotado a la ventana. Devuelve (insertadas, repetidas).

    El paso extra es comparar contra la base ANTES de insertar, porque la PK no puede
    desempatar el `ts` del volcado con el `ts` ya guardado.
    """
    if not rows:
        return 0, 0
    ids = {r.trade_id for r in rows}
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute("SET LOCAL TIME ZONE 'UTC'")
            cur.execute(
                "SELECT trade_id FROM trades WHERE exchange=%s AND symbol=%s "
                "  AND ts >= to_timestamp(%s/1000.0) AND ts <= to_timestamp(%s/1000.0) "
                "  AND trade_id = ANY(%s::text[])",
                (exchange, rows[0].symbol, ventana_desde_ms, ventana_hasta_ms, sorted(ids)))
            ya_existentes = {r[0] for r in cur.fetchall()}
        nuevas = [r for r in rows if r.trade_id not in ya_existentes]
        repetidas = len(rows) - len(nuevas)
        if not nuevas:
            return 0, repetidas
        datos = filas_trade_marcado(exchange, nuevas, source)
        cols, tipos = list(TRADE_COLS), list(TRADE_TYPES)
        args = [list(f[i] for f in datos) for i in range(len(cols))]
        cur2 = conn.execute(_insert_sql("trades", tuple(cols), tuple(tipos)), args)
    insertadas = cur2.rowcount if cur2.rowcount and cur2.rowcount > 0 else 0
    return insertadas, repetidas


def insert_candles(conn, exchange: str, rows) -> int:
    """Velas 1m. Aqui si es `DO UPDATE`: una vela corregida debe **reemplazar** a la que habia.

    El daemon solo guarda velas cerradas, pero un hueco reparado por REST trae la version final
    de la vela; si la anterior estaba a medias (o no existia) lo correcto es sobrescribir, no
    ignorar. Con `DO NOTHING` una vela reparada no se aplicaria nunca.
    """
    if not rows:
        return 0
    datos = []
    for r in rows:
        datos.append((r.symbol, exchange, _utc(r.open_time_ms),
                      float(r.open), float(r.high), float(r.low), float(r.close),
                      float(r.volume), None, int(r.trades), None))
    cols, tipos = list(CANDLE_COLS), list(CANDLE_TYPES)
    args = [list(f[i] for f in datos) for i in range(len(cols))]
    updates = ("open", "high", "low", "close", "volume", "quote_volume", "trades",
               "taker_buy_volume")
    with conn.transaction():
        cur = conn.execute(_upsert_sql("candles_1m", tuple(cols), tuple(tipos),
                                       ("symbol", "exchange", "open_time"), updates), args)
    return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0