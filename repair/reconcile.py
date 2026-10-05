"""Reconciliacion D-1 de los volcados publicos de Bybit.

Por que existe: el WebSocket de Bybit pierde trades cuando hay cortes, y el REST
`recent-trade` solo llega ~1000 operaciones hacia atras (~2 min medidos). Para todo lo mas viejo
la unica fuente es el volcado diario, que se publica con un dia de retraso. El worker de huecos
(`repair/worker.py`) solo puede cerrar lo que el REST alcanza; esto cierra el resto.

Por que NO repara "el hueco" sino "el dia": el volcado es la verdad del dia completo, no de una
ventana. Se compara el dia entero contra lo que hay en la tabla y se inserta solo lo que falta,
por `trade_id`. Un dia ya completo no cuesta nada: el anti-join responde que no hay nada nuevo.

Garantias:
- **Nunca borra ni actualiza**. Si el volcado contradice a la tabla, manda la tabla y se avisa.
- **Idempotente**: segunda pasada sobre el mismo dia inserta 0 filas.
- **No pisa el `ts` del WS**: el anti-join va por `trade_id`, no por `(ts, trade_id)`.
"""

from __future__ import annotations

from collections import Counter
from datetime import date, datetime, timedelta, timezone

from bulk.logfmt import log
from common.exchanges import canonico
from repair.adapters.bybit import BybitAdapter
from repair.ingest import insert_trades_por_id

#: Margen para el anti-join. Acota el recorrido del indice por `ts`.
VENTANA_MS = 60_000


def ventana_del_dia(dia: date) -> tuple[int, int]:
    """`00:00:00.000` a `23:59:59.999` UTC del dia, en ms enteros."""
    desde = int(datetime(dia.year, dia.month, dia.day, tzinfo=timezone.utc).timestamp() * 1000)
    hasta = desde + 86_399_999
    return desde, hasta


def dias_desde_hasta(desde: date, hasta: date) -> list[date]:
    out, d = [], desde
    while d <= hasta:
        out.append(d)
        d += timedelta(days=1)
    return out


def reconciliar_dia(conn, symbol: str, dia: date, *, modo: str = "floor",
                    dry_run: bool = False) -> dict:
    """Compara un dia del volcado con la tabla e inserta lo que falte.

    Devuelve `disponibles`, `fuente`, `nuevas`, `repetidas` y `conflicto_ts`.
    """
    desde_ms, hasta_ms = ventana_del_dia(dia)
    dia_iso = dia.isoformat()
    ad = BybitAdapter(client=None)
    ad.ts_modo = modo
    filas = ad.leer_volcado(symbol, dia_iso)
    if filas is None:
        return {"symbol": symbol, "day": dia_iso, "disponibles": False, "fuente": 0,
                "nuevas": 0, "repetidas": 0, "conflicto_ts": 0}

    ids = {f.trade_id for f in filas}
    with conn.cursor() as cur:
        cur.execute("SET TIME ZONE 'UTC'")
        # Mismo criterio que `insert_trades_por_id`: acotado por la ventana del dia.
        cur.execute(
            "SELECT trade_id, EXTRACT(EPOCH FROM ts)*1000 FROM trades "
            "WHERE exchange=%s AND symbol=%s "
            "  AND ts >= to_timestamp(%s/1000.0) - interval '1 minute' "
            "  AND ts <= to_timestamp(%s/1000.0) + interval '1 minute' "
            "  AND trade_id = ANY(%s::text[])",
            (canonico("BYBIT"), symbol, desde_ms, hasta_ms, sorted(ids)))
        ya = {r[0]: r[1] for r in cur.fetchall()}

    fuente_ms = {f.trade_id: f.ts_ms for f in filas}
    # El volcado y la tabla pueden discrepar 1 ms si el modo de normalizacion no es el bueno.
    # No es un error de datos (el `trade_id` es el mismo) asi que no se toca nada: se cuenta.
    conflicto = sum(1 for tid, ms in ya.items()
                    if tid in fuente_ms and abs(fuente_ms[tid] - ms) > 1.5)

    if dry_run:
        nuevas = len(ids - set(ya))
        return {"symbol": symbol, "day": dia_iso, "disponibles": True, "fuente": len(filas),
                "nuevas": nuevas, "repetidas": len(filas) - nuevas, "conflicto_ts": conflicto}

    insertadas, repetidas = insert_trades_por_id(
        conn, canonico("BYBIT"), filas, "dump", desde_ms - VENTANA_MS, hasta_ms + VENTANA_MS)
    return {"symbol": symbol, "day": dia_iso, "disponibles": True, "fuente": len(filas),
            "nuevas": insertadas, "repetidas": repetidas, "conflicto_ts": conflicto}


def reconciliar(conn, symbols, *, dias: int = 1, modo: str = "floor",
                dry_run: bool = False, hoy: date | None = None) -> list[dict]:
    """D-1 (y opcionalmente mas dias hacia atras). El dia en curso se salta siempre."""
    hoy = hoy or datetime.now(timezone.utc).date()
    objetivo = [hoy - timedelta(days=n + 1) for n in range(dias)]
    out = []
    for dia in objetivo:
        for symbol in symbols:
            r = reconciliar_dia(conn, symbol, dia, modo=modo, dry_run=dry_run)
            out.append(r)
            log(component="reconcile", event="day_done", exchange="BYBIT", **r)
    return out


def verificar_alineacion(conn, symbol: str, dia: date, modo: str = "floor") -> dict:
    """Cuanto se parece el `ts` del volcado al `ts` que ya tenemos por WS.

    Resuelve la duda de `floor` vs `round` **medido** y no de memoria. Bybit publica el volcado
    en segundos con 4 decimales; al pasar a ms enteros un `floor` y un `round` dan valores
    distintos cuando el 4o decimal es >= 5. Gana el modo que produced el mismo ms que el WS.
    """
    desde_ms, hasta_ms = ventana_del_dia(dia)
    ad = BybitAdapter(client=None)
    ad.ts_modo = modo
    filas = ad.leer_volcado(symbol, dia.isoformat())
    if filas is None:
        return {"symbol": symbol, "day": dia.isoformat(), "disponible": False}

    ids = [f.trade_id for f in filas]
    with conn.cursor() as cur:
        cur.execute("SET TIME ZONE 'UTC'")
        cur.execute(
            "SELECT trade_id, EXTRACT(EPOCH FROM ts)*1000 FROM trades "
            "WHERE exchange=%s AND symbol=%s AND ts >= to_timestamp(%s/1000.0) "
            "  AND ts <= to_timestamp(%s/1000.0) AND trade_id = ANY(%s::text[])",
            (canonico("BYBIT"), symbol, desde_ms, hasta_ms, ids))
        db = {r[0]: r[1] for r in cur.fetchall()}

    if not db:
        return {"symbol": symbol, "day": dia.isoformat(), "disponible": True, "modo": modo,
                "comunes": 0, "coinciden": 0, "desplazan": {}, "nota":
                "ningun trade_id del volcado esta en la tabla: no hay solapamiento con el WS, "
                "asi que este dia no sirve para decidir el modo"}

    dump_ms = {f.trade_id: f.ts_ms for f in filas}
    deltas = Counter()
    coinciden = 0
    for tid, ms in db.items():
        d = int(round(dump_ms[tid] - ms))
        deltas[d] += 1
        if abs(d) <= 0.5:
            coinciden += 1
    comun = len(db)
    return {"symbol": symbol, "day": dia.isoformat(), "disponible": True, "modo": modo,
            "comunes": comun, "coinciden": coinciden,
            "desplazan": dict(sorted(deltas.items())),
            "nota": ("el modo coincide" if coinciden == comun else
                     f"{comun - coinciden} trades desplazados: este modo NO es el bueno")}
