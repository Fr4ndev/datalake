"""Bybit: REST `/v5/market/recent-trade` (inmediato) y volcado diario de `public.bybit.com`.

**El REST solo sirve para reparar al instante.** Medido: ignora `startTime` y devuelve siempre las
~1000 operaciones mas recientes, que en BTC son ~2 minutos. Por eso hay dos caminos:

- `fetch_trades`: REST, para el gap que se acaba de abrir. Si el trade mas antiguo que devuelve es
  posterior a `gap_from`, el REST **no cubre** el hueco y se marca `partial`, no `repaired`.
- `fetch_dump_trades`: el volcado diario del dia D-1, que si tiene el dia entero. Es lo que
  repara de verdad los huecos viejos (reconciliacion diaria).

La trampa del volcado:精度 sub-milisegundo
----------------------------------------
`timestamp` viene en **SEGUNDOS con 4 decimales** y el 4o decimal se distribuye de forma uniforme
entre 0 y 9 (medido sobre 60.000 lineas), o sea que no es ruido de formato: Bybit guarda mas
precision que el ms que expone por WS y REST. Redondear a ms puede dar un valor distinto del que
ya esta en la tabla, y entonces la PK `(symbol, exchange, ts, trade_id)` **no lo reconoce** como
duplicado: lo insertaria otra vez. Por eso el volcado se deduplica por `trdMatchID` de forma
explicita y acotada, no confiando en la PK.
"""

from __future__ import annotations

import csv
import gzip
import io
import math
import urllib.request
from datetime import datetime, timezone

from bulk.logfmt import log

from .base import Adapter, CandleRow, RepairResult, TradeRow, side_normalized

RECENT_TRADE = "https://api.bybit.com/v5/market/recent-trade"
KLINE = "https://api.bybit.com/v5/market/kline"
DUMP_BASE = "https://public.bybit.com/trading"

#: Margen de la ventana del anti-join. Acota el recorrido del indice para que Timescale pode
#: chunks; sin esta cota el indice no unico recorre la hypertable entera.
VENTANA_ANTIJOIN_MS = 60_000


def ms_desde_segundos(valor: str | float, modo: str = "floor") -> int:
    """`1790985600.1199` -> ms enteros, con la normalizacion que se elija.

    - `floor` (por defecto): trunca. `int(1790985600.1199 * 1000)` -> 1790985600119.
    - `round`: half-up. -> 1790985600120.

    Cual coincide con el ms que Bybit expone por WS/REST hay que **medirlo** en un dia que tenga
    ambas fuentes (`repair verify-dump-alignment`); el anti-join hace que la correccion no dependa
    de ello, pero un ts coherente evita duplicados cuando el REST repare despues la misma ventana.
    """
    seg = float(valor)
    if modo == "round":
        return int(math.floor(seg * 1000.0 + 0.5))
    return int(math.floor(seg * 1000.0))


class BybitAdapter(Adapter):
    exchange = "BYBIT"
    name = "bybit"

    def __init__(self, client):
        self.http = client
        self.ts_modo = "floor"

    def can_repair(self, gap) -> tuple[bool, str | None]:
        return True, None

    # -------------------------------------------------------------- REST inmediato
    def fetch_trades(self, gap) -> RepairResult:
        datos = self.http.get(self.exchange, RECENT_TRADE, {
            "category": "linear", "symbol": gap.symbol, "limit": 1000})
        if datos.get("retCode") != 0:
            return RepairResult(source="rest", limitation=f"retCode {datos.get('retCode')}",
                                note=str(datos.get("retMsg"))[:200])
        filas: dict[str, TradeRow] = {}
        for x in datos["result"]["list"]:
            fila = TradeRow(
                trade_id=str(x["execId"]), ts_ms=int(x["time"]),
                side=side_normalized(x.get("side")), price=float(x["price"]),
                amount=float(x["size"]), symbol=gap.symbol)
            if gap.gap_from_ms <= fila.ts_ms <= gap.gap_to_ms:
                filas[fila.trade_id] = fila
        mas_antiguo = min((int(x["time"]) for x in datos["result"]["list"]), default=None)
        limitacion = None
        if mas_antiguo is not None and mas_antiguo > gap.gap_from_ms:
            # El REST no llega tan atras: el hueco quedara parcial hasta el volcado del dia.
            limitacion = (f"recent-trade solo llega a {mas_antiguo} ms y el hueco empieza en "
                          f"{gap.gap_from_ms} ms: hacen falta {gap.gap_from_ms - mas_antiguo} ms "
                          f"que solo estan en el volcado diario")
        return RepairResult(
            rows=list(filas.values()), source="rest",
            covered_from_ms=mas_antiguo,
            covered_through_ms=gap.gap_to_ms if filas else None,
            limitation=limitacion,
            note=f"{len(filas)} trades por recent-trade (~2 min de cobertura)")

    # -------------------------------------------------------------- volcado diario
    def fetch_dump_trades(self, gap, dia: str | None = None) -> RepairResult:
        dias = [dia] if dia else _dias_utc(gap.gap_from_ms, gap.gap_to_ms)
        filas: dict[str, TradeRow] = {}
        dias_leidos = []
        for d in dias:
            leidas = self.leer_volcado(gap.symbol, d)
            if leidas is None:
                continue
            dias_leidos.append(d)
            for fila in leidas:
                if gap.gap_from_ms <= fila.ts_ms <= gap.gap_to_ms:
                    filas[fila.trade_id] = fila
        # La cobertura se declara dia a dia: si uno de los dias no se pudo leer, el hueco NO esta
        # cubierto aunque haya miles de filas. Marcarlo entero por "llego alguna fila" deja huecos
        # rotos con apariencia de cerrados.
        pedidos = set(dias)
        leidos = set(dias_leidos)
        faltan = pedidos - leidos
        covered = None
        if filas and not faltan:
            covered = (gap.gap_from_ms, gap.gap_to_ms)
        return RepairResult(
            rows=list(filas.values()), source="dump",
            covered_from_ms=covered[0] if covered else None,
            covered_through_ms=covered[1] if covered else None,
            limitation=(f"faltan los volcados de {','.join(sorted(faltan))}: el dia se esta "
                        "serviendo todavia o no existe") if faltan else None,
            note=f"{len(filas)} trades de los volcados de {','.join(dias)} (ts normalizado con "
                 f"'{self.ts_modo}')")

    def leer_volcado(self, symbol: str, dia: str) -> list[TradeRow] | None:
        """TradeRows de un dia del volcado, o `None` si ese dia no esta disponible.

        `None` y no `[]` a proposito: "el dia no existe todavia" y "el dia existe y no tiene
        trades" son cosas distintas y el llamante las tiene que poder distinguir.
        """
        url = f"{DUMP_BASE}/{symbol}/{symbol}{dia}.csv.gz"
        try:
            crudo = _descargar(url)
        except Exception as exc:  # noqa: BLE001 - un dia sin volcado no es un error fatal
            log(component="repair", event="dump_missing", exchange=self.exchange,
                symbol=symbol, day=dia, error=str(exc)[:140])
            return None
        # Deduplicar DENTRO del volcado antes de comparar con la BD: el CSV puede traer filas
        # repetidas y compararlas de mas solo encarece el anti-join.
        salida: dict[str, TradeRow] = {}
        for fila in _parsear_dump(crudo, symbol, self.ts_modo):
            salida[fila.trade_id] = fila
        return list(salida.values())

    def fetch_candles(self, gap) -> RepairResult:
        filas: dict[int, CandleRow] = {}
        cursor = gap.gap_from_ms
        while cursor <= gap.gap_to_ms:
            datos = self.http.get(self.exchange, KLINE, {
                "category": "linear", "symbol": gap.symbol, "interval": "1",
                "start": int(cursor), "end": int(min(cursor + 1000 * 60_000 - 1, gap.gap_to_ms)),
                "limit": 1000})
            if datos.get("retCode") != 0 or not datos["result"]["list"]:
                break
            pagina = datos["result"]["list"]
            for x in pagina:
                fila = CandleRow(open_time_ms=int(x[0]), open=float(x[1]), high=float(x[2]),
                                 low=float(x[3]), close=float(x[4]), volume=float(x[5]),
                                 trades=0, symbol=gap.symbol)
                filas[fila.open_time_ms] = fila
            cursor = int(pagina[0][0]) + 60_000
        dentro = [f for f in filas.values() if gap.gap_from_ms - 60_000 <= f.open_time_ms <= gap.gap_to_ms]
        return RepairResult(
            rows=dentro, source="rest",
            covered_from_ms=gap.gap_from_ms if dentro else None,
            covered_through_ms=gap.gap_to_ms if dentro else None,
            note=f"{len(dentro)} velas 1m de Bybit")


def _dias_utc(desde_ms: int, hasta_ms: int) -> list[str]:
    d1 = datetime.fromtimestamp(desde_ms / 1000, tz=timezone.utc).date()
    d2 = datetime.fromtimestamp(hasta_ms / 1000, tz=timezone.utc).date()
    out, d = [], d1
    while d <= d2:
        out.append(d.isoformat())
        d = (d.fromordinal(d.toordinal() + 1))
    return out


def _descargar(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "cripto-marketdata/0.2"})
    with urllib.request.urlopen(req, timeout=180) as resp:
        return resp.read()


def _parsear_dump(crudo: bytes, symbol: str, modo: str) -> list[TradeRow]:
    """`timestamp,symbol,side,size,price,...,trdMatchID,...` con timestamp en SEGUNDOS."""
    texto = gzip.decompress(crudo).decode()
    salida: dict[str, TradeRow] = {}
    for fila in csv.DictReader(io.StringIO(texto)):
        tid = fila["trdMatchID"]
        if tid in salida:
            continue  # dedup dentro del propio volcado
        salida[tid] = TradeRow(
            trade_id=tid,
            ts_ms=ms_desde_segundos(fila["timestamp"], modo),
            side=side_normalized(fila.get("side")), price=float(fila["price"]),
            amount=float(fila["size"]), symbol=symbol)
    return list(salida.values())
