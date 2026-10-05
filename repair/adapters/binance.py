"""Binance UM: aggTrades por REST (hasta ~48 h) y volcado de data.binance.vision (historia).

Por que **aggTrades** y no el stream `trade`: cryptofeed 3.0.1 ya usa `aggTrade` para el WS
(`cryptofeed/exchanges/binance.py:43`, `websocket_channels = {TRADES: 'aggTrade'}`) y pone
`id=str(msg['a'])`. El endpoint `/fapi/v1/aggTrades` devuelve el mismo `a`. Por eso el reparador y
el daemon hablan el mismo idioma y la PK deduplica. Medido: 96/96 ids de WS presentes en el REST.

Por que el volcado ademas del REST: el REST **no llega mas atras de ~48 h** (medido:
`startTime` a 48 h OK, a 60 h -> `-4166 "Search window is restricted to recent 2 days only"`), pero
`data.binance.vision/data/futures/um/daily/aggTrades/` si tiene todos los dias desde 2019-12-31 con
la misma columna `agg_trade_id`. Sin el volcado, cualquier hueco de mas de dos dias de Binance
quedaria irrecuperable, y un reinicio del contenedor de un fin de semana lo es.
"""

from __future__ import annotations

import csv
import io
import urllib.request
import zipfile
from datetime import date, datetime, timedelta, timezone

from bulk.logfmt import log

from ..http import Client, HttpError
from .base import Adapter, RepairResult, TradeRow, side_from_is_buyer_maker

AGG_TRADES = "https://fapi.binance.com/fapi/v1/aggTrades"
KLINES = "https://fapi.binance.com/fapi/v1/klines"
VISION = "https://data.binance.vision"

#: Ventana maxima por peticion de aggTrades. El limite esta en ~2 dias, pero con 1000 registros
#: por pagina y BTC moviendo ~2000 aggTrades/hora, 1 hora de ventana son un puñado de paginas.
#: Se usa 30 min para tener margen de error ante el -4166.
VENTANA_MS = 30 * 60 * 1000
LIMITE_PAGINA = 1000
MAX_PAGINAS = 400  # tope de seguridad: 400 paginas x 1000 = 400k trades

#: Profundidad del REST medida: `startTime` tiene que estar dentro de ~48 h.
REST_MAX_AGE_MS = 47 * 3600 * 1000


class BinanceFuturesAdapter(Adapter):
    exchange = "binance_um"
    name = "binance"

    def __init__(self, client: Client, now_ms: int | None = None):
        self.http = client
        self._now_ms = now_ms

    def now_ms(self) -> int:
        return self._now_ms if self._now_ms is not None else int(datetime.now(tz=timezone.utc).timestamp() * 1000)

    # ------------------------------------------------------------------ trades
    def can_repair(self, gap) -> tuple[bool, str | None]:
        if gap.dtype != "trades":
            return True, None
        antiguedad = self.now_ms() - gap.gap_from_ms
        if antiguedad <= REST_MAX_AGE_MS:
            return True, None
        return True, None  # el volcado cubre lo que el REST no cubre

    def fetch_trades(self, gap) -> RepairResult:
        ahora = self.now_ms()
        if gap.gap_from_ms >= ahora - REST_MAX_AGE_MS:
            return self._por_rest(gap)
        # El REST no cubre tan atras: se repara con el volcado, que si llega a 2019.
        return self._por_volcado(gap)

    # -------------------------------------------------------------- REST
    def _por_rest(self, gap) -> RepairResult:
        filas: dict[str, TradeRow] = {}
        cubierta_hasta = gap.gap_from_ms
        truncado_en: int | None = None
        # Se avanza en ventanas de 30 min porque `startTime`+`endTime` se rechaza si la ventana
        # es demasiado ancha. Dentro de cada ventana, si la pagina viene LLENA hay que pedir la
        # siguiente por `fromId`: el endpoint devuelve `limit` filas y ninguna pista de si quedan
        # mas. Antes se daba por agotada y `covered_through` avanzaba igual, con lo que un hueco
        # de 20 min con 12.000 trades se declaraba `repaired` con 1.000 filas insertadas.
        cursor = gap.gap_from_ms
        for _ in range(MAX_PAGINAS):
            fin = min(cursor + VENTANA_MS, gap.gap_to_ms + 1000)
            agotada, motivo = self._ventana(gap, cursor, fin, filas)
            if not agotada:
                truncado_en = cursor
                break
            # `covered_through` es el FIN DE LA VENTANA CONSULTADA, no el ts del ultimo trade.
            # Una respuesta vacia (o cuya ultima fila es anterior) es precisamente la prueba de que
            # no habia trades ahi. Medirlo por el ultimo trade daba huecos "partial" con 929 filas
            # ya insertadas, que es lo que obliga al worker a reintentarlo para siempre.
            cubierta_hasta = max(cubierta_hasta, fin)
            if fin >= gap.gap_to_ms:
                break
            cursor = fin + 1
        limitacion = None
        if truncado_en is not None:
            limitacion = (f"el REST se quedo corto: tras {len(filas)} aggTrades la ventana que "
                          f"empieza en {truncado_en} seguia llena de {LIMITE_PAGINA} filas "
                          f"({motivo}), asi que no se sabe si faltan trades mas alla")
        return RepairResult(
            rows=[r for r in filas.values() if gap.gap_from_ms <= r.ts_ms <= gap.gap_to_ms],
            source="rest",
            covered_from_ms=gap.gap_from_ms,
            covered_through_ms=None if truncado_en is not None else cubierta_hasta,
            note=f"{len(filas)} aggTrades por REST en ventanas de 30 min",
            limitation=limitacion,
        )

    def _ventana(self, gap, desde_ms: int, hasta_ms: int,
                 filas: dict[str, TradeRow]) -> tuple[bool, str]:
        """Recorre UNA ventana, paginando por `fromId`. Devuelve (agotada, motivo).

        `fromId` y `startTime`/`endTime` son excluyentes en Binance: si se manda `fromId`, la
        pagina empieza en ese trade id y `endTime` se ignora. Por eso al seguir un `fromId` hay
        que filtrar por `ts` para no arrastrar trades de la ventana siguiente, y la ventana se
        considera agotada cuando la pagina viene corta o cuando la ultima fila ya paso su `fin`.
        """
        siguiente_id: int | None = None
        for _ in range(MAX_PAGINAS):
            pagina = self._agg_trades(gap.symbol, desde_ms, hasta_ms, desde_id=siguiente_id)
            if not pagina:
                return True, "pagina vacia"
            for fila in pagina:
                if desde_ms <= fila.ts_ms <= hasta_ms:
                    filas[fila.trade_id] = fila
            ultima = pagina[-1]
            if len(pagina) < LIMITE_PAGINA:
                return True, "pagina corta"
            if ultima.ts_ms > hasta_ms:
                return True, "la ultima fila ya paso el fin de la ventana"
            siguiente_id = int(ultima.trade_id) + 1
        return False, f"agotadas {MAX_PAGINAS} paginas de {LIMITE_PAGINA} filas"

    def _agg_trades(self, symbol: str, desde_ms: int, hasta_ms: int,
                    desde_id: int | None = None) -> list[TradeRow]:
        params: dict = {"symbol": symbol, "limit": LIMITE_PAGINA}
        if desde_id is None:
            params["startTime"] = int(desde_ms)
            params["endTime"] = int(hasta_ms)
        else:
            params["fromId"] = int(desde_id)
        datos = self.http.get(self.exchange, AGG_TRADES, params)
        salida: list[TradeRow] = []
        for x in datos if isinstance(datos, list) else []:
            salida.append(TradeRow(
                trade_id=str(x["a"]),
                ts_ms=int(x["T"]),
                side=side_from_is_buyer_maker(bool(x.get("m"))),
                price=float(x["p"]), amount=float(x["q"]),
                symbol=symbol))
        salida.sort(key=lambda r: r.ts_ms)
        return salida

    # -------------------------------------------------------------- volcado
    def _por_volcado(self, gap) -> RepairResult:
        dias = _rango_dias(gap.gap_from_ms, gap.gap_to_ms)
        filas: dict[str, TradeRow] = {}
        for dia in dias:
            url = (f"{VISION}/data/futures/um/daily/aggTrades/{gap.symbol}/"
                   f"{gap.symbol}-aggTrades-{dia}.zip")
            try:
                crudo = _descargar(url)
            except HttpError as exc:
                log(component="repair", event="dump_missing", exchange=self.exchange,
                    symbol=gap.symbol, day=dia, error=str(exc)[:120])
                continue
            for fila in _parsear_volcado(crudo, gap.symbol):
                if gap.gap_from_ms <= fila.ts_ms <= gap.gap_to_ms:
                    filas[fila.trade_id] = fila
        cubierta = (gap.gap_from_ms, gap.gap_to_ms) if filas else (None, None)
        return RepairResult(
            rows=list(filas.values()), source="dump",
            covered_from_ms=cubierta[0], covered_through_ms=cubierta[1],
            note=f"{len(filas)} aggTrades del volcado de Vision en {len(dias)} dia(s)")

    # -------------------------------------------------------------- velas
    def fetch_candles(self, gap) -> RepairResult:
        filas: dict[int, object] = {}
        cursor = gap.gap_from_ms
        while cursor <= gap.gap_to_ms:
            fin = cursor + 1500 * 60 * 1000  # Binance admite 1500 velas por peticion
            datos = self.http.get(self.exchange, KLINES, {
                "symbol": gap.symbol, "interval": "1m",
                "startTime": int(cursor), "endTime": int(fin), "limit": 1500})
            for x in datos if isinstance(datos, list) else []:
                from .base import CandleRow
                fila = CandleRow(
                    open_time_ms=int(x[0]), open=float(x[1]), high=float(x[2]), low=float(x[3]),
                    close=float(x[4]), volume=float(x[5]), trades=int(x[8]), symbol=gap.symbol)
                filas[fila.open_time_ms] = fila
            if not datos:
                break
            cursor = int(datos[-1][0]) + 60_000
        desde = gap.gap_from_ms - 60_000
        return RepairResult(
            rows=list(filas.values()), source="rest", covered_from_ms=desde,
            covered_through_ms=gap.gap_to_ms + 60_000,
            note=f"{len(filas)} velas 1m por REST")


def _rango_dias(desde_ms: int, hasta_ms: int) -> list[str]:
    d1 = datetime.fromtimestamp(desde_ms / 1000, tz=timezone.utc).date()
    d2 = datetime.fromtimestamp(hasta_ms / 1000, tz=timezone.utc).date()
    out, d = [], d1
    while d <= d2:
        out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def _descargar(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "cripto-marketdata/0.2"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return resp.read()


def _parsear_volcado(crudo: bytes, symbol: str) -> list[TradeRow]:
    """`agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker`."""
    with zipfile.ZipFile(io.BytesIO(crudo)) as z:
        nombre = z.namelist()[0]
        texto = z.read(nombre).decode()
    salida: list[TradeRow] = []
    for fila in csv.DictReader(io.StringIO(texto)):
        salida.append(TradeRow(
            trade_id=str(fila["agg_trade_id"]),
            ts_ms=int(fila["transact_time"]),
            side=side_from_is_buyer_maker(str(fila["is_buyer_maker"]).lower() == "true"),
            price=float(fila["price"]), amount=float(fila["quantity"]), symbol=symbol))
    salida.sort(key=lambda r: r.ts_ms)
    return salida
