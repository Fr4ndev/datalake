"""Hyperliquid: trades IRRECUPERABLES, velas 1m reparables.

`POST /info {"type":"recentTrades","coin":"BTC"}` devuelve **exactamente 10 trades** y no hay
historia: medido, los ids que devuelve estan todos en la base, pero solo 10 de los 152 que tenia el
WS en la misma ventana. No hay endpoint de historico. Es decir: los trades perdidos en
Hyperliquid **no se pueden recuperar**, y lo unico honesto es decirlo.

Sus velas 1m si: `candleSnapshot` devuelve las ultimas ~5000 (medido: 5116 para un pedido de 10
dias, o sea **3,55 dias** de historico). Con eso las velas siempre se autorreparan, que es lo que
alimenta los backtests.
"""

from __future__ import annotations

from .base import Adapter, CandleRow, RepairResult, TradeRow, side_normalized

INFO = "https://api.hyperliquid.xyz/info"
#: Cota medida: un pedido de 14.400 velas devuelve 5116 -> ~3,5 dias. Se piden por trozos.
MAX_CANDLE_SNAPSHOT = 5000


class HyperliquidAdapter(Adapter):
    exchange = "hyperliquid"
    name = "hyperliquid"

    def __init__(self, client):
        self.http = client

    def can_repair(self, gap) -> tuple[bool, str | None]:
        if gap.dtype == "trades":
            return False, ("la API publica de Hyperliquid solo ofrece `recentTrades` con un maximo "
                           "de 10 operaciones y sin paginacion ni historico: los trades perdidos "
                           "en este exchange son irrecuperables. Sus velas 1m si se reparan.")
        return True, None

    def fetch_trades(self, gap) -> RepairResult:
        # No se llega aqui: `can_repair` devuelve False para trades. Se deja el metodo para que el
        # contrato sea explicito y no se rompa si alguien lo llama igual.
        return RepairResult(source="rest", limitation="irrecuperable")

    def fetch_candles(self, gap) -> RepairResult:
        filas: dict[int, CandleRow] = {}
        cursor = gap.gap_from_ms
        fin = gap.gap_to_ms + 60_000
        while cursor <= fin:
            datos = self.http.post(self.exchange, INFO, {
                "type": "candleSnapshot",
                "req": {"coin": gap.symbol, "interval": "1m",
                        "startTime": int(cursor), "endTime": int(fin)}})
            if not isinstance(datos, list) or not datos:
                break
            for x in datos:
                fila = CandleRow(
                    open_time_ms=int(x["t"]), open=float(x["o"]), high=float(x["h"]),
                    low=float(x["l"]), close=float(x["c"]), volume=float(x["v"]),
                    trades=int(x.get("n", 0)), symbol=gap.symbol)
                filas[fila.open_time_ms] = fila
            mas_antiguo = min(int(x["t"]) for x in datos)
            if mas_antiguo <= cursor:
                break  # no avanza: es el limite del endpoint
            cursor = mas_antiguo + 60_000
        dentro = [f for f in filas.values() if gap.gap_from_ms <= f.open_time_ms <= gap.gap_to_ms]
        return RepairResult(
            rows=dentro, source="rest",
            covered_from_ms=gap.gap_from_ms if dentro else None,
            covered_through_ms=gap.gap_to_ms if dentro else None,
            limitation=None if dentro else
            "candleSnapshot solo cubre ~3,5 dias de historico (5116 velas medidos)",
            note=f"{len(dentro)} velas 1m por candleSnapshot")
