"""OKX: `/api/v5/market/history-trades` con `type=2`.

Dos detalles que seORKX no se puede deducir leyendo el endpoint (los dos medidos):

1. **`type=2`, no `type=1`.** `type=2` son los trades sin block trades, y es **exactamente** lo
   que llega por el WS: 0 ids de WS ausentes del REST. `type=1` (todos los trades) devolvio 2 de
   131 ausentes dentro de la ventana cubierta, asi que usaria el endpoint equivocado.
2. **`after` es un timestamp y devuelve lo ANTERIOR a el**, y hay que restarle 1 al minimo de cada
   pagina. Sin ese `-1` se repite la misma pagina indefinidamente y el bucle no termina nunca.
   `before` no existe para paginacion por timestamp: OKX devuelve
   `50039 "The before parameter isn't available for timestamp pagination"`.
"""

from __future__ import annotations

from .base import Adapter, CandleRow, RepairResult, TradeRow, side_normalized

HISTORY_TRADES = "https://www.okx.com/api/v5/market/history-trades"
HISTORY_CANDLES = "https://www.okx.com/api/v5/market/history-candles"
LIMITE_PAGINA = 100
MAX_PAGINAS = 2000


class OKXAdapter(Adapter):
    exchange = "OKX"
    name = "okx"

    def __init__(self, client):
        self.http = client

    def can_repair(self, gap) -> tuple[bool, str | None]:
        return True, None

    def fetch_trades(self, gap) -> RepairResult:
        inst = f"{gap.symbol}-SWAP"
        filas: dict[str, TradeRow] = {}
        cursor = gap.gap_to_ms + 1  # `after` devuelve lo anterior: empezamos por el final
        cubierta_desde = gap.gap_to_ms
        for _ in range(MAX_PAGINAS):
            datos = self.http.get(self.exchange, HISTORY_TRADES, {
                "instId": inst, "type": 2, "after": int(cursor), "limit": LIMITE_PAGINA})
            if not datos.get("data"):
                break
            pagina = datos["data"]
            for x in pagina:
                fila = TradeRow(
                    trade_id=str(x["tradeId"]), ts_ms=int(x["ts"]),
                    side=side_normalized(x.get("side")), price=float(x["px"]),
                    amount=float(x["sz"]), symbol=gap.symbol)
                # El padding del hueco se respeta aqui: el endpoint devuelve tambien lo que ya
                # tenemos y no vamos a filtrar en memoria, se filtra al insertar.
                filas[fila.trade_id] = fila
            mas_antiguo = min(int(x["ts"]) for x in pagina)
            if mas_antiguo < gap.gap_from_ms:
                cubierta_desde = mas_antiguo
                break
            cursor = mas_antiguo - 1  # <- el -1 que evita el bucle infinito
        dentro = [f for f in filas.values() if gap.gap_from_ms <= f.ts_ms <= gap.gap_to_ms]
        return RepairResult(
            rows=dentro, source="rest",
            covered_from_ms=gap.gap_from_ms if dentro else None,
            covered_through_ms=gap.gap_to_ms if dentro else None,
            note=f"{len(dentro)} trades de OKX paginando hacia atras con type=2")

    def fetch_candles(self, gap) -> RepairResult:
        inst = f"{gap.symbol}-SWAP"
        filas: dict[int, CandleRow] = {}
        cursor = gap.gap_to_ms + 1
        for _ in range(MAX_PAGINAS):
            datos = self.http.get(self.exchange, HISTORY_CANDLES, {
                "instId": inst, "bar": "1m", "after": int(cursor), "limit": 100})
            if not datos.get("data"):
                break
            pagina = datos["data"]
            for x in pagina:
                fila = CandleRow(open_time_ms=int(x[0]), open=float(x[1]), high=float(x[2]),
                                 low=float(x[3]), close=float(x[4]), volume=float(x[5]),
                                 trades=0, symbol=gap.symbol)
                if gap.gap_from_ms <= fila.open_time_ms <= gap.gap_to_ms:
                    filas[fila.open_time_ms] = fila
            mas_antiguo = min(int(x[0]) for x in pagina)
            if mas_antiguo < gap.gap_from_ms:
                break
            cursor = mas_antiguo - 1
        return RepairResult(
            rows=list(filas.values()), source="rest",
            covered_from_ms=gap.gap_from_ms if filas else None,
            covered_through_ms=gap.gap_to_ms if filas else None,
            note=f"{len(filas)} velas 1m de OKX")
