"""Bitget: `/api/v2/mix/market/fills-history`.

Profundidad medida: `startTime` a 88 dias devuelve datos de 81 dias atras; a 90+ dias el
endpoint devuelve `code=None` con la lista vacia. El limite efectivo es ~90 dias.

`tradeId` es el mismo campo que lee cryptofeed del WS (`entry['tradeId']`), asi que el dedup por
PK funciona. Medido: 90/90 ids de WS presentes en el REST.
"""

from __future__ import annotations

from .base import Adapter, CandleRow, RepairResult, TradeRow, side_normalized

FILLS = "https://api.bitget.com/api/v2/mix/market/fills-history"
CANDLES = "https://api.bitget.com/api/v2/mix/market/candles"
LIMITE_PAGINA = 1000
MAX_PAGINAS = 500


class BitgetAdapter(Adapter):
    exchange = "BITGET"
    name = "bitget"

    def __init__(self, client):
        self.http = client

    def can_repair(self, gap) -> tuple[bool, str | None]:
        return True, None

    def fetch_trades(self, gap) -> RepairResult:
        filas: dict[str, TradeRow] = {}
        cursor = gap.gap_from_ms
        for _ in range(MAX_PAGINAS):
            datos = self.http.get(self.exchange, FILLS, {
                "symbol": gap.symbol, "productType": "USDT-FUTURES",
                "startTime": int(cursor), "endTime": int(gap.gap_to_ms),
                "limit": LIMITE_PAGINA})
            pagina = datos.get("data") or []
            if not pagina:
                break
            for x in pagina:
                fila = TradeRow(
                    trade_id=str(x["tradeId"]), ts_ms=int(x["ts"]),
                    side=side_normalized(x.get("side")), price=float(x["price"]),
                    amount=float(x["size"]), symbol=gap.symbol)
                if gap.gap_from_ms <= fila.ts_ms <= gap.gap_to_ms:
                    filas[fila.trade_id] = fila
            cursor = int(pagina[-1]["ts"]) + 1
            if cursor > gap.gap_to_ms:
                break
        return RepairResult(
            rows=list(filas.values()), source="rest",
            covered_from_ms=gap.gap_from_ms if filas else None,
            covered_through_ms=gap.gap_to_ms if filas else None,
            note=f"{len(filas)} fills de Bitget")

    def fetch_candles(self, gap) -> RepairResult:
        # Bitget pagina con `endTime` hacia atras.
        filas: dict[int, CandleRow] = {}
        cursor = gap.gap_to_ms
        for _ in range(MAX_PAGINAS):
            datos = self.http.get(self.exchange, CANDLES, {
                "symbol": gap.symbol, "productType": "USDT-FUTURES", "granularity": "1m",
                "endTime": int(cursor), "limit": 1000})
            pagina = datos.get("data") or []
            if not pagina:
                break
            for x in pagina:
                fila = CandleRow(open_time_ms=int(x[0]), open=float(x[1]), high=float(x[2]),
                                 low=float(x[3]), close=float(x[4]), volume=float(x[5]),
                                 trades=0, symbol=gap.symbol)
                if gap.gap_from_ms <= fila.open_time_ms <= gap.gap_to_ms:
                    filas[fila.open_time_ms] = fila
            cursor = int(pagina[-1][0]) - 1
            if cursor < gap.gap_from_ms:
                break
        return RepairResult(
            rows=list(filas.values()), source="rest",
            covered_from_ms=gap.gap_from_ms if filas else None,
            covered_through_ms=gap.gap_to_ms if filas else None,
            note=f"{len(filas)} velas 1m de Bitget")
