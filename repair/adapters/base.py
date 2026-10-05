"""Contrato de los adaptadores de reparacion y utilidades comunes.

Un adaptador sabe **una** cosa: traer trades (o velas) de un exchange para un rango. No sabe nada
de la base de datos ni del estado del hueco; el worker (`repair/worker.py`) orchestra.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from feed.gaps import Gap


@dataclass
class TradeRow:
    """Un trade ya normalizado a ms enteros y simbolo canonico (regla 16)."""

    trade_id: str
    ts_ms: int
    side: str
    price: float
    amount: float
    symbol: str


@dataclass
class CandleRow:
    """Vela 1m normalizada. `open_time_ms` es el inicio de la vela."""

    open_time_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    trades: int
    symbol: str


@dataclass
class RepairResult:
    """Lo que un adaptador trae para un hueco.

    `covered_through_ms` es lo importante: dice hasta donde llega **de verdad** la fuente. Si no
    llega al `gap_to` del hueco, el worker lo deja en `partial`, no en `repaired`. Confundir esas
    dos cosas es como se declara "reparado" un hueco que se ha dejado a medias.
    """

    rows: list = field(default_factory=list)
    source: str = "rest"
    covered_from_ms: int | None = None
    covered_through_ms: int | None = None
    #: Razon por la que esta fuente no puede cubrir el hueco entero, si aplica.
    limitation: str | None = None
    note: str | None = None

    @property
    def covered(self) -> bool:
        """Cubre el hueco entero. Por defecto, no: un adaptador tiene que decirlo."""
        return self.covered_from_ms is not None and self.covered_through_ms is not None


class Adapter:
    """Interfaz. Cada exchange implementa lo suyo."""

    exchange: str = ""
    name: str = ""

    def can_repair(self, gap: Gap) -> tuple[bool, str | None]:
        """(puede, motivo si no puede). El motivo va al `note` del hueco."""
        raise NotImplementedError

    def fetch_trades(self, gap: Gap):
        raise NotImplementedError

    def fetch_candles(self, gap: Gap):
        raise NotImplementedError


def side_normalized(side: str | None) -> str:
    """`buy`/`sell` en minuscula, que es lo que exige el CHECK de la tabla.

    Los exchanges usan las tres convenciones: `true`/`false` (Binance `m` invertido), `Buy`/`Sell`
    (Bybit, Bitget) y `A`/`B` (Hyperliquid, donde `A` es *ask*, o sea venta).
    """
    s = (side or "").strip().lower()
    if s in ("buy", "b", "bought"):
        return "buy"
    if s in ("sell", "s", "sold", "a", "ask"):
        return "sell"
    return "buy"


def side_from_is_buyer_maker(is_buyer_maker: bool) -> str:
    """Binance manda `m` = "is buyer the maker". Si el comprador es el *maker*, el que vendio fue
    el que cruzo a la contra: es una VENTA."""
    return "sell" if is_buyer_maker else "buy"