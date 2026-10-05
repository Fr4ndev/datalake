"""Config del feed-daemon: que exchanges, que canales y que simbolos.

Arranca **solo con BTC y ETH** en los 5 exchanges, porque en un mini PC lo que se paga es CPU/RAM
y no se ha medido todavia cuantos simbolos caben (ver docs/acceptance-fase2.md). Ampliar es cambiar
`FEED_SYMBOLS` y reiniciar: no hay que tocar codigo.

La lista de canales NO es un capricho, es lo que cada exchange soporta de verdad en cryptofeed
3.0.1 (medido en runtime, no asumido; ver `capabilities()` y docs/decisions.md D28):

    exchange        trades funding open_interest liquidations candles
    BinanceFutures   si      si        si           si           si
    Bybit            si      si        si           si           si
    OKX              si      si        si           si           si
    Bitget           si      si        si           NO           si
    Hyperliquid      si      NO        NO           NO           NO

Los canales no soportados se **documentan como limitacion**, no se parchea cryptofeed. Pedirle un
canal que no expone aborta el arranque: es mejor que el daemon no levante antes de
# quedarse colgado esperando datos que nunca llegan.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field

#: Canales que el proyecto usa. `book`/`l2_book` fuera a proposito: en un mini PC el I/O y la RAM
#: no dan, y ademas nadie los va a leer (las estrategias usan velas, OI, funding y liquidaciones).
CHANNELS = ("trades", "funding", "open_interest", "liquidations", "candles")

#: exchange -> canales que NO soporta, medido en cryptofeed 3.0.1.
UNSUPPORTED: dict[str, frozenset[str]] = {
    "binance_futures": frozenset(),
    "bybit": frozenset(),
    "okx": frozenset(),
    "bitget": frozenset({"liquidations"}),
    "hyperliquid": frozenset({"funding", "open_interest", "liquidations", "candles"}),
}

DEFAULT_EXCHANGES = ("binance_futures", "bybit", "okx", "bitget", "hyperliquid")
DEFAULT_SYMBOLS = ("BTC", "ETH")


@dataclass(frozen=True)
class FeedConfig:
    exchanges: tuple[str, ...] = DEFAULT_EXCHANGES
    symbols: tuple[str, ...] = DEFAULT_SYMBOLS
    channels: tuple[str, ...] = CHANNELS
    #: Tamano de buffer por (dtype, symbol). 1000 es lo que dice la skill; con BTC/ETH y 5
    #: exchanges el flush por tiempo (1 s) es el que manda en la practica, no el de tamano.
    buffer_size: int = 1000
    #: Cadencia del flush por tiempo.
    flush_interval: float = 1.0
    #: Segundos para el flush final al recibir SIGTERM. Si no cabe el buffer en ese tiempo se corta
    #: y se avisa por log: es preferible perder filas periodos a no perder el daemon entero.
    shutdown_grace: float = 20.0
    #: Log interno de cryptofeed. INFO solo para diagnostico de conexion; en produccion WARNING.
    log_level: str = "WARNING"

    def channels_for(self, exchange: str) -> tuple[str, ...]:
        """Canales pedidos para `exchange` menos los que no soporta."""
        return tuple(c for c in self.channels if c not in UNSUPPORTED.get(exchange, frozenset()))

    def symbols_for(self, exchange: str) -> tuple[str, ...]:
        """Simbolo **normalizado de cryptofeed** que hay que pasarle a `add_feed`.

        No es inventado: son los pares normalizado->nativo que devuelve
        `Feed.symbol_mapping()` en 3.0.1, medidos en runtime. A `add_feed` se le da el
        normalizado y cryptofeed translatea al nativo el solo
        (`exchange.py::std_symbol_to_exchange_symbol`), asi que el nativo no se toca nunca:

            exchange        normalizado        -> nativo que usa por dentro
            BinanceFutures  BTC-USDT-PERP      -> BTCUSDT
            Bybit           BTC-USDT-PERP      -> BTCUSDT
            OKX             BTC-USDT-PERP      -> BTC-USDT-SWAP
            Bitget          BTC-USDT-PERP      -> BTCUSDT_USDT-FUTURES
            Hyperliquid     BTC-USD-PERP       -> BTC

        Hyperliquid es el unico que se sale: su normalizado es `BTC-USD-PERP` y no
        `BTC-USDT-PERP`, asi que el generico revienta con
        `UnsupportedSymbol: BTC-USDT-PERP is not supported on HYPERLIQUID` (visto en runtime).
        """
        quote = "USD" if exchange == "hyperliquid" else "USDT"
        return tuple(f"{s}-{quote}-PERP" for s in self.symbols)


def _split_env(name: str, default: tuple[str, ...]) -> tuple[str, ...]:
    raw = os.environ.get(name)
    if not raw or not raw.strip():
        return default
    return tuple(p.strip() for p in raw.split(",") if p.strip())


def load() -> FeedConfig:
    """Config desde entorno, con los valores por defecto del proyecto."""
    exchanges = _split_env("FEED_EXCHANGES", DEFAULT_EXCHANGES)
    symbols = _split_env("FEED_SYMBOLS", DEFAULT_SYMBOLS)
    channels = _split_env("FEED_CHANNELS", CHANNELS)
    unknown = [e for e in exchanges if e not in UNSUPPORTED]
    if unknown:
        raise ValueError(
            f"exchanges desconocidos: {unknown}; conocidos: {sorted(UNSUPPORTED)}"
        )
    return FeedConfig(
        exchanges=exchanges,
        symbols=symbols,
        channels=channels,
        buffer_size=int(os.environ.get("FEED_BUFFER_SIZE", "1000")),
        flush_interval=float(os.environ.get("FEED_FLUSH_INTERVAL", "1.0")),
        shutdown_grace=float(os.environ.get("FEED_SHUTDOWN_GRACE", "20")),
        log_level=os.environ.get("FEED_LOG_LEVEL", "WARNING"),
    )