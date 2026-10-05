"""Esquemas reales de los productos de Binance Vision (UM perpetuos).

Todo lo de aqui sale de abrir ficheros de verdad y mirar la cabecera y la primera fila
(`docs/source-survey.md`), no de memoria. Los detalles que abajo parecen raros estan porque el
origen los tiene asi:

- `klines` y `aggTrades` **cambian de formato con el tiempo**: hasta 2021-12 sus CSV no tienen
  linea de cabecera (la primera fila ya son datos) y **desde 2022-01 si la llevan**. Con
  `has_header=None` el parser lo decide por fichero, porque asumir cualquiera de las dos epocas
  rompe la mitad del historico.
- En la era con cabecera, klines renombro `trades` a `count`, pero el **orden de las 12 columnas
  no cambia**, por eso el esquema fijo sigue valiendo y solo hay que saltarse la linea.
- `aggTrades` **no tiene el timestamp en la columna 0**: la 0 es el `aggTradeId`. Asumir
  "columna 0 = ts" produciria una columna de enteros monotonos que parece un timestamp valido.
- `metrics` trae `create_time` como texto `2020-09-01 00:00:00`, no como epoch.
- `metrics` llega con cada fila **duplicada byte a byte** (288 timestamps/dia x 2). El dedup por
  (symbol, ts) es obligatorio.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pyarrow as pa


@dataclass(frozen=True)
class Schema:
    """Como convertir un CSV de Binance Vision en una tabla del lake."""

    name: str
    columns: tuple[str, ...]
    types: tuple[pa.DataType, ...]
    has_header: bool | None  # None = autodetectar por fichero
    ts_column: str
    ts_kind: str  # "epoch" | "iso"
    keep: tuple[str, ...]
    time_column: str  # columna de tiempo en el lake (normalizada a timestamptz UTC)
    tf_default: str
    granularity_default: str
    granularity_available: tuple[str, ...]
    # Margen que se tolera al medir el paso entre timestamps, en segundos.
    #
    # `fundingRate` lo necesita: sus `calc_time` no caen exactos en la cuadrícula de 8h, miden
    # entre 28799.991 y 28800.009 s de diferencia (jitter de +-10 ms del exchange). Sin margen,
    # un check estricto de "delta > 8h" daria un falso positivo por CADA intervalo de funding.
    # 60 s esta muy por debajo de un hueco real (que serian multiples de 8h) y muy por encima
    # del jitter medido.
    gap_tolerance_seconds: int = 0
    # Intervalo en horas que el exchange declara para el producto. Lo usan el relleno por REST
    # (el endpoint no devuelve el intervalo) y los checks. 0 = no aplica.
    funding_interval_hours: int = 0

    @property
    def arrow_schema(self) -> pa.Schema:
        return pa.schema(list(zip(self.columns, self.types)))

    @property
    def read_schema(self) -> pa.Schema:
        """Esquema en el orden de las columnas del CSV, para `pyarrow.csv`."""
        return pa.schema(list(zip(self.columns, self.types)))

    @property
    def keep_indices(self) -> tuple[int, ...]:
        return tuple(self.columns.index(c) for c in self.keep)


_TS_MS = pa.timestamp("ms", tz="UTC")

KLINES = Schema(
    name="klines",
    columns=(
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_volume",
        "trades",
        "taker_buy_volume",
        "taker_buy_quote_volume",
        "ignore",
    ),
    types=(
        _TS_MS,
        pa.float64(),
        pa.float64(),
        pa.float64(),
        pa.float64(),
        pa.float64(),
        _TS_MS,
        pa.float64(),
        pa.int64(),
        pa.float64(),
        pa.float64(),
        pa.int64(),
    ),
    has_header=None,  # sin cabecera hasta 2021-12, con cabecera desde 2022-01
    ts_column="open_time",
    ts_kind="epoch",
    keep=(
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_volume",
        "trades",
        "taker_buy_volume",
        "taker_buy_quote_volume",
    ),
    time_column="open_time",
    tf_default="1m",
    granularity_default="monthly",
    # Verificado: klines tiene mensual y diario para BTCUSDT.
    granularity_available=("monthly", "daily"),
)

FUNDING_RATE = Schema(
    name="fundingRate",
    columns=("calc_time", "funding_interval_hours", "last_funding_rate"),
    types=(_TS_MS, pa.int64(), pa.float64()),
    has_header=True,  # si: calc_time,funding_interval_hours,last_funding_rate
    ts_column="calc_time",
    ts_kind="epoch",
    keep=("calc_time", "funding_interval_hours", "last_funding_rate"),
    time_column="calc_time",
    tf_default="8h",
    granularity_default="monthly",
    # Verificado: `daily/fundingRate/BTCUSDT/` esta VACIO. Solo mensual.
    granularity_available=("monthly",),
    gap_tolerance_seconds=60,
    funding_interval_hours=8,
)

METRICS = Schema(
    name="metrics",
    columns=(
        "create_time",
        "symbol",
        "sum_open_interest",
        "sum_open_interest_value",
        "count_toptrader_long_short_ratio",
        "sum_toptrader_long_short_ratio",
        "count_long_short_ratio",
        "sum_taker_long_short_vol_ratio",
    ),
    types=(
        _TS_MS,
        pa.string(),
        pa.float64(),
        pa.float64(),
        pa.float64(),
        pa.float64(),
        pa.float64(),
        pa.float64(),
    ),
    has_header=True,
    ts_column="create_time",
    ts_kind="iso",  # llega como "2020-09-01 00:00:00", sin timezone
    keep=(
        "create_time",
        "symbol",
        "sum_open_interest",
        "sum_open_interest_value",
        "count_toptrader_long_short_ratio",
        "sum_toptrader_long_short_ratio",
        "count_long_short_ratio",
        "sum_taker_long_short_vol_ratio",
    ),
    time_column="create_time",
    tf_default="5m",
    granularity_default="daily",
    # Verificado: `monthly/metrics/` esta VACIO. Solo diario.
    granularity_available=("daily",),
    funding_interval_hours=0,  # metrics es 5m, no funding
)

AGG_TRADES = Schema(
    name="aggTrades",
    columns=(
        "agg_trade_id",
        "price",
        "quantity",
        "first_trade_id",
        "last_trade_id",
        "timestamp",
        "is_buyer_maker",
    ),
    types=(
        pa.int64(),
        pa.float64(),
        pa.float64(),
        pa.int64(),
        pa.int64(),
        _TS_MS,
        pa.bool_(),
    ),
    has_header=None,
    ts_column="timestamp",  # columna 5, NO la 0
    ts_kind="epoch",
    keep=(
        "agg_trade_id",
        "price",
        "quantity",
        "first_trade_id",
        "last_trade_id",
        "timestamp",
        "is_buyer_maker",
    ),
    time_column="timestamp",
    tf_default="tick",
    granularity_default="monthly",
    granularity_available=("monthly", "daily"),
)

SCHEMAS: dict[str, Schema] = {
    s.name: s for s in (KLINES, FUNDING_RATE, METRICS, AGG_TRADES)
}

# Nombre del dtype -> nombre de carpeta en el lake. Binance Vision llama "fundingRate" al
# producto; en el lake se usa minuscula para que las rutas sean consistentes.
LAKE_DIRNAME: dict[str, str] = {
    "klines": "klines",
    "fundingRate": "funding",
    "metrics": "metrics",
    "aggTrades": "agg_trades",
}

# Sufijo del fichero .zip por producto dentro del path del bucket.
VISION_KIND: dict[str, str] = {
    "klines": "klines",
    "fundingRate": "fundingRate",
    "metrics": "metrics",
    "aggTrades": "aggTrades",
}


@dataclass(frozen=True)
class Product:
    """Un producto concreto a descargar: dtype + timeframe + simbolo."""

    dtype: str
    symbol: str
    tf: str

    @property
    def schema(self) -> Schema:
        return SCHEMAS[self.dtype]

    @property
    def lake_dirname(self) -> str:
        return LAKE_DIRNAME[self.dtype]


def prefix_for(product: Product, granularity: str, vision_root: str) -> str:
    """Prefijo del bucket para un producto/granularidad/simbolo.

    `klines` lleva el timeframe en el path (`klines/BTCUSDT/1m/`); el resto no
    (`fundingRate/BTCUSDT/`, `metrics/BTCUSDT/`).
    """
    kind = VISION_KIND[product.dtype]
    parts = [vision_root, granularity, kind, product.symbol]
    if product.dtype == "klines":
        parts.append(product.tf)
    return "/".join(parts) + "/"


def filename_for(product: Product, granularity: str, date: str) -> str:
    """Nombre del .zip. Reproduce el formato real del bucket."""
    kind = VISION_KIND[product.dtype]
    if product.dtype == "klines":
        return f"{product.symbol}-{product.tf}-{date}.zip"
    if product.dtype == "fundingRate":
        return f"{product.symbol}-{kind}-{date}.zip"
    return f"{product.symbol}-{kind}-{date}.zip"
