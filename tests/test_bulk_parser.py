"""Tests de `bulk.parser` y `bulk.schemas` contra los formatos REALES del bucket.

Las filas de referencia son copias literales de ficheros de Binance Vision (ver
`docs/source-survey.md`). Los casos que se testean son los que rompen un parser ingenuo:

- `klines` y `aggTrades` **no tienen cabecera**: la primera fila ya son datos.
- `aggTrades` tiene el timestamp en la **columna 5**, porque la 0 es el `aggTradeId`.
- `metrics` trae el tiempo como **texto ISO** y llega **duplicado** byte a byte.
"""

from __future__ import annotations

import io
import zipfile
from datetime import datetime, timezone

import pyarrow as pa
import pytest

from bulk import parser
from bulk.schemas import KLINES, METRICS, SCHEMAS, filename_for, prefix_for, Product

# Filas reales de BTCUSDT-1m-2020-01.zip (SIN cabecera).
KLINES_ROWS = [
    "1577836860000,7182.43,7182.44,7178.75,7179.01,70.909,1577836919999,509145.78482,140,32.597,234063.27884,0",
    "1577836920000,7179.01,7179.01,7175.25,7177.93,99.420,1577836979999,713539.55348,148,16.311,117066.92118,0",
]
KLINES_HEADER_NOTE = (
    "1577836800000,7189.43,7190.52,7177,7182.44,246.092,1577836859999,1767430.16121,336,46.630,334813.19820,0"
)

# Fila real de BTCUSDT-metrics-2020-09-01.zip (CON cabecera, tiempo ISO, duplicada).
METRICS_HEADER = (
    "create_time,symbol,sum_open_interest,sum_open_interest_value,"
    "count_toptrader_long_short_ratio,sum_toptrader_long_short_ratio,"
    "count_long_short_ratio,sum_taker_long_short_vol_ratio"
)
METRICS_ROW = (
    "2020-09-01 00:00:00,BTCUSDT,39080.23100000,456144339.23360443,"
    "1.17547937,1.23012681,1.35731217,0.78373373"
)

# Fila real de BTCUSDT-fundingRate-2026-09.zip (CON cabecera).
FUNDING_HEADER = "calc_time,funding_interval_hours,last_funding_rate"
FUNDING_ROWS = ["1788220800005,8,0.00008482", "1790784000002,8,0.00006606"]

# Fila real de BTCUSDT-aggTrades-2020-01.zip (SIN cabecera, ts en la columna 5).
AGG_ROWS = [
    "18374167,7189.43,0.030,25247504,25247504,1577836801481,true",
    "18374168,7189.42,1.470,25247505,25247505,1577836801481,true",
    "18374169,7189.42,4.222,25247506,25247506,1577836801708,false",
]


def make_zip(rows: list[str], *, header: str | None = None, name: str = "data.csv") -> bytes:
    lines = ([header] if header else []) + rows
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(name, "\n".join(lines) + "\n")
    return buf.getvalue()


# Cabecera REAL de BTCUSDT-1m-2022-01.zip: desde 2022-01 Binance anadio cabecera a los CSV de
# klines y renombro la columna `trades` a `count`. El orden de las 12 columnas no cambia.
KLINES_HEADER_2022 = (
    "open_time,open,high,low,close,volume,close_time,quote_volume,count,"
    "taker_buy_volume,taker_buy_quote_volume,ignore"
)

# --------------------------------------------------------------------- klines


def test_klines_has_no_header_and_first_row_is_data():
    """La primera fila de klines es un epoch: un detector de cabecera la perderia."""
    table = parser.parse_payload(make_zip(KLINES_ROWS), KLINES)
    assert table.num_rows == 2, "la primera fila se ha tratado como cabecera"
    assert table.column("open_time")[0].as_py() == datetime(2020, 1, 1, 0, 1, tzinfo=timezone.utc)


def test_klines_with_header_is_detected_and_skipped():
    """Epoca con cabecera (2022-01 en adelante): la linea no debe convertirse en datos."""
    table = parser.parse_payload(make_zip(KLINES_ROWS, header=KLINES_HEADER_2022), KLINES)
    assert table.num_rows == 2
    assert table.column("open_time")[0].as_py() == datetime(2020, 1, 1, 0, 1, tzinfo=timezone.utc)
    assert table.column("trades")[0].as_py() == 140


def test_looks_like_header_detection():
    assert parser.looks_like_header(KLINES_HEADER_2022) is True
    assert parser.looks_like_header(KLINES_ROWS[0]) is False
    # Mirando la columna 0 de una fila de aggTrades se ve un entero (el aggTradeId): no es
    # cabecera. Mirando la 5, que es el ts, tambien es un entero. Por eso el detector usa SIEMPRE
    # el indice de la columna de tiempo del esquema, no "la primera".
    assert parser.looks_like_header(AGG_ROWS[0], time_column_index=5) is False
    assert parser.looks_like_header(AGG_ROWS[0], time_column_index=0) is False
    # Una cabecera de aggTrades trae texto en la columna de tiempo.
    assert parser.looks_like_header("agg_trade_id,price,quantity,f,l,t,timestamp,maker", 5) is True


def test_both_klines_eras_parse_to_the_same_shape():
    """Las dos epocas deben producir exactamente el mismo esquema y numero de filas."""
    sin = parser.parse_payload(make_zip(KLINES_ROWS), KLINES)
    con = parser.parse_payload(make_zip(KLINES_ROWS, header=KLINES_HEADER_2022), KLINES)
    assert sin.schema.equals(con.schema)
    assert sin.num_rows == con.num_rows
    assert sin.column("close").to_pylist() == con.column("close").to_pylist()


def test_klines_numeric_columns_are_not_strings():
    table = parser.parse_payload(make_zip(KLINES_ROWS), KLINES)
    assert pa.types.is_floating(table.column("close").type)
    assert pa.types.is_integer(table.column("trades").type)
    assert table.column("close")[0].as_py() == pytest.approx(7179.01)


def test_klines_drops_the_ignore_column():
    table = parser.parse_payload(make_zip(KLINES_ROWS), KLINES)
    assert "ignore" not in table.column_names
    assert table.column("close_time")[0].as_py().microsecond == 999000


def test_klines_timestamps_are_tz_aware_utc():
    table = parser.parse_payload(make_zip(KLINES_ROWS), KLINES)
    assert table.schema.field("open_time").type.tz == "UTC"


# -------------------------------------------------------------------- metrics


def test_metrics_header_is_skipped():
    table = parser.parse_payload(make_zip([METRICS_ROW, METRICS_ROW], header=METRICS_HEADER), METRICS)
    # 2 filas de datos; el header no cuenta como fila.
    assert table.num_rows == 2


def test_metrics_iso_string_is_parsed_as_utc():
    table = parser.parse_payload(make_zip([METRICS_ROW], header=METRICS_HEADER), METRICS)
    ts = table.column("create_time")[0].as_py()
    assert ts.year == 2020 and ts.month == 9 and ts.day == 1
    assert ts.tzinfo is not None


def test_metrics_rows_are_duplicated_at_source():
    """Confirma el caso real: el origen duplica cada fila. El dedup es del writer, no del parser."""
    table = parser.parse_payload(make_zip([METRICS_ROW, METRICS_ROW], header=METRICS_HEADER), METRICS)
    assert table.num_rows == 2
    assert table.column("create_time")[0].as_py() == table.column("create_time")[1].as_py()


# ---------------------------------------------------------------- aggTrades


def test_aggtrades_timestamp_is_column_five_not_zero():
    """Columna 0 es el aggTradeId (18374167); el ts es la 5 (1577836801481)."""
    table = parser.parse_payload(make_zip(AGG_ROWS), SCHEMAS["aggTrades"])
    ts = table.column("timestamp")[0].as_py()
    trade_id = table.column("agg_trade_id")[0].as_py()
    assert trade_id == 18374167
    assert ts.year == 2020 and ts.minute == 0 and ts.microsecond == 481000


def test_aggtrades_bool_column_is_real_bool():
    table = parser.parse_payload(make_zip(AGG_ROWS), SCHEMAS["aggTrades"])
    assert table.column("is_buyer_maker").type == pa.bool_()
    assert table.column("is_buyer_maker")[0].as_py() is True
    assert table.column("is_buyer_maker")[2].as_py() is False


# ------------------------------------------------------------------- funding


def test_funding_rate_header_and_interval_column():
    table = parser.parse_payload(make_zip(FUNDING_ROWS, header=FUNDING_HEADER), SCHEMAS["fundingRate"])
    assert table.num_rows == 2
    # El intervalo viene en el propio dato (8h), no hay que suponerlo.
    assert table.column("funding_interval_hours")[0].as_py() == 8


# ------------------------------------------------------------------ errores


def test_empty_csv_returns_empty_table():
    table = parser.parse_payload(make_zip([]), KLINES)
    assert table.num_rows == 0


def test_zip_with_two_members_is_rejected():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("a.csv", KLINES_ROWS[0])
        zf.writestr("b.csv", KLINES_ROWS[1])
    with pytest.raises(parser.ParseError):
        parser.parse_payload(buf.getvalue(), KLINES)


# ------------------------------------------------------- rutas del bucket


def test_prefix_puts_tf_only_for_klines():
    klines = Product("klines", "BTCUSDT", "1m")
    assert prefix_for(klines, "monthly", "data/futures/um") == "data/futures/um/monthly/klines/BTCUSDT/1m/"
    funding = Product("fundingRate", "BTCUSDT", "8h")
    assert prefix_for(funding, "monthly", "data/futures/um") == "data/futures/um/monthly/fundingRate/BTCUSDT/"
    metrics = Product("metrics", "BTCUSDT", "5m")
    assert prefix_for(metrics, "daily", "data/futures/um") == "data/futures/um/daily/metrics/BTCUSDT/"


def test_filename_matches_bucket_naming():
    assert filename_for(Product("klines", "BTCUSDT", "1m"), "monthly", "2020-01") == "BTCUSDT-1m-2020-01.zip"
    assert filename_for(Product("metrics", "BTCUSDT", "5m"), "daily", "2020-09-01") == "BTCUSDT-metrics-2020-09-01.zip"
    assert filename_for(Product("fundingRate", "BTCUSDT", "8h"), "monthly", "2020-01") == "BTCUSDT-fundingRate-2020-01.zip"


def test_granularity_availability_matches_the_survey():
    """Verificado contra el bucket: funding no tiene diario, metrics no tiene mensual."""
    assert SCHEMAS["fundingRate"].granularity_available == ("monthly",)
    assert SCHEMAS["metrics"].granularity_available == ("daily",)
    assert set(SCHEMAS["klines"].granularity_available) == {"monthly", "daily"}
