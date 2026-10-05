"""Tests de `bulk.rest`: el relleno por REST que hace falta para `fundingRate`.

No se pega a la red: `_get` se sustituye por un doble que devuelve paginas sinteticas, porque un
test que dependa de fapi.binance.com falla de forma aleatoria y no prueba nada del codigo. Lo que
si se comprueba contra la API real es que el endpoint y sus campos existen, y eso se verifico a
mano al escribir el modulo (`docs/source-survey.md`).
"""

from __future__ import annotations

from datetime import datetime, timezone

import pyarrow as pa
import pytest

from bulk import rest


class FakeRest:
    """Doble de `rest._get`: un servidor que devuelve como mucho `cap` filas por llamada.

    `cap` simula un servidor que corta antes de lo pedido (`limit=1000`). Es lo que obliga al
    cliente a encadenar paginas en vez de asumir que una llamada lo trae todo.
    """

    def __init__(self, cap: int = 1000) -> None:
        self.cap = cap
        self.calls: list[dict] = []

    def __call__(self, path, params, *, retries=4):
        assert path == rest.FUNDING_PATH
        self.calls.append(dict(params))
        start = int(params["startTime"])
        end = int(params["endTime"])
        rows = []
        ts = start
        while ts <= end:
            rows.append(
                {
                    "symbol": params["symbol"],
                    "fundingTime": ts,
                    "fundingRate": "0.00010000",
                    "markPrice": "71000.0",
                }
            )
            ts += 8 * 3600 * 1000
        return rows[: self.cap]


@pytest.fixture
def fake(monkeypatch):
    def install(cap: int = 1000) -> FakeRest:
        double = FakeRest(cap)
        monkeypatch.setattr(rest, "_get", double)
        return double

    return install


START = datetime(2026, 10, 1, tzinfo=timezone.utc)
# `end` es INCLUSIVO, asi que para un dia completo son 3 pagos (00:00, 08:00, 16:00).
END = datetime(2026, 10, 1, 23, 59, tzinfo=timezone.utc)


def test_fetch_funding_returns_sorted_unique_records(fake):
    fake(cap=1000)
    records = rest.fetch_funding("BTCUSDT", START, END)
    assert len(records) == 3, "un dia tiene 3 pagos de funding cada 8h"
    stamps = [r["fundingTime"] for r in records]
    assert stamps == sorted(stamps), "la respuesta debe venir ordenada"
    assert len(set(stamps)) == len(stamps), "no debe repetir timestamps al paginar"


def test_fetch_funding_paginates_instead_of_truncating(fake):
    """Con paginas de 5 hay que encadenar varias llamadas; si no, se perderia el resto."""
    double = fake(cap=5)
    records = rest.fetch_funding("BTCUSDT", START, datetime(2026, 10, 5, tzinfo=timezone.utc))
    assert len(double.calls) > 1, "no se ha paginado"
    # Del 01 al 05 inclusive: 3 pagos por dia x 4 dias + el del 05 a las 00:00 = 13.
    assert len(records) == 13
    assert len(set(r["fundingTime"] for r in records)) == 13, "la paginacion ha duplicado"


def test_fetch_funding_makes_no_redundant_calls(fake):
    """La paginacion avanza por el ultimo fundingTime visto: sin solapes ni bucles."""
    double = fake(cap=1000)
    rest.fetch_funding("BTCUSDT", START, END)
    starts = [c["startTime"] for c in double.calls]
    assert starts == sorted(starts)
    assert len(set(starts)) == len(starts)


def test_funding_to_table_matches_the_lake_schema(fake):
    """El REST no manda `funding_interval_hours`: hay que rellenarlo, no dejarlo a null."""
    fake(cap=1000)
    records = rest.fetch_funding("BTCUSDT", START, END)
    table = rest.funding_to_table(records, "BTCUSDT", interval_hours=8)

    assert table.column_names == [
        "calc_time",
        "funding_interval_hours",
        "last_funding_rate",
        "symbol",
        "exchange",
    ]
    assert table.schema.field("calc_time").type == pa.timestamp("ms", tz="UTC")
    assert table.column("funding_interval_hours").to_pylist() == [8, 8, 8]
    assert table.column("last_funding_rate").to_pylist() == [0.0001, 0.0001, 0.0001]
    assert table.column("symbol").to_pylist() == ["BTCUSDT"] * 3


def test_funding_to_table_on_empty_input_does_not_explode(fake):
    fake(cap=1000)
    # start > end: el bucle de paginacion no entra y se devuelve una lista vacia.
    records = rest.fetch_funding("BTCUSDT", datetime(2030, 1, 2, tzinfo=timezone.utc),
                                 datetime(2030, 1, 1, tzinfo=timezone.utc))
    table = rest.funding_to_table(records, "BTCUSDT")
    assert table.num_rows == 0


def test_rest_error_is_raised_not_swallowed(monkeypatch):
    def boom(path, params, *, retries=4):
        raise rest.RestError("500 en /fapi/v1/fundingRate")

    monkeypatch.setattr(rest, "_get", boom)
    with pytest.raises(rest.RestError):
        rest.fetch_funding("BTCUSDT", START, END)


def test_open_interest_to_table_leaves_ratios_as_null(fake):
    """El endpoint de open interest NO trae ratios: se dejan a null, no se inventan."""
    records = [
        {"timestamp": 1756684800000, "sumOpenInterest": "123.5", "sumOpenInterestValue": "456.7"}
    ]
    table = rest.open_interest_to_table(records, "BTCUSDT")
    assert table.column("sum_open_interest").to_pylist() == [123.5]
    for column in (
        "count_toptrader_long_short_ratio",
        "sum_toptrader_long_short_ratio",
        "count_long_short_ratio",
        "sum_taker_long_short_vol_ratio",
    ):
        assert table.column(column).to_pylist() == [None], column
