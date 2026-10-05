"""Tests de `bulk.tsutil`: autodeteccion de unidad y normalizacion a UTC (regla 3).

Las filas de referencia son REALES, sacadas de ficheros de Binance Vision (ver
`docs/source-survey.md`), no inventadas.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from bulk import tsutil

# Fila real de BTCUSDT-1m-2020-01.zip
KLINES_ROW_1 = "1577836860000,7182.43,7182.44,7178.75,7179.01,70.909,1577836919999,509145.78482,140,32.597,234063.27884,0"
# Fila real de BTCUSDT-metrics-2020-09-01.zip
METRICS_ROW_1 = "2020-09-01 00:00:00,BTCUSDT,39080.23100000,456144339.23360443,1.17547937,1.23012681,1.35731217,0.78373373"
# Fila real de BTCUSDT-fundingRate-2026-09.zip
FUNDING_ROW_1 = "1788220800005,8,0.00008482"


def test_detect_unit_epoch_ms():
    # 13 digitos: las tres Magnitudes que trae el bucket son 10, 13 y 16.
    assert tsutil.detect_unit(KLINES_ROW_1.split(",")[0]) == "ms"
    assert tsutil.detect_unit(FUNDING_ROW_1.split(",")[0]) == "ms"
    assert tsutil.detect_unit("1577836801481") == "ms"


def test_detect_unit_epoch_us_and_s():
    assert tsutil.detect_unit("1577836801481123") == "us"
    assert tsutil.detect_unit("1577836801") == "s"


def test_detect_unit_iso_string():
    # metrics NO trae epoch: trae texto. La regla 3 original no contemplaba este caso.
    assert tsutil.detect_unit(METRICS_ROW_1.split(",")[0]) == "iso"
    assert tsutil.detect_unit("2020-09-01T00:05:00") == "iso"


def test_detect_unit_rejects_unknown_length():
    with pytest.raises(tsutil.TimestampError):
        tsutil.detect_unit("123456789")  # 9 digitos: ni s, ni ms, ni us


def test_to_utc_klines_real_row():
    got = tsutil.to_utc(KLINES_ROW_1.split(",")[0])
    assert got == datetime(2020, 1, 1, 0, 1, tzinfo=timezone.utc)


def test_to_utc_metrics_iso_is_utc_not_naive():
    got = tsutil.to_utc(METRICS_ROW_1.split(",")[0])
    assert got.tzinfo is not None
    assert got == datetime(2020, 9, 1, 0, 0, 0, tzinfo=timezone.utc)


def test_to_utc_iso_with_t_separator():
    assert tsutil.to_utc("2020-09-01T00:05:00") == datetime(2020, 9, 1, 0, 5, tzinfo=timezone.utc)


def test_to_utc_us_and_ms_agree():
    # El mismo instante expresado en ms y en us debe dar el mismo resultado.
    ms = tsutil.to_utc("1577836860000")
    us = tsutil.to_utc("1577836860000000")
    assert ms == us


def test_to_utc_never_returns_naive():
    for raw in (KLINES_ROW_1, METRICS_ROW_1, "1577836801481", "2020-09-01 00:00:00"):
        assert tsutil.to_utc(raw.split(",")[0]).tzinfo is timezone.utc


def test_to_utc_naive_datetime_is_treated_as_utc():
    # Un datetime sin tzinfo se interpreta como UTC, nunca como hora local.
    got = tsutil.to_utc(datetime(2020, 1, 1, 0, 0))
    assert got == datetime(2020, 1, 1, 0, 0, tzinfo=timezone.utc)


def test_to_epoch_ms_roundtrip():
    assert tsutil.to_epoch_ms("1577836860000") == 1577836860000
    assert tsutil.to_epoch_ms("2020-09-01 00:00:00") == 1598918400000


def test_int_input_also_goes_through_detection():
    # Un int no se asume en segundos: pasa por el detector de magnitud.
    assert tsutil.to_utc(1577836860000) == datetime(2020, 1, 1, 0, 1, tzinfo=timezone.utc)
    # 10 digitos = segundos, no ms: por eso el mismo instante en s lleva un segundo de mas.
    assert tsutil.to_utc(1577836860) == datetime(2020, 1, 1, 0, 1, tzinfo=timezone.utc)
    assert tsutil.to_utc(1577836861) == datetime(2020, 1, 1, 0, 1, 1, tzinfo=timezone.utc)
