"""Tests de `bulk.gapscan`: los checks 1, 2, 3 y 6 de la skill data-quality-validator.

Se siembra un lake pequeno y se comprueba que:

- un hueco sin explicar sale **CRIT**;
- el mismo hueco, si esta en `lake/known_gaps.json`, sale **INFO** (delta pedido sobre la 3.5);
- la cola todavia no publicada por el exchange sale **INFO**, no CRIT;
- duplicados y outliers salen CRIT.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from bulk import config, gapscan, writer
from bulk.schemas import Product

TS = pa.timestamp("us", tz="UTC")


@pytest.fixture(autouse=True)
def lake(tmp_path, monkeypatch):
    monkeypatch.setenv("LAKE_DIR", str(tmp_path / "lake"))
    # La conexion DuckDB esta cacheada a nivel de modulo y guarda el estado entre tests; sin
    # resetearla un test podria leer las particiones de otro.
    gapscan.reset_connection()
    yield tmp_path / "lake"
    gapscan.reset_connection()


def product() -> Product:
    return Product(dtype="klines", symbol="BTCUSDT", tf="1m")


def minutes(count: int, *, start: datetime | None = None, holes: set[int] | None = None) -> pa.Table:
    """Velas 1m consecutivas desde `start`, quitando los indices de `holes`."""
    base = start or datetime(2023, 1, 1, tzinfo=timezone.utc)
    holes = holes or set()
    stamps = [base + timedelta(minutes=i) for i in range(count) if i not in holes]
    n = len(stamps)
    return pa.table(
        {
            "symbol": pa.array(["BTCUSDT"] * n, pa.string()),
            "exchange": pa.array(["binance"] * n, pa.string()),
            "open_time": pa.array(stamps, TS),
            "open": pa.array([1.0] * n, pa.float64()),
            "high": pa.array([2.0] * n, pa.float64()),
            "low": pa.array([0.5] * n, pa.float64()),
            "close": pa.array([1.5] * n, pa.float64()),
            "volume": pa.array([10.0] * n, pa.float64()),
        }
    )


def write_seed(table: pa.Table) -> None:
    """Escribe el seed pasando por el writer (que deduplica)."""
    writer.write(table, product())


def write_raw(table: pa.Table, year: int) -> None:
    """Escribe el parquet SIN pasar por el writer, para simular una particion corrupta."""
    directory = config.partition_path("klines", "BTCUSDT", "1m", year)
    directory.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, directory / "part.parquet")


def crits(report: gapscan.Report) -> list[gapscan.Finding]:
    return [f for f in report.findings if f.severity == gapscan.SEV_CRIT]


def by_check(report: gapscan.Report, check: str) -> list[gapscan.Finding]:
    return [f for f in report.findings if f.check == check]


# ------------------------------------------------------------------ check 1


def test_contiguous_data_has_no_gaps():
    write_seed(minutes(120))
    report = gapscan.scan(product())
    assert crits(report) == []
    assert report.stats["gaps_unexplained"] == 0


def test_hole_is_crit_with_exact_range():
    """Criterio de done de la skill: hueco -> CRIT con el rango exacto."""
    # 47 minutos quitados en medio: el rango debe ser 47 velas.
    write_seed(minutes(200, holes=set(range(100, 147))))
    report = gapscan.scan(product())
    found = crits(report)
    assert len(found) == 1
    finding = found[0]
    assert finding.check == gapscan.CHECK_GAPS
    assert finding.rows == 47
    assert finding.from_ts.startswith("2023-01-01T01:40")
    assert finding.to_ts.startswith("2023-01-01T02:26")


def test_hole_listed_in_known_gaps_is_info_not_crit():
    """El delta pedido: un gap listado en known_gaps.json se reporta como INFO, no CRIT."""
    gap_from = (datetime(2023, 1, 1, 1, 40, tzinfo=timezone.utc)).isoformat()
    gap_to = (datetime(2023, 1, 1, 2, 26, tzinfo=timezone.utc)).isoformat()
    path = config.known_gaps_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "gaps": [
                    {
                        "exchange": "binance",
                        "symbol": "BTCUSDT",
                        "dtype": "klines",
                        "from": gap_from,
                        "to": gap_to,
                        "reason": "mantenimiento del exchange",
                        "verified_at": "2023-02-01T00:00:00+00:00",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    write_seed(minutes(200, holes=set(range(100, 147))))
    report = gapscan.scan(product())

    assert crits(report) == [], "un hueco listado en known_gaps no puede ser CRIT"
    known = [f for f in by_check(report, gapscan.CHECK_GAPS) if "hueco conocido" in f.detail]
    assert len(known) == 1
    assert known[0].severity == gapscan.SEV_INFO
    assert known[0].rows == 47
    assert "mantenimiento" in known[0].detail


def test_unpublished_tail_is_info_not_crit():
    """La cola que el exchange aun no publica no es un hueco."""
    year = datetime.now(timezone.utc).year
    start = datetime(year, 1, 1, tzinfo=timezone.utc) + timedelta(hours=1)
    write_seed(minutes(60, start=start))
    report = gapscan.scan(product())
    assert crits(report) == []
    pending = [f for f in by_check(report, gapscan.CHECK_GAPS) if f.severity == gapscan.SEV_INFO]
    assert any("pendiente de publicacion" in f.detail for f in pending)


def test_closed_year_is_not_reported_as_pending():
    """Un ano ya cerrado acaba el 31-dic por definicion: no es una cola pendiente.

    Antes de acotar el check a la particion del ano en curso, un historico completo de 7 anos
    generaba 7 "pendiente de publicacion" falsos de INFO.
    """
    write_seed(minutes(60, start=datetime(2019, 1, 1, tzinfo=timezone.utc)))
    report = gapscan.scan(product(), expected_start=datetime(2019, 1, 1, tzinfo=timezone.utc))
    pending = [f for f in by_check(report, gapscan.CHECK_GAPS) if f.severity == gapscan.SEV_INFO]
    assert pending == [], [f.detail for f in pending]


def test_known_gaps_of_another_symbol_does_not_hide_the_hole():
    path = config.known_gaps_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "gaps": [
                    {
                        "symbol": "ETHUSDT",
                        "dtype": "klines",
                        "from": "2023-01-01T01:40:00+00:00",
                        "to": "2023-01-01T02:26:00+00:00",
                        "reason": "otro simbolo",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    write_seed(minutes(200, holes=set(range(100, 147))))
    report = gapscan.scan(product())
    assert len(crits(report)) == 1, "un hueco de otro simbolo no puede tapar este"


# ------------------------------------------------------------------ check 2


def test_duplicates_are_crit():
    """Se salta el writer a proposito: el writer deduplica, asi que hay que sembrar el parquet
    a mano para que el detector tenga algo que encontrar."""
    table = minutes(30)
    write_raw(pa.concat_tables([table, table]), 2023)
    report = gapscan.scan(product())
    dups = by_check(report, gapscan.CHECK_DUP)
    assert len(dups) == 1
    assert dups[0].severity == gapscan.SEV_CRIT
    assert dups[0].rows == 30


def test_no_duplicates_after_dedupe_write():
    """El writer deduplica, asi que el seed duplicado no debe llegar al parquet."""
    table = minutes(30)
    write_seed(pa.concat_tables([table, table]))
    path = config.partition_path("klines", "BTCUSDT", "1m", 2023) / "part.parquet"
    assert pq.read_table(path).num_rows == 30


# ------------------------------------------------------------------ check 3


def test_outliers_high_low_and_nonpositive_price_are_crit():
    stamps = [datetime(2023, 1, 1, tzinfo=timezone.utc) + timedelta(minutes=i) for i in range(10)]
    table = pa.table(
        {
            "symbol": pa.array(["BTCUSDT"] * 10, pa.string()),
            "exchange": pa.array(["binance"] * 10, pa.string()),
            "open_time": pa.array(stamps, TS),
            "open": pa.array([1.0] * 10, pa.float64()),
            "high": pa.array([2.0] + [0.1] * 9, pa.float64()),  # 9 filas con high < low
            "low": pa.array([1.5] * 10, pa.float64()),
            "close": pa.array([1.2] * 10, pa.float64()),
            "volume": pa.array([-1.0] + [10.0] * 9, pa.float64()),
        }
    )
    write_seed(table)
    report = gapscan.scan(product())
    outliers = by_check(report, gapscan.CHECK_OUTLIERS)
    labels = {f.detail.split(":")[0] for f in outliers}
    assert "high<low" in labels
    assert "volume<0" in labels
    assert all(f.severity == gapscan.SEV_CRIT for f in outliers)


# ------------------------------------------------------------------ check 6


def test_coverage_warns_about_year_before_listing():
    old = datetime(2019, 6, 1, tzinfo=timezone.utc)
    write_seed(minutes(10, start=old))
    report = gapscan.scan(product(), expected_start=datetime(2023, 1, 1, tzinfo=timezone.utc))
    coverage = by_check(report, gapscan.CHECK_COVERAGE)

    # El ano 2019 es anterior al listing: WARN, no CRIT.
    warns = [f for f in coverage if f.severity == gapscan.SEV_WARN]
    assert len(warns) == 1, [f.detail for f in warns]
    assert "2019" in warns[0].detail

    # Los anos desde el listing hasta hoy sin particion son datos que no estan: CRIT.
    crits = [f for f in coverage if f.severity == gapscan.SEV_CRIT]
    assert len(crits) == 4, [f.detail for f in crits]
    assert all("sin particion" in f.detail for f in crits)
    assert report.stats["partitions"] == 1


def test_coverage_is_silent_when_grid_is_complete():
    """Con la rejilla de anos completa el check 6 no dice nada: no duplica el trabajo del 1."""
    year = datetime.now(timezone.utc).year
    write_seed(minutes(10, start=datetime(year, 1, 1, tzinfo=timezone.utc)))
    report = gapscan.scan(product(), expected_start=datetime(year, 1, 1, tzinfo=timezone.utc))
    assert by_check(report, gapscan.CHECK_COVERAGE) == []


# ------------------------------------------------------------------ salida


def test_report_written_as_json_and_markdown():
    write_seed(minutes(60))
    report = gapscan.scan(product())
    json_path, md_path = gapscan.write_report(report)
    assert json_path.exists() and md_path.exists()
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert set(payload) == {"generated_at", "stats", "counts", "findings"}
    assert payload["counts"]["CRIT"] == 0
    assert "Reporte de calidad" in md_path.read_text(encoding="utf-8")


def test_empty_lake_produces_no_findings():
    report = gapscan.scan(product())
    assert report.findings == []
    assert report.stats["partitions"] == 0


# ------------------------------------------------------------------ DuckDB


def test_checks_run_in_duckdb_over_the_parquet_not_in_python():
    """Los checks de datos van en SQL sobre el parquet: `lag()` y `count(DISTINCT)`."""
    import duckdb

    write_seed(minutes(120, holes={40}))
    path = config.partition_path("klines", "BTCUSDT", "1m", 2023) / "part.parquet"

    # El hueco sale de la window function de DuckDB, no de un LAG en Python.
    rows = gapscan.connection().execute(
        """
        WITH d AS (
            SELECT DISTINCT open_time AS ts FROM read_parquet(?)
        )
        SELECT epoch(ts) - epoch(lag(ts) OVER (ORDER BY ts)) AS delta
        FROM d
        """,
        [str(path)],
    ).fetchall()
    deltas = [r[0] for r in rows if r[0] is not None]
    assert deltas.count(120) == 1, "el hueco no es visible desde SQL"
    assert set(deltas) <= {60, 120}

    assert isinstance(gapscan.connection(), duckdb.DuckDBPyConnection)


def test_duckdb_reads_the_columns_of_a_funding_like_partition():
    """`check_outliers` no puede asumir OHLCV: fundingRate/metrics no lo tienen."""
    write_seed(minutes(60))
    path = config.partition_path("klines", "BTCUSDT", "1m", 2023) / "part.parquet"
    assert {"open_time", "open", "high", "low", "close", "volume"} <= set(
        gapscan.parquet_columns(path)
    )
    assert gapscan.row_count(path) == 60


def test_gapscan_is_deterministic_across_repeated_runs():
    """Dos scans seguidos sobre el mismo lake dan el mismo reporte: el SQL no es acumulativo."""
    write_seed(minutes(300, holes={10, 250}))
    first = gapscan.scan(product(), expected_start=datetime(2023, 1, 1, tzinfo=timezone.utc))
    second = gapscan.scan(product(), expected_start=datetime(2023, 1, 1, tzinfo=timezone.utc))

    gaps_first = by_check(first, gapscan.CHECK_GAPS)
    gaps_second = by_check(second, gapscan.CHECK_GAPS)
    assert len(gaps_first) == 2, [f.detail for f in gaps_first]
    assert [f.detail for f in gaps_first] == [f.detail for f in gaps_second]
    assert [f.from_ts for f in gaps_first] == [f.from_ts for f in gaps_second]
    assert first.stats == second.stats
    assert first.stats["rows"] == 298
    assert first.stats["gaps_unexplained"] == 2


def test_la_conexion_del_gapscan_esta_en_utc():
    """Regresion de la regla 3.bis aplicada a gapscan.

    Si `_con()` deja de fijar UTC, los buckets por año de los informes desplazan filas entre años
    sin cambiar el total, y el informe sigue pareciendo correcto.
    """
    con = gapscan.connection()
    assert con.execute("SELECT current_setting('TimeZone')").fetchone()[0] == "UTC"


def test_el_bucket_de_ano_no_se_desplaza_en_la_conexion_del_gapscan():
    con = gapscan.connection()
    ts = "2020-01-01 00:30:00+00"  # 00:30 UTC = 01:30 en Madrid, mismo año, pero el día sí cambia
    assert con.execute(f"SELECT date_trunc('day', TIMESTAMPTZ '{ts}')::DATE").fetchone()[0].isoformat() == "2020-01-01"
    ts2 = "2019-12-31 23:30:00+00"  # 23:30 UTC = 00:30 del año siguiente en Madrid
    assert con.execute(f"SELECT year(TIMESTAMPTZ '{ts2}')").fetchone()[0] == 2019
