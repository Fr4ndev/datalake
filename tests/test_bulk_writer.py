"""Tests de `bulk.writer` y `bulk.manifest`: dedup, atomicidad e idempotencia.

El criterio de done de la skill exige que un SIGKILL a mitad y una re-ejecucion reanuden sin
duplicar filas. Aqui se prueba eso sin red: se escribe la misma particion dos veces y se compara
el numero de filas.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from bulk import manifest, writer
from bulk.config import partition_path
from bulk.schemas import KLINES, METRICS, Product

TS = pa.timestamp("us", tz="UTC")


@pytest.fixture(autouse=True)
def lake(tmp_path, monkeypatch):
    """Redirige LAKE_DIR a un temporal para no tocar el lake real."""
    monkeypatch.setenv("LAKE_DIR", str(tmp_path / "lake"))
    return tmp_path / "lake"


def candles(rows: list[tuple[str, float]], *, symbol: str = "BTCUSDT") -> pa.Table:
    """Tabla de velas minima con la columna de tiempo ya en UTC."""
    return pa.table(
        {
            "symbol": pa.array([symbol] * len(rows), pa.string()),
            "open_time": pa.array([datetime.fromisoformat(ts).replace(tzinfo=timezone.utc) for ts, _ in rows], TS),
            "close": pa.array([c for _, c in rows], pa.float64()),
        }
    )


def product() -> Product:
    return Product(dtype="klines", symbol="BTCUSDT", tf="1m")


def test_write_creates_partition_with_year():
    table = candles([("2020-01-01T00:00", 1.0), ("2020-01-01T00:01", 2.0)])
    result = writer.write(table, product())
    assert 2020 in result
    path, rows = result[2020]
    assert rows == 2
    assert path == partition_path("klines", "BTCUSDT", "1m", 2020) / "part.parquet"
    assert path.exists()


def test_partition_path_layout():
    path = partition_path("klines", "BTCUSDT", "1m", 2020)
    assert path.parts[-4:] == ("klines", "symbol=BTCUSDT", "tf=1m", "year=2020")


def test_splits_rows_across_years():
    table = candles([("2019-12-31T23:59", 1.0), ("2020-01-01T00:00", 2.0)])
    result = writer.write(table, product())
    assert set(result) == {2019, 2020}
    assert result[2019][1] == 1 and result[2020][1] == 1


def test_dedupe_removes_repeated_rows():
    """El caso real de metrics: cada timestamp viene dos veces, identico."""
    ts = datetime(2020, 9, 1, 0, 0, tzinfo=timezone.utc)
    table = pa.table(
        {
            "symbol": pa.array(["BTCUSDT", "BTCUSDT"], pa.string()),
            "create_time": pa.array([ts, ts], TS),
            "sum_open_interest": pa.array([1.0, 1.0], pa.float64()),
        }
    )
    assert table.num_rows == 2
    out = writer.dedupe(table, ("symbol", "create_time"))
    assert out.num_rows == 1


def test_writing_same_data_twice_is_idempotent():
    """Re-ejecucion del bulk: no debe duplicar filas."""
    table = candles([("2020-01-01T00:00", 1.0), ("2020-01-01T00:01", 2.0)])
    first = writer.write(table, product())
    second = writer.write(table, product())
    assert first[2020][1] == 2
    assert second[2020][1] == 2, "la segunda escritura ha duplicado filas"
    path = partition_path("klines", "BTCUSDT", "1m", 2020) / "part.parquet"
    assert pq.read_table(path).num_rows == 2


def test_overlapping_and_disjoint_writes_accumulate():
    jan_a = candles([("2020-01-01T00:00", 1.0)])
    jan_b = candles([("2020-01-01T00:01", 2.0), ("2020-01-01T00:02", 3.0)])
    writer.write(jan_a, product())
    writer.write(jan_b, product())
    path = partition_path("klines", "BTCUSDT", "1m", 2020) / "part.parquet"
    stored = pq.read_table(path)
    assert stored.num_rows == 3
    # y la tabla queda ordenada por tiempo
    stamps = stored.column("open_time").to_pylist()
    assert stamps == sorted(stamps)


def test_no_temporary_files_left_behind():
    table = candles([("2020-01-01T00:00", 1.0)])
    writer.write(table, product())
    directory = partition_path("klines", "BTCUSDT", "1m", 2020)
    # Solo `part.parquet` y el lock de escritura; ningun temporal.
    assert sorted(p.name for p in directory.iterdir()) == [".write.lock", "part.parquet"]


def test_write_is_atomic_via_replace(tmp_path, monkeypatch):
    """Si falla la escritura, el parquet anterior sigue intacto (no queda un temporal)."""
    table = candles([("2020-01-01T00:00", 1.0)])
    writer.write(table, product())
    path = partition_path("klines", "BTCUSDT", "1m", 2020) / "part.parquet"
    before = path.read_bytes()

    boom = RuntimeError("disco lleno")
    real_write = pq.write_table

    def exploding(*a, **k):
        boom_exc = boom
        raise boom_exc

    monkeypatch.setattr(pq, "write_table", exploding)
    with pytest.raises(RuntimeError):
        writer.write(candles([("2020-01-01T00:05", 9.0)]), product())
    monkeypatch.setattr(pq, "write_table", real_write)

    assert path.read_bytes() == before, "el parquet anterior se ha corrompido"
    leftovers = [p.name for p in path.parent.iterdir() if p.name.endswith(".tmp")]
    assert leftovers == [], f"han quedado temporales: {leftovers}"


def test_concurrent_writes_to_same_year_do_not_lose_rows():
    """Regresion: 12 workers escriben periodos del mismo ano en paralelo.

    Sin lock por particion, todos leen el mismo estado inicial y el ultimo `os.replace` pisa al
    resto. En la primera ejecucion real se perdieron 2.096.640 de 3.553.920 filas.
    """
    from concurrent.futures import ThreadPoolExecutor

    months = 12
    base = datetime(2020, 1, 1, tzinfo=timezone.utc)

    def chunk(month: int) -> pa.Table:
        start = base + timedelta(days=31 * month)
        stamps = [start + timedelta(minutes=i) for i in range(200)]
        n = len(stamps)
        return pa.table(
            {
                "symbol": pa.array(["BTCUSDT"] * n, pa.string()),
                "open_time": pa.array(stamps, TS),
                "close": pa.array([float(month)] * n, pa.float64()),
            }
        )

    with ThreadPoolExecutor(max_workers=months) as pool:
        list(pool.map(lambda m: writer.write(chunk(m), product()), range(months)))

    path = partition_path("klines", "BTCUSDT", "1m", 2020) / "part.parquet"
    stored = pq.read_table(path)
    assert stored.num_rows == months * 200, (
        f"se perdieron filas por escrituras concurrentes: {stored.num_rows} != {months * 200}"
    )
    assert len(set(stored.column("open_time").to_pylist())) == months * 200


def test_concurrent_identical_writes_do_not_duplicate():
    """Lo mismo, pero con los mismos datos en paralelo: el dedup debe absorberlo."""
    from concurrent.futures import ThreadPoolExecutor

    table = candles([("2020-01-01T00:00", 1.0), ("2020-01-01T00:01", 2.0)])
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda _: writer.write(table, product()), range(8)))

    path = partition_path("klines", "BTCUSDT", "1m", 2020) / "part.parquet"
    assert pq.read_table(path).num_rows == 2


def test_lock_file_does_not_break_partition_listing():
    """El `.write.lock` no debe aparecer como si fuera una particion de datos."""
    writer.write(candles([("2020-01-01T00:00", 1.0)]), product())
    directory = partition_path("klines", "BTCUSDT", "1m", 2020)
    assert (directory / ".write.lock").exists()
    assert writer.read_partition(directory / "part.parquet") is not None


def test_ensure_identity_adds_and_overwrites_columns():
    table = pa.table({"symbol": pa.array(["XXX"]), "create_time": pa.array([datetime(2020, 9, 1, tzinfo=timezone.utc)], TS)})
    out = writer.ensure_identity(table, symbol="BTCUSDT", exchange="binance_um")
    assert out.column("symbol")[0].as_py() == "BTCUSDT"
    assert out.column("exchange")[0].as_py() == "binance_um"
    assert out.column_names[:2] == ["symbol", "exchange"]


def test_metrics_time_column_name_is_respected():
    product_metrics = Product(dtype="metrics", symbol="BTCUSDT", tf="5m")
    assert product_metrics.schema.time_column == "create_time"
    assert product_metrics.lake_dirname == "metrics"
    assert KLINES.time_column == "open_time"


# ------------------------------------------------------------------ manifest


def test_manifest_append_and_read():
    manifest.append(
        manifest.Entry(
            exchange="binance_um",
            symbol="BTCUSDT",
            dtype="klines",
            period="monthly:2020-01",
            file="data/futures/um/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2020-01.zip",
            sha256="ab" * 32,
            rows=44640,
            status="done",
        )
    )
    entries = manifest.read_all()
    assert len(entries) == 1
    assert entries[0].rows == 44640


def test_manifest_done_keys_drives_resume():
    key = ("binance_um", "BTCUSDT", "klines", "monthly:2020-02")
    assert key not in manifest.done_keys()
    manifest.append(
        manifest.Entry(exchange="binance_um", symbol="BTCUSDT", dtype="klines", period="monthly:2020-02", file="x", status="done")
    )
    assert key in manifest.done_keys()


def test_manifest_latest_status_wins():
    """Un periodo fallido y luego reintentado bien debe quedar como done."""
    for status in ("failed", "done"):
        manifest.append(
            manifest.Entry(
                exchange="binance_um",
                symbol="BTCUSDT",
                dtype="klines",
                period="monthly:2020-03",
                file="x",
                status=status,
            )
        )
    assert ("binance_um", "BTCUSDT", "klines", "monthly:2020-03") in manifest.done_keys()


def test_manifest_tolerates_truncated_last_line():
    """Un SIGKILL escribiendo el manifest no puede invalidar el historico."""
    manifest.append(
        manifest.Entry(exchange="binance_um", symbol="BTCUSDT", dtype="klines", period="monthly:2020-04", file="x", status="done")
    )
    with manifest.config.manifest_path().open("a", encoding="utf-8") as fh:
        fh.write('{"exchange": "binance_um", "sym')  # linea a medias
    entries = manifest.read_all()
    assert len(entries) == 1


def test_split_by_year_keeps_sub_second_precision():
    """Regresion: `fundingRate` tiene `calc_time` con ms y el reparto por ano no puede castear a s.

    Con `pc.cast(..., timestamp[s])` pyarrow lanza
    `ArrowInvalid: Casting from timestamp[ms, tz=UTC] to timestamp[s, tz=UTC] would lose data`.
    """
    from bulk import writer as w

    # microsecond=1000 son 1 ms: datetime solo llega a microsegundos, asi que el milisegundo
    # se expresa como multiplo de 1000.
    stamps = [
        datetime(2024, 1, 1, 0, 0, 0, 1000, tzinfo=timezone.utc),  # .001
        datetime(2024, 6, 1, 0, 0, 0, 999000, tzinfo=timezone.utc),  # .999
        datetime(2025, 1, 1, 0, 0, 0, 500000, tzinfo=timezone.utc),  # .500
    ]
    table = pa.table(
        {
            "symbol": pa.array(["BTCUSDT"] * 3, pa.string()),
            "calc_time": pa.array(stamps, pa.timestamp("ms", tz="UTC")),
        }
    )
    parts = w.split_by_year(table, "calc_time")
    assert sorted(parts) == [2024, 2025]
    assert parts[2024].num_rows == 2 and parts[2025].num_rows == 1
    # El milisegundo sobrevive al reparto.
    assert parts[2025].column("calc_time")[0].as_py() == stamps[2]
    assert parts[2024].column("calc_time")[0].as_py().microsecond == 1000
    assert parts[2024].column("calc_time")[1].as_py().microsecond == 999000
    assert w.years_in(table, "calc_time") == [2024, 2025]
