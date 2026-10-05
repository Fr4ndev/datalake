"""Tests de `bulk.known_gaps`: el hueco solo se registra si esta verificado.

El modulo existe por la regla 4 de `AGENTS.md`: un hueco en `known_gaps.json` tiene que estar
confirmado contra una segunda fuente. Aqui se comprueba que:

- un hueco que el REST **no** tiene se registra;
- un hueco que el REST **si** tiene se clasifica como rellenable (falta en el archivo, no en el
  exchange) y por tanto NO se escribe en known_gaps.json;
- el REST se consulta un numero acotado de veces, no una por hueco;
- el archivo escrito es idempotente y ordenado.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pytest

from bulk import config, gapscan, known_gaps, rest, writer
from bulk.schemas import Product


@pytest.fixture(autouse=True)
def lake(tmp_path, monkeypatch):
    monkeypatch.setenv("LAKE_DIR", str(tmp_path / "lake"))
    # Por defecto el exchange "estuvo vivo": asi los tests no tocan la red. Los tests que
    # necesitan el detalle stubban `probe_rest` y este explicitamente.
    monkeypatch.setattr(known_gaps, "exchange_was_up", lambda p, s, e: True)
    gapscan.reset_connection()
    known_gaps._PROBE_CACHE.clear()
    yield tmp_path / "lake"
    gapscan.reset_connection()
    known_gaps._PROBE_CACHE.clear()


def product() -> Product:
    return Product(dtype="metrics", symbol="BTCUSDT", tf="5m")


def series(start: datetime, count: int, step_minutes: int = 5) -> pa.Table:
    stamps = [start + timedelta(minutes=step_minutes * i) for i in range(count)]
    n = len(stamps)
    return pa.table(
        {
            "symbol": pa.array(["BTCUSDT"] * n, pa.string()),
            "exchange": pa.array(["binance"] * n, pa.string()),
            "create_time": pa.array(stamps, pa.timestamp("us", tz="UTC")),
            "sum_open_interest": pa.array([1.0] * n, pa.float64()),
        }
    )


def seed(holes: set[int], count: int = 600) -> None:
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    writer.write(series(start, count, 5).take([i for i in range(count) if i not in holes]), product())


def rest_stub(present: list[tuple[datetime, datetime]]):
    """Sustituye `probe_rest` por un doble que devuelve PRESENT solo en las ventanas dadas."""
    calls: list[tuple[datetime, datetime]] = []

    def fake(product_, start, end):
        calls.append((start, end))
        if any(s <= start <= e or s <= end <= e for s, e in present):
            return known_gaps.PRESENT
        return known_gaps.UNVERIFIABLE

    return fake, calls


def test_gap_absent_from_rest_is_registered(monkeypatch):
    """Si el REST tiene datos en la ventana pero NO en el hueco, el hueco es real y se registra."""
    seed(holes={10})

    def fake(product_, start, end):
        # `probe_rest` recibe el rango del hueco y decide mirando la ventana +-1 dia: aqui la
        # ventana tiene datos pero el timestamp del hueco no, o sea hueco real confirmado.
        return known_gaps.ABSENT

    monkeypatch.setattr(known_gaps, "probe_rest", fake)
    known_gaps._PROBE_CACHE.clear()

    registrables, rellenables, unverifiable = known_gaps.classify(
        product(), known_gaps.find_gaps(product())
    )
    assert len(registrables) == 1
    assert rellenables == [] and unverifiable == []
    assert registrables[0].reason == known_gaps.REASON_NO_SOURCE
    assert registrables[0].rows == 1
    assert "hueco real" in registrables[0].note


def test_gap_present_in_rest_is_fillable_and_not_registered(monkeypatch):
    """Si el REST tiene el timestamp, el dato existe: falta en el zip, no en el exchange.

    Registrar esto como hueco seria un error: se declararia perdida una fila que se puede recuperar.
    """
    seed(holes={10})
    window = (datetime(2024, 1, 1, 0, 40, tzinfo=timezone.utc), datetime(2024, 1, 1, 0, 50, tzinfo=timezone.utc))
    fake, _ = rest_stub([window])
    monkeypatch.setattr(known_gaps, "probe_rest", fake)
    known_gaps._PROBE_CACHE.clear()

    registrables, rellenables, unverifiable = known_gaps.classify(
        product(), known_gaps.find_gaps(product())
    )
    assert registrables == [] and unverifiable == []
    assert len(rellenables) == 1
    assert rellenables[0].reason == known_gaps.REASON_ARCHIVE_SLOT
    assert rellenables[0].verified_by == rest.OPEN_INTEREST_PATH


def test_rest_is_probed_a_bounded_number_of_times(monkeypatch):
    """Con 50 huecos y probe=5 no se pueden hacer 50 llamadas al endpoint."""
    seed(holes=set(range(50, 100)), count=200)
    fake, calls = rest_stub([])
    monkeypatch.setattr(known_gaps, "probe_rest", fake)
    known_gaps._PROBE_CACHE.clear()

    known_gaps.classify(product(), known_gaps.find_gaps(product()), probe=5)
    assert len(calls) <= 5, f"se han hecho {len(calls)} llamadas al REST"
    # Con cache por dia, el numero de llamadas no puede superar el de dias distintos.
    assert len(calls) == len({c[0].date() for c in calls})


def test_probe_sample_spans_the_whole_range(monkeypatch):
    """La muestra se reparte uniformemente: si se cogiera solo el principio no serviria."""
    # 3000 slots de 5m son ~10 dias, con un hueco cada 30 (~2.5 h).
    seed(holes=set(range(0, 3000, 30)), count=3000)
    fake, calls = rest_stub([])
    monkeypatch.setattr(known_gaps, "probe_rest", fake)
    known_gaps._PROBE_CACHE.clear()

    known_gaps.classify(product(), known_gaps.find_gaps(product()), probe=4)
    dias = {c[0].date() for c in calls}
    assert len(dias) >= 3, f"la muestra solo cubre {len(dias)} dias"
    assert max(dias) - min(dias) >= timedelta(days=2), "la muestra no cubre el rango"


def test_no_gaps_writes_an_empty_but_valid_file(monkeypatch):
    """Sin huecos el archivo se crea vacio, que es distinto de no crear el archivo."""
    seed(holes=set(), count=200)
    fake, _ = rest_stub([])
    monkeypatch.setattr(known_gaps, "probe_rest", fake)
    known_gaps._PROBE_CACHE.clear()

    path, registered, fillable = known_gaps.generate(product())
    assert path == config.known_gaps_path()
    assert registered == 0 and fillable == 0
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["gaps"] == []
    assert payload["version"] == 1


def test_write_is_idempotent_and_sorted(monkeypatch):
    """Dos ejecuciones con los mismos huecos producen el mismo fichero."""
    seed(holes={5, 100, 300})
    fake, _ = rest_stub([])
    monkeypatch.setattr(known_gaps, "probe_rest", fake)
    known_gaps._PROBE_CACHE.clear()

    first = known_gaps.generate(product(), verified_at="2024-01-01T00:00:00+00:00")[0]
    content_first = first.read_text(encoding="utf-8")
    second = known_gaps.generate(product(), verified_at="2024-01-01T00:00:00+00:00")[0]

    assert second.read_text(encoding="utf-8") == content_first
    gaps = json.loads(content_first)["gaps"]
    assert [g["from"] for g in gaps] == sorted(g["from"] for g in gaps)


def test_unpublished_tail_is_not_a_gap(monkeypatch):
    """La cola que el exchange aun no publica no se cuela en known_gaps.json."""
    year = datetime.now(timezone.utc).year
    start = datetime(year, 1, 1, tzinfo=timezone.utc)
    stamps = [start + timedelta(minutes=5 * i) for i in range(3)]
    n = len(stamps)
    writer.write(
        pa.table(
            {
                "symbol": pa.array(["BTCUSDT"] * n, pa.string()),
                "exchange": pa.array(["binance"] * n, pa.string()),
                "create_time": pa.array(stamps, pa.timestamp("us", tz="UTC")),
                "sum_open_interest": pa.array([1.0] * n, pa.float64()),
            }
        ),
        product(),
    )
    fake, _ = rest_stub([])
    monkeypatch.setattr(known_gaps, "probe_rest", fake)
    known_gaps._PROBE_CACHE.clear()
    assert known_gaps.find_gaps(product()) == []


def test_registered_gap_is_reported_as_info_by_the_gapscan(monkeypatch):
    """Cierre del circulo: lo que se registra como conocido, el check 1 lo baja a INFO."""
    seed(holes={10}, count=200)
    path = config.known_gaps_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "gaps": [
                    {
                        "symbol": "BTCUSDT",
                        "dtype": "metrics",
                        "tf": "5m",
                        "from": "2024-01-01T00:40:00+00:00",
                        "to": "2024-01-01T00:50:00+00:00",
                        "rows": 1,
                        "reason": "vision_daily_missing_slot",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    fake, _ = rest_stub([])
    monkeypatch.setattr(known_gaps, "probe_rest", fake)
    known_gaps._PROBE_CACHE.clear()

    report = gapscan.scan(product())
    assert [f for f in report.findings if f.severity == gapscan.SEV_CRIT] == []
    info = [f for f in report.findings if f.severity == gapscan.SEV_INFO]
    assert any("hueco conocido" in f.detail for f in info)


def test_gap_outside_rest_retention_is_confirmed_with_klines(monkeypatch):
    """`openInterestHist` solo conserva ~30 dias: para fechas antiguas se usa klines.

    Si el exchange publico velas de 1m continuas durante la ventana del hueco, no fue una caida
    suya y el punto se registra como hueco del ARCHIVO de Vision (no recuperable, pero explica).
    """
    seed(holes={10})
    monkeypatch.setattr(known_gaps, "probe_rest", lambda p, s, e: known_gaps.UNVERIFIABLE)
    seen: list[tuple] = []

    def up(product_, start, end):
        seen.append((start, end))
        return True

    monkeypatch.setattr(known_gaps, "exchange_was_up", up)

    registrables, rellenables, unverifiable = known_gaps.classify(
        product(), known_gaps.find_gaps(product())
    )
    assert len(registrables) == 1
    assert rellenables == [] and unverifiable == []
    gap = registrables[0]
    assert gap.reason == known_gaps.REASON_ARCHIVE_SLOT
    assert rest.KLINES_PATH in gap.verified_by
    assert "~30 dias" in gap.note
    assert len(seen) == 1


def test_gap_with_no_evidence_at_all_is_not_registered(monkeypatch):
    """Sin REST y sin klines no hay nada que confirmar: el hueco NO entra en known_gaps.json."""
    seed(holes={10})
    monkeypatch.setattr(known_gaps, "probe_rest", lambda p, s, e: known_gaps.UNVERIFIABLE)
    monkeypatch.setattr(known_gaps, "exchange_was_up", lambda p, s, e: False)

    registrables, rellenables, unverifiable = known_gaps.classify(
        product(), known_gaps.find_gaps(product())
    )
    assert registrables == [] and rellenables == []
    assert len(unverifiable) == 1
    assert unverifiable[0].verified_by == "ninguna"


def test_written_file_is_read_back_by_the_gapscan(monkeypatch):
    """Regresion de contrato: lo que escribe el generador, el gap-scan tiene que encontrarlo.

    El dataclass usa `from_ts`/`to_ts` porque `from` es palabra reservada, pero el JSON se
    serializa como `from`/`to`. Si las dos graficas no casan, el hueco se registra y sigue
    apareciendo como CRIT en cada gap-scan, sin que nada falle de forma visible.
    """
    seed(holes={10}, count=200)
    monkeypatch.setattr(known_gaps, "probe_rest", lambda p, s, e: known_gaps.ABSENT)
    path, registered, _ = known_gaps.generate(product())

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert "from" in payload["gaps"][0] and "to" in payload["gaps"][0]
    assert "from_ts" not in payload["gaps"][0]
    assert registered == 1

    report = gapscan.scan(product())
    assert [f for f in report.findings if f.severity == gapscan.SEV_CRIT] == []
    assert any("hueco conocido" in f.detail for f in report.findings)
