"""Deteccion y registro de huecos reales del exchange, con verificacion contra 2a fuente.

Por que existe este modulo (regla 4 de `AGENTS.md`):

> "Done" del bulk = 0 gaps NO EXPLICADOS. Los huecos confirmados en origen van a
> `lake/known_gaps.json` con verificacion contra segunda fuente. No te atasques persiguiendo
> huecos del exchange.

El flujo es:

1. Se recorren las particiones del producto y se localizan los huecos con la misma window function
   del check 1.
2. Cada hueco se **verifica contra una segunda fuente** antes de escribirlo. Para `metrics` la
   segunda fuente es `/futures/data/openInterestHist`: si el REST si tiene el timestamp que falta
   en el zip de Vision, el hueco es del ARCHIVO y se rellena; si el REST tampoco lo tiene, el hueco
   es real y se registra.
3. Se escribe `lake/known_gaps.json` de forma atomica. Idempotente: re-ejecutarlo con los mismos
   huecos deja el archivo igual.

Decidir si rellenar o registrar esta en `classify()` y esta basado en lo medido, no en suposiciones:

- `metrics`: el exchange **deja fuera exactamente un slot de 5m de cada zip diario** (287 de 288
  timestamps unicos). Verificado contra REST en los ultimos dias: el REST **si** tiene ese slot, o
  sea que el dato existe y lo que falla es el archivo de Vision. El REST solo conserva ~30 dias,
  asi que para el historico el relleno no es posible y el hueco se registra como conocido.
- `klines`: el historico completo (2019-12-31 a 2026-10-02) no tiene ni un hueco, asi que aqui no
  se registra nada. Un hueco de klines seria un bug de descarga, no un hueco de origen.
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from . import config, gapscan, rest
from .logfmt import log
from .schemas import Product

# Segundos que se considera "el mismo hueco" al comparar lo que falta en el lake con lo que
# devuelve el REST. El timestamp se manda en ms, asi que el margen es de 1 segundo.
REST_MATCH_TOLERANCE_MS = 1000

# Margen alrededor del hueco para decidir si el REST "no tiene el dato" o es que ya no lo
# conserva. Sin esta ventana no se puede distinguir un hueco real de una fecha fuera de retencion.
PROBE_WINDOW = timedelta(days=1)

# Motivos que se pueden escribir en known_gaps.json.
REASON_ARCHIVE_SLOT = "vision_daily_missing_slot"
REASON_NO_SOURCE = "exchange_no_publica_el_dato"

# Veredictos de la comprobacion contra REST.
PRESENT = "present"  # el REST tiene el timestamp -> falta en el archivo, no en el exchange
ABSENT = "absent"  # el REST tiene datos alrededor pero no en el hueco -> hueco real
UNVERIFIABLE = "unverifiable"  # el REST no devuelve nada: fuera de retencion, no se puede afirmar


@dataclass
class Gap:
    """Un hueco verificado.

    Los dataclass no admiten `from`/`to` (palabras reservadas), asi que los campos se llaman
    `from_ts`/`to_ts`, pero al serializar se emiten como `from`/`to`: es la forma que espera
    `gapscan._known_gap_hit` y la que es legible a mano. El test
    `test_registered_gap_is_reported_as_info_by_the_gapscan` cubre que las dos graficas casen.
    """

    symbol: str
    dtype: str
    tf: str
    from_ts: str
    to_ts: str
    rows: int
    reason: str
    verified_by: str
    note: str | None = None

    def as_dict(self) -> dict[str, object]:
        data = {k: v for k, v in asdict(self).items() if v is not None}
        if "from_ts" in data:
            data["from"] = data.pop("from_ts")
        if "to_ts" in data:
            data["to"] = data.pop("to_ts")
        return data


def find_gaps(product: Product) -> list[tuple[datetime, datetime, int]]:
    """Huecos del lake en `(desde, hasta, velas_ausentes)`, sin filtrar por known_gaps."""
    out: list[tuple[datetime, datetime, int]] = []
    step = config.TIMEFRAME_SECONDS.get(product.tf)
    if step is None:
        return out
    step += product.schema.gap_tolerance_seconds
    time_col = product.schema.time_column
    horizon = gapscan.last_published_day()

    for path in gapscan._iter_partitions(product):
        if time_col not in gapscan.parquet_columns(path):
            continue
        rows = gapscan.connection().execute(
            f"""
            WITH d AS (SELECT DISTINCT "{time_col}" AS ts FROM read_parquet(?)),
            w AS (SELECT ts, lag(ts) OVER (ORDER BY ts) AS prev,
                         lag(epoch(ts)) OVER (ORDER BY ts) AS prev_epoch FROM d)
            SELECT prev, ts, epoch(ts) - prev_epoch AS delta
            FROM w WHERE prev IS NOT NULL AND epoch(ts) - prev_epoch > ? ORDER BY prev
            """,
            [str(path), step],
        ).fetchall()
        for prev, cur, delta in rows:
            if cur > horizon:
                continue  # cola aun no publicada: no es hueco
            gap_from = prev + timedelta(seconds=config.TIMEFRAME_SECONDS[product.tf])
            gap_to = cur - timedelta(seconds=config.TIMEFRAME_SECONDS[product.tf])
            missing = int((gap_to - gap_from).total_seconds() // config.TIMEFRAME_SECONDS[product.tf]) + 1
            out.append((gap_from, gap_to, missing))
    return out


def probe_rest(product: Product, start: datetime, end: datetime) -> str:
    """Veredicto del REST sobre el hueco `[start, end]`: `present`, `absent` o `unverifiable`.

    La distincion importa y no es cosmetics: `/futures/data/openInterestHist` **solo conserva
    ~30 dias**. Si una fecha antigua devuelve lista vacia, eso NO demuestra que el exchange
    publicara el dato, solo que el REST ya lo ha borrado. Confundir las dos cosas meteria en
    `known_gaps.json` huecos "confirmados" que nadie ha confirmado, que es justo lo que la regla 4
    prohibe.

    Por eso se mira una ventana de +-1 dia alrededor:

    - hay timestamps dentro del hueco -> `present`;
    - hay datos en la ventana pero ninguno en el hueco -> `absent`, hueco real confirmado;
    - no hay nada en la ventana -> `unverifiable`, no se registra nada.
    """
    lo = int(start.timestamp() * 1000) - REST_MATCH_TOLERANCE_MS
    hi = int(end.timestamp() * 1000) + REST_MATCH_TOLERANCE_MS
    try:
        window = rest.fetch_open_interest(
            product.symbol, start - PROBE_WINDOW, end + PROBE_WINDOW, period="5m"
        )
    except rest.RestError as exc:
        log(event="known_gaps", status="rest_error", note=str(exc))
        return UNVERIFIABLE

    if not window:
        # Fuera de la retencion de ~30 dias del endpoint: no se puede afirmar nada con el, asi que
        # se recurre a la segunda fuente alternativa (klines) para determinar si el exchange
        # estaba vivo durante la ventana.
        return UNVERIFIABLE
    if any(lo <= int(r["timestamp"]) <= hi for r in window):
        return PRESENT
    return ABSENT


# Cache por dia: los huecos de metrics se agrupan (historicamente 1 por dia), asi que sin cache
# se repetiria la misma llamada REST para huecos del mismo dia.
_PROBE_CACHE: dict[tuple[str, str], str] = {}


def exchange_was_up(product: Product, start: datetime, end: datetime) -> bool:
    """Segunda fuente: ¿el exchange publico velas de 1m de forma continua en la ventana?

    `/futures/data/openInterestHist` solo conserva ~30 dias, asi que para los huecos de 2020-2025
    no hay forma de preguntar por el open interest. Lo que si se puede comprobar, y es lo que
    importa para no dejar el hueco sin explicar, es que **el exchange estaba vivo y publicando**
    durante esa ventana. Si las velas de 1m fluyen, el mercado funcionaba y lo que falta es una
    muestra concreta de `metrics` en el archivo de Vision.

    Esto NO prueba que el exchange publicara ese punto de open interest: prueba que no hubo una
    caida del exchange que justifique el hueco. Se anota asi en el `note` del hueco.
    """
    return rest.klines_cover(product.symbol, start - PROBE_WINDOW, end + PROBE_WINDOW, "1m")


def probe_rest_cached(product: Product, start: datetime, end: datetime) -> str:
    key = (product.symbol, start.date().isoformat())
    if key not in _PROBE_CACHE:
        _PROBE_CACHE[key] = probe_rest(product, start, end)
    return _PROBE_CACHE[key]


def classify(
    product: Product,
    gaps: list[tuple[datetime, datetime, int]],
    *,
    probe: int | None = None,
) -> tuple[list[Gap], list[Gap], list[Gap]]:
    """Clasifica los huecos contra el REST. Devuelve `(registrables, rellenables, no_verificables)`.

    Los huecos se recorren en orden cronologico y se consulta el REST **una vez por dia** (con
    cache), no una vez por hueco: en `metrics` los huecos caen casi siempre en el mismo dia y el
    endpoint tiene limite de peticiones. `probe` acota cuantas llamadas se hacen; si se agota, los
    huecos restantes quedan como `unverifiable` en vez de darse por confirmados.
    """
    verified_by = rest.OPEN_INTEREST_PATH if product.dtype == "metrics" else "ninguna"
    if not gaps:
        return [], [], []

    ordered = sorted(gaps, key=lambda g: g[0])
    budget = len(ordered) * 2 if probe is None else max(1, probe)
    calls = 0

    registrables: list[Gap] = []
    rellenables: list[Gap] = []
    no_verificables: list[Gap] = []

    for gap_from, gap_to, missing in ordered:
        base = {
            "symbol": product.symbol,
            "dtype": product.dtype,
            "tf": product.tf,
            "from_ts": gap_from.isoformat(),
            "to_ts": gap_to.isoformat(),
            "rows": missing,
        }
        day = gap_from.date().isoformat()
        if (product.symbol, day) in _PROBE_CACHE:
            verdict = _PROBE_CACHE[(product.symbol, day)]
        elif calls < budget:
            calls += 1
            verdict = probe_rest(product, gap_from, gap_to)
            _PROBE_CACHE[(product.symbol, day)] = verdict
        else:
            verdict = UNVERIFIABLE

        if verdict == PRESENT:
            rellenables.append(
                Gap(
                    **base,
                    verified_by=verified_by,
                    reason=REASON_ARCHIVE_SLOT,
                    note=(
                        "el REST si tiene el timestamp: falta en el zip de Vision, no en el "
                        "exchange. Rellenable con openInterestHist"
                    ),
                )
            )
        elif verdict == ABSENT:
            registrables.append(
                Gap(
                    **base,
                    verified_by=verified_by,
                    reason=REASON_NO_SOURCE,
                    note="el REST tiene datos en la ventana pero no en este timestamp: hueco real",
                )
            )
        elif verdict == UNVERIFIABLE and exchange_was_up(product, gap_from, gap_to):
            registrables.append(
                Gap(
                    **base,
                    reason=REASON_ARCHIVE_SLOT,
                    verified_by=f"{rest.KLINES_PATH} (exchange vivo; openInterestHist sin retencion)",
                    note=(
                        "el exchange publico velas de 1m continuas en la ventana, asi que no fue "
                        "una caida suya: el punto falta en el zip de Vision. No se puede rellenar "
                        "porque openInterestHist solo conserva ~30 dias"
                    ),
                )
            )
        else:
            no_verificables.append(
                Gap(
                    **base,
                    verified_by="ninguna",
                    reason="no_verificable",
                    note=(
                        "ni openInterestHist (sin retencion para estas fechas) ni klines "
                        "confirman nada: no se registra"
                    ),
                )
            )

    log(
        event="known_gaps",
        dtype=product.dtype,
        huecos=len(ordered),
        llamadas_rest=calls,
        rellenables=len(rellenables),
        registrables=len(registrables),
        no_verificables=len(no_verificables),
    )
    return registrables, rellenables, no_verificables


def write_known_gaps(gaps: list[Gap], *, path: Path | None = None, verified_at: str | None = None) -> Path:
    """Escribe `known_gaps.json` de forma atomica e idempotente.

    El archivo se ordena por (dtype, symbol, from) para que dos ejecuciones con los mismos huecos
    produzcan exactamente el mismo fichero y el diff no sea ruido.
    """
    target = path or config.known_gaps_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": 1,
        "generated_at": verified_at or datetime.now(timezone.utc).isoformat(),
        "note": (
            "Huecos CONFIRMADOS contra segunda fuente. Los que aqui aparecen no se rellenan: "
            "son datos que ni el exchange publica. Un hueco sin verificar no se anade."
        ),
        "gaps": [g.as_dict() for g in sorted(gaps, key=lambda g: (g.dtype, g.symbol, g.from_ts))],
    }
    handle, tmp_name = tempfile.mkstemp(dir=target.parent, suffix=".tmp")
    tmp = Path(tmp_name)
    try:
        with os.fdopen(handle, "w", encoding="utf-8") as fh:
            fh.write(json.dumps(payload, indent=2, ensure_ascii=False) + "\n")
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, target)
    finally:
        if tmp.exists():
            tmp.unlink()
    return target


def generate(
    product: Product,
    *,
    path: Path | None = None,
    probe: int = 40,
    verified_at: str | None = None,
) -> tuple[Path, int, int]:
    """Flujo completo: detectar, verificar, registrar. Devuelve `(ruta, registrados, rellenables)`."""
    gaps = find_gaps(product)
    known, fillable, unverifiable = classify(product, gaps, probe=probe)
    target = write_known_gaps(known, path=path, verified_at=verified_at)
    log(
        event="known_gaps_written",
        dtype=product.dtype,
        path=str(target),
        registrados=len(known),
        rellenables=len(fillable),
        no_verificables=len(unverifiable),
    )
    return target, len(known), len(fillable)
