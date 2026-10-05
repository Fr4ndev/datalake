"""Gap-scan del lake: los checks 1, 2, 3 y 6 de la skill data-quality-validator.

Los checks 4 (continuidad de funding) y 5 (cross-exchange) no se implementan aqui: pertenecen a
Fase 3 y necesitan `funding` y al menos un segundo exchange en el lake.

Criterio de done del bulk (0 gaps NO EXPLICADOS):

- Un hueco que este en `lake/known_gaps.json` se reporta como **INFO**, no CRIT. Es el delta que
  pidio el usuario sobre la seccion 3.5 de la skill.
- Un hueco sin explicar es **CRIT**.
- La cola todavia no publicada por el exchange (el dia en curso y los ultimos dias que el exchange aun no ha subido) **no** es un hueco: se reporta como INFO `pending` con el rango exacto que
  falta, para no dejar un CRIT permanente por un retraso de publicacion conocido.

Salida: `lake/_qa/report-YYYY-MM-DD.json` (maquina) y `.md` (humano).
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb

from common import db

from . import config
from .logfmt import log
from .schemas import Product

SEV_CRIT = "CRIT"
SEV_WARN = "WARN"
SEV_INFO = "INFO"

CHECK_GAPS = "gaps"
CHECK_DUP = "duplicates"
CHECK_OUTLIERS = "outliers"
CHECK_COVERAGE = "coverage"

_CON: "duckdb.DuckDBPyConnection | None" = None


@dataclass
class Finding:
    check: str
    severity: str
    exchange: str
    symbol: str
    dtype: str
    tf: str
    detail: str
    from_ts: str | None = None
    to_ts: str | None = None
    rows: int | None = None

    def as_dict(self) -> dict[str, object]:
        return {k: v for k, v in asdict(self).items() if v is not None}


@dataclass
class Report:
    generated_at: str
    findings: list[Finding] = field(default_factory=list)
    stats: dict[str, object] = field(default_factory=dict)

    def add(self, finding: Finding) -> None:
        self.findings.append(finding)

    @property
    def crit(self) -> list[Finding]:
        return [f for f in self.findings if f.severity == SEV_CRIT]

    def counts(self) -> dict[str, int]:
        out = {SEV_CRIT: 0, SEV_WARN: 0, SEV_INFO: 0}
        for finding in self.findings:
            out[finding.severity] = out.get(finding.severity, 0) + 1
        return out


def load_known_gaps() -> list[dict[str, object]]:
    """Rangos de hueco confirmados en origen (verificados contra segunda fuente)."""
    path = config.known_gaps_path()
    if not path.exists():
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    return data.get("gaps", data if isinstance(data, list) else [])


def _known_gap_hit(product: Product, start: datetime, end: datetime) -> dict | None:
    """Primer hueco conocido que solape con [start, end] para este producto."""
    for gap in load_known_gaps():
        if gap.get("symbol") not in (None, product.symbol):
            continue
        if gap.get("dtype") not in (None, product.dtype):
            continue
        # Se aceptan las dos grafias: `from`/`to` es lo que escribe el generador y lo que
        # escribiria una persona, pero `from` es palabra reservada en Python, asi que el
        # dataclass usa `from_ts`/`to_ts`. Sin esta tolerancia el hueco se leeria pero no se
        # encontraria y seguiria saliendo como CRIT.
        raw_from = gap.get("from") or gap.get("from_ts")
        raw_to = gap.get("to") or gap.get("to_ts")
        if not raw_from or not raw_to:
            continue
        g_from = datetime.fromisoformat(str(raw_from).replace("Z", "+00:00"))
        g_to = datetime.fromisoformat(str(raw_to).replace("Z", "+00:00"))
        if g_from <= end and start <= g_to:
            return gap
    return None


def last_published_day() -> datetime:
    """Ultimo dia completo que el exchange ya publica (hoy a las 00:00 UTCexclusive).

    Los ficheros diarios suben con ~1 dia de retraso, asi que se toma ayer. Es el limite hasta el
    que el gap-scan exige continuidad; a partir de ahi se marca `pending`.
    """
    now = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    return now - timedelta(days=1)


def _iter_partitions(product: Product) -> list[Path]:
    root = config.lake_dir() / config.EXCHANGE_BINANCE / product.lake_dirname
    base = root / f"symbol={product.symbol}" / f"tf={product.tf}"
    return sorted(base.glob("year=*/part.parquet"))


def check_coverage(
    product: Product,
    report: Report,
    *,
    expected_start: datetime | None,
    horizon: datetime | None = None,
) -> tuple[list[Path], int]:
    """Check 6: rejilla symbol x year x tf frente a lo presente en disco.

    Distingue tres cosas que antes se confundian:

    - un ano **anterior** al listing del simbolo: WARN, es esperado;
    - un ano **intermedio** sin particion: CRIT, son datos que no estan;
    - el ano en curso: se evalua con el hueco final, no como ano incompleto.
    """
    paths = _iter_partitions(product)
    present = {int(path.parent.name.split("=", 1)[1]) for path in paths}

    if expected_start is not None:
        for year in sorted(y for y in present if y < expected_start.year):
            report.add(
                Finding(
                    check=CHECK_COVERAGE,
                    severity=SEV_WARN,
                    exchange=config.EXCHANGE_BINANCE,
                    symbol=product.symbol,
                    dtype=product.dtype,
                    tf=product.tf,
                    detail=f"anio {year} anterior al inicio del exchange ({expected_start.date()})",
                )
            )
        last = (horizon or last_published_day()).year
        for year in range(expected_start.year, last + 1):
            if year not in present:
                report.add(
                    Finding(
                        check=CHECK_COVERAGE,
                        severity=SEV_CRIT,
                        exchange=config.EXCHANGE_BINANCE,
                        symbol=product.symbol,
                        dtype=product.dtype,
                        tf=product.tf,
                        detail=f"anio {year} sin particion: expected={year} presente=False",
                    )
                )
    return paths, len(paths)


def connection() -> "duckdb.DuckDBPyConnection":
    """Conexion DuckDB en memoria, reutilizada entre particiones.

    Los checks de datos van en SQL sobre el parquet (no se materializa la tabla en Python):
    `lag()` para los huecos y `count(DISTINCT ...)` para duplicados son justo el caso para el que
    la skill pide DuckDB. Abrir una conexion por particion tiene coste, asi que se cachea.
    """
    global _CON
    if _CON is None:
        # UTC fijo por sesion (regla 3.bis): `year()`/`date_trunc()` sobre TIMESTAMPTZ usan la zona
        # local si no, y desplazan filas entre anos sin que cambie el total.
        _CON = db.duckdb_utc()
    return _CON


def reset_connection() -> None:
    """Cierra la conexion cacheada. Lo usan los tests para no arrastrar estado entre casos."""
    global _CON
    if _CON is not None:
        _CON.close()
        _CON = None


def parquet_columns(path: Path) -> list[str]:
    """Columnas reales del parquet, leidas del propio fichero."""
    return [
        row[0]
        for row in connection()
        .execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(path)])
        .fetchall()
    ]


def check_gaps(path: Path, product: Product, report: Report, *, horizon: datetime) -> int:
    """Check 1: huecos con `lag(ts)` en SQL, excluyendo known_gaps.json (que son INFO)."""
    step = config.TIMEFRAME_SECONDS.get(product.tf)
    if step is None:
        return 0
    time_col = product.schema.time_column
    if time_col not in parquet_columns(path):
        return 0

    # `DISTINCT` antes del `lag`: si un timestamp estuviera repetido, la ventana lo veria dos
    # veces y un delta 0 no es un hueco. El check 2 mide los duplicados por separado.
    #
    # El margen (`gap_tolerance_seconds`) absorbe el jitter de `fundingRate`: sin el, cada
    # intervalo de 8h con +9 ms se contaria como hueco.
    step = step + product.schema.gap_tolerance_seconds
    rows = connection().execute(
        f"""
        WITH distinct_ts AS (
            SELECT DISTINCT "{time_col}" AS ts FROM read_parquet(?)
        ),
        windowed AS (
            SELECT
                ts,
                lag(ts) OVER (ORDER BY ts) AS prev,
                lag(epoch(ts)) OVER (ORDER BY ts) AS prev_epoch
            FROM distinct_ts
        )
        SELECT prev, ts, epoch(ts) - prev_epoch AS delta
        FROM windowed
        WHERE prev IS NOT NULL AND epoch(ts) - prev_epoch > ?
        ORDER BY prev
        """,
        [str(path), step],
    ).fetchall()

    gaps = 0
    for prev, cur, delta in rows:
        # El hueco va de prev+step hasta cur-step, ambos extremos presentes.
        gap_from = prev + timedelta(seconds=step)
        gap_to = cur - timedelta(seconds=step)
        # Velas ausentes en un rango inclusivo: (gap_to - gap_from)/step + 1.
        missing = int((gap_to - gap_from).total_seconds() // step) + 1
        known = _known_gap_hit(product, gap_from, gap_to)
        if known is not None:
            report.add(
                Finding(
                    check=CHECK_GAPS,
                    severity=SEV_INFO,
                    exchange=config.EXCHANGE_BINANCE,
                    symbol=product.symbol,
                    dtype=product.dtype,
                    tf=product.tf,
                    detail=f"hueco conocido ({known.get('reason', 'sin motivo')}): {missing} velas",
                    from_ts=gap_from.isoformat(),
                    to_ts=gap_to.isoformat(),
                    rows=missing,
                )
            )
        elif cur <= horizon:
            report.add(
                Finding(
                    check=CHECK_GAPS,
                    severity=SEV_CRIT,
                    exchange=config.EXCHANGE_BINANCE,
                    symbol=product.symbol,
                    dtype=product.dtype,
                    tf=product.tf,
                    detail=f"huecos sin explicar: {missing} velas ({int(delta) - 1} faltantes)",
                    from_ts=gap_from.isoformat(),
                    to_ts=gap_to.isoformat(),
                    rows=missing,
                )
            )
            gaps += 1
    return gaps


def check_duplicates(path: Path, product: Product, report: Report) -> int:
    """Check 2: count(*) vs count(DISTINCT (symbol, ts))."""
    time_col = product.schema.time_column
    n_rows, n_distinct = connection().execute(
        f"""
        WITH t AS (SELECT symbol, "{time_col}" AS ts FROM read_parquet(?))
        SELECT
            (SELECT count(*) FROM t),
            (SELECT count(*) FROM (SELECT DISTINCT symbol, ts FROM t))
        """,
        [str(path)],
    ).fetchone()

    if n_rows == n_distinct:
        return 0
    report.add(
        Finding(
            check=CHECK_DUP,
            severity=SEV_CRIT,
            exchange=config.EXCHANGE_BINANCE,
            symbol=product.symbol,
            dtype=product.dtype,
            tf=product.tf,
            detail=f"duplicados: {n_rows} filas vs {n_distinct} distintas",
            rows=n_rows - n_distinct,
        )
    )
    return n_rows - n_distinct


def check_outliers(path: Path, product: Product, report: Report) -> int:
    """Check 3: high < low, precios <= 0, volumen < 0.

    El SQL se arma con las columnas que el producto tenga de verdad: `fundingRate` y `metrics` no
    tienen OHLCV, y consultarlas daria un error de columna inexistente.
    """
    present = set(parquet_columns(path))
    price_cols = [c for c in ("open", "high", "low", "close") if c in present]
    if not price_cols and "volume" not in present and "balance" not in present:
        return 0

    predicates: list[str] = []
    labels: list[str] = []
    if {"high", "low"} <= present:
        predicates.append("high < low")
        labels.append("high<low")
    if price_cols:
        predicates.append(f"{' OR '.join(f'{c} <= 0' for c in price_cols)}")
        labels.append("precios<=0")
    if "volume" in present:
        predicates.append("volume < 0")
        labels.append("volume<0")
    if "balance" in present:
        predicates.append("balance < 0")
        labels.append("balance<0")

    counts = connection().execute(
        "SELECT "
        + ", ".join(f"count(*) FILTER (WHERE {p})" for p in predicates)
        + " FROM read_parquet(?)",
        [str(path)],
    ).fetchone()

    found = 0
    for label, n_bad in zip(labels, counts):
        if n_bad:
            found += n_bad
            report.add(
                Finding(
                    check=CHECK_OUTLIERS,
                    severity=SEV_CRIT,
                    exchange=config.EXCHANGE_BINANCE,
                    symbol=product.symbol,
                    dtype=product.dtype,
                    tf=product.tf,
                    detail=f"{label}: {n_bad} filas en {path.parent.name}",
                    rows=n_bad,
                )
            )
    return found


def row_count(path: Path) -> int:
    """Filas de la particion, contadas por DuckDB sin materializar la tabla."""
    return connection().execute("SELECT count(*) FROM read_parquet(?)", [str(path)]).fetchone()[0]


def check_pending(
    path: Path,
    product: Product,
    report: Report,
    *,
    horizon: datetime,
    step: int,
    partition_year: int,
) -> None:
    """Lo que falta entre el ultimo dato y el horizonte de publicacion: INFO, no CRIT.

    Solo se evalua la particion del ano en curso. En un ano ya cerrado el historico acaba el 31
    de diciembre por definicion, asi que comparar su ultimo dato contra el horizonte de hoy
    produciria un "pendiente" enorme y falso en cada ano anterior: el check 1 ya cubre el hueco
    final de la particion en curso.
    """
    if partition_year != horizon.year:
        return
    time_col = product.schema.time_column
    if time_col not in parquet_columns(path):
        return

    last = connection().execute(
        f'SELECT max("{time_col}") FROM read_parquet(?)', [str(path)]
    ).fetchone()[0]
    if last is None or last >= horizon:
        return

    missing = int((horizon - last).total_seconds() // step)
    report.add(
        Finding(
            check=CHECK_GAPS,
            severity=SEV_INFO,
            exchange=config.EXCHANGE_BINANCE,
            symbol=product.symbol,
            dtype=product.dtype,
            tf=product.tf,
            detail=(
                f"pendiente de publicacion en origen: {missing} velas hasta "
                f"{horizon.date()} (no cuenta como hueco)"
            ),
            from_ts=(last + timedelta(seconds=step)).isoformat(),
            to_ts=horizon.isoformat(),
            rows=missing,
        )
    )


def scan(product: Product, *, expected_start: datetime | None = None) -> Report:
    """Ejecuta los checks del lake para un producto y devuelve el reporte."""
    report = Report(generated_at=datetime.now(timezone.utc).isoformat())
    horizon = last_published_day()
    step = config.TIMEFRAME_SECONDS.get(product.tf)

    paths, n_paths = check_coverage(product, report, expected_start=expected_start, horizon=horizon)
    total_rows = 0
    total_gaps = 0
    total_dups = 0
    total_outliers = 0

    for path in paths:
        year = int(path.parent.name.split("=", 1)[1])
        total_rows += row_count(path)
        total_gaps += check_gaps(path, product, report, horizon=horizon)
        total_dups += check_duplicates(path, product, report)
        total_outliers += check_outliers(path, product, report)
        if step:
            check_pending(path, product, report, horizon=horizon, step=step, partition_year=year)

    report.stats = {
        "partitions": n_paths,
        "rows": total_rows,
        "gaps_unexplained": total_gaps,
        "duplicates": total_dups,
        "outliers": total_outliers,
        "horizon": horizon.isoformat(),
        "known_gaps_loaded": len(load_known_gaps()),
    }
    log(
        event="gapscan",
        exchange=config.EXCHANGE_BINANCE,
        symbol=product.symbol,
        dtype=product.dtype,
        tf=product.tf,
        partitions=n_paths,
        rows=total_rows,
        gaps_unexplained=total_gaps,
        duplicates=total_dups,
        outliers=total_outliers,
    )
    return report


def write_report(
    report: Report, *, outdir: Path | None = None, product: Product | None = None
) -> tuple[Path, Path]:
    """Escribe el reporte en JSON (maquina) y Markdown (humano).

    El nombre incluye producto: `report-YYYY-MM-DD-klines-BTCUSDT-1m.json`. Sin el sufijo, los
    gap-scan de klines, fundingRate y metrics del mismo dia se pisan entre si y solo queda el
    ultimo, que es justo el que no interesa.
    """
    directory = outdir or config.qa_dir()
    directory.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    suffix = f"-{product.dtype}-{product.symbol}-{product.tf}" if product is not None else ""
    json_path = directory / f"report-{stamp}{suffix}.json"
    md_path = directory / f"report-{stamp}{suffix}.md"

    payload = {
        "generated_at": report.generated_at,
        "stats": report.stats,
        "counts": report.counts(),
        "findings": [f.as_dict() for f in report.findings],
    }
    json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    counts = report.counts()
    title = f"{product.dtype}/{product.symbol}/{product.tf}" if product is not None else "lake"
    lines = [
        f"# Reporte de calidad {stamp} - {title}",
        "",
        f"Generado: {report.generated_at}",
        "",
        f"- CRIT: **{counts[SEV_CRIT]}**",
        f"- WARN: **{counts[SEV_WARN]}**",
        f"- INFO: {counts[SEV_INFO]}",
        "",
        "## Estadisticas",
        "",
    ]
    for key, value in report.stats.items():
        lines.append(f"- `{key}`: {value}")
    lines += ["", "## Hallazgos", ""]
    if not report.findings:
        lines.append("Sin hallazgos.")
    for severity in (SEV_CRIT, SEV_WARN, SEV_INFO):
        for finding in [f for f in report.findings if f.severity == severity]:
            rng = ""
            if finding.from_ts and finding.to_ts:
                rng = f" `{finding.from_ts}` -> `{finding.to_ts}`"
            lines.append(
                f"- **{severity}** [{finding.check}] {finding.symbol}/{finding.dtype}/{finding.tf}{rng}: "
                f"{finding.detail}"
            )
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return json_path, md_path
