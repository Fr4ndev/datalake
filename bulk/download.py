"""Descarga y parseo de periodos de Binance Vision a Parquet.

Flujo por periodo (reglas 1, 2, 3, 5 y 7 de la skill bulk-parquet-downloader):

1. Si el manifest ya lo tiene en `done`, se salta (idempotencia).
2. GET del `.zip` con reintentos y backoff exponencial con jitter.
3. GET del `.CHECKSUM` y comparacion sha256 **antes** de descomprimir. Si no cuadra, se reintenta
   el fichero entero hasta 5 veces y nunca se convierte el contenido corrupto.
4. Parseo a tabla con tiempos en UTC.
5. Escritura por anos con dedup y rename atomico.
6. Linea en el manifest con el estado final.

Un fichero fallido **nunca** aborta el lote: se registra como `failed` con su nota y el lote
sigue, que es lo que permite descargar 81 periodos con workers sin que uno tumbe el resto.
"""

from __future__ import annotations

import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone

import pyarrow as pa

from . import config, manifest, rest, s3list, writer
from .logfmt import Timer, log
from .parser import ParseError, parse_payload
from .schemas import Product, prefix_for
from .s3list import Period, S3Error


# Productos cuyo origen necesita REST ademas de los .zip de Vision (D19).
# `metrics` NO entra: sus diarios si existen. El exchange se deja un slot de 5m fuera de cada
# zip diario, pero eso es un hueco del ARCHIVO, no del exchange, asi que se documenta en
# known_gaps.json en vez de rellenarse aqui.
REST_FILL = {"fundingRate"}


@dataclass
class PeriodResult:
    period: Period
    status: str
    rows: int | None = None  # filas aportadas por este fichero
    partition_rows: int | None = None  # filas totales de la particion tras fusionar
    sha256: str | None = None
    note: str | None = None
    seconds: float = 0.0


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def process_period(
    product: Product, period: Period, *, retries: int = config.DEFAULT_RETRIES
) -> PeriodResult:
    """Descarga, verifica, parsea y escribe un periodo. Nunca lanza excepcion."""
    with Timer() as timer:
        last_note = ""
        for attempt in range(1, retries + 1):
            try:
                payload = s3list.fetch_with_retry(period.key, retries=retries)
                expected = s3list.fetch_checksum(period.key)
                actual = _sha256(payload)
                if actual != expected:
                    # Nunca se convierte un fichero corrupto: se reintenta la descarga entera.
                    last_note = f"checksum_mismatch esperado={expected} obtenido={actual}"
                    log(
                        event="checksum",
                        status="mismatch",
                        file=period.name,
                        attempt=attempt,
                        retries=retries,
                    )
                    continue

                table = parse_payload(payload, product.schema)
                if table.num_rows == 0:
                    return PeriodResult(
                        period=period,
                        status=manifest.STATUS_SKIPPED,
                        note="csv_vacio",
                        sha256=actual,
                        seconds=timer.now,
                    )

                written = writer.write(table, product)
                partition_rows = sum(total for _path, total in written.values())
                return PeriodResult(
                    period=period,
                    status=manifest.STATUS_DONE,
                    # `rows` es lo que aporto ESTE fichero, no el total de la particion: si no,
                    # al acumular los meses de un mismo ano el manifest daria filas duplicadas y el
                    # contraste "esperadas vs obtenidas" por ano no cuadra.
                    rows=table.num_rows,
                    sha256=actual,
                    partition_rows=partition_rows,
                    seconds=timer.now,
                )
            except (S3Error, ParseError, KeyError, ValueError) as exc:
                last_note = f"{type(exc).__name__}: {exc}"
                log(event="retry", file=period.name, attempt=attempt, retries=retries, error=last_note)

        return PeriodResult(
            period=period,
            status=manifest.STATUS_FAILED,
            note=last_note or "sin detalle",
            seconds=timer.now,
        )


def plan_periods(
    product: Product,
    *,
    start: str | None = None,
    end: str | None = None,
    granularity: str | None = None,
) -> tuple[list[Period], list[tuple[Period, str]]]:
    """Decide que periodos descargar y devuelve `(a_descargar, no_disponibles)`.

    Politica (D16): `monthly` para meses cerrados y `daily` unicamente para lo que el mensual no
    cubre, que son el dia mas antiguo (2019-12-31, anterior al primer mensual) y el mes en curso.
    Se listan ambos prefijos de verdad; si un producto no tiene una granularidad (verificado:
    `fundingRate` no tiene diarios y `metrics` no tiene mensuales), simplemente se omite sin error.
    """
    available = product.schema.granularity_available
    wanted = [granularity] if granularity else list(available)

    monthly: list[Period] = []
    daily: list[Period] = []
    for gran in wanted:
        if gran not in available:
            continue
        periods = s3list.list_periods(product, gran, vision_root=config.VISION_ROOT)
        (monthly if gran == "monthly" else daily).extend(periods)

    closed_months = [p for p in monthly]
    monthly_dates = {p.date for p in closed_months}

    # Dias del mes en curso: los diarios cuya fecha no esta cubierta por ningun mensual.
    current = [p for p in daily if p.date[:7] not in monthly_dates]

    selected: list[Period] = []
    for period in [*closed_months, *current]:
        if start and period.date < start:
            continue
        if end and period.date > end:
            continue
        selected.append(period)

    unavailable: list[tuple[Period, str]] = []
    for gran in available:
        if gran in wanted:
            continue
        unavailable.append(
            (
                Period(key=prefix_for(product, gran, config.VISION_ROOT), granularity=gran, date="", size=0),
                f"{gran}_no_existe_para_{product.symbol}",
            )
        )

    return sorted(selected, key=lambda p: p.date), unavailable


def fetch_funding_current_month(product: Product) -> PeriodResult | None:
    """Rellena por REST el mes en curso de `fundingRate` (D19).

    `fundingRate` no publica ficheros diarios, asi que el mes en curso solo se puede completar
    por `/fapi/v1/fundingRate`. Se registra en el manifest como un periodo mas, con `file` = la
    ruta del endpoint y `note` explicando el origen, para que quede claro que esos registros no
    vienen de un .zip y por tanto no tienen sha256 de fichero.

    Es idempotente por construccion: la escritura deduplica por `(symbol, ts)`, asi que volver a
    pedir el mes solo anade las filas que falten.
    """
    now = datetime.now(timezone.utc)
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if start > now:
        return None

    with Timer() as timer:
        try:
            records = rest.fetch_funding(
                product.symbol, start, now, interval_hours=product.schema.funding_interval_hours
            )
        except rest.RestError as exc:
            log(event="rest", dtype=product.dtype, status=manifest.STATUS_FAILED, note=str(exc))
            return None

        rest.log_fetch(product.dtype, product.symbol, rest.FUNDING_PATH, len(records), timer.now)
        if not records:
            return None

        table = rest.funding_to_table(
            records, product.symbol, interval_hours=product.schema.funding_interval_hours
        )
        writer.write(table, product)
        manifest.append(
            manifest.Entry(
                exchange=config.EXCHANGE_BINANCE,
                symbol=product.symbol,
                dtype=product.dtype,
                period=f"rest:{now:%Y-%m}",
                file=rest.FUNDING_PATH,
                rows=table.num_rows,
                status=manifest.STATUS_DONE,
                note="mes en curso rellenado por REST; sin sha256 de fichero",
            )
        )
        return PeriodResult(
            period=Period(
                key=rest.FUNDING_PATH, granularity="rest", date=f"{now:%Y-%m}", size=0
            ),
            status=manifest.STATUS_DONE,
            rows=table.num_rows,
            seconds=timer.now,
        )


def run(
    product: Product,
    *,
    start: str | None = None,
    end: str | None = None,
    granularity: str | None = None,
    workers: int = config.DEFAULT_WORKERS,
    retries: int = config.DEFAULT_RETRIES,
    limit: int | None = None,
    dry_run: bool = False,
    rest_fill: bool = True,
) -> int:
    """Descarga el producto completo. Devuelve el codigo de salida del proceso."""
    with Timer() as timer:
        periods, unavailable = plan_periods(product, start=start, end=end, granularity=granularity)
        log(
            event="plan",
            exchange=config.EXCHANGE_BINANCE,
            symbol=product.symbol,
            dtype=product.dtype,
            tf=product.tf,
            periods=len(periods),
            first=periods[0].date if periods else None,
            last=periods[-1].date if periods else None,
            bytes=sum(p.size for p in periods),
        )

        for period, reason in unavailable:
            log(
                event="unavailable",
                exchange=config.EXCHANGE_BINANCE,
                symbol=product.symbol,
                dtype=product.dtype,
                status=manifest.STATUS_UNAVAILABLE,
                note=reason,
            )

        if dry_run:
            for period in periods:
                log(event="dry_run", file=period.key, bytes=period.size)
            log(
                event="done",
                exchange=config.EXCHANGE_BINANCE,
                symbol=product.symbol,
                dtype=product.dtype,
                mode="dry_run",
                periods=len(periods),
                elapsed=timer.now,
            )
            return 0

        done = manifest.done_keys()
        pending = [
            p
            for p in periods
            if (config.EXCHANGE_BINANCE, product.symbol, product.dtype, p.period_id) not in done
        ]
        skipped = len(periods) - len(pending)
        if skipped:
            log(event="resume", skipped=skipped, pending=len(pending))
        if limit is not None:
            pending = pending[:limit]

        results: list[PeriodResult] = []
        rows_total = 0
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(process_period, product, period, retries=retries): period for period in pending
            }
            for future in as_completed(futures):
                period = futures[future]
                result = future.result()
                results.append(result)
                rows_total += result.rows or 0
                manifest.append(
                    manifest.Entry(
                        exchange=config.EXCHANGE_BINANCE,
                        symbol=product.symbol,
                        dtype=product.dtype,
                        period=result.period.period_id,
                        file=result.period.key,
                        sha256=result.sha256,
                        rows=result.rows,
                        bytes=result.period.size,
                        status=result.status,
                        note=result.note,
                    )
                )
                log(
                    event="period",
                    exchange=config.EXCHANGE_BINANCE,
                    symbol=product.symbol,
                    dtype=product.dtype,
                    period=result.period.period_id,
                    status=result.status,
                    rows=result.rows,
                    elapsed=result.seconds,
                    note=result.note,
                )

        # El mes en curso de `fundingRate` no existe en Vision: se despues de los ficheros.
        rest_result: PeriodResult | None = None
        if rest_fill and product.dtype in REST_FILL:
            rest_result = fetch_funding_current_month(product)
            if rest_result is not None:
                results.append(rest_result)
                rows_total += rest_result.rows or 0

        failed = [r for r in results if r.status == manifest.STATUS_FAILED]
        log(
            event="done",
            exchange=config.EXCHANGE_BINANCE,
            symbol=product.symbol,
            dtype=product.dtype,
            tf=product.tf,
            periods=len(results),
            failed=len(failed),
            rows=rows_total,
            rest=bool(rest_result),
            elapsed=timer.now,
        )
        for result in failed:
            log(
                event="failed",
                dtype=product.dtype,
                period=result.period.period_id,
                note=result.note,
            )
        # Un fallo puntual no es un fallo del lote: se sale 0 para que el re-ejecute de la
        # idempotencia pueda reintentar solo esos periodos.
        return 0
