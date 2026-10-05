"""CLI del bulk: `python -m bulk <comando>`.

    python -m bulk download --dtype klines --symbol BTCUSDT --tf 1m [--dry-run]
    python -m bulk gap-scan --dtype klines --symbol BTCUSDT --tf 1m
    python -m bulk plan --dtype klines --symbol BTCUSDT --tf 1m
    python -m bulk known-gaps --dtype metrics --symbol BTCUSDT

`download` es idempotente: lo que ya esta en el manifest como `done` se salta.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone

from . import config, download, gapscan, known_gaps, manifest
from .logfmt import log
from .schemas import LAKE_DIRNAME, SCHEMAS, Product


def _product(args: argparse.Namespace) -> Product:
    if args.dtype not in SCHEMAS:
        raise SystemExit(
            f"dtype desconocido {args.dtype!r}; opciones: {', '.join(sorted(SCHEMAS))}"
        )
    schema = SCHEMAS[args.dtype]
    tf = args.tf or schema.tf_default
    if args.dtype == "klines" and tf not in config.TIMEFRAME_SECONDS:
        raise SystemExit(
            f"tf {tf!r} no soportado para gap-scan; opciones: {', '.join(sorted(config.TIMEFRAME_SECONDS))}"
        )
    return Product(dtype=args.dtype, symbol=args.symbol, tf=tf)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="bulk", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--dtype", default="klines", help="klines|fundingRate|metrics|aggTrades")
    common.add_argument("--symbol", default="BTCUSDT")
    common.add_argument("--tf", default=None, help="timeframe; por defecto el del producto")

    dl = sub.add_parser("download", parents=[common], help="descarga a Parquet (idempotente)")
    dl.add_argument("--start", default=None, help="YYYY-MM o YYYY-MM-DD, inclusivo")
    dl.add_argument("--end", default=None, help="YYYY-MM o YYYY-MM-DD, inclusivo")
    dl.add_argument(
        "--granularity",
        default=None,
        choices=["monthly", "daily"],
        help="fuerza una granularidad; por defecto usa la disponible (monthly + diario de cola)",
    )
    dl.add_argument("--workers", type=int, default=config.DEFAULT_WORKERS)
    dl.add_argument("--retries", type=int, default=config.DEFAULT_RETRIES)
    dl.add_argument("--limit", type=int, default=None, help="corta el lote tras N periodos")
    dl.add_argument("--dry-run", action="store_true", help="lista el plan y no descarga")
    dl.add_argument(
        "--no-rest-fill",
        action="store_true",
        help="no rellena por REST el mes en curso de los productos que lo necesitan (fundingRate)",
    )

    sub.add_parser("plan", parents=[common], help="solo muestra el plan de periodos")

    scan = sub.add_parser("gap-scan", parents=[common], help="checks de calidad sobre el lake")
    scan.add_argument("--start", default=None, help="YYYY-MM-DD, inicio esperado del exchange")
    scan.add_argument(
        "--fail-on-crit",
        action="store_true",
        help="sale con codigo 1 si hay CRIT (por defecto sale 0 y solo informa)",
    )

    kg = sub.add_parser(
        "known-gaps",
        parents=[common],
        help="detecta huecos, los verifica contra REST y escribe lake/known_gaps.json",
    )
    kg.add_argument("--probe", type=int, default=40, help="cuantos huecos se comprueban contra REST")
    kg.add_argument(
        "--verify-only",
        action="store_true",
        help="no escribe el archivo: solo informa de cuantos huecos hay y su veredicto",
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    product = _product(args)

    if args.command == "plan":
        periods, unavailable = download.plan_periods(
            product, start=getattr(args, "start", None), end=getattr(args, "end", None)
        )
        for period in periods:
            log(
                event="planned",
                period=period.period_id,
                file=period.name,
                bytes=period.size,
                symbol=product.symbol,
                dtype=product.dtype,
            )
        for _period, reason in unavailable:
            log(event="unavailable", dtype=product.dtype, note=reason)
        log(
            event="plan_total",
            symbol=product.symbol,
            dtype=product.dtype,
            periods=len(periods),
            bytes=sum(p.size for p in periods),
        )
        return 0

    if args.command == "download":
        return download.run(
            product,
            start=args.start,
            end=args.end,
            granularity=args.granularity,
            workers=args.workers,
            retries=args.retries,
            limit=args.limit,
            dry_run=args.dry_run,
            rest_fill=not args.no_rest_fill,
        )

    if args.command == "gap-scan":
        expected_start = None
        if args.start:
            expected_start = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
        report = gapscan.scan(product, expected_start=expected_start)
        json_path, md_path = gapscan.write_report(report, product=product)
        counts = report.counts()
        log(
            event="report",
            json=str(json_path),
            md=str(md_path),
            crit=counts[gapscan.SEV_CRIT],
            warn=counts[gapscan.SEV_WARN],
            info=counts[gapscan.SEV_INFO],
            gaps_unexplained=report.stats.get("gaps_unexplained"),
        )
        if args.fail_on_crit and counts[gapscan.SEV_CRIT]:
            return 1
        return 0

    if args.command == "known-gaps":
        gaps = known_gaps.find_gaps(product)
        registrables, rellenables, unverifiable = known_gaps.classify(
            product, gaps, probe=args.probe
        )
        if args.verify_only:
            log(
                event="known_gaps_verify_only",
                dtype=product.dtype,
                huecos=len(gaps),
                registrables=len(registrables),
                rellenables=len(rellenables),
                no_verificables=len(unverifiable),
            )
            return 0
        path = known_gaps.write_known_gaps(registrables)
        log(
            event="known_gaps_written",
            path=str(path),
            dtype=product.dtype,
            huecos=len(gaps),
            registrados=len(registrables),
            rellenables=len(rellenables),
            no_verificables=len(unverifiable),
        )
        return 0

    raise SystemExit(f"comando desconocido {args.command!r}")


if __name__ == "__main__":
    sys.exit(main())
