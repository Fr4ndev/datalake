"""CLI del loader: carga puntual y backfill diario.

Subcomandos:

    backfill   carga un rango explicito (o "ayer" por defecto) para los dtypes del lake
    catchup    mete en la base todo lo que falte, anyo a anyo
    cagg       refresca las caggs en un rango

Por que `catchup` va por ficheros y no por rango de fechas: los ficheros yearly ya estan
particionados y son la unidad natural de idempotencia: recorrerlos en orden es lo unico que se
puede reanudar sin tener que saber cuanto se metio la ultima vez. El filtro por rango se aplica igualmente,
porque el daily mete solo un dia.

Por que el daily corre a las 00:05 y no a las 00:00: Binance Vision publica el dia anterior con
un retraso de unos minutos, y data.binance.vision todavia esta sirviendo el fichero del dia
anterior. A las 00:00 se cargaria un dia incompleto y habria que volver a cargarlo; a las 00:05 el
dia esta cerrado.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone

from loader.loader import Loader, log, lake_dir, parquet_files
from loader.targets import CAGG_BY_TARGET, TARGETS

#: dtypes que se cargan del lake. `trades` y `liquidations` no estan: el lake solo tiene klines,
#: funding y metrics (aggTrades quedo fuera de alcance en D20).
LAKE_DTYPES = ("klines", "funding", "metrics")


def _yesterday_utc(now=None):
    now = now or datetime.now(timezone.utc)
    hoy = now.replace(hour=0, minute=0, second=0, microsecond=0)
    return hoy - timedelta(days=1), hoy


def cmd_backfill(args) -> int:
    if args.since == "ayer":
        since, until = _yesterday_utc()
    else:
        since = datetime.fromisoformat(args.since).replace(tzinfo=timezone.utc)
        until = datetime.fromisoformat(args.until).replace(tzinfo=timezone.utc)
    log("backfill_begin", since=since.isoformat(), until=until.isoformat(),
        dtypes=",".join(args.dtypes), symbol=args.symbol)
    fallos = 0
    with Loader() as ld:
        for dtype in args.dtypes:
            res = ld.load(
                dtype=dtype,
                symbol=args.symbol,
                exchange=args.exchange,
                tf=None,
                since=since,
                until=until,
                refresh=not args.no_refresh,
            )
            if res.errors:
                fallos += 1
                for err in res.errors:
                    log("load_error", dtype=dtype, error=err)
    log("backfill_end", status="error" if fallos else "ok", failed_dtypes=fallos)
    return 1 if fallos else 0


def cmd_catchup(args) -> int:
    """Recorre todos los ficheros del lake, en orden, y los mete todos."""
    root = lake_dir()
    log("catchup_begin", lake=str(root), symbol=args.symbol, exchange=args.exchange)
    fallos = 0
    total = 0
    with Loader() as ld:
        for dtype in args.dtypes:
            target = TARGETS[dtype]
            for tfdir in sorted(
                p for p in (root / args.exchange / dtype / f"symbol={args.symbol}").glob("tf=*")
            ):
                for part in sorted(tfdir.glob("year=*/part.parquet")):
                    res = ld.load(
                        dtype=dtype,
                        symbol=args.symbol,
                        exchange=args.exchange,
                        tf=None,
                        since=args.since,
                        until=args.until,
                        files=[part],
                        refresh=False,
                    )
                    if res.errors:
                        fallos += 1
                        log("load_error", dtype=dtype, file=part.name, error=res.errors[0])
                    total += res.rows_inserted
            log("catchup_dtype_done", dtype=dtype, table=target.table, rows_inserted=total)
        if not args.no_refresh:
            for table, cagg in CAGG_BY_TARGET.items():
                if args.since and args.until:
                    try:
                        ld.refresh_cagg(cagg, args.since, args.until)
                    except Exception as exc:  # noqa: BLE001
                        log("cagg_error", cagg=cagg, error=str(exc)[:160])
                        fallos += 1
    log("catchup_end", status="error" if fallos else "ok", rows_inserted=total, failed=fallos)
    return 1 if fallos else 0


def cmd_cagg(args) -> int:
    since = datetime.fromisoformat(args.since).replace(tzinfo=timezone.utc)
    until = datetime.fromisoformat(args.until).replace(tzinfo=timezone.utc)
    with Loader() as ld:
        for cagg in args.caggs:
            ld.refresh_cagg(cagg, since, until)
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="loader", description="Carga lake -> TimescaleDB")
    sub = p.add_subparsers(dest="cmd", required=True)

    def common(sp):
        sp.add_argument("--symbol", default="BTCUSDT")
        sp.add_argument("--exchange", default="binance")
        sp.add_argument("--dtypes", nargs="+", default=list(LAKE_DTYPES), choices=sorted(TARGETS))
        sp.add_argument("--no-refresh", action="store_true", help="no refrescar caggs al terminar")

    b = sub.add_parser("backfill", help="carga un rango (por defecto, ayer)")
    common(b)
    b.add_argument("--since", default="ayer", help="'ayer' o ISO-8601")
    b.add_argument("--until", default="1970-01-01", help="ISO-8601 (excluido)")
    b.set_defaults(func=cmd_backfill)

    c = sub.add_parser("catchup", help="carga todo el lake, anyo a anyo")
    common(c)
    c.add_argument("--since", default=None)
    c.add_argument("--until", default=None)
    c.set_defaults(func=cmd_catchup)

    g = sub.add_parser("cagg", help="refresca caggs en un rango")
    g.add_argument("--caggs", nargs="+", default=["candles_1h", "funding_daily", "oi_5m"])
    g.add_argument("--since", required=True)
    g.add_argument("--until", required=True)
    g.set_defaults(func=cmd_cagg)
    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())