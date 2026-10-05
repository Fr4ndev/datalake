#!/usr/bin/env python3
"""Proceso stub para los servicios que aun no tienen implementacion (Fase 0).

No es un no-op silencioso: emite un heartbeat en formato clave=valor (regla 9)
para que `docker-compose logs` demuestre que el servicio esta vivo, y gestiona
SIGTERM para que `docker-compose down` no espere al timeout de 10s.

Los servicios de larga vida (feed-daemon, bot, validator) se quedan en el bucle
de heartbeat. Los de tipo batch (bulk, loader) son de una sola ejecucion, asi que
usan `--once` y salen con codigo 0 en lugar de colgarse para siempre.

Uso:
    python ops/stub.py <nombre_servicio> [--interval 30] [--once]
"""

from __future__ import annotations

import argparse
import signal
import sys
import time

_running = True


def _stop(signum: int, _frame: object) -> None:
    global _running
    print(f"component=stub event=signal signal={signal.Signals(signum).name}", flush=True)
    _running = False


def log(**fields: object) -> None:
    print(" ".join(f"{k}={v}" for k, v in fields.items()), flush=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("name")
    parser.add_argument("--interval", type=int, default=30)
    parser.add_argument(
        "--once",
        action="store_true",
        help="servicio batch de una sola ejecucion: informa y sale con codigo 0",
    )
    args = parser.parse_args(argv)

    if args.once:
        log(
            component=args.name,
            event="not_implemented",
            status="stub",
            phase="fase0",
            note="sin implementacion en Fase 0; el servicio real llega en la fase siguiente",
        )
        return 0

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    log(
        component=args.name,
        event="start",
        status="stub",
        pid=__import__("os").getpid(),
        python=sys.version.split()[0],
        interval=args.interval,
    )
    tick = 0
    while _running:
        tick += 1
        log(component=args.name, event="heartbeat", status="stub", tick=tick)
        deadline = time.monotonic() + args.interval
        while _running and time.monotonic() < deadline:
            time.sleep(0.5)

    log(component=args.name, event="stop", status="stub", ticks=tick)
    return 0


if __name__ == "__main__":
    sys.exit(main())