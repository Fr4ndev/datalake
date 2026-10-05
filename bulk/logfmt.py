"""Logging en formato clave=valor a stdout (regla 9 de AGENTS.md).

Todos los procesos del stack escriben en stdout con este formato para que `docker-compose logs`
sea grepeable: `exchange=binance symbol=BTCUSDT dtype=klines rows=525600 elapsed=41.2`.
"""

from __future__ import annotations

import sys
import time


def log(**fields: object) -> None:
    """Imprime una linea clave=valor. Sin valores con espacios ni comillas."""
    parts = []
    for key, value in fields.items():
        if value is None:
            continue
        text = str(value)
        if text == "" or any(ch.isspace() for ch in text):
            text = repr(text)
        parts.append(f"{key}={text}")
    print(" ".join(parts), file=sys.stdout, flush=True)


class Timer:
    """Mide `elapsed` en segundos para las lineas de resumen."""

    def __init__(self) -> None:
        self._start = time.monotonic()
        self.elapsed = 0.0

    def __enter__(self) -> Timer:
        self._start = time.monotonic()
        return self

    def __exit__(self, *_exc: object) -> None:
        self.elapsed = round(time.monotonic() - self._start, 2)

    @property
    def now(self) -> float:
        return time.monotonic() - self._start
