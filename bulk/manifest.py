"""Manifest del lake: una linea JSON por periodo descargado.

Es el mecanismo de idempotencia (regla 7): re-ejecutar el bulk lee el manifest y se salta todo
lo que ya esta en `status=done`, sin volver a descargar ni volver a escribir.

Se escribe en modo append y una linea por evento, con `flush`, para que un SIGKILL no pierda lo
que ya se habia descargado. Al releer solo se qua el ultimo estado de cada clave
`(exchange, symbol, dtype, period)`, de modo que un periodo puede pasar de `failed` a `done`.
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Iterable

from . import config
from .logfmt import log

STATUS_DONE = "done"
STATUS_FAILED = "failed"
STATUS_SKIPPED = "skipped"
STATUS_UNAVAILABLE = "unavailable"


@dataclass
class Entry:
    """Una linea del manifest. Los campos son los que fija la skill, mas `ts` y `bytes`."""

    exchange: str
    symbol: str
    dtype: str
    period: str
    file: str
    sha256: str | None = None
    rows: int | None = None
    status: str = STATUS_DONE
    note: str | None = None
    bytes: int | None = None
    ts: str | None = None

    @property
    def key(self) -> tuple[str, str, str, str]:
        return (self.exchange, self.symbol, self.dtype, self.period)


def append(entry: Entry) -> None:
    """Anade una linea al manifest y hace flush immediately."""
    if entry.ts is None:
        entry.ts = datetime.now(timezone.utc).isoformat()
    path = config.manifest_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(asdict(entry), sort_keys=True, ensure_ascii=False) + "\n")
        fh.flush()
        os.fsync(fh.fileno())


def read_all() -> list[Entry]:
    """Lee el manifest tolerando lineas corruptas al final (escritura interrumpida)."""
    path = config.manifest_path()
    if not path.exists():
        return []
    entries: list[Entry] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            entries.append(Entry(**json.loads(line)))
        except (json.JSONDecodeError, TypeError):
            # Una linea a medias no puede invalidar el historico entero.
            continue
    return entries


def done_keys() -> set[tuple[str, str, str, str]]:
    """Claves de los periodos ya completados (ultimo estado = done)."""
    latest: dict[tuple[str, str, str, str], str] = {}
    for entry in read_all():
        latest[entry.key] = entry.status
    return {key for key, status in latest.items() if status == STATUS_DONE}


def latest_statuses() -> dict[tuple[str, str, str, str], Entry]:
    latest: dict[tuple[str, str, str, str], Entry] = {}
    for entry in read_all():
        latest[entry.key] = entry
    return latest


def summary(entries: Iterable[Entry]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for entry in entries:
        counts[entry.status] = counts.get(entry.status, 0) + 1
    return counts


def log_summary(tag: str, counts: dict[str, int]) -> None:
    log(event="manifest", op=tag, **{f"n_{k}": v for k, v in sorted(counts.items())})
