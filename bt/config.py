"""Splits congelados y lock del holdout.

Las fechas viven aqui (config versionada); su hash va en cada fila de `bt_runs`. Cambiarlas
tras ver resultados invalida los ensayos previos: el runner avisa si el spec_hash ya existe
con un holdout distinto.
"""
from __future__ import annotations

import hashlib
import json

CFG: dict = {
    "holdout_start": "2025-01-01",
    "holdout_locked": True,
    "fee_taker_bps": 5.0,
    "slippage_bps": 2.0,
    "max_leverage": 3.0,
    "min_trades_dev": 100,
    "estabilidad_min": 0.70,
    "boost_sharpe_min": 0.0,
    "dsr_min": 0.95,
    "pbo_max": 0.5,
    "split": {
        "dev_start": "2020-01-01",
        "dev_end": "2024-01-01",      # abierto por la derecha: [2020-01-01, 2024-01-01)
        "val_start": "2024-01-01",
        "val_end": "2025-01-01",
    },
}


def cfg_hash() -> str:
    return hashlib.sha256(json.dumps(CFG, sort_keys=True).encode()).hexdigest()[:16]


def ventana(split: str) -> tuple[str, str]:
    if split == "dev":
        return CFG["split"]["dev_start"], CFG["split"]["dev_end"]
    if split == "val":
        return CFG["split"]["val_start"], CFG["split"]["val_end"]
    if split == "holdout":
        return CFG["holdout_start"], None
    raise ValueError(f"split desconocido: {split}")


class HoldoutBloqueado(Exception):
    pass


def exige_final(desde: str, hasta: str | None, split: str, final: bool) -> None:
    """Tocar el holdout sin `--final` tiene que FALLAR (AC del canario de holdout).

    El holdout es [holdout_start, +inf) y las ventanas son semiabiertas [desde, hasta).
    Hay solape cuando la ventana no termina antes del inicio del holdout.
    """
    if not CFG["holdout_locked"]:
        return
    inicio = CFG["holdout_start"]
    solapa = split == "holdout" or hasta is None or hasta > inicio
    if solapa and not final:
        raise HoldoutBloqueado(
            f"holdout bloqueado desde {inicio}: la ventana [{desde}, {hasta or '+'}) "
            "lo alcanza. Unicamente `--final` (un disparo por spec_hash).")
