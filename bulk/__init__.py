"""Backfill historico de Binance Vision a Parquet particionado (Fase 1).

El lake vive en `LAKE_DIR` (por defecto `lake/` en el repo, `/data/lake` en el contenedor).
"""

from __future__ import annotations

__all__ = ["config", "schemas", "tsutil", "s3list", "parser", "writer", "manifest", "download", "gapscan", "cli"]
