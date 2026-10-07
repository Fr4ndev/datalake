"""Motor mínimo para cumplir semántica básica: open t+1, costes/funding.
Placeholder hasta tener datos/semántica completa."""
import os
import hashlib
import json
from datetime import datetime, timezone


class Config:
    HOLDOUT_START = "2025-01-01"
    DEV_START = "2020-01-01"
    DEV_END = "2023-12-31"
    VAL_START = "2024-01-01"
    VAL_END = "2024-12-31"


def spec_hash(spec: dict) -> str:
    s = json.dumps(spec, sort_keys=True)
    return hashlib.sha256(s.encode()).hexdigest()
