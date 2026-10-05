"""Rutas y constantes del bulk.

`LAKE_DIR` es la unica variable de entorno que controla donde vive el lake: en el host es
`lake/`, dentro del contenedor `/data/lake` (regla: el repo nunca guarda datos, estan en
.gitignore).
"""

from __future__ import annotations

import os
from pathlib import Path

# Endpoint S3 real de data.binance.vision. El indice web (data.binance.vision/?prefix=) es una
# pagina JS que no devuelve el listado, asi que hay que ir al endpoint S3 de la region.
S3_ENDPOINT = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
S3_BUCKET = "data.binance.vision"

# Origen de los datos historicos de perpetuos UM de Binance.
VISION_ROOT = "data/futures/um"

EXCHANGE_BINANCE = "binance"

# Timeframes que este modulo sabe particionar. La clave es el `tf` de la ruta del lake y el
# valor son los segundos entre velas, usado por el gap-scan.
TIMEFRAME_SECONDS = {"1m": 60, "5m": 300, "1h": 3600, "4h": 14400, "8h": 28800, "1d": 86400}

# Numero de workers por defecto para S3 (skill bulk-parquet-downloader: 8-16).
DEFAULT_WORKERS = 12

# Reintentos por fichero corrupto o fallo de red. Un fichero fallido nunca aborta el lote.
DEFAULT_RETRIES = 5

# Numero de filas por row group de Parquet (regla 4 de la skill).
ROW_GROUP_SIZE = 512 * 1024


def lake_dir() -> Path:
    return Path(os.environ.get("LAKE_DIR", "lake")).resolve()


def manifest_path() -> Path:
    return lake_dir() / "manifest.jsonl"


def known_gaps_path() -> Path:
    return lake_dir() / "known_gaps.json"


def qa_dir() -> Path:
    return lake_dir() / "_qa"


def partition_path(
    dtype: str, symbol: str, tf: str, year: int, *, exchange: str = EXCHANGE_BINANCE
) -> Path:
    """Ruta de la particion anual: lake/{exchange}/{dtype}/symbol={SYM}/tf={TF}/year={YYYY}/."""
    return lake_dir() / exchange / dtype / f"symbol={symbol}" / f"tf={tf}" / f"year={year}"
