# stack-base: imagen base unica compartida por migrate, bulk, loader, feed-daemon, bot y validator.
# Fija python 3.13 slim + uv con versiones pineadas por uv.lock (regla 6 de AGENTS.md).
#
# Por que python 3.13 y no 3.12: cryptofeed 3.x (el unico que trae Hyperliquid y el que arregla el
# endpoint de simbolos de Bitget) declara requires-python >=3.13. Ver docs/decisions.md D27.
#
# Por que sigue siendo multi-stage: se compila/instala en un stage aparte y al final solo se copia
# el venv. Con cryptofeed 3.0.1 desaparecio yapic.json (sustituido por msgspec, que trae ruedas),
# asi que ya no hay nada que compilar desde sdist y este patron es por aislamiento, no por
# compilacion.
#
# Por que se copia el venv y NO el wheelhouse: si se hiciese `COPY --from=builder /wheels /wheels`
# y luego `rm -rf /wheels`, los ~700 MB de ruedas seguirian contando en el tamano de la imagen
# ( una capa borrada solo crea un whiteout, no recupera espacio). Copiar solo /app/.venv mantiene
# la imagen final limpia.
#
# La lista de paquetes sale siempre de uv.lock via `uv export` (ninguna version escrita a mano).

# ------------------------------------------------------------------ stage 1: build
FROM python:3.13-slim-bookworm AS builder

ENV UV_PYTHON_DOWNLOADS=never \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1

COPY --from=ghcr.io/astral-sh/uv:0.11.19 /uv /usr/local/bin/uv

RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY pyproject.toml uv.lock .python-version ./

# uv 0.11 no tiene `uv pip wheel`, asi que el wheelhouse se genera con pip (solo en este stage
# descartable). Aqui si se verifican los hashes del lock: pip comprueba lo que descarga de PyPI.
# IMPORTANTE: este export y el del final deben describir el MISMO conjunto de paquetes (los dos
# incluyen el grupo dev), si no la instalacion final no encuentra ficheros en el wheelhouse.
RUN uv export --frozen --no-emit-project --format requirements-txt -o requirements.txt \
 && pip wheel --wheel-dir /wheels --requirement requirements.txt

# Se exporta el lock a requirements.txt (versiones pineadas desde uv.lock) y se instala con
# `uv pip sync`, que es un install exacto: no resuelve, aplica lo que hay en el fichero.
RUN uv venv /app/.venv \
 && uv pip sync --python /app/.venv --no-index --find-links /wheels \
      --no-verify-hashes requirements.txt \
 && /app/.venv/bin/python -c "import vectorbt, numba, polars, pyarrow, ccxt, cryptofeed, psycopg, aiogram" \
 && rm -rf /wheels /root/.cache

# ------------------------------------------------------------------ stage 2: runtime
FROM python:3.13-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    UV_PYTHON_DOWNLOADS=never \
    TZ=UTC

WORKDIR /app

# Solo el venv instalado. Sin este patron la imagen engorda ~700 MB de mas.
COPY --from=builder /app/.venv /app/.venv

COPY tsdb ./tsdb
COPY ops ./ops
COPY bulk ./bulk
COPY common ./common
COPY feed ./feed
COPY loader ./loader
COPY repair ./repair
COPY tests ./tests

ENV PATH="/app/.venv/bin:$PATH" \
    LAKE_DIR=/data/lake

CMD ["python", "ops/stub.py", "stack-base"]