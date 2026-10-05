"""Escritura de Parquet particionado por ano, con dedup y rename atomico.

Reglas de la skill bulk-parquet-downloader que se cumplen aqui:

- zstd, `write_statistics=True`, `row_group_size` 512k.
- ruta `lake/{exchange}/{dtype}/symbol={SYM}/tf={TF}/year={YYYY}/part.parquet`.
- dedup por `(symbol, ts)` **antes** de escribir y otra vez tras fusionar con lo que ya habia:
  sin el segundo dedup, re-ejecutar el bulk duplicaria el mes que ya estaba.
- el fichero se escribe en un temporal y se publica con `os.replace`, que es atomico dentro del
  mismo sistema de ficheros. Un SIGKILL a mitad deja el `part.parquet` anterior intacto en vez de
  un parquet truncado (criterio de done: reanudar sin duplicar filas).
"""

from __future__ import annotations

import fcntl
import os
import uuid
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from . import config
from .logfmt import log
from .schemas import Product


def dedupe(table: pa.Table, keys: tuple[str, ...]) -> pa.Table:
    """Ordena por `(symbol, ts)` y deja una sola fila por clave.

    Se ordenan las claves en el orden dado para que el resultado sea estable y reproducible.
    """
    if table.num_rows <= 1:
        return table
    missing = [k for k in keys if k not in table.column_names]
    if missing:
        raise KeyError(f"faltan columnas de dedup {missing} en {table.column_names}")
    ordered = sorted_by_ts(table, keys)
    # Se conserva la primera fila de cada grupo: en un fichero re-descargado la repetida es
    # identica, y con "primera" el resultado no depende del orden de lectura.
    return ordered.filter(_first_of_group(ordered, keys))


def sorted_by_ts(table: pa.Table, keys: tuple[str, ...]) -> pa.Table:
    if table.num_rows <= 1:
        return table
    return table.take(pc.sort_indices(table, sort_keys=[(k, "ascending") for k in keys]))


def _first_of_group(ordered: pa.Table, keys: tuple[str, ...]) -> pa.Array:
    """Booleano que marca la primera fila de cada grupo de `keys` ya ordenado."""
    n = ordered.num_rows
    keep = [True] * n
    columns = [ordered.column(k).to_pylist() for k in keys]
    for i in range(1, n):
        if all(columns[j][i] == columns[j][i - 1] for j in range(len(keys))):
            keep[i] = False
    return pa.array(keep, type=pa.bool_())


def ensure_identity(table: pa.Table, *, symbol: str, exchange: str) -> pa.Table:
    """Anade/sobrescribe `symbol` y `exchange` para que la particion sea autodescriptiva.

    `metrics` ya trae su propia columna `symbol`; se sobrescribe con el solicitado para que no
    puedan contradecirse (y porque el CSV es la fuente, no la autoridad).
    """
    n = table.num_rows
    arrays: dict[str, pa.Array] = {}
    for name in table.column_names:
        if name == "symbol":
            arrays[name] = pa.array([symbol] * n, type=pa.string())
        elif name == "exchange":
            arrays[name] = pa.array([exchange] * n, type=pa.string())
        else:
            arrays[name] = table.column(name)
    if "symbol" not in arrays:
        arrays["symbol"] = pa.array([symbol] * n, type=pa.string())
    if "exchange" not in arrays:
        arrays["exchange"] = pa.array([exchange] * n, type=pa.string())
    # symbol y exchange delante, para que un `head` del parquet sea legible.
    return pa.table(
        {k: arrays[k] for k in ("symbol", "exchange", *[c for c in arrays if c not in ("symbol", "exchange")])}
    )


def _years(table: pa.Table, time_column: str) -> pa.Array:
    """Año de cada fila.

    Sin castear la columna: `fundingRate` guarda `calc_time` con milisegundos (el exchange los
    anade, jitter de +-10 ms) y un cast a `timestamp[s]` falla con
    `Casting from timestamp[ms] to timestamp[s] would lose data`. Para extraer el año no hace
    falta perder precision.
    """
    return pc.year(table.column(time_column))


def years_in(table: pa.Table, time_column: str) -> list[int]:
    if table.num_rows == 0:
        return []
    return sorted(int(v) for v in pc.unique(_years(table, time_column)).to_pylist() if v is not None)


def split_by_year(table: pa.Table, time_column: str) -> dict[int, pa.Table]:
    if table.num_rows == 0:
        return {}
    years = _years(table, time_column)
    out: dict[int, pa.Table] = {}
    for year in sorted({int(v) for v in pc.unique(years).to_pylist() if v is not None}):
        out[year] = table.filter(pc.equal(years, year))
    return out


def read_partition(path: Path) -> pa.Table | None:
    if not path.exists():
        return None
    return pq.read_table(path)


def _partition_lock(parquet_path: Path):
    """Lock exclusivo de una particion, para serializar su read-modify-write.

    Sin esto hay perdida de datos silenciosa: con 12 workers, los periodos de un mismo ano
    (2020-01 y 2020-02 van ambos a `year=2020/part.parquet`) leen el mismo estado inicial,
    construyen cada uno su tabla y el ultimo `os.replace` pisa al otro. Se perdian ~2.1 M filas
    de 3.5 M en la primera ejecucion real.

    Se usa `fcntl.flock` y no un `threading.Lock` porque flock bloqueo por file-description
    abierta: serializa tambien entre hilos (cada uno abre su propio fd) y entre procesos, que es
    lo que haria falta si se lanzan dos `bulk` a la vez.
    """
    lock_path = parquet_path.parent / ".write.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = lock_path.open("a+")
    fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
    return handle  # el caller lo cierra, lo que libera el lock


def write_year(
    table: pa.Table, product: Product, year: int, *, exchange: str = config.EXCHANGE_BINANCE
) -> tuple[Path, int]:
    """Fusiona `table` con la particion anual existente, deduplica y publica de forma atomica."""
    path = config.partition_path(product.lake_dirname, product.symbol, product.tf, year, exchange=exchange)
    # `path` ya termina en `year={year}`: hay que crear `path` entero, no `path.parent`.
    path.mkdir(parents=True, exist_ok=True)
    parquet_path = path / "part.parquet"

    keys = ("symbol", product.schema.time_column)

    # Todo el read-modify-write va dentro del lock: fuera de el, otro worker puede publicar
    # entre nuestra lectura y nuestro os.replace y perderíamos sus filas.
    with _partition_lock(parquet_path):
        incoming = dedupe(table, keys)

        existing = read_partition(parquet_path)
        if existing is not None:
            # Se iguala el esquema del parquet viejo al entrante: si le faltan columnas se
            # rellenan con nulos, para que anadir una columna nueva no obligue a reescribir todo.
            incoming = pa.concat_tables([_align(existing, incoming.schema), incoming])
            merged = dedupe(incoming, keys)
            added, total = merged.num_rows - existing.num_rows, merged.num_rows
        else:
            merged, added, total = incoming, incoming.num_rows, incoming.num_rows

        tmp = parquet_path.with_name(f".part.parquet.{os.getpid()}.{uuid.uuid4().hex[:8]}.tmp")
        try:
            pq.write_table(
                merged,
                tmp,
                compression="zstd",
                compression_level=9,
                write_statistics=True,
                use_dictionary=True,
                row_group_size=config.ROW_GROUP_SIZE,
            )
            os.replace(tmp, parquet_path)
        finally:
            if tmp.exists():
                tmp.unlink()

    log(
        event="write",
        exchange=exchange,
        symbol=product.symbol,
        dtype=product.dtype,
        year=year,
        rows=total,
        added=added,
        path=str(parquet_path.relative_to(config.lake_dir())),
    )
    return parquet_path, total


def _align(table: pa.Table, schema: pa.Schema) -> pa.Table:
    """Iguala las columnas de `table` a `schema`, rellenando con nulos las que falten."""
    arrays = []
    for field in schema:
        if field.name in table.column_names:
            arrays.append(table.column(field.name).cast(field.type))
        else:
            arrays.append(pa.nulls(table.num_rows, type=field.type))
    return pa.table(arrays, schema=schema)


def write(
    table: pa.Table, product: Product, *, exchange: str = config.EXCHANGE_BINANCE
) -> dict[int, tuple[Path, int]]:
    """Reparte la tabla por anos y escribe cada particion. Devuelve {year: (path, filas)}."""
    stamped = ensure_identity(table, symbol=product.symbol, exchange=exchange)
    result: dict[int, tuple[Path, int]] = {}
    for year, chunk in split_by_year(stamped, product.schema.time_column).items():
        result[year] = write_year(chunk, product, year, exchange=exchange)
    return result
