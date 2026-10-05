"""De CSV de Binance Vision a tabla pyarrow con tiempos en UTC.

El CSV se lee con `pyarrow.csv` usando el esquema exacto del producto (no autodeteccion), porque
la autodeteccion de pyarrow se equivocaria justo en los casos que importan:

- en `klines` la primera fila es un epoch de 13 digitos, asi que un detector de "primera fila de
  texto = header" la trataria como cabecera y perderia la primera vela;
- en `metrics` la columna de tiempo es texto ISO, que `pyarrow.csv` no castea a timestamp por si
  solo.

La conversion a UTC se hace en Python con `tsutil`, que autodetecta la unidad por magnitud. Es un
coste de CPU acotado (un fichero de 1 mes son ~44.000 filas) a cambio de no depender de que
pyarrow adivine la unidad.
"""

from __future__ import annotations

import io
import zipfile

import pyarrow as pa
import pyarrow.csv as pacsv

from . import tsutil
from .logfmt import log
from .schemas import Schema


class ParseError(RuntimeError):
    """El fichero no se pudo interpretar con el esquema del producto."""


def looks_like_header(first_line: str, time_column_index: int = 0) -> bool:
    """Decide si la primera linea de un CSV es cabecera.

    Binance cambio el formato de los CSV de klines a mitad del historico: hasta 2021-12 no hay
    cabecera y desde 2022-01 si la hay (`trades` paso ademas a llamarse `count`). La unica cosa
    fiable es mirar si el primer campo de la columna de tiempo es un entero: una cabecera tiene
    ahi el nombre (`open_time`), un dato tiene un epoch.

    No se intenta "detectar por si la primera fila es numerica" porque en un fichero SIN
    cabecera la primera fila tambien es numerica (y en metrics es texto, pero ahi el esquema ya
    fija que hay cabecera).
    """
    fields = first_line.split(",")
    if len(fields) <= time_column_index:
        return True
    return not fields[time_column_index].strip().lstrip("-").isdigit()


def _read_csv(payload: bytes, schema: Schema) -> pa.Table:
    """Parsea el CSV con los tipos declarados, forzando texto solo en las columnas de tiempo."""
    # Las columnas de tiempo se leen como texto porque `pyarrow.csv` no autodetecta la unidad
    # (ms/us) ni parsea el formato ISO de `metrics`: esa conversion la hace `tsutil`. El resto
    # se lee con su tipo real para no acabar con precios y volumen como cadenas.
    column_types = {
        name: (pa.string() if pa.types.is_timestamp(dtype) else dtype)
        for name, dtype in zip(schema.columns, schema.types)
    }
    convert = pacsv.ConvertOptions(column_types=column_types, strings_can_be_null=False)

    if schema.has_header is None:
        first_line = payload.split(b"\n", 1)[0].decode("utf-8", "replace")
        skip_rows = 1 if looks_like_header(first_line, schema.columns.index(schema.ts_column)) else 0
    else:
        skip_rows = 1 if schema.has_header else 0

    read = pacsv.ReadOptions(
        column_names=list(schema.columns),
        skip_rows=skip_rows,
        autogenerate_column_names=False,
    )
    try:
        return pacsv.read_csv(io.BytesIO(payload), read_options=read, convert_options=convert)
    except pa.ArrowInvalid as exc:
        raise ParseError(f"CSV invalido: {exc}") from exc


def _extract_member(payload: bytes) -> bytes:
    """Saca el CSV del zip. Los .zip del bucket contienen un unico .csv."""
    with zipfile.ZipFile(io.BytesIO(payload)) as zf:
        names = [n for n in zf.namelist() if not n.endswith("/")]
        if len(names) != 1:
            raise ParseError(f"se esperaba 1 miembro en el zip, hay {len(names)}: {names[:5]}")
        return zf.read(names[0])


def _to_utc_column(values: pa.Array, *, what: str) -> pa.Array:
    """Convierte una columna de tiempo a `timestamp[us, UTC]` usando el detector de magnitud."""
    converted = [tsutil.to_utc(v.as_py()) for v in values]
    # `.as_py()` devuelve datetime con tzinfo=UTC; pyarrow los lleva a timestamp[us, UTC].
    return pa.array(converted, type=pa.timestamp("us", tz="UTC"))


def parse_payload(payload: bytes, schema: Schema) -> pa.Table:
    """zip -> tabla del lake, con la columna de tiempo normalizada a UTC.

    Devuelve solo las columnas de `schema.keep` mas `symbol` y `exchange`, que anade el
    downloader para que la particion sea autodescriptiva.
    """
    csv_bytes = _extract_member(payload)
    raw = _read_csv(csv_bytes, schema)

    if raw.num_rows == 0:
        return pa.table({"symbol": pa.array([], pa.string()), "exchange": pa.array([], pa.string())})

    # Columnas de tiempo del esquema original -> UTC.
    time_cols: dict[str, pa.Array] = {}
    for name in {schema.ts_column, *_other_time_columns(schema)}:
        if name in schema.columns and name not in time_cols:
            time_cols[name] = _to_utc_column(raw.column(name), what=name)

    arrays: dict[str, pa.Array] = {}
    for name in schema.keep:
        col = raw.column(name)
        if name in time_cols:
            arrays[name] = time_cols[name]
        elif schema.types[schema.columns.index(name)] == pa.timestamp("ms", tz="UTC"):
            # klines trae `close_time` ademas de `open_time`.
            arrays[name] = _to_utc_column(col, what=name)
        else:
            arrays[name] = col.combine_chunks()

    table = pa.table(arrays)
    log(event="parse", dtype=schema.name, rows=table.num_rows, columns=len(schema.columns))
    return table


def _other_time_columns(schema: Schema) -> tuple[str, ...]:
    """Columnas adicionales del esquema que son timestamps (p. ej. `close_time`)."""
    return tuple(
        name
        for name, dtype in zip(schema.columns, schema.types)
        if pa.types.is_timestamp(dtype) and name != schema.ts_column
    )
