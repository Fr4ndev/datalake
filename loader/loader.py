"""Carga lake (Parquet) -> TimescaleDB con DuckDB como lector.

Por que DuckDB y no pyarrow directo: el filtro de rango y el `SELECT` de columnas se resuelven en
DuckDB, que solo trae a memoria el tramo pedido. Un dia de klines 1m son ~1440 filas pero un ano son
~525k, y el lake son 3.5M filas en 8 ficheros yearly: cargarlo entero en memoria en un mini PC no
tiene sentido. DuckDB ademas hace pushdown del filtro por columna al leer el Parquet.

Por que el rango se filtra en el `WHERE` de DuckDB y no en Python: es el motor quien lee y el
filtro baja a la fuente, asi que la memoria del proceso queda plana sea cual sea el rango.

Por que se escribe por lotes y no de una vez: una sola sentencia con 500k filas es un unico
parametro gigante y revienta la memoria de Postgres. El lote son 20k filas por defecto.

Por que `ON CONFLICT`: la carga tiene que ser idempotente (regla 7). Relanzar un backfill de un ano
entero no puede duplicar nada, y no hace falta llevar un registro de "que se ha cargado": la base
es su propio manifiesto.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from common.db import assert_session_is_utc, conninfo, duckdb_utc
from loader.targets import CAGG_BY_TARGET, Target, target_for


@dataclass
class LoadResult:
    dtype: str
    table: str
    files: int = 0
    rows_read: int = 0
    rows_sent: int = 0
    rows_inserted: int = 0
    files_skipped: int = 0
    elapsed: float = 0.0
    errors: list[str] = field(default_factory=list)

    def as_log_fields(self) -> dict:
        return {
            "dtype": self.dtype,
            "table": self.table,
            "files": self.files,
            "rows_read": self.rows_read,
            "rows_sent": self.rows_sent,
            "rows_inserted": self.rows_inserted,
            "files_skipped": self.files_skipped,
            "rows_inserted_pct": (
                f"{100.0 * self.rows_inserted / self.rows_sent:.1f}" if self.rows_sent else "0.0"
            ),
            "elapsed": f"{self.elapsed:.2f}",
            "errors": len(self.errors),
        }


def log(event: str, **fields) -> None:
    """Log en formato clave=valor a stdout (regla 9)."""
    parts = " ".join(f"{k}={v}" for k, v in fields.items())
    print(f"component=loader event={event} {parts}", flush=True)


def lake_dir() -> Path:
    return Path(os.environ.get("LAKE_DIR", "/data/lake"))


def cobertura_lake(root: Path, dtype: str, exchange: str, symbol: str, tf: str | None = None):
    """Primer y ultimo `open_time` del lake, leyendo SOLO los metadatos del Parquet.

    Sin esto, pedir un rango que el lake no cubre devuelve `rows_read=0` sin explicar por que, y
    `reason=no_files` llega a significar dos cosas distintas ("el exchange no publico esto" y
    "el lake llega hasta ayer y tu pides la semana pasada"). Con estos dos numeros el log dice si
    lo que falta es datos de origen o rango pedido.
    """
    import pyarrow.parquet as pq

    lo = hi = None
    for f in parquet_files(root, dtype, exchange, symbol, tf):
        try:
            md = pq.ParquetFile(f)
        except Exception:  # noqa: BLE001 - un fichero ilegible no puede romper la cobertura
            continue
        idx = None
        for i, name in enumerate(md.schema_arrow.names):
            if name in ("open_time", "ts", "calc_time", "create_time"):
                idx = i
                break
        if idx is None:
            continue
        try:
            stats = md.metadata.row_group(0).column(idx).statistics
            f_lo, f_hi = (stats.min, stats.max) if stats is not None else (None, None)
        except Exception:  # noqa: BLE001 - sin estadisticas: no hay cobertura que informar
            continue
        if f_lo is None or f_hi is None:
            continue
        # Las estadisticas vienen como datetime (pyarrow) o como entero (columna numerica).
        # Cualquiera de los dos casos es un instante UTC: el lake se escribe en UTC.
        def _dt(v):
            if isinstance(v, datetime):
                return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
            return datetime.fromtimestamp(v / 1000.0, tz=timezone.utc)
        f_lo, f_hi = _dt(f_lo), _dt(f_hi)
        lo = f_lo if lo is None or f_lo < lo else lo
        hi = f_hi if hi is None or f_hi > hi else hi
    return lo, hi


def parquet_files(root: Path, dtype: str, exchange: str, symbol: str, tf: str | None = None):
    """Ficheros `lake/{exchange}/{dtype}/symbol={s}/tf={tf}/year=*/part.parquet`, ordenados.

    Se ordenan por anyo para que una carga interrumpida se pueda relanzar y siga avanzando en
    orden, y se devuelven como texto porque van dentro del `read_parquet([...])` de DuckDB.
    """
    base = root / exchange / dtype / f"symbol={symbol}"
    if not base.is_dir():
        return []
    if tf:
        candidates = [base / f"tf={tf}"]
    else:
        candidates = [p for p in sorted(base.iterdir()) if p.is_dir() and p.name.startswith("tf=")]
    files: list[Path] = []
    for tfdir in candidates:
        if not tfdir.is_dir():
            continue
        for year_dir in sorted(t for t in tfdir.iterdir() if t.is_dir() and t.name.startswith("year=")):
            part = year_dir / "part.parquet"
            if part.is_file():
                files.append(part)
    return files


class Loader:
    def __init__(self, batch_rows: int = 20_000, autocommit: bool = True):
        self.batch_rows = batch_rows
        self.autocommit = autocommit
        self._duck = None
        self._pg = None

    # ------------------------------------------------------------------ conexiones
    def open(self) -> None:
        """Abre DuckDB y verifica UTC. Postgres se abre solo cuando va a hacer falta escribir.

        Abrir Postgres aqui obligaba a tener base de datos para poder **leer** el lake, y hace
        que cualquier prueba del mapeo o del filtro por rango dependa de que tsdb este arriba.
        """
        self._duck = duckdb_utc()
        assert_session_is_utc(self._duck)

    def _ensure_pg(self):
        """Abre Postgres la primera vez que se va a escribir, y comprueba su zona."""
        if self._pg is not None:
            return self._pg
        import psycopg

        self._pg = psycopg.connect(
            conninfo(),
            autocommit=self.autocommit,
            options="-c timezone=UTC",
        )
        try:
            assert_session_is_utc(self._pg)
        except Exception:
            self._pg.close()
            self._pg = None
            raise
        return self._pg

    def close(self) -> None:
        for con in (self._duck, self._pg):
            if con is not None:
                con.close()
        self._duck = None
        self._pg = None

    def __enter__(self) -> "Loader":
        self.open()
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # ------------------------------------------------------------------ lectura
    def _read_batches(self, files, target: Target, time_col: str, since, until):
        """Devuelve generador de listas de filas (una por lote) desde el Parquet.

        El `SELECT` proyecta **solo** las columnas que van a la base. El Parquet tiene mas
        (`close_time`, `taker_buy_quote_volume`, `tf`, `year`) y trayendolas seria tirar ancho de
        banda de DuckDB a Postgres para nada.
        """
        listing = ", ".join("'" + str(p).replace("'", "''") + "'" for p in files)
        projection = ", ".join(f'"{c}"' for c in target.source_columns)
        where = [f'"{time_col}" IS NOT NULL']
        if since is not None:
            where.append(f'"{time_col}" >= ?')
        if until is not None:
            where.append(f'"{time_col}" < ?')
        sql = (
            f"SELECT {projection} FROM read_parquet([{listing}]) "
            f"WHERE {' AND '.join(where)} ORDER BY \"{time_col}\""
        )
        params = [p for p in (since, until) if p is not None]
        cur = self._duck.execute(sql, params) if params else self._duck.execute(sql)

        batch: list[tuple] = []
        while True:
            rows = cur.fetchmany(self.batch_rows)
            if not rows:
                break
            total = 0
            for row in rows:
                batch.append(tuple(row))
                total += 1
            yield batch, total
            batch = []

    # ------------------------------------------------------------------ escritura
    def _write_batch(self, target: Target, batch: list[tuple]) -> int:
        pg = self._ensure_pg()
        sql = target.insert_sql()
        params = [list(col) for col in zip(*batch)]
        with pg.cursor() as cur:
            cur.execute(sql, params)
            return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0

    # ------------------------------------------------------------------ cagg
    def refresh_cagg(self, cagg: str, since, until) -> None:
        """Refresca la cagg en el rango cargado.

        Sin esto la cagg seguiria mostrando el valor viejo aunque la tabla base ya tenga la fila
        nueva: las caggs solo se recalculan cuando se refrescan, y por defecto solo para el
        periodo anterior al ultimo bucket materializado.
        """
        lo = since.isoformat() if hasattr(since, "isoformat") else str(since)
        hi = until.isoformat() if hasattr(until, "isoformat") else str(until)
        # TimescaleDB rechaza subqueries en el CALL (`FeatureNotSupported: cannot use subquery in
        # CALL argument`), asi que los limites van como parametros, no como `(SELECT ...)`. El
        # nombre de la cagg si se interpola, pero sale de `CAGG_BY_TARGET`, no de entrada externa.
        sql = "CALL refresh_continuous_aggregate(%s, %s::timestamptz, %s::timestamptz)"
        pg = self._ensure_pg()
        with pg.cursor() as cur:
            cur.execute(sql, (cagg, lo, hi))
        log("cagg_refreshed", cagg=cagg, since=lo, until=hi)

    # ------------------------------------------------------------------ carga
    def load(
        self,
        dtype: str,
        symbol: str,
        exchange: str = "binance_um",
        tf: str | None = None,
        since=None,
        until=None,
        files=None,
        refresh: bool = True,
        escribir: bool = True,
    ) -> LoadResult:
        """Carga un dtype del lake a su tabla. Idempotente.

        `escribir=False` lee y cuenta sin insertar nada. Lo usan los tests: `test_el_bucket_por_
        dia_y_ano_es_utc` comprobaba el bucketing en UTC con una fila de 2019 y, al pasar por
        `load()` de verdad, la dejaba escrita en la tabla `funding` de produccion. Un test que
        escribe en la base real no es un test: es una carga disfrazada.
        """
        from loader.targets import TIME_COLUMN

        t0 = time.monotonic()
        target = target_for(dtype)
        time_col = TIME_COLUMN[dtype]
        res = LoadResult(dtype=dtype, table=target.table)

        paths = list(files) if files is not None else parquet_files(lake_dir(), dtype, exchange, symbol, tf)
        if not paths:
            res.elapsed = time.monotonic() - t0
            log("load_skipped", dtype=dtype, symbol=symbol, exchange=exchange,
                reason="no_files")
            return res
        cov_lo, cov_hi = cobertura_lake(lake_dir(), dtype, exchange, symbol, tf)
        res.files = len(paths)

        if self._duck is None:
            self.open()

        try:
            for batch, read in self._read_batches(paths, target, time_col, since, until):
                res.rows_read += read
                if not escribir:
                    res.rows_sent += read
                    continue
                try:
                    inserted = self._write_batch(target, batch)
                except Exception as exc:  # noqa: BLE001
                    # Un lote que falla no puede dejar la carga a medias sin avisar: se anota y
                    # se sigue con el siguiente. Reintentar el fichero entero es idempotente.
                    res.errors.append(f"{type(exc).__name__}: {exc}"[:200])
                    continue
                res.rows_sent += read
                res.rows_inserted += inserted
        except Exception as exc:  # noqa: BLE001
            res.errors.append(f"{type(exc).__name__}: {exc}"[:200])

        res.elapsed = time.monotonic() - t0
        log("load", **res.as_log_fields())

        # Habia ficheros pero ninguno cae en el rango pedido. Sin esta linea el operador lee
        # `rows_read=0` y no sabe si el exchange no publico ese dia o se ha pedido una fecha que el
        # lake todavia no cubre (el lake se queda dias por detras del presente).
        if res.files and not res.rows_read:
            log("load_range_empty", dtype=dtype, symbol=symbol, exchange=exchange, tf=tf,
                lake_first_ts=cov_lo.isoformat() if cov_lo else None,
                lake_last_ts=cov_hi.isoformat() if cov_hi else None,
                since=since.isoformat() if since else None,
                until=until.isoformat() if until else None)

        # Se refresca si se ha LEIDO algo del rango, no solo si se ha insertado. Si el refresh
        # fallara (p.ej. una cagg recien creada) y se relanzara la carga, la segunda pasada
        # insertaria 0 filas pero la cagg seguiria sin materializar: la condicion sobre
        # `rows_inserted` dejaba el cagg sin datos para siempre.
        if refresh and res.rows_read and not res.errors:
            cagg = CAGG_BY_TARGET.get(target.table)
            if cagg and since is not None and until is not None:
                try:
                    self.refresh_cagg(cagg, since, until)
                except Exception as exc:  # noqa: BLE001
                    res.errors.append(f"cagg {cagg}: {type(exc).__name__}: {exc}"[:200])
        return res