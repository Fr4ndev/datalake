"""Mapeo dtype -> tabla de Timescale, con el reparto de responsabilidades lake/daemon.

Por que este modulo existe y no un `dict` suelto en el loader:

El lake (Parquet) y el daemon (WebSocket) **escriben en las mismas tablas**, y cada uno tiene
columnas que el otro no tiene. El loader tiene que saber que columna del Parquet va a que columna
de Postgres, porque los nombres no coinciden en ningun dtype:

    lake `klines`   open_time, close_time, quote_volume, taker_buy_volume, tf, year...
    tsdb candles_1m open_time, quote_volume, taker_buy_volume          (+ NO tf/year)

Y hay un renombrado real:

    lake `funding`  calc_time, last_funding_rate
    tsdb `funding`  funding_time, funding_rate, mark_price, next_funding_time

    lake `metrics`  create_time, sum_open_interest, sum_open_interest_value
    tsdb oi         ts, open_interest, open_interest_value

Por que el conflicto es DO NOTHING en casi todo y DO UPDATE en open_interest:

`ON CONFLICT DO NOTHING` es lo que hace idempotente la carga: se puede relanzar el backfill de un
ano entero sin duplicar nada. En `open_interest` hay una excepcion a proposito: el daemon escribe
la serie en vivo **sin** `open_interest_value` (esa columna no viene por WebSocket), asi que si el
lake llega despues con DO NOTHING la fila del daemon se queda con la columna a NULL para siempre.
Con DO UPDATE la carga rellena el valor y sigue habiendo una sola fila por clave, que es lo que
importa para el backtest. `metrics` es ademas la unica fuente de OI historico completa (el
historial de OI en REST son 30 dias), asi que perder el valor seria perder el dato.
"""

from __future__ import annotations

from dataclasses import dataclass

#: Tipos Postgres por columna, alineados con `columns`. El `unnest` posiciona por indice, asi que
#: una desincronizacion escribiria el precio en la columna de tiempo sin fallar.
TEXT = "text"
FLOAT = "double precision"
TS = "timestamptz"
INT = "integer"


@dataclass(frozen=True)
class Target:
    #: dtype del lake (el nombre del directorio bajo `lake/{exchange}/`)
    dtype: str
    table: str
    #: (columna_en_parquet -> columna_en_postgres). El orden marca las columnas del INSERT.
    mapping: tuple[tuple[str, str], ...]
    pg_types: tuple[str, ...]
    #: Clave del ON CONFLICT. Vacio = `DO NOTHING` sin objetivo (usa la PK de la tabla).
    conflict: tuple[str, ...] = ()
    #: Si el conflicto actualiza en vez de descartar. Solo OI, por lo de arriba.
    update_on_conflict: tuple[str, ...] = ()

    @property
    def columns(self) -> tuple[str, ...]:
        return tuple(dst for _, dst in self.mapping)

    @property
    def source_columns(self) -> tuple[str, ...]:
        return tuple(src for src, _ in self.mapping)

    def _select_sql(self) -> str:
        return ", ".join(
            f'"{src}"::{t}' for (src, _), t in zip(self.mapping, self.pg_types)
        )

    def _conflict_sql(self) -> str:
        if not self.conflict:
            return "ON CONFLICT DO NOTHING"
        cols = ", ".join(f'"{c}"' for c in self.conflict)
        if not self.update_on_conflict:
            return f"ON CONFLICT ({cols}) DO NOTHING"
        sets = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in self.update_on_conflict)
        return f"ON CONFLICT ({cols}) DO UPDATE SET {sets}"

    def insert_sql(self) -> str:
        """`INSERT ... SELECT ... FROM unnest(...) ON CONFLICT ...`, una ida y vuelta por lote."""
        if len(self.mapping) != len(self.pg_types):
            raise ValueError(
                f"{self.table}: mapping y pg_types desincronizados "
                f"({len(self.mapping)} vs {len(self.pg_types)})"
            )
        cols = ", ".join(f'"{c}"' for c in self.columns)
        args = ", ".join(f"%s::{t}[]" for t in self.pg_types)
        alias = ", ".join(f"v{i}" for i in range(1, len(self.columns) + 1))
        sel = ", ".join(f"u.v{i}::{t}" for i, t in enumerate(self.pg_types, 1))
        return (
            f"INSERT INTO {self.table} ({cols}) "
            f"SELECT {sel} FROM unnest({args}) AS u({alias}) "
            f"{self._conflict_sql()}"
        )


#: Columnas del lake que no van a la base. `tf` y `year` son metadatos de particionado: el
#: `year` ya se aplica en el `WHERE`, y el `tf` vive en la particion, no en la fila.
_LAKE_ONLY = ("tf", "year")

TARGETS: dict[str, Target] = {
    "klines": Target(
        dtype="klines",
        table="candles_1m",
        mapping=(
            ("symbol", "symbol"),
            ("exchange", "exchange"),
            ("open_time", "open_time"),
            ("open", "open"),
            ("high", "high"),
            ("low", "low"),
            ("close", "close"),
            ("volume", "volume"),
            ("quote_volume", "quote_volume"),
            ("trades", "trades"),
            ("taker_buy_volume", "taker_buy_volume"),
        ),
        pg_types=(TEXT, TEXT, TS, FLOAT, FLOAT, FLOAT, FLOAT, FLOAT, FLOAT, INT, FLOAT),
        conflict=("symbol", "exchange", "open_time"),
    ),
    "funding": Target(
        dtype="funding",
        table="funding",
        mapping=(
            ("symbol", "symbol"),
            ("exchange", "exchange"),
            ("calc_time", "funding_time"),
            ("last_funding_rate", "funding_rate"),
        ),
        pg_types=(TEXT, TEXT, TS, FLOAT),
        conflict=("symbol", "exchange", "funding_time"),
    ),
    "metrics": Target(
        dtype="metrics",
        table="open_interest",
        mapping=(
            ("symbol", "symbol"),
            ("exchange", "exchange"),
            ("create_time", "ts"),
            ("sum_open_interest", "open_interest"),
            ("sum_open_interest_value", "open_interest_value"),
        ),
        pg_types=(TEXT, TEXT, TS, FLOAT, FLOAT),
        conflict=("symbol", "exchange", "ts"),
        update_on_conflict=("open_interest", "open_interest_value"),
    ),
}

#: Columna de tiempo de cada dtype en el lake. Es la que se usa para acotar el rango a cargar y
#: la que se compara con la cagg.
TIME_COLUMN: dict[str, str] = {d: t.mapping[2][0] for d, t in TARGETS.items()}

#: cagg que hay que refrescar tras cargar cada dtype.
CAGG_BY_TARGET: dict[str, str] = {
    "candles_1m": "candles_1h",
    "funding": "funding_daily",
    "open_interest": "oi_5m",
    "trades": "liq_1h",  # trades no tiene cagg propia; se deja el mapeo explicito
}


def target_for(dtype: str) -> Target:
    try:
        return TARGETS[dtype]
    except KeyError:
        raise KeyError(
            f"dtype desconocido: {dtype!r}; conocidos: {sorted(TARGETS)}"
        ) from None