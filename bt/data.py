"""Carga SOLO del lake Parquet, con TimeZone='UTC' fijado antes de cualquier consulta.

La regla 3.bis del proyecto: sin fijar la zona, `year()`/`date_trunc()` del cliente operan con la
zona local del host (Europe/Madrid en el mini PC) y desplazan datos entre dias y anos. El test
`test_zona_horaria_utc` de bt/tests falla si esto se relaja.
"""
from __future__ import annotations

import glob
import hashlib
import os

import duckdb
import numpy as np

LAKE = os.environ.get("QUANT_LAKE", "lake")
CAMBIOS = ("open_time", "calc_time", "ts")


def _con() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(config={"threads": max(1, (os.cpu_count() or 2) - 1)})
    con.execute("SET TimeZone='UTC'")          # ANTES de cualquier consulta
    return con


def snapshot(lake: str = LAKE) -> str:
    """sha256 de manifest.jsonl + known_gaps.json: cambia el lake, cambia la huella."""
    h = hashlib.sha256()
    for nombre in ("manifest.jsonl", "known_gaps.json"):
        ruta = os.path.join(lake, nombre)
        if not os.path.exists(ruta):
            raise FileNotFoundError(f"falta {ruta}: el lake no esta completo")
        with open(ruta, "rb") as fh:
            h.update(nombre.encode())
            h.update(fh.read())
    return h.hexdigest()


def _particiones(lake: str, cambio: str, dtype: str, symbol: str, tf: str) -> list[str]:
    base = os.path.join(lake, cambio, dtype, f"symbol={symbol}", f"tf={tf}", "year=*", "part.parquet")
    ficheros = sorted(glob.glob(base))
    if not ficheros:
        raise FileNotFoundError(f"sin particiones para {base}")
    return ficheros


def cargar_ohlcv(symbol: str = "BTCUSDT", exchange: str = "binance_um",
                 tf: str = "1m", lake: str = LAKE,
                 desde: str | None = None, hasta: str | None = None) -> dict:
    """Devuelve o,h,l,c (float64), ts en ms UTC (int64) y el recuento de barras."""
    ficheros = _particiones(lake, exchange, "klines", symbol, tf)
    con = _con()
    con.execute("CREATE TEMP TABLE src AS SELECT * FROM read_parquet(?)", [ficheros])
    where, params = [], []
    if desde:
        where.append("open_time >= ?::TIMESTAMPTZ")
        params.append(desde)
    if hasta:
        where.append("open_time < ?::TIMESTAMPTZ")
        params.append(hasta)
    cond = ("WHERE " + " AND ".join(where)) if where else ""
    df = con.execute(f"""
        SELECT open_time, open, high, low, close
        FROM src {cond}
        ORDER BY open_time
    """, params).df()
    con.close()

    ts = _a_ms(df["open_time"])
    o = df["open"].to_numpy(np.float64)
    h = df["high"].to_numpy(np.float64)
    l = df["low"].to_numpy(np.float64)
    c = df["close"].to_numpy(np.float64)
    _comprobar_continuidad(ts, tf)
    return {"o": o, "h": h, "l": l, "c": c, "ts": ts}


def cargar_funding(symbol: str = "BTCUSDT", exchange: str = "binance_um",
                   lake: str = LAKE, desde: str | None = None,
                   hasta: str | None = None) -> dict:
    """Timestamps y rates de funding. `calc_time` es el instante al que aplica el rate."""
    ficheros = _particiones(lake, exchange, "funding", symbol, "8h")
    con = _con()
    con.execute("CREATE TEMP TABLE src AS SELECT * FROM read_parquet(?)", [ficheros])
    where, params = [], []
    if desde:
        where.append("calc_time >= ?::TIMESTAMPTZ")
        params.append(desde)
    if hasta:
        where.append("calc_time < ?::TIMESTAMPTZ")
        params.append(hasta)
    cond = ("WHERE " + " AND ".join(where)) if where else ""
    df = con.execute(f"""
        SELECT calc_time, last_funding_rate, funding_interval_hours
        FROM src {cond} ORDER BY calc_time
    """, params).df()
    con.close()
    return {"fund_ts": _a_ms(df["calc_time"]),
            "fund_rate": df["last_funding_rate"].to_numpy(np.float64),
            "intervalo_h": df["funding_interval_hours"].to_numpy(np.float64)}


def _a_ms(serie) -> np.ndarray:
    """Autodetecta la unidad por magnitud y normaliza a ms enteros UTC (regla 3)."""
    import pandas as pd

    if isinstance(serie, pd.Series) and pd.api.types.is_datetime64_any_dtype(serie.dtype):
        s = serie
        tz = getattr(s.dtype, "tz", None)
        if tz is not None:
            s = s.dt.tz_convert("UTC").dt.tz_localize(None)
        return s.astype("datetime64[ms]").astype(np.int64).to_numpy()

    ns = serie.to_numpy() if hasattr(serie, "to_numpy") else np.asarray(serie)
    if np.issubdtype(ns.dtype, np.datetime64):
        return ns.astype("datetime64[ms]").astype(np.int64)
    v = np.abs(ns.astype(np.float64))
    v = v[v > 0]
    if v.size == 0:
        return np.zeros(0, dtype=np.int64)
    d = int(np.median(v))
    if d >= 10**15:                      # microsegundos o mas
        escala = 1e3 if d < 10**18 else 1e-3
    elif d >= 10**12:                    # milisegundos
        escala = 1.0
    else:                                # segundos
        escala = 1e3
    return np.round(ns.astype(np.float64) * escala).astype(np.int64)


def _comprobar_continuidad(ts: np.ndarray, tf: str) -> None:
    """Solo valida que no haya saltos entre barras: barra ausente = sin operar (no se interpola)."""
    if ts.size < 2:
        return
    paso = {"1m": 60_000, "5m": 300_000, "8h": 8 * 3_600_000}[tf]
    difs = np.diff(ts)
    saltos = int(np.sum(difs > paso))
    if saltos:
        print(f"event=salto_barras tf={tf} n={saltos} max_ms={int(difs.max())}")
