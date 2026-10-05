"""Relleno por REST de lo que Binance Vision no publica todavia.

Que hace falta y por que (D19):

- `fundingRate` **no tiene ficheros diarios**: `daily/fundingRate/BTCUSDT/` esta vacio. Asi que
  el mes en curso no se puede completar con Vision y hay que paginar `/fapi/v1/fundingRate`.
- `metrics` si tiene diarios, pero cada zip diario del exchange **deja fuera exactamente un slot
  de 5m** (medido: 287 de 288 timestamps unicos al dia). El endpoint
  `/futures/data/openInterestHist` si tiene ese slot, asi que ahi el relleno es posible para los
  ultimos ~30 dias (lo que el REST conserva) y no para el historico.

Los endpoints se verificó contra la API real antes de escribir esto
(`docs/source-survey.md`): el que se usa aqui es `/fapi/v1/fundingRate`, que devuelve

    {"symbol": "BTCUSDT", "fundingTime": 1577836800000, "fundingRate": "0.00010000",
     "markPrice": "7100.0"}

y pagina con `startTime` + `limit` (max 1000), no con `fromId`.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import pyarrow as pa

from .logfmt import log

FAPI_BASE = "https://fapi.binance.com"
FUNDING_PATH = "/fapi/v1/fundingRate"
OPEN_INTEREST_PATH = "/futures/data/openInterestHist"
KLINES_PATH = "/fapi/v1/klines"

# El endpoint de funding devuelve como maximo 1000 registros por llamada.
FUNDING_PAGE = 1000
USER_AGENT = "curl/8.5.0"
DEFAULT_RETRIES = 4
BACKOFF_SECONDS = 1.5


class RestError(RuntimeError):
    """Fallo de red o respuesta inesperada del REST de Binance."""


def _get(path: str, params: dict[str, object], *, retries: int = DEFAULT_RETRIES) -> list[dict]:
    """GET paginado simple con reintentos y backoff exponencial.

    Sin API key: son endpoints publicos de solo lectura. Si Binance responde 429 o 5xx se
    reintenta; un 4xx definitivo (400 por parametro mal puesto) no se reintenta.
    """
    url = f"{FAPI_BASE}{path}?{urllib.parse.urlencode(params)}"
    last: Exception | None = None
    for attempt in range(1, retries + 1):
        request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return json.loads(response.read())
        except urllib.error.HTTPError as exc:
            last = exc
            if exc.code < 500 and exc.code != 429:
                raise RestError(f"HTTP {exc.code} en {path}: {exc.reason}") from exc
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            last = exc
        if attempt < retries:
            time.sleep(BACKOFF_SECONDS**attempt)
    raise RestError(f"REST fallo tras {retries} intentos en {path}: {last}")


def fetch_funding(
    symbol: str, start: datetime, end: datetime, *, interval_hours: int = 8
) -> list[dict]:
    """Registros de funding de `[start, end]` paginando por `startTime` + `limit`.

    Se pagina avanzando por el ultimo `fundingTime` visto, no por indice, porque el endpoint no
    ofrece `fromId`. El avance es de un intervalo completo para no repetir el ultimo registro.
    """
    step_ms = interval_hours * 3600 * 1000
    cursor = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)
    seen: dict[int, dict] = {}

    while cursor <= end_ms:
        batch = _get(
            FUNDING_PATH,
            {"symbol": symbol, "startTime": cursor, "endTime": end_ms, "limit": FUNDING_PAGE},
        )
        if not batch:
            break
        for item in batch:
            seen[int(item["fundingTime"])] = item
        last = max(int(item["fundingTime"]) for item in batch)
        if last < cursor:
            break  # sin avance: evitar bucle infinito
        cursor = last + step_ms
        # No se corta por "pagina corta": el exchange puede responder con menos filas que el
        # `limit` pedido sin que se haya acabado el rango, y parar ahi perderia registros. Se
        # sigue hasta que una pagina venga vacia.

    return [seen[k] for k in sorted(seen)]


def funding_to_table(records: list[dict], symbol: str, interval_hours: int = 8) -> pa.Table:
    """Convierte la respuesta de `/fapi/v1/fundingRate` al esquema `FUNDING_RATE`.

    El REST no manda `funding_interval_hours` (lo deduce el exchange), asi que se rellena con el
    intervalo configurado para no dejar nulos en una columna `NOT NULL` del lake.
    """
    stamps = [int(r["fundingTime"]) for r in records]
    return pa.table(
        {
            "calc_time": pa.array(stamps, pa.timestamp("ms", tz="UTC")),
            "funding_interval_hours": pa.array(
                [interval_hours] * len(records), pa.int64()
            ),
            "last_funding_rate": pa.array(
                [float(r["fundingRate"]) for r in records], pa.float64()
            ),
            "symbol": pa.array([symbol] * len(records), pa.string()),
            "exchange": pa.array(["binance"] * len(records), pa.string()),
        }
    )


def fetch_open_interest(
    symbol: str, start: datetime, end: datetime, *, period: str = "5m"
) -> list[dict]:
    """Open interest historico de `[start, end]`.

    Solo devuelve los ultimos ~30 dias: es lo que el exchange conserva para este endpoint, asi
    que para fechas mas antiguas la respuesta llega vacia. Se usa como **segunda fuente** para
    confirmar si un slot ausente en los zips de Vision existio de verdad en el exchange.
    """
    cursor = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)
    seen: dict[int, dict] = {}
    while cursor <= end_ms:
        batch = _get(
            OPEN_INTEREST_PATH,
            {
                "symbol": symbol,
                "period": period,
                "startTime": cursor,
                "endTime": end_ms,
                "limit": 500,
            },
        )
        if not batch:
            break
        for item in batch:
            seen[int(item["timestamp"])] = item
        last = max(int(item["timestamp"]) for item in batch)
        if last < cursor:
            break
        cursor = last + 1
        # Igual que en `fetch_funding`: una pagina corta no significa que se acabo el rango.
    return [seen[k] for k in sorted(seen)]


def fetch_klines(symbol: str, start: datetime, end: datetime, interval: str = "1m") -> list[dict]:
    """Velas de `/fapi/v1/klines` en `[start, end]`, paginando por `startTime` + `limit`.

    Se usa como **segunda fuente** para confirmar huecos de otros productos: si el exchange
    estaba publicando velas de 1m durante la ventana del hueco, el mercado funcionaba y lo que
    falta es una muestra concreta del producto (`metrics`), no una caida del exchange.

    El endpoint devuelve el limite máximo de 1500 velas por llamada.
    """
    limit = 1500
    step_ms = {"1m": 60_000, "5m": 300_000, "1h": 3_600_000, "1d": 86_400_000}[interval]
    cursor = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)
    seen: dict[int, dict] = {}

    while cursor <= end_ms:
        batch = _get(
            KLINES_PATH,
            {
                "symbol": symbol,
                "interval": interval,
                "startTime": cursor,
                "endTime": end_ms,
                "limit": limit,
            },
        )
        if not batch:
            break
        for item in batch:
            seen[int(item[0])] = item
        last = max(int(item[0]) for item in batch)
        if last < cursor:
            break
        cursor = last + step_ms
    return [seen[k] for k in sorted(seen)]


def klines_cover(symbol: str, start: datetime, end: datetime, interval: str = "1m") -> bool:
    """¿El exchange publico velas de forma continua durante `[start, end]`?

    Se recorre la ventana comparando el conteo esperado con el recibido y exigiendo que no haya
    un salto mayor que un intervalo: 10 velas no consecutivas no probarían que el exchange estaba
    vivo durante toda la ventana.
    """
    step_ms = {"1m": 60_000, "5m": 300_000, "1h": 3_600_000, "1d": 86_400_000}[interval]
    expected = int((end - start).total_seconds() * 1000) // step_ms + 1
    try:
        candles = fetch_klines(symbol, start, end, interval)
    except RestError as exc:
        log(event="rest_klines", status="error", note=str(exc))
        return False
    if len(candles) < expected:
        return False
    stamps = [int(c[0]) for c in candles]
    return all(b - a == step_ms for a, b in zip(stamps, stamps[1:]))


def open_interest_to_table(records: list[dict], symbol: str) -> pa.Table:
    """REST -> esquema `METRICS`.

    El endpoint solo trae `sumOpenInterest` y `sumOpenInterestValue`. Las cuatro ratios de
    long/short **no son recuperables por REST**, asi que se dejan a null en vez de inventarlas
    (quedan para el WS daemon). Es la misma limitacion que ya documenta D19.
    """
    stamps = [int(r["timestamp"]) for r in records]
    n = len(records)
    return pa.table(
        {
            "create_time": pa.array(stamps, pa.timestamp("ms", tz="UTC")),
            "symbol": pa.array([symbol] * n, pa.string()),
            "sum_open_interest": pa.array(
                [float(r["sumOpenInterest"]) for r in records], pa.float64()
            ),
            "sum_open_interest_value": pa.array(
                [float(r["sumOpenInterestValue"]) for r in records], pa.float64()
            ),
            "count_toptrader_long_short_ratio": pa.array([None] * n, pa.float64()),
            "sum_toptrader_long_short_ratio": pa.array([None] * n, pa.float64()),
            "count_long_short_ratio": pa.array([None] * n, pa.float64()),
            "sum_taker_long_short_vol_ratio": pa.array([None] * n, pa.float64()),
            "exchange": pa.array(["binance"] * n, pa.string()),
        }
    )


def log_fetch(product_dtype: str, symbol: str, source: str, rows: int, elapsed: float) -> None:
    log(
        event="rest",
        exchange="binance",
        symbol=symbol,
        dtype=product_dtype,
        source=source,
        rows=rows,
        elapsed=elapsed,
    )


def utc(year: int, month: int, day: int) -> datetime:
    return datetime(year, month, day, tzinfo=timezone.utc)
