"""Listado del bucket de Binance Vision.

Dos trampas del bucket, ambas medidas contra el origen (ver `docs/source-survey.md`, D15):

1. `data.binance.vision/?prefix=...` devuelve una pagina HTML de bootstrap, NO el listado. El
   listado real es el XML del endpoint S3 de la region.
2. Sin `list-type=2` el bucket responde `IsTruncated=true` **con `NextContinuationToken` vacio**.
   Un paginado ingenuo se queda eternamente en la primera pagina y da por buena una serie que solo
   ha leido 1000 claves (justo lo que pasaria con las 2468 velas diarias de BTCUSDT). Con
   `list-type=2` el token llega bien. `start-after` devuelve InvalidArgument sin ese parametro.

No se usa la libreria `requests`: `urllib` de la stdlib evita anadir dependencias (regla 6) y
basta para GET/HEAD con reintentos.
"""

from __future__ import annotations

import random
import re
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass

from .config import S3_BUCKET, S3_ENDPOINT
from .schemas import Product, prefix_for

_S3_NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
_UA = {"User-Agent": "cripto-marketdata/1.0 (+bulk)"}
_PAGE_SIZE = 1000

# Convenciones de nombre reales en el bucket (verificadas):
#   klines       -> BTCUSDT-1m-2020-01.zip        (simbolo-timeframe-fecha; SIN nombre de producto)
#   fundingRate  -> BTCUSDT-fundingRate-2020-01.zip
#   metrics      -> BTCUSDT-metrics-2020-09-01.zip
#   aggTrades    -> BTCUSDT-aggTrades-2020-01.zip
# El timeframe de klines NO puede confundirse con el nombre de producto porque el resto de
# productos llevan un token alfabetico fijo. Los sufijos tienen que ser EXACTO: en
# `monthly/aggTrades/` hay un `part-00000-<uuid>-c000.zip` que no es un periodo y se ignora.
_DATE = r"(?P<date>\d{4}-\d{2}(?:-\d{2})?)\.zip$"
FILENAME_PATTERNS: dict[str, re.Pattern[str]] = {
    "klines": re.compile(r"^(?P<sym>[A-Za-z0-9]+)-(?P<tf>[0-9a-z]+)-" + _DATE),
    "fundingRate": re.compile(r"^(?P<sym>[A-Za-z0-9]+)-fundingRate-" + _DATE),
    "metrics": re.compile(r"^(?P<sym>[A-Za-z0-9]+)-metrics-" + _DATE),
    "aggTrades": re.compile(r"^(?P<sym>[A-Za-z0-9]+)-aggTrades-" + _DATE),
}


class S3Error(RuntimeError):
    """Fallo de red o de protocolo hablando con el bucket."""


@dataclass(frozen=True)
class Object:
    """Un objeto .zip del bucket, con su tamano en bytes."""

    key: str
    size: int
    last_modified: str | None


@dataclass(frozen=True)
class Period:
    """Un periodo descargable: un fichero .zip de granularidad monthly o daily."""

    key: str
    granularity: str  # "monthly" | "daily"
    date: str  # "2020-01" | "2020-01-13"
    size: int

    @property
    def period_id(self) -> str:
        """Identificador para el manifest: `monthly:2020-01` / `daily:2019-12-31`."""
        return f"{self.granularity}:{self.date}"

    @property
    def name(self) -> str:
        return self.key.rsplit("/", 1)[-1]


def _request(url: str, *, method: str = "GET") -> tuple[bytes, dict[str, str]]:
    req = urllib.request.Request(url, headers=_UA, method=method)
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return resp.read(), {k.lower(): v for k, v in resp.headers.items()}
    except urllib.error.HTTPError as exc:
        raise S3Error(f"HTTP {exc.code} en {url}") from exc
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise S3Error(f"fallo de red en {url}: {exc}") from exc


def list_keys(prefix: str, *, delimiter: str = "/", max_pages: int = 200) -> list[Object]:
    """Lista recursivamente todas las claves bajo `prefix` paginando con list-type=2."""
    objects: dict[str, Object] = {}
    token: str | None = None
    for _ in range(max_pages):
        query = {"prefix": prefix, "delimiter": delimiter, "max-keys": str(_PAGE_SIZE), "list-type": "2"}
        if token:
            query["continuation-token"] = token
        body, _ = _request(f"{S3_ENDPOINT}?{urllib.parse.urlencode(query)}")
        root = ET.fromstring(body)

        page_items = root.findall("s3:Contents", _S3_NS)
        for item in page_items:
            key = item.findtext("s3:Key", namespaces=_S3_NS) or ""
            size = int(item.findtext("s3:Size", default="0", namespaces=_S3_NS) or 0)
            modified = item.findtext("s3:LastModified", namespaces=_S3_NS)
            objects[key] = Object(key=key, size=size, last_modified=modified)

        if root.findtext("s3:IsTruncated", default="false", namespaces=_S3_NS) != "true":
            break
        token = root.findtext("s3:NextContinuationToken", namespaces=_S3_NS)
        if not token:
            # Sin token no hay forma segura de continuar: cortar es preferible a asumir
            # cobertura completa y perder periodos en silencio.
            break
    return sorted(objects.values(), key=lambda o: o.key)


def list_periods(product: Product, granularity: str, *, vision_root: str) -> list[Period]:
    """Lista los periodos reales de un producto/granularidad, ignorando ficheros no conformes.

    Se valida cada nombre contra el patron exacto del producto y se descarta lo que no encaje
    (simbolo distinto, fichero de tipo `part-...`, extension rara).
    """
    pattern = FILENAME_PATTERNS.get(product.dtype)
    if pattern is None:
        raise ValueError(f"producto sin patron de nombre conocido: {product.dtype!r}")

    expected_len = 7 if granularity == "monthly" else 10
    periods: list[Period] = []
    for obj in list_keys(prefix_for(product, granularity, vision_root)):
        if not obj.key.endswith(".zip"):
            continue
        match = pattern.match(obj.key.rsplit("/", 1)[-1])
        if match is None:
            continue
        if match.group("sym") != product.symbol:
            continue
        date = match.group("date")
        if len(date) != expected_len:
            continue
        periods.append(Period(key=obj.key, granularity=granularity, date=date, size=obj.size))
    return sorted(periods, key=lambda p: p.date)


def fetch(key: str) -> bytes:
    """Descarga el contenido de un objeto."""
    body, _ = _request(f"{S3_ENDPOINT}/{urllib.parse.quote(key)}")
    return body


def fetch_checksum(key: str) -> str:
    """Lee el `.CHECKSUM` hermano y devuelve el sha256 en minusculas.

    Formato real verificado: `sha256  BTCUSDT-1m-2020-01.zip` (dos espacios, texto plano).
    """
    body, _ = _request(f"{S3_ENDPOINT}/{urllib.parse.quote(key + '.CHECKSUM')}")
    text = body.decode("utf-8", "replace").strip()
    if not text:
        raise S3Error(f"CHECKSUM vacio para {key}")
    return text.split()[0].lower()


def fetch_with_retry(key: str, *, retries: int = 5, base_delay: float = 0.5) -> bytes:
    """GET con reintentos y backoff exponencial con jitter (regla 1 de la skill)."""
    last: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            return fetch(key)
        except S3Error as exc:
            last = exc
            if attempt == retries:
                break
            # Backoff exponencial + jitter completo para no sincronizar 12 workers en el retry.
            delay = base_delay * (2 ** (attempt - 1))
            time.sleep(delay * (0.5 + random.random()))
    raise S3Error(f"fallo tras {retries} intentos en {key}: {last}")
