"""Cliente HTTP con cubo de tokens por exchange, backoff y manejo de 418/429.

Por que no `requests`: no es dependencia directa del proyecto y anadirla obliga a tocar
`pyproject.toml` + `uv.lock` para algo que `urllib` de la stdlib hace igual. Regla 6.

Por que un cubo de tokens y no un `sleep` fijo
------------------------------------------------
Los limites por IP son **compartidos con el resto del stack**. Si el reparador los quemara, el
backfill diario y el propio daemon se empezarian a comer los limites mutuos. Un cubo de tokens
con coste por peticion hace que "el token que yo pido, lo pagas tu" sea explicito.

Limites medidos/contra-documentacion (ver `docs/source-survey.md`):

- **Binance** `/fapi/v1/aggTrades`: peso **20** por peticion y 2400/min por IP -> 2 peticiones/s
  efectivas. Es el unico caro: una pagina de 1000 aggTrades gasta el equivalente a 20 peticiones
  normales, asi que paginar un hueco de 45 s son varias peticiones.
- **OKX** `history-trades`: 20 peticiones / 2 s -> 10/s.
- **Bitget** endpoints publicos: 10/s.
- **Bybit** publicos: 600 / 5 s -> 20/s.
- **Hyperliquid**: muy generoso; 20/s de sobra.

418 vs 429
----------
Binance devuelve **418** cuando te banea la IP y **429** cuando vas demasiado rapido. No son lo
mismo: un 429 se espera y se reintenta; un 418 puede durar minutos y reintentar es empeorar la
cosa. Por eso el ban se trata con una pausa larga y **sin** marcar el hueco como fallido
(`repair/http.py::RateLimited.banned`), porque el hueco sigue siendo reparable, solo que despues.
"""

from __future__ import annotations

import json
import random
import time
import urllib.error
import urllib.parse
import urllib.request

from bulk.logfmt import log
from common.exchanges import canonico

USER_AGENT = "cripto-marketdata/0.2 (gap-repair)"


class HttpError(RuntimeError):
    """Fallo recuperable con la excepcion de origen adjunta."""


class RateLimited(HttpError):
    """429: hay que esperar `retry_after` segundos."""

    def __init__(self, retry_after: float, message: str = ""):
        super().__init__(f"429: esperar {retry_after:.1f}s {message}".strip())
        self.retry_after = retry_after


class Banned(HttpError):
    """418: baneo temporal de IP. Pausa larga, sin consumir intentos del hueco."""

    def __init__(self, retry_after: float, message: str = ""):
        super().__init__(f"418: baneo, esperar {retry_after:.1f}s {message}".strip())
        self.retry_after = retry_after


class Bucket:
    """Cubo de tokens con coste por peticion."""

    def __init__(self, capacity: float, refill_per_s: float):
        self.capacity = float(capacity)
        self.refill_per_s = float(refill_per_s)
        self.tokens = float(capacity)
        self.updated = time.monotonic()

    def take(self, weight: float = 1.0) -> float:
        """Consume `weight` tokens. Devuelve cuantos segundos hay que dormir (0 si no hace falta)."""
        self.refill()
        self.tokens -= weight
        if self.tokens >= 0:
            return 0.0
        return -self.tokens / self.refill_per_s

    def refill(self) -> None:
        ahora = time.monotonic()
        self.tokens = min(self.capacity, self.tokens + (ahora - self.updated) * self.refill_per_s)
        self.updated = ahora


#: exchange canonico -> (capacidad del cubo, refill por segundo, peso de una peticion).
#: Las claves son las de `common.exchanges.CANONICOS`: un cubo por exchange, con su nombre.
LIMITS: dict[str, tuple[float, float, float]] = {
    "binance_um": (20.0, 2.0, 20.0),
    "okx": (40.0, 10.0, 1.0),
    "bitget": (20.0, 10.0, 1.0),
    "bybit": (100.0, 20.0, 1.0),
    "hyperliquid": (60.0, 20.0, 1.0),
}


def clave_exchange(exchange: str) -> str:
    """Limites de `exchange`, admitiendo cualquier grafia (cryptofeed o lake).

    Antes esto era un diccionario escrito a mano, `"OKX": "okx"` junto a `"bitget": "bitget"`, y
    por eso `Client("okx")` y `Client("bybit")` -ya canonicos- no aparecian en el mapa y caian al
    cubo de hyperliquid: OKX con 60 de capacidad en vez de 40, y Bybit con 60 en vez de 100. No
    reventaba nada; solo estaba limitando a otro ritmo del que creiamos, que es exactamente como
    aparecen luego los 429 sin explicacion.
    """
    try:
        return canonico(exchange)
    except KeyError:
        return exchange.lower()


class Client:
    """Cliente HTTP con cubo de tokens, ban y backoff.

    `sleep` es inyectable para que los tests no tarden: con un `sleep` falso el cubo se comporta
    igual pero instantaneo.
    """

    def __init__(self, sleep=time.sleep, jitter=lambda: random.random()):
        self.buckets: dict[str, Bucket] = {}
        self.banned_until: dict[str, float] = {}
        self._sleep = sleep
        self._jitter = jitter
        self.peticiones = 0
        self.esperas = 0.0

    def _bucket(self, exchange: str) -> Bucket:
        # Un cubo por exchange CANONICO: indexarlo por la grafina que llega haria que "OKX" y
        # "okx" tuvieran cada uno su cubo, es decir el doble de limite para el mismo exchange.
        clave = clave_exchange(exchange)
        cap, refill, _ = LIMITS[clave]
        b = self.buckets.get(clave)
        if b is None:
            b = self.buckets[clave] = Bucket(cap, refill)
        return b

    def weight(self, exchange: str) -> float:
        return LIMITS[clave_exchange(exchange)][2]

    # ------------------------------------------------------------------ ban
    def penalize(self, exchange: str, seconds: float) -> None:
        hasta = time.monotonic() + seconds
        self.banned_until[exchange] = max(self.banned_until.get(exchange, 0.0), hasta)
        log(component="repair", event="ban", exchange=exchange, retry_after=round(seconds, 1))

    def _esperar_ban(self, exchange: str) -> None:
        hasta = self.banned_until.get(exchange)
        if hasta is None:
            return
        restante = hasta - time.monotonic()
        if restante <= 0:
            self.banned_until.pop(exchange, None)
            return
        self.espera_ban(exchange, restante)
        self.banned_until.pop(exchange, None)

    def espera_ban(self, exchange: str, restante: float) -> None:
        """Espera el baneo. Inyectable para tests."""
        log(component="repair", event="ban_wait", exchange=exchange, seconds=round(restante, 1))
        self.esperas += restante
        self._sleep(min(restante, 900.0))

    # ------------------------------------------------------------------ get
    def get(self, exchange: str, url: str, params: dict | None = None,
            *, retries: int = 3, timeout: float = 20.0):
        """GET con JSON. Devuelve el objeto parseado."""
        self._esperar_ban(exchange)
        peso = self.weight(exchange)
        ultimo: Exception | None = None
        for intento in range(retries + 1):
            self.espera_peticion(exchange, peso)
            url_final = f"{url}?{urllib.parse.urlencode(params)}" if params else url
            req = urllib.request.Request(url_final, headers={"User-Agent": USER_AGENT})
            try:
                self.peticiones += 1
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    return json.loads(resp.read().decode())
            except urllib.error.HTTPError as exc:
                cuerpo = exc.read()[:300].decode(errors="replace")
                retry_after = _retry_after(exc.headers)
                if exc.code == 418:
                    # Ban: pausa larga y NO se propaga como fallo del hueco. Es un problema de la
                    # IP, no del rango que estamos reparando.
                    self.penalize(exchange, retry_after or 120.0)
                    ultimo = Banned(retry_after or 120.0, cuerpo)
                    break
                if exc.code == 429:
                    self.penalize(exchange, retry_after or 2.0)
                    ultimo = RateLimited(retry_after or 2.0, cuerpo)
                    if intento < retries:
                        continue
                    break
                if exc.code in (418, 451) or 500 <= exc.code < 600:
                    ultimo = HttpError(f"HTTP {exc.code}: {cuerpo[:200]}")
                    if intento < retries:
                        self.backoff(intento)
                        continue
                else:
                    # 4xx de cliente (400 ventana demasiado ancha, 400 simbolo, ...): reintentar
                    # es perder el tiempo, lo relevante es el mensaje.
                    raise HttpError(f"HTTP {exc.code}: {cuerpo[:240]}") from exc
            except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError) as exc:
                ultimo = HttpError(f"{type(exc).__name__}: {exc}")
                if intento < retries:
                    self.backoff(intento)
                    continue
        raise ultimo or HttpError("fallo desconocido")

    def post(self, exchange: str, url: str, body: dict, *, retries: int = 3, timeout: float = 20.0):
        """POST con JSON (Hyperliquid lo exige)."""
        self._esperar_ban(exchange)
        peso = self.weight(exchange)
        ultimo: Exception | None = None
        for intento in range(retries + 1):
            self.espera_peticion(exchange, peso)
            datos = json.dumps(body).encode()
            req = urllib.request.Request(url, data=datos, method="POST", headers={
                "User-Agent": USER_AGENT, "Content-Type": "application/json"})
            try:
                self.peticiones += 1
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    return json.loads(resp.read().decode())
            except urllib.error.HTTPError as exc:
                cuerpo = exc.read()[:300].decode(errors="replace")
                retry_after = _retry_after(exc.headers)
                if exc.code == 418:
                    self.penalize(exchange, retry_after or 120.0)
                    raise Banned(retry_after or 120.0, cuerpo) from exc
                if exc.code == 429:
                    self.penalize(exchange, retry_after or 2.0)
                    ultimo = RateLimited(retry_after or 2.0, cuerpo)
                    if intento < retries:
                        continue
                    raise ultimo from exc
                if 500 <= exc.code < 600:
                    ultimo = HttpError(f"HTTP {exc.code}: {cuerpo[:200]}")
                    if intento < retries:
                        self.backoff(intento)
                        continue
                    raise ultimo from exc
                raise HttpError(f"HTTP {exc.code}: {cuerpo[:240]}") from exc
            except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError) as exc:
                ultimo = HttpError(f"{type(exc).__name__}: {exc}")
                if intento < retries:
                    self.backoff(intento)
                    continue
                raise ultimo from exc
        raise ultimo or HttpError("fallo desconocido")

    # ------------------------------------------------------------------ esperas
    def espera_peticion(self, exchange: str, peso: float) -> None:
        segundos = self._bucket(exchange).take(peso)
        if segundos <= 0:
            return
        # Jitter: sin el, N workersignon un wakeson y revientan el limite a la vez.
        segundos = min(segundos * (0.8 + 0.4 * self._jitter()), 60.0)
        self.esperas += segundos
        self._sleep(segundos)

    def backoff(self, intento: int) -> None:
        """Exponencial con jitter: 0,5s, 1s, 2s, 4s... mas un factor aleatorio."""
        base = min(0.5 * (2 ** intento), 30.0)
        self.esperas += base
        self._sleep(base * (0.5 + self._jitter()))


def _retry_after(headers) -> float | None:
    if headers is None:
        return None
    valor = headers.get("Retry-After") or headers.get("retry-after")
    if valor is None:
        return None
    try:
        return float(valor)
    except (TypeError, ValueError):
        return None