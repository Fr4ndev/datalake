"""Un unico nombre por exchange. Modulo de verdad, no un diccionario mas.

Por que hace falta: `data.binance.vision` y el lake usan `binance` en minusculas, y cryptofeed
identifica Binance Futures como `BINANCE_FUTURES`. Como la PK de `trades` es
`(symbol, exchange, ts, trade_id)`, los dos son **series distintas**: el historico y el tiempo real
del mismo symbol no se juntan nunca, y el anti-join del reparador no encuentra lo que deberia.
No es un problema de nombre, es un problema de datos que se pierde en silencio.

Por que `canonico()` lanza ante un valor desconocido y no lo devuelve tal cual: devolverlo
tal cual es exactamente lo que produjo la division. Un exchange nuevo tiene que aparecer aqui a
propósito, con su canónico escrito, no colarse por el paso de tamaño de una función.

Regla de nombres: minusculas, sin separadores, y el producto cuando el mismo exchange tiene
varios (el de perpetuos de Binance no es el mismo que el de spot). De ahi `binance_um` y no
`binance`.
"""

from __future__ import annotations

import re

#: Los cinco exchanges del stack. Este es el unico sitio donde se declaran.
CANONICOS: tuple[str, ...] = ("binance_um", "bybit", "okx", "bitget", "hyperliquid")

#: Origen -> canonico. La clave es la forma normalizada (ver `_normaliza`).
_ORIGEN: dict[str, str] = {
    # Binance: el lake y el bulk dicen `binance`, cryptofeed dice `BINANCE_FUTURES`, y las
    # librerias de terceros usan variantes que no coinciden entre si.
    "binance": "binance_um",
    "binanceum": "binance_um",
    "binanceperp": "binance_um",
    "binanceperpetual": "binance_um",
    "binanceperpetualfutures": "binance_um",
    "binancefutures": "binance_um",
    "binancefuturesum": "binance_um",
    "binancecoinmfutures": "binance_um",
    "binanceusdm": "binance_um",
    "binanceumfutures": "binance_um",
    # Hyperliquid: cryptofeed lo llama `HYPERLIQUID`, su API REST lo llama `hyperliquid`.
    "hyperliquid": "hyperliquid",
    "hyperliquiddex": "hyperliquid",
}


#: Valores que se aceptan tal cual sin entrar en el catalogo. **Solo para los tests**: las
#: fixtures aíslan sus filas por exchange y asi pueden coexistir con los datos reales de cualquier
#: exchange real. Se declara aqui y no como tolerancia general porque una tolerancia general es
#: exactamente el fallo que produjo la division inicial: un valor desconocido pasaba de largo.
SOLO_TESTS: dict[str, str] = {
    "testex": "TESTEX",
    "visex": "VISEX",
}


def _normaliza(nombre: str) -> str:
    """`BINANCE_FUTURES`, `Binance-Futures-UM`, `binance futures` -> la misma clave.

    Se quitan todos los no alfanumericos y se pasa a minusculas, para que las tres grafias que
    aparecen en el proyecto (y las que traen las librerias) caigan en la misma entrada.
    """
    return re.sub(r"[^a-z0-9]", "", str(nombre).lower())


def canonico(nombre: str) -> str:
    """`nombre` -> su forma canónica. Lanza `ValueError` si no lo conoce.

    Lanzar es deliberado. Un `return str(nombre)` silencioso deja pasar el problema al ledger y a
    la PK, que es donde se manifestaba: dos series que no se juntan y ningun error visible.
    """
    if not nombre:
        raise ValueError("exchange vacio")
    clave = _normaliza(nombre)
    if clave in SOLO_TESTS:
        return SOLO_TESTS[clave]
    if clave in _ORIGEN:
        return _ORIGEN[clave]
    if clave in CANONICOS:
        return clave
    raise ValueError(
        f"exchange desconocido: {nombre!r} (normalizado: {clave!r}). "
        f"Anadelo a common/exchanges.py::{CANONICOS} con su forma canonica, "
        f"que no se admite el valor tal cual porque eso parte las series en dos.")


def es_canonico(nombre: str) -> bool:
    try:
        return canonico(nombre) == nombre
    except ValueError:
        return False


def canonicos_de(*nombres: str) -> list[str]:
    return [canonico(n) for n in nombres]


#: Valores antigos que hay que migrar, grouped por canonico. Solo para la migracion y sus tests:
#: el camino normal entra siempre por `canonico()`.
OBSOLETOS: dict[str, tuple[str, ...]] = {
    "binance_um": ("binance", "BINANCE_FUTURES", "BinanceFutures", "BINANCE"),
}
