"""Indicadores y generación de la serie de posicion deseada.

`target[i]` son UNIDADES del activo que se quiere tener al cierre de la barra `i`; el motor
las ejecuta en el OPEN de `i+1`. El tamaño sale del precio de cierre de `i` (dato ya cerrado,
sin lookahead) y del apalancamiento declarado.
"""
from __future__ import annotations

import numpy as np


def ema(x: np.ndarray, n: int) -> np.ndarray:
    """EMA con alfa = 2/(n+1) y arranque en el primer dato (sin warm-up inventado)."""
    a = 2.0 / (n + 1.0)
    out = np.empty_like(x)
    out[0] = x[0]
    for i in range(1, x.shape[0]):
        out[i] = a * x[i] + (1.0 - a) * out[i - 1]
    return out


def warmup(n: int) -> int:
    """Barras a descartar: la skill exige descartar el warm-up de cada indicador."""
    return n + 1


def target_ema_cross(c: np.ndarray, fast: int, slow: int, cash0: float = 1000.0,
                     apalancamiento: float = 1.0) -> np.ndarray:
    """Largo cuando la EMA rápida esta sobre la lenta, plano cuando cruza a la baja.

    Sin cortos: el cruce invertido sale del mercado, no abre corto (menos grados de libertad
    para el data-snooping). El warm-up queda en plano.

    Las unidades se calculan UNA vez en la entrada y se congelan mientras la posicion esta
    abierta: si se recalcularan contra el precio de cada barra, el objetivo cambiaria cada
    barra y el motor rebalancearia en todas ellas (n_trades = n_bars, que fue exactamente el
    primer fallo de esta funcion).
    """
    ef = ema(c, fast)
    es = ema(c, slow)
    arriba = ef > es
    arriba[:warmup(max(fast, slow))] = False

    n = c.shape[0]
    target = np.zeros(n, dtype=np.float64)
    entras = np.flatnonzero(np.diff(arriba.astype(np.int8), prepend=0) == 1)
    salidas = np.flatnonzero(np.diff(arriba.astype(np.int8), prepend=0) == -1)
    tamano = cash0 * apalancamiento
    for k in range(entras.size):
        i = int(entras[k])
        fin = int(salidas[k]) if k < salidas.size else n
        if c[i] <= 0:
            continue
        u = tamano / c[i]
        target[i:fin] = u
    return target


def target_de_senal(senal: np.ndarray, c: np.ndarray, cash0: float = 1000.0,
                    apalancamiento: float = 1.0) -> np.ndarray:
    """Convierte ±1/0 (ya sin lookahead) en unidades del activo, congeladas por tramo."""
    largo = senal > 0
    n = c.shape[0]
    target = np.zeros(n, dtype=np.float64)
    entras = np.flatnonzero(np.diff(largo.astype(np.int8), prepend=0) == 1)
    salidas = np.flatnonzero(np.diff(largo.astype(np.int8), prepend=0) == -1)
    tamano = cash0 * apalancamiento
    for k in range(entras.size):
        i = int(entras[k])
        fin = int(salidas[k]) if k < salidas.size else n
        if c[i] <= 0:
            continue
        target[i:fin] = tamano / c[i]
    return target
