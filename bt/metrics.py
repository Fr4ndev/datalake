"""Gates estadísticos. Devuelven el valor y si PASAN; el runner arma el veredicto.

Todo es determinista: la semilla fija en cada funcion, porque el canario 7 exige que un rerun
reproduzca las metricas exactas.
"""
from __future__ import annotations

import numpy as np
from scipy import stats as st

ANUAL_DIARIO = 365.0
SEED = 20261007


def retornos_diarios(equity: np.ndarray, ts: np.ndarray) -> np.ndarray:
    """Retorno por dia natural UTC (ultimo equity de cada dia)."""
    if equity.size < 3:
        return np.zeros(0)
    dias = (ts // 86_400_000).astype(np.int64)
    _, idx = np.unique(dias, return_index=True)
    eq = equity[np.sort(idx)]
    r = np.diff(eq) / eq[:-1]
    return r[np.isfinite(r)]


def sharpe(rets: np.ndarray) -> float:
    if rets.size < 2 or np.std(rets) == 0:
        return 0.0
    return float(np.mean(rets) / np.std(rets) * np.sqrt(ANUAL_DIARIO))


def ic_sharpe(rets: np.ndarray, n_boot: int = 2000, bloque: int = 20,
              semilla: int = SEED) -> tuple[float, float]:
    """IC 95% por block bootstrap sobre los retornos diarios.

    Se remuestrean bloques contiguos: los retornos diarios son correlacionados y un bootstrap
    i.i.d. daria un intervalo demasiado estrecho.
    """
    if rets.size < bloque * 3:
        return (sharpe(rets), sharpe(rets))
    rng = np.random.default_rng(semilla)
    n = rets.size
    n_bloques = int(np.ceil(n / bloque))
    valores = np.empty(n_boot)
    for b in range(n_boot):
        ini = rng.integers(0, n - bloque + 1, size=n_bloques)
        muestra = np.concatenate([rets[i:i + bloque] for i in ini])[:n]
        valores[b] = sharpe(muestra)
    return (float(np.percentile(valores, 2.5)), float(np.percentile(valores, 97.5)))


def deflated_sharpe(sr: float, n_obs: int, g3: float, g4: float, n_trials: int) -> float:
    """Deflated Sharpe Ratio (Bailey & Lopez de Prado): prob. de que el SR observe sea el mejor
    de N ensayos aun si todas las ideas son ruido."""
    if n_obs < 3 or n_trials < 1:
        return 0.0
    sr = float(sr)
    var_sr = (1.0 - g3 * sr + (g4 - 1.0) / 4.0 * sr * sr) / (n_obs - 1)
    if var_sr <= 0:
        return 0.0
    g = 0.5772156649015329          # Euler-Mascheroni
    nn = float(n_trials)
    esperado_max = ((1.0 - g) * st.norm.ppf(1.0 - 1.0 / nn)
                    + g * st.norm.ppf(1.0 - 1.0 / (nn * np.e)))
    sr0 = np.sqrt(var_sr) * esperado_max
    return float(st.norm.cdf((sr - sr0) / np.sqrt(var_sr)))


def max_drawdown(equity: np.ndarray) -> float:
    if equity.size == 0:
        return 0.0
    pico = np.maximum.accumulate(equity)
    dd = (equity - pico) / pico
    return float(-dd.min())


def monte_carlo_maxdd(trade_rets: np.ndarray, semilla: int = SEED,
                      n_sim: int = 4000) -> tuple[float, float]:
    """Reordena los retornos por trade (MC del orden) y devuelve (maxDD medio, p95)."""
    if trade_rets.size < 5:
        return (0.0, 0.0)
    # con millones de trades, 4000 permutaciones son 4.000 x 1M: se ajusta el numero de
    # simulaciones al tamaño para que el gate siga siendo util y el run termine. A la baja
    # tambien: con ~700 trades 4000 permutaciones costaban 9,5 s por combinacion y 45
    # combinaciones (o 200 estrategias aleatorias) no terminaban.
    if trade_rets.size > 200_000:
        n_sim = min(n_sim, 200)
    else:
        n_sim = min(n_sim, 1_000)
    rng = np.random.default_rng(semilla)
    dds = np.empty(n_sim)
    for i in range(n_sim):
        orden = rng.permutation(trade_rets)
        eq = np.concatenate([[1.0], np.cumprod(1.0 + orden)])
        dds[i] = max_drawdown(eq * 1000.0)
    return (float(np.mean(dds)), float(np.percentile(dds, 95)))


def estabilidad_anual(equity: np.ndarray, ts: np.ndarray) -> tuple[float, int]:
    """Fraccion de anos naturales UTC con Sharpe > 0 (gate: >= 70%)."""
    if equity.size < 1000:
        return (0.0, 0)
    fechas = ts.astype("datetime64[ms]")
    anios = (fechas.astype("datetime64[Y]").astype(np.int64) + 1970)
    positivos = total = 0
    for a in np.unique(anios):
        m = anios == a
        if m.sum() < 1000:
            continue
        r = retornos_diarios(equity[m], ts[m])
        total += 1
        if sharpe(r) > 0:
            positivos += 1
    return ((positivos / total) if total else 0.0, total)
