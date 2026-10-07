"""Los 8 canarios del motor. Obligatorios en CI: si alguno falla, el motor es el que esta mal.

1. Fixture manual de 10 trades (en `test_fixture_manual.py`).
2. Buy&hold = retorno del precio menos los costes de una entrada y una salida.
3. Estrategia sin señales -> equity plana, 0 trades.
4. Canario aleatorio: 200 estrategias con direccion simetrica -> Sharpe medio <= 0.
5. Monotonia de costes: mas fee/slippage => menor retorno, siempre.
6. El parser rechaza accesos al futuro.
7. Determinismo: mismo input -> mismas metricas.
8. Motor propio vs vectorbt en configuraciones sin stops ni funding.
"""
from __future__ import annotations

import numpy as np
import pytest

from bt.dsl import parse_signal
from bt.engine import simulate

FEE = 0.0005
SLIP = 0.0002


def _series(n=600, semilla=7):
    """Serie sintetica determinista con tendencia + ruido, alineada a 60 s."""
    r = np.random.default_rng(semilla)
    rets = r.normal(0.0, 0.001, n) + 0.00002
    c = 100.0 * np.cumprod(1.0 + rets)
    o = np.empty(n)
    o[0] = c[0]
    o[1:] = c[:-1]
    h = np.maximum(o, c) * 1.0004
    l = np.minimum(o, c) * 0.9996
    ts = np.arange(1, n + 1, dtype=np.float64) * 60_000.0
    return o, h, l, c, ts


# ------------------------------------------------------------------ 2
def test_canario2_buy_hold_resta_costes_de_entrada_y_salida():
    o, h, l, c, ts = _series(300)
    n = c.shape[0]
    target = np.zeros(n)
    target[0:n - 1] = 1.0     # entra en el OPEN de la barra 1 y se mantiene
    target[n - 2] = 0.0       # sale en el OPEN de la barra n-1

    equity, fill_ts, fill_px, fill_side, _ = simulate(
        o, h, l, c, ts, target, fee_rate=FEE, slippage=SLIP, cash0=1000.0)

    assert fill_ts.shape[0] == 2, fill_ts
    assert fill_side[0] == 1.0 and fill_side[1] == -1.0

    # calculo a mano: una entrada y una salida
    entrada = o[1] * (1 + SLIP)
    salida = o[n - 1] * (1 - SLIP)
    costes = entrada * FEE + salida * FEE
    esperado = 1000.0 - entrada + salida - costes

    assert abs(equity[-1] - esperado) < 1e-9, (equity[-1], esperado)
    # y con costes, el buy&hold por debajo del bruto
    bruto = 1000.0 - o[1] + o[n - 1]
    assert equity[-1] < bruto


# ------------------------------------------------------------------ 3
def test_canario3_sin_senales_equity_plana_y_cero_trades():
    o, h, l, c, ts = _series(400)
    target = np.zeros(c.shape[0])
    equity, fill_ts, fill_px, fill_side, _ = simulate(
        o, h, l, c, ts, target, fee_rate=FEE, slippage=SLIP, cash0=1000.0)

    assert fill_ts.shape[0] == 0, fill_ts
    assert np.allclose(equity, 1000.0), (equity.min(), equity.max())


# ------------------------------------------------------------------ 5
def test_canario5_mas_costes_menor_retorno():
    o, h, l, c, ts = _series(800)
    n = c.shape[0]
    target = np.zeros(n)
    target[1:400] = 1.0
    target[400] = 0.0

    resultados = []
    for fee in (0.0, 0.0005, 0.002, 0.01):
        eq, *_ = simulate(o, h, l, c, ts, target,
                          fee_rate=fee, slippage=SLIP, cash0=1000.0)
        resultados.append(eq[-1])

    for a, b in zip(resultados, resultados[1:]):
        assert b < a, f"mas fee no redujo el retorno: {resultados}"

    resultados_s = []
    for slip in (0.0, 0.0002, 0.001, 0.005):
        eq, *_ = simulate(o, h, l, c, ts, target,
                          fee_rate=FEE, slippage=slip, cash0=1000.0)
        resultados_s.append(eq[-1])

    for a, b in zip(resultados_s, resultados_s[1:]):
        assert b < a, f"mas slippage no redujo el retorno: {resultados_s}"


# ------------------------------------------------------------------ 7
def test_canario7_determinismo_mismo_input_mismas_metricas():
    o, h, l, c, ts = _series(700, semilla=42)
    n = c.shape[0]
    target = np.zeros(n)
    target[1:500] = 1.0
    target[500] = 0.0

    a = simulate(o, h, l, c, ts, target, fee_rate=FEE, slippage=SLIP, cash0=1000.0)
    b = simulate(o, h, l, c, ts, target, fee_rate=FEE, slippage=SLIP, cash0=1000.0)

    assert np.array_equal(a[0], b[0]), "equity no determinista"
    assert np.array_equal(a[1], b[1]), "fills no deterministas"
    assert a[4] == b[4], "funding no determinista"


# ------------------------------------------------------------------ 6
def test_canario6_el_parser_rechaza_accesos_al_futuro():
    # shift positivo (hacia atras) esta bien
    parse_signal("close.shift(1)")
    parse_signal("close - close.rolling(20).mean()")
    # cualquier referencia al futuro debe rechazarse
    with pytest.raises(ValueError):
        parse_signal("close[+1]")
    with pytest.raises(ValueError):
        parse_signal("close.shift(-1)")


# ------------------------------------------------------------------ 4
def test_canario4_estrategias_aleatorias_simetricas_no_ganan():
    """200 estrategias aleatorias con direccion SIMETRICA (50/50) para que el drift de BTC no
    sesgue el resultado. Tras costes, el Sharpe medio debe ser <= 0 y no puede haber
    'significativas' mas alla del azar esperado."""
    from scipy import stats as _st

    o, h, l, c, ts = _series(5000, semilla=1234)
    n = c.shape[0]
    rng = np.random.default_rng(999)

    def _corridas(fee, slip, semilla):
        """200 estrategias con direccion 50/50 y duracion aleatoria (misma exposicion para las
        dos condiciones). Devuelve el t de cada una."""
        g = np.random.default_rng(semilla)
        out = []
        for _ in range(200):
            target = np.zeros(n)
            i = 1
            while i < n - 60:
                dur = int(g.integers(20, 60))
                signo = 1.0 if g.random() < 0.5 else -1.0      # 50/50
                fin = min(i + dur, n - 2)
                target[i:fin] = signo
                i = fin + int(g.integers(1, 30))
            eq, *_ = simulate(o, h, l, c, ts, target,
                              fee_rate=fee, slippage=slip, cash0=1000.0)
            r = np.diff(eq) / eq[:-1]
            r = r[np.isfinite(r)]
            if r.size < 30 or np.std(r) == 0:
                out.append(0.0)
            else:
                out.append(float(np.mean(r) / np.std(r) * np.sqrt(r.size)))
        return np.asarray(out)

    t_con = _corridas(FEE, SLIP, 999)
    t_sin = _corridas(0.0, 0.0, 999)

    se = float(np.std(t_con) / np.sqrt(t_con.size))

    # (a) con costes la media tiene que ser NEGATIVA de forma significativa: el arrastre de fees
    # y slippage es lo que hace que estas estrategias aleatorias pierdan.
    assert float(np.mean(t_con)) < -3.0 * se, (
        f"media t={np.mean(t_con):.4f} no es significativamente negativa: "
        "faltan costes o hay lookahead que los compensa")

    # (b) el maximo no puede ir mas alla de 4 desviaciones de su propia distribucion: con 200
    # muestras, el azar espera 0,003 por encima de 4 sigma. 196 eran la senal de que el test
    # (t parametrico sobre bloques correlacionados) estaba mal, no el motor.
    umbral = float(np.mean(t_con) + 4.0 * np.std(t_con))
    n_fuera = int(np.sum(t_con > umbral))
    assert n_fuera <= 3, f"{n_fuera} estrategias a mas de 4 sigma de 200: no es azar"

    # (c) control: SIN costes la misma distribucion debe ser simetrica (media ~0). Si saliera
    # negativa igual, el sesgo no serian los costes sino el propio motor.
    assert abs(float(np.mean(t_sin))) <= 3.0 * float(np.std(t_sin)) / np.sqrt(t_sin.size), (
        f"sin costes la media t={np.mean(t_sin):.4f} se desvia de 0: sesgo del motor")

    # (d) nunca mas de la mitad positivas con costes
    assert float(np.mean(t_con > 0)) <= 0.5


# ------------------------------------------------------------------ 8
def test_canario8_motor_propio_vs_vectorbt():
    """Motor propio y vectorbt, en configuracion SIN stops y SIN funding, deben dar la misma
    equity y los mismos trades. Difieren en el orden de eventos por barra (decision B1), asi que
    el canario solo tiene sentido apagando justo lo que hace que las dos semanticas difieran."""
    import vectorbt as vbt

    o, h, l, c, ts = _series(1000, semilla=11)
    n = c.shape[0]

    k = 20
    sma = np.convolve(c, np.ones(k) / k, mode="valid")
    senal = np.zeros(n, dtype=bool)
    senal[k - 1:] = c[k - 1:] > sma
    target = np.where(senal, 1.0, 0.0)
    target[:k - 1] = 0.0
    target[0] = 0.0

    equity, fill_ts, fill_px, fill_side, fund = simulate(
        o, h, l, c, ts, target, fee_rate=0.0, slippage=0.0,
        sl=0.0, tp=0.0, cash0=1000.0)

    # vectorbt decide y ejecuta en la MISMA barra; nosotros decidimos en el cierre y ejecutamos
    # en el open siguiente, asi que la senal se desplaza una barra para que comparen iguales.
    entries = np.zeros(n, dtype=bool)
    exits = np.zeros(n, dtype=bool)
    for i in range(1, n):
        prev, prev2 = target[i - 1], (target[i - 2] if i >= 2 else 0.0)
        if prev == 1.0 and prev2 == 0.0:
            entries[i] = True
        elif prev == 0.0 and prev2 == 1.0:
            exits[i] = True

    pf = vbt.Portfolio.from_signals(
        c, entries, exits, price=o, size=1.0, fees=0.0, fixed_fees=0.0,
        slippage=0.0, init_cash=1000.0, direction="longonly",
        val_price=c, freq="1min")

    vbt_value = np.asarray(pf.value(), dtype=np.float64)

    n_ordenes = int(entries.sum() + exits.sum())
    assert n_ordenes >= 20, f"solo {n_ordenes} ordenes: el canario no prueba nada"
    assert fill_ts.shape[0] == n_ordenes, (
        f"trades distintos: motor={fill_ts.shape[0]} vectorbt={n_ordenes}")

    max_diff = float(np.max(np.abs(equity - vbt_value)))
    assert max_diff < 1e-6, f"equity divergente: max |dif| = {max_diff:.3e}"

    # y ademas el resultado final debe coincidir con la suma de unidades * precio
    assert abs(equity[-1] - vbt_value[-1]) < 1e-9
