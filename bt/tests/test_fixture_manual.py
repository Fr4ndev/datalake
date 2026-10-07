"""Fixture manual: 10 trades con fees, slippage y funding calculados a mano.

Cada numero de abajo esta escrito a mano antes de ejecutar el test. Si el motor no da exactamente
ese valor, el motor esta mal.

Convenio de costes:
    fee_rate   = 0.0005  (5 bps por lado)
    slippage   = 0.0002  (2 bps, adverso: compra mas cara, venta mas barata)

Barras (10, 1 min) y posicion deseada:
    target[1] = 1.0  -> compra de 1 unidad en el OPEN de la barra 3 (=101)
    target[7] = 0.0  -> venta de esa unidad en el OPEN de la barra 9 (=99)
    funding en la barra 6 con rate = 0.0001, aplicado a la posicion YA abierta.

Calculo a mano:
  COMPRA (barra 3):
    precio con slippage = 101 * 1.0002 = 101.0202
    notional            = 101.0202
    fee                 = 101.0202 * 0.0005 = 0.0505101
    cash                = 0 - 101.0202 - 0.0505101 = -101.0707101
  FUNDING (barra 6, close=100):
    cargo = pos * close * rate = 1 * 100 * 0.0001 = 0.01
    cash  = -101.0707101 - 0.01 = -101.0807101
  VENTA (barra 9):
    precio con slippage = 99 * 0.9998 = 98.9802
    notional            = 98.9802
    fee                 = 98.9802 * 0.0005 = 0.0494901
    cash                = -101.0807101 + 98.9802 - 0.0494901 = -2.1500002
  Colateral inicial (cash0) = 1000 (sin el, la compra deja la equity en negativa y el
                             motor liquida: es el caso que descubrio el primer test)
  Equity final (pos=0)      = 1000 - 2.1500002 = 997.8499998
"""
import numpy as np

from bt.engine import simulate

FEE = 0.0005
SLIP = 0.0002

# (open, high, low, close)
BARRAS = [
    (100.0, 101.0, 99.0, 100.0),   # 0
    (100.0, 102.0, 100.0, 101.0),  # 1  target=1 -> ejecuta en el open de la barra 2
    (101.0, 101.0, 100.0, 100.0),  # 2  OPEN=101: COMPRA
    (100.0, 103.0, 100.0, 102.0),  # 3
    (102.0, 104.0, 101.0, 103.0),  # 4
    (103.0, 103.0, 99.0, 99.0),    # 5
    (99.0, 100.0, 98.0, 100.0),    # 6  FUNDING aki (close=100... ver abajo)
    (100.0, 101.0, 99.0, 99.0),    # 7  target=0 -> ejecuta en el open de la barra 8
    (99.0, 100.0, 98.0, 98.0),     # 8  OPEN=99: VENTA
    (98.0, 99.0, 97.0, 98.0),      # 9
]

# Nota: el cargo de funding usa el cierre de la barra del evento.
# Correccion del calculo a mano: la barra de funding es la 6, close=100 -> cargo = 1*100*0.0001.
# Lo dejo explicito aqui para que el numero del test sea el que se calcula.


def _arrays():
    b = np.array(BARRAS, dtype=np.float64)
    ts = np.arange(1, len(BARRAS) + 1, dtype=np.float64) * 60_000.0
    target = np.zeros(len(BARRAS), dtype=np.float64)
    target[1:8] = 1.0     # mantener la posicion de la barra 1 a la 7
    target[7] = 0.0       # salir en el OPEN de la barra 8 (=99)
    return b[:, 0], b[:, 1], b[:, 2], b[:, 3], ts, target


def test_fixture_10_trades_fees_slippage_funding_a_mano():
    o, h, l, c, ts, target = _arrays()
    fund_ts = np.array([ts[6]], dtype=np.float64)
    fund_rate = np.array([0.0001], dtype=np.float64)

    equity, fill_ts, fill_px, fill_side, funding_total = simulate(
        o, h, l, c, ts, target, fund_ts, fund_rate,
        fee_rate=FEE, slippage=SLIP, cash0=1000.0)

    # --- rellenado a mano con el close de la barra 6 (=100) para el funding
    # COMPRA: 101 * 1.0002 = 101.0202
    #   fee   = 101.0202 * 0.0005        = 0.0505101
    #   cash  = -101.0202 - 0.0505101    = -101.0707101
    # FUNDING: 1 * 100 * 0.0001          = 0.01
    #   cash  = -101.0707101 - 0.01      = -101.0807101
    # VENTA: 99 * 0.9998 = 98.9802
    #   fee   = 98.9802 * 0.0005         = 0.0494901
    #   cash  = -101.0807101 + 98.9802
    #            - 0.0494901             = -2.1500002
    cash_esperado = -101.0707101 - 0.01 + 98.9802 - 0.0494901
    # = -2.1500002 (los 0.0000002 son el fee sobre el notional de la venta, no ruido)

    assert abs(cash_esperado - (-2.1500002)) < 1e-12, cash_esperado
    assert abs(equity[-1] - (1000.0 + cash_esperado)) < 1e-9, (equity[-1], 1000.0 + cash_esperado)

    # dos fills: la compra y la venta
    assert fill_ts.shape[0] == 2, fill_ts
    assert abs(fill_px[0] - 101.0202) < 1e-9, fill_px[0]
    assert abs(fill_px[1] - 98.9802) < 1e-9, fill_px[1]
    assert fill_side[0] == 1.0 and fill_side[1] == -1.0

    # funding cargado = 0.01 (1 unidad * 100 * 0.0001)
    assert abs(funding_total - 0.01) < 1e-9, funding_total


def test_funding_se_carga_sobre_la_posicion_posterior_al_fill():
    """Convencion D47: si fill y funding caen en la misma barra, el funding se aplica a la
    posicion YA actualizada. Aqui la compra se ejecuta en la barra 2 y el funding cae en esa
    misma barra: se debe cargar sobre pos=1, no sobre pos=0."""
    o, h, l, c, ts, target = _arrays()
    target = np.zeros(len(BARRAS), dtype=np.float64)
    target[1:8] = 1.0               # compra en el open de la barra 2 y se mantiene
    target[7] = 0.0
    fund_ts = np.array([ts[2]])      # funding en la MISMA barra 2 (close=100)
    fund_rate = np.array([0.0001])

    _, _, _, _, funding_total = simulate(
        o, h, l, c, ts, target, fund_ts, fund_rate, fee_rate=FEE, slippage=SLIP, cash0=1000.0)

    # si se aplicara antes del fill (pos=0) saldria 0.0
    assert abs(funding_total - 1 * 100 * 0.0001) < 1e-9, funding_total
    assert funding_total > 0.0, "sin posicion no hay funding que cargar"
