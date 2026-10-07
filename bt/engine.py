"""Motor propio (bucle numba) para ejecución y contabilidad.

Decisión B1: el motor es propio porque la semántica de la skill exige control exacto del orden de
eventos por barra (orden pendiente en OPEN, SL antes que TP, funding en su timestamp, costes
siempre). vectorbt se queda para indicadores/señales y como verificador independiente (canario 8).

Orden de eventos por barra `i` (fijado, ver decisions.md D47):
1. Ejecución de la orden pendiente (decidida en el cierre de `i-1`) en `open[i]`, con slippage
   adverso y fee.
2. Funding si `ts[i]` coincide con un evento. **Se aplica a la posición YA actualizada por la
   orden del paso 1**: si el fill y el funding caen en la misma barra, se carga sobre la nueva
   posición. Documentado en D47 y cubierto por el fixture manual.
3. Stops intrabarra con `high[i]`/`low[i]`: SL y TP en la misma barra -> SL; gap a traves del
   stop -> fill en `open[i]` (peor caso).
4. Liquidacion aproximada (margen aislado) al `close[i]`.
5. Mark-to-market al `close[i]`.
"""
from __future__ import annotations

import numpy as np
from numba import njit


@njit(cache=True, fastmath=False)
def _simulate(o, h, l, c, ts, target, fund_ts, fund_rate,
              fee_rate, slippage, sl, tp, mmr, cash0):
    """Corazon del motor.

    `target[i]` es la posicion deseada al CIERRE de la barra `i` y se ejecuta en el OPEN de `i+1`.
    `cash0` es el colateral inicial: sin el, cualquier compra deja la equity en negativa y el
    motor liquida en la siguiente comprobacion.
    `target[i] = 0` con `i == 0` no genera orden.
    Devuelve (equity, fill_ts, fill_px, fill_side, funding_total).
    """
    n = o.shape[0]
    equity = np.empty(n)
    pos = 0.0            # cantidad en unidades del activo (+largo, -corto)
    cash = cash0         # colateral en quote (margen inicial)
    avg_entry = 0.0
    pending = 0.0        # orden pendiente de ejecutar en el proximo open
    has_pending = False
    funding_total = 0.0

    f_i = 0
    m = fund_ts.shape[0]

    fill_ts = np.empty(n)
    fill_px = np.empty(n)
    fill_side = np.empty(n)
    n_fill = 0

    for i in range(n):
        # ---------------------------------------------------- 1. orden pendiente en el OPEN
        if has_pending:
            if pending != pos:
                px = o[i]
                if pending > pos:
                    px_exec = px * (1.0 + slippage)   # comprar mas caro
                else:
                    px_exec = px * (1.0 - slippage)   # vender mas barato

                delta = pending - pos
                notional = abs(delta) * px_exec
                cash -= delta * px_exec + notional * fee_rate

                cruza = (pos > 0.0 and delta < 0.0 and abs(delta) >= abs(pos)) or \
                        (pos < 0.0 and delta > 0.0 and abs(delta) >= abs(pos))
                if pos == 0.0 or (np.sign(delta) != np.sign(pos)) or cruza:
                    avg_entry = px_exec
                elif abs(delta) > 0.0:
                    nuevo = abs(pos) + abs(delta)
                    avg_entry = (avg_entry * abs(pos) + px_exec * abs(delta)) / nuevo

                fill_ts[n_fill] = ts[i]
                fill_px[n_fill] = px_exec
                fill_side[n_fill] = np.sign(delta)
                n_fill += 1
                pos = pending
            has_pending = False

        # ---------------------------------------------------- 2. funding en su timestamp
        while f_i < m and fund_ts[f_i] <= ts[i]:
            cargo = pos * c[i] * fund_rate[f_i]     # largo paga con rate > 0
            cash -= cargo
            funding_total += cargo
            f_i += 1

        # ---------------------------------------------------- 3. stops intrabarra
        if pos != 0.0 and (sl > 0.0 or tp > 0.0):
            exit_px = 0.0
            if pos > 0.0:                            # largo: SL debajo, TP encima
                hit_sl = sl > 0.0 and l[i] <= sl
                hit_tp = tp > 0.0 and h[i] >= tp
                if hit_sl and hit_tp:
                    exit_px = sl                     # misma barra -> SL primero
                elif hit_sl:
                    exit_px = sl
                elif hit_tp:
                    exit_px = tp
                # gap a traves del stop: el open ya estaba mas alla -> peor caso
                if exit_px > 0.0 and o[i] < exit_px:
                    exit_px = o[i]
            else:                                    # corto: SL encima, TP debajo
                hit_sl = sl > 0.0 and h[i] >= sl
                hit_tp = tp > 0.0 and l[i] <= tp
                if hit_sl and hit_tp:
                    exit_px = sl
                elif hit_sl:
                    exit_px = sl
                elif hit_tp:
                    exit_px = tp
                if exit_px > 0.0 and o[i] > exit_px:
                    exit_px = o[i]

            if exit_px > 0.0:
                if pos > 0.0:
                    px_exec = exit_px * (1.0 - slippage)
                else:
                    px_exec = exit_px * (1.0 + slippage)
                delta = -pos
                notional = abs(delta) * px_exec
                cash -= delta * px_exec + notional * fee_rate
                pos = 0.0
                avg_entry = 0.0
                fill_ts[n_fill] = ts[i]
                fill_px[n_fill] = px_exec
                fill_side[n_fill] = np.sign(delta)
                n_fill += 1

        # ---------------------------------------------------- 4. liquidacion (margen aislado)
        if pos != 0.0:
            eq = cash + pos * c[i]
            mant = mmr * abs(pos) * c[i]
            if eq <= mant:
                cash -= -pos * c[i]                 # cierra toda la posicion
                pos = 0.0
                avg_entry = 0.0
                fill_ts[n_fill] = ts[i]
                fill_px[n_fill] = c[i]
                fill_side[n_fill] = 1.0
                n_fill += 1

        # ---------------------------------------------------- 5. mark-to-market
        equity[i] = cash + pos * c[i]

        # orden decidida en el cierre, ejecutada en el open de i+1
        if i + 1 < n:
            pending = target[i]
            has_pending = True
        # quiebra: margen aislado, no se entra ni se reinvierte. Sin esto la equity puede
        # seguir cayendo por debajo de cero mientras la posicion esta plana (nunca se
        # alcanza el paso 4, que solo actua con pos != 0) y el drawdown sale sin sentido.
        if has_pending and equity[i] <= 0.0:
            has_pending = False

    return (equity, fill_ts[:n_fill], fill_px[:n_fill], fill_side[:n_fill], funding_total)


def simulate(o, h, l, c, ts, target, fund_ts=None, fund_rate=None,
             fee_rate=0.0005, slippage=0.0002, sl=0.0, tp=0.0, mmr=0.005,
             cash0=0.0):
    """Wrapper en Python. Arrays float64 indexados en UTC."""
    o = np.ascontiguousarray(o, dtype=np.float64)
    h = np.ascontiguousarray(h, dtype=np.float64)
    l = np.ascontiguousarray(l, dtype=np.float64)
    c = np.ascontiguousarray(c, dtype=np.float64)
    ts = np.ascontiguousarray(ts, dtype=np.float64)
    target = np.ascontiguousarray(target, dtype=np.float64)
    if fund_ts is None:
        fund_ts = np.zeros(0, dtype=np.float64)
        fund_rate = np.zeros(0, dtype=np.float64)
    else:
        fund_ts = np.ascontiguousarray(fund_ts, dtype=np.float64)
        fund_rate = np.ascontiguousarray(fund_rate, dtype=np.float64)
    return _simulate(o, h, l, c, ts, target, fund_ts, fund_rate,
                     float(fee_rate), float(slippage), float(sl), float(tp), float(mmr),
                     float(cash0))
