#!/usr/bin/env python
"""Bench del pipeline de backtest sobre una ventana arbitraria.

Para poder medir la ventana COMPLETA del lake (3,5 M de barras) sin tocar los splits, que
estan congelados en bt/config.py y solo se pueden usar via `bt run`. No registra nada: es una
medicion, no un ensayo.

    docker-compose --profile batch run --rm -T -v $(pwd):/quant -w /quant \
        loader python ops/bench_backtest.py [--desde D] [--hasta D] [--combos N] [--aleatorias N]

Los tiempos van a stdout con formato clave=valor (regla 9) y se recogen en docs/decisions.md.
"""
from __future__ import annotations

import argparse
import os
import sys
import time

# sirve tanto `python ops/bench_backtest.py` como `python -m ops.bench_backtest`
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bt import data, runner  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--desde", default="2019-12-31")
    ap.add_argument("--hasta", default="2026-10-02")
    ap.add_argument("--aleatorias", type=int, default=200)
    a = ap.parse_args()

    t0 = time.time()
    datos = {**data.cargar_ohlcv(desde=a.desde, hasta=a.hasta),
             **data.cargar_funding(desde=a.desde, hasta=a.hasta)}
    t_carga = time.time() - t0
    n = int(datos["ts"].size)
    print(f"component=bench step=carga barras={n} elapsed={t_carga:.2f}", flush=True)

    cfg = (5.0, 2.0, 1000.0, 1.0)
    runner._W["datos"], runner._W["cfg"] = datos, cfg
    runner._calentar(datos, {"fast": 9, "slow": 21}, cfg)
    workers = runner._trabajadores()
    combos = runner.rejilla({"params": {"fast": 9, "slow": 21}})

    t1 = time.time()
    pool = runner._pool(workers)
    if pool is None:
        filas = [runner._t_rejilla(p) for p in combos]
    else:
        with pool:
            filas = pool.map(runner._t_rejilla, combos, chunksize=1)
    t_grid = time.time() - t1
    mejor = max(filas, key=lambda f: f["sharpe"])
    print(f"component=bench step=rejilla barras={n} n_combos={len(combos)} "
          f"workers={workers} elapsed={t_grid:.1f} por_combo={t_grid / len(combos):.3f}",
          flush=True)

    t2 = time.time()
    runner._aleatorias(datos, mejor["p"], 5.0, 2.0, 1000.0, 1.0,
                       n=a.aleatorias, workers=workers)
    t_rand = time.time() - t2
    print(f"component=bench step=aleatorias n={a.aleatorias} workers={workers} "
          f"elapsed={t_rand:.1f}", flush=True)
    print(f"component=bench step=total barras={n} n_combos={len(combos)} "
          f"workers={workers} elapsed={time.time() - t0:.1f}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
