# ema_cross_trivial — REJECT

- run_id: `24374f7885-dev-20261007-1791376890`
- family_id: `ema_cross_trivial`  |  n_trials: **45**
- spec_hash: `24374f78856d8bda4a3ee520a5c6497d84768ea580ad4a107c02a6e5e5dabdf1`
- data_snapshot: `20cb964b0066250b19c7730df343b0d8373cad1fb81a54a71368041db8ef8a7d`
- git_sha: `e623d7979d73`  |  seed: `20261007`
- split: **dev**  |  periodo: `2020-01-01..2024-01-01`
- params: `{"fast": 10, "slow": 19}`

## Hipótesis

Cruce EMA rápido/lento es ruido tras costes+funding en BTC 1m (ruido de mercado, sin ineficiencia estable).

## Veredicto

**REJECT** — gates en rojo: sharpe_dev, ic95_inferior, deflated_sharpe, p_aleatorias, estabilidad_anual, meseta_vecindad, costes_x2_sharpe, retraso_1barra_sharpe

## Gates

| gate | valor | umbral | estado |
|---|---:|---|:--:|
| n_trades_dev | 1426 | >= 100 | PASS |
| sharpe_dev | -1.309695 | > 0 | FAIL |
| ic95_inferior | -2.084486 | > 0 | FAIL |
| deflated_sharpe | 0.0 | >= 0.95 | FAIL |
| p_aleatorias | 0.555 | <= 0.05 | FAIL |
| estabilidad_anual | 0.0 | >= 0.7 | FAIL |
| montecarlo_maxdd_p95 | 0.303071 | <= 0.50 | PASS |
| meseta_vecindad | 0.0 | >= 0.5 (45 combos) | FAIL |
| costes_x2_sharpe | -1.210387 | > 0 | FAIL |
| retraso_1barra_sharpe | -1.334526 | > 0 | FAIL |

## Métricas

| métrica | valor |
|---|---:|
| Sharpe (diario, anualizado) | -1.309695 |
| IC 95% (block bootstrap) | [-2.084486, 0.0] |
| Deflated Sharpe | 0.0 |
| p de entradas aleatorias | 0.555 |
| n_trades | 1426 |
| retorno total | -1.001133 |
| max drawdown | 1.001129 |
| MC maxDD p95 | 0.303071 |
| años con Sharpe>0 | 0.0 de 4 |
| funding total | 4.434233 |
| vecindad ±20% (Sharpe medio) | -1.353013 |
| costes ×2 (Sharpe) | -1.210387 |
| retraso +1 barra (Sharpe) | -1.334526 |
| duración del run | 127.35 s |

## Costes y semántica

- fee taker: 5.0 bps por lado
- slippage: 2.0 bps por lado
- señal con barras cerradas; orden ejecutada en el OPEN de t+1
- funding aplicado en su timestamp sobre la posición ya actualizada por el fill

