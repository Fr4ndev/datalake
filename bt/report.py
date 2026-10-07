"""Genera runs/{run_id}/report.md a partir del registro del ensayo."""
from __future__ import annotations

import json
import os


def escribir(result: dict, salida: str | None = None) -> str:
    fila = result["fila"]
    m = fila["metrics"]
    gates = result["gates"]
    spec = fila["spec_json"]
    fallidos = [g["nombre"] for g in gates if not g["pasa"]]

    lineas = [
        f"# {spec.get('name', fila['family_id'])} — {fila['veredicto']}",
        "",
        f"- run_id: `{fila['run_id']}`",
        f"- family_id: `{fila['family_id']}`  |  n_trials: **{m.get('n_trials', 1)}**",
        f"- spec_hash: `{fila['spec_hash']}`",
        f"- data_snapshot: `{fila['data_snapshot']}`",
        f"- git_sha: `{fila['git_sha']}`  |  seed: `{fila['seed']}`",
        f"- split: **{fila['split']}**  |  periodo: `{fila['periodo']}`",
        f"- params: `{json.dumps(fila['params'], sort_keys=True)}`",
        "",
        "## Hipótesis",
        "",
        spec.get("hypothesis", "_sin hipótesis declarada_"),
        "",
        "## Veredicto",
        "",
        (f"**{fila['veredicto']}**" if not fallidos
         else f"**{fila['veredicto']}** — gates en rojo: {', '.join(fallidos)}"),
        "",
        "## Gates",
        "",
        "| gate | valor | umbral | estado |",
        "|---|---:|---|:--:|",
    ]
    for g in gates:
        estado = "PASS" if g["pasa"] else "FAIL"
        lineas.append(f"| {g['nombre']} | {g['valor']} | {g['umbral']} | {estado} |")

    lineas += [
        "",
        "## Métricas",
        "",
        "| métrica | valor |",
        "|---|---:|",
        f"| Sharpe (diario, anualizado) | {m.get('sharpe')} |",
        f"| IC 95% (block bootstrap) | [{m.get('ic95_lo')}, {m.get('ic95_hi')}] |",
        f"| Deflated Sharpe | {m.get('dsr')} |",
        f"| p de entradas aleatorias | {m.get('p_aleatorias')} |",
        f"| n_trades | {m.get('n_trades')} |",
        f"| retorno total | {m.get('retorno_total')} |",
        f"| max drawdown | {m.get('max_dd')} |",
        f"| MC maxDD p95 | {m.get('mc_maxdd_p95')} |",
        f"| años con Sharpe>0 | {m.get('frac_anios_pos')} de {m.get('n_anios')} |",
        f"| funding total | {m.get('funding_total')} |",
        f"| vecindad ±20% (Sharpe medio) | {m.get('vecindad_sharpe_media')} |",
        f"| costes ×2 (Sharpe) | {m.get('costes_x2_sharpe')} |",
        f"| retraso +1 barra (Sharpe) | {m.get('retraso_1barra_sharpe')} |",
        f"| duración del run | {m.get('segundos')} s |",
        "",
        "## Costes y semántica",
        "",
        f"- fee taker: {spec.get('costs', {}).get('fee_taker_bps')} bps por lado",
        f"- slippage: {spec.get('costs', {}).get('slippage_bps')} bps por lado",
        "- señal con barras cerradas; orden ejecutada en el OPEN de t+1",
        "- funding aplicado en su timestamp sobre la posición ya actualizada por el fill",
        "",
    ]

    texto = "\n".join(lineas) + "\n"
    if salida is None:
        salida = os.path.join("runs", fila["run_id"], "report.md")
    os.makedirs(os.path.dirname(salida), exist_ok=True)
    with open(salida, "w") as fh:
        fh.write(texto)
    with open(os.path.join(os.path.dirname(salida), "metrics.json"), "w") as fh:
        json.dump({"metrics": m, "gates": gates, "veredicto": fila["veredicto"]},
                  fh, indent=2, sort_keys=True)
    return salida
