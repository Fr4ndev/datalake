"""CLI del backtester.

    bt run  specs/ema_cross_trivial.yaml [--split dev|val] [--seed N] [--final] [--dry]
    bt rerun <run_id>
    bt report <run_id>

El lock del holdout vive en bt/config.py y NO se puede saltar: sin `--final` lanza
HoldoutBloqueado; con `--final` solo se admite un disparo por spec_hash.
"""
from __future__ import annotations

import argparse
import sys

from bt import registry, report, runner


def main(argv: list[str] | None = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    if not argv:
        print(__doc__)
        return 1

    cmd, resto = argv[0], argv[1:]
    if cmd == "run":
        return _run(resto)
    if cmd == "rerun":
        return _rerun(resto)
    if cmd == "report":
        return _report(resto)
    print(f"comando desconocido: {cmd}\n{__doc__}")
    return 1


def _run(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(prog="bt run")
    ap.add_argument("spec")
    ap.add_argument("--split", default="dev", choices=["dev", "val", "holdout"])
    ap.add_argument("--seed", type=int, default=runner.SEED_DEFECTO)
    ap.add_argument("--final", action="store_true",
                    help="unico disparo permitido sobre el holdout")
    ap.add_argument("--dry", action="store_true", help="no registrar en bt_runs")
    a = ap.parse_args(argv)

    if a.final:
        n = registry.finals_registrados(_spec_hash(a.spec))
        if n >= 1:
            print(f"holdout: ya hay {n} disparo(s) para este spec_hash; uno solo")
            return 1

    try:
        res = runner.ejecutar(a.spec, split=a.split, semilla=a.seed,
                              final=a.final, dry=a.dry)
    except Exception as exc:                       # HoldoutBloqueado y demas
        print(f"error: {exc}")
        return 1

    ruta = report.escribir(res)
    _resumen(res, ruta)
    return 0 if res["fila"]["veredicto"] != "REJECT" else 0


def _rerun(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(prog="bt rerun")
    ap.add_argument("run_id")
    a = ap.parse_args(argv)
    out = runner.rerun(a.run_id)
    print(f"reproducido={out['reproducido']} claves_comparadas={out['n_claves']}")
    difs = out["diferencias"]
    if difs:
        for k, (x, y) in difs.items():
            print(f"  {k}: {x} != {y}")
        return 1
    return 0


def _report(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(prog="bt report")
    ap.add_argument("run_id")
    a = ap.parse_args(argv)
    fila = registry.obtener(a.run_id)
    if fila is None:
        print(f"no existe {a.run_id}")
        return 1
    res = {"fila": fila, "gates": _gates_desde_metricas(fila), "spec_hash": fila["spec_hash"],
           "run_id": fila["run_id"]}
    ruta = report.escribir(res)
    print(open(ruta).read())
    return 0


def _gates_desde_metricas(fila: dict) -> list[dict]:
    """Regenera la tabla de gates desde metrics.json ya persistido."""
    import json
    import os
    ruta = os.path.join("runs", fila["run_id"], "metrics.json")
    if os.path.exists(ruta):
        with open(ruta) as fh:
            return json.load(fh).get("gates", [])
    return []


def _resumen(res: dict, ruta: str) -> None:
    f, m = res["fila"], res["fila"]["metrics"]
    rojos = [g["nombre"] for g in res["gates"] if not g["pasa"]]
    print(f"veredicto={f['veredicto']} split={f['split']} "
          f"sharpe={m['sharpe']} n_trades={f['n_trades']} n_trials={m.get('n_trials')}")
    if rojos:
        print("gates_fallidos=" + ",".join(rojos))
    print(f"reporte={ruta}")


def _spec_hash(ruta: str) -> str:
    return runner.cargar_spec(ruta)[1]


if __name__ == "__main__":
    sys.exit(main())
