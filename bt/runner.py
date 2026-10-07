"""Ejecuta un ensayo: datos -> señales -> motor -> gates -> registro -> reporte.

Determinista de punta a punta: misma spec + mismo seed + mismo data_snapshot reproducen las
mismas metricas (canario 7).
"""
from __future__ import annotations

import hashlib
import json
import multiprocessing as mp
import os
import pathlib
import time

import numpy as np
import yaml

from bt import config, data, metrics, registry, signals
from bt.engine import simulate

SEED_DEFECTO = 20261007

# Estado de los workers. Se rellena ANTES de crear el Pool: con fork los hijos heredan estas
# referencias por copy-on-write, asi que los 2,1 M de barras no se copian ni se serializan.
_W: dict = {}


def _trabajadores() -> int:
    return max(1, min(4, (os.cpu_count() or 2) - 1))


def _pool(workers: int):
    """None si workers<=1 (modo serie, que es lo que usan los canarios)."""
    if workers <= 1:
        return None
    # fork, no spawn: spawn volveria a importar el modulo (y a compilar numba) en cada hijo
    return mp.get_context("fork").Pool(processes=workers)


def git_sha() -> str:
    """SHA corto del commit sin depender del binario `git` (la imagen loader no lo lleva).
    Lee `.git/HEAD` y la ref a la que apunta, con `packed-refs` como respaldo."""
    try:
        raiz = pathlib.Path.cwd()
        while raiz != raiz.parent and not (raiz / ".git").exists():
            raiz = raiz.parent
        git = raiz / ".git"
        if not git.is_dir() and not git.is_file():
            return "desconocido"
        if git.is_file():                                  # worktree: gitdir: /ruta
            git = pathlib.Path(git.read_text().split("gitdir:", 1)[1].strip())
        head = (git / "HEAD").read_text().strip()
        if not head.startswith("ref:"):
            return head[:12]
        ref = head.split(":", 1)[1].strip()
        destino = git / ref
        if destino.exists():
            return destino.read_text().strip()[:12]
        with open(git / "packed-refs") as fh:               # rama ya empaquetada
            for linea in fh:
                if linea.startswith("#"):
                    continue
                sha, _, nombre = linea.rstrip("\n").partition(" ")
                if nombre == ref:
                    return sha[:12]
        return "desconocido"
    except Exception:
        return "desconocido"


def cargar_spec(ruta: str) -> tuple[dict, str]:
    with open(ruta) as fh:
        spec = yaml.safe_load(fh)
    canonico = json.dumps(spec, sort_keys=True, ensure_ascii=False)
    return spec, hashlib.sha256(canonico.encode()).hexdigest()


def rejilla(spec: dict) -> list[dict]:
    """Vecindad ±20% de cada parametro declarado. Sirve a la vez para exigir meseta (robustez)
    y para contar N_trials de la familia (Deflated Sharpe)."""
    base = spec.get("params", {})
    ejes: list[list[int]] = []
    for k, v in base.items():
        if not isinstance(v, (int, float)) or isinstance(v, bool):
            continue
        p = int(v)
        lo, hi = max(1, round(p * 0.8)), max(1, round(p * 1.2))
        paso = max(1, round((hi - lo) / 6)) if hi > lo else 1
        ejes.append(list(range(lo, hi + 1, paso)))
    combos: list[dict] = []
    if not ejes:
        return [dict(base)]
    import itertools
    for valores in itertools.product(*ejes):
        combos.append(dict(zip(base.keys(), valores)))
    return [c for c in combos if c.get("slow", 1) > c.get("fast", 0)] or [dict(base)]


def _cargar(spec: dict, split: str) -> dict:
    desde, hasta = config.ventana(split)
    ohlcv = data.cargar_ohlcv(desde=desde, hasta=hasta)
    fund = data.cargar_funding(desde=desde, hasta=hasta)
    return {**ohlcv, **fund}


def _simular(datos: dict, params: dict, fee_bps: float, slip_bps: float,
             cash0: float, apal: float) -> dict:
    target = signals.target_ema_cross(
        datos["c"], int(params["fast"]), int(params["slow"]),
        cash0=cash0, apalancamiento=apal)
    equity, f_ts, f_px, f_side, fund_total = simulate(
        datos["o"], datos["h"], datos["l"], datos["c"], datos["ts"], target,
        fund_ts=datos["fund_ts"], fund_rate=datos["fund_rate"],
        fee_rate=fee_bps / 10_000.0, slippage=slip_bps / 10_000.0,
        cash0=cash0)
    return {"equity": equity, "fill_ts": f_ts, "fill_px": f_px,
            "fill_side": f_side, "funding": fund_total, "target": target}


def _metricas(res: dict, datos: dict) -> dict:
    eq, ts = res["equity"], datos["ts"]
    rets_dia = metrics.retornos_diarios(eq, ts)
    sr = metrics.sharpe(rets_dia)
    ic_lo, ic_hi = metrics.ic_sharpe(rets_dia)
    n_trades = int(res["fill_ts"].size)

    # retornos por trade para el Monte Carlo del orden
    tr = _retornos_por_trade(res["fill_px"], res["fill_side"])
    dd_medio, dd_p95 = metrics.monte_carlo_maxdd(tr if tr.size else np.array([0.0]))
    frac_anios, n_anios = metrics.estabilidad_anual(eq, ts)

    g3 = float(st_skew(rets_dia))
    g4 = float(st_kurt(rets_dia))

    return {
        "sharpe": round(sr, 6),
        "ic95_lo": round(ic_lo, 6),
        "ic95_hi": round(ic_hi, 6),
        "n_trades": n_trades,
        "max_dd": round(metrics.max_drawdown(eq), 6),
        "retorno_total": round(float(eq[-1] / eq[0] - 1.0), 6) if eq.size else 0.0,
        "funding_total": round(float(res["funding"]), 6),
        "mc_maxdd_medio": round(dd_medio, 6),
        "mc_maxdd_p95": round(dd_p95, 6),
        "frac_anios_pos": round(float(frac_anios), 4),
        "n_anios": int(n_anios),
        "skew": round(g3, 6),
        "kurtosis": round(g4, 6),
    }


def _retornos_por_trade(px: np.ndarray, lado: np.ndarray) -> np.ndarray:
    """Empareja cada apertura con el cierre siguiente (lado opuesto) y devuelve el retorno."""
    out = []
    entrada = None
    for p, s_ in zip(px, lado):
        if entrada is None:
            entrada = (p, s_)
            continue
        if s_ * entrada[1] >= 0:            # no cierra: reabre o ignora
            entrada = (p, s_)
            continue
        base = entrada[0]
        if base == 0:
            entrada = None
            continue
        out.append((p - base) / base if entrada[1] > 0 else (base - p) / base)
        entrada = None
    arr = np.asarray(out, dtype=np.float64)
    return arr[np.isfinite(arr)] if arr.size else np.zeros(0)


def _sharpe_rapido(res: dict, datos: dict) -> dict:
    """Metricas que la rejilla necesita: solo Sharpe y nº de operaciones.

    El resto (IC, Monte Carlo, estabilidad anual, sesgo) es ~10 s y solo importa para los
    parametros elegidos, no para los 45 o 1000 vecinos descartados.
    """
    rets = metrics.retornos_diarios(res["equity"], datos["ts"])
    return {"sharpe": float(metrics.sharpe(rets)),
            "n_trades": int(res["fill_ts"].size)}


def _t_rejilla(params: dict) -> dict:
    datos, cfg = _W["datos"], _W["cfg"]
    res = _simular(datos, params, *cfg)
    m = _sharpe_rapido(res, datos)
    m["p"] = params
    return m


def _t_aleatoria(clave: tuple) -> float:
    """Una estrategia aleatoria con la misma exposicion y duracion (canario 4)."""
    datos, cfg = _W["datos"], _W["cfg"]
    dur, tam, med = _W["duraciones"], _W["tamano"], _W["mediana"]
    semilla, k = clave
    # semilla independiente por simulacion: el orden de ejecucion no puede cambiar el resultado
    rng = np.random.default_rng(np.random.SeedSequence([semilla, k]))
    c = datos["c"]
    t = np.zeros(c.shape[0])
    i = int(rng.integers(0, max(1, c.shape[0] - 60)))
    while i < c.shape[0] - 1:
        d = int(rng.choice(dur))
        signo = 1.0 if rng.random() < 0.5 else -1.0
        fin = min(i + d, c.shape[0] - 1)
        t[i:fin] = signo * tam
        i = fin + int(rng.integers(1, max(2, int(med))))
    r = simulate(datos["o"], datos["h"], datos["l"], c, datos["ts"], t,
                 fund_ts=datos["fund_ts"], fund_rate=datos["fund_rate"],
                 fee_rate=cfg[0] / 10_000.0, slippage=cfg[1] / 10_000.0,
                 cash0=cfg[2])[0]
    return float(metrics.sharpe(metrics.retornos_diarios(r, datos["ts"])))


def _calentar(datos: dict, params: dict, cfg: tuple) -> None:
    """Compila numba en el proceso padre: con fork los hijos heredan la version compilada."""
    _simular(datos, params, *cfg)


def st_skew(x: np.ndarray) -> float:
    if x.size < 3 or np.std(x) == 0:
        return 0.0
    z = (x - x.mean()) / x.std()
    return float(np.mean(z ** 3))


def st_kurt(x: np.ndarray) -> float:
    if x.size < 4 or np.std(x) == 0:
        return 3.0
    z = (x - x.mean()) / x.std()
    return float(np.mean(z ** 4))


def _aleatorias(datos: dict, params: dict, fee_bps: float, slip_bps: float,
                cash0: float, apal: float, n: int = 200, semilla: int = SEED_DEFECTO,
                workers: int = 1) -> dict:
    """Misma exposicion y duracion que la estrategia real, direccion simetrica (canario 4)."""
    objetivo = signals.target_ema_cross(datos["c"], int(params["fast"]),
                                        int(params["slow"]), cash0, apal)
    activa = objetivo != 0.0
    if activa.sum() < 10:
        return {"sharpes": np.zeros(0)}
    duraciones = _duracion_bloques(activa)
    tamano = float(np.median(np.abs(objetivo[activa])))
    claves = [(semilla, k) for k in range(n)]
    _W["duraciones"], _W["tamano"] = duraciones, tamano
    _W["mediana"] = float(np.median(duraciones))
    pool = _pool(workers)
    if pool is None:
        sharpes = np.array([_t_aleatoria(c) for c in claves])
    else:
        with pool:
            sharpes = np.array(pool.map(_t_aleatoria, claves, chunksize=1))
    return {"sharpes": sharpes}


def _duracion_bloques(activa: np.ndarray) -> np.ndarray:
    difs = np.diff(np.concatenate([[False], activa, [False]]).astype(np.int8))
    ini = np.where(difs == 1)[0]
    fin = np.where(difs == -1)[0]
    d = fin - ini
    return d[d > 0] if d.size else np.array([1])


def _p_aleatorias(sharpes: np.ndarray, sr_real: float) -> float:
    if sharpes.size == 0:
        return 1.0
    return float(np.mean(sharpes >= sr_real))


def ejecutar(ruta_spec: str, split: str = "dev", semilla: int = SEED_DEFECTO,
             final: bool = False, dry: bool = False) -> dict:
    t0 = time.time()
    spec, spec_hash = cargar_spec(ruta_spec)
    desde, hasta = config.ventana(split)
    config.exige_final(desde, hasta, split, final)          # AC: tocar el holdout sin --final falla

    snapshot = data.snapshot()
    fee = float(spec.get("costs", {}).get("fee_taker_bps", config.CFG["fee_taker_bps"]))
    slip = float(spec.get("costs", {}).get("slippage_bps", config.CFG["slippage_bps"]))
    cash0, apal = 1000.0, 1.0

    datos = _cargar(spec, split)
    combos = rejilla(spec)
    n_trials = len(combos)

    cfg = (fee, slip, cash0, apal)
    _W["datos"], _W["cfg"] = datos, cfg
    _calentar(datos, combos[0], cfg)               # compila numba antes del primer fork
    workers = _trabajadores()

    pool = _pool(workers)
    if pool is None:
        filas = [_t_rejilla(p) for p in combos]
    else:
        with pool:
            filas = pool.map(_t_rejilla, combos, chunksize=1)

    # el mejor de Desarrollo es el que se lleva a Validacion (parametros elegidos solo en IS)
    mejor_f = max(filas, key=lambda f: f["sharpe"])
    mejor_p = mejor_f["p"]
    vecindad = np.array([f["sharpe"] for f in filas], dtype=np.float64)
    mejor_res = _simular(datos, mejor_p, fee, slip, cash0, apal)
    mejor_m = _metricas(mejor_res, datos)
    mejor_m["dsr"] = round(metrics.deflated_sharpe(
        mejor_m["sharpe"],
        max(3, len(metrics.retornos_diarios(mejor_res["equity"], datos["ts"]))),
        mejor_m["skew"], mejor_m["kurtosis"], n_trials), 6)

    # aleatorias con la misma exposicion y duracion
    rng_info = _aleatorias(datos, mejor_p, fee, slip, cash0, apal, n=200,
                           semilla=semilla, workers=workers)
    p_rand = _p_aleatorias(rng_info.get("sharpes", np.zeros(0)), mejor_m["sharpe"])

    # robustez: costes x2 y +1 barra de retraso, sobre los parametros elegidos
    doble = _metricas(_simular(datos, mejor_p, fee * 2.0, slip * 2.0, cash0, apal), datos)
    retraso = _metricas(_simular_retraso(datos, mejor_p, fee, slip, cash0, apal), datos)

    gates = _gates(mejor_m, p_rand, vecindad, doble, retraso, n_trials)
    if mejor_m["n_trades"] < config.CFG["min_trades_dev"]:
        veredicto = "INCONCLUSIVE"
    elif all(g["pasa"] for g in gates):
        veredicto = "CANDIDATE" if split in ("dev", "val") else "CONFIRMED"
    else:
        veredicto = "REJECT"

    run_id = f"{spec_hash[:10]}-{split}-{semilla}-{int(time.time())}"
    fila = {
        "run_id": run_id, "family_id": spec.get("family_id", spec["name"]),
        "spec_hash": spec_hash, "spec_json": spec,
        "hypothesis": spec.get("hypothesis", ""), "data_snapshot": snapshot,
        "git_sha": git_sha(), "seed": semilla, "split": split,
        "periodo": f"{desde}..{hasta or 'abierto'}", "params": mejor_p,
        "metrics": {**mejor_m, "p_aleatorias": round(p_rand, 6),
                    "n_trials": n_trials,                     "costes_x2_sharpe": doble["sharpe"],
                    "retraso_1barra_sharpe": retraso["sharpe"],
                    "vecindad_sharpe_media": round(float(vecindad.mean()), 6),
                    "segundos": round(time.time() - t0, 2)},
        "n_trades": mejor_m["n_trades"], "veredicto": veredicto,
    }
    if not dry:
        registry.registrar(fila)
    return {"fila": fila, "gates": gates, "spec_hash": spec_hash, "run_id": run_id}


def _simular_retraso(datos: dict, params: dict, fee: float, slip: float,
                     cash0: float, apal: float) -> dict:
    """Robustez: la senal se evalua una barra mas tarde (coste de ejecucion real)."""
    obj = signals.target_ema_cross(datos["c"], int(params["fast"]), int(params["slow"]),
                                   cash0, apal)
    obj = np.concatenate([[0.0], obj[:-1]])           # +1 barra de retraso
    tmp = dict(datos)
    equity, f_ts, f_px, f_side, fund_total = simulate(
        datos["o"], datos["h"], datos["l"], datos["c"], datos["ts"], obj,
        fund_ts=datos["fund_ts"], fund_rate=datos["fund_rate"],
        fee_rate=fee / 10_000.0, slippage=slip / 10_000.0, cash0=cash0)
    return {"equity": equity, "fill_ts": f_ts, "fill_px": f_px,
            "fill_side": f_side, "funding": fund_total, "target": obj}


def _gates(m: dict, p_rand: float, vecindad: np.ndarray, doble: dict,
           retraso: dict, n_trials: int) -> list[dict]:
    cf = config.CFG
    n_vec = vecindad.size
    # meseta: al menos la mitad de los vecinos mantiene medio Sharpe del elegido
    if m["sharpe"] > 0:
        meseta_frac = float(np.mean(vecindad >= 0.5 * m["sharpe"]))
    else:
        meseta_frac = 0.0
    g = [
        {"nombre": "n_trades_dev", "valor": m["n_trades"],
         "umbral": f">= {cf['min_trades_dev']}",
         "pasa": m["n_trades"] >= cf["min_trades_dev"]},
        {"nombre": "sharpe_dev", "valor": m["sharpe"], "umbral": "> 0",
         "pasa": m["sharpe"] > 0},
        {"nombre": "ic95_inferior", "valor": m["ic95_lo"], "umbral": "> 0",
         "pasa": m["ic95_lo"] > 0},
        {"nombre": "deflated_sharpe", "valor": m["dsr"],
         "umbral": f">= {cf['dsr_min']}", "pasa": m["dsr"] >= cf["dsr_min"]},
        {"nombre": "p_aleatorias", "valor": round(p_rand, 4), "umbral": "<= 0.05",
         "pasa": p_rand <= 0.05},
        {"nombre": "estabilidad_anual", "valor": m["frac_anios_pos"],
         "umbral": f">= {cf['estabilidad_min']}",
         "pasa": m["frac_anios_pos"] >= cf["estabilidad_min"]},
        {"nombre": "montecarlo_maxdd_p95", "valor": m["mc_maxdd_p95"],
         "umbral": "<= 0.50", "pasa": m["mc_maxdd_p95"] <= 0.50},
        {"nombre": "meseta_vecindad", "valor": round(meseta_frac, 4),
         "umbral": f">= 0.5 ({n_vec} combos)", "pasa": meseta_frac >= 0.5},
        {"nombre": "costes_x2_sharpe", "valor": doble["sharpe"], "umbral": "> 0",
         "pasa": doble["sharpe"] > 0},
        {"nombre": "retraso_1barra_sharpe", "valor": retraso["sharpe"], "umbral": "> 0",
         "pasa": retraso["sharpe"] > 0},
    ]
    return g


def rerun(run_id: str) -> dict:
    """Vuelve a ejecutar la spec y los parametros registrados y compara las metricas."""
    previo = registry.obtener(run_id)
    if previo is None:
        raise SystemExit(f"no existe el run {run_id}")
    spec = previo["spec_json"]
    import tempfile
    with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as fh:
        yaml.safe_dump(spec, fh)
        ruta = fh.name
    try:
        nuevo = ejecutar(ruta, split=previo["split"], semilla=int(previo["seed"]),
                         dry=True)
    finally:
        os.unlink(ruta)
    a, b = previo["metrics"], nuevo["fila"]["metrics"]
    # `segundos` es tiempo de pared: cambia en cada ejecucion y no puede entrar en la
    # comparacion de determinismo (canario 7).
    claves = sorted((set(a) & set(b)) - {"segundos"})
    difs = {k: (a[k], b[k]) for k in claves if isinstance(a[k], (int, float))
            and isinstance(b[k], (int, float)) and abs(a[k] - b[k]) > 1e-9}
    return {"run_id": run_id, "reproducido": not difs, "diferencias": difs,
            "n_claves": len(claves), "nuevo_run_id": nuevo["run_id"]}
