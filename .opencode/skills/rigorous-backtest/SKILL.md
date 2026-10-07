---
name: rigorous-backtest
description: Motor y protocolo de backtesting riguroso sobre el lake Parquet (futuros perpetuos 1m): ejecución sin lookahead, costes y funding reales, splits con holdout bloqueado, registro de ensayos, gates estadísticos y tests canario del propio motor. Usar en cualquier tarea de backtest.
---

# Principio
Un backtest bonito no es evidencia. Cada resultado debe poder rechazarse. El sistema está diseñado para que sea difícil engañarse: costes siempre activos, holdout bloqueado, todos los ensayos registrados y veredicto automático.

# Datos
- Entrada SOLO desde el lake Parquet (nunca TSDB para históricos), con TimeZone='UTC'. Hoy: BTCUSDT 1m (2019-12-31→hoy), fundingRate, metrics 5m (OI y ratios, solo desde 2020-09-01, con known_gaps).
- Cada run guarda data_snapshot = sha256 de manifest.jsonl + known_gaps.json.
- Nunca interpolar precios; barra ausente = sin operar. Descartar las primeras N barras de warm-up de cada indicador.

# Semántica de ejecución (sin lookahead)
1. Señal con barras CERRADAS hasta t; orden ejecutada en el OPEN de t+1. Cualquier otra opción requiere flag explícito y queda marcada en el informe.
2. El DSL/parser solo permite referencias al pasado (shift ≥ 0). Rechazar cualquier acceso al futuro.
3. Stops/targets intrabarra con OHLC: si SL y TP caen en la misma barra → SL primero. Gap a través del stop → fill al open (peor caso). Órdenes límite: fill solo si el precio atraviesa el nivel, nunca "al toque".
4. Costes SIEMPRE activos (no existe modo sin costes): fee taker por lado (default 0.05%, verificar la tarifa real de la cuenta), slippage default 2 bps por lado, ambos parametrizables.
5. Funding: en cada timestamp de funding, pnl −= nocional_abierto × rate (el long paga con rate > 0). Usar la tabla fundingRate del lake con el intervalo real.
6. Apalancamiento máx. 3x por defecto; modelar liquidación aproximada (margen aislado) y marcar el run si se acerca. Sizing fijo (fracción o nocional); sin composición salvo que la spec lo declare.

# Splits (se congelan ANTES del primer ensayo)
- Desarrollo: 2020-01 → 2023-12 (walk-forward dentro). Validación: 2024. HOLDOUT: 2025-01 → hoy, BLOQUEADO.
- El runner rechaza cualquier run que toque el holdout salvo `--final`, permitido UNA sola vez por spec_hash. Se registra siempre.
- Las fechas viven en config versionada con hash; cambiarlas tras ver resultados invalida los ensayos previos y queda anotado.
- Walk-forward: optimizar en ventana IS, evaluar en OOS contiguo, rodar. Reportar SOLO la equity OOS concatenada; parámetros elegidos solo con datos IS.

# Registro de ensayos (anti data-snooping)
- Tabla bt_runs (migración nueva): run_id, family_id, spec_hash, spec_json, hypothesis, data_snapshot, git_sha, seed, split, periodo, params, métricas, n_trades, veredicto, created_at. SE REGISTRAN TODOS los runs, también los malos.
- family_id agrupa variantes de una misma idea. N_trials = nº de ensayos de la familia (y global); se usa para deflactar.
- La spec exige `hypothesis` (qué ineficiencia explota y por qué existe) y la rejilla de parámetros declarada ANTES de ejecutar.

# Gates estadísticos (veredicto automático)
- n_trades ≥ 100 en Desarrollo (configurable); si no → INCONCLUSIVE.
- Sharpe con IC 95% por block bootstrap sobre retornos diarios.
- Deflated Sharpe Ratio con N_trials de la familia; PBO (CSCV) cuando hay rejilla.
- Test de entradas aleatorias con la misma exposición y distribución de duración → p-value.
- Monte Carlo del orden de trades: distribución de maxDD (reportar p95).
- Estabilidad: Sharpe por año positivo en ≥ 70% de los años.
- Robustez: vecindad de parámetros ±20% (exigir meseta, no pico), costes ×2, retraso de ejecución +1 barra, desglose por régimen (volatilidad por terciles, tendencia vs rango).
- Fuera de activo: con más símbolos en el lake, evaluar SIN re-optimizar en ≥ 3 símbolos adicionales.
- Veredictos: REJECT / INCONCLUSIVE / CANDIDATE (pasa Desarrollo + Validación) / CONFIRMED (pasa holdout, un disparo). Cada veredicto lista el gate que falló.

# Tests del propio motor (canarios; obligatorios en CI)
1. Fixture manual de 10 trades con fees, slippage y funding calculados a mano: coincide al céntimo.
2. Buy&hold = retorno del precio menos los costes de una entrada y una salida.
3. Estrategia sin señales → equity plana, 0 trades.
4. Canario aleatorio: 200 estrategias de entradas aleatorias con dirección long/short SIMÉTRICA (50/50, para que el drift de BTC no sesgue) → la media del *t* de las 200 es significativamente negativa (< −3·SE) por arrastre de costes, ninguna sale a >4σ de su propia distribución, y como CONTRASEÑA la misma tanda sin costes da media ≈ 0. OJO: el *t* va sobre las observaciones, NO sobre el Sharpe anualizado (que escala por √525600 y daba 196/200 "significativas" de mentira); y el *t* paramétrico por estrategia tampoco vale, porque las exposiciones son bloques correlacionados sobre una trayectoria de mercado fija.
5. Monotonía de costes: más fee/slippage ⇒ menor retorno, siempre.
6. El DSL rechaza accesos al futuro (test con `close[+1]`).
7. Determinismo: mismo spec_hash + data_snapshot + seed ⇒ métricas idénticas (`bt rerun <run_id>`).
8. Motor propio vs vectorbt, en configuración SIN stops y SIN funding (lo único que hace que las dos semánticas difieran, decisión B1): mismos trades y misma equity con tolerancia 1e-6, y con ≥20 órdenes para que el canario no sea vacío. La señal se desplaza una barra, porque nosotros decidimos en el cierre y ejecutamos en el open siguiente.

# Salida
runs/{run_id}/: report.md (veredicto + gates + IC), metrics.json, equity.png, trades.parquet, spec.yaml. Cualquier interfaz (CLI o Telegram) devuelve veredicto, N_trials de la familia y el gate que falló, y NO puede saltarse el lock de holdout ni el registro.

# Operación (medido en el N100, 4核)
- El runner paraleliza rejilla y aleatorias con `multiprocessing` en **fork** (`fork`, no `spawn`: con `spawn` cada hijo recompila numba) usando `min(4, cpus-1)` workers. Los datos se ponen en un global ANTES de crear el Pool y los hijos los heredan por copy-on-write; nada se serializa. Workers = 1 ⇒ modo serie, que es el que usan los canarios.
- Solo los parametros elegidos reciben las metricas caras (IC, Monte Carlo, estabilidad anual). A los vecinos descartados les basta Sharpe y nº de operaciones. El Monte Carlo adapta `n_sim` al nº de trades.
- Tiempos reales: `bt run --split dev` completo = **127 s**; 1 combinación = 0,82 s; **1.000 combinaciones = 819 s**; `bt rerun` ≈ 130 s. Si un run no termina en ~10 min, mide antes de tocar nada.
- `bt rerun` excluye `segundos` (tiempo de pared) de la comparación; todo lo demás debe coincidir bit a bit.

# Criterio de done
Los 8 canarios en verde; un EMA-cross trivial recorre todo el pipeline y da veredicto (se espera REJECT o INCONCLUSIVE tras costes + funding); tocar el holdout sin --final falla; rerun reproduce las métricas.
