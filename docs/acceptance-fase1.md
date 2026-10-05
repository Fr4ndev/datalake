# Aceptacion Fase 1 — Binance UM BTCUSDT

Fecha: 2026-10-04. Alcance: `klines 1m`, `fundingRate 8h`, `metrics 5m`.

## Resultado

| Producto | Periodos | Filas | Particiones | Huecos no explicados | Duplicados | Outliers |
|---|---|---|---|---|---|---|
| klines 1m | 84 | 3.553.920 | 8 | **0** | **0** | **0** |
| fundingRate 8h | 82 | 7.405 | 7 | **0** | **0** | **0** |
| metrics 5m | 2.223 | 639.590 | 7 | **0** | **0** | **0** |

`metrics` tiene 156 rangos sin datos (634 slots de 5 m). **No son 156 huecos abiertos**: estan
registrados en `lake/known_gaps.json` como **`vision_daily_missing_slot`** (ver mas abajo).

> **Como hay que citarlos: "faltan en el archivo de Vision". NO "confirmados sin dato en origen".**
> Lo unico verificado es que (a) los slots no estan en los zips de Vision y (b) el exchange estaba
> vivo y publicando durante esas ventanas. **No** esta verificado que el exchange publicara esos
> puntos de open interest, porque `/futures/data/openInterestHist` no conserva mas de ~30 dias y
> devuelve HTTP 400 para 2024. Son huecos del archivo, no huecos probados del exchange. Si alguien
> cita estos 156 como "verificados en origen", esta citando algo mas fuerte que lo medido.

## Checklist

| # | Criterio | Estado | Como se verifica |
|---|---|---|---|
| 1 | Los 3 productos descargados de `data.binance.vision` | ✅ | `docker-compose run --rm -T --no-deps bulk python -m bulk download --dtype metrics --symbol BTCUSDT --tf 5m --start 2020-09-01` |
| 2 | Sin periodos `failed` tras reintentar | ✅ | `grep -c '"status": "failed"' lake/manifest.jsonl` → ver nota (1) |
| 3 | Reanudable sin duplicar (SIGKILL) | ✅ | ver nota (2) |
| 4 | 0 huecos no explicados | ✅ | `docker-compose run --rm -T --no-deps bulk python -m bulk gap-scan --dtype metrics --symbol BTCUSDT --start 2020-09-01 --fail-on-crit` → `crit=0`, exit 0 |
| 5 | 0 duplicados y 0 outliers | ✅ | mismo comando, `duplicates=0 outliers=0` |
| 6 | `known_gaps.json` solo con huecos de los que hay evidencia | ✅ | ver "known_gaps.json" mas abajo |
| 7 | Timestamps UTC, nunca naive ni zona local | ✅ | `docker-compose run --rm -T --no-deps bulk python -c "import duckdb; print(duckdb.connect().execute(\"describe select open_time from read_parquet('lake/binance/klines/**/*.parquet',union_by_name=true)\").fetchall()[0])"` → `TIMESTAMP WITH TIME ZONE` |
| 8 | Header autodetectado (klines cambia en 2022-01) | ✅ | `head -c 120 lake/_qa/report-*.md` no aplica; ver `docs/source-survey.md` y `bulk/parser.py::looks_like_header` |
| 9 | DuckDB como validador, no pandas | ✅ | `docker-compose run --rm -T --no-deps bulk python -m bulk gap-scan --dtype klines --symbol BTCUSDT --tf 1m --start 2019-12-31 --fail-on-crit` |
| 10 | Tests del modulo | ✅ | `.venv/bin/python -m pytest -q` → **110 passed, 1 skipped** |
| 11 | lake escribible por el usuario, no root | ✅ | `stat -c '%U:%G' lake lake/manifest.jsonl lake/known_gaps.json` → `fran:fran` |
| 12 | Reports QA por producto | ✅ | `ls lake/_qa/` → 3 JSON + 3 MD |
| 13 | Decision documentada para cada chose no cubierta | ✅ | `docs/decisions.md` → D1..D26 |

### Notas

1. `lake/manifest.jsonl` tiene **2470 lineas para 2389 periodos unicos**. Las 81 lineas extra son
   exactamente el historico `("failed", "done")` de los meses de `fundingRate` que fallaron por el
   bug de cast de `split_by_year` y se reintentaron. El manifest es append-only, asi que el evento
   `failed` historico se conserva a proposito: **no hay ningun periodo en estado final `failed`**.

   ```bash
   .venv/bin/python -c "
   import json,collections
   last={}
   for l in open('lake/manifest.jsonl'):
       r=json.loads(l); last[(r['dtype'],r['period'])]=r
   print(collections.Counter(r['status'] for r in last.values()))"   # -> Counter({'done': 2389})
   ```

2. AC de reanudacion: con `metrics` a mitad (~1812 periodos, 522.665 filas) se ejecuto
   `docker kill -s KILL`. Todos los Parquet seguido legibles, sin `.tmp` huerfanos, y el siguiente
   `download` completo los 405 periodos restantes sin duplicar.

3. Al auditar los datos encontre que mi propio query de conteo por ano estaba mal: `year()` sobre
   `TIMESTAMP WITH TIME ZONE` usa la zona **local del host** (Europe/Madrid), no UTC, y desplazaba
   60 filas de 2019 a 2026. Las tablas de abajo estan calculadas con `SET TimeZone='UTC'`.

## Tablas de filas esperadas vs obtenidas

### klines 1m — 2019-12-31 → 2026-10-02

Esperado = minutos exactos del rango (la cuadricula es perfecta, no hay jitter).

| Año | Esperado | Obtenido | Diff |
|---|---|---|---|
| 2019 | 1.440 | 1.440 | 0 |
| 2020 | 527.040 | 527.040 | 0 |
| 2021 | 525.600 | 525.600 | 0 |
| 2022 | 525.600 | 525.600 | 0 |
| 2023 | 525.600 | 525.600 | 0 |
| 2024 | 527.040 | 527.040 | 0 |
| 2025 | 525.600 | 525.600 | 0 |
| 2026 | 396.000 | 396.000 | 0 |
| **Total** | **3.553.920** | **3.553.920** | **0** |

### fundingRate 8h — 2020-01-01 → 2026-10-03

Esperado = 3 eventos/dia x dias, calculados sobre el rango real de cada mes cerrado. Binance publica
exactamente 3 al dia (00:00, 08:00, 16:00).

| Año | Esperado | Obtenido | Diff |
|---|---|---|---|
| 2020 | 1.098 | 1.098 | 0 |
| 2021 | 1.095 | 1.095 | 0 |
| 2022 | 1.095 | 1.095 | 0 |
| 2023 | 1.095 | 1.095 | 0 |
| 2024 | 1.098 | 1.098 | 0 |
| 2025 | 1.095 | 1.095 | 0 |
| 2026 | 828 | 828 | 0 |
| **Total** | **7.404** | **7.405** | **+1** |

El `+1` es el evento REST de `2026-10-03 16:00`, que entro entre la descarga inicial y el test de
idempotencia. Es el comportamiento correcto: el mes en curso avanza. Sigue sin duplicados
(`count(*) == count(distinct calc_time) == 7405`).

### metrics 5m — 2020-09-01 → 2026-10-02

Esperado = 288 slots/dia x dias presentes en el lake. Los duplicados (576 filas/dia en el zip) ya
están eliminados en la escritura, asi que la columna "esperado" es sobre filas unicas.

| Año | Días | Esperado | Obtenido | Diff | Registrado en `known_gaps.json` |
|---|---|---|---|---|---|
| 2020 | 122 | 35.136 | 35.103 | −33 | 4 rangos |
| 2021 | 365 | 105.120 | 104.652 | −468 | 148 rangos |
| 2022 | 365 | 105.120 | 105.120 | **0** | — |
| 2023 | 365 | 105.120 | 105.117 | −3 | 1 rango |
| 2024 | 366 | 105.408 | 105.281 | −127 | 2 rangos |
| 2025 | 365 | 105.120 | 105.117 | −3 | 1 rango |
| 2026 | 275 | 79.200 | 79.200 | **0** | — |
| **Total** | **2.223** | **640.224** | **639.590** | **−634** | **156 rangos** |

## known_gaps.json

- **156 rangos, 634 slots**, todos de `metrics 5m`.
- `reason`: `vision_daily_missing_slot` (156/156). El nombre del campo ya dice lo que es: un slot
  que falta en el archivo de Vision.
- `verified_by`: `/fapi/v1/klines (exchange vivo; openInterestHist sin retencion)` (1 fuente). El
  campo se llama `verified_by` por el contrato del schema, pero **lo que verifica es que el exchange
  funcionaba, no que el dato existiera**. Leerlo asi es un error de interpretacion.
- Rango: `2020-09-27T11:15:00+00:00` → `2025-08-29T06:30:00+00:00`.
- Distribucion por tamaño: 108 rangos de 1 slot, 17 de 2, y uno de 125 slots
  (`2024-02-16 13:35` → `2024-02-16 23:55`).
- `klines` y `fundingRate` **no aportan ni un hueco**.

Que significa cada `note`, sin adornos:

> el exchange publico velas de 1m continuas en la ventana, asi que no fue una caida suya: el punto
> falta en el zip de Vision. No se puede rellenar porque openInterestHist solo conserva ~30 dias

Lo que esto prueba y lo que no:

- **Si prueba** que el exchange estaba vivo y publicando durante la ventana, asi que el hueco no se
  explica por una caida del exchange.
- **No prueba** que el exchange publicara ese punto de open interest. `/futures/data/openInterestHist`
  devuelve **HTTP 400** para fechas de 2024, asi que no hay forma de confirmarlo ni de descartarlo.
  Por eso el `note` lo dice en vez de presentar el hueco como confirmado en origen.

En resumen: son huecos **del archivo de Vision**, no huecos **del exchange**. La distincion importa
porque un hueco del exchange no se arregla y uno del archivo se podria rellenar con otra fuente si
alguna vez aparece. Ninguna de las dos ha cambiado el resultado (0 huecos sin explicar en el
gap-scan), pero la etiqueta correcta es "faltan en Vision".

Los huecos que no se pueden confirmar con **ninguna** fuente no se registran. En esta ejecucion no
hubo ninguno: los 156 tienen klines como segunda fuente. Si en el futuro un hueco cae fuera del
rango de klines consultable, se queda sin registrar y el gap-scan lo seguira marcando como CRIT,
que es el comportamiento correcto (preferimos un CRIT visible a un hueco dado por bueno).

Comprobar el fichero:

```bash
.venv/bin/python -c "
import json,collections
d=json.load(open('lake/known_gaps.json'))
print('gaps',len(d['gaps']),'slots',sum(g['rows'] for g in d['gaps']))
print(collections.Counter(g['reason'] for g in d['gaps']))
print(json.dumps(d['gaps'][0],indent=2,ensure_ascii=False))"
```

Regenerarlo (idempotente: no duplica entradas):

```bash
docker-compose run --rm -T --no-deps bulk python -m bulk known-gaps --dtype metrics --symbol BTCUSDT --probe 60
```

## Fuera de alcance

- `aggTrades` (45 GB, ~7.4 M filas/mes) → D20.
- Las 4 columnas de ratios long/short de `metrics` en el mes en curso: `openInterestHist` no las
  devuelve. historical viene de los zips diarios; el mes en curso las dejara vacias hasta que las
  llene el WS daemon → D19.
- `status=unavailable` en el manifest para OKX/Bitget/Hyperliquid: su valor es el WS daemon, no el
  backfill de historico profundo → D15.
