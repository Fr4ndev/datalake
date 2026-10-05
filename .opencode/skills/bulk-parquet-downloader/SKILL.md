---
name: bulk-parquet-downloader
description: Descarga históricos bulk de futuros perpetuos (Binance Vision, Bybit public, ccxt como fallback) a Parquet particionado con checksum, manifest, known_gaps y reanudación. Usar en cualquier backfill histórico.
---

# Fuentes por exchange
| Exchange | Fuente | Notas |
|---|---|---|
| Binance UM | https://data.binance.vision/data/futures/um/{monthly,daily}/{klines,fundingRate,metrics,aggTrades}/{SYM}/... | Cada zip tiene .CHECKSUM (sha256). |
| Bybit | https://public.bybit.com/trading/{SYM}/ | Trades diarios csv.gz. Klines vía API v5 paginada. |
| OKX / Bitget / Hyperliquid | ccxt async / SDK | Histórico PROFUNDO NO garantizado (ver abajo). |

# VERIFICAR ANTES DE CODIFICAR (regla dura)
Lista el bucket real (el índice S3 de data.binance.vision o `?prefix=`) para cada producto y confirma:
- qué granularidad existe (monthly, daily o ambas) por producto;
- fecha de inicio por símbolo;
- convención de timestamps.
No asumas rutas de memoria. Notas que debes comprobar: `metrics` parece existir SOLO como ficheros diarios (no mensuales); klines/funding/aggTrades tienen mensual y diario.

# Timestamps
- NO fiarse de "futuros=ms, spot=µs" (cambió con el tiempo y entre mercados).
- Autodetectar por magnitud de la columna: 13 dígitos = ms, 16 dígitos = µs. Normalizar SIEMPRE a timestamptz UTC.
- Test unitario con una fila de cada magnitud.

# Reglas duras
1. Concurrencia: 8-16 workers para Vision (S3); 1-4 para REST con rate-limit por exchange; backoff exponencial con jitter en 429.
2. Verificar sha256 del .CHECKSUM antes de descomprimir. Corrupto → reintento (5×), nunca conversión. Un archivo fallido NUNCA aborta el lote.
3. Manifest lake/manifest.jsonl: una línea por periodo {exchange, symbol, dtype, period, file, sha256, rows, status, note}. Re-ejecución salta status=done.
4. Escritura: zstd, statistics=True, row_group_size≈512k. Ruta: lake/{exchange}/{dtype}/symbol={SYM}/tf={TF}/year={YYYY}/part.parquet (trades: month={YYYY-MM}).
5. Dedup en escritura: sort por ts + drop_duplicates([symbol, ts]); merge con parquet existente + rename atómico.
6. Mes en curso: usar diarios; dedup contra el mensual al compactar.
7. Post-lote: gap-scan (skill data-quality-validator).

# known_gaps.json (clave para que "done" sea alcanzable)
- lake/known_gaps.json: huecos confirmados EN ORIGEN (mantenimientos, minutos sin datos de Binance).
- Un gap solo entra aquí si se verificó contra una segunda fuente (diario vs mensual, o REST) y sigue ausente.
- Cada entrada: {exchange, symbol, dtype, from, to, reason, verified_at}.
- DONE = 0 gaps NO EXPLICADOS (los listados en known_gaps no cuentan).

# Limitaciones por exchange (documentar en manifest con status=unavailable, no "arreglar")
- Hyperliquid: REST solo da unas ~5000 velas recientes → sin histórico 1m profundo. Su valor es el WS daemon desde hoy. Funding: 1h.
- OKX/Bitget: ccxt limita profundidad por timeframe; medir hasta dónde llega antes de prometer rango. Si no llega a 2020, anotar el inicio real.
- Binance liquidationSnapshot: solo ~2020-01→2021-12; después solo WS (y parcial, ~1 de cada 20).
- Binance OI histórico = producto `metrics` (OI + ratios a 5m), desde ~2022. No usar REST para backfill de OI.
- Funding Binance: 8h por defecto pero hay símbolos con 4h/1h → verificar intervalo por símbolo.

# Criterio de done
BTCUSDT 1m desde listing→hoy: 0 gaps no explicados; SIGKILL a mitad → re-ejecución reanuda sin duplicar filas; tabla filas esperadas vs obtenidas por año (525.600/año en 1m, menos en el año de listing y los known_gaps).