---
name: ingest-gap-repair
description: Detecta, registra y repara huecos de ingesta del daemon WebSocket (reconexiones, reinicios, cortes de red) con un ledger ingest_gaps y adaptadores REST por exchange. Usar en Fase 2 y siempre que se toque feed/ o repair/.
---

# Principio
El WebSocket NO reenvía lo perdido. La garantía no es "0 pérdidas" sino "0 pérdidas en silencio": detectar → registrar → reparar con la mejor fuente → declarar lo irrecuperable. Prohibido relajar un AC para que pase; el AC se define por tipo de dato (sección final).

# Ledger (migración NUEVA; no editar las existentes)
Tabla normal (no hypertable) ingest_gaps: id BIGSERIAL PK, exchange, symbol, dtype ('trades','candles','funding','open_interest','liquidations'), gap_from timestamptz, gap_to timestamptz, reason ('disconnect','restart','silence','id_jump'), status ('open','repairing','repaired','partial','unrecoverable'), source TEXT, rows_repaired INT, attempts INT, detected_at, updated_at, note TEXT. Índice por status. Nunca borrar filas: se cierran con status.
Además: columna source ('ws','rest','dump') en trades.

# Detección (dentro del daemon)
1. Al arrancar: por cada (exchange, symbol, dtype) con filas, abrir gap [max(ts) en BD, now()] reason=restart.
2. En cada reconexión: gap [último_ts_visto − 5 s, primer_ts_tras_reconectar + 5 s] reason=disconnect. Verificar en cryptofeed 3.0.1 qué hook/evento expone la reconexión.
3. Watchdog de silencio: canal líquido (trades BTC/ETH) sin eventos > 15 s → gap reason=silence; se cierra al llegar un evento.
4. Continuidad de ID donde es secuencial (Binance aggTrade `a`): si a_nuevo > a_previo + 1 → gap reason=id_jump, reparable con fromId.
5. Padding ±5 s siempre (el dedup absorbe el solape). Fusionar gaps solapados del mismo (exchange, symbol, dtype).

# Clave de dedup (CRÍTICO)
- PK trades: (exchange, symbol, ts, trade_id) (ts debe estar en la PK por la hypertable).
- Normalizar ts ANTES de insertar: cryptofeed entrega float en segundos → ts_ms = round(ts*1000) entero; REST ya viene en ms. Con floats, WS y REST no deduplican por micro-diferencias.
- trade_id debe ser el mismo en WS y REST. Verificar en el cryptofeed clonado qué stream usa Binance para trades (aggTrade → id = `a`) y que OKX/Bitget/Bybit exponen el id nativo. Si no coincide: clave (ts_ms, price, size, side) y documentarlo en decisions.md.
- Test: el mismo trade por vía WS-fake y REST-fake → 1 fila.
- Insertar siempre con INSERT ... SELECT FROM unnest(...) ON CONFLICT DO NOTHING (COPY directo no soporta ON CONFLICT; ya medido).

# Adaptadores de reparación (verificar cada endpoint EN VIVO antes de codificar)
- Binance UM trades: GET /fapi/v1/aggTrades. Primera llamada con startTime=gap_from (sin endTime), limit=1000; paginar con fromId=último a + 1 hasta T > gap_to. No mezclar fromId con startTime/endTime en la misma petición (timeouts). Si se usan startTime+endTime, ventana < 1 h. Peso 20 por petición; el límite por IP es compartido con el bulk → token bucket. Histórico ≤ 1 año.
- OKX trades: GET /api/v5/market/history-trades?instId=...&type=2&after=<ms>&limit=100. Devuelve registros anteriores a `after`, los más nuevos primero: empezar con after=gap_to y retroceder hasta ts < gap_from.
- Bitget trades: GET /api/v2/mix/market/fills-history?symbol&productType=USDT-FUTURES&startTime&endTime&limit=1000 (hasta 90 días, 10 req/s por IP; idLessThan para paginar hacia atrás).
- Bybit trades: /v5/market/recent-trade devuelve solo ~1000 trades recientes sin paginación temporal. Reparación INMEDIATA al cerrar el gap; si el trade más antiguo devuelto > gap_from → status=partial. Reconciliación diaria: volcado public.bybit.com/trading/ del día D-1 (verificar nombre de fichero y hora de publicación reales) → source=dump, ON CONFLICT DO NOTHING; partial → repaired.
- Hyperliquid trades: la API pública no ofrece histórico de trades → status=unrecoverable con note, nunca silencioso. Sus velas 1m sí: candleSnapshot (solo las últimas 5000 por intervalo).
- Candles 1m (todos los exchanges): re-fetch de klines REST del rango + UPSERT (Binance /fapi/v1/klines, Bybit /v5/market/kline, OKX history-candles, Bitget candles, Hyperliquid candleSnapshot). Es la garantía fuerte: las velas siempre se autorreparan.
- Funding / OI: refrescar por REST del exchange (el OI histórico REST de Binance solo cubre ~30 días).
- Liquidations: sin REST fiable → unrecoverable (INFO), ya documentado.

# Repair worker (repair/, servicio compose con restart)
- Bucle cada 30 s: toma gaps open con FOR UPDATE SKIP LOCKED → repairing → adaptador → verificación → status.
- Token bucket por exchange compartido con el bulk. Backoff exponencial con jitter en 429; en 418 (ban) pausa larga SIN marcar failed. Respetar Retry-After.
- Máx. 5 intentos; después status=partial con el error en note. Nunca dejar un gap open eternamente.
- Verificación: filas en [from, to] > 0 si el REST devolvió datos; Binance: continuidad de `a` sin saltos; log clave=valor {exchange, symbol, dtype, gap_from, gap_to, status, rows_repaired, elapsed}.
- Idempotente: reejecutar un gap no duplica filas.

# Reducir pérdida en origen (verificar parámetros en cryptofeed 3.0.1)
- Timeout de inactividad corto (10-15 s) en feeds de trades de pares líquidos; liquidations/funding/OI en instancias de Feed separadas con timeout largo (si no, reconectan sin parar por silencio legítimo).
- Backoff de reconexión acotado (≈10 s máx.).
- Métrica: nº de gaps registrados == nº de reconexiones.

# Criterios de aceptación (sustituyen al "0 perdidas en 60 s" global)
Pruebas: `docker-compose` + `docker network disconnect` del feed-daemon 60 s y reconexión; y `docker kill -s KILL` + reinicio tras 60 s.
1. Sin silencio: tras cada prueba, cada (exchange, símbolo, canal activo) con rango sin datos tiene ≥1 fila en ingest_gaps que lo cubre. Query de verificación: 0 rangos sin datos > 15 s en BTC sin gap registrado.
2. Candles 1m: 0 minutos faltantes tras reparar, en los 5 exchanges, y valores iguales a REST klines.
3. Trades Binance/OKX/Bitget: count de filas en [from, to] == count de una consulta REST independiente de referencia; status=repaired. En Binance, 0 saltos de `a`.
4. Trades Bybit: gap registrado; status ∈ {repaired, partial}; tras reconciliar el volcado de un día cerrado → repaired.
5. Trades Hyperliquid: gap registrado con status=unrecoverable + note. Velas reparadas.
6. Idempotencia: reparar dos veces el mismo gap → 0 filas nuevas. Dedup WS+REST → 1 fila por trade.
7. Reinicio: SIGKILL + restart → gap reason=restart abierto y reparado.
8. Validador (Fase 3): CRIT si hay gaps open > 1 h; INFO por cada unrecoverable.