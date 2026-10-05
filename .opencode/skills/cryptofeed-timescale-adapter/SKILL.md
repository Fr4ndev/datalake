---
name: cryptofeed-timescale-adapter
description: Adapta cryptofeed para escribir trades/funding/OI/liquidations en TimescaleDB con COPY por lotes, pool de conexiones y flush graceful. Usar para el daemon WebSocket de la Fase 2.
---

# Piezas del repo
- cryptofeed/backends/postgres.py → copiar el PATRÓN de callbacks, no el código (hace INSERT fila a fila).
- cryptofeed/feedhandler.py → reconexión y backoff ya resueltos: no reinventar.
- cryptofeed/exchanges/ + defines.py → verificar en runtime qué canales soporta cada exchange (BinanceFutures, Bybit, OKX PerpetualSwaps, Bitget USDT-Futuros, Hyperliquid).

# Patrón del adaptador
- Callback por tipo de dato con buffer en memoria (deque).
- Flush cuando: len(buffer) >= 1000 O timer de 1 s (asyncio task).
- Flush → psycopg3 COPY a la hypertable; fallback execute_values si COPY falla con timestamptz.
- ON CONFLICT DO NOTHING en todo (idempotencia ante resubscribes).
- finally → flush final al cerrar (SIGTERM no pierde filas).
- psycopg_pool.ConnectionPool(min=1, max=4). Daemon single-process: nunca conexión por tick.

# Config de feeds
- SOLO canales: trades, funding, open_interest, liquidations, candles.
- NO suscribir book depth: en un mini PC el I/O y la RAM no dan.
- Liquidaciones Binance por WS son parciales (~1/20) desde 2021: documentarlo en el schema, no "arreglarlo".

# Criterio de done
p95 latencia trade→fila < 2 s con 5 exchanges activos; SIGTERM → 0 filas perdidas (verificado con contador de pruebas); caída de red de 60 s no duplica ni pierde. Si ENABLE_COMPRESSION=true, los inserts en tiempo real solo tocan chunks recientes (sin descomprimir).
