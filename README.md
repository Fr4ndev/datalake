# cripto-marketdata

Stack self-hosted de marketdata de futuros perpetuos: **lake Parquet** + **TimescaleDB**
(Binance UM, Bybit, OKX, Bitget, Hyperliquid). Todo dockerizado, sin cloud ni GPU.

> **Estado: Fase 0 completada** (base docker + schema TimescaleDB + runner de migraciones).
> El historical bulk (Fase 1) todavia no esta descargado: las tablas estan vacias.

## Requisitos

- Docker + Compose. En esta maquina el subcomando `docker compose` **no existe**: se usa el
  binario standalone `docker-compose` (v2.29.5). Todos los comandos de este README usan
  `docker-compose`.
- ~6 GB de disco para las imagenes, ~2 GB para la imagen de stack-base.

## Arranque

```bash
cp .env.example .env
$EDITOR .env                      # POSTGRES_PASSWORD y (opcional) TELEGRAM_BOT_TOKEN

docker-compose build              # construye stack-base (imagen unica compartida)
docker-compose up -d tsdb         # TimescaleDB con healthcheck
docker-compose run --rm migrate   # aplica las migraciones pendientes
```

`migrate` espera a que `tsdb` este healthy (`depends_on: condition: service_healthy`), asi que
no hace falta esperar a mano. Es idempotente: re-ejecutarlo no aplica nada.

### Ver el schema

```bash
docker exec cmd_tsdb psql -U marketdata -d marketdata -c \
  "SELECT hypertable_name, compression_enabled FROM timescaledb_information.hypertables ORDER BY 1;"

docker exec cmd_tsdb psql -U marketdata -d marketdata -c \
  "SELECT view_name, materialized_only FROM timescaledb_information.continuous_aggregates ORDER BY 1;"

docker exec cmd_tsdb psql -U marketdata -d marketdata -c \
  "SELECT version, applied_at FROM schema_migrations ORDER BY version;"
```

### Comprobacion de Fase 0 (idempotencia + cagg refresh)

```bash
docker exec -i cmd_tsdb psql -U marketdata -d marketdata < tsdb/verify_fase0.sql
```

Inserta 3 velas + 1 funding + 1 open_interest + 1 liquidacion con `ON CONFLICT DO NOTHING`,
hace `CALL refresh_continuous_aggregate(...)` y comprueba que `candles_1h` refleja los datos.
Al repetirlo los conteos no cambian.

## Servicios

| Servicio | Perfil | Estado en Fase 0 | Que sera |
|---|---|---|---|
| `tsdb` | (default) | real | TimescaleDB `2.30.2-pg16`, puerto solo en `127.0.0.1` |
| `migrate` | `batch` | real | aplica `tsdb/migrations/*.sql` |
| `bulk` | `batch` | stub | Fase 1: descarga Binance Vision -> Parquet |
| `loader` | `batch` | stub | Fase 1: Parquet -> hypertables + refresh de caggs |
| `feed-daemon` | (default) | stub | cryptofeed (WS) -> TimescaleDB |
| `bot` | (default) | stub | aiogram: receptor de senales |
| `validator` | (default) | stub | gap-scan del lake y de las tablas |

```bash
docker-compose --profile batch run --rm loader     # los de perfil batch no arrancan con `up`
docker-compose up -d feed-daemon bot validator
docker-compose logs -f feed-daemon
```

Los stubs no son silenciosos: emiten heartbeat `key=value` para que se vea que el proceso vive.

## Schema

Hypertables (PK compuesta `(symbol, exchange, <ts>)`, particionadas por `symbol`):

| Tabla | Grano | chunk | Notas |
|---|---|---|---|
| `candles_1m` | 1 min | 7 dias | `open/high/low/close/volume/quote_volume/trades/taker_buy_volume` |
| `funding` | 8h/4h/1h | 30 dias | el intervalo real varia por simbolo |
| `open_interest` | 5 min | 7 dias | historico solo desde el producto `metrics` (~2022+) |
| `liquidations` | 1 seg + `side` | 7 dias | snapshot historico cubre ~2020-01 -> 2021-12 |

Continuous aggregates (`materialized_only=false`, realtime):

| Cagg | Origen | Policy |
|---|---|---|
| `candles_1h` | `candles_1m` | 3 dias / 15 min |
| `funding_daily` | `funding` | 30 dias / 1 h |
| `oi_5m` | `open_interest` | 3 dias / 15 min |
| `liq_1h` | `liquidations` | 3 dias / 15 min |

**Las policies solo refrescan la ventana reciente.** Tras cualquier carga masiva hay que
refrescar a mano el rango cargado:

```bash
docker exec cmd_tsdb psql -U marketdata -d marketdata -c \
  "CALL refresh_continuous_aggregate('candles_1h', '2020-01-01', '2021-01-01');"
```

## Compression

Desactivada por defecto (`ENABLE_COMPRESSION=false` en `.env`) durante todo el backfill:
insertar con `ON CONFLICT` sobre chunks ya comprimidos es muy lento. El orden correcto es:

1. cargar historico
2. refrescar caggs
3. `ENABLE_COMPRESSION=true` + `docker-compose run --rm migrate` (aplica `50_compression.sql`)

## Layout

```
pyproject.toml / uv.lock      # versiones pineadas (python 3.12)
docker/stack-base.Dockerfile  # imagen base unica (2 stages: build con compilador / runtime sin)
docker-compose.yml
tsdb/migrations/*.sql         # 10-13 hypertables, 20-23 caggs, 50 compression
tsdb/migrate.py               # runner idempotente
tsdb/verify_fase0.sql         # comprobacion manual de la AC
ops/stub.py                   # stub de los servicios sin implementar
tests/test_migrate.py         # pytest del runner
lake/                         # datos Parquet (fuera de git)
```

## Comandos utiles

```bash
docker-compose run --rm migrate pytest -q      # 29 tests, incluye el de idempotencia contra la DB
docker-compose run --rm migrate python tsdb/migrate.py --status
docker-compose run --rm migrate python tsdb/migrate.py --dry-run
docker-compose --profile batch run --rm bulk   # stub
docker-compose down                           # para; `down -v` BORRA el volumen de la DB
```

## Notas de esta maquina

- `docker compose` no esta instalado; usar `docker-compose` (binario v2.29.5).
- Postgres esta publicado solo en `127.0.0.1:5432`.
- `.env` es local y esta en `.gitignore`. `.env.example` es la plantilla.