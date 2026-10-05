#!/bin/sh
# Backfill diario: carga el dia anterior del lake a TimescaleDB.
#
# Por que 00:05 UTC y no 00:00: Binance Vision publica el dia anterior con unos minutos de retraso.
# A las 00:00 el fichero del dia anterior sigue escribiendose en origen y se cargaria a medias,
# obligando a una segunda pasada. A las 00:05 el dia esta cerrado.
#
# Por que UTC: el reparto diario es la operacion mas sensible a la zona horaria del proyecto. Con
# la sesion en `Europe/Madrid` (regla 3.bis de AGENTS.md) el dia local empieza una hora antes que
# el UTC y las velas de medianoche caen en el dia anterior. El loader fuerza UTC en las dos
# conexiones y `tests/test_loader.py::test_el_bucket_por_dia_y_ano_es_utc` lo vigila.
#
# Idempotencia: se puede relanzar las veces que haga falta. `ON CONFLICT DO NOTHING` (y DO UPDATE
# en open_interest) hacen que repetir un dia no duplique nada, asi que un fallo a medias se
# recupera relanzando este mismo script.
set -eu

LOG_PREFIX="component=backfill event=daily"

echo "${LOG_PREFIX} step=start tz=UTC target=ayer"
python -m loader backfill --since ayer "${LOADER_EXTRA_ARGS:-}"

# Reconciliacion del volcado publico de Bybit (D-1). El backfill de arriba solo cubre Binance
# Vision: Binance, Bybit, OKX, Bitget e Hyperliquid no tienen historico publico de trades, y lo
# que trae el daemon por WebSocket es lo unico que hay. Si el WS perdio un dia entero, nadie lo
# detecto porque no hubo hueco que medir: esto barre el dia completo del volcado y mete solo lo
# que falte, por `trade_id`. Es idempotente, asi que relanzarlo no hace nada.
#
# Por que D-1 y no el de hoy: Bybit publica el dia cuando ya esta cerrado. Pedirlo antes daria
# "dia no disponible", que es justo como se distingue un volcado aun sin servir de uno que no
# existe.
echo "${LOG_PREFIX} step=bybit_dump"
python -m repair.worker reconcile-bybit --dias "${BYBIT_DUMP_DIAS:-1}"

echo "${LOG_PREFIX} step=cagg_refresh"
# Las caggs se refrescan en el rango del dia anterior. El backfill ya lo hace por dtype, pero
# aquí se cubre el caso de que un dia se haya cargado a mano con `--no-refresh`.
YESTERDAY=$(date -u -d 'yesterday' +%Y-%m-%dT00:00:00)
TODAY=$(date -u +%Y-%m-%dT00:00:00)
python -m loader cagg --since "${YESTERDAY}" --until "${TODAY}"

echo "${LOG_PREFIX} step=done status=ok"