#!/bin/sh
# El cron de Ubuntu (vixie-cron 3.0pl1) NO admite CRON_TZ, y el backfill diario tiene que
# salir a las 00:05 UTC (Binance Vision publica D-1 con retraso; ver ops/daily.sh). Con
# `5 0 * * *` en hora local saldria a las 22:05 UTC en verano y el fichero del dia anterior
# todavia no existiria. Truco: se lanza cada hora a los 5 minutos y aqui se descarta todo lo
# que no sea la hora UTC 00. Cuesta un fork y sale exactamente a las 00:05 UTC todo el ano.
set -eu
[ "$(date -u +%H)" = "00" ] || exit 0
cd /home/fran/Escritorio/quant
# cron trae un entorno casi vacio: sin .env no hay POSTGRES_* ni rutas
if [ -f .env ]; then
  set -a
  . ./.env
  set +a
fi
exec ./ops/daily.sh
