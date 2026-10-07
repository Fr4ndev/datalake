#!/usr/bin/env bash
set -euo pipefail
# pg_dump corre DENTRO del contenedor tsdb (ahi esta el binario). Rotacion 14 en el host.
#
# Se guardan dos ficheros de conteos, ANTES y DESPUES del volcado: el snapshot de pg_dump cae
# entre ambos, y las tablas solo crecen (el feed anade filas y el repair repara huecos antiguos
# con ts anterior al volcado). Por eso la verificacion de restauracion se mide contra ese
# intervalo y NO contra la BD viva, que seguira creciendo y daria falsos positivos.
BACKUP_DIR="${HOME}/.local/pgbackups"
LOG="${HOME}/.local/log/pgbackup.log"
mkdir -p "$BACKUP_DIR" "$(dirname "$LOG")"
DATE="$(date -u +%Y%m%d%H%M%S)"
OUT="${BACKUP_DIR}/marketdata_${DATE}.dump"

CONTEOS() {
  docker-compose exec -T tsdb psql -U marketdata -qAt -d marketdata -c \
"SET TIME ZONE 'UTC';
SELECT 'candles_1m', count(*) FROM candles_1m
UNION ALL SELECT 'trades', count(*) FROM trades
UNION ALL SELECT 'funding', count(*) FROM funding
UNION ALL SELECT 'open_interest', count(*) FROM open_interest
UNION ALL SELECT 'liquidations', count(*) FROM liquidations
UNION ALL SELECT 'ingest_gaps', count(*) FROM ingest_gaps
UNION ALL SELECT 'bt_runs', count(*) FROM bt_runs
UNION ALL SELECT 'schema_migrations', count(*) FROM schema_migrations
ORDER BY 1;"
}

CONTEOS > "${OUT}.counts.before" 2>>"$LOG"
docker-compose exec -T tsdb pg_dump -U marketdata -Fc marketdata > "$OUT" 2>>"$LOG"
CONTEOS > "${OUT}.counts.after" 2>>"$LOG"

gzip -f "$OUT"
ls -dt "${BACKUP_DIR}"/marketdata_*.dump.gz 2>/dev/null | tail -n +15 | xargs -r rm -f
ls -dt "${BACKUP_DIR}"/marketdata_*.counts.* 2>/dev/null | tail -n +30 | xargs -r rm -f
echo "$(date -u +%F\ %T)Z pgdump completado -> $(basename "$OUT").gz $(du -h "$OUT.gz" | cut -f1)" >> "$LOG"
