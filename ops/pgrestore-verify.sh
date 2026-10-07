#!/usr/bin/env bash
set -euo pipefail
# Restaura un volcado de marketdata en una BD temporal y verifica los conteos.
#
# OBLIGATORIO: timescaledb_pre_restore()/post_restore(). Sin ellos el COPY de los chunks
# falla con "could not find hypertable with id N": pg_restore devuelve exit=1 y se pierden
# los chunks afectados mientras el resto de tablas queda correcta, asi que un conteo
# parcial no delata la perdida. Verificado con el volcado de 2026-10-07 (chunk
# _hyper_1_100_chunk, 10080 velas, se perdia al 100 %).
#
# La referencia NO es la BD viva: el feed escribe en continuo y el repair repara huecos con ts
# antiguo, asi que la viva siempre tendra mas filas que el volcado. Se compara contra los
# conteos que pgbackup.sh tomo ANTES y DESPUES del dump; el snapshot de pg_dump cae entre ellos.
#
# Uso: ops/pgrestore-verify.sh [ruta/al/volcado.dump.gz]
# el </dev/null es OBLIGATORIO: sin el, docker-compose exec se come el stdin del
# `while read` de abajo y el bucle se traga el fichero de conteos tras la primera linea
# (verificado: salia "1 tabla de 8" con exit=0).
PSQL()  { docker-compose exec -T tsdb psql -U marketdata -q "$@" < /dev/null; }
PSQLA() { docker-compose exec -T tsdb psql -U marketdata -At "$@" < /dev/null; }

FICHERO="${1:-$(ls -t "${HOME}"/.local/pgbackups/marketdata_*.dump.gz 2>/dev/null | head -1)}"
[ -n "$FICHERO" ] || { echo "no hay volcados en ~/.local/pgbackups"; exit 2; }
[ -f "$FICHERO" ] || { echo "no existe: $FICHERO"; exit 2; }

# marketdata_TS.dump.gz -> marketdata_TS.dump.counts.{before,after}
RAIZ="${FICHERO%.gz}"
BEFORE="${RAIZ}.counts.before"
AFTER="${RAIZ}.counts.after"
if [ ! -f "$BEFORE" ] || [ ! -f "$AFTER" ]; then
  echo "component=pgrestore event=fail motivo=falta_sidecar_de_conteos before=$BEFORE after=$AFTER"
  echo "volcado antiguo: regenera con ops/pgbackup.sh para poder verificarlo"
  exit 2
fi

TMPDB="restore_check"
echo "component=pgrestore event=start dump=$(basename "$FICHERO") tmpdb=$TMPDB"

PSQL -d postgres -c "DROP DATABASE IF EXISTS $TMPDB;"
PSQL -d postgres -c "CREATE DATABASE $TMPDB;"
trap 'PSQL -d postgres -c "DROP DATABASE IF EXISTS '$TMPDB';" >/dev/null 2>&1 || true' EXIT

# 1) restauracion, con el procedimiento de TimescaleDB
PSQL -d "$TMPDB" -c "SELECT public.timescaledb_pre_restore();"
if ! docker-compose exec -T tsdb bash -c \
     "set -o pipefail; gunzip -c | pg_restore -U marketdata -d $TMPDB --no-owner --no-privileges" \
     < "$FICHERO"; then
  echo "component=pgrestore event=fail motivo=pg_restore_exit_!=0"
  exit 1
fi
PSQL -d "$TMPDB" -c "SELECT public.timescaledb_post_restore();"

# 2) cada conteo restaurado debe caer en el intervalo [antes, despues] del propio volcado
while IFS='|' read -r tabla antes; do
  despues="$(awk -F'|' -v t="$tabla" '$1==t {print $2}' "$AFTER")"
  if [ -z "$despues" ]; then
    echo "component=pgrestore event=fail motivo=sin_conteo_en_sidecar tabla=$tabla"
    exit 1
  fi
  ahora="$(PSQLA -d "$TMPDB" -c "SELECT count(*) FROM $tabla;")"
  if [ "$ahora" -lt "$antes" ] || [ "$ahora" -gt "$despues" ]; then
    echo "component=pgrestore event=fail tabla=$tabla antes=$antes restaurado=$ahora despues=$despues"
    fallo=1
  else
    echo "component=pgrestore event=ok tabla=$tabla antes=$antes restaurado=$ahora despues=$despues"
  fi
done < "$BEFORE"

if [ "${fallo:-0}" -ne 0 ]; then
  echo "aviso: tmpdb=$TMPDB NO se borra para inspeccion (borrala con DROP DATABASE)"
  trap - EXIT
  exit 1
fi
echo "component=pgrestore event=ok total_tablas=$(wc -l < "$BEFORE")"
