#!/usr/bin/env bash
set -euo pipefail
SRC="/home/fran/Escritorio/quant/lake"
DST_BASE="/home/fran/Escritorio/quant_backup/lake"
LOG="${HOME}/.local/log/crypto-lake-backup.log"
DATE="$(date -u +%Y%m%d%H%M%S)"
SNAP="${DST_BASE}.${DATE}"
mkdir -p "$(dirname "$LOG")"
rsync -a --info=stats "$SRC/" "${SNAP}/" >> "$LOG" 2>&1
# Link latest
rm -f "${DST_BASE}.latest"
ln -sfn "$(basename "$SNAP")" "${DST_BASE}.latest" >> "$LOG" 2>&1
# Keep last 14 snapshots
ls -dt "${DST_BASE}".[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9] 2>/dev/null | tail -n +15 | xargs -r rm -rf
echo "$(date -u +%F\ %T)Z backup_snapshot completado: $(du -sh "$SNAP" | awk '{print $1}')" >> "$LOG"
