#!/usr/bin/env bash
set -euo pipefail
SRC="/home/fran/Escritorio/quant/lake"
DST="/home/fran/Escritorio/quant_backup/lake"
LOG="${HOME}/.local/log/crypto-lake-backup.log"
mkdir -p "$DST" "$(dirname "$LOG")"
rsync -a --delete --info=stats "$SRC/" "$DST/" >> "$LOG" 2>&1
echo "$(date -u +%F\ %T)Z backup completado: $(du -sh "$DST" | awk '{print $1}')" >> "$LOG"
