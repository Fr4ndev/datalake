---
name: data-quality-validator
description: Valida lake Parquet y TimescaleDB: gaps, duplicados, outliers, continuidad de funding y sincronía cross-exchange. Produce reporte JSON/Markdown y alerta CRIT a Telegram. Usar en Fase 3 y tras cada lote de bulk.
---

# Checks
1. Gaps: lag(ts) != intervalo esperado (1m: 60 s) con window functions (DuckDB en Parquet, SQL en TSDB). La fecha de listing del símbolo es inicio válido. Excluir los rangos de lake/known_gaps.json: un gap listado ahí se reporta como INFO, no CRIT.
2. Duplicados: count(*) vs count(DISTINCT (symbol, ts)) por partición.
3. Outliers: high < low, price <= 0, volume < 0, returns > 10×σ rolling 1d.
4. Funding: continuidad según intervalo real del símbolo (8h/4h/1h) desde listing; |rate| > 0.02 → WARN.
5. Cross-exchange: BTCUSDT 1m entre pares de exchanges en ventana común → correlación de close > 0.995 y desfase mediano < 5 s; divergencias → reportar (suelen ser downtime propio).
6. Cobertura: grid esperado (symbol × año × tf) del manifest vs presente; % completitud por celda.

# Salida e integración
- lake/_qa/report-YYYY-MM-DD.json (máquina) + .md (humano) con severidades CRIT/WARN/INFO.
- Servicio compose `validator` con loop cada 6 h; si hay CRIT → mensaje al bot.

# Criterio de done
Hueco de 47 min sembrado a mano en BTCUSDT 1m 2023 → CRIT con rango exacto; duplicado sembrado → CRIT.