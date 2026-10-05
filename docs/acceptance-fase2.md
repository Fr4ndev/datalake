# Acceptance Fase 2: feed en vivo y carga a Timescale

Fechas de medicion: 2026-10-04. Entorno: mini PC, `docker-compose`, 5 exchanges, BTC y ETH.

Todo lo de aqui son **mediciones**, no esperas. Los numeros que llevan `AC` son los que hay que
comprobar a mano; al final esta el comando exacto de cada uno.

---

## 1. Sumario

| # | AC | Resultado |
|---|---|---|
| 1 | 5 exchanges x 2 simbolos levantan | ✅ |
| 2 | Solo canales que el exchange soporta | ✅ (con limite en Hyperliquid y Bitget) |
| 3 | Nunca book depth | ✅ |
| 4 | p95 trade -> fila < 2000 ms | ✅ **984 ms** |
| 5 | SIGTERM -> 0 filas perdidas | ✅ buffer 599 -> 599 escritas, 0 perdidas |
| 6 | Reenvio del exchange no duplica | ✅ 3000 identicas -> 0 anadidas |
| 7 | Corte de red 60 s sin duplicar | ✅ 0 duplicados |
| 8 | Corte de red 60 s **sin perder** | ❌ **hay perdida**: Bybit 45 s, Binance 38 s, OKX 38 s, Bitget 20 s |
| 9 | CPU / RAM en el mini PC | ✅ CPU 3.8-5.8 %, RAM 76 MB |
| 10 | Loader idempotente | ✅ 1440 filas y luego 0 |
| 11 | Loader UTC | ✅ test de regresion que falla si la zona no es UTC |
| 12 | Caggs refrescadas tras cargar | ✅ |
| 13 | Compresion apagada | ✅ 0 politicas |
| 14 | Retention solo en trades | ✅ 180 dias |
| 15 | Suite de tests en verde | ✅ 188 pasan en host, 18/18 de loader en contenedor |

**El AC 8 es el que no se cumple** y no es un bug del codigo. Ver seccion 3.

---

## 2. Que hay en las tablas ahora mismo

```
       tabla        |     n     |           desde           |           hasta
-------------------+-----------+----------------------------+----------------------------
 candles_1m        |  3555090  | 2019-12-31 00:00:00        | 2026-10-04 01:43:00
 candles_1h (cagg) |    59258  | 2019-12-31 00:00:00        | 2026-10-04 01:00:00
 funding           |    48447  | 2019-12-31 23:30:00        | 2026-10-04 01:44:51
 funding_daily     |     2478  | 2019-12-31 00:00:00        | 2026-10-04 00:00:00
 open_interest     |   680630  | 2020-09-01 00:00:00        | 2026-10-04 01:44:51
 oi_5m (cagg)      |   639662  | 2020-09-01 00:00:00        | 2026-10-04 01:55:00
 trades            |    29831  | 2026-10-04 01:17:07        | 2026-10-04 01:44:51
 liquidations      |      101  | 2026-10-04 01:18           | 2026-10-04 01:44
```

Cobertura del feed en los ultimos 3 minutos, todos los 5 exchanges con BTCUSDT y ETHUSDT y ningun
simbolo fuera de canon:

```
BINANCE_FUTURES | BTCUSDT | ETHUSDT
BITGET          | BTCUSDT | ETHUSDT
BYBIT           | BTCUSDT | ETHUSDT
HYPERLIQUID     | BTCUSDT | ETHUSDT
OKX             | BTCUSDT | ETHUSDT
```

`candles_1m` son 3.553.920 del lake mas lo que ha ido metiendo el daemon. `open_interest` son
639.590 de `metrics` mas el vivo: el historico de OI por REST son 30 dias, asi que **el lake es la
unica fuente de OI historico completa**.

---

## 3. AC 8: el corte de red pierde trades. Por que y que se puede hacer

Medido con `docker network disconnect` durante 60 s. Al reconectar:

- **0 duplicados** (AC 7): correcto, por `ON CONFLICT DO NOTHING` con la PK
  `(symbol, exchange, ts, trade_id)`.
- **Si hay perdida**, y es real:

| exchange | hueco maximo | huecos >20 s |
|---|---|---|
| BYBIT | 45.4 s | 3 |
| BINANCE_FUTURES | 38.5 s | 3 |
| OKX | 38.5 s | 3 |
| BITGET | 20.2 s | 1 |
| HYPERLIQUID | 17.5 s | 0 |

No es un bug: **WebSocket no reenvia lo que se ha perdido mientras estabas desconectado**. El
exchange entrega el estado actual, no el historico. Da igual cuantas veces se re-suscriba.

Lo que **si** queda a salvo de la perdida es el buffer: durante el corte las filas se acumularon y
salieron todas al reconectar (`buffered` nunca dejo de vaciarse con normalidad, `rows_dropped=0` en
todos los stats). Lo que se pierde es lo que el exchange nunca volvio a mandar.

Y el "backfill lo repara" que se daba por hecho en el enunciado **no aplica a trades**: el lake solo
tiene `klines`, `funding` y `metrics`; los `aggTrades` quedaron fuera de alcance en D20. Las otras
series si se reparan solas, porque el backfill diario del lake las vuelve a meter.

Opciones, con lo que cuesta cada una:

1. **Dejarlo asi y documentarlo** (lo que se ha hecho). Los huecos se pueden medir y acepta.
2. **`aggTrades` de Binance** (`data.binance.vision`, mensual y diario): repararia Binance y solo
   Binance. Los otros cuatro siguen sin fuente. Es la extension natural de D20.
3. **Polling REST periodico** de trades mientras no haya WS: Binance, Bybit y OKX lo dan con ventana
   reciente. Cuesta una conexion HTTP por symbol y no da el mismo dato (REST son trades, WS de
   Binance es `aggTrade`, que agrega).

Lo unico que **no** se puede es prometer "sin perder" para trades mientras la fuente sea
WebSocket.

---

## 4. Como se midio cada cosa

### p95 trade -> fila

La latencia se mide por fila: `Store.add_trade` guarda `time.monotonic()` de la llegada y
`Store.flush` lo resta del instante del volcado. Se agrega en una ventana deslizante de 20k
muestras.

Antes se media `(ahora - llegada_del_tick_anterior)`, que es el **intervalo entre trades**:
daba 150 ms y parecia bueno, pero no media nada de lo que pide el AC. Lo dice el test
`tests/test_feed.py::test_el_p95_mide_llegada_a_fila_y_no_el_intervalo_entre_trades`.

El valor es practicamente `0.95 x intervalo_de_flush`, porque casi todas las filas esperan al
flush:

| `FEED_FLUSH_INTERVAL` | p95 medido |
|---|---|
| 1.0 s (por defecto) | **984 ms** |
| 20 s | 18222 ms |

Con el default de 1 s queda holgadamente por debajo de los 2000 ms.

### SIGTERM

Con `FEED_FLUSH_INTERVAL=20` para que el buffer este lleno en el momento del corte:

```
event=shutdown_flush_begin buffered=599 grace_seconds=20.0
event=shutdown_flush_done  rows=599 within_grace=True
event=stats trades_written=955 trades_dropped=0 trades_buffered=0
```

599 en cola, 599 escritas, 0 perdidas, 0 pendientes al salir. De las señales se encarga
cryptofeed (`run_async(install_signal_handlers=True)`: la primera hace apagado graceful, la segunda
cancela); no se reinstalan handlers porque `loop.add_signal_handler` pisa el anterior.

### Reenvio / idempotencia

3000 trades identicos insertados dos veces contra la base real: `trades=3000`,
`unicas tradeid=3000`. La segunda pasada no anadio ninguna fila.

### Recursos

5 exchanges, BTC y ETH, todos los canales soportados, ~15 min en marcha:

```
CPU 3.8 % -> 5.8 %
RAM 76 MB de 15.39 GiB
```

Va sobrado para ampliar simbolos. Marcarlo en `FEED_SYMBOLS` en `docker-compose.yml` y reiniciar.

---

## 5. Comandos para comprobarlo a mano

```sh
cd /home/fran/Escritorio/quant

# AC 1, 2, 3: que exchanges y canales levantan
docker-compose logs feed-daemon | grep feed_added

# AC 4: p95 (ultimo flush)
docker-compose logs feed-daemon | grep 'event=flush' | tail -1

# AC 5: SIGTERM con 0 perdidas (con el buffer lleno)
FEED_FLUSH_INTERVAL=20 docker-compose up -d feed-daemon
sleep 45 && docker-compose kill -s SIGTERM feed-daemon
docker-compose logs --tail=5 feed-daemon | grep -E 'shutdown_flush|event=stats'

# AC 6, 7: no duplica
docker-compose exec -T tsdb psql -U marketdata -d marketdata -c "
  SET TIME ZONE 'UTC';
  SELECT exchange, symbol, trade_id, count(*) FROM trades
  GROUP BY 1,2,3 HAVING count(*) > 1 LIMIT 5;"

# AC 8: huecos del corte de red (>20 s, ultima hora)
docker-compose exec -T tsdb psql -U marketdata -d marketdata -c "
  SET TIME ZONE 'UTC';
  WITH t AS (SELECT exchange, ts, lag(ts) OVER (PARTITION BY exchange ORDER BY ts) prev
             FROM trades WHERE ts > now() - interval '1 hour')
  SELECT exchange, round(max(EXTRACT(epoch FROM (ts-prev)))::numeric,1) AS hueco_max_s,
         count(*) FILTER (WHERE ts-prev > interval '20 seconds') AS n_huecos
  FROM t GROUP BY 1 ORDER BY 2 DESC;"

# AC 9: recursos
docker stats --no-stream $(docker-compose ps -q feed-daemon)

# AC 10, 11, 12: loader
docker-compose --profile batch run --rm -T loader python -m loader backfill --since ayer
docker-compose --profile batch run --rm -T loader python -m pytest tests/test_loader.py -q

# AC 13, 14: compresion y retention
docker-compose exec -T tsdb psql -U marketdata -d marketdata -tAc \
  "SELECT count(*) FROM timescaledb_information.jobs WHERE proc_name='policy_compression';"
docker-compose exec -T tsdb psql -U marketdata -d marketdata -tAc \
  "SELECT hypertable_name FROM timescaledb_information.jobs WHERE proc_name='policy_retention';"

# AC 15: suite
.venv/bin/python -m pytest -q
```

---

## 6. Limitaciones documentadas (no bugs)

- **Hyperliquid solo trades** (D28). Sin funding, open interest, liquidations ni candles en
  cryptofeed 3.0.1. Pendiente un poller propio contra la API real.
- **Bitget sin liquidations** (D28).
- **Los trades se pierden si hay corte de red** (seccion 3).
- **`funding` mezcla liquidaciones del lake con ticks del mark price** (D34). 48.447 filas, de las
  que 7.405 son liquidaciones cada 8 h.
- **El exchange se guarda en mayusculas** (`BINANCE_FUTURES`) porque es el `id` de cryptofeed,
  mientras que el lake lo guarda en minusculas (`binance`). El loader no necesita traducir porque
  cada lado escribe en su tabla y la clave incluye el exchange, pero cualquier join entre lake y
  tsdb tiene que tener en cuenta el case.
---

## Fase 2b: deteccion y reparacion de huecos (medido 2026-10-04)

### Como se midio

```bash
# 1) corte de red real de 90 s contra el contenedor del daemon
CID=$(docker-compose ps -q feed-daemon)
docker network disconnect cripto-marketdata_default "$CID"; sleep 90
docker network connect cripto-marketdata_default "$CID"

# 2) que se registro en el ledger
docker-compose exec -T tsdb psql -U marketdata -d marketdata -c \
  "SELECT id, exchange, symbol, dtype, reason, status, source, rows_repaired, attempts
   FROM ingest_gaps ORDER BY id"

# 3) reparacion
docker-compose --profile batch run --rm -T loader python -m repair.worker once

# 4) invariante de idempotencia
docker-compose exec -T tsdb psql -U marketdata -d marketdata -c \
  "SELECT count(*) AS filas, count(DISTINCT trade_id) AS ids FROM trades
   WHERE exchange='BINANCE_FUTURES' AND ts BETWEEN '<inicio>' AND '<fin>'"
```

### Resultados

| Que | Resultado | Como se comproba |
|---|---|---|
| Corte de red de 90 s detectado | 8 huecos de silencio, hasta **122 s** | `reason='silence'` en el ledger |
| Corte de red de 75 s (prueba anterior) | 9 huecos, **116-123 s** | idem |
| Salto de id de Binance detectado | 2 huecos `reason='id_jump'`, 104 s | `id IN (290,291)` |
| Binance reparado | `repaired`, 929 + 729 filas de la fuente | `rows_repaired` |
| Bitget velas 1m reparadas | `repaired`, 5 velas via REST | `id=176` |
| Hyperliquid trades | `unrecoverable` con el motivo escrito | `id=172,179` |
| Bybit trades | `partial`: `recent-trade` solo cubre ~2 min | nota en la fila |
| Sin duplicados | 146.423 filas / 146.423 claves unicas en 2 h | conteo |
| Idempotencia de la reparacion | 957 filas / 957 ids antes y despues de repetir | repetido 2 veces |
| Atribucion `trades.source` | `rest` se escribe correctamente | `insert_trades(..., 'rest')` |
| Tests | **252 pasan** (los de BD corren dentro del contenedor) | `pytest tests/` |

### Hallazgo importante: "repaired" significa "ventana verificada completa", no "filas rellenadas"

Las dos reparaciones de Binancegoneputsaron **0 filas nuevas**: las 1.658 que trajo el REST ya
estaban en la tabla. Al reconectar, cryptofeed reenvia lo ultimo y el `ON CONFLICT DO NOTHING` de
D30 lo absorvio. Es decir: **un silencio de recepcion no es por si mismo una perdida de datos**, y el
valor de la reparacion es/demoostrarlo en vez de darlo por supuesto.

Como se distingue un caso del otro: una perdida real se ve como filas nuevas con `source='rest'`.
Comprobado que el campo se escribe. En estas pruebas no hubo ninguna, porque no hubo perdida: el
`id_jump` de Binance fue un hueco de recepcion que el replay cubrio.

### AC por tipo de dato

| AC | Estado | Nota |
|---|---|---|
| Todo hueco detectado queda en el ledger | ✅ | 11 filas, ninguna perdida |
| Todo hueco termina en `repaired`/`partial`/`unrecoverable` | ✅ | ninguno se queda `open` |
| `unrecoverable` solo con motivo escrito | ✅ | Hyperliquid, con la limitacion de la API |
| Reejecutar la reparacion no duplica | ✅ | 957 = 957 |
| Repaired solo si la fuente cubrio el hueco entero | ✅ | cubierto por `test_cero_filas_es_partial...` y `test_binance_cubre_por_ventana...` |
| Cortar la red no rompe el daemon | ✅ | el ledger falla sin tumbar la ingesta (`gap_ledger_off`) |
| Tests de BD no destruyen el ledger | ✅ | las fixtures borran por `exchange`, no la tabla entera |

### Lo que NO esta hecho

| Que | Estado | Por que |
|---|---|---|
| Volcado de Binance Vision | ⚠️ implementado, sin probar contra datos reales | el camino `>48 h` no se ha ejecutado end-to-end |
| Canonicalizacion `exchange` lake vs daemon | ⚠️ sin resolver | `binance` (lake) y `BINANCE_FUTURES` (daemon) siguen siendo series distintas |
| Servicio `repair` | ⚠️ en perfil `repair`, apagado por defecto | se enciende con `docker-compose --profile repair up -d repair` |
| Backoff de cryptofeed sin tope | ❌ no arreglable | 3.0.1 no expone el parametro y no se parchea (D43) |
| OKX devolviendo 0 filas con historico disponible | ⚠️ abierto | queda `partial` con nota; hay que revisar la paginacion `after` a mano |
| Velocidad de reparacion de Bitget | ⚠️ 106-158 s por hueco | pagina en ventanas de 1 ms;admite mejora, no bloquea |
| Canonicalizacion `exchange` lake vs daemon | ⚠️ sin resolver | `binance` (lake) y `BINANCE_FUTURES` (daemon) siguen siendo series distintas |
| Servicio `repair` | ⚠️ en perfil `repair`, apagado por defecto | se enciende con `docker-compose --profile repair up -d repair` |
| Backoff de cryptofeed sin tope | ❌ no arreglable | 3.0.1 no expone el parametro y no se parchea (D43) |


---

## Fase 2c: reconciliacion por volcado publico (Bybit D-1)

### Por que hace falta, y por que no la cubre el worker de huecos

Solo Binance publica historico publico de trades. De los otros cuatro, el backfill del lake no
tiene nada, asi que lo unico que hay es WebSocket. Y el worker de huecos **solo puede cerrar lo
que el REST alcanza**: `recent-trade` llega ~1000 operaciones atras (~2 min medidos), asi que un
hueco de Bybit de ayer se quedaba en `partial` para siempre aunque el volcado diario tuviera el
dato. Ademas, un dia perdido **entero** no genera ningun hueco que medir, porque no hubo corte: no
hay forma de detectarlo desde el daemon.

Por eso `ops/daily.sh` barre el dia anterior del volcado completo, que es la verdad del dia.

### `floor` vs `round`: medido

```bash
docker-compose --profile batch run --rm -T loader   python -m repair.worker verify-dump-alignment --dias 1 --modo floor
docker-compose --profile batch run --rm -T loader   python -m repair.worker verify-dump-alignment --dias 1 --modo round
```

Con **91.747** `trade_id` comunes entre el volcado del 2026-10-04 y lo ya guardado por WS:

| Modo | Coinciden | Desplazan |
|---|---|---|
| **`floor`** | **91.747 (100 %)** | — |
| `round` | 45.382 (49,5 %) | 46.365 con **+1 ms** |

El 50/50 es justo lo que predice un 4º decimal uniforme. `round` habria metido 46.365 duplicados
silenciosos cada vez que volcado y REST tocaran la misma ventana. Aun asi el anti-join va por
`trade_id` (D44).

### Los tres tests obligatorios

```bash
# (a) volcado + WS a la vez -> solo entra lo que faltaba
docker-compose --profile batch run --rm -T loader python -m repair.worker reconcile-bybit --dias 1 --simular
docker-compose --profile batch run --rm -T loader python -m repair.worker reconcile-bybit --dias 1

# (b) segunda pasada -> 0
docker-compose --profile batch run --rm -T loader python -m repair.worker reconcile-bybit --dias 1

# (c) un hueco que el REST no alcanza lo cierra el volcado, sin duplicar
docker-compose --profile batch run --rm -T loader python -m repair.worker once --exchange BYBIT
```

| Test | Resultado |
|---|---|
| (a) volcado + WS solo inserta los que faltan | ✅ 960.391 nuevas de 1.164.095; ventana con **1.164.095 filas / 1.164.095 ids / 1.164.095 claves** |
| (b) segunda carga inserta 0 | ✅ `insertadas=0, ya_presentes=1164095` |
| (c) REST posterior no duplica | ✅ hueco de 5 min cerrado `repaired` via `dump`, `rows_repaired=0`, ventana sigue en 2.228 filas / 2.228 ids |

En (a) la ventana queda con las dos fuentes conviviendo y sin una sola colision:
**960.391 `source='dump'` + 203.704 `source='ws'`**. `conflicto_ts=0` en ambos dias: el `ts` del
volcado y el del WS coinciden en las 203.704 operaciones que estaban en las dos.

Un detalle que sale de (c): el volcado devolvio 2.266 trades y la ventana tiene 2.228 filas. La
diferencia son 38 trades de los 5 s de padding que el ledger anade a cada lado del hueco. Es lo
esperado y esta medido, no es una perdida.

### Estado

- `repair/reconcile.py`: reconciliador + verificador de alineacion.
- Subcomandos `reconcile-bybit` y `verify-dump-alignment`; `--simular` no escribe nada.
- `fetch_dump_trades` conectado al worker como segunda pasada cuando el REST no cubre (D45).
- `ops/daily.sh` lo ejecuta cada noche en `step=bybit_dump`, con `BYBIT_DUMP_DIAS` para mirar mas
  atras.
- Tests: **255 pasan**.

### Lo que sigue sin cerrar

El volcado de Binance Vision (`>48 h`) sigue sin probarse end-to-end, la canonicalizacion
`binance`/`BINANCE_FUTURES` continua pendiente, y los 4 exchanges sin volcado dependen para siempre
de que el WebSocket no se corte mas de lo que el REST puede recuperar.
