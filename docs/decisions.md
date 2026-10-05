# Decisiones

Decisiones no cubiertas por las skills ni por `AGENTS.md`, con la opcion mas simple elegida
(regla 13). Se anaden aqui las que se takeoveran durante una fase.

## Fase 0

### D1. `cryptofeed 2.5.0` y no `3.0.1`
`cryptofeed 3.0.x` declara `requires-python >=3.13`. Como la regla 6 fija Python 3.12, se
pino `cryptofeed==2.5.0`, ultima version compatible con 3.12.
Consecuencia: no se pueden usar las APIs nuevas de cryptofeed 3.x. Al implementar el
feed-daemon hay que verificar las rutas contra el tag 2.5.0 del repo, no contra `main`.

### D2. `vectorbt 1.1.1` cambia la API respecto a 0.x
`vectorbt 1.1.1` es la version actual de PyPI y funciona en 3.12 con `numba 0.68`
(verificado ejecutando un `Portfolio.from_holding`). Ojo: `Portfolio.from_holding_close` ya
**no existe** (0.x) y los datos se construyen con `vbt.Data`/DataFrame, no con `PriceArray`.
Verificar siempre la API introspeccionando, no de memoria.

### D3. `yapic.json` hay que compilarlo -> Dockerfile multi-stage
`cryptofeed` depende de `yapic.json`, que **no publica rueda manylinux cp312** en PyPI
(solo cp37..cp311; cp312 existe solo para Windows). En linux + Python 3.12 se compila desde
el sdist, y hace falta un compilador de C (no hace falta Cython).
Solución: stage `builder` con `build-essential` que compila y stage `runtime` sin compilador.
Se verificó que no hay `gcc`/`cc`/`make` en la imagen final.

### D4. `uv sync --frozen` no sirve en la imagen; se usa `uv pip sync`
`--frozen` obliga a uv a instalar exactamente las distribuciones registradas en el lock, y para
`yapic.json` el lock solo registra la **sdist**, asi que uv ignora la rueda del wheelhouse e
intenta construirla (fallando al no haber `pdm-pep517`). Ademas los hashes del lock son de la
sdist/las ruedas de PyPI, no de la rueda que compila el propio build.
Solución: `uv export` (versiones desde `uv.lock`) + `uv pip sync`, que es un install exacto
(no resuelve). Se usan `--no-verify-hashes` y `--no-index --find-links` para instalar solo desde
el wheelhouse local. El pinning por version sigue absolutamente ligado a `uv.lock`.

### D5. Se copia el venv, no el wheelhouse
`COPY --from=builder /wheels /wheels` seguido de `rm -rf /wheels` **no** reduce la imagen: la
capa borrada solo crea un whiteout y los ~700 MB siguen contando. Copiar solo `/app/.venv`
(ya instalado) deja la imagen en **1412 MB**. Medido, no estimado.

### D6. `UV_COMPILE_BYTECODE` desactivado en runtime
Precompilar los `.pyc` anade **205 MB** a la imagen. Con el objetivo de <1.5 GB se desactiva:
la primera importacion compila y escribe en la capa de escritura del contenedor. Para daemons
que viven semanas es un coste de arranque de un solo uso.

### D7. Grupo `dev` dentro de la imagen
`pytest`/`pytest-asyncio` se instalan en `stack-base` para poder cumplir la regla 11 (correr
tests dentro del contenedor, con la DB real). Cuestan ~15 MB. El export del `builder` y el del
stage final **deben** incluir el grupo dev: si no, la instalacion final no encuentra ficheros
en el wheelhouse.

### D8. PK de `liquidations` incluye `side`
La regla de la skill dice PK `(symbol, exchange, ts)`, pero en el mismo segundo coexisten
liquidaciones long y short. Con `(symbol, exchange, ts)` la segunda insertaria fallaria y el
`ON CONFLICT` del feed descartaria datos reales. Se usa
`(symbol, exchange, ts, side)`, que ademas encaja con `liq_1h` agregado por `side`.
Comprobado con un `CHECK (side IN ('buy','sell'))`.

### D9. Los caggs se ejecutan con autocommit
Siguen la skill: los ficheros `20-23` llevan `-- no-transaction` y `migrate.py` los aplica con
autocommit. (En TimescaleDB 2.30 un cagg se podria crear dentro de una transaccion, pero la
skill lo exige y asi queda protegido si el motor vuelve a restringirlo.)

### D10. `systemd`-free: stubs con heartbeat
Los servicios de Fase 0 sin implementar no son `sleep infinity`: emiten heartbeat
`key=value` cada N s y gestionan SIGTERM, para que `docker-compose logs` demuestre que viven
y `down` no espere al timeout.

### D11. Puerto de Postgres en loopback
`127.0.0.1:${POSTGRES_PORT}:5432`. Sin exposicion en red. La red de compose sigue dando
conectividad entre servicios por el nombre `tsdb`.

### D12. `docker-compose` en vez de `docker compose`
En esta maquina el plugin v2 de compose no esta instalado; solo el binario standalone
`docker-compose` 2.29.5. Los comandos del README usan el binario.

### D13. Advisory lock en el runner
`migrate.py` toma `pg_advisory_lock` antes de aplicar. Dos `migrate` simultaneos (p. ej. dos
`docker-compose run` a la vez) se serializan en vez de competir por la misma migracion.
Coste de una linea, evita una clase de fallo dificil de reproducir.

### D14. Migraciones inmutables
`schema_migrations` solo guarda `version` + `applied_at` (exactamente lo que dice la skill), sin
hash del contenido. Editar una migracion ya aplicada **no** la re-aplica. Para cambiar algo ya
en la DB hay que crear un fichero nuevo. A cambio, se cumple el schema de la skill al pie de
la letra y no hay columna extra que mantener.

## Fase 1 (ver `docs/source-survey.md` para la evidencia)

### D15. Listado del bucket con `list-type=2`
El indice de `data.binance.vision` es JS y no sirve listados, asi que se usa el endpoint S3
`s3-ap-northeast-1.amazonaws.com/data.binance.vision`. **Sin `list-type=2` devuelve
`IsTruncated=true` con `NextContinuationToken` vacio**: el paginado se queda en la pagina 0 y
reporta falsamente "serie completa" tras 1000 claves. Con `list-type=2` funciona. Se fija
`list-type=2` en toda enumeracion de periodos.

### D16. `monthly` para meses cerrados y `daily` solo donde hace falta
De los dos coexistentes se usa `monthly` para meses cerrados (81 ficheros frente a 2468, mismos
datos) y `daily` unicamente para (a) `2019-12-31`, unico dia que no esta en ningun mensual, y
(b) el mes en curso. Es la opcion mas simple que cumple la regla 6 de la skill y evita 2400
descargas redundantes. El dia suelto 2019-12-31 se descarga del diario.

### D17. `metrics` se parsea desde diario y con dedup obligatorio
No existe `monthly/metrics`, asi que el year-partitioning sale de los ficheros diarios.
Cada timestamp de `metrics` viene **duplicado byte a byte** (288 timestamps/dia x 2 filas
identicas): el dedup por `(symbol, ts)` es obligatorio, no una optimizacion. Sin el, la tabla
`open_interest` sale al doble y `oi_5h` queda mal.

### D18. El detector de timestamps se amplia a fechas ISO
La regla 3 de `AGENTS.md` dice autodetectar 13/16 digitos. Eso **no cubre `metrics`**, cuya
columna `create_time` es el string `2020-09-01 00:00:00` (19 chars, sin timezone). Se aniade un
tercer caso al detector: string con forma de fecha -> parseo como **UTC**. Nunca naive ni zona
local. La posicion de la columna ts se fija **por producto** (en `aggTrades` es la columna 5,
porque la 0 es el `aggTradeId`), no por suposicion.

### D19. El mes en curso de `fundingRate` se rellena por REST
`fundingRate` **no tiene ficheros diarios** para BTCUSDT, asi que no hay forma de actualizar el
mes en curso desde ficheros. Se pagina `/fapi/v1/fundingRate` y se marca en el manifest como
origen `rest`. `metrics` en cambio si usa los diarios del mes en curso, pero los ratios long/short
no se pueden rellenar por REST (`/futures/data/openInterestHist` no los trae): quedan vacios
hasta que los llene el WS daemon.

### D20. `aggTrades` fuera del alcance de Fase 1
Son 45 GB comprimidos y ~7.4 M filas por mes solo en BTCUSDT. No cabe en el criterio de done de
la skill (que habla de velas 1m) y necesita su propia fase y su propia estrategia de
particionado. Se documenta como `unavailable`/fuera de alcance en el manifest, no se "intenta".
### D21. El header de `klines` se autodetecta por fichero
Los CSV de klines **cambian de formato en `2022-01`**: hasta `2021-12` no tienen cabecera y desde
`2022-01` si (Binance renombro `trades` a `count`, sin cambiar el orden de las 12 columnas). Asi
que el schema del lake sirve para las dos epocas y lo unico variable es si se salta la primera
linea. Fijar `has_header` a `False` (como parecian todos los ficheros al principio) rompe 56 de
los 81 periodos; fijarlo a `True` rompe los otros 25. Se autodetecta por fichero mirando si el
primer campo de la **columna de tiempo** es un entero. Se mira la columna de tiempo y no "la
primera" porque en un fichero sin cabecera la primera fila tambien es numerica, y en `aggTrades` la
columna 0 es el `aggTradeId`.

### D22. Escritura serializada por particion con `fcntl` (no lock por worker)
Descargando en paralelo, dos periodos del mismo año (2020-01 y 2020-02 van ambos a
`year=2020/part.parquet`) hacian read-modify-write del mismo fichero y el ultimo `os.replace` pisaba
el resultado del otro: **2.096.640 de 3.553.920 filas perdidas (59%)**, sin error ni aviso. El
lock es por particion y no por fichero global porque asi los años distintos siguen en paralelo.
Escribir un `.part.tmp` por hilo y renombrar bajo lock mantiene la atomicidad del rename.

### D23. `metrics` anual requiere concatenar los 2223 dias
Sin `monthly/metrics` no hay un fichero anual: el year-partitioning se hace uniendo los diarios. Un
año son ~105.000 filas, que es pequeño para memoria, asi que se hace en memoria con dedup por
`(symbol, create_time)`.

### D24. Los huecos de `metrics` se verifican contra `/fapi/v1/klines`
`/futures/data/openInterestHist` solo conserva ~30 dias (pedir 2024 da HTTP 400), asi que **no puede
servir de segunda fuente** para los huecos de 2020-2025. Se usa `/fapi/v1/klines`: si el exchange
publico velas de 1m continuas durante la ventana, no fue una caida del exchange y lo que falta es
la muestra en el zip de Vision. Se registra como `reason=vision_daily_missing_slot` deixando
claro en el `note` que **esto no prueba que existiera el dato de open interest**, solo que el
exchange estaba vivo. Los huecos que no se pueden confirmar con ninguna fuente **no se registran**.

### D25. Tolerancia de hueco de 60 s en `fundingRate`
Los `calc_time` no caen en la cuadricula de 8h: la desviacion medida va de -9 ms a +9 ms. Un check
estricto (`delta > 28800`) marca un falso positivo **en cada intervalo de funding**. Se anade
`gap_tolerance_seconds` al schema en vez de redondear el timestamp, porque el timestamp real es
informacion que no se debe tirar. El jitter no podra eliminarse tampoco desde el WS: es el
exchange el que manda el timestamp.

### D26. Los servicios Python corren como `LAKE_UID`/`LAKE_GID`, no como `UID`/`GID`
En un `docker-compose.yml` `user: "${UID}:${GID}"` no expande: bash no exporta `UID` ni `GID` (son
variables de shell, no de entorno), asi que el contenedor arrancaba como root y los ficheros del lake
quedaban como root:root, rompiendo el acceso del usuario. Se usan `LAKE_UID`/`LAKE_GID` con default
`1000`, declarados en `.env`. Es la version que funciona de la idea original, no un cambio de
intencion.

## Fase 2: feed en vivo y carga a Timescale

### D27. Python 3.13 y `cryptofeed==3.0.1`

El stack pasa a Python 3.13 porque cryptofeed 3.x lo exige (`requires-python >=3.13`) y es la
primera version con las dos cosas que hacen falta:

- **Hyperliquid**, que no existe en 2.5.0.
- **Bitget arreglado**. En 2.5.0 el endpoint de spot `/api/spot/v1/public/products` devuelve
  HTTP 400; en 3.x usa el de futuros.

Se fija el **tag `v3.0.1`** (commit `7714c10`), no `main`. `main` estaba en 3.0.2 sin publicar en
PyPI, y un stack que se actualiza solo al reiniciar no es reproducible. El bump obliga a rehacer
la imagen y a re-verificar el smoke de numba/vectorbt, que se paso en 3.13.

Efectos colaterales del bump, comprobados: `uv.lock` cambia (sale `yapic-json`, `aiofile`, `caio`;
entran `msgspec`, `order-book`, `zstandard`) y la imagen se queda en **1.49 GB**, dentro del limite
de 1.5 GB pero sin holgura.

### D28. Los canales de cada exchange son los que tiene, no los que se piden

`UNSUPPORTED` en `feed/config.py` fija lo que **no** existe, medido en cryptofeed 3.0.1 leyendo el
canal de cada clase de exchange:

| exchange | trades | funding | open_interest | liquidations | candles |
|---|---|---|---|---|---|
| BinanceFutures | si | si | si | si | si |
| Bybit | si | si | si | si | si |
| OKX | si | si | si | si | si |
| Bitget | si | si | si | **NO** | si |
| Hyperliquid | si | **NO** | **NO** | **NO** | **NO** |

Pedirle a `add_feed` un canal que el exchange no expone aborta el arranque. Se prefiere que el
daemon no levante a que se quede colgado esperando datos que no van a llegar.

**Hyperliquid solo aporta trades.** Es una limitacion del exchange, no del código: no se parchea
cryptofeed (regla 5). Queda pendiente un poller propio de funding y open interest contra la API
real de Hyperliquid, que se documentara cuando se verifique que la API responde.

### D29. Los simbolos se pasan normalizados, no nativos

A `add_feed` hay que darle el simbolo **normalizado** de cryptofeed, no el nativo, porque es el
propio cryptofeed quien translatea (`exchange.py::std_symbol_to_exchange_symbol`). Los pares
normalizado -> nativo, medidos con `Feed.symbol_mapping()` en 3.0.1:

| exchange | normalizado | nativo que usa por dentro |
|---|---|---|
| BinanceFutures | `BTC-USDT-PERP` | `BTCUSDT` |
| Bybit | `BTC-USDT-PERP` | `BTCUSDT` |
| OKX | `BTC-USDT-PERP` | `BTC-USDT-SWAP` |
| Bitget | `BTC-USDT-PERP` | `BTCUSDT_USDT-FUTURES` |
| Hyperliquid | **`BTC-USD-PERP`** | `BTC` |

Hyperliquid es el unico que se sale del generico. Con `BTC-USDT-PERP` los cinco feeds caen con
`UnsupportedSymbol: BTC-USDT-PERP is not supported on HYPERLIQUID`.

### D30. COPY no sirve para nada que exija `ON CONFLICT`

Descartado como camino principal tras verlo fallar contra la base real. COPY es lo mas rapido para
meter filas, pero **no admite clausula de conflicto**: la idempotencia no la daba el `ON CONFLICT`,
la daba una violacion de clave primaria. Medido: el primer lote de 3000 entraba bien y el segundo,
identico, reventaba y caia al camino de vuelta. El fallback era `cur.execute(sql, batch)`, que en
psycopg3 pasa la lista entera como un solo parametro: `the query has 9 placeholders but 3000
parameters were passed`.

Se usa `INSERT ... SELECT ... FROM unnest(...) ON CONFLICT`: una ida y vuelta por lote, con la
deduplicacion en la base. Es el mismo patron que usa el backend de Postgres de cryptofeed
(`cryptofeed/backends/postgres.py`), que es el "patron" que la skill manda copiar.

Vale para el daemon y para el loader, y por el mismo motivo.

### D31. Los callbacks van en el constructor del exchange, no en `add_feed`

`FeedHandler.add_feed(feed, **kwargs)` **descarta `kwargs`** cuando `feed` ya es una instancia:
solo los usa si `feed` es un str (`feedhandler.py::add_feed` -> `self.feeds.append(feed)`). Con los
callbacks ahi, `Feed.__init__` deja `Callback(None)` en cada canal y `Feed.callback()` no
encuentra a quien avisar.

Medido: la WS recibia **76 mensajes en 25 s y 0 callbacks**, sin un solo error en el log. Es el fallo
mas caro de la fase porque no falla: parece que funciona. Para encontrarlo hubo que instrumentar
`Connection.raw_data_callback` y comparar mensajes crudos con ticks.

Las claves de `callbacks` son ademas las constantes de `cryptofeed.defines` (`TRADES`, `CANDLES`,
...), no strings: `Feed.callbacks` se indexa por `CALLBACK_CHANNELS`.

### D32. `ON CONFLICT DO UPDATE` solo en `open_interest`

En el resto de dtypes `DO NOTHING` basta. En `open_interest` no: el daemon escribe la serie en
vivo **sin** `open_interest_value` (esa columna no viene por WebSocket), asi que si el lake llega
despues con `DO NOTHING` la fila del daemon se queda con la columna a NULL para siempre. Y
`metrics` es la unica fuente de OI historico completa, porque el historial de OI por REST son solo
30 dias. Con `DO UPDATE` la carga rellena el valor y sigue habiendo una sola fila por clave.

### D33. `refresh_continuous_aggregate` recibe parametros, no subquery

TimescaleDB rechaza subqueries en el `CALL` (`FeatureNotSupported: cannot use subquery in CALL
argument`), asi que el rango va como parametros tipados. Y el refresh se dispara si se ha **leido**
algo del rango, no si se ha **insertado**: si el refresh falla y se relanza la carga, la segunda
pasada inserta 0 filas pero la cagg seguiria sin materializar, y con la condicion sobre
`rows_inserted` no habria ninguna tercera pasada que lo arreglara.

### D34. `funding` mezcla liquidaciones del lake con ticks del mark price

La tabla `funding` recibe dos cosas distintas con la misma clave `(symbol, exchange, funding_time)`:

- del lake, la **liquidacion** cada 8 h (7.405 filas, `calc_time` exacto);
- del daemon, cada actualizacion del mark price que llega por WebSocket (decenas de miles).

Se mezclan en la misma tabla porque comparten clave natural y porque el backfill diario completa
lo que el daemon no puede. Para backtest de coste de funding interesa la fila de liquidacion, y es
la que esta en el rango del lake. Se documenta en vez de partir la tabla: separarlas duplicaria el
esquema para un dataset que son 48k filas.

### D35. El backfill diario corre a las 00:05 UTC

A las 00:00 el fichero del dia anterior todavia se esta sirviendo en `data.binance.vision` y se
cargaria a medias. A las 00:05 el dia esta cerrado. Y en UTC, no en hora local: el reparto diario es
la operacion mas sensible a la zona horaria (regla 3.bis).

### D36. Los gaps absorbidos por una fusion se conservan como `merged`, no se borran

La regla dice "no borrar filas: se cierran con status", y fusionar huecos solapados obliga a que
alguien deje de ser la fila canonica. Borrarla seria lo comodo y perderia justo lo que hay que
auditar: la deteccion original. Anade `status='merged'` + `merged_into` (migracion `61`) y las
filas absorbidas quedan enganchadas a la canonica. Lo otro que se perdia al fusionar era el
`reason`: ahora se queda el mas grave (y los demotas, en el `note` unido).

Lo mismo con los limites de tiempo: se leen en ms con `EXTRACT(EPOCH FROM ...)*1000`, no con
`.timestamp()*1000` en Python, que pasa por float64 y pierde precision en el ultimo digito.

### D37. El padding del hueco se aplica en un unico sitio: `GapLedger.record()`

El watchdog y el ledger anadian los 5 s de margen, de modo que un silencio de 45 s se declaraba de
55 s. Con el doble margen el ledger se hincha y el worker pide de mas a los exchanges. El punto
unico de paso es `record()`, que es por donde entran los tres detectores.

### D38. El umbral de hueco de arranque es la cadencia del dtype, no 60 s planos

Con 60 s el daemon abria **17 huecos falsos en cada arranque**: 2 min sin velas y 40 min sin
liquidaciones son el intervalo NORMAL de esos datos, no un corte. `restart_gaps` usa ahora
`SILENCE_MS[dtype]` como umbral y `min_stale_ms` como suelo. Y las velas 1m **cerradas** suben a
180 s: la ultima vela no aparece hasta que se cierra, asi que entre una y otra hay 1 min de
silencio mas hasta ~1 min de retraso de publicacion; con 90 s se declaraban huecos de velas en
cada arranque.

Y solo se miran las claves que el proceso escucha de verdad: sin ese filtro se declaran huecos para
las filas del lake (exchange en minuscula, `binance`), que no son un corte de ingesta sino el
problema de canonicalizacion, y el worker no tendria nada que reparar.

### D39. Cero filas de la fuente NO es lo mismo que la fuente no tiene el dato

El worker marcaba `unrecoverable` cuando un adaptador no devolvia ninguna fila. Medido: OKX tiene
historico de sobra y devolvio 0 filas con una paginacion sospechosa. Declararlo irrecuperable
cerraba el hueco como perdido para siempre y apagaba la pista de que el adaptador esta mal.
`unrecoverable` queda solo para cuando el propio adaptador lo dice (`can_repair() -> False`), que es
el caso real de Hyperliquid. Cero filas es `partial` con la nota de que puede ser retencion del
exchange o un fallo de paginacion.

### D40. `covered_through` es el fin de la ventana consultada, no el ts del ultimo trade

Medido: Binance devolvia 929 trades, los insertaba, y aun asi el hueco salia `partial` porque el
ultimo trade caia 30 s antes del final del hueco. Una respuesta vacia ES la prueba de que no habia
trades ahi, asi que la cobertura se mide por lo que se ha preguntado. Con esto los dos huecos de
Binance cerraron como `repaired` con 929 y 729 filas.

### D41. El watchdog rearma con el ledger, no con su memoria

`SilenceWatchdog` guarda en `open_id` las claves para las que ya abrio un hueco, para no abrir
otro. Como `repair/` cierra el hueco por su cuenta, la clave se quedaba marcada **para siempre** y
el daemon dejaba de detectar cortes en ese par sin decir nada: el ledger pareceria sano y no
registraria ni un hueco mas. `GapLedger.open_keys()` es la fuente de verdad y el daemon sincroniza
cada 12 sondeos (1 min).

`partial` cuenta como vivo a proposito: si se tomara por muerto, el watchdog rearmaria y volveria a
abrir el mismo silencio cada minuto, una fila por intento y peticiones de API en bucle. El worker
tampoco reintenta los `partial` solo; pasarlos a `open` a mano es la via de reintento, y queda
constancia de que fue una decision.

### D42. `rows_repaired` cuenta lo que entrego la fuente, no lo que se inserto

Verificado con la reparacion repetida de los mismos huecos de Binance: 957 filas y 957
`trade_id` distintos antes y despues, con `rows_repaired=929` las dos veces. El contador es lo que
trajo el endpoint (con lo que se puede contrastar contra la fuente); lo que de verdad entro nuevo se
comprueba con el conteo de filas, que es el invariante que importa para la idempotencia.

### D43. El backoff de cryptofeed no se toca

`ConnectionHandler._recover` hace `delay *= 2` a partir de 1 s **sin tope** (1, 2, 4, ..., 512 s) y
`Feed` no expone ningun parametro para acotarlo. Parchear cryptofeed esta descartado: es
actualizar una dependencia, no un flag. Lo que se hace en su lugar es deteccion por silencio y por
`conn.connects`, y el trade-off se acepta a sabiendas: un corte largo puede tardar hasta 8 min en
reconectar, y durante ese tiempo el hueco crece en silencio y se reparara entero despues.

### D44. `floor` en el volcado de Bybit, medido y no supuesto

El volcado publica `timestamp` en segundos con 4 decimales, y al pasarlo a ms enteros `floor` y
`round` dan valores distintos cuando el 4º decimal es >= 5. Gana el que produce el mismo ms que el
WebSocket, asi que la pregunta se responde mirando: `repair/worker.py verify-dump-alignment`
cruza el volcado con lo ya guardado y cuenta los desplazamientos.

Medido sobre `BTCUSDT` del 2026-10-04, con **91.747** `trade_id` comunes:

| Modo | Coinciden | Desplazan |
|---|---|---|
| `floor` | **91.747 (100 %)** | — |
| `round` | 45.382 (49,5 %) | 46.365 con **+1 ms** |

El reparto 50/50 es exactamente lo que predice un 4º decimal uniforme: la mitad de los valores caen
en el último medio milisegundo. `floor` es el bueno, y `round` habria metido 46.365 duplicados
silenciosos cada vez que el volcado y el REST tocaran la misma ventana.

Aun asi el anti-join va **por `trade_id`, no por `(ts, trade_id)`**: el modo correcto evita el
problema en el caso limpio, pero el anti-join es lo que lo hace imposible en el sucio. Son dos
defensas y solo hace falta una que falle.

### D45. El hueco se cierra por REST **o** por volcado, y se decide antes de insertar

El REST de Bybit solo llega a ~2 min, asi que un hueco antiguo se quedaba `partial` para siempre
aunque el volcado tuviera el dato. El worker consulta el volcado cuando el REST no cubre, pero
**decide qué va a cerrar el hueco antes de insertar**: si el REST trajo filas, el volcado se
anti-une aparte (las del REST comparten `ts` exacto con la tabla); si el REST no trajo nada
utilizable, el volcado es la respuesta y se inserta por su cuenta. Insertar primero y decidir
después metía dos veces el mismo conjunto por dos caminos distintos.

Y la decisión de estado mira **las dos fuentes**: con el REST limitado y el volcado cubriendo el
día entero, el hueco sí está completo. Mirar solo la limitación del REST lo dejaba en `partial`
para siempre aunque el volcado hubiera rellenado justo lo que faltaba.

### D46. La cobertura del volcado se declara día a día

Un volcado que llega con 500.000 filas pero al que le falta un día no cubre el hueco. La versión
anterior marcaba el rango entero cubierto en cuanto llegaba "alguna fila", con lo que un día sin
servir (o inexistente) producía huecos rotos con apariencia de cerrados. Ahora la cobertura solo
se declara si se leyeron **todos** los días pedidos, y si falta alguno va en `limitation`, que
impide `repaired` por construcción.
