# Survey de fuentes (Fase 1, BTCUSDT)

Todo lo de aqui esta **verificado contra el origen** el 2026-10-03, no estimado. Los ficheros
de referencia se descargaron, se validaron contra su `.CHECKSUM` y se abrio la primera fila real
para confirmar unidades de timestamp y columnas.

## Como se listo el bucket (metodo)

`data.binance.vision` **no** sirve listado de directorios y su indice web es JS. El listado XML
real va contra el endpoint S3:

```
https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?list-type=2&prefix=<PREFIX>&delimiter=/
```

Trampa importante: **sin `list-type=2` el bucket responde `IsTruncated=true` pero con
`NextContinuationToken` VACIO**, asi que un paginado ingenuo se queda eternamente en la pagina 0
y parece que la serie esta completa cuando solo ha leido las primeras 1000 claves. Con
`list-type=2` el token llega bien. `start-after` da `InvalidArgument` ("startAfter only supported
in REST.GET.BUCKET with list-type=2").

## Cobertura real de BTCUSDT en `data/futures/um`

| Producto | Granularidad | zips | Primer | Ultimo | Huecos en origen | Tamaño |
|---|---|---|---|---|---|---|
| klines 1m | monthly | 81 | 2020-01 | 2026-09 | 0 meses | 151.8 MB |
| klines 1m | daily | 2468 | **2019-12-31** | 2026-10-02 | 0 dias | 154.0 MB |
| fundingRate | monthly | 81 | 2020-01 | 2026-09 | 0 meses | 0.1 MB |
| fundingRate | daily | **0** | — | — | **no existe** | — |
| metrics | monthly | **0** | — | — | **no existe** | — |
| metrics | daily | 2223 | **2020-09-01** | 2026-10-02 | 0 dias | 25.7 MB |
| aggTrades | monthly | 81 (+1 sobra) | 2020-01 | 2026-09 | 0 meses | 44.9 GB |
| aggTrades | daily | 2468 | 2019-12-31 | 2026-10-02 | 0 dias | 44.9 GB |

Series diarias completas: en los 2468 dias de klines 1m y los 2223 de metrics **no falta ni un
dia**. Velas 1m teoricas del rango completo: **3.552.000**.

### Desviaciones frente a lo que dice la skill (la skill lo pedia verificar)

1. **`fundingRate` NO tiene ficheros diarios para BTCUSDT** (0 objetos). La skill (linea 18)
   afirma que funding tiene mensual y diario: es incorrecto para funding. El hueco mensual en
   curso hay que rellenarlo por REST, no con diarios.
2. **`metrics` es solo diario y empieza 2020-09-01**, no "~2022" como dice la skill (linea 44).
   Hay **mas** historico de OI del que la skill asumia.
3. **El dato mas antiguo solo existe en diario**: klines 1m arranca el 2019-12-31 en diario, pero
   el primer mensual es 2020-01. Descargar solo el mensual perderia ese dia.
4. En `monthly/aggTrades/` hay un fichero basura
   `part-00000-0fae7358-a956-4804-a6d4-1682c07a5127-c000.zip` (resto de un job Spark). No es un
   mes: el downloader debe filtrar por nombre y no reventar con el.

## Timestamps: convencion real (fichero abierto, fila real)

| Producto | Columna ts | Tipo real | Ejemplo real | Unidad |
|---|---|---|---|---|
| klines 1m | col **0** (`open_time`) | entero | `1577836860000` | 13 digitos = **ms** |
| aggTrades | col **5** (!!) | entero | `1577836801481` | 13 digitos = **ms** |
| fundingRate | col 0 (`calc_time`) | entero | `1788220800005` | 13 digitos = **ms** |
| metrics | col 0 (`create_time`) | **string** | `2020-09-01 00:00:00` | **19 chars, NO epoch** |

Consecuencias directas para el parser:

- **La autodeteccion por magnitud (13 vs 16 digitos) NO alcanza para `metrics`**: no hay digitos,
  hay una fecha ISO sin timezone. Hay que anadir ese caso al detector, y tratar el string como
  **UTC** (no naive, no zona local).
- **`aggTrades` no tiene el ts en la columna 0**: la 0 es el `aggTradeId` (`18374167`). Asumir
  "columna 0 = ts" daria una columna de enteros monotona que parece un timestamp valido y no lo
  es. Hay que fijarla por nombre de producto.
- `fundingRate` llega con jitter de milisegundos (`1788220800005`, `1790784000002`), no alineado
  a minuto exacto. Redondear al intervalo de funding antes de indexar.

### Header: **cambia a mitad del historico**

| Producto | Fila de cabecera |
|---|---|
| klines 1m | **depende de la fecha**: NO hasta `2021-12`, SI desde `2022-01` |
| aggTrades | **NO** tiene header (7 columnas) |
| fundingRate | SI: `calc_time,funding_interval_hours,last_funding_rate` |
| metrics | SI: `create_time,symbol,sum_open_interest,sum_open_interest_value,count_toptrader_long_short_ratio,sum_toptrader_long_short_ratio,count_long_short_ratio,sum_taker_long_short_vol_ratio` |

**Hallazgo importante (medido, no supuesto).** Los CSV de `klines` cambiaron de formato a mitad
del historico. Localizado por busqueda binaria sobre los mensuales:

| Fichero | Formato |
|---|---|
| `BTCUSDT-1m-2021-11.zip` … `BTCUSDT-1m-2021-12.zip` | 12 columnas, **primera fila ya son datos** |
| `BTCUSDT-1m-2022-01.zip` … `BTCUSDT-1m-2026-09.zip` | cabecera + 12 columnas |

La cabecera nueva es:

```
open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore
```

Es decir, Binance **renombro `trades` a `count`**, pero **el orden de las 12 columnas no cambio**.
Por eso el esquema fijo del lake sigue valiendo para las dos epocas y lo unico que hay que hacer es
saltarse la linea.

Consecuencia en el codigo: el parser **no puede fijar `has_header`** para klines, tiene que
**autodetectarlo por fichero** (`bulk/parser.py::looks_like_header`). Si se fija `False` (que es lo
que parecian todos los ficheros al principio) fallan 56 de los 81 periodos con
`CSV conversion error to double: invalid value 'open'`. Si se fija `True` fallan los otros 25.

El criterio de deteccion es el primer campo de la columna de tiempo: si es un entero, es una fila de
datos; si no, es cabecera. **No** vale "la primera fila es numerica", porque en un fichero SIN
cabecera la primera fila tambien es numerica (`1577836800000` hay que parsearlo como ms). Por eso el
detector mira **la columna de tiempo del esquema**, no "la primera": en `aggTrades` la columna 0 es
el `aggTradeId`, tambien entero, y la de tiempo es la 5.

## Trampa de duplicados en `metrics`

Cada dia de `metrics` trae **576 filas para 288 timestamps**: cada timestamp de 5 min aparece
**exactamente dos veces, byte a byte identico** (comprobado: 288/288 pares identicos, deltas
0 s x288 y 300 s x287).

Sin dedup, la tabla `open_interest` receberia el doble de filas y los agregados de `oi_5m`
quedarian contaminados. El dedup por `(symbol, ts)` de la skill (regla 5) es aqui
**obligatorio, no opcional**.

## Cadencia y contenido confirmados

- `metrics`: 288 timestamps/dia -> **5 min**, cubre todo el dia (00:00 -> 23:55).
- `fundingRate`: 90 filas en 2026-09 = 3/dia -> **8 h**, y la propia columna
  `funding_interval_hours` viene con valor `8` en las 90 filas. El intervalo esta **verificado
  con el dato**, no supuesto (la skill pedia verificarlo por simbolo).

## Descarga: SHA-256

Los 6 ficheros de referencia pasaron `sha256` contra su `.CHECKSUM` (`OK` en los 6). El formato
del `.CHECKSUM` es `sha256  <nombre_del_zip>` en texto plano.

## Profundidad historica real por ccxt (regla 5)

Medido con ccxt **4.5.85** (el del venv del proyecto), paginando y sondeando `since` explicito:

| Exchange | Simbolo ccxt | Velas por llamada | `since` respetado | Inicio real del historico | status |
|---|---|---|---|---|---|
| OKX | `BTC/USDT:USDT` | **300** (~5 h) | si | entre 2019-12 y 2020-01 | limited |
| Bitget | `BTC/USDT:USDT` | 1000 (~16.6 h) | si | entre 2019-07 y 2019-08 | limited |
| Hyperliquid | **`BTC/USDT:USDT` NO EXISTE** | 1000 | **NO** | solo ~5000 velas (~3.5 dias) | unavailable |

- **Hyperliquid** esta en cotizacion **USDC**, no USDT: el simbolo correcto es `BTC/USDC:USDC`
  (en `load_markets` solo aparecen `BTC/USDC`, `BTC/USDH`, `BTC/USDC:USDC`...). Ademas su
  `fetch_ohlcv` **ignora `since`**: con cualquier fecha pedida devuelve lo mas reciente. Comprobado
  con `limit=5000` -> llega solo hasta 2026-09-29 23:38 (~3.5 dias). Confirmado el "unas ~5000
  velas recientes" de la skill. **No sirve para backfill historico**; su valor es el WS daemon.
- **OKX**: 300 velas por llamada. Para el rango 2020-01->hoy en 1m (~3.5 M velas) harian unas
  **11.700 llamadas**: inviable para bulk. Medido que respeta `since` hasta 2020-01 con precios
  coherentes con Binance (7192.9 vs 7189.43 en el mismo minuto).
- **Bitget**: respeta `since` hasta ~2019-08, 1000 velas/llamada (~11.700 llamadas tambien para
  todo el rango). Precios coherentes con BTC ago-2019 (~10081).
- Conclusion: **la fuente unica de historico 1m profundo es Binance Vision**. OKX/Bitget se
  podrian usar para un rango corto o bajo demanda, no para el backfill. Anotar
  `status=unavailable` en el manifest para el backfill largo, sin intentar "arreglarlo".

## REST verificado (solo para el mes/mes en curso, no para historico)

Los tres existen y responden (probados con `BTCUSDT` el 2026-10-03):

| Endpoint | Campos | Nota |
|---|---|---|
| `/fapi/v1/fundingRate` | `symbol, fundingTime, fundingRate, markPrice, rateType` | paginable con `startTime`; necessary porque funding **no tiene diario** |
| `/futures/data/openInterestHist` (`period=5m`) | `symbol, sumOpenInterest, sumOpenInterestValue, CMCCirculatingSupply, timestamp` | cubre OI pero **NO** los ratios long/short de `metrics` |
| `/fapi/v1/klines` (`interval=1m`) | 12 columnas, mismo esquema que el bulk | |

Ojo: `openInterestHist` **no** devuelve los ratios de top traders ni el taker long/short ratio, que
son 4 de las 6 columnas de `metrics`. Para el mes en curso esas columnas van a quedar vacias salvo
que se fillings desde el WS daemon; el backfill de ratios historicos solo existe en los ficheros
diarios de `metrics`.

**`openInterestHist` solo conserva ~30 dias.** Comprobado: pedir `startTime` de 2024 devuelve
**HTTP 400**, no una lista vacia. Consecuencia directa: para los huecos de `metrics` de 2020-2025
ese endpoint **no puede servir de segunda fuente**, y por eso el verificador cae a
`/fapi/v1/klines` (ver `bulk/known_gaps.py::exchange_was_up`).

## `metrics` tiene huecos reales en el archivo de Vision

Restando lo que hay en el lake contra los 288 slots diarios teoricos (2223 dias):

| Año | Filas en disco | Teorico | Slots ausentes |
|---|---|---|---|
| 2020 | 35.103 | 35.136 | 33 |
| 2021 | 104.652 | 105.120 | 468 |
| 2022 | 105.120 | 105.120 | **0** |
| 2023 | 105.117 | 105.120 | 3 |
| 2024 | 105.281 | 105.408 | 127 |
| 2025 | 105.117 | 105.120 | 3 |
| 2026 | 79.200 | 79.200 | **0** |
| **Total** | **639.590** | **640.224** | **634** |

Esos 634 slots son **156 rangos** (108 de un solo slot, y uno de 125 slots: 2024-02-16 13:35 ->
23:55). Dos Conclusions importantes:

1. **No es un hueco del exchange.** Verificado con `/fapi/v1/klines`: el exchange publico velas de
   1m de forma continua durante todas esas ventanas. Lo que falta es la muestra de open interest en
   el zip de Vision.
2. **Cada zip diario suelto pierde 1 slot** (287 de 288 unicos, siempre de madrugada), pero los
   zips vecinos se solapan y se cubren: por eso 2022 y 2026 salen **completos** apesar de que
   cualquier dia suelto tenga un hueco. Comprobarlo dia a dia en vez de sobre el lake daria un
   diagnostico equivocado.

Consecuencia: los 156 rangos van a `lake/known_gaps.json` con `reason=vision_daily_missing_slot`.

**Cuidado con como se citan.** Lo verificado contra `/fapi/v1/klines` es que el exchange estaba vivo
en esas ventanas, **no** que el exchange publicara esos puntos de open interest: `openInterestHist`
no conserva esas fechas. Son **huecos del archivo de Vision**, no huecos probados del exchange. La
distincion no cambia el resultado del gap-scan (0 sin explicar) pero cambia lo que se puede afinar.
No se rellenan porque no hay fuente con esos datos: `openInterestHist` no los conserva.

## Dos fallos que solo aparecieron con datos reales

Ninguno de los dos se habria visto con ficheros de ejemplo:

1. **Perdida de datos por escritura concurrente.** Con 12 workers, los periodos de un mismo ano
   (2020-01 y 2020-02 van ambos a `year=2020/part.parquet`) hacian read-modify-write del mismo
   fichero a la vez y el ultimo `os.replace` pisaba al otro: **2.096.640 de 3.553.920 filas
   perdidas (59%)** sin ningun error ni aviso. Se arreglo con un lock por particion
   (`bulk/writer.py::_partition_lock`).
2. **Jitter de `fundingRate`.** Los `calc_time` no caen en la cuadricula de 8h: miden entre
   28799.991 y 28800.009 s de diferencia. Un check de hueco estricto (`delta > 28800`) da un
   **falso positivo por cada intervalo de funding**. Se resolvio con `gap_tolerance_seconds=60`
   en el esquema, no redondeando el timestamp (que seria perder informacion real).

## Volumen del alcance de Fase 1

Con `klines 1m` + `fundingRate` + `metrics` el total es **~180 MB comprimido**, no los 45 GB de
`aggTrades`. `aggTrades` (7.4 M filas/mes, 45 GB) queda **fuera de alcance** y necesita su propia
fase con su propia estrategia de particionado.
---

# Fase 2: capacidades reales del feed en vivo

Medido en runtime contra **cryptofeed 3.0.1** (tag `v3.0.1`, commit `7714c10`), no deducido de la
documentacion. Se lee el canal de cada clase de exchange y su `symbol_mapping()`.

## Canales por exchange

| exchange | trades | funding | open_interest | liquidations | candles |
|---|---|---|---|---|---|
| BinanceFutures | si | si | si | si | si |
| Bybit | si | si | si | si | si |
| OKX | si | si | si | si | si |
| Bitget | si | si | si | **NO** | si |
| Hyperliquid | si | **NO** | **NO** | **NO** | **NO** |

**Hyperliquid solo aporta trades** y **Bitget no tiene liquidations**. No son limitaciones del
código: son del exchange segun cryptofeed 3.0.1, y la regla 5 dice documentarlas en vez de parchear
cryptofeed. Pedir un canal inexistente aborta el arranque, asi que `feed/config.py` los filtra
antes (`UNSUPPORTED`).

## Simbolo normalizado -> nativo

| exchange | normalizado (el que se pasa a `add_feed`) | nativo interno |
|---|---|---|
| BinanceFutures | `BTC-USDT-PERP` | `BTCUSDT` |
| Bybit | `BTC-USDT-PERP` | `BTCUSDT` |
| OKX | `BTC-USDT-PERP` | `BTC-USDT-SWAP` |
| Bitget | `BTC-USDT-PERP` | `BTCUSDT_USDT-FUTURES` |
| Hyperliquid | **`BTC-USD-PERP`** | `BTC` |

A `add_feed` se le da el **normalizado**: cryptofeed translatea al nativo el solo
(`exchange.py::std_symbol_to_exchange_symbol`). Hyperliquid rompe el patron (USD en vez de USDT) y
con el generico los cinco feeds caen con `UnsupportedSymbol`.

## Identificadores y granularidad

- Los cinco exchanges dan `trade.id` nativo, que es lo que se usa para deduplicar.
- `aggTrades` de Binance **queda fuera de alcance** (D20): son 45 GB y no cabe en el presupuesto
  del mini PC. Por eso un corte de red deja huecos en `trades` sin fuente que los reparen.

## Lo que la REST no da

- **Open interest historico: 30 dias.** El historico completo de OI sale de `metrics` en Vision, no
  de REST. Por eso el lake es la unica fuente de OI historico completa y la tabla `open_interest` se
  rellena con `metrics`, no con el feed.

---

# Fase 2b: fuentes de reparacion de trades (verificadas EN VIVO)

Sondeo real del2026-10-04 contra los 5 exchanges. **Una llamada real por endpoint**, no deducido
de la documentacion. Timestamps de las muestras: `now` ~ 1791102840000 ms.

## 1. Deduplicacion WS <-> REST: el `trade_id` coincide

El riesgo real de un reparador es que WS y REST usen identificadores distintos y la reparacion
**duplique** cada trade. Verificado exchange por exchange contra los trades que el daemon ya
tenia en la base (poblada por WS):

| exchange | id en WS (campo cryptofeed lee) | id en REST | veredicto |
|---|---|---|---|
| BinanceFutures | `str(msg['a'])` (stream `aggTrade`) | `a` | **96/96 comunes, 0 divergentes, ts identico al ms** |
| Bybit | `trade['i']` | `execId` | **151/151 comunes, 0 divergentes** |
| Bitget | `entry['tradeId']` | `tradeId` | **90/90 comunes, 0 divergentes** |
| OKX | `trade['tradeId']` | `tradeId` | **103/103 comunes dentro de la ventana cubierta, 0 divergentes** |
| Hyperliquid | `str(entry['tid'])` | `tid` | **10/10 comunes** (el REST solo devuelve 10) |

Conclusion: `trade_id` es el mismo por WS y por REST en los 5. La PK
`(symbol, exchange, ts, trade_id)` deduplica WS contra REST. **Salvo Bybit por dump** (ver §4).

## 2. Trades: respuestas reales

### Binance UM — `GET /fapi/v1/aggTrades`

```json
[{"a":3474491307,"p":"85079.10","q":"0.023","nq":"0.023","f":8144210172,"l":8144210172,
  "T":1791102056684,"m":false},
 {"a":3474491308,"p":"85079.00","q":"0.293","nq":"0.293","f":8144210173,"l":8144210183,
  "T":1791102056720,"m":true}]
```

`a` = aggregate trade id secuencial, `T` = ms, `m` = isBuyerMaker (invierte el side).

**LIMITE MEDIDO: ~48 horas, NO un año.** La skill decia "historico <= 1 ano"; es incorrecto:

```
startTime hace  24h -> OK n=1000
startTime hace  48h -> OK n=1000
startTime hace  60h -> {"code":-4166,"msg":"Search window is restricted to recent 2 days only."}
startTime hace 168h -> {"code":-4166,...}
```

`fromId` tambien esta limitado: `fromId=a-100000` (0,4 dias) OK, `fromId=a-10000000` -> -4166.

**Pero el volcado si llega a 2019.** `data.binance.vision`:

```
data/futures/um/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2019-12-31.zip
data/futures/um/monthly/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2020-01.zip
```

Esquema (zip de 3,9 MB por dia para BTCUSDT):

```
agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker
3474072865,84482.7,0.011,8143048463,8143048465,1790985600011,true
```

`agg_trade_id` es el mismo `a`. **Es la unica fuente de trades de Binance a mas de 48 h.**

### OKX — `GET /api/v5/market/history-trades?instId=BTC-USDT-SWAP&type=2`

```json
{"code":"0","data":[{"instId":"BTC-USDT-SWAP","tradeId":"2972052352","px":"85073.1",
  "sz":"0.01","side":"sell","ts":"1791102065937","source":"0"}]}
```

- `type=2` (sin block trades) es el que **coincide exactamente con el WS** (0 ids WS ausentes).
  `type=1` (todos) devolvio 2 de 131 ausentes en la ventana cubierta: no usarlo para reparar.
- `after=<ms>` pagina hacia atras y devuelve los anteriores. Hay que restar 1 al minimo de cada
  pagina, si no se repite la misma pagina indefinidamente.
- **`before` NO existe para paginacion por timestamp**: code `50039`,
  `"The before parameter isn't available for implementing timestamp pagination."`
- Profundidad: `after` funciona a 60 dias, devuelve vacio a 90 -> **60-90 dias**.

### Bitget — `GET /api/v2/mix/market/fills-history?productType=USDT-FUTURES`

```json
{"code":"00000","data":[{"price":"85075","side":"Buy","size":"0.0017","symbol":"BTCUSDT",
  "tradeId":"1490555590356516864","ts":"1791102066601"}]}
```

Profundidad medida: `startTime` a 88 dias devuelve datos de 81 dias atras. Vacio a 90+.
**Limite efectivo ~90 dias**, como dice la skill.

### Bybit — `GET /v5/market/recent-trade?category=linear`

```json
{"retCode":0,"result":{"list":[
 {"execId":"375a5d8b-8517-50e1-8393-5dd312543eca","symbol":"BTCUSDT","price":"85089.20",
  "size":"0.023","side":"Sell","time":"1791102058954","seq":"820198947557"}]}}
```

**Ignora `startTime`.** Con `startTime` de hace 30 dias devuelve las mismas 1000 filas de los
ultimos ~2 minutos. Confirmado: la reparacion solo sirve si es inmediata.

### Bybit — volcado diario `public.bybit.com` (fuente de largo plazo)

`https://public.bybit.com/trading/BTCUSDT/BTCUSDT2026-10-03.csv.gz`, 11,7 MB, `Last-Modified:
Sun, 04 Oct 2026 01:10:25 GMT` (publicado a las **01:10 UTC** del dia siguiente), 317.185 trades.

```
timestamp,symbol,side,size,price,tickDirection,trdMatchID,grossValue,homeNotional,foreignNotional,RPI
1790985600.1199,BTCUSDT,Buy,0.005,84476.00,PlusTick,3bad2089-5044-576e-894f-4023df24e558,...
```

`trdMatchID` es el `execId`. **PERO `timestamp` esta en SEGUNDOS con 4 decimales.**

### Hyperliquid — `POST /info {"type":"recentTrades","coin":"BTC"}`

```json
[{"coin":"BTC","side":"A","px":"85089.0","sz":"0.00029","time":1791102066783,
  "hash":"0x171bb5...","tid":277493838368321,"users":["0xa62b...","0x4d2e..."]}]
```

Devuelve **exactamente 10 trades**, sin paginacion ni historico. `tid` es el id.

## 3. Velas 1m: los 5 endpoints responden

| exchange | endpoint | forma | n |
|---|---|---|---|
| Binance | `/fapi/v1/klines?interval=1m` | array posicional de 12 | 5 en el rango |
| Bybit | `/v5/market/kline?interval=1` | array posicional de 7 | 5 |
| OKX | `/api/v5/market/history-candles?bar=1m` | array posicional de 9 | 10 |
| Bitget | `/api/v2/mix/market/candles?granularity=1m` | array posicional de 7 | 10 |
| Hyperliquid | `POST /info candleSnapshot` | objeto `{t,T,s,i,o,c,h,l,v,n}` | 61 |

Muestras reales (los 4 primeros son arrays posicionales, el orden de columnas NO es uniforme):

```
BINANCE : [1791102840000,"85023.10","85043.50","85023.00","85043.50","26.474",1791102899999,...]
BYBIT   : ["1791102840000","85032.1","85047","85023","85047","7.648","650309.0057"]
OKX     : ["1791102840000","85024.1","85040.1","85024","85040.1","1370.42","13.7042",...]
BITGET  : ["1791102900000","85042.1","85042.2","85042.1","85042.2","0.001","85.0422"]
HYPERLIQ: {"t":1791102720000,"T":1791102779999,"s":"BTC","i":"1m","o":"85035.0","c":"85062.0",
           "h":"85063.0","l":"85035.0","v":"19.20143","n":116}
```

**Hyperliquid `candleSnapshot` no son 5000 exactos**: pidiendo 10 dias (14.400 velas) devuelve
**5.116 velas = 3,55 dias**. Es el limite real de historico de velas 1m.

## 4. Lo que NO coincide con la skill (decidir antes de codificar)

1. **Binance aggTrades REST llega a ~48 h, no a 1 año.** Reparar con REST solo vale para cortes
   recientes. Para lo mas viejo, la unica fuente es el volcado de `data.binance.vision`
   (aggTrades, diario y mensual, desde 2019-12-31, mismo `agg_trade_id`).
2. **El dump de Bybit tiene precision sub-ms real.** El 4o decimal de `timestamp` se distribuye
   uniforme entre 0 y 9 (medido sobre 60.000 lineas), o sea que no es ruido de formato: Bybit
   guarda mas precision que el ms que expone por WS y REST. Redondear a ms **no** reproduce
   necesariamente el ms entero de WS/REST, asi que la PK `(ts, trade_id)` **no deduplicaria** el
   dump contra lo ya insertado: haria falta un anti-join explicito por `trdMatchID`.
3. **cryptofeed 3.0.1 no expone ningun hook de reconexion.** `ConnectionHandler.run()`
   reconecta por su cuenta en un bucle interno (`delay=1; delay*=2`) y no notifica a nadie. Lo
   unico observable es `conn.connects` y `conn.watchdog_trips`. La deteccion de `disconnect` hay
   que construirla con silencio + discontinuidad de ts/id, usando `connects` como corroboracion.
4. **El backoff de cryptofeed no tiene tope**: `delay` se duplica sin maximo (1,2,4,...,512 s).
   La skill pide acotarlo a ~10 s y no hay parametro para eso; habria que parchear cryptofeed, que
   la regla 5 prohibe.
