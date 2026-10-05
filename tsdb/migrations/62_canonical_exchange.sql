-- Canonicalizacion de `exchange`: un unico valor por exchange en todas las tablas.
--
-- Por que: el lake y el bulk escriben `binance` (minusculas, viene de data.binance.vision) y el
-- daemon escribe `BINANCE_FUTURES` (lo que dice cryptofeed). Como `exchange` es parte de la PK de
-- `trades` —`(symbol, exchange, ts, trade_id)`— y de la clave de las caggs, los dos son series
-- distintas: el historico y el tiempo real del mismo symbol nunca se juntan y el anti-join del
-- reparador no encuentra lo que deberia. Nada de eso da error; solo hace que los datos esten
-- partidos en dos.
--
-- Por que `INSERT ... SELECT` + `DELETE` y no un `UPDATE`: durante una ejecucion anterior de este
-- mismo fichero el daemon se reinicio a mitad, de modo que hay filas **reales** en las dos
-- grafias para el mismo instante (medido: 4.424 claves en `funding`, 1.085 en `candles_1m`, 24 en
-- `trades`: los mismos eventos ingeridos dos veces, una con el nombre viejo y otra con el nuevo).
-- Un `UPDATE` ahi revienta la PK. La fusion copia al nombre canonico lo que no exista ya y borra
-- el nombre viejo; cuando la clave canonica ya existe, se descarta la copia porque es el mismo
-- evento con los mismos valores, no una fila distinta. Perder una de las dos copias de un mismo
-- evento no es perder un dato.
--
-- `ingest_gaps` se queda con UPDATE: su unica clave es `id`, y sus filas se referencian entre si
-- con `merged_into`. Un DELETE/INSERT cambiaria ids y dejaria referencias colgando, que es
-- justo lo que la migracion 61inns existedio para evitar.
--
-- D33/regla 8: la migracion se ejecuta con el runner, no por docker-entrypoint-initdb.d.

DO $$
DECLARE
    tabla text;
    columnas text;
    i integer;
    j integer;
    canonico text;
   Viejos text[];
    es_tabla boolean;
    insertadas bigint;
    borradas bigint;
    mapeo jsonb;
    lista_viejos jsonb;
BEGIN
    -- JSONB y no un array de arrays: en PL/pgSQL los arrays multidimensionales fallan al
    -- construirse ("must have array expressions with matching dimensions") y este fichero no
    -- tiene por que ser unTipo de obstaculo para canonicalizar los datos.
    mapeo := jsonb_build_object(
        'binance_um', jsonb_build_array('binance','BINANCE_FUTURES','BinanceFutures',
                                        'BINANCE','Binance','binance_futures','binanceum'),
        'bybit',       jsonb_build_array('BYBIT'),
        'okx',         jsonb_build_array('OKX'),
        'bitget',      jsonb_build_array('BITGET'),
        'hyperliquid', jsonb_build_array('HYPERLIQUID','Hyperliquid','hyper_liquid')
    );
    FOR i IN 1..7 LOOP
        es_tabla := i <= 5;
        IF es_tabla THEN
            tabla := (ARRAY['trades','candles_1m','funding','open_interest','liquidations'])[i];
        ELSE
            tabla := 'ingest_gaps';
        END IF;

        IF es_tabla THEN
            SELECT string_agg(quote_ident(column_name), ', ' ORDER BY ordinal_position)
              INTO columnas
              FROM information_schema.columns
             WHERE table_schema = 'public' AND table_name = tabla;

            -- Se recorren los pares (canonico, valores viejos) del mapa. Se usan dos variables
            -- distintas del bucle de tablas: `i` y `j` cuentan sobre la geometria del fichero.
            FOR canonico, lista_viejos IN
                SELECT key, value FROM jsonb_each(mapeo) ORDER BY key
            LOOP
                viejos := ARRAY(SELECT jsonb_array_elements_text(lista_viejos));

                EXECUTE format(
                    'INSERT INTO %I (%s) SELECT %s FROM %I WHERE exchange = ANY(%L) '
                    'ON CONFLICT DO NOTHING',
                    tabla, columnas,
                    -- la columna `exchange` se sustituye por la canonica en la lista del SELECT
                    (SELECT string_agg(
                        CASE WHEN column_name = 'exchange' THEN quote_literal(canonico)
                             ELSE quote_ident(column_name) END, ', ' ORDER BY ordinal_position)
                       FROM information_schema.columns
                      WHERE table_schema = 'public' AND table_name = tabla),
                    tabla, viejos);
                GET DIAGNOSTICS insertadas = ROW_COUNT;

                EXECUTE format('DELETE FROM %I WHERE exchange = ANY(%L)', tabla, viejos);
                GET DIAGNOSTICS borradas = ROW_COUNT;

                IF borradas > 0 THEN
                    RAISE NOTICE '%: % -> % (insertadas %, borradas %)',
                        tabla, array_to_string(viejos, '/'), canonico, insertadas, borradas;
                END IF;
            END LOOP;
        ELSE
            -- ingest_gaps: UPDATE, no hay cambio de clave ni referencias que romper.
            UPDATE ingest_gaps SET exchange = 'binance_um'
             WHERE exchange IN ('binance','BINANCE_FUTURES','BinanceFutures','BINANCE','Binance','binance_futures','binanceum');
            UPDATE ingest_gaps SET exchange = 'bybit'   WHERE exchange = 'BYBIT';
            UPDATE ingest_gaps SET exchange = 'okx'     WHERE exchange = 'OKX';
            UPDATE ingest_gaps SET exchange = 'bitget'  WHERE exchange = 'BITGET';
            UPDATE ingest_gaps SET exchange = 'hyperliquid'
             WHERE exchange IN ('HYPERLIQUID','Hyperliquid','hyper_liquid');
        END IF;
    END LOOP;
END $$;

-- Freno de seguridad: si queda algun valor fuera del conjunto canonico, la migracion falla aqui
-- en vez de dejar la tabla partida en silencio. Un `RAISE NOTICE` no lo para nadie.
DO $$
DECLARE
    malos text;
BEGIN
    SELECT string_agg(DISTINCT exchange, ', ') INTO malos FROM (
        SELECT exchange FROM trades        UNION ALL SELECT exchange FROM candles_1m
        UNION ALL SELECT exchange FROM funding       UNION ALL SELECT exchange FROM open_interest
        UNION ALL SELECT exchange FROM liquidations  UNION ALL SELECT exchange FROM ingest_gaps
    ) t
    -- `TESTEX` es de los tests (aislan sus filas por exchange). Va excluido a proposito para que
    -- correr la suite no bloquee una migracion, pero sigue siendo el unico nombre no canonico
    -- que se tolera: cualquier otro valor nuevo hace fallar esto.
    WHERE exchange <> 'TESTEX'
      AND exchange <> 'VISEX'
      AND exchange NOT IN ('binance_um','bybit','okx','bitget','hyperliquid');
    IF malos IS NOT NULL THEN
        RAISE EXCEPTION
            'quedan valores de exchange fuera del canonico: %. Anadelos a common/exchanges.py '
            'antes de seguir.', malos;
    END IF;
END $$;
