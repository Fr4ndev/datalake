---
name: telegram-strategy-receiver
description: Bot de Telegram (aiogram v3) que recibe señales/órdenes de estrategia, las valida contra el schema y las persiste para su ejecución en el backtester. Usar al implementar el servicio bot y el contrato de señales.
---

# Alcance
Receptor de señales. NO ejecuta órdenes contra el exchange: valida, normaliza, persiste y responde. El envío de órdenes es una fase posterior y debe vivir detrás de un flag `--live` explícito (por defecto apagado).

# Secretos (regla 10)
- `TELEGRAM_BOT_TOKEN` y `TELEGRAM_CHAT_ID` solo vía `.env` + `env_file`. Nunca en el repo, ni en logs, ni en el mensaje de error.
- Al loggear un update, enmascarar el token si aparece por error.

# Contrato de señal (estable)
```json
{
  "signal_id": "uuid4",
  "ts": "2026-01-01T00:00:00Z",
  "exchange": "binance",
  "symbol": "BTCUSDT",
  "dtype": "entry|exit|close",
  "side": "long|short|flat",
  "price": 0.0,
  "size": 0.0,
  "tf": "1m",
  "strategy": "nombre_versionado",
  "meta": {}
}
```
Reglas de validación (pydantic, rechaza con mensaje claro):
- `ts` obligatorio, ISO-8601 con tz; se normaliza a UTC. Naive o con offset no-UTC → error.
- `symbol` en mayúsculas y contra la lista de Perpetuos del exchange.
- `price > 0` y `size > 0` salvo `dtype=close`.
- `size` en quote o en base: fijarlo en el schema (por defecto quote) y no cambiarlo bajo los pies del consumidor.
- `strategy` con versión semántica; sin versión no se persiste.
- Rechazar duplicados por `signal_id` (PK): reenvíos del usuario no crean dos señales.

# Comportamiento del handler
- Comandos: `/start`, `/help`, `/health`, `/stats`, `/last`. Respuestas en menos de 2 s.
- Un mensaje que no parsea → respuesta con el error exacto y sin stacktrace al usuario.
- Persistir en Postgres (`signals`) con `ON CONFLICT (signal_id) DO NOTHING` (regla 7).
- Tras persistir, confirmar al usuario con `signal_id` corto (8 chars) para que sea referenciable.

# aiogram v3 (verificar API, no memorizar)
- v3 es async: `Dispatcher()`, `dp.start_polling(bot)`.
- handlers con `@dp.message(Command("start"))` y `@dp.message(F.text)`.
- Filtros en `dp.message.register(handler, F.text)`.
- El token va al construir `Bot(token=...)`. Si el bot ya estaba arrancado, la sesión actual no lo verá: reiniciar.

# Logging (regla 9)
`key=value`: `{update_id, chat_id, symbol, dtype, accepted, reason, elapsed}`. Nunca el texto completo del mensaje si puede contener credenciales.

# Criterio de done
- Tests pytest del parser del contrato: señal válida, ts naive (rechazo), precio 0 (rechazo), duplicado por signal_id (idempotente), símbolo desconocido (rechazo).
- `/health` responde y el servicio sobrevive a un mensaje malformado sin reiniciarse.
- Enviar la misma señal 3 veces → 1 fila en `signals`.
- Secretos ausentes → el servicio falla al arrancar con mensaje claro, no con traceback de import.