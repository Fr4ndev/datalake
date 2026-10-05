"""Normalizacion de timestamps a UTC.

Regla 3 de AGENTS.md: nunca asumir la unidad, autodetectar por magnitud y normalizar a UTC, nunca
naive ni zona local. La regla original solo cubria 13 digitos (ms) y 16 (µs), pero el survey
(`docs/source-survey.md`) encontro un tercer caso real que la regla no preveia:

  - `metrics` de Binance Vision trae la columna `create_time` como el STRING `2020-09-01 00:00:00`,
    sin timezone y sin digitos. Si se aplicase a ciega la deteccion por magnitud, un parser
    ingenioso la convertiria a "epoch" y devolveria 1970-01-01.

Por eso el detector tiene tres ramas (epoch por magnitud / fecha ISO / ya es datetime) y las
fechas sin timezone se interpretan SIEMPRE como UTC, nunca como hora local.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone

# 13 digitos = ms, 16 = us, 10 = s. Se comparan longitudes de la parte entera, no magnitudes
# aproximadas: asi un microtimestamp no se confunde con un milisegundo.
_UNIT_BY_DIGITS = {10: "s", 13: "ms", 16: "us"}

_ISO_FORMATS = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%d",
)

_ISO_RE = re.compile(r"^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?(\.\d+)?)?$")


class TimestampError(ValueError):
    """El valor no se pudo convertir a un instante UTC inequivoco."""


def detect_unit(text: str) -> str:
    """Devuelve 's' | 'ms' | 'us' | 'iso' segun la forma del valor."""
    value = text.strip()
    if _ISO_RE.match(value):
        return "iso"
    negative = value.startswith("-")
    digits = value[1:] if negative else value
    if digits.isdigit():
        unit = _UNIT_BY_DIGITS.get(len(digits))
        if unit:
            return unit
    raise TimestampError(f"no se reconoce la unidad del timestamp {text!r}")


def to_utc(value: str | int | float | datetime) -> datetime:
    """Convierte a `datetime` con tzinfo=UTC.

    Acepta epoch (s/ms/us segun magnitud) o fecha ISO sin timezone (se asume UTC). Un entero o
    float tambien pasa por el detector de magnitud: no se asume que un `int` venga en segundos,
    porque la regla 3 prohibe precisamente esa suposicion.
    """
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)

    if isinstance(value, (int, float)):
        if float(value).is_integer():
            text = str(int(value))
        else:
            raise TimestampError(f"epoch con decimales no soportado: {value!r}")
    else:
        text = str(value)

    unit = detect_unit(text)
    if unit == "iso":
        normalized = text.strip().replace("T", " ")
        for fmt in _ISO_FORMATS:
            try:
                parsed = datetime.strptime(normalized, fmt)
            except ValueError:
                continue
            # Sin tzinfo en el origen: se interpreta como UTC de forma explicita.
            return parsed.replace(tzinfo=timezone.utc)
        raise TimestampError(f"fecha ISO no reconocida: {value!r}")

    divisor = {"s": 1, "ms": 1_000, "us": 1_000_000}[unit]
    return datetime.fromtimestamp(int(text) / divisor, tz=timezone.utc)


def to_epoch_ms(value: str | int | float | datetime) -> int:
    """Mismo criterio, devuelto en milisegundos epoch (para claves de dedup y del manifest)."""
    return int(to_utc(value).timestamp() * 1000)
