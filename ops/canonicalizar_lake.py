#!/usr/bin/env python
"""Renombra un exchange en el lake: ruta, Parquet y metadata. Idempotente.

Por que hace falta ademas de mover el directorio: el `exchange` esta **tambien dentro del
fichero** (`part.parquet`), no solo en la ruta. Mover `lake/binance/` a `lake/binance_um/` deja
177 MB de ficheros diciendo `binance` y una ruta diciendo `binance_um`, y uno de los dos se va a
equivocar al leer. Peor: el que se equivoca no da error, porque ambos nombres son "validos".

Que hace, en orden:
  1. Renombra `lake/{viejo}/` a `lake/{nuevo}/` con `os.rename` (atomico en el mismo sistema de
     ficheros). Si el viejo no existe y el nuevo si, no hace nada: ya esta migrado.
  2. Reescribe la columna `exchange` de cada `.parquet` que aun tenga el valor viejo, escribiendo
     antes en `.tmp` y moviendo despues (regla 7: renombre atomico, nunca escritura encima).
  3. Reescribe `manifest.jsonl` y `known_gaps.json`.

Idempotente de verdad: si se relanza, la carpeta vieja no existe, ningun Parquet tiene el valor viejo
y los JSON ya estan canónicos, asi que la segunda pasada no toca nada.

Uso:
    python ops/canonicalizar_lake.py --viejo binance --nuevo binance_um
    python ops/canonicalizar_lake.py --viejo binance --nuevo binance_um --simular
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent


def _canonico(nombre: str) -> str:
    sys.path.insert(0, str(RAIZ))
    from common.exchanges import canonico  # noqa: PLC0415 - se importa tras fijar sys.path

    return canonico(nombre)


def renombrar_carpeta(viejo: str, nuevo: str, raiz: Path, simular: bool) -> str:
    origen, destino = raiz / viejo, raiz / nuevo
    if not origen.exists():
        if destino.exists():
            return f"carpeta ya migrada ({destino.name})"
        raise SystemExit(f"no existe ni {origen} ni {destino}")
    if destino.exists():
        raise SystemExit(
            f"{destino} ya existe y {origen} tambien. Fusionar dos carpetas a mano: esto no lo "
            "hace por si, porque mover ficheros dentro de otra particion deja huecos sin avisar.")
    if simular:
        return f"moveria {origen} -> {destino}"
    os.rename(origen, destino)
    return f"{origen.name} -> {destino.name}"


def reescribir_parquet(viejo: str, nuevo: str, raiz: Path, simular: bool) -> tuple[int, int]:
    """Devuelve (ficheros con el valor viejo, ficheros reescritos)."""
    base = raiz / nuevo
    if not base.exists():
        base = raiz / viejo
    if not base.exists():
        return 0, 0
    import pyarrow.parquet as pq  # noqa: PLC0415

    con_valor_viejo = reescritos = 0
    import pyarrow as pa  # noqa: PLC0415

    for f in sorted(base.rglob("*.parquet")):
        # `ParquetFile.read()` y no `pq.read_table(f)`: al pasar una ruta dentro de un arbol
        # hive-partitioned, `read_table` infiere `symbol`/`tf`/`year` de la ruta y las junta con
        # las columnas del fichero, que traen `symbol` como diccionario. El resultado es
        # "Unable to merge: Field symbol has incompatible types: string vs dictionary", al
        # leer UN solo fichero. `ParquetFile` lee el fichero y nada mas.
        tabla = pq.ParquetFile(f).read()
        if "exchange" not in tabla.schema.names:
            continue
        valores = set(tabla.column("exchange").to_pylist())
        if viejo not in valores:
            continue
        con_valor_viejo += 1
        if simular:
            continue
        idx = tabla.schema.names.index("exchange")
        columnas = list(tabla.columns)
        columnas[idx] = pa.array([nuevo] * tabla.num_rows, type=pa.string())
        tmp = f.with_suffix(".parquet.tmp")
        pq.write_table(pa.Table.from_arrays(columnas, names=tabla.schema.names), tmp,
                       compression="zstd")
        os.replace(tmp, f)  # atomico: nunca se escribe encima del fichero bueno
        reescritos += 1
    return con_valor_viejo, reescritos


def reescribir_json(nombre: str, viejo: str, nuevo: str, simular: bool) -> int:
    """Devuelve cuantas entradas cambiaron. Aplica a `manifest.jsonl` y `known_gaps.json`."""
    total = 0
    manifest = RAIZ / "lake" / nombre
    if not manifest.exists():
        return 0
    if nombre.endswith(".jsonl"):
        salida = []
        for linea in manifest.read_text().splitlines():
            if not linea.strip():
                continue
            d = json.loads(linea)
            if d.get("exchange") == viejo:
                d["exchange"] = nuevo
                total += 1
            salida.append(json.dumps(d, ensure_ascii=False, sort_keys=True))
        if not simular and total:
            tmp = manifest.with_suffix(".jsonl.tmp")
            tmp.write_text("\n".join(salida) + "\n")
            os.replace(tmp, manifest)
        return total
    doc = json.loads(manifest.read_text())
    for hueco in doc.get("gaps", []):
        if hueco.get("exchange") == viejo:
            hueco["exchange"] = nuevo
            total += 1
    if not simular and total:
        tmp = manifest.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(doc, ensure_ascii=False, indent=2) + "\n")
        os.replace(tmp, manifest)
    return total


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--viejo", required=True)
    ap.add_argument("--nuevo", required=True)
    ap.add_argument("--simular", action="store_true")
    args = ap.parse_args()

    # Que el nombre nuevo sea el canonico de verdad y no una persona escribiendo a mano.
    canonico = _canonico(args.nuevo)
    if canonico != args.nuevo:
        raise SystemExit(f"--nuevo debe ser el canonico: {args.nuevo!r} se normaliza a "
                         f"{canonico!r}")
    _canonico(args.viejo)  # valida que el viejo tambien es conocido

    print(f"component=canonicalizar step=carpeta detalle={renombrar_carpeta(args.viejo, canonico, RAIZ / 'lake', args.simular)}")
    con_viejo, reescritos = reescribir_parquet(args.viejo, canonico, RAIZ / "lake", args.simular)
    print(f"component=canonicalizar step=parquet con_valor_viejo={con_viejo} reescritos={reescritos}")
    for nombre in ("manifest.jsonl", "known_gaps.json"):
        n = reescribir_json(nombre, args.viejo, canonico, args.simular)
        print(f"component=canonicalizar step={nombre} cambiadas={n}")
    print(f"component=canonicalizar step=done exchange={canonico} simular={args.simular}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
