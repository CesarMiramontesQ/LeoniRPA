"""
Script: Sincronizar tabla partes desde ventas.producto_condensado.

- Ajusta la secuencia partes_id_seq por si está desincronizada (evita duplicate key en id).
- Lee los valores distintos de ventas.producto_condensado (no nulos, no vacíos).
- Para cada uno: si ya existe en partes.numero_parte → omite; si no existe → inserta en partes.
- Usa savepoint por inserción para que un fallo no invalide toda la transacción.
- Al final imprime un resumen: cuántos se agregaron y cuántos se omitieron.

Ejecutar desde la raíz del proyecto:
    python scripts/sync_partes_desde_ventas.py
"""

import asyncio
import sys
from pathlib import Path

root = Path(__file__).resolve().parent.parent
if str(root) not in sys.path:
    sys.path.insert(0, str(root))


async def main():
    from sqlalchemy import select, text
    from sqlalchemy.exc import IntegrityError
    from app.db.base import engine, AsyncSessionLocal
    from app.db.models import Venta, Parte
    from app.db import crud

    print("=" * 60)
    print("Sincronizar partes desde ventas.producto_condensado")
    print("=" * 60)

    agregados = 0
    omitidos = 0
    errores = 0

    async with AsyncSessionLocal() as db:
        try:
            # 1) Ajustar secuencia de partes.id por si quedó desincronizada (evita "Key (id)=(1) already exists")
            await db.execute(text(
                "SELECT setval(pg_get_serial_sequence('partes', 'id'), COALESCE((SELECT MAX(id) FROM partes), 1))"
            ))
            await db.commit()
            print("\nSecuencia partes_id_seq ajustada al MAX(id) actual.")

            # 2) Valores distintos de producto_condensado (no nulos, no vacíos)
            result = await db.execute(
                select(Venta.producto_condensado)
                .where(
                    Venta.producto_condensado.isnot(None),
                    Venta.producto_condensado != "",
                )
                .distinct()
            )
            productos = [row[0].strip() for row in result.all() if row[0] and row[0].strip()]
            productos = list(dict.fromkeys(productos))
            total = len(productos)
            print(f"Valores distintos de producto_condensado en ventas: {total}\n")

            for numero_parte in productos:
                existe = await crud.get_parte_by_numero(db, numero_parte)
                if existe:
                    omitidos += 1
                    continue
                # Inserción aislada con savepoint: si falla (p. ej. duplicado por race), no invalida el resto
                savepoint = await db.begin_nested()
                try:
                    parte = Parte(numero_parte=numero_parte, descripcion=None)
                    db.add(parte)
                    await db.flush()
                    await savepoint.commit()
                    agregados += 1
                except IntegrityError:
                    await savepoint.rollback()
                    omitidos += 1
                except Exception as e:
                    await savepoint.rollback()
                    errores += 1
                    print(f"  Error con '{numero_parte}': {e}", file=sys.stderr)

            await db.commit()
        except Exception as e:
            await db.rollback()
            print(f"\nError general: {e}", file=sys.stderr)
            raise

    print("\n" + "=" * 60)
    print("RESUMEN")
    print("=" * 60)
    print(f"  Total de producto_condensado distintos procesados: {total}")
    print(f"  Agregados a partes (nuevos):                      {agregados}")
    print(f"  Omitidos (ya existían en partes):                 {omitidos}")
    if errores:
        print(f"  Errores:                                         {errores}")
    print("=" * 60)

    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
