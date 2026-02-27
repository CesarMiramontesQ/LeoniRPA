"""
Recalcula partes.qty_total con regla de UOM y actualiza partes.diferencia.

Regla de qty_total:
- Si measure == 'M': contribución = (qty / 1000) * kgm(componente)
  donde kgm(componente) se busca en peso_neto por número de parte completo.
- Si measure != 'M': contribución = qty / 1000.

Regla de diferencia:
- Si una parte tiene al menos un componente en 'M' sin kgm encontrado, diferencia = NULL.
- Si no hay faltantes y existe peso_neto.kgm de la parte padre (match exacto), diferencia = qty_total - kgm.
- Si no existe kgm de la parte padre, diferencia = NULL.

Uso:
    python scripts/recalcular_qty_total_regla_m.py
"""

import asyncio
from pathlib import Path
import sys

from sqlalchemy import text


# Asegurar raíz del proyecto en sys.path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.db.base import engine  # noqa: E402


SQL_UPDATE_QTY_TOTAL = text(
    """
    WITH componentes AS (
        SELECT
            b.parte_id,
            bi.qty::numeric AS qty,
            upper(trim(coalesce(bi.measure, ''))) AS measure,
            pcomp.numero_parte AS comp_no
        FROM bom_item bi
        JOIN bom_revision br
            ON br.id = bi.bom_revision_id
           AND br.effective_to IS NULL
        JOIN bom b
            ON b.id = br.bom_id
        JOIN partes pcomp
            ON pcomp.id = bi.componente_id
    ),
    componentes_con_kgm AS (
        SELECT
            c.parte_id,
            c.qty,
            c.measure,
            pn.kgm
        FROM componentes c
        LEFT JOIN peso_neto pn
            ON pn.numero_parte = c.comp_no
    ),
    totales AS (
        SELECT
            parte_id,
            SUM(
                CASE
                    WHEN measure = 'M' THEN (qty / 1000.0) * coalesce(kgm, 0)
                    ELSE (qty / 1000.0)
                END
            )::numeric(18, 6) AS qty_total_nuevo
        FROM componentes_con_kgm
        GROUP BY parte_id
    )
    UPDATE partes p
    SET qty_total = coalesce(t.qty_total_nuevo, 0)
    FROM totales t
    WHERE t.parte_id = p.id
    """
)


SQL_SET_QTY_ZERO_WITHOUT_BOM = text(
    """
    UPDATE partes p
    SET qty_total = 0
    WHERE NOT EXISTS (
        SELECT 1
        FROM bom b
        JOIN bom_revision br
            ON br.bom_id = b.id
           AND br.effective_to IS NULL
        JOIN bom_item bi
            ON bi.bom_revision_id = br.id
        WHERE b.parte_id = p.id
    )
    """
)


SQL_UPDATE_DIFERENCIA = text(
    """
    WITH faltantes_m AS (
        SELECT DISTINCT b.parte_id
        FROM bom_item bi
        JOIN bom_revision br
            ON br.id = bi.bom_revision_id
           AND br.effective_to IS NULL
        JOIN bom b
            ON b.id = br.bom_id
        JOIN partes pcomp
            ON pcomp.id = bi.componente_id
        LEFT JOIN peso_neto pn_comp
            ON pn_comp.numero_parte = pcomp.numero_parte
        WHERE upper(trim(coalesce(bi.measure, ''))) = 'M'
          AND pn_comp.kgm IS NULL
    ),
    estado AS (
        SELECT
            p.id AS parte_id,
            pn_padre.kgm AS kgm_padre,
            (fm.parte_id IS NOT NULL) AS tiene_faltante_m
        FROM partes p
        LEFT JOIN peso_neto pn_padre
            ON pn_padre.numero_parte = p.numero_parte
        LEFT JOIN faltantes_m fm
            ON fm.parte_id = p.id
    )
    UPDATE partes p
    SET diferencia = CASE
        WHEN e.tiene_faltante_m THEN NULL
        WHEN e.kgm_padre IS NULL THEN NULL
        ELSE p.qty_total - e.kgm_padre
    END
    FROM estado e
    WHERE e.parte_id = p.id
    """
)


SQL_SET_DIFERENCIA_NULL_NO_MATCH = text(
    """
    UPDATE partes p
    SET diferencia = NULL
    WHERE NOT EXISTS (
        SELECT 1
        FROM peso_neto pn
        WHERE pn.numero_parte = p.numero_parte
          AND pn.kgm IS NOT NULL
    )
    """
)


SQL_RESUMEN = text(
    """
    WITH comps_m AS (
        SELECT DISTINCT bi.componente_id
        FROM bom_item bi
        JOIN bom_revision br
            ON br.id = bi.bom_revision_id
           AND br.effective_to IS NULL
        WHERE upper(trim(coalesce(bi.measure, ''))) = 'M'
    ),
    faltantes_m AS (
        SELECT DISTINCT b.parte_id
        FROM bom_item bi
        JOIN bom_revision br
            ON br.id = bi.bom_revision_id
           AND br.effective_to IS NULL
        JOIN bom b
            ON b.id = br.bom_id
        JOIN partes pcomp
            ON pcomp.id = bi.componente_id
        LEFT JOIN peso_neto pn_comp
            ON pn_comp.numero_parte = pcomp.numero_parte
        WHERE upper(trim(coalesce(bi.measure, ''))) = 'M'
          AND pn_comp.kgm IS NULL
    )
    SELECT 'componentes_m_unicos', (SELECT COUNT(*) FROM comps_m)::text
    UNION ALL
    SELECT 'partes_con_faltantes_m', (SELECT COUNT(*) FROM faltantes_m)::text
    UNION ALL
    SELECT 'partes_diferencia_null', (SELECT COUNT(*) FROM partes WHERE diferencia IS NULL)::text
    """
)


async def main() -> None:
    print("=" * 70)
    print("Recalculando qty_total con regla UOM=M + KGM (match exacto)")
    print("=" * 70)

    try:
        async with engine.begin() as conn:
            qty_updated = await conn.execute(SQL_UPDATE_QTY_TOTAL)
            qty_zeroed = await conn.execute(SQL_SET_QTY_ZERO_WITHOUT_BOM)
            diff_updated = await conn.execute(SQL_UPDATE_DIFERENCIA)
            diff_null_no_match = await conn.execute(SQL_SET_DIFERENCIA_NULL_NO_MATCH)
            resumen = await conn.execute(SQL_RESUMEN)

        print(f"✓ qty_total actualizado en partes con BOM: {int(qty_updated.rowcount or 0)}")
        print(f"✓ qty_total = 0 en partes sin BOM vigente: {int(qty_zeroed.rowcount or 0)}")
        print(f"✓ diferencia recalculada (incluye NULL por faltantes M): {int(diff_updated.rowcount or 0)}")
        print(f"✓ diferencia = NULL por falta de KGM en parte padre: {int(diff_null_no_match.rowcount or 0)}")

        print("\nResumen:")
        for row in resumen.fetchall():
            print(f"- {row[0]}: {row[1]}")

        print("\nProceso completado.")
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
