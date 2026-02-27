"""
Importa descripción aduanal y fracción desde fraciones.xlsx a la tabla partes.

Uso (desde la raíz del proyecto):
  python scripts/import_fracciones_desde_excel.py
  python scripts/import_fracciones_desde_excel.py --file "fraciones.xlsx" --sheet "Hoja1"
  python scripts/import_fracciones_desde_excel.py --dry-run
  python scripts/import_fracciones_desde_excel.py --match-prefix9

Comportamiento:
  - Lee columnas:
      - Numero Parte (obligatoria)
      - DESCRIPCION / Descripcion (obligatoria)
      - Descripcion temporal (obligatoria)
  - Busca en partes por este orden:
      1) Coincidencia exacta por numero_parte.
      2) (Opcional) Coincidencia normalizada (mayúsculas + quita punto final).
      3) (Opcional) Coincidencia por prefijo de 9 caracteres.
  - Solo actualiza registros existentes:
      - descripcion_aduanal = DESCRIPCION
      - fraccion según mapeo de Descripcion temporal
"""

import argparse
import asyncio
import sys
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from sqlalchemy import select, text

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.db.base import AsyncSessionLocal, engine  # noqa: E402
from app.db.models import Parte  # noqa: E402


FRACCION_MAP = {
    "cuerda": "7413000200",
    "alambre": "7408199900",
    "cable estandar": "8544499904",
    "coaxial": "8544200100",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Importar fracciones a la tabla partes desde Excel.")
    parser.add_argument("--file", default="fraciones.xlsx", help='Ruta del archivo Excel (default: "fraciones.xlsx")')
    parser.add_argument("--sheet", default="Hoja1", help='Nombre de hoja (default: "Hoja1")')
    parser.add_argument("--dry-run", action="store_true", help="Simula la importación sin guardar cambios")
    parser.add_argument(
        "--match-normalized",
        action="store_true",
        default=True,
        help="Habilita match normalizado de numero_parte (default: activo).",
    )
    parser.add_argument(
        "--no-match-normalized",
        action="store_false",
        dest="match_normalized",
        help="Desactiva match normalizado de numero_parte.",
    )
    parser.add_argument(
        "--match-prefix9",
        action="store_true",
        help="Habilita fallback por prefijo de 9 caracteres (solo si match es único).",
    )
    return parser.parse_args()


def normalize_col(name: Any) -> str:
    if name is None:
        return ""
    s = str(name).strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return " ".join(s.split())


def normalize_part_no(value: Any) -> str:
    s = str(value or "").strip().upper()
    while s.endswith("."):
        s = s[:-1]
    return s


def normalize_temp_desc(value: Any) -> str:
    s = str(value or "").strip()
    if not s or s.lower() == "nan":
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return " ".join(s.lower().split())


async def run_import(file_path: Path, sheet_name: str, dry_run: bool, match_normalized: bool, match_prefix9: bool) -> None:
    if not file_path.is_file():
        raise FileNotFoundError(f"No existe el archivo: {file_path}")

    print("=" * 78)
    print("Importar fracciones y descripción aduanal desde Excel")
    print("=" * 78)
    print(f"Archivo:           {file_path}")
    print(f"Hoja:              {sheet_name}")
    print(f"Modo:              {'DRY-RUN (sin guardar)' if dry_run else 'ESCRITURA'}")
    print(f"Match normalizado: {'Sí' if match_normalized else 'No'}")
    print(f"Match prefijo9:    {'Sí' if match_prefix9 else 'No'}")

    excel = pd.ExcelFile(file_path)
    hojas = list(excel.sheet_names)
    if sheet_name in hojas:
        hoja_objetivo = sheet_name
    elif hojas:
        hoja_objetivo = hojas[0]
        print(f"Hoja '{sheet_name}' no existe. Se usará: {hoja_objetivo}")
    else:
        raise ValueError("El archivo no contiene hojas.")

    df = pd.read_excel(file_path, sheet_name=hoja_objetivo, dtype=object)
    if df.empty:
        print("\nEl archivo no contiene filas.")
        return

    col_map = {normalize_col(c): c for c in df.columns}
    col_num = col_map.get("numero parte") or col_map.get("numero de parte")
    col_desc = col_map.get("descripcion")
    col_temp = col_map.get("descripcion temporal") or col_map.get("descripcion_temporal")

    if not col_num or not col_desc or not col_temp:
        raise ValueError(
            "No se encontraron columnas requeridas: Numero Parte, Descripcion, Descripcion temporal."
        )

    total_filas = len(df)
    filas_invalidas = 0
    duplicados_archivo = 0
    candidatos: Dict[str, Tuple[str, Optional[str], str]] = {}

    # Última ocurrencia gana por Numero Parte
    for _, row in df.iterrows():
        numero_raw = row.get(col_num)
        numero = str(numero_raw).strip() if numero_raw is not None else ""
        if not numero or numero.lower() == "nan":
            filas_invalidas += 1
            continue

        desc_aduanal = str(row.get(col_desc) or "").strip()
        temp_norm = normalize_temp_desc(row.get(col_temp))
        fraccion = FRACCION_MAP.get(temp_norm)

        if numero in candidatos:
            duplicados_archivo += 1
        candidatos[numero] = (desc_aduanal, fraccion, temp_norm)

    async with AsyncSessionLocal() as db:
        cols = await db.execute(text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'partes'
              AND column_name IN ('descripcion_aduanal', 'fraccion')
        """))
        cols_present = {r[0] for r in cols.all()}
        if "descripcion_aduanal" not in cols_present or "fraccion" not in cols_present:
            raise RuntimeError(
                "Faltan columnas en BD (descripcion_aduanal/fraccion). Ejecuta run_migrations.py primero."
            )

        partes = (await db.execute(select(Parte))).scalars().all()
        by_exact = {p.numero_parte: p for p in partes}

        norm_map: Dict[str, List[Parte]] = {}
        for p in partes:
            norm_map.setdefault(normalize_part_no(p.numero_parte), []).append(p)

        prefix9_map: Dict[str, List[Parte]] = {}
        for p in partes:
            key = normalize_part_no(p.numero_parte)[:9]
            if key:
                prefix9_map.setdefault(key, []).append(p)

        exact_hits = 0
        normalized_hits = 0
        prefix9_hits = 0
        actualizados = 0
        fraccion_asignada = 0
        fraccion_sin_mapeo = 0
        no_encontrados: List[str] = []
        ambiguos: List[Tuple[str, List[str]]] = []

        for numero_excel, (desc_aduanal, fraccion, _) in candidatos.items():
            target: Optional[Parte] = by_exact.get(numero_excel)
            hit_mode = None

            if target is not None:
                hit_mode = "exact"
            elif match_normalized:
                cands = norm_map.get(normalize_part_no(numero_excel), [])
                if len(cands) == 1:
                    target = cands[0]
                    hit_mode = "normalized"
                elif len(cands) > 1:
                    ambiguos.append((numero_excel, [c.numero_parte for c in cands]))
                    continue

            if target is None and match_prefix9:
                cands = prefix9_map.get(normalize_part_no(numero_excel)[:9], [])
                if len(cands) == 1:
                    target = cands[0]
                    hit_mode = "prefix9"
                elif len(cands) > 1:
                    ambiguos.append((numero_excel, [c.numero_parte for c in cands]))
                    continue

            if target is None:
                no_encontrados.append(numero_excel)
                continue

            if hit_mode == "exact":
                exact_hits += 1
            elif hit_mode == "normalized":
                normalized_hits += 1
            elif hit_mode == "prefix9":
                prefix9_hits += 1

            target.descripcion_aduanal = desc_aduanal or None
            target.fraccion = fraccion
            if fraccion:
                fraccion_asignada += 1
            else:
                fraccion_sin_mapeo += 1
            actualizados += 1

        if dry_run:
            await db.rollback()
        else:
            await db.commit()

    print("\n" + "=" * 78)
    print("RESUMEN")
    print("=" * 78)
    print(f"  Total filas en Excel:                {total_filas}")
    print(f"  Filas inválidas (sin número parte):  {filas_invalidas}")
    print(f"  Duplicados en archivo:               {duplicados_archivo}")
    print(f"  Candidatos únicos:                   {len(candidatos)}")
    print(f"  Actualizados en partes:              {actualizados}")
    print(f"    - Match exacto:                    {exact_hits}")
    print(f"    - Match normalizado:               {normalized_hits}")
    print(f"    - Match prefijo9:                  {prefix9_hits}")
    print(f"  Fracción asignada:                   {fraccion_asignada}")
    print(f"  Fracción sin mapeo:                  {fraccion_sin_mapeo}")
    print(f"  No encontrados:                      {len(no_encontrados)}")
    print(f"  Ambiguos (sin actualizar):           {len(ambiguos)}")
    print("=" * 78)

    if no_encontrados:
        print("\nNo encontrados:")
        for n in sorted(no_encontrados):
            print(f"  - {n}")

    if ambiguos:
        print("\nAmbiguos:")
        for src, cands in ambiguos:
            print(f"  - {src} -> {cands}")


async def main() -> None:
    args = parse_args()
    file_path = Path(args.file)
    if not file_path.is_absolute():
        file_path = (ROOT / file_path).resolve()

    await run_import(
        file_path=file_path,
        sheet_name=args.sheet,
        dry_run=args.dry_run,
        match_normalized=args.match_normalized,
        match_prefix9=args.match_prefix9,
    )
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
