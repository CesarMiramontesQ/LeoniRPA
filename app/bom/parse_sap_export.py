"""
Parser del archivo de exportación SAP BOM (transacción CS13).
Formato: cabecera con Material, Plant/Usage/Alt., Description, Base Qty, Reqd Qty;
luego tabla pipe-delimited con Object ID, Object description, Item, Un, Quantity, Component.
"""
import re
from decimal import Decimal
from typing import List, Optional

from app.bom.schemas import LoadBomInput, BomComponenteInput


def _parse_european_number(s: str) -> Optional[Decimal]:
    """Convierte número en formato europeo (1.000,000 o 1,175) a Decimal."""
    if not s or not s.strip():
        return None
    s = s.strip().replace(" ", "")
    # Eliminar separador de miles (.) y usar (,) como decimal
    s = s.replace(".", "").replace(",", ".")
    try:
        return Decimal(s)
    except Exception:
        return None


def _normalize_col(s: str) -> str:
    return " ".join(s.strip().lower().split())


def _parse_header_columns(header_line: str) -> dict:
    """Extrae índices de columnas por nombre (normalizado sin espacios extra)."""
    parts = [p.strip() for p in header_line.split("|")]
    indices = {}
    for i, part in enumerate(parts):
        key = _normalize_col(part)
        if key == "object id":
            indices["object_id"] = i
        elif key == "object description":
            indices["object_description"] = i
        elif key == "component":
            indices["component"] = i
        elif key == "item":
            indices["item"] = i
        elif key == "un":
            indices["un"] = i  # última Un gana (la del componente)
        elif key == "quantity":
            indices["quantity"] = i  # última Quantity (cantidad del componente)
    return indices


def parse_sap_bom_txt(content: str) -> Optional[LoadBomInput]:
    """
    Parsea el contenido de un .txt exportado por SAP CS13.
    Retorna LoadBomInput o None si el formato no es válido.
    """
    lines = [line.rstrip("\r\n") for line in content.splitlines()]
    if len(lines) < 10:
        return None

    # Cabecera fija (líneas 3-7, índice 2-6)
    parte_no = ""
    plant = "US10"
    usage = "1"
    alternative = "01"
    descripcion_padre: Optional[str] = None
    base_qty: Optional[Decimal] = None
    reqd_qty: Optional[Decimal] = None
    base_unit = "M"

    for i in range(2, min(7, len(lines))):
        line = lines[i]
        if line.startswith("Material"):
            m = re.match(r"Material\s+(.+)", line, re.IGNORECASE)
            if m:
                parte_no = m.group(1).strip()
        elif "Plant/Usage/Alt." in line or "Plant" in line:
            m = re.search(r"(\w+)\s*/\s*(\w+)\s*/\s*(\w+)", line)
            if m:
                plant, usage, alternative = m.group(1).strip(), m.group(2).strip(), m.group(3).strip()
        elif line.startswith("Description"):
            m = re.match(r"Description\s+(.+)", line, re.IGNORECASE)
            if m:
                descripcion_padre = m.group(1).strip() or None
        elif "Base Qty" in line:
            m = re.search(r"Base Qty\s*\(([^)]*)\)\s*(.+)", line, re.IGNORECASE)
            if m:
                base_unit = m.group(1).strip() or "M"
                base_qty = _parse_european_number(m.group(2))
            else:
                parts = line.split()
                for p in reversed(parts):
                    n = _parse_european_number(p)
                    if n is not None:
                        base_qty = n
                        break
        elif "Reqd Qty" in line:
            m = re.search(r"Reqd Qty\s*\(([^)]*)\)\s*(.+)", line, re.IGNORECASE)
            if m:
                reqd_qty = _parse_european_number(m.group(2))
            else:
                parts = line.split()
                for p in reversed(parts):
                    n = _parse_european_number(p)
                    if n is not None:
                        reqd_qty = n
                        break

    if not parte_no:
        return None

    # Buscar línea de cabecera de la tabla (contiene "Object" y "Component")
    header_idx: Optional[int] = None
    col_map: Optional[dict] = None
    for idx in range(7, min(12, len(lines))):
        if "|" in lines[idx] and "Object" in lines[idx] and "Component" in lines[idx]:
            col_map = _parse_header_columns(lines[idx])
            if col_map.get("object_id") is not None:
                header_idx = idx
                break
    if header_idx is None or col_map is None:
        return LoadBomInput(
            parte_no=parte_no,
            descripcion_padre=descripcion_padre,
            plant=plant,
            usage=usage,
            alternative=alternative,
            base_qty=base_qty,
            reqd_qty=reqd_qty,
            base_unit=base_unit,
            componentes=[],
        )

    componentes: List[BomComponenteInput] = []
    for idx in range(header_idx + 1, len(lines)):
        line = lines[idx]
        if not line.strip() or not line.startswith("|") or "---" in line:
            continue
        parts = [p.strip() for p in line.split("|")]
        if len(parts) <= max(col_map.get("object_id", 0), col_map.get("quantity", 0)):
            continue
        component_no = (parts[col_map["object_id"]] if col_map.get("object_id") is not None else "").strip()
        if not component_no:
            continue
        desc = (parts[col_map["object_description"]] if col_map.get("object_description") is not None else "").strip() or None
        item_no = (parts[col_map["item"]] if col_map.get("item") is not None else "").strip() or None
        un = (parts[col_map["un"]] if col_map.get("un") is not None else "").strip() or None
        qty_str = (parts[col_map["quantity"]] if col_map.get("quantity") is not None else "").strip()
        qty = _parse_european_number(qty_str)
        if qty is None:
            qty = Decimal("0")
        componentes.append(
            BomComponenteInput(
                componente_no=component_no,
                descripcion=desc,
                qty=qty,
                measure=un,
                origin=None,
                item_no=item_no,
            )
        )

    return LoadBomInput(
        parte_no=parte_no,
        descripcion_padre=descripcion_padre,
        plant=plant,
        usage=usage,
        alternative=alternative,
        base_qty=base_qty,
        reqd_qty=reqd_qty,
        base_unit=base_unit,
        componentes=componentes,
    )
