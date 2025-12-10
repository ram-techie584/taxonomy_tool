# mapping_engine.py
"""
Enterprise mapping of raw system-specific columns → normalized master schema
with full source_payload JSON preserved.
"""

import math
from typing import List, Dict, Any


def _val(v):
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    s = str(v).strip()
    if not s or s.lower() in ("nan", "null", "none"):
        return None
    return s


# Source priority for canonical fields: later in list = higher priority
PRIORITY = [
    "powerbi",
    "vault",
    "sap",
    "pos",
    "invoices",
    "user_upload",
]

FIELD_MAP = {
    "description": [
        "description",
        "sap_description_raw",
        "sap_description_clean",
        "vault_description",
        "powerbi_description",
        "invoice_description_raw",
        "po_description_raw",
        "description_raw",
        "user_description",
    ],
    "material": [
        "material",
        "sap_material",
        "sap_spec_material",
        "vault_material",
        "powerbi_material",
        "spec_material",
        "user_material",
    ],
    "dimensions": [
        "dimensions",
        "sap_spec_dimensions",
        "vault_dimensions",
        "user_dimensions",
    ],
    "category_raw": [
        "category_raw",
        "sap_category",
        "powerbi_category",
        "vault_category",
        "sub_category",
        "user_category",
    ],
    "cost": [
        "cost",  # after normalization, most prices map here
        "sap_price_per_uom",
        "price_per_unom",
        "invoice_unit_price",
        "po_price_per_unit",
    ],
    "currency": [
        "currency",
        "sap_currency",
        "invoice_currency",
        "po_currency",
    ],
    "vendor_name": [
        "vendor_name",
        "sap_vendor_name",
        "invoice_vendor_name",
        "po_vendor_name",
    ],
}


def resolve_field(fieldname: str, rows: List[Dict[str, Any]]) -> Any:
    keys = FIELD_MAP[fieldname]

    # first, by priority (high-priority system wins)
    for src in reversed(PRIORITY):
        for r in rows:
            if r.get("source_system") != src:
                continue
            for k in keys:
                v = _val(r.get(k))
                if v is not None:
                    return v

    # fallback: any row
    for r in rows:
        for k in keys:
            v = _val(r.get(k))
            if v is not None:
                return v

    return None


def _clean_row_for_payload(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Remove None/NaN and ensure everything is JSON-friendly.
    """
    cleaned = {}
    for k, v in row.items():
        if v is None:
            continue
        if isinstance(v, float) and math.isnan(v):
            continue
        cleaned[str(k)] = v
    return cleaned


def build_payload(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Build payload structure like:
    {
      "sap": [ {...}, ... ],
      "vault": [...],
      ...
    }
    """
    payload: Dict[str, Any] = {}
    for r in rows:
        sys = r.get("source_system", "unknown") or "unknown"
        sys = str(sys)
        cleaned = _clean_row_for_payload(r)
        if not cleaned:
            continue
        if sys not in payload:
            payload[sys] = []
        payload[sys].append(cleaned)
    return payload


def map_group_to_master(part_number: str, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    master = {
        "part_number": part_number,
        "description": resolve_field("description", rows),
        "material": resolve_field("material", rows),
        "dimensions": resolve_field("dimensions", rows),
        "category_raw": resolve_field("category_raw", rows),
        "category_master": None,  # will be re-derived if needed
        "cost": resolve_field("cost", rows),
        "currency": resolve_field("currency", rows),
        "vendor_name": resolve_field("vendor_name", rows),
        "source_system": None,
        "source_file": None,
        "source_payload": build_payload(rows),
    }

    # pick "best" source_system/file by priority
    for src in reversed(PRIORITY):  # last is highest
        for r in rows:
            if r.get("source_system") == src:
                master["source_system"] = r.get("source_system")
                master["source_file"] = r.get("source_file")
                return master

    # fallback: first row
    if rows:
        master["source_system"] = rows[0].get("source_system")
        master["source_file"] = rows[0].get("source_file")

    return master
