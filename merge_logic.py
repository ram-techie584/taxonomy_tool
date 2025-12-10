# merge_logic.py
"""
Final merge logic for taxonomy tool.

- Stage 1: merge multiple source rows (SAP/Vault/PowerBI/PO/Invoice)
           into ONE canonical record per part_number.
- Stage 2: merge existing DB row + one or more user-upload rows.
"""

from __future__ import annotations

import json
import math
from typing import Dict, List, Any, Optional


# === Utility functions ======================================================

def _is_missing(v: Any) -> bool:
    """Treat None / empty / NaN / 'nan' / 'null' as missing."""
    if v is None:
        return True
    if isinstance(v, float) and math.isnan(v):
        return True
    if isinstance(v, str):
        s = v.strip().lower()
        if s in ("", "nan", "none", "null"):
            return True
    return False


def _safe_str(v: Any) -> Optional[str]:
    if _is_missing(v):
        return None
    return str(v)


def _clean_for_json(obj: Any) -> Any:
    """
    Ensure no NaN/None weirdness goes into JSON columns.
    We only keep simple serialisable values.
    """
    if isinstance(obj, dict):
        return {k: _clean_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_clean_for_json(x) for x in obj]
    if _is_missing(obj):
        return None
    return obj


# === Stage 1: merge rows from multiple systems ==============================

def merge_records_by_part_number(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Merge multiple source rows for the same part_number.

    Strategy:
    - Iterate rows in order; later rows override earlier ones
      (you already control priority by ordering input list).
    - 'sources' field becomes a JSON list of:
        {"source_system": "...", "source_file": "..."}
    """

    merged: Dict[str, Any] = {}
    sources: List[Dict[str, Any]] = []

    for r in rows:
        # accumulate sources
        src = {
            "source_system": _safe_str(r.get("source_system")),
            "source_file": _safe_str(r.get("source_file")),
        }
        if src["source_system"] or src["source_file"]:
            sources.append(src)

        # merge non-missing fields
        for k, v in r.items():
            if k in ("sources",):  # we overwrite this ourselves
                continue
            if not _is_missing(v):
                merged[k] = v

    # final sources JSON (text)
    merged["sources"] = json.dumps(_clean_for_json(sources), ensure_ascii=False)

    return merged


# === Stage 2: merge DB row + user uploads ===================================

# Full schema except 'id' (we don't set that from Python)
DB_COLUMNS: List[str] = [
    "part_number",
    "updated_at",
    "stock_qty",
    "vendor_code",
    "abc_class",
    "commodity_code",
    "utilization_score",
    "material_group",
    "risk_rating",
    "cost",
    "purchase_uom",
    "notes",
    "description_clean",
    "drawing_no",
    "is_standard_part",
    "order_uom",
    "spec_grade",
    "spec_finish",
    "material",
    "dimensions",
    "last_modified",
    "description",
    "category_master",
    "analysis_comment",
    "created_date",
    "plant",
    "currency",
    "flag",
    "checkout_status",
    "remarks",
    "approval_status",
    "revision_no",
    "material_type",
    "avg_lead_time_days",
    "spec_weight",
    "no",
    "cad_type",
    "storage_location",
    "quantity",
    "criticality_index",
    "category_raw",
    "engineer_name",
    "active_flag",
    "file_size_mb",
    "valuation_type",
    "spec_tolerance",
    "movement_frequency",
    "order_date",
    "delivery_date",
    "pdf_page",
    "date",
    "due_date",
    "file_name",
    "sources",
    "lifecycle_state",
    "vendor_name",
    "cad_file",
    "source_system",
    "source_file",
]


def _parse_sources_json(s: Any) -> List[Dict[str, Any]]:
    """Decode existing sources JSON from DB row."""
    if _is_missing(s):
        return []
    if isinstance(s, list):
        # already structured
        return [_clean_for_json(x) for x in s]
    if isinstance(s, str):
        try:
            val = json.loads(s)
            if isinstance(val, list):
                return [_clean_for_json(x) for x in val]
            return [_clean_for_json(val)]
        except Exception:
            # treat plain stray string as one source entry
            return [{"raw": s}]
    # anything else
    return [_clean_for_json(s)]


def merge_db_with_user(
    db_row: Optional[Dict[str, Any]], user_rows: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Final Stage-2 merge rule:

    - Start with an empty record with all DB columns = None.
    - Overlay DB row values.
    - Overlay each user row (last user row wins).
      User non-empty values override DB values.
    - Maintain 'sources' as a JSON array:
        - existing entries from DB
        - plus user_upload entries for each user file
    """

    merged: Dict[str, Any] = {col: None for col in DB_COLUMNS}

    # 1) base from DB
    if db_row:
        for col in DB_COLUMNS:
            if col in db_row and not _is_missing(db_row[col]):
                merged[col] = db_row[col]

    # 2) overlay user rows (later rows win)
    for r in user_rows:
        for col in DB_COLUMNS:
            if col in r and not _is_missing(r[col]):
                merged[col] = r[col]

    # 3) ensure part_number is string and not empty
    pn = user_rows[-1].get("part_number") or (db_row or {}).get("part_number")
    merged["part_number"] = _safe_str(pn)

    # 4) sources: existing DB + user uploads
    base_sources = _parse_sources_json((db_row or {}).get("sources"))

    for r in user_rows:
        base_sources.append(
            {
                "source_system": _safe_str(r.get("source_system", "user_upload")),
                "source_file": _safe_str(r.get("source_file")),
            }
        )

    merged["sources"] = json.dumps(_clean_for_json(base_sources), ensure_ascii=False)

    return merged
