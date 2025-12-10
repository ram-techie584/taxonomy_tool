# enrichment_text.py

import re
from typing import Dict, Any

import pandas as pd


MATERIAL_KEYWORDS = {
    "steel": "Steel",
    "stainless": "Stainless Steel",
    "aluminum": "Aluminum",
    "aluminium": "Aluminum",
    "copper": "Copper",
    "brass": "Brass",
    "nylon": "Nylon",
    "plastic": "Plastic",
    "rubber": "Rubber",
}


CATEGORY_KEYWORDS = {
    "bearing": "Bearing",
    "bracket": "Bracket",
    "valve": "Valve",
    "roller": "Roller",
    "screw": "Screw",
    "motor": "Motor",
    "clamp": "Clamp",
    "solenoid": "Solenoid",
}


def _infer_material_from_text(text: str) -> str | None:
    low = text.lower()
    for k, v in MATERIAL_KEYWORDS.items():
        if k in low:
            return v
    return None


def _infer_category_from_text(text: str) -> str | None:
    low = text.lower()
    for k, v in CATEGORY_KEYWORDS.items():
        if k in low:
            return v
    return None


def enrich_from_description(df: pd.DataFrame) -> pd.DataFrame:
    """
    Use simple NLP-style keyword rules to fill material, category_raw/category_master
    if missing, based on description.
    """
    df = df.copy()

    def _enrich_row(row: Dict[str, Any]) -> Dict[str, Any]:
        desc = row.get("description") or ""
        desc = str(desc).strip()
        if not desc:
            return row

        # material
        if not row.get("material"):
            mat = _infer_material_from_text(desc)
            if mat:
                row["material"] = mat

        # category
        if not row.get("category_raw"):
            cat = _infer_category_from_text(desc)
            if cat:
                row["category_raw"] = cat
                row["category_master"] = cat
        elif not row.get("category_master"):
            row["category_master"] = row["category_raw"]

        return row

    df = df.apply(lambda r: pd.Series(_enrich_row(r.to_dict())), axis=1)
    return df
