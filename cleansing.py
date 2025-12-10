# cleansing.py

import re
import math
from typing import List

import numpy as np
import pandas as pd

from cleansing_config import COLUMN_SYNONYMS


PREFIXES = ["sap", "vault", "powerbi", "po", "invoice", "user"]


def _normalize_name(name: str) -> str:
    """
    Normalize column/field name:
    - lowercase
    - non-alnum -> '_'
    - strip leading prefixes like 'sap_', 'vault_', etc.
    """
    if not isinstance(name, str):
        name = str(name)
    s = name.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = s.strip("_")

    # strip known prefixes
    for p in PREFIXES:
        prefix = p + "_"
        if s.startswith(prefix):
            s = s[len(prefix):]
            break

    return s


def normalize_and_merge_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    1) Normalize names.
    2) Apply COLUMN_SYNONYMS.
    3) If multiple columns map to same canonical, keep ONE
       and merge non-null values with priority left-to-right.
    """
    df = df.copy()
    original_cols = list(df.columns)

    # map original -> normalized
    norm_map = {c: _normalize_name(c) for c in original_cols}

    # map normalized -> canonical
    groups = {}  # canonical -> list of original column names
    for orig, norm in norm_map.items():
        canonical = COLUMN_SYNONYMS.get(norm, norm)
        groups.setdefault(canonical, []).append(orig)

    new_df = df.copy()

    for canonical, cols in groups.items():
        if len(cols) == 1:
            orig = cols[0]
            if orig != canonical:
                new_df = new_df.rename(columns={orig: canonical})
        else:
            # combine multiple synonym columns into one
            new_df[canonical] = None
            for c in cols:
                series = new_df[c]
                series = series.replace({np.nan: None})
                # where canonical is null and this column has value -> fill
                mask = new_df[canonical].isna() & series.notna()
                new_df.loc[mask, canonical] = series[mask]
            # drop old synonym columns
            for c in cols:
                if c != canonical and c in new_df.columns:
                    new_df = new_df.drop(columns=[c])

    return new_df


def _clean_str(v):
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    s = str(v).strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return None
    return s


def basic_cleaning(df: pd.DataFrame) -> pd.DataFrame:
    """
    - Strip whitespace
    - Normalize 'nan' / 'None' / empty -> None
    """
    df = df.copy()
    for col in df.columns:
        df[col] = df[col].apply(_clean_str)
    return df


DIM_PATTERN = re.compile(
    r"(\d+(?:\.\d+)?)\s*[xX]\s*(\d+(?:\.\d+)?)\s*(mm|cm|inch|in)?",
    re.IGNORECASE,
)


def ensure_dimensions(df: pd.DataFrame) -> pd.DataFrame:
    """
    If 'dimensions' missing or empty, try to infer from description.
    """
    df = df.copy()
    if "dimensions" not in df.columns:
        df["dimensions"] = None

    def _infer_dim(row):
        dim = row.get("dimensions")
        if dim:
            return dim
        desc = row.get("description")
        if not desc:
            return None
        m = DIM_PATTERN.search(desc)
        if m:
            a, b, unit = m.groups()
            unit = unit or "mm"
            return f"{a}x{b} {unit}"
        return None

    df["dimensions"] = df.apply(_infer_dim, axis=1)
    return df


def ensure_category_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Make sure category_raw and category_master exist.
    For now, category_master = category_raw (enrichment may change later).
    """
    df = df.copy()
    if "category_raw" not in df.columns:
        df["category_raw"] = None
    if "category_master" not in df.columns:
        df["category_master"] = df["category_raw"]
    return df


def ensure_core_fields(df: pd.DataFrame) -> pd.DataFrame:
    """
    Make sure some core columns exist so later code doesn't break.
    """
    df = df.copy()
    need_cols = [
        "part_number",
        "description",
        "material",
        "dimensions",
        "category_raw",
        "category_master",
        "cost",
        "currency",
        "vendor_name",
    ]
    for c in need_cols:
        if c not in df.columns:
            df[c] = None
    return df


def cleanup_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    """
    Full cleansing pipeline used by Stage1 and Stage2.
    """
    if df is None or df.empty:
        return pd.DataFrame()

    df = normalize_and_merge_columns(df)
    df = basic_cleaning(df)
    df = ensure_dimensions(df)
    df = ensure_category_columns(df)
    df = ensure_core_fields(df)
    return df
