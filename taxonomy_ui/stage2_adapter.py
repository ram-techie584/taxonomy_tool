# taxonomy_ui/stage2_adapter.py

"""
Stage-2 adapter: runs the ingestion / cleansing / enrichment pipeline
from Django, updates PartMaster, and generates an Excel snapshot.

Used by: taxonomy_ui.views.upload_and_process
"""

from pathlib import Path
from collections import defaultdict

import pandas as pd
from django.db import transaction

from ingestion_utils import load_file
from cleansing import cleanup_pipeline
from enrichment_text import enrich_from_description
from merge_logic import merge_db_with_user
from taxonomy_ui.models import PartMaster


# --------------------------------------------------------------------
# PATHS / CONSTANTS
# --------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent   # project root (where manage.py is)
OUTPUT_DIR = BASE_DIR / "output"
SNAPSHOT_PATH = OUTPUT_DIR / "stage1_master_snapshot.xlsx"

REQUIRED_COLS = [
    "part_number",
    "dimensions",
    "description",
    "cost",
    "material",
    "vendor_name",
    "currency",
    "category_raw",
    "category_master",
    "source_system",
    "source_file",
]


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure all required columns exist for DB + snapshot.
    Extra columns (from enrichment) are kept at the end.
    """
    for col in REQUIRED_COLS:
        if col not in df.columns:
            df[col] = None

    # Required columns first, then all others
    other_cols = [c for c in df.columns if c not in REQUIRED_COLS]
    return df[REQUIRED_COLS + other_cols]


def run_stage2_from_django(uploaded_files):
    """
    Stage-2 pipeline (Render-safe, ORM-only).

    Steps:
      1. Read uploaded file(s) via `load_file`
      2. Run cleansing + enrichment
      3. Merge user rows with existing DB rows via `merge_db_with_user`
      4. Upsert into PartMaster
      5. Write snapshot Excel to output/stage1_master_snapshot.xlsx
      6. Return DataFrame for preview table

    Returns:
        pandas.DataFrame: final merged/cleaned dataset
    """

    if not uploaded_files:
        raise ValueError("No files uploaded.")

    # -------------------------------------------------
    # 1. Read all uploaded files into a single DataFrame
    # -------------------------------------------------
    dfs = []

    for f in uploaded_files:
        df_file = load_file(f)
        if df_file is None or df_file.empty:
            continue

        # Track provenance
        df_file["source_system"] = "user"
        df_file["source_file"] = f.name

        dfs.append(df_file)

    if not dfs:
        raise ValueError("No valid data found in uploaded files.")

    df_raw = pd.concat(dfs, ignore_index=True)

    # -------------------------------------------------
    # 2. Clean + enrich
    # -------------------------------------------------
    df_clean = cleanup_pipeline(df_raw)
    df_clean = enrich_from_description(df_clean)

    if "part_number" not in df_clean.columns:
        raise ValueError("Required column 'part_number' is missing after processing.")

    # Drop rows with missing part_number
    df_clean = df_clean[df_clean["part_number"].notna()].copy()
    df_clean["part_number"] = df_clean["part_number"].astype(str).str.strip()

    records = df_clean.to_dict(orient="records")

    # -------------------------------------------------
    # 3. Group by part_number
    # -------------------------------------------------
    grouped = defaultdict(list)
    for r in records:
        pn = r.get("part_number")
        if pn:
            grouped[pn].append(r)

    merged_results = []

    # -------------------------------------------------
    # 4. Merge USER data with DB (ORM) via merge_db_with_user
    # -------------------------------------------------
    for pn, user_rows in grouped.items():
        db_obj = PartMaster.objects.filter(part_number=pn).first()

        db_row = None
        if db_obj:
            # Only fields used in merge + required by Stage-1
            db_row = {
                "part_number": db_obj.part_number,
                "dimensions": db_obj.dimensions,
                "description": db_obj.description,
                "cost": db_obj.cost,
                "material": db_obj.material,
                "vendor_name": db_obj.vendor_name,
                "currency": db_obj.currency,
                "category_raw": db_obj.category_raw,
                "category_master": db_obj.category_master,
                "source_system": db_obj.source_system,
                "source_file": db_obj.source_file,
            }

        merged = merge_db_with_user(db_row, user_rows)
        merged_results.append(merged)

    if not merged_results:
        raise ValueError("Stage-2 pipeline produced no merged results.")

    df_out = pd.DataFrame(merged_results)
    df_out = normalize_columns(df_out)

    # -------------------------------------------------
    # 5. Write Excel snapshot for download + Stage-1
    # -------------------------------------------------
    OUTPUT_DIR.mkdir(exist_ok=True)
    df_out.to_excel(SNAPSHOT_PATH, index=False)

    # -------------------------------------------------
    # 6. UPSERT into PartMaster
    # -------------------------------------------------
    with transaction.atomic():
        for row in merged_results:
            pn = row.get("part_number")
            if not pn:
                continue

            PartMaster.objects.update_or_create(
                part_number=pn,
                defaults={
                    "dimensions": row.get("dimensions"),
                    "description": row.get("description"),
                    "cost": row.get("cost"),
                    "material": row.get("material"),
                    "vendor_name": row.get("vendor_name"),
                    "currency": row.get("currency"),
                    "category_raw": row.get("category_raw"),
                    "category_master": row.get("category_master"),
                    "source_system": row.get("source_system"),
                    "source_file": row.get("source_file"),
                },
            )

    # DataFrame is returned for preview in the UI
    return df_out
