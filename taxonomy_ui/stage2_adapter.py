# taxonomy_ui/stage2_adapter.py

"""
Stage-2 adapter: runs the ingestion / cleansing / enrichment pipeline
from Django, updates PartMaster, and generates an Excel snapshot.

Render-safe version: processes files directly from memory using the 
existing load_file() function from ingestion_utils.py
"""

from pathlib import Path
from collections import defaultdict
import io
import os

import pandas as pd
from django.db import transaction

# ✅ Use the EXISTING load_file function from ingestion_utils
#    It's already Render-safe!
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
    
    Uses the existing load_file() function from ingestion_utils.py
    which is already written to handle files in memory.

    Steps:
      1. Read uploaded file(s) directly from memory
      2. Run cleansing + enrichment
      3. Merge user rows with existing DB rows via `merge_db_with_user`
      4. Upsert into PartMaster
      5. Write snapshot Excel to output/stage1_master_snapshot.xlsx
      6. Return DataFrame for preview table

    Returns:
        pandas.DataFrame: final merged/cleaned dataset
    """

    if not uploaded_files:
        raise ValueError("No files uploaded. Please select a CSV/XLS/XLSX/PDF file.")

    dfs = []
    skipped_files = []

    # -------------------------------------------------
    # 1. Read all uploaded files directly from memory
    # -------------------------------------------------
    for f in uploaded_files:
        # Check file size for Render free tier limits (10MB)
        file_size = f.size if hasattr(f, 'size') else 0
        if file_size > 10 * 1024 * 1024:  # 10MB
            print(f"[WARNING] File {f.name} is too large for Render free tier: {file_size/1024/1024:.2f}MB")
            skipped_files.append(f"{f.name} (too large: {file_size/1024/1024:.2f}MB)")
            continue

        try:
            # ✅ Use the EXISTING load_file function - it's already memory-safe!
            df_file = load_file(f)
            
            if df_file is None:
                print(f"[Stage2] load_file returned None for {f.name}")
                skipped_files.append(f"{f.name} (failed to load)")
                continue
                
            if df_file.empty:
                print(f"[Stage2] File {f.name} produced no rows, skipping.")
                skipped_files.append(f"{f.name} (empty or no tables)")
                continue

            # Track provenance
            df_file["source_system"] = "user"
            df_file["source_file"] = f.name

            dfs.append(df_file)
            print(f"[SUCCESS] Loaded {len(df_file)} rows from {f.name}")

        except Exception as e:
            print(f"[Stage2] Failed to process {f.name}: {e}")
            skipped_files.append(f"{f.name} (error: {str(e)[:50]})")
            continue

    # Show summary of skipped files
    if skipped_files:
        print(f"[INFO] Skipped {len(skipped_files)} files: {skipped_files}")

    if not dfs:
        # Provide helpful error message
        error_msg = "No valid data found in uploaded files. "
        if skipped_files:
            error_msg += f"Skipped files: {', '.join(skipped_files)}. "
        error_msg += "Please upload CSV, XLS, XLSX files, or PDFs with tables."
        raise ValueError(error_msg)

    df_raw = pd.concat(dfs, ignore_index=True)
    print(f"[INFO] Combined data: {len(df_raw)} total rows")

    # -------------------------------------------------
    # 2. Clean + enrich
    # -------------------------------------------------
    try:
        df_clean = cleanup_pipeline(df_raw)
        df_clean = enrich_from_description(df_clean)
    except Exception as e:
        print(f"[ERROR] Cleanup/enrichment failed: {e}")
        raise ValueError(f"Data processing failed: {e}")

    if "part_number" not in df_clean.columns:
        # Try to find alternative column names
        possible_cols = [col for col in df_clean.columns if 'part' in col.lower() or 'number' in col.lower()]
        if possible_cols:
            df_clean = df_clean.rename(columns={possible_cols[0]: "part_number"})
            print(f"[INFO] Renamed column '{possible_cols[0]}' to 'part_number'")
        else:
            raise ValueError("Required column 'part_number' is missing after processing. Please ensure your files have a part number column.")

    # Drop rows with missing part_number
    df_clean = df_clean[df_clean["part_number"].notna()].copy()
    df_clean["part_number"] = df_clean["part_number"].astype(str).str.strip()
    
    if df_clean.empty:
        raise ValueError("No rows with valid part numbers found after cleaning.")

    records = df_clean.to_dict(orient="records")

    # -------------------------------------------------
    # 3. Group by part_number
    # -------------------------------------------------
    grouped = defaultdict(list)
    for r in records:
        pn = r.get("part_number")
        if pn:
            grouped[pn].append(r)

    print(f"[INFO] Grouped into {len(grouped)} unique part numbers")

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
    print(f"[INFO] Final output: {len(df_out)} rows with {len(df_out.columns)} columns")

    # -------------------------------------------------
    # 5. Write Excel snapshot for download + Stage-1
    #    This is OK because it's processed output, not uploaded files
    # -------------------------------------------------
    try:
        OUTPUT_DIR.mkdir(exist_ok=True)
        df_out.to_excel(SNAPSHOT_PATH, index=False)
        print(f"[SUCCESS] Snapshot saved to: {SNAPSHOT_PATH}")
    except Exception as e:
        print(f"[ERROR] Failed to save snapshot: {e}")
        # Don't crash, just continue without saving file
        # The output DataFrame is still returned for preview

    # -------------------------------------------------
    # 6. UPSERT into PartMaster
    # -------------------------------------------------
    try:
        with transaction.atomic():
            for row in df_out.to_dict(orient="records"):
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
        print(f"[SUCCESS] Updated {len(df_out)} records in PartMaster")
    except Exception as e:
        print(f"[ERROR] Database update failed: {e}")
        # Continue even if DB update fails, at least show the preview

    # -------------------------------------------------
    # 7. Return DataFrame for preview in the UI
    # -------------------------------------------------
    return df_out