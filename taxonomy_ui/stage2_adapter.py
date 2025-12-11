# taxonomy_ui/stage2_adapter.py

import io
from collections import defaultdict

import pandas as pd

from ingestion_utils import load_file
from cleansing import cleanup_pipeline
from enrichment_text import enrich_from_description
from merge_logic import merge_db_with_user

# 👇 use dynamic-column DB helpers
from db import fetch_part_by_number, upsert_part_master


def run_stage2_from_django(uploaded_files):
    """
    Stage-2 pipeline (Render safe, dynamic columns)

    ✅ Uses Django-managed connection via db.py
    ✅ No localhost/DB_CONFIG
    ✅ Dynamic columns in part_master
    """

    dfs = []

    # -------------------------------------------------
    # 1. Read uploaded files
    # -------------------------------------------------
    for f in uploaded_files:
        df = load_file(f)  # must support file-like object
        if df is None or df.empty:
            continue

        df["source_system"] = "user"
        df["source_file"] = f.name
        dfs.append(df)

    if not dfs:
        raise ValueError("No valid data found in uploaded files.")

    # -------------------------------------------------
    # 2. Clean + enrich
    # -------------------------------------------------
    df_raw = pd.concat(dfs, ignore_index=True)

    df_clean = cleanup_pipeline(df_raw)
    df_clean = enrich_from_description(df_clean)

    # keep only rows with part_number
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
    # 4. Merge USER data with DB using db.py
    # -------------------------------------------------
    for pn, user_rows in grouped.items():
        # dynamic row from DB (may contain more columns than ORM model)
        db_row = fetch_part_by_number(pn)
        merged = merge_db_with_user(db_row, user_rows)
        merged_results.append(merged)

    # -------------------------------------------------
    # 5. UPSERT into DB with dynamic columns
    # -------------------------------------------------
    upsert_part_master(merged_results)

    # -------------------------------------------------
    # 6. Generate Excel output (all columns from merged_results)
    # -------------------------------------------------
    output_buffer = io.BytesIO()
    df_out = pd.DataFrame(merged_results)

    with pd.ExcelWriter(output_buffer, engine="xlsxwriter") as writer:
        df_out.to_excel(writer, index=False, sheet_name="Parts")

    output_buffer.seek(0)
    return output_buffer.getvalue(), "user_output.xlsx"
