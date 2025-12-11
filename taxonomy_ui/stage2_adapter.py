# taxonomy_ui/stage2_adapter.py

import io
from collections import defaultdict

import pandas as pd

from ingestion_utils import load_file
from cleansing import cleanup_pipeline
from enrichment_text import enrich_from_description
from merge_logic import merge_db_with_user

from db import fetch_part_by_number, upsert_part_master


def run_stage2_from_django(uploaded_files):
    """
    Stage-2 pipeline (Render safe, dynamic columns)

    ✅ Safe part_number handling
    ✅ Guaranteed DB commit
    ✅ Dynamic column support
    """

    dfs = []

    # -------------------------------------------------
    # 1. Read uploaded files
    # -------------------------------------------------
    for f in uploaded_files:
        df = load_file(f)
        if df is None or df.empty:
            continue

        df["source_system"] = "user"
        df["source_file"] = getattr(f, "name", "uploaded")
        dfs.append(df)

    if not dfs:
        raise ValueError("No valid data found in uploaded files.")

    # -------------------------------------------------
    # 2. Clean + enrich
    # -------------------------------------------------
    df_raw = pd.concat(dfs, ignore_index=True)

    df_clean = cleanup_pipeline(df_raw)
    df_clean = enrich_from_description(df_clean)

    # -------------------------------------------------
    # 3. Validate part_number column
    # -------------------------------------------------
    if "part_number" not in df_clean.columns:
        raise ValueError("part_number column missing after cleanup")

    df_clean["part_number"] = (
        df_clean["part_number"]
        .astype(str)
        .str.strip()
    )

    df_clean = df_clean[
        (df_clean["part_number"] != "") &
        (df_clean["part_number"].str.lower() != "nan")
    ].copy()

    if df_clean.empty:
        raise ValueError("No valid part_number values after cleanup")

    records = df_clean.to_dict(orient="records")

    # -------------------------------------------------
    # 4. Group by part_number
    # -------------------------------------------------
    grouped = defaultdict(list)
    for r in records:
        pn = r.get("part_number")
        if pn:
            grouped[pn].append(r)

    if not grouped:
        raise ValueError("No grouped records found")

    merged_results = []

    # -------------------------------------------------
    # 5. Merge USER data with DB
    # -------------------------------------------------
    for pn, user_rows in grouped.items():
        db_row = fetch_part_by_number(pn)

        merged = merge_db_with_user(db_row, user_rows)

        # ✅ HARD GUARANTEE
        merged["part_number"] = pn

        merged_results.append(merged)

    if not merged_results:
        raise ValueError("Merge produced no output")

    # -------------------------------------------------
    # 6. UPSERT into DB
    # -------------------------------------------------
    upsert_part_master(merged_results)

    # -------------------------------------------------
    # 7. Generate Excel output
    # -------------------------------------------------
    output_buffer = io.BytesIO()
    df_out = pd.DataFrame(merged_results)

    with pd.ExcelWriter(output_buffer, engine="xlsxwriter") as writer:
        df_out.to_excel(writer, index=False, sheet_name="Parts")

    output_buffer.seek(0)
    return output_buffer.getvalue(), "user_output.xlsx"
