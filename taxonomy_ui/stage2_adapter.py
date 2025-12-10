# taxonomy_ui/stage2_adapter.py

import io
import pandas as pd
from django.conf import settings

from ingestion_utils import load_file
from cleansing import cleanup_pipeline
from enrichment_text import enrich_from_description
from merge_logic import merge_db_with_user
from db import fetch_part_by_number, upsert_part_master


def run_stage2_from_django(uploaded_files):
    """
    Accepts a list of Django InMemoryUploadedFile objects,
    converts each to a DataFrame, runs full Stage-2 cleansing + merge,
    and returns (excel_bytes, filename).
    """
    dfs = []

    for f in uploaded_files:
        # Read file directly from Django's uploaded file (NO PATH NEEDED)
        df = load_file(f)     # <-- load_file must support file-like object
        if df is None or df.empty:
            continue

        df["source_system"] = "user"
        df["source_file"] = f.name
        dfs.append(df)

    if not dfs:
        raise ValueError("No valid data found in uploaded files.")

    # Merge all DataFrames like Stage-2 logic
    df_raw = pd.concat(dfs, ignore_index=True)

    df_clean = cleanup_pipeline(df_raw)
    df_clean = enrich_from_description(df_clean)

    df_clean = df_clean[df_clean["part_number"].notna()].copy()
    df_clean["part_number"] = df_clean["part_number"].astype(str).str.strip()

    records = df_clean.to_dict(orient="records")

    merged_results = []
    from collections import defaultdict
    groups = defaultdict(list)

    for r in records:
        pn = r.get("part_number")
        if pn:
            groups[pn].append(r)

    for pn, user_rows in groups.items():
        db_row = fetch_part_by_number(pn)
        merged = merge_db_with_user(db_row, user_rows)
        merged_results.append(merged)

    # Upsert into database
    upsert_part_master(merged_results)

    # Create Excel output
    output_buffer = io.BytesIO()
    df_out = pd.DataFrame(merged_results)

    with pd.ExcelWriter(output_buffer, engine="xlsxwriter") as writer:
        df_out.to_excel(writer, index=False, sheet_name="Parts")

    output_buffer.seek(0)

    return output_buffer.getvalue(), "user_output.xlsx"
