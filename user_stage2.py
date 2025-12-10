# user_stage2.py
"""
Stage 2 (FINAL FIXED VERSION - TOP NOTCH):
- Load user PDFs / Excels
- Clean like Stage 1
- Fetch FULL DB row for each part_number
- Merge: USER values override DB values
- Preserve ALL DB columns
- Append user source in "sources"
- Upsert merged rows
- Export full Excel with ALL columns
"""

import os
import json
import pandas as pd
from collections import defaultdict

from config import USER_UPLOAD_DIR, OUTPUT_DIR
from ingestion_utils import load_file
from cleansing import cleanup_pipeline
from enrichment_text import enrich_from_description
from db import fetch_part_by_number, upsert_part_master


def load_user_files():
    dfs = []
    if not os.path.isdir(USER_UPLOAD_DIR):
        return pd.DataFrame()

    for fname in os.listdir(USER_UPLOAD_DIR):
        path = os.path.join(USER_UPLOAD_DIR, fname)
        print(f"🔹 User file: {path}")
        df = load_file(path)
        if df is not None and not df.empty:
            df["source_system"] = "user"
            df["source_file"] = fname
            dfs.append(df)

    if not dfs:
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=True)


def clean_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    df = cleanup_pipeline(df)
    df = enrich_from_description(df)
    return df


def merge_db_and_user(db_row: dict, user_rows: list) -> dict:
    """
    FINAL FIXED MERGE:
    - start with FULL DB ROW
    - apply USER values on top
    - keep all DB fields ALWAYS
    """
    if db_row is None:
        # new part: combine all user rows
        base = {}
    else:
        base = db_row.copy()

    # merge ALL user rows (last user row wins)
    for u in user_rows:
        for k, v in u.items():
            if v not in [None, "", "nan", "NaN"]:
                base[k] = v

    # Ensure sources is a list
    old_sources = []
    if "sources" in base and base["sources"]:
        try:
            old_sources = json.loads(base["sources"])
            if not isinstance(old_sources, list):
                old_sources = []
        except:
            old_sources = []

    # append user source
    for u in user_rows:
        old_sources.append({
            "source": "user",
            "file": u.get("source_file"),
            "description": u.get("description"),
        })

    base["sources"] = json.dumps(old_sources, ensure_ascii=False)
    return base


def autofit_excel(path: str, df: pd.DataFrame):
    with pd.ExcelWriter(path, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Sheet1")
        ws = writer.sheets["Sheet1"]

        for i, col in enumerate(df.columns):
            series = df[col].astype(str)
            width = max(series.map(len).max(), len(col)) + 2
            ws.set_column(i, i, min(width, 60))


def run_stage2():
    print("📥 Stage 2: Processing user uploads...\n")

    df_raw = load_user_files()
    if df_raw.empty:
        print("⚠️ No user files found.")
        return

    print(f"📊 Raw user rows: {len(df_raw)}")

    df_clean = clean_pipeline(df_raw)
    df_clean = df_clean[df_clean["part_number"].notna()].copy()

    print(f"📊 Cleaned user rows with part_number: {len(df_clean)}\n")

    user_records = df_clean.to_dict(orient="records")

    # Group by part_number
    groups = defaultdict(list)
    for row in user_records:
        pn = str(row.get("part_number")).strip()
        if pn:
            groups[pn].append(row)

    merged_results = []

    for pn, user_rows in groups.items():
        print(f"   🔄 Merging user updates for part_number={pn}")
        db_row = fetch_part_by_number(pn)
        merged = merge_db_and_user(db_row, user_rows)
        merged_results.append(merged)

    print(f"\n💾 Upserting {len(merged_results)} rows into DB...")
    upsert_part_master(merged_results)
    print("✅ DB updated.")

    # Output Excel (sort by part_number)
    df_out = pd.DataFrame(merged_results)
    df_out = df_out.sort_values("part_number").reset_index(drop=True)
    df_out = df_out.sort_values(
        by="part_number",
        key=lambda x: x.str.extract(r'(\d+)', expand=False).fillna(0).astype(int)
    )

    out_path = os.path.join(OUTPUT_DIR, "user_stage2_output.xlsx")
    autofit_excel(out_path, df_out)

    print(f"📂 User output saved to: {out_path}")
    print("✅ Stage 2 complete.\n")


if __name__ == "__main__":
    run_stage2()
