# background_stage1.py
"""
Stage 1: Background ingestion of SAP / Vault / PowerBI / PO / Invoice
→ Cleansing → Merging by part_number → Upsert into ONE flat table part_master.
"""

import os
from collections import defaultdict

import pandas as pd

from config import SOURCES_DIRS, OUTPUT_DIR
from ingestion_utils import load_file
from cleansing import cleanup_pipeline
from enrichment_text import enrich_from_description
#from merge_logic import merge_records_for_part
from db import init_db, upsert_part_master


def load_all_sources() -> pd.DataFrame:
    all_rows = []
    for system, folder in SOURCES_DIRS.items():
        if not os.path.isdir(folder):
            continue
        for fname in os.listdir(folder):
            path = os.path.join(folder, fname)
            print(f"📄 [{system}] Processing: {path}")
            df = load_file(path)
            if df is None or df.empty:
                continue
            df["source_system"] = system
            df["source_file"] = fname
            all_rows.append(df)
    if not all_rows:
        return pd.DataFrame()
    return pd.concat(all_rows, ignore_index=True)


def clean_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    df = cleanup_pipeline(df)
    df = enrich_from_description(df)
    return df


from merge_logic import merge_records_by_part_number

def run_stage1():
    print("🚀 Stage 1: Background Ingestion Started\n")

    init_db()

    df_raw = load_all_sources()
    print(f"📊 Raw rows loaded: {len(df_raw)}")

    df_clean = clean_pipeline(df_raw)

    # Convert to records
    records = df_clean.to_dict(orient="records")

    # Group by part number
    grouped = {}
    for r in records:
        pn = r.get("part_number")
        if not pn:
            continue
        grouped.setdefault(pn, []).append(r)

    # Merge rows for each part
    merged_records = []
    for pn, rows in grouped.items():
        merged = merge_records_by_part_number(rows)
        merged_records.append(merged)

    print(f"📊 Unique merged part_numbers: {len(merged_records)}")

    # Upsert
    upsert_part_master(merged_records)


    # Save snapshot for inspection
    snapshot_path = os.path.join(OUTPUT_DIR, "stage1_master_snapshot.xlsx")
    pd.DataFrame(merged_records).to_excel(snapshot_path, index=False)
    print(f"📂 Snapshot saved to: {snapshot_path}")
    print("✅ Stage 1 complete.")


if __name__ == "__main__":
    run_stage1()
