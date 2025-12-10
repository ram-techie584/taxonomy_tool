# background_stage1.py
"""
Stage-1 (ORM-based, production safe)

✅ No Excel
✅ No filesystem dependency
✅ No psycopg2
✅ DB is the master
"""

import os
import pandas as pd
from collections import defaultdict

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "taxonomy_portal.settings")

import django
django.setup()

from django.db import transaction
from taxonomy_ui.models import PartMaster

from ingestion_utils import load_file
from cleansing import cleanup_pipeline
from enrichment_text import enrich_from_description


def run_stage1_from_sources(source_files):
    """
    source_files = list of Excel / CSV / PDF / API dataframes
    """

    dfs = []

    for f in source_files:
        df = load_file(f)
        if df is None or df.empty:
            continue

        df["source_system"] = "stage1"
        df["source_file"] = getattr(f, "name", "system")
        dfs.append(df)

    if not dfs:
        print("[Stage1] ❌ No valid source data")
        return

    # Combine all sources
    df_raw = pd.concat(dfs, ignore_index=True)

    # Clean & enrich
    df_clean = cleanup_pipeline(df_raw)
    df_clean = enrich_from_description(df_clean)

    df_clean = df_clean[df_clean["part_number"].notna()].copy()
    df_clean["part_number"] = df_clean["part_number"].astype(str).str.strip()

    records = df_clean.to_dict(orient="records")

    grouped = defaultdict(list)
    for r in records:
        grouped[r["part_number"]].append(r)

    with transaction.atomic():
        for pn, rows in grouped.items():
            row = rows[-1]  # latest wins

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

    print(f"[Stage1] ✅ Loaded {len(grouped)} records into DB")


if __name__ == "__main__":
    print("[Stage1] ✅ ORM-based Stage-1 ready")
