# background_stage1.py
"""
Stage 1 loader for Render + local.

Reads output/stage1_master_snapshot.xlsx and populates the PartMaster table
using Django ORM (Postgres on Render, SQLite locally).
"""

import os
from pathlib import Path

import pandas as pd

# --------------------------------------------------------------------
# 1. Bootstrap Django
# --------------------------------------------------------------------
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "taxonomy_portal.settings")

import django  # noqa: E402
django.setup()

from django.db import transaction  # noqa: E402
from taxonomy_ui.models import PartMaster  # noqa: E402


# --------------------------------------------------------------------
# 2. Paths
# --------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
SNAPSHOT_PATH = BASE_DIR / "output" / "stage1_master_snapshot.xlsx"


# --------------------------------------------------------------------
# 3. Helpers
# --------------------------------------------------------------------
def safe_val(val):
    """Convert pandas NaN to None (Postgres safe)."""
    if pd.isna(val):
        return None
    return val


# --------------------------------------------------------------------
# 4. Main Loader
# --------------------------------------------------------------------
def load_part_master_from_snapshot():
    if not SNAPSHOT_PATH.exists():
        print(f"[Stage1] ❌ Snapshot file not found: {SNAPSHOT_PATH}")
        return

    print(f"[Stage1] ✅ Loading snapshot from: {SNAPSHOT_PATH}")

    df = pd.read_excel(SNAPSHOT_PATH)

    if df.empty:
        print("[Stage1] ⚠️ Snapshot is empty. No rows to load.")
        return

    print(f"[Stage1] Rows in snapshot: {len(df)}")

    required_cols = [
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

    for col in required_cols:
        if col not in df.columns:
            df[col] = None

    objects = []

    for _, row in df.iterrows():
        objects.append(
            PartMaster(
                part_number=str(safe_val(row.get("part_number")) or "").strip(),
                dimensions=safe_val(row.get("dimensions")),
                description=safe_val(row.get("description")),
                cost=str(safe_val(row.get("cost"))) if safe_val(row.get("cost")) else None,
                material=safe_val(row.get("material")),
                vendor_name=safe_val(row.get("vendor_name")),
                currency=safe_val(row.get("currency")),
                category_raw=safe_val(row.get("category_raw")),
                category_master=safe_val(row.get("category_master")),
                source_system=safe_val(row.get("source_system")),
                source_file=safe_val(row.get("source_file")),
            )
        )

    with transaction.atomic():
        deleted_count, _ = PartMaster.objects.all().delete()
        print(f"[Stage1] 🧹 Deleted {deleted_count} existing rows")

        PartMaster.objects.bulk_create(objects, batch_size=500)
        print(f"[Stage1] ✅ Inserted {len(objects)} rows into part_master")

    print("[Stage1] 🎉 Stage-1 completed successfully")


# --------------------------------------------------------------------
# 5. Entry Point
# --------------------------------------------------------------------
if __name__ == "__main__":
    load_part_master_from_snapshot()
