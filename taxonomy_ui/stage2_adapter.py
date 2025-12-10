# taxonomy_ui/stage2_adapter.py

import io
from collections import defaultdict

import pandas as pd
from django.db import transaction

from ingestion_utils import load_file
from cleansing import cleanup_pipeline
from enrichment_text import enrich_from_description
from merge_logic import merge_db_with_user
from taxonomy_ui.models import PartMaster


def run_stage2_from_django(uploaded_files):
    """
    Stage-2 pipeline (Render safe)

    ✅ No psycopg2
    ✅ No localhost
    ✅ Django ORM only
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
    # 4. Merge USER data with DB (ORM)
    # -------------------------------------------------
    for pn, user_rows in grouped.items():
        db_obj = PartMaster.objects.filter(part_number=pn).first()

        db_row = None
        if db_obj:
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

    # -------------------------------------------------
    # 5. UPSERT using ORM (atomic)
    # -------------------------------------------------
    with transaction.atomic():
        for row in merged_results:
            PartMaster.objects.update_or_create(
                part_number=row.get("part_number"),
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

    # -------------------------------------------------
    # 6. Generate Excel output
    # -------------------------------------------------
    output_buffer = io.BytesIO()
    df_out = pd.DataFrame(merged_results)

    with pd.ExcelWriter(output_buffer, engine="xlsxwriter") as writer:
        df_out.to_excel(writer, index=False, sheet_name="Parts")

    output_buffer.seek(0)

    return output_buffer.getvalue(), "user_output.xlsx"
