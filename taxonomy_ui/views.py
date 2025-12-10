# taxonomy_ui/views.py

import os
import io
import sys
import subprocess
from collections import defaultdict

import pandas as pd
from django.conf import settings
from django.http import HttpResponse, JsonResponse
from django.shortcuts import render

from .models import PartMaster
from taxonomy_ui.stage2_adapter import run_stage2_from_django

# Path to background_stage1.py
STAGE1_SCRIPT = os.path.join(settings.BASE_DIR, "background_stage1.py")

# Column list for checkbox UI (Stage 2)
COLUMN_CHOICES = [
    "part_number", "updated_at", "stock_qty", "vendor_code", "abc_class",
    "commodity_code", "utilization_score", "material_group", "risk_rating",
    "cost", "purchase_uom", "notes", "description_clean", "drawing_no",
    "is_standard_part", "order_uom", "spec_grade", "spec_finish", "material",
    "dimensions", "last_modified", "description", "category_master",
    "analysis_comment", "created_date", "plant", "currency", "flag",
    "checkout_status", "remarks", "approval_status", "revision_no",
    "material_type", "avg_lead_time_days", "spec_weight", "no", "cad_type",
    "storage_location", "quantity", "criticality_index", "category_raw",
    "engineer_name", "active_flag", "file_size_mb", "valuation_type",
    "spec_tolerance", "movement_frequency", "order_date", "delivery_date",
    "pdf_page", "date", "due_date", "file_name", "sources", "lifecycle_state",
    "vendor_name", "cad_file", "source_system", "source_file",
]


# ----------------------------------------------------------
# Home
# ----------------------------------------------------------
def home(request):
    return render(request, "taxonomy_ui/home.html")


# ----------------------------------------------------------
# Stage 1 DB parts view  (NO pandas, pure ORM)
# ----------------------------------------------------------
def part_list(request):
    """
    Display Part Master data from DB using Django ORM.
    """

    parts = PartMaster.objects.all()

    columns = [
        "id",
        "part_number",
        "updated_at",
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

    rows = []
    for p in parts:
        rows.append(
            {
                "id": p.id,
                "part_number": p.part_number,
                "updated_at": p.updated_at,
                "dimensions": p.dimensions,
                "description": p.description,
                "cost": p.cost,
                "material": p.material,
                "vendor_name": p.vendor_name,
                "currency": p.currency,
                "category_raw": p.category_raw,
                "category_master": p.category_master,
                "source_system": p.source_system,
                "source_file": p.source_file,
            }
        )

    return render(
        request,
        "taxonomy_ui/parts_list.html",
        {"columns": columns, "rows": rows},
    )


# ----------------------------------------------------------
# UPLOAD + PROCESS (Stage 2)
# ----------------------------------------------------------
def upload_and_process(request):
    """
    Upload user file(s), run Stage 2, show preview, and enable downloads.
    """

    df = None
    has_df = False
    download_link = None
    output_filename = None
    all_columns = COLUMN_CHOICES
    error = None

    if request.method == "GET":
        return render(
            request,
            "taxonomy_ui/upload.html",
            {
                "df": df,
                "has_df": has_df,
                "download_link": download_link,
                "output_filename": output_filename,
                "all_columns": all_columns,
                "error": error,
            },
        )

    # POST
    uploaded_files = request.FILES.getlist("files")

    if not uploaded_files:
        error = "No files were submitted!"
        return render(
            request,
            "taxonomy_ui/upload.html",
            {
                "df": df,
                "has_df": has_df,
                "download_link": download_link,
                "output_filename": output_filename,
                "all_columns": all_columns,
                "error": error,
            },
        )

    try:
        # Run Stage 2
        output_bytes, filename = run_stage2_from_django(uploaded_files)

        # Save full output to MEDIA_ROOT/output/
        output_dir = os.path.join(settings.MEDIA_ROOT, "output")
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, filename)

        with open(output_path, "wb") as f:
            f.write(output_bytes)

        download_link = f"/download-full/{filename}/"
        output_filename = filename

        # Preview: load into DataFrame
        df = pd.read_excel(io.BytesIO(output_bytes))

        if "sources" in df.columns:
            df["sources"] = df["sources"].astype(str).str.replace(",", ",\n")

        has_df = not df.empty

    except Exception as e:
        error = str(e)
        df = None
        has_df = False

    return render(
        request,
        "taxonomy_ui/upload.html",
        {
            "df": df,
            "has_df": has_df,
            "download_link": download_link,
            "output_filename": output_filename,
            "all_columns": all_columns,
            "error": error,
        },
    )


# ----------------------------------------------------------
# FULL OUTPUT DOWNLOAD
# ----------------------------------------------------------
def download_full_output(request, filename):
    output_path = os.path.join(settings.MEDIA_ROOT, "output", filename)

    if not os.path.exists(output_path):
        return HttpResponse("File not found.", status=404)

    with open(output_path, "rb") as f:
        data = f.read()

    response = HttpResponse(
        data,
        content_type=(
            "application/"
            "vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ),
    )
    response["Content-Disposition"] = f'attachment; filename="{filename}"'
    return response


# ----------------------------------------------------------
# SELECTED COLUMNS DOWNLOAD
# ----------------------------------------------------------
def download_selected_columns(request):
    if request.method != "POST":
        return HttpResponse("Invalid method", status=405)

    output_filename = request.POST.get("output_filename")
    selected_columns = request.POST.getlist("selected_columns")

    if not output_filename:
        return HttpResponse("Missing output file reference.", status=400)

    output_path = os.path.join(settings.MEDIA_ROOT, "output", output_filename)

    if not os.path.exists(output_path):
        return HttpResponse("Output file not found.", status=404)

    df = pd.read_excel(output_path)

    if selected_columns:
        df = df[[c for c in selected_columns if c in df.columns]]

    buffer = io.BytesIO()
    df.to_excel(buffer, index=False)
    buffer.seek(0)

    response = HttpResponse(
        buffer.getvalue(),
        content_type=(
            "application/"
            "vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ),
    )
    response["Content-Disposition"] = (
        f'attachment; filename="selected_{output_filename}"'
    )
    return response


# ----------------------------------------------------------
# REFRESH STAGE 1 (BACKGROUND)
# ----------------------------------------------------------
def run_stage1_refresh(request):
    if request.method != "POST":
        return JsonResponse(
            {"status": "error", "message": "Invalid method"}, status=405
        )

    try:
        subprocess.Popen([sys.executable, STAGE1_SCRIPT])
        return JsonResponse({"status": "ok", "message": "Stage 1 started"})
    except Exception as e:
        return JsonResponse(
            {"status": "error", "message": str(e)}, status=500
        )
