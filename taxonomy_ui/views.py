# taxonomy_ui/views.py

import os
import io
import sys
import subprocess

import pandas as pd
from django.conf import settings
from django.http import HttpResponse, JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt

from .models import PartMaster
from taxonomy_ui.stage2_adapter import run_stage2_from_django


# ----------------------------------------------------------
# CONFIG
# ----------------------------------------------------------

STAGE1_SCRIPT = os.path.join(settings.BASE_DIR, "background_stage1.py")

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
    "pdf_page", "date", "due_date", "file_name", "sources",
    "lifecycle_state", "vendor_name", "cad_file",
    "source_system", "source_file",
]


# ----------------------------------------------------------
# HOME
# ----------------------------------------------------------
from django.shortcuts import redirect
def home(request):
    return redirect("taxonomy_ui:upload_and_process")



# ----------------------------------------------------------
# PART MASTER VIEW
# ----------------------------------------------------------

def part_list(request):
    parts = PartMaster.objects.all().order_by("id")

    columns = [
        "id", "part_number", "updated_at", "dimensions", "description",
        "cost", "material", "vendor_name", "currency",
        "category_raw", "category_master",
        "source_system", "source_file",
    ]

    rows = [
        {col: getattr(p, col, "") for col in columns}
        for p in parts
    ]

    return render(
        request,
        "taxonomy_ui/parts_list.html",
        {"columns": columns, "rows": rows},
    )


# ----------------------------------------------------------
# STAGE-2 UPLOAD + PROCESS
# ----------------------------------------------------------

def upload_and_process(request):
    context = {
        "has_df": False,
        "all_columns": COLUMN_CHOICES,   # default for initial GET
    }

    if request.method == "GET":
        return render(request, "taxonomy_ui/upload.html", context)

    uploaded_files = request.FILES.getlist("files")
    if not uploaded_files:
        context["error"] = "No files were submitted!"
        return render(request, "taxonomy_ui/upload.html", context)

    try:
        # run Stage-2 (now dynamic DB version)
        output_bytes, filename = run_stage2_from_django(uploaded_files)

        # save output file
        output_dir = os.path.join(settings.MEDIA_ROOT, "output")
        os.makedirs(output_dir, exist_ok=True)

        output_path = os.path.join(output_dir, filename)
        with open(output_path, "wb") as f:
            f.write(output_bytes)

        # load preview from bytes (Excel)
        df = pd.read_excel(io.BytesIO(output_bytes))

        context.update({
            "has_df": not df.empty,
            "download_link": f"/download-full/{filename}/",
            "output_filename": filename,
            "preview_columns": list(df.columns),
            "preview_rows": df.head(50).values.tolist(),
            # 👇 IMPORTANT: override COLUMN_CHOICES with real columns
            "all_columns": list(df.columns),
        })

    except Exception as e:
        context["error"] = str(e)

    return render(request, "taxonomy_ui/upload.html", context)



# ----------------------------------------------------------
# DOWNLOAD FULL OUTPUT
# ----------------------------------------------------------

def download_full_output(request, filename):
    output_path = os.path.join(settings.MEDIA_ROOT, "output", filename)

    if not os.path.exists(output_path):
        return HttpResponse("File not found", status=404)

    with open(output_path, "rb") as f:
        data = f.read()

    response = HttpResponse(
        data,
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    response["Content-Disposition"] = f'attachment; filename="{filename}"'
    return response


# ----------------------------------------------------------
# DOWNLOAD SELECTED COLUMNS
# ----------------------------------------------------------

def download_selected_columns(request):
    if request.method != "POST":
        return HttpResponse("Invalid method", status=405)

    output_filename = request.POST.get("output_filename")
    if not output_filename:
        return HttpResponse("Missing output file", status=400)

    output_path = os.path.join(settings.MEDIA_ROOT, "output", output_filename)
    if not os.path.exists(output_path):
        return HttpResponse("File not found", status=404)

    df = pd.read_excel(output_path)
    selected_columns = request.POST.getlist("selected_columns")

    if selected_columns:
        df = df[[c for c in selected_columns if c in df.columns]]

    buffer = io.BytesIO()
    df.to_excel(buffer, index=False)
    buffer.seek(0)

    response = HttpResponse(
        buffer.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    response["Content-Disposition"] = f'attachment; filename="selected_{output_filename}"'
    return response


# ----------------------------------------------------------
# STAGE-1 REFRESH (BACKGROUND)
# ----------------------------------------------------------

@csrf_exempt
def run_stage1_refresh(request):
    if request.method != "POST":
        return JsonResponse(
            {"status": "error", "message": "POST required"},
            status=405,
        )

    try:
        subprocess.Popen([sys.executable, STAGE1_SCRIPT])
        return JsonResponse(
            {"status": "ok", "message": "Stage 1 started in background"}
        )
    except Exception as e:
        return JsonResponse(
            {"status": "error", "message": str(e)},
            status=500,
        )
