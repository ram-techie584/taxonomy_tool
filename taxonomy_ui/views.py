# taxonomy_ui/views.py

import os
import io
import sys
import subprocess
import pandas as pd

from django.conf import settings
from django.http import HttpResponse, JsonResponse
from django.shortcuts import render, redirect
from django.views.decorators.csrf import csrf_exempt
from django.db import connection

from .models import PartMaster
from taxonomy_ui.stage2_adapter import run_stage2_from_django


# ----------------------------------------------------------
# CONFIG
# ----------------------------------------------------------

STAGE1_SCRIPT = os.path.join(settings.BASE_DIR, "background_stage1.py")


# ----------------------------------------------------------
# HELPERS
# ----------------------------------------------------------

def get_part_master_columns():
    """
    ✅ SINGLE SOURCE OF TRUTH for UI checkboxes
    Reads actual DB schema (dynamic columns)
    """
    with connection.cursor() as cur:
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'part_master'
              AND column_name NOT IN ('id')
            ORDER BY column_name;
        """)
        return [r[0] for r in cur.fetchall()]


# ----------------------------------------------------------
# HOME
# ----------------------------------------------------------

def home(request):
    return redirect("taxonomy_ui:upload_and_process")


# ----------------------------------------------------------
# PART MASTER VIEW
# ----------------------------------------------------------

def part_list(request):
    qs = PartMaster.objects.all().values()
    df = pd.DataFrame(list(qs))

    if df.empty:
        return render(
            request,
            "taxonomy_ui/parts_list.html",
            {"columns": [], "rows": []},
        )

    return render(
        request,
        "taxonomy_ui/parts_list.html",
        {
            "columns": df.columns.tolist(),
            "rows": df.to_dict(orient="records"),
        },
    )


# ----------------------------------------------------------
# STAGE-2 UPLOAD + PROCESS
# ----------------------------------------------------------

def upload_and_process(request):
    if request.method == "GET":
        return render(
            request,
            "taxonomy_ui/upload.html",
            {
                "has_df": False,
                "all_columns": get_part_master_columns(),  # ✅ DB driven
            },
        )

    uploaded_files = request.FILES.getlist("files")
    if not uploaded_files:
        return render(
            request,
            "taxonomy_ui/upload.html",
            {
                "error": "No files were submitted!",
                "has_df": False,
                "all_columns": get_part_master_columns(),
            },
        )

    try:
        # ✅ Stage-2 updates DB
        output_bytes, filename = run_stage2_from_django(uploaded_files)

        # ✅ preview from DB, NOT uploaded file
        qs = PartMaster.objects.all().values()
        df = pd.DataFrame(list(qs))

        return render(
            request,
            "taxonomy_ui/upload.html",
            {
                "has_df": not df.empty,
                "preview_columns": df.columns.tolist(),
                "preview_rows": df.head(50).values.tolist(),
                "all_columns": df.columns.tolist(),  # ✅ checkbox fix
            },
        )

    except Exception as e:
        return render(
            request,
            "taxonomy_ui/upload.html",
            {
                "error": str(e),
                "has_df": False,
                "all_columns": get_part_master_columns(),
            },
        )


# ----------------------------------------------------------
# DOWNLOAD FULL OUTPUT (DB → EXCEL)
# ----------------------------------------------------------

def download_full_output(request):
    qs = PartMaster.objects.all().values()
    df = pd.DataFrame(list(qs))

    response = HttpResponse(
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    response["Content-Disposition"] = "attachment; filename=full_output.xlsx"

    df.to_excel(response, index=False)
    return response


# ----------------------------------------------------------
# DOWNLOAD SELECTED COLUMNS (FIXED ✅)
# ----------------------------------------------------------

def download_selected_columns(request):
    if request.method != "POST":
        return HttpResponse("Invalid request", status=400)

    selected_cols = request.POST.getlist("columns")
    if not selected_cols:
        return HttpResponse("No columns selected", status=400)

    qs = PartMaster.objects.all().values(*selected_cols)
    df = pd.DataFrame(list(qs))

    if df.empty:
        return HttpResponse("No data", status=400)

    response = HttpResponse(
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    response["Content-Disposition"] = "attachment; filename=selected_output.xlsx"

    df.to_excel(response, index=False)
    return response


# ----------------------------------------------------------
# STAGE-1 REFRESH (BACKGROUND)
# ----------------------------------------------------------

@csrf_exempt
def run_stage1_refresh(request):
    if request.method != "POST":
        return JsonResponse({"error": "POST required"}, status=405)

    subprocess.Popen([sys.executable, STAGE1_SCRIPT])
    return JsonResponse({"status": "ok", "message": "Stage-1 started"})
