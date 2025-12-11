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
# DB HELPERS
# ----------------------------------------------------------

def get_part_master_columns():
    """
    ✅ SINGLE SOURCE OF TRUTH FOR UI COLUMNS
    Always read schema from DB
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
    parts = PartMaster.objects.all().order_by("id")

    columns = get_part_master_columns()

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
    """
    ✅ Upload → Clean → Enrich → Merge with DB
    ✅ Preview from output
    ✅ Column selector from DB (NOT upload)
    """

    context = {
        "has_df": False,
        "all_columns": get_part_master_columns(),
    }

    if request.method == "GET":
        return render(request, "taxonomy_ui/upload.html", context)

    uploaded_files = request.FILES.getlist("files")
    if not uploaded_files:
        context["error"] = "No files were submitted!"
        return render(request, "taxonomy_ui/upload.html", context)

    try:
        # --------------------------------------------------
        # Run Stage-2
        # --------------------------------------------------
        output_bytes, filename = run_stage2_from_django(uploaded_files)

        # --------------------------------------------------
        # Save output (optional, for user download)
        # --------------------------------------------------
        output_dir = os.path.join(settings.MEDIA_ROOT, "output")
        os.makedirs(output_dir, exist_ok=True)

        output_path = os.path.join(output_dir, filename)
        with open(output_path, "wb") as f:
            f.write(output_bytes)

        # --------------------------------------------------
        # Preview (ONLY for UI)
        # --------------------------------------------------
        df = pd.read_excel(io.BytesIO(output_bytes))

        context.update({
            "has_df": not df.empty,
            "download_link": "/download-full/",
            "output_filename": filename,

            # ✅ Preview
            "df": df,
            "preview_columns": list(df.columns),
            "preview_rows": df.head(50).values.tolist(),

            # ✅ DB schema for column selector
            "all_columns": get_part_master_columns(),
        })

    except Exception as e:
        context["error"] = str(e)

    return render(request, "taxonomy_ui/upload.html", context)


# ----------------------------------------------------------
# DOWNLOAD FULL OUTPUT (FROM DB)
# ----------------------------------------------------------

def download_full_output(request):
    qs = PartMaster.objects.all().values()
    df = pd.DataFrame(list(qs))

    if df.empty:
        return HttpResponse("No data in database", status=400)

    response = HttpResponse(
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    response["Content-Disposition"] = "attachment; filename=full_output.xlsx"

    df.to_excel(response, index=False)
    return response


# ----------------------------------------------------------
# DOWNLOAD SELECTED COLUMNS (FROM DB)
# ----------------------------------------------------------

def download_selected_columns(request):
    if request.method != "POST":
        return HttpResponse("Invalid request", status=400)

    # ✅ FIXED: matches upload.html name
    selected_cols = request.POST.getlist("selected_columns")

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
