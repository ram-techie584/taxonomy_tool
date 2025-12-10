# taxonomy_ui/views.py

import os
import io
import sys
import subprocess

from django.conf import settings
from django.http import HttpResponse, JsonResponse
from django.shortcuts import render

from .models import PartMaster
from taxonomy_ui.stage2_adapter import run_stage2_from_django

# Path to background_stage1.py
STAGE1_SCRIPT = os.path.join(settings.BASE_DIR, "background_stage1.py")


# ----------------------------------------------------------
# HOME
# ----------------------------------------------------------
def home(request):
    return render(request, "taxonomy_ui/home.html")


# ----------------------------------------------------------
# PART MASTER LIST (✅ FIXED)
# ----------------------------------------------------------
def part_list(request):
    """
    Display ALL PartMaster columns dynamically using Django ORM
    """

    parts = PartMaster.objects.all()

    # ✅ Auto-fetch all model columns
    columns = [field.name for field in PartMaster._meta.fields]

    rows = []
    for p in parts:
        row = {}
        for col in columns:
            row[col] = getattr(p, col)
        rows.append(row)

    return render(
        request,
        "taxonomy_ui/parts_list.html",
        {
            "columns": columns,
            "rows": rows,
        },
    )


# ----------------------------------------------------------
# UPLOAD + PROCESS (STAGE 2)
# ----------------------------------------------------------
def upload_and_process(request):
    """
    Upload file(s), run Stage 2, show preview and enable download
    """

    df = None
    has_df = False
    download_link = None
    output_filename = None
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
                "error": error,
            },
        )

    uploaded_files = request.FILES.getlist("files")

    if not uploaded_files:
        error = "No files uploaded"
        return render(
            request,
            "taxonomy_ui/upload.html",
            {
                "df": df,
                "has_df": has_df,
                "download_link": download_link,
                "output_filename": output_filename,
                "error": error,
            },
        )

    try:
        output_bytes, filename = run_stage2_from_django(uploaded_files)

        output_dir = os.path.join(settings.MEDIA_ROOT, "output")
        os.makedirs(output_dir, exist_ok=True)

        output_path = os.path.join(output_dir, filename)
        with open(output_path, "wb") as f:
            f.write(output_bytes)

        download_link = f"/download-full/{filename}/"
        output_filename = filename
        has_df = True

    except Exception as e:
        error = str(e)

    return render(
        request,
        "taxonomy_ui/upload.html",
        {
            "df": df,
            "has_df": has_df,
            "download_link": download_link,
            "output_filename": output_filename,
            "error": error,
        },
    )


# ----------------------------------------------------------
# FULL OUTPUT DOWNLOAD
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
# STAGE 1 REFRESH (BACKGROUND)
# ----------------------------------------------------------
def run_stage1_refresh(request):
    if request.method != "POST":
        return JsonResponse({"status": "error"}, status=405)

    try:
        subprocess.Popen([sys.executable, STAGE1_SCRIPT])
        return JsonResponse({"status": "ok"})
    except Exception as e:
        return JsonResponse({"status": "error", "message": str(e)}, status=500)
