# taxonomy_ui/views.py

from pathlib import Path

import pandas as pd

from django.http import HttpResponse, JsonResponse
from django.shortcuts import render
from django.urls import reverse
from django.views.decorators.http import require_POST

from taxonomy_ui.models import PartMaster
from taxonomy_ui.stage2_adapter import run_stage2_from_django
from background_stage1 import load_part_master_from_snapshot


BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = BASE_DIR / "output"
SNAPSHOT_FILENAME = "stage1_master_snapshot.xlsx"
SNAPSHOT_PATH = OUTPUT_DIR / SNAPSHOT_FILENAME


# --------------------------------------------------------------------
# HOME VIEW (root "/") → reuse upload page
# --------------------------------------------------------------------
def home(request):
    # Reuse the same logic as upload_and_process for GET/POST
    return upload_and_process(request)


# --------------------------------------------------------------------
# UPLOAD + PROCESS
# --------------------------------------------------------------------
def upload_and_process(request):
    """
    Handles:
      - file upload
      - Stage-2 pipeline
      - context for upload.html template:
          has_df, df, all_columns, download_link, output_filename, error
    """

    context = {
        "has_df": False,
        "df": None,
        "all_columns": [],
        "download_link": None,
        "output_filename": SNAPSHOT_FILENAME,
    }

    if request.method == "POST":
        try:
            # Input name in template is "files"
            uploaded_files = request.FILES.getlist("files")
            if not uploaded_files:
                raise ValueError("Please select at least one file to upload.")

            df_out = run_stage2_from_django(uploaded_files)

            # Build download link for "Download All" button
            download_url = reverse(
                "taxonomy_ui:download_full_output",
                args=[SNAPSHOT_FILENAME],
            )

            context.update(
                {
                    "has_df": True,
                    "df": df_out,
                    "all_columns": list(df_out.columns),
                    "download_link": download_url,
                    "output_filename": SNAPSHOT_FILENAME,
                }
            )

        except Exception as e:
            context["error"] = str(e)

    return render(request, "taxonomy_ui/upload.html", context)


# --------------------------------------------------------------------
# VIEW DB PARTS
# --------------------------------------------------------------------
def part_list(request):
    """
    Shows all rows from PartMaster.
    You should have a template: taxonomy_ui/part_list.html
    """
    parts = PartMaster.objects.all().order_by("id")
    return render(request, "taxonomy_ui/part_list.html", {"parts": parts})


# --------------------------------------------------------------------
# DOWNLOAD FULL SNAPSHOT
# --------------------------------------------------------------------
def download_full_output(request, filename):
    """
    Serves the Excel snapshot created by Stage-2:
    output/stage1_master_snapshot.xlsx
    """

    file_path = OUTPUT_DIR / filename

    if not file_path.exists():
        return HttpResponse("Snapshot file not found. Please upload and process first.", status=404)

    with open(file_path, "rb") as f:
        data = f.read()

    resp = HttpResponse(
        data,
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    resp["Content-Disposition"] = f'attachment; filename="{filename}"'
    return resp


# --------------------------------------------------------------------
# DOWNLOAD SELECTED COLUMNS
# --------------------------------------------------------------------
def download_selected_columns(request):
    """
    Reads the same snapshot Excel and returns only the selected columns.
    Used by the 'Download Selected' button in the upload.html template.
    """

    if request.method != "POST":
        return HttpResponse("Invalid method", status=405)

    selected_cols = request.POST.getlist("selected_columns")
    output_filename = request.POST.get("output_filename", SNAPSHOT_FILENAME)

    if not selected_cols:
        return HttpResponse("No columns selected.", status=400)

    file_path = OUTPUT_DIR / output_filename
    if not file_path.exists():
        return HttpResponse("Snapshot file not found. Please upload and process first.", status=404)

    try:
        df = pd.read_excel(file_path)

        # Ensure selected columns exist
        missing = [c for c in selected_cols if c not in df.columns]
        if missing:
            return HttpResponse(
                f"These columns are not present in the output file: {', '.join(missing)}",
                status=400,
            )

        df = df[selected_cols]

    except Exception as e:
        return HttpResponse(f"Failed to read or filter snapshot: {e}", status=500)

    # Return as XLSX response
    resp = HttpResponse(
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    resp["Content-Disposition"] = 'attachment; filename="selected_columns.xlsx"'
    df.to_excel(resp, index=False)
    return resp


# --------------------------------------------------------------------
# RUN STAGE-1 REFRESH
# --------------------------------------------------------------------
@require_POST
def run_stage1_refresh(request):
    """
    Called by the 'Refresh Stage 1' button via fetch().
    It will read output/stage1_master_snapshot.xlsx and reload PartMaster.
    """

    try:
        # This function already prints logs to console.
        load_part_master_from_snapshot()
        return JsonResponse(
            {
                "status": "ok",
                "message": "Stage-1 refresh completed from snapshot.",
            }
        )
    except Exception as e:
        return JsonResponse(
            {
                "status": "error",
                "message": f"Stage-1 refresh failed: {e}",
            },
            status=500,
        )
