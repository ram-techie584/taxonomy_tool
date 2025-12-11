# taxonomy_ui/views.py

from pathlib import Path
import os
import traceback

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
# UPLOAD + PROCESS (ENHANCED ERROR HANDLING)
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
        "error": None,
    }

    if request.method == "POST":
        try:
            # Input name in template is "files"
            uploaded_files = request.FILES.getlist("files")
            if not uploaded_files:
                context["error"] = "Please select at least one file to upload."
                return render(request, "taxonomy_ui/upload.html", context)

            # Log upload info for debugging
            print(f"[UPLOAD] Received {len(uploaded_files)} files")
            for f in uploaded_files:
                print(f"[UPLOAD] File: {f.name}, Size: {f.size} bytes, Type: {f.content_type}")

            # Process files
            df_out = run_stage2_from_django(uploaded_files)

            # Check if we got valid data
            if df_out is None or df_out.empty:
                context["error"] = "Processing completed but no data was produced."
                return render(request, "taxonomy_ui/upload.html", context)

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
            print(f"[SUCCESS] Upload processed successfully: {len(df_out)} rows")

        except Exception as e:
            # Capture full error for logging
            error_msg = str(e)
            print(f"[ERROR] Upload failed: {error_msg}")
            print(f"[ERROR] Traceback: {traceback.format_exc()}")
            
            # User-friendly error messages
            if "pdfplumber" in error_msg.lower():
                context["error"] = "PDF processing requires pdfplumber. Please install it in requirements.txt"
            elif "memory" in error_msg.lower() or "large" in error_msg.lower():
                context["error"] = f"File too large for processing. Please upload files under 10MB. Error: {error_msg}"
            elif "part_number" in error_msg.lower():
                context["error"] = f"Data error: {error_msg}. Please ensure your files have a 'part_number' column."
            else:
                context["error"] = f"Processing error: {error_msg}"

    return render(request, "taxonomy_ui/upload.html", context)


# --------------------------------------------------------------------
# VIEW DB PARTS  (/parts/)
# --------------------------------------------------------------------
def part_list(request):
    """
    Shows all rows from PartMaster.

    To avoid template-related 500 errors on Render, we render
    a simple HTML table directly here instead of depending on
    an external part_list.html template.
    """

    parts = PartMaster.objects.all().order_by("id")

    if not parts.exists():
        return HttpResponse(
            "<h2>No parts found in PartMaster table.</h2>",
            content_type="text/html",
        )

    # Build a very simple HTML table
    cols = [
        "id",
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

    rows_html = []
    for p in parts:
        cells = []
        for c in cols:
            val = getattr(p, c, "")
            cells.append(f"<td>{val}</td>")
        rows_html.append(f"<tr>{''.join(cells)}</tr>")

    header_html = "".join(f"<th>{c}</th>" for c in cols)

    html = f"""
    <html>
      <head>
        <title>PartMaster Data</title>
      </head>
      <body>
        <h2>PartMaster Records (total: {parts.count()})</h2>
        <table border="1" cellpadding="4" cellspacing="0">
          <thead>
            <tr>{header_html}</tr>
          </thead>
          <tbody>
            {''.join(rows_html)}
          </tbody>
        </table>
        <p><a href="/">⬅ Back to Upload</a></p>
      </body>
    </html>
    """

    return HttpResponse(html)


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

    try:
        with open(file_path, "rb") as f:
            data = f.read()

        resp = HttpResponse(
            data,
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        resp["Content-Disposition"] = f'attachment; filename="{filename}"'
        return resp
    except Exception as e:
        return HttpResponse(f"Error downloading file: {e}", status=500)


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


# --------------------------------------------------------------------
# DEBUG ENDPOINTS
# --------------------------------------------------------------------
def debug_upload(request):
    """Debug endpoint to check file upload handling"""
    if request.method == 'POST' and request.FILES:
        info = {
            'file_count': len(request.FILES),
            'file_names': list(request.FILES.keys()),
            'files_info': [],
            'render_info': {
                'RENDER_EXTERNAL_HOSTNAME': os.environ.get('RENDER_EXTERNAL_HOSTNAME', 'Not set'),
                'DATABASE_URL_set': 'DATABASE_URL' in os.environ,
                'DEBUG': os.environ.get('DJANGO_DEBUG', 'Not set'),
            }
        }
        
        for name, file in request.FILES.items():
            file_info = {
                'name': file.name,
                'size': file.size,
                'content_type': file.content_type,
                'readable': file.readable(),
                'ext': os.path.splitext(file.name)[1].lower()
            }
            
            # Read a bit to test
            try:
                content = file.read(100)
                file_info['preview'] = str(content[:50])
                file.seek(0)  # Reset for processing
            except Exception as e:
                file_info['error'] = str(e)
            
            info['files_info'].append(file_info)
        
        return JsonResponse(info)
    
    return HttpResponse("""
    <html>
    <body>
        <h2>Debug File Upload</h2>
        <form method="post" enctype="multipart/form-data">
            <input type="file" name="test" multiple>
            <button type="submit">Test Upload</button>
        </form>
        <hr>
        <h3>Environment Info:</h3>
        <ul>
            <li>RENDER_EXTERNAL_HOSTNAME: {hostname}</li>
            <li>DATABASE_URL set: {db_set}</li>
            <li>DJANGO_DEBUG: {debug}</li>
        </ul>
    </body>
    </html>
    """.format(
        hostname=os.environ.get('RENDER_EXTERNAL_HOSTNAME', 'Not set'),
        db_set='Yes' if 'DATABASE_URL' in os.environ else 'No',
        debug=os.environ.get('DJANGO_DEBUG', 'Not set')
    ))


def health_check(request):
    """Simple health check endpoint for Render"""
    return JsonResponse({
        'status': 'ok',
        'timestamp': pd.Timestamp.now().isoformat(),
        'database': 'connected' if PartMaster.objects.exists() else 'empty',
        'output_dir_exists': OUTPUT_DIR.exists(),
        'requirements_installed': {
            'pandas': True,  # If we got here, pandas is installed
            'pdfplumber': __import__('pdfplumber').__version__ if 'pdfplumber' in globals() else 'Not loaded'
        }
    })