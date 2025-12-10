# ingestion_utils.py

import os
from typing import Optional
import pandas as pd

VALID_EXT = {".csv", ".xlsx", ".xls", ".pdf"}


# ---------------------------------------------------
# EXCEL / CSV — FILE-LIKE (Django upload)
# ---------------------------------------------------

def load_excel_or_csv_from_filelike(file_obj, ext: str) -> pd.DataFrame:
    """
    Load CSV or Excel from Django InMemoryUploadedFile / TemporaryUploadedFile
    """
    file_obj.seek(0)

    if ext == ".csv":
        return pd.read_csv(file_obj)
    else:
        return pd.read_excel(file_obj)


# ---------------------------------------------------
# PDF — FILE-LIKE (Django upload)
# ---------------------------------------------------

def load_pdf_tables_from_filelike(file_obj) -> pd.DataFrame:
    """
    Extract tables from an uploaded PDF using pdfplumber.
    pdfplumber is imported lazily to avoid startup crashes.
    """
    import pdfplumber  # ✅ lazy import (IMPORTANT)

    file_obj.seek(0)
    rows = []

    with pdfplumber.open(file_obj) as pdf:
        for page_idx, page in enumerate(pdf.pages):
            tables = page.extract_tables()

            for table in tables or []:
                if not table:
                    continue

                header = table[0]
                data_rows = table[1:]

                for row in data_rows:
                    row_dict = {}

                    for col, val in zip(header, row):
                        if col is None:
                            continue
                        col = str(col).strip()
                        row_dict[col] = val

                    if row_dict:
                        row_dict["pdf_page"] = page_idx + 1
                        rows.append(row_dict)

    return pd.DataFrame(rows) if rows else pd.DataFrame()


# ---------------------------------------------------
# EXCEL / CSV — FILE PATH
# ---------------------------------------------------

def load_excel_or_csv(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()

    if ext == ".csv":
        return pd.read_csv(path)
    else:
        return pd.read_excel(path)


# ---------------------------------------------------
# PDF — FILE PATH
# ---------------------------------------------------

def load_pdf_tables(path: str) -> pd.DataFrame:
    import pdfplumber  # ✅ lazy import

    rows = []

    with pdfplumber.open(path) as pdf:
        for page_idx, page in enumerate(pdf.pages):
            tables = page.extract_tables()

            for table in tables or []:
                if not table:
                    continue

                header = table[0]
                data_rows = table[1:]

                for row in data_rows:
                    row_dict = {}

                    for col, val in zip(header, row):
                        if col is None:
                            continue
                        col = str(col).strip()
                        row_dict[col] = val

                    if row_dict:
                        row_dict["pdf_page"] = page_idx + 1
                        rows.append(row_dict)

    return pd.DataFrame(rows) if rows else pd.DataFrame()


# ---------------------------------------------------
# MAIN ENTRY POINT
# ---------------------------------------------------

def load_file(path_or_file) -> Optional[pd.DataFrame]:
    """
    Accepts BOTH:
    - File path string
    - Django InMemoryUploadedFile / TemporaryUploadedFile
    """

    # ---------------------------------------------------
    # CASE 1: Django uploaded file
    # ---------------------------------------------------
    if hasattr(path_or_file, "name") and hasattr(path_or_file, "read"):
        filename = path_or_file.name
        ext = os.path.splitext(filename)[1].lower()

        print("DEBUG — load_file detected upload:", filename)

        if ext not in VALID_EXT:
            print("⚠️ Unsupported uploaded file type:", ext)
            return pd.DataFrame()

        try:
            if ext in {".csv", ".xlsx", ".xls"}:
                return load_excel_or_csv_from_filelike(path_or_file, ext)

            if ext == ".pdf":
                return load_pdf_tables_from_filelike(path_or_file)

        except Exception as e:
            print(f"⚠️ Failed to load uploaded file {filename}: {e}")
            return pd.DataFrame()

    # ---------------------------------------------------
    # CASE 2: File path string
    # ---------------------------------------------------
    if isinstance(path_or_file, str):
        ext = os.path.splitext(path_or_file)[1].lower()

        if ext not in VALID_EXT:
            print("⚠️ Unsupported file type:", ext)
            return pd.DataFrame()

        try:
            if ext in {".csv", ".xlsx", ".xls"}:
                return load_excel_or_csv(path_or_file)

            if ext == ".pdf":
                return load_pdf_tables(path_or_file)

        except Exception as e:
            print(f"⚠️ Failed to load {path_or_file}: {e}")
            return pd.DataFrame()

    # ---------------------------------------------------
    # INVALID INPUT
    # ---------------------------------------------------
    raise TypeError("load_file() expected a file path string or file-like object.")
