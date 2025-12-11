# db.py
import math
from typing import Dict, Any, Iterable, List, Sequence

from django.db import connection, transaction
from psycopg2.extras import execute_values


# ============================================================
# INTERNAL HELPERS
# ============================================================

def _sanitize_value(v: Any) -> Any:
    """Normalize empty / NaN values to None."""
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    s = str(v).strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return None
    return s


def _get_existing_columns(cur) -> List[str]:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'part_master'
          AND table_schema = 'public';
        """
    )
    return [r[0] for r in cur.fetchall()]


# ============================================================
# DB INITIALIZATION (SAFE ON RENDER)
# ============================================================

def init_db() -> None:
    """
    Ensure base part_master table exists.
    Safe to call multiple times.
    """
    with connection.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS part_master (
                id SERIAL PRIMARY KEY,
                part_number TEXT UNIQUE NOT NULL,
                updated_at TIMESTAMP DEFAULT NOW()
            );
            """
        )


# ============================================================
# ENSURE DYNAMIC COLUMNS
# ============================================================

def ensure_columns(columns: Iterable[str]) -> None:
    """
    Add missing columns dynamically as TEXT.
    """
    cols = set(columns) - {"id", "part_number", "updated_at"}
    if not cols:
        return

    with connection.cursor() as cur:
        existing = set(_get_existing_columns(cur))
        new_cols = [c for c in cols if c not in existing]

        for c in new_cols:
            cur.execute(f'ALTER TABLE part_master ADD COLUMN "{c}" TEXT;')


# ============================================================
# UPSERT LOGIC (CRITICAL FIXED VERSION)
# ============================================================

def upsert_part_master(records: Sequence[Dict[str, Any]]) -> None:
    """
    Insert / update part_master with dynamic columns.
    FULLY TRANSACTION SAFE.
    """
    if not records:
        return

    # Ensure table exists
    init_db()

    # Collect all keys
    all_keys = set()
    for r in records:
        all_keys.update(r.keys())

    if "part_number" not in all_keys:
        raise ValueError("part_number is required for upsert")

    # Ensure columns exist
    ensure_columns(all_keys)

    # ✅ CRITICAL: wrap raw SQL in transaction
    with transaction.atomic():
        with connection.cursor() as cur:
            existing_cols = set(_get_existing_columns(cur))

            used_cols = [
                c for c in all_keys
                if c in existing_cols and c not in {"id", "updated_at"}
            ]

            # Ensure part_number is first
            used_cols = ["part_number"] + [
                c for c in used_cols if c != "part_number"
            ]

            cols_sql = ", ".join(f'"{c}"' for c in used_cols)

            update_assignments = ", ".join(
                f'"{c}" = EXCLUDED."{c}"'
                for c in used_cols if c != "part_number"
            )

            sql = f"""
                INSERT INTO part_master ({cols_sql})
                VALUES %s
                ON CONFLICT (part_number)
                DO UPDATE SET
                    {update_assignments},
                    updated_at = NOW();
            """

            values = []
            for r in records:
                pn = r.get("part_number")
                if not pn:
                    continue

                values.append([
                    _sanitize_value(r.get(c)) for c in used_cols
                ])

            if values:
                execute_values(cur, sql, values)


# ============================================================
# FETCH SINGLE PART (USED IN STAGE-2)
# ============================================================

def fetch_part_by_number(part_number: str) -> Dict[str, Any] | None:
    """
    Fetch a single part row as dict (dynamic columns supported).
    """
    if not part_number:
        return None

    init_db()

    with connection.cursor() as cur:
        cur.execute(
            'SELECT * FROM part_master WHERE part_number = %s;',
            (part_number,)
        )
        row = cur.fetchone()
        if not row:
            return None

        cols = [desc[0] for desc in cur.description]
        return dict(zip(cols, row))
