# db.py
import math
from typing import Dict, Any, Iterable, List, Sequence

from django.db import connection, transaction
from psycopg2.extras import execute_values


# ------------------------------------------------------------
# INTERNAL HELPERS
# ------------------------------------------------------------

def _get_cursor():
    """
    Always use Django-managed DB connection.
    Works on Render + local.
    """
    return connection.cursor()


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


def _sanitize_value(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    s = str(v).strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return None
    return s


# ------------------------------------------------------------
# INIT DB (SAFE ON RENDER)
# ------------------------------------------------------------

def init_db():
    """
    Create part_master table if not exists.
    """
    with connection.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS part_master (
                id SERIAL PRIMARY KEY,
                part_number TEXT UNIQUE,
                updated_at TIMESTAMP DEFAULT NOW()
            );
            """
        )


# ------------------------------------------------------------
# ENSURE DYNAMIC COLUMNS
# ------------------------------------------------------------

def ensure_columns(columns: Iterable[str]) -> None:
    cols = set(columns) - {"id", "part_number", "updated_at"}
    if not cols:
        return

    with connection.cursor() as cur:
        existing = set(_get_existing_columns(cur))
        new_cols = [c for c in cols if c not in existing]

        for c in new_cols:
            cur.execute(f'ALTER TABLE part_master ADD COLUMN "{c}" TEXT;')


# ------------------------------------------------------------
# UPSERT LOGIC
# ------------------------------------------------------------

def upsert_part_master(records: Sequence[Dict[str, Any]]) -> None:
    if not records:
        return

    all_keys = set()
    for r in records:
        all_keys.update(r.keys())

    if "part_number" not in all_keys:
        return

    ensure_columns(all_keys)

    with connection.cursor() as cur:
        existing_cols = set(_get_existing_columns(cur))

        used_cols = [
            c for c in all_keys
            if c in existing_cols and c not in {"id", "updated_at"}
        ]

        used_cols = ["part_number"] + [c for c in used_cols if c != "part_number"]

        cols_sql = ", ".join(f'"{c}"' for c in used_cols)

        update_assignments = ", ".join(
            f'"{c}" = EXCLUDED."{c}"'
            for c in used_cols if c != "part_number"
        )

        sql = f"""
            INSERT INTO part_master ({cols_sql})
            VALUES %s
            ON CONFLICT (part_number) DO UPDATE SET
                {update_assignments},
                updated_at = NOW();
        """

        values = []
        for r in records:
            pn = r.get("part_number")
            if not pn:
                continue
            values.append([_sanitize_value(r.get(c)) for c in used_cols])

        if values:
            execute_values(cur, sql, values)


# ------------------------------------------------------------
# FETCH SINGLE PART
# ------------------------------------------------------------

def fetch_part_by_number(part_number: str) -> Dict[str, Any] | None:
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
