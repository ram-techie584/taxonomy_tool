# taxonomy_ui/migrations/0002_ensure_part_master_table.py

from django.db import migrations

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS part_master (
    id SERIAL PRIMARY KEY,
    part_number VARCHAR(100) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    dimensions VARCHAR(255),
    description TEXT,
    cost VARCHAR(100),
    material VARCHAR(255),
    vendor_name VARCHAR(255),
    currency VARCHAR(50),
    category_raw VARCHAR(255),
    category_master VARCHAR(255),
    source_system VARCHAR(50),
    source_file VARCHAR(255)
);
"""


class Migration(migrations.Migration):

    dependencies = [
        ("taxonomy_ui", "0001_initial"),
    ]

    operations = [
        migrations.RunSQL(
            sql=CREATE_TABLE_SQL,
            reverse_sql="DROP TABLE IF EXISTS part_master;",
        ),
    ]
