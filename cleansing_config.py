# cleansing_config.py

# This dict is applied AFTER:
#  1. lower-case
#  2. replace non-alnum with '_'
#  3. strip sap_/vault_/powerbi_/po_/invoice_/user_ prefixes
#
# Key = normalized name, Value = canonical master name.

COLUMN_SYNONYMS = {
    # ---- identifiers ----
    "part_no": "part_number",
    "partno": "part_number",
    "material_code": "part_number",
    "material_no": "part_number",
    "mat_code": "part_number",
    "mat_no": "part_number",

    # ---- description ----
    "description_raw": "description",
    "item_description": "description",
    "desc": "description",
    "long_description": "description",
    "short_description": "description",

    # ---- material ----
    "spec_material": "material",
    "mat": "material",

    # ---- dimensions ----
    "spec_dimensions": "dimensions",
    "size": "dimensions",

    # ---- vendor ----
    "vendor": "vendor_name",
    "vendorname": "vendor_name",
    "vendor_code": "vendor_code",

    # ---- cost / price ----
    "price": "cost",
    "unit_price": "cost",
    "unitprice": "cost",
    "price_per_uom": "cost",
    "price_per_unom": "cost",
    "po_price_per_unit": "cost",
    "invoice_unit_price": "cost",
    "price_per_unit": "cost",

    # ---- category ----
    "category": "category_raw",
    "sub_category": "category_raw",
    "categoryname": "category_raw",

    # ---- flags ----
    "is_standard_part": "is_standard_part",
    "standard_part": "is_standard_part",
    "active": "active_flag",
    "active_status": "active_flag",

    # ---- engineer / drawing ----
    "engineer": "engineer_name",
    "engineername": "engineer_name",
    "drawing": "drawing_no",
    "drawing_number": "drawing_no",

    # ---- dates ----
    "created_on": "created_date",
    "creation_date": "created_date",
    "last_modified_date": "last_modified",
    "modified_on": "last_modified",

    # ---- quantities ----
    "qty": "quantity",
    "qty_ordered": "quantity",
    "order_qty": "quantity",
    "qty_supplied": "quantity",

    # ---- remarks / notes ----
    "comment": "remarks",
    "comments": "remarks",
    "note": "notes",
    "notes_field": "notes",
}
