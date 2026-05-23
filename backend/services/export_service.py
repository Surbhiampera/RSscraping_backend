import io
import csv
import json
import sys
from pathlib import Path
from typing import List

# Ensure db_complete_flow is importable so we can reuse its Excel logic
_db_flow_dir = str(Path(__file__).resolve().parent.parent / "db_complete_flow" / "db_pipeline")
if _db_flow_dir not in sys.path:
    sys.path.insert(0, _db_flow_dir)

EXCLUDED_KEYS = {"run_id", "plan_hidden"}


# -------------------------------
# COMMON CLEANING
# -------------------------------
def _strip_internal(rows: List[dict]) -> List[dict]:
    return [{k: v for k, v in row.items() if k not in EXCLUDED_KEYS} for row in rows]


def _normalize_rows(rows: List[dict]) -> List[dict]:
    """
    Apply same normalization as original script:
    - Rename fields
    - Ensure required columns exist
    """
    normalized = []

    for row in rows:
        new_row = dict(row)

        # Rename fields like original script
        if "sr_no" in new_row:
            new_row["Sr No"] = new_row.pop("sr_no")

        if "Company" in new_row:
            new_row["Insurer"] = new_row.pop("Company")

        # Ensure IDV Type exists
        if "IDV Type" not in new_row:
            new_row["IDV Type"] = None

        normalized.append(new_row)

    return normalized


# -------------------------------
# CSV EXPORT
# -------------------------------
def export_to_csv(rows: List[dict]) -> bytes:
    rows = _normalize_rows(_strip_internal(rows))

    if not rows:
        return b""

    # Collect all keys (same logic as before)
    all_keys = []
    seen = set()

    for row in rows:
        for k in row:
            if k not in seen:
                all_keys.append(k)
                seen.add(k)

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=all_keys, extrasaction="ignore")

    writer.writeheader()
    writer.writerows(rows)

    return output.getvalue().encode("utf-8")


# -------------------------------
# JSON EXPORT
# -------------------------------
def export_to_json(rows: List[dict]) -> bytes:
    rows = _normalize_rows(_strip_internal(rows))
    return json.dumps(rows, indent=2, default=str).encode("utf-8")


# -------------------------------
# EXCEL EXPORT (MAIN FIX)
# -------------------------------
def export_to_excel(rows: List[dict], idv_type: str = None) -> bytes:
    """
    Produce an Excel file using the same pipeline logic as flatdb_excel.py.
    If idv_type ('default' or 'median') is given, returns a single-sheet workbook
    for that type only. Otherwise returns Default + Median sheets.
    """
    import pandas as pd
    from flatdb_excel import build_excel_bytes, build_excel_bytes_for_type

    rows = _normalize_rows(_strip_internal(rows))

    if not rows:
        output = io.BytesIO()
        pd.DataFrame().to_excel(output, index=False, engine="openpyxl")
        output.seek(0)
        return output.read()

    if idv_type:
        return build_excel_bytes_for_type(rows, idv_type)
    return build_excel_bytes(rows)


# -------------------------------
# ORM → FLAT DICT
# -------------------------------
def rows_to_flat_dicts(cars) -> List[dict]:
    """Convert CarInfo ORM objects to flat dicts for export."""
    results = []

    for car in cars:
        results.append({
            "registration_number": car.registration_number,
            "make": car.make_name,
            "model": car.model_name,
            "variant": car.vehicle_variant,
            "fuel_type": car.fuel_type,
            "cubic_capacity": car.cubic_capacity,
            "state_code": car.state_code,
            "city_tier": car.city_tier,
            "car_age": car.car_age,
            "registration_date": str(car.registration_date) if car.registration_date else None,
            "status": car.status.value if hasattr(car.status, "value") else car.status,
            "error_message": car.error_message,
        })

    return results