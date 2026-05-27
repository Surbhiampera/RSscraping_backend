# main.py
#
# Upload Parser — handles all four entry modes:
#
#   Mode 1  bulk_with_reg      File upload containing Indian vehicle registration numbers
#   Mode 2  bulk_without_reg   File upload containing structured fields (Make/Model/RTO/...)
#   Mode 3  manual_with_reg    Single car registration number (no file)
#   Mode 4  manual_without_reg Single structured record (no file)
#
# Bulk modes are auto-detected by detect_upload_type().
# Manual modes are handled by parse_manual_car() and parse_manual_no_reg().
# parse_upload() is the unified entry point for bulk file uploads.

import sys
import os
import io
import re
import pandas as pd

from backend.upload_parser.utils import (
    auto_header_detect,
    header_match_score,
    matches_vehicle_pattern,
    vehicle_pattern_ratio,
    VEHICLE_REGEX,
    normalize_text,
    safe_ratio,
    state_code_ratio,
    uniqueness_ratio,
    structure_ratio,
    negative_penalty,
    numeric_range_score,
    parse_dates,
    future_ratio,
    recent_ratio,
    old_ratio,
    binary_ratio,
    ncb_slab_ratio,
    ncb_decimal_ratio,
    VEHICLE_HEADER_ALIASES,
    EXPIRY_HEADER_ALIASES,
    CLAIM_HEADER_ALIASES,
    CLAIM_VALID_VALUES,
    ALL_HEADER_ALIASES,
    ALL_KNOWN_DATA_VALUES,
    NCB_VALID_SLABS,
    NCB_VALID_SLABS_DECIMAL,
    HEADER_WEIGHT_WITH_HEADER,
    HEADER_WEIGHT_WITHOUT_HEADER,
    HEADER_DETECT_FUZZY_THRESHOLD,
    HEADER_SCORE_FUZZY_THRESHOLD,
    MIN_ROWS_FOR_UNIQUENESS_PENALTY,
    HOMOGENEITY_COLUMN_THRESHOLD,
    DATE_FORMATS,
    VALID_STATE_CODES,
    STRUCTURED_FIELD_ALIASES,
)

from .detectors import (
    detect_vehicle_registration,
    detect_expiry_date,
    detect_claim_status,
    detect_ncb,
)
from .engine import run_detection_v2
from .quality import run_quality_check, run_cross_column_quality_check


# ----------------------------------
# Upload Mode Constants
# ----------------------------------

UPLOAD_MODE_WITH_REG    = "with_reg"      # bulk file — has registration numbers
UPLOAD_MODE_WITHOUT_REG = "without_reg"   # bulk file — structured fields only
UPLOAD_MODE_AUTO        = "auto"          # bulk file — mixed: some rows with reg, some without

# No-reg required fields (Make + Model + RTO must be present for a row to be valid)
NO_REG_REQUIRED_FIELDS = {"make", "model", "rto_location"}

# Structured field display order used for labels
NO_REG_LABEL_FIELDS = ["make", "model", "rto_location", "yom"]


# ----------------------------------
# Constants
# ----------------------------------

CONFIDENCE_THRESHOLD = 50

DETECTORS = [
    detect_vehicle_registration,
    detect_expiry_date,
    detect_claim_status,
    detect_ncb,
]


# ----------------------------------
# Validation Helpers
# ----------------------------------

def validate_car_number(number: str) -> bool:
    cleaned = normalize_text(number)
    return bool(VEHICLE_REGEX.match(cleaned))


def clean_car_number(number: str) -> str:
    return normalize_text(number)


# ----------------------------------
# Preview Row Builders
# Vectorized — no iterrows(), scales to any number of rows
# ----------------------------------

def _build_preview_rows(df: pd.DataFrame, vehicle_col: str | None) -> list:
    """
    Build preview rows from the detected vehicle registration column.
    Uses list comprehension over pre-extracted arrays — O(n) with minimal overhead.
    """
    def _rows_from_col(col_name: str) -> list:
        raw_vals     = df[col_name].astype(str).str.strip().tolist()
        cleaned_vals = [clean_car_number(v) for v in raw_vals]
        valid_flags  = [validate_car_number(c) for c in cleaned_vals]
        return [
            {
                "car_number": c if v else r,
                "is_valid":   v,
                "error":      None if v else "Invalid registration number format",
            }
            for r, c, v in zip(raw_vals, cleaned_vals, valid_flags)
        ]

    if vehicle_col and vehicle_col in df.columns:
        return _rows_from_col(vehicle_col)

    # Fallback: scan for first column with high vehicle_pattern_ratio
    for col in df.columns:
        series = df[col].dropna()
        if not series.empty and vehicle_pattern_ratio(series) > 0.3:
            return _rows_from_col(col)

    # Last resort: use first column
    if df.columns.size > 0:
        return _rows_from_col(df.columns[0])

    return []


def _build_no_reg_preview_rows(df: pd.DataFrame, col_map: dict) -> list:
    """
    Build structured preview rows for no-reg upload mode.
    Pre-extracts column arrays so each row is built without DataFrame overhead.
    Scales to any number of rows.
    """
    # Pre-extract only the mapped columns as plain Python lists
    col_arrays: dict = {
        field: df[col].tolist()
        for field, col in col_map.items()
        if col in df.columns
    }
    n_rows = len(df)
    rows   = []

    for i in range(n_rows):
        input_data: dict = {}
        for field, values in col_arrays.items():
            raw = values[i]
            val = str(raw).strip() if pd.notna(raw) else ""
            if val and val.lower() not in ("nan", "none", ""):
                input_data[field] = val

        make  = input_data.get("make",         "")
        model = input_data.get("model",        "")
        rto   = input_data.get("rto_location", "")
        yom   = input_data.get("yom",          "")

        label_parts = [p for p in [make, model, rto, yom] if p]
        label    = " | ".join(label_parts) if label_parts else f"Row {i + 1}"
        is_valid = bool(make and model and rto)

        rows.append({
            "car_number": label,
            "is_valid":   is_valid,
            "error":      None if is_valid else "Missing required: Make, Model, RTO Location",
            "input_data": input_data,
        })
    return rows


# ----------------------------------
# Mixed Preview Row Builder
# Handles files where some rows have valid registration numbers
# and others only have structured fields (Make/Model/RTO).
# ----------------------------------

def _build_mixed_preview_rows(df: pd.DataFrame, vehicle_col: str | None, col_map: dict) -> list:
    """
    Per-row detection: for each row check if a valid vehicle registration exists.
    If yes → with_reg row (input_data=None).
    If no  → check structured cols; if Make+Model+RTO present → without_reg row.
    Otherwise → invalid row.
    """
    veh_values = None
    if vehicle_col and vehicle_col in df.columns:
        veh_values = df[vehicle_col].astype(str).str.strip().tolist()

    col_arrays: dict = {
        field: df[col].tolist()
        for field, col in col_map.items()
        if col in df.columns
    }

    rows = []
    for i in range(len(df)):
        raw_reg = veh_values[i] if veh_values else ""
        cleaned = clean_car_number(raw_reg)

        if validate_car_number(cleaned):
            rows.append({"car_number": cleaned, "is_valid": True, "error": None, "input_data": None})
            continue

        input_data: dict = {}
        for field, values in col_arrays.items():
            raw = values[i]
            val = str(raw).strip() if pd.notna(raw) else ""
            if val and val.lower() not in ("nan", "none", ""):
                input_data[field] = val

        make  = input_data.get("make",         "")
        model = input_data.get("model",        "")
        rto   = input_data.get("rto_location", "")
        yom   = input_data.get("yom",          "")

        if make and model and rto:
            label = " | ".join(p for p in [make, model, rto, yom] if p)
            rows.append({"car_number": label, "is_valid": True, "error": None, "input_data": input_data})
        else:
            rows.append({
                "car_number": raw_reg.strip() or f"Row {i + 1}",
                "is_valid":   False,
                "error":      "No valid vehicle number or Make/Model/RTO found",
                "input_data": None,
            })

    return rows


# ----------------------------------
# Empty Result Template
# ----------------------------------

def _empty_result(header_present=False, total_rows=0, preview=None):
    preview = preview or []
    valid_rows   = sum(1 for r in preview if r["is_valid"])
    invalid_rows = len(preview) - valid_rows
    return {
        "header_present": header_present,
        "schema": {},
        "metadata": {
            "total_columns": 0,
            "detected_fields": [],
            "undetected_columns": [],
            "column_details": [],
        },
        "quality": {
            "nulls": {},
            "duplicates": {},
            "summary": {
                "total_rows": total_rows,
                "total_fields_checked": 0,
                "fields_with_nulls": [],
                "fields_with_duplicates": [],
                "clean_fields": [],
            },
        },
        "cross_column_quality": [],
        "preview": preview,
        "total_rows": total_rows,
        "valid_rows": valid_rows,
        "invalid_rows": invalid_rows,
        "df": pd.DataFrame(),
    }


# ----------------------------------
# PDF Parser
# ----------------------------------

def _parse_pdf(content: bytes) -> dict:
    try:
        import pdfplumber
    except ImportError:
        return _empty_result()

    numbers = []
    with pdfplumber.open(io.BytesIO(content)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            found = VEHICLE_REGEX.findall(text)
            numbers.extend(found)

    preview = []
    for raw in numbers:
        cleaned  = clean_car_number(raw)
        is_valid = validate_car_number(cleaned)
        preview.append({
            "car_number": cleaned if is_valid else raw.strip(),
            "is_valid":   is_valid,
            "error":      None if is_valid else "Invalid registration number format",
        })

    return _empty_result(
        header_present=False,
        total_rows=len(preview),
        preview=preview,
    )


# ----------------------------------
# Core Engine (file-path based)
# ----------------------------------

def run_detection(file_path: str) -> dict:
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: '{file_path}'")
    if not os.path.isfile(file_path):
        raise ValueError(f"Path is not a file: '{file_path}'")

    supported = (".csv", ".xls", ".xlsx")
    if not file_path.lower().endswith(supported):
        raise ValueError(
            f"Unsupported file type: '{file_path}'. "
            f"Please provide a CSV or Excel file."
        )

    df, header_present = auto_header_detect(file_path)
    header_weight = HEADER_WEIGHT_WITH_HEADER if header_present else HEADER_WEIGHT_WITHOUT_HEADER

    all_results = []
    for col in df.columns:
        series = df[col]
        for detector in DETECTORS:
            result          = detector(col, series, header_weight=header_weight)
            result["column"] = col
            all_results.append(result)

    mappings = {}
    for result in all_results:
        field = result["field"]
        if result["score"] < CONFIDENCE_THRESHOLD:
            continue
        if field not in mappings or result["score"] > mappings[field]["score"]:
            mappings[field] = {
                "column":   result["column"],
                "score":    result["score"],
                "evidence": result["evidence"],
            }

    return {
        "header_present": header_present,
        "mappings":       mappings,
        "all_results":    all_results,
    }


# ----------------------------------
# Upload Mode Detection
# ----------------------------------

def detect_upload_type(df: pd.DataFrame) -> str:
    """
    Auto-detects whether a bulk upload file contains registration numbers or not.

    Returns:
        UPLOAD_MODE_WITH_REG    ("with_reg")    if any column has vehicle reg pattern
        UPLOAD_MODE_WITHOUT_REG ("without_reg") otherwise
    """
    for col in df.columns:
        non_null = df[col].dropna()
        if not non_null.empty and vehicle_pattern_ratio(non_null) > 0.3:
            return UPLOAD_MODE_WITH_REG
    return UPLOAD_MODE_WITHOUT_REG


# ----------------------------------
# No-Reg Column Mapper
# ----------------------------------

def _detect_no_reg_column_map(df: pd.DataFrame) -> dict:
    """
    Map DataFrame columns → structured field names using header alias matching.
    Returns {field_name: column_name} for the best-scoring match per field.
    Handles all 12 expected fields:
        Sr No, RTO/Location, Make, Model, Variant, CC, CC Range,
        Fuel Type, Insured Company, Earned NCB %, Tariff Rate, YOM/Age
    """
    col_map: dict = {}
    for col in df.columns:
        col_str    = str(col).strip().lower().replace("_", " ").replace("/", " ")
        best_field: str | None = None
        best_score = 0.0
        for field, aliases in STRUCTURED_FIELD_ALIASES.items():
            if field in col_map:
                continue
            score = header_match_score(col_str, aliases)
            if score > best_score and score >= 0.6:
                best_score = score
                best_field = field
        if best_field:
            col_map[best_field] = col
    return col_map


# ----------------------------------
# parse_upload — Bulk file entry point
#
# Handles Mode 1 (bulk_with_reg) and Mode 2 (bulk_without_reg).
# Auto-detects which mode applies via detect_upload_type().
# Supports any number of rows via vectorized row builders.
# ----------------------------------

def parse_upload(file_content: bytes, filename: str) -> dict:
    """
    Main entry point for bulk file uploads.
    Auto-detects format and returns structured result for all rows.
    """
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""

    if ext not in ("pdf", "csv", "xlsx", "xls"):
        raise ValueError(f"Unsupported file type: .{ext}")

    if ext == "pdf":
        return _parse_pdf(file_content)

    stream      = io.BytesIO(file_content)
    stream.name = filename

    df, header_present = auto_header_detect(stream)
    if df.empty:
        return {
            "total_rows":     0,
            "preview":        [],
            "header_present": header_present,
            "metadata":       {},
            "quality":        {},
            "schema":         {},
        }

    # Attempt header promotion when none detected
    if not header_present and len(df) > 1:
        first_row = df.iloc[0]
        candidate_cols = [
            str(v).strip() if pd.notnull(v) and str(v).strip() else f"col_{i}"
            for i, v in enumerate(first_row)
        ]
        vehicle_re = re.compile(r'^[A-Z]{2}\d{1,2}[A-Z]{0,3}\d{1,4}$', re.IGNORECASE)
        text_cells = sum(
            1 for c in candidate_cols
            if not c.startswith("col_")
            and not c.replace(".", "").isdigit()
            and not vehicle_re.match(c.replace(" ", "").replace("-", ""))
        )
        if text_cells > 0:
            df_with_header = df.iloc[1:].reset_index(drop=True).copy()
            df_with_header.columns = candidate_cols

            result_no_header   = run_detection_v2(df,             header_present=False)
            result_with_header = run_detection_v2(df_with_header, header_present=True)

            if len(result_with_header["schema"]) > len(result_no_header["schema"]):
                df             = df_with_header
                header_present = True

    detection_result = run_detection_v2(df, header_present=header_present)
    schema           = detection_result["schema"]
    metadata         = detection_result["metadata"]

    quality              = run_quality_check(df, schema)
    cross_column_quality = run_cross_column_quality_check(df, schema)

    # detect_upload_type uses value-based vehicle_pattern_ratio as the primary signal
    upload_mode = detect_upload_type(df)
    field_stats = {}

    if upload_mode == UPLOAD_MODE_WITHOUT_REG:
        col_map  = _detect_no_reg_column_map(df)
        preview  = _build_no_reg_preview_rows(df, col_map)
        valid_rows   = sum(1 for r in preview if r["is_valid"])
        invalid_rows = len(preview) - valid_rows

    else:
        # Normalize expiry dates
        for field_name, info in schema.items():
            if field_name.startswith("expiry_date"):
                col_name = info["column"]
                if col_name in df.columns:
                    parsed = parse_dates(df[col_name])
                    if not parsed.empty:
                        df[col_name] = df[col_name].copy()
                        for idx in parsed.index:
                            df.at[idx, col_name] = parsed[idx].strftime("%Y-%m-%d")

        # Normalize NCB values
        for field_name, info in schema.items():
            if field_name.startswith("ncb_percentage"):
                col_name = info["column"]
                if col_name in df.columns:
                    df[col_name] = df[col_name].apply(_normalize_ncb_value)

        vehicle_col = None
        for field_name, info in schema.items():
            if field_name.startswith("vehicle_registration_number"):
                vehicle_col = info["column"]
                break

        # Check if structured columns (Make/Model/RTO) are also present.
        # If so, use per-row mixed detection so rows without a valid reg number
        # can fall back to the without_reg flow instead of being marked invalid.
        col_map        = _detect_no_reg_column_map(df)
        has_structured = bool(col_map.get("make") and col_map.get("model") and col_map.get("rto_location"))

        if has_structured:
            preview      = _build_mixed_preview_rows(df, vehicle_col, col_map)
            valid_valid  = [r for r in preview if r["is_valid"]]
            if not valid_valid or all(r.get("input_data") is None for r in valid_valid):
                upload_mode = UPLOAD_MODE_WITH_REG
            elif all(r.get("input_data") is not None for r in valid_valid):
                upload_mode = UPLOAD_MODE_WITHOUT_REG
            else:
                upload_mode = UPLOAD_MODE_AUTO
        else:
            preview     = _build_preview_rows(df, vehicle_col)
            col_map     = {}  # pure with_reg: no structured cols

        valid_rows   = sum(1 for r in preview if r["is_valid"])
        invalid_rows = len(preview) - valid_rows

        for field_name, info in schema.items():
            col_name = info["column"]
            if col_name not in df.columns:
                continue
            series = df[col_name]
            stats  = {"uniqueness": round(uniqueness_ratio(series), 4)}

            if field_name.startswith("vehicle_registration_number"):
                stats["state_code_ratio"]     = round(state_code_ratio(series), 4)
                stats["structure_ratio"]      = round(structure_ratio(series), 4)
                stats["vehicle_pattern_ratio"]= round(vehicle_pattern_ratio(series), 4)
                stats["negative_penalty"]     = negative_penalty(series)

            if field_name.startswith("expiry_date"):
                parsed = parse_dates(series)
                if not parsed.empty:
                    stats["future_ratio"] = round(future_ratio(parsed), 4)
                    stats["recent_ratio"] = round(recent_ratio(parsed), 4)
                    stats["old_ratio"]    = round(old_ratio(parsed), 4)

            if field_name.startswith("ncb_percentage"):
                stats["ncb_slab_ratio"]      = round(ncb_slab_ratio(series), 4)
                stats["ncb_decimal_ratio"]   = round(ncb_decimal_ratio(series), 4)
                stats["numeric_range_score"] = round(numeric_range_score(
                    series.astype(str).str.replace("%", "", regex=False).str.strip(), 0, 100
                ), 4)

            if field_name.startswith("claim_status"):
                stats["binary_ratio"] = round(binary_ratio(series, CLAIM_VALID_VALUES), 4)

            field_stats[field_name] = stats

    return {
        "header_present":       header_present,
        "schema":               schema,
        "metadata":             metadata,
        "quality":              quality,
        "cross_column_quality": cross_column_quality,
        "field_stats":          field_stats,
        "preview":              preview,
        "total_rows":           len(df),
        "valid_rows":           valid_rows,
        "invalid_rows":         invalid_rows,
        "df":                   df,
        "upload_mode":          upload_mode,
        "col_map":              col_map,
    }


# ----------------------------------
# Manual Entry Parsers
# ----------------------------------

def parse_manual_car(car_number: str) -> dict:
    """
    Mode 3 — Manual entry with car registration number.
    Parses and validates a single car number string.

    Returns a dict with the same shape as parse_upload() preview rows
    plus top-level summary fields.
    """
    car_number = car_number.strip().upper()
    cleaned    = clean_car_number(car_number)
    is_valid   = validate_car_number(cleaned)

    row = {
        "car_number": cleaned if is_valid else car_number,
        "is_valid":   is_valid,
        "error":      None if is_valid else "Invalid registration number format",
    }

    return {
        "upload_mode":    UPLOAD_MODE_WITH_REG,
        "header_present": False,
        "schema":         {},
        "metadata":       None,
        "quality":        None,
        "preview":        [row],
        "total_rows":     1,
        "valid_rows":     1 if is_valid else 0,
        "invalid_rows":   0 if is_valid else 1,
        "col_map":        {},
        "field_stats":    {},
    }


def parse_manual_no_reg(data: dict) -> dict:
    """
    Mode 4 — Manual entry without car registration number.
    Validates the 12 structured fields; Make, Model, and RTO Location are required.

    Expected keys (all optional except make, model, rto_location):
        make, model, rto_location, variant, cc, cc_range,
        fuel_type, insured_company, ncb_percent, tariff_rate, yom, sr_no

    Returns a dict with the same shape as parse_upload() preview rows.
    """
    make  = (data.get("make")         or "").strip()
    model = (data.get("model")        or "").strip()
    rto   = (data.get("rto_location") or "").strip()

    missing = [f for f, v in [("make", make), ("model", model), ("rto_location", rto)] if not v]
    is_valid = len(missing) == 0
    error    = f"Missing required: {', '.join(missing)}" if missing else None

    # Keep only non-empty values
    input_data = {
        k: str(v).strip()
        for k, v in data.items()
        if v and str(v).strip() and str(v).strip().lower() not in ("nan", "none", "")
    }

    yom         = input_data.get("yom", "")
    label_parts = [p for p in [make, model, rto, yom] if p]
    label       = " | ".join(label_parts) if label_parts else "Manual Entry"

    row = {
        "car_number": label,
        "is_valid":   is_valid,
        "error":      error,
        "input_data": input_data,
    }

    return {
        "upload_mode":    UPLOAD_MODE_WITHOUT_REG,
        "header_present": False,
        "schema":         {},
        "metadata":       None,
        "quality":        None,
        "preview":        [row],
        "total_rows":     1,
        "valid_rows":     1 if is_valid else 0,
        "invalid_rows":   0 if is_valid else 1,
        "col_map":        {},
        "field_stats":    {},
    }


# ----------------------------------
# NCB Normalizer
# ----------------------------------

def _normalize_ncb_value(value):
    if pd.isna(value) or str(value).strip() == "":
        return value

    s = str(value).strip().replace("%", "")
    try:
        num = float(s)
    except (ValueError, TypeError):
        return value

    if 0 < num < 1 and num in NCB_VALID_SLABS_DECIMAL:
        return str(int(round(num * 100)))

    if num in NCB_VALID_SLABS:
        return str(int(num))

    return value


# ----------------------------------
# Output Printer (CLI)
# ----------------------------------

def print_results(output: dict):
    print("\n" + "=" * 55)
    print(f"  Header Detected : {'YES' if output['header_present'] else 'NO (headerless mode)'}")
    print("=" * 55)

    mappings = output["mappings"]

    if not mappings:
        print("\n  No fields detected above confidence threshold.\n")
        return

    print("\n  DETECTED FIELD MAPPINGS\n")
    print(f"  {'Field':<35} {'Column':<25} {'Score'}")
    print(f"  {'-'*35} {'-'*25} {'-'*5}")

    for field, info in mappings.items():
        print(f"  {field:<35} {str(info['column']):<25} {info['score']}")
        print(f"  {'':35} Evidence : {', '.join(info['evidence'])}")
        print()

    print("=" * 55)

    print("\n  FULL COLUMN SCORES (all detectors)\n")
    print(f"  {'Column':<25} {'Field':<35} {'Score'}")
    print(f"  {'-'*25} {'-'*35} {'-'*5}")
    for r in sorted(output["all_results"], key=lambda x: -x["score"]):
        if r["score"] > 0:
            print(f"  {str(r['column']):<25} {r['field']:<35} {r['score']}")

    print("=" * 55 + "\n")


# ----------------------------------
# Entry Point (CLI)
# ----------------------------------

if __name__ == "__main__":
    print("\n  Smart Parser — Field Detection Engine")
    print("  Supported formats: CSV, XLS, XLSX")
    print("  Type 'exit' to quit\n")

    while True:
        try:
            file_path = input("  Enter file path: ").strip()
        except (KeyboardInterrupt, EOFError):
            print("\n\n  Exiting. Goodbye!\n")
            sys.exit(0)

        if file_path.lower() in ("exit", "quit", "q"):
            print("\n  Exiting. Goodbye!\n")
            sys.exit(0)

        if not file_path:
            print("  No path entered. Please try again.\n")
            continue

        try:
            output = run_detection(file_path)
            print_results(output)
        except FileNotFoundError as e:
            print(f"\n  {e}")
            print(f"  Current directory : {os.getcwd()}")
            print(f"  Full path checked : {os.path.abspath(file_path)}\n")
        except ValueError as e:
            print(f"\n  {e}\n")
        except Exception as e:
            print(f"\n  Unexpected error: {e}\n")

        try:
            again = input("  Process another file? (y/n): ").strip().lower()
        except (KeyboardInterrupt, EOFError):
            print("\n\n  Exiting. Goodbye!\n")
            sys.exit(0)

        if again not in ("y", "yes"):
            print("\n  Exiting. Goodbye!\n")
            break
