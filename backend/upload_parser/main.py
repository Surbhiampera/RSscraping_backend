# main.py
import sys
import os
import io
import re
import pandas as pd

# ----------------------------------
# ✅ Import ALL functions from utils.py
# ----------------------------------
from backend.upload_parser.utils import (
    # V2 Auto Header Detection
    auto_header_detect,
    # Auto Header Detection Helpers (used internally by auto_header_detect)
    # _cell_is_header_like_fuzzy, _homogeneity_check — private, called via auto_header_detect
    # Internal File Readers (used internally by auto_header_detect)
    # _read_raw, _read_with_header — private, called via auto_header_detect
    # Excel Cell Cleaner (used internally by _read_raw / _read_with_header)
    # _clean_excel_cell — private, called via file readers

    # Header Match Score — Hybrid: substring fast-path + fuzzy partial_ratio fallback
    header_match_score,

    # Vehicle Pattern Helpers
    matches_vehicle_pattern,
    vehicle_pattern_ratio,
    VEHICLE_REGEX,

    # General Helpers
    normalize_text,
    safe_ratio,

    # State Code Ratio
    state_code_ratio,

    # Uniqueness Ratio
    uniqueness_ratio,

    # Structure Ratio
    structure_ratio,

    # Negative Penalty
    negative_penalty,

    # Numeric Range Score
    numeric_range_score,

    # Date Parsing Helpers — ordered date formats (MM-DD-YYYY primary)
    parse_dates,

    # Date Distribution Helpers
    future_ratio,
    recent_ratio,
    old_ratio,

    # Binary Pattern Helper
    binary_ratio,

    # NCB Slab Helpers — integer (20), percentage (20%), decimal (0.20)
    ncb_slab_ratio,
    ncb_decimal_ratio,

    # Constants — Flattened alias list
    VEHICLE_HEADER_ALIASES,
    EXPIRY_HEADER_ALIASES,
    CLAIM_HEADER_ALIASES,
    CLAIM_VALID_VALUES,
    ALL_HEADER_ALIASES,
    ALL_KNOWN_DATA_VALUES,

    # Constants — NCB
    NCB_VALID_SLABS,
    NCB_VALID_SLABS_DECIMAL,

    # Constants — Global header weight
    HEADER_WEIGHT_WITH_HEADER,
    HEADER_WEIGHT_WITHOUT_HEADER,

    # Constants — Fuzzy matching thresholds
    HEADER_DETECT_FUZZY_THRESHOLD,
    HEADER_SCORE_FUZZY_THRESHOLD,

    # Constants — Minimum uniqueness row thresholds
    MIN_ROWS_FOR_UNIQUENESS_PENALTY,

    # Constants — Type mismatch fraction thresholds
    HOMOGENEITY_COLUMN_THRESHOLD,

    # Constants — Ordered date formats
    DATE_FORMATS,

    # Constants — State codes
    VALID_STATE_CODES,
)

# ----------------------------------
# ✅ Import ALL detectors from detectors.py
# ----------------------------------
from .detectors import (
    detect_vehicle_registration,
    detect_expiry_date,
    detect_claim_status,
    detect_ncb,
)

# ----------------------------------
# ✅ Import V2 engine from engine.py
# ----------------------------------
from .engine import run_detection_v2

# ----------------------------------
# ✅ Import ALL quality functions from quality.py
# ----------------------------------
from .quality import run_quality_check, run_cross_column_quality_check

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
# ✅ Uses: normalize_text(), VEHICLE_REGEX from utils.py
# ----------------------------------

def validate_car_number(number: str) -> bool:
    """Validates using normalize_text from utils.py + VEHICLE_REGEX from utils.py."""
    cleaned = normalize_text(number)
    return bool(VEHICLE_REGEX.match(cleaned))


def clean_car_number(number: str) -> str:
    """Cleans using normalize_text from utils.py."""
    return normalize_text(number)


# ----------------------------------
# Preview Row Builder
# ✅ Uses: vehicle_pattern_ratio(), matches_vehicle_pattern(),
#          normalize_text(), VEHICLE_REGEX from utils.py
# ----------------------------------

def _build_preview_rows(df: pd.DataFrame, vehicle_col: str | None) -> list:
    """
    Build preview rows from the DataFrame using the detected vehicle column.
    Falls back to vehicle_pattern_ratio() from utils.py to find columns.
    """
    rows = []

    if vehicle_col and vehicle_col in df.columns:
        for _, row_data in df.iterrows():
            raw = str(row_data[vehicle_col]).strip()
            cleaned = clean_car_number(raw)
            is_valid = validate_car_number(cleaned)
            rows.append({
                "car_number": cleaned if is_valid else raw,
                "is_valid": is_valid,
                "error": None if is_valid else "Invalid registration number format",
            })
        return rows

    # Fallback: use vehicle_pattern_ratio from utils.py to find best column
    for col in df.columns:
        series = df[col].dropna()
        if series.empty:
            continue
        ratio = vehicle_pattern_ratio(series)
        if ratio > 0.3:
            for _, row_data in df.iterrows():
                raw = str(row_data[col]).strip()
                cleaned = clean_car_number(raw)
                is_valid = validate_car_number(cleaned)
                rows.append({
                    "car_number": cleaned if is_valid else raw,
                    "is_valid": is_valid,
                    "error": None if is_valid else "Invalid registration number format",
                })
            return rows

    # No vehicle column found — return first column values
    if len(df.columns) > 0:
        first_col = df.columns[0]
        for _, row_data in df.iterrows():
            raw = str(row_data[first_col]).strip()
            cleaned = clean_car_number(raw)
            is_valid = validate_car_number(cleaned)
            rows.append({
                "car_number": cleaned if is_valid else raw,
                "is_valid": is_valid,
                "error": None if is_valid else "Invalid registration number format",
            })

    return rows


# ----------------------------------
# Empty Result Template
# ----------------------------------

def _empty_result(header_present=False, total_rows=0, preview=None):
    """Returns a standardized empty result dict."""
    preview = preview or []
    valid_rows = sum(1 for r in preview if r["is_valid"])
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
# ✅ Uses: normalize_text(), VEHICLE_REGEX from utils.py
# ----------------------------------

def _parse_pdf(content: bytes) -> dict:
    """
    Parse PDF files using pdfplumber.
    Extracts car numbers via VEHICLE_REGEX from utils.py.
    """
    try:
        import pdfplumber
    except ImportError:
        return _empty_result()

    numbers = []
    with pdfplumber.open(io.BytesIO(content)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            # Use VEHICLE_REGEX from utils.py for pattern matching
            found = VEHICLE_REGEX.findall(text)
            numbers.extend(found)

    preview = []
    for raw in numbers:
        cleaned = clean_car_number(raw)
        is_valid = validate_car_number(cleaned)
        preview.append({
            "car_number": cleaned if is_valid else raw.strip(),
            "is_valid": is_valid,
            "error": None if is_valid else "Invalid registration number format",
        })

    return _empty_result(
        header_present=False,
        total_rows=len(preview),
        preview=preview,
    )


# ----------------------------------
# Core Engine (file-path based)
# ✅ Uses: auto_header_detect, HEADER_WEIGHT_WITH_HEADER,
#          HEADER_WEIGHT_WITHOUT_HEADER from utils.py
# ✅ Uses: all 4 detectors from detectors.py
# ----------------------------------

def run_detection(file_path: str) -> dict:
    """
    Reads a CSV/Excel file, auto-detects headers,
    runs all detectors on every column, and returns
    the best field mapping with confidence scores.
    """
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

    # ✅ auto_header_detect from utils.py
    df, header_present = auto_header_detect(file_path)
    # ✅ HEADER_WEIGHT constants from utils.py
    header_weight = HEADER_WEIGHT_WITH_HEADER if header_present else HEADER_WEIGHT_WITHOUT_HEADER

    all_results = []

    for col in df.columns:
        series = df[col]
        # ✅ All 4 detectors from detectors.py
        for detector in DETECTORS:
            result = detector(col, series, header_weight=header_weight)
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
# parse_upload — Main entry point for backend
#
# Full pipeline: main.py → engine.py → detectors.py → utils.py → quality.py
#
# Every function below is imported from uploade_parser modules.
# No duplicate logic — all reused from utils/detectors/engine/quality.
# ----------------------------------

def parse_upload(file_content: bytes, filename: str) -> dict:
    """
    Main entry point for the backend upload flow.
    Processes file bytes through the complete uploade_parser pipeline.
    """
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""

    # --- Step 0: Validate file type ---
    if ext not in ("pdf", "csv", "xlsx", "xls"):
        raise ValueError(f"Unsupported file type: .{ext}")

    # --- Step 1: PDF extraction ---
    if ext == "pdf":
        return _parse_pdf(file_content)

    # --- Step 2: Read bytes into stream for CSV/XLSX ---
    stream = io.BytesIO(file_content)
    stream.name = filename

    # Step 2a: Auto header detection & basic cleaning
    df, header_present = auto_header_detect(stream)
    if df.empty:
        return {
            "total_rows": 0,
            "preview": [],
            "header_present": header_present,
            "metadata": {},
            "quality": {},
            "schema": {},
        }

    # Step 2a-fix: When no header detected, try promoting first row as header.
    # Only promote if first row looks like actual header text (not data like car numbers).
    # Run detection on both variants; keep whichever yields more detected fields.
    if not header_present and len(df) > 1:
        first_row = df.iloc[0]
        candidate_cols = [
            str(v).strip() if pd.notnull(v) and str(v).strip() else f"col_{i}"
            for i, v in enumerate(first_row)
        ]
        # Only promote if first row has non-numeric, non-empty, non-vehicle-number text values
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

            result_no_header = run_detection_v2(df, header_present=False)
            result_with_header = run_detection_v2(df_with_header, header_present=True)

            if len(result_with_header["schema"]) > len(result_no_header["schema"]):
                df = df_with_header
                header_present = True

    # Step 2b: Run detection engine for field detection
    # ✅ Use HEADER_WEIGHT constants from utils.py
    header_weight = HEADER_WEIGHT_WITH_HEADER if header_present else HEADER_WEIGHT_WITHOUT_HEADER

    detection_result = run_detection_v2(df, header_present=header_present)
    schema = detection_result["schema"]
    metadata = detection_result["metadata"]

    # Step 3: Run quality checks
    # ✅ run_quality_check from quality.py — null + duplicate analysis per field
    # ✅ run_cross_column_quality_check from quality.py — cross-column duplicates
    quality = run_quality_check(df, schema)
    cross_column_quality = run_cross_column_quality_check(df, schema)

    # Step 4: Normalize & build preview
    #
    # 4a. Normalize dates in detected expiry_date columns
    # ✅ parse_dates from utils.py — uses DATE_FORMATS (MM-DD-YYYY primary)
    for field_name, info in schema.items():
        if field_name.startswith("expiry_date"):
            col_name = info["column"]
            if col_name in df.columns:
                parsed = parse_dates(df[col_name])
                if not parsed.empty:
                    df[col_name] = df[col_name].copy()
                    for idx in parsed.index:
                        df.at[idx, col_name] = parsed[idx].strftime("%Y-%m-%d")

    # 4b. Normalize NCB values in detected ncb_percentage columns
    # ✅ ncb_slab_ratio, ncb_decimal_ratio from utils.py
    # ✅ NCB_VALID_SLABS, NCB_VALID_SLABS_DECIMAL constants from utils.py
    # Handles: integer (20), percentage (20%), decimal (0.20)
    for field_name, info in schema.items():
        if field_name.startswith("ncb_percentage"):
            col_name = info["column"]
            if col_name in df.columns:
                df[col_name] = df[col_name].apply(_normalize_ncb_value)

    # 4c. Build preview rows from detected vehicle column
    # ✅ vehicle_pattern_ratio from utils.py — fallback column detection
    # ✅ matches_vehicle_pattern from utils.py — via validate_car_number
    # ✅ normalize_text from utils.py — via clean_car_number
    # ✅ VEHICLE_REGEX from utils.py — via validate_car_number
    vehicle_col = None
    for field_name, info in schema.items():
        if field_name.startswith("vehicle_registration_number"):
            vehicle_col = info["column"]
            break

    preview = _build_preview_rows(df, vehicle_col)
    valid_rows = sum(1 for r in preview if r["is_valid"])
    invalid_rows = len(preview) - valid_rows

    # 4d. Compute additional field-level stats using utils.py functions
    # ✅ state_code_ratio — for vehicle columns
    # ✅ structure_ratio  — for vehicle columns
    # ✅ uniqueness_ratio — for all detected columns
    field_stats = {}
    for field_name, info in schema.items():
        col_name = info["column"]
        if col_name not in df.columns:
            continue
        series = df[col_name]

        stats = {
            "uniqueness": round(uniqueness_ratio(series), 4),
        }

        if field_name.startswith("vehicle_registration_number"):
            stats["state_code_ratio"] = round(state_code_ratio(series), 4)
            stats["structure_ratio"] = round(structure_ratio(series), 4)
            stats["vehicle_pattern_ratio"] = round(vehicle_pattern_ratio(series), 4)
            stats["negative_penalty"] = negative_penalty(series)

        if field_name.startswith("expiry_date"):
            parsed = parse_dates(series)
            if not parsed.empty:
                stats["future_ratio"] = round(future_ratio(parsed), 4)
                stats["recent_ratio"] = round(recent_ratio(parsed), 4)
                stats["old_ratio"] = round(old_ratio(parsed), 4)

        if field_name.startswith("ncb_percentage"):
            stats["ncb_slab_ratio"] = round(ncb_slab_ratio(series), 4)
            stats["ncb_decimal_ratio"] = round(ncb_decimal_ratio(series), 4)
            stats["numeric_range_score"] = round(numeric_range_score(
                series.astype(str).str.replace("%", "", regex=False).str.strip(), 0, 100
            ), 4)

        if field_name.startswith("claim_status"):
            stats["binary_ratio"] = round(binary_ratio(series, CLAIM_VALID_VALUES), 4)

        field_stats[field_name] = stats

    return {
        "header_present": header_present,
        "schema": schema,
        "metadata": metadata,
        "quality": quality,
        "cross_column_quality": cross_column_quality,
        "field_stats": field_stats,
        "preview": preview,
        "total_rows": len(df),
        "valid_rows": valid_rows,
        "invalid_rows": invalid_rows,
        "df": df,
    }


# ----------------------------------
# NCB Normalizer
# ✅ Uses: NCB_VALID_SLABS, NCB_VALID_SLABS_DECIMAL from utils.py
# Handles all 3 forms: integer (20), percentage (20%), decimal (0.20)
# ----------------------------------

def _normalize_ncb_value(value):
    """
    Normalize a single NCB cell value to standard percentage integer string.
    "20%"  → "20"
    "20"   → "20"
    "0.20" → "20"
    0.45   → "45"
    """
    if pd.isna(value) or str(value).strip() == "":
        return value

    s = str(value).strip().replace("%", "")
    try:
        num = float(s)
    except (ValueError, TypeError):
        return value

    # Decimal form (Excel): 0.20 → 20
    if 0 < num < 1 and num in NCB_VALID_SLABS_DECIMAL:
        return str(int(round(num * 100)))

    # Integer/percentage form: 20 → "20"
    if num in NCB_VALID_SLABS:
        return str(int(num))

    return value


# ----------------------------------
# Output Printer
# ----------------------------------

def print_results(output: dict):
    """Pretty-prints detection results to the terminal."""

    print("\n" + "=" * 55)
    print(f"  Header Detected : {'YES' if output['header_present'] else 'NO (headerless mode)'}")
    print("=" * 55)

    mappings = output["mappings"]

    if not mappings:
        print("\n  ⚠  No fields detected above confidence threshold.\n")
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
# Entry Point
# ✅ Accepts file path via interactive input()
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
            print("  ⚠  No path entered. Please try again.\n")
            continue

        try:
            output = run_detection(file_path)
            print_results(output)
        except FileNotFoundError as e:
            print(f"\n  ❌  {e}")
            print(f"  📂  Current directory : {os.getcwd()}")
            print(f"  💡  Full path checked : {os.path.abspath(file_path)}\n")
        except ValueError as e:
            print(f"\n  ❌  {e}\n")
        except Exception as e:
            print(f"\n  ❌  Unexpected error: {e}\n")

        # Ask whether to process another file
        try:
            again = input("  Process another file? (y/n): ").strip().lower()
        except (KeyboardInterrupt, EOFError):
            print("\n\n  Exiting. Goodbye!\n")
            sys.exit(0)

        if again not in ("y", "yes"):
            print("\n  Exiting. Goodbye!\n")
            break
