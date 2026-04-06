import re
import io
import pandas as pd
from rapidfuzz import fuzz


# ----------------------------------
# Constants
# ----------------------------------

VALID_STATE_CODES = {
    "TN", "KA", "MH", "DL", "AP", "TS", "KL", "GJ",
    "RJ", "UP", "MP", "WB", "HR", "PB", "OD", "BR"
}

VEHICLE_HEADER_ALIASES = [
    "reg no",
    "registration",
    "registration no",
    "vehicle no",
    "vehicle number",
    "car number",
    "regn no",
    "rc number"
]

EXPIRY_HEADER_ALIASES = [
    "expiry",
    "expiration",
    "exp date",
    "policy end",
    "valid till",
    "valid upto",
    "due date",
    "renewal date"
]

CLAIM_HEADER_ALIASES = [
    "claim",
    "claimed",
    "claim status",
    "is claimed",
    "has claim"
]

CLAIM_VALID_VALUES = {
    "yes", "no",
    "y", "n",
    "true", "false",
    "claimed", "not claimed",
    "1", "0"
}

# Flattened alias list used by auto_header_detect
ALL_HEADER_ALIASES = [
    a.lower() for a in (
        VEHICLE_HEADER_ALIASES + EXPIRY_HEADER_ALIASES + CLAIM_HEADER_ALIASES
    )
]

# Known data values that must NEVER be treated as headers.
ALL_KNOWN_DATA_VALUES = CLAIM_VALID_VALUES | {
    "no claim",
    "not claimed",
    "yes claimed",
}

# Minimum row count before uniqueness penalties are applied
MIN_ROWS_FOR_UNIQUENESS_PENALTY = 10

# Global header weight constants
HEADER_WEIGHT_WITH_HEADER    = 1.0
HEADER_WEIGHT_WITHOUT_HEADER = 0.1

# Fuzzy matching thresholds
HEADER_DETECT_FUZZY_THRESHOLD = 85
HEADER_SCORE_FUZZY_THRESHOLD  = 80

# Minimum fraction of columns that must show type mismatch
HOMOGENEITY_COLUMN_THRESHOLD = 0.4

# Ordered list of date formats — MM-DD-YYYY is primary
DATE_FORMATS = [
    # MM-DD-YYYY variants (primary)
    "%m-%d-%Y",
    "%m/%d/%Y",
    "%m.%d.%Y",
    "%m-%d-%y",
    "%m/%d/%y",

    # YYYY-MM-DD variants (ISO)
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%Y.%m.%d",

    # DD-MM-YYYY variants
    "%d-%m-%Y",
    "%d/%m/%Y",
    "%d.%m.%Y",
    "%d-%m-%y",
    "%d/%m/%y",

    # Month name formats
    "%b %d %Y",     # Jan 01 2026
    "%d %b %Y",     # 01 Jan 2026
    "%B %d %Y",     # January 01 2026
    "%d %B %Y",     # 01 January 2026
    "%b-%d-%Y",     # Jan-01-2026
    "%d-%b-%Y",     # 01-Jan-2026
]


# ----------------------------------
# NCB Constants
# ✅ Valid NCB slab values as per IRDAI motor insurance guidelines
#
# Percentage / integer form:
#   0%  → new policy or claim made this year
#   20% → 1 claim-free year
#   25% → 2 claim-free years
#   35% → 3 claim-free years
#   45% → 4 claim-free years
#   50% → 5+ claim-free years (maximum cap)
#
# Decimal form (how Excel/openpyxl stores "20%" internally):
#   "20%" in Excel cell → openpyxl reads as 0.2 float
#   Multiply by 100 to recover the slab value
# ----------------------------------

NCB_VALID_SLABS = {0, 20, 25, 35, 45, 50}

NCB_VALID_SLABS_DECIMAL = {0.0, 0.20, 0.25, 0.35, 0.45, 0.50}


# ----------------------------------
# General Helpers
# ----------------------------------

def safe_ratio(numerator, denominator):
    """Returns numerator/denominator, or 0 if denominator is zero."""
    return numerator / denominator if denominator else 0


def normalize_text(value):
    """Strips, uppercases, removes dashes and spaces from a value."""
    if pd.isna(value):
        return ""
    return str(value).strip().upper().replace("-", "").replace(" ", "")


# ----------------------------------
# Header Match Score
# ✅ Hybrid: substring fast path + fuzzy partial_ratio fallback
# ----------------------------------

def header_match_score(column_name, aliases):
    """
    Returns a score 0.0–1.0 indicating how strongly the column name
    matches any of the provided aliases.

    Strategy:
        1. Normalize: lowercase + strip + underscores → spaces.
        2. Exact substring check (fast path) → 1.0 immediately.
        3. Fuzzy partial_ratio fallback for typos/abbreviations.
        4. Returns 0.0 if neither passes.

    Examples:
        "registration no"  → substring hit           → 1.0
        "registraton_no"   → fuzzy partial_ratio=94  → 0.94
        "exp_dt"           → fuzzy partial_ratio=88  → 0.88
        "clm_status"       → fuzzy partial_ratio=91  → 0.91
        "random_col"       → fuzzy score=22          → 0.0
    """
    if not isinstance(column_name, str):
        return 0.0

    col = column_name.lower().strip().replace("_", " ")

    # Step 1: Fast exact substring check
    for alias in aliases:
        if alias in col:
            return 1.0

    # Step 2: Fuzzy partial_ratio fallback
    best_score = max(fuzz.partial_ratio(alias, col) for alias in aliases)
    if best_score >= HEADER_SCORE_FUZZY_THRESHOLD:
        return round(best_score / 100, 2)

    return 0.0


# ----------------------------------
# Vehicle Pattern Helpers
# ----------------------------------

VEHICLE_REGEX = re.compile(r'^[A-Z]{2}\d{1,2}[A-Z]{0,2}\d{4}$')


def matches_vehicle_pattern(value):
    """Returns True if value matches the Indian vehicle registration regex."""
    return bool(VEHICLE_REGEX.match(normalize_text(value)))


def vehicle_pattern_ratio(series):
    """Fraction of non-null values matching the vehicle registration pattern."""
    non_null = series.dropna()
    if non_null.empty:
        return 0
    return safe_ratio(non_null.apply(matches_vehicle_pattern).sum(), len(non_null))


# ----------------------------------
# State Code Ratio
# ----------------------------------

def state_code_ratio(series):
    """Fraction of non-null values whose first two characters are valid state codes."""
    non_null = series.dropna()
    if non_null.empty:
        return 0
    return safe_ratio(
        non_null.apply(lambda v: normalize_text(v)[:2] in VALID_STATE_CODES).sum(),
        len(non_null)
    )


# ----------------------------------
# Uniqueness Ratio
# ----------------------------------

def uniqueness_ratio(series):
    """Fraction of unique values among non-null entries."""
    non_null = series.dropna()
    if non_null.empty:
        return 0
    return safe_ratio(non_null.nunique(), len(non_null))


# ----------------------------------
# Structure Ratio
# ----------------------------------

def structure_ratio(series):
    """Fraction of values containing both alphabetic and numeric characters."""
    non_null = series.dropna()
    if non_null.empty:
        return 0

    def has_alpha_and_digit(value):
        v = normalize_text(value)
        return any(c.isalpha() for c in v) and any(c.isdigit() for c in v)

    return safe_ratio(non_null.apply(has_alpha_and_digit).sum(), len(non_null))


# ----------------------------------
# Negative Penalty
# ----------------------------------

def negative_penalty(series):
    """
    Returns 1.0 if the column is predominantly pure numeric.
    Strips '%' first so NCB values like '20%' don't falsely trigger this.
    """
    non_null = series.dropna()
    if non_null.empty:
        return 0
    cleaned = non_null.astype(str).str.replace("%", "", regex=False).str.strip()
    numeric = pd.to_numeric(cleaned, errors="coerce")
    numeric_ratio = numeric.notna().sum() / len(non_null)
    return 1.0 if numeric_ratio > 0.8 else 0.0


# ----------------------------------
# Date Parsing
# ----------------------------------

def _try_parse_single_format(series, fmt):
    """Attempt to parse a series with one explicit format. Returns parsed Series."""
    return pd.to_datetime(series, format=fmt, errors="coerce")


def parse_dates(series):
    """
    Parses a series as datetime values using an explicit ordered format list.
    MM-DD-YYYY is tried first — it is the primary expected format.
    Returns a Series of successfully parsed (non-NaT) Timestamps.
    """
    non_null = series.dropna()
    if non_null.empty:
        return pd.Series([], dtype="datetime64[ns]")

    result = pd.Series(pd.NaT, index=non_null.index, dtype="datetime64[ns]")

    for fmt in DATE_FORMATS:
        unparsed_mask = result.isna()
        if not unparsed_mask.any():
            break
        parsed = _try_parse_single_format(non_null[unparsed_mask], fmt)
        result[unparsed_mask] = parsed

    return result.dropna()


# ----------------------------------
# Date Distribution Helpers
# ----------------------------------

def future_ratio(parsed_dates):
    """Fraction of dates that are in the future."""
    if parsed_dates.empty:
        return 0
    today = pd.Timestamp.today().normalize()
    return safe_ratio((parsed_dates > today).sum(), len(parsed_dates))


def recent_ratio(parsed_dates, years=1):
    """Fraction of dates within the last N years up to today."""
    if parsed_dates.empty:
        return 0
    today = pd.Timestamp.today().normalize()
    start = today - pd.Timedelta(days=365 * years)
    return safe_ratio(
        ((parsed_dates >= start) & (parsed_dates <= today)).sum(),
        len(parsed_dates)
    )


def old_ratio(parsed_dates, years=5):
    """Fraction of dates older than N years."""
    if parsed_dates.empty:
        return 0
    today = pd.Timestamp.today().normalize()
    cutoff = today - pd.Timedelta(days=365 * years)
    return safe_ratio((parsed_dates < cutoff).sum(), len(parsed_dates))


# ----------------------------------
# Numeric Range Score
# ----------------------------------

def numeric_range_score(series, min_val=0, max_val=100):
    """
    Fraction of values that fall within [min_val, max_val].
    Strips '%' before conversion so values like '20%' are handled correctly.
    """
    non_null = series.dropna()
    if non_null.empty:
        return 0
    cleaned = non_null.astype(str).str.replace("%", "", regex=False).str.strip()
    numeric = pd.to_numeric(cleaned, errors="coerce")
    valid = numeric[(numeric >= min_val) & (numeric <= max_val)]
    return safe_ratio(len(valid), len(non_null))


# ----------------------------------
# Binary Pattern Helper
# ----------------------------------

def binary_ratio(series, valid_values):
    """Fraction of values (normalized to lowercase) that match a set of valid values."""
    normalized = series.astype(str).str.lower().str.strip()
    return safe_ratio(normalized.isin(valid_values).sum(), len(normalized))


# ----------------------------------
# NCB Slab Helpers
# ✅ Handles BOTH integer/percentage form AND decimal form
#
# Integer/% form : "20%", "25", "35.0"  → cleaned → 20, 25, 35 → NCB_VALID_SLABS
# Decimal form   : "0.20", 0.45         → 0 < v < 1            → NCB_VALID_SLABS_DECIMAL
# ----------------------------------

def _is_ncb_slab(value):
    """
    Returns True if a single numeric value matches a valid NCB slab
    in either percentage/integer OR decimal form.

    Rules:
        Form 1 — Integer/percentage: value in {0, 20, 25, 35, 45, 50}
        Form 2 — Decimal           : 0 < value < 1
                                     AND value in {0.20, 0.25, 0.35, 0.45, 0.50}

    Why exclude 0 and 1 from decimal check:
        0   already covered by Form 1 (NCB_VALID_SLABS contains 0)
        1.0 is NOT a valid NCB decimal (100% NCB discount doesn't exist)

    Examples:
        20    → Form 1 match  → True  ✅
        0.20  → Form 2 match  → True  ✅  (Excel stores "20%" as 0.2)
        0.45  → Form 2 match  → True  ✅  (Excel stores "45%" as 0.45)
        0.50  → Form 2 match  → True  ✅
        0.30  → 0 < 0.30 < 1, but NOT in NCB_VALID_SLABS_DECIMAL → False ❌
        10    → not in NCB_VALID_SLABS                            → False ❌
        0.10  → not in NCB_VALID_SLABS_DECIMAL                   → False ❌
        100   → not in NCB_VALID_SLABS                            → False ❌
    """
    # Form 1: integer / percentage form
    if value in NCB_VALID_SLABS:
        return True

    # Form 2: decimal form — strictly between 0 and 1
    if 0 < value < 1 and value in NCB_VALID_SLABS_DECIMAL:
        return True

    return False


def ncb_slab_ratio(series):
    """
    Fraction of non-null values that match a valid NCB slab.

    Handles THREE input formats transparently:
        "20%"  → strip % → 20.0  → Form 1 match  ✅
        "20"   → numeric → 20.0  → Form 1 match  ✅
        "0.20" → numeric → 0.20  → Form 2 match  ✅
        0.45   → numeric → 0.45  → Form 2 match  ✅

    Processing pipeline:
        1. Drop nulls
        2. Strip "%" sign
        3. Convert to float via pd.to_numeric
        4. Apply _is_ncb_slab() per value
        5. Return match_count / total_non_null

    Examples:
        ["20%", "25%", "35%", "50%"]    → 4/4 = 1.0  ✅ all match
        ["20",  "25",  "35",  "45"]     → 4/4 = 1.0  ✅ all match
        ["0.20","0.25","0.35","0.50"]   → 4/4 = 1.0  ✅ all match (Excel)
        [0.20,  0.45,  0.25,  0.35]    → 4/4 = 1.0  ✅ all match (Excel float)
        ["10",  "20",  "30",  "40"]    → 1/4 = 0.25 ❌ only 20 is valid
        ["0.1", "0.3", "0.4", "0.6"]   → 0/4 = 0.0  ❌ none are valid slabs
    """
    non_null = series.dropna()
    if non_null.empty:
        return 0

    cleaned = non_null.astype(str).str.replace("%", "", regex=False).str.strip()
    numeric = pd.to_numeric(cleaned, errors="coerce")
    valid   = numeric.dropna()

    if valid.empty:
        return 0

    slab_matches = valid.apply(_is_ncb_slab).sum()
    return safe_ratio(slab_matches, len(non_null))


def ncb_decimal_ratio(series):
    """
    Fraction of non-null values that are in PURE decimal NCB form (0.20–0.50).

    Used as a standalone bonus signal in detect_ncb() to give extra weight
    when the column is exclusively in decimal form (Excel export pattern).

    Unlike ncb_slab_ratio() which handles both forms, this function ONLY
    returns > 0 for the decimal form — confirming it is an Excel-exported
    NCB column specifically.

    Examples:
        [0.20, 0.25, 0.35, 0.50]   → 1.0   ✅ pure decimal NCB column
        [0.20, 0.45, 0.25, 0.10]   → 0.75  ⚠️ 3/4 are valid decimal slabs
        [20,   25,   35,   50]     → 0.0   ❌ integer form, not decimal
        [0.1,  0.3,  0.6,  0.9]   → 0.0   ❌ not valid NCB decimal slabs
    """
    non_null = series.dropna()
    if non_null.empty:
        return 0

    cleaned = non_null.astype(str).str.replace("%", "", regex=False).str.strip()
    numeric = pd.to_numeric(cleaned, errors="coerce").dropna()

    if numeric.empty:
        return 0

    decimal_matches = numeric.apply(
        lambda v: 0 < v < 1 and v in NCB_VALID_SLABS_DECIMAL
    ).sum()

    return safe_ratio(decimal_matches, len(non_null))


# ----------------------------------
# Excel Cell Cleaner
# ✅ Neutralizes openpyxl type conversion side effects
# ----------------------------------

def _clean_excel_cell(value):
    """
    Cleans a single Excel cell value after openpyxl type conversion.

    Problems this solves:
        "20%"          → stored as 0.2 float  → "0.2"
        "20"           → stored as 20.0 float → "20.0" → stripped to "20"
        date cell      → datetime object      → "2026-12-31 00:00:00" → "2026-12-31"
        None / NaN     → ""

    Rules:
        1. None/NaN              → ""
        2. "20.0" (int-like)     → "20"    (strip trailing .0)
        3. "2026-12-31 00:00:00" → "2026-12-31"  (truncate midnight datetime)
        4. Everything else       → str(value).strip()
    """
    if pd.isna(value) or value is None:
        return ""
    s = str(value).strip()
    # Strip ".0" from integer-looking floats: "20.0" → "20"
    if s.endswith(".0") and s[:-2].lstrip("-").isdigit():
        return s[:-2]
    # Truncate Excel midnight datetime strings: "2026-12-31 00:00:00" → "2026-12-31"
    if len(s) == 19 and s[10] == " " and s[11:] == "00:00:00":
        return s[:10]
    return s


# ----------------------------------
# Internal File Readers
# ----------------------------------

def _read_raw(source, name):
    """
    Reads file without headers into a fully string DataFrame.
    keep_default_na=False prevents NaN injection into string columns.
    Excel files get post-processed through _clean_excel_cell().
    """
    if name.lower().endswith(".csv"):
        return pd.read_csv(
            source,
            header=None,
            dtype=str,
            keep_default_na=False
        )
    elif name.lower().endswith((".xls", ".xlsx")):
        df = pd.read_excel(
            source,
            header=None,
            dtype=str,
            keep_default_na=False
        )
        for col in df.columns:
            df[col] = df[col].apply(_clean_excel_cell)
        return df
    else:
        raise ValueError(
            f"Unsupported file type '{name}'. Please upload a CSV or Excel file."
        )


def _read_with_header(source, name):
    """
    Reads file with header row into a fully string DataFrame.
    Normalizes column names (strip whitespace).
    Excel files get post-processed through _clean_excel_cell().
    """
    if name.lower().endswith(".csv"):
        df = pd.read_csv(source, dtype=str, keep_default_na=False)
    elif name.lower().endswith((".xls", ".xlsx")):
        df = pd.read_excel(source, dtype=str, keep_default_na=False)
        for col in df.columns:
            df[col] = df[col].apply(_clean_excel_cell)
    else:
        raise ValueError(
            f"Unsupported file type '{name}'. Please upload a CSV or Excel file."
        )
    df.columns = [str(c).strip() for c in df.columns]
    return df


# ----------------------------------
# Auto Header Detection Helpers
# ----------------------------------

def _cell_is_header_like_fuzzy(cell):
    """
    Gate 1 — Fuzzy check.
    Returns True if a cell value looks like a column header alias.

    Three guards applied in order:
        1. Block known data values ("no claim", "yes", "claimed", "0", etc.)
        2. Block cells with >30% digits — data values like "TN01AB1234",
           "12-31-2026", "20%" would otherwise match via partial_ratio.
        3. fuzz.partial_ratio >= HEADER_DETECT_FUZZY_THRESHOLD.

    Examples:
        "claim status"       → not data, 0% digits, partial_ratio=100 → True  ✅
        "Policy Expiry Date" → not data, 0% digits, partial_ratio=100 → True  ✅
        "no claim"           → in ALL_KNOWN_DATA_VALUES               → False ✅
        "TN01AB1234"         → 70% digits                             → False ✅
        "20%"                → 67% digits                             → False ✅
    """
    # Guard 1: block known data values
    if cell in ALL_KNOWN_DATA_VALUES:
        return False

    # Guard 2: block digit-heavy cells
    digit_ratio = sum(c.isdigit() for c in cell) / max(len(cell), 1)
    if digit_ratio > 0.3:
        return False

    # Guard 3: partial_ratio
    best = max(fuzz.partial_ratio(alias, cell) for alias in ALL_HEADER_ALIASES)
    return best >= HEADER_DETECT_FUZZY_THRESHOLD


def _homogeneity_check(df_raw):
    """
    Gate 2 — Column Homogeneity check.
    Returns True if >= HOMOGENEITY_COLUMN_THRESHOLD fraction of columns
    show a type mismatch between the first row and the rest of the data.
    """
    if len(df_raw) < 2:
        return False

    first_row  = df_raw.iloc[0]
    data_rows  = df_raw.iloc[1:]
    n_cols     = len(df_raw.columns)
    mismatches = 0

    for col_idx in range(n_cols):
        cell     = str(first_row.iloc[col_idx]).strip()
        data_col = data_rows.iloc[:, col_idx].astype(str).str.strip()

        data_numeric_ratio = pd.to_numeric(data_col, errors="coerce").notna().mean()
        data_date_ratio    = safe_ratio(
            len(parse_dates(data_col)), max(len(data_col.dropna()), 1)
        )
        data_reg_ratio     = vehicle_pattern_ratio(data_col)

        cell_numeric     = pd.to_numeric(cell, errors="coerce")
        cell_is_numeric  = not pd.isna(cell_numeric)
        cell_is_date     = len(parse_dates(pd.Series([cell]))) > 0
        cell_is_reg      = matches_vehicle_pattern(cell)

        if data_numeric_ratio > 0.7 and not cell_is_numeric:
            mismatches += 1
        elif data_date_ratio > 0.7 and not cell_is_date:
            mismatches += 1
        elif data_reg_ratio > 0.7 and not cell_is_reg:
            mismatches += 1

    return safe_ratio(mismatches, n_cols) >= HOMOGENEITY_COLUMN_THRESHOLD


# ----------------------------------
# V2: Auto Header Detection
# ----------------------------------

def auto_header_detect(file_input):
    """
    Reads a CSV/Excel file and detects whether a header row exists
    using a two-gate combined approach.

    Gate 1 — Fuzzy alias match (fuzz.partial_ratio + digit guard)
    Gate 2 — Column type homogeneity mismatch

    Header declared present if EITHER gate returns True.

    Returns:
        df            : pandas DataFrame ready for detection
        header_present: bool
    """
    is_stream = hasattr(file_input, "read")
    name = file_input.name if is_stream else str(file_input)

    if is_stream:
        raw_bytes = file_input.read()
    else:
        raw_bytes = None

    def _make_source():
        return io.BytesIO(raw_bytes) if is_stream else file_input

    try:
        df_raw = _read_raw(_make_source(), name)
    except ValueError:
        raise
    except Exception as e:
        raise RuntimeError(f"Failed to read file '{name}': {e}") from e

    if df_raw.empty:
        return df_raw, False

    first_row = df_raw.iloc[0].astype(str).str.lower().str.strip()

    fuzzy_says_header       = any(_cell_is_header_like_fuzzy(cell) for cell in first_row)
    homogeneity_says_header = _homogeneity_check(df_raw)
    header_like             = fuzzy_says_header or homogeneity_says_header

    # Safety net: if ANY cell in the first row looks like a vehicle registration
    # number, it's data — not a header row.
    if header_like:
        first_row_raw = df_raw.iloc[0].astype(str).str.strip()
        if any(matches_vehicle_pattern(cell) for cell in first_row_raw):
            header_like = False

    if header_like:
        try:
            df = _read_with_header(_make_source(), name)
        except Exception as e:
            raise RuntimeError(f"Failed to re-read file with headers: {e}") from e
        return df, True
    else:
        df_raw.columns = [f"col_{c}" for c in df_raw.columns]
        return df_raw, False
