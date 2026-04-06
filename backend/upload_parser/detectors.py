from .utils import (
    vehicle_pattern_ratio,
    uniqueness_ratio,
    header_match_score,
    state_code_ratio,
    structure_ratio,
    negative_penalty,
    numeric_range_score,
    parse_dates,
    future_ratio,
    recent_ratio,
    old_ratio,
    binary_ratio,
    ncb_slab_ratio,           # ✅ NEW — primary NCB signal
    ncb_decimal_ratio,        # ✅ NEW — bonus for Excel decimal form
    VEHICLE_HEADER_ALIASES,
    EXPIRY_HEADER_ALIASES,
    CLAIM_HEADER_ALIASES,
    CLAIM_VALID_VALUES,
    NCB_VALID_SLABS,
    NCB_VALID_SLABS_DECIMAL,
    MIN_ROWS_FOR_UNIQUENESS_PENALTY,
    HEADER_WEIGHT_WITH_HEADER,
)

import pandas as pd


# --------------------------------------------------
# Vehicle Registration Detection
# --------------------------------------------------
def detect_vehicle_registration(column_name, series, header_weight=HEADER_WEIGHT_WITH_HEADER):
    """
    Detects vehicle registration number columns.
    header_weight: 1.0 when header present, 0.1 when headerless (set by engine).
    Max score with header    : 40 + 30 + 15 + 10 + 15 = 110 → capped at 100
    Max score without header : ~4 + 30 + 15 + 10 + 15 = ~74
    """
    column_name = str(column_name)
    evidence    = []
    reasons     = []
    score       = 0

    # Header score scaled by header_weight — near-zero when headerless
    header_score_val = header_match_score(column_name, VEHICLE_HEADER_ALIASES)
    score += header_score_val * 40 * header_weight
    if header_score_val:
        evidence.append("header_match")
    else:
        reasons.append("header_mismatch")

    pattern_ratio = vehicle_pattern_ratio(series)
    score += pattern_ratio * 30
    if pattern_ratio > 0.7:
        evidence.append("regex_pattern_high")
    else:
        reasons.append("low_regex_match")

    state_ratio = state_code_ratio(series)
    score += state_ratio * 15
    if state_ratio > 0.7:
        evidence.append("valid_state_code")
    else:
        reasons.append("state_code_mismatch")

    struct_ratio = structure_ratio(series)
    score += struct_ratio * 10
    if struct_ratio > 0.8:
        evidence.append("alphanumeric_structure")
    else:
        reasons.append("structure_low")

    uniq_ratio = uniqueness_ratio(series)
    score += uniq_ratio * 15
    if uniq_ratio > 0.9:
        evidence.append("high_uniqueness")
    else:
        reasons.append("low_uniqueness")

    if negative_penalty(series):
        score -= 40
        evidence.append("numeric_penalty")
        reasons.append("numeric_penalty_triggered")

    confidence = round(max(min(score, 100), 0), 2)
    reason     = None if confidence >= 50 else ", ".join(reasons)

    return {
        "field":    "vehicle_registration_number",
        "score":    confidence,
        "evidence": evidence,
        "reason":   reason,
    }


# --------------------------------------------------
# NCB Detection
# ✅ UPDATED: slab-based detection replaces range + multiples-of-5
#
# Old approach problems:
#   - numeric_range_score(0–100) matched ANY 0–100 column (age, score, %)
#   - multiples-of-5 matched years, round numbers, scores
#   - False positives on completely unrelated numeric columns
#
# New approach:
#   - ncb_slab_ratio()   → PRIMARY: exact match against {0,20,25,35,45,50}
#                          AND decimal form {0.20,0.25,0.35,0.45,0.50}
#   - ncb_decimal_ratio() → BONUS: fires only for Excel-exported decimal form
#   - numeric_range_score → WEAK FALLBACK: only when slab+decimal both low
#
# Scoring breakdown (max ~100 pts):
#   Header match     → 0–40 pts
#   NCB slab ratio   → 0–45 pts   ← PRIMARY
#   Decimal bonus    → 0–10 pts   ← BONUS (Excel form)
#   Range fallback   → 0–5  pts   ← WEAK FALLBACK only
#   Uniqueness pen.  → −20 pts    ← only on large datasets
#
# Max score with header    : 40 + 45 + 10 + 5 = 100
# Max score without header : ~4 + 45 + 10 + 5 = ~64 ✅ crosses threshold
# --------------------------------------------------
def detect_ncb(column_name, series, header_weight=HEADER_WEIGHT_WITH_HEADER):
    """
    Detects NCB (No Claim Bonus) percentage columns.
    header_weight: 1.0 when header present, 0.1 when headerless (set by engine).

    Handles all three value formats:
        "20%"  → strip % → 20  → matches NCB_VALID_SLABS        ✅
        "20"   → numeric → 20  → matches NCB_VALID_SLABS        ✅
        "0.20" → numeric → 0.20 → matches NCB_VALID_SLABS_DECIMAL ✅
        0.45   → float   → 0.45 → matches NCB_VALID_SLABS_DECIMAL ✅
    """
    column_name = str(column_name)
    evidence    = []
    reasons     = []
    score       = 0

    # ----------------------------------
    # Header Score (0–40 pts)
    # ----------------------------------
    NCB_HEADER_ALIASES = [
        "ncb",
        "no claim bonus",
        "no claim benefit",
        "ncb %",
        "ncb percent",
        "bonus",
        "discount",
        "ncb discount",
    ]
    header_score_val = header_match_score(column_name, NCB_HEADER_ALIASES)
    score += header_score_val * 40 * header_weight
    if header_score_val:
        evidence.append("header_match")
    else:
        reasons.append("header_mismatch")

    # ----------------------------------
    # NCB Slab Ratio (0–45 pts) — PRIMARY signal
    # Handles "20%", "20", "0.20", 0.45 — all forms
    # Valid slabs: {0, 20, 25, 35, 45, 50} or {0.0, 0.20, 0.25, 0.35, 0.45, 0.50}
    # ----------------------------------
    slab_score = ncb_slab_ratio(series)
    slab_pts   = slab_score * 45
    score     += slab_pts
    if slab_score >= 0.5:
        evidence.append("ncb_slab_match")
    else:
        reasons.append("ncb_slab_mismatch")

    # ----------------------------------
    # Decimal Bonus (0–10 pts)
    # Extra confidence for pure Excel decimal form: 0.20, 0.45, etc.
    # ncb_decimal_ratio() only returns > 0 for the decimal form specifically
    # ----------------------------------
    dec_score = ncb_decimal_ratio(series)
    dec_pts   = dec_score * 10
    score    += dec_pts
    if dec_score >= 0.5:
        evidence.append("ncb_decimal_form")

    # ----------------------------------
    # Numeric Range Fallback (0–5 pts)
    # Very weak signal — only fires when slab AND decimal both fail
    # Prevents over-scoring unrelated numeric columns
    # ----------------------------------
    if slab_score < 0.3 and dec_score < 0.3:
        cleaned     = series.astype(str).str.replace("%", "", regex=False).str.strip()
        range_score = numeric_range_score(cleaned, 0, 100)
        score      += range_score * 5
        if range_score >= 0.7:
            evidence.append("numeric_range_fallback")
        else:
            reasons.append("range_fallback_weak")

    # ----------------------------------
    # Uniqueness Penalty (−20 pts)
    # NCB values repeat heavily (everyone has 0%, 20%, 25% etc.)
    # If every single value is unique on a large dataset → unlikely to be NCB
    # ----------------------------------
    non_null = series.dropna()
    uniq     = uniqueness_ratio(series)
    if uniq > 0.95 and len(non_null) > MIN_ROWS_FOR_UNIQUENESS_PENALTY:
        score -= 20
        evidence.append("too_unique_penalty")
        reasons.append("too_unique_penalty_triggered")

    confidence = round(max(min(score, 100), 0), 2)
    reason     = None if confidence >= 50 else ", ".join(reasons)

    return {
        "field":    "ncb_percentage",
        "score":    confidence,
        "evidence": evidence,
        "reason":   reason,
    }


# --------------------------------------------------
# Expiry Date Detection
# --------------------------------------------------
def detect_expiry_date(column_name, series, header_weight=HEADER_WEIGHT_WITH_HEADER):
    """
    Detects policy expiry date columns.
    header_weight: 1.0 when header present, 0.1 when headerless (set by engine).
    Max score with header    : 50 + 35 + 15 + 25 + 15 = 140 → capped at 100
    Max score without header : ~5 + 35 + 15 + 25 + 15 = ~95
    """
    column_name = str(column_name)
    evidence    = []
    reasons     = []
    score       = 0

    parsed_dates = parse_dates(series)
    if parsed_dates.empty:
        return {
            "field":    "expiry_date",
            "score":    0,
            "evidence": [],
            "reason":   "no valid dates found",
        }

    # Header score scaled by header_weight — near-zero when headerless
    header_score_val = header_match_score(column_name, EXPIRY_HEADER_ALIASES)
    score += header_score_val * 50 * header_weight
    if header_score_val:
        evidence.append("strong_header_match")
    else:
        reasons.append("header_mismatch")

    non_null_count = len(series.dropna())
    valid_ratio    = len(parsed_dates) / non_null_count if non_null_count else 0

    if valid_ratio < 0.6:
        return {
            "field":    "expiry_date",
            "score":    0,
            "evidence": evidence,
            "reason":   "insufficient valid dates",
        }

    # Boosted weight: 20 → 35 to carry more signal in headerless mode
    score += valid_ratio * 35
    evidence.append("valid_date_format")

    # Bonus when nearly all rows parse as valid dates
    if valid_ratio >= 0.9:
        score += 15
        evidence.append("high_parse_confidence")

    f_ratio = future_ratio(parsed_dates)
    r_ratio = recent_ratio(parsed_dates)
    o_ratio = old_ratio(parsed_dates)

    # Boosted weights: future 20 → 25, recent 10 → 15
    score += f_ratio * 25
    if f_ratio > 0.5:
        evidence.append("future_dominant")
    else:
        reasons.append("future_not_dominant")

    score += r_ratio * 15
    if r_ratio > 0.3:
        evidence.append("recent_expiry_pattern")
    else:
        reasons.append("recent_ratio_low")

    if o_ratio > 0.4:
        score -= 30
        evidence.append("too_old_penalty")
        reasons.append("old_ratio_high")

    confidence = round(max(min(score, 100), 0), 2)
    reason     = None if confidence >= 50 else ", ".join(reasons)

    return {
        "field":    "expiry_date",
        "score":    confidence,
        "evidence": evidence,
        "reason":   reason,
    }


# --------------------------------------------------
# Claim Status Detection
# --------------------------------------------------
def detect_claim_status(column_name, series, header_weight=HEADER_WEIGHT_WITH_HEADER):
    """
    Detects claim status columns (binary Yes/No pattern).
    header_weight: 1.0 when header present, 0.1 when headerless (set by engine).
    Max score with header    : 40 + 50 + 15 = 105 → capped at 100
    Max score without header : ~4 + 50 + 15 = ~69
    """
    column_name = str(column_name)
    evidence    = []
    reasons     = []
    score       = 0

    # Header score scaled by header_weight — near-zero when headerless
    header_score_val = header_match_score(column_name, CLAIM_HEADER_ALIASES)
    score += header_score_val * 40 * header_weight
    if header_score_val:
        evidence.append("header_match")
    else:
        reasons.append("header_mismatch")

    # Boosted weight: 40 → 50 — primary content signal in headerless mode
    match_ratio = binary_ratio(series, CLAIM_VALID_VALUES)
    score += match_ratio * 50
    if match_ratio > 0.8:
        evidence.append("binary_pattern")
    else:
        reasons.append("binary_pattern_low")

    non_null = series.dropna()
    n_unique = non_null.nunique()
    uniq     = uniqueness_ratio(series)

    # Cardinality check: Yes/No always has ≤ 3 unique values
    if n_unique <= 3:
        score += 15
        evidence.append("low_cardinality_boost")
    elif uniq < 0.3:
        score += 10
        evidence.append("low_unique_count")
    else:
        reasons.append("high_uniqueness_no_boost")

    confidence = round(max(min(score, 100), 0), 2)
    reason     = None if confidence >= 50 else ", ".join(reasons)

    return {
        "field":    "claim_status",
        "score":    confidence,
        "evidence": evidence,
        "reason":   reason,
    }
