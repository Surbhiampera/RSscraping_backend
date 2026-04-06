import re
import pandas as pd
from .detectors import (
    detect_vehicle_registration,
    detect_ncb,
    detect_expiry_date,
    detect_claim_status,
)
from .utils import (
    HEADER_WEIGHT_WITH_HEADER,
    HEADER_WEIGHT_WITHOUT_HEADER,
)


CONFIDENCE_THRESHOLD = 50

DETECTORS = [
    detect_vehicle_registration,
    detect_ncb,
    detect_expiry_date,
    detect_claim_status,
]


# ----------------------------------
# Field Name Assignment Helper
# ✅ Allows multiple columns to map to the same base field type
#    by appending _2, _3, ... suffixes for extra detections
#
# Examples:
#   "vehicle_registration_number"         (first detection)
#   "vehicle_registration_number_2"       (second column same type)
#   "vehicle_registration_number_3"       (third column same type)
# ----------------------------------

def _assign_field_name(base_field, already_assigned):
    """
    Returns a unique field name by appending a numeric suffix
    if the base field name is already taken in already_assigned.

    Args:
        base_field       : original field name from detector ("vehicle_registration_number")
        already_assigned : set of field names already in the schema

    Returns:
        str — unique field name safe to insert into schema
    """
    if base_field not in already_assigned:
        return base_field
    i = 2
    while f"{base_field}_{i}" in already_assigned:
        i += 1
    return f"{base_field}_{i}"


def run_detection_v2(df, header_present=True, threshold=CONFIDENCE_THRESHOLD):
    """
    V2 detection engine with multi-column field support.

    Changes from previous version:
        - Multiple columns scoring above threshold for the SAME field type
          are ALL included in the schema with suffixed names:
          vehicle_registration_number, vehicle_registration_number_2, etc.
        - Previously only the highest-scoring column per field was kept —
          this caused col_1 to be silently dropped when col_0 also matched
          vehicle_registration_number.
        - _assign_field_name() handles suffix generation cleanly.
        - Cross-column quality checks in quality.py use the base field name
          (strip _2, _3) to group and compare these columns.

    Detection flow:
        1. Compute header_weight once globally.
        2. For each column, run all detectors and track best score.
        3. All columns scoring >= threshold are treated as candidates.
        4. Candidates are sorted by score descending and assigned unique
           field names — best column gets the base name, rest get suffixes.
        5. Verdict assigned: detected / undetected / extra.
        6. Returns schema + full per-column metadata.
    """
    # ✅ Single global computation — applied uniformly across all detectors
    header_weight = (
        HEADER_WEIGHT_WITH_HEADER if header_present
        else HEADER_WEIGHT_WITHOUT_HEADER
    )

    # ✅ Normalize all column names to strings to prevent AttributeError
    df.columns = [str(c) for c in df.columns]

    # field → list of all candidates above threshold
    # { "vehicle_registration_number": [{"column": "col_0", "score": 95, ...}, ...] }
    field_candidates = {}
    column_metadata  = []
    detected_columns = set()

    for col in df.columns:
        series     = df[col]
        col_scores = []
        best_field    = None
        best_score    = 0
        best_evidence = []
        best_reason   = None

        for detector in DETECTORS:
            result = detector(col, series, header_weight=header_weight)

            field    = result.get("field")
            score    = result.get("score", 0)
            evidence = result.get("evidence", [])
            reason   = result.get("reason", None)

            col_scores.append({
                "field":    field,
                "score":    score,
                "evidence": evidence,
                "reason":   reason,
            })

            # Track best scoring field for this column
            if score > best_score:
                best_score    = score
                best_field    = field
                best_evidence = evidence
                best_reason   = reason

            # ✅ Collect ALL candidates above threshold — not just the first
            if score >= threshold:
                if field not in field_candidates:
                    field_candidates[field] = []
                field_candidates[field].append({
                    "column":   col,
                    "score":    score,
                    "evidence": evidence,
                })
                detected_columns.add(col)

        # Assign verdict for this column
        if best_score >= threshold:
            verdict        = "detected"
            failure_reason = None
        elif best_field is not None:
            verdict        = "undetected"
            failure_reason = best_reason
        else:
            verdict        = "extra"
            failure_reason = best_reason

        column_metadata.append({
            "column":     col,
            "field":      best_field,
            "score":      best_score,
            "verdict":    verdict,
            "evidence":   best_evidence,
            "reason":     failure_reason,
            "all_scores": col_scores,
        })

    # ----------------------------------
    # Build final schema
    # ✅ All candidates above threshold are included with suffixed names
    # Sorted by score descending so highest scorer gets the base name
    # ----------------------------------
    final_schema     = {}
    assigned_fields  = set()   # tracks field names already used in schema

    for base_field, candidates in field_candidates.items():
        # Sort highest score first — best column gets base name (no suffix)
        sorted_candidates = sorted(candidates, key=lambda x: x["score"], reverse=True)

        for candidate in sorted_candidates:
            field_name = _assign_field_name(base_field, assigned_fields)
            assigned_fields.add(field_name)

            final_schema[field_name] = {
                "column":   candidate["column"],
                "score":    candidate["score"],
                "evidence": candidate["evidence"],
            }

    # ----------------------------------
    # Build metadata
    # ----------------------------------
    # A column is "undetected" only if it scored below threshold for ALL fields
    undetected_columns = [
        c["column"] for c in column_metadata
        if c["verdict"] in ("undetected", "extra")
    ]

    metadata = {
        "total_columns":      len(df.columns),
        "detected_fields":    list(final_schema.keys()),
        "undetected_columns": undetected_columns,
        "column_details":     column_metadata,
    }

    return {
        "schema":   final_schema,
        "metadata": metadata,
    }
