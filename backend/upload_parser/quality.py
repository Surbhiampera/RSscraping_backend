import pandas as pd


def run_quality_check(df, schema):
    """
    Runs null and duplicate analysis on detected columns only.
    Uses _null_mask() to catch both NaN and empty strings.
    """
    total_rows = len(df)
    nulls      = {}
    duplicates = {}

    for field, info in schema.items():
        col_name = info["column"]

        if col_name not in df.columns:
            continue

        series = df[col_name]

        # ----------------------------------
        # Null Analysis — NaN + empty string
        # ----------------------------------
        null_mask    = series.isnull() | (series.astype(str).str.strip() == "")
        null_count   = int(null_mask.sum())
        null_pct     = round(null_count / total_rows * 100, 2) if total_rows else 0.0
        null_indices = list(df.index[null_mask])

        nulls[field] = {
            "column":       col_name,
            "total_rows":   total_rows,
            "null_count":   null_count,
            "null_pct":     null_pct,
            "null_indices": null_indices,
        }

        # ----------------------------------
        # Duplicate Analysis (within column)
        # ----------------------------------
        non_null        = series[~null_mask]
        dup_mask        = non_null.duplicated(keep=False)
        duplicate_count = int(dup_mask.sum())
        duplicate_pct   = round(duplicate_count / total_rows * 100, 2) if total_rows else 0.0
        duplicate_values = list(non_null[dup_mask].unique())

        duplicates[field] = {
            "column":           col_name,
            "total_rows":       total_rows,
            "duplicate_count":  duplicate_count,
            "duplicate_pct":    duplicate_pct,
            "duplicate_values": duplicate_values,
        }

    # ----------------------------------
    # Summary
    # ----------------------------------
    fields_with_nulls      = [f for f, v in nulls.items()      if v["null_count"] > 0]
    fields_with_duplicates = [f for f, v in duplicates.items() if v["duplicate_count"] > 0]
    clean_fields           = [
        f for f in schema
        if f not in fields_with_nulls and f not in fields_with_duplicates
    ]

    return {
        "nulls":      nulls,
        "duplicates": duplicates,
        "summary": {
            "total_rows":             total_rows,
            "total_fields_checked":   len(schema),
            "fields_with_nulls":      fields_with_nulls,
            "fields_with_duplicates": fields_with_duplicates,
            "clean_fields":           clean_fields,
        }
    }


def run_cross_column_quality_check(df, schema):
    """
    Detects cross-column duplicates when multiple columns share the same
    base field type (e.g., col_0 and col_1 both detected as
    vehicle_registration_number).

    How it works:
        1. Group all detected columns by their BASE field name.
           "vehicle_registration_number"   → col_0
           "vehicle_registration_number_2" → col_1
           Both share base: "vehicle_registration_number"

        2. For each group with 2+ columns, pool all non-null values
           across all columns and find values that appear in more than
           one column — these are CROSS-COLUMN duplicates.

        3. Also checks if any value in col_A appears in col_B (direct overlap).

    Args:
        df     : original DataFrame
        schema : output from run_detection_v2()

    Returns:
        List of cross-column duplicate findings, one per field group.
        Empty list if no multi-column groups exist.

    Example output:
        [
            {
                "base_field":      "vehicle_registration_number",
                "columns":         ["col_0", "col_1"],
                "cross_dup_values": ["MH12CD5678", "TN01AB1234"],
                "cross_dup_count":  4,
                "cross_dup_pct":   50.0,
                "null_per_column": {
                    "col_0": {"null_count": 0, "null_pct": 0.0},
                    "col_1": {"null_count": 1, "null_pct": 25.0},
                }
            }
        ]
    """
    total_rows = len(df)

    # ----------------------------------
    # Step 1: Group fields by base name
    # Strip trailing "_2", "_3" suffixes to find same-type columns
    # e.g. "vehicle_registration_number_2" → base "vehicle_registration_number"
    # ----------------------------------
    from collections import defaultdict
    import re

    groups = defaultdict(list)

    for field, info in schema.items():
        # Strip numeric suffix: "vehicle_registration_number_2" → "vehicle_registration_number"
        base = re.sub(r"_\d+$", "", field)
        col_name = info["column"]
        if col_name in df.columns:
            groups[base].append((field, col_name))

    results = []

    for base_field, field_col_pairs in groups.items():
        # Only process groups with 2+ columns
        if len(field_col_pairs) < 2:
            continue

        columns = [col for _, col in field_col_pairs]

        # ----------------------------------
        # Step 2: Per-column null analysis
        # ----------------------------------
        null_per_column = {}
        col_values      = {}

        for _, col in field_col_pairs:
            series    = df[col]
            null_mask = series.isnull() | (series.astype(str).str.strip() == "")
            null_count = int(null_mask.sum())
            null_pct   = round(null_count / total_rows * 100, 2) if total_rows else 0.0

            null_per_column[col] = {
                "null_count": null_count,
                "null_pct":   null_pct,
            }
            # Store non-null values as a set for overlap detection
            col_values[col] = set(series[~null_mask].astype(str).str.strip())

        # ----------------------------------
        # Step 3: Cross-column duplicate detection
        # Find values that appear in MORE THAN ONE column
        # ----------------------------------
        all_col_list = list(col_values.keys())
        cross_dup_values = set()

        for i in range(len(all_col_list)):
            for j in range(i + 1, len(all_col_list)):
                overlap = col_values[all_col_list[i]] & col_values[all_col_list[j]]
                cross_dup_values.update(overlap)

        # Count total rows affected by cross-column duplicates
        cross_dup_count = 0
        for _, col in field_col_pairs:
            series    = df[col]
            null_mask = series.isnull() | (series.astype(str).str.strip() == "")
            non_null  = series[~null_mask].astype(str).str.strip()
            cross_dup_count += int(non_null.isin(cross_dup_values).sum())

        cross_dup_pct = round(cross_dup_count / (total_rows * len(columns)) * 100, 2) \
                        if total_rows else 0.0

        results.append({
            "base_field":       base_field,
            "columns":          columns,
            "cross_dup_values": sorted(cross_dup_values),
            "cross_dup_count":  cross_dup_count,
            "cross_dup_pct":    cross_dup_pct,
            "null_per_column":  null_per_column,
        })

    return results
