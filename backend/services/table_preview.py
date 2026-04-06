from __future__ import annotations

import io
from typing import Any

import pandas as pd

# backend/services/table_preview.py
from backend.upload_parser.utils import auto_header_detect


def build_table_preview(
    content: bytes,
    filename: str,
    max_rows: int = 100,
) -> dict[str, Any]:
    """
    Returns a JSON-serializable preview of an uploaded tabular file.
    Supports: .csv, .xls, .xlsx (PDF is not tabular-previewed here).
    """
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    if ext not in ("csv", "xls", "xlsx"):
        raise ValueError("Preview only supported for .csv, .xls, .xlsx")

    stream = io.BytesIO(content)
    stream.name = filename
    df, header_present = auto_header_detect(stream)

    if df is None or df.empty:
        return {"columns": [], "rows": [], "total_rows": 0, "truncated": False}

    # If no header was detected, assign generic column names
    # but keep ALL data rows (do NOT promote first row to header)
    if not header_present:
        df.columns = [f"Column {i+1}" for i in range(len(df.columns))]

    total_rows = int(len(df))
    head = df.head(max_rows)
    truncated = total_rows > len(head)

    # Make everything JSON-safe and stable
    head = head.where(pd.notnull(head), None)
    columns = [str(c) for c in head.columns.tolist()]
    rows = head.values.tolist()

    return {
        "columns": columns,
        "rows": rows,
        "total_rows": total_rows,
        "truncated": truncated,
    }

