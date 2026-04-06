from pydantic import BaseModel
from typing import Any, List


class PreviewTable(BaseModel):
    columns: List[str]
    rows: List[List[Any]]
    total_rows: int
    truncated: bool

