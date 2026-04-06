from pydantic import BaseModel
from typing import Optional, List
from uuid import UUID
from datetime import datetime


class UploadPreviewRow(BaseModel):
    car_number: str
    is_valid: bool
    error: Optional[str] = None


class FieldMapping(BaseModel):
    column: str
    score: float
    evidence: List[str]


class ColumnDetail(BaseModel):
    column: str
    field: Optional[str] = None
    score: float
    verdict: str
    evidence: List[str]
    reason: Optional[str] = None


class QualityFieldInfo(BaseModel):
    column: str
    total_rows: int
    null_count: int
    null_pct: float
    null_indices: List[int] = []


class DuplicateFieldInfo(BaseModel):
    column: str
    total_rows: int
    duplicate_count: int
    duplicate_pct: float
    duplicate_values: List[str] = []


class QualitySummary(BaseModel):
    total_rows: int
    total_fields_checked: int
    fields_with_nulls: List[str]
    fields_with_duplicates: List[str]
    clean_fields: List[str]


class QualityResult(BaseModel):
    nulls: dict
    duplicates: dict
    summary: QualitySummary


class DetectionMetadata(BaseModel):
    total_columns: int
    detected_fields: List[str]
    undetected_columns: List[str]
    column_details: List[ColumnDetail]


class ParsedUploadResult(BaseModel):
    file_id: UUID
    filename: str
    header_present: bool
    schema_mapping: dict
    metadata: DetectionMetadata
    quality: QualityResult
    preview: List[UploadPreviewRow]
    total_rows: int
    valid_rows: int
    invalid_rows: int


class UploadResponse(BaseModel):
    run_id: UUID  # last run (kept for backward compat)
    run_ids: List[UUID]
    file_id: UUID
    original_filename: str
    total_rows: int
    valid_rows: int
    invalid_rows: int
    header_present: bool
    schema_mapping: dict
    metadata: DetectionMetadata
    quality: QualityResult
    preview: List[UploadPreviewRow]


class ScrapeRunOut(BaseModel):
    run_id: UUID
    user_id: UUID
    file_id: Optional[UUID] = None
    status: str
    total_rows: int
    valid_rows: int
    invalid_rows: int
    success_count: int
    fail_count: int
    started_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None
    total_duration_ms: Optional[int] = None
    notes: Optional[str] = None
    created_at: datetime

    model_config = {
        "from_attributes": True  # <-- allows SQLAlchemy ORM instances to be validated
    }


class ScrapeRunUpdate(BaseModel):
    notes: Optional[str] = None
    status: Optional[str] = None