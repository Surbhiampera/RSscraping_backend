from pydantic import BaseModel, ConfigDict, Field
from typing import Optional, List, Dict, Any
from uuid import UUID
from datetime import datetime, date
from enum import Enum


class ExportFormat(str, Enum):
    xlsx = "xlsx"
    csv = "csv"
    json = "json"
    pdf = "pdf"


class ExportFilter(str, Enum):
    all = "all"
    success = "success"
    failed = "failed"
    selected = "selected"


class CarInfoOut(BaseModel):
    id: UUID
    run_id: UUID
    registration_number: str
    make_name: Optional[str] = None
    model_name: Optional[str] = None
    vehicle_variant: Optional[str] = None
    fuel_type: Optional[str] = None
    cubic_capacity: Optional[int] = None
    state_code: Optional[str] = None
    city_tier: Optional[str] = None
    car_age: Optional[int] = None
    registration_date: Optional[date] = None
    status: str
    error_message: Optional[str] = None
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class ResultRow(BaseModel):
    id: UUID
    run_id: UUID
    registration_number: str
    make_name: Optional[str] = None
    model_name: Optional[str] = None
    vehicle_variant: Optional[str] = None
    fuel_type: Optional[str] = None
    cubic_capacity: Optional[int] = None
    state_code: Optional[str] = None
    city_tier: Optional[str] = None
    car_age: Optional[int] = None
    registration_date: Optional[date] = None
    status: str
    error_message: Optional[str] = None
    insurer_name: Optional[str] = None
    plan_id: Optional[int] = None
    ncb_percent: Optional[int] = None
    basic_od_premium: Optional[float] = None
    total_tp_premium: Optional[float] = None
    final_premium: Optional[float] = None
    total_addons: int = 0
    raw_data: Optional[Dict[str, Any]] = None

    model_config = ConfigDict(from_attributes=True)


class ExportRequest(BaseModel):
    format: ExportFormat
    filter: Optional[ExportFilter] = None
    ids: Optional[List[UUID]] = None
    run_id: Optional[UUID] = None


class FlatOutputOut(BaseModel):
    id: str
    run_id: str
    flat_output: Any
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)
