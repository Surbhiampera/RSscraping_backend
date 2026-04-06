from pydantic import BaseModel, ConfigDict, EmailStr, Field, validator
from typing import Optional
from uuid import UUID
from datetime import datetime


class UserCreate(BaseModel):
    name: str
    email: EmailStr
    password: str = Field(..., min_length=6)
    role: str = "USER"


class UserUpdate(BaseModel):
    name: Optional[str] = None
    email: Optional[EmailStr] = None
    role: Optional[str] = None
    is_active: Optional[bool] = None


class UserChangeRole(BaseModel):
    role: str

    @validator("role")
    @classmethod
    def validate_role(cls, v: str) -> str:
        if v not in ("ADMIN", "USER"):
            raise ValueError("Role must be 'ADMIN' or 'USER'")
        return v


class UserListOut(BaseModel):
    id: UUID
    name: str
    email: str
    role: str
    is_active: bool
    created_at: datetime
    total_uploads: int = 0
    total_cars_uploaded: int = 0
    success_count: int = 0
    fail_count: int = 0
    last_upload_date: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)
