from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from pydantic import BaseModel, ConfigDict
from typing import Optional, List
from datetime import datetime
from uuid import UUID

from backend.db.session import get_db
from backend.db.models import User, UserSession
from backend.core.dependencies import get_current_user
from backend.core.security import hash_password, verify_password
from backend.schemas.common import APIResponse

router = APIRouter()


# ==============================
# Request / Response Schemas
# ==============================

class ProfileOut(BaseModel):
    id: UUID
    name: str
    email: str
    role: str
    is_active: bool
    created_at: datetime
    updated_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class ProfileUpdateRequest(BaseModel):
    name: Optional[str] = None
    email: Optional[str] = None


class ChangePasswordRequest(BaseModel):
    current_password: str
    new_password: str


class SessionOut(BaseModel):
    id: UUID
    created_at: datetime
    expires_at: datetime

    model_config = ConfigDict(from_attributes=True)


def _profile_from_user(user: User) -> ProfileOut:
    role = user.role.value if hasattr(user.role, "value") else user.role
    profile = ProfileOut.model_validate(user)
    profile.role = role
    return profile


# ==============================
# GET PROFILE
# ==============================

@router.get("", response_model=APIResponse[ProfileOut])
def get_profile(current_user: User = Depends(get_current_user)):
    profile = _profile_from_user(current_user)
    return APIResponse[ProfileOut](data=profile, message="Profile retrieved successfully")


# ==============================
# UPDATE PROFILE
# ==============================

@router.put("/update", response_model=APIResponse[ProfileOut])
def update_profile(
    body: ProfileUpdateRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if body.name:
        current_user.name = body.name

    if body.email:
        existing = db.query(User).filter(
            User.email == body.email,
            User.id != current_user.id,
        ).first()

        if existing:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Email already in use",
            )

        current_user.email = body.email

    db.commit()
    db.refresh(current_user)

    profile = _profile_from_user(current_user)

    return APIResponse[ProfileOut](data=profile, message="Profile updated successfully")


# ==============================
# CHANGE PASSWORD
# ==============================

@router.put("/change-password", response_model=APIResponse)
def change_password(
    body: ChangePasswordRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if not verify_password(body.current_password, current_user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Current password is incorrect",
        )

    current_user.password_hash = hash_password(body.new_password)
    db.commit()

    return APIResponse(message="Password changed successfully")


# ==============================
# LIST ACTIVE SESSIONS
# ==============================

@router.get("/sessions", response_model=APIResponse[List[SessionOut]])
def list_sessions(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    sessions = (
        db.query(UserSession)
        .filter(
            UserSession.user_id == current_user.id,
            UserSession.is_revoked == False,
            UserSession.expires_at > datetime.utcnow(),
        )
        .order_by(UserSession.created_at.desc())
        .all()
    )

    data = [SessionOut.model_validate(s) for s in sessions]

    return APIResponse[List[SessionOut]](data=data, message="Active sessions retrieved successfully")
