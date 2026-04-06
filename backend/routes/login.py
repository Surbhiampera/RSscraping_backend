from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from pydantic import BaseModel, EmailStr
from backend.db.session import get_db
from backend.db.models import User
from backend.core.security import (
    hash_password,
    verify_password,
    create_access_token,
)
import uuid

router = APIRouter(prefix="/api/auth", tags=["Auth"])

# ===============================
# SCHEMAS
# ===============================

class RegisterRequest(BaseModel):
    name: str
    email: EmailStr
    password: str


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class ForgotPasswordRequest(BaseModel):
    email: EmailStr


class AuthResponse(BaseModel):
    access_token: str
    token_type: str
    user: dict


# ===============================
# REGISTER
# ===============================

@router.post("/register", response_model=AuthResponse)
def register(payload: RegisterRequest, db: Session = Depends(get_db)):

    # 1️⃣ Check if email already exists
    existing_user = db.query(User).filter(User.email == payload.email).first()
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Email already registered"
        )

    # 2️⃣ Create new user
    new_user = User(
        id=uuid.uuid4(),
        name=payload.name,  # ✅ fixed
        email=payload.email,
        password_hash=hash_password(payload.password),  # ✅ fixed
        role="USER",
        is_active=True
    )

    db.add(new_user)
    db.commit()
    db.refresh(new_user)

    # 3️⃣ Auto-login after register
    access_token = create_access_token({
        "sub": str(new_user.id),
        "email": new_user.email,
        "role": new_user.role
    })

    return {
        "access_token": access_token,
        "token_type": "bearer",
        "user": {
            "id": str(new_user.id),
            "email": new_user.email,
            "role": new_user.role,
            "name": new_user.name  # ✅ fixed
        }
    }


# ===============================
# LOGIN
# ===============================

@router.post("/login", response_model=AuthResponse)
def login(payload: LoginRequest, db: Session = Depends(get_db)):

    user = db.query(User).filter(User.email == payload.email).first()

    if not user or not verify_password(payload.password, user.password_hash):  # ✅ fixed
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password"
        )

    access_token = create_access_token({
        "sub": str(user.id),
        "email": user.email,
        "role": user.role
    })

    return {
        "access_token": access_token,
        "token_type": "bearer",
        "user": {
            "id": str(user.id),
            "email": user.email,
            "role": user.role,
            "name": user.name  # ✅ fixed
        }
    }


# ===============================
# FORGOT PASSWORD
# ===============================

@router.post("/forgot-password")
def forgot_password(payload: ForgotPasswordRequest, db: Session = Depends(get_db)):

    user = db.query(User).filter(User.email == payload.email).first()

    if user:
        # Production logic goes here
        pass

    return {
        "message": "If an account exists, a password reset link has been sent."
    }