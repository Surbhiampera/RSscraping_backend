from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
import uuid
import hashlib

from backend.db.session import get_db
from backend.db.models import (
    User,
    RoleEnum,
    UserSession,
    PasswordResetToken,
)
from backend.core.security import hash_password, verify_password, create_access_token
from backend.core.dependencies import get_current_user
from backend.schemas.auth import (
    LoginRequest,
    RegisterRequest,
    ForgotPasswordRequest,
    ResetPasswordRequest,
    TokenResponse,
    UserOut,
)
from backend.schemas.common import APIResponse

router = APIRouter()

# ==============================
# Helpers
# ==============================

def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


# ==============================
# LOGIN
# ==============================

@router.post("/login", response_model=APIResponse[TokenResponse])
def login(body: LoginRequest, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.email == body.email).first()

    if not user or not verify_password(body.password, user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password",
        )

    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account is deactivated. Contact admin.",
        )

    # Create JWT
    token = create_access_token({
        "sub": str(user.id),
        "role": user.role.value if hasattr(user.role, "value") else user.role,
    })

    # Store session in DB
    session = UserSession(
        user_id=user.id,
        session_token_hash=hash_token(token),
        expires_at=datetime.utcnow() + timedelta(days=7),
    )

    db.add(session)
    db.commit()

    user_out = UserOut.model_validate(user)
    user_out.role = user.role.value if hasattr(user.role, "value") else user.role

    return APIResponse[TokenResponse](
        data=TokenResponse(access_token=token, user=user_out),
        message="Login successful",
    )


# ==============================
# REGISTER
# ==============================

@router.post(
    "/register",
    response_model=APIResponse[TokenResponse],
    status_code=status.HTTP_201_CREATED,
)
def register(body: RegisterRequest, db: Session = Depends(get_db)):
    existing = db.query(User).filter(User.email == body.email).first()

    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="An account with this email already exists",
        )

    user = User(
        name=body.name,
        email=body.email,
        password_hash=hash_password(body.password),
        role=RoleEnum.USER,
    )

    db.add(user)
    db.commit()
    db.refresh(user)

    token = create_access_token({"sub": str(user.id), "role": user.role})

    # Store session
    session = UserSession(
        user_id=user.id,
        session_token_hash=hash_token(token),
        expires_at=datetime.utcnow() + timedelta(days=7),
    )
    db.add(session)
    db.commit()

    user_out = UserOut.model_validate(user)
    user_out.role = user.role.value if hasattr(user.role, "value") else user.role

    return APIResponse[TokenResponse](
        data=TokenResponse(access_token=token, user=user_out),
        message="Account created successfully",
    )


# ==============================
# FORGOT PASSWORD
# ==============================

@router.post("/forgot-password", response_model=APIResponse)
def forgot_password(body: ForgotPasswordRequest, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.email == body.email).first()

    if user:
        raw_token = str(uuid.uuid4())
        token_hash = hash_token(raw_token)

        reset_token = PasswordResetToken(
            user_id=user.id,
            token_hash=token_hash,
            expires_at=datetime.utcnow() + timedelta(hours=1),
        )

        db.add(reset_token)
        db.commit()

        # TODO: Send raw_token via email
        print("RESET TOKEN:", raw_token)

    return APIResponse(message="If an account exists, a reset link has been sent.")


# ==============================
# RESET PASSWORD
# ==============================

@router.post("/reset-password", response_model=APIResponse)
def reset_password(body: ResetPasswordRequest, db: Session = Depends(get_db)):
    token_hash = hash_token(body.token)

    reset_token = (
        db.query(PasswordResetToken)
        .filter(
            PasswordResetToken.token_hash == token_hash,
            PasswordResetToken.is_used == False,
            PasswordResetToken.expires_at > datetime.utcnow(),
        )
        .first()
    )

    if not reset_token:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid or expired reset token",
        )

    user = db.query(User).filter(User.id == reset_token.user_id).first()

    user.password_hash = hash_password(body.new_password)
    reset_token.is_used = True

    db.commit()

    return APIResponse(message="Password has been reset successfully.")


# ==============================
# LOGOUT
# ==============================

@router.post("/logout", response_model=APIResponse)
def logout(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    # Revoke all active sessions for user
    db.query(UserSession).filter(
        UserSession.user_id == current_user.id,
        UserSession.is_revoked == False
    ).update({"is_revoked": True})

    db.commit()

    return APIResponse(message="Logged out successfully.")


# ==============================
# GET ME
# ==============================

@router.get("/me", response_model=APIResponse[UserOut])
def get_me(current_user: User = Depends(get_current_user)):
    user_out = UserOut.model_validate(current_user)
    user_out.role = current_user.role.value if hasattr(current_user.role, "value") else current_user.role
    return APIResponse[UserOut](data=user_out)


# ==============================
# CHANGE PASSWORD
# ==============================

@router.post("/change-password", response_model=APIResponse)
def change_password(
    current_password: str,
    new_password: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if not verify_password(current_password, current_user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Current password is incorrect",
        )

    if len(new_password) < 6:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="New password must be at least 6 characters",
        )

    current_user.password_hash = hash_password(new_password)

    # revoke old sessions after password change
    db.query(UserSession).filter(
        UserSession.user_id == current_user.id
    ).update({"is_revoked": True})

    db.commit()

    return APIResponse(message="Password changed successfully")