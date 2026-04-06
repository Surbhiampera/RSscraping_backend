"""
Admin Routes
------------
These routes are accessible only by ADMIN users.

Features:
- User management (CRUD)
- Role management
- Activate / deactivate users
- System statistics
"""

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session
from uuid import UUID

from backend.db.session import get_db
from backend.db.models import User, RoleEnum
from backend.core.dependencies import require_admin
from backend.core.security import hash_password

from backend.schemas.user import (
    UserCreate,
    UserUpdate,
    UserChangeRole,
    UserListOut,
)

from backend.schemas.common import (
    APIResponse,
    PaginatedResponse,
    PaginationMeta,
)

from backend.services.dashboard_service import (
    get_user_admin_stats,
    get_system_metrics,
)

router = APIRouter()

# List All Users (With Filters + Pagination)

@router.get("/users", response_model=PaginatedResponse[UserListOut])
def list_users(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(20, ge=1, le=100, description="Items per page"),
    search: str | None = Query(None, description="Search by name or email"),
    role: str | None = Query(None, description="Filter by role (ADMIN / USER)"),
    admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    """
    Get a paginated list of users.

    Features:
    - Search by name or email
    - Filter by role
    - Pagination support
    - Includes admin statistics for each user
    """

    query = db.query(User)

    # Apply search filter
    if search:
        term = f"%{search}%"
        query = query.filter(
            (User.name.ilike(term)) | (User.email.ilike(term))
        )

    # Filter by role
    if role:
        query = query.filter(User.role == role)

    query = query.order_by(User.created_at.desc())

    total = query.count()
    users = query.offset((page - 1) * limit).limit(limit).all()

    data = [
        UserListOut(**get_user_admin_stats(db, user))
        for user in users
    ]

    return PaginatedResponse(
        data=data,
        pagination=PaginationMeta(
            page=page,
            limit=limit,
            total=total,
            total_pages=(total + limit - 1) // limit,
        ),
    )
    
# Get Single User
@router.get("/users/{user_id}", response_model=APIResponse[UserListOut])
def get_user(
    user_id: UUID,
    admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    """
    Retrieve a single user with admin statistics.
    """

    user = db.query(User).filter(User.id == user_id).first()

    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found",
        )

    stats = get_user_admin_stats(db, user)

    return APIResponse(data=UserListOut(**stats))

# Create User
@router.post(
    "/users",
    response_model=APIResponse[UserListOut],
    status_code=status.HTTP_201_CREATED,
)
def create_user(
    body: UserCreate,
    admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    """
    Create a new user.

    Steps:
    - Check if email already exists
    - Hash password
    - Save user
    """

    existing = db.query(User).filter(User.email == body.email).first()

    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="User with this email already exists",
        )

    user = User(
        name=body.name,
        email=body.email,
        password_hash=hash_password(body.password),
        role=RoleEnum(body.role) if body.role else RoleEnum.USER,
    )

    db.add(user)
    db.commit()
    db.refresh(user)

    stats = get_user_admin_stats(db, user)

    return APIResponse(
        data=UserListOut(**stats),
        message="User created successfully",
    )

# Update User
@router.put("/users/{user_id}", response_model=APIResponse[UserListOut])
def update_user(
    user_id: UUID,
    body: UserUpdate,
    admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    """
    Update user details.

    Editable fields:
    - name
    - email
    - role
    - active status
    """

    user = db.query(User).filter(User.id == user_id).first()

    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    if body.name:
        user.name = body.name

    if body.email:
        existing = db.query(User).filter(
            User.email == body.email,
            User.id != user_id,
        ).first()

        if existing:
            raise HTTPException(
                status_code=409,
                detail="Email already in use",
            )

        user.email = body.email

    if body.role:
        user.role = RoleEnum(body.role)

    if body.is_active is not None:
        user.is_active = body.is_active

    db.commit()
    db.refresh(user)

    stats = get_user_admin_stats(db, user)

    return APIResponse(
        data=UserListOut(**stats),
        message="User updated successfully",
    )
# Delete User
@router.delete("/users/{user_id}", response_model=APIResponse)
def delete_user(
    user_id: UUID,
    admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    """
    Delete a user and all associated data.
    """

    user = db.query(User).filter(User.id == user_id).first()

    if not user:
        raise HTTPException(404, "User not found")

    # Prevent admin from deleting themselves
    if user.id == admin.id:
        raise HTTPException(
            status_code=400,
            detail="Cannot delete your own account",
        )

    db.delete(user)
    db.commit()

    return APIResponse(message="User deleted successfully")

# Change User Role
@router.patch("/users/{user_id}/role", response_model=APIResponse[UserListOut])
def change_user_role(
    user_id: UUID,
    body: UserChangeRole,
    admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    """
    Change role of a user.

    Allowed roles:
    - ADMIN
    - USER
    """

    user = db.query(User).filter(User.id == user_id).first()

    if not user:
        raise HTTPException(404, "User not found")

    if user.id == admin.id:
        raise HTTPException(
            status_code=400,
            detail="Cannot change your own role",
        )

    user.role = RoleEnum(body.role)

    db.commit()
    db.refresh(user)

    stats = get_user_admin_stats(db, user)

    return APIResponse(
        data=UserListOut(**stats),
        message=f"Role updated to {body.role}",
    )
    
# Toggle User Active Status
@router.patch("/users/{user_id}/toggle-active", response_model=APIResponse[UserListOut])
def toggle_user_active(
    user_id: UUID,
    admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    """
    Activate or deactivate a user account.
    """

    user = db.query(User).filter(User.id == user_id).first()

    if not user:
        raise HTTPException(404, "User not found")

    if user.id == admin.id:
        raise HTTPException(
            status_code=400,
            detail="Cannot deactivate your own account",
        )

    user.is_active = not user.is_active

    db.commit()
    db.refresh(user)

    status_text = "activated" if user.is_active else "deactivated"

    stats = get_user_admin_stats(db, user)

    return APIResponse(
        data=UserListOut(**stats),
        message=f"User {status_text}",
    )
# System Statistics
@router.get("/stats", response_model=APIResponse)
def admin_system_stats(
    admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    """
    Retrieve system-wide metrics.

    Includes:
    - total users
    - total uploads
    - total scrape runs
    - success rate
    """

    metrics = get_system_metrics(db)

    return APIResponse(
        data=metrics,
        message="System stats retrieved successfully",
    )
    
