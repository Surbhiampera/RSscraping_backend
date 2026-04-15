from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from backend.db.session import get_db
from backend.db.models import User
from backend.core.dependencies import get_current_user
from backend.schemas.common import APIResponse
from backend.services.insights_service import (
    get_insurer_pattern_analysis,
    get_premium_by_vehicle_age,
    get_addon_coverage_patterns,
)

router = APIRouter()


@router.get("/insurer-patterns", response_model=APIResponse)
def insurer_patterns(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    data = get_insurer_pattern_analysis(db)
    return APIResponse(data=data)


@router.get("/premium-by-age", response_model=APIResponse)
def premium_by_age(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    data = get_premium_by_vehicle_age(db)
    return APIResponse(data=data)


@router.get("/addon-patterns", response_model=APIResponse)
def addon_patterns(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    data = get_addon_coverage_patterns(db)
    return APIResponse(data=data)
