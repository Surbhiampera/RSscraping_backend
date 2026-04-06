from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import func

from backend.db.session import get_db
from backend.db.models import ScrapeRun, CarInfo, StatusEnum, User
from backend.core.dependencies import get_current_user, require_admin
from backend.schemas.common import APIResponse

router = APIRouter()


@router.get("", response_model=APIResponse)
def dashboard_overview(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Aggregated dashboard metrics for the current user."""
    total_uploaded_cars = db.query(func.count(CarInfo.id)).scalar() or 0
    scraped_successfully = db.query(func.count(CarInfo.id)).filter(CarInfo.status == StatusEnum.PASS_).scalar() or 0
    total_failed = db.query(func.count(CarInfo.id)).filter(CarInfo.status == StatusEnum.FAIL).scalar() or 0
    total_pending = db.query(func.count(CarInfo.id)).filter(CarInfo.status == StatusEnum.PENDING).scalar() or 0
    total_batches = db.query(func.count(ScrapeRun.run_id)).scalar() or 0

    # User-specific
    my_run_ids = [r.run_id for r in db.query(ScrapeRun.run_id).filter(ScrapeRun.user_id == current_user.id).all()]
    my_upload_count = len(my_run_ids)
    if my_run_ids:
        my_total_cars = db.query(func.count(CarInfo.id)).filter(CarInfo.run_id.in_(my_run_ids)).scalar() or 0
        my_success = db.query(func.count(CarInfo.id)).filter(CarInfo.run_id.in_(my_run_ids), CarInfo.status == StatusEnum.PASS_).scalar() or 0
        my_failed = db.query(func.count(CarInfo.id)).filter(CarInfo.run_id.in_(my_run_ids), CarInfo.status == StatusEnum.FAIL).scalar() or 0
    else:
        my_total_cars = my_success = my_failed = 0

    total_users = db.query(func.count(User.id)).scalar() or 0

    return APIResponse(data={
        "total_uploaded_cars": total_uploaded_cars,
        "scraped_successfully": scraped_successfully,
        "total_failed": total_failed,
        "total_pending": total_pending,
        "total_batches": total_batches,
        "total_users": total_users,
        "my_upload_count": my_upload_count,
        "my_total_cars": my_total_cars,
        "my_success": my_success,
        "my_failed": my_failed,
    })


# ---------------------------------------------------------
# 1. TOTAL UPLOADED VEHICLES
# ---------------------------------------------------------
@router.get("/total-vehicles", response_model=APIResponse)
def total_uploaded_vehicles(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Returns total number of vehicle records uploaded in the system.
    """
    total = db.query(func.count(CarInfo.id)).scalar()

    return APIResponse(data={"total_uploaded_vehicles": total})


# ---------------------------------------------------------
# 2. SCRAPED SUCCESSFULLY
# ---------------------------------------------------------
@router.get("/success-count", response_model=APIResponse)
def scraped_successfully(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Returns number of vehicles successfully scraped.
    """
    total = db.query(func.count(CarInfo.id)).filter(
        CarInfo.status == StatusEnum.PASS_
    ).scalar()

    return APIResponse(data={"scraped_successfully": total})


# ---------------------------------------------------------
# 3. TOTAL FAILED
# ---------------------------------------------------------
@router.get("/failed-count", response_model=APIResponse)
def total_failed(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Returns number of vehicle scrape failures.
    """
    total = db.query(func.count(CarInfo.id)).filter(
        CarInfo.status == StatusEnum.FAIL
    ).scalar()

    return APIResponse(data={"total_failed": total})


# ---------------------------------------------------------
# 4. TOTAL PENDING
# ---------------------------------------------------------
@router.get("/pending-count", response_model=APIResponse)
def total_pending(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Returns number of vehicles waiting to be processed.
    """
    total = db.query(func.count(CarInfo.id)).filter(
        CarInfo.status == StatusEnum.PENDING
    ).scalar()

    return APIResponse(data={"total_pending": total})


# ---------------------------------------------------------
# 5. TOTAL BATCHES (SCRAPE RUNS)
# ---------------------------------------------------------
@router.get("/total-batches", response_model=APIResponse)
def total_batches(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Returns total number of scrape batches created.
    Each batch corresponds to one uploaded file.
    """
    total = db.query(func.count(ScrapeRun.run_id)).scalar()

    return APIResponse(data={"total_batches": total})


# ---------------------------------------------------------
# 6. TOTAL USERS (ADMIN ONLY)
# ---------------------------------------------------------
@router.get("/total-users", response_model=APIResponse)
def total_users(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    """
    Returns total number of registered users in the system.
    Accessible only by admins.
    """
    total = db.query(func.count(User.id)).scalar()

    return APIResponse(data={"total_users": total})


# ---------------------------------------------------------
# 7. MY UPLOADS (USER SPECIFIC)
# ---------------------------------------------------------
@router.get("/my-uploads", response_model=APIResponse)
def my_uploads(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Returns total number of files/batches uploaded by the current user.
    """
    total = db.query(func.count(ScrapeRun.run_id)).filter(
        ScrapeRun.user_id == current_user.id
    ).scalar()

    return APIResponse(data={"my_uploads": total})