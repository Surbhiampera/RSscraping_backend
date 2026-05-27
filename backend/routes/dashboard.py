from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import func, cast, String as SAString

from backend.db.session import get_db
from backend.db.models import ScrapeRun, ScrapeRunInput, StatusEnum, User
from backend.core.dependencies import get_current_user, require_admin
from backend.schemas.common import APIResponse

router = APIRouter()

# Status values written by the Celery pipeline (raw strings, bypassing ORM enum)
# and by the ORM path (StatusEnum values).
_SUCCESS_STATUSES = ("SUCCESS", "PASS")
_FAILURE_STATUSES = ("FAIL", "FAILURE")
_PENDING_STATUSES = ("PENDING", "pending")
_PROCESSING_STATUSES = ("PROCESSING", "processing")


def _status_in(column, values: tuple):
    """Case-insensitive status filter that works with both enum and varchar columns."""
    return cast(column, SAString).in_(values)


@router.get("", response_model=APIResponse)
def dashboard_overview(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Aggregated dashboard metrics for the current user."""

    # Total cars = total ScrapeRunInput rows (one per uploaded car)
    total_uploaded_cars = db.query(func.count(ScrapeRunInput.id)).scalar() or 0

    # Scraped successfully / failed / pending — read from ScrapeRun.status
    # The Celery pipeline writes "SUCCESS"/"FAILURE"; the ORM writes "PASS"/"FAIL".
    scraped_successfully = db.query(func.count(ScrapeRun.run_id)).filter(
        _status_in(ScrapeRun.status, _SUCCESS_STATUSES)
    ).scalar() or 0

    total_failed = db.query(func.count(ScrapeRun.run_id)).filter(
        _status_in(ScrapeRun.status, _FAILURE_STATUSES)
    ).scalar() or 0

    total_pending = db.query(func.count(ScrapeRun.run_id)).filter(
        _status_in(ScrapeRun.status, _PENDING_STATUSES + _PROCESSING_STATUSES)
    ).scalar() or 0

    total_batches = db.query(func.count(ScrapeRun.run_id)).scalar() or 0

    # User-specific counts
    my_runs = db.query(ScrapeRun).filter(ScrapeRun.user_id == current_user.id).all()
    my_run_ids = [r.run_id for r in my_runs]
    my_upload_count = len(my_run_ids)

    if my_run_ids:
        my_total_cars = db.query(func.count(ScrapeRunInput.id)).filter(
            ScrapeRunInput.run_id.in_(my_run_ids)
        ).scalar() or 0

        my_success = db.query(func.count(ScrapeRun.run_id)).filter(
            ScrapeRun.run_id.in_(my_run_ids),
            _status_in(ScrapeRun.status, _SUCCESS_STATUSES),
        ).scalar() or 0

        my_failed = db.query(func.count(ScrapeRun.run_id)).filter(
            ScrapeRun.run_id.in_(my_run_ids),
            _status_in(ScrapeRun.status, _FAILURE_STATUSES),
        ).scalar() or 0
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
    """Returns total number of vehicle inputs uploaded in the system."""
    total = db.query(func.count(ScrapeRunInput.id)).scalar() or 0
    return APIResponse(data={"total_uploaded_vehicles": total})


# ---------------------------------------------------------
# 2. SCRAPED SUCCESSFULLY
# ---------------------------------------------------------
@router.get("/success-count", response_model=APIResponse)
def scraped_successfully(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Returns number of scrape runs that completed successfully."""
    total = db.query(func.count(ScrapeRun.run_id)).filter(
        _status_in(ScrapeRun.status, _SUCCESS_STATUSES)
    ).scalar() or 0
    return APIResponse(data={"scraped_successfully": total})


# ---------------------------------------------------------
# 3. TOTAL FAILED
# ---------------------------------------------------------
@router.get("/failed-count", response_model=APIResponse)
def total_failed(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Returns number of scrape runs that failed."""
    total = db.query(func.count(ScrapeRun.run_id)).filter(
        _status_in(ScrapeRun.status, _FAILURE_STATUSES)
    ).scalar() or 0
    return APIResponse(data={"total_failed": total})


# ---------------------------------------------------------
# 4. TOTAL PENDING
# ---------------------------------------------------------
@router.get("/pending-count", response_model=APIResponse)
def total_pending(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Returns number of scrape runs waiting to be processed."""
    total = db.query(func.count(ScrapeRun.run_id)).filter(
        _status_in(ScrapeRun.status, _PENDING_STATUSES + _PROCESSING_STATUSES)
    ).scalar() or 0
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


# ---------------------------------------------------------
# 8. STATUS RECORDS (failed / pending / processing)
# ---------------------------------------------------------
@router.get("/status-records", response_model=APIResponse)
def status_records(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Returns recent runs grouped by FAILED, PENDING, and PROCESSING status."""

    def _build_records(runs) -> list:
        if not runs:
            return []
        run_ids = [r.run_id for r in runs]
        inputs = (
            db.query(ScrapeRunInput.run_id, ScrapeRunInput.car_number)
            .filter(ScrapeRunInput.run_id.in_(run_ids))
            .all()
        )
        cars_map: dict = {}
        for inp in inputs:
            cars_map.setdefault(inp.run_id, []).append(inp.car_number)

        result = []
        for r in runs:
            all_cars = cars_map.get(r.run_id, [])
            result.append({
                "run_id": str(r.run_id),
                "status": r.status if isinstance(r.status, str) else r.status.value,
                "total_inputs": r.total_inputs or 0,
                "success_count": r.success_count or 0,
                "fail_count": r.fail_count or 0,
                "created_at": r.created_at.isoformat() if r.created_at else None,
                "started_at": r.started_at.isoformat() if r.started_at else None,
                "notes": r.notes,
                "sample_cars": all_cars[:3],
                "total_cars": len(all_cars),
            })
        return result

    failed_runs = (
        db.query(ScrapeRun)
        .filter(_status_in(ScrapeRun.status, _FAILURE_STATUSES))
        .order_by(ScrapeRun.created_at.desc())
        .limit(20)
        .all()
    )
    pending_runs = (
        db.query(ScrapeRun)
        .filter(_status_in(ScrapeRun.status, _PENDING_STATUSES))
        .order_by(ScrapeRun.created_at.desc())
        .limit(20)
        .all()
    )
    processing_runs = (
        db.query(ScrapeRun)
        .filter(_status_in(ScrapeRun.status, _PROCESSING_STATUSES))
        .order_by(ScrapeRun.created_at.desc())
        .limit(20)
        .all()
    )

    return APIResponse(data={
        "failed": _build_records(failed_runs),
        "pending": _build_records(pending_runs),
        "processing": _build_records(processing_runs),
    })