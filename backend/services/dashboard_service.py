from typing import Optional
from uuid import UUID
from sqlalchemy.orm import Session
from sqlalchemy import func, cast, String as SAString
from backend.db.models import User, ScrapeRun, ScrapeRunInput, StatusEnum

# Status strings written by the Celery pipeline and the ORM path respectively.
_SUCCESS_STATUSES  = ("SUCCESS", "PASS")
_FAILURE_STATUSES  = ("FAIL", "FAILURE")
_PENDING_STATUSES  = ("PENDING", "pending", "PROCESSING", "processing")


def _status_in(column, values: tuple):
    return cast(column, SAString).in_(values)


def get_system_metrics(db: Session, vehicle_number: Optional[str] = None) -> dict:
    # Total uploaded = ScrapeRunInput rows (one per car)
    total_cars = db.query(func.count(ScrapeRunInput.id)).scalar() or 0

    # Success / fail / pending counts come from ScrapeRun.status
    total_success = db.query(func.count(ScrapeRun.run_id)).filter(
        _status_in(ScrapeRun.status, _SUCCESS_STATUSES)
    ).scalar() or 0

    total_failed = db.query(func.count(ScrapeRun.run_id)).filter(
        _status_in(ScrapeRun.status, _FAILURE_STATUSES)
    ).scalar() or 0

    total_pending = db.query(func.count(ScrapeRun.run_id)).filter(
        _status_in(ScrapeRun.status, _PENDING_STATUSES)
    ).scalar() or 0

    total_runs = db.query(func.count(ScrapeRun.run_id)).scalar() or 0
    total_users = db.query(User).filter(User.is_active == True).count()

    return {
        "total_uploaded_cars": total_cars,
        "scraped_successfully": total_success,
        "total_failed": total_failed,
        "total_pending": total_pending,
        "total_batches": total_runs,
        "total_users": total_users,
    }


def get_user_metrics(db: Session, user_id: UUID) -> dict:
    runs = db.query(ScrapeRun).filter(ScrapeRun.user_id == user_id).all()
    run_ids = [r.run_id for r in runs]

    total_cars = 0
    total_success = 0
    total_failed = 0

    if run_ids:
        total_cars = db.query(func.count(ScrapeRunInput.id)).filter(
            ScrapeRunInput.run_id.in_(run_ids)
        ).scalar() or 0

        total_success = db.query(func.count(ScrapeRun.run_id)).filter(
            ScrapeRun.run_id.in_(run_ids),
            _status_in(ScrapeRun.status, _SUCCESS_STATUSES),
        ).scalar() or 0

        total_failed = db.query(func.count(ScrapeRun.run_id)).filter(
            ScrapeRun.run_id.in_(run_ids),
            _status_in(ScrapeRun.status, _FAILURE_STATUSES),
        ).scalar() or 0

    return {
        "my_upload_count": len(runs),
        "my_total_cars": total_cars,
        "my_success": total_success,
        "my_failed": total_failed,
    }


def get_user_admin_stats(db: Session, user: "User") -> dict:
    """Build admin panel stats for a single user."""
    runs = db.query(ScrapeRun).filter(ScrapeRun.user_id == user.id).all()
    run_ids = [r.run_id for r in runs]

    total_cars = 0
    success_count = 0
    fail_count = 0
    last_upload = None

    if run_ids:
        total_cars = db.query(func.count(ScrapeRunInput.id)).filter(
            ScrapeRunInput.run_id.in_(run_ids)
        ).scalar() or 0

        success_count = db.query(func.count(ScrapeRun.run_id)).filter(
            ScrapeRun.run_id.in_(run_ids),
            _status_in(ScrapeRun.status, _SUCCESS_STATUSES),
        ).scalar() or 0

        fail_count = db.query(func.count(ScrapeRun.run_id)).filter(
            ScrapeRun.run_id.in_(run_ids),
            _status_in(ScrapeRun.status, _FAILURE_STATUSES),
        ).scalar() or 0

        last_upload = db.query(func.max(ScrapeRun.created_at)).filter(
            ScrapeRun.user_id == user.id
        ).scalar()

    return {
        "id": str(user.id),
        "name": user.name,
        "email": user.email,
        "role": user.role.value if hasattr(user.role, "value") else user.role,
        "is_active": user.is_active,
        "created_at": user.created_at,
        "total_uploads": len(runs),
        "total_cars_uploaded": total_cars,
        "success_count": success_count,
        "fail_count": fail_count,
        "last_upload_date": last_upload,
    }
