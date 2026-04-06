from typing import Optional
from uuid import UUID
from sqlalchemy.orm import Session
from sqlalchemy import func
from backend.db.models import User, ScrapeRun, CarInfo, StatusEnum


def get_system_metrics(db: Session, vehicle_number: Optional[str] = None) -> dict:
    car_query = db.query(CarInfo)
    if vehicle_number:
        car_query = car_query.filter(CarInfo.registration_number == vehicle_number.upper())

    total_cars = car_query.count()
    total_success = car_query.filter(CarInfo.status == StatusEnum.PASS_).count()
    total_failed = car_query.filter(CarInfo.status == StatusEnum.FAIL).count()
    total_pending = car_query.filter(
        CarInfo.status.in_([StatusEnum.PENDING, StatusEnum.PROCESSING])
    ).count()
    total_runs = db.query(ScrapeRun).count()
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
        total_cars = db.query(CarInfo).filter(CarInfo.run_id.in_(run_ids)).count()
        total_success = db.query(CarInfo).filter(
            CarInfo.run_id.in_(run_ids), CarInfo.status == StatusEnum.PASS_
        ).count()
        total_failed = db.query(CarInfo).filter(
            CarInfo.run_id.in_(run_ids), CarInfo.status == StatusEnum.FAIL
        ).count()

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
        total_cars = db.query(CarInfo).filter(CarInfo.run_id.in_(run_ids)).count()
        success_count = db.query(CarInfo).filter(
            CarInfo.run_id.in_(run_ids), CarInfo.status == StatusEnum.PASS_
        ).count()
        fail_count = db.query(CarInfo).filter(
            CarInfo.run_id.in_(run_ids), CarInfo.status == StatusEnum.FAIL
        ).count()
        latest = db.query(func.max(ScrapeRun.created_at)).filter(
            ScrapeRun.user_id == user.id
        ).scalar()
        last_upload = latest

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
