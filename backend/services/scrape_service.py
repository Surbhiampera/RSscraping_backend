from datetime import datetime
from uuid import UUID
from sqlalchemy.orm import Session
from backend.db.models import ScrapeRun, CarInfo, StatusEnum
from backend.db.models import ScrapeRun, ScrapeRunInput
from backend.task_sender import send_scrape_row


def start_scrape_run(db: Session, run_id: UUID):
    """Start run + send one task per input"""

    run = db.query(ScrapeRun).filter(ScrapeRun.run_id == run_id).first()
    if not run:
        raise ValueError(f"ScrapeRun {run_id} not found")

    # ✅ Base query (NO .all())
    query = db.query(ScrapeRunInput).filter(
        ScrapeRunInput.run_id == run_id,
        ScrapeRunInput.is_valid == True
    )

    total_inputs = query.count()
    if total_inputs == 0:
        raise ValueError("No valid inputs found")

    # ✅ Update run
    run.status = StatusEnum.PROCESSING
    run.started_at = datetime.utcnow()
    run.total_inputs = total_inputs
    db.commit()

    # ✅ Send tasks (streaming)
    task_ids = []

    for inp in query.yield_per(50):
        task_id = send_scrape_row(str(run_id), inp)

        if task_id:
            inp.task_id = task_id
            task_ids.append(task_id)

    db.commit()

    return {
        "run": run,
        "task_ids": task_ids,
        "total": total_inputs
    }

def complete_scrape_run(db: Session, run_id: UUID, success: bool = True) -> ScrapeRun:
    run = db.query(ScrapeRun).filter(ScrapeRun.run_id == run_id).first()
    if not run:
        raise ValueError(f"ScrapeRun {run_id} not found")

    run.ended_at = datetime.utcnow()
    if run.started_at:
        run.total_duration_ms = int((run.ended_at - run.started_at).total_seconds() * 1000)

    cars = db.query(CarInfo).filter(CarInfo.run_id == run_id).all()
    run.success_count = sum(1 for c in cars if c.status == StatusEnum.PASS_)
    run.fail_count = sum(1 for c in cars if c.status == StatusEnum.FAIL)
    run.status = StatusEnum.PASS_ if success else StatusEnum.FAIL

    db.commit()
    db.refresh(run)
    return run


def cancel_scrape_run(db: Session, run_id: UUID) -> ScrapeRun:
    run = db.query(ScrapeRun).filter(ScrapeRun.run_id == run_id).first()
    if not run:
        raise ValueError(f"ScrapeRun {run_id} not found")

    run.status = StatusEnum.CANCELLED
    run.ended_at = datetime.utcnow()
    db.commit()
    db.refresh(run)
    return run


def retry_failed_cars(db: Session, run_id: UUID) -> int:
    """Reset all FAIL cars in a run back to PENDING. Returns count reset."""
    cars = db.query(CarInfo).filter(
        CarInfo.run_id == run_id,
        CarInfo.status == StatusEnum.FAIL,
    ).all()
    for car in cars:
        car.status = StatusEnum.PENDING
        car.error_message = None

    run = db.query(ScrapeRun).filter(ScrapeRun.run_id == run_id).first()
    if run:
        run.status = StatusEnum.PROCESSING
        run.fail_count = 0

    db.commit()
    return len(cars)


def retry_single_car(db: Session, car_id: UUID) -> CarInfo:
    car = db.query(CarInfo).filter(CarInfo.id == car_id).first()
    if not car:
        raise ValueError(f"CarInfo {car_id} not found")
    car.status = StatusEnum.PENDING
    car.error_message = None
    db.commit()
    db.refresh(car)
    return car
