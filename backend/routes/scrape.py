import logging
from typing import List
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy.orm import Session
from uuid import UUID

logger = logging.getLogger(__name__)

from backend.db.session import get_db
from backend.db.models import ScrapeRun, ScrapeRunInput, CarInfo, RunLog, StatusEnum, User
from backend.core.dependencies import get_current_user
from backend.services.scrape_service import (
    start_scrape_run,
    cancel_scrape_run,
    retry_failed_cars,
    retry_single_car,
)

from backend.task_sender import send_scrape_row, send_scrape_run, get_task_status

from backend.schemas.batch import ScrapeRunOut
from backend.schemas.common import APIResponse

router = APIRouter()


# Utility to extract celery task id from notes
def _extract_celery_task_id(notes: str | None) -> str | None:
    if not notes:
        return None
    for part in notes.split():
        if part.startswith("celery_task_id="):
            return part.split("=", 1)[1] or None
    return None


# =========================
# START SCRAPE
# =========================
@router.post("/start/{run_id}", response_model=APIResponse[ScrapeRunOut])
def start_scrape(
    run_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Start scraping for a given run."""

    # ✅ Fetch run
    run = db.query(ScrapeRun).filter(
        ScrapeRun.run_id == run_id,
        ScrapeRun.user_id == current_user.id,
    ).first()

    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    # ✅ Validate status
    if run.status not in (StatusEnum.PENDING, StatusEnum.FAIL, StatusEnum.CANCELLED):
        raise HTTPException(
            status_code=400,
            detail=f"Cannot start scraping. Current status: {run.status}",
        )

    # ✅ Base query (NO .all())
    base_query = db.query(ScrapeRunInput).filter(
        ScrapeRunInput.run_id == run_id,
        ScrapeRunInput.is_valid == True
    )

    # ✅ Count only (efficient)
    total_inputs = base_query.count()

    if total_inputs == 0:
        raise HTTPException(status_code=400, detail="No valid car inputs found")

    # ✅ Update run
    # ✅ Start scrape run (updates DB status/timestamps + sends tasks)
    result = start_scrape_run(db, run_id)
    task_ids = result["task_ids"]

    logger.info(f"✅ Total tasks pushed: {len(task_ids)}/{total_inputs} for run {run_id}")

    # ✅ Store metadata
    run = db.query(ScrapeRun).filter(ScrapeRun.run_id == run_id).first()
    run.notes = f"task_count={len(task_ids)}"
    db.commit()
    db.refresh(run)

    # ✅ Response formatting (Pydantic v2)
    out = ScrapeRunOut.model_validate(run)
    out.status = run.status.value if isinstance(run.status, StatusEnum) else run.status

    return APIResponse(
        success=True,
        message=f"Scraping started for {total_inputs} cars",
        data=out,
    )
# =========================
# START BATCH SCRAPE
# =========================
class BatchStartRequest(BaseModel):
    run_ids: List[UUID]


@router.post("/start-batch")
def start_scrape_batch(
    body: BatchStartRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Start scraping for multiple runs at once."""
    results = []
    for rid in body.run_ids:
        run = db.query(ScrapeRun).filter(
            ScrapeRun.run_id == rid,
            ScrapeRun.user_id == current_user.id,
        ).first()

        if not run or run.status not in (StatusEnum.PENDING, StatusEnum.FAIL, StatusEnum.CANCELLED):
            continue

        inputs = db.query(ScrapeRunInput).filter(
            ScrapeRunInput.run_id == rid,
            ScrapeRunInput.is_valid == True
        ).all()

        if not inputs:
            continue

        result = start_scrape_run(db, rid)
        logger.info(f"📤 Started run {rid} with {result['total']} tasks")

        results.append(str(rid))

    return APIResponse(
        success=True,
        message=f"Started {len(results)} runs",
        data={"started_run_ids": results},
    )


# =========================
# CANCEL SCRAPE
# =========================
@router.post("/cancel/{run_id}", response_model=APIResponse[ScrapeRunOut])
def cancel_scrape(
    run_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Cancel an in-progress scraping run."""

    run = db.query(ScrapeRun).filter(
        ScrapeRun.run_id == run_id,
        ScrapeRun.user_id == current_user.id,
    ).first()

    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    if run.status != StatusEnum.PROCESSING:
        raise HTTPException(
            status_code=400,
            detail="Only in-progress runs can be cancelled",
        )

    run = cancel_scrape_run(db, run_id)

    db.refresh(run)
    out = ScrapeRunOut.model_validate(run)
    out.status = run.status.value if isinstance(run.status, StatusEnum) else run.status

    return APIResponse(data=out, message="Scraping cancelled")


# =========================
# GET SCRAPE STATUS
# =========================
@router.get("/status/{run_id}", response_model=APIResponse)
def get_scrape_status(
    run_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Get run progress + Celery status"""

    run = db.query(ScrapeRun).filter(
        ScrapeRun.run_id == run_id,
        ScrapeRun.user_id == current_user.id,
    ).first()

    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    cars = db.query(CarInfo).filter(CarInfo.run_id == run_id).all()

    run.success_count = sum(1 for c in cars if c.status == StatusEnum.PASS_)
    run.fail_count = sum(1 for c in cars if c.status == StatusEnum.FAIL)

    db.commit()
    db.refresh(run)

    celery_status = None
    task_id = _extract_celery_task_id(run.notes)
    if task_id:
        status_info = get_task_status(task_id)
        celery_status = status_info["state"]

    return APIResponse(
        data={
            "run_id": run.run_id,
            "status": run.status.value if isinstance(run.status, StatusEnum) else run.status,
            "success_count": run.success_count,
            "fail_count": run.fail_count,
            "celery_status": celery_status,
        }
    )


# =========================
# RETRY FAILED CARS
# =========================
@router.post("/retry/{run_id}", response_model=APIResponse)
def retry_failed(
    run_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Retry all failed cars"""

    run = db.query(ScrapeRun).filter(ScrapeRun.run_id == run_id).first()

    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    count = retry_failed_cars(db, run_id)

    if count == 0:
        return APIResponse(message="No failed cars to retry")

    # Re-read the cars that were reset to PENDING
    failed_inputs = (
        db.query(ScrapeRunInput)
        .filter(ScrapeRunInput.run_id == run_id, ScrapeRunInput.is_valid == True)
        .all()
    )
    cars = [
        {
            "car_number": inp.car_number,
            "customer_name": inp.customer_name,
            "phone": inp.phone,
            "policy_expiry": inp.policy_expiry,
            "claim_status": inp.claim_status,
        }
        for inp in failed_inputs
    ]
    queued = send_scrape_run(str(run_id), cars)

    if not queued:
        raise HTTPException(status_code=500, detail="Failed to queue retry tasks")

    run.notes = f"task_count={len(queued)}"
    db.commit()

    return APIResponse(
        data={"retried_count": count, "tasks_queued": len(queued)},
        message=f"{count} failed car(s) queued for retry",
    )


# =========================
# RETRY SINGLE CAR
# =========================
@router.post("/retry-car/{car_id}", response_model=APIResponse)
def retry_single(
    car_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Retry a single failed car"""

    car = db.query(CarInfo).filter(CarInfo.id == car_id).first()

    if not car:
        raise HTTPException(status_code=404, detail="Car not found")

    run = db.query(ScrapeRun).filter(
        ScrapeRun.run_id == car.run_id,
        ScrapeRun.user_id == current_user.id,
    ).first()

    if not run:
        raise HTTPException(status_code=403, detail="Access denied")

    if car.status != StatusEnum.FAIL:
        raise HTTPException(status_code=400, detail="Only failed cars can be retried")

    retry_single_car(db, car_id)

    queued = send_scrape_run(str(car.run_id), [{
        "car_number": car.registration_number,
    }])

    if not queued:
        raise HTTPException(status_code=500, detail="Failed to queue retry task")

    db.commit()

    return APIResponse(
        data={"task_id": queued[0]["task_id"]},
        message=f"Car {car.registration_number} queued for retry",
    )


# =========================
# RUN LOGS
# =========================
@router.get("/logs/{run_id}", response_model=APIResponse)
def get_run_logs(
    run_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Get detailed run logs"""

    run = db.query(ScrapeRun).filter(
        ScrapeRun.run_id == run_id,
        ScrapeRun.user_id == current_user.id,
    ).first()

    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    logs = db.query(RunLog).filter(
        RunLog.run_id == run_id,
    ).order_by(RunLog.step_number.asc()).all()

    return APIResponse(
        data=[
            {
                "id": log.id,
                "step_number": log.step_number,
                "step_key": log.step_key,
                "status": log.status,
                "start_ts": log.start_ts.isoformat() if log.start_ts else None,
                "end_ts": log.end_ts.isoformat() if log.end_ts else None,
                "duration_ms": log.duration_ms,
                "data": log.data,
            }
            for log in logs
        ]
    )