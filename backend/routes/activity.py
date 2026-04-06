from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from uuid import UUID

from backend.db.session import get_db
from backend.db.models import RunLog, ScrapeRun, User
from backend.core.dependencies import get_current_user
from backend.schemas.common import APIResponse, PaginatedResponse, PaginationMeta

router = APIRouter()


@router.get("", response_model=PaginatedResponse)
def get_activity_logs(
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=100),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Get recent activity logs across all of the current user's runs."""
    base_query = (
        db.query(RunLog)
        .join(ScrapeRun, ScrapeRun.run_id == RunLog.run_id)
        .filter(ScrapeRun.user_id == current_user.id)
    )

    total = base_query.count()

    logs = (
        base_query
        .order_by(RunLog.created_at.desc())
        .offset((page - 1) * limit)
        .limit(limit)
        .all()
    )

    data = [
        {
            "id": log.id,
            "run_id": str(log.run_id),
            "step_number": log.step_number,
            "step_key": log.step_key,
            "status": log.status,
            "start_ts": log.start_ts,
            "end_ts": log.end_ts,
            "duration_ms": log.duration_ms,
            "created_at": log.created_at,
        }
        for log in logs
    ]

    return PaginatedResponse(
        data=data,
        pagination=PaginationMeta(
            page=page,
            limit=limit,
            total=total,
            total_pages=(total + limit - 1) // limit if total else 0,
        ),
    )


@router.get("/{run_id}", response_model=APIResponse)
def get_run_logs(
    run_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Get all logs for a specific run."""
    run = db.query(ScrapeRun).filter(
        ScrapeRun.run_id == run_id,
        ScrapeRun.user_id == current_user.id,
    ).first()

    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    logs = (
        db.query(RunLog)
        .filter(RunLog.run_id == run_id)
        .order_by(RunLog.step_number.asc())
        .all()
    )

    data = [
        {
            "id": log.id,
            "run_id": str(log.run_id),
            "step_number": log.step_number,
            "step_key": log.step_key,
            "status": log.status,
            "start_ts": log.start_ts,
            "end_ts": log.end_ts,
            "duration_ms": log.duration_ms,
            "created_at": log.created_at,
        }
        for log in logs
    ]

    return APIResponse(data=data)
