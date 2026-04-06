from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session
from sqlalchemy import func
from uuid import UUID
from typing import Optional

from backend.db.session import get_db
from backend.db.models import ScrapeRun, ScrapeRunInput, User
from backend.core.dependencies import get_current_user
from backend.schemas.common import APIResponse, PaginatedResponse, PaginationMeta

router = APIRouter()


@router.get("", response_model=PaginatedResponse)
def list_runs(
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    status: Optional[str] = Query(None),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """List all scrape runs for the current user with pagination."""
    query = db.query(ScrapeRun).filter(ScrapeRun.user_id == current_user.id)

    if status:
        query = query.filter(ScrapeRun.status == status)

    total = query.count()
    runs = (
        query.order_by(ScrapeRun.created_at.desc())
        .offset((page - 1) * limit)
        .limit(limit)
        .all()
    )

    run_ids = [r.run_id for r in runs]
    inputs = (
        db.query(ScrapeRunInput.run_id, ScrapeRunInput.car_number)
        .filter(ScrapeRunInput.run_id.in_(run_ids))
        .all()
    ) if run_ids else []

    car_numbers_map: dict[UUID, list[str]] = {}
    for inp in inputs:
        car_numbers_map.setdefault(inp.run_id, []).append(inp.car_number)

    data = []
    for r in runs:
        data.append({
            "run_id": str(r.run_id),
            "status": r.status if r.status else None,
            "total_inputs": r.total_inputs,
            "success_count": r.success_count,
            "fail_count": r.fail_count,
            "started_at": r.started_at.isoformat() if r.started_at else None,
            "ended_at": r.ended_at.isoformat() if r.ended_at else None,
            "total_duration_ms": r.total_duration_ms,
            "created_at": r.created_at.isoformat() if r.created_at else None,
            "car_numbers": car_numbers_map.get(r.run_id, []),
        })

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
def get_run_detail(
    run_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Get detail of a single scrape run including its inputs."""
    run = db.query(ScrapeRun).filter(
        ScrapeRun.run_id == run_id,
        ScrapeRun.user_id == current_user.id,
    ).first()

    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    inputs = db.query(ScrapeRunInput).filter(
        ScrapeRunInput.run_id == run_id,
    ).all()

    return APIResponse(data={
        "run_id": str(run.run_id),
        "status": run.status if run.status else None,
        "total_inputs": run.total_inputs,
        "success_count": run.success_count,
        "fail_count": run.fail_count,
        "started_at": run.started_at.isoformat() if run.started_at else None,
        "ended_at": run.ended_at.isoformat() if run.ended_at else None,
        "total_duration_ms": run.total_duration_ms,
        "created_at": run.created_at.isoformat() if run.created_at else None,
        "notes": run.notes,
        "inputs": [
            {
                "car_number": inp.car_number,
                "is_valid": inp.is_valid,
            }
            for inp in inputs
        ],
    })
