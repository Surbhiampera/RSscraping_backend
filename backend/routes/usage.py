from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func
from uuid import UUID

from backend.db.session import get_db
from backend.db.models import ScrapeDataUsage, ScrapeRun, User
from backend.core.dependencies import get_current_user
from backend.schemas.common import APIResponse

router = APIRouter()


@router.get("", response_model=APIResponse)
def get_usage_summary(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Aggregated data usage summary for the current user."""
    user_run_ids = [
        r.run_id for r in db.query(ScrapeRun.run_id)
        .filter(ScrapeRun.user_id == current_user.id).all()
    ]

    if not user_run_ids:
        return APIResponse(data={
            "total_calls": 0,
            "total_request_bytes": 0,
            "total_response_bytes": 0,
            "total_bytes": 0,
            "per_run": [],
        })

    totals = db.query(
        func.sum(ScrapeDataUsage.call_count).label("total_calls"),
        func.sum(ScrapeDataUsage.request_bytes).label("total_request_bytes"),
        func.sum(ScrapeDataUsage.response_bytes).label("total_response_bytes"),
        func.sum(ScrapeDataUsage.total_bytes).label("total_bytes"),
    ).filter(ScrapeDataUsage.run_id.in_(user_run_ids)).first()

    per_run_rows = db.query(ScrapeDataUsage).filter(
        ScrapeDataUsage.run_id.in_(user_run_ids)
    ).order_by(ScrapeDataUsage.created_at.desc()).all()

    per_run = [
        {
            "run_id": str(row.run_id),
            "phase": row.phase,
            "category": row.category,
            "call_count": row.call_count,
            "total_size": row.total_size,
            "created_at": row.created_at.isoformat() if row.created_at else None,
        }
        for row in per_run_rows
    ]

    return APIResponse(data={
        "total_calls": totals.total_calls or 0,
        "total_request_bytes": totals.total_request_bytes or 0,
        "total_response_bytes": totals.total_response_bytes or 0,
        "total_bytes": totals.total_bytes or 0,
        "per_run": per_run,
    })


@router.get("/{run_id}", response_model=APIResponse)
def get_run_usage(
    run_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Detailed data usage for a specific run."""
    run = db.query(ScrapeRun).filter(
        ScrapeRun.run_id == run_id,
        ScrapeRun.user_id == current_user.id,
    ).first()

    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    rows = db.query(ScrapeDataUsage).filter(
        ScrapeDataUsage.run_id == run_id
    ).order_by(ScrapeDataUsage.created_at.desc()).all()

    data = [
        {
            "id": row.id,
            "run_id": str(row.run_id),
            "phase": row.phase,
            "category": row.category,
            "call_count": row.call_count,
            "request_bytes": row.request_bytes,
            "response_bytes": row.response_bytes,
            "total_bytes": row.total_bytes,
            "request_size": row.request_size,
            "response_size": row.response_size,
            "total_size": row.total_size,
            "top_urls": row.top_urls,
            "created_at": row.created_at.isoformat() if row.created_at else None,
        }
        for row in rows
    ]

    return APIResponse(data=data)
