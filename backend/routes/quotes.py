from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List

from backend.db.session import get_db
from backend.db.models import FinalFlatOutput, ScrapeRun, User
from backend.core.dependencies import get_current_user
from backend.schemas.common import APIResponse

router = APIRouter()

CAR_IDENTITY_KEYS = ("Make", "Model", "Variant", "Rto Location", "YOM", "CC", "Fuel Type")


def _car_identity(plan: dict) -> str:
    """Build a stable string key that identifies a unique car."""
    return "|".join(str(plan.get(k) or "") for k in CAR_IDENTITY_KEYS)


def _get_all_flat_rows(db: Session, user_id) -> list[dict]:
    """Collect every plan dict from FinalFlatOutput rows belonging to the user."""
    run_ids = [
        r.run_id for r in db.query(ScrapeRun.run_id)
        .filter(ScrapeRun.user_id == user_id).all()
    ]
    if not run_ids:
        return []

    db_rows = db.query(FinalFlatOutput).filter(
        FinalFlatOutput.run_id.in_(run_ids)
    ).all()

    plans: list[dict] = []
    for r in db_rows:
        items = r.flat_output if isinstance(r.flat_output, list) else [r.flat_output]
        plans.extend(items)
    return plans


@router.get("/cars", response_model=APIResponse[List[dict]])
def list_cars(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """List unique cars available for quote comparison."""
    plans = _get_all_flat_rows(db, current_user.id)

    seen: dict[str, dict] = {}
    for p in plans:
        key = _car_identity(p)
        if key not in seen:
            seen[key] = {
                "car_key": key,
                "make": p.get("Make"),
                "model": p.get("Model"),
                "variant": p.get("Variant"),
                "fuel_type": p.get("Fuel Type"),
                "yom": p.get("YOM"),
                "rto_location": p.get("Rto Location"),
            }

    return APIResponse(data=list(seen.values()))


@router.get("/compare", response_model=APIResponse[List[dict]])
def compare_quotes(
    car_key: str = Query(..., description="Pipe-separated car identity string"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Get all insurer quotes for a specific car, sorted by Final Premium ascending."""
    plans = _get_all_flat_rows(db, current_user.id)

    QUOTE_KEYS = [
        "Company", "IDV", "Net Premium", "Final Premium", "NCB %", "IDV Type",
        "Nil Dep Premium", "EP Premium", "RTI Premium", "RSA Premium",
        "Consumables", "Key & Lock Replacement", "Tyre Protector",
        "Loss of Personal Belongings", "Emergency Transport and Hotel Allowance",
        "Daily Allowance", "NCB Protector",
    ]

    matched = []
    for p in plans:
        if _car_identity(p) != car_key:
            continue
        row = {k: p.get(k) for k in QUOTE_KEYS}
        matched.append(row)

    def _premium_sort_key(row: dict):
        val = row.get("Final Premium")
        try:
            return float(val)
        except (TypeError, ValueError):
            return float("inf")

    matched.sort(key=_premium_sort_key)

    return APIResponse(data=matched)
