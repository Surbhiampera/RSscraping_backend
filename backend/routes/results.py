from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
from uuid import UUID
from typing import Optional, List
import io

from backend.db.session import get_db
from backend.db.models import CarInfo, ScrapeRun, QuotesDetail, FinalData, FinalFlatOutput, StatusEnum, User
from backend.core.dependencies import get_current_user
from backend.schemas.car import CarInfoOut, FlatOutputOut, ResultRow, ExportRequest, ExportFormat, ExportFilter
from backend.schemas.common import APIResponse, PaginatedResponse, PaginationMeta
from backend.services.export_service import export_to_csv, export_to_json, rows_to_flat_dicts

router = APIRouter()


CAR_IDENTITY_KEYS = ("Make", "Model", "Variant", "Rto Location", "YOM", "CC", "Fuel Type")


def _car_identity(plan: dict) -> str:
    """Build a stable string key that identifies a unique car."""
    return "|".join(str(plan.get(k) or "") for k in CAR_IDENTITY_KEYS)


def _get_user_flat_rows(db: Session, car_key: str | None = None) -> list:
    """Get all flat_output plan rows, optionally filtered by car_key."""
    db_rows = db.query(FinalFlatOutput).all()

    all_plans: list = []
    for r in db_rows:
        items = r.flat_output if isinstance(r.flat_output, list) else [r.flat_output]
        all_plans.extend(items)

    if car_key is not None:
        all_plans = [p for p in all_plans if _car_identity(p) == car_key]

    return all_plans


@router.get("", response_model=PaginatedResponse[FlatOutputOut])
def get_results(
    run_id: UUID = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    search: str = Query(None, description="Search across flat_output fields"),
    sort_by: str = Query("created_at"),
    sort_dir: str = Query("desc"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Get paginated scraping results from FinalFlatOutput table.

    Each FinalFlatOutput row stores flat_output as a JSON **list** of dicts.
    We explode these into individual table rows for the frontend.
    """

    query = db.query(FinalFlatOutput)

    if run_id:
        query = query.filter(FinalFlatOutput.run_id == run_id)

    if sort_dir == "asc":
        query = query.order_by(FinalFlatOutput.created_at.asc())
    else:
        query = query.order_by(FinalFlatOutput.created_at.desc())

    db_rows = query.all()

    # Addon keys — any premium/add-on column counts as an addon if it has a value
    ADDON_KEYS = [
        "Nil Dep Premium", "EP Premium", "RTI Premium", "RSA Premium",
        "Consumables", "Key & Lock Replacement", "Tyre Protector",
        "Loss of Personal Belongings", "Emergency Transport and Hotel Allowance",
        "Daily Allowance", "NCB Protector",
    ]

    # Group all plans by car identity across all FinalFlatOutput rows
    all_items = []
    sr_counter = 1
    for r in db_rows:
        items = r.flat_output if isinstance(r.flat_output, list) else [r.flat_output]

        groups: dict = {}
        for item in items:
            key = _car_identity(item)
            if key not in groups:
                groups[key] = []
            groups[key].append(item)

        for car_key, plan_rows in groups.items():
            first = plan_rows[0]
            addon_count = 0
            for plan in plan_rows:
                addon_count += sum(
                    1 for k in ADDON_KEYS
                    if plan.get(k) and str(plan.get(k)).lower() not in ("", "not included", "null", "none")
                )

            # Unique counts for the TABLE summary only:
            # - Plan Count: unique insurers (Company / Insurer)
            # - Addon Count: unique addon columns that have a real value
            unique_plans = {
                r.get("Company") or r.get("Insurer")
                for r in plan_rows
                if r.get("Company") or r.get("Insurer")
            }
            unique_addons = {
                k
                for r in plan_rows
                for k in ADDON_KEYS
                if r.get(k) and str(r.get(k)).lower() not in ("", "not included", "null", "none")
            }

            summary = {
                            "Sr No": sr_counter,
                "CC": first.get("CC"),
                "IDV": first.get("IDV"),
                "YOM": first.get("YOM"),
                "Make": first.get("Make"),
                "Model": first.get("Model"),
                "NCB %": first.get("NCB %"),
                "Variant": first.get("Variant"),
                "CC Range": first.get("CC Range"),
                "IDV Type": first.get("IDV Type"),
                "Fuel Type": first.get("Fuel Type"),
                "Rto Location": first.get("Rto Location"),

                # UNIQUE COUNTS
                "Plan Count": len(unique_plans),
                "Addon Count": len(unique_addons),

                "car_key": car_key,
            }

            all_items.append({
                "id": str(r.id),
                "run_id": str(r.run_id),
                "flat_output": summary,
                "created_at": r.created_at,
                "updated_at": r.updated_at,
            })
            sr_counter += 1

    # Apply search filter across summary values
    if search:
        term = search.lower()
        all_items = [
            row for row in all_items
            if any(term in str(v).lower() for v in row["flat_output"].values() if v)
        ]

    total = len(all_items)

    # Paginate
    start = (page - 1) * limit
    page_items = all_items[start : start + limit]

    return PaginatedResponse(
        data=page_items,
        pagination=PaginationMeta(
            page=page,
            limit=limit,
            total=total,
            total_pages=(total + limit - 1) // limit if total else 0,
        )
    )

@router.post("/export")
def export_results(
    body: ExportRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Export all insurer plan details as Excel/CSV/JSON."""
    from backend.services.export_service import export_to_excel

    rows = _get_user_flat_rows(db)

    if not rows:
        raise HTTPException(status_code=404, detail="No data to export")

    fmt = body.format.value if hasattr(body.format, "value") else str(body.format)

    if fmt == "csv":
        content = export_to_csv(rows)
        return StreamingResponse(
            io.BytesIO(content),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=results.csv"},
        )
    elif fmt == "json":
        content = export_to_json(rows)
        return StreamingResponse(
            io.BytesIO(content),
            media_type="application/json",
            headers={"Content-Disposition": "attachment; filename=results.json"},
        )
    else:
        content = export_to_excel(rows)
        return StreamingResponse(
            io.BytesIO(content),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": "attachment; filename=results.xlsx"},
        )


@router.get("/export/{ffo_id}")
def export_single_car(
    ffo_id: UUID,
    car_key: str = Query(..., description="Car identity key for filtering"),
    format: str = Query("xlsx"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Download all insurer plan details for a single car."""
    from backend.services.export_service import export_to_excel

    ffo = db.query(FinalFlatOutput).filter(FinalFlatOutput.id == ffo_id).first()
    if not ffo:
        raise HTTPException(status_code=404, detail="Record not found")

    items = ffo.flat_output if isinstance(ffo.flat_output, list) else [ffo.flat_output]
    rows = [p for p in items if _car_identity(p) == car_key] if car_key else items

    if not rows:
        raise HTTPException(status_code=404, detail="No data found for this car")

    fname = f"car_{rows[0].get('Make', '')}_{rows[0].get('Model', '')}"

    if format == "csv":
        content = export_to_csv(rows)
        return StreamingResponse(
            io.BytesIO(content),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={fname}.csv"},
        )
    elif format == "json":
        content = export_to_json(rows)
        return StreamingResponse(
            io.BytesIO(content),
            media_type="application/json",
            headers={"Content-Disposition": f"attachment; filename={fname}.json"},
        )
    else:
        content = export_to_excel(rows)
        return StreamingResponse(
            io.BytesIO(content),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={fname}.xlsx"},
        )


@router.get("/{ffo_id}/raw-json", response_model=APIResponse)
def get_raw_json(
    ffo_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Return the full flat_output JSON for a FinalFlatOutput row."""
    ffo = db.query(FinalFlatOutput).filter(FinalFlatOutput.id == ffo_id).first()
    if not ffo:
        raise HTTPException(status_code=404, detail="Record not found")

    HIDDEN = {"run_id", "plan_hidden"}
    raw = ffo.flat_output
    if isinstance(raw, list):
        cleaned = [{k: v for k, v in item.items() if k not in HIDDEN} for item in raw]
    elif isinstance(raw, dict):
        cleaned = {k: v for k, v in raw.items() if k not in HIDDEN}
    else:
        cleaned = raw
    return APIResponse(data=cleaned)


@router.post("/retry-failed", response_model=APIResponse)
def retry_all_failed(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Retry all failed rows"""

    user_run_ids = [
        r.run_id for r in db.query(ScrapeRun.run_id)
        .filter(ScrapeRun.user_id == current_user.id).all()
    ]

    if not user_run_ids:
        return APIResponse(message="0 jobs restarted")

    failed = db.query(CarInfo).filter(
        CarInfo.run_id.in_(user_run_ids),
        CarInfo.status == StatusEnum.FAIL
    ).all()

    for row in failed:
        row.status = StatusEnum.PENDING

    db.commit()

    return APIResponse(message=f"{len(failed)} jobs restarted")

@router.get("/search", response_model=APIResponse)
def search_results(
    q: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Search across flat_output fields.

    NOTE: We only run the query when the term length is >= 3
    to avoid hammering the DB for each single character typed.
    """
    from sqlalchemy import cast, String as SAString

    term = (q or "").strip()
    if len(term) < 3:
        return APIResponse(data=[])

    user_run_ids = [
        r.run_id for r in db.query(ScrapeRun.run_id)
        .filter(ScrapeRun.user_id == current_user.id).all()
    ]

    if not user_run_ids:
        return APIResponse(data=[])

    rows = db.query(FinalFlatOutput).filter(
        FinalFlatOutput.run_id.in_(user_run_ids),
        cast(FinalFlatOutput.flat_output, SAString).ilike(f"%{term}%")
    ).limit(50).all()

    return APIResponse(data=[FlatOutputOut.model_validate(r) for r in rows])


@router.post("/{car_id}/generate", response_model=APIResponse)
def generate_row_scrape(
    car_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Generate scraping result for selected row"""

    car = db.query(CarInfo).filter(CarInfo.id == car_id).first()

    if not car:
        raise HTTPException(status_code=404, detail="Car not found")

    run = db.query(ScrapeRun).filter(
        ScrapeRun.run_id == car.run_id,
        ScrapeRun.user_id == current_user.id
    ).first()

    if not run:
        raise HTTPException(status_code=403, detail="Access denied")

    # Trigger scraping worker here
    # scrape_vehicle(car)

    car.status = StatusEnum.PROCESSING
    db.commit()

    return APIResponse(message="Scraping started for selected row")


@router.post("/{car_id}/retry", response_model=APIResponse)
def retry_failed(
    car_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Retry scraping for failed record"""

    # Ensure the car belongs to a run owned by the current user
    car = (
        db.query(CarInfo)
        .join(ScrapeRun, ScrapeRun.run_id == CarInfo.run_id)
        .filter(CarInfo.id == car_id, ScrapeRun.user_id == current_user.id)
        .first()
    )

    if not car:
        raise HTTPException(status_code=404, detail="Record not found")

    if car.status != StatusEnum.FAIL:
        raise HTTPException(status_code=400, detail="Only failed rows can be retried")

    car.status = StatusEnum.PENDING
    db.commit()

    return APIResponse(message="Retry started")


