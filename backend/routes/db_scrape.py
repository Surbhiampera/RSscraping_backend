import json
import uuid
from datetime import datetime, timedelta
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import and_, distinct, insert, or_
from sqlalchemy.orm import Session

from backend.core.dependencies import get_current_user
from backend.db.models import (
    QuotesUrlV3Record,
    ScrapeRun,
    ScrapeRunInput,
    StatusEnum,
    User,
)
from backend.db.session import get_db
from backend.schemas.common import APIResponse
from backend.services.scrape_service import start_scrape_run

router = APIRouter()


def _today_utc_window():
    """Return (start_of_today_utc, start_of_tomorrow_utc) for date-bounded queries."""
    today_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    return today_start, today_start + timedelta(days=1)


# ========================
# GET /api/db-scrape/options
# ========================

@router.get("/options", response_model=APIResponse)
def get_filter_options(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Return distinct values currently present in quotes_url_v3 for every dropdown filter.
    Only non-null values are included. Results are sorted alphabetically.
    """

    def distinct_values(col):
        rows = db.query(distinct(col)).filter(col.isnot(None)).order_by(col).all()
        return [row[0] for row in rows]

    return APIResponse(
        success=True,
        message="Filter options loaded",
        data={
            "make":                  distinct_values(QuotesUrlV3Record.make),
            "model":                 distinct_values(QuotesUrlV3Record.model),
            "ncb_percent":           distinct_values(QuotesUrlV3Record.earned_ncb_percent),
            "rto_code":              distinct_values(QuotesUrlV3Record.rto_code),
            "rs_present":            distinct_values(QuotesUrlV3Record.rs_present),
            "profile_unique_key":    distinct_values(QuotesUrlV3Record.profile_unique_key),
            "yom":                   distinct_values(QuotesUrlV3Record.yom_age),
            "profile_identifier_key": distinct_values(QuotesUrlV3Record.profile_identifier_key),
            "status":                distinct_values(QuotesUrlV3Record.status),
        },
    )


# ========================
# GET /api/db-scrape/records
# ========================

@router.get("/records", response_model=APIResponse)
def list_db_records(
    make: Optional[str] = Query(None),
    model: Optional[str] = Query(None),
    ncb_percent: Optional[List[str]] = Query(None, description="Filter by NCB%: 0, 25, 35"),
    rto_code: Optional[str] = Query(None),
    rs_present: Optional[bool] = Query(None),
    profile_unique_key: Optional[str] = Query(None),
    yom: Optional[str] = Query(None),
    profile_identifier_key: Optional[str] = Query(None),
    status: Optional[str] = Query(None, description="pending | inprogress | completed"),
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=1000),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Return all records from quotes_url_v3 with optional filtering and pagination."""

    query = db.query(QuotesUrlV3Record)

    if make:
        query = query.filter(QuotesUrlV3Record.make == make)
    if model:
        query = query.filter(QuotesUrlV3Record.model == model)
    if ncb_percent:
        query = query.filter(QuotesUrlV3Record.earned_ncb_percent.in_(ncb_percent))
    if rto_code:
        query = query.filter(QuotesUrlV3Record.rto_code == rto_code)
    if rs_present is not None:
        query = query.filter(QuotesUrlV3Record.rs_present == rs_present)
    if profile_unique_key:
        query = query.filter(QuotesUrlV3Record.profile_unique_key == profile_unique_key)
    if yom:
        query = query.filter(QuotesUrlV3Record.yom_age == yom)
    if profile_identifier_key:
        query = query.filter(QuotesUrlV3Record.profile_identifier_key == profile_identifier_key)
    if status:
        query = query.filter(QuotesUrlV3Record.status == status)

    total = query.count()
    offset = (page - 1) * page_size
    records = query.offset(offset).limit(page_size).all()

    return APIResponse(
        success=True,
        message=f"{total} record(s) found",
        data={
            "records": [_serialize_record(r) for r in records],
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": (total + page_size - 1) // page_size,
        },
    )


# ========================
# POST /api/db-scrape/validate
# ========================

class ValidateRequest(BaseModel):
    profile_unique_keys: List[str]


@router.post("/validate", response_model=APIResponse)
def validate_scrape(
    body: ValidateRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Check which selected profiles have already been scraped or are in progress today.
    Returns two lists: already_done (with identity details) and safe_to_scrape.
    """
    if not body.profile_unique_keys:
        raise HTTPException(status_code=400, detail="No profile_unique_keys provided")

    # Check scrape_runs (via scrape_run_inputs) for actual run status.
    # Block: PENDING (queued), PROCESSING (running), SUCCESS today (already done today).
    # Allow: SUCCESS before today, FAILURE, TIMEOUT, or no run at all.
    today_start, tomorrow_start = _today_utc_window()
    blocked_rows = (
        db.query(ScrapeRunInput.profile_unique_key, ScrapeRun.status)
        .join(ScrapeRun, ScrapeRunInput.run_id == ScrapeRun.run_id)
        .filter(
            ScrapeRunInput.profile_unique_key.in_(body.profile_unique_keys),
            ScrapeRunInput.profile_unique_key.isnot(None),
            or_(
                ScrapeRun.status.in_(["PENDING", "PROCESSING"]),
                and_(
                    ScrapeRun.status == "SUCCESS",
                    ScrapeRun.ended_at >= today_start,
                    ScrapeRun.ended_at < tomorrow_start,
                ),
            ),
        )
        .all()
    )

    # Deduplicate per key — keep highest-priority status (PROCESSING > PENDING > SUCCESS)
    _priority = {"PROCESSING": 3, "PENDING": 2, "SUCCESS": 1}
    blocked_key_status = {}
    for row in blocked_rows:
        key = row.profile_unique_key
        if key not in blocked_key_status or _priority.get(row.status, 0) > _priority.get(blocked_key_status[key], 0):
            blocked_key_status[key] = row.status

    blocked_key_set = set(blocked_key_status.keys())

    # Fetch v3 identity details (make/model/rto etc.) for blocked keys
    blocked_v3 = (
        db.query(QuotesUrlV3Record)
        .filter(QuotesUrlV3Record.profile_unique_key.in_(blocked_key_set))
        .all()
    ) if blocked_key_set else []

    already_done = [
        {
            "profile_unique_key": r.profile_unique_key,
            "make":               r.make,
            "model":              r.model,
            "rto_code":           r.rto_code,
            "earned_ncb_percent": r.earned_ncb_percent,
            "yom_age":            r.yom_age,
            "status":             blocked_key_status.get(r.profile_unique_key, ""),
        }
        for r in blocked_v3
    ]

    safe_to_scrape = [k for k in body.profile_unique_keys if k not in blocked_key_set]

    return APIResponse(
        success=True,
        message=f"{len(already_done)} conflict(s) found, {len(safe_to_scrape)} safe to scrape",
        data={
            "already_done":   already_done,
            "safe_to_scrape": safe_to_scrape,
            "has_conflicts":  len(already_done) > 0,
        },
    )


# ========================
# POST /api/db-scrape/start
# ========================

class StartDbScrapeRequest(BaseModel):
    profile_unique_keys: List[str]


@router.post("/start", response_model=APIResponse, status_code=201)
def start_db_scrape(
    body: StartDbScrapeRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Create ScrapeRun rows for the selected profiles and immediately start scraping.

    Returns 409 if any of the submitted keys already have an active/completed run
    today — the response body includes identity details (make/model/rto/ncb%/yom)
    for every conflicting record so the frontend can display a proper warning.

    Reuses the exact same start_scrape_run pipeline as the Excel-upload flow.
    """
    if not body.profile_unique_keys:
        raise HTTPException(status_code=400, detail="No profile_unique_keys provided")

    # Check scrape_runs (via scrape_run_inputs) for actual run status.
    # Block: PENDING, PROCESSING, SUCCESS today. Allow: SUCCESS before today, FAILURE, TIMEOUT.
    today_start, tomorrow_start = _today_utc_window()
    blocked_rows = (
        db.query(ScrapeRunInput.profile_unique_key, ScrapeRun.status)
        .join(ScrapeRun, ScrapeRunInput.run_id == ScrapeRun.run_id)
        .filter(
            ScrapeRunInput.profile_unique_key.in_(body.profile_unique_keys),
            ScrapeRunInput.profile_unique_key.isnot(None),
            or_(
                ScrapeRun.status.in_(["PENDING", "PROCESSING"]),
                and_(
                    ScrapeRun.status == "SUCCESS",
                    ScrapeRun.ended_at >= today_start,
                    ScrapeRun.ended_at < tomorrow_start,
                ),
            ),
        )
        .all()
    )

    _priority = {"PROCESSING": 3, "PENDING": 2, "SUCCESS": 1}
    blocked_key_status = {}
    for row in blocked_rows:
        key = row.profile_unique_key
        if key not in blocked_key_status or _priority.get(row.status, 0) > _priority.get(blocked_key_status[key], 0):
            blocked_key_status[key] = row.status

    blocked_key_set = set(blocked_key_status.keys())

    # Fetch v3 identity details for blocked keys to return in 409 body
    blocked_v3 = (
        db.query(QuotesUrlV3Record)
        .filter(QuotesUrlV3Record.profile_unique_key.in_(blocked_key_set))
        .all()
    ) if blocked_key_set else []

    blocked_records = blocked_v3

    if blocked_records:
        conflicts = [
            {
                "profile_unique_key": r.profile_unique_key,
                "make":               r.make,
                "model":              r.model,
                "rto_code":           r.rto_code,
                "earned_ncb_percent": r.earned_ncb_percent,
                "yom_age":            r.yom_age,
                "status":             blocked_key_status.get(r.profile_unique_key, ""),
            }
            for r in blocked_records
        ]
        raise HTTPException(
            status_code=409,
            detail={
                "message": (
                    f"{len(conflicts)} profile(s) are blocked "
                    f"({', '.join(sorted({r['status'] for r in conflicts}))}). "
                    "Remove them from your selection and try again."
                ),
                "conflicts": conflicts,
            },
        )

    # Fetch the v3 records for the selected keys
    records = (
        db.query(QuotesUrlV3Record)
        .filter(QuotesUrlV3Record.profile_unique_key.in_(body.profile_unique_keys))
        .all()
    )

    if not records:
        raise HTTPException(status_code=404, detail="No matching records found in quotes_url_v3")

    run_dicts   = []
    input_dicts = []
    run_ids     = []

    for idx, rec in enumerate(records):
        input_data: dict = {}
        if rec.rto_location:
            input_data["rto_location"] = rec.rto_location
        if rec.rto_code:
            input_data["rto_code"] = rec.rto_code
        if rec.make:
            input_data["make"] = rec.make
        if rec.model:
            input_data["model"] = rec.model
        if rec.variant:
            input_data["variant"] = rec.variant
        if rec.fuel_type:
            input_data["fuel_type"] = rec.fuel_type
        if rec.earned_ncb_percent:
            input_data["ncb_percent"] = rec.earned_ncb_percent
        if rec.yom_age:
            input_data["yom"] = rec.yom_age
        if rec.quotes_url:
            input_data["quotes_url"] = rec.quotes_url

        make_slug  = (rec.make  or "")[:8].replace(" ", "")
        model_slug = (rec.model or "")[:8].replace(" ", "")
        car_number = f"NOREG{idx:05d}_{make_slug}_{model_slug}"[:50]

        run_id = uuid.uuid4()
        run_dicts.append({
            "run_id":                 run_id,
            "user_id":                current_user.id,
            "status":                 StatusEnum.PENDING,
            "total_inputs":           1,
            "profile_unique_key":     rec.profile_unique_key,
            "profile_identifier_key": rec.profile_identifier_key,
        })
        input_dicts.append({
            "run_id":                 run_id,
            "car_number":             car_number,
            "is_valid":               True,
            "input_data":             json.dumps(input_data),
            "profile_unique_key":     rec.profile_unique_key,
            "profile_identifier_key": rec.profile_identifier_key,
        })
        run_ids.append(run_id)

    for rd in run_dicts:
        db.add(ScrapeRun(**rd))
    db.flush()

    if input_dicts:
        db.execute(insert(ScrapeRunInput), input_dicts)

    db.commit()

    # Auto-start every run immediately (same pipeline as Excel-upload flow)
    started_run_ids = []
    for run_id in run_ids:
        try:
            start_scrape_run(db, run_id)
            started_run_ids.append(str(run_id))
        except Exception:
            pass

    return APIResponse(
        success=True,
        message=f"{len(started_run_ids)} run(s) created and started from quotes_url_v3",
        data={
            "run_ids":     started_run_ids,
            "total_runs":  len(started_run_ids),
            "source":      "quotes_url_v3",
        },
    )


# ========================
# HELPERS
# ========================

def _serialize_record(r: QuotesUrlV3Record) -> dict:
    return {
        "id":                    r.id,
        "rto_location":          r.rto_location,
        "rto_code":              r.rto_code,
        "make":                  r.make,
        "model":                 r.model,
        "variant":               r.variant,
        "cc":                    r.cc,
        "cc_range":              r.cc_range,
        "fuel_type":             r.fuel_type,
        "earned_ncb_percent":    r.earned_ncb_percent,
        "yom_age":               r.yom_age,
        "quotes_url":            r.quotes_url,
        "rs_present":            r.rs_present,
        "remarks":               r.remarks,
        "profile_unique_key":    r.profile_unique_key,
        "profile_identifier_key": r.profile_identifier_key,
        "status":                r.status,
        "created_at":            r.created_at.isoformat() if r.created_at else None,
        "updated_at":            r.updated_at.isoformat() if r.updated_at else None,
    }
