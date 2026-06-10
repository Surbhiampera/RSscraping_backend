import json
import uuid
from typing import Optional

from fastapi import APIRouter, UploadFile, File, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import exists
from pydantic import BaseModel

from backend import db
from backend.db.session import get_db
from backend.db.models import (
    ScrapeRun,
    ScrapeRunInput,
    StatusEnum,
    User,
    UploadedFile,
    QuotesUrlRecord,
)
from backend.core.dependencies import get_current_user
from backend.upload_parser.main import (
    parse_upload,
    parse_manual_car,
    parse_manual_no_reg,
    UPLOAD_MODE_WITH_REG,
    UPLOAD_MODE_WITHOUT_REG,
)
from backend.services.upload_storage import save_upload_bytes

from backend.schemas.batch import (
    UploadPreviewRow,
    UploadResponse,
)
from sqlalchemy import insert
from backend.schemas.common import APIResponse


class SingleCarRequest(BaseModel):
    car_number: str


class SingleNoRegRequest(BaseModel):
    rto_location: str
    make: str
    model: str
    variant: Optional[str] = None
    cc: Optional[str] = None
    cc_range: Optional[str] = None
    fuel_type: Optional[str] = None
    insured_company: Optional[str] = None
    ncb_percent: Optional[str] = None
    tariff_rate: Optional[str] = None
    yom: Optional[str] = None


router = APIRouter()


def _build_scrape_run_rows(
    preview_rows: list,
    upload_mode: str,
    user_id,
) -> tuple[list[dict], list[dict], list]:
    """
    Convert parsed preview rows into (scrape_run_dicts, scrape_run_input_dicts, run_ids).
    Pre-generates UUIDs so all inserts can be batched without per-row flushes.
    Scales to any number of rows.
    Per-row flow is inferred from input_data presence (None → with_reg, dict → without_reg)
    so mixed-mode uploads ("auto") work without extra branching.
    """
    seen        = set()
    run_dicts   = []
    input_dicts = []
    run_ids     = []

    for idx, row in enumerate(preview_rows):
        row_input_data = row.get("input_data")  # None → with_reg, dict → without_reg

        if row_input_data:
            input_data     = row_input_data if isinstance(row_input_data, dict) else {}
            make_slug      = (input_data.get("make",  "") or "")[:8].replace(" ", "")
            model_slug     = (input_data.get("model", "") or "")[:8].replace(" ", "")
            car_number     = f"NOREG{idx:05d}_{make_slug}_{model_slug}"[:50]
            input_data_str = json.dumps(input_data)
        else:
            car_number     = (row.get("car_number") or "").strip().upper()
            input_data_str = None

        if not car_number:
            continue
        # Deduplicate only registration-number rows
        if row_input_data is None and car_number in seen:
            continue
        if row_input_data is None:
            seen.add(car_number)

        run_id = uuid.uuid4()
        run_dicts.append({
            "run_id":       run_id,
            "user_id":      user_id,
            "status":       StatusEnum.PENDING,
            "total_inputs": 1,
        })
        run_ids.append(run_id)
        input_dicts.append({
            "run_id":        run_id,
            "car_number":    car_number,
            "customer_name": row.get("customer_name"),
            "phone":         row.get("phone"),
            "policy_expiry": row.get("policy_expiry"),
            "claim_status":  row.get("claim_status"),
            "is_valid":      row.get("is_valid", False),
            "input_data":    input_data_str,
        })

    return run_dicts, input_dicts, run_ids


@router.post("", response_model=APIResponse[UploadResponse], status_code=status.HTTP_201_CREATED)
async def upload_file(
    file: UploadFile = File(...),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Bulk upload (Mode 1 or 2) — auto-detects with-reg vs without-reg format."""

    filename = file.filename or ""
    ext      = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""

    if ext not in ("xlsx", "xls", "csv", "pdf"):
        raise HTTPException(status_code=400, detail="Unsupported file type")

    file_content = await file.read()
    file_size    = len(file_content)

    try:
        parsed = parse_upload(file_content, filename)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    if not parsed or parsed.get("total_rows", 0) == 0:
        raise HTTPException(status_code=400, detail="No data found")

    uploaded_file = UploadedFile(
        user_id=current_user.id,
        filename=filename,
        filetype=ext,
        filesize=file_size,
    )
    db.add(uploaded_file)
    db.flush()

    try:
        save_upload_bytes(uploaded_file.id, ext, file_content)
    except Exception:
        pass

    preview_rows = parsed.get("preview", [])
    upload_mode  = parsed.get("upload_mode", UPLOAD_MODE_WITH_REG)

    run_dicts, input_dicts, run_ids = _build_scrape_run_rows(
        preview_rows, upload_mode, current_user.id
    )

    # Batch insert all ScrapeRun rows — one flush instead of N flushes
    for rd in run_dicts:
        db.add(ScrapeRun(**rd))
    db.flush()

    # Batch insert all ScrapeRunInput rows in a single execute
    if input_dicts:
        db.execute(insert(ScrapeRunInput), input_dicts)

    db.commit()

    preview  = [UploadPreviewRow(**r) for r in preview_rows]
    metadata = parsed.get("metadata")
    quality  = parsed.get("quality")

    return APIResponse(
        data=UploadResponse(
            run_id=str(run_ids[-1]) if run_ids else "00000000-0000-0000-0000-000000000000",
            run_ids=[str(r) for r in run_ids],
            file_id=uploaded_file.id,
            original_filename=filename,
            total_rows=parsed.get("total_rows", 0),
            valid_rows=sum(1 for r in preview_rows if r.get("is_valid")),
            invalid_rows=sum(1 for r in preview_rows if not r.get("is_valid")),
            header_present=parsed.get("header_present", False),
            schema_mapping=parsed.get("schema", {}),
            metadata=metadata,
            quality=quality,
            preview=preview,
            upload_mode=upload_mode,
        ),
        message=f"{len(run_ids)} run(s) created ({upload_mode})",
    )


@router.post("/single", response_model=APIResponse[UploadResponse], status_code=status.HTTP_201_CREATED)
async def upload_single_car(
    body: SingleCarRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Mode 3 — Entre car registration number."""

    parsed = parse_manual_car(body.car_number)
    row    = parsed["preview"][0]

    run_id = uuid.uuid4()
    db.add(ScrapeRun(
        run_id=run_id,
        user_id=current_user.id,
        status=StatusEnum.PENDING,
        total_inputs=1,
    ))
    db.flush()

    db.execute(
        insert(ScrapeRunInput),
        [{
            "run_id":     run_id,
            "car_number": row["car_number"],
            "is_valid":   row["is_valid"],
        }]
    )
    db.commit()

    preview = [UploadPreviewRow(
        car_number=row["car_number"],
        is_valid=row["is_valid"],
        error=row.get("error"),
    )]

    return APIResponse(
        data=UploadResponse(
            run_id=str(run_id),
            run_ids=[str(run_id)],
            file_id=None,
            original_filename=row["car_number"],
            total_rows=1,
            valid_rows=parsed["valid_rows"],
            invalid_rows=parsed["invalid_rows"],
            header_present=False,
            schema_mapping={},
            metadata=None,
            quality=None,
            preview=preview,
            upload_mode=UPLOAD_MODE_WITH_REG,
        ),
        message="Single car run created",
    )


@router.post("/single-no-reg", response_model=APIResponse[UploadResponse], status_code=status.HTTP_201_CREATED)
async def upload_single_no_reg(
    body: SingleNoRegRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Mode 4 — Entre without car registration number."""

    raw_data = {
        "make":            body.make,
        "model":           body.model,
        "rto_location":    body.rto_location,
        "variant":         body.variant,
        "cc":              body.cc,
        "cc_range":        body.cc_range,
        "fuel_type":       body.fuel_type,
        "insured_company": body.insured_company,
        "ncb_percent":     body.ncb_percent,
        "tariff_rate":     body.tariff_rate,
        "yom":             body.yom,
    }

    parsed = parse_manual_no_reg(raw_data)
    row    = parsed["preview"][0]

    if not row["is_valid"]:
        raise HTTPException(status_code=400, detail=row["error"] or "Missing required fields")

    input_data  = row.get("input_data", {})
    make_slug   = (input_data.get("make",  "") or "")[:8].replace(" ", "")
    model_slug  = (input_data.get("model", "") or "")[:8].replace(" ", "")
    car_number  = f"NOREG00000_{make_slug}_{model_slug}".upper()[:50]

    run_id = uuid.uuid4()
    db.add(ScrapeRun(
        run_id=run_id,
        user_id=current_user.id,
        status=StatusEnum.PENDING,
        total_inputs=1,
    ))
    db.flush()

    db.execute(
        insert(ScrapeRunInput),
        [{
            "run_id":     run_id,
            "car_number": car_number,
            "is_valid":   True,
            "input_data": json.dumps(input_data),
        }]
    )
    db.commit()

    preview = [UploadPreviewRow(
        car_number=row["car_number"],
        is_valid=True,
        input_data=input_data,
    )]

    return APIResponse(
        data=UploadResponse(
            run_id=str(run_id),
            run_ids=[str(run_id)],
            file_id=None,
            original_filename=row["car_number"],
            total_rows=1,
            valid_rows=1,
            invalid_rows=0,
            header_present=False,
            schema_mapping={},
            metadata=None,
            quality=None,
            preview=preview,
            upload_mode=UPLOAD_MODE_WITHOUT_REG,
        ),
        message="Single no-reg run created",
    )


class FromDbRequest(BaseModel):
    limit: int
    keys: Optional[list[str]] = None  # when provided, create runs only for these profile_unique_keys


@router.get("/from-db/preview")
def preview_from_db(
    limit: int = 100,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Read-only preview of pending quotes_url records.
    Applies the same deduplication filter as POST /from-db but creates nothing.
    """
    if limit < 1:
        raise HTTPException(status_code=400, detail="limit must be at least 1")

    done_keys_subquery = (
        db.query(ScrapeRunInput.profile_unique_key)
        .join(ScrapeRun, ScrapeRun.run_id == ScrapeRunInput.run_id)
        .filter(
            ScrapeRun.status == StatusEnum.PASS_,
            ScrapeRunInput.profile_unique_key.isnot(None),
        )
        .subquery()
    )

    records = (
        db.query(QuotesUrlRecord)
        .filter(
            QuotesUrlRecord.profile_unique_key.notin_(
                db.query(done_keys_subquery.c.profile_unique_key)
            )
        )
        .limit(limit)
        .all()
    )

    return APIResponse(
        success=True,
        message=f"{len(records)} record(s) available",
        data={
            "records": [
                {
                    "profile_unique_key": rec.profile_unique_key,
                    "rto_location":       rec.rto_location,
                    "rto_code":           rec.rto_code,
                    "make":               rec.make,
                    "model":              rec.model,
                    "variant":            rec.variant,
                    "cc":                 rec.cc,
                    "cc_range":           rec.cc_range,
                    "fuel_type":          rec.fuel_type,
                    "earned_ncb_percent": rec.earned_ncb_percent,
                    "yom_age":            rec.yom_age,
                    "quotes_url":         rec.quotes_url,
                }
                for rec in records
            ],
            "total": len(records),
        },
    )


@router.post("/from-db", response_model=APIResponse, status_code=status.HTTP_201_CREATED)
def upload_from_db(
    body: FromDbRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Create ScrapeRun + ScrapeRunInput rows by fetching records directly from
    the quotes_url table instead of an Excel upload.

    Deduplication: records whose profile_unique_key already appears in a
    successfully completed ScrapeRunInput are excluded, so the same vehicle
    profile is never re-queued after a successful scrape.

    Returns run_ids so the frontend can call POST /api/scrape/start/{run_id}
    using the exact same Start Scraping flow as after a file upload.
    """
    if body.limit < 1:
        raise HTTPException(status_code=400, detail="limit must be at least 1")

    # Subquery: profile_unique_keys that have already been successfully processed.
    # A record is considered done when its ScrapeRun reached PASS status.
    done_keys_subquery = (
        db.query(ScrapeRunInput.profile_unique_key)
        .join(ScrapeRun, ScrapeRun.run_id == ScrapeRunInput.run_id)
        .filter(
            ScrapeRun.status == StatusEnum.PASS_,
            ScrapeRunInput.profile_unique_key.isnot(None),
        )
        .subquery()
    )

    base_query = db.query(QuotesUrlRecord).filter(
        QuotesUrlRecord.profile_unique_key.notin_(
            db.query(done_keys_subquery.c.profile_unique_key)
        )
    )

    if body.keys:
        records = base_query.filter(
            QuotesUrlRecord.profile_unique_key.in_(body.keys)
        ).all()
    else:
        records = base_query.limit(body.limit).all()

    if not records:
        raise HTTPException(
            status_code=404,
            detail="No unprocessed records found in quotes_url table",
        )

    run_dicts   = []
    input_dicts = []
    run_ids     = []

    for rec in records:
        input_data = {}
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
        idx        = len(run_ids)
        car_number = f"NOREG{idx:05d}_{make_slug}_{model_slug}"[:50]

        run_id = uuid.uuid4()
        run_dicts.append({
            "run_id":       run_id,
            "user_id":      current_user.id,
            "status":       StatusEnum.PENDING,
            "total_inputs": 1,
        })
        input_dicts.append({
            "run_id":            run_id,
            "car_number":        car_number,
            "is_valid":          True,
            "input_data":        json.dumps(input_data),
            "profile_unique_key": rec.profile_unique_key,
        })
        run_ids.append(run_id)

    for rd in run_dicts:
        db.add(ScrapeRun(**rd))
    db.flush()

    if input_dicts:
        db.execute(insert(ScrapeRunInput), input_dicts)

    db.commit()

    return APIResponse(
        success=True,
        message=f"{len(run_ids)} run(s) created from database",
        data={
            "run_ids":    [str(r) for r in run_ids],
            "total_runs": len(run_ids),
            "source":     "database",
        },
    )
