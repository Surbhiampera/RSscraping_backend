import json
import uuid
from typing import Optional

from fastapi import APIRouter, UploadFile, File, Depends, HTTPException, status
from sqlalchemy.orm import Session
from pydantic import BaseModel

from backend import db
from backend.db.session import get_db
from backend.db.models import (
    ScrapeRun,
    ScrapeRunInput,
    StatusEnum,
    User,
    UploadedFile,
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
    """
    seen        = set()
    run_dicts   = []
    input_dicts = []
    run_ids     = []

    for idx, row in enumerate(preview_rows):
        if upload_mode == UPLOAD_MODE_WITH_REG:
            car_number     = (row.get("car_number") or "").strip().upper()
            input_data_str = None
        else:
            input_data     = row.get("input_data") or {}
            make_slug      = (input_data.get("make",  "") or "")[:8].replace(" ", "")
            model_slug     = (input_data.get("model", "") or "")[:8].replace(" ", "")
            car_number     = f"NOREG{idx:05d}_{make_slug}_{model_slug}"[:50]
            input_data_str = json.dumps(input_data)

        if not car_number:
            continue
        if upload_mode == UPLOAD_MODE_WITH_REG and car_number in seen:
            continue
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
