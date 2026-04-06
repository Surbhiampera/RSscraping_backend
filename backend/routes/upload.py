from fastapi import APIRouter, UploadFile, File, Depends, HTTPException, status
from sqlalchemy.orm import Session

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
from backend.upload_parser.main import parse_upload
from backend.services.upload_storage import save_upload_bytes

from backend.schemas.batch import (
    UploadPreviewRow,
    UploadResponse,
)
from sqlalchemy import insert
from backend.schemas.common import APIResponse

router = APIRouter()


@router.post("", response_model=APIResponse[UploadResponse], status_code=status.HTTP_201_CREATED)
async def upload_file(
    file: UploadFile = File(...),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Upload file → create ONE run per car"""

    filename = file.filename or ""
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""

    if ext not in ("xlsx", "xls", "csv", "pdf"):
        raise HTTPException(status_code=400, detail="Unsupported file type")

    # ---------------------------
    # Read file
    # ---------------------------
    file_content = await file.read()
    file_size = len(file_content)

    # ---------------------------
    # Parse file
    # ---------------------------
    try:
        parsed = parse_upload(file_content, filename)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    if not parsed or parsed.get("total_rows", 0) == 0:
        raise HTTPException(status_code=400, detail="No data found")

    # ---------------------------
    # Save Uploaded File
    # ---------------------------
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

    # ---------------------------
    # Process rows
    # ---------------------------
    preview_rows = parsed.get("preview", [])

    seen = set()
    run_ids = []

    for row in preview_rows:
        car_number = (row.get("car_number") or "").strip().upper()

        # ❌ skip empty / duplicate
        if not car_number or car_number in seen:
            continue

        seen.add(car_number)

        # ✅ Create ONE run per car
        run = ScrapeRun(
            user_id=current_user.id,
            status=StatusEnum.PENDING,
            total_inputs=1,
        )
        db.add(run)
        db.flush()  # get run_id
        run_ids.append(run.run_id)

        # ✅ Bulk-style insert (dict, NOT ORM object)
        db.execute(
            insert(ScrapeRunInput),
            [{
                "run_id": run.run_id,
                "car_number": car_number,
                "customer_name": row.get("customer_name"),
                "phone": row.get("phone"),
                "policy_expiry": row.get("policy_expiry"),
                "claim_status": row.get("claim_status"),
                "is_valid": row.get("is_valid", False),
            }]
        )

    # ✅ Single commit at end (important)
    db.commit()

    # ---------------------------
    # Preview
    # ---------------------------
    preview = [UploadPreviewRow(**r) for r in preview_rows]

    # ---------------------------
    # Safe metadata & quality
    # ---------------------------
    metadata = parsed.get("metadata")
    quality = parsed.get("quality")

    # If parser doesn't return them → keep None (requires Optional in model)
    # DO NOT send {} (causes validation error)

    return APIResponse(
        data=UploadResponse(
            run_id=str(run_ids[-1]),
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
        ),
        message=f"1 run created with {len(preview)} car inputs",
    )