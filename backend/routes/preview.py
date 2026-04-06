from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from uuid import UUID

from backend.core.dependencies import get_current_user
from backend.db.session import get_db
from backend.db.models import User, UploadedFile
from backend.schemas.common import APIResponse
from backend.schemas.preview import PreviewTable
from backend.services.table_preview import build_table_preview
from backend.services.upload_storage import find_upload_path


router = APIRouter()


@router.get("/{file_id}", response_model=APIResponse[PreviewTable])
def get_file_preview(
    file_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    # Ensure file exists and belongs to user (DB is the source of truth for ownership)
    record = (
        db.query(UploadedFile)
        .filter(UploadedFile.id == file_id, UploadedFile.user_id == current_user.id)
        .first()
    )
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="File not found")

    path = find_upload_path(file_id)
    if not path:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="File content not found on server")

    try:
        preview = build_table_preview(path.read_bytes(), record.filename)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

    return APIResponse[PreviewTable](data=PreviewTable(**preview))
