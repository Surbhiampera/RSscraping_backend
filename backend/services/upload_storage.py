from __future__ import annotations

from pathlib import Path
from uuid import UUID


_BACKEND_DIR = Path(__file__).resolve().parents[1]  # backend/
UPLOAD_DIR = _BACKEND_DIR / "storage" / "uploads"


def ensure_upload_dir() -> Path:
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    return UPLOAD_DIR


def build_upload_path(file_id: UUID, ext: str) -> Path:
    safe_ext = (ext or "").lower().lstrip(".")
    return ensure_upload_dir() / f"{file_id}.{safe_ext}"


def save_upload_bytes(file_id: UUID, ext: str, content: bytes) -> Path:
    path = build_upload_path(file_id, ext)
    path.write_bytes(content)
    return path


def load_upload_bytes(file_id: UUID, ext: str) -> bytes:
    path = build_upload_path(file_id, ext)
    return path.read_bytes()


def find_upload_path(file_id: UUID) -> Path | None:
    """
    Find an upload on disk regardless of extension.
    Useful for preview endpoints when only file_id is known.
    """
    ensure_upload_dir()
    matches = sorted(UPLOAD_DIR.glob(f"{file_id}.*"))
    return matches[0] if matches else None