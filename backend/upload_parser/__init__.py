from .main import (
    # Bulk file upload (Modes 1 & 2 — auto-detected)
    parse_upload,
    detect_upload_type,

    # Manual entry (Mode 3 — single car registration)
    parse_manual_car,

    # Manual entry (Mode 4 — single structured record)
    parse_manual_no_reg,

    # Mode constants
    UPLOAD_MODE_WITH_REG,
    UPLOAD_MODE_WITHOUT_REG,
    UPLOAD_MODE_AUTO,
    NO_REG_REQUIRED_FIELDS,

    # Validation helpers
    validate_car_number,
    clean_car_number,
)
