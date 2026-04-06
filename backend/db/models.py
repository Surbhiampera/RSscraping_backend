import uuid
import enum
from datetime import datetime

from sqlalchemy import (
    BigInteger, Column, String, Integer, ForeignKey,
    DateTime, Date, Enum, JSON, Text, Boolean,
    CheckConstraint, func, Float, UniqueConstraint, Index
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from backend.db.session import Base




# ========================
# ENUMS
# ========================

class RoleEnum(str, enum.Enum):
    ADMIN = "ADMIN"
    USER = "USER"


class StatusEnum(str, enum.Enum):
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    PROCESSING = "PROCESSING"
    PASS_ = "PASS"
    FAIL = "FAIL"
    CANCELLED = "CANCELLED"


class StepStatusEnum(str, enum.Enum):
    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


# ========================
# USER
# ========================

class User(Base):
    __tablename__ = "users"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    name = Column(String(255), nullable=False)  # changed from username
    email = Column(String(255), nullable=False, unique=True)
    password_hash = Column(String(255), nullable=False)

    role = Column(String(20), nullable=False, default="USER")
    is_active = Column(Boolean, nullable=False, default=True)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    scrape_runs = relationship(
        "ScrapeRun",
        back_populates="user",
        cascade="all, delete-orphan"
    )

    __table_args__ = (
        CheckConstraint("role IN ('USER', 'ADMIN')", name="chk_user_role"),
    )
class UserSession(Base):
    __tablename__ = "user_sessions"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    session_token_hash = Column(String, nullable=False)
    expires_at = Column(DateTime(timezone=True), nullable=False)
    is_revoked = Column(Boolean, default=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    user = relationship("User", backref="sessions")
    
class PasswordResetToken(Base):
    __tablename__ = "password_reset_tokens"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    token_hash = Column(String, nullable=False)
    expires_at = Column(DateTime(timezone=True), nullable=False)
    is_used = Column(Boolean, default=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    user = relationship("User", backref="password_tokens")

class UploadedFile(Base):
    __tablename__ = "uploaded_files"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    )
    filename = Column(String(255), nullable=False)
    filetype = Column(String(50), nullable=False)
    filesize = Column(BigInteger, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    user = relationship("User", backref="uploaded_files")
# ========================
# SCRAPE RUN
# ========================

class ScrapeRun(Base):
    __tablename__ = "scrape_runs"

    run_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    user_id = Column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )

    status = Column(
        String(100),
        default="PENDING",
        nullable=False
    )

    total_rows = Column(Integer, default=0)
    valid_rows = Column(Integer, default=0)
    invalid_rows = Column(Integer, default=0)
    total_inputs = Column(Integer, default=0)
    success_count = Column(Integer, default=0)
    fail_count = Column(Integer, default=0)

    started_at = Column(DateTime, nullable=True)
    ended_at = Column(DateTime, nullable=True)
    total_duration_ms = Column(Integer, nullable=True)

    notes = Column(Text, nullable=True)

    created_at = Column(
        DateTime,
        default=datetime.utcnow,
        nullable=False
    )

    updated_at = Column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False
    )

    # Relationships
    user = relationship("User", back_populates="scrape_runs")

    inputs = relationship(
        "ScrapeRunInput",
        back_populates="scrape_run",
        cascade="all, delete-orphan"
    )

    car_infos = relationship(
        "CarInfo",
        back_populates="scrape_run",
        cascade="all, delete-orphan"
    )

    quotes_details = relationship(
        "QuotesDetail",
        back_populates="scrape_run",
        cascade="all, delete-orphan"
    )

    final_data = relationship(
        "FinalData",
        back_populates="scrape_run",
        uselist=False,
        cascade="all, delete-orphan"
    )

    flat_outputs = relationship(
        "FinalFlatOutput",
        back_populates="scrape_run",
        cascade="all, delete-orphan"
    )

    run_logs = relationship(
        "RunLog",
        back_populates="scrape_run",
        cascade="all, delete-orphan"
    )

    akamai_events = relationship(
        "AkamaiEvent",
        back_populates="scrape_run",
        cascade="all, delete-orphan"
    )

    data_usages = relationship(
        "ScrapeDataUsage",
        back_populates="scrape_run",
        cascade="all, delete-orphan"
    )
# ========================
# SCRAPE INPUT
# ========================

class ScrapeRunInput(Base):
    __tablename__ = "scrape_run_inputs"


    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4
    )

    run_id = Column(
        UUID(as_uuid=True),
        ForeignKey("scrape_runs.run_id", ondelete="CASCADE"),
        nullable=False
    )

    car_number = Column(
        String(50),
        nullable=False
    )

    is_valid = Column(
        Boolean,
        default=True
    )

    policy_expiry = Column(
        String(50),
        nullable=True
    )

    claim_status = Column(
        String(100),
        nullable=True
    )

    phone = Column(
        String(20),
        nullable=True
    )

    customer_name = Column(
        String(255),
        nullable=True
    )

    created_at = Column(
        DateTime,
        default=datetime.utcnow,
        nullable=False
    )

    updated_at = Column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False
    )

    # Relationship with parent run
    scrape_run = relationship(
        "ScrapeRun",
        back_populates="inputs"
    )

# ========================
# CAR INFO
# ========================

class CarInfo(Base):
    __tablename__ = "car_info"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    run_id = Column(
        UUID(as_uuid=True),
        ForeignKey("scrape_runs.run_id", ondelete="CASCADE"),
        nullable=False,
    )

    registration_number = Column(String(50), nullable=False)
    make_name = Column(String(255))
    model_name = Column(String(255))
    vehicle_variant = Column(String(255))
    fuel_type = Column(String(100))
    cubic_capacity = Column(Integer)
    state_code = Column(String(10))
    city_tier = Column(String(20))
    car_age = Column(Integer)
    registration_date = Column(Date)

    status = Column(Enum(StatusEnum), default=StatusEnum.PENDING, nullable=False)
    error_message = Column(Text)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    scrape_run = relationship("ScrapeRun", back_populates="car_infos")
    
    
# ========================
# SCRAPE DATA USAGE
# ========================

class ScrapeDataUsage(Base):
    __tablename__ = "scrape_data_usage"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    run_id = Column(
        UUID(as_uuid=True),
        ForeignKey("scrape_runs.run_id", ondelete="CASCADE"),
        nullable=False,
    )

    phase = Column(String(50), nullable=False)
    category = Column(String(100), nullable=False)

    call_count = Column(Integer, nullable=False, default=0)
    request_bytes = Column(BigInteger, nullable=False, default=0)
    response_bytes = Column(BigInteger, nullable=False, default=0)
    total_bytes = Column(BigInteger, nullable=False, default=0)

    request_size = Column(String(20), nullable=False, default="0 B")
    response_size = Column(String(20), nullable=False, default="0 B")
    total_size = Column(String(20), nullable=False, default="0 B")

    top_urls = Column(JSON, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow)

    scrape_run = relationship("ScrapeRun", back_populates="data_usages")

    __table_args__ = (
        UniqueConstraint("run_id", "phase", "category", name="uq_data_usage_run_phase_cat"),
    )
# ========================
# RUN LOG
# ========================

class RunLog(Base):
    __tablename__ = "run_logs"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    run_id = Column(
        UUID(as_uuid=True),
        ForeignKey("scrape_runs.run_id", ondelete="CASCADE"),
        nullable=False,
    )

    step_number = Column(Integer, nullable=False)
    step_key = Column(String(100), nullable=False)
    status = Column(String(20))

    start_ts = Column(DateTime(timezone=True))
    end_ts = Column(DateTime(timezone=True))
    duration_ms = Column(Integer)

    data = Column(JSON)

    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)

    scrape_run = relationship("ScrapeRun", back_populates="run_logs")
    akamai_events = relationship(
        "AkamaiEvent",
        back_populates="run_log",
        cascade="all, delete-orphan"
    )
# ========================
# FINAL DATA (JSON AGGREGATED)
# ========================

class FinalData(Base):
    __tablename__ = "final_data"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    run_id = Column(
        UUID(as_uuid=True),
        ForeignKey("scrape_runs.run_id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    )

    final_data = Column(JSON, nullable=False)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    scrape_run = relationship("ScrapeRun", back_populates="final_data")


# ========================
# FINAL FLAT OUTPUT
# ========================


class FinalFlatOutput(Base):
    __tablename__ = "final_flat_output"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    run_id = Column(
        UUID(as_uuid=True),
        ForeignKey("scrape_runs.run_id", ondelete="CASCADE"),
        nullable=False,
    )

    flat_output = Column(JSON, nullable=False)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    scrape_run = relationship("ScrapeRun", back_populates="flat_outputs")

# ========================
# AKAMAI EVENT
# ========================

class AkamaiEvent(Base):
    __tablename__ = "akamai_events"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    run_id = Column(
        UUID(as_uuid=True),
        ForeignKey("scrape_runs.run_id", ondelete="CASCADE"),
        nullable=False,
    )

    log_id = Column(
        BigInteger,
        ForeignKey("run_logs.id", ondelete="CASCADE")
    )

    step_after = Column(Integer)
    step_key_after = Column(String(100))
    event_timestamp = Column(DateTime(timezone=True), nullable=False)

    data = Column(JSON)

    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)

    scrape_run = relationship("ScrapeRun", back_populates="akamai_events")
    run_log = relationship("RunLog", back_populates="akamai_events")

# ========================
# QUOTES DETAIL
# ========================

class QuotesDetail(Base):
    __tablename__ = "quotes_details"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    run_id = Column(UUID(as_uuid=True), ForeignKey("scrape_runs.run_id", ondelete="CASCADE"), nullable=False)

    insurer_name = Column(String(255))
    plan_id = Column(BigInteger, nullable=False)
    plan_json = Column(JSON, nullable=False)
    addon_combo_id = Column(BigInteger, nullable=False)
    idv_type = Column(String(100))
    idv_selected = Column(BigInteger)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    scrape_run = relationship("ScrapeRun", back_populates="quotes_details")