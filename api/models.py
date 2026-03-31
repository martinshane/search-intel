"""
Data models for the Search Intelligence API.

Provides SQLAlchemy ORM models for database tables and Pydantic schemas
for API request/response validation.

Tables:
- users: OAuth-authenticated users
- reports: SEO analysis reports (one per domain per user)
- report_modules: Individual module results within a report
"""

from datetime import datetime
from typing import Optional, Any, Dict, List
from enum import Enum

from pydantic import BaseModel, Field
from sqlalchemy import (
    Column, Integer, String, Text, Boolean, DateTime, Float,
    ForeignKey, JSON, Enum as SAEnum, UniqueConstraint
)
from sqlalchemy.orm import relationship, declarative_base
from sqlalchemy.sql import func

Base = declarative_base()


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class ReportStatus(str, Enum):
    """Status of a report or module run."""
    pending = "pending"
    running = "running"
    completed = "completed"
    failed = "failed"
    cancelled = "cancelled"


class ModuleName(str, Enum):
    """All 12 analysis modules."""
    health_trajectory = "health_trajectory"               # Module 1
    page_triage = "page_triage"                           # Module 2
    serp_landscape = "serp_landscape"                     # Module 3
    content_intelligence = "content_intelligence"         # Module 4
    gameplan = "gameplan"                                  # Module 5
    algorithm_updates = "algorithm_updates"               # Module 6
    intent_migration = "intent_migration"                 # Module 7
    technical_health = "technical_health"                  # Module 8
    site_architecture = "site_architecture"               # Module 9
    branded_split = "branded_split"                        # Module 10
    competitive_threats = "competitive_threats"            # Module 11
    revenue_attribution = "revenue_attribution"            # Module 12


# ---------------------------------------------------------------------------
# SQLAlchemy ORM Models
# ---------------------------------------------------------------------------

class User(Base):
    """
    Authenticated user.

    Populated on first OAuth callback; updated on subsequent logins.
    Stores encrypted Google OAuth tokens for GSC + GA4 access.
    """
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String(255), unique=True, nullable=False, index=True)
    name = Column(String(255), nullable=True)
    picture = Column(Text, nullable=True)

    # Encrypted OAuth tokens (Fernet-encrypted JSON blob)
    google_access_token = Column(Text, nullable=True)
    google_refresh_token = Column(Text, nullable=True)
    token_expires_at = Column(DateTime(timezone=True), nullable=True)

    # GSC / GA4 property selections
    gsc_property = Column(String(512), nullable=True)
    ga4_property_id = Column(String(64), nullable=True)

    # Metadata
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    last_login_at = Column(DateTime(timezone=True), nullable=True)

    # Relationships
    reports = relationship("Report", back_populates="user", cascade="all, delete-orphan")

    def __repr__(self) -> str:
        return f"<User id={self.id} email={self.email!r}>"


class Report(Base):
    """
    A single SEO analysis report for a domain.

    Each report tracks:
    - Which domain is being analyzed
    - Overall status (pending → running → completed/failed)
    - Which module is currently executing
    - Date range for the GSC/GA4 data pull
    """
    __tablename__ = "reports"
    __table_args__ = (
        UniqueConstraint("user_id", "domain", name="uq_user_domain"),
    )

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    domain = Column(String(512), nullable=False, index=True)
    display_name = Column(String(255), nullable=True)

    # Execution state
    status = Column(
        SAEnum(ReportStatus, name="report_status", create_type=True),
        default=ReportStatus.pending,
        nullable=False,
    )
    current_module = Column(Integer, nullable=True)
    progress_pct = Column(Float, default=0.0, nullable=False)
    error_message = Column(Text, nullable=True)

    # Data range
    date_start = Column(DateTime(timezone=True), nullable=True)
    date_end = Column(DateTime(timezone=True), nullable=True)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    completed_at = Column(DateTime(timezone=True), nullable=True)

    # Relationships
    user = relationship("User", back_populates="reports")
    modules = relationship("ReportModule", back_populates="report", cascade="all, delete-orphan")

    def __repr__(self) -> str:
        return f"<Report id={self.id} domain={self.domain!r} status={self.status}>"


class ReportModule(Base):
    """
    Result of a single analysis module within a report.

    Stores:
    - Module number (1-12) and name enum
    - Status of this individual module run
    - JSON results blob containing all analysis output
    - Execution metadata (duration, error info)
    """
    __tablename__ = "report_modules"
    __table_args__ = (
        UniqueConstraint("report_id", "module_number", name="uq_report_module"),
    )

    id = Column(Integer, primary_key=True, index=True)
    report_id = Column(Integer, ForeignKey("reports.id", ondelete="CASCADE"), nullable=False, index=True)
    module_number = Column(Integer, nullable=False)
    module_name = Column(
        SAEnum(ModuleName, name="module_name", create_type=True),
        nullable=False,
    )

    # Execution state
    status = Column(
        SAEnum(ReportStatus, name="module_status", create_type=True),
        default=ReportStatus.pending,
        nullable=False,
    )
    error_message = Column(Text, nullable=True)
    duration_seconds = Column(Float, nullable=True)

    # Results
    results = Column(JSON, nullable=True)
    summary = Column(Text, nullable=True)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    # Relationships
    report = relationship("Report", back_populates="modules")

    def __repr__(self) -> str:
        return f"<ReportModule report={self.report_id} module={self.module_number} status={self.status}>"


# ---------------------------------------------------------------------------
# Pydantic Schemas (for FastAPI request/response validation)
# ---------------------------------------------------------------------------

class UserResponse(BaseModel):
    """User info returned by API endpoints."""
    id: int
    email: str
    name: Optional[str] = None
    picture: Optional[str] = None
    gsc_property: Optional[str] = None
    ga4_property_id: Optional[str] = None
    created_at: datetime

    class Config:
        from_attributes = True


class ReportCreate(BaseModel):
    """Request body to create a new report."""
    domain: str = Field(..., min_length=3, max_length=512, description="Domain to analyze (e.g. example.com)")
    display_name: Optional[str] = Field(None, max_length=255, description="Friendly name for the report")


class ReportResponse(BaseModel):
    """Report info returned by API endpoints."""
    id: int
    domain: str
    display_name: Optional[str] = None
    status: ReportStatus
    current_module: Optional[int] = None
    progress_pct: float = 0.0
    error_message: Optional[str] = None
    date_start: Optional[datetime] = None
    date_end: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime
    completed_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class ModuleResponse(BaseModel):
    """Module result returned by API endpoints."""
    id: int
    report_id: int
    module_number: int
    module_name: ModuleName
    status: ReportStatus
    error_message: Optional[str] = None
    duration_seconds: Optional[float] = None
    results: Optional[Dict[str, Any]] = None
    summary: Optional[str] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class ModuleRunRequest(BaseModel):
    """Request body to run a specific module."""
    report_id: int = Field(..., description="ID of the report to run the module against")


class ReportListResponse(BaseModel):
    """Paginated list of reports."""
    reports: List[ReportResponse]
    total: int
    page: int = 1
    per_page: int = 20
