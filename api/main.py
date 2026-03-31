from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional
from uuid import UUID

from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field
import httpx
from supabase import create_client, Client

from config import get_settings
from auth.oauth import router as oauth_router
from reports.router import router as reports_router

settings = get_settings()

# Initialize Supabase client
supabase: Client = create_client(settings.SUPABASE_URL, settings.SUPABASE_KEY)

# HTTP bearer token security
security = HTTPBearer()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup/shutdown events."""
    # Startup
    app.state.httpx_client = httpx.AsyncClient(timeout=30.0)
    app.state.supabase = supabase
    
    yield
    
    # Shutdown
    await app.state.httpx_client.aclose()


# Initialize FastAPI app
app = FastAPI(
    title="Search Intelligence Report API",
    description="Backend API for generating comprehensive search intelligence reports combining GSC, GA4, and SERP data",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(oauth_router, prefix="/api/auth", tags=["Authentication"])
app.include_router(reports_router, prefix="/api/reports", tags=["Reports"])


# Auth dependency
async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)) -> dict:
    """Verify JWT token and return user data."""
    try:
        token = credentials.credentials
        
        # Verify token with Supabase
        response = supabase.auth.get_user(token)
        
        if not response or not response.user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid authentication token"
            )
        
        return {
            "id": response.user.id,
            "email": response.user.email,
            "token": token
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Authentication failed: {str(e)}"
        )


# Request/Response models
class HealthCheckResponse(BaseModel):
    status: str
    timestamp: datetime
    version: str


class ReportStatusResponse(BaseModel):
    id: str
    status: str = Field(..., description="Report status: pending, ingesting, analyzing, generating, complete, failed")
    progress: dict = Field(default_factory=dict, description="Progress by module")
    created_at: datetime
    completed_at: Optional[datetime] = None
    error: Optional[str] = None


# Root endpoint
@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "message": "Search Intelligence Report API",
        "version": "1.0.0",
        "docs": "/docs"
    }


# Health check endpoint
@app.get("/health", response_model=HealthCheckResponse)
async def health_check():
    """Health check endpoint."""
    return HealthCheckResponse(
        status="healthy",
        timestamp=datetime.now(timezone.utc),
        version="1.0.0"
    )


# Report status endpoint (for polling)
@app.get("/api/reports/{report_id}/status", response_model=ReportStatusResponse)
async def get_report_status(
    report_id: UUID,
    current_user: dict = Depends(get_current_user)
):
    """
    Get the current status of a report generation job.
    
    This endpoint is polled by the frontend to track progress.
    
    Status values:
    - pending: Report created, waiting to start
    - ingesting: Fetching data from GSC, GA4, DataForSEO
    - analyzing: Running analysis modules
    - generating: Creating final report structure
    - complete: Report ready
    - failed: Error occurred
    
    Progress object contains module-level status:
    {
        "data_ingestion": "complete",
        "module_1_health": "complete",
        "module_2_triage": "running",
        "module_3_serp": "pending",
        ...
    }
    """
    try:
        # Fetch report from database
        response = supabase.table("reports").select("*").eq("id", str(report_id)).execute()
        
        if not response.data or len(response.data) == 0:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Report {report_id} not found"
            )
        
        report = response.data[0]
        
        # Verify ownership
        if report["user_id"] != current_user["id"]:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You don't have access to this report"
            )
        
        # Parse timestamps
        created_at = datetime.fromisoformat(report["created_at"].replace("Z", "+00:00"))
        completed_at = None
        if report.get("completed_at"):
            completed_at = datetime.fromisoformat(report["completed_at"].replace("Z", "+00:00"))
        
        # Build response
        return ReportStatusResponse(
            id=report["id"],
            status=report["status"],
            progress=report.get("progress", {}),
            created_at=created_at,
            completed_at=completed_at,
            error=report.get("error")
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch report status: {str(e)}"
        )


# Error handlers
@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    """Handle HTTP exceptions."""
    return {
        "error": exc.detail,
        "status_code": exc.status_code
    }


@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    """Handle general exceptions."""
    return {
        "error": "Internal server error",
        "detail": str(exc),
        "status_code": 500
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )
