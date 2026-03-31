import os
from contextlib import asynccontextmanager
from typing import Optional
from uuid import UUID

from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import httpx

from db.supabase_client import get_supabase_client
from worker.pipeline import start_report_generation


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events."""
    # Startup
    app.state.http_client = httpx.AsyncClient(timeout=30.0)
    yield
    # Shutdown
    await app.state.http_client.aclose()


app = FastAPI(
    title="Search Intelligence Report API",
    description="Backend API for generating comprehensive search intelligence reports",
    version="1.0.0",
    lifespan=lifespan
)

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ORIGINS", "http://localhost:3000").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================================
# Request/Response Models
# ============================================================================

class CreateReportRequest(BaseModel):
    """Request body for creating a new report."""
    gsc_property: str
    ga4_property: Optional[str] = None


class ReportStatusResponse(BaseModel):
    """Response for report status polling."""
    id: str
    user_id: str
    gsc_property: str
    ga4_property: Optional[str]
    status: str  # pending, ingesting, analyzing, generating, complete, failed
    progress: dict
    error_message: Optional[str] = None
    report_data: Optional[dict] = None
    created_at: str
    completed_at: Optional[str] = None


class CreateReportResponse(BaseModel):
    """Response after creating a new report."""
    report_id: str
    status: str
    message: str


# ============================================================================
# Dependencies
# ============================================================================

async def get_user_id(authorization: Optional[str] = Header(None)) -> str:
    """
    Extract and validate user ID from Authorization header.
    
    In production, this would validate a JWT token from Supabase Auth.
    For MVP, we accept a simple Bearer token with user_id.
    """
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization header required")
    
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid authorization format")
    
    token = authorization[7:]  # Remove "Bearer " prefix
    
    # TODO: In production, validate JWT token with Supabase Auth
    # For now, treat token as direct user_id
    if not token:
        raise HTTPException(status_code=401, detail="Invalid token")
    
    return token


# ============================================================================
# Routes
# ============================================================================

@app.get("/")
async def root():
    """Health check endpoint."""
    return {
        "service": "Search Intelligence Report API",
        "status": "operational",
        "version": "1.0.0"
    }


@app.get("/health")
async def health_check():
    """Detailed health check."""
    supabase = get_supabase_client()
    
    try:
        # Test database connection
        result = supabase.table("users").select("id").limit(1).execute()
        db_status = "ok"
    except Exception as e:
        db_status = f"error: {str(e)}"
    
    return {
        "status": "ok" if db_status == "ok" else "degraded",
        "database": db_status,
        "timestamp": "2025-03-20T00:00:00Z"  # Would use real timestamp in production
    }


@app.post("/reports", response_model=CreateReportResponse)
async def create_report(
    request: CreateReportRequest,
    user_id: str = Depends(get_user_id)
):
    """
    Create a new report generation job.
    
    This endpoint:
    1. Validates user has OAuth tokens for the requested properties
    2. Creates a report record with status 'pending'
    3. Starts async report generation pipeline
    4. Returns immediately with report_id for polling
    """
    supabase = get_supabase_client()
    
    try:
        # Verify user exists and has required OAuth tokens
        user_result = supabase.table("users").select("*").eq("id", user_id).execute()
        
        if not user_result.data:
            raise HTTPException(status_code=404, detail="User not found")
        
        user = user_result.data[0]
        
        # Check for GSC token
        if not user.get("gsc_token"):
            raise HTTPException(
                status_code=400,
                detail="Google Search Console authorization required"
            )
        
        # Check for GA4 token if GA4 property is requested
        if request.ga4_property and not user.get("ga4_token"):
            raise HTTPException(
                status_code=400,
                detail="Google Analytics 4 authorization required"
            )
        
        # Create report record
        report_data = {
            "user_id": user_id,
            "gsc_property": request.gsc_property,
            "ga4_property": request.ga4_property,
            "status": "pending",
            "progress": {}
        }
        
        report_result = supabase.table("reports").insert(report_data).execute()
        
        if not report_result.data:
            raise HTTPException(status_code=500, detail="Failed to create report")
        
        report = report_result.data[0]
        report_id = report["id"]
        
        # Start async report generation
        # This runs in the background and updates the report status
        try:
            await start_report_generation(report_id, user_id)
        except Exception as pipeline_error:
            # Update report status to failed
            supabase.table("reports").update({
                "status": "failed",
                "progress": {"error": str(pipeline_error)}
            }).eq("id", report_id).execute()
            
            raise HTTPException(
                status_code=500,
                detail=f"Failed to start report generation: {str(pipeline_error)}"
            )
        
        return CreateReportResponse(
            report_id=report_id,
            status="pending",
            message="Report generation started. Poll /reports/{id} for status."
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )


@app.get("/reports/{report_id}", response_model=ReportStatusResponse)
async def get_report_status(
    report_id: str,
    user_id: str = Depends(get_user_id)
):
    """
    Get the status of a report generation job.
    
    This endpoint is polled by the frontend to track progress.
    Returns current status, progress details, and the complete report data
    when status is 'complete'.
    """
    supabase = get_supabase_client()
    
    try:
        # Fetch report
        report_result = supabase.table("reports").select("*").eq("id", report_id).execute()
        
        if not report_result.data:
            raise HTTPException(status_code=404, detail="Report not found")
        
        report = report_result.data[0]
        
        # Verify ownership
        if report["user_id"] != user_id:
            raise HTTPException(status_code=403, detail="Access denied")
        
        # Build response
        response = ReportStatusResponse(
            id=report["id"],
            user_id=report["user_id"],
            gsc_property=report["gsc_property"],
            ga4_property=report.get("ga4_property"),
            status=report["status"],
            progress=report.get("progress", {}),
            error_message=report.get("progress", {}).get("error"),
            report_data=report.get("report_data") if report["status"] == "complete" else None,
            created_at=report["created_at"],
            completed_at=report.get("completed_at")
        )
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )


@app.get("/reports")
async def list_reports(
    user_id: str = Depends(get_user_id),
    limit: int = 10,
    offset: int = 0
):
    """
    List all reports for the authenticated user.
    
    Returns a paginated list of reports, newest first.
    """
    supabase = get_supabase_client()
    
    try:
        # Fetch reports for user
        reports_result = (
            supabase.table("reports")
            .select("id, gsc_property, ga4_property, status, created_at, completed_at")
            .eq("user_id", user_id)
            .order("created_at", desc=True)
            .range(offset, offset + limit - 1)
            .execute()
        )
        
        # Get total count
        count_result = (
            supabase.table("reports")
            .select("id", count="exact")
            .eq("user_id", user_id)
            .execute()
        )
        
        total = count_result.count if hasattr(count_result, 'count') else len(reports_result.data)
        
        return {
            "reports": reports_result.data,
            "total": total,
            "limit": limit,
            "offset": offset
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )


@app.delete("/reports/{report_id}")
async def delete_report(
    report_id: str,
    user_id: str = Depends(get_user_id)
):
    """
    Delete a report.
    
    This removes the report record from the database.
    """
    supabase = get_supabase_client()
    
    try:
        # Verify ownership before deletion
        report_result = supabase.table("reports").select("user_id").eq("id", report_id).execute()
        
        if not report_result.data:
            raise HTTPException(status_code=404, detail="Report not found")
        
        if report_result.data[0]["user_id"] != user_id:
            raise HTTPException(status_code=403, detail="Access denied")
        
        # Delete report
        supabase.table("reports").delete().eq("id", report_id).execute()
        
        return {"message": "Report deleted successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
        reload=os.getenv("ENV") == "development"
    )
