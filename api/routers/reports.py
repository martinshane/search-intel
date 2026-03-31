"""
Report generation and management router.
"""
import os
import uuid
from typing import Any, Dict, List, Optional
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query, Body
from pydantic import BaseModel

router = APIRouter()


class ReportRequest(BaseModel):
    """Request body for creating a new report."""
    user_id: str
    gsc_property: str
    ga4_property: Optional[str] = None
    domain: str


class ReportResponse(BaseModel):
    """Response for report operations."""
    report_id: str
    status: str
    message: str


def _get_supabase():
    """Get Supabase client."""
    from supabase import create_client
    url = os.getenv("SUPABASE_URL", "")
    key = os.getenv("SUPABASE_KEY", "")
    if not url or not key:
        raise HTTPException(status_code=500, detail="Supabase not configured")
    return create_client(url, key)


@router.post("/create", response_model=ReportResponse)
async def create_report(req: ReportRequest) -> Dict[str, Any]:
    """
    Create a new search intelligence report.
    
    Initiates data ingestion and analysis pipeline.
    """
    supabase = _get_supabase()
    
    report_id = str(uuid.uuid4())
    
    try:
        supabase.table("reports").insert({
            "id": report_id,
            "user_id": req.user_id,
            "gsc_property": req.gsc_property,
            "ga4_property": req.ga4_property,
            "domain": req.domain,
            "status": "queued",
            "created_at": datetime.utcnow().isoformat(),
        }).execute()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create report: {str(e)}")
    
    return {
        "report_id": report_id,
        "status": "queued",
        "message": "Report created. Analysis pipeline will start shortly."
    }


@router.get("/{report_id}")
async def get_report(report_id: str) -> Dict[str, Any]:
    """Get report status and results."""
    supabase = _get_supabase()
    
    try:
        result = supabase.table("reports").select("*").eq("id", report_id).execute()
        
        if not result.data:
            raise HTTPException(status_code=404, detail="Report not found")
        
        return result.data[0]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch report: {str(e)}")


@router.get("/user/{user_id}")
async def list_user_reports(user_id: str) -> List[Dict[str, Any]]:
    """List all reports for a user."""
    supabase = _get_supabase()
    
    try:
        result = (
            supabase.table("reports")
            .select("id, domain, status, created_at")
            .eq("user_id", user_id)
            .order("created_at", desc=True)
            .execute()
        )
        return result.data or []
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list reports: {str(e)}")
