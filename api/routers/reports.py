"""
Report generation, management, and PDF export router.
"""
import os
import uuid
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query, Body
from fastapi.responses import Response
from pydantic import BaseModel

logger = logging.getLogger(__name__)
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


@router.get("/{report_id}/pdf")
async def export_report_pdf(report_id: str) -> Response:
    """
    Export a completed report as a PDF document.

    Returns a downloadable PDF containing the executive summary,
    all module results, metrics, and recommendations.
    """
    supabase = _get_supabase()

    # 1. Fetch report metadata
    try:
        result = supabase.table("reports").select("*").eq("id", report_id).execute()
        if not result.data:
            raise HTTPException(status_code=404, detail="Report not found")
        report_data = result.data[0]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch report: {str(e)}")

    if report_data.get("status") not in ("completed", "done", "ready"):
        raise HTTPException(
            status_code=400,
            detail=f"Report is not yet completed (status: {report_data.get('status')}). "
                   "PDF export is only available for finished reports.",
        )

    # 2. Fetch all module results for this report
    try:
        modules_result = (
            supabase.table("report_modules")
            .select("module_number, results")
            .eq("report_id", report_id)
            .order("module_number")
            .execute()
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch module results: {str(e)}",
        )

    module_results: Dict[int, Dict[str, Any]] = {}
    for row in modules_result.data or []:
        num = row.get("module_number")
        data = row.get("results")
        if num is not None and data:
            module_results[int(num)] = data if isinstance(data, dict) else {}

    if not module_results:
        raise HTTPException(
            status_code=400,
            detail="No module results found for this report. Run the analysis first.",
        )

    # 3. Generate PDF
    try:
        from api.services.pdf_export import generate_pdf_report
        pdf_bytes = generate_pdf_report(report_data, module_results)
    except ImportError as e:
        logger.error(f"PDF export dependency missing: {e}")
        raise HTTPException(
            status_code=500,
            detail="PDF generation is not available. Missing dependency: reportlab.",
        )
    except Exception as e:
        logger.exception("PDF generation failed")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate PDF: {str(e)}",
        )

    # 4. Return PDF as downloadable response
    domain = report_data.get("domain", "report").replace(".", "_")
    filename = f"search_intelligence_{domain}_{datetime.utcnow().strftime('%Y%m%d')}.pdf"

    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Content-Length": str(len(pdf_bytes)),
        },
    )


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


# ---------------------------------------------------------------------------
# Email delivery endpoints
# ---------------------------------------------------------------------------


class EmailRequest(BaseModel):
    """Request body for sending a report via email."""
    to_email: str
    subject: Optional[str] = None


class EmailResponse(BaseModel):
    """Response for email delivery."""
    success: bool
    message: str
    provider: Optional[str] = None
    to_email: Optional[str] = None


@router.post("/{report_id}/email", response_model=EmailResponse)
async def email_report(report_id: str, req: EmailRequest) -> Dict[str, Any]:
    """
    Email a completed report as a PDF attachment.

    Generates the PDF on-the-fly (reuses the /pdf logic) and sends it
    to the specified email address using the configured email provider.
    """
    supabase = _get_supabase()

    # 1. Fetch report
    try:
        result = supabase.table("reports").select("*").eq("id", report_id).execute()
        if not result.data:
            raise HTTPException(status_code=404, detail="Report not found")
        report_data = result.data[0]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch report: {str(e)}")

    if report_data.get("status") not in ("completed", "done", "ready"):
        raise HTTPException(
            status_code=400,
            detail=f"Report is not yet completed (status: {report_data.get('status')}). "
                   "Email delivery is only available for finished reports.",
        )

    # 2. Fetch module results
    try:
        modules_result = (
            supabase.table("report_modules")
            .select("module_number, results")
            .eq("report_id", report_id)
            .order("module_number")
            .execute()
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch module results: {str(e)}")

    module_results: Dict[int, Dict[str, Any]] = {}
    for row in modules_result.data or []:
        num = row.get("module_number")
        data = row.get("results")
        if num is not None and data:
            module_results[int(num)] = data if isinstance(data, dict) else {}

    if not module_results:
        raise HTTPException(
            status_code=400,
            detail="No module results found for this report.",
        )

    # 3. Generate PDF
    try:
        from api.services.pdf_export import generate_pdf_report
        pdf_bytes = generate_pdf_report(report_data, module_results)
    except ImportError as e:
        logger.error(f"PDF export dependency missing: {e}")
        raise HTTPException(status_code=500, detail="PDF generation not available.")
    except Exception as e:
        logger.exception("PDF generation failed for email delivery")
        raise HTTPException(status_code=500, detail=f"Failed to generate PDF: {str(e)}")

    # 4. Send email
    try:
        from api.services.email_delivery import send_report_email

        domain = report_data.get("domain", "report").replace(".", "_")
        pdf_filename = f"search_intelligence_{domain}_{datetime.utcnow().strftime('%Y%m%d')}.pdf"

        email_result = await send_report_email(
            to_email=req.to_email,
            report_data=report_data,
            pdf_bytes=pdf_bytes,
            pdf_filename=pdf_filename,
            subject=req.subject,
        )
    except ImportError as e:
        logger.error(f"Email delivery dependency missing: {e}")
        raise HTTPException(status_code=500, detail="Email delivery not available.")
    except Exception as e:
        logger.exception("Email delivery failed")
        raise HTTPException(status_code=500, detail=f"Failed to send email: {str(e)}")

    if not email_result.get("success"):
        raise HTTPException(
            status_code=502,
            detail=f"Email delivery failed: {email_result.get('error', 'Unknown error')}",
        )

    # 5. Log delivery in Supabase
    try:
        supabase.table("email_log").insert({
            "report_id": report_id,
            "to_email": req.to_email,
            "provider": email_result.get("provider", "unknown"),
            "status": "sent",
            "sent_at": datetime.utcnow().isoformat(),
        }).execute()
    except Exception as e:
        # Non-fatal — email was already sent
        logger.warning(f"Failed to log email delivery: {e}")

    return {
        "success": True,
        "message": f"Report emailed successfully to {req.to_email}",
        "provider": email_result.get("provider"),
        "to_email": req.to_email,
    }


@router.get("/email/status")
async def email_status() -> Dict[str, Any]:
    """Check whether email delivery is configured and which provider is active."""
    try:
        from api.services.email_delivery import check_email_config
        return await check_email_config()
    except ImportError:
        return {"configured": False, "error": "Email delivery module not available"}
    except Exception as e:
        return {"configured": False, "error": str(e)}
