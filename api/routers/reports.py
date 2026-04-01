"""
Report generation, management, and PDF export router.

All endpoints require JWT authentication via the ``get_current_user``
dependency (``Authorization: Bearer <token>``).  Report ownership is
enforced -- users can only access their own reports.
"""
import os
import uuid
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, Body
from fastapi.responses import Response
from pydantic import BaseModel

from api.auth.dependencies import get_current_user

logger = logging.getLogger(__name__)
router = APIRouter()


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------

class ReportRequest(BaseModel):
    """Request body for creating a new report."""
    gsc_property: str
    ga4_property: Optional[str] = None
    domain: str


class ReportResponse(BaseModel):
    """Response for report operations."""
    report_id: str
    status: str
    message: str


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


# ---------------------------------------------------------------------------
# Supabase helper
# ---------------------------------------------------------------------------

def _get_supabase():
    """Get Supabase client."""
    from supabase import create_client
    url = os.getenv("SUPABASE_URL", "")
    key = os.getenv("SUPABASE_KEY", "")
    if not url or not key:
        raise HTTPException(status_code=500, detail="Supabase not configured")
    return create_client(url, key)


def _user_id(user: dict) -> str:
    """Extract the canonical user identifier from the JWT user dict."""
    return user.get("sub", user.get("id", user.get("user_id", "")))


async def _get_owned_report(report_id: str, user: dict) -> dict:
    """Fetch a report and verify it belongs to *user*."""
    supabase = _get_supabase()
    try:
        result = supabase.table("reports").select("*").eq("id", report_id).execute()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch report: {str(e)}")
    if not result.data:
        raise HTTPException(status_code=404, detail="Report not found")
    report = result.data[0]
    if report.get("user_id") != _user_id(user):
        raise HTTPException(status_code=403, detail="Not authorised for this report")
    return report


def _fetch_module_results(report_id: str) -> Dict[int, Dict[str, Any]]:
    """Return module results keyed by module number."""
    supabase = _get_supabase()
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
    return module_results


def _require_completed(report_data: dict, action: str = "This action") -> None:
    """Raise 400 if the report is not in a completed state."""
    if report_data.get("status") not in ("completed", "done", "ready"):
        raise HTTPException(
            status_code=400,
            detail=(
                f"Report is not yet completed (status: {report_data.get('status')}). "
                f"{action} is only available for finished reports."
            ),
        )


# ---------------------------------------------------------------------------
# Report CRUD
# ---------------------------------------------------------------------------

@router.post("/create", response_model=ReportResponse)
async def create_report(
    req: ReportRequest,
    user: dict = Depends(get_current_user),
) -> Dict[str, Any]:
    """
    Create a new search intelligence report.

    The authenticated user is automatically set as the report owner.
    """
    supabase = _get_supabase()
    report_id = str(uuid.uuid4())

    try:
        supabase.table("reports").insert({
            "id": report_id,
            "user_id": _user_id(user),
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
        "message": "Report created. Analysis pipeline will start shortly.",
    }


@router.get("/{report_id}")
async def get_report(
    report_id: str,
    user: dict = Depends(get_current_user),
) -> Dict[str, Any]:
    """Get report status and results (owner only)."""
    return await _get_owned_report(report_id, user)


@router.get("/user/me")
async def list_my_reports(
    user: dict = Depends(get_current_user),
) -> List[Dict[str, Any]]:
    """List all reports for the authenticated user."""
    supabase = _get_supabase()
    uid = _user_id(user)
    try:
        result = (
            supabase.table("reports")
            .select("id, domain, status, created_at")
            .eq("user_id", uid)
            .order("created_at", desc=True)
            .execute()
        )
        return result.data or []
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list reports: {str(e)}")


# ---------------------------------------------------------------------------
# PDF export
# ---------------------------------------------------------------------------

@router.get("/{report_id}/pdf")
async def export_report_pdf(
    report_id: str,
    user: dict = Depends(get_current_user),
) -> Response:
    """
    Export a completed report as a PDF document.

    Returns a downloadable PDF containing the executive summary,
    all module results, metrics, and recommendations.
    """
    report_data = await _get_owned_report(report_id, user)
    _require_completed(report_data, "PDF export")

    module_results = _fetch_module_results(report_id)
    if not module_results:
        raise HTTPException(status_code=400, detail="No module results found. Run the analysis first.")

    try:
        from api.services.pdf_export import generate_pdf_report
        pdf_bytes = generate_pdf_report(report_data, module_results)
    except ImportError as e:
        logger.error("PDF export dependency missing: %s", e)
        raise HTTPException(status_code=500, detail="PDF generation not available. Missing dependency: reportlab.")
    except Exception as e:
        logger.exception("PDF generation failed")
        raise HTTPException(status_code=500, detail=f"Failed to generate PDF: {str(e)}")

    domain = report_data.get("domain", "report").replace(".", "_")
    ts = datetime.utcnow().strftime("%Y%m%d")
    filename = f"search_intelligence_{domain}_{ts}.pdf"

    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Content-Length": str(len(pdf_bytes)),
        },
    )


# ---------------------------------------------------------------------------
# Email delivery
# ---------------------------------------------------------------------------

@router.post("/{report_id}/email", response_model=EmailResponse)
async def email_report(
    report_id: str,
    req: EmailRequest,
    user: dict = Depends(get_current_user),
) -> Dict[str, Any]:
    """
    Email a completed report as a PDF attachment.

    Generates the PDF on-the-fly and sends it to the specified email
    address using the configured email provider.
    """
    report_data = await _get_owned_report(report_id, user)
    _require_completed(report_data, "Email delivery")

    module_results = _fetch_module_results(report_id)
    if not module_results:
        raise HTTPException(status_code=400, detail="No module results found for this report.")

    # Generate PDF
    try:
        from api.services.pdf_export import generate_pdf_report
        pdf_bytes = generate_pdf_report(report_data, module_results)
    except ImportError as e:
        logger.error("PDF export dependency missing: %s", e)
        raise HTTPException(status_code=500, detail="PDF generation not available.")
    except Exception as e:
        logger.exception("PDF generation failed for email delivery")
        raise HTTPException(status_code=500, detail=f"Failed to generate PDF: {str(e)}")

    # Send email
    try:
        from api.services.email_delivery import send_report_email

        domain = report_data.get("domain", "report").replace(".", "_")
        ts = datetime.utcnow().strftime("%Y%m%d")
        pdf_filename = f"search_intelligence_{domain}_{ts}.pdf"

        email_result = await send_report_email(
            to_email=req.to_email,
            report_data=report_data,
            pdf_bytes=pdf_bytes,
            pdf_filename=pdf_filename,
            subject=req.subject,
        )
    except ImportError as e:
        logger.error("Email delivery dependency missing: %s", e)
        raise HTTPException(status_code=500, detail="Email delivery not available.")
    except Exception as e:
        logger.exception("Email delivery failed")
        raise HTTPException(status_code=500, detail=f"Failed to send email: {str(e)}")

    if not email_result.get("success"):
        raise HTTPException(
            status_code=502,
            detail=f"Email delivery failed: {email_result.get('error', 'Unknown error')}",
        )

    # Log delivery (non-fatal)
    supabase = _get_supabase()
    try:
        supabase.table("email_log").insert({
            "report_id": report_id,
            "to_email": req.to_email,
            "provider": email_result.get("provider", "unknown"),
            "status": "sent",
            "sent_at": datetime.utcnow().isoformat(),
        }).execute()
    except Exception as e:
        logger.warning("Failed to log email delivery: %s", e)

    return {
        "success": True,
        "message": f"Report emailed successfully to {req.to_email}",
        "provider": email_result.get("provider"),
        "to_email": req.to_email,
    }


@router.get("/email/status")
async def email_status(
    user: dict = Depends(get_current_user),
) -> Dict[str, Any]:
    """Check whether email delivery is configured and which provider is active."""
    try:
        from api.services.email_delivery import check_email_config
        return await check_email_config()
    except ImportError:
        return {"configured": False, "error": "Email delivery module not available"}
    except Exception as e:
        return {"configured": False, "error": str(e)}


# ---------------------------------------------------------------------------
# Consulting CTAs
# ---------------------------------------------------------------------------

@router.get("/{report_id}/ctas")
async def get_report_ctas(
    report_id: str,
    user: dict = Depends(get_current_user),
    max_ctas: int = Query(default=5, ge=1, le=10),
) -> Dict[str, Any]:
    """
    Generate contextual consulting CTAs for a completed report.

    Analyses each module results and returns data-driven CTAs ranked
    by urgency.
    """
    report_data = await _get_owned_report(report_id, user)
    _require_completed(report_data, "CTA generation")

    module_results = _fetch_module_results(report_id)

    try:
        from api.services.consulting_ctas import generate_report_ctas
        return generate_report_ctas(module_results, max_ctas=max_ctas)
    except ImportError as e:
        logger.error("Consulting CTA module missing: %s", e)
        raise HTTPException(status_code=500, detail="CTA generation not available.")
    except Exception as e:
        logger.exception("CTA generation failed")
        raise HTTPException(status_code=500, detail=f"Failed to generate CTAs: {str(e)}")


@router.get("/consulting/services")
async def list_consulting_services(
    user: dict = Depends(get_current_user),
) -> Dict[str, Any]:
    """
    Return the full catalogue of consulting services.

    Used by the frontend to render a services page, pricing table,
    or consulting menu.
    """
    try:
        from api.services.consulting_ctas import get_available_services, CONTACT_URL, BOOKING_URL, AUDIT_URL
        return {
            "services": get_available_services(),
            "contact_url": CONTACT_URL,
            "booking_url": BOOKING_URL,
            "audit_url": AUDIT_URL,
        }
    except ImportError as e:
        logger.error("Consulting CTA module missing: %s", e)
        raise HTTPException(status_code=500, detail="Consulting services not available.")
    except Exception as e:
        logger.exception("Failed to list consulting services")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
