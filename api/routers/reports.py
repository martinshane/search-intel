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

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, Body
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
    """Get Supabase client via centralized database module."""
    try:
        from api.database import get_supabase_client
        return get_supabase_client()
    except (ValueError, Exception) as e:
        raise HTTPException(status_code=500, detail=f"Supabase not configured: {e}")


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
    if report_data.get("status") not in ("completed", "complete", "done", "ready", "partial"):
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
    background_tasks: BackgroundTasks,
    user: dict = Depends(get_current_user),
) -> Dict[str, Any]:
    """
    Create a new search intelligence report.

    The authenticated user is automatically set as the report owner.
    The analysis pipeline is triggered as a background task — poll
    GET /reports/{id} for status updates.
    """
    supabase = _get_supabase()
    report_id = str(uuid.uuid4())
    uid = _user_id(user)

    try:
        supabase.table("reports").insert({
            "id": report_id,
            "user_id": uid,
            "gsc_property": req.gsc_property,
            "ga4_property": req.ga4_property,
            "domain": req.domain,
            "status": "pending",
            "created_at": datetime.utcnow().isoformat(),
        }).execute()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create report: {str(e)}")

    # Trigger the analysis pipeline in the background
    try:
        from api.worker.report_runner import run_report_pipeline
        background_tasks.add_task(
            run_report_pipeline,
            report_id=report_id,
            user_id=uid,
            gsc_property=req.gsc_property,
            ga4_property=req.ga4_property,
            domain=req.domain,
        )
        logger.info("Background pipeline started for report %s (status=pending)", report_id)
    except Exception as e:
        logger.error("Failed to queue pipeline for report %s: %s", report_id, e)
        # Report is created — pipeline can be retried via /reports/{id}/retry
        supabase.table("reports").update({
            "error_message": f"Failed to queue pipeline: {str(e)}",
        }).eq("id", report_id).execute()

    return {
        "report_id": report_id,
        "status": "pending",
        "message": "Report created. Analysis pipeline will start shortly. Poll GET /reports/{id} for progress.",
    }


@router.post("/generate", response_model=ReportResponse)
async def generate_report(
    req: ReportRequest,
    background_tasks: BackgroundTasks,
    user: dict = Depends(get_current_user),
) -> Dict[str, Any]:
    """
    Alias for ``/create`` — the frontend calls ``/api/reports/generate``.

    Delegates to ``create_report`` so both paths produce identical behaviour.
    """
    return await create_report(req, background_tasks, user)


@router.post("/{report_id}/retry", response_model=ReportResponse)
async def retry_report(
    report_id: str,
    background_tasks: BackgroundTasks,
    user: dict = Depends(get_current_user),
) -> Dict[str, Any]:
    """
    Retry a failed or stalled report by re-running the pipeline.
    """
    report = await _get_owned_report(report_id, user)
    uid = _user_id(user)

    if report.get("status") in ("running", "ingesting", "analyzing"):
        raise HTTPException(status_code=409, detail="Report is already running")

    # Reset status
    supabase = _get_supabase()
    supabase.table("reports").update({
        "status": "pending",
        "error_message": None,
        "updated_at": datetime.utcnow().isoformat(),
    }).eq("id", report_id).execute()

    try:
        from api.worker.report_runner import run_report_pipeline
        background_tasks.add_task(
            run_report_pipeline,
            report_id=report_id,
            user_id=uid,
            gsc_property=report.get("gsc_property", ""),
            ga4_property=report.get("ga4_property"),
            domain=report.get("domain", ""),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to queue pipeline: {str(e)}")

    return {
        "report_id": report_id,
        "status": "pending",
        "message": "Report re-queued. Pipeline will restart shortly.",
    }


@router.get("/{report_id}")
async def get_report(
    report_id: str,
    user: dict = Depends(get_current_user),
) -> Dict[str, Any]:
    """Get report status and results (owner only)."""
    return await _get_owned_report(report_id, user)


@router.get("/{report_id}/progress")
async def get_report_progress(
    report_id: str,
    user: dict = Depends(get_current_user),
) -> Dict[str, Any]:
    """
    Lightweight progress endpoint for the real-time progress UI.

    Returns only the fields the frontend progress page needs — no
    report_data blob, keeping the payload tiny during polling.

    The progress dict stored in the reports table uses pipeline status
    values ("success", "failed", "skipped").  This endpoint maps them
    to the UI-friendly values the frontend expects:

        "success"  → "complete"
        "failed"   → "failed"
        "skipped"  → "failed"
        (absent)   → "pending"

    If the report is still "analyzing" and current_module is set, the
    module at current_module is marked "running" (unless it already
    finished) so the UI shows a spinner on the active module.
    """
    report = await _get_owned_report(report_id, user)

    raw_progress = report.get("progress") or {}
    status = report.get("status", "pending")
    current_module = report.get("current_module")

    # Map pipeline statuses to frontend-expected values
    STATUS_MAP = {
        "success": "complete",
        "completed": "complete",
        "failed": "failed",
        "skipped": "failed",
        "running": "running",
        "pending": "pending",
    }

    mapped_progress: Dict[str, str] = {}
    for key, val in raw_progress.items():
        mapped_progress[key] = STATUS_MAP.get(val, val)

    # If the report is still being analyzed, mark the current module
    # as "running" so the frontend shows a spinner.
    if status == "analyzing" and current_module is not None:
        current_key = f"module_{current_module}"
        if mapped_progress.get(current_key) not in ("complete", "failed"):
            mapped_progress[current_key] = "running"

    return {
        "report": {
            "id": report.get("id", report_id),
            "gscProperty": report.get("gsc_property", ""),
            "ga4Property": report.get("ga4_property", ""),
            "status": status,
            "progress": mapped_progress,
            "createdAt": report.get("created_at", ""),
            "completedAt": report.get("completed_at"),
        }
    }


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
            .select("id, domain, status, created_at, current_module, progress")
            .eq("user_id", uid)
            .order("created_at", desc=True)
            .execute()
        )
        return result.data or []
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list reports: {str(e)}")



# ---------------------------------------------------------------------------
# Real-time module results (for progressive rendering during generation)
# ---------------------------------------------------------------------------

@router.get("/{report_id}/modules")
async def get_report_modules(
    report_id: str,
    user: dict = Depends(get_current_user),
) -> Dict[str, Any]:
    """
    Return individual module results from the report_modules table.

    Unlike GET /{report_id} which returns report_data (only written after
    the entire pipeline completes), this endpoint reads from report_modules
    which is populated **in real time** as each module finishes.

    This enables the frontend to progressively render completed module
    sections during the 2-5 minute generation window instead of showing
    a blank loading screen until all 12 modules finish.

    Response shape::

        {
            "report_id": "...",
            "status": "analyzing",
            "modules": {
                "health_trajectory":    { "status": "success", "data": {...} },
                "page_triage":          { "status": "success", "data": {...} },
                "serp_landscape":       { "status": "running" },
                "content_intelligence": { "status": "pending" },
                ...
            }
        }
    """
    report = await _get_owned_report(report_id, user)

    supabase = _get_supabase()
    try:
        rows = (
            supabase.table("report_modules")
            .select("module_number, module_name, results, status")
            .eq("report_id", report_id)
            .order("module_number")
            .execute()
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch modules: {str(e)}")

    # Map module numbers to canonical names
    MODULE_NAMES = {
        1: "health_trajectory",
        2: "page_triage",
        3: "serp_landscape",
        4: "content_intelligence",
        5: "gameplan",
        6: "algorithm_impact",
        7: "intent_migration",
        8: "technical_health",
        9: "site_architecture",
        10: "branded_split",
        11: "competitive_threats",
        12: "revenue_attribution",
    }

    modules: Dict[str, Any] = {}

    # Initialize all 12 modules as pending
    for num, name in MODULE_NAMES.items():
        modules[name] = {"status": "pending", "number": num}

    # Fill in completed/failed modules from DB
    for row in rows.data or []:
        num = row.get("module_number")
        name = MODULE_NAMES.get(num, row.get("module_name", f"module_{num}"))
        status = row.get("status", "success")
        results = row.get("results")

        module_entry: Dict[str, Any] = {"status": status, "number": num}

        if status in ("success", "completed") and results and isinstance(results, dict):
            # Check if this is a "skipped" result stored as success
            if results.get("skipped"):
                module_entry["status"] = "skipped"
                module_entry["reason"] = results.get("reason", "Skipped due to dependency failure")
            else:
                module_entry["status"] = "success"
                module_entry["data"] = results
        elif status == "failed":
            module_entry["error"] = results.get("error", "Module execution failed") if isinstance(results, dict) else str(results)

        modules[name] = module_entry

    # Mark the currently running module
    report_status = report.get("status", "")
    current_module = report.get("current_module")
    if report_status in ("analyzing", "running") and current_module:
        for name, entry in modules.items():
            if entry.get("number") == current_module and entry.get("status") == "pending":
                entry["status"] = "running"

    return {
        "report_id": report_id,
        "status": report.get("status", "unknown"),
        "domain": report.get("domain", ""),
        "modules": modules,
    }

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

    supabase = _get_supabase()

    if not email_result.get("success"):
        # Log the failed delivery for audit trail
        try:
            supabase.table("email_log").insert({
                "report_id": report_id,
                "to_email": req.to_email,
                "provider": email_result.get("provider", "unknown"),
                "status": "failed",
                "error_message": email_result.get("error", "Unknown error")[:500],
                "sent_at": datetime.utcnow().isoformat(),
            }).execute()
        except Exception as log_err:
            logger.warning("Failed to log email failure: %s", log_err)

        raise HTTPException(
            status_code=502,
            detail=f"Email delivery failed: {email_result.get('error', 'Unknown error')}",
        )

    # Log successful delivery
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
    """
    Check whether email delivery is configured and return recent stats.

    Returns the active provider configuration plus aggregate stats from
    the email_log table (total sent, recent failures).
    """
    result: Dict[str, Any] = {}

    # Provider config
    try:
        from api.services.email_delivery import check_email_config
        result = await check_email_config()
    except ImportError:
        result = {"configured": False, "error": "Email delivery module not available"}
    except Exception as e:
        result = {"configured": False, "error": str(e)}

    # Email stats from email_log
    try:
        supabase = _get_supabase()
        uid = _user_id(user)

        # Count emails for this user's reports in the last 30 days
        thirty_days_ago = (datetime.utcnow() - __import__("datetime").timedelta(days=30)).isoformat()
        log_rows = (
            supabase.table("email_log")
            .select("status")
            .gte("sent_at", thirty_days_ago)
            .in_("report_id",
                 [r["id"] for r in (
                     supabase.table("reports")
                     .select("id")
                     .eq("user_id", uid)
                     .execute()
                 ).data or []]
            )
            .execute()
        )
        logs = log_rows.data or []
        result["stats"] = {
            "emails_last_30d": len(logs),
            "sent": sum(1 for r in logs if r.get("status") == "sent"),
            "failed": sum(1 for r in logs if r.get("status") == "failed"),
        }
    except Exception as e:
        logger.warning("Could not fetch email stats: %s", e)
        result["stats"] = {"emails_last_30d": 0, "sent": 0, "failed": 0}

    return result


@router.get("/{report_id}/email/history")
async def email_history(
    report_id: str,
    user: dict = Depends(get_current_user),
    limit: int = Query(default=20, ge=1, le=100),
) -> Dict[str, Any]:
    """
    Return email delivery history for a specific report.

    Shows all emails sent for this report, including status, provider,
    recipient, and any error messages.
    """
    await _get_owned_report(report_id, user)

    supabase = _get_supabase()
    try:
        rows = (
            supabase.table("email_log")
            .select("id, to_email, provider, status, error_message, sent_at")
            .eq("report_id", report_id)
            .order("sent_at", desc=True)
            .limit(limit)
            .execute()
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch email history: {str(e)}")

    return {
        "report_id": report_id,
        "emails": rows.data or [],
        "total": len(rows.data or []),
    }


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


# ---------------------------------------------------------------------------
# Report comparison (historical delta)
# ---------------------------------------------------------------------------

class ComparisonResponse(BaseModel):
    """Response for report comparison."""
    metadata: Dict[str, Any]
    executive_summary: Dict[str, Any]
    module_deltas: List[Dict[str, Any]]
    modules_compared: int
    modules_missing: List[int]


@router.get("/{report_id}/compare")
async def compare_reports(
    report_id: str,
    baseline_id: str = Query(..., description="Report ID of the baseline (older) report to compare against"),
    user: dict = Depends(get_current_user),
) -> Dict[str, Any]:
    """
    Compare two completed reports and return a structured delta.

    Compares the current report (report_id) against a baseline report
    (baseline_id). Both reports must belong to the authenticated user
    and must be in a completed state.

    The response includes:
    - executive_summary: high-level highlights and warnings
    - module_deltas: per-module structured comparison for all 12 modules
    - metadata: report IDs, domains, timestamps

    Use this for:
    - Historical comparison (this month vs last month)
    - Tracking progress after implementing recommendations
    - Weekly re-run email content
    """
    # Fetch both reports (ownership verified inside)
    current_report = await _get_owned_report(report_id, user)
    baseline_report = await _get_owned_report(baseline_id, user)

    _require_completed(current_report, "Report comparison")
    _require_completed(baseline_report, "Report comparison (baseline)")

    # Verify same domain (comparing different sites makes no sense)
    if current_report.get("domain") != baseline_report.get("domain"):
        raise HTTPException(
            status_code=400,
            detail=(
                f"Cannot compare reports for different domains: "
                f"{current_report.get('domain')} vs {baseline_report.get('domain')}"
            ),
        )

    # Fetch module results for both reports
    current_modules = _fetch_module_results(report_id)
    baseline_modules = _fetch_module_results(baseline_id)

    if not current_modules:
        raise HTTPException(status_code=400, detail="No module results found for the current report.")
    if not baseline_modules:
        raise HTTPException(status_code=400, detail="No module results found for the baseline report.")

    # Run comparison
    try:
        from api.services.report_comparison import compare_reports as do_compare
        return do_compare(
            current_modules=current_modules,
            baseline_modules=baseline_modules,
            current_meta=current_report,
            baseline_meta=baseline_report,
        )
    except ImportError as e:
        logger.error("Report comparison module missing: %s", e)
        raise HTTPException(status_code=500, detail="Report comparison not available.")
    except Exception as e:
        logger.exception("Report comparison failed")
        raise HTTPException(status_code=500, detail=f"Failed to compare reports: {str(e)}")


@router.get("/user/history")
async def list_report_history(
    domain: Optional[str] = Query(None, description="Filter by domain"),
    limit: int = Query(default=10, ge=1, le=50),
    user: dict = Depends(get_current_user),
) -> List[Dict[str, Any]]:
    """
    List completed reports for the user, suitable for selecting comparison pairs.

    Returns reports ordered by creation date (newest first), optionally
    filtered by domain. Each entry includes the report ID, domain, status,
    and creation timestamp — enough to populate a comparison picker UI.
    """
    supabase = _get_supabase()
    uid = _user_id(user)
    try:
        query = (
            supabase.table("reports")
            .select("id, domain, status, created_at, completed_at")
            .eq("user_id", uid)
            .in_("status", ["completed", "complete", "partial"])
            .order("created_at", desc=True)
            .limit(limit)
        )
        if domain:
            query = query.eq("domain", domain)
        result = query.execute()
        return result.data or []
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list report history: {str(e)}")
