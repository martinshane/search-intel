"""
Scheduled report management router.

Allows users to subscribe to automatic weekly/biweekly/monthly report
re-runs.  Each scheduled run generates a new report, compares it to the
previous run (if any), and emails the results including an optional PDF.

Endpoints:
    POST   /schedules/create          — create a new schedule
    GET    /schedules/mine             — list my active schedules
    GET    /schedules/{schedule_id}    — get schedule detail
    PATCH  /schedules/{schedule_id}    — update a schedule
    DELETE /schedules/{schedule_id}    — deactivate a schedule
    POST   /schedules/trigger          — cron endpoint: process all due schedules

All endpoints except /trigger require JWT auth.  The /trigger endpoint
uses a shared CRON_SECRET header for authentication.
"""

import os
import uuid
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Header, Query
from pydantic import BaseModel, EmailStr

from api.auth.dependencies import get_current_user

logger = logging.getLogger(__name__)
router = APIRouter()


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------

class ScheduleCreateRequest(BaseModel):
    """Create a new report schedule."""
    domain: str
    gsc_property: str
    ga4_property: Optional[str] = None
    frequency: str = "weekly"            # weekly | biweekly | monthly
    day_of_week: int = 1                 # 0=Mon … 6=Sun
    email_to: str
    include_pdf: bool = True
    include_comparison: bool = True


class ScheduleUpdateRequest(BaseModel):
    """Update an existing schedule (partial)."""
    frequency: Optional[str] = None
    day_of_week: Optional[int] = None
    email_to: Optional[str] = None
    include_pdf: Optional[bool] = None
    include_comparison: Optional[bool] = None
    active: Optional[bool] = None


class ScheduleResponse(BaseModel):
    """Single schedule detail."""
    id: str
    domain: str
    gsc_property: str
    ga4_property: Optional[str]
    frequency: str
    day_of_week: int
    email_to: str
    include_pdf: bool
    include_comparison: bool
    active: bool
    last_run_at: Optional[str]
    next_run_at: Optional[str]
    run_count: int
    created_at: str


class TriggerResponse(BaseModel):
    """Response from the cron trigger endpoint."""
    processed: int
    succeeded: int
    failed: int
    details: List[Dict[str, Any]]


# ---------------------------------------------------------------------------
# Supabase + helpers
# ---------------------------------------------------------------------------

def _get_supabase():
    """Lazy Supabase client."""
    from supabase import create_client
    url = os.getenv("SUPABASE_URL", "")
    key = os.getenv("SUPABASE_SERVICE_KEY", "")
    if not url or not key:
        raise HTTPException(500, "Supabase not configured")
    return create_client(url, key)


def _user_id(user: dict) -> str:
    return user.get("sub") or user.get("user_id") or user.get("id", "")


def _compute_next_run(frequency: str, day_of_week: int, from_dt: Optional[datetime] = None) -> datetime:
    """Compute the next run timestamp based on frequency and preferred day."""
    now = from_dt or datetime.now(timezone.utc)

    if frequency == "monthly":
        # First occurrence of day_of_week in the next month
        if now.month == 12:
            candidate = now.replace(year=now.year + 1, month=1, day=1, hour=6, minute=0, second=0, microsecond=0)
        else:
            candidate = now.replace(month=now.month + 1, day=1, hour=6, minute=0, second=0, microsecond=0)
        # Find the first matching weekday
        while candidate.weekday() != day_of_week:
            candidate += timedelta(days=1)
        return candidate

    # Weekly or biweekly
    days_ahead = day_of_week - now.weekday()
    if days_ahead <= 0:
        days_ahead += 7
    if frequency == "biweekly":
        days_ahead += 7
    next_run = now + timedelta(days=days_ahead)
    return next_run.replace(hour=6, minute=0, second=0, microsecond=0)


def _row_to_response(row: dict) -> dict:
    """Normalise a Supabase row into ScheduleResponse-compatible dict."""
    return {
        "id": str(row["id"]),
        "domain": row["domain"],
        "gsc_property": row["gsc_property"],
        "ga4_property": row.get("ga4_property"),
        "frequency": row["frequency"],
        "day_of_week": row.get("day_of_week", 1),
        "email_to": row["email_to"],
        "include_pdf": row.get("include_pdf", True),
        "include_comparison": row.get("include_comparison", True),
        "active": row.get("active", True),
        "last_run_at": str(row["last_run_at"]) if row.get("last_run_at") else None,
        "next_run_at": str(row["next_run_at"]) if row.get("next_run_at") else None,
        "run_count": row.get("run_count", 0),
        "created_at": str(row["created_at"]),
    }


# ---------------------------------------------------------------------------
# CRUD endpoints
# ---------------------------------------------------------------------------

@router.post("/create", response_model=ScheduleResponse)
async def create_schedule(
    body: ScheduleCreateRequest,
    user: dict = Depends(get_current_user),
):
    """Create a new report schedule for the authenticated user."""
    uid = _user_id(user)
    if not uid:
        raise HTTPException(401, "Invalid user token")

    # Validate frequency
    if body.frequency not in ("weekly", "biweekly", "monthly"):
        raise HTTPException(400, "frequency must be weekly, biweekly, or monthly")
    if body.day_of_week < 0 or body.day_of_week > 6:
        raise HTTPException(400, "day_of_week must be 0 (Mon) through 6 (Sun)")

    sb = _get_supabase()

    # Prevent duplicate schedules for the same domain
    existing = (
        sb.table("report_schedules")
        .select("id")
        .eq("user_id", uid)
        .eq("domain", body.domain)
        .eq("active", True)
        .execute()
    )
    if existing.data:
        raise HTTPException(
            409,
            f"An active schedule already exists for {body.domain}. "
            "Update the existing schedule or deactivate it first.",
        )

    next_run = _compute_next_run(body.frequency, body.day_of_week)
    schedule_id = str(uuid.uuid4())

    row = {
        "id": schedule_id,
        "user_id": uid,
        "domain": body.domain,
        "gsc_property": body.gsc_property,
        "ga4_property": body.ga4_property,
        "frequency": body.frequency,
        "day_of_week": body.day_of_week,
        "email_to": body.email_to,
        "include_pdf": body.include_pdf,
        "include_comparison": body.include_comparison,
        "active": True,
        "next_run_at": next_run.isoformat(),
        "run_count": 0,
    }

    result = sb.table("report_schedules").insert(row).execute()
    if not result.data:
        raise HTTPException(500, "Failed to create schedule")

    logger.info("Schedule %s created for %s (%s)", schedule_id, body.domain, body.frequency)
    return _row_to_response(result.data[0])


@router.get("/mine")
async def list_my_schedules(
    active_only: bool = Query(True, description="Only return active schedules"),
    user: dict = Depends(get_current_user),
):
    """List all report schedules for the authenticated user."""
    uid = _user_id(user)
    sb = _get_supabase()

    query = sb.table("report_schedules").select("*").eq("user_id", uid)
    if active_only:
        query = query.eq("active", True)
    result = query.order("created_at", desc=True).execute()

    return {"schedules": [_row_to_response(r) for r in (result.data or [])]}


@router.get("/{schedule_id}", response_model=ScheduleResponse)
async def get_schedule(
    schedule_id: str,
    user: dict = Depends(get_current_user),
):
    """Get a single schedule by ID (must belong to the user)."""
    uid = _user_id(user)
    sb = _get_supabase()

    result = (
        sb.table("report_schedules")
        .select("*")
        .eq("id", schedule_id)
        .eq("user_id", uid)
        .execute()
    )
    if not result.data:
        raise HTTPException(404, "Schedule not found")
    return _row_to_response(result.data[0])


@router.patch("/{schedule_id}", response_model=ScheduleResponse)
async def update_schedule(
    schedule_id: str,
    body: ScheduleUpdateRequest,
    user: dict = Depends(get_current_user),
):
    """Update an existing schedule (partial update)."""
    uid = _user_id(user)
    sb = _get_supabase()

    # Verify ownership
    existing = (
        sb.table("report_schedules")
        .select("*")
        .eq("id", schedule_id)
        .eq("user_id", uid)
        .execute()
    )
    if not existing.data:
        raise HTTPException(404, "Schedule not found")

    updates: Dict[str, Any] = {"updated_at": datetime.now(timezone.utc).isoformat()}
    current = existing.data[0]

    if body.frequency is not None:
        if body.frequency not in ("weekly", "biweekly", "monthly"):
            raise HTTPException(400, "frequency must be weekly, biweekly, or monthly")
        updates["frequency"] = body.frequency
    if body.day_of_week is not None:
        if body.day_of_week < 0 or body.day_of_week > 6:
            raise HTTPException(400, "day_of_week must be 0-6")
        updates["day_of_week"] = body.day_of_week
    if body.email_to is not None:
        updates["email_to"] = body.email_to
    if body.include_pdf is not None:
        updates["include_pdf"] = body.include_pdf
    if body.include_comparison is not None:
        updates["include_comparison"] = body.include_comparison
    if body.active is not None:
        updates["active"] = body.active

    # Recompute next_run if frequency or day changed
    freq = updates.get("frequency", current["frequency"])
    dow = updates.get("day_of_week", current.get("day_of_week", 1))
    updates["next_run_at"] = _compute_next_run(freq, dow).isoformat()

    result = (
        sb.table("report_schedules")
        .update(updates)
        .eq("id", schedule_id)
        .execute()
    )
    if not result.data:
        raise HTTPException(500, "Failed to update schedule")

    logger.info("Schedule %s updated", schedule_id)
    return _row_to_response(result.data[0])


@router.delete("/{schedule_id}")
async def delete_schedule(
    schedule_id: str,
    user: dict = Depends(get_current_user),
):
    """Deactivate (soft-delete) a schedule."""
    uid = _user_id(user)
    sb = _get_supabase()

    existing = (
        sb.table("report_schedules")
        .select("id")
        .eq("id", schedule_id)
        .eq("user_id", uid)
        .execute()
    )
    if not existing.data:
        raise HTTPException(404, "Schedule not found")

    sb.table("report_schedules").update({
        "active": False,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }).eq("id", schedule_id).execute()

    logger.info("Schedule %s deactivated", schedule_id)
    return {"message": "Schedule deactivated", "schedule_id": schedule_id}


# ---------------------------------------------------------------------------
# Cron trigger endpoint
# ---------------------------------------------------------------------------

CRON_SECRET = os.getenv("CRON_SECRET", "")


async def _run_single_schedule(schedule: dict) -> Dict[str, Any]:
    """Execute a single scheduled report: generate, compare, email.

    Orchestrates:
      1. Create report row in Supabase
      2. Run the full report pipeline (ingestion → analysis → storage)
      3. Wait for completion
      4. Optionally compare to the previous report
      5. Optionally generate PDF
      6. Send email with results
      7. Update schedule metadata

    Returns a dict with the outcome for logging.
    """
    import traceback as tb
    import asyncio

    schedule_id = schedule["id"]
    domain = schedule["domain"]
    user_id = schedule["user_id"]
    gsc_property = schedule["gsc_property"]
    ga4_property = schedule.get("ga4_property")
    result: Dict[str, Any] = {"schedule_id": schedule_id, "domain": domain}

    try:
        sb = _get_supabase()

        # 1. Create a new report row
        report_id = str(uuid.uuid4())
        sb.table("reports").insert({
            "id": report_id,
            "user_id": user_id,
            "domain": domain,
            "gsc_property": gsc_property,
            "ga4_property": ga4_property,
            "status": "pending",
        }).execute()

        # 2. Run the full report pipeline (sync function — run in executor
        #    to avoid blocking the async event loop)
        from api.worker.report_runner import run_report_pipeline

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None,
            run_report_pipeline,
            report_id,
            user_id,
            gsc_property,
            ga4_property,
            domain,
        )

        # 3. Refresh report status from DB
        report_row = (
            sb.table("reports")
            .select("*")
            .eq("id", report_id)
            .execute()
        )
        report_data = report_row.data[0] if report_row.data else {}
        report_status = report_data.get("status", "unknown")

        if report_status not in ("complete", "completed"):
            result["status"] = "pipeline_failed"
            result["error"] = f"Pipeline ended with status: {report_status}"
            return result

        result["report_id"] = report_id

        # 4. Compare to previous report (if enabled and available)
        comparison = None
        previous_id = schedule.get("last_report_id")
        if schedule.get("include_comparison", True) and previous_id:
            try:
                from api.services.report_comparison import compare_reports
                prev_row = (
                    sb.table("reports")
                    .select("report_data")
                    .eq("id", previous_id)
                    .execute()
                )
                if prev_row.data and prev_row.data[0].get("report_data"):
                    comparison = compare_reports(
                        current=report_data.get("report_data", {}),
                        baseline=prev_row.data[0]["report_data"],
                    )
                    result["comparison"] = "included"
            except Exception as cmp_err:
                logger.warning("Comparison failed for schedule %s: %s", schedule_id, cmp_err)
                result["comparison"] = f"failed: {cmp_err}"

        # 5. Generate PDF (if enabled)
        pdf_bytes = None
        if schedule.get("include_pdf", True):
            try:
                from api.services.pdf_export import generate_report_pdf
                # Gather module results for PDF — column name is "results"
                module_results = {}
                mod_rows = (
                    sb.table("report_modules")
                    .select("module_number, results")
                    .eq("report_id", report_id)
                    .order("module_number")
                    .execute()
                )
                for mr in (mod_rows.data or []):
                    module_results[mr["module_number"]] = mr.get("results", {})

                pdf_bytes = generate_report_pdf(
                    report_data=report_data.get("report_data", {}),
                    module_results=module_results,
                )
                result["pdf"] = "generated"
            except Exception as pdf_err:
                logger.warning("PDF generation failed for schedule %s: %s", schedule_id, pdf_err)
                result["pdf"] = f"failed: {pdf_err}"

        # 6. Send email
        try:
            from api.services.email_delivery import send_report_email

            # Build a report_data dict enriched with domain + id for the
            # email template, plus embed comparison summary if available.
            email_report_data = report_data.get("report_data", {})
            if not isinstance(email_report_data, dict):
                email_report_data = {}
            email_report_data.setdefault("domain", domain)
            email_report_data.setdefault("id", report_id)

            # Include comparison highlights in the email report data so
            # the template can reference them if desired.
            if comparison:
                email_report_data["comparison_summary"] = comparison.get("summary", {})

            email_result = await send_report_email(
                to_email=schedule["email_to"],
                report_data=email_report_data,
                pdf_bytes=pdf_bytes,
                subject=f"Search Intelligence Report — {domain} ({datetime.now(timezone.utc).strftime('%b %d, %Y')})",
            )
            result["email"] = "sent" if email_result.get("success") else f"failed: {email_result.get('error')}"
        except Exception as email_err:
            logger.warning("Email delivery failed for schedule %s: %s", schedule_id, email_err)
            result["email"] = f"failed: {email_err}"

        # 7. Update schedule metadata
        now = datetime.now(timezone.utc)
        next_run = _compute_next_run(
            schedule["frequency"],
            schedule.get("day_of_week", 1),
            from_dt=now,
        )
        sb.table("report_schedules").update({
            "last_run_at": now.isoformat(),
            "last_report_id": report_id,
            "next_run_at": next_run.isoformat(),
            "run_count": schedule.get("run_count", 0) + 1,
            "updated_at": now.isoformat(),
        }).eq("id", schedule_id).execute()

        result["status"] = "success"
        result["next_run_at"] = next_run.isoformat()

    except Exception as exc:
        logger.exception("Schedule %s failed: %s", schedule_id, exc)
        result["status"] = "error"
        result["error"] = str(exc)
        result["traceback"] = tb.format_exc()

    return result


@router.post("/trigger", response_model=TriggerResponse)
async def trigger_due_schedules(
    background_tasks: BackgroundTasks,
    x_cron_secret: Optional[str] = Header(None, alias="X-Cron-Secret"),
):
    """Process all schedules that are due for execution.

    This endpoint is called by an external cron service (e.g. Railway cron,
    GitHub Actions, or an uptime monitor hitting the URL on a schedule).

    Authentication is via the ``X-Cron-Secret`` header matching the
    ``CRON_SECRET`` environment variable.
    """
    if not CRON_SECRET:
        raise HTTPException(503, "Cron trigger not configured (CRON_SECRET not set)")
    if x_cron_secret != CRON_SECRET:
        raise HTTPException(403, "Invalid cron secret")

    sb = _get_supabase()
    now = datetime.now(timezone.utc).isoformat()

    # Find all active schedules where next_run_at <= now
    due = (
        sb.table("report_schedules")
        .select("*")
        .eq("active", True)
        .lte("next_run_at", now)
        .execute()
    )

    schedules = due.data or []
    if not schedules:
        return TriggerResponse(processed=0, succeeded=0, failed=0, details=[])

    logger.info("Found %d due schedules to process", len(schedules))

    results: List[Dict[str, Any]] = []
    succeeded = 0
    failed = 0

    for schedule in schedules:
        outcome = await _run_single_schedule(schedule)
        results.append(outcome)
        if outcome.get("status") == "success":
            succeeded += 1
        else:
            failed += 1

    logger.info(
        "Cron trigger complete: %d processed, %d succeeded, %d failed",
        len(schedules), succeeded, failed,
    )

    return TriggerResponse(
        processed=len(schedules),
        succeeded=succeeded,
        failed=failed,
        details=results,
    )
