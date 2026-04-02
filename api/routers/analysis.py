"""
Analysis router — trigger, monitor, and manage analysis pipeline runs.

Provides the /api/analysis/* endpoint group that main.py expects.
Covers:
  - POST /run/{report_id}     — kick off the full 12-module analysis pipeline
  - GET  /status/{report_id}  — poll pipeline progress (which modules done/running/pending)
  - POST /rerun/{report_id}/{module_number} — re-run a single failed module
  - GET  /config              — return current pipeline config (module order, timeouts)

All endpoints require JWT authentication and enforce report ownership.
"""

import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query

from api.auth.dependencies import get_current_user

logger = logging.getLogger(__name__)
router = APIRouter()

# ---------------------------------------------------------------------------
# Module execution order — matches pipeline.py sequencing
# Phase 1 first, then 2, then 3, then 4. Module 5 (Gameplan) runs last
# within Phase 1 because it synthesises Modules 1 + 2.
# ---------------------------------------------------------------------------

MODULE_EXECUTION_ORDER = [1, 2, 5, 3, 8, 11, 4, 6, 7, 9, 10, 12]

MODULE_NAMES = {
    1: "health_trajectory",
    2: "page_triage",
    3: "serp_landscape",
    4: "content_intelligence",
    5: "gameplan",
    6: "algorithm_impact",
    7: "intent_migration",
    8: "ctr_modeling",
    9: "site_architecture",
    10: "branded_split",
    11: "competitive_radar",
    12: "revenue_attribution",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_supabase():
    """Get Supabase client via centralized database module."""
    try:
        from api.database import get_supabase_client
        return get_supabase_client()
    except (ValueError, Exception) as e:
        raise HTTPException(status_code=500, detail=f"Supabase not configured: {e}")


def _user_id(user: dict) -> str:
    """Extract canonical user ID from JWT payload."""
    return user.get("sub", user.get("id", user.get("user_id", "")))


async def _verify_report_ownership(report_id: str, user: dict) -> dict:
    """Fetch a report row and verify it belongs to the authenticated user."""
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


# ---------------------------------------------------------------------------
# Background task — run pipeline
# ---------------------------------------------------------------------------


async def _run_pipeline_background(report_id: str, user_id: str):
    """
    Execute the full analysis pipeline as a background task.

    Imports pipeline.py and runs all 12 modules in order.
    Updates report status in Supabase as it progresses.
    """
    supabase = _get_supabase()

    try:
        # Mark report as analysing
        supabase.table("reports").update({
            "status": "analyzing",
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }).eq("id", report_id).execute()

        # Import and run pipeline
        try:
            from api.worker.pipeline import run_pipeline

            await run_pipeline(report_id=report_id, user_id=user_id)
        except ImportError:
            logger.error("Cannot import pipeline — api.worker.pipeline.run_pipeline not found")
            supabase.table("reports").update({
                "status": "failed",
                "error": "Pipeline module not available",
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }).eq("id", report_id).execute()
            return

        # Mark complete
        supabase.table("reports").update({
            "status": "complete",
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }).eq("id", report_id).execute()

        logger.info("Pipeline completed for report %s", report_id)

    except Exception as e:
        logger.exception("Pipeline failed for report %s: %s", report_id, str(e))
        try:
            supabase.table("reports").update({
                "status": "failed",
                "error": str(e)[:500],
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }).eq("id", report_id).execute()
        except Exception:
            logger.exception("Failed to update report status after pipeline error")


async def _rerun_single_module(report_id: str, module_number: int, user_id: str):
    """Re-run a single analysis module as a background task."""
    supabase = _get_supabase()

    module_name = MODULE_NAMES.get(module_number, f"module_{module_number}")

    try:
        # Update module status to running
        supabase.table("report_modules").update({
            "status": "running",
        }).eq("report_id", report_id).eq("module_number", module_number).execute()

        # Import the specific module runner
        try:
            from api.worker.pipeline import run_single_module

            await run_single_module(
                report_id=report_id,
                module_number=module_number,
                user_id=user_id,
            )
        except ImportError:
            # Fallback: try importing the module directly
            logger.warning("run_single_module not available — attempting direct module import")
            supabase.table("report_modules").update({
                "status": "failed",
                "results": {"error": "Single module re-run not yet supported"},
            }).eq("report_id", report_id).eq("module_number", module_number).execute()
            return

        logger.info("Module %d (%s) re-run complete for report %s", module_number, module_name, report_id)

    except Exception as e:
        logger.exception("Module %d re-run failed for report %s: %s", module_number, report_id, str(e))
        try:
            supabase.table("report_modules").update({
                "status": "failed",
                "results": {"error": str(e)[:500]},
            }).eq("report_id", report_id).eq("module_number", module_number).execute()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post("/run/{report_id}")
async def trigger_analysis(
    report_id: str,
    background_tasks: BackgroundTasks,
    user: dict = Depends(get_current_user),
) -> Dict[str, Any]:
    """
    Trigger the full 12-module analysis pipeline for a report.

    Prerequisites:
    - Report must exist and belong to the authenticated user.
    - Report status should be 'ingested' (data ingestion complete).
    - If report is already 'analyzing' or 'complete', returns an error.

    The pipeline runs asynchronously as a background task. Poll
    GET /status/{report_id} to track progress.
    """
    report = await _verify_report_ownership(report_id, user)

    current_status = report.get("status", "")
    if current_status == "analyzing":
        raise HTTPException(
            status_code=409,
            detail="Analysis is already running for this report.",
        )

    # Allow re-running from 'complete' or 'failed' states
    if current_status not in ("ingested", "complete", "failed", "pending"):
        raise HTTPException(
            status_code=400,
            detail=f"Cannot start analysis — report status is '{current_status}'. "
                   f"Expected 'ingested', 'complete', or 'failed'.",
        )

    uid = _user_id(user)
    background_tasks.add_task(_run_pipeline_background, report_id, uid)

    return {
        "report_id": report_id,
        "status": "analyzing",
        "message": "Analysis pipeline started. Poll GET /api/analysis/status/{report_id} for progress.",
        "modules_total": len(MODULE_EXECUTION_ORDER),
        "execution_order": MODULE_EXECUTION_ORDER,
    }


@router.get("/status/{report_id}")
async def get_analysis_status(
    report_id: str,
    user: dict = Depends(get_current_user),
) -> Dict[str, Any]:
    """
    Get detailed analysis pipeline status for a report.

    Returns per-module completion status so the frontend can show
    a progress indicator during report generation.
    """
    report = await _verify_report_ownership(report_id, user)

    supabase = _get_supabase()
    try:
        result = (
            supabase.table("report_modules")
            .select("module_number, module_name, status, completed_at")
            .eq("report_id", report_id)
            .order("module_number")
            .execute()
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch module status: {str(e)}",
        )

    stored = {}
    for row in result.data or []:
        num = row.get("module_number")
        if num is not None:
            stored[int(num)] = row

    modules: List[Dict[str, Any]] = []
    for num in MODULE_EXECUTION_ORDER:
        row = stored.get(num, {})
        modules.append({
            "module_number": num,
            "name": MODULE_NAMES.get(num, f"module_{num}"),
            "status": row.get("status", "pending"),
            "completed_at": row.get("completed_at"),
        })

    completed = sum(1 for m in modules if m["status"] == "completed")
    running = sum(1 for m in modules if m["status"] == "running")
    failed = sum(1 for m in modules if m["status"] == "failed")
    pending = len(modules) - completed - running - failed

    # Estimate progress percentage
    progress_pct = round((completed / len(modules)) * 100) if modules else 0

    return {
        "report_id": report_id,
        "report_status": report.get("status", "unknown"),
        "progress_percent": progress_pct,
        "modules": modules,
        "summary": {
            "total": len(modules),
            "completed": completed,
            "running": running,
            "failed": failed,
            "pending": pending,
        },
    }


@router.post("/rerun/{report_id}/{module_number}")
async def rerun_module(
    report_id: str,
    module_number: int,
    background_tasks: BackgroundTasks,
    user: dict = Depends(get_current_user),
) -> Dict[str, Any]:
    """
    Re-run a single analysis module that previously failed.

    Useful when a module failed due to a transient error (API timeout,
    rate limit) and the user wants to retry just that module without
    re-running the entire pipeline.
    """
    if module_number < 1 or module_number > 12:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid module number {module_number}. Must be 1-12.",
        )

    report = await _verify_report_ownership(report_id, user)

    # Only allow re-run if the report is in a terminal state
    report_status = report.get("status", "")
    if report_status == "analyzing":
        raise HTTPException(
            status_code=409,
            detail="Cannot re-run a module while the pipeline is still running.",
        )

    uid = _user_id(user)
    background_tasks.add_task(_rerun_single_module, report_id, module_number, uid)

    return {
        "report_id": report_id,
        "module_number": module_number,
        "name": MODULE_NAMES.get(module_number, f"module_{module_number}"),
        "status": "running",
        "message": f"Module {module_number} re-run started.",
    }


@router.get("/config")
async def get_pipeline_config(
    user: dict = Depends(get_current_user),
) -> Dict[str, Any]:
    """
    Return the current analysis pipeline configuration.

    Useful for the frontend to understand module ordering,
    dependencies, and display metadata.
    """
    return {
        "execution_order": MODULE_EXECUTION_ORDER,
        "modules": [
            {"number": num, "name": MODULE_NAMES.get(num, f"module_{num}")}
            for num in MODULE_EXECUTION_ORDER
        ],
        "total_modules": len(MODULE_EXECUTION_ORDER),
        "phases": {
            "phase_1": {"modules": [1, 2, 5], "name": "MVP (Health, Triage, Gameplan)"},
            "phase_2": {"modules": [3, 8, 11], "name": "SERP Intelligence"},
            "phase_3": {"modules": [4, 6, 7, 9], "name": "Deep Analysis"},
            "phase_4": {"modules": [10, 12], "name": "Revenue & Polish"},
        },
    }
