"""
Individual module result endpoints.

Provides per-module access to analysis results stored in the
``report_modules`` table.  Used by the frontend to:

- Display individual module loading states during report generation
- Fetch a single module's data without loading the entire report
- List module completion status for progress indicators

All endpoints require JWT authentication via ``get_current_user``.
Report ownership is enforced — users can only access their own reports'
module results.
"""

import logging
import os
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from api.auth.dependencies import get_current_user

logger = logging.getLogger(__name__)
router = APIRouter()

# ---------------------------------------------------------------------------
# Module metadata — maps module numbers to human-readable names and
# section titles used in the frontend report viewer.
# ---------------------------------------------------------------------------

MODULE_META = {
    1: {"name": "health_trajectory", "title": "Health & Trajectory", "phase": 1},
    2: {"name": "page_triage", "title": "Page-Level Triage", "phase": 1},
    3: {"name": "serp_landscape", "title": "SERP Landscape", "phase": 2},
    4: {"name": "content_intelligence", "title": "Content Intelligence", "phase": 3},
    5: {"name": "gameplan", "title": "Strategic Gameplan", "phase": 1},
    6: {"name": "algorithm_impact", "title": "Algorithm Impact", "phase": 3},
    7: {"name": "intent_migration", "title": "Intent Migration", "phase": 3},
    8: {"name": "technical_health", "title": "CTR & Technical Health", "phase": 2},
    9: {"name": "site_architecture", "title": "Site Architecture", "phase": 3},
    10: {"name": "branded_split", "title": "Branded vs Non-Branded", "phase": 4},
    11: {"name": "competitive_threats", "title": "Competitive Radar", "phase": 2},
    12: {"name": "revenue_attribution", "title": "Revenue Attribution", "phase": 4},
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
    """Extract the canonical user identifier from the JWT user dict."""
    return user.get("sub", user.get("id", user.get("user_id", "")))


async def _verify_report_ownership(report_id: str, user: dict) -> dict:
    """Fetch a report and verify it belongs to the authenticated user."""
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
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/meta")
async def list_module_metadata() -> Dict[str, Any]:
    """
    Return metadata for all 12 analysis modules.

    Public endpoint (no auth required) — used by the frontend to render
    module names, titles, and phase information in the UI before a report
    is generated.
    """
    return {
        "modules": [
            {"number": num, **meta}
            for num, meta in sorted(MODULE_META.items())
        ],
        "total": len(MODULE_META),
        "phases": {
            1: "MVP (Health, Triage, Gameplan)",
            2: "SERP Intelligence",
            3: "Deep Analysis",
            4: "Revenue & Polish",
        },
    }


@router.get("/{report_id}")
async def list_module_results(
    report_id: str,
    user: dict = Depends(get_current_user),
) -> Dict[str, Any]:
    """
    List all module results for a report with status and metadata.

    Returns a summary for each of the 12 modules including:
    - completion status (pending, running, completed, failed)
    - execution metadata (timing, error info)
    - whether data is available

    Does NOT include the full results payload — use
    GET /{report_id}/{module_number} for that.
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
            detail=f"Failed to fetch module results: {str(e)}",
        )

    # Build a lookup of stored modules
    stored = {}
    for row in result.data or []:
        num = row.get("module_number")
        if num is not None:
            stored[int(num)] = row

    # Merge with metadata to ensure all 12 modules appear
    modules: List[Dict[str, Any]] = []
    for num in sorted(MODULE_META.keys()):
        meta = MODULE_META[num]
        row = stored.get(num, {})

        modules.append({
            "module_number": num,
            "name": meta["name"],
            "title": meta["title"],
            "phase": meta["phase"],
            "status": row.get("status", "pending"),
            "completed_at": row.get("completed_at"),
            "has_data": num in stored and row.get("status") == "completed",
        })

    completed = sum(1 for m in modules if m["status"] == "completed")
    failed = sum(1 for m in modules if m["status"] == "failed")

    return {
        "report_id": report_id,
        "domain": report.get("domain", ""),
        "report_status": report.get("status", "unknown"),
        "modules": modules,
        "summary": {
            "total": len(MODULE_META),
            "completed": completed,
            "failed": failed,
            "pending": len(MODULE_META) - completed - failed,
        },
    }


@router.get("/{report_id}/{module_number}")
async def get_module_result(
    report_id: str,
    module_number: int,
    user: dict = Depends(get_current_user),
) -> Dict[str, Any]:
    """
    Get the full result data for a specific module.

    Returns the complete analysis output including all charts, tables,
    and recommendation data for the specified module number (1-12).
    """
    if module_number < 1 or module_number > 12:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid module number {module_number}. Must be 1-12.",
        )

    await _verify_report_ownership(report_id, user)

    supabase = _get_supabase()
    try:
        result = (
            supabase.table("report_modules")
            .select("*")
            .eq("report_id", report_id)
            .eq("module_number", module_number)
            .execute()
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch module result: {str(e)}",
        )

    if not result.data:
        meta = MODULE_META.get(module_number, {})
        return {
            "report_id": report_id,
            "module_number": module_number,
            "name": meta.get("name", f"module_{module_number}"),
            "title": meta.get("title", f"Module {module_number}"),
            "status": "pending",
            "results": None,
            "message": "This module has not been executed yet.",
        }

    row = result.data[0]
    meta = MODULE_META.get(module_number, {})

    response: Dict[str, Any] = {
        "report_id": report_id,
        "module_number": module_number,
        "name": meta.get("name", row.get("module_name", "")),
        "title": meta.get("title", f"Module {module_number}"),
        "status": row.get("status", "unknown"),
        "completed_at": row.get("completed_at"),
        "results": row.get("results"),
    }

    # Add user-friendly message for non-success states
    if row.get("status") == "failed":
        results = row.get("results") or {}
        if isinstance(results, dict) and results.get("skipped"):
            response["message"] = results.get("reason", "This module was skipped.")
        else:
            response["message"] = (
                "This analysis encountered an error. "
                "Other sections of the report are unaffected."
            )
    elif row.get("status") == "completed":
        response["message"] = "Analysis completed successfully."

    return response
