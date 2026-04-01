"""
Background report runner — bridges report creation to pipeline execution.

When a user creates a report via POST /reports/create, this module is
invoked as a FastAPI BackgroundTask. It:

1. Updates the report status to "ingesting"
2. Fetches the user's OAuth credentials from Supabase
3. Pulls GSC + GA4 data via the ingestion layer
4. Runs the AnalysisPipeline across all 12 modules
5. Stores each module result in the report_modules table
6. Writes the assembled report_data JSON to the reports table
7. Updates report status to "completed" (or "partial" / "failed")

This is the critical glue between the REST API and the analysis engine.
"""

import logging
import os
import traceback
from datetime import datetime
from typing import Any, Dict, Optional

from supabase import create_client, Client

logger = logging.getLogger(__name__)

# Module number mapping for report_modules table
MODULE_NUMBERS = {
    "health_trajectory": 1,
    "page_triage": 2,
    "serp_landscape": 3,
    "content_intelligence": 4,
    "gameplan": 5,
    "algorithm_impact": 6,
    "intent_migration": 7,
    "technical_health": 8,
    "site_architecture": 9,
    "branded_split": 10,
    "competitive_threats": 11,
    "revenue_attribution": 12,
}


def _get_supabase() -> Client:
    """Get a Supabase client using the service role key for background work."""
    url = os.getenv("SUPABASE_URL", "")
    # Prefer service role key for background tasks (bypasses RLS)
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "") or os.getenv("SUPABASE_KEY", "")
    if not url or not key:
        raise RuntimeError("Supabase not configured — set SUPABASE_URL and SUPABASE_KEY")
    return create_client(url, key)


def _update_report_status(
    supabase: Client,
    report_id: str,
    status: str,
    *,
    current_module: Optional[int] = None,
    progress: Optional[Dict] = None,
    error_message: Optional[str] = None,
    report_data: Optional[Dict] = None,
) -> None:
    """Update report row in Supabase."""
    update: Dict[str, Any] = {
        "status": status,
        "updated_at": datetime.utcnow().isoformat(),
    }
    if current_module is not None:
        update["current_module"] = current_module
    if progress is not None:
        update["progress"] = progress
    if error_message is not None:
        update["error_message"] = error_message
    if report_data is not None:
        update["report_data"] = report_data
    if status in ("completed", "complete", "partial", "failed"):
        update["completed_at"] = datetime.utcnow().isoformat()

    try:
        supabase.table("reports").update(update).eq("id", report_id).execute()
    except Exception as exc:
        logger.error("Failed to update report %s status to %s: %s", report_id, status, exc)


def _store_module_result(
    supabase: Client,
    report_id: str,
    module_name: str,
    module_number: int,
    results: Optional[Dict],
    status: str,
) -> None:
    """Upsert a row in report_modules for one analysis module."""
    row = {
        "report_id": report_id,
        "module_number": module_number,
        "module_name": module_name,
        "results": results,
        "status": status,
        "completed_at": datetime.utcnow().isoformat() if status in ("completed", "failed") else None,
    }
    try:
        # Try insert first; on conflict (report_id, module_number) do update
        supabase.table("report_modules").upsert(
            row, on_conflict="report_id,module_number"
        ).execute()
    except Exception as exc:
        logger.warning("Failed to store module %s result for report %s: %s", module_name, report_id, exc)


def _fetch_user_credentials(supabase: Client, user_id: str) -> Dict[str, Any]:
    """Fetch OAuth tokens for a user from the users table."""
    try:
        result = supabase.table("users").select("gsc_token, ga4_token, email").eq("id", user_id).execute()
        if result.data:
            return result.data[0]
    except Exception as exc:
        logger.error("Failed to fetch credentials for user %s: %s", user_id, exc)
    return {}


def _ingest_gsc_data(credentials: Dict[str, Any], gsc_property: str) -> Dict[str, Any]:
    """
    Pull GSC data using the ingestion module.
    Returns a dict of DataFrames/lists keyed by data type.
    Falls back to empty data on failure.
    """
    gsc_data: Dict[str, Any] = {}
    try:
        from api.ingestion.gsc import GSCClient
        client = GSCClient(credentials)

        # Daily time series (clicks, impressions)
        gsc_data["gsc_daily_data"] = client.get_performance_by_date(
            site_url=gsc_property, months=16
        )

        # Per-page aggregates
        gsc_data["gsc_page_summary"] = client.get_performance_by_page(
            site_url=gsc_property, months=16
        )

        # Per-page daily time series
        gsc_data["gsc_page_daily_data"] = client.get_performance_by_page_and_date(
            site_url=gsc_property, months=16
        )

        # Keyword data
        gsc_data["gsc_keyword_data"] = client.get_performance_by_query(
            site_url=gsc_property, months=16
        )

        # Query-page mapping
        gsc_data["gsc_query_page_data"] = client.get_performance_by_query_and_page(
            site_url=gsc_property, months=16
        )

        # Query time series
        gsc_data["gsc_query_date_data"] = client.get_performance_by_query_and_date(
            site_url=gsc_property, months=16
        )

        # Query-level summary
        gsc_data["gsc_query_data"] = gsc_data["gsc_keyword_data"]

        logger.info("GSC ingestion complete for %s", gsc_property)
    except Exception as exc:
        logger.error("GSC ingestion failed for %s: %s", gsc_property, exc)
    return gsc_data


def _ingest_ga4_data(credentials: Dict[str, Any], ga4_property: str) -> Dict[str, Any]:
    """
    Pull GA4 data using the ingestion module.
    Falls back to empty data on failure.
    """
    ga4_data: Dict[str, Any] = {}
    try:
        from api.ingestion.ga4 import GA4Client
        client = GA4Client(credentials)

        ga4_data["ga4_landing_pages"] = client.get_landing_pages(
            property_id=ga4_property, months=16
        )
        ga4_data["ga4_conversions"] = client.get_conversions(
            property_id=ga4_property, months=16
        )
        ga4_data["ga4_engagement_data"] = client.get_engagement_metrics(
            property_id=ga4_property, months=16
        )

        logger.info("GA4 ingestion complete for %s", ga4_property)
    except Exception as exc:
        logger.error("GA4 ingestion failed for %s: %s", ga4_property, exc)
    return ga4_data


def run_report_pipeline(report_id: str, user_id: str, gsc_property: str, ga4_property: Optional[str], domain: str) -> None:
    """
    Main entry point — run as a FastAPI BackgroundTask.

    Orchestrates: ingestion → pipeline → storage → status update.
    Catches all exceptions so it never crashes the API process.
    """
    logger.info("=== Starting report pipeline for report_id=%s, domain=%s ===", report_id, domain)

    try:
        supabase = _get_supabase()
    except Exception as exc:
        logger.critical("Cannot connect to Supabase: %s", exc)
        return

    try:
        # ----- Phase 1: Ingestion -----
        _update_report_status(supabase, report_id, "ingesting", current_module=0)

        user_creds = _fetch_user_credentials(supabase, user_id)
        gsc_token = user_creds.get("gsc_token") or {}
        ga4_token = user_creds.get("ga4_token") or {}

        # Build data context from ingestion
        data_context: Dict[str, Any] = {"brand_terms": [domain.replace(".", ""), domain.split(".")[0]]}

        # GSC ingestion
        if gsc_token:
            gsc_data = _ingest_gsc_data(gsc_token, gsc_property)
            data_context.update(gsc_data)
        else:
            logger.warning("No GSC token for user %s — modules will receive empty data", user_id)

        # GA4 ingestion (optional)
        if ga4_property and ga4_token:
            ga4_data = _ingest_ga4_data(ga4_token, ga4_property)
            data_context.update(ga4_data)
        else:
            logger.info("GA4 not configured — skipping GA4 ingestion")

        # ----- Phase 2: Pipeline execution -----
        _update_report_status(supabase, report_id, "running", current_module=1)

        from api.worker.pipeline import AnalysisPipeline
        pipeline = AnalysisPipeline()

        # Run all 12 modules
        pipeline_result = pipeline.execute(data_context)
        report_data = pipeline.get_report_data(pipeline_result)

        # ----- Phase 3: Store results -----
        _update_report_status(supabase, report_id, "analyzing")

        progress: Dict[str, str] = {}
        for module_result in pipeline_result.modules:
            module_name = module_result.module_name
            module_num = MODULE_NUMBERS.get(module_name, 0)

            _store_module_result(
                supabase,
                report_id,
                module_name,
                module_num,
                module_result.data,
                "completed" if module_result.status == "success" else module_result.status,
            )
            progress[f"module_{module_num}"] = module_result.status

            # Update progress in real time so frontend can poll
            _update_report_status(
                supabase, report_id, "running",
                current_module=module_num,
                progress=progress,
            )

        # ----- Phase 4: Finalize -----
        final_status = "completed" if pipeline_result.status == "complete" else pipeline_result.status
        _update_report_status(
            supabase,
            report_id,
            final_status,
            progress=progress,
            report_data=report_data,
        )

        successful = sum(1 for m in pipeline_result.modules if m.status == "success")
        total = len(pipeline_result.modules)
        logger.info(
            "=== Report %s finished: %s (%d/%d modules) ===",
            report_id, final_status, successful, total,
        )

    except Exception as exc:
        tb = traceback.format_exc()
        logger.error("Report pipeline failed for %s: %s\n%s", report_id, exc, tb)
        try:
            _update_report_status(
                supabase, report_id, "failed",
                error_message=f"{type(exc).__name__}: {str(exc)[:500]}",
            )
        except Exception:
            logger.critical("Could not mark report %s as failed", report_id)
