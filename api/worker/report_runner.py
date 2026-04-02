"""
Background report runner — bridges report creation to pipeline execution.

When a user creates a report via POST /reports/create, this module is
invoked as a FastAPI BackgroundTask. It:

1. Updates the report status to "ingesting"
2. Fetches the user's OAuth credentials from Supabase
3. Pulls GSC + GA4 data via the ingestion layer
4. Pulls live SERP data via DataForSEO for top non-branded keywords
5. Crawls the site for internal link graph + page metadata
6. Runs the AnalysisPipeline across all 12 modules
7. Stores each module result in the report_modules table **in real time**
   via a progress_callback (so the frontend sees per-module updates)
8. Writes the assembled report_data JSON to the reports table
9. Updates report status to "completed" (or "partial" / "failed")

This is the critical glue between the REST API and the analysis engine.
"""

import asyncio
import logging
import os
import traceback
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from supabase import Client

from api.database import get_service_role_client

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
    """Get a Supabase client using the service role key for background work.

    Delegates to the centralized api.database.get_service_role_client()
    which tries all env-var naming conventions (SUPABASE_SERVICE_ROLE_KEY,
    SUPABASE_SERVICE_KEY, SUPABASE_KEY, SUPABASE_ANON_KEY) and caches the
    client instance via @lru_cache.

    This ensures the report runner uses the same client configuration as
    all API routers — eliminating env-var mismatch bugs that could silently
    break report generation.
    """
    return get_service_role_client()


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
    """Upsert a row in report_modules for one analysis module.

    The report_modules table has a CHECK constraint limiting status to:
    ('pending', 'running', 'completed', 'failed').  The pipeline also
    produces 'skipped' (dependency not met) and 'success' statuses —
    we map those to DB-safe values here.
    """
    # Map pipeline statuses to DB-allowed values
    ALLOWED_STATUSES = {"pending", "running", "completed", "failed"}
    if status == "success":
        db_status = "completed"
    elif status in ALLOWED_STATUSES:
        db_status = status
    else:
        # 'skipped' and any unexpected status → 'failed'
        db_status = "failed"

    row = {
        "report_id": report_id,
        "module_number": module_number,
        "module_name": module_name,
        "results": results,
        "status": db_status,
        "completed_at": datetime.utcnow().isoformat() if db_status in ("completed", "failed") else None,
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


def _decrypt_token(raw_token: Any) -> Dict[str, Any]:
    """
    Decrypt an OAuth token from its stored (potentially encrypted) form.

    The oauth.py module encrypts tokens via Fernet before storing them in
    Supabase JSONB fields.  The stored value is a base64-encoded encrypted
    string.  This function handles three cases:

    1. The token is already a plain dict (unencrypted or test data) → return as-is.
    2. The token is an encrypted string → decrypt via TokenEncryption.
    3. The token is None / empty → return empty dict.

    Without this step, passing an encrypted string to GSCClient/GA4Client
    causes AttributeError because str has no .get() method, silently
    breaking ALL report generation.
    """
    if not raw_token:
        return {}

    # Already a dict — no decryption needed (backward compat / test data)
    if isinstance(raw_token, dict):
        # Verify it looks like a credential dict (has at least 'token' or 'access_token')
        if raw_token.get("token") or raw_token.get("access_token") or raw_token.get("refresh_token"):
            return raw_token
        # Could be a nested encrypted structure — try decryption of values
        logger.warning("Token dict has unexpected structure (keys: %s) — using as-is", list(raw_token.keys()))
        return raw_token

    # Must be an encrypted string — decrypt it
    if isinstance(raw_token, str):
        try:
            from api.auth.oauth import encryptor
            decrypted = encryptor.decrypt(raw_token)
            logger.info("Successfully decrypted OAuth token (%d fields)", len(decrypted))
            return decrypted
        except ImportError:
            logger.error("Cannot import encryptor from api.auth.oauth — token decryption unavailable")
            return {}
        except ValueError as exc:
            logger.error("Failed to decrypt OAuth token: %s", exc)
            return {}
        except Exception as exc:
            logger.error("Unexpected error decrypting token: %s", exc)
            return {}

    logger.warning("Token has unexpected type %s — returning empty", type(raw_token).__name__)
    return {}


def _ingest_gsc_data(credentials: Dict[str, Any], gsc_property: str) -> Dict[str, Any]:
    """
    Pull GSC data using the ingestion module.

    Returns a dict of DataFrames keyed by data type, ready for the
    pipeline's data_context.

    The GSCClient exposes ``fetch_*`` methods that accept
    ``(site_url, start_date, end_date)`` where dates are
    ``datetime.datetime`` objects.  We compute a 16-month window here.

    Falls back to empty data on failure so the pipeline can still run
    with whatever data is available.
    """
    gsc_data: Dict[str, Any] = {}
    try:
        from api.ingestion.gsc import GSCClient

        client = GSCClient(credentials)

        # 16 months of history, matching the spec requirement
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=16 * 30)

        # Daily time series (clicks, impressions) — Module 1, 6
        gsc_data["gsc_daily_data"] = client.fetch_daily_data(
            gsc_property, start_date, end_date
        )

        # Per-page aggregates — Module 2, 9, 12
        gsc_data["gsc_page_summary"] = client.fetch_page_data(
            gsc_property, start_date, end_date
        )

        # Per-page daily time series — Module 2
        gsc_data["gsc_page_daily_data"] = client.fetch_page_date_data(
            gsc_property, start_date, end_date
        )

        # Keyword data — Module 3, 8, 10, 11
        gsc_data["gsc_keyword_data"] = client.fetch_query_data(
            gsc_property, start_date, end_date
        )

        # Query-page mapping — Module 4
        gsc_data["gsc_query_page_data"] = client.fetch_query_page_data(
            gsc_property, start_date, end_date
        )

        # Query time series — Module 7
        gsc_data["gsc_query_date_data"] = client.fetch_query_date_data(
            gsc_property, start_date, end_date
        )

        # Query-level summary (alias used by Module 10 branded_split)
        gsc_data["gsc_query_data"] = gsc_data["gsc_keyword_data"]

        logger.info("GSC ingestion complete for %s", gsc_property)
    except Exception as exc:
        logger.error("GSC ingestion failed for %s: %s", gsc_property, exc)
    return gsc_data


def _ingest_ga4_data(credentials: Dict[str, Any], ga4_property: str) -> Dict[str, Any]:
    """
    Pull GA4 data using the comprehensive ingestion function.

    Uses ``api.ingestion.ga4.ingest_ga4_data`` which handles:
      - Credentials construction
      - Date range calculation (16 months)
      - All 8 GA4 report sections with individual error handling
      - Graceful fallbacks for missing data

    The returned dict maps ingestion keys to the pipeline's
    data_context keys:
      - ``landing_pages``  →  ``ga4_landing_pages``
      - ``conversions``    →  ``ga4_conversions``
      - ``traffic_overview`` → ``ga4_engagement_data``

    Falls back to empty data on failure.
    """
    ga4_data: Dict[str, Any] = {}
    try:
        from google.oauth2.credentials import Credentials as GoogleCredentials
        from api.ingestion.ga4 import ingest_ga4_data

        # Convert the decrypted token dict into a Google Credentials object.
        # The GA4 client needs a real Credentials instance (not a plain dict)
        # because the BetaAnalyticsDataClient calls .token and .refresh() on it.
        creds = GoogleCredentials(
            token=credentials.get("token"),
            refresh_token=credentials.get("refresh_token"),
            token_uri=credentials.get(
                "token_uri", "https://oauth2.googleapis.com/token"
            ),
            client_id=credentials.get("client_id"),
            client_secret=credentials.get("client_secret"),
        )

        # ingest_ga4_data handles property_id formatting, date range
        # calculation, client construction, and all 8 report sections.
        result = ingest_ga4_data(creds, ga4_property, months_back=16)

        # Map ingestion output keys → pipeline data_context keys.
        # ingest_ga4_data returns 8 sections.  Previously only 3 were
        # mapped, discarding ecommerce, channel, source/medium, device,
        # and page-date-series data.  Module 12 (Revenue Attribution)
        # accepts ga4_ecommerce but it was always None.
        _empty = {"rows": [], "metadata": {}}
        ga4_data["ga4_landing_pages"] = result.get("landing_pages", _empty)
        ga4_data["ga4_conversions"] = result.get("conversions", _empty)
        ga4_data["ga4_engagement_data"] = result.get("traffic_overview", _empty)
        ga4_data["ga4_ecommerce"] = result.get("ecommerce", _empty)
        ga4_data["ga4_channel_data"] = result.get("channel_performance", _empty)
        ga4_data["ga4_source_medium"] = result.get("source_medium", _empty)
        ga4_data["ga4_device_data"] = result.get("device_breakdown", _empty)
        ga4_data["ga4_page_date_series"] = result.get("page_date_series", _empty)

        logger.info(
            "GA4 ingestion complete for %s — %d sections with data",
            ga4_property,
            len([k for k, v in ga4_data.items() if v.get("rows")]),
        )
    except Exception as exc:
        logger.error("GA4 ingestion failed for %s: %s", ga4_property, exc)
    return ga4_data


def _extract_top_nonbranded_keywords(
    gsc_keyword_data: Any,
    brand_terms: List[str],
    max_keywords: int = 50,
) -> List[str]:
    """
    Extract top non-branded keywords from GSC keyword data, sorted by
    impressions descending.

    Handles both DataFrame (pandas) and list-of-dicts formats from the
    GSC ingestion layer.
    """
    keywords: List[str] = []
    try:
        # Normalise brand terms for case-insensitive matching
        brand_lower = [b.lower() for b in brand_terms if b]

        rows: List[Dict[str, Any]] = []

        # Accept pandas DataFrame
        try:
            import pandas as pd
            if isinstance(gsc_keyword_data, pd.DataFrame) and not gsc_keyword_data.empty:
                rows = gsc_keyword_data.to_dict("records")
        except ImportError:
            pass

        # Accept list of dicts
        if not rows and isinstance(gsc_keyword_data, list):
            rows = gsc_keyword_data

        if not rows:
            logger.info("No keyword rows available for SERP extraction")
            return []

        # Filter out branded queries
        filtered: List[Dict[str, Any]] = []
        for row in rows:
            query = str(row.get("query", row.get("keys", [""])[0] if isinstance(row.get("keys"), list) else "")).lower()
            if not query:
                continue
            is_branded = any(brand in query for brand in brand_lower)
            if not is_branded:
                filtered.append({"query": query, "impressions": float(row.get("impressions", 0))})

        # Sort by impressions descending and take top N
        filtered.sort(key=lambda r: r["impressions"], reverse=True)
        keywords = [r["query"] for r in filtered[:max_keywords]]

        logger.info(
            "Extracted %d non-branded keywords from %d total (brand terms: %s)",
            len(keywords), len(rows), brand_lower,
        )
    except Exception as exc:
        logger.error("Failed to extract top keywords: %s", exc)

    return keywords


def _ingest_serp_data(
    supabase: Client,
    keywords: List[str],
    domain: str,
    budget: float = 0.20,
) -> Dict[str, Any]:
    """
    Pull live SERP data from DataForSEO for the given keywords.

    The DataForSEO client is async, so we run it in an event loop.
    Falls back gracefully if credentials are missing or API errors occur.

    Returns a dict suitable for data_context["serp_data"].
    """
    if not keywords:
        logger.info("No keywords provided for SERP ingestion — skipping")
        return {}

    # Check credentials early to avoid noisy errors
    login = os.getenv("DATAFORSEO_LOGIN", "")
    password = os.getenv("DATAFORSEO_PASSWORD", "")
    if not login or not password:
        logger.warning(
            "DATAFORSEO_LOGIN / DATAFORSEO_PASSWORD not set — skipping SERP ingestion. "
            "Modules 3, 8, 11 will run without live SERP data."
        )
        return {}

    try:
        from api.ingestion.dataforseo import fetch_serps_for_top_keywords

        # Run the async function in a new event loop (we are in a sync
        # background-task context, so there is no running loop).
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            # Already inside an async context — create a task
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                serp_result = pool.submit(
                    asyncio.run,
                    fetch_serps_for_top_keywords(
                        supabase=supabase,
                        keywords=keywords,
                        user_domain=domain,
                        max_keywords=min(len(keywords), 50),
                        budget=budget,
                    ),
                ).result(timeout=300)
        else:
            serp_result = asyncio.run(
                fetch_serps_for_top_keywords(
                    supabase=supabase,
                    keywords=keywords,
                    user_domain=domain,
                    max_keywords=min(len(keywords), 50),
                    budget=budget,
                )
            )

        successful = serp_result.get("successful_fetches", 0)
        failed = serp_result.get("failed_fetches", 0)
        spending = serp_result.get("spending", {})
        logger.info(
            "SERP ingestion complete: %d succeeded, %d failed, spent $%.4f",
            successful, failed, spending.get("total_spent", 0),
        )
        return serp_result

    except ValueError as exc:
        # DataForSEOClient raises ValueError when credentials are missing
        logger.warning("DataForSEO client init failed (credentials?): %s", exc)
        return {}
    except Exception as exc:
        logger.error("SERP ingestion failed: %s", exc)
        return {}


def _ingest_crawl_data(domain: str, max_pages: int = 200) -> Dict[str, Any]:
    """
    Crawl the site to extract page metadata and internal link graph.

    Falls back gracefully if the crawler fails or the site is unreachable.
    The crawl_result dict is designed to be consumed directly by the
    pipeline's data_context (keys: crawl_data, internal_link_graph,
    sitemap_urls).

    Returns a dict with:
      - crawl_data: full crawl result (pages + link_graph + stats)
      - internal_link_graph: alias for crawl_data (Module 9 compat)
      - sitemap_urls: list of URLs found in sitemap
    """
    try:
        from api.ingestion.crawler import crawl_site

        logger.info("Starting site crawl for %s (max %d pages)", domain, max_pages)

        crawl_result = crawl_site(domain, max_pages=max_pages)

        stats = crawl_result.get("stats", {})
        logger.info(
            "Site crawl complete: %d pages crawled, %d links found, %.1fs",
            stats.get("pages_crawled", 0),
            stats.get("total_internal_links", 0),
            stats.get("crawl_time_seconds", 0),
        )

        return {
            "crawl_data": crawl_result,
            "internal_link_graph": crawl_result,
            "sitemap_urls": crawl_result.get("sitemap_urls", []),
        }

    except ImportError as exc:
        logger.warning("Crawler module not available: %s", exc)
        return {}
    except Exception as exc:
        logger.error("Site crawl failed for %s: %s", domain, exc)
        return {}


def run_report_pipeline(report_id: str, user_id: str, gsc_property: str, ga4_property: Optional[str], domain: str) -> None:
    """
    Main entry point — run as a FastAPI BackgroundTask.

    Orchestrates: ingestion → pipeline → storage → status update.

    Progress updates are pushed to Supabase **in real time** as each
    analysis module completes (via a progress_callback passed to the
    pipeline).  This means the frontend's polling loop sees per-module
    status changes during the 2-5 minute analysis window — instead of
    the old behaviour where everything stayed at "running module 1"
    until the entire pipeline finished and results were bulk-stored.

    Catches all exceptions so it never crashes the API process.
    """
    logger.info("=== Starting report pipeline for report_id=%s, domain=%s ===", report_id, domain)

    try:
        supabase = _get_supabase()
    except Exception as exc:
        logger.critical("Cannot connect to Supabase: %s", exc)
        return

    # Mutable state shared between the main function and the callback.
    # Using a dict so the nested function can mutate it.
    progress_state: Dict[str, Any] = {"progress": {}}

    def _on_module_complete(module_name: str, module_result) -> None:
        """
        Callback invoked by AnalysisPipeline after each module finishes.

        Stores the module result in report_modules and pushes a progress
        update to the reports table — all in real time so the frontend
        can display per-module status as each one completes.
        """
        module_num = MODULE_NUMBERS.get(module_name, 0)

        # Prepare result data for storage
        if module_result.status == "skipped":
            skip_reason = (
                module_result.error.user_message
                if module_result.error and module_result.error.user_message
                else "This analysis was skipped because a required previous analysis did not complete successfully."
            )
            results_to_store = {
                "skipped": True,
                "reason": skip_reason,
                "dependency_error": module_result.error.error_message if module_result.error else None,
            }
        else:
            results_to_store = module_result.data

        # Store in report_modules table
        _store_module_result(
            supabase,
            report_id,
            module_name,
            module_num,
            results_to_store,
            module_result.status,
        )

        # Update progress dict and push to reports table
        progress_state["progress"][f"module_{module_num}"] = module_result.status
        _update_report_status(
            supabase, report_id, "analyzing",
            current_module=module_num,
            progress=progress_state["progress"],
        )

        logger.info(
            "Progress update: module %d (%s) → %s",
            module_num, module_name, module_result.status,
        )

    try:
        # ----- Phase 1: Ingestion -----
        _update_report_status(supabase, report_id, "ingesting", current_module=0)

        user_creds = _fetch_user_credentials(supabase, user_id)
        gsc_token = _decrypt_token(user_creds.get("gsc_token"))
        ga4_token = _decrypt_token(user_creds.get("ga4_token"))

        # Build data context from ingestion
        brand_terms = [domain.replace(".", ""), domain.split(".")[0]]
        data_context: Dict[str, Any] = {"brand_terms": brand_terms, "domain": domain}

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

        # DataForSEO SERP ingestion (Phase 2)
        # Extract top non-branded keywords from GSC data, then pull live SERPs
        top_keywords = _extract_top_nonbranded_keywords(
            data_context.get("gsc_keyword_data"),
            brand_terms,
            max_keywords=50,
        )
        if top_keywords:
            serp_result = _ingest_serp_data(supabase, top_keywords, domain)
            if serp_result:
                data_context["serp_data"] = serp_result
                logger.info(
                    "Added SERP data for %d keywords to pipeline context",
                    serp_result.get("successful_fetches", 0),
                )
        else:
            logger.info("No non-branded keywords found — skipping SERP ingestion")

        # Site crawl ingestion (Phase 3)
        # Crawl the site for page metadata + internal link graph
        crawl_result = _ingest_crawl_data(domain, max_pages=200)
        if crawl_result:
            data_context.update(crawl_result)
            logger.info(
                "Added crawl data to pipeline context (%d pages)",
                crawl_result.get("crawl_data", {}).get("stats", {}).get("pages_crawled", 0),
            )
        else:
            logger.info("No crawl data available — modules 4, 9 will run without crawl input")

        # ----- Phase 2: Pipeline execution with real-time progress -----
        _update_report_status(supabase, report_id, "analyzing", current_module=1)

        from api.worker.pipeline import AnalysisPipeline
        pipeline = AnalysisPipeline()

        # Run all 12 modules — _on_module_complete fires after EACH one,
        # storing results and pushing progress to Supabase immediately.
        pipeline_result = pipeline.execute(
            data_context,
            progress_callback=_on_module_complete,
        )
        report_data = pipeline.get_report_data(pipeline_result)

        # ----- Phase 3: Finalize -----
        # All module results are already stored by the callback above.
        # Just write the assembled report_data and set final status.
        final_status = "completed" if pipeline_result.status == "complete" else pipeline_result.status
        _update_report_status(
            supabase,
            report_id,
            final_status,
            progress=progress_state["progress"],
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


# ---------------------------------------------------------------------------
# Stale report recovery
# ---------------------------------------------------------------------------

def recover_stale_reports(stale_threshold_minutes: int = 30) -> Dict[str, Any]:
    """
    Find reports stuck in running states and mark them as failed.

    When the API restarts (Railway deploy, crash, OOM kill), any report
    that was mid-pipeline gets permanently stuck in "pending", "ingesting",
    or "analyzing" status.  The user sees a perpetual loading screen with
    no way to recover except manually hitting the /retry endpoint — which
    they can't do because the UI shows the report as still running.

    This function:
    1. Finds all reports in a running state that haven't been updated
       in the last ``stale_threshold_minutes`` minutes.
    2. Marks them as "failed" with a descriptive error message.
    3. Returns a summary of what was recovered.

    Called automatically during API startup (lifespan) so that every
    deploy or restart cleans up orphaned reports.

    Args:
        stale_threshold_minutes: How long a report must be in a running
            state before it's considered stale.  Default 30 minutes.
            Reports that are genuinely still running (unlikely after a
            restart) will get at most a 30-minute grace period.

    Returns:
        Dict with "recovered" count, "report_ids" list, and "errors" list.
    """
    result: Dict[str, Any] = {"recovered": 0, "report_ids": [], "errors": []}

    try:
        supabase = _get_supabase()
    except Exception as exc:
        logger.error("Cannot connect to Supabase for stale report recovery: %s", exc)
        result["errors"].append(f"Supabase connection failed: {exc}")
        return result

    running_statuses = ("pending", "ingesting", "analyzing", "running")
    cutoff = (datetime.utcnow() - timedelta(minutes=stale_threshold_minutes)).isoformat()

    try:
        # Find reports that are in a running state and haven't been
        # updated recently.  We check updated_at (if set) or fall back
        # to created_at for reports that never got a status update.
        stale_reports = (
            supabase.table("reports")
            .select("id, status, updated_at, created_at, domain")
            .in_("status", list(running_statuses))
            .lt("updated_at", cutoff)
            .execute()
        )

        if not stale_reports.data:
            logger.info(
                "Stale report recovery: no reports stuck in running state "
                "for >%d minutes — nothing to recover.",
                stale_threshold_minutes,
            )
            return result

        for report in stale_reports.data:
            report_id = report["id"]
            old_status = report.get("status", "unknown")
            domain = report.get("domain", "unknown")

            try:
                supabase.table("reports").update({
                    "status": "failed",
                    "error_message": (
                        f"Report was interrupted during '{old_status}' phase "
                        f"(likely due to a server restart or timeout). "
                        f"Click 'Retry' to regenerate this report."
                    ),
                    "updated_at": datetime.utcnow().isoformat(),
                }).eq("id", report_id).execute()

                result["recovered"] += 1
                result["report_ids"].append(report_id)
                logger.info(
                    "Recovered stale report %s (domain=%s, was=%s, stuck since %s)",
                    report_id, domain, old_status,
                    report.get("updated_at") or report.get("created_at"),
                )
            except Exception as exc:
                logger.error("Failed to recover report %s: %s", report_id, exc)
                result["errors"].append(f"Report {report_id}: {exc}")

    except Exception as exc:
        logger.error("Stale report recovery query failed: %s", exc)
        result["errors"].append(f"Query failed: {exc}")

    if result["recovered"] > 0:
        logger.info(
            "Stale report recovery complete: %d reports recovered (%s)",
            result["recovered"],
            ", ".join(result["report_ids"][:5]),
        )

    return result
