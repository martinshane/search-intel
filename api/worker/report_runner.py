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
7. Stores each module result in the report_modules table
8. Writes the assembled report_data JSON to the reports table
9. Updates report status to "completed" (or "partial" / "failed")

This is the critical glue between the REST API and the analysis engine.
"""

import asyncio
import logging
import os
import traceback
from datetime import datetime
from typing import Any, Dict, List, Optional

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

        # Query-level summary (alias)
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
        gsc_token = _decrypt_token(user_creds.get("gsc_token"))
        ga4_token = _decrypt_token(user_creds.get("ga4_token"))

        # Build data context from ingestion
        brand_terms = [domain.replace(".", ""), domain.split(".")[0]]
        data_context: Dict[str, Any] = {"brand_terms": brand_terms}

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

            # For skipped modules, store a structured result explaining why
            # so the frontend can display a meaningful message.
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

            _store_module_result(
                supabase,
                report_id,
                module_name,
                module_num,
                results_to_store,
                module_result.status,
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
