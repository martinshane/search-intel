"""
Module execution router for Search Intelligence Report API.

Each GET endpoint runs one of the 12 analysis modules for a given report.
The flow for every module:
    1. Authenticate the user (JWT via get_current_user).
    2. Look up the report in Supabase and verify ownership.
    3. Retrieve the user's decrypted OAuth credentials.
    4. Fetch the required data via helpers (GSC, GA4, SERP, crawl).
    5. Convert raw API responses to the format each analysis function expects.
    6. Run the analysis function.
    7. Store results in the report_modules table.
"""

import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException

from api.auth.dependencies import get_current_user
from api.config import settings

# Analysis functions
from api.analysis.module_1_health_trajectory import analyze_health_trajectory
from api.analysis.module_2_page_triage import analyze_page_triage
from api.analysis.module_3_serp_landscape import analyze_serp_landscape
from api.analysis.module_4_content_intelligence import analyze_content_intelligence
from api.analysis.module_5_gameplan import generate_gameplan
from api.analysis.module_6_algorithm_updates import analyze_algorithm_impacts
from api.analysis.module_7_intent_migration import analyze_intent_migration
from api.analysis.module_8_technical_health import analyze_technical_health
from api.analysis.module_9_site_architecture import analyze_site_architecture
from api.analysis.module_10_branded_split import analyze_branded_split
from api.analysis.module_11_competitive_threats import analyze_competitive_threats
from api.analysis.module_12_revenue_attribution import estimate_revenue_attribution

# Data helpers
from api.helpers.gsc_helper import get_gsc_data
from api.helpers.ga4_helper import get_ga4_data
from api.helpers.serp_helper import get_serp_data
from api.helpers.crawl_helper import get_crawl_data

logger = logging.getLogger(__name__)
router = APIRouter()


# ---------------------------------------------------------------------------
# Supabase helpers (lazy-initialised to avoid import-time env-var crashes)
# ---------------------------------------------------------------------------

def _get_supabase():
    """Return a Supabase client, raising 500 if not configured."""
    from supabase import create_client
    url = os.getenv("SUPABASE_URL", "")
    key = os.getenv("SUPABASE_KEY", "")
    if not url or not key:
        raise HTTPException(status_code=500, detail="Supabase is not configured")
    return create_client(url, key)


async def _get_report(report_id: str, user: dict) -> dict:
    """Fetch a report row and verify it belongs to *user*."""
    sb = _get_supabase()
    result = sb.table("reports").select("*").eq("id", report_id).execute()
    if not result.data:
        raise HTTPException(status_code=404, detail="Report not found")
    report = result.data[0]
    if report.get("user_id") != user.get("sub", user.get("id", user.get("user_id"))):
        raise HTTPException(status_code=403, detail="Not authorised for this report")
    return report


async def _get_access_token(user: dict) -> str:
    """Retrieve the user's decrypted Google OAuth access token."""
    from api.auth.oauth import get_user_credentials
    user_id = user.get("sub", user.get("id", user.get("user_id")))
    credentials = await get_user_credentials(user_id)
    return credentials.token


def _update_report_status(report_id: str, status: str, module_num: int | None = None):
    """Update the report status (and optionally current_module) in Supabase."""
    sb = _get_supabase()
    payload: Dict[str, Any] = {"status": status, "updated_at": datetime.utcnow().isoformat()}
    if module_num is not None:
        payload["current_module"] = module_num
    sb.table("reports").update(payload).eq("id", report_id).execute()


def _store_module_result(report_id: str, module_number: int, module_name: str, results: dict):
    """Upsert module results into report_modules."""
    sb = _get_supabase()
    row = {
        "report_id": report_id,
        "module_number": module_number,
        "module_name": module_name,
        "results": results,
        "status": "completed",
        "completed_at": datetime.utcnow().isoformat(),
    }
    # Try update first, insert if not found
    existing = (
        sb.table("report_modules")
        .select("id")
        .eq("report_id", report_id)
        .eq("module_number", module_number)
        .execute()
    )
    if existing.data:
        sb.table("report_modules").update(row).eq("id", existing.data[0]["id"]).execute()
    else:
        sb.table("report_modules").insert(row).execute()


# ---------------------------------------------------------------------------
# GSC API response → DataFrame converters
# ---------------------------------------------------------------------------

def _gsc_to_date_df(gsc_response: Dict[str, Any]) -> pd.DataFrame:
    """Convert GSC API response (dimensions=[date]) to a DataFrame.

    Expected GSC row format:
        {"keys": ["2025-01-15"], "clicks": 120, "impressions": 5000, "ctr": 0.024, "position": 12.3}

    Returns DataFrame with columns: date, clicks, impressions, ctr, position
    """
    rows = gsc_response.get("rows", [])
    records = []
    for row in rows:
        records.append({
            "date": row["keys"][0],
            "clicks": row.get("clicks", 0),
            "impressions": row.get("impressions", 0),
            "ctr": row.get("ctr", 0.0),
            "position": row.get("position", 0.0),
        })
    df = pd.DataFrame(records)
    if not df.empty and "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date").reset_index(drop=True)
    return df


def _gsc_to_page_date_df(gsc_response: Dict[str, Any]) -> pd.DataFrame:
    """Convert GSC API response (dimensions=[page, date]) to a DataFrame.

    Returns DataFrame with columns: page, date, clicks, impressions, ctr, position
    """
    rows = gsc_response.get("rows", [])
    records = []
    for row in rows:
        records.append({
            "page": row["keys"][0],
            "date": row["keys"][1],
            "clicks": row.get("clicks", 0),
            "impressions": row.get("impressions", 0),
            "ctr": row.get("ctr", 0.0),
            "position": row.get("position", 0.0),
        })
    df = pd.DataFrame(records)
    if not df.empty and "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    return df


def _gsc_to_query_page_df(gsc_response: Dict[str, Any]) -> pd.DataFrame:
    """Convert GSC API response (dimensions=[query, page]) to a DataFrame.

    Returns DataFrame with columns: query, page, clicks, impressions, ctr, position
    """
    rows = gsc_response.get("rows", [])
    records = []
    for row in rows:
        records.append({
            "query": row["keys"][0],
            "page": row["keys"][1],
            "clicks": row.get("clicks", 0),
            "impressions": row.get("impressions", 0),
            "ctr": row.get("ctr", 0.0),
            "position": row.get("position", 0.0),
        })
    return pd.DataFrame(records)


def _gsc_to_query_date_df(gsc_response: Dict[str, Any]) -> pd.DataFrame:
    """Convert GSC API response (dimensions=[query, date]) to a DataFrame.

    Returns DataFrame with columns: query, date, clicks, impressions, ctr, position
    """
    rows = gsc_response.get("rows", [])
    records = []
    for row in rows:
        records.append({
            "query": row["keys"][0],
            "date": row["keys"][1],
            "clicks": row.get("clicks", 0),
            "impressions": row.get("impressions", 0),
            "ctr": row.get("ctr", 0.0),
            "position": row.get("position", 0.0),
        })
    df = pd.DataFrame(records)
    if not df.empty and "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    return df


def _gsc_to_query_df(gsc_response: Dict[str, Any]) -> pd.DataFrame:
    """Convert GSC API response (dimensions=[query]) to a DataFrame.

    Returns DataFrame with columns: query, clicks, impressions, ctr, position
    """
    rows = gsc_response.get("rows", [])
    records = []
    for row in rows:
        records.append({
            "query": row["keys"][0],
            "clicks": row.get("clicks", 0),
            "impressions": row.get("impressions", 0),
            "ctr": row.get("ctr", 0.0),
            "position": row.get("position", 0.0),
        })
    return pd.DataFrame(records)


def _derive_brand_terms(domain: str) -> List[str]:
    """Derive likely brand terms from a domain name.

    E.g. "kixie.com" -> ["kixie"]
         "tradeify.co" -> ["tradeify"]
         "my-brand-name.com" -> ["my brand name", "my-brand-name", "mybrandname"]
    """
    # Strip TLD and www
    name = domain.lower().replace("www.", "")
    name = name.split(".")[0]  # strip TLD
    terms = [name]
    if "-" in name:
        terms.append(name.replace("-", " "))
        terms.append(name.replace("-", ""))
    if "_" in name:
        terms.append(name.replace("_", " "))
        terms.append(name.replace("_", ""))
    return terms


# ---------------------------------------------------------------------------
# Module endpoints
# ---------------------------------------------------------------------------

@router.get("/1")
async def run_module_1(
    report_id: str,
    current_user: dict = Depends(get_current_user),
):
    """Module 1: Health & Trajectory — MSTL decomposition, change-point and anomaly detection."""
    report = await _get_report(report_id, current_user)
    _update_report_status(report_id, "running", 1)
    try:
        access_token = await _get_access_token(current_user)
        gsc_data = await get_gsc_data(
            access_token=access_token,
            site_url=report["gsc_property"],
            dimensions=["date"],
        )
        if not gsc_data.get("rows"):
            raise HTTPException(status_code=400, detail="Insufficient GSC data for analysis")
        df = _gsc_to_date_df(gsc_data)
        results = analyze_health_trajectory(df)
        _store_module_result(report_id, 1, "health_trajectory", results)
        return {"module": 1, "status": "completed", "results": results}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Module 1 failed for report %s", report_id)
        _update_report_status(report_id, "failed")
        raise HTTPException(status_code=500, detail=f"Module 1 analysis failed: {e}")


@router.get("/2")
async def run_module_2(
    report_id: str,
    current_user: dict = Depends(get_current_user),
):
    """Module 2: Page Triage — identify pages that are rising, declining, or stagnant."""
    report = await _get_report(report_id, current_user)
    _update_report_status(report_id, "running", 2)
    try:
        access_token = await _get_access_token(current_user)
        gsc_data = await get_gsc_data(
            access_token=access_token,
            site_url=report["gsc_property"],
            dimensions=["page", "date"],
        )
        if not gsc_data.get("rows"):
            raise HTTPException(status_code=400, detail="Insufficient GSC data for page triage")
        df = _gsc_to_page_date_df(gsc_data)
        results = analyze_page_triage(df)
        _store_module_result(report_id, 2, "page_triage", results)
        return {"module": 2, "status": "completed", "results": results}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Module 2 failed for report %s", report_id)
        _update_report_status(report_id, "failed")
        raise HTTPException(status_code=500, detail=f"Module 2 analysis failed: {e}")


@router.get("/3")
async def run_module_3(
    report_id: str,
    current_user: dict = Depends(get_current_user),
):
    """Module 3: SERP Landscape — competitive SERP analysis via DataForSEO."""
    report = await _get_report(report_id, current_user)
    _update_report_status(report_id, "running", 3)
    try:
        login = settings.dataforseo_login
        password = settings.dataforseo_password
        if not login or not password:
            raise HTTPException(status_code=500, detail="DataForSEO credentials not configured")

        # Get top keywords from GSC to feed into SERP analysis
        access_token = await _get_access_token(current_user)
        gsc_data = await get_gsc_data(
            access_token=access_token,
            site_url=report["gsc_property"],
            dimensions=["query"],
            row_limit=100,
        )
        keywords = [row["keys"][0] for row in gsc_data.get("rows", [])[:100]]
        if not keywords:
            raise HTTPException(status_code=400, detail="No keywords found in GSC data")

        serp_data = await get_serp_data(
            login=login,
            password=password,
            keywords=keywords,
            target_domain=report["domain"],
        )
        # Also pass GSC keyword data for richer analysis
        gsc_keyword_df = _gsc_to_query_df(gsc_data)
        results = analyze_serp_landscape(serp_data, gsc_keyword_data=gsc_keyword_df)
        _store_module_result(report_id, 3, "serp_landscape", results)
        return {"module": 3, "status": "completed", "results": results}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Module 3 failed for report %s", report_id)
        _update_report_status(report_id, "failed")
        raise HTTPException(status_code=500, detail=f"Module 3 analysis failed: {e}")


@router.get("/4")
async def run_module_4(
    report_id: str,
    current_user: dict = Depends(get_current_user),
):
    """Module 4: Content Intelligence — content gap and quality analysis."""
    report = await _get_report(report_id, current_user)
    _update_report_status(report_id, "running", 4)
    try:
        access_token = await _get_access_token(current_user)
        gsc_data = await get_gsc_data(
            access_token=access_token,
            site_url=report["gsc_property"],
            dimensions=["query", "page"],
        )
        gsc_query_page_df = _gsc_to_query_page_df(gsc_data)

        crawl_data = await get_crawl_data(domain=report["domain"], max_pages=200)
        # crawl_data is a dict with page-level info; convert to DataFrame for page_data
        page_records = crawl_data.get("pages", []) if isinstance(crawl_data, dict) else []
        page_data_df = pd.DataFrame(page_records) if page_records else pd.DataFrame()

        # GA4 engagement data (optional — pass empty DataFrame if not available)
        ga4_engagement_df = pd.DataFrame()
        if report.get("ga4_property"):
            try:
                ga4_raw = await get_ga4_data(
                    access_token=access_token,
                    property_id=report["ga4_property"],
                    metrics=["sessions", "engagementRate", "averageSessionDuration"],
                    dimensions=["pagePath"],
                )
                ga4_engagement_df = pd.DataFrame(ga4_raw.get("rows", []))
            except Exception as ga4_err:
                logger.warning("GA4 engagement data unavailable for module 4: %s", ga4_err)

        results = analyze_content_intelligence(gsc_query_page_df, page_data_df, ga4_engagement_df)
        _store_module_result(report_id, 4, "content_intelligence", results)
        return {"module": 4, "status": "completed", "results": results}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Module 4 failed for report %s", report_id)
        _update_report_status(report_id, "failed")
        raise HTTPException(status_code=500, detail=f"Module 4 analysis failed: {e}")


@router.get("/5")
async def run_module_5(
    report_id: str,
    current_user: dict = Depends(get_current_user),
):
    """Module 5: Game Plan — prioritised action items from all prior modules."""
    report = await _get_report(report_id, current_user)
    _update_report_status(report_id, "running", 5)
    try:
        # Gather results from all previously completed modules
        sb = _get_supabase()
        prior = (
            sb.table("report_modules")
            .select("module_number, results")
            .eq("report_id", report_id)
            .in_("module_number", list(range(1, 13)))
            .execute()
        )
        prior_map = {r["module_number"]: r["results"] for r in (prior.data or [])}

        # Unpack into the keyword args that generate_gameplan expects
        results = generate_gameplan(
            health=prior_map.get(1, {}),
            triage=prior_map.get(2, {}),
            serp=prior_map.get(3, {}),
            content=prior_map.get(4, {}),
            algorithm=prior_map.get(6),
            intent=prior_map.get(7),
            ctr=prior_map.get(8),
            architecture=prior_map.get(9),
            branded=prior_map.get(10),
        )
        _store_module_result(report_id, 5, "gameplan", results)
        return {"module": 5, "status": "completed", "results": results}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Module 5 failed for report %s", report_id)
        _update_report_status(report_id, "failed")
        raise HTTPException(status_code=500, detail=f"Module 5 analysis failed: {e}")


@router.get("/6")
async def run_module_6(
    report_id: str,
    current_user: dict = Depends(get_current_user),
):
    """Module 6: Algorithm Updates — detect Google algorithm update impacts."""
    report = await _get_report(report_id, current_user)
    _update_report_status(report_id, "running", 6)
    try:
        access_token = await _get_access_token(current_user)
        gsc_data = await get_gsc_data(
            access_token=access_token,
            site_url=report["gsc_property"],
            dimensions=["date"],
        )
        daily_df = _gsc_to_date_df(gsc_data)

        # Optionally feed in change points from module 1 if already completed
        sb = _get_supabase()
        m1 = (
            sb.table("report_modules")
            .select("results")
            .eq("report_id", report_id)
            .eq("module_number", 1)
            .execute()
        )
        change_points = None
        if m1.data:
            change_points = m1.data[0]["results"].get("change_points")

        results = analyze_algorithm_impacts(daily_df, change_points_from_module1=change_points)
        _store_module_result(report_id, 6, "algorithm_updates", results)
        return {"module": 6, "status": "completed", "results": results}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Module 6 failed for report %s", report_id)
        _update_report_status(report_id, "failed")
        raise HTTPException(status_code=500, detail=f"Module 6 analysis failed: {e}")


@router.get("/7")
async def run_module_7(
    report_id: str,
    current_user: dict = Depends(get_current_user),
):
    """Module 7: Intent Migration — track search intent shifts over time."""
    report = await _get_report(report_id, current_user)
    _update_report_status(report_id, "running", 7)
    try:
        access_token = await _get_access_token(current_user)
        gsc_data = await get_gsc_data(
            access_token=access_token,
            site_url=report["gsc_property"],
            dimensions=["query", "date"],
        )
        query_ts_df = _gsc_to_query_date_df(gsc_data)
        results = analyze_intent_migration(query_ts_df)
        _store_module_result(report_id, 7, "intent_migration", results)
        return {"module": 7, "status": "completed", "results": results}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Module 7 failed for report %s", report_id)
        _update_report_status(report_id, "failed")
        raise HTTPException(status_code=500, detail=f"Module 7 analysis failed: {e}")


@router.get("/8")
async def run_module_8(
    report_id: str,
    current_user: dict = Depends(get_current_user),
):
    """Module 8: Technical Health — CTR modelling & technical SEO checks."""
    report = await _get_report(report_id, current_user)
    _update_report_status(report_id, "running", 8)
    try:
        access_token = await _get_access_token(current_user)
        gsc_data = await get_gsc_data(
            access_token=access_token,
            site_url=report["gsc_property"],
            dimensions=["query", "page"],
        )
        crawl_data = await get_crawl_data(domain=report["domain"], max_pages=500)

        # Pass data as the keyword args the function expects
        results = analyze_technical_health(
            gsc_coverage=gsc_data,
            crawl_technical=crawl_data,
        )
        _store_module_result(report_id, 8, "technical_health", results)
        return {"module": 8, "status": "completed", "results": results}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Module 8 failed for report %s", report_id)
        _update_report_status(report_id, "failed")
        raise HTTPException(status_code=500, detail=f"Module 8 analysis failed: {e}")


@router.get("/9")
async def run_module_9(
    report_id: str,
    current_user: dict = Depends(get_current_user),
):
    """Module 9: Site Architecture — internal link graph and orphan page detection."""
    report = await _get_report(report_id, current_user)
    _update_report_status(report_id, "running", 9)
    try:
        crawl_data = await get_crawl_data(domain=report["domain"], max_pages=1000)
        results = analyze_site_architecture(link_graph=crawl_data)
        _store_module_result(report_id, 9, "site_architecture", results)
        return {"module": 9, "status": "completed", "results": results}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Module 9 failed for report %s", report_id)
        _update_report_status(report_id, "failed")
        raise HTTPException(status_code=500, detail=f"Module 9 analysis failed: {e}")


@router.get("/10")
async def run_module_10(
    report_id: str,
    current_user: dict = Depends(get_current_user),
):
    """Module 10: Branded Split — branded vs non-branded traffic breakdown."""
    report = await _get_report(report_id, current_user)
    _update_report_status(report_id, "running", 10)
    try:
        access_token = await _get_access_token(current_user)
        gsc_data = await get_gsc_data(
            access_token=access_token,
            site_url=report["gsc_property"],
            dimensions=["query", "date"],
        )
        query_date_df = _gsc_to_query_date_df(gsc_data)
        brand_terms = _derive_brand_terms(report["domain"])
        results = analyze_branded_split(query_date_df, brand_terms=brand_terms)
        _store_module_result(report_id, 10, "branded_split", results)
        return {"module": 10, "status": "completed", "results": results}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Module 10 failed for report %s", report_id)
        _update_report_status(report_id, "failed")
        raise HTTPException(status_code=500, detail=f"Module 10 analysis failed: {e}")


@router.get("/11")
async def run_module_11(
    report_id: str,
    current_user: dict = Depends(get_current_user),
):
    """Module 11: Competitive Threats — competitor overlap and threat analysis."""
    report = await _get_report(report_id, current_user)
    _update_report_status(report_id, "running", 11)
    try:
        login = settings.dataforseo_login
        password = settings.dataforseo_password
        if not login or not password:
            raise HTTPException(status_code=500, detail="DataForSEO credentials not configured")

        access_token = await _get_access_token(current_user)
        gsc_data = await get_gsc_data(
            access_token=access_token,
            site_url=report["gsc_property"],
            dimensions=["query"],
            row_limit=200,
        )
        keywords = [row["keys"][0] for row in gsc_data.get("rows", [])[:200]]
        serp_data = await get_serp_data(
            login=login,
            password=password,
            keywords=keywords,
            target_domain=report["domain"],
        )
        results = analyze_competitive_threats(
            serp_data,
            gsc_data=gsc_data,
            user_domain=report["domain"],
        )
        _store_module_result(report_id, 11, "competitive_threats", results)
        return {"module": 11, "status": "completed", "results": results}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Module 11 failed for report %s", report_id)
        _update_report_status(report_id, "failed")
        raise HTTPException(status_code=500, detail=f"Module 11 analysis failed: {e}")


@router.get("/12")
async def run_module_12(
    report_id: str,
    current_user: dict = Depends(get_current_user),
):
    """Module 12: Revenue Attribution — estimate traffic value and revenue at risk."""
    report = await _get_report(report_id, current_user)
    _update_report_status(report_id, "running", 12)
    try:
        access_token = await _get_access_token(current_user)
        gsc_data = await get_gsc_data(
            access_token=access_token,
            site_url=report["gsc_property"],
            dimensions=["query", "page"],
        )

        # Fetch GA4 data for conversion/engagement/ecommerce if GA4 is connected
        ga4_conversions = None
        ga4_engagement = None
        ga4_ecommerce = None
        if report.get("ga4_property"):
            try:
                ga4_conversions = await get_ga4_data(
                    access_token=access_token,
                    property_id=report["ga4_property"],
                    metrics=["conversions", "totalUsers"],
                    dimensions=["pagePath", "date"],
                )
            except Exception as ga4_err:
                logger.warning("GA4 conversions unavailable for module 12: %s", ga4_err)
            try:
                ga4_engagement = await get_ga4_data(
                    access_token=access_token,
                    property_id=report["ga4_property"],
                    metrics=["sessions", "engagementRate"],
                    dimensions=["pagePath"],
                )
            except Exception as ga4_err:
                logger.warning("GA4 engagement unavailable for module 12: %s", ga4_err)
            try:
                ga4_ecommerce = await get_ga4_data(
                    access_token=access_token,
                    property_id=report["ga4_property"],
                    metrics=["totalRevenue", "ecommercePurchases"],
                    dimensions=["pagePath"],
                )
            except Exception as ga4_err:
                logger.warning("GA4 ecommerce unavailable for module 12: %s", ga4_err)

        results = estimate_revenue_attribution(
            gsc_data,
            ga4_conversions=ga4_conversions,
            ga4_engagement=ga4_engagement,
            ga4_ecommerce=ga4_ecommerce,
        )
        _store_module_result(report_id, 12, "revenue_attribution", results)
        _update_report_status(report_id, "completed")
        return {"module": 12, "status": "completed", "results": results}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Module 12 failed for report %s", report_id)
        _update_report_status(report_id, "failed")
        raise HTTPException(status_code=500, detail=f"Module 12 analysis failed: {e}")


@router.get("/run-all")
async def run_all_modules(
    report_id: str,
    current_user: dict = Depends(get_current_user),
):
    """
    Run all 12 modules sequentially for a report.

    Returns a summary of which modules succeeded and which failed.
    """
    report = await _get_report(report_id, current_user)
    _update_report_status(report_id, "running")

    module_runners = [
        (1, run_module_1),
        (2, run_module_2),
        (3, run_module_3),
        (4, run_module_4),
        (5, run_module_5),
        (6, run_module_6),
        (7, run_module_7),
        (8, run_module_8),
        (9, run_module_9),
        (10, run_module_10),
        (11, run_module_11),
        (12, run_module_12),
    ]

    summary: Dict[int, str] = {}
    for num, runner in module_runners:
        try:
            await runner(report_id=report_id, current_user=current_user)
            summary[num] = "completed"
        except Exception as e:
            logger.warning("Module %d failed during run-all: %s", num, e)
            summary[num] = f"failed: {e}"

    all_ok = all(v == "completed" for v in summary.values())
    _update_report_status(report_id, "completed" if all_ok else "partial")

    return {
        "report_id": report_id,
        "status": "completed" if all_ok else "partial",
        "modules": summary,
    }
