"""
GA4 Data Ingestion Module

Fetches all 8 report types from GA4 Data API:
1. Traffic overview (sessions, users, pageviews, bounce, engagement)
2. Landing pages with engagement metrics
3. Traffic by channel group
4. Traffic by source/medium
5. Conversions (event-based)
6. Page path × date for per-page daily time series
7. Page path × session source for source attribution per page
8. Device breakdown

All with date ranges matching GSC pull (16 months).
Implements caching via Supabase to avoid re-fetching within 24h.
"""

import hashlib
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import logging

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
    FilterExpression,
    Filter,
    OrderBy,
)
from google.oauth2.credentials import Credentials
import pandas as pd

from db.supabase_client import get_supabase_client

logger = logging.getLogger(__name__)


class GA4IngestionError(Exception):
    """Raised when GA4 data ingestion fails"""
    pass


def _generate_cache_key(property_id: str, report_type: str, start_date: str, end_date: str) -> str:
    """Generate a unique cache key for a GA4 API request"""
    key_string = f"ga4:{property_id}:{report_type}:{start_date}:{end_date}"
    return hashlib.sha256(key_string.encode()).hexdigest()


def _get_cached_response(user_id: str, cache_key: str) -> Optional[Dict]:
    """Retrieve cached GA4 response if still valid"""
    try:
        supabase = get_supabase_client()
        result = supabase.table("api_cache").select("*").eq(
            "user_id", user_id
        ).eq(
            "cache_key", cache_key
        ).execute()
        
        if result.data and len(result.data) > 0:
            cache_entry = result.data[0]
            expires_at = datetime.fromisoformat(cache_entry["expires_at"].replace("Z", "+00:00"))
            
            if datetime.utcnow().replace(tzinfo=expires_at.tzinfo) < expires_at:
                logger.info(f"Cache hit for key: {cache_key}")
                return cache_entry["response"]
            else:
                # Cache expired, delete it
                supabase.table("api_cache").delete().eq("id", cache_entry["id"]).execute()
                logger.info(f"Cache expired for key: {cache_key}")
        
        return None
    except Exception as e:
        logger.warning(f"Error retrieving cache: {e}")
        return None


def _cache_response(user_id: str, cache_key: str, response: Dict, ttl_hours: int = 24):
    """Cache a GA4 API response"""
    try:
        supabase = get_supabase_client()
        expires_at = datetime.utcnow() + timedelta(hours=ttl_hours)
        
        # Upsert to handle duplicates
        supabase.table("api_cache").upsert({
            "user_id": user_id,
            "cache_key": cache_key,
            "response": response,
            "expires_at": expires_at.isoformat()
        }, on_conflict="user_id,cache_key").execute()
        
        logger.info(f"Cached response for key: {cache_key}")
    except Exception as e:
        logger.warning(f"Error caching response: {e}")


def _get_date_range(months_back: int = 16) -> tuple[str, str]:
    """
    Get date range for GA4 query matching GSC pull.
    Returns (start_date, end_date) in YYYY-MM-DD format.
    
    Default: 16 months of data ending yesterday.
    """
    end_date = datetime.now() - timedelta(days=1)
    start_date = end_date - timedelta(days=months_back * 30)
    
    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")


def _run_ga4_report(
    credentials: Credentials,
    property_id: str,
    dimensions: List[str],
    metrics: List[str],
    start_date: str,
    end_date: str,
    limit: int = 100000,
    order_by: Optional[List[Dict[str, str]]] = None,
    dimension_filter: Optional[FilterExpression] = None
) -> Dict[str, Any]:
    """
    Run a GA4 report request and return structured results.
    
    Args:
        credentials: Google OAuth2 credentials
        property_id: GA4 property ID (format: properties/XXXXXX)
        dimensions: List of dimension names (e.g., ['date', 'pagePath'])
        metrics: List of metric names (e.g., ['sessions', 'totalUsers'])
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        limit: Maximum rows to return
        order_by: Optional list of ordering specs
        dimension_filter: Optional dimension filter expression
    
    Returns:
        Dict with 'rows' (list of dicts) and 'metadata' (row count, etc.)
    """
    try:
        client = BetaAnalyticsDataClient(credentials=credentials)
        
        # Build request
        request = RunReportRequest(
            property=property_id,
            dimensions=[Dimension(name=d) for d in dimensions],
            metrics=[Metric(name=m) for m in metrics],
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            limit=limit
        )
        
        # Add optional filters
        if dimension_filter:
            request.dimension_filter = dimension_filter
        
        # Add optional ordering
        if order_by:
            request.order_bys = [
                OrderBy(
                    dimension=OrderBy.DimensionOrderBy(dimension_name=ob.get("dimension"))
                ) if "dimension" in ob else OrderBy(
                    metric=OrderBy.MetricOrderBy(metric_name=ob.get("metric")),
                    desc=ob.get("desc", False)
                )
                for ob in order_by
            ]
        
        # Execute request
        response = client.run_report(request)
        
        # Parse response
        rows = []
        for row in response.rows:
            row_dict = {}
            
            # Add dimensions
            for i, dimension_value in enumerate(row.dimension_values):
                dimension_name = dimensions[i]
                row_dict[dimension_name] = dimension_value.value
            
            # Add metrics
            for i, metric_value in enumerate(row.metric_values):
                metric_name = metrics[i]
                row_dict[metric_name] = metric_value.value
            
            rows.append(row_dict)
        
        return {
            "rows": rows,
            "row_count": response.row_count,
            "metadata": {
                "property_id": property_id,
                "date_range": f"{start_date} to {end_date}",
                "dimensions": dimensions,
                "metrics": metrics
            }
        }
    
    except Exception as e:
        logger.error(f"GA4 report execution failed: {e}")
        raise GA4IngestionError(f"Failed to run GA4 report: {e}")


def fetch_traffic_overview(
    credentials: Credentials,
    property_id: str,
    user_id: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    use_cache: bool = True
) -> Dict[str, Any]:
    """
    Report 1: Traffic overview by date
    
    Metrics: sessions, totalUsers, screenPageViews, bounceRate, 
             averageSessionDuration, engagementRate
    Dimension: date
    """
    if not start_date or not end_date:
        start_date, end_date = _get_date_range()
    
    cache_key = _generate_cache_key(property_id, "traffic_overview", start_date, end_date)
    
    if use_cache:
        cached = _get_cached_response(user_id, cache_key)
        if cached:
            return cached
    
    result = _run_ga4_report(
        credentials=credentials,
        property_id=property_id,
        dimensions=["date"],
        metrics=[
            "sessions",
            "totalUsers",
            "screenPageViews",
            "bounceRate",
            "averageSessionDuration",
            "engagementRate"
        ],
        start_date=start_date,
        end_date=end_date,
        order_by=[{"dimension": "date"}]
    )
    
    if use_cache:
        _cache_response(user_id, cache_key, result)
    
    return result


def fetch_landing_pages(
    credentials: Credentials,
    property_id: str,
    user_id: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    use_cache: bool = True
) -> Dict[str, Any]:
    """
    Report 2: Landing pages with engagement metrics
    
    Metrics: sessions, totalUsers, engagementRate, bounceRate, 
             averageSessionDuration, conversions
    Dimension: landingPage
    """
    if not start_date or not end_date:
        start_date, end_date = _get_date_range()
    
    cache_key = _generate_cache_key(property_id, "landing_pages", start_date, end_date)
    
    if use_cache:
        cached = _get_cached_response(user_id, cache_key)
        if cached:
            return cached
    
    result = _run_ga4_report(
        credentials=credentials,
        property_id=property_id,
        dimensions=["landingPage"],
        metrics=[
            "sessions",
            "totalUsers",
            "engagementRate",
            "bounceRate",
            "averageSessionDuration",
            "conversions"
        ],
        start_date=start_date,
        end_date=end_date,
        order_by=[{"metric": "sessions", "desc": True}]
    )
    
    if use_cache:
        _cache_response(user_id, cache_key, result)
    
    return result


def fetch_traffic_by_channel(
    credentials: Credentials,
    property_id: str,
    user_id: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    use_cache: bool = True
) -> Dict[str, Any]:
    """
    Report 3: Traffic by channel group
    
    Metrics: sessions, totalUsers, engagementRate, conversions
    Dimension: sessionDefaultChannelGroup
    """
    if not start_date or not end_date:
        start_date, end_date = _get_date_range()
    
    cache_key = _generate_cache_key(property_id, "traffic_by_channel", start_date, end_date)
    
    if use_cache:
        cached = _get_cached_response(user_id, cache_key)
        if cached:
            return cached
    
    result = _run_ga4_report(
        credentials=credentials,
        property_id=property_id,
        dimensions=["sessionDefaultChannelGroup"],
        metrics=[
            "sessions",
            "totalUsers",
            "engagementRate",
            "conversions"
        ],
        start_date=start_date,
        end_date=end_date,
        order_by=[{"metric": "sessions", "desc": True}]
    )
    
    if use_cache:
        _cache_response(user_id, cache_key, result)
    
    return result


def fetch_traffic_by_source_medium(
    credentials: Credentials,
    property_id: str,
    user_id: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    use_cache: bool = True
) -> Dict[str, Any]:
    """
    Report 4: Traffic by source/medium
    
    Metrics: sessions, totalUsers, engagementRate, conversions
    Dimensions: sessionSource, sessionMedium
    """
    if not start_date or not end_date:
        start_date, end_date = _get_date_range()
    
    cache_key = _generate_cache_key(property_id, "traffic_by_source_medium", start_date, end_date)
    
    if use_cache:
        cached = _get_cached_response(user_id, cache_key)
        if cached:
            return cached
    
    result = _run_ga4_report(
        credentials=credentials,
        property_id=property_id,
        dimensions=["sessionSource", "sessionMedium"],
        metrics=[
            "sessions",
            "totalUsers",
            "engagementRate",
            "conversions"
        ],
        start_date=start_date,
        end_date=end_date,
        order_by=[{"metric": "sessions", "desc": True}]
    )
    
    if use_cache:
        _cache_response(user_id, cache_key, result)
    
    return result


def fetch_conversions(
    credentials: Credentials,
    property_id: str,
    user_id: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    use_cache: bool = True
) -> Dict[str, Any]:
    """
    Report 5: Conversions by event name
    
    Metrics: conversions, totalUsers
    Dimensions: eventName, date
    """
    if not start_date or not end_date:
        start_date, end_date = _get_date_range()
    
    cache_key = _generate_cache_key(property_id, "conversions", start_date, end_date)
    
    if use_cache:
        cached = _get_cached_response(user_id, cache_key)
        if cached:
            return cached
    
    result = _run_ga4_report(
        credentials=credentials,
        property_id=property_id,
        dimensions=["eventName", "date"],
        metrics=[
            "conversions",
            "totalUsers"
        ],
        start_date=start_date,
        end_date=end_date,
        order_by=[{"dimension": "date"}, {"metric": "conversions", "desc": True}]
    )
    
    if use_cache:
        _cache_response(user_id, cache_key, result)
    
    return result


def fetch_page_date_time_series(
    credentials: Credentials,
    property_id: str,
    user_id: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    use_cache: bool = True
) -> Dict[str, Any]:
    """
    Report 6: Page path × date for per-page daily time series
    
    Metrics: sessions, totalUsers, engagementRate, averageSessionDuration
    Dimensions: pagePath, date
    
    Critical for Module 2 (Page-Level Triage) to track per-page trends.
    """
    if not start_date or not end_date:
        start_date, end_date = _get_date_range()
    
    cache_key = _generate_cache_key(property_id, "page_date_series", start_date, end_date)
    
    if use_cache:
        cached = _get_cached_response(user_id, cache_key)
        if cached:
            return cached
    
    result = _run_ga4_report(
        credentials=credentials,
        property_id=property_id,
        dimensions=["pagePath", "date"],
        metrics=[
            "sessions",
            "totalUsers",
            "engagementRate",
            "averageSessionDuration"
        ],
        start_date=start_date,
        end_date=end_date,
        order_by=[{"dimension": "date"}, {"metric": "sessions", "desc": True}]
    )
    
    if use_cache:
        _cache_response(user_id, cache_key, result)
    
    return result


def fetch_page_source_attribution(
    credentials: Credentials,
    property_id: str,
    user_id: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    use_cache: bool = True
) -> Dict[str, Any]:
    """
    Report 7: Page path × session source for source attribution per page
    
    Metrics: sessions, totalUsers, conversions
    Dimensions: pagePath, sessionSource
    
    Allows us to understand which pages attract organic vs other traffic.
    """
    if not start_date or not end_date:
        start_date, end_date = _get_date_range()
    
    cache_key = _generate_cache_key(property_id, "page_source_attribution", start_date, end_date)
    
    if use_cache:
        cached = _get_cached_response(user_id, cache_key)
        if cached:
            return cached
    
    result = _run_ga4_report(
        credentials=credentials,
        property_id=property_id,
        dimensions=["pagePath", "sessionSource"],
        metrics=[
            "sessions",
            "totalUsers",
            "conversions"
        ],
        start_date=start_date,
        end_date=end_date,
        order_by=[{"metric": "sessions", "desc": True}]
    )
    
    if use_cache:
        _cache_response(user_id, cache_key, result)
    
    return result


def fetch_device_breakdown(
    credentials: Credentials,
    property_id: str,
    user_id: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    use_cache: bool = True
) -> Dict[str, Any]:
    """
    Report 8: Device breakdown
    
    Metrics: sessions, totalUsers, engagementRate, conversions
    Dimension: deviceCategory
    """
    if not start_date or not end_date:
        start_date, end_date = _get_date_range()
    
    cache_key = _generate_cache_key(property_id, "device_breakdown", start_date, end_date)
    
    if use_cache:
        cached = _get_cached_response(user_id, cache_key)
        if cached:
            return cached
    
    result = _run_ga4_report(
        credentials=credentials,
        property_id=property_id,
        dimensions=["deviceCategory"],
        metrics=[
            "sessions",
            "totalUsers",
            "engagementRate",
            "conversions"
        ],
        start_date=start_date,
        end_date=end_date,
        order_by=[{"metric": "sessions", "desc": True}]
    )
    
    if use_cache:
        _cache_response(user_id, cache_key, result)
    
    return result


def fetch_all_ga4_reports(
    credentials: Credentials,
    property_id: str,
    user_id: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    use_cache: bool = True
) -> Dict[str, Any]:
    """
    Fetch all 8 GA4 report types in a single call.
    
    Returns a dict with keys matching report names.
    Handles errors gracefully - if one report fails, others still return.
    """
    if not start_date or not end_date:
        start_date, end_date = _get_date_range()
    
    logger.info(f"Fetching all GA4 reports for property {property_id} from {start_date} to {end_date}")
    
    reports = {}
    errors = {}
    
    # Define all report functions
    report_functions = {
        "traffic_overview": fetch_traffic_overview,
        "landing_pages": fetch_landing_pages,
        "traffic_by_channel": fetch_traffic_by_channel,
        "traffic_by_source_medium": fetch_traffic_by_source_medium,
        "conversions": fetch_conversions,
        "page_date_time_series": fetch_page_date_time_series,
        "page_source_attribution": fetch_page_source_attribution,
        "device_breakdown": fetch_device_breakdown,
    }
    
    # Fetch each report
    for report_name, report_func in report_functions.items():
        try:
            logger.info(f"Fetching {report_name}...")
            reports[report_name] = report_func(
                credentials=credentials,
                property_id=property_id,
                user_id=user_id,
                start_date=start_date,
                end_date=end_date,
                use_cache=use_cache
            )
            logger.info(f"✓ {report_name}: {reports[report_name]['row_count']} rows")
        except Exception as e:
            logger.error(f"✗ {report_name} failed: {e}")
            errors[report_name] = str(e)
            reports[report_name] = None
    
    return {
        "reports": reports,
        "errors": errors,
        "date_range": {"start": start_date, "end": end_date},
        "success_count": len([r for r in reports.values() if r is not None]),
        "total_count": len(report_functions)
    }


def get_organic_landing_pages_with_engagement(
    credentials: Credentials,
    property_id: str,
    user_id: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    use_cache: bool = True
) -> pd.DataFrame:
    """
    Helper function to get landing pages specifically from organic search
    with engagement metrics. Critical for cross-referencing with GSC data.
    
    Returns a DataFrame for easy analysis.
    """
    if not start_date or not end_date:
        start_date, end_date = _get_date_range()
    
    cache_key = _generate_cache_key(property_id, "organic_landing_pages", start_date, end_date)
    
    if use_cache:
        cached = _get_cached_response(user_id, cache_key)
        if cached:
            return pd.DataFrame(cached["rows"])
    
    # Filter for organic search traffic only
    organic_filter = FilterExpression(
        filter=Filter(
            field_name="sessionDefaultChannelGroup",
            string_filter=Filter.StringFilter(
                match_type=Filter.StringFilter.MatchType.EXACT,
                value="Organic Search"
            )
        )
    )
    
    result = _run_ga4_report(
        credentials=credentials,
        property_id=property_id,
        dimensions=["landingPage"],
        metrics=[
            "sessions",
            "totalUsers",
            "engag