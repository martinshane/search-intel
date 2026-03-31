"""
Google Analytics 4 data fetching helper.

Provides get_ga4_data() which uses the user's stored OAuth tokens
to pull metrics and dimensions from the GA4 Data API.
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import httpx

logger = logging.getLogger(__name__)

GA4_API_BASE = "https://analyticsdata.googleapis.com/v1beta"


async def get_ga4_data(
    access_token: str,
    property_id: str,
    *,
    metrics: Optional[List[str]] = None,
    dimensions: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    dimension_filter: Optional[Dict[str, Any]] = None,
    order_bys: Optional[List[Dict[str, Any]]] = None,
    limit: int = 10000,
    offset: int = 0,
) -> Dict[str, Any]:
    """
    Fetch analytics data from Google Analytics 4 Data API.

    Args:
        access_token: Valid Google OAuth access token with analytics.readonly scope.
        property_id: GA4 property ID (numeric, e.g. "123456789").
        metrics: List of metric names (e.g. ["sessions", "totalUsers"]).
        dimensions: List of dimension names (e.g. ["pagePath", "date"]).
        start_date: ISO date string or relative (e.g. "30daysAgo"). Defaults to "16monthsAgo".
        end_date: ISO date string or relative. Defaults to "yesterday".
        dimension_filter: Optional filter expression dict.
        order_bys: Optional ordering specification.
        limit: Max rows per request (max 100000).
        offset: Row offset for pagination.

    Returns:
        Dict with "rows", "rowCount", "metadata" from GA4 API.

    Raises:
        httpx.HTTPStatusError: On non-2xx responses from GA4 API.
    """
    if metrics is None:
        metrics = ["sessions", "totalUsers", "screenPageViews", "engagementRate"]
    if dimensions is None:
        dimensions = ["pagePath", "date"]
    if start_date is None:
        start_date = "480daysAgo"  # ~16 months
    if end_date is None:
        end_date = "yesterday"

    url = f"{GA4_API_BASE}/properties/{property_id}:runReport"
    headers = {"Authorization": f"Bearer {access_token}"}
    body: Dict[str, Any] = {
        "dateRanges": [{"startDate": start_date, "endDate": end_date}],
        "metrics": [{"name": m} for m in metrics],
        "dimensions": [{"name": d} for d in dimensions],
        "limit": limit,
        "offset": offset,
    }
    if dimension_filter:
        body["dimensionFilter"] = dimension_filter
    if order_bys:
        body["orderBys"] = order_bys

    logger.info(
        "Fetching GA4 data for property %s (metrics=%s, dims=%s)",
        property_id, metrics, dimensions,
    )

    all_rows: List[Dict[str, Any]] = []
    current_offset = offset

    async with httpx.AsyncClient(timeout=60.0) as client:
        while True:
            body["offset"] = current_offset
            resp = await client.post(url, headers=headers, json=body)
            resp.raise_for_status()
            data = resp.json()

            rows = data.get("rows", [])
            if not rows:
                break
            all_rows.extend(rows)

            row_count = data.get("rowCount", 0)
            current_offset += len(rows)
            if current_offset >= row_count or len(rows) < limit:
                break

    # Flatten GA4 response rows into simpler dicts
    dimension_headers = [h["name"] for h in data.get("dimensionHeaders", [])]
    metric_headers = [h["name"] for h in data.get("metricHeaders", [])]

    flat_rows = []
    for row in all_rows:
        flat = {}
        for i, dh in enumerate(dimension_headers):
            flat[dh] = row.get("dimensionValues", [{}])[i].get("value", "")
        for i, mh in enumerate(metric_headers):
            flat[mh] = row.get("metricValues", [{}])[i].get("value", "0")
        flat_rows.append(flat)

    logger.info("Fetched %d rows from GA4 for property %s", len(flat_rows), property_id)
    return {
        "rows": flat_rows,
        "row_count": len(flat_rows),
        "dimension_headers": dimension_headers,
        "metric_headers": metric_headers,
    }
