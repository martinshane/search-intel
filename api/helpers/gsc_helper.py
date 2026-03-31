"""
Google Search Console data fetching helper.

Provides get_gsc_data() which uses the user's stored OAuth tokens
to pull query-level and page-level performance data from GSC.
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import httpx

logger = logging.getLogger(__name__)

GSC_API_BASE = "https://www.googleapis.com/webmasters/v3"


async def get_gsc_data(
    access_token: str,
    site_url: str,
    *,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    dimensions: Optional[List[str]] = None,
    row_limit: int = 25000,
    data_type: str = "web",
    dimension_filter_groups: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    Fetch search analytics data from Google Search Console.

    Args:
        access_token: Valid Google OAuth access token with webmasters.readonly scope.
        site_url: GSC property URL (e.g. "sc-domain:example.com").
        start_date: ISO date string (YYYY-MM-DD). Defaults to 16 months ago.
        end_date: ISO date string (YYYY-MM-DD). Defaults to 3 days ago.
        dimensions: List of dimensions (query, page, date, country, device).
        row_limit: Max rows to return (max 25000 per request).
        data_type: "web", "image", "video", "news", or "discover".
        dimension_filter_groups: Optional filters for the request.

    Returns:
        Dict with "rows" list and "responseAggregationType" from GSC API.

    Raises:
        httpx.HTTPStatusError: On non-2xx responses from GSC API.
    """
    if dimensions is None:
        dimensions = ["query", "page", "date"]

    now = datetime.utcnow()
    if start_date is None:
        start_date = (now - timedelta(days=16 * 30)).strftime("%Y-%m-%d")
    if end_date is None:
        end_date = (now - timedelta(days=3)).strftime("%Y-%m-%d")

    url = f"{GSC_API_BASE}/sites/{site_url}/searchAnalytics/query"
    headers = {"Authorization": f"Bearer {access_token}"}
    body: Dict[str, Any] = {
        "startDate": start_date,
        "endDate": end_date,
        "dimensions": dimensions,
        "rowLimit": row_limit,
        "type": data_type,
    }
    if dimension_filter_groups:
        body["dimensionFilterGroups"] = dimension_filter_groups

    logger.info(
        "Fetching GSC data for %s (%s to %s, dims=%s)",
        site_url, start_date, end_date, dimensions,
    )

    all_rows: List[Dict[str, Any]] = []
    start_row = 0

    async with httpx.AsyncClient(timeout=60.0) as client:
        while True:
            body["startRow"] = start_row
            resp = await client.post(url, headers=headers, json=body)
            resp.raise_for_status()
            data = resp.json()
            rows = data.get("rows", [])
            if not rows:
                break
            all_rows.extend(rows)
            if len(rows) < row_limit:
                break
            start_row += len(rows)

    logger.info("Fetched %d rows from GSC for %s", len(all_rows), site_url)
    return {
        "rows": all_rows,
        "responseAggregationType": data.get("responseAggregationType", "auto"),
        "row_count": len(all_rows),
    }
