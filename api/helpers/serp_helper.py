"""
SERP data fetching helper (DataForSEO integration).

Provides get_serp_data() for Phase 2 modules that need live SERP results
and competitive intelligence data.
"""

import logging
import base64
from typing import Any, Dict, List, Optional

import httpx

logger = logging.getLogger(__name__)

DATAFORSEO_API_BASE = "https://api.dataforseo.com/v3"


async def get_serp_data(
    login: str,
    password: str,
    *,
    keywords: Optional[List[str]] = None,
    target_domain: Optional[str] = None,
    location_code: int = 2840,  # US
    language_code: str = "en",
    device: str = "desktop",
    serp_type: str = "organic",
) -> Dict[str, Any]:
    """
    Fetch SERP data from DataForSEO API.

    Args:
        login: DataForSEO API login.
        password: DataForSEO API password.
        keywords: List of keywords to check SERP results for.
        target_domain: Domain to focus competitive analysis on.
        location_code: DataForSEO location code (default 2840 = US).
        language_code: Language code (default "en").
        device: "desktop" or "mobile".
        serp_type: "organic", "paid", "featured_snippet", etc.

    Returns:
        Dict with "results" list containing SERP data per keyword.

    Raises:
        httpx.HTTPStatusError: On non-2xx responses from DataForSEO.
        ValueError: If no keywords are provided.
    """
    if not keywords:
        raise ValueError("At least one keyword is required for SERP analysis")

    credentials = base64.b64encode(f"{login}:{password}".encode()).decode()
    headers = {"Authorization": f"Basic {credentials}", "Content-Type": "application/json"}

    # Build task posts for batch processing
    tasks = []
    for kw in keywords:
        task: Dict[str, Any] = {
            "keyword": kw,
            "location_code": location_code,
            "language_code": language_code,
            "device": device,
        }
        if target_domain:
            task["target"] = target_domain
        tasks.append(task)

    url = f"{DATAFORSEO_API_BASE}/serp/google/{serp_type}/live/advanced"

    logger.info(
        "Fetching SERP data for %d keywords (location=%d, device=%s)",
        len(keywords), location_code, device,
    )

    async with httpx.AsyncClient(timeout=120.0) as client:
        resp = await client.post(url, headers=headers, json=tasks)
        resp.raise_for_status()
        data = resp.json()

    results = data.get("tasks", [])

    # Flatten into per-keyword results
    keyword_results = []
    for task_result in results:
        result_data = task_result.get("result", [])
        for r in result_data:
            keyword_results.append({
                "keyword": r.get("keyword", ""),
                "se_type": r.get("type", serp_type),
                "items_count": r.get("items_count", 0),
                "items": r.get("items", []),
                "spell": r.get("spell", None),
                "check_url": r.get("check_url", ""),
            })

    logger.info("Fetched SERP data for %d keywords", len(keyword_results))
    return {
        "results": keyword_results,
        "keyword_count": len(keyword_results),
        "cost": data.get("cost", 0),
    }
