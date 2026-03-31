"""
Module 2: Page-Level Triage

Analyzes individual page performance using GSC page-level daily data:
- Per-page trend fitting (linear regression on clicks/impressions)
- CTR anomaly detection (page CTR vs expected CTR for its position)
- Engagement cross-reference with GA4 landing page data
- Priority scoring to surface pages needing attention

Input:
  page_daily_data: DataFrame with columns [date, page, clicks, impressions, ctr, position]
  ga4_landing_data: DataFrame with columns [page, sessions, bounce_rate, avg_session_duration, conversions] (optional)
  gsc_page_summary: DataFrame with columns [page, clicks, impressions, ctr, position] (aggregated) (optional)

Output: Dict with pages list (scored and categorized), priority_actions, summary
"""

import logging
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# CTR curve — expected CTR by average position (based on industry benchmarks)
# ---------------------------------------------------------------------------
_CTR_CURVE = {
    1: 0.30, 2: 0.15, 3: 0.10, 4: 0.07, 5: 0.05,
    6: 0.04, 7: 0.03, 8: 0.025, 9: 0.02, 10: 0.015,
}


def _expected_ctr(position: float) -> float:
    """Return expected CTR for a given average position using benchmark curve."""
    if position <= 0:
        return 0.30
    pos_int = max(1, min(int(round(position)), 10))
    return _CTR_CURVE.get(pos_int, 0.01)


# ---------------------------------------------------------------------------
# Helper: linear trend for a single page
# ---------------------------------------------------------------------------
def _fit_page_trend(page_df: pd.DataFrame, metric: str = "clicks") -> Dict[str, Any]:
    """
    Fit a linear trend for a single page's daily time series.

    Returns dict with slope, direction, r_squared, pct_change_30d.
    """
    if len(page_df) < 7:
        return {"direction": "insufficient_data", "slope": 0.0, "r_squared": 0.0, "pct_change_30d": 0.0}

    y = page_df[metric].values.astype(float)
    x = np.arange(len(y), dtype=float)

    # Guard against constant series
    if np.std(y) == 0:
        return {"direction": "flat", "slope": 0.0, "r_squared": 1.0, "pct_change_30d": 0.0}

    coeffs = np.polyfit(x, y, 1)
    slope = float(coeffs[0])

    # R-squared
    y_pred = np.polyval(coeffs, x)
    ss_res = np.sum((y - y_pred) ** 2)
    ss_tot = np.sum((y - np.mean(y)) ** 2)
    r_squared = float(1 - ss_res / ss_tot) if ss_tot > 0 else 0.0

    # Direction
    if abs(slope) < 0.05:
        direction = "flat"
    elif slope > 0:
        direction = "rising"
    else:
        direction = "declining"

    # 30-day percentage change estimate
    mean_val = np.mean(y)
    pct_change_30d = float((slope * 30) / mean_val * 100) if mean_val > 0 else 0.0

    return {
        "direction": direction,
        "slope": round(slope, 4),
        "r_squared": round(r_squared, 4),
        "pct_change_30d": round(pct_change_30d, 2),
    }


# ---------------------------------------------------------------------------
# Helper: CTR anomaly detection for a single page
# ---------------------------------------------------------------------------
def _detect_ctr_anomaly(actual_ctr: float, avg_position: float) -> Dict[str, Any]:
    """
    Compare page's actual CTR against the expected CTR for its position.

    Returns dict with expected_ctr, ctr_gap, ctr_ratio, anomaly_type.
    """
    expected = _expected_ctr(avg_position)
    gap = actual_ctr - expected
    ratio = actual_ctr / expected if expected > 0 else 1.0

    if ratio >= 1.5:
        anomaly_type = "overperforming"
    elif ratio <= 0.5:
        anomaly_type = "underperforming"
    else:
        anomaly_type = "normal"

    return {
        "expected_ctr": round(expected, 4),
        "actual_ctr": round(actual_ctr, 4),
        "ctr_gap": round(gap, 4),
        "ctr_ratio": round(ratio, 2),
        "anomaly_type": anomaly_type,
    }


# ---------------------------------------------------------------------------
# Helper: engagement cross-reference
# ---------------------------------------------------------------------------
def _cross_reference_engagement(
    page_url: str,
    ga4_landing_data: Optional[pd.DataFrame],
) -> Dict[str, Any]:
    """
    Look up GA4 engagement metrics for a page URL.

    Returns dict with sessions, bounce_rate, avg_session_duration, conversions.
    If no GA4 data, returns None values.
    """
    defaults = {
        "ga4_sessions": None,
        "ga4_bounce_rate": None,
        "ga4_avg_duration": None,
        "ga4_conversions": None,
        "engagement_quality": "unknown",
    }

    if ga4_landing_data is None or ga4_landing_data.empty:
        return defaults

    # Try exact match first, then path match
    match = ga4_landing_data[ga4_landing_data["page"] == page_url]
    if match.empty:
        # Try matching on path portion only
        try:
            from urllib.parse import urlparse
            path = urlparse(page_url).path
            if path:
                match = ga4_landing_data[
                    ga4_landing_data["page"].str.contains(path, regex=False, na=False)
                ]
        except Exception:
            pass

    if match.empty:
        return defaults

    row = match.iloc[0]
    sessions = float(row.get("sessions", 0))
    bounce_rate = float(row.get("bounce_rate", 0))
    avg_duration = float(row.get("avg_session_duration", 0))
    conversions = float(row.get("conversions", 0))

    # Classify engagement quality
    if bounce_rate > 0.75 and avg_duration < 30:
        quality = "poor"
    elif bounce_rate < 0.40 and avg_duration > 120:
        quality = "excellent"
    elif bounce_rate < 0.55:
        quality = "good"
    else:
        quality = "moderate"

    return {
        "ga4_sessions": int(sessions),
        "ga4_bounce_rate": round(bounce_rate, 4),
        "ga4_avg_duration": round(avg_duration, 1),
        "ga4_conversions": int(conversions),
        "engagement_quality": quality,
    }


# ---------------------------------------------------------------------------
# Helper: priority scoring
# ---------------------------------------------------------------------------
def _compute_priority_score(
    clicks: float,
    impressions: float,
    trend: Dict[str, Any],
    ctr_anomaly: Dict[str, Any],
    engagement: Dict[str, Any],
    avg_position: float,
) -> Tuple[float, str]:
    """
    Compute a 0-100 priority score and category for a page.

    Scoring factors:
    - Traffic volume (impressions weight)    — 20 pts
    - Trend direction (declining = higher)   — 25 pts
    - CTR gap (underperforming = higher)     — 25 pts
    - Position opportunity (pos 5-20)        — 15 pts
    - Engagement quality (poor = higher)     — 15 pts

    Returns (score, category) where category is one of:
        critical, high, medium, low, monitor
    """
    score = 0.0

    # Traffic volume — higher impressions = more important to fix
    if impressions > 10000:
        score += 20
    elif impressions > 5000:
        score += 15
    elif impressions > 1000:
        score += 10
    elif impressions > 100:
        score += 5

    # Trend direction — declining pages need attention
    direction = trend.get("direction", "flat")
    pct_change = abs(trend.get("pct_change_30d", 0))
    if direction == "declining":
        score += min(25, 15 + pct_change * 0.2)
    elif direction == "flat":
        score += 8
    elif direction == "rising":
        score += 2

    # CTR anomaly — underperforming CTR is actionable
    anomaly_type = ctr_anomaly.get("anomaly_type", "normal")
    ctr_ratio = ctr_anomaly.get("ctr_ratio", 1.0)
    if anomaly_type == "underperforming":
        score += min(25, 15 + (1 - ctr_ratio) * 20)
    elif anomaly_type == "normal":
        score += 8
    elif anomaly_type == "overperforming":
        score += 2

    # Position opportunity — pages in striking distance (pos 5-20)
    if 5 <= avg_position <= 10:
        score += 15  # Close to page 1 — high opportunity
    elif 10 < avg_position <= 20:
        score += 12  # Page 2 — moderate opportunity
    elif 3 <= avg_position < 5:
        score += 8   # Already top 5 — less upside
    elif avg_position < 3:
        score += 3   # Already top 3
    else:
        score += 5   # Position > 20 — harder to move

    # Engagement quality
    quality = engagement.get("engagement_quality", "unknown")
    if quality == "poor":
        score += 15
    elif quality == "moderate":
        score += 10
    elif quality == "good":
        score += 5
    elif quality == "excellent":
        score += 2
    else:  # unknown
        score += 7  # neutral when no data

    # Clamp to 0-100
    score = max(0, min(100, score))

    # Categorize
    if score >= 75:
        category = "critical"
    elif score >= 55:
        category = "high"
    elif score >= 35:
        category = "medium"
    elif score >= 20:
        category = "low"
    else:
        category = "monitor"

    return round(score, 1), category


# ---------------------------------------------------------------------------
# Helper: generate action recommendation
# ---------------------------------------------------------------------------
def _recommend_action(
    trend: Dict[str, Any],
    ctr_anomaly: Dict[str, Any],
    engagement: Dict[str, Any],
    avg_position: float,
) -> str:
    """Generate a concise action recommendation for a page."""
    actions = []

    # CTR-based recommendations
    if ctr_anomaly.get("anomaly_type") == "underperforming":
        if avg_position <= 5:
            actions.append("Optimize title tag and meta description to improve CTR")
        else:
            actions.append("Improve on-page relevance and meta tags for better CTR")

    # Trend-based recommendations
    direction = trend.get("direction", "flat")
    if direction == "declining":
        pct = abs(trend.get("pct_change_30d", 0))
        if pct > 20:
            actions.append(
                "Urgent: rapidly declining — audit for content freshness and technical issues"
            )
        else:
            actions.append(
                "Investigate decline — check for content staleness or increased competition"
            )

    # Position-based recommendations
    if 5 <= avg_position <= 15:
        actions.append(
            "Striking distance — strengthen content depth and internal linking to push to page 1"
        )
    elif avg_position > 30:
        actions.append("Low visibility — consider content rewrite or consolidation")

    # Engagement-based recommendations
    quality = engagement.get("engagement_quality", "unknown")
    if quality == "poor":
        actions.append(
            "High bounce rate / low engagement — improve content quality and user experience"
        )

    if not actions:
        if (
            ctr_anomaly.get("anomaly_type") == "overperforming"
            and direction == "rising"
        ):
            return "Page is performing well — monitor and protect rankings"
        return "Monitor page performance — no immediate action needed"

    return "; ".join(actions)


# ---------------------------------------------------------------------------
# Main analysis function
# ---------------------------------------------------------------------------
def analyze_page_triage(
    page_daily_data: pd.DataFrame,
    ga4_landing_data: Optional[pd.DataFrame] = None,
    gsc_page_summary: Optional[pd.DataFrame] = None,
) -> Dict[str, Any]:
    """
    Module 2: Page-Level Triage — full implementation.

    Analyzes individual page performance across GSC and GA4 data to produce
    a prioritized list of pages needing attention.

    Args:
        page_daily_data: DataFrame with columns [date, page, clicks, impressions, ctr, position].
            Daily time series data at the page level.
        ga4_landing_data: Optional DataFrame with GA4 engagement metrics per landing page.
        gsc_page_summary: Optional DataFrame with aggregated GSC page-level metrics.

    Returns:
        Dict containing:
            - summary: human-readable summary string
            - total_pages_analyzed: int
            - pages: list of page analysis dicts (sorted by priority score desc)
            - priority_actions: list of top recommended actions
            - category_counts: dict mapping category -> count
            - ctr_anomaly_summary: dict with over/underperforming counts
            - trend_summary: dict with rising/declining/flat counts
    """
    logger.info("Running analyze_page_triage (full implementation)")

    results: Dict[str, Any] = {}

    try:
        # ---- Validate and prepare input ----
        if page_daily_data is None or (
            hasattr(page_daily_data, "empty") and page_daily_data.empty
        ):
            return {
                "summary": "No page-level data available for triage analysis.",
                "total_pages_analyzed": 0,
                "pages": [],
                "priority_actions": [],
                "category_counts": {},
                "ctr_anomaly_summary": {},
                "trend_summary": {},
            }

        df = page_daily_data.copy()

        # Normalize column names
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

        # Ensure date column is datetime
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df = df.dropna(subset=["date"])
            df = df.sort_values("date")

        # Ensure numeric columns
        for col in ["clicks", "impressions", "ctr", "position"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        # Get unique pages
        if "page" not in df.columns:
            return {
                "summary": "Page column not found in data.",
                "total_pages_analyzed": 0,
                "pages": [],
                "priority_actions": [],
                "category_counts": {},
                "ctr_anomaly_summary": {},
                "trend_summary": {},
            }

        unique_pages = df["page"].unique()
        logger.info(f"Analyzing {len(unique_pages)} unique pages")

        # Use summary data if available, otherwise aggregate from daily
        if gsc_page_summary is not None and not gsc_page_summary.empty:
            summary_df = gsc_page_summary.copy()
            summary_df.columns = [
                c.strip().lower().replace(" ", "_") for c in summary_df.columns
            ]
        else:
            summary_df = (
                df.groupby("page")
                .agg(
                    clicks=("clicks", "sum"),
                    impressions=("impressions", "sum"),
                    ctr=("ctr", "mean"),
                    position=("position", "mean"),
                )
                .reset_index()
            )

        # ---- Analyze each page ----
        page_results: List[Dict[str, Any]] = []
        category_counts: Dict[str, int] = {}
        trend_counts = {
            "rising": 0,
            "declining": 0,
            "flat": 0,
            "insufficient_data": 0,
        }
        ctr_counts = {"overperforming": 0, "underperforming": 0, "normal": 0}

        for page_url in unique_pages:
            page_df = df[df["page"] == page_url].copy()

            # Get summary metrics
            page_summary = summary_df[summary_df["page"] == page_url]
            if page_summary.empty:
                total_clicks = page_df["clicks"].sum()
                total_impressions = page_df["impressions"].sum()
                avg_ctr = page_df["ctr"].mean()
                avg_position = page_df["position"].mean()
            else:
                row = page_summary.iloc[0]
                total_clicks = float(row.get("clicks", 0))
                total_impressions = float(row.get("impressions", 0))
                avg_ctr = float(row.get("ctr", 0))
                avg_position = float(row.get("position", 0))

            # 1. Trend fitting
            trend = _fit_page_trend(page_df, metric="clicks")
            trend_counts[trend["direction"]] = (
                trend_counts.get(trend["direction"], 0) + 1
            )

            # 2. CTR anomaly detection
            ctr_anomaly = _detect_ctr_anomaly(avg_ctr, avg_position)
            ctr_counts[ctr_anomaly["anomaly_type"]] = (
                ctr_counts.get(ctr_anomaly["anomaly_type"], 0) + 1
            )

            # 3. Engagement cross-reference
            engagement = _cross_reference_engagement(page_url, ga4_landing_data)

            # 4. Priority scoring
            priority_score, category = _compute_priority_score(
                clicks=total_clicks,
                impressions=total_impressions,
                trend=trend,
                ctr_anomaly=ctr_anomaly,
                engagement=engagement,
                avg_position=avg_position,
            )
            category_counts[category] = category_counts.get(category, 0) + 1

            # 5. Action recommendation
            action = _recommend_action(trend, ctr_anomaly, engagement, avg_position)

            page_results.append(
                {
                    "page": page_url,
                    "total_clicks": int(total_clicks),
                    "total_impressions": int(total_impressions),
                    "avg_ctr": round(avg_ctr, 4),
                    "avg_position": round(avg_position, 1),
                    "trend": trend,
                    "ctr_anomaly": ctr_anomaly,
                    "engagement": engagement,
                    "priority_score": priority_score,
                    "category": category,
                    "recommended_action": action,
                    "data_points": len(page_df),
                }
            )

        # Sort by priority score descending
        page_results.sort(key=lambda p: p["priority_score"], reverse=True)

        # ---- Build priority actions list (top 10 critical/high pages) ----
        priority_actions: List[Dict[str, Any]] = []
        for p in page_results:
            if p["category"] in ("critical", "high") and len(priority_actions) < 10:
                priority_actions.append(
                    {
                        "page": p["page"],
                        "score": p["priority_score"],
                        "category": p["category"],
                        "action": p["recommended_action"],
                    }
                )

        # ---- Generate summary ----
        total = len(page_results)
        critical = category_counts.get("critical", 0)
        high = category_counts.get("high", 0)
        declining = trend_counts.get("declining", 0)
        underperf = ctr_counts.get("underperforming", 0)

        summary_parts = [f"Analyzed {total} pages."]
        if critical + high > 0:
            summary_parts.append(
                f"{critical + high} pages need attention "
                f"({critical} critical, {high} high priority)."
            )
        if declining > 0:
            summary_parts.append(
                f"{declining} pages show declining traffic trends."
            )
        if underperf > 0:
            summary_parts.append(
                f"{underperf} pages have CTR below expected for their position."
            )

        summary = " ".join(summary_parts)

        results = {
            "summary": summary,
            "total_pages_analyzed": total,
            "pages": page_results,
            "priority_actions": priority_actions,
            "category_counts": category_counts,
            "ctr_anomaly_summary": ctr_counts,
            "trend_summary": trend_counts,
        }

    except Exception as e:
        logger.error(f"Error in page triage analysis: {str(e)}", exc_info=True)
        results = {
            "summary": f"Page triage analysis failed: {str(e)}",
            "total_pages_analyzed": 0,
            "pages": [],
            "priority_actions": [],
            "category_counts": {},
            "ctr_anomaly_summary": {},
            "trend_summary": {},
            "error": str(e),
        }

    return results
