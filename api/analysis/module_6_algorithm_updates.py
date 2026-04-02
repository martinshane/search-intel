"""
Module 6: Algorithm Update Impact Analysis

Detects algorithm update impacts by correlating traffic change points
with known Google algorithm updates. Identifies which pages and content
types were most affected, and assesses site vulnerability to future updates.

Phase 3 — full implementation replacing the stub.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import pandas as pd
import numpy as np
from dataclasses import dataclass

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class AlgorithmUpdate:
    """Represents a known Google algorithm update."""
    date: datetime
    name: str
    type: str  # core, spam, helpful_content, link, product_reviews, etc.
    source: str
    description: Optional[str] = None


@dataclass
class ImpactAssessment:
    """Assessment of an algorithm update's impact on the site."""
    update_name: str
    update_date: datetime
    update_type: str
    site_impact: str  # positive, negative, neutral
    click_change_pct: float
    impression_change_pct: float
    position_change_avg: float
    pages_most_affected: List[Dict[str, Any]]
    common_characteristics: List[str]
    recovery_status: str  # recovered, partial_recovery, not_recovered, ongoing
    days_since_update: int


# ---------------------------------------------------------------------------
# Known Google algorithm updates (2023-2026)
# ---------------------------------------------------------------------------

KNOWN_ALGORITHM_UPDATES: List[AlgorithmUpdate] = [
    # 2026
    AlgorithmUpdate(datetime(2026, 3, 13), "March 2026 Core Update", "core", "Google Search Status Dashboard"),
    AlgorithmUpdate(datetime(2026, 1, 15), "January 2026 Spam Update", "spam", "Google Search Status Dashboard"),
    # 2025
    AlgorithmUpdate(datetime(2025, 12, 12), "December 2025 Core Update", "core", "Google Search Status Dashboard"),
    AlgorithmUpdate(datetime(2025, 11, 11), "November 2025 Core Update", "core", "Google Search Status Dashboard"),
    AlgorithmUpdate(datetime(2025, 8, 22), "August 2025 Core Update", "core", "Google Search Status Dashboard"),
    AlgorithmUpdate(datetime(2025, 6, 5), "June 2025 Spam Update", "spam", "Google Search Status Dashboard"),
    AlgorithmUpdate(datetime(2025, 3, 13), "March 2025 Core Update", "core", "Google Search Status Dashboard"),
    # 2024
    AlgorithmUpdate(datetime(2024, 12, 16), "December 2024 Spam Update", "spam", "Google Search Status Dashboard"),
    AlgorithmUpdate(datetime(2024, 11, 11), "November 2024 Core Update", "core", "Google Search Status Dashboard"),
    AlgorithmUpdate(datetime(2024, 8, 15), "August 2024 Core Update", "core", "Google Search Status Dashboard"),
    AlgorithmUpdate(datetime(2024, 6, 20), "June 2024 Spam Update", "spam", "Google Search Status Dashboard"),
    AlgorithmUpdate(datetime(2024, 3, 5), "March 2024 Core Update", "core", "Google Search Status Dashboard"),
    AlgorithmUpdate(datetime(2024, 3, 5), "March 2024 Spam Update", "spam", "Google Search Status Dashboard"),
    # 2023
    AlgorithmUpdate(datetime(2023, 11, 2), "November 2023 Core Update", "core", "Google Search Status Dashboard"),
    AlgorithmUpdate(datetime(2023, 11, 8), "November 2023 Reviews Update", "product_reviews", "Google Search Status Dashboard"),
    AlgorithmUpdate(datetime(2023, 10, 4), "October 2023 Spam Update", "spam", "Google Search Status Dashboard"),
    AlgorithmUpdate(datetime(2023, 10, 5), "October 2023 Core Update", "core", "Google Search Status Dashboard"),
    AlgorithmUpdate(datetime(2023, 9, 14), "September 2023 Helpful Content Update", "helpful_content", "Google Search Status Dashboard"),
    AlgorithmUpdate(datetime(2023, 8, 22), "August 2023 Core Update", "core", "Google Search Status Dashboard"),
    AlgorithmUpdate(datetime(2023, 4, 12), "April 2023 Reviews Update", "product_reviews", "Google Search Status Dashboard"),
    AlgorithmUpdate(datetime(2023, 3, 15), "March 2023 Core Update", "core", "Google Search Status Dashboard"),
]


# ---------------------------------------------------------------------------
# Core analyzer class
# ---------------------------------------------------------------------------

class AlgorithmImpactAnalyzer:
    """Analyzes the impact of algorithm updates on site performance."""

    def __init__(self, algorithm_updates: List[AlgorithmUpdate]):
        self.algorithm_updates = sorted(
            algorithm_updates, key=lambda x: x.date, reverse=True
        )

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def analyze(
        self,
        daily_data: pd.DataFrame,
        change_points: List[Dict[str, Any]],
        page_daily_data: Optional[pd.DataFrame] = None,
        page_metadata: Optional[pd.DataFrame] = None,
    ) -> Dict[str, Any]:
        """
        Analyze algorithm update impacts on site performance.

        Args:
            daily_data: Daily aggregate metrics (date, clicks, impressions, position)
            change_points: Change points detected in Module 1
            page_daily_data: Optional per-page daily metrics
            page_metadata: Optional page metadata (word_count, has_schema, etc.)

        Returns:
            Dict with updates_impacting_site, vulnerability_score, etc.
        """
        logger.info("Starting algorithm update impact analysis")

        try:
            daily_data = daily_data.copy()
            daily_data["date"] = pd.to_datetime(daily_data["date"])

            if page_daily_data is not None and not page_daily_data.empty:
                page_daily_data = page_daily_data.copy()
                page_daily_data["date"] = pd.to_datetime(page_daily_data["date"])

            # Match change points to algorithm updates
            matched_impacts: List[ImpactAssessment] = []
            unexplained_changes: List[Dict[str, Any]] = []

            for cp in change_points:
                cp_date = pd.to_datetime(cp.get("date", cp.get("timestamp", "")))
                matched_update = self._find_matching_update(cp_date)

                if matched_update:
                    impact = self._assess_update_impact(
                        matched_update, cp, daily_data, page_daily_data, page_metadata
                    )
                    matched_impacts.append(impact)
                else:
                    unexplained_changes.append(cp)

            # Also check for algorithm updates that may not have generated change points
            # but still overlap with the data window
            if len(daily_data) > 0:
                data_start = daily_data["date"].min()
                data_end = daily_data["date"].max()
                matched_dates = {imp.update_date for imp in matched_impacts}

                for update in self.algorithm_updates:
                    if data_start <= update.date <= data_end and update.date not in matched_dates:
                        # Check if there was a notable shift around this update
                        impact = self._assess_update_impact(
                            update, {"date": update.date.isoformat()},
                            daily_data, page_daily_data, page_metadata
                        )
                        # Only include if impact was meaningful (>5% click change)
                        if abs(impact.click_change_pct) > 5:
                            matched_impacts.append(impact)

            # Sort by date descending
            matched_impacts.sort(key=lambda x: x.update_date, reverse=True)

            # Calculate vulnerability
            vulnerability_score, vulnerability_factors = self._calculate_vulnerability(
                matched_impacts, daily_data
            )

            # Generate recommendation
            recommendation = self._generate_recommendation(
                matched_impacts, vulnerability_score, vulnerability_factors
            )

            # Build update timeline
            update_timeline = self._build_update_timeline(daily_data)

            # Build weekly traffic series for frontend charting
            traffic_series = self._build_traffic_series(daily_data)

            return {
                "summary": self._build_summary(matched_impacts, vulnerability_score),
                "updates_impacting_site": [
                    self._impact_to_dict(imp) for imp in matched_impacts
                ],
                "vulnerability_score": round(vulnerability_score, 2),
                "vulnerability_factors": vulnerability_factors,
                "recommendation": recommendation,
                "unexplained_changes": unexplained_changes,
                "total_updates_in_period": len(update_timeline),
                "updates_with_site_impact": len(matched_impacts),
                "update_timeline": update_timeline,
                "traffic_series": traffic_series,
            }

        except Exception as e:
            logger.error(f"Error in algorithm impact analysis: {str(e)}")
            raise

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _find_matching_update(
        self, change_point_date: datetime, window_days: int = 7
    ) -> Optional[AlgorithmUpdate]:
        """Find algorithm update within ±window_days of change point."""
        for update in self.algorithm_updates:
            days_diff = abs((update.date - change_point_date).days)
            if days_diff <= window_days:
                return update
        return None

    def _assess_update_impact(
        self,
        update: AlgorithmUpdate,
        change_point: Dict[str, Any],
        daily_data: pd.DataFrame,
        page_daily_data: Optional[pd.DataFrame],
        page_metadata: Optional[pd.DataFrame],
    ) -> ImpactAssessment:
        """Assess impact of a single algorithm update on the site."""
        update_date = update.date

        # Pre / post windows (14 days each)
        pre_start = update_date - timedelta(days=14)
        pre_end = update_date - timedelta(days=1)
        post_start = update_date
        post_end = update_date + timedelta(days=14)

        pre_data = daily_data[
            (daily_data["date"] >= pre_start) & (daily_data["date"] <= pre_end)
        ]
        post_data = daily_data[
            (daily_data["date"] >= post_start) & (daily_data["date"] <= post_end)
        ]

        if len(pre_data) == 0 or len(post_data) == 0:
            click_change_pct = 0.0
            impression_change_pct = 0.0
            position_change_avg = 0.0
        else:
            pre_clicks = pre_data["clicks"].mean()
            post_clicks = post_data["clicks"].mean()
            click_change_pct = (
                ((post_clicks - pre_clicks) / pre_clicks * 100) if pre_clicks > 0 else 0
            )

            pre_imp = pre_data["impressions"].mean()
            post_imp = post_data["impressions"].mean()
            impression_change_pct = (
                ((post_imp - pre_imp) / pre_imp * 100) if pre_imp > 0 else 0
            )

            pre_pos = pre_data["position"].mean()
            post_pos = post_data["position"].mean()
            position_change_avg = post_pos - pre_pos

        # Impact direction
        if click_change_pct > 5:
            site_impact = "positive"
        elif click_change_pct < -5:
            site_impact = "negative"
        else:
            site_impact = "neutral"

        # Most affected pages
        pages_most_affected = self._find_affected_pages(
            page_daily_data, update_date, top_n=10
        )

        # Common characteristics
        common_characteristics = self._identify_common_characteristics(
            pages_most_affected, page_metadata
        )

        # Recovery status
        recovery_status = self._assess_recovery_status(
            daily_data, update_date, click_change_pct
        )

        days_since_update = (datetime.now() - update_date).days

        return ImpactAssessment(
            update_name=update.name,
            update_date=update_date,
            update_type=update.type,
            site_impact=site_impact,
            click_change_pct=round(click_change_pct, 1),
            impression_change_pct=round(impression_change_pct, 1),
            position_change_avg=round(position_change_avg, 2),
            pages_most_affected=pages_most_affected,
            common_characteristics=common_characteristics,
            recovery_status=recovery_status,
            days_since_update=days_since_update,
        )

    def _find_affected_pages(
        self,
        page_daily_data: Optional[pd.DataFrame],
        update_date: datetime,
        top_n: int = 10,
    ) -> List[Dict[str, Any]]:
        """Find pages most affected by an algorithm update."""
        if page_daily_data is None or page_daily_data.empty:
            return []

        pre_start = update_date - timedelta(days=14)
        pre_end = update_date - timedelta(days=1)
        post_start = update_date
        post_end = update_date + timedelta(days=14)

        page_impacts: List[Dict[str, Any]] = []

        for page in page_daily_data["page"].unique():
            page_data = page_daily_data[page_daily_data["page"] == page]

            pre_page = page_data[
                (page_data["date"] >= pre_start) & (page_data["date"] <= pre_end)
            ]
            post_page = page_data[
                (page_data["date"] >= post_start) & (page_data["date"] <= post_end)
            ]

            if len(pre_page) == 0 or len(post_page) == 0:
                continue

            pre_clicks = pre_page["clicks"].sum()
            post_clicks = post_page["clicks"].sum()
            click_change = post_clicks - pre_clicks
            click_change_pct = (click_change / pre_clicks * 100) if pre_clicks > 0 else 0

            pre_position = pre_page["position"].mean()
            post_position = post_page["position"].mean()
            position_change = post_position - pre_position

            page_impacts.append(
                {
                    "page": page,
                    "click_change": int(click_change),
                    "click_change_pct": round(click_change_pct, 1),
                    "position_change": round(position_change, 2),
                    "pre_clicks": int(pre_clicks),
                    "post_clicks": int(post_clicks),
                }
            )

        page_impacts.sort(key=lambda x: abs(x["click_change"]), reverse=True)
        return page_impacts[:top_n]

    def _identify_common_characteristics(
        self,
        affected_pages: List[Dict[str, Any]],
        page_metadata: Optional[pd.DataFrame],
    ) -> List[str]:
        """Identify common characteristics among negatively affected pages."""
        if page_metadata is None or len(affected_pages) == 0:
            return []

        if isinstance(page_metadata, pd.DataFrame) and page_metadata.empty:
            return []

        characteristics: List[str] = []
        negative_pages = [p["page"] for p in affected_pages if p["click_change"] < 0]

        if not negative_pages:
            return characteristics

        affected_meta = page_metadata[page_metadata["page"].isin(negative_pages)]
        if len(affected_meta) == 0:
            return characteristics

        # Word count check
        if "word_count" in affected_meta.columns:
            avg_wc = affected_meta["word_count"].mean()
            if avg_wc < 500:
                characteristics.append("thin_content")
            elif avg_wc < 1000:
                characteristics.append("short_content")

        # Schema presence
        if "has_schema" in affected_meta.columns:
            schema_pct = affected_meta["has_schema"].mean()
            if schema_pct < 0.3:
                characteristics.append("no_schema")

        # Content type concentration
        if "content_type" in affected_meta.columns:
            ct_counts = affected_meta["content_type"].value_counts()
            if len(ct_counts) > 0:
                dominant = ct_counts.index[0]
                if ct_counts.iloc[0] / len(affected_meta) > 0.6:
                    characteristics.append(f"content_type_{dominant}")

        # Backlinks
        if "backlink_count" in affected_meta.columns:
            if affected_meta["backlink_count"].mean() < 5:
                characteristics.append("low_backlinks")

        # Freshness
        if "last_modified" in affected_meta.columns:
            try:
                days_col = (
                    datetime.now() - pd.to_datetime(affected_meta["last_modified"])
                ).dt.days
                if days_col.mean() > 365:
                    characteristics.append("outdated_content")
            except Exception:
                pass

        return characteristics

    def _assess_recovery_status(
        self,
        daily_data: pd.DataFrame,
        update_date: datetime,
        initial_impact_pct: float,
    ) -> str:
        """Assess whether the site has recovered from an update impact."""
        days_since = (datetime.now() - update_date).days
        if days_since < 30:
            return "ongoing"

        pre_start = update_date - timedelta(days=14)
        pre_end = update_date - timedelta(days=1)
        recent_start = datetime.now() - timedelta(days=14)
        recent_end = datetime.now()

        pre_data = daily_data[
            (daily_data["date"] >= pre_start) & (daily_data["date"] <= pre_end)
        ]
        recent_data = daily_data[
            (daily_data["date"] >= recent_start) & (daily_data["date"] <= recent_end)
        ]

        if len(pre_data) == 0 or len(recent_data) == 0:
            return "unknown"

        pre_clicks = pre_data["clicks"].mean()
        recent_clicks = recent_data["clicks"].mean()
        recovery_pct = (
            ((recent_clicks - pre_clicks) / pre_clicks * 100) if pre_clicks > 0 else 0
        )

        if initial_impact_pct < 0:
            if recovery_pct >= -5:
                return "recovered"
            elif recovery_pct >= initial_impact_pct / 2:
                return "partial_recovery"
            else:
                return "not_recovered"
        else:
            if recovery_pct >= initial_impact_pct * 0.8:
                return "recovered"
            else:
                return "partial_recovery"

    def _calculate_vulnerability(
        self,
        matched_impacts: List[ImpactAssessment],
        daily_data: pd.DataFrame,
    ) -> Tuple[float, List[str]]:
        """Calculate overall algorithmic vulnerability score (0-1)."""
        factors: List[str] = []
        score_components: List[float] = []

        negative_impacts = [
            imp for imp in matched_impacts if imp.site_impact == "negative"
        ]

        # Factor 1: Frequency of negative impacts
        if len(matched_impacts) > 0:
            negative_rate = len(negative_impacts) / len(matched_impacts)
            score_components.append(negative_rate)
            if negative_rate > 0.5:
                factors.append("frequent_negative_impacts")

        # Factor 2: Severity
        if len(negative_impacts) > 0:
            avg_neg = np.mean([imp.click_change_pct for imp in negative_impacts])
            severity = min(abs(avg_neg) / 50, 1.0)
            score_components.append(severity)
            if avg_neg < -20:
                factors.append("severe_impact_history")

        # Factor 3: Recovery capability
        if len(negative_impacts) > 0:
            not_recovered = len(
                [imp for imp in negative_impacts if imp.recovery_status == "not_recovered"]
            )
            recovery_rate = 1 - (not_recovered / len(negative_impacts))
            score_components.append(1 - recovery_rate)
            if recovery_rate < 0.5:
                factors.append("poor_recovery_rate")

        # Factor 4: Recurring vulnerability patterns
        all_chars: List[str] = []
        for imp in negative_impacts:
            all_chars.extend(imp.common_characteristics)
        if all_chars:
            char_counts = pd.Series(all_chars).value_counts()
            recurring = char_counts[char_counts >= 2].index.tolist()
            if recurring:
                factors.append(f"recurring_issues: {', '.join(recurring[:3])}")
                score_components.append(min(0.3 * len(recurring), 1.0))

        # Factor 5: Traffic volatility
        if len(daily_data) > 30:
            cv = (
                daily_data["clicks"].std() / daily_data["clicks"].mean()
                if daily_data["clicks"].mean() > 0
                else 0
            )
            if cv > 0.3:
                factors.append("high_traffic_volatility")
                score_components.append(min(cv, 1.0))

        vulnerability_score = (
            min(float(np.mean(score_components)), 1.0) if score_components else 0.0
        )
        return vulnerability_score, factors

    def _generate_recommendation(
        self,
        matched_impacts: List[ImpactAssessment],
        vulnerability_score: float,
        vulnerability_factors: List[str],
    ) -> str:
        """Generate strategic recommendation based on update history."""
        if not matched_impacts:
            return "No significant algorithm update impacts detected in the analysis period."

        negative_impacts = [
            imp for imp in matched_impacts if imp.site_impact == "negative"
        ]

        if not negative_impacts:
            return (
                "Your site has shown resilience to recent algorithm updates. "
                "Continue current content strategy."
            )

        # Aggregate characteristics
        char_counter: Dict[str, int] = {}
        for imp in negative_impacts:
            for char in imp.common_characteristics:
                char_counter[char] = char_counter.get(char, 0) + 1

        sorted_chars = sorted(char_counter.items(), key=lambda x: x[1], reverse=True)

        rec_parts: List[str] = []

        if vulnerability_score > 0.7:
            rec_parts.append(
                "HIGH VULNERABILITY: Your site is highly susceptible to algorithm updates."
            )
        elif vulnerability_score > 0.4:
            rec_parts.append(
                "MODERATE VULNERABILITY: Your site shows some algorithmic weakness."
            )

        if sorted_chars:
            most_common = sorted_chars[0][0]
            if "thin_content" in most_common or "short_content" in most_common:
                rec_parts.append(
                    "Focus on content depth: expand thin pages to 1000+ words "
                    "with comprehensive coverage."
                )
            elif "no_schema" in most_common:
                rec_parts.append(
                    "Implement structured data: add relevant schema markup "
                    "to improve SERP presentation."
                )
            elif "low_backlinks" in most_common:
                rec_parts.append(
                    "Build authority: focus link building efforts on "
                    "algorithmically vulnerable pages."
                )
            elif "outdated_content" in most_common:
                rec_parts.append(
                    "Content freshness critical: prioritize updating old pages "
                    "with current information."
                )
            elif "content_type" in most_common:
                content_type = most_common.split("_")[-1]
                rec_parts.append(
                    f"Your {content_type} pages are most vulnerable. "
                    "Review and strengthen this content type."
                )

        not_recovered = [
            imp for imp in negative_impacts if imp.recovery_status == "not_recovered"
        ]
        if not_recovered:
            rec_parts.append(
                f"{len(not_recovered)} update impact(s) have not recovered. "
                "Immediate remediation needed for affected pages."
            )

        return (
            " ".join(rec_parts)
            if rec_parts
            else "Continue monitoring algorithm updates and maintain content quality standards."
        )

    def _build_traffic_series(
        self, daily_data: pd.DataFrame
    ) -> List[Dict[str, Any]]:
        """Build a weekly-aggregated traffic series for frontend charting.

        Returns a list of {date, clicks, impressions} dicts aggregated by
        ISO week to keep the payload compact (~70 entries for 16 months).
        """
        if len(daily_data) == 0:
            return []

        try:
            df = daily_data.copy()
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date")

            # Aggregate by ISO week (Monday-based)
            df["week"] = df["date"].dt.to_period("W").apply(lambda p: p.start_time)
            weekly = df.groupby("week").agg(
                clicks=("clicks", "sum"),
                impressions=("impressions", "sum"),
            ).reset_index()

            series = []
            for _, row in weekly.iterrows():
                series.append({
                    "date": row["week"].strftime("%Y-%m-%d"),
                    "clicks": int(row["clicks"]),
                    "impressions": int(row["impressions"]),
                })
            return series
        except Exception as e:
            logger.warning(f"Failed to build traffic series: {e}")
            return []

    def _build_update_timeline(
        self, daily_data: pd.DataFrame
    ) -> List[Dict[str, Any]]:
        """Build a timeline of algorithm updates within the data window."""
        if len(daily_data) == 0:
            return []

        data_start = daily_data["date"].min()
        data_end = daily_data["date"].max()

        timeline = []
        for update in self.algorithm_updates:
            if data_start <= update.date <= data_end:
                timeline.append(
                    {
                        "date": update.date.isoformat(),
                        "name": update.name,
                        "type": update.type,
                        "source": update.source,
                    }
                )
        return sorted(timeline, key=lambda x: x["date"], reverse=True)

    def _build_summary(
        self,
        matched_impacts: List[ImpactAssessment],
        vulnerability_score: float,
    ) -> str:
        """Build a human-readable summary string."""
        if not matched_impacts:
            return (
                "No algorithm update impacts detected in the analysis period. "
                "Your site appears unaffected by recent Google updates."
            )

        negative = [i for i in matched_impacts if i.site_impact == "negative"]
        positive = [i for i in matched_impacts if i.site_impact == "positive"]

        parts = [
            f"Detected {len(matched_impacts)} algorithm update(s) "
            f"impacting your site."
        ]

        if negative:
            worst = min(negative, key=lambda x: x.click_change_pct)
            parts.append(
                f"{len(negative)} had negative impact; "
                f"worst was {worst.update_name} ({worst.click_change_pct:+.1f}% clicks)."
            )

        if positive:
            best = max(positive, key=lambda x: x.click_change_pct)
            parts.append(
                f"{len(positive)} had positive impact; "
                f"best was {best.update_name} ({best.click_change_pct:+.1f}% clicks)."
            )

        not_recovered = [
            i for i in negative if i.recovery_status == "not_recovered"
        ]
        if not_recovered:
            parts.append(
                f"{len(not_recovered)} negative impact(s) have not yet recovered."
            )

        vuln_label = (
            "high" if vulnerability_score > 0.7
            else "moderate" if vulnerability_score > 0.4
            else "low"
        )
        parts.append(f"Overall algorithmic vulnerability: {vuln_label} ({vulnerability_score:.0%}).")

        return " ".join(parts)

    def _impact_to_dict(self, impact: ImpactAssessment) -> Dict[str, Any]:
        """Convert ImpactAssessment to serializable dictionary."""
        return {
            "update_name": impact.update_name,
            "date": impact.update_date.isoformat(),
            "update_type": impact.update_type,
            "site_impact": impact.site_impact,
            "click_change_pct": impact.click_change_pct,
            "impression_change_pct": impact.impression_change_pct,
            "position_change_avg": impact.position_change_avg,
            "pages_most_affected": impact.pages_most_affected,
            "common_characteristics": impact.common_characteristics,
            "recovery_status": impact.recovery_status,
            "days_since_update": impact.days_since_update,
        }


# ---------------------------------------------------------------------------
# Public function (called by routes/modules.py)
# ---------------------------------------------------------------------------

def analyze_algorithm_impacts(
    daily_data,
    change_points_from_module1=None,
    page_daily_data=None,
    page_metadata=None,
) -> Dict[str, Any]:
    """
    Module 6: Algorithm Update Impact Analysis.

    Correlates traffic change points with known Google algorithm updates,
    identifies affected pages, assesses vulnerability, and provides
    recovery recommendations.

    Args:
        daily_data: DataFrame or dict with daily aggregate GSC metrics
                    (columns: date, clicks, impressions, position)
        change_points_from_module1: List of change-point dicts from Module 1
                                   (each should have at least a 'date' key)
        page_daily_data: Optional per-page daily DataFrame
        page_metadata: Optional page metadata DataFrame

    Returns:
        Analysis results dict.
    """
    logger.info("Running Module 6: Algorithm Update Impact Analysis (full)")

    # Normalise inputs
    if isinstance(daily_data, dict):
        daily_data = pd.DataFrame(daily_data)
    if not isinstance(daily_data, pd.DataFrame) or daily_data.empty:
        logger.warning("No daily data provided to Module 6")
        return {
            "summary": "Insufficient data for algorithm impact analysis.",
            "updates_impacting_site": [],
            "vulnerability_score": 0.0,
            "vulnerability_factors": [],
            "recommendation": "Cannot assess algorithm impacts without daily traffic data.",
            "unexplained_changes": [],
            "total_updates_in_period": 0,
            "updates_with_site_impact": 0,
            "update_timeline": [],
            "traffic_series": [],
        }

    change_points = change_points_from_module1 or []

    if isinstance(page_daily_data, dict):
        page_daily_data = pd.DataFrame(page_daily_data)
    if isinstance(page_metadata, dict):
        page_metadata = pd.DataFrame(page_metadata)

    analyzer = AlgorithmImpactAnalyzer(KNOWN_ALGORITHM_UPDATES)
    return analyzer.analyze(
        daily_data=daily_data,
        change_points=change_points,
        page_daily_data=page_daily_data,
        page_metadata=page_metadata,
    )
