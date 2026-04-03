"""
Module 1: Traffic Overview
Fetches GA4 metrics for last 90 days, compares period-over-period growth,
stores results in Supabase, and returns formatted data structure.
"""

import os
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import logging

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
)
from google.oauth2.credentials import Credentials

from ..database import get_supabase_client

logger = logging.getLogger(__name__)


class Module1TrafficOverview:
    """Traffic Overview Analysis Module"""

    def __init__(
        self,
        property_id: str,
        credentials: Credentials,
        report_id: str,
    ):
        self.property_id = property_id
        self.credentials = credentials
        self.report_id = report_id
        self.client = BetaAnalyticsDataClient(credentials=credentials)
        self.supabase = get_supabase_client()

    def run(self) -> Dict[str, Any]:
        """
        Execute Module 1 analysis.

        Returns:
            Dict containing metrics, trends, and insights
        """
        logger.info(f"Starting Module 1 analysis for property {self.property_id}")

        try:
            # Fetch current period data (last 90 days)
            current_data = self._fetch_traffic_data(days=90, offset=0)

            # Fetch comparison period data (previous 90 days)
            comparison_data = self._fetch_traffic_data(days=90, offset=90)

            # Calculate metrics and growth
            metrics = self._calculate_metrics(current_data, comparison_data)

            # Generate trends analysis
            trends = self._analyze_trends(current_data)

            # Generate insights
            insights = self._generate_insights(metrics, trends)

            # Prepare result structure
            result = {
                "module_id": 1,
                "module_name": "Traffic Overview",
                "generated_at": datetime.utcnow().isoformat(),
                "metrics": metrics,
                "trends": trends,
                "insights": insights,
                "status": "completed",
            }

            # Store in Supabase
            self._store_results(result)

            logger.info("Module 1 analysis completed successfully")
            return result

        except Exception as e:
            logger.error(f"Error in Module 1 analysis: {str(e)}", exc_info=True)
            error_result = {
                "module_id": 1,
                "module_name": "Traffic Overview",
                "generated_at": datetime.utcnow().isoformat(),
                "status": "failed",
                "error": str(e),
            }
            self._store_results(error_result)
            raise

    def _fetch_traffic_data(
        self, days: int = 90, offset: int = 0
    ) -> Dict[str, Any]:
        """
        Fetch GA4 traffic data for specified period.

        Args:
            days: Number of days to fetch
            offset: Days to offset from today (for comparison periods)

        Returns:
            Dict containing raw GA4 response data
        """
        end_date = datetime.utcnow() - timedelta(days=offset)
        start_date = end_date - timedelta(days=days)

        request = RunReportRequest(
            property=f"properties/{self.property_id}",
            date_ranges=[
                DateRange(
                    start_date=start_date.strftime("%Y-%m-%d"),
                    end_date=end_date.strftime("%Y-%m-%d"),
                )
            ],
            dimensions=[Dimension(name="date")],
            metrics=[
                Metric(name="sessions"),
                Metric(name="totalUsers"),
                Metric(name="screenPageViews"),
                Metric(name="bounceRate"),
                Metric(name="averageSessionDuration"),
                Metric(name="engagementRate"),
            ],
        )

        response = self.client.run_report(request)

        # Parse response into structured format
        data = {
            "date_range": {
                "start": start_date.strftime("%Y-%m-%d"),
                "end": end_date.strftime("%Y-%m-%d"),
            },
            "daily_data": [],
            "totals": {},
        }

        # Extract daily data
        for row in response.rows:
            date_str = row.dimension_values[0].value
            data["daily_data"].append(
                {
                    "date": date_str,
                    "sessions": int(row.metric_values[0].value),
                    "users": int(row.metric_values[1].value),
                    "pageviews": int(row.metric_values[2].value),
                    "bounce_rate": float(row.metric_values[3].value),
                    "avg_session_duration": float(row.metric_values[4].value),
                    "engagement_rate": float(row.metric_values[5].value),
                }
            )

        # Calculate totals
        if data["daily_data"]:
            data["totals"] = {
                "sessions": sum(d["sessions"] for d in data["daily_data"]),
                "users": sum(d["users"] for d in data["daily_data"]),
                "pageviews": sum(d["pageviews"] for d in data["daily_data"]),
                "bounce_rate": (
                    sum(d["bounce_rate"] for d in data["daily_data"])
                    / len(data["daily_data"])
                ),
                "avg_session_duration": (
                    sum(d["avg_session_duration"] for d in data["daily_data"])
                    / len(data["daily_data"])
                ),
                "engagement_rate": (
                    sum(d["engagement_rate"] for d in data["daily_data"])
                    / len(data["daily_data"])
                ),
            }

        return data

    def _calculate_metrics(
        self, current_data: Dict[str, Any], comparison_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Calculate key metrics and period-over-period growth.

        Args:
            current_data: Current period data
            comparison_data: Previous period data

        Returns:
            Dict containing calculated metrics and growth percentages
        """
        current_totals = current_data["totals"]
        comparison_totals = comparison_data["totals"]

        def calculate_growth(current: float, previous: float) -> float:
            """Calculate percentage growth"""
            if previous == 0:
                return 0.0 if current == 0 else 100.0
            return ((current - previous) / previous) * 100

        metrics = {
            "current_period": {
                "sessions": current_totals["sessions"],
                "users": current_totals["users"],
                "pageviews": current_totals["pageviews"],
                "bounce_rate": round(current_totals["bounce_rate"], 2),
                "avg_session_duration": round(
                    current_totals["avg_session_duration"], 2
                ),
                "engagement_rate": round(current_totals["engagement_rate"], 2),
            },
            "previous_period": {
                "sessions": comparison_totals["sessions"],
                "users": comparison_totals["users"],
                "pageviews": comparison_totals["pageviews"],
                "bounce_rate": round(comparison_totals["bounce_rate"], 2),
                "avg_session_duration": round(
                    comparison_totals["avg_session_duration"], 2
                ),
                "engagement_rate": round(comparison_totals["engagement_rate"], 2),
            },
            "growth": {
                "sessions": round(
                    calculate_growth(
                        current_totals["sessions"], comparison_totals["sessions"]
                    ),
                    2,
                ),
                "users": round(
                    calculate_growth(
                        current_totals["users"], comparison_totals["users"]
                    ),
                    2,
                ),
                "pageviews": round(
                    calculate_growth(
                        current_totals["pageviews"], comparison_totals["pageviews"]
                    ),
                    2,
                ),
                "bounce_rate": round(
                    calculate_growth(
                        current_totals["bounce_rate"],
                        comparison_totals["bounce_rate"],
                    ),
                    2,
                ),
                "avg_session_duration": round(
                    calculate_growth(
                        current_totals["avg_session_duration"],
                        comparison_totals["avg_session_duration"],
                    ),
                    2,
                ),
                "engagement_rate": round(
                    calculate_growth(
                        current_totals["engagement_rate"],
                        comparison_totals["engagement_rate"],
                    ),
                    2,
                ),
            },
        }

        # Calculate derived metrics
        if current_totals["sessions"] > 0:
            metrics["current_period"]["pages_per_session"] = round(
                current_totals["pageviews"] / current_totals["sessions"], 2
            )
        else:
            metrics["current_period"]["pages_per_session"] = 0.0

        if comparison_totals["sessions"] > 0:
            metrics["previous_period"]["pages_per_session"] = round(
                comparison_totals["pageviews"] / comparison_totals["sessions"], 2
            )
        else:
            metrics["previous_period"]["pages_per_session"] = 0.0

        if metrics["previous_period"]["pages_per_session"] > 0:
            metrics["growth"]["pages_per_session"] = round(
                calculate_growth(
                    metrics["current_period"]["pages_per_session"],
                    metrics["previous_period"]["pages_per_session"],
                ),
                2,
            )
        else:
            metrics["growth"]["pages_per_session"] = 0.0

        return metrics

    def _analyze_trends(self, current_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze trends in the traffic data.

        Args:
            current_data: Current period data with daily breakdown

        Returns:
            Dict containing trend analysis
        """
        daily_data = current_data["daily_data"]

        if not daily_data:
            return {
                "daily_average": {},
                "peak_day": None,
                "lowest_day": None,
                "weekly_pattern": None,
            }

        # Calculate daily averages
        total_days = len(daily_data)
        daily_average = {
            "sessions": round(
                sum(d["sessions"] for d in daily_data) / total_days, 1
            ),
            "users": round(sum(d["users"] for d in daily_data) / total_days, 1),
            "pageviews": round(
                sum(d["pageviews"] for d in daily_data) / total_days, 1
            ),
        }

        # Find peak and lowest days
        peak_day = max(daily_data, key=lambda x: x["sessions"])
        lowest_day = min(daily_data, key=lambda x: x["sessions"])

        # Analyze weekly patterns
        from collections import defaultdict

        day_of_week_totals = defaultdict(lambda: {"sessions": 0, "count": 0})

        for day in daily_data:
            date_obj = datetime.strptime(day["date"], "%Y%m%d")
            dow = date_obj.strftime("%A")
            day_of_week_totals[dow]["sessions"] += day["sessions"]
            day_of_week_totals[dow]["count"] += 1

        weekly_pattern = {}
        for dow, data in day_of_week_totals.items():
            weekly_pattern[dow] = round(data["sessions"] / data["count"], 1)

        # Find best and worst days of week
        if weekly_pattern:
            best_day = max(weekly_pattern.items(), key=lambda x: x[1])
            worst_day = min(weekly_pattern.items(), key=lambda x: x[1])
        else:
            best_day = None
            worst_day = None

        return {
            "daily_average": daily_average,
            "peak_day": {
                "date": peak_day["date"],
                "sessions": peak_day["sessions"],
            },
            "lowest_day": {
                "date": lowest_day["date"],
                "sessions": lowest_day["sessions"],
            },
            "weekly_pattern": {
                "by_day": weekly_pattern,
                "best_day": best_day[0] if best_day else None,
                "worst_day": worst_day[0] if worst_day else None,
                "variation": (
                    round(
                        ((best_day[1] - worst_day[1]) / worst_day[1]) * 100, 1
                    )
                    if best_day and worst_day and worst_day[1] > 0
                    else 0.0
                ),
            },
        }

    def _generate_insights(
        self, metrics: Dict[str, Any], trends: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Generate actionable insights based on metrics and trends.

        Args:
            metrics: Calculated metrics
            trends: Trend analysis

        Returns:
            List of insight objects
        """
        insights = []

        # Traffic growth insight
        sessions_growth = metrics["growth"]["sessions"]
        if sessions_growth > 10:
            insights.append(
                {
                    "type": "positive",
                    "category": "growth",
                    "title": "Strong Traffic Growth",
                    "description": f"Sessions increased by {sessions_growth}% compared to the previous 90 days, indicating strong positive momentum.",
                    "impact": "high",
                }
            )
        elif sessions_growth < -10:
            insights.append(
                {
                    "type": "negative",
                    "category": "growth",
                    "title": "Traffic Decline Detected",
                    "description": f"Sessions decreased by {abs(sessions_growth)}% compared to the previous period. Immediate investigation recommended.",
                    "impact": "high",
                }
            )
        else:
            insights.append(
                {
                    "type": "neutral",
                    "category": "growth",
                    "title": "Stable Traffic",
                    "description": f"Sessions changed by {sessions_growth}% compared to the previous period, indicating stable performance.",
                    "impact": "medium",
                }
            )

        # Engagement insight
        bounce_rate = metrics["current_period"]["bounce_rate"]
        engagement_rate = metrics["current_period"]["engagement_rate"]

        if bounce_rate > 70:
            insights.append(
                {
                    "type": "warning",
                    "category": "engagement",
                    "title": "High Bounce Rate",
                    "description": f"Bounce rate of {bounce_rate}% suggests visitors aren't finding what they need. Review landing page content and user experience.",
                    "impact": "high",
                }
            )
        elif engagement_rate > 60:
            insights.append(
                {
                    "type": "positive",
                    "category": "engagement",
                    "title": "Strong User Engagement",
                    "description": f"Engagement rate of {engagement_rate}% indicates visitors are actively interacting with your content.",
                    "impact": "medium",
                }
            )

        # Session duration insight
        avg_duration = metrics["current_period"]["avg_session_duration"]
        duration_growth = metrics["growth"]["avg_session_duration"]

        if avg_duration < 60:
            insights.append(
                {
                    "type": "warning",
                    "category": "engagement",
                    "title": "Low Session Duration",
                    "description": f"Average session duration of {round(avg_duration, 1)} seconds is very low. Consider improving content depth and internal linking.",
                    "impact": "medium",
                }
            )
        elif duration_growth > 20:
            insights.append(
                {
                    "type": "positive",
                    "category": "engagement",
                    "title": "Improved Session Duration",
                    "description": f"Average session duration increased by {duration_growth}%, indicating improved content quality or relevance.",
                    "impact": "medium",
                }
            )

        # Weekly pattern insight
        if trends.get("weekly_pattern"):
            variation = trends["weekly_pattern"].get("variation", 0)
            if variation > 30:
                best_day = trends["weekly_pattern"]["best_day"]
                worst_day = trends["weekly_pattern"]["worst_day"]
                insights.append(
                    {
                        "type": "informational",
                        "category": "pattern",
                        "title": "Strong Weekly Seasonality",
                        "description": f"Traffic varies by {variation}% across days of the week. {best_day} performs best while {worst_day} is slowest. Consider timing content releases and campaigns accordingly.",
                        "impact": "low",
                    }
                )

        # Pages per session insight
        pages_per_session = metrics["current_period"]["pages_per_session"]
        if pages_per_session < 1.5:
            insights.append(
                {
                    "type": "warning",
                    "category": "engagement",
                    "title": "Low Pages Per Session",
                    "description": f"Users view only {pages_per_session} pages per session on average. Improve internal linking and content discovery.",
                    "impact": "medium",
                }
            )
        elif pages_per_session > 3:
            insights.append(
                {
                    "type": "positive",
                    "category": "engagement",
                    "title": "Strong Content Navigation",
                    "description": f"Users view {pages_per_session} pages per session on average, indicating effective content structure and internal linking.",
                    "impact": "low",
                }
            )

        # User growth vs session growth insight
        user_growth = metrics["growth"]["users"]
        if abs(sessions_growth - user_growth) > 15:
            if sessions_growth > user_growth:
                insights.append(
                    {
                        "type": "positive",
                        "category": "retention",
                        "title": "Improved User Retention",
                        "description": f"Sessions grew {sessions_growth}% while users grew {user_growth}%, suggesting returning users are visiting more frequently.",
                        "impact": "medium",
                    }
                )
            else:
                insights.append(
                    {
                        "type": "informational",
                        "category": "acquisition",
                        "title": "New User Acquisition",
                        "description": f"Users grew {user_growth}% while sessions grew {sessions_growth}%, indicating strong new user acquisition.",
                        "impact": "medium",
                    }
                )

        return insights

    def _store_results(self, result: Dict[str, Any]) -> None:
        """
        Store module results in Supabase.

        Args:
            result: The complete result dictionary to store
        """
        try:
            # Prepare data for storage
            storage_data = {
                "report_id": self.report_id,
                "module_id": result["module_id"],
                "module_name": result["module_name"],
                "status": result.get("status", "completed"),
                "data": result,
                "generated_at": result["generated_at"],
            }

            # Check if record exists
            existing = (
                self.supabase.table("report_modules")
                .select("id")
                .eq("report_id", self.report_id)
                .eq("module_id", result["module_id"])
                .execute()
            )

            if existing.data:
                # Update existing record
                self.supabase.table("report_modules").update(storage_data).eq(
                    "report_id", self.report_id
                ).eq("module_id", result["module_id"]).execute()
                logger.info(
                    f"Updated Module 1 results for report {self.report_id}"
                )
            else:
                # Insert new record
                self.supabase.table("report_modules").insert(
                    storage_data
                ).execute()
                logger.info(
                    f"Inserted Module 1 results for report {self.report_id}"
                )

        except Exception as e:
            logger.error(
                f"Error storing Module 1 results: {str(e)}", exc_info=True
            )
            # Don't raise - we still want to return results even if storage fails


def run_module_1(
    property_id: str, credentials: Credentials, report_id: str
) -> Dict[str, Any]:
    """
    Convenience function to run Module 1 analysis.

    Args:
        property_id: GA4 property ID
        credentials: Google OAuth credentials
        report_id: Report UUID

    Returns:
        Dict containing module results
    """
    module = Module1TrafficOverview(property_id, credentials, report_id)
    return module.run()
