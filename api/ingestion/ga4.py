"""
GA4 Data API ingestion module.

Fetches traffic, engagement, and conversion data from Google Analytics 4
to support cross-dataset correlation analysis throughout the report.
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
    FilterExpression,
    Filter,
)
from google.oauth2.credentials import Credentials

logger = logging.getLogger(__name__)


class GA4Client:
    """Client for fetching data from Google Analytics 4 Data API."""

    def __init__(self, credentials: Credentials):
        """
        Initialize GA4 client with OAuth credentials.

        Args:
            credentials: Google OAuth2 credentials with GA4 read scope
        """
        self.credentials = credentials
        self.client = BetaAnalyticsDataClient(credentials=credentials)

    def fetch_traffic_overview(
        self,
        property_id: str,
        start_date: str,
        end_date: str,
    ) -> Dict[str, Any]:
        """
        Fetch overall traffic metrics (sessions, users, pageviews, engagement).

        Args:
            property_id: GA4 property ID (format: "properties/123456789")
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format

        Returns:
            Dict containing aggregated traffic metrics
        """
        try:
            request = RunReportRequest(
                property=property_id,
                date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
                metrics=[
                    Metric(name="sessions"),
                    Metric(name="activeUsers"),
                    Metric(name="screenPageViews"),
                    Metric(name="bounceRate"),
                    Metric(name="engagementRate"),
                    Metric(name="averageSessionDuration"),
                    Metric(name="engagedSessions"),
                ],
            )

            response = self.client.run_report(request)

            if not response.rows:
                return {
                    "sessions": 0,
                    "users": 0,
                    "pageviews": 0,
                    "bounce_rate": 0,
                    "engagement_rate": 0,
                    "avg_session_duration": 0,
                    "engaged_sessions": 0,
                }

            row = response.rows[0]
            return {
                "sessions": int(row.metric_values[0].value),
                "users": int(row.metric_values[1].value),
                "pageviews": int(row.metric_values[2].value),
                "bounce_rate": float(row.metric_values[3].value),
                "engagement_rate": float(row.metric_values[4].value),
                "avg_session_duration": float(row.metric_values[5].value),
                "engaged_sessions": int(row.metric_values[6].value),
            }

        except Exception as e:
            logger.error(f"Error fetching traffic overview: {str(e)}")
            raise

    def fetch_landing_pages(
        self,
        property_id: str,
        start_date: str,
        end_date: str,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        """
        Fetch landing page performance with engagement metrics.

        Args:
            property_id: GA4 property ID
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            limit: Maximum number of pages to return

        Returns:
            List of dicts, each containing landing page URL and metrics
        """
        try:
            request = RunReportRequest(
                property=property_id,
                date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
                dimensions=[Dimension(name="landingPage")],
                metrics=[
                    Metric(name="sessions"),
                    Metric(name="bounceRate"),
                    Metric(name="engagementRate"),
                    Metric(name="averageSessionDuration"),
                    Metric(name="conversions"),
                    Metric(name="engagedSessions"),
                ],
                limit=limit,
                order_bys=[
                    {"metric": {"metric_name": "sessions"}, "desc": True}
                ],
            )

            response = self.client.run_report(request)

            landing_pages = []
            for row in response.rows:
                landing_pages.append({
                    "landing_page": row.dimension_values[0].value,
                    "sessions": int(row.metric_values[0].value),
                    "bounce_rate": float(row.metric_values[1].value),
                    "engagement_rate": float(row.metric_values[2].value),
                    "avg_session_duration": float(row.metric_values[3].value),
                    "conversions": float(row.metric_values[4].value),
                    "engaged_sessions": int(row.metric_values[5].value),
                })

            return landing_pages

        except Exception as e:
            logger.error(f"Error fetching landing pages: {str(e)}")
            raise

    def fetch_traffic_by_channel(
        self,
        property_id: str,
        start_date: str,
        end_date: str,
    ) -> List[Dict[str, Any]]:
        """
        Fetch traffic breakdown by default channel group.

        Args:
            property_id: GA4 property ID
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format

        Returns:
            List of dicts with channel and traffic metrics
        """
        try:
            request = RunReportRequest(
                property=property_id,
                date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
                dimensions=[Dimension(name="sessionDefaultChannelGroup")],
                metrics=[
                    Metric(name="sessions"),
                    Metric(name="activeUsers"),
                    Metric(name="conversions"),
                ],
                order_bys=[
                    {"metric": {"metric_name": "sessions"}, "desc": True}
                ],
            )

            response = self.client.run_report(request)

            channels = []
            for row in response.rows:
                channels.append({
                    "channel": row.dimension_values[0].value,
                    "sessions": int(row.metric_values[0].value),
                    "users": int(row.metric_values[1].value),
                    "conversions": float(row.metric_values[2].value),
                })

            return channels

        except Exception as e:
            logger.error(f"Error fetching traffic by channel: {str(e)}")
            raise

    def fetch_traffic_by_source_medium(
        self,
        property_id: str,
        start_date: str,
        end_date: str,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Fetch traffic breakdown by source/medium.

        Args:
            property_id: GA4 property ID
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            limit: Maximum number of source/medium combinations

        Returns:
            List of dicts with source, medium, and metrics
        """
        try:
            request = RunReportRequest(
                property=property_id,
                date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
                dimensions=[
                    Dimension(name="sessionSource"),
                    Dimension(name="sessionMedium"),
                ],
                metrics=[
                    Metric(name="sessions"),
                    Metric(name="activeUsers"),
                    Metric(name="conversions"),
                ],
                limit=limit,
                order_bys=[
                    {"metric": {"metric_name": "sessions"}, "desc": True}
                ],
            )

            response = self.client.run_report(request)

            sources = []
            for row in response.rows:
                sources.append({
                    "source": row.dimension_values[0].value,
                    "medium": row.dimension_values[1].value,
                    "sessions": int(row.metric_values[0].value),
                    "users": int(row.metric_values[1].value),
                    "conversions": float(row.metric_values[2].value),
                })

            return sources

        except Exception as e:
            logger.error(f"Error fetching traffic by source/medium: {str(e)}")
            raise

    def fetch_conversions(
        self,
        property_id: str,
        start_date: str,
        end_date: str,
    ) -> List[Dict[str, Any]]:
        """
        Fetch conversion events breakdown.

        Args:
            property_id: GA4 property ID
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format

        Returns:
            List of dicts with event name and conversion metrics
        """
        try:
            request = RunReportRequest(
                property=property_id,
                date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
                dimensions=[Dimension(name="eventName")],
                metrics=[
                    Metric(name="conversions"),
                    Metric(name="totalRevenue"),
                ],
                dimension_filter=FilterExpression(
                    filter=Filter(
                        field_name="eventName",
                        string_filter=Filter.StringFilter(
                            match_type=Filter.StringFilter.MatchType.EXACT,
                            value="",
                            case_sensitive=False,
                        ),
                    )
                ),
                order_bys=[
                    {"metric": {"metric_name": "conversions"}, "desc": True}
                ],
            )

            response = self.client.run_report(request)

            conversions = []
            for row in response.rows:
                conversions.append({
                    "event_name": row.dimension_values[0].value,
                    "conversions": float(row.metric_values[0].value),
                    "revenue": float(row.metric_values[1].value),
                })

            return conversions

        except Exception as e:
            logger.error(f"Error fetching conversions: {str(e)}")
            raise

    def fetch_page_time_series(
        self,
        property_id: str,
        start_date: str,
        end_date: str,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        """
        Fetch per-page daily time series for cross-referencing with GSC decay analysis.

        Args:
            property_id: GA4 property ID
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            limit: Maximum number of page-date combinations

        Returns:
            List of dicts with page, date, and daily metrics
        """
        try:
            request = RunReportRequest(
                property=property_id,
                date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
                dimensions=[
                    Dimension(name="pagePath"),
                    Dimension(name="date"),
                ],
                metrics=[
                    Metric(name="sessions"),
                    Metric(name="engagementRate"),
                    Metric(name="averageSessionDuration"),
                    Metric(name="bounceRate"),
                ],
                limit=limit,
            )

            response = self.client.run_report(request)

            time_series = []
            for row in response.rows:
                # Parse YYYYMMDD date format
                date_str = row.dimension_values[1].value
                formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"

                time_series.append({
                    "page_path": row.dimension_values[0].value,
                    "date": formatted_date,
                    "sessions": int(row.metric_values[0].value),
                    "engagement_rate": float(row.metric_values[1].value),
                    "avg_session_duration": float(row.metric_values[2].value),
                    "bounce_rate": float(row.metric_values[3].value),
                })

            return time_series

        except Exception as e:
            logger.error(f"Error fetching page time series: {str(e)}")
            raise

    def fetch_page_source_attribution(
        self,
        property_id: str,
        start_date: str,
        end_date: str,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        """
        Fetch landing page performance by traffic source.

        Args:
            property_id: GA4 property ID
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            limit: Maximum number of page-source combinations

        Returns:
            List of dicts with page, source, and metrics
        """
        try:
            request = RunReportRequest(
                property=property_id,
                date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
                dimensions=[
                    Dimension(name="landingPage"),
                    Dimension(name="sessionSource"),
                ],
                metrics=[
                    Metric(name="sessions"),
                    Metric(name="conversions"),
                    Metric(name="engagementRate"),
                ],
                limit=limit,
                order_bys=[
                    {"metric": {"metric_name": "sessions"}, "desc": True}
                ],
            )

            response = self.client.run_report(request)

            attributions = []
            for row in response.rows:
                attributions.append({
                    "landing_page": row.dimension_values[0].value,
                    "source": row.dimension_values[1].value,
                    "sessions": int(row.metric_values[0].value),
                    "conversions": float(row.metric_values[1].value),
                    "engagement_rate": float(row.metric_values[2].value),
                })

            return attributions

        except Exception as e:
            logger.error(f"Error fetching page source attribution: {str(e)}")
            raise

    def fetch_device_breakdown(
        self,
        property_id: str,
        start_date: str,
        end_date: str,
    ) -> List[Dict[str, Any]]:
        """
        Fetch traffic breakdown by device category.

        Args:
            property_id: GA4 property ID
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format

        Returns:
            List of dicts with device category and metrics
        """
        try:
            request = RunReportRequest(
                property=property_id,
                date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
                dimensions=[Dimension(name="deviceCategory")],
                metrics=[
                    Metric(name="sessions"),
                    Metric(name="activeUsers"),
                    Metric(name="engagementRate"),
                    Metric(name="conversions"),
                ],
                order_bys=[
                    {"metric": {"metric_name": "sessions"}, "desc": True}
                ],
            )

            response = self.client.run_report(request)

            devices = []
            for row in response.rows:
                devices.append({
                    "device": row.dimension_values[0].value,
                    "sessions": int(row.metric_values[0].value),
                    "users": int(row.metric_values[1].value),
                    "engagement_rate": float(row.metric_values[2].value),
                    "conversions": float(row.metric_values[3].value),
                })

            return devices

        except Exception as e:
            logger.error(f"Error fetching device breakdown: {str(e)}")
            raise


async def fetch_ga4_data(
    credentials: Credentials,
    property_id: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Fetch complete GA4 dataset for report generation.

    Args:
        credentials: Google OAuth2 credentials
        property_id: GA4 property ID (format: "properties/123456789")
        start_date: Start date in YYYY-MM-DD (default: 16 months ago)
        end_date: End date in YYYY-MM-DD (default: yesterday)

    Returns:
        Dict containing all GA4 data sections needed for analysis modules
    """
    if not start_date:
        start_date = (datetime.now() - timedelta(days=480)).strftime("%Y-%m-%d")
    if not end_date:
        end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    client = GA4Client(credentials)

    try:
        logger.info(f"Fetching GA4 data for {property_id} from {start_date} to {end_date}")

        data = {
            "property_id": property_id,
            "date_range": {"start": start_date, "end": end_date},
            "traffic_overview": client.fetch_traffic_overview(
                property_id, start_date, end_date
            ),
            "landing_pages": client.fetch_landing_pages(
                property_id, start_date, end_date
            ),
            "channels": client.fetch_traffic_by_channel(
                property_id, start_date, end_date
            ),
            "sources": client.fetch_traffic_by_source_medium(
                property_id, start_date, end_date
            ),
            "conversions": client.fetch_conversions(
                property_id, start_date, end_date
            ),
            "page_time_series": client.fetch_page_time_series(
                property_id, start_date, end_date
            ),
            "page_source_attribution": client.fetch_page_source_attribution(
                property_id, start_date, end_date
            ),
            "devices": client.fetch_device_breakdown(
                property_id, start_date, end_date
            ),
            "fetched_at": datetime.utcnow().isoformat(),
        }

        logger.info(f"Successfully fetched GA4 data: {len(data['landing_pages'])} landing pages")
        return data

    except Exception as e:
        logger.error(f"Error in fetch_ga4_data: {str(e)}")
        raise


def calculate_conversion_rate(
    landing_page_data: List[Dict[str, Any]]
) -> Dict[str, float]:
    """
    Calculate conversion rate per landing page for revenue attribution.

    Args:
        landing_page_data: List of landing page records from fetch_landing_pages

    Returns:
        Dict mapping landing page URL to conversion rate (0-1)
    """
    conversion_rates = {}

    for page in landing_page_data:
        url = page["landing_page"]
        sessions = page["sessions"]
        conversions = page["conversions"]

        if sessions > 0:
            conversion_rates[url] = conversions / sessions
        else:
            conversion_rates[url] = 0.0

    return conversion_rates


def identify_low_engagement_pages(
    landing_page_data: List[Dict[str, Any]],
    min_sessions: int = 100,
    bounce_threshold: float = 80.0,
    duration_threshold: float = 30.0,
) -> List[Dict[str, Any]]:
    """
    Identify landing pages with high traffic but poor engagement metrics.

    Used in Module 2 (Page Triage) to flag content mismatch issues.

    Args:
        landing_page_data: List of landing page records
        min_sessions: Minimum sessions to consider (avoid low-volume noise)
        bounce_threshold: Bounce rate % threshold for flagging
        duration_threshold: Avg session duration (seconds) threshold for flagging

    Returns:
        List of problematic pages with engagement metrics
    """
    low_engagement = []

    for page in landing_page_data:
        if page["sessions"] < min_sessions:
            continue

        if (
            page["bounce_rate"] > bounce_threshold
            and page["avg_session_duration"] < duration_threshold
        ):
            low_engagement.append({
                "landing_page": page["landing_page"],
                "sessions": page["sessions"],
                "bounce_rate": page["bounce_rate"],
                "avg_session_duration": page["avg_session_duration"],
                "engagement_rate": page["engagement_rate"],
                "flag": "content_mismatch",
            })

    # Sort by sessions (prioritize high-traffic pages)
    low_engagement.sort(key=lambda x: x["sessions"], reverse=True)

    return low_engagement
