"""
GA4 Data API integration for Search Intelligence Report.
Handles authentication, data fetching, and response processing with graceful degradation.
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
import time

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


class GA4IngestionError(Exception):
    """Custom exception for GA4 ingestion errors."""
    pass


class GA4Client:
    """Client for fetching data from Google Analytics 4 Data API."""

    def __init__(self, credentials: Credentials, property_id: str):
        """
        Initialize GA4 client.

        Args:
            credentials: Google OAuth2 credentials
            property_id: GA4 property ID (format: properties/123456789)
        """
        self.credentials = credentials
        self.property_id = property_id if property_id.startswith('properties/') else f'properties/{property_id}'
        self.client = None
        self._initialize_client()

    def _initialize_client(self):
        """Initialize the BetaAnalyticsDataClient."""
        try:
            self.client = BetaAnalyticsDataClient(credentials=self.credentials)
            logger.info(f"GA4 client initialized for property: {self.property_id}")
        except Exception as e:
            logger.error(f"Failed to initialize GA4 client: {e}")
            raise GA4IngestionError(f"GA4 client initialization failed: {e}")

    def _run_report_with_retry(
        self,
        request: RunReportRequest,
        max_retries: int = 3,
        backoff_factor: float = 2.0
    ) -> Any:
        """
        Run report with exponential backoff retry logic.

        Args:
            request: The report request
            max_retries: Maximum number of retry attempts
            backoff_factor: Multiplier for backoff delay

        Returns:
            Report response

        Raises:
            GA4IngestionError: If all retries fail
        """
        last_error = None
        for attempt in range(max_retries):
            try:
                response = self.client.run_report(request)
                return response
            except Exception as e:
                last_error = e
                if attempt < max_retries - 1:
                    delay = backoff_factor ** attempt
                    logger.warning(f"GA4 API request failed (attempt {attempt + 1}/{max_retries}), retrying in {delay}s: {e}")
                    time.sleep(delay)
                else:
                    logger.error(f"GA4 API request failed after {max_retries} attempts: {e}")

        raise GA4IngestionError(f"GA4 API request failed after {max_retries} retries: {last_error}")

    def _parse_report_response(
        self,
        response: Any,
        dimensions: List[str],
        metrics: List[str]
    ) -> List[Dict[str, Any]]:
        """
        Parse GA4 report response into structured records.

        Args:
            response: GA4 API response
            dimensions: List of dimension names requested
            metrics: List of metric names requested

        Returns:
            List of records as dictionaries
        """
        records = []
        
        if not response.rows:
            logger.warning("GA4 report returned no rows")
            return records

        for row in response.rows:
            record = {}
            
            # Parse dimensions
            for i, dimension in enumerate(dimensions):
                if i < len(row.dimension_values):
                    record[dimension] = row.dimension_values[i].value
                else:
                    record[dimension] = None
            
            # Parse metrics
            for i, metric in enumerate(metrics):
                if i < len(row.metric_values):
                    value = row.metric_values[i].value
                    # Try to convert to appropriate type
                    try:
                        # Check if it's an integer
                        if '.' not in value:
                            record[metric] = int(value)
                        else:
                            record[metric] = float(value)
                    except (ValueError, AttributeError):
                        record[metric] = value
                else:
                    record[metric] = None
            
            records.append(record)

        return records

    def fetch_traffic_overview(
        self,
        start_date: str,
        end_date: str,
        date_granularity: str = "date"
    ) -> Dict[str, Any]:
        """
        Fetch overall traffic metrics with daily granularity.
        Gracefully handles missing metrics.

        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            date_granularity: Date dimension ("date" or "month")

        Returns:
            Dictionary with daily_data list and summary metrics
        """
        logger.info(f"Fetching GA4 traffic overview from {start_date} to {end_date}")
        
        # Core metrics with fallbacks
        metric_names = [
            "sessions",
            "totalUsers",
            "screenPageViews",
            "engagementRate",
            "averageSessionDuration",
            "bounceRate"
        ]
        
        request = RunReportRequest(
            property=self.property_id,
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimensions=[Dimension(name=date_granularity)],
            metrics=[Metric(name=name) for name in metric_names],
        )

        try:
            response = self._run_report_with_retry(request)
            daily_data = self._parse_report_response(response, [date_granularity], metric_names)
            
            # Calculate summary statistics with fallbacks
            summary = {
                "total_sessions": 0,
                "total_users": 0,
                "total_pageviews": 0,
                "avg_engagement_rate": 0.0,
                "avg_session_duration": 0.0,
                "avg_bounce_rate": 0.0,
                "data_quality": "complete"
            }
            
            if daily_data:
                valid_sessions = [d.get("sessions", 0) for d in daily_data if d.get("sessions") is not None]
                valid_users = [d.get("totalUsers", 0) for d in daily_data if d.get("totalUsers") is not None]
                valid_pageviews = [d.get("screenPageViews", 0) for d in daily_data if d.get("screenPageViews") is not None]
                valid_engagement = [d.get("engagementRate", 0) for d in daily_data if d.get("engagementRate") is not None]
                valid_duration = [d.get("averageSessionDuration", 0) for d in daily_data if d.get("averageSessionDuration") is not None]
                valid_bounce = [d.get("bounceRate", 0) for d in daily_data if d.get("bounceRate") is not None]
                
                summary["total_sessions"] = sum(valid_sessions)
                summary["total_users"] = sum(valid_users)
                summary["total_pageviews"] = sum(valid_pageviews)
                summary["avg_engagement_rate"] = sum(valid_engagement) / len(valid_engagement) if valid_engagement else 0.0
                summary["avg_session_duration"] = sum(valid_duration) / len(valid_duration) if valid_duration else 0.0
                summary["avg_bounce_rate"] = sum(valid_bounce) / len(valid_bounce) if valid_bounce else 0.0
                
                # Assess data quality
                missing_metrics = []
                if not valid_sessions:
                    missing_metrics.append("sessions")
                if not valid_engagement:
                    missing_metrics.append("engagementRate")
                
                if missing_metrics:
                    summary["data_quality"] = "partial"
                    summary["missing_metrics"] = missing_metrics
                    logger.warning(f"GA4 traffic overview has incomplete data. Missing: {missing_metrics}")
            
            return {
                "daily_data": daily_data,
                "summary": summary
            }
            
        except Exception as e:
            logger.error(f"Failed to fetch GA4 traffic overview: {e}")
            # Return minimal structure with error flag
            return {
                "daily_data": [],
                "summary": {
                    "total_sessions": 0,
                    "total_users": 0,
                    "total_pageviews": 0,
                    "avg_engagement_rate": 0.0,
                    "avg_session_duration": 0.0,
                    "avg_bounce_rate": 0.0,
                    "data_quality": "unavailable",
                    "error": str(e)
                }
            }

    def fetch_landing_pages(
        self,
        start_date: str,
        end_date: str,
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """
        Fetch landing page performance with engagement metrics.
        Gracefully handles missing metrics.

        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            limit: Maximum number of pages to return

        Returns:
            List of landing page records
        """
        logger.info(f"Fetching GA4 landing pages from {start_date} to {end_date}")
        
        metric_names = [
            "sessions",
            "totalUsers",
            "engagementRate",
            "bounceRate",
            "averageSessionDuration",
            "conversions"
        ]
        
        request = RunReportRequest(
            property=self.property_id,
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimensions=[Dimension(name="landingPage")],
            metrics=[Metric(name=name) for name in metric_names],
            limit=limit,
            order_bys=[{"metric": {"metric_name": "sessions"}, "desc": True}]
        )

        try:
            response = self._run_report_with_retry(request)
            landing_pages = self._parse_report_response(response, ["landingPage"], metric_names)
            
            # Add data quality flags per page
            for page in landing_pages:
                missing = []
                if page.get("engagementRate") is None:
                    missing.append("engagementRate")
                if page.get("bounceRate") is None:
                    missing.append("bounceRate")
                if page.get("conversions") is None:
                    missing.append("conversions")
                
                if missing:
                    page["data_quality"] = "partial"
                    page["missing_metrics"] = missing
                else:
                    page["data_quality"] = "complete"
            
            logger.info(f"Fetched {len(landing_pages)} landing pages")
            return landing_pages
            
        except Exception as e:
            logger.error(f"Failed to fetch GA4 landing pages: {e}")
            return []

    def fetch_traffic_by_source(
        self,
        start_date: str,
        end_date: str
    ) -> List[Dict[str, Any]]:
        """
        Fetch traffic breakdown by source/medium.

        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)

        Returns:
            List of source/medium records
        """
        logger.info(f"Fetching GA4 traffic by source from {start_date} to {end_date}")
        
        metric_names = [
            "sessions",
            "totalUsers",
            "conversions"
        ]
        
        request = RunReportRequest(
            property=self.property_id,
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimensions=[
                Dimension(name="sessionSource"),
                Dimension(name="sessionMedium")
            ],
            metrics=[Metric(name=name) for name in metric_names],
            limit=500,
            order_bys=[{"metric": {"metric_name": "sessions"}, "desc": True}]
        )

        try:
            response = self._run_report_with_retry(request)
            sources = self._parse_report_response(
                response,
                ["sessionSource", "sessionMedium"],
                metric_names
            )
            logger.info(f"Fetched {len(sources)} traffic sources")
            return sources
            
        except Exception as e:
            logger.error(f"Failed to fetch GA4 traffic sources: {e}")
            return []

    def fetch_traffic_by_channel(
        self,
        start_date: str,
        end_date: str
    ) -> List[Dict[str, Any]]:
        """
        Fetch traffic breakdown by default channel group.

        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)

        Returns:
            List of channel group records
        """
        logger.info(f"Fetching GA4 traffic by channel from {start_date} to {end_date}")
        
        metric_names = [
            "sessions",
            "totalUsers",
            "engagementRate",
            "conversions"
        ]
        
        request = RunReportRequest(
            property=self.property_id,
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimensions=[Dimension(name="sessionDefaultChannelGroup")],
            metrics=[Metric(name=name) for name in metric_names],
            order_bys=[{"metric": {"metric_name": "sessions"}, "desc": True}]
        )

        try:
            response = self._run_report_with_retry(request)
            channels = self._parse_report_response(
                response,
                ["sessionDefaultChannelGroup"],
                metric_names
            )
            logger.info(f"Fetched {len(channels)} channel groups")
            return channels
            
        except Exception as e:
            logger.error(f"Failed to fetch GA4 channels: {e}")
            return []

    def fetch_conversions(
        self,
        start_date: str,
        end_date: str
    ) -> Dict[str, Any]:
        """
        Fetch conversion data (event-based).

        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)

        Returns:
            Dictionary with conversion metrics
        """
        logger.info(f"Fetching GA4 conversions from {start_date} to {end_date}")
        
        metric_names = [
            "conversions",
            "sessions"
        ]
        
        request = RunReportRequest(
            property=self.property_id,
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimensions=[Dimension(name="eventName")],
            metrics=[Metric(name=name) for name in metric_names],
            dimension_filter=FilterExpression(
                filter=Filter(
                    field_name="eventName",
                    string_filter=Filter.StringFilter(
                        match_type=Filter.StringFilter.MatchType.EXACT,
                        value="conversion"
                    )
                )
            )
        )

        try:
            response = self._run_report_with_retry(request)
            conversions = self._parse_report_response(
                response,
                ["eventName"],
                metric_names
            )
            
            # Calculate overall conversion rate
            total_conversions = sum(c.get("conversions", 0) for c in conversions)
            total_sessions = sum(c.get("sessions", 0) for c in conversions)
            conversion_rate = (total_conversions / total_sessions * 100) if total_sessions > 0 else 0.0
            
            return {
                "total_conversions": total_conversions,
                "total_sessions": total_sessions,
                "conversion_rate": conversion_rate,
                "conversion_events": conversions
            }
            
        except Exception as e:
            logger.error(f"Failed to fetch GA4 conversions: {e}")
            return {
                "total_conversions": 0,
                "total_sessions": 0,
                "conversion_rate": 0.0,
                "conversion_events": [],
                "data_quality": "unavailable",
                "error": str(e)
            }

    def fetch_page_time_series(
        self,
        start_date: str,
        end_date: str,
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """
        Fetch per-page daily time series.

        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            limit: Maximum number of rows to return

        Returns:
            List of page×date records
        """
        logger.info(f"Fetching GA4 page time series from {start_date} to {end_date}")
        
        metric_names = [
            "sessions",
            "engagementRate"
        ]
        
        request = RunReportRequest(
            property=self.property_id,
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimensions=[
                Dimension(name="pagePath"),
                Dimension(name="date")
            ],
            metrics=[Metric(name=name) for name in metric_names],
            limit=limit
        )

        try:
            response = self._run_report_with_retry(request)
            time_series = self._parse_report_response(
                response,
                ["pagePath", "date"],
                metric_names
            )
            logger.info(f"Fetched {len(time_series)} page×date records")
            return time_series
            
        except Exception as e:
            logger.error(f"Failed to fetch GA4 page time series: {e}")
            return []

    def fetch_device_breakdown(
        self,
        start_date: str,
        end_date: str
    ) -> List[Dict[str, Any]]:
        """
        Fetch traffic breakdown by device category.

        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)

        Returns:
            List of device category records
        """
        logger.info(f"Fetching GA4 device breakdown from {start_date} to {end_date}")
        
        metric_names = [
            "sessions",
            "totalUsers",
            "engagementRate",
            "conversions"
        ]
        
        request = RunReportRequest(
            property=self.property_id,
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimensions=[Dimension(name="deviceCategory")],
            metrics=[Metric(name=name) for name in metric_names]
        )

        try:
            response = self._run_report_with_retry(request)
            devices = self._parse_report_response(
                response,
                ["deviceCategory"],
                metric_names
            )
            logger.info(f"Fetched {len(devices)} device categories")
            return devices
            
        except Exception as e:
            logger.error(f"Failed to fetch GA4 device breakdown: {e}")
            return []


def fetch_all_ga4_data(
    credentials: Credentials,
    property_id: str,
    months_back: int = 16
) -> Dict[str, Any]:
    """
    Fetch all GA4 data needed for the Search Intelligence Report.
    Implements comprehensive error handling and graceful degradation.

    Args:
        credentials: Google OAuth2 credentials
        property_id: GA4 property ID
        months_back: Number of months of historical data to fetch

    Returns:
        Dictionary containing all GA4 data with quality indicators
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(days=months_back * 30)
    
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")
    
    logger.info(f"Starting GA4 data ingestion for property {property_id}")
    logger.info(f"Date range: {start_date_str} to {end_date_str}")
    
    try:
        client = GA4Client(credentials, property_id)
    except GA4IngestionError as e:
        logger.error(f"Failed to initialize GA4 client: {e}")
        return {
            "status": "failed",
            "error": str(e),
            "data_quality": "unavailable"
        }
    
    result = {
        "property_id": property_id,
        "date_range": {
            "start": start_date_str,
            "end": end_date_str
        },
        "status": "success",
        "data_quality": "complete",
        "warnings": []
    }
    
    # Fetch traffic overview
    try:
        traffic_overview = client.fetch_traffic_overview(start_date_str, end_date_str)
        result["traffic_overview"] = traffic_overview
        
        if traffic_overview["summary"].get("data_quality") != "complete":
            result["warnings"].append("Traffic overview has incomplete data")
            result["data_quality"] = "partial"
    except Exception as e:
        logger.error(f"Error fetching traffic overview: {e}")
        result["traffic_overview"] = {"error": str(e)}
        result["warnings"].append(f"Traffic overview unavailable: {e}")
        result["data_quality"] = "partial"
    
    # Fetch landing pages
    try:
        landing_pages = client.fetch_landing_pages(start_date_str, end_date_str)
        result["landing_pages"] = landing_pages
        
        partial_count = sum(1 for p in landing_pages if p.get("data_quality") == "partial")
        if partial_count > 0:
            result["warnings"].append(f"{partial_count} landing pages have incomplete metrics")
    except Exception as e:
        logger.error(f"Error fetching landing pages: {e}")
        result["landing_pages"] = []
        result["warnings"].append(f"Landing pages unavailable: {e}")
        result["data_quality"] = "partial"
    
    # Fetch traffic by source
    try:
        traffic_sources = client.fetch_traffic_by_source(start_date_str, end_date_str)
        result["traffic_sources"] = traffic_sources
    except Exception as e:
        logger.error(f"Error fetching traffic sources: {e}")
        result["traffic_sources"] = []
        result["warnings"].append(f"Traffic sources unavailable: {e}")
    
    # Fetch traffic by channel
    try:
        traffic_channels = client.fetch_traffic_by_channel(start_date_str, end_date_str)
        result["traffic_channels"] = traffic_channels
    except Exception as e:
        logger.error(f"Error fetching traffic channels: {e}")
        result["traffic_channels