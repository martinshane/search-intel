"""
GA4 Service Layer

Handles OAuth token refresh, constructs GA4 Data API requests for traffic metrics,
processes dimension/metric responses into clean dictionaries, handles date range
comparisons, and includes error handling for expired tokens and API rate limits.
"""

import logging
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import httpx
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Filter,
    FilterExpression,
    Metric,
    RunReportRequest,
    RunReportResponse,
)
from google.oauth2.credentials import Credentials
from googleapiclient.errors import HttpError

from ..config import settings
from ..database import supabase_client
from ..models.schemas import GA4Credentials

logger = logging.getLogger(__name__)


class GA4ServiceError(Exception):
    """Base exception for GA4 service errors"""
    pass


class GA4AuthError(GA4ServiceError):
    """Authentication/authorization errors"""
    pass


class GA4RateLimitError(GA4ServiceError):
    """Rate limit exceeded errors"""
    pass


class GA4Service:
    """Service for interacting with Google Analytics 4 Data API"""

    def __init__(self):
        self.client_id = settings.GOOGLE_CLIENT_ID
        self.client_secret = settings.GOOGLE_CLIENT_SECRET
        self.max_retries = 3
        self.retry_delay = 2  # seconds
        self.rate_limit_delay = 60  # seconds

    def _refresh_access_token(
        self, 
        refresh_token: str
    ) -> Tuple[str, int]:
        """
        Refresh OAuth access token using refresh token.
        
        Args:
            refresh_token: The refresh token from OAuth flow
            
        Returns:
            Tuple of (new_access_token, expires_in_seconds)
            
        Raises:
            GA4AuthError: If token refresh fails
        """
        try:
            response = httpx.post(
                "https://oauth2.googleapis.com/token",
                data={
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "refresh_token": refresh_token,
                    "grant_type": "refresh_token",
                },
                timeout=30.0,
            )
            response.raise_for_status()
            
            data = response.json()
            return data["access_token"], data["expires_in"]
            
        except httpx.HTTPError as e:
            logger.error(f"Failed to refresh GA4 token: {e}")
            raise GA4AuthError(f"Token refresh failed: {str(e)}")

    def _get_valid_credentials(
        self, 
        user_id: str
    ) -> Credentials:
        """
        Get valid credentials for user, refreshing if necessary.
        
        Args:
            user_id: The user ID to get credentials for
            
        Returns:
            Valid Google OAuth credentials
            
        Raises:
            GA4AuthError: If credentials cannot be obtained or refreshed
        """
        try:
            # Fetch credentials from database
            result = supabase_client.table("oauth_tokens").select("*").eq(
                "user_id", user_id
            ).eq("provider", "google").single().execute()
            
            if not result.data:
                raise GA4AuthError(f"No GA4 credentials found for user {user_id}")
            
            creds_data = result.data
            
            # Check if token needs refresh
            expires_at = datetime.fromisoformat(creds_data["expires_at"].replace("Z", "+00:00"))
            needs_refresh = expires_at <= datetime.utcnow() + timedelta(minutes=5)
            
            if needs_refresh:
                logger.info(f"Refreshing GA4 token for user {user_id}")
                new_token, expires_in = self._refresh_access_token(
                    creds_data["refresh_token"]
                )
                
                # Update database with new token
                new_expires_at = datetime.utcnow() + timedelta(seconds=expires_in)
                supabase_client.table("oauth_tokens").update({
                    "access_token": new_token,
                    "expires_at": new_expires_at.isoformat(),
                    "updated_at": datetime.utcnow().isoformat(),
                }).eq("id", creds_data["id"]).execute()
                
                access_token = new_token
            else:
                access_token = creds_data["access_token"]
            
            # Create credentials object
            credentials = Credentials(
                token=access_token,
                refresh_token=creds_data["refresh_token"],
                token_uri="https://oauth2.googleapis.com/token",
                client_id=self.client_id,
                client_secret=self.client_secret,
            )
            
            return credentials
            
        except Exception as e:
            logger.error(f"Failed to get valid credentials: {e}")
            raise GA4AuthError(f"Credentials error: {str(e)}")

    def _get_client(self, user_id: str) -> BetaAnalyticsDataClient:
        """
        Get authenticated GA4 client.
        
        Args:
            user_id: The user ID
            
        Returns:
            Authenticated BetaAnalyticsDataClient instance
        """
        credentials = self._get_valid_credentials(user_id)
        return BetaAnalyticsDataClient(credentials=credentials)

    def _run_report_with_retry(
        self,
        client: BetaAnalyticsDataClient,
        request: RunReportRequest,
    ) -> RunReportResponse:
        """
        Execute report request with retry logic for rate limits.
        
        Args:
            client: The GA4 client
            request: The report request
            
        Returns:
            The report response
            
        Raises:
            GA4RateLimitError: If rate limit persists after retries
            GA4ServiceError: For other API errors
        """
        for attempt in range(self.max_retries):
            try:
                response = client.run_report(request)
                return response
                
            except Exception as e:
                error_str = str(e).lower()
                
                # Check for rate limit errors
                if "quota" in error_str or "rate limit" in error_str:
                    if attempt < self.max_retries - 1:
                        wait_time = self.rate_limit_delay * (attempt + 1)
                        logger.warning(
                            f"Rate limit hit, waiting {wait_time}s before retry {attempt + 1}/{self.max_retries}"
                        )
                        time.sleep(wait_time)
                        continue
                    else:
                        raise GA4RateLimitError("Rate limit exceeded after retries")
                
                # Check for auth errors
                elif "auth" in error_str or "permission" in error_str or "credentials" in error_str:
                    raise GA4AuthError(f"Authentication error: {str(e)}")
                
                # Other errors
                else:
                    if attempt < self.max_retries - 1:
                        wait_time = self.retry_delay * (attempt + 1)
                        logger.warning(
                            f"API error, retrying in {wait_time}s: {str(e)}"
                        )
                        time.sleep(wait_time)
                        continue
                    else:
                        raise GA4ServiceError(f"API request failed: {str(e)}")
        
        raise GA4ServiceError("Max retries exceeded")

    def _parse_report_response(
        self,
        response: RunReportResponse,
    ) -> List[Dict[str, Any]]:
        """
        Parse GA4 report response into clean dictionaries.
        
        Args:
            response: The API response
            
        Returns:
            List of dictionaries with dimension and metric values
        """
        results = []
        
        # Get dimension and metric headers
        dimension_headers = [header.name for header in response.dimension_headers]
        metric_headers = [header.name for header in response.metric_headers]
        
        # Parse each row
        for row in response.rows:
            row_data = {}
            
            # Add dimensions
            for i, dimension_value in enumerate(row.dimension_values):
                row_data[dimension_headers[i]] = dimension_value.value
            
            # Add metrics
            for i, metric_value in enumerate(row.metric_values):
                metric_name = metric_headers[i]
                value = metric_value.value
                
                # Try to convert to appropriate type
                try:
                    # Check if it's an integer
                    if "." not in value:
                        row_data[metric_name] = int(value)
                    else:
                        row_data[metric_name] = float(value)
                except ValueError:
                    # Keep as string if conversion fails
                    row_data[metric_name] = value
            
            results.append(row_data)
        
        return results

    def get_traffic_overview(
        self,
        user_id: str,
        property_id: str,
        start_date: str,
        end_date: str,
        comparison_start_date: Optional[str] = None,
        comparison_end_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Get traffic overview metrics.
        
        Args:
            user_id: The user ID
            property_id: The GA4 property ID
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            comparison_start_date: Optional comparison period start
            comparison_end_date: Optional comparison period end
            
        Returns:
            Dictionary with current and comparison period metrics
        """
        client = self._get_client(user_id)
        
        # Build date ranges
        date_ranges = [DateRange(start_date=start_date, end_date=end_date)]
        if comparison_start_date and comparison_end_date:
            date_ranges.append(
                DateRange(start_date=comparison_start_date, end_date=comparison_end_date)
            )
        
        # Build request
        request = RunReportRequest(
            property=f"properties/{property_id}",
            date_ranges=date_ranges,
            metrics=[
                Metric(name="sessions"),
                Metric(name="totalUsers"),
                Metric(name="screenPageViews"),
                Metric(name="bounceRate"),
                Metric(name="averageSessionDuration"),
                Metric(name="engagementRate"),
                Metric(name="conversions"),
            ],
        )
        
        response = self._run_report_with_retry(client, request)
        
        # Parse response
        result = {"current": {}, "comparison": {}}
        
        if response.rows:
            current_row = response.rows[0]
            metric_headers = [header.name for header in response.metric_headers]
            
            for i, metric_value in enumerate(current_row.metric_values):
                metric_name = metric_headers[i]
                value = metric_value.value
                
                try:
                    if "." not in value:
                        result["current"][metric_name] = int(value)
                    else:
                        result["current"][metric_name] = float(value)
                except ValueError:
                    result["current"][metric_name] = value
        
        # Parse comparison if present
        if len(response.rows) > 1:
            comparison_row = response.rows[1]
            for i, metric_value in enumerate(comparison_row.metric_values):
                metric_name = metric_headers[i]
                value = metric_value.value
                
                try:
                    if "." not in value:
                        result["comparison"][metric_name] = int(value)
                    else:
                        result["comparison"][metric_name] = float(value)
                except ValueError:
                    result["comparison"][metric_name] = value
        
        return result

    def get_landing_pages(
        self,
        user_id: str,
        property_id: str,
        start_date: str,
        end_date: str,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        """
        Get landing page performance with engagement metrics.
        
        Args:
            user_id: The user ID
            property_id: The GA4 property ID
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            limit: Maximum number of rows to return
            
        Returns:
            List of landing page data dictionaries
        """
        client = self._get_client(user_id)
        
        request = RunReportRequest(
            property=f"properties/{property_id}",
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimensions=[Dimension(name="landingPage")],
            metrics=[
                Metric(name="sessions"),
                Metric(name="totalUsers"),
                Metric(name="bounceRate"),
                Metric(name="averageSessionDuration"),
                Metric(name="engagementRate"),
                Metric(name="conversions"),
                Metric(name="screenPageViews"),
            ],
            limit=limit,
            order_bys=[{"metric": {"metric_name": "sessions"}, "desc": True}],
        )
        
        response = self._run_report_with_retry(client, request)
        return self._parse_report_response(response)

    def get_traffic_by_channel(
        self,
        user_id: str,
        property_id: str,
        start_date: str,
        end_date: str,
    ) -> List[Dict[str, Any]]:
        """
        Get traffic breakdown by channel group.
        
        Args:
            user_id: The user ID
            property_id: The GA4 property ID
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            List of channel data dictionaries
        """
        client = self._get_client(user_id)
        
        request = RunReportRequest(
            property=f"properties/{property_id}",
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimensions=[Dimension(name="sessionDefaultChannelGroup")],
            metrics=[
                Metric(name="sessions"),
                Metric(name="totalUsers"),
                Metric(name="engagementRate"),
                Metric(name="conversions"),
            ],
            order_bys=[{"metric": {"metric_name": "sessions"}, "desc": True}],
        )
        
        response = self._run_report_with_retry(client, request)
        return self._parse_report_response(response)

    def get_traffic_by_source_medium(
        self,
        user_id: str,
        property_id: str,
        start_date: str,
        end_date: str,
        limit: int = 500,
    ) -> List[Dict[str, Any]]:
        """
        Get traffic breakdown by source/medium.
        
        Args:
            user_id: The user ID
            property_id: The GA4 property ID
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            limit: Maximum number of rows to return
            
        Returns:
            List of source/medium data dictionaries
        """
        client = self._get_client(user_id)
        
        request = RunReportRequest(
            property=f"properties/{property_id}",
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimensions=[
                Dimension(name="sessionSource"),
                Dimension(name="sessionMedium"),
            ],
            metrics=[
                Metric(name="sessions"),
                Metric(name="totalUsers"),
                Metric(name="engagementRate"),
                Metric(name="conversions"),
            ],
            limit=limit,
            order_bys=[{"metric": {"metric_name": "sessions"}, "desc": True}],
        )
        
        response = self._run_report_with_retry(client, request)
        return self._parse_report_response(response)

    def get_page_daily_timeseries(
        self,
        user_id: str,
        property_id: str,
        start_date: str,
        end_date: str,
        limit: int = 10000,
    ) -> List[Dict[str, Any]]:
        """
        Get daily time series data per page path.
        
        Args:
            user_id: The user ID
            property_id: The GA4 property ID
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            limit: Maximum number of rows to return
            
        Returns:
            List of page × date data dictionaries
        """
        client = self._get_client(user_id)
        
        request = RunReportRequest(
            property=f"properties/{property_id}",
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimensions=[
                Dimension(name="date"),
                Dimension(name="pagePath"),
            ],
            metrics=[
                Metric(name="screenPageViews"),
                Metric(name="sessions"),
                Metric(name="totalUsers"),
                Metric(name="bounceRate"),
                Metric(name="averageSessionDuration"),
            ],
            limit=limit,
        )
        
        response = self._run_report_with_retry(client, request)
        return self._parse_report_response(response)

    def get_page_source_attribution(
        self,
        user_id: str,
        property_id: str,
        start_date: str,
        end_date: str,
        limit: int = 10000,
    ) -> List[Dict[str, Any]]:
        """
        Get traffic source attribution per page.
        
        Args:
            user_id: The user ID
            property_id: The GA4 property ID
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            limit: Maximum number of rows to return
            
        Returns:
            List of page × source data dictionaries
        """
        client = self._get_client(user_id)
        
        request = RunReportRequest(
            property=f"properties/{property_id}",
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimensions=[
                Dimension(name="pagePath"),
                Dimension(name="sessionSource"),
            ],
            metrics=[
                Metric(name="sessions"),
                Metric(name="totalUsers"),
                Metric(name="conversions"),
            ],
            limit=limit,
        )
        
        response = self._run_report_with_retry(client, request)
        return self._parse_report_response(response)

    def get_device_breakdown(
        self,
        user_id: str,
        property_id: str,
        start_date: str,
        end_date: str,
    ) -> List[Dict[str, Any]]:
        """
        Get traffic breakdown by device category.
        
        Args:
            user_id: The user ID
            property_id: The GA4 property ID
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            List of device data dictionaries
        """
        client = self._get_client(user_id)
        
        request = RunReportRequest(
            property=f"properties/{property_id}",
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimensions=[Dimension(name="deviceCategory")],
            metrics=[
                Metric(name="sessions"),
                Metric(name="totalUsers"),
                Metric(name="bounceRate"),
                Metric(name="engagementRate"),
                Metric(name="conversions"),
            ],
            order_bys=[{"metric": {"metric_name": "sessions"}, "desc": True}],
        )
        
        response = self._run_report_with_retry(client, request)
        return self._parse_report_response(response)

    def get_conversions(
        self,
        user_id: str,
        property_id: str,
        start_date: str,
        end_date: str,
    ) -> List[Dict[str, Any]]:
        """
        Get conversion events breakdown.
        
        Args:
            user_id: The user ID
            property_id: The GA4 property ID
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            List of conversion event data dictionaries
        """
        client = self._get_client(user_id)
        
        request = RunReportRequest(
            property=f"properties/{property_id}",
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimensions=[Dimension(name="eventName")],
            metrics=[
                Metric(name="conversions"),
                Metric(name="totalUsers"),
            ],
            dimension_filter=FilterExpression(
                filter=Filter(
                    field_name="eventName",
                    string_filter=Filter.StringFilter(match_type="CONTAINS", value=""),
                )
            ),
            order_bys=[{"metric": {"metric_name": "conversions"}, "desc": True}],
        )
        
        response = self._run_report_with_retry(client, request)
        return self._parse_report_response(response)

    def get_organic_landing_pages(
        self,
        user_id: str,
        property_id: str,
        start_date: str,
        end_date: str,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        """
        Get landing pages specifically for organic search traffic.
        
        Args:
            user_id: The user ID
            property_id: The GA4 property ID
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            limit: Maximum number of rows to return
            
        Returns:
            List of organic landing page data dictionaries
        """
        client = self._get_client(user_id)
        
        request = RunReportRequest(
            property=f"properties/{property_id}",
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimensions=[Dimension(name="landingPage")],
            metrics=[
                Metric(name="sessions"),
                Metric(name="totalUsers"),
                Metric(name="bounceRate"),
                Metric(name="averageSessionDuration"),
                Metric(name="engagementRate"),
                Metric(name="conversions"),
            ],
            dimension_filter=FilterExpression(
                filter=Filter(
                    field_name="sessionDefaultChannelGroup",
                    string_filter=Filter.StringFilter(
                        match_type="EXACT", 
                        value="Organic Search"
                    ),
                )
            ),
            limit=limit,
            order_bys=[{"metric": {"metric_name": "sessions"}, "desc": True}],
        )
        
        response = self._run_report_with_retry(client, request)
        return self._parse_report_response(response)

    def get_daily_aggregate_timeseries(
        self,
        user_id: str,
        property_id: str,
        start_date: str,
        end_date: str,
    ) -> List[Dict[str, Any]]:
        """
        Get daily aggregate metrics time series.
        
        Args:
            user_id: The user ID
            property_id: The GA4 property ID
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            List of daily aggregate data dictionaries
        """
        client = self._get_client(user_id)
        
        request = RunReportRequest(
            property=f"properties/{property_id}",
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimensions=[Dimension(name="date")],
            metrics=[
                Metric(name="sessions"),
                Metric(name="totalUsers"),
                Metric(name="screenPageViews"),
                Metric(name="bounceRate"),
                Metric(name="averageSessionDuration"),
                Metric(name="engagementRate"),
                Metric(name="conversions"),
            ],
        )
        
        response = self._run_report_with_retry(client, request)
        return self._parse_report_response(response)


# Singleton instance
ga4_service = GA4Service()

