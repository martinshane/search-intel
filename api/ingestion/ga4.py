"""
GA4 data ingestion with comprehensive retry logic and graceful fallbacks.

Handles:
- OAuth token refresh
- API rate limiting with exponential backoff
- Missing properties (no GA4 linked)
- Partial data availability (some metrics missing)
- Network errors
- User-friendly error messages
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import time

import httpx
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    RunReportRequest,
    DateRange,
    Dimension,
    Metric,
)
from google.oauth2.credentials import Credentials
from google.auth.exceptions import RefreshError
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)


class GA4IngestionError(Exception):
    """Base exception for GA4 ingestion errors"""
    pass


class GA4NotConfiguredError(GA4IngestionError):
    """Raised when GA4 property is not configured or accessible"""
    pass


class GA4AuthError(GA4IngestionError):
    """Raised when authentication fails"""
    pass


class GA4RateLimitError(GA4IngestionError):
    """Raised when rate limit is exceeded"""
    pass


def retry_with_backoff(
    func,
    max_retries: int = 5,
    initial_delay: float = 1.0,
    backoff_factor: float = 2.0,
    max_delay: float = 60.0,
):
    """
    Retry a function with exponential backoff.
    
    Args:
        func: Function to retry
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay in seconds
        backoff_factor: Multiplier for delay after each retry
        max_delay: Maximum delay between retries
    
    Returns:
        Result of successful function call
    
    Raises:
        Last exception encountered if all retries fail
    """
    delay = initial_delay
    last_exception = None
    
    for attempt in range(max_retries):
        try:
            return func()
        except HttpError as e:
            last_exception = e
            status_code = e.resp.status if hasattr(e, 'resp') else None
            
            # Don't retry on client errors (except rate limit)
            if status_code and 400 <= status_code < 500 and status_code != 429:
                logger.error(f"GA4 API client error (non-retryable): {e}")
                raise GA4IngestionError(f"GA4 API error: {str(e)}") from e
            
            # Rate limit or server error - retry with backoff
            if attempt < max_retries - 1:
                logger.warning(
                    f"GA4 API call failed (attempt {attempt + 1}/{max_retries}): {e}. "
                    f"Retrying in {delay:.1f}s..."
                )
                time.sleep(delay)
                delay = min(delay * backoff_factor, max_delay)
            else:
                logger.error(f"GA4 API call failed after {max_retries} attempts")
        except Exception as e:
            last_exception = e
            logger.error(f"Unexpected error in GA4 API call: {e}")
            if attempt < max_retries - 1:
                logger.warning(f"Retrying in {delay:.1f}s...")
                time.sleep(delay)
                delay = min(delay * backoff_factor, max_delay)
    
    # All retries exhausted
    raise GA4IngestionError(
        f"GA4 API call failed after {max_retries} attempts: {str(last_exception)}"
    ) from last_exception


class GA4Client:
    """Wrapper for GA4 API client with error handling and retries"""
    
    def __init__(self, credentials: Credentials, property_id: str):
        """
        Initialize GA4 client.
        
        Args:
            credentials: OAuth credentials
            property_id: GA4 property ID (format: "properties/123456789")
        """
        self.credentials = credentials
        self.property_id = property_id
        self._client = None
    
    def _ensure_client(self):
        """Ensure client is initialized with valid credentials"""
        if self._client is None:
            try:
                self._client = BetaAnalyticsDataClient(credentials=self.credentials)
            except Exception as e:
                logger.error(f"Failed to initialize GA4 client: {e}")
                raise GA4AuthError(
                    "Failed to connect to Google Analytics. Please reconnect your account."
                ) from e
    
    def _refresh_credentials_if_needed(self):
        """Refresh OAuth credentials if expired"""
        try:
            if self.credentials.expired and self.credentials.refresh_token:
                self.credentials.refresh(httpx.Request())
                logger.info("GA4 credentials refreshed successfully")
        except RefreshError as e:
            logger.error(f"Failed to refresh GA4 credentials: {e}")
            raise GA4AuthError(
                "Your Google Analytics connection has expired. Please reconnect your account."
            ) from e
    
    def run_report(
        self,
        dimensions: List[str],
        metrics: List[str],
        start_date: str,
        end_date: str,
        dimension_filter: Optional[Any] = None,
        metric_filter: Optional[Any] = None,
        limit: int = 100000,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """
        Run a GA4 report with retry logic.
        
        Args:
            dimensions: List of dimension names
            metrics: List of metric names
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            dimension_filter: Optional dimension filter
            metric_filter: Optional metric filter
            limit: Row limit
            offset: Row offset for pagination
        
        Returns:
            Dict with 'rows' and 'metadata'
        
        Raises:
            GA4IngestionError: On API errors
        """
        self._ensure_client()
        self._refresh_credentials_if_needed()
        
        def _run():
            request = RunReportRequest(
                property=self.property_id,
                dimensions=[Dimension(name=d) for d in dimensions],
                metrics=[Metric(name=m) for m in metrics],
                date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
                limit=limit,
                offset=offset,
            )
            
            if dimension_filter:
                request.dimension_filter = dimension_filter
            if metric_filter:
                request.metric_filter = metric_filter
            
            return self._client.run_report(request)
        
        try:
            response = retry_with_backoff(_run)
            
            # Parse response into structured format
            rows = []
            for row in response.rows:
                row_data = {}
                
                # Add dimensions
                for i, dim in enumerate(dimensions):
                    row_data[dim] = row.dimension_values[i].value
                
                # Add metrics
                for i, metric in enumerate(metrics):
                    value = row.metric_values[i].value
                    # Try to convert to appropriate type
                    try:
                        if '.' in value:
                            row_data[metric] = float(value)
                        else:
                            row_data[metric] = int(value)
                    except (ValueError, AttributeError):
                        row_data[metric] = value
                
                rows.append(row_data)
            
            metadata = {
                'row_count': response.row_count if hasattr(response, 'row_count') else len(rows),
                'dimensions': dimensions,
                'metrics': metrics,
                'date_range': {'start': start_date, 'end': end_date},
            }
            
            return {'rows': rows, 'metadata': metadata}
        
        except Exception as e:
            logger.error(f"Error running GA4 report: {e}")
            raise


def fetch_traffic_overview(
    client: GA4Client,
    start_date: str,
    end_date: str,
) -> Dict[str, Any]:
    """
    Fetch high-level traffic metrics.
    
    Returns:
        Dict with daily traffic data or graceful fallback
    """
    try:
        return client.run_report(
            dimensions=['date'],
            metrics=[
                'sessions',
                'totalUsers',
                'screenPageViews',
                'bounceRate',
                'averageSessionDuration',
                'engagementRate',
            ],
            start_date=start_date,
            end_date=end_date,
        )
    except GA4IngestionError as e:
        logger.warning(f"Failed to fetch traffic overview: {e}")
        # Return minimal fallback structure
        return {
            'rows': [],
            'metadata': {
                'error': 'Unable to fetch traffic data',
                'user_message': 'Traffic overview data is not available. This may affect some report sections.',
            }
        }


def fetch_landing_pages(
    client: GA4Client,
    start_date: str,
    end_date: str,
    limit: int = 1000,
) -> Dict[str, Any]:
    """
    Fetch landing page performance with engagement metrics.
    
    Returns:
        Dict with landing page data or graceful fallback
    """
    try:
        return client.run_report(
            dimensions=['landingPage', 'sessionDefaultChannelGroup'],
            metrics=[
                'sessions',
                'totalUsers',
                'bounceRate',
                'averageSessionDuration',
                'engagementRate',
                'conversions',
            ],
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )
    except GA4IngestionError as e:
        logger.warning(f"Failed to fetch landing pages: {e}")
        return {
            'rows': [],
            'metadata': {
                'error': 'Unable to fetch landing page data',
                'user_message': 'Landing page engagement data is not available. Content quality analysis will be limited.',
            }
        }


def fetch_channel_performance(
    client: GA4Client,
    start_date: str,
    end_date: str,
) -> Dict[str, Any]:
    """
    Fetch traffic by channel group.
    
    Returns:
        Dict with channel data or graceful fallback
    """
    try:
        return client.run_report(
            dimensions=['sessionDefaultChannelGroup'],
            metrics=[
                'sessions',
                'totalUsers',
                'engagementRate',
                'conversions',
            ],
            start_date=start_date,
            end_date=end_date,
        )
    except GA4IngestionError as e:
        logger.warning(f"Failed to fetch channel performance: {e}")
        return {
            'rows': [],
            'metadata': {
                'error': 'Unable to fetch channel data',
                'user_message': 'Traffic channel data is not available.',
            }
        }


def fetch_source_medium(
    client: GA4Client,
    start_date: str,
    end_date: str,
    limit: int = 500,
) -> Dict[str, Any]:
    """
    Fetch traffic by source/medium.
    
    Returns:
        Dict with source/medium data or graceful fallback
    """
    try:
        return client.run_report(
            dimensions=['sessionSource', 'sessionMedium'],
            metrics=[
                'sessions',
                'totalUsers',
                'engagementRate',
                'conversions',
            ],
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )
    except GA4IngestionError as e:
        logger.warning(f"Failed to fetch source/medium data: {e}")
        return {
            'rows': [],
            'metadata': {
                'error': 'Unable to fetch source/medium data',
                'user_message': 'Traffic source data is not available.',
            }
        }


def fetch_conversions(
    client: GA4Client,
    start_date: str,
    end_date: str,
) -> Dict[str, Any]:
    """
    Fetch conversion events.
    
    Returns:
        Dict with conversion data or graceful fallback
    """
    try:
        return client.run_report(
            dimensions=['eventName'],
            metrics=['conversions', 'eventValue'],
            start_date=start_date,
            end_date=end_date,
        )
    except GA4IngestionError as e:
        logger.warning(f"Failed to fetch conversions: {e}")
        return {
            'rows': [],
            'metadata': {
                'error': 'Unable to fetch conversion data',
                'user_message': 'Conversion data is not available. Revenue attribution will be limited.',
            }
        }


def fetch_page_date_series(
    client: GA4Client,
    start_date: str,
    end_date: str,
    limit: int = 10000,
) -> Dict[str, Any]:
    """
    Fetch per-page daily time series for engagement tracking.
    
    Returns:
        Dict with page×date data or graceful fallback
    """
    try:
        return client.run_report(
            dimensions=['pagePath', 'date'],
            metrics=[
                'screenPageViews',
                'sessions',
                'engagementRate',
                'bounceRate',
            ],
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )
    except GA4IngestionError as e:
        logger.warning(f"Failed to fetch page×date series: {e}")
        return {
            'rows': [],
            'metadata': {
                'error': 'Unable to fetch page time series',
                'user_message': 'Daily page-level data is not available. Time-based analysis will be limited.',
            }
        }


def fetch_device_breakdown(
    client: GA4Client,
    start_date: str,
    end_date: str,
) -> Dict[str, Any]:
    """
    Fetch traffic by device category.
    
    Returns:
        Dict with device data or graceful fallback
    """
    try:
        return client.run_report(
            dimensions=['deviceCategory'],
            metrics=[
                'sessions',
                'totalUsers',
                'engagementRate',
                'conversions',
            ],
            start_date=start_date,
            end_date=end_date,
        )
    except GA4IngestionError as e:
        logger.warning(f"Failed to fetch device breakdown: {e}")
        return {
            'rows': [],
            'metadata': {
                'error': 'Unable to fetch device data',
                'user_message': 'Device category data is not available.',
            }
        }


def fetch_ecommerce_data(
    client: GA4Client,
    start_date: str,
    end_date: str,
) -> Dict[str, Any]:
    """
    Fetch ecommerce metrics if available (for revenue attribution).
    
    Returns:
        Dict with ecommerce data or empty structure if not available
    """
    try:
        return client.run_report(
            dimensions=['landingPage'],
            metrics=[
                'ecommercePurchases',
                'totalRevenue',
                'averagePurchaseRevenue',
            ],
            start_date=start_date,
            end_date=end_date,
            limit=1000,
        )
    except Exception as e:
        # Ecommerce data is optional - many sites won't have it
        logger.info(f"Ecommerce data not available (this is normal for non-ecommerce sites): {e}")
        return {
            'rows': [],
            'metadata': {
                'error': 'No ecommerce data',
                'user_message': 'Ecommerce tracking is not enabled. Revenue estimates will use conversion values instead.',
                'is_optional': True,
            }
        }


def ingest_ga4_data(
    credentials: Credentials,
    property_id: str,
    months_back: int = 16,
) -> Dict[str, Any]:
    """
    Main entry point for GA4 data ingestion with comprehensive error handling.
    
    Args:
        credentials: OAuth credentials
        property_id: GA4 property ID
        months_back: Number of months of historical data to fetch
    
    Returns:
        Dict containing all GA4 data with graceful fallbacks for missing sections
    
    Raises:
        GA4NotConfiguredError: If property is not accessible
        GA4AuthError: If authentication fails
    """
    # Validate property_id format
    if not property_id:
        raise GA4NotConfiguredError(
            "No Google Analytics property is connected. Please connect a GA4 property to enable engagement analysis."
        )
    
    if not property_id.startswith('properties/'):
        property_id = f'properties/{property_id}'
    
    # Calculate date range
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=months_back * 30)
    
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    
    logger.info(
        f"Starting GA4 ingestion for property {property_id}, "
        f"date range {start_date_str} to {end_date_str}"
    )
    
    try:
        client = GA4Client(credentials, property_id)
    except GA4AuthError as e:
        logger.error(f"GA4 authentication failed: {e}")
        raise
    except Exception as e:
        logger.error(f"Failed to initialize GA4 client: {e}")
        raise GA4NotConfiguredError(
            f"Unable to access Google Analytics property. Error: {str(e)}"
        ) from e
    
    # Fetch all data sections with individual error handling
    data = {
        'property_id': property_id,
        'date_range': {
            'start': start_date_str,
            'end': end_date_str,
        },
        'ingestion_timestamp': datetime.now().isoformat(),
    }
    
    sections = [
        ('traffic_overview', lambda: fetch_traffic_overview(client, start_date_str, end_date_str)),
        ('landing_pages', lambda: fetch_landing_pages(client, start_date_str, end_date_str)),
        ('channel_performance', lambda: fetch_channel_performance(client, start_date_str, end_date_str)),
        ('source_medium', lambda: fetch_source_medium(client, start_date_str, end_date_str)),
        ('conversions', lambda: fetch_conversions(client, start_date_str, end_date_str)),
        ('page_date_series', lambda: fetch_page_date_series(client, start_date_str, end_date_str)),
        ('device_breakdown', lambda: fetch_device_breakdown(client, start_date_str, end_date_str)),
        ('ecommerce', lambda: fetch_ecommerce_data(client, start_date_str, end_date_str)),
    ]
    
    errors = []
    warnings = []
    
    for section_name, fetch_func in sections:
        try:
            logger.info(f"Fetching GA4 section: {section_name}")
            section_data = fetch_func()
            data[section_name] = section_data
            
            # Check for section-level errors/warnings
            if 'metadata' in section_data and 'error' in section_data['metadata']:
                if section_data['metadata'].get('is_optional'):
                    warnings.append(section_data['metadata']['user_message'])
                else:
                    warnings.append(section_data['metadata']['user_message'])
            
            logger.info(
                f"Successfully fetched {section_name}: "
                f"{len(section_data.get('rows', []))} rows"
            )
        except Exception as e:
            error_msg = f"Failed to fetch {section_name}: {str(e)}"
            logger.error(error_msg)
            errors.append(error_msg)
            # Store empty structure so downstream code doesn't break
            data[section_name] = {
                'rows': [],
                'metadata': {'error': str(e)},
            }
    
    # Summary metadata
    data['ingestion_summary'] = {
        'total_sections': len(sections),
        'successful_sections': len([s for s in sections if s[0] in data and data[s[0]].get('rows')]),
        'errors': errors,
        'warnings': warnings,
        'total_rows': sum(len(data[s[0]].get('rows', [])) for s in sections),
    }
    
    # Log final status
    if errors:
        logger.warning(
            f"GA4 ingestion completed with {len(errors)} errors and {len(warnings)} warnings. "
            f"Some analysis sections may be limited."
        )
    else:
        logger.info(
            f"GA4 ingestion completed successfully. "
            f"{data['ingestion_summary']['total_rows']} total rows fetched."
        )
    
    return data


def validate_ga4_connection(credentials: Credentials, property_id: str) -> Dict[str, Any]:
    """
    Validate GA4 connection without fetching full data.
    
    Args:
        credentials: OAuth credentials
        property_id: GA4 property ID
    
    Returns:
        Dict with validation status and basic property info
    """
    if not property_id.startswith('properties/'):
        property_id = f'properties/{property_id}'
    
    try:
        client = GA4Client(credentials, property_id)
        
        # Try a minimal request to verify access
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=7)
        
        result = client.run_report(
            dimensions=['date'],
            metrics=['sessions'],
            start_date=start_date.strftime('%Y-%m-%d'),
            end_date=end_date.strftime('%Y-%m-%d'),
            limit=10,
        )
        
        return {
            'valid': True,
            'property_id': property_id,
            'has_data': len(result['rows']) > 0,
            'message': 'GA4 connection is valid and data is accessible.',
        }
    
    except GA4AuthError as e:
        return {
            'valid': False,
            'error': 'authentication_failed',
            'message': str(e),
        }
    except GA4IngestionError as e:
        return {
            'valid': False,
            'error': 'api_error',
            'message': str(e),
        }
    except Exception as e:
        logger.error(f"Unexpected error validating GA4 connection: {e}")
        return {
            'valid': False,
            'error': 'unknown',
            'message': f'Unable to validate GA4 connection: {str(e)}',
        }
