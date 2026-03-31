"""
Google Search Console data ingestion module.

Handles OAuth authentication, data fetching, pagination, caching, and error handling
for all GSC API endpoints needed by the Search Intelligence Report.
"""

import hashlib
import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import httpx
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from ..core.cache import cache_get, cache_set
from ..core.errors import GSCAuthError, GSCDataError, GSCQuotaError


class GSCClient:
    """Google Search Console API client with caching and error handling."""
    
    def __init__(self, credentials_dict: Dict[str, Any]):
        """
        Initialize GSC client with OAuth credentials.
        
        Args:
            credentials_dict: OAuth token dictionary from Supabase
        """
        self.credentials = Credentials.from_authorized_user_info(credentials_dict)
        self.service = None
        self._init_service()
    
    def _init_service(self):
        """Initialize the GSC API service, refreshing token if needed."""
        try:
            if self.credentials.expired and self.credentials.refresh_token:
                self.credentials.refresh(Request())
            
            self.service = build('searchconsole', 'v1', credentials=self.credentials)
        except Exception as e:
            raise GSCAuthError(f"Failed to initialize GSC service: {str(e)}")
    
    def _make_cache_key(self, endpoint: str, params: Dict[str, Any]) -> str:
        """Generate a consistent cache key for API requests."""
        key_string = f"gsc:{endpoint}:{json.dumps(params, sort_keys=True)}"
        return hashlib.sha256(key_string.encode()).hexdigest()
    
    async def _cached_request(
        self,
        endpoint: str,
        params: Dict[str, Any],
        ttl_hours: int = 24
    ) -> Optional[Dict[str, Any]]:
        """
        Check cache before making API request.
        
        Args:
            endpoint: API endpoint identifier
            params: Request parameters
            ttl_hours: Cache TTL in hours
            
        Returns:
            Cached response if available, None otherwise
        """
        cache_key = self._make_cache_key(endpoint, params)
        cached = await cache_get("gsc", cache_key)
        return cached
    
    async def _cache_response(
        self,
        endpoint: str,
        params: Dict[str, Any],
        response: Dict[str, Any],
        ttl_hours: int = 24
    ):
        """Cache API response."""
        cache_key = self._make_cache_key(endpoint, params)
        await cache_set("gsc", cache_key, response, ttl_hours=ttl_hours)
    
    def _handle_api_error(self, error: HttpError):
        """Convert Google API errors to our custom exceptions."""
        if error.resp.status == 429:
            raise GSCQuotaError("GSC API quota exceeded. Try again later.")
        elif error.resp.status in [401, 403]:
            raise GSCAuthError(f"GSC authentication failed: {error._get_reason()}")
        else:
            raise GSCDataError(f"GSC API error: {error._get_reason()}")
    
    async def get_properties(self) -> List[str]:
        """
        Fetch list of GSC properties the user has access to.
        
        Returns:
            List of property URLs
        """
        try:
            sites = self.service.sites().list().execute()
            return [site['siteUrl'] for site in sites.get('siteEntry', [])]
        except HttpError as e:
            self._handle_api_error(e)
    
    async def fetch_performance_data(
        self,
        property_url: str,
        start_date: str,
        end_date: str,
        dimensions: List[str],
        row_limit: int = 25000,
        start_row: int = 0,
        filters: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Fetch performance data from GSC with pagination support.
        
        Args:
            property_url: GSC property URL
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            dimensions: List of dimensions (query, page, date, country, device, searchAppearance)
            row_limit: Max rows per request (GSC caps at 25K)
            start_row: Pagination start row
            filters: Optional dimension filters
            
        Returns:
            Performance data response
        """
        cache_params = {
            'property': property_url,
            'start_date': start_date,
            'end_date': end_date,
            'dimensions': sorted(dimensions),
            'row_limit': row_limit,
            'start_row': start_row,
            'filters': filters or []
        }
        
        # Check cache
        cached = await self._cached_request('performance', cache_params)
        if cached:
            return cached
        
        # Build request body
        request_body = {
            'startDate': start_date,
            'endDate': end_date,
            'dimensions': dimensions,
            'rowLimit': row_limit,
            'startRow': start_row
        }
        
        if filters:
            request_body['dimensionFilterGroups'] = [{'filters': filters}]
        
        try:
            response = self.service.searchanalytics().query(
                siteUrl=property_url,
                body=request_body
            ).execute()
            
            # Cache response
            await self._cache_response('performance', cache_params, response)
            
            return response
        except HttpError as e:
            self._handle_api_error(e)
    
    async def fetch_all_performance_data(
        self,
        property_url: str,
        start_date: str,
        end_date: str,
        dimensions: List[str],
        filters: Optional[List[Dict[str, Any]]] = None,
        max_rows: int = 500000
    ) -> List[Dict[str, Any]]:
        """
        Fetch all performance data with automatic pagination.
        
        For large sites, this will make multiple requests to get all data.
        Implements smart pagination by breaking into date chunks if needed.
        
        Args:
            property_url: GSC property URL
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            dimensions: List of dimensions
            filters: Optional dimension filters
            max_rows: Safety limit on total rows
            
        Returns:
            List of all rows across all paginated requests
        """
        all_rows = []
        start_row = 0
        row_limit = 25000
        
        # First attempt: try to get all data in one date range
        while len(all_rows) < max_rows:
            response = await self.fetch_performance_data(
                property_url=property_url,
                start_date=start_date,
                end_date=end_date,
                dimensions=dimensions,
                row_limit=row_limit,
                start_row=start_row,
                filters=filters
            )
            
            rows = response.get('rows', [])
            if not rows:
                break
            
            all_rows.extend(rows)
            
            # If we got fewer than row_limit rows, we're done
            if len(rows) < row_limit:
                break
            
            start_row += row_limit
            
            # If we hit 25K rows multiple times, data is too large for single range
            # Need to chunk by date instead
            if start_row >= 100000:  # After 4 pagination requests
                return await self._fetch_with_date_chunking(
                    property_url=property_url,
                    start_date=start_date,
                    end_date=end_date,
                    dimensions=dimensions,
                    filters=filters,
                    max_rows=max_rows
                )
        
        return all_rows
    
    async def _fetch_with_date_chunking(
        self,
        property_url: str,
        start_date: str,
        end_date: str,
        dimensions: List[str],
        filters: Optional[List[Dict[str, Any]]] = None,
        max_rows: int = 500000,
        chunk_days: int = 30
    ) -> List[Dict[str, Any]]:
        """
        Fetch data by breaking date range into chunks (for very large sites).
        
        Args:
            property_url: GSC property URL
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            dimensions: List of dimensions
            filters: Optional dimension filters
            max_rows: Safety limit on total rows
            chunk_days: Days per chunk
            
        Returns:
            Combined list of all rows
        """
        all_rows = []
        
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        
        current = start
        while current <= end and len(all_rows) < max_rows:
            chunk_end = min(current + timedelta(days=chunk_days - 1), end)
            
            chunk_start_str = current.strftime('%Y-%m-%d')
            chunk_end_str = chunk_end.strftime('%Y-%m-%d')
            
            # Fetch all data for this chunk (with pagination)
            chunk_rows = await self.fetch_all_performance_data(
                property_url=property_url,
                start_date=chunk_start_str,
                end_date=chunk_end_str,
                dimensions=dimensions,
                filters=filters,
                max_rows=max_rows - len(all_rows)
            )
            
            all_rows.extend(chunk_rows)
            current = chunk_end + timedelta(days=1)
        
        return all_rows
    
    async def fetch_query_data(
        self,
        property_url: str,
        start_date: str,
        end_date: str,
        include_date: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Fetch all query performance data.
        
        Args:
            property_url: GSC property URL
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            include_date: Whether to include date dimension (for time series)
            
        Returns:
            List of query performance rows
        """
        dimensions = ['query', 'date'] if include_date else ['query']
        return await self.fetch_all_performance_data(
            property_url=property_url,
            start_date=start_date,
            end_date=end_date,
            dimensions=dimensions
        )
    
    async def fetch_page_data(
        self,
        property_url: str,
        start_date: str,
        end_date: str,
        include_date: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Fetch all page performance data.
        
        Args:
            property_url: GSC property URL
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            include_date: Whether to include date dimension (for time series)
            
        Returns:
            List of page performance rows
        """
        dimensions = ['page', 'date'] if include_date else ['page']
        return await self.fetch_all_performance_data(
            property_url=property_url,
            start_date=start_date,
            end_date=end_date,
            dimensions=dimensions
        )
    
    async def fetch_query_page_mapping(
        self,
        property_url: str,
        start_date: str,
        end_date: str
    ) -> List[Dict[str, Any]]:
        """
        Fetch query-page mapping (which queries drive traffic to which pages).
        
        Args:
            property_url: GSC property URL
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            
        Returns:
            List of query-page performance rows
        """
        return await self.fetch_all_performance_data(
            property_url=property_url,
            start_date=start_date,
            end_date=end_date,
            dimensions=['query', 'page']
        )
    
    async def fetch_daily_summary(
        self,
        property_url: str,
        start_date: str,
        end_date: str
    ) -> List[Dict[str, Any]]:
        """
        Fetch daily aggregated performance data.
        
        Args:
            property_url: GSC property URL
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            
        Returns:
            List of daily summary rows
        """
        return await self.fetch_all_performance_data(
            property_url=property_url,
            start_date=start_date,
            end_date=end_date,
            dimensions=['date']
        )
    
    async def fetch_device_breakdown(
        self,
        property_url: str,
        start_date: str,
        end_date: str
    ) -> List[Dict[str, Any]]:
        """
        Fetch performance data by device type.
        
        Args:
            property_url: GSC property URL
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            
        Returns:
            List of device performance rows
        """
        return await self.fetch_all_performance_data(
            property_url=property_url,
            start_date=start_date,
            end_date=end_date,
            dimensions=['device']
        )
    
    async def fetch_country_breakdown(
        self,
        property_url: str,
        start_date: str,
        end_date: str
    ) -> List[Dict[str, Any]]:
        """
        Fetch performance data by country.
        
        Args:
            property_url: GSC property URL
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            
        Returns:
            List of country performance rows
        """
        return await self.fetch_all_performance_data(
            property_url=property_url,
            start_date=start_date,
            end_date=end_date,
            dimensions=['country']
        )
    
    async def get_sitemaps(self, property_url: str) -> List[Dict[str, Any]]:
        """
        Get list of sitemaps for the property.
        
        Args:
            property_url: GSC property URL
            
        Returns:
            List of sitemap information
        """
        cache_params = {'property': property_url}
        cached = await self._cached_request('sitemaps', cache_params, ttl_hours=168)  # 1 week
        if cached:
            return cached
        
        try:
            response = self.service.sitemaps().list(siteUrl=property_url).execute()
            sitemaps = response.get('sitemap', [])
            
            await self._cache_response('sitemaps', cache_params, sitemaps, ttl_hours=168)
            return sitemaps
        except HttpError as e:
            self._handle_api_error(e)
    
    async def inspect_url(
        self,
        property_url: str,
        url: str
    ) -> Dict[str, Any]:
        """
        Inspect a specific URL for indexing status.
        
        Args:
            property_url: GSC property URL
            url: URL to inspect
            
        Returns:
            URL inspection results
        """
        cache_params = {'property': property_url, 'url': url}
        cached = await self._cached_request('url_inspection', cache_params, ttl_hours=24)
        if cached:
            return cached
        
        try:
            request_body = {'inspectionUrl': url, 'siteUrl': property_url}
            response = self.service.urlInspection().index().inspect(body=request_body).execute()
            
            await self._cache_response('url_inspection', cache_params, response, ttl_hours=24)
            return response
        except HttpError as e:
            # URL inspection can fail for non-indexed URLs, which is not always an error
            if e.resp.status == 404:
                return {'error': 'URL not found in index'}
            self._handle_api_error(e)


async def ingest_gsc_data(
    credentials_dict: Dict[str, Any],
    property_url: str,
    months_back: int = 16
) -> Dict[str, Any]:
    """
    Main ingestion function: pull all GSC data needed for the report.
    
    Args:
        credentials_dict: OAuth credentials
        property_url: GSC property URL
        months_back: How many months of historical data to pull
        
    Returns:
        Dictionary containing all ingested data:
        {
            'daily_summary': [...],
            'query_data': [...],
            'query_date_data': [...],
            'page_data': [...],
            'page_date_data': [...],
            'query_page_mapping': [...],
            'device_breakdown': [...],
            'country_breakdown': [...],
            'sitemaps': [...],
            'metadata': {...}
        }
    """
    client = GSCClient(credentials_dict)
    
    # Calculate date range
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=months_back * 30)
    
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    
    # Handle edge case: properties with very low traffic
    # Try to fetch a small sample first to detect if property has any data
    try:
        sample = await client.fetch_daily_summary(
            property_url=property_url,
            start_date=(end_date - timedelta(days=7)).strftime('%Y-%m-%d'),
            end_date=end_date_str
        )
        
        if not sample or all(row.get('clicks', 0) == 0 for row in sample):
            # Property has no traffic - return minimal dataset
            return {
                'daily_summary': [],
                'query_data': [],
                'query_date_data': [],
                'page_data': [],
                'page_date_data': [],
                'query_page_mapping': [],
                'device_breakdown': [],
                'country_breakdown': [],
                'sitemaps': [],
                'metadata': {
                    'property_url': property_url,
                    'start_date': start_date_str,
                    'end_date': end_date_str,
                    'has_traffic': False,
                    'ingestion_timestamp': datetime.now().isoformat()
                }
            }
    except Exception as e:
        # If even sample fetch fails, property might not exist or have permission issues
        raise GSCDataError(f"Unable to access property data: {str(e)}")
    
    # Property has data - proceed with full ingestion
    results = {
        'metadata': {
            'property_url': property_url,
            'start_date': start_date_str,
            'end_date': end_date_str,
            'has_traffic': True,
            'ingestion_timestamp': datetime.now().isoformat()
        }
    }
    
    # Fetch all data in parallel where possible
    # For now, sequential to avoid rate limits
    
    # 1. Daily summary (for Module 1: Health & Trajectory)
    results['daily_summary'] = await client.fetch_daily_summary(
        property_url=property_url,
        start_date=start_date_str,
        end_date=end_date_str
    )
    
    # 2. Query data aggregated (for Module 3, 4, 7, 10)
    results['query_data'] = await client.fetch_query_data(
        property_url=property_url,
        start_date=start_date_str,
        end_date=end_date_str,
        include_date=False
    )
    
    # 3. Query data with date dimension (for time series analysis)
    results['query_date_data'] = await client.fetch_query_data(
        property_url=property_url,
        start_date=start_date_str,
        end_date=end_date_str,
        include_date=True
    )
    
    # 4. Page data aggregated (for Module 2, 4, 9, 12)
    results['page_data'] = await client.fetch_page_data(
        property_url=property_url,
        start_date=start_date_str,
        end_date=end_date_str,
        include_date=False
    )
    
    # 5. Page data with date dimension (for per-page time series)
    results['page_date_data'] = await client.fetch_page_data(
        property_url=property_url,
        start_date=start_date_str,
        end_date=end_date_str,
        include_date=True
    )
    
    # 6. Query-page mapping (for Module 4: cannibalization detection)
    results['query_page_mapping'] = await client.fetch_query_page_mapping(
        property_url=property_url,
        start_date=start_date_str,
        end_date=end_date_str
    )
    
    # 7. Device breakdown
    results['device_breakdown'] = await client.fetch_device_breakdown(
        property_url=property_url,
        start_date=start_date_str,
        end_date=end_date_str
    )
    
    # 8. Country breakdown
    results['country_breakdown'] = await client.fetch_country_breakdown(
        property_url=property_url,
        start_date=start_date_str,
        end_date=end_date_str
    )
    
    # 9. Sitemaps
    results['sitemaps'] = await client.get_sitemaps(property_url=property_url)
    
    # Add data quality metrics
    results['metadata']['row_counts'] = {
        'daily_summary': len(results['daily_summary']),
        'query_data': len(results['query_data']),
        'query_date_data': len(results['query_date_data']),
        'page_data': len(results['page_data']),
        'page_date_data': len(results['page_date_data']),
        'query_page_mapping': len(results['query_page_mapping']),
        'device_breakdown': len(results['device_breakdown']),
        'country_breakdown': len(results['country_breakdown']),
        'sitemaps': len(results['sitemaps'])
    }
    
    # Calculate total clicks and impressions for context
    if results['daily_summary']:
        total_clicks = sum(row.get('clicks', 0) for row in results['daily_summary'])
        total_impressions = sum(row.get('impressions', 0) for