"""
GSC data ingestion with concurrent pagination support.

Handles date-range chunking for sites exceeding GSC's 25K row limit.
Uses ThreadPoolExecutor for parallel fetches across date ranges.
"""

import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
import hashlib
import json

from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class GSCDataRequest:
    """Configuration for a GSC API request."""
    property_url: str
    start_date: str
    end_date: str
    dimensions: List[str]
    row_limit: int = 25000
    start_row: int = 0


class GSCClient:
    """Google Search Console API client with caching and concurrent pagination."""
    
    def __init__(self, credentials: Credentials, cache_manager=None):
        """
        Initialize GSC client.
        
        Args:
            credentials: Google OAuth2 credentials
            cache_manager: Optional cache manager for storing API responses
        """
        self.service = build('searchconsole', 'v1', credentials=credentials)
        self.cache = cache_manager
        
    def _generate_cache_key(self, request: GSCDataRequest) -> str:
        """Generate cache key from request parameters."""
        key_data = f"{request.property_url}|{request.start_date}|{request.end_date}|{','.join(request.dimensions)}|{request.row_limit}|{request.start_row}"
        return hashlib.sha256(key_data.encode()).hexdigest()
    
    def _fetch_single_request(self, request: GSCDataRequest) -> Dict[str, Any]:
        """
        Fetch a single GSC API request.
        
        Args:
            request: GSCDataRequest configuration
            
        Returns:
            API response as dict
        """
        # Check cache first
        if self.cache:
            cache_key = self._generate_cache_key(request)
            cached = self.cache.get(cache_key)
            if cached:
                logger.debug(f"Cache hit for {request.start_date} to {request.end_date}")
                return cached
        
        # Build request body
        request_body = {
            'startDate': request.start_date,
            'endDate': request.end_date,
            'dimensions': request.dimensions,
            'rowLimit': request.row_limit,
            'startRow': request.start_row
        }
        
        logger.info(f"Fetching GSC data: {request.start_date} to {request.end_date}, dims={request.dimensions}, start_row={request.start_row}")
        
        try:
            response = self.service.searchanalytics().query(
                siteUrl=request.property_url,
                body=request_body
            ).execute()
            
            # Cache the response
            if self.cache:
                cache_key = self._generate_cache_key(request)
                self.cache.set(cache_key, response, ttl_hours=24)
            
            return response
            
        except Exception as e:
            logger.error(f"GSC API error for {request.start_date} to {request.end_date}: {str(e)}")
            raise
    
    def _split_date_range(
        self, 
        start_date: str, 
        end_date: str, 
        chunk_days: int = 30
    ) -> List[Tuple[str, str]]:
        """
        Split date range into smaller chunks for pagination.
        
        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            chunk_days: Number of days per chunk
            
        Returns:
            List of (start_date, end_date) tuples
        """
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        
        chunks = []
        current = start
        
        while current < end:
            chunk_end = min(current + timedelta(days=chunk_days - 1), end)
            chunks.append((
                current.strftime('%Y-%m-%d'),
                chunk_end.strftime('%Y-%m-%d')
            ))
            current = chunk_end + timedelta(days=1)
        
        return chunks
    
    def fetch_data_concurrent(
        self,
        property_url: str,
        start_date: str,
        end_date: str,
        dimensions: List[str],
        chunk_days: int = 30,
        max_workers: int = 5
    ) -> pd.DataFrame:
        """
        Fetch GSC data with concurrent date-range chunking.
        
        For large sites, GSC limits responses to 25K rows. This method splits
        the date range into chunks and fetches them concurrently, then merges.
        
        Args:
            property_url: GSC property URL
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            dimensions: List of dimensions (e.g., ['query', 'page', 'date'])
            chunk_days: Days per chunk (default 30 for monthly chunks)
            max_workers: Max concurrent requests
            
        Returns:
            DataFrame with all results merged and deduplicated
        """
        # Split date range into chunks
        date_chunks = self._split_date_range(start_date, end_date, chunk_days)
        logger.info(f"Split date range into {len(date_chunks)} chunks for concurrent fetching")
        
        # Create requests for each chunk
        requests = [
            GSCDataRequest(
                property_url=property_url,
                start_date=chunk_start,
                end_date=chunk_end,
                dimensions=dimensions
            )
            for chunk_start, chunk_end in date_chunks
        ]
        
        # Fetch concurrently
        all_rows = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all requests
            future_to_request = {
                executor.submit(self._fetch_single_request, req): req 
                for req in requests
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_request):
                request = future_to_request[future]
                try:
                    response = future.result()
                    rows = response.get('rows', [])
                    logger.info(f"Fetched {len(rows)} rows for {request.start_date} to {request.end_date}")
                    all_rows.extend(rows)
                except Exception as e:
                    logger.error(f"Failed to fetch chunk {request.start_date} to {request.end_date}: {str(e)}")
                    # Continue with other chunks rather than failing completely
        
        if not all_rows:
            logger.warning("No data returned from GSC API")
            return pd.DataFrame()
        
        # Convert to DataFrame
        df = self._rows_to_dataframe(all_rows, dimensions)
        
        # Deduplicate and aggregate
        # If we have overlapping data from chunking, sum metrics by dimension keys
        if 'date' in dimensions:
            group_cols = dimensions
        else:
            group_cols = dimensions
        
        df_grouped = df.groupby(group_cols, as_index=False).agg({
            'clicks': 'sum',
            'impressions': 'sum',
            'ctr': 'mean',  # Recalculate CTR after grouping
            'position': 'mean'
        })
        
        # Recalculate CTR properly
        df_grouped['ctr'] = df_grouped['clicks'] / df_grouped['impressions']
        df_grouped['ctr'] = df_grouped['ctr'].fillna(0)
        
        logger.info(f"Final dataset: {len(df_grouped)} rows after deduplication")
        
        return df_grouped
    
    def _rows_to_dataframe(self, rows: List[Dict], dimensions: List[str]) -> pd.DataFrame:
        """
        Convert GSC API rows to DataFrame.
        
        Args:
            rows: List of row dicts from GSC API
            dimensions: Dimension names
            
        Returns:
            DataFrame with dimensions and metrics
        """
        data = []
        for row in rows:
            record = {}
            
            # Extract dimension keys
            keys = row.get('keys', [])
            for i, dim in enumerate(dimensions):
                record[dim] = keys[i] if i < len(keys) else None
            
            # Extract metrics
            record['clicks'] = row.get('clicks', 0)
            record['impressions'] = row.get('impressions', 0)
            record['ctr'] = row.get('ctr', 0.0)
            record['position'] = row.get('position', 0.0)
            
            data.append(record)
        
        return pd.DataFrame(data)
    
    def fetch_query_data(
        self,
        property_url: str,
        start_date: str,
        end_date: str,
        chunk_days: int = 30
    ) -> pd.DataFrame:
        """Fetch all query-level data."""
        return self.fetch_data_concurrent(
            property_url=property_url,
            start_date=start_date,
            end_date=end_date,
            dimensions=['query'],
            chunk_days=chunk_days
        )
    
    def fetch_page_data(
        self,
        property_url: str,
        start_date: str,
        end_date: str,
        chunk_days: int = 30
    ) -> pd.DataFrame:
        """Fetch all page-level data."""
        return self.fetch_data_concurrent(
            property_url=property_url,
            start_date=start_date,
            end_date=end_date,
            dimensions=['page'],
            chunk_days=chunk_days
        )
    
    def fetch_date_data(
        self,
        property_url: str,
        start_date: str,
        end_date: str,
        chunk_days: int = 30
    ) -> pd.DataFrame:
        """Fetch daily time series data."""
        return self.fetch_data_concurrent(
            property_url=property_url,
            start_date=start_date,
            end_date=end_date,
            dimensions=['date'],
            chunk_days=chunk_days
        )
    
    def fetch_query_page_data(
        self,
        property_url: str,
        start_date: str,
        end_date: str,
        chunk_days: int = 30
    ) -> pd.DataFrame:
        """Fetch query-page mapping data."""
        return self.fetch_data_concurrent(
            property_url=property_url,
            start_date=start_date,
            end_date=end_date,
            dimensions=['query', 'page'],
            chunk_days=chunk_days
        )
    
    def fetch_query_date_data(
        self,
        property_url: str,
        start_date: str,
        end_date: str,
        chunk_days: int = 30
    ) -> pd.DataFrame:
        """Fetch per-query time series data."""
        return self.fetch_data_concurrent(
            property_url=property_url,
            start_date=start_date,
            end_date=end_date,
            dimensions=['query', 'date'],
            chunk_days=chunk_days
        )
    
    def fetch_page_date_data(
        self,
        property_url: str,
        start_date: str,
        end_date: str,
        chunk_days: int = 30
    ) -> pd.DataFrame:
        """Fetch per-page time series data."""
        return self.fetch_data_concurrent(
            property_url=property_url,
            start_date=start_date,
            end_date=end_date,
            dimensions=['page', 'date'],
            chunk_days=chunk_days
        )
    
    def fetch_all_standard_reports(
        self,
        property_url: str,
        start_date: str,
        end_date: str,
        chunk_days: int = 30,
        max_workers: int = 5
    ) -> Dict[str, pd.DataFrame]:
        """
        Fetch all standard GSC reports concurrently.
        
        Returns dict with keys: query, page, date, query_page, query_date, page_date
        """
        reports = {
            'query': (self.fetch_query_data, ['query']),
            'page': (self.fetch_page_data, ['page']),
            'date': (self.fetch_date_data, ['date']),
            'query_page': (self.fetch_query_page_data, ['query', 'page']),
            'query_date': (self.fetch_query_date_data, ['query', 'date']),
            'page_date': (self.fetch_page_date_data, ['page', 'date'])
        }
        
        results = {}
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_name = {
                executor.submit(
                    self.fetch_data_concurrent,
                    property_url,
                    start_date,
                    end_date,
                    dims,
                    chunk_days
                ): name
                for name, (_, dims) in reports.items()
            }
            
            for future in as_completed(future_to_name):
                name = future_to_name[future]
                try:
                    df = future.result()
                    results[name] = df
                    logger.info(f"Completed {name} report: {len(df)} rows")
                except Exception as e:
                    logger.error(f"Failed to fetch {name} report: {str(e)}")
                    results[name] = pd.DataFrame()
        
        return results


class CacheManager:
    """Simple cache manager for API responses."""
    
    def __init__(self, storage_backend=None):
        """
        Initialize cache manager.
        
        Args:
            storage_backend: Optional backend (e.g., Supabase client) for persistent cache
        """
        self.backend = storage_backend
        self.memory_cache = {}
    
    def get(self, key: str) -> Optional[Dict]:
        """Get cached value if not expired."""
        # Try memory cache first
        if key in self.memory_cache:
            cached = self.memory_cache[key]
            if datetime.now() < cached['expires_at']:
                return cached['data']
            else:
                del self.memory_cache[key]
        
        # Try persistent backend if available
        if self.backend:
            try:
                result = self.backend.table('api_cache').select('*').eq('cache_key', key).single().execute()
                if result.data:
                    expires_at = datetime.fromisoformat(result.data['expires_at'].replace('Z', '+00:00'))
                    if datetime.now(expires_at.tzinfo) < expires_at:
                        return result.data['response']
            except Exception as e:
                logger.debug(f"Cache backend miss: {str(e)}")
        
        return None
    
    def set(self, key: str, data: Dict, ttl_hours: int = 24):
        """Cache value with TTL."""
        expires_at = datetime.now() + timedelta(hours=ttl_hours)
        
        # Store in memory cache
        self.memory_cache[key] = {
            'data': data,
            'expires_at': expires_at
        }
        
        # Store in persistent backend if available
        if self.backend:
            try:
                self.backend.table('api_cache').upsert({
                    'cache_key': key,
                    'response': data,
                    'expires_at': expires_at.isoformat()
                }).execute()
            except Exception as e:
                logger.warning(f"Failed to cache in backend: {str(e)}")


def calculate_date_range(months_back: int = 16) -> Tuple[str, str]:
    """
    Calculate date range for GSC data pull.
    
    Args:
        months_back: Number of months to go back (default 16 for spec)
        
    Returns:
        Tuple of (start_date, end_date) as YYYY-MM-DD strings
    """
    end_date = datetime.now().date() - timedelta(days=3)  # GSC has 3-day lag
    start_date = end_date - timedelta(days=months_back * 30)
    
    return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')
