"""
GSC (Google Search Console) data ingestion module.

Handles OAuth authentication and data extraction from Google Search Console API.
Implements smart pagination, caching, and multi-dimensional data pulls.
"""

import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import hashlib
import json

import httpx
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd

from ..core.config import settings
from ..core.supabase_client import get_supabase_client
from ..core.logger import get_logger

logger = get_logger(__name__)


class GSCIngestionError(Exception):
    """Base exception for GSC ingestion errors."""
    pass


class GSCAuthError(GSCIngestionError):
    """Authentication-related errors."""
    pass


class GSCAPIError(GSCIngestionError):
    """API request errors."""
    pass


class GSCDataExtractor:
    """
    Extracts comprehensive data from Google Search Console API.
    
    Implements multiple query dimensions to build complete datasets:
    - By query (all queries, paginated)
    - By page (all pages)
    - By date (daily granularity)
    - By query+page (mapping queries to landing pages)
    - By query+date (per-keyword time series)
    - By page+date (per-page time series)
    
    Handles GSC's 25K row limit via smart pagination strategies.
    Caches responses in Supabase to avoid redundant API calls.
    """
    
    # GSC API limits
    MAX_ROWS_PER_REQUEST = 25000
    DEFAULT_BATCH_SIZE = 25000
    
    # Date range: 16 months for comprehensive analysis
    DEFAULT_LOOKBACK_DAYS = 480  # ~16 months
    
    def __init__(self, oauth_token: Dict, property_url: str):
        """
        Initialize GSC extractor with OAuth credentials.
        
        Args:
            oauth_token: OAuth token dict with 'access_token', 'refresh_token', etc.
            property_url: GSC property URL (e.g., 'sc-domain:example.com')
        """
        self.property_url = property_url
        self.credentials = self._build_credentials(oauth_token)
        self.service = None
        self.supabase = get_supabase_client()
        
    def _build_credentials(self, token: Dict) -> Credentials:
        """Build Google OAuth2 credentials from token dict."""
        try:
            return Credentials(
                token=token.get('access_token'),
                refresh_token=token.get('refresh_token'),
                token_uri='https://oauth2.googleapis.com/token',
                client_id=settings.GOOGLE_CLIENT_ID,
                client_secret=settings.GOOGLE_CLIENT_SECRET,
                scopes=['https://www.googleapis.com/auth/webmasters.readonly']
            )
        except Exception as e:
            raise GSCAuthError(f"Failed to build credentials: {str(e)}")
    
    async def initialize(self):
        """Initialize GSC API service."""
        try:
            self.service = build('searchconsole', 'v1', credentials=self.credentials)
            logger.info(f"GSC service initialized for property: {self.property_url}")
        except Exception as e:
            raise GSCAPIError(f"Failed to initialize GSC service: {str(e)}")
    
    def _generate_cache_key(self, request_params: Dict) -> str:
        """Generate cache key from request parameters."""
        # Create deterministic hash of params
        params_json = json.dumps(request_params, sort_keys=True)
        return hashlib.sha256(params_json.encode()).hexdigest()
    
    async def _get_cached_response(self, cache_key: str) -> Optional[Dict]:
        """Retrieve cached API response if not expired."""
        try:
            result = self.supabase.table('api_cache').select('*').eq('cache_key', cache_key).execute()
            
            if result.data and len(result.data) > 0:
                cached = result.data[0]
                expires_at = datetime.fromisoformat(cached['expires_at'].replace('Z', '+00:00'))
                
                if expires_at > datetime.now():
                    logger.info(f"Cache hit for key: {cache_key[:16]}...")
                    return cached['response']
                else:
                    logger.info(f"Cache expired for key: {cache_key[:16]}...")
            
            return None
        except Exception as e:
            logger.warning(f"Cache retrieval failed: {str(e)}")
            return None
    
    async def _cache_response(self, cache_key: str, response: Dict, user_id: str):
        """Cache API response with 24-hour TTL."""
        try:
            expires_at = datetime.now() + timedelta(hours=24)
            
            self.supabase.table('api_cache').upsert({
                'user_id': user_id,
                'cache_key': cache_key,
                'response': response,
                'expires_at': expires_at.isoformat()
            }).execute()
            
            logger.info(f"Cached response for key: {cache_key[:16]}...")
        except Exception as e:
            logger.warning(f"Cache storage failed: {str(e)}")
    
    def _execute_query(self, request_body: Dict) -> Dict:
        """Execute GSC API query with error handling."""
        try:
            response = self.service.searchanalytics().query(
                siteUrl=self.property_url,
                body=request_body
            ).execute()
            
            return response
        except HttpError as e:
            raise GSCAPIError(f"GSC API request failed: {str(e)}")
        except Exception as e:
            raise GSCAPIError(f"Unexpected error in GSC query: {str(e)}")
    
    async def _fetch_with_pagination(
        self,
        dimensions: List[str],
        start_date: str,
        end_date: str,
        filters: Optional[List[Dict]] = None,
        max_rows: int = 100000
    ) -> pd.DataFrame:
        """
        Fetch data with automatic pagination to handle 25K row limit.
        
        For queries > 25K rows, implements date-based chunking strategy.
        """
        all_rows = []
        start_row = 0
        
        # Build base request
        request_body = {
            'startDate': start_date,
            'endDate': end_date,
            'dimensions': dimensions,
            'rowLimit': min(self.DEFAULT_BATCH_SIZE, max_rows),
            'startRow': start_row
        }
        
        if filters:
            request_body['dimensionFilterGroups'] = [{'filters': filters}]
        
        # First request to determine if pagination needed
        response = self._execute_query(request_body)
        rows = response.get('rows', [])
        all_rows.extend(rows)
        
        # Check if we hit the limit (potential for more data)
        if len(rows) >= self.MAX_ROWS_PER_REQUEST and len(all_rows) < max_rows:
            logger.info(f"Hit 25K limit for {dimensions}, implementing date chunking...")
            
            # Re-fetch with monthly chunks
            return await self._fetch_with_date_chunks(
                dimensions=dimensions,
                start_date=start_date,
                end_date=end_date,
                filters=filters,
                max_rows=max_rows
            )
        
        # Simple pagination (under 25K total)
        while len(rows) == self.DEFAULT_BATCH_SIZE and len(all_rows) < max_rows:
            start_row += self.DEFAULT_BATCH_SIZE
            request_body['startRow'] = start_row
            
            response = self._execute_query(request_body)
            rows = response.get('rows', [])
            
            if not rows:
                break
                
            all_rows.extend(rows)
            logger.info(f"Fetched {len(all_rows)} total rows for {dimensions}")
        
        return self._rows_to_dataframe(all_rows, dimensions)
    
    async def _fetch_with_date_chunks(
        self,
        dimensions: List[str],
        start_date: str,
        end_date: str,
        filters: Optional[List[Dict]] = None,
        max_rows: int = 100000,
        chunk_days: int = 30
    ) -> pd.DataFrame:
        """
        Fetch data in date chunks to overcome 25K row limit.
        
        Splits date range into smaller chunks, fetches each, then merges.
        """
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        
        chunks = []
        current_start = start_dt
        
        while current_start < end_dt:
            current_end = min(current_start + timedelta(days=chunk_days), end_dt)
            
            chunk_start_str = current_start.strftime('%Y-%m-%d')
            chunk_end_str = current_end.strftime('%Y-%m-%d')
            
            logger.info(f"Fetching chunk: {chunk_start_str} to {chunk_end_str}")
            
            request_body = {
                'startDate': chunk_start_str,
                'endDate': chunk_end_str,
                'dimensions': dimensions,
                'rowLimit': self.DEFAULT_BATCH_SIZE
            }
            
            if filters:
                request_body['dimensionFilterGroups'] = [{'filters': filters}]
            
            response = self._execute_query(request_body)
            rows = response.get('rows', [])
            
            if rows:
                chunk_df = self._rows_to_dataframe(rows, dimensions)
                chunks.append(chunk_df)
            
            current_start = current_end + timedelta(days=1)
        
        if not chunks:
            return pd.DataFrame()
        
        # Merge all chunks and deduplicate
        merged_df = pd.concat(chunks, ignore_index=True)
        
        # Group by dimensions and sum metrics (handles any overlaps)
        dimension_cols = dimensions
        metric_cols = ['clicks', 'impressions', 'ctr', 'position']
        
        # For CTR and position, we need weighted averages
        merged_df['ctr_weighted'] = merged_df['ctr'] * merged_df['impressions']
        merged_df['position_weighted'] = merged_df['position'] * merged_df['impressions']
        
        grouped = merged_df.groupby(dimension_cols).agg({
            'clicks': 'sum',
            'impressions': 'sum',
            'ctr_weighted': 'sum',
            'position_weighted': 'sum'
        }).reset_index()
        
        # Recalculate weighted averages
        grouped['ctr'] = grouped['ctr_weighted'] / grouped['impressions']
        grouped['position'] = grouped['position_weighted'] / grouped['impressions']
        
        grouped = grouped.drop(['ctr_weighted', 'position_weighted'], axis=1)
        
        logger.info(f"Merged {len(chunks)} chunks into {len(grouped)} unique rows")
        
        return grouped.head(max_rows)
    
    def _rows_to_dataframe(self, rows: List[Dict], dimensions: List[str]) -> pd.DataFrame:
        """Convert GSC API response rows to pandas DataFrame."""
        if not rows:
            return pd.DataFrame()
        
        records = []
        for row in rows:
            record = {}
            
            # Extract dimension values
            keys = row.get('keys', [])
            for i, dim in enumerate(dimensions):
                record[dim] = keys[i] if i < len(keys) else None
            
            # Extract metrics
            record['clicks'] = row.get('clicks', 0)
            record['impressions'] = row.get('impressions', 0)
            record['ctr'] = row.get('ctr', 0.0)
            record['position'] = row.get('position', 0.0)
            
            records.append(record)
        
        return pd.DataFrame(records)
    
    def _get_date_range(self, lookback_days: Optional[int] = None) -> Tuple[str, str]:
        """Get date range for queries (default: 16 months back)."""
        end_date = datetime.now() - timedelta(days=3)  # GSC data has 2-3 day lag
        start_date = end_date - timedelta(days=lookback_days or self.DEFAULT_LOOKBACK_DAYS)
        
        return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')
    
    async def fetch_query_data(self, user_id: str) -> pd.DataFrame:
        """
        Fetch all query-level performance data.
        
        Returns DataFrame with columns: query, clicks, impressions, ctr, position
        """
        start_date, end_date = self._get_date_range()
        
        cache_key = self._generate_cache_key({
            'type': 'query',
            'property': self.property_url,
            'start': start_date,
            'end': end_date
        })
        
        cached = await self._get_cached_response(cache_key)
        if cached:
            return pd.DataFrame(cached)
        
        logger.info("Fetching query-level data from GSC...")
        df = await self._fetch_with_pagination(
            dimensions=['query'],
            start_date=start_date,
            end_date=end_date
        )
        
        await self._cache_response(cache_key, df.to_dict('records'), user_id)
        
        logger.info(f"Fetched {len(df)} unique queries")
        return df
    
    async def fetch_page_data(self, user_id: str) -> pd.DataFrame:
        """
        Fetch all page-level performance data.
        
        Returns DataFrame with columns: page, clicks, impressions, ctr, position
        """
        start_date, end_date = self._get_date_range()
        
        cache_key = self._generate_cache_key({
            'type': 'page',
            'property': self.property_url,
            'start': start_date,
            'end': end_date
        })
        
        cached = await self._get_cached_response(cache_key)
        if cached:
            return pd.DataFrame(cached)
        
        logger.info("Fetching page-level data from GSC...")
        df = await self._fetch_with_pagination(
            dimensions=['page'],
            start_date=start_date,
            end_date=end_date
        )
        
        await self._cache_response(cache_key, df.to_dict('records'), user_id)
        
        logger.info(f"Fetched {len(df)} unique pages")
        return df
    
    async def fetch_daily_data(self, user_id: str) -> pd.DataFrame:
        """
        Fetch daily time series data.
        
        Returns DataFrame with columns: date, clicks, impressions, ctr, position
        """
        start_date, end_date = self._get_date_range()
        
        cache_key = self._generate_cache_key({
            'type': 'daily',
            'property': self.property_url,
            'start': start_date,
            'end': end_date
        })
        
        cached = await self._get_cached_response(cache_key)
        if cached:
            return pd.DataFrame(cached)
        
        logger.info("Fetching daily time series from GSC...")
        df = await self._fetch_with_pagination(
            dimensions=['date'],
            start_date=start_date,
            end_date=end_date
        )
        
        # Convert date strings to datetime
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date')
        
        await self._cache_response(cache_key, df.to_dict('records'), user_id)
        
        logger.info(f"Fetched {len(df)} days of data")
        return df
    
    async def fetch_query_page_mapping(self, user_id: str) -> pd.DataFrame:
        """
        Fetch query+page mapping to understand which queries drive traffic to which pages.
        
        Returns DataFrame with columns: query, page, clicks, impressions, ctr, position
        """
        start_date, end_date = self._get_date_range()
        
        cache_key = self._generate_cache_key({
            'type': 'query_page',
            'property': self.property_url,
            'start': start_date,
            'end': end_date
        })
        
        cached = await self._get_cached_response(cache_key)
        if cached:
            return pd.DataFrame(cached)
        
        logger.info("Fetching query+page mapping from GSC...")
        df = await self._fetch_with_pagination(
            dimensions=['query', 'page'],
            start_date=start_date,
            end_date=end_date,
            max_rows=50000  # This can be huge, cap at 50K
        )
        
        await self._cache_response(cache_key, df.to_dict('records'), user_id)
        
        logger.info(f"Fetched {len(df)} query-page combinations")
        return df
    
    async def fetch_query_date_series(self, user_id: str, queries: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Fetch per-query daily time series.
        
        If queries list is provided, only fetch those queries.
        Otherwise, fetches top 100 queries by impressions.
        
        Returns DataFrame with columns: query, date, clicks, impressions, ctr, position
        """
        start_date, end_date = self._get_date_range()
        
        # If no specific queries provided, get top queries first
        if queries is None:
            query_data = await self.fetch_query_data(user_id)
            queries = query_data.nlargest(100, 'impressions')['query'].tolist()
            logger.info(f"Fetching time series for top {len(queries)} queries")
        
        cache_key = self._generate_cache_key({
            'type': 'query_date',
            'property': self.property_url,
            'queries': sorted(queries)[:10],  # Cache key based on first 10 queries
            'start': start_date,
            'end': end_date
        })
        
        cached = await self._get_cached_response(cache_key)
        if cached:
            return pd.DataFrame(cached)
        
        logger.info(f"Fetching daily time series for {len(queries)} queries...")
        
        # For specific queries, use filters
        filters = [{
            'dimension': 'query',
            'operator': 'equals',
            'expression': query
        } for query in queries]
        
        # Fetch in batches to avoid filter limits
        batch_size = 10
        all_dfs = []
        
        for i in range(0, len(queries), batch_size):
            batch_queries = queries[i:i+batch_size]
            batch_filters = filters[i:i+batch_size]
            
            logger.info(f"Fetching batch {i//batch_size + 1}/{(len(queries)-1)//batch_size + 1}")
            
            batch_df = await self._fetch_with_pagination(
                dimensions=['query', 'date'],
                start_date=start_date,
                end_date=end_date,
                filters=batch_filters
            )
            
            if not batch_df.empty:
                all_dfs.append(batch_df)
        
        if not all_dfs:
            return pd.DataFrame()
        
        df = pd.concat(all_dfs, ignore_index=True)
        
        # Convert date strings to datetime
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values(['query', 'date'])
        
        await self._cache_response(cache_key, df.to_dict('records'), user_id)
        
        logger.info(f"Fetched {len(df)} query-date data points")
        return df
    
    async def fetch_page_date_series(self, user_id: str, pages: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Fetch per-page daily time series.
        
        If pages list is provided, only fetch those pages.
        Otherwise, fetches top 100 pages by clicks.
        
        Returns DataFrame with columns: page, date, clicks, impressions, ctr, position
        """
        start_date, end_date = self._get_date_range()
        
        # If no specific pages provided, get top pages first
        if pages is None:
            page_data = await self.fetch_page_data(user_id)
            pages = page_data.nlargest(100, 'clicks')['page'].tolist()
            logger.info(f"Fetching time series for top {len(pages)} pages")
        
        cache_key = self._generate_cache_key({
            'type': 'page_date',
            'property': self.property_url,
            'pages': sorted(pages)[:10],  # Cache key based on first 10 pages
            'start': start_date,
            'end': end_date
        })
        
        cached = await self._get_cached_response(cache_key)
        if cached:
            return pd.DataFrame(cached)
        
        logger.info(f"Fetching daily time series for {len(pages)} pages...")
        
        # Fetch all page+date data (no filters, as we want all pages)
        df = await self._fetch_with_pagination(
            dimensions=['page', 'date'],
            start_date=start_date,
            end_date=end_date
        )
        
        # Filter to requested pages if specified
        if pages:
            df = df[df['page'].isin(pages)]
        
        # Convert date strings to datetime
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values(['page', 'date'])
        
        await self._cache_response(cache_key, df.to_dict('records'), user_id)
        
        logger.info(f"Fetched {len(df)} page-date data points")
        return df
    
    async def fetch_all_data(self, user_id: str) -> Dict[str, pd.DataFrame]:
        """
        Fetch all GSC data dimensions needed for comprehensive analysis.
        
        Returns dict with keys:
        - 'queries': Query-level aggregated data
        - 'pages': Page-level aggregated data
        - 'daily': Daily time series
        - 'query_page': Query-page mapping
        - 'query_date': Per-query time series (top 100 queries)
        - 'page_date': Per-page time series (top 100 pages)
        """
        logger.info("Starting comprehensive GSC data extraction...")
        
        # Fetch all dimensions concurrently
        results = await asyncio.gather(
            self.fetch_query_data(user_id),
            self.fetch_page