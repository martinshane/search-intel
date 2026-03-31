"""
Google Search Console data ingestion module.

Handles all GSC API interactions with:
- Six main data pull functions (by query, page, date, and combinations)
- Monthly chunking for pagination (GSC limits to 25K rows per request)
- Response caching in api_cache table with 24h TTL
- Automatic OAuth token refresh on expiry
- Helper utilities for date ranges and cache management
"""

import hashlib
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Literal

import httpx
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from ..database import supabase_client
from ..config import settings


# Type aliases for clarity
DimensionType = Literal["query", "page", "date", "country", "device"]
DateRange = tuple[datetime, datetime]


class GSCError(Exception):
    """Base exception for GSC-related errors."""
    pass


class GSCAuthError(GSCError):
    """Authentication/authorization error."""
    pass


class GSCRateLimitError(GSCError):
    """Rate limit exceeded."""
    pass


def _get_gsc_client(gsc_token: Dict):
    """
    Build authenticated GSC API client from stored OAuth token.
    
    Args:
        gsc_token: OAuth token dict from database (encrypted)
        
    Returns:
        Google Search Console service client
        
    Raises:
        GSCAuthError: If token is invalid or expired beyond refresh
    """
    try:
        creds = Credentials(
            token=gsc_token.get("access_token"),
            refresh_token=gsc_token.get("refresh_token"),
            token_uri="https://oauth2.googleapis.com/token",
            client_id=settings.GOOGLE_CLIENT_ID,
            client_secret=settings.GOOGLE_CLIENT_SECRET,
            scopes=["https://www.googleapis.com/auth/webmasters.readonly"]
        )
        
        service = build("searchconsole", "v1", credentials=creds, cache_discovery=False)
        return service
    except Exception as e:
        raise GSCAuthError(f"Failed to create GSC client: {str(e)}")


def _refresh_token_if_needed(user_id: str, gsc_token: Dict) -> Dict:
    """
    Check token expiry and refresh if needed. Update database with new token.
    
    Args:
        user_id: User ID for database update
        gsc_token: Current OAuth token dict
        
    Returns:
        Refreshed token dict (or original if still valid)
    """
    try:
        creds = Credentials(
            token=gsc_token.get("access_token"),
            refresh_token=gsc_token.get("refresh_token"),
            token_uri="https://oauth2.googleapis.com/token",
            client_id=settings.GOOGLE_CLIENT_ID,
            client_secret=settings.GOOGLE_CLIENT_SECRET,
            scopes=["https://www.googleapis.com/auth/webmasters.readonly"]
        )
        
        # Check if expired
        if creds.expired and creds.refresh_token:
            creds.refresh(httpx.Request())
            
            # Update token in database
            new_token = {
                "access_token": creds.token,
                "refresh_token": creds.refresh_token,
                "token_uri": creds.token_uri,
                "client_id": creds.client_id,
                "client_secret": creds.client_secret,
                "scopes": creds.scopes,
                "expiry": creds.expiry.isoformat() if creds.expiry else None
            }
            
            supabase_client.table("users").update(
                {"gsc_token": new_token}
            ).eq("id", user_id).execute()
            
            return new_token
        
        return gsc_token
        
    except Exception as e:
        raise GSCAuthError(f"Token refresh failed: {str(e)}")


def _generate_cache_key(site_url: str, dimensions: List[str], 
                       start_date: str, end_date: str, row_limit: int) -> str:
    """
    Generate deterministic cache key for API request parameters.
    
    Args:
        site_url: GSC property URL
        dimensions: List of dimension strings
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        row_limit: Row limit for the request
        
    Returns:
        SHA256 hash of normalized parameters
    """
    # Sort dimensions for deterministic ordering
    sorted_dims = sorted(dimensions)
    
    key_data = {
        "site_url": site_url,
        "dimensions": sorted_dims,
        "start_date": start_date,
        "end_date": end_date,
        "row_limit": row_limit
    }
    
    key_string = json.dumps(key_data, sort_keys=True)
    return hashlib.sha256(key_string.encode()).hexdigest()


def _get_cached_response(user_id: str, cache_key: str) -> Optional[Dict]:
    """
    Retrieve cached API response if exists and not expired.
    
    Args:
        user_id: User ID for cache lookup
        cache_key: Cache key from _generate_cache_key
        
    Returns:
        Cached response dict or None if not found/expired
    """
    try:
        result = supabase_client.table("api_cache").select("*").eq(
            "user_id", user_id
        ).eq("cache_key", cache_key).single().execute()
        
        if result.data:
            expires_at = datetime.fromisoformat(result.data["expires_at"].replace("Z", "+00:00"))
            if expires_at > datetime.utcnow():
                return result.data["response"]
            else:
                # Expired, delete it
                supabase_client.table("api_cache").delete().eq(
                    "id", result.data["id"]
                ).execute()
        
        return None
    except Exception:
        # Cache miss or error - continue without cache
        return None


def _set_cached_response(user_id: str, cache_key: str, response: Dict):
    """
    Store API response in cache with 24h TTL.
    
    Args:
        user_id: User ID for cache storage
        cache_key: Cache key from _generate_cache_key
        response: API response dict to cache
    """
    try:
        expires_at = datetime.utcnow() + timedelta(hours=24)
        
        # Upsert (insert or update)
        supabase_client.table("api_cache").upsert({
            "user_id": user_id,
            "cache_key": cache_key,
            "response": response,
            "expires_at": expires_at.isoformat()
        }).execute()
    except Exception as e:
        # Non-critical error - log but don't fail the request
        print(f"Cache write failed: {str(e)}")


def _split_into_monthly_chunks(start_date: datetime, end_date: datetime) -> List[DateRange]:
    """
    Split date range into monthly chunks for pagination.
    
    GSC limits to 25K rows per request. For large sites, we need to
    chunk by month and merge results.
    
    Args:
        start_date: Range start date
        end_date: Range end date
        
    Returns:
        List of (start, end) date tuples, each covering roughly one month
    """
    chunks = []
    current = start_date
    
    while current < end_date:
        # Calculate end of current month
        if current.month == 12:
            next_month = current.replace(year=current.year + 1, month=1, day=1)
        else:
            next_month = current.replace(month=current.month + 1, day=1)
        
        chunk_end = min(next_month - timedelta(days=1), end_date)
        chunks.append((current, chunk_end))
        
        current = next_month
    
    return chunks


def _make_gsc_request(
    service,
    site_url: str,
    dimensions: List[DimensionType],
    start_date: str,
    end_date: str,
    row_limit: int = 25000,
    start_row: int = 0
) -> Dict:
    """
    Make a single GSC API request.
    
    Args:
        service: Authenticated GSC service client
        site_url: GSC property URL
        dimensions: List of dimensions to group by
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        row_limit: Maximum rows to return (max 25000)
        start_row: Pagination offset
        
    Returns:
        API response dict
        
    Raises:
        GSCRateLimitError: If rate limit exceeded
        GSCError: For other API errors
    """
    try:
        request_body = {
            "startDate": start_date,
            "endDate": end_date,
            "dimensions": dimensions,
            "rowLimit": min(row_limit, 25000),
            "startRow": start_row
        }
        
        response = service.searchanalytics().query(
            siteUrl=site_url,
            body=request_body
        ).execute()
        
        return response
        
    except HttpError as e:
        if e.resp.status == 429:
            raise GSCRateLimitError("GSC API rate limit exceeded")
        elif e.resp.status in [401, 403]:
            raise GSCAuthError(f"GSC authorization error: {str(e)}")
        else:
            raise GSCError(f"GSC API error: {str(e)}")
    except Exception as e:
        raise GSCError(f"Unexpected GSC error: {str(e)}")


def _fetch_with_monthly_chunking(
    service,
    site_url: str,
    dimensions: List[DimensionType],
    start_date: datetime,
    end_date: datetime,
    row_limit: int = 25000
) -> List[Dict]:
    """
    Fetch GSC data with automatic monthly chunking for large date ranges.
    
    Splits request into monthly chunks, fetches each, and merges results.
    This handles sites that would exceed 25K row limit on a single request.
    
    Args:
        service: Authenticated GSC service client
        site_url: GSC property URL
        dimensions: List of dimensions to group by
        start_date: Range start date
        end_date: Range end date
        row_limit: Rows per chunk request
        
    Returns:
        List of all row dicts merged from chunks
    """
    chunks = _split_into_monthly_chunks(start_date, end_date)
    all_rows = []
    
    for chunk_start, chunk_end in chunks:
        start_str = chunk_start.strftime("%Y-%m-%d")
        end_str = chunk_end.strftime("%Y-%m-%d")
        
        # Fetch chunk with pagination within the chunk
        start_row = 0
        while True:
            response = _make_gsc_request(
                service=service,
                site_url=site_url,
                dimensions=dimensions,
                start_date=start_str,
                end_date=end_str,
                row_limit=row_limit,
                start_row=start_row
            )
            
            rows = response.get("rows", [])
            if not rows:
                break
            
            all_rows.extend(rows)
            
            # Check if there are more rows in this chunk
            if len(rows) < row_limit:
                break
            
            start_row += row_limit
    
    return all_rows


def fetch_performance_by_query(
    user_id: str,
    gsc_token: Dict,
    site_url: str,
    start_date: datetime,
    end_date: datetime,
    use_cache: bool = True
) -> List[Dict]:
    """
    Fetch GSC performance data grouped by query.
    
    Returns all queries with clicks, impressions, CTR, and position.
    Handles pagination via monthly chunking for sites with >25K queries.
    
    Args:
        user_id: User ID for cache and token refresh
        gsc_token: OAuth token dict from database
        site_url: GSC property URL (e.g., "https://example.com/")
        start_date: Range start date
        end_date: Range end date
        use_cache: Whether to use cached response if available
        
    Returns:
        List of dicts with structure:
        [
            {
                "keys": ["query text"],
                "clicks": 123,
                "impressions": 4567,
                "ctr": 0.0269,
                "position": 8.3
            },
            ...
        ]
        
    Raises:
        GSCError: On API or processing errors
    """
    # Generate cache key
    cache_key = _generate_cache_key(
        site_url=site_url,
        dimensions=["query"],
        start_date=start_date.strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d"),
        row_limit=25000
    )
    
    # Check cache
    if use_cache:
        cached = _get_cached_response(user_id, cache_key)
        if cached:
            return cached
    
    # Refresh token if needed
    gsc_token = _refresh_token_if_needed(user_id, gsc_token)
    
    # Build service client
    service = _get_gsc_client(gsc_token)
    
    # Fetch with chunking
    rows = _fetch_with_monthly_chunking(
        service=service,
        site_url=site_url,
        dimensions=["query"],
        start_date=start_date,
        end_date=end_date
    )
    
    # Cache response
    if use_cache:
        _set_cached_response(user_id, cache_key, rows)
    
    return rows


def fetch_performance_by_page(
    user_id: str,
    gsc_token: Dict,
    site_url: str,
    start_date: datetime,
    end_date: datetime,
    use_cache: bool = True
) -> List[Dict]:
    """
    Fetch GSC performance data grouped by page.
    
    Returns all pages with clicks, impressions, CTR, and position.
    Handles pagination via monthly chunking.
    
    Args:
        user_id: User ID for cache and token refresh
        gsc_token: OAuth token dict from database
        site_url: GSC property URL
        start_date: Range start date
        end_date: Range end date
        use_cache: Whether to use cached response if available
        
    Returns:
        List of dicts with structure:
        [
            {
                "keys": ["https://example.com/page"],
                "clicks": 456,
                "impressions": 8901,
                "ctr": 0.0512,
                "position": 5.2
            },
            ...
        ]
    """
    cache_key = _generate_cache_key(
        site_url=site_url,
        dimensions=["page"],
        start_date=start_date.strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d"),
        row_limit=25000
    )
    
    if use_cache:
        cached = _get_cached_response(user_id, cache_key)
        if cached:
            return cached
    
    gsc_token = _refresh_token_if_needed(user_id, gsc_token)
    service = _get_gsc_client(gsc_token)
    
    rows = _fetch_with_monthly_chunking(
        service=service,
        site_url=site_url,
        dimensions=["page"],
        start_date=start_date,
        end_date=end_date
    )
    
    if use_cache:
        _set_cached_response(user_id, cache_key, rows)
    
    return rows


def fetch_performance_by_date(
    user_id: str,
    gsc_token: Dict,
    site_url: str,
    start_date: datetime,
    end_date: datetime,
    use_cache: bool = True
) -> List[Dict]:
    """
    Fetch GSC performance data grouped by date.
    
    Returns daily time series with clicks, impressions, CTR, and position.
    Used for trend analysis, seasonality detection, and forecasting.
    
    Args:
        user_id: User ID for cache and token refresh
        gsc_token: OAuth token dict from database
        site_url: GSC property URL
        start_date: Range start date
        end_date: Range end date
        use_cache: Whether to use cached response if available
        
    Returns:
        List of dicts with structure:
        [
            {
                "keys": ["2025-01-15"],
                "clicks": 234,
                "impressions": 5678,
                "ctr": 0.0412,
                "position": 7.8
            },
            ...
        ]
    """
    cache_key = _generate_cache_key(
        site_url=site_url,
        dimensions=["date"],
        start_date=start_date.strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d"),
        row_limit=25000
    )
    
    if use_cache:
        cached = _get_cached_response(user_id, cache_key)
        if cached:
            return cached
    
    gsc_token = _refresh_token_if_needed(user_id, gsc_token)
    service = _get_gsc_client(gsc_token)
    
    rows = _fetch_with_monthly_chunking(
        service=service,
        site_url=site_url,
        dimensions=["date"],
        start_date=start_date,
        end_date=end_date
    )
    
    if use_cache:
        _set_cached_response(user_id, cache_key, rows)
    
    return rows


def fetch_performance_by_query_page(
    user_id: str,
    gsc_token: Dict,
    site_url: str,
    start_date: datetime,
    end_date: datetime,
    use_cache: bool = True
) -> List[Dict]:
    """
    Fetch GSC performance data grouped by query + page combination.
    
    Used to build query-page mapping for cannibalization detection.
    This shows which pages rank for which queries.
    
    Args:
        user_id: User ID for cache and token refresh
        gsc_token: OAuth token dict from database
        site_url: GSC property URL
        start_date: Range start date
        end_date: Range end date
        use_cache: Whether to use cached response if available
        
    Returns:
        List of dicts with structure:
        [
            {
                "keys": ["query text", "https://example.com/page"],
                "clicks": 45,
                "impressions": 890,
                "ctr": 0.0506,
                "position": 6.1
            },
            ...
        ]
    """
    cache_key = _generate_cache_key(
        site_url=site_url,
        dimensions=["query", "page"],
        start_date=start_date.strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d"),
        row_limit=25000
    )
    
    if use_cache:
        cached = _get_cached_response(user_id, cache_key)
        if cached:
            return cached
    
    gsc_token = _refresh_token_if_needed(user_id, gsc_token)
    service = _get_gsc_client(gsc_token)
    
    rows = _fetch_with_monthly_chunking(
        service=service,
        site_url=site_url,
        dimensions=["query", "page"],
        start_date=start_date,
        end_date=end_date
    )
    
    if use_cache:
        _set_cached_response(user_id, cache_key, rows)
    
    return rows


def fetch_performance_by_query_date(
    user_id: str,
    gsc_token: Dict,
    site_url: str,
    start_date: datetime,
    end_date: datetime,
    use_cache: bool = True
) -> List[Dict]:
    """
    Fetch GSC performance data grouped by query + date combination.
    
    Provides per-keyword time series for tracking individual query
    trajectory, detecting position changes, and identifying algorithm impacts.
    
    Args:
        user_id: User ID for cache and token refresh
        gsc_token: OAuth token dict from database
        site_url: GSC property URL
        start_date: Range start date
        end_date: Range end date
        use_cache: Whether to use cached response if available
        
    Returns:
        List of dicts with structure:
        [
            {
                "keys": ["query text", "2025-01-15"],
                "clicks": 12,
                "impressions": 234,
                "ctr": 0.0513,
                "position": 5.8
            },
            ...
        ]
    """
    cache_key = _generate_cache_key(
        site_url=site_url,
        dimensions=["query", "date"],
        start_date=start_date.strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d"),
        row_limit=25000
    )
    
    if use_cache:
        cached = _get_cached_response(user_id, cache_key)
        if cached:
            return cached
    
    gsc_token = _refresh_token_if_needed(user_id, gsc_token)
    service = _get_gsc_client(gsc_token)
    
    rows = _fetch_with_monthly_chunking(
        service=service,
        site_url=site_url,
        dimensions=["query", "date"],
        start_date=start_date,
        end_date=end_date
    )
    
    if use_cache:
        _set_cached_response(user_id, cache_key, rows)
    
    return rows


def fetch_performance_by_page_date(
    user_id: str,
    gsc_token: Dict,
    site_url: str,
    start_date: datetime,
    end_date: datetime,
    use_cache: bool = True
) -> List[Dict]:
    """
    Fetch GSC performance data grouped by page + date combination.
    
    Provides per-page time series for page-level trajectory analysis,
    decay detection, and correlation with content updates.
    
    Args:
        user_id: User ID for cache and token refresh
        gsc_token: OAuth token dict from database
        site_url: GSC property URL
        start_date: Range start date
        end_date: Range end date
        use_cache: Whether to use cached response if available
        
    Returns:
        List of dicts with structure:
        [
            {
                "keys": ["https://example.com/page", "2025-01-15"],
                "clicks": 34,
                "impressions": 678,
                "ctr": 0.0501,
                "position":