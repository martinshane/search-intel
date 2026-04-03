"""
Google Search Console data ingestion with retry logic and graceful error handling.

Fetches performance data from GSC API with automatic pagination, exponential backoff
retry logic, and graceful degradation when data is unavailable.
"""

import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import httpx
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd

logger = logging.getLogger(__name__)


class GSCIngestionError(Exception):
    """Base exception for GSC ingestion errors."""
    pass


class GSCAuthError(GSCIngestionError):
    """Authentication-related errors."""
    pass


class GSCQuotaError(GSCIngestionError):
    """API quota exceeded errors."""
    pass


class GSCClient:
    """
    Google Search Console API client with retry logic and error handling.
    """
    
    MAX_RETRIES = 3
    RETRY_DELAY_BASE = 2  # seconds
    MAX_ROWS_PER_REQUEST = 25000
    
    def __init__(self, credentials: Dict[str, Any]):
        """
        Initialize GSC client with OAuth credentials.
        
        Args:
            credentials: OAuth2 credentials dict with token, refresh_token, etc.
        
        Raises:
            GSCAuthError: If credentials are invalid or expired
        """
        try:
            creds = Credentials(
                token=credentials.get('token'),
                refresh_token=credentials.get('refresh_token'),
                token_uri=credentials.get('token_uri', 'https://oauth2.googleapis.com/token'),
                client_id=credentials.get('client_id'),
                client_secret=credentials.get('client_secret'),
                scopes=credentials.get('scopes', ['https://www.googleapis.com/auth/webmasters.readonly'])
            )
            
            self.service = build('searchconsole', 'v1', credentials=creds)
            logger.info("GSC client initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize GSC client: {str(e)}")
            raise GSCAuthError(f"Authentication failed: {str(e)}")
    
    def _retry_with_backoff(self, func, *args, **kwargs):
        """
        Execute a function with exponential backoff retry logic.
        
        Args:
            func: Function to execute
            *args, **kwargs: Arguments to pass to function
            
        Returns:
            Function result
            
        Raises:
            GSCQuotaError: If quota is exceeded
            GSCIngestionError: If all retries fail
        """
        last_error = None
        
        for attempt in range(self.MAX_RETRIES):
            try:
                return func(*args, **kwargs)
                
            except HttpError as e:
                last_error = e
                
                # Handle quota errors specially
                if e.resp.status == 429:
                    logger.warning(f"GSC API quota exceeded on attempt {attempt + 1}")
                    if attempt == self.MAX_RETRIES - 1:
                        raise GSCQuotaError("API quota exceeded. Please try again later.")
                
                # Don't retry auth errors
                elif e.resp.status in [401, 403]:
                    logger.error(f"GSC authentication error: {str(e)}")
                    raise GSCAuthError(f"Authentication failed: {str(e)}")
                
                # Retry on server errors and rate limits
                elif e.resp.status >= 500 or e.resp.status == 429:
                    delay = self.RETRY_DELAY_BASE ** attempt
                    logger.warning(
                        f"GSC API error (status {e.resp.status}) on attempt {attempt + 1}/{self.MAX_RETRIES}. "
                        f"Retrying in {delay}s..."
                    )
                    time.sleep(delay)
                    
                else:
                    # Don't retry client errors
                    logger.error(f"GSC API client error: {str(e)}")
                    raise GSCIngestionError(f"API request failed: {str(e)}")
                    
            except Exception as e:
                last_error = e
                logger.error(f"Unexpected error in GSC API call: {str(e)}")
                
                if attempt < self.MAX_RETRIES - 1:
                    delay = self.RETRY_DELAY_BASE ** attempt
                    logger.info(f"Retrying in {delay}s...")
                    time.sleep(delay)
                else:
                    raise GSCIngestionError(f"Failed after {self.MAX_RETRIES} attempts: {str(e)}")
        
        # If we exhausted retries
        raise GSCIngestionError(f"Failed after {self.MAX_RETRIES} attempts: {str(last_error)}")
    
    def fetch_performance_data(
        self,
        site_url: str,
        start_date: datetime,
        end_date: datetime,
        dimensions: List[str],
        row_limit: int = MAX_ROWS_PER_REQUEST,
        search_type: str = 'web'
    ) -> List[Dict[str, Any]]:
        """
        Fetch performance data from GSC with automatic pagination.
        
        Args:
            site_url: GSC property URL (e.g., 'sc-domain:example.com')
            start_date: Start date for data pull
            end_date: End date for data pull
            dimensions: List of dimensions (e.g., ['query', 'page', 'date'])
            row_limit: Maximum rows to fetch (will paginate if needed)
            search_type: Type of search ('web', 'image', 'video')
            
        Returns:
            List of row dicts with keys: keys (dimension values), clicks, impressions, ctr, position
            
        Raises:
            GSCIngestionError: If data fetch fails after retries
        """
        all_rows = []
        start_row = 0
        
        logger.info(
            f"Fetching GSC data for {site_url} from {start_date.date()} to {end_date.date()} "
            f"with dimensions: {dimensions}"
        )
        
        while True:
            request_body = {
                'startDate': start_date.strftime('%Y-%m-%d'),
                'endDate': end_date.strftime('%Y-%m-%d'),
                'dimensions': dimensions,
                'rowLimit': min(self.MAX_ROWS_PER_REQUEST, row_limit - len(all_rows)),
                'startRow': start_row,
                'searchType': search_type
            }
            
            try:
                # Execute request with retry logic
                response = self._retry_with_backoff(
                    lambda: self.service.searchanalytics().query(
                        siteUrl=site_url,
                        body=request_body
                    ).execute()
                )
                
                rows = response.get('rows', [])
                
                if not rows:
                    logger.info(f"No more rows returned. Total rows fetched: {len(all_rows)}")
                    break
                
                all_rows.extend(rows)
                logger.info(f"Fetched {len(rows)} rows (total: {len(all_rows)})")
                
                # Check if we've hit the row limit or there are no more rows
                if len(rows) < self.MAX_ROWS_PER_REQUEST or len(all_rows) >= row_limit:
                    break
                
                start_row += len(rows)
                
                # Small delay between pagination requests to be polite
                time.sleep(0.1)
                
            except GSCQuotaError:
                # Quota errors should bubble up
                raise
                
            except GSCIngestionError as e:
                # If we have some data, return it with a warning
                if all_rows:
                    logger.warning(
                        f"Pagination failed at row {start_row}, but returning {len(all_rows)} rows: {str(e)}"
                    )
                    break
                else:
                    # No data yet, re-raise
                    raise
        
        return all_rows
    
    def fetch_query_data(
        self,
        site_url: str,
        start_date: datetime,
        end_date: datetime,
        row_limit: int = MAX_ROWS_PER_REQUEST
    ) -> pd.DataFrame:
        """
        Fetch query-level performance data.
        
        Args:
            site_url: GSC property URL
            start_date: Start date
            end_date: End date
            row_limit: Maximum rows to return
            
        Returns:
            DataFrame with columns: query, clicks, impressions, ctr, position
        """
        try:
            rows = self.fetch_performance_data(
                site_url=site_url,
                start_date=start_date,
                end_date=end_date,
                dimensions=['query'],
                row_limit=row_limit
            )
            
            if not rows:
                logger.warning("No query data returned from GSC")
                return pd.DataFrame(columns=['query', 'clicks', 'impressions', 'ctr', 'position'])
            
            data = []
            for row in rows:
                data.append({
                    'query': row['keys'][0],
                    'clicks': row['clicks'],
                    'impressions': row['impressions'],
                    'ctr': row['ctr'],
                    'position': row['position']
                })
            
            df = pd.DataFrame(data)
            logger.info(f"Successfully fetched {len(df)} queries")
            return df
            
        except Exception as e:
            logger.error(f"Failed to fetch query data: {str(e)}")
            return pd.DataFrame(columns=['query', 'clicks', 'impressions', 'ctr', 'position'])
    
    def fetch_page_data(
        self,
        site_url: str,
        start_date: datetime,
        end_date: datetime,
        row_limit: int = MAX_ROWS_PER_REQUEST
    ) -> pd.DataFrame:
        """
        Fetch page-level performance data.
        
        Args:
            site_url: GSC property URL
            start_date: Start date
            end_date: End date
            row_limit: Maximum rows to return
            
        Returns:
            DataFrame with columns: page, clicks, impressions, ctr, position
        """
        try:
            rows = self.fetch_performance_data(
                site_url=site_url,
                start_date=start_date,
                end_date=end_date,
                dimensions=['page'],
                row_limit=row_limit
            )
            
            if not rows:
                logger.warning("No page data returned from GSC")
                return pd.DataFrame(columns=['page', 'clicks', 'impressions', 'ctr', 'position'])
            
            data = []
            for row in rows:
                data.append({
                    'page': row['keys'][0],
                    'clicks': row['clicks'],
                    'impressions': row['impressions'],
                    'ctr': row['ctr'],
                    'position': row['position']
                })
            
            df = pd.DataFrame(data)
            logger.info(f"Successfully fetched {len(df)} pages")
            return df
            
        except Exception as e:
            logger.error(f"Failed to fetch page data: {str(e)}")
            return pd.DataFrame(columns=['page', 'clicks', 'impressions', 'ctr', 'position'])
    
    def fetch_daily_data(
        self,
        site_url: str,
        start_date: datetime,
        end_date: datetime
    ) -> pd.DataFrame:
        """
        Fetch daily time series data.
        
        Args:
            site_url: GSC property URL
            start_date: Start date
            end_date: End date
            
        Returns:
            DataFrame with columns: date, clicks, impressions, ctr, position
        """
        try:
            rows = self.fetch_performance_data(
                site_url=site_url,
                start_date=start_date,
                end_date=end_date,
                dimensions=['date'],
                row_limit=100000  # Daily data shouldn't hit limits
            )
            
            if not rows:
                logger.warning("No daily data returned from GSC")
                return pd.DataFrame(columns=['date', 'clicks', 'impressions', 'ctr', 'position'])
            
            data = []
            for row in rows:
                data.append({
                    'date': pd.to_datetime(row['keys'][0]),
                    'clicks': row['clicks'],
                    'impressions': row['impressions'],
                    'ctr': row['ctr'],
                    'position': row['position']
                })
            
            df = pd.DataFrame(data)
            df = df.sort_values('date')
            logger.info(f"Successfully fetched {len(df)} days of data")
            return df
            
        except Exception as e:
            logger.error(f"Failed to fetch daily data: {str(e)}")
            return pd.DataFrame(columns=['date', 'clicks', 'impressions', 'ctr', 'position'])
    
    def fetch_query_page_data(
        self,
        site_url: str,
        start_date: datetime,
        end_date: datetime,
        row_limit: int = MAX_ROWS_PER_REQUEST
    ) -> pd.DataFrame:
        """
        Fetch query-page mapping data for cannibalization detection.
        
        Args:
            site_url: GSC property URL
            start_date: Start date
            end_date: End date
            row_limit: Maximum rows to return
            
        Returns:
            DataFrame with columns: query, page, clicks, impressions, ctr, position
        """
        try:
            rows = self.fetch_performance_data(
                site_url=site_url,
                start_date=start_date,
                end_date=end_date,
                dimensions=['query', 'page'],
                row_limit=row_limit
            )
            
            if not rows:
                logger.warning("No query-page data returned from GSC")
                return pd.DataFrame(columns=['query', 'page', 'clicks', 'impressions', 'ctr', 'position'])
            
            data = []
            for row in rows:
                data.append({
                    'query': row['keys'][0],
                    'page': row['keys'][1],
                    'clicks': row['clicks'],
                    'impressions': row['impressions'],
                    'ctr': row['ctr'],
                    'position': row['position']
                })
            
            df = pd.DataFrame(data)
            logger.info(f"Successfully fetched {len(df)} query-page combinations")
            return df
            
        except Exception as e:
            logger.error(f"Failed to fetch query-page data: {str(e)}")
            return pd.DataFrame(columns=['query', 'page', 'clicks', 'impressions', 'ctr', 'position'])
    
    def fetch_query_date_data(
        self,
        site_url: str,
        start_date: datetime,
        end_date: datetime,
        row_limit: int = MAX_ROWS_PER_REQUEST
    ) -> pd.DataFrame:
        """
        Fetch per-query time series data.
        
        Due to GSC row limits, this may need to be split into date chunks for large sites.
        
        Args:
            site_url: GSC property URL
            start_date: Start date
            end_date: End date
            row_limit: Maximum rows to return
            
        Returns:
            DataFrame with columns: query, date, clicks, impressions, ctr, position
        """
        all_data = []
        
        # For large date ranges, chunk by month to avoid hitting row limits
        current_start = start_date
        while current_start < end_date:
            current_end = min(
                current_start + timedelta(days=30),
                end_date
            )
            
            try:
                rows = self.fetch_performance_data(
                    site_url=site_url,
                    start_date=current_start,
                    end_date=current_end,
                    dimensions=['query', 'date'],
                    row_limit=row_limit
                )
                
                if rows:
                    for row in rows:
                        all_data.append({
                            'query': row['keys'][0],
                            'date': pd.to_datetime(row['keys'][1]),
                            'clicks': row['clicks'],
                            'impressions': row['impressions'],
                            'ctr': row['ctr'],
                            'position': row['position']
                        })
                
                logger.info(
                    f"Fetched query-date data for {current_start.date()} to {current_end.date()}: "
                    f"{len(rows) if rows else 0} rows"
                )
                
            except GSCIngestionError as e:
                logger.warning(
                    f"Failed to fetch query-date data for chunk {current_start.date()} to {current_end.date()}: {str(e)}"
                )
                # Continue with other chunks
            
            current_start = current_end + timedelta(days=1)
        
        if not all_data:
            logger.warning("No query-date data returned from GSC")
            return pd.DataFrame(columns=['query', 'date', 'clicks', 'impressions', 'ctr', 'position'])
        
        df = pd.DataFrame(all_data)
        df = df.sort_values(['query', 'date'])
        logger.info(f"Successfully fetched {len(df)} query-date combinations")
        return df
    
    def fetch_page_date_data(
        self,
        site_url: str,
        start_date: datetime,
        end_date: datetime,
        row_limit: int = MAX_ROWS_PER_REQUEST
    ) -> pd.DataFrame:
        """
        Fetch per-page time series data.
        
        Args:
            site_url: GSC property URL
            start_date: Start date
            end_date: End date
            row_limit: Maximum rows to return
            
        Returns:
            DataFrame with columns: page, date, clicks, impressions, ctr, position
        """
        all_data = []
        
        # Chunk by month for large sites
        current_start = start_date
        while current_start < end_date:
            current_end = min(
                current_start + timedelta(days=30),
                end_date
            )
            
            try:
                rows = self.fetch_performance_data(
                    site_url=site_url,
                    start_date=current_start,
                    end_date=current_end,
                    dimensions=['page', 'date'],
                    row_limit=row_limit
                )
                
                if rows:
                    for row in rows:
                        all_data.append({
                            'page': row['keys'][0],
                            'date': pd.to_datetime(row['keys'][1]),
                            'clicks': row['clicks'],
                            'impressions': row['impressions'],
                            'ctr': row['ctr'],
                            'position': row['position']
                        })
                
                logger.info(
                    f"Fetched page-date data for {current_start.date()} to {current_end.date()}: "
                    f"{len(rows) if rows else 0} rows"
                )
                
            except GSCIngestionError as e:
                logger.warning(
                    f"Failed to fetch page-date data for chunk {current_start.date()} to {current_end.date()}: {str(e)}"
                )
                # Continue with other chunks
            
            current_start = current_end + timedelta(days=1)
        
        if not all_data:
            logger.warning("No page-date data returned from GSC")
            return pd.DataFrame(columns=['page', 'date', 'clicks', 'impressions', 'ctr', 'position'])
        
        df = pd.DataFrame(all_data)
        df = df.sort_values(['page', 'date'])
        logger.info(f"Successfully fetched {len(df)} page-date combinations")
        return df
    
    def list_sites(self) -> List[str]:
        """
        List all GSC properties the user has access to.
        
        Returns:
            List of site URLs
            
        Raises:
            GSCIngestionError: If listing fails
        """
        try:
            response = self._retry_with_backoff(
                lambda: self.service.sites().list().execute()
            )
            
            sites = [site['siteUrl'] for site in response.get('siteEntry', [])]
            logger.info(f"Found {len(sites)} GSC properties")
            return sites
            
        except Exception as e:
            logger.error(f"Failed to list GSC sites: {str(e)}")
            raise GSCIngestionError(f"Failed to list sites: {str(e)}")

    def list_sitemaps(self, site_url: str) -> List[Dict[str, Any]]:
        """
        List all sitemaps submitted for a GSC property.

        Uses the Search Console API sitemaps resource to retrieve all
        submitted sitemaps along with their status, type, error/warning
        counts, and submission timestamps.

        Spec reference (supabase/spec.md — GSC Data Pull, item #3):
          "Sitemaps list"

        This data feeds Module 9 (Site Architecture) for sitemap
        coverage analysis and Module 2 (Page Triage) for identifying
        pages missing from sitemaps.

        Args:
            site_url: GSC property URL (e.g., 'sc-domain:example.com')

        Returns:
            List of sitemap dicts, each containing:
              - path: Sitemap URL
              - type: Sitemap type (sitemap, sitemapIndex, atom, rss, etc.)
              - lastSubmitted: ISO timestamp of last submission
              - lastDownloaded: ISO timestamp of last Google download
              - isPending: Whether the sitemap is pending processing
              - errors: Number of errors found
              - warnings: Number of warnings found
              - contents: List of content type breakdowns (type, submitted, indexed)

        Returns empty list on failure (graceful degradation).
        """
        try:
            response = self._retry_with_backoff(
                lambda: self.service.sitemaps().list(siteUrl=site_url).execute()
            )

            sitemaps_raw = response.get('sitemap', [])
            sitemaps = []
            for sm in sitemaps_raw:
                sitemap_entry = {
                    'path': sm.get('path', ''),
                    'type': sm.get('type', 'unknown'),
                    'lastSubmitted': sm.get('lastSubmitted'),
                    'lastDownloaded': sm.get('lastDownloaded'),
                    'isPending': sm.get('isPending', False),
                    'errors': int(sm.get('errors', 0)),
                    'warnings': int(sm.get('warnings', 0)),
                }

                # Content type breakdown (e.g., web pages, images, videos)
                contents = []
                for ct in sm.get('contents', []):
                    contents.append({
                        'type': ct.get('type', 'web'),
                        'submitted': int(ct.get('submitted', 0)),
                        'indexed': int(ct.get('indexed', 0)),
                    })
                sitemap_entry['contents'] = contents
                sitemaps.append(sitemap_entry)

            logger.info(
                "Found %d sitemaps for %s (total errors: %d, warnings: %d)",
                len(sitemaps), site_url,
                sum(s['errors'] for s in sitemaps),
                sum(s['warnings'] for s in sitemaps),
            )
            return sitemaps

        except HttpError as e:
            if e.resp.status in [401, 403]:
                logger.warning("Sitemaps API access denied for %s: %s", site_url, e)
            else:
                logger.warning("Failed to list sitemaps for %s: %s", site_url, e)
            return []
        except Exception as e:
            logger.warning("Failed to list sitemaps for %s: %s", site_url, e)
            return []

    def inspect_urls(
        self,
        site_url: str,
        urls: List[str],
        max_urls: int = 25,
    ) -> List[Dict[str, Any]]:
        """
        Inspect indexing status of URLs via the GSC URL Inspection API.

        Spec reference (supabase/spec.md — GSC Data Pull, item #2):
          "URL inspection (for indexing status on key pages)"

        The URL Inspection API provides per-URL details on:
          - Index coverage verdict (PASS, NEUTRAL, FAIL, etc.)
          - Crawl status (whether Googlebot can access the page)
          - Mobile usability
          - Rich results eligibility

        This data enriches Module 2 (Page Triage) — pages with indexing
        issues get higher triage priority — and Module 9 (Site
        Architecture) for identifying crawlability gaps.

        Rate limits: 600 inspections per property per day, 2000 total
        per day.  We cap at max_urls (default 25) to stay well within
        limits for a single report run.

        Args:
            site_url: GSC property URL (e.g., 'sc-domain:example.com')
            urls: List of page URLs to inspect
            max_urls: Maximum number of URLs to inspect (default 25)

        Returns:
            List of inspection result dicts, each containing:
              - url: The inspected URL
              - verdict: Overall index status verdict
              - coverage_state: Detailed coverage state
              - crawl_allowed: Whether robots.txt allows crawling
              - indexing_allowed: Whether the page allows indexing
              - page_fetch_state: HTTP status of Googlebot fetch
              - robots_txt_state: robots.txt blocking status
              - last_crawl_time: When Google last crawled the page
              - mobile_usability: Mobile-friendly verdict (if available)
              - rich_results: Rich result types detected (if available)
              - error: Error message if inspection failed for this URL

        Returns empty list on total failure (graceful degradation).
        """
        if not urls:
            return []

        # Cap to prevent quota exhaustion
        inspect_urls_list = urls[:max_urls]
        results = []

        logger.info(
            "Inspecting %d URLs for %s (of %d requested, max %d)",
            len(inspect_urls_list), site_url, len(urls), max_urls,
        )

        for url in inspect_urls_list:
            try:
                response = self._retry_with_backoff(
                    lambda u=url: self.service.urlInspection().index().inspect(
                        body={
                            'inspectionUrl': u,
                            'siteUrl': site_url,
                        }
                    ).execute()
                )

                inspection = response.get('inspectionResult', {})

                # Index status
                index_status = inspection.get('indexStatusResult', {})
                verdict = index_status.get('verdict', 'VERDICT_UNSPECIFIED')
                coverage_state = index_status.get('coverageState', '')
                robots_txt_state = index_status.get('robotsTxtState', '')
                indexing_state = index_status.get('indexingState', '')
                last_crawl_time = index_status.get('lastCrawlTime')
                page_fetch_state = index_status.get('pageFetchState', '')
                crawl_allowed = index_status.get('crawledAs', '') != ''

                # Mobile usability (may not be present)
                mobile_result = inspection.get('mobileUsabilityResult', {})
                mobile_verdict = mobile_result.get('verdict', 'VERDICT_UNSPECIFIED')

                # Rich results (may not be present)
                rich_result = inspection.get('richResultsResult', {})
                rich_types = []
                for item in rich_result.get('detectedItems', []):
                    rich_types.append(item.get('richResultType', 'unknown'))

                result_entry = {
                    'url': url,
                    'verdict': verdict,
                    'coverage_state': coverage_state,
                    'crawl_allowed': crawl_allowed,
                    'indexing_allowed': indexing_state != 'INDEXING_NOT_ALLOWED',
                    'page_fetch_state': page_fetch_state,
                    'robots_txt_state': robots_txt_state,
                    'last_crawl_time': last_crawl_time,
                    'mobile_usability': mobile_verdict,
                    'rich_results': rich_types,
                    'error': None,
                }

                results.append(result_entry)

                # Small delay between inspection calls to respect rate limits
                time.sleep(0.25)

            except HttpError as e:
                if e.resp.status in [401, 403]:
                    logger.warning(
                        "URL Inspection API access denied for %s — stopping inspection",
                        site_url,
                    )
                    results.append({
                        'url': url,
                        'verdict': 'ERROR',
                        'error': f'Access denied: {e}',
                    })
                    break
                elif e.resp.status == 429:
                    logger.warning("URL Inspection API rate limited — stopping")
                    break
                else:
                    logger.warning("URL inspection failed for %s: %s", url, e)
                    results.append({
                        'url': url,
                        'verdict': 'ERROR',
                        'error': str(e),
                    })
            except Exception as e:
                logger.warning("URL inspection failed for %s: %s", url, e)
                results.append({
                    'url': url,
                    'verdict': 'ERROR',
                    'error': str(e),
                })

        # Summary logging
        verdicts = {}
        for r in results:
            v = r.get('verdict', 'ERROR')
            verdicts[v] = verdicts.get(v, 0) + 1
        logger.info(
            "URL inspection complete for %s: %d inspected, verdicts: %s",
            site_url, len(results), verdicts,
        )

        return results


def ingest_gsc_data(
    credentials: Dict[str, Any],
    site_url: str,
    months_back: int = 16
) -> Dict[str, pd.DataFrame]:
    """
    Main ingestion function: fetch all required GSC data for analysis.
    
    Args:
        credentials: OAuth2 credentials dict
        site_url: GSC property URL
        months_back: Number of months of historical data to fetch
        
    Returns:
        Dict with keys:
            - daily: Daily time series
            - queries: Query-level aggregated data
            - pages: Page-level aggregated data
            - query_page: Query-page mapping
            - query_date: Per-query time series (may be empty for large sites)
            - page_date: Per-page time series
            - sitemaps: List of submitted sitemaps with status/errors
            - url_inspection: Indexing status for top 25 pages by clicks
            
    Raises:
        GSCIngestionError: If critical data cannot be fetched
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(days=months_back * 30)
    
    logger.info(f"Starting GSC ingestion for {site_url} from {start_date.date()} to {end_date.date()}")
    
    try:
        client = GSCClient(credentials)
    except GSCAuthError as e:
        logger.error(f"Authentication failed: {str(e)}")
        raise GSCIngestionError(
            "Unable to connect to Google Search Console. Please reconnect your account."
        )
    
    results = {}
    
    # Daily data is critical - fail if we can't get it
    try:
        results['daily'] = client.fetch_daily_data(site_url, start_date, end_date)
        if results['daily'].empty:
            raise GSCIngestionError("No daily data available. Please check that your site has search traffic.")
    except GSCQuotaError:
        raise GSCIngestionError(
            "Google Search Console API quota exceeded. Please try again in a few hours."
        )
    except Exception as e:
        raise GSCIngestionError(f"Failed to fetch daily data: {str(e)}")
    
    # Query data is critical
    try:
        results['queries'] = client.fetch_query_data(site_url, start_date, end_date)
        if results['queries'].empty:
            logger.warning("No query data available")
    except Exception as e:
        logger.warning(f"Failed to fetch query data: {str(e)}")
        results['queries'] = pd.DataFrame(columns=['query', 'clicks', 'impressions', 'ctr', 'position'])
    
    # Page data is critical
    try:
        results['pages'] = client.fetch_page_data(site_url, start_date, end_date)
        if results['pages'].empty:
            logger.warning("No page data available")
    except Exception as e:
        logger.warning(f"Failed to fetch page data: {str(e)}")
        results['pages'] = pd.DataFrame(columns=['page', 'clicks', 'impressions', 'ctr', 'position'])
    
    # Query-page mapping (for cannibalization detection)
    try:
        results['query_page'] = client.fetch_query_page_data(site_url, start_date, end_date)
    except Exception as e:
        logger.warning(f"Failed to fetch query-page data: {str(e)}")
        results['query_page'] = pd.DataFrame(columns=['query', 'page', 'clicks', 'impressions', 'ctr', 'position'])
    
    # Per-query time series (may be large, graceful degradation)
    try:
        results['query_date'] = client.fetch_query_date_data(site_url, start_date, end_date)
    except Exception as e:
        logger.warning(f"Failed to fetch query-date data: {str(e)}")
        results['query_date'] = pd.DataFrame(columns=['query', 'date', 'clicks', 'impressions', 'ctr', 'position'])
    
    # Per-page time series
    try:
        results['page_date'] = client.fetch_page_date_data(site_url, start_date, end_date)
    except Exception as e:
        logger.warning(f"Failed to fetch page-date data: {str(e)}")
        results['page_date'] = pd.DataFrame(columns=['page', 'date', 'clicks', 'impressions', 'ctr', 'position'])
    
    # Sitemaps list (spec item #3 — feeds Module 9 site architecture analysis)
    try:
        results['sitemaps'] = client.list_sitemaps(site_url)
    except Exception as e:
        logger.warning(f"Failed to list sitemaps: {str(e)}")
        results['sitemaps'] = []
    
    # URL inspection for top pages (spec item #2 — feeds Module 2 page triage)
    # Inspect the top 25 pages by clicks to check indexing status
    try:
        if 'pages' in results and not results['pages'].empty:
            top_pages = results['pages'].nlargest(25, 'clicks')['page'].tolist()
            results['url_inspection'] = client.inspect_urls(site_url, top_pages, max_urls=25)
        else:
            results['url_inspection'] = []
    except Exception as e:
        logger.warning(f"Failed to inspect URLs: {str(e)}")
        results['url_inspection'] = []
    
    total_rows = sum(len(v) if hasattr(v, '__len__') else 0 for v in results.values())
    logger.info(f"GSC ingestion complete for {site_url}: {total_rows} total rows across {len(results)} datasets")
    
    return results
