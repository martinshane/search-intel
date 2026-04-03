"""
DataForSEO API client for Search Intelligence Report.

Handles SERP requests, competitor analysis, and ranking data retrieval
with authentication, rate limiting, error handling, and response parsing.
"""

import os
import time
import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
import hashlib
import json

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import backoff

logger = logging.getLogger(__name__)


class DataForSEOError(Exception):
    """Base exception for DataForSEO API errors."""
    pass


class DataForSEORateLimitError(DataForSEOError):
    """Raised when API rate limit is exceeded."""
    pass


class DataForSEOAuthError(DataForSEOError):
    """Raised when authentication fails."""
    pass


class DataForSEOClient:
    """
    Client for DataForSEO API with support for SERP requests,
    competitor analysis, and ranking data retrieval.
    
    Features:
    - Automatic retry with exponential backoff
    - Rate limiting (respects DataForSEO API limits)
    - Request caching to avoid redundant API calls
    - Comprehensive error handling
    - Response parsing and validation
    """
    
    BASE_URL = "https://api.dataforseo.com/v3"
    
    # Rate limits (requests per second)
    DEFAULT_RATE_LIMIT = 2  # Conservative default
    
    def __init__(
        self,
        login: Optional[str] = None,
        password: Optional[str] = None,
        rate_limit: Optional[float] = None,
        cache_ttl: int = 3600,
    ):
        """
        Initialize DataForSEO client.
        
        Args:
            login: DataForSEO login (defaults to DATAFORSEO_LOGIN env var)
            password: DataForSEO password (defaults to DATAFORSEO_PASSWORD env var)
            rate_limit: Max requests per second (defaults to DEFAULT_RATE_LIMIT)
            cache_ttl: Cache time-to-live in seconds
        """
        self.login = login or os.getenv("DATAFORSEO_LOGIN")
        self.password = password or os.getenv("DATAFORSEO_PASSWORD")
        
        if not self.login or not self.password:
            raise DataForSEOAuthError(
                "DataForSEO credentials not provided. "
                "Set DATAFORSEO_LOGIN and DATAFORSEO_PASSWORD environment variables."
            )
        
        self.rate_limit = rate_limit or self.DEFAULT_RATE_LIMIT
        self.cache_ttl = cache_ttl
        
        # Request tracking for rate limiting
        self._request_times: List[float] = []
        self._last_request_time = 0.0
        
        # Setup session with retry logic
        self.session = self._create_session()
        
        # In-memory cache for responses
        self._cache: Dict[str, Dict[str, Any]] = {}
        
        logger.info(f"DataForSEO client initialized with rate limit: {self.rate_limit} req/s")
    
    def _create_session(self) -> requests.Session:
        """Create requests session with retry configuration."""
        session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Set authentication
        session.auth = (self.login, self.password)
        
        return session
    
    def _wait_for_rate_limit(self):
        """Implement rate limiting by waiting if necessary."""
        current_time = time.time()
        
        # Remove requests older than 1 second
        self._request_times = [
            t for t in self._request_times 
            if current_time - t < 1.0
        ]
        
        # If we've hit the rate limit, wait
        if len(self._request_times) >= self.rate_limit:
            sleep_time = 1.0 - (current_time - self._request_times[0])
            if sleep_time > 0:
                logger.debug(f"Rate limit reached, sleeping {sleep_time:.2f}s")
                time.sleep(sleep_time)
        
        # Record this request time
        self._request_times.append(time.time())
    
    def _generate_cache_key(self, endpoint: str, payload: Dict[str, Any]) -> str:
        """Generate cache key from endpoint and payload."""
        payload_str = json.dumps(payload, sort_keys=True)
        key_str = f"{endpoint}:{payload_str}"
        return hashlib.md5(key_str.encode()).hexdigest()
    
    def _get_cached(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """Retrieve cached response if not expired."""
        if cache_key in self._cache:
            cached = self._cache[cache_key]
            if time.time() - cached["timestamp"] < self.cache_ttl:
                logger.debug(f"Cache hit for key: {cache_key}")
                return cached["data"]
            else:
                # Expired
                del self._cache[cache_key]
        return None
    
    def _set_cache(self, cache_key: str, data: Dict[str, Any]):
        """Store response in cache."""
        self._cache[cache_key] = {
            "timestamp": time.time(),
            "data": data
        }
    
    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.RequestException, DataForSEORateLimitError),
        max_tries=3,
        max_time=30
    )
    def _make_request(
        self,
        endpoint: str,
        payload: List[Dict[str, Any]],
        use_cache: bool = True
    ) -> Dict[str, Any]:
        """
        Make API request with rate limiting and caching.
        
        Args:
            endpoint: API endpoint path
            payload: Request payload (list of task objects)
            use_cache: Whether to use cached responses
            
        Returns:
            Parsed JSON response
            
        Raises:
            DataForSEOError: On API errors
            DataForSEORateLimitError: On rate limit exceeded
            DataForSEOAuthError: On authentication failure
        """
        url = f"{self.BASE_URL}/{endpoint}"
        
        # Check cache
        cache_key = self._generate_cache_key(endpoint, payload[0] if payload else {})
        if use_cache:
            cached = self._get_cached(cache_key)
            if cached:
                return cached
        
        # Wait for rate limit
        self._wait_for_rate_limit()
        
        # Make request
        try:
            response = self.session.post(url, json=payload)
            
            # Handle authentication errors
            if response.status_code == 401:
                raise DataForSEOAuthError("Authentication failed. Check credentials.")
            
            # Handle rate limit
            if response.status_code == 429:
                raise DataForSEORateLimitError("Rate limit exceeded")
            
            # Handle other errors
            response.raise_for_status()
            
            data = response.json()
            
            # Check DataForSEO API response status
            if data.get("status_code") != 20000:
                error_msg = data.get("status_message", "Unknown error")
                raise DataForSEOError(f"API error: {error_msg}")
            
            # Cache successful response
            if use_cache:
                self._set_cache(cache_key, data)
            
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise DataForSEOError(f"Request failed: {e}")
    
    def get_serp_data(
        self,
        keyword: str,
        location_name: str = "United States",
        language_code: str = "en",
        device: str = "desktop",
        depth: int = 100
    ) -> Dict[str, Any]:
        """
        Get live SERP data for a keyword.
        
        Args:
            keyword: Search query
            location_name: Geographic location for search
            language_code: Language code (e.g., 'en')
            device: Device type ('desktop', 'mobile')
            depth: Number of results to return (max 100)
            
        Returns:
            Normalized SERP data with structure:
            {
                "keyword": str,
                "location": str,
                "search_engine_domain": str,
                "organic_results": [
                    {
                        "position": int,
                        "url": str,
                        "domain": str,
                        "title": str,
                        "description": str,
                        "breadcrumb": str
                    }
                ],
                "serp_features": {
                    "featured_snippet": {...} or None,
                    "people_also_ask": [{...}],
                    "knowledge_panel": {...} or None,
                    "local_pack": [{...}],
                    "video_carousel": [{...}],
                    "image_pack": bool,
                    "shopping_results": bool,
                    "top_stories": bool,
                    "ai_overview": {...} or None,
                    "reddit_threads": [{...}]
                },
                "total_results": int,
                "timestamp": str
            }
        """
        payload = [{
            "keyword": keyword,
            "location_name": location_name,
            "language_code": language_code,
            "device": device,
            "os": "windows" if device == "desktop" else "ios",
            "depth": depth
        }]
        
        response = self._make_request("serp/google/organic/live/advanced", payload)
        
        # Parse response
        return self._parse_serp_response(response)
    
    def _parse_serp_response(self, response: Dict[str, Any]) -> Dict[str, Any]:
        """Parse and normalize SERP API response."""
        if not response.get("tasks") or not response["tasks"][0].get("result"):
            raise DataForSEOError("Invalid SERP response structure")
        
        task_result = response["tasks"][0]["result"][0]
        items = task_result.get("items", [])
        
        # Extract organic results
        organic_results = []
        serp_features = {
            "featured_snippet": None,
            "people_also_ask": [],
            "knowledge_panel": None,
            "local_pack": [],
            "video_carousel": [],
            "image_pack": False,
            "shopping_results": False,
            "top_stories": False,
            "ai_overview": None,
            "reddit_threads": []
        }
        
        for item in items:
            item_type = item.get("type")
            
            if item_type == "organic":
                organic_results.append({
                    "position": item.get("rank_absolute", 0),
                    "url": item.get("url", ""),
                    "domain": item.get("domain", ""),
                    "title": item.get("title", ""),
                    "description": item.get("description", ""),
                    "breadcrumb": item.get("breadcrumb", "")
                })
            
            elif item_type == "featured_snippet":
                serp_features["featured_snippet"] = {
                    "url": item.get("url", ""),
                    "domain": item.get("domain", ""),
                    "title": item.get("title", ""),
                    "description": item.get("description", "")
                }
            
            elif item_type == "people_also_ask":
                serp_features["people_also_ask"].append({
                    "question": item.get("title", ""),
                    "answer": item.get("expanded_element", {}).get("description", "")
                })
            
            elif item_type == "knowledge_panel":
                serp_features["knowledge_panel"] = {
                    "title": item.get("title", ""),
                    "description": item.get("description", "")
                }
            
            elif item_type == "local_pack":
                for local_item in item.get("items", []):
                    serp_features["local_pack"].append({
                        "title": local_item.get("title", ""),
                        "domain": local_item.get("domain", "")
                    })
            
            elif item_type == "video":
                for video_item in item.get("items", []):
                    serp_features["video_carousel"].append({
                        "title": video_item.get("title", ""),
                        "url": video_item.get("url", ""),
                        "source": video_item.get("source", "")
                    })
            
            elif item_type == "images":
                serp_features["image_pack"] = True
            
            elif item_type == "shopping":
                serp_features["shopping_results"] = True
            
            elif item_type == "top_stories":
                serp_features["top_stories"] = True
            
            elif item_type == "ai_overview":
                serp_features["ai_overview"] = {
                    "text": item.get("text", ""),
                    "sources": [s.get("url") for s in item.get("sources", [])]
                }
            
            elif item_type == "organic" and "reddit.com" in item.get("domain", ""):
                serp_features["reddit_threads"].append({
                    "title": item.get("title", ""),
                    "url": item.get("url", "")
                })
        
        return {
            "keyword": task_result.get("keyword", ""),
            "location": task_result.get("location_code", 0),
            "search_engine_domain": task_result.get("se_domain", ""),
            "organic_results": organic_results,
            "serp_features": serp_features,
            "total_results": task_result.get("se_results_count", 0),
            "timestamp": datetime.utcnow().isoformat()
        }
    
    def get_serp_data_batch(
        self,
        keywords: List[str],
        location_name: str = "United States",
        language_code: str = "en",
        device: str = "desktop"
    ) -> List[Dict[str, Any]]:
        """
        Get SERP data for multiple keywords in batch.
        
        Args:
            keywords: List of search queries
            location_name: Geographic location
            language_code: Language code
            device: Device type
            
        Returns:
            List of normalized SERP data dictionaries
        """
        results = []
        
        for keyword in keywords:
            try:
                serp_data = self.get_serp_data(
                    keyword=keyword,
                    location_name=location_name,
                    language_code=language_code,
                    device=device
                )
                results.append(serp_data)
            except Exception as e:
                logger.error(f"Failed to fetch SERP data for '{keyword}': {e}")
                results.append({
                    "keyword": keyword,
                    "error": str(e),
                    "timestamp": datetime.utcnow().isoformat()
                })
        
        return results
    
    def get_keyword_data(
        self,
        keywords: List[str],
        location_name: str = "United States",
        language_code: str = "en"
    ) -> List[Dict[str, Any]]:
        """
        Get keyword metrics (search volume, CPC, competition).
        
        Args:
            keywords: List of keywords to analyze
            location_name: Geographic location
            language_code: Language code
            
        Returns:
            List of keyword data with structure:
            [{
                "keyword": str,
                "search_volume": int,
                "cpc": float,
                "competition": float,  # 0-1
                "monthly_searches": [{
                    "month": int,
                    "year": int,
                    "search_volume": int
                }]
            }]
        """
        payload = [{
            "keywords": keywords,
            "location_name": location_name,
            "language_code": language_code
        }]
        
        response = self._make_request(
            "keywords_data/google_ads/search_volume/live",
            payload
        )
        
        return self._parse_keyword_data_response(response)
    
    def _parse_keyword_data_response(self, response: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parse and normalize keyword data response."""
        if not response.get("tasks") or not response["tasks"][0].get("result"):
            raise DataForSEOError("Invalid keyword data response structure")
        
        results = []
        for item in response["tasks"][0]["result"]:
            results.append({
                "keyword": item.get("keyword", ""),
                "search_volume": item.get("search_volume", 0),
                "cpc": item.get("cpc", 0.0),
                "competition": item.get("competition", 0.0),
                "monthly_searches": item.get("monthly_searches", [])
            })
        
        return results
    
    def get_competitor_domains(
        self,
        target_domain: str,
        location_name: str = "United States",
        language_code: str = "en",
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Get competitor domains based on keyword overlap.
        
        Args:
            target_domain: Domain to analyze
            location_name: Geographic location
            language_code: Language code
            limit: Maximum number of competitors to return
            
        Returns:
            List of competitor data with structure:
            [{
                "domain": str,
                "avg_position": float,
                "sum_position": int,
                "intersections": int,
                "etv": float,  # Estimated traffic value
                "keywords_count": int
            }]
        """
        payload = [{
            "target": target_domain,
            "location_name": location_name,
            "language_code": language_code,
            "limit": limit
        }]
        
        response = self._make_request(
            "dataforseo_labs/google/competitors_domain/live",
            payload
        )
        
        return self._parse_competitor_response(response)
    
    def _parse_competitor_response(self, response: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parse and normalize competitor domain response."""
        if not response.get("tasks") or not response["tasks"][0].get("result"):
            raise DataForSEOError("Invalid competitor response structure")
        
        items = response["tasks"][0]["result"][0].get("items", [])
        
        competitors = []
        for item in items:
            competitors.append({
                "domain": item.get("domain", ""),
                "avg_position": item.get("avg_position", 0.0),
                "sum_position": item.get("sum_position", 0),
                "intersections": item.get("intersections", 0),
                "etv": item.get("etv", 0.0),
                "keywords_count": item.get("keywords_count", 0)
            })
        
        return competitors
    
    def get_backlink_summary(
        self,
        target: str,
        include_subdomains: bool = True
    ) -> Dict[str, Any]:
        """
        Get backlink summary for a domain or URL.
        
        Args:
            target: Domain or URL to analyze
            include_subdomains: Include subdomain backlinks
            
        Returns:
            Backlink summary with structure:
            {
                "target": str,
                "backlinks": int,
                "referring_domains": int,
                "referring_main_domains": int,
                "referring_ips": int,
                "dofollow_links": int,
                "nofollow_links": int,
                "broken_backlinks": int,
                "broken_pages": int,
                "rank": int  # Domain rank
            }
        """
        payload = [{
            "target": target,
            "include_subdomains": include_subdomains
        }]
        
        response = self._make_request(
            "backlinks/summary/live",
            payload
        )
        
        return self._parse_backlink_summary_response(response)
    
    def _parse_backlink_summary_response(self, response: Dict[str, Any]) -> Dict[str, Any]:
        """Parse and normalize backlink summary response."""
        if not response.get("tasks") or not response["tasks"][0].get("result"):
            raise DataForSEOError("Invalid backlink summary response structure")
        
        item = response["tasks"][0]["result"][0]
        
        return {
            "target": item.get("target", ""),
            "backlinks": item.get("backlinks", 0),
            "referring_domains": item.get("referring_domains", 0),
            "referring_main_domains": item.get("referring_main_domains", 0),
            "referring_ips": item.get("referring_ips", 0),
            "dofollow_links": item.get("backlinks_dofollow", 0),
            "nofollow_links": item.get("backlinks_nofollow", 0),
            "broken_backlinks": item.get("broken_backlinks", 0),
            "broken_pages": item.get("broken_pages", 0),
            "rank": item.get("rank", 0)
        }
    
    def get_referring_domains(
        self,
        target: str,
        limit: int = 100,
        order_by: str = "rank",
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Get list of referring domains with backlink metrics.
        
        Args:
            target: Domain or URL to analyze
            limit: Maximum number of domains to return
            order_by: Sort field ('rank', 'backlinks', 'domain_from')
            filters: Additional filters (e.g., minimum rank, dofollow only)
            
        Returns:
            List of referring domain data with structure:
            [{
                "domain": str,
                "backlinks": int,
                "dofollow_links": int,
                "rank": int,
                "first_seen": str,
                "last_seen": str
            }]
        """
        payload = [{
            "target": target,
            "limit": limit,
            "order_by": [f"{order_by},desc"]
        }]
        
        if filters:
            payload[0]["filters"] = filters
        
        response = self._make_request(
            "backlinks/referring_domains/live",
            payload
        )
        
        return self._parse_referring_domains_response(response)
    
    def _parse_referring_domains_response(self, response: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parse and normalize referring domains response."""
        if not response.get("tasks") or not response["tasks"][0].get("result"):
            raise DataForSEOError("Invalid referring domains response structure")
        
        items = response["tasks"][0]["result"][0].get("items", [])
        
        domains = []
        for item in items:
            domains.append({
                "domain": item.get("domain_from", ""),
                "backlinks": item.get("backlinks", 0),
                "dofollow_links": item.get("dofollow", 0),
                "rank": item.get("rank", 0),
                "first_seen": item.get("first_seen", ""),
                "last_seen": item.get("last_seen", "")
            })
        
        return domains
    
    def get_onpage_summary(
        self,
        target: str,
        max_crawl_pages: int = 100
    ) -> Dict[str, Any]:
        """
        Get on-page SEO summary for a website.
        
        Note: This requires creating a crawl task and waiting for completion.
        For production use, implement async task handling.
        
        Args:
            target: Target website URL
            max_crawl_pages: Maximum pages to crawl
            
        Returns:
            On-page summary with issues and recommendations
        """
        # Create crawl task
        payload = [{
            "target": target,
            "max_crawl_pages": max_crawl_pages,
            "load_resources": True,
            "enable_javascript": True,
            "enable_browser_rendering": False,
            "store_raw_html": False
        }]
        
        response = self._make_request(
            "on_page/task_post",
            payload,
            use_cache=False
        )
        
        if not response.get("tasks") or not response["tasks"][0].get("result"):
            raise DataForSEOError("Failed to create on-page crawl task")
        
        task_id = response["tasks"][0]["id"]
        
        # Wait for task completion (simplified - should be async in production)
        max_wait = 300  # 5 minutes
        wait_interval = 10
        elapsed = 0
        
        while elapsed < max_wait:
            time.sleep(wait_interval)
            elapsed += wait_interval
            
            summary = self._get_onpage_summary_result(task_id)
            if summary:
                return summary
        
        raise DataForSEOError("On-page crawl task timeout")
    
    def _get_onpage_summary_result(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Get on-page crawl task results."""
        payload = [{
            "id": task_id
        }]
        
        try:
            response = self._make_request(
                "on_page/summary",
                payload,
                use_cache=False
            )
            
            if not response.get("tasks") or not response["tasks"][0].get("result"):
                return None
            
            result = response["tasks"][0]["result"][0]
            
            if result.get("crawl_progress") != "finished":
                return None
            
            return {
                "target": result.get("target", ""),
                "pages_crawled": result.get("crawl_status", {}).get("pages_crawled", 0),
                "pages_in_queue": result.get("crawl_status", {}).get("pages_in_queue", 0),
                "onpage_score": result.get("onpage_score", 0),
                "checks": result.get("checks", {}),
                "total_warnings": result.get("total_warnings", 0),
                "total_errors": result.get("total_errors", 0),
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.debug(f"Task not ready: {e}")
            return None
    
    def get_ranking_keywords(
        self,
        target: str,
        location_name: str = "United States",
        language_code: str = "en",
        limit: int = 1000,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Get all ranking keywords for a domain.
        
        Args:
            target: Target domain
            location_name: Geographic location
            language_code: Language code
            limit: Maximum keywords to return
            filters: Additional filters (e.g., position range)
            
        Returns:
            List of ranking keywords with structure:
            [{
                "keyword": str,
                "position": int,
                "previous_position": int,
                "search_volume": int,
                "cpc": float,
                "competition": float,
                "url": str,
                "etv": float
            }]
        """
        payload = [{
            "target": target,
            "location_name": location_name,
            "language_code": language_code,
            "limit": limit
        }]
        
        if filters:
            payload[0]["filters"] = filters
        
        response = self._make_request(
            "dataforseo_labs/google/ranked_keywords/live",
            payload
        )
        
        return self._parse_ranking_keywords_response(response)
    
    def _parse_ranking_keywords_response(self, response: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parse and normalize ranking keywords response."""
        if not response.get("tasks") or not response["tasks"][0].get("result"):
            raise DataForSEOError("Invalid ranking keywords response structure")
        
        items = response["tasks"][0]["result"][0].get("items", [])
        
        keywords = []
        for item in items:
            keywords.append({
                "keyword": item.get("keyword", ""),
                "position": item.get("se_results_position", 0),
                "previous_position": item.get("previous_se_results_position", 0),
                "search_volume": item.get("search_volume", 0),
                "cpc": item.get("cpc", 0.0),
                "competition": item.get("competition", 0.0),
                "url": item.get("ranked_serp_element", {}).get("url", ""),
                "etv": item.get("etv", 0.0)
            })
        
        return keywords
    
    def get_serp_competitors(
        self,
        keywords: List[str],
        location_name: str = "United States",
        language_code: str = "en"
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Analyze competitor presence across multiple keywords.
        
        Args:
            keywords: List of keywords to analyze
            location_name: Geographic location
            language_code: Language code
            
        Returns:
            Dictionary mapping keywords to competitor lists:
            {
                "keyword": [{
                    "domain": str,
                    "position": int,
                    "url": str,
                    "title": str
                }]
            }
        """
        results = {}
        
        serp_batch = self.get_serp_data_batch(
            keywords=keywords,
            location_name=location_name,
            language_code=language_code
        )
        
        for serp_data in serp_batch:
            if "error" in serp_data:
                continue
            
            keyword = serp_data["keyword"]
            competitors = []
            
            for result in serp_data.get("organic_results", []):
                competitors.append({
                    "domain": result["domain"],
                    "position": result["position"],
                    "url": result["url"],
                    "title": result["title"]
                })
            
            results[keyword] = competitors
        
        return results
    
    def calculate_visual_position(self, serp_data: Dict[str, Any], target_url: str) -> Tuple[int, int]:
        """
        Calculate organic rank vs visual position accounting for SERP features.
        
        Args:
            serp_data: Normalized SERP data from get_serp_data()
            target_url: URL to find position for
            
        Returns:
            Tuple of (organic_position, visual_position)
            Returns (0, 0) if URL not found
        """
        organic_position = 0
        
        # Find organic position
        for result in serp_data.get("organic_results", []):
            if target_url in result["url"] or result["url"] in target_url:
                organic_position = result["position"]
                break
        
        if organic_position == 0:
            return (0, 0)
        
        # Calculate visual displacement from SERP features
        visual_offset = 0.0
        features = serp_data.get("serp_features", {})
        
        # Featured snippet = 2 positions
        if features.get("featured_snippet"):
            visual_offset += 2.0
        
        # Each PAA question = 0.5 positions
        paa_count = len(features.get("people_also_ask", []))
        visual_offset += paa_count * 0.5
        
        # Knowledge panel = 1 position
        if features.get("knowledge_panel"):
            visual_offset += 1.0
        
        # Local pack = 1.5 positions
        if features.get("local_pack"):
            visual_offset += 1.5
        
        # Video carousel = 1 position
        if features.get("video_carousel"):
            visual_offset += 1.0
        
        # Image pack = 0.5 positions
        if features.get("image_pack"):
            visual_offset += 0.5
        
        # Shopping results = 1.5 positions
        if features.get("shopping_results"):
            visual_offset += 1.5
        
        # Top stories = 1 position
        if features.get("top_stories"):
            visual_offset += 1.0
        
        # AI Overview = 2.5 positions
        if features.get("ai_overview"):
            visual_offset += 2.5
        
        visual_position = int(organic_position + visual_offset)
        
        return (organic_position, visual_position)
    
    def close(self):
        """Close the session and clean up resources."""
        if self.session:
            self.session.close()
        
        logger.info("DataForSEO client closed")
