import os
import asyncio
import logging
from typing import List, Dict, Any, Optional, Set
from datetime import datetime, timedelta
import httpx
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)
import hashlib
import json

logger = logging.getLogger(__name__)


class DataForSEOError(Exception):
    """Base exception for DataForSEO API errors"""
    pass


class DataForSEORateLimitError(DataForSEOError):
    """Raised when rate limit is exceeded"""
    pass


class DataForSEOAuthError(DataForSEOError):
    """Raised when authentication fails"""
    pass


class DataForSEOClient:
    """
    Async client for DataForSEO API with rate limiting, retries, error handling, and caching.
    
    Supports:
    - Live SERP results retrieval
    - Competitor analysis (top ranking domains)
    - SERP feature detection (featured snippets, PAA, knowledge panels, etc.)
    - Batch processing of multiple keywords
    - Response caching via Supabase
    
    Example:
        >>> client = DataForSEOClient()
        >>> await client.authenticate()
        >>> results = await client.fetch_serp_results(
        ...     keywords=["best crm software"],
        ...     location_code=2840,
        ...     language_code="en"
        ... )
    """
    
    BASE_URL = "https://api.dataforseo.com/v3"
    
    # SERP feature type mappings
    SERP_FEATURE_TYPES = {
        "featured_snippet": ["featured_snippet"],
        "people_also_ask": ["people_also_ask"],
        "knowledge_graph": ["knowledge_graph"],
        "local_pack": ["local_pack", "map"],
        "video": ["video"],
        "image": ["images"],
        "shopping": ["shopping", "google_shopping"],
        "top_stories": ["top_stories"],
        "twitter": ["twitter"],
        "recipes": ["recipes"],
        "ai_overview": ["ai_overview"],
        "related_searches": ["people_also_search", "related_searches"],
    }
    
    def __init__(
        self,
        login: Optional[str] = None,
        password: Optional[str] = None,
        timeout: int = 60,
        max_retries: int = 3,
        supabase_client: Optional[Any] = None,
        cache_ttl_hours: int = 24,
        rate_limit_per_second: int = 2,
    ):
        """
        Initialize DataForSEO client.
        
        Args:
            login: DataForSEO login (defaults to DATAFORSEO_LOGIN env var)
            password: DataForSEO password (defaults to DATAFORSEO_PASSWORD env var)
            timeout: Request timeout in seconds
            max_retries: Maximum number of retry attempts
            supabase_client: Supabase client instance for caching
            cache_ttl_hours: Cache TTL in hours
            rate_limit_per_second: Maximum requests per second
        """
        self.login = login or os.getenv("DATAFORSEO_LOGIN")
        self.password = password or os.getenv("DATAFORSEO_PASSWORD")
        
        if not self.login or not self.password:
            raise ValueError(
                "DataForSEO credentials not provided. Set DATAFORSEO_LOGIN and "
                "DATAFORSEO_PASSWORD environment variables or pass credentials explicitly."
            )
        
        self.timeout = timeout
        self.max_retries = max_retries
        self.supabase = supabase_client
        self.cache_ttl_hours = cache_ttl_hours
        self.rate_limit_per_second = rate_limit_per_second
        
        # Rate limiting
        self._rate_limiter = asyncio.Semaphore(rate_limit_per_second)
        self._last_request_times: List[float] = []
        
        # HTTP client
        self._client: Optional[httpx.AsyncClient] = None
        self._authenticated = False
    
    async def __aenter__(self):
        """Async context manager entry"""
        await self.authenticate()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close()
    
    async def authenticate(self):
        """
        Initialize HTTP client with authentication.
        """
        if self._client is None:
            auth = httpx.BasicAuth(self.login, self.password)
            self._client = httpx.AsyncClient(
                auth=auth,
                timeout=httpx.Timeout(self.timeout),
                limits=httpx.Limits(max_keepalive_connections=5, max_connections=10),
            )
            self._authenticated = True
            logger.info("DataForSEO client authenticated successfully")
    
    async def close(self):
        """Close HTTP client"""
        if self._client:
            await self._client.aclose()
            self._client = None
            self._authenticated = False
            logger.info("DataForSEO client closed")
    
    def _generate_cache_key(self, endpoint: str, params: Dict[str, Any]) -> str:
        """Generate cache key from endpoint and parameters"""
        param_str = json.dumps(params, sort_keys=True)
        key_data = f"{endpoint}:{param_str}"
        return hashlib.sha256(key_data.encode()).hexdigest()
    
    async def _get_cached_response(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """Retrieve cached response from Supabase if available"""
        if not self.supabase:
            return None
        
        try:
            result = self.supabase.table("dataforseo_cache").select("*").eq(
                "cache_key", cache_key
            ).single().execute()
            
            if result.data:
                cached_at = datetime.fromisoformat(result.data["cached_at"])
                expiry = cached_at + timedelta(hours=self.cache_ttl_hours)
                
                if datetime.utcnow() < expiry:
                    logger.info(f"Cache hit for key: {cache_key}")
                    return result.data["response_data"]
                else:
                    logger.info(f"Cache expired for key: {cache_key}")
        except Exception as e:
            logger.warning(f"Cache retrieval failed: {e}")
        
        return None
    
    async def _cache_response(self, cache_key: str, response_data: Dict[str, Any]):
        """Cache response in Supabase"""
        if not self.supabase:
            return
        
        try:
            self.supabase.table("dataforseo_cache").upsert({
                "cache_key": cache_key,
                "response_data": response_data,
                "cached_at": datetime.utcnow().isoformat(),
            }).execute()
            logger.info(f"Response cached with key: {cache_key}")
        except Exception as e:
            logger.warning(f"Cache storage failed: {e}")
    
    async def _rate_limit(self):
        """Implement rate limiting"""
        async with self._rate_limiter:
            now = asyncio.get_event_loop().time()
            
            # Remove timestamps older than 1 second
            self._last_request_times = [
                t for t in self._last_request_times if now - t < 1.0
            ]
            
            # If we've hit the rate limit, wait
            if len(self._last_request_times) >= self.rate_limit_per_second:
                wait_time = 1.0 - (now - self._last_request_times[0])
                if wait_time > 0:
                    await asyncio.sleep(wait_time)
            
            self._last_request_times.append(now)
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.ConnectError)),
    )
    async def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
        use_cache: bool = True,
    ) -> Dict[str, Any]:
        """
        Make authenticated request to DataForSEO API with retries and rate limiting.
        
        Args:
            method: HTTP method (GET, POST)
            endpoint: API endpoint
            data: Request payload
            use_cache: Whether to use cached responses
            
        Returns:
            API response data
            
        Raises:
            DataForSEOAuthError: Authentication failed
            DataForSEORateLimitError: Rate limit exceeded
            DataForSEOError: Other API errors
        """
        if not self._authenticated or not self._client:
            await self.authenticate()
        
        # Check cache for POST requests with data
        cache_key = None
        if use_cache and method == "POST" and data:
            cache_key = self._generate_cache_key(endpoint, data)
            cached_response = await self._get_cached_response(cache_key)
            if cached_response:
                return cached_response
        
        # Apply rate limiting
        await self._rate_limit()
        
        url = f"{self.BASE_URL}/{endpoint}"
        
        try:
            if method == "GET":
                response = await self._client.get(url)
            elif method == "POST":
                response = await self._client.post(url, json=data)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            # Handle HTTP errors
            if response.status_code == 401:
                raise DataForSEOAuthError("Authentication failed. Check credentials.")
            elif response.status_code == 429:
                raise DataForSEORateLimitError("Rate limit exceeded")
            elif response.status_code >= 400:
                raise DataForSEOError(
                    f"API request failed with status {response.status_code}: {response.text}"
                )
            
            response_data = response.json()
            
            # DataForSEO wraps responses in status_code/status_message
            if response_data.get("status_code") != 20000:
                raise DataForSEOError(
                    f"API error: {response_data.get('status_message', 'Unknown error')}"
                )
            
            # Cache successful responses
            if cache_key and use_cache:
                await self._cache_response(cache_key, response_data)
            
            return response_data
        
        except httpx.HTTPError as e:
            logger.error(f"HTTP error during request to {url}: {e}")
            raise DataForSEOError(f"HTTP error: {e}") from e
    
    async def get_serp_results(
        self,
        keyword: str,
        location_code: int = 2840,
        language_code: str = "en",
        device: str = "desktop",
        depth: int = 100,
    ) -> Dict[str, Any]:
        """
        Get live SERP results for a single keyword.
        
        Args:
            keyword: Search query
            location_code: DataForSEO location code (2840 = United States)
            language_code: Language code (en, es, fr, etc.)
            device: Device type (desktop, mobile, tablet)
            depth: Number of results to retrieve (max 100)
            
        Returns:
            Parsed SERP data with organic results and features
        """
        payload = [{
            "keyword": keyword,
            "location_code": location_code,
            "language_code": language_code,
            "device": device,
            "depth": depth,
        }]
        
        response = await self._make_request(
            "POST",
            "serp/google/organic/live/advanced",
            data=payload,
        )
        
        if not response.get("tasks") or not response["tasks"][0].get("result"):
            logger.warning(f"No results for keyword: {keyword}")
            return self._empty_serp_result(keyword)
        
        task_result = response["tasks"][0]["result"][0]
        return self._parse_serp_result(keyword, task_result)
    
    async def get_batch_serp_results(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
        device: str = "desktop",
        depth: int = 100,
        batch_size: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Get SERP results for multiple keywords in batches.
        
        Args:
            keywords: List of search queries
            location_code: DataForSEO location code
            language_code: Language code
            device: Device type
            depth: Number of results per keyword
            batch_size: Number of keywords per API call
            
        Returns:
            List of parsed SERP results
        """
        results = []
        
        for i in range(0, len(keywords), batch_size):
            batch = keywords[i:i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}: {len(batch)} keywords")
            
            batch_tasks = [
                self.get_serp_results(
                    keyword=kw,
                    location_code=location_code,
                    language_code=language_code,
                    device=device,
                    depth=depth,
                )
                for kw in batch
            ]
            
            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            
            for keyword, result in zip(batch, batch_results):
                if isinstance(result, Exception):
                    logger.error(f"Error fetching SERP for '{keyword}': {result}")
                    results.append(self._empty_serp_result(keyword))
                else:
                    results.append(result)
        
        return results
    
    def _parse_serp_result(self, keyword: str, task_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse raw DataForSEO SERP result into structured format.
        
        Extracts:
        - Organic results with positions and URLs
        - SERP features (featured snippets, PAA, knowledge graph, etc.)
        - Competitor domains
        - Visual position adjustments
        """
        items = task_result.get("items", [])
        
        organic_results = []
        serp_features = {}
        competitors = set()
        
        for item in items:
            item_type = item.get("type", "")
            
            # Organic results
            if item_type == "organic":
                rank_group = item.get("rank_group", 0)
                rank_absolute = item.get("rank_absolute", 0)
                domain = item.get("domain", "")
                url = item.get("url", "")
                title = item.get("title", "")
                description = item.get("description", "")
                
                organic_results.append({
                    "position": rank_absolute,
                    "domain": domain,
                    "url": url,
                    "title": title,
                    "description": description,
                })
                
                if domain:
                    competitors.add(domain)
            
            # SERP features
            elif item_type == "featured_snippet":
                serp_features["featured_snippet"] = {
                    "present": True,
                    "domain": item.get("domain", ""),
                    "url": item.get("url", ""),
                    "title": item.get("title", ""),
                }
            
            elif item_type == "people_also_ask":
                if "people_also_ask" not in serp_features:
                    serp_features["people_also_ask"] = {
                        "present": True,
                        "count": 0,
                        "questions": [],
                    }
                
                paa_items = item.get("items", [])
                serp_features["people_also_ask"]["count"] = len(paa_items)
                serp_features["people_also_ask"]["questions"] = [
                    q.get("title", "") for q in paa_items
                ]
            
            elif item_type == "knowledge_graph":
                serp_features["knowledge_graph"] = {
                    "present": True,
                    "title": item.get("title", ""),
                    "description": item.get("description", ""),
                }
            
            elif item_type in ["local_pack", "map"]:
                serp_features["local_pack"] = {
                    "present": True,
                    "count": len(item.get("items", [])),
                }
            
            elif item_type == "video":
                serp_features["video"] = {
                    "present": True,
                    "count": len(item.get("items", [])),
                }
            
            elif item_type in ["images", "image"]:
                serp_features["image"] = {
                    "present": True,
                }
            
            elif item_type in ["shopping", "google_shopping"]:
                serp_features["shopping"] = {
                    "present": True,
                    "count": len(item.get("items", [])),
                }
            
            elif item_type == "top_stories":
                serp_features["top_stories"] = {
                    "present": True,
                    "count": len(item.get("items", [])),
                }
            
            elif item_type == "twitter":
                serp_features["twitter"] = {
                    "present": True,
                }
            
            elif item_type == "recipes":
                serp_features["recipes"] = {
                    "present": True,
                }
            
            elif item_type == "ai_overview":
                serp_features["ai_overview"] = {
                    "present": True,
                }
            
            elif item_type in ["people_also_search", "related_searches"]:
                if "related_searches" not in serp_features:
                    serp_features["related_searches"] = {
                        "present": True,
                        "queries": [],
                    }
                
                related_items = item.get("items", [])
                serp_features["related_searches"]["queries"].extend([
                    r.get("title", "") for r in related_items
                ])
        
        # Calculate visual position adjustment
        visual_adjustment = 0
        if serp_features.get("featured_snippet", {}).get("present"):
            visual_adjustment += 2
        if serp_features.get("people_also_ask", {}).get("present"):
            visual_adjustment += serp_features["people_also_ask"]["count"] * 0.5
        if serp_features.get("local_pack", {}).get("present"):
            visual_adjustment += 3
        if serp_features.get("knowledge_graph", {}).get("present"):
            visual_adjustment += 2
        if serp_features.get("video", {}).get("present"):
            visual_adjustment += 1
        if serp_features.get("shopping", {}).get("present"):
            visual_adjustment += 1
        if serp_features.get("ai_overview", {}).get("present"):
            visual_adjustment += 3
        
        return {
            "keyword": keyword,
            "organic_results": organic_results,
            "serp_features": serp_features,
            "competitors": list(competitors),
            "total_results": len(organic_results),
            "visual_position_adjustment": visual_adjustment,
            "fetched_at": datetime.utcnow().isoformat(),
        }
    
    def _empty_serp_result(self, keyword: str) -> Dict[str, Any]:
        """Return empty SERP result structure"""
        return {
            "keyword": keyword,
            "organic_results": [],
            "serp_features": {},
            "competitors": [],
            "total_results": 0,
            "visual_position_adjustment": 0,
            "fetched_at": datetime.utcnow().isoformat(),
            "error": "No results available",
        }
    
    async def get_rankings_data(
        self,
        domain: str,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
    ) -> Dict[str, Dict[str, Any]]:
        """
        Get ranking positions for a specific domain across multiple keywords.
        
        This is a convenience method that fetches SERP results and extracts
        the target domain's position.
        
        Args:
            domain: Target domain to track
            keywords: List of keywords to check
            location_code: DataForSEO location code
            language_code: Language code
            
        Returns:
            Dict mapping keywords to ranking data
        """
        serp_results = await self.get_batch_serp_results(
            keywords=keywords,
            location_code=location_code,
            language_code=language_code,
        )
        
        rankings = {}
        
        for result in serp_results:
            keyword = result["keyword"]
            position = None
            url = None
            
            for organic in result["organic_results"]:
                if domain in organic["domain"]:
                    position = organic["position"]
                    url = organic["url"]
                    break
            
            rankings[keyword] = {
                "position": position,
                "url": url,
                "visual_position": position + result["visual_position_adjustment"] if position else None,
                "serp_features": result["serp_features"],
                "competitors_in_top10": [
                    c for c in result["competitors"] if c != domain
                ][:10],
            }
        
        return rankings
    
    async def get_competitor_analysis(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
    ) -> Dict[str, Any]:
        """
        Analyze competitor domains across a keyword set.
        
        Returns:
        - Most frequent competitor domains
        - Average positions for each competitor
        - Keyword overlap with competitors
        
        Args:
            keywords: List of keywords to analyze
            location_code: DataForSEO location code
            language_code: Language code
            
        Returns:
            Competitor analysis data
        """
        serp_results = await self.get_batch_serp_results(
            keywords=keywords,
            location_code=location_code,
            language_code=language_code,
        )
        
        competitor_data = {}
        
        for result in serp_results:
            keyword = result["keyword"]
            
            for organic in result["organic_results"]:
                domain = organic["domain"]
                position = organic["position"]
                
                if domain not in competitor_data:
                    competitor_data[domain] = {
                        "domain": domain,
                        "keywords": [],
                        "positions": [],
                        "top_3_count": 0,
                        "top_10_count": 0,
                    }
                
                competitor_data[domain]["keywords"].append(keyword)
                competitor_data[domain]["positions"].append(position)
                
                if position <= 3:
                    competitor_data[domain]["top_3_count"] += 1
                if position <= 10:
                    competitor_data[domain]["top_10_count"] += 1
        
        # Calculate averages and sort
        competitors = []
        for domain, data in competitor_data.items():
            competitors.append({
                "domain": domain,
                "keyword_count": len(data["keywords"]),
                "avg_position": sum(data["positions"]) / len(data["positions"]),
                "top_3_count": data["top_3_count"],
                "top_10_count": data["top_10_count"],
                "keyword_overlap_pct": (len(data["keywords"]) / len(keywords)) * 100,
            })
        
        # Sort by keyword count descending
        competitors.sort(key=lambda x: x["keyword_count"], reverse=True)
        
        return {
            "total_keywords_analyzed": len(keywords),
            "total_competitors_found": len(competitors),
            "primary_competitors": competitors[:10],
            "all_competitors": competitors,
        }