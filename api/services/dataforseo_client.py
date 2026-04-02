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
                "DATAFORSEO_PASSWORD environment variables or pass them to constructor."
            )
        
        self.timeout = timeout
        self.max_retries = max_retries
        self.supabase = supabase_client
        self.cache_ttl_hours = cache_ttl_hours
        self.rate_limit_per_second = rate_limit_per_second
        
        self._client: Optional[httpx.AsyncClient] = None
        self._rate_limiter = asyncio.Semaphore(rate_limit_per_second)
        self._last_request_time = 0.0
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
                timeout=self.timeout,
                headers={"Content-Type": "application/json"},
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
        """
        Generate cache key for request.
        
        Args:
            endpoint: API endpoint
            params: Request parameters
            
        Returns:
            MD5 hash of endpoint + params
        """
        cache_data = f"{endpoint}:{json.dumps(params, sort_keys=True)}"
        return hashlib.md5(cache_data.encode()).hexdigest()
    
    async def _get_cached_response(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve cached response from Supabase.
        
        Args:
            cache_key: Cache key to lookup
            
        Returns:
            Cached response data or None if not found/expired
        """
        if not self.supabase:
            return None
        
        try:
            result = self.supabase.table("dataforseo_cache").select("*").eq("cache_key", cache_key).execute()
            
            if result.data and len(result.data) > 0:
                cached = result.data[0]
                cached_at = datetime.fromisoformat(cached["cached_at"])
                
                # Check if cache is still valid
                if datetime.utcnow() - cached_at < timedelta(hours=self.cache_ttl_hours):
                    logger.info(f"Cache hit for key {cache_key}")
                    return cached["response_data"]
                else:
                    # Delete expired cache
                    self.supabase.table("dataforseo_cache").delete().eq("cache_key", cache_key).execute()
                    logger.info(f"Cache expired for key {cache_key}")
            
            return None
        except Exception as e:
            logger.warning(f"Error retrieving cached response: {e}")
            return None
    
    async def _cache_response(self, cache_key: str, response_data: Dict[str, Any]):
        """
        Cache response to Supabase.
        
        Args:
            cache_key: Cache key
            response_data: Response data to cache
        """
        if not self.supabase:
            return
        
        try:
            self.supabase.table("dataforseo_cache").upsert({
                "cache_key": cache_key,
                "response_data": response_data,
                "cached_at": datetime.utcnow().isoformat(),
            }).execute()
            logger.info(f"Cached response for key {cache_key}")
        except Exception as e:
            logger.warning(f"Error caching response: {e}")
    
    async def _rate_limit(self):
        """Enforce rate limiting"""
        async with self._rate_limiter:
            now = asyncio.get_event_loop().time()
            time_since_last = now - self._last_request_time
            min_interval = 1.0 / self.rate_limit_per_second
            
            if time_since_last < min_interval:
                await asyncio.sleep(min_interval - time_since_last)
            
            self._last_request_time = asyncio.get_event_loop().time()
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((httpx.HTTPError, DataForSEORateLimitError)),
        reraise=True,
    )
    async def _make_request(
        self,
        endpoint: str,
        method: str = "POST",
        data: Optional[List[Dict[str, Any]]] = None,
        use_cache: bool = True,
    ) -> Dict[str, Any]:
        """
        Make HTTP request to DataForSEO API with retries and error handling.
        
        Args:
            endpoint: API endpoint path
            method: HTTP method
            data: Request payload
            use_cache: Whether to use caching
            
        Returns:
            Response data
            
        Raises:
            DataForSEOAuthError: Authentication failed
            DataForSEORateLimitError: Rate limit exceeded
            DataForSEOError: Other API errors
        """
        if not self._authenticated:
            await self.authenticate()
        
        # Check cache
        cache_key = None
        if use_cache and method == "POST" and data:
            cache_key = self._generate_cache_key(endpoint, data[0] if data else {})
            cached_response = await self._get_cached_response(cache_key)
            if cached_response:
                return cached_response
        
        # Rate limiting
        await self._rate_limit()
        
        url = f"{self.BASE_URL}/{endpoint}"
        
        try:
            if method == "POST":
                response = await self._client.post(url, json=data)
            else:
                response = await self._client.get(url)
            
            # Handle HTTP errors
            if response.status_code == 401:
                raise DataForSEOAuthError("Authentication failed. Check credentials.")
            elif response.status_code == 429:
                raise DataForSEORateLimitError("Rate limit exceeded")
            elif response.status_code >= 400:
                raise DataForSEOError(f"API error: {response.status_code} - {response.text}")
            
            response.raise_for_status()
            result = response.json()
            
            # Check API-level errors
            if result.get("status_code") != 20000:
                error_msg = result.get("status_message", "Unknown error")
                raise DataForSEOError(f"API returned error: {error_msg}")
            
            # Cache successful response
            if use_cache and cache_key and result.get("tasks"):
                await self._cache_response(cache_key, result)
            
            return result
            
        except httpx.HTTPError as e:
            logger.error(f"HTTP error during request to {endpoint}: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during request to {endpoint}: {e}")
            raise DataForSEOError(f"Request failed: {e}")
    
    async def fetch_serp_results(
        self,
        keywords: List[str],
        location_code: int = 2840,  # USA
        language_code: str = "en",
        device: str = "desktop",
        depth: int = 100,
        use_cache: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Fetch live SERP results for multiple keywords.
        
        Args:
            keywords: List of keywords to fetch SERPs for
            location_code: DataForSEO location code (2840 = USA)
            language_code: Language code (e.g., "en")
            device: Device type ("desktop", "mobile")
            depth: Number of results to fetch (max 700)
            use_cache: Whether to use caching
            
        Returns:
            List of SERP result dictionaries, one per keyword
            
        Example:
            >>> results = await client.fetch_serp_results(
            ...     keywords=["best crm", "crm software"],
            ...     location_code=2840,
            ...     language_code="en"
            ... )
        """
        endpoint = "serp/google/organic/live/advanced"
        
        # Build request payloads
        tasks = []
        for keyword in keywords:
            tasks.append({
                "keyword": keyword,
                "location_code": location_code,
                "language_code": language_code,
                "device": device,
                "depth": depth,
                "calculate_rectangles": True,  # For SERP feature positioning
            })
        
        # Make request
        response = await self._make_request(endpoint, data=tasks, use_cache=use_cache)
        
        # Parse results
        results = []
        for task in response.get("tasks", []):
            if task.get("status_code") == 20000 and task.get("result"):
                for item in task["result"]:
                    results.append(self._parse_serp_result(item, keyword))
        
        return results
    
    def _parse_serp_result(self, raw_result: Dict[str, Any], keyword: str) -> Dict[str, Any]:
        """
        Parse raw SERP result into structured format.
        
        Args:
            raw_result: Raw API response for single keyword
            keyword: Keyword queried
            
        Returns:
            Structured SERP data
        """
        items = raw_result.get("items", [])
        
        # Extract organic results
        organic_results = []
        for item in items:
            if item.get("type") == "organic":
                organic_results.append({
                    "position": item.get("rank_absolute"),
                    "url": item.get("url"),
                    "domain": item.get("domain"),
                    "title": item.get("title"),
                    "description": item.get("description"),
                    "breadcrumb": item.get("breadcrumb"),
                    "rectangle": item.get("rectangle"),
                })
        
        # Extract SERP features
        serp_features = self._extract_serp_features(items)
        
        # Extract competitor domains (top 10)
        competitors = []
        seen_domains = set()
        for result in organic_results[:10]:
            domain = result["domain"]
            if domain and domain not in seen_domains:
                competitors.append(domain)
                seen_domains.add(domain)
        
        # Calculate visual displacement
        visual_displacement = self._calculate_visual_displacement(items, organic_results)
        
        return {
            "keyword": keyword,
            "organic_results": organic_results,
            "serp_features": serp_features,
            "competitors": competitors,
            "total_results": raw_result.get("se_results_count", 0),
            "visual_displacement": visual_displacement,
            "fetched_at": datetime.utcnow().isoformat(),
        }
    
    def _extract_serp_features(self, items: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Extract and categorize SERP features from results.
        
        Args:
            items: Raw SERP items
            
        Returns:
            Dictionary of SERP features with counts and details
        """
        features = {
            "featured_snippet": None,
            "people_also_ask": [],
            "knowledge_graph": None,
            "local_pack": None,
            "video": [],
            "image": [],
            "shopping": [],
            "top_stories": [],
            "twitter": [],
            "recipes": [],
            "ai_overview": None,
            "related_searches": [],
        }
        
        for item in items:
            item_type = item.get("type", "")
            
            # Featured snippet
            if item_type == "featured_snippet":
                features["featured_snippet"] = {
                    "title": item.get("title"),
                    "description": item.get("description"),
                    "url": item.get("url"),
                    "domain": item.get("domain"),
                }
            
            # People Also Ask
            elif item_type == "people_also_ask":
                features["people_also_ask"].append({
                    "question": item.get("title"),
                    "answer": item.get("expanded_element", [{}])[0].get("description") if item.get("expanded_element") else None,
                })
            
            # Knowledge graph
            elif item_type in ["knowledge_graph", "knowledge_graph_expanded_item"]:
                if not features["knowledge_graph"]:
                    features["knowledge_graph"] = {
                        "title": item.get("title"),
                        "description": item.get("description"),
                        "url": item.get("url"),
                    }
            
            # Local pack
            elif item_type == "local_pack":
                features["local_pack"] = {
                    "count": len(item.get("items", [])),
                }
            
            # Video carousel
            elif item_type == "video":
                features["video"].append({
                    "title": item.get("title"),
                    "url": item.get("url"),
                    "source": item.get("source"),
                })
            
            # Images
            elif item_type == "images":
                features["image"].append({
                    "title": item.get("title"),
                    "url": item.get("url"),
                })
            
            # Shopping results
            elif item_type in ["shopping", "google_shopping"]:
                features["shopping"].append({
                    "title": item.get("title"),
                    "price": item.get("price"),
                })
            
            # Top stories
            elif item_type == "top_stories":
                features["top_stories"].append({
                    "title": item.get("title"),
                    "url": item.get("url"),
                    "source": item.get("source"),
                })
            
            # Twitter
            elif item_type == "twitter":
                features["twitter"].append({
                    "title": item.get("title"),
                    "url": item.get("url"),
                })
            
            # Recipes
            elif item_type == "recipes":
                features["recipes"].append({
                    "title": item.get("title"),
                    "rating": item.get("rating"),
                })
            
            # AI Overview (Google SGE)
            elif item_type == "ai_overview":
                features["ai_overview"] = {
                    "text": item.get("text"),
                    "sources": item.get("items", []),
                }
            
            # Related searches
            elif item_type in ["people_also_search", "related_searches"]:
                for related_item in item.get("items", []):
                    features["related_searches"].append(related_item.get("title"))
        
        return features
    
    def _calculate_visual_displacement(
        self,
        items: List[Dict[str, Any]],
        organic_results: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Calculate visual displacement caused by SERP features.
        
        For each organic result, count how many SERP elements appear above it,
        and estimate the "visual position" vs organic rank.
        
        Args:
            items: All SERP items
            organic_results: Organic results only
            
        Returns:
            Visual displacement data
        """
        if not organic_results:
            return {"displacement_positions": 0, "displaced_results": []}
        
        # Count SERP features above each organic result
        displaced = []
        for org_result in organic_results[:10]:  # Top 10 only
            organic_rank = org_result["position"]
            
            # Count items above this organic result
            features_above = 0
            for item in items:
                item_rank = item.get("rank_absolute", 999)
                if item_rank < organic_rank and item.get("type") != "organic":
                    # Weight different feature types
                    item_type = item.get("type", "")
                    if item_type == "featured_snippet":
                        features_above += 2  # Takes more space
                    elif item_type == "people_also_ask":
                        features_above += 0.5  # Each PAA question
                    elif item_type in ["local_pack", "video", "shopping"]:
                        features_above += 1.5
                    else:
                        features_above += 1
            
            visual_position = organic_rank + features_above
            
            if features_above > 0:
                displaced.append({
                    "url": org_result["url"],
                    "organic_position": organic_rank,
                    "visual_position": round(visual_position, 1),
                    "displacement": round(features_above, 1),
                })
        
        avg_displacement = sum(d["displacement"] for d in displaced) / len(displaced) if displaced else 0
        
        return {
            "average_displacement_positions": round(avg_displacement, 1),
            "displaced_results": displaced,
        }
    
    async def discover_competitors(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
        min_keyword_overlap: int = 2,
    ) -> List[Dict[str, Any]]:
        """
        Discover competitor domains based on keyword overlap.
        
        Args:
            keywords: List of keywords to analyze
            location_code: DataForSEO location code
            language_code: Language code
            min_keyword_overlap: Minimum keywords a domain must rank for to be included
            
        Returns:
            List of competitor domains with ranking data
            
        Example:
            >>> competitors = await client.discover_competitors(
            ...     keywords=["crm software", "best crm", "crm comparison"],
            ...     min_keyword_overlap=2
            ... )
        """
        # Fetch SERP results
        serp_results = await self.fetch_serp_results(
            keywords=keywords,
            location_code=location_code,
            language_code=language_code,
        )
        
        # Build domain frequency map
        domain_data = {}
        for serp in serp_results:
            keyword = serp["keyword"]
            for result in serp["organic_results"][:20]:  # Top 20
                domain = result["domain"]
                if not domain:
                    continue
                
                if domain not in domain_data:
                    domain_data[domain] = {
                        "domain": domain,
                        "keywords": set(),
                        "positions": [],
                        "urls": set(),
                    }
                
                domain_data[domain]["keywords"].add(keyword)
                domain_data[domain]["positions"].append(result["position"])
                domain_data[domain]["urls"].add(result["url"])
        
        # Filter and format competitors
        competitors = []
        for domain, data in domain_data.items():
            if len(data["keywords"]) >= min_keyword_overlap:
                competitors.append({
                    "domain": domain,
                    "keywords_ranked": len(data["keywords"]),
                    "avg_position": round(sum(data["positions"]) / len(data["positions"]), 1),
                    "best_position": min(data["positions"]),
                    "total_urls_ranked": len(data["urls"]),
                    "keyword_overlap_pct": round(len(data["keywords"]) / len(keywords) * 100, 1),
                })
        
        # Sort by keyword overlap
        competitors.sort(key=lambda x: x["keywords_ranked"], reverse=True)
        
        return competitors
    
    async def get_keyword_data(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
    ) -> List[Dict[str, Any]]:
        """
        Get keyword-level data including search volume, competition, CPC.
        
        Note: This uses DataForSEO's Keywords Data API which has separate pricing.
        
        Args:
            keywords: List of keywords
            location_code: DataForSEO location code
            language_code: Language code
            
        Returns:
            List of keyword data dictionaries
        """
        endpoint = "keywords_data/google_ads/search_volume/live"
        
        tasks = [{
            "keywords": keywords,
            "location_code": location_code,
            "language_code": language_code,
        }]
        
        response = await self._make_request(endpoint, data=tasks, use_cache=True)
        
        results = []
        for task in response.get("tasks", []):
            if task.get("status_code") == 20000 and task.get("result"):
                for item in task["result"]:
                    results.append({
                        "keyword": item.get("keyword"),
                        "search_volume": item.get("search_volume"),
                        "competition": item.get("competition"),
                        "competition_level": item.get("competition_level"),
                        "cpc": item.get("cpc"),
                        "low_top_of_page_bid": item.get("low_top_of_page_bid"),
                        "high_top_of_page_bid": item.get("high_top_of_page_bid"),
                    })
        
        return results
    
    async def batch_fetch_serps(
        self,
        keywords: List[str],
        batch_size: int = 50,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """
        Fetch SERPs for large keyword lists in batches.
        
        Args:
            keywords: List of keywords (can be hundreds)
            batch_size: Keywords per batch (max 100 per DataForSEO request)
            **kwargs: Additional arguments passed to fetch_serp_results
            
        Returns:
            List of all SERP results
        """
        all_results = []
        
        for i in range(0, len(keywords), batch_size):
            batch = keywords[i:i + batch_size]
            logger.info(f"Fetching SERP batch {i//batch_size + 1}/{(len(keywords)-1)//batch_size + 1} ({len(batch)} keywords)")
            
            batch_results = await self.fetch_serp_results(batch, **kwargs)
            all_results.extend(batch_results)
            
            # Small delay between batches
            if i + batch_size < len(keywords):
                await asyncio.sleep(1)
        
        return all_results
    
    def categorize_serp_intent(self, serp_features: Dict[str, Any]) -> str:
        """
        Classify search intent based on SERP features present.
        
        Args:
            serp_features: SERP features dictionary from _extract_serp_features
            
        Returns:
            Intent category: "informational", "commercial", "transactional", "navigational"
        """
        # Transactional signals
        if serp_features.get("shopping") or serp_features.get("local_pack"):
            return "transactional"
        
        # Navigational signals
        if serp_features.get("knowledge_graph"):
            return "navigational"
        
        # Commercial signals
        if serp_features.get("video") or (
            serp_features.get("featured_snippet") and 
            serp_features.get("people_also_ask")
        ):
            return "commercial"
        
        # Informational (default)
        return "informational"
    
    async def get_serp_volatility(
        self,
        keyword: str,
        location_code: int = 2840,
        language_code: str = "en",
    ) -> Dict[str, Any]:
        """
        Get SERP volatility/change data for a keyword.
        
        Note: This requires historical SERP data which may not be available
        for all keywords. Falls back to single snapshot.
        
        Args:
            keyword: Keyword to analyze
            location_code: DataForSEO location code
            language_code: Language code
            
        Returns:
            Volatility metrics
        """
        # For now, just fetch current SERP
        # In production, you'd compare against cached historical data
        current_serp = await self.fetch_serp_results(
            keywords=[keyword],
            location_code=location_code,
            language_code=language_code,
        )
        
        if not current_serp:
            return {"error": "No SERP data available"}
        
        serp = current_serp[0]
        
        # Check cache for historical comparison
        cache_key = self._generate_cache_key(
            "serp/google/organic/live/advanced",
            {"keyword": keyword, "location_code": location_code}
        )
        
        historical = await self._get_cached_response(cache_key)
        
        if historical and historical.get("tasks"):
            # Compare domain positions
            # This is a simplified version - production would do deeper analysis
            return {
                "keyword": keyword,
                "has_historical_data": True,
                "volatility": "medium",  # Placeholder
            }
        
        return {
            "keyword": keyword,
            "has_historical_data": False,
            "current_competitors": len(serp["competitors"]),
            "current_features": len([f for f in serp["serp_features"].values() if f]),
        }

