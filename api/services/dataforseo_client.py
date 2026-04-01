import os
import asyncio
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import httpx
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)
from functools import wraps
import hashlib
import json

logger = logging.getLogger(__name__)


class DataForSEOError(Exception):
    """Base exception for DataForSEO API errors"""
    pass


class DataForSEORateLimitError(DataForSEOError):
    """Raised when rate limit is exceeded"""
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
    """
    
    BASE_URL = "https://api.dataforseo.com/v3"
    
    def __init__(
        self,
        login: Optional[str] = None,
        password: Optional[str] = None,
        timeout: int = 60,
        max_retries: int = 3,
        supabase_client: Optional[Any] = None,
        cache_ttl_hours: int = 24,
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
        """
        self.login = login or os.getenv("DATAFORSEO_LOGIN")
        self.password = password or os.getenv("DATAFORSEO_PASSWORD")
        
        if not self.login or not self.password:
            raise ValueError(
                "DataForSEO credentials not provided. "
                "Set DATAFORSEO_LOGIN and DATAFORSEO_PASSWORD environment variables."
            )
        
        self.timeout = timeout
        self.max_retries = max_retries
        self.auth = (self.login, self.password)
        self.supabase = supabase_client
        self.cache_ttl_hours = cache_ttl_hours
        
        # Rate limiting: DataForSEO allows ~2000 API units/minute
        # Conservative semaphore to prevent hammering
        self._semaphore = asyncio.Semaphore(50)
        self._last_request_time = 0
        self._min_request_interval = 0.03  # 30ms between requests
    
    async def authenticate(self) -> bool:
        """
        Test authentication by making a simple API call.
        
        Returns:
            True if authentication successful, False otherwise
        """
        try:
            async with httpx.AsyncClient(auth=self.auth, timeout=self.timeout) as client:
                response = await client.get(f"{self.BASE_URL}/user_data")
                
                if response.status_code == 200:
                    data = response.json()
                    if data.get("status_code") == 20000:
                        logger.info("DataForSEO authentication successful")
                        return True
                    else:
                        logger.error(f"DataForSEO auth failed: {data.get('status_message')}")
                        return False
                else:
                    logger.error(f"DataForSEO auth failed with status {response.status_code}")
                    return False
                    
        except Exception as e:
            logger.error(f"DataForSEO authentication error: {str(e)}")
            return False
    
    def _generate_cache_key(self, endpoint: str, params: Dict[str, Any]) -> str:
        """
        Generate a cache key from endpoint and parameters.
        
        Args:
            endpoint: API endpoint
            params: Request parameters
            
        Returns:
            MD5 hash of the normalized request
        """
        cache_data = {
            "endpoint": endpoint,
            "params": params,
        }
        cache_string = json.dumps(cache_data, sort_keys=True)
        return hashlib.md5(cache_string.encode()).hexdigest()
    
    async def _get_cached_response(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve cached response from Supabase.
        
        Args:
            cache_key: Cache key to look up
            
        Returns:
            Cached response data or None if not found/expired
        """
        if not self.supabase:
            return None
        
        try:
            result = self.supabase.table("dataforseo_cache")\
                .select("*")\
                .eq("cache_key", cache_key)\
                .single()\
                .execute()
            
            if result.data:
                cached_at = datetime.fromisoformat(result.data["cached_at"])
                expiry = cached_at + timedelta(hours=self.cache_ttl_hours)
                
                if datetime.utcnow() < expiry:
                    logger.info(f"Cache hit for key {cache_key}")
                    return result.data["response_data"]
                else:
                    logger.info(f"Cache expired for key {cache_key}")
                    # Delete expired cache entry
                    self.supabase.table("dataforseo_cache")\
                        .delete()\
                        .eq("cache_key", cache_key)\
                        .execute()
            
            return None
            
        except Exception as e:
            logger.warning(f"Error retrieving cached response: {str(e)}")
            return None
    
    async def _cache_response(self, cache_key: str, response_data: Dict[str, Any]) -> None:
        """
        Cache response in Supabase.
        
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
            logger.warning(f"Error caching response: {str(e)}")
    
    async def _rate_limit(self):
        """Implement rate limiting to avoid API throttling"""
        async with self._semaphore:
            now = asyncio.get_event_loop().time()
            time_since_last = now - self._last_request_time
            
            if time_since_last < self._min_request_interval:
                await asyncio.sleep(self._min_request_interval - time_since_last)
            
            self._last_request_time = asyncio.get_event_loop().time()
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.NetworkError)),
    )
    async def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """
        Make an API request with retry logic and error handling.
        
        Args:
            method: HTTP method (GET or POST)
            endpoint: API endpoint
            data: Request payload for POST requests
            
        Returns:
            API response data
            
        Raises:
            DataForSEOError: If API returns an error
            DataForSEORateLimitError: If rate limit is exceeded
        """
        await self._rate_limit()
        
        url = f"{self.BASE_URL}/{endpoint}"
        
        async with httpx.AsyncClient(auth=self.auth, timeout=self.timeout) as client:
            try:
                if method == "POST":
                    response = await client.post(url, json=data)
                else:
                    response = await client.get(url)
                
                response.raise_for_status()
                result = response.json()
                
                # Check DataForSEO-specific status codes
                if result.get("status_code") == 20000:
                    return result
                elif result.get("status_code") == 40101:
                    raise DataForSEORateLimitError("Rate limit exceeded")
                else:
                    error_msg = result.get("status_message", "Unknown error")
                    raise DataForSEOError(f"API error: {error_msg}")
                    
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:
                    raise DataForSEORateLimitError("Rate limit exceeded")
                else:
                    raise DataForSEOError(f"HTTP error {e.response.status_code}: {str(e)}")
            
            except httpx.TimeoutException:
                logger.warning(f"Request timeout for {endpoint}")
                raise
            
            except httpx.NetworkError as e:
                logger.warning(f"Network error for {endpoint}: {str(e)}")
                raise
    
    async def get_serp_data(
        self,
        keyword: str,
        location: str = "United States",
        language: str = "en",
        device: str = "desktop",
        depth: int = 100,
    ) -> Dict[str, Any]:
        """
        Get live SERP data for a keyword.
        
        Args:
            keyword: Search query
            location: Target location (country name or location code)
            language: Language code (e.g., "en")
            device: Device type ("desktop" or "mobile")
            depth: Number of results to retrieve (max 100)
            
        Returns:
            Parsed SERP data including organic results, SERP features, and positions
        """
        # Check cache first
        cache_key = self._generate_cache_key(
            "serp/google/organic/live/advanced",
            {
                "keyword": keyword,
                "location": location,
                "language": language,
                "device": device,
                "depth": depth,
            }
        )
        
        cached = await self._get_cached_response(cache_key)
        if cached:
            return cached
        
        # Make API request
        payload = [{
            "keyword": keyword,
            "location_name": location,
            "language_code": language,
            "device": device,
            "depth": depth,
            "calculate_rectangles": True,  # For visual position calculation
        }]
        
        response = await self._make_request("POST", "serp/google/organic/live/advanced", payload)
        
        # Parse and structure the response
        parsed_data = self._parse_serp_response(response)
        
        # Cache the response
        await self._cache_response(cache_key, parsed_data)
        
        return parsed_data
    
    def _parse_serp_response(self, raw_response: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse raw SERP API response into structured format.
        
        Args:
            raw_response: Raw API response
            
        Returns:
            Structured SERP data
        """
        if not raw_response.get("tasks") or not raw_response["tasks"][0].get("result"):
            return {
                "organic_results": [],
                "serp_features": {},
                "competitors": [],
                "total_results": 0,
            }
        
        result = raw_response["tasks"][0]["result"][0]
        items = result.get("items", [])
        
        organic_results = []
        serp_features = {
            "featured_snippet": None,
            "people_also_ask": [],
            "knowledge_panel": None,
            "video_carousel": [],
            "local_pack": None,
            "image_pack": None,
            "shopping_results": [],
            "top_stories": [],
            "ai_overview": None,
            "reddit_threads": [],
        }
        
        visual_offset = 0
        
        for item in items:
            item_type = item.get("type")
            
            if item_type == "organic":
                organic_results.append({
                    "position": item.get("rank_absolute"),
                    "url": item.get("url"),
                    "domain": item.get("domain"),
                    "title": item.get("title"),
                    "description": item.get("description"),
                    "visual_position": item.get("rank_absolute", 0) + visual_offset,
                })
            
            elif item_type == "featured_snippet":
                serp_features["featured_snippet"] = {
                    "url": item.get("url"),
                    "domain": item.get("domain"),
                    "title": item.get("title"),
                    "description": item.get("description"),
                }
                visual_offset += 2  # Featured snippets take significant visual space
            
            elif item_type == "people_also_ask":
                serp_features["people_also_ask"].append({
                    "question": item.get("title"),
                    "answer": item.get("expanded_element", {}).get("description"),
                })
                visual_offset += 0.5  # Each PAA adds visual distance
            
            elif item_type == "knowledge_graph":
                serp_features["knowledge_panel"] = {
                    "title": item.get("title"),
                    "description": item.get("description"),
                    "type": item.get("knowledge_graph_type"),
                }
                visual_offset += 3  # Knowledge panels are visually prominent
            
            elif item_type == "video":
                serp_features["video_carousel"].append({
                    "title": item.get("title"),
                    "url": item.get("url"),
                    "source": item.get("source"),
                })
            
            elif item_type == "local_pack":
                serp_features["local_pack"] = {
                    "title": item.get("title"),
                    "results_count": len(item.get("items", [])),
                }
                visual_offset += 2
            
            elif item_type == "images":
                serp_features["image_pack"] = {
                    "title": item.get("title"),
                    "results_count": len(item.get("items", [])),
                }
                visual_offset += 1
            
            elif item_type == "shopping":
                serp_features["shopping_results"].append({
                    "title": item.get("title"),
                    "price": item.get("price"),
                    "url": item.get("url"),
                })
            
            elif item_type == "top_stories":
                serp_features["top_stories"].append({
                    "title": item.get("title"),
                    "url": item.get("url"),
                    "source": item.get("source"),
                })
            
            elif item_type == "ai_overview" or item_type == "generative_ai":
                serp_features["ai_overview"] = {
                    "text": item.get("text"),
                    "sources": [src.get("url") for src in item.get("items", [])],
                }
                visual_offset += 3
            
            elif "reddit" in item.get("url", "").lower():
                serp_features["reddit_threads"].append({
                    "title": item.get("title"),
                    "url": item.get("url"),
                })
        
        # Extract unique competitors (domains in top 10)
        competitors = []
        seen_domains = set()
        for result in organic_results[:10]:
            domain = result["domain"]
            if domain and domain not in seen_domains:
                competitors.append({
                    "domain": domain,
                    "position": result["position"],
                    "url": result["url"],
                    "title": result["title"],
                })
                seen_domains.add(domain)
        
        return {
            "organic_results": organic_results,
            "serp_features": serp_features,
            "competitors": competitors,
            "total_results": result.get("items_count", 0),
            "keyword": result.get("keyword"),
            "location": result.get("location_name"),
        }
    
    async def get_competitor_data(self, domain: str, limit: int = 1000) -> Dict[str, Any]:
        """
        Get competitor data for a domain (keywords they rank for).
        
        Args:
            domain: Target domain
            limit: Maximum number of keywords to retrieve
            
        Returns:
            Competitor keyword data
        """
        cache_key = self._generate_cache_key(
            "dataforseo_labs/google/ranked_keywords/live",
            {"target": domain, "limit": limit}
        )
        
        cached = await self._get_cached_response(cache_key)
        if cached:
            return cached
        
        payload = [{
            "target": domain,
            "location_name": "United States",
            "language_code": "en",
            "limit": limit,
            "filters": [
                ["metrics.organic.pos", "<=", 20]  # Top 20 positions only
            ],
        }]
        
        response = await self._make_request(
            "POST",
            "dataforseo_labs/google/ranked_keywords/live",
            payload
        )
        
        parsed_data = self._parse_competitor_response(response)
        await self._cache_response(cache_key, parsed_data)
        
        return parsed_data
    
    def _parse_competitor_response(self, raw_response: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse competitor API response.
        
        Args:
            raw_response: Raw API response
            
        Returns:
            Structured competitor data
        """
        if not raw_response.get("tasks") or not raw_response["tasks"][0].get("result"):
            return {
                "domain": "",
                "keywords": [],
                "total_keywords": 0,
                "avg_position": 0,
            }
        
        result = raw_response["tasks"][0]["result"][0]
        items = result.get("items", [])
        
        keywords = []
        total_position = 0
        
        for item in items:
            metrics = item.get("metrics", {}).get("organic", {})
            keywords.append({
                "keyword": item.get("keyword_data", {}).get("keyword"),
                "position": metrics.get("pos"),
                "search_volume": item.get("keyword_data", {}).get("keyword_info", {}).get("search_volume"),
                "cpc": item.get("keyword_data", {}).get("keyword_info", {}).get("cpc"),
                "competition": item.get("keyword_data", {}).get("keyword_info", {}).get("competition"),
            })
            total_position += metrics.get("pos", 0)
        
        avg_position = total_position / len(keywords) if keywords else 0
        
        return {
            "domain": result.get("target"),
            "keywords": keywords,
            "total_keywords": len(keywords),
            "avg_position": round(avg_position, 2),
        }
    
    async def get_serp_features(
        self,
        keyword: str,
        location: str = "United States",
    ) -> Dict[str, Any]:
        """
        Get SERP features for a keyword (lightweight version of get_serp_data).
        
        Args:
            keyword: Search query
            location: Target location
            
        Returns:
            SERP features only (no full organic results)
        """
        full_data = await self.get_serp_data(keyword, location, depth=10)
        
        return {
            "keyword": keyword,
            "location": location,
            "serp_features": full_data.get("serp_features", {}),
            "has_featured_snippet": full_data["serp_features"].get("featured_snippet") is not None,
            "paa_count": len(full_data["serp_features"].get("people_also_ask", [])),
            "has_knowledge_panel": full_data["serp_features"].get("knowledge_panel") is not None,
            "has_local_pack": full_data["serp_features"].get("local_pack") is not None,
            "has_video": len(full_data["serp_features"].get("video_carousel", [])) > 0,
            "has_ai_overview": full_data["serp_features"].get("ai_overview") is not None,
        }
    
    async def batch_get_serp_data(
        self,
        keywords: List[str],
        location: str = "United States",
        language: str = "en",
        device: str = "desktop",
        batch_size: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Get SERP data for multiple keywords in batches.
        
        Args:
            keywords: List of search queries
            location: Target location
            language: Language code
            device: Device type
            batch_size: Number of concurrent requests
            
        Returns:
            List of SERP data for each keyword
        """
        results = []
        
        for i in range(0, len(keywords), batch_size):
            batch = keywords[i:i + batch_size]
            tasks = [
                self.get_serp_data(kw, location, language, device)
                for kw in batch
            ]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for kw, result in zip(batch, batch_results):
                if isinstance(result, Exception):
                    logger.error(f"Error fetching SERP data for '{kw}': {str(result)}")
                    results.append({
                        "keyword": kw,
                        "error": str(result),
                        "organic_results": [],
                        "serp_features": {},
                        "competitors": [],
                    })
                else:
                    results.append(result)
            
            # Brief pause between batches
            if i + batch_size < len(keywords):
                await asyncio.sleep(1)
        
        return results
    
    async def get_historical_serp_data(
        self,
        keyword: str,
        location: str = "United States",
    ) -> List[Dict[str, Any]]:
        """
        Get historical SERP data if available in cache.
        
        Args:
            keyword: Search query
            location: Target location
            
        Returns:
            List of historical SERP snapshots
        """
        if not self.supabase:
            return []
        
        try:
            result = self.supabase.table("dataforseo_cache")\
                .select("*")\
                .like("cache_key", f"%{keyword}%")\
                .order("cached_at", desc=True)\
                .limit(10)\
                .execute()
            
            historical_data = []
            for row in result.data:
                historical_data.append({
                    "cached_at": row["cached_at"],
                    "data": row["response_data"],
                })
            
            return historical_data
            
        except Exception as e:
            logger.warning(f"Error retrieving historical SERP data: {str(e)}")
            return []