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
        
        # Rate limiting
        self._last_request_time = 0.0
        self._request_lock = asyncio.Lock()
        
        # HTTP client
        self._client: Optional[httpx.AsyncClient] = None
        
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
        if self._client is not None:
            return
        
        auth = httpx.BasicAuth(self.login, self.password)
        self._client = httpx.AsyncClient(
            auth=auth,
            timeout=self.timeout,
            headers={
                "Content-Type": "application/json",
            }
        )
        logger.info("DataForSEO client authenticated successfully")
    
    async def close(self):
        """Close HTTP client"""
        if self._client:
            await self._client.aclose()
            self._client = None
    
    async def _rate_limit(self):
        """Enforce rate limiting"""
        async with self._request_lock:
            now = asyncio.get_event_loop().time()
            time_since_last = now - self._last_request_time
            min_interval = 1.0 / self.rate_limit_per_second
            
            if time_since_last < min_interval:
                await asyncio.sleep(min_interval - time_since_last)
            
            self._last_request_time = asyncio.get_event_loop().time()
    
    def _generate_cache_key(self, endpoint: str, params: Dict[str, Any]) -> str:
        """Generate cache key from endpoint and parameters"""
        # Sort params for consistent hashing
        params_str = json.dumps(params, sort_keys=True)
        key_str = f"{endpoint}:{params_str}"
        return hashlib.sha256(key_str.encode()).hexdigest()
    
    async def _get_cached_response(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """Retrieve cached response from Supabase"""
        if not self.supabase:
            return None
        
        try:
            result = self.supabase.table("dataforseo_cache").select("*").eq(
                "cache_key", cache_key
            ).execute()
            
            if result.data and len(result.data) > 0:
                cache_entry = result.data[0]
                cached_at = datetime.fromisoformat(cache_entry["cached_at"])
                
                # Check if cache is still valid
                if datetime.utcnow() - cached_at < timedelta(hours=self.cache_ttl_hours):
                    logger.info(f"Cache hit for key: {cache_key}")
                    return cache_entry["response_data"]
                else:
                    logger.info(f"Cache expired for key: {cache_key}")
            
        except Exception as e:
            logger.warning(f"Failed to retrieve cache: {e}")
        
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
            logger.info(f"Cached response for key: {cache_key}")
        except Exception as e:
            logger.warning(f"Failed to cache response: {e}")
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((httpx.HTTPError, DataForSEORateLimitError)),
    )
    async def _make_request(
        self,
        endpoint: str,
        data: Dict[str, Any],
        use_cache: bool = True,
    ) -> Dict[str, Any]:
        """
        Make API request with retries and rate limiting.
        
        Args:
            endpoint: API endpoint (e.g., "/serp/google/organic/live/advanced")
            data: Request payload
            use_cache: Whether to use caching
            
        Returns:
            API response data
            
        Raises:
            DataForSEOError: On API errors
            DataForSEORateLimitError: On rate limit errors
            DataForSEOAuthError: On authentication errors
        """
        if not self._client:
            await self.authenticate()
        
        # Check cache first
        cache_key = self._generate_cache_key(endpoint, data)
        if use_cache:
            cached = await self._get_cached_response(cache_key)
            if cached:
                return cached
        
        # Rate limiting
        await self._rate_limit()
        
        url = f"{self.BASE_URL}{endpoint}"
        
        try:
            response = await self._client.post(url, json=[data])
            response.raise_for_status()
            
            result = response.json()
            
            # Check for API-level errors
            if "status_code" in result and result["status_code"] != 20000:
                status_code = result["status_code"]
                status_message = result.get("status_message", "Unknown error")
                
                if status_code == 40100:
                    raise DataForSEOAuthError(f"Authentication failed: {status_message}")
                elif status_code == 50000:
                    raise DataForSEORateLimitError(f"Rate limit exceeded: {status_message}")
                else:
                    raise DataForSEOError(f"API error {status_code}: {status_message}")
            
            # Cache successful response
            if use_cache:
                await self._cache_response(cache_key, result)
            
            return result
            
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                raise DataForSEOAuthError(f"Authentication failed: {e}")
            elif e.response.status_code == 429:
                raise DataForSEORateLimitError(f"Rate limit exceeded: {e}")
            else:
                raise DataForSEOError(f"HTTP error: {e}")
        except httpx.HTTPError as e:
            raise DataForSEOError(f"Request failed: {e}")
    
    async def fetch_serp_results(
        self,
        keywords: List[str],
        location_code: int = 2840,  # United States
        language_code: str = "en",
        device: str = "desktop",
        depth: int = 100,
        use_cache: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Fetch live SERP results for multiple keywords.
        
        Args:
            keywords: List of keywords to fetch
            location_code: DataForSEO location code (2840 = United States)
            language_code: Language code (e.g., "en")
            device: Device type ("desktop" or "mobile")
            depth: Number of results to fetch (max 100)
            use_cache: Whether to use caching
            
        Returns:
            List of SERP result dictionaries, one per keyword
        """
        results = []
        
        for keyword in keywords:
            try:
                data = {
                    "keyword": keyword,
                    "location_code": location_code,
                    "language_code": language_code,
                    "device": device,
                    "depth": depth,
                }
                
                response = await self._make_request(
                    "/serp/google/organic/live/advanced",
                    data,
                    use_cache=use_cache,
                )
                
                # Extract results from response
                if "tasks" in response and len(response["tasks"]) > 0:
                    task = response["tasks"][0]
                    if "result" in task and len(task["result"]) > 0:
                        result = task["result"][0]
                        results.append({
                            "keyword": keyword,
                            "data": result,
                            "timestamp": datetime.utcnow().isoformat(),
                        })
                    else:
                        logger.warning(f"No results for keyword: {keyword}")
                else:
                    logger.warning(f"Invalid response structure for keyword: {keyword}")
                    
            except Exception as e:
                logger.error(f"Failed to fetch SERP for keyword '{keyword}': {e}")
                # Continue with other keywords
                continue
        
        logger.info(f"Fetched SERP results for {len(results)}/{len(keywords)} keywords")
        return results
    
    def extract_organic_results(self, serp_result: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract organic search results from SERP data.
        
        Args:
            serp_result: SERP result dictionary from fetch_serp_results
            
        Returns:
            List of organic result dictionaries with position, URL, title, description
        """
        organic_results = []
        
        if "data" not in serp_result or "items" not in serp_result["data"]:
            return organic_results
        
        for item in serp_result["data"]["items"]:
            if item.get("type") == "organic":
                organic_results.append({
                    "position": item.get("rank_group", 0),
                    "url": item.get("url", ""),
                    "domain": item.get("domain", ""),
                    "title": item.get("title", ""),
                    "description": item.get("description", ""),
                    "breadcrumb": item.get("breadcrumb", ""),
                })
        
        return organic_results
    
    def extract_serp_features(self, serp_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract SERP features from SERP data.
        
        Args:
            serp_result: SERP result dictionary from fetch_serp_results
            
        Returns:
            Dictionary with detected SERP features and their details
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
        
        if "data" not in serp_result or "items" not in serp_result["data"]:
            return features
        
        for item in serp_result["data"]["items"]:
            item_type = item.get("type", "")
            
            # Featured snippet
            if item_type == "featured_snippet":
                features["featured_snippet"] = {
                    "url": item.get("url", ""),
                    "domain": item.get("domain", ""),
                    "title": item.get("title", ""),
                    "description": item.get("description", ""),
                }
            
            # People Also Ask
            elif item_type == "people_also_ask":
                if "items" in item:
                    for paa_item in item["items"]:
                        features["people_also_ask"].append({
                            "question": paa_item.get("title", ""),
                            "url": paa_item.get("url", ""),
                            "domain": paa_item.get("domain", ""),
                        })
            
            # Knowledge Graph
            elif item_type == "knowledge_graph":
                features["knowledge_graph"] = {
                    "title": item.get("title", ""),
                    "description": item.get("description", ""),
                    "type": item.get("knowledge_graph_type", ""),
                }
            
            # Local Pack
            elif item_type in ["local_pack", "map"]:
                features["local_pack"] = {
                    "count": len(item.get("items", [])),
                }
            
            # Video
            elif item_type == "video":
                if "items" in item:
                    for video_item in item["items"]:
                        features["video"].append({
                            "title": video_item.get("title", ""),
                            "url": video_item.get("url", ""),
                            "source": video_item.get("source", ""),
                        })
            
            # Images
            elif item_type == "images":
                features["image"] = {
                    "count": len(item.get("items", [])),
                }
            
            # Shopping
            elif item_type in ["shopping", "google_shopping"]:
                if "items" in item:
                    for shopping_item in item["items"]:
                        features["shopping"].append({
                            "title": shopping_item.get("title", ""),
                            "price": shopping_item.get("price", ""),
                            "source": shopping_item.get("source", ""),
                        })
            
            # Top Stories
            elif item_type == "top_stories":
                if "items" in item:
                    for story_item in item["items"]:
                        features["top_stories"].append({
                            "title": story_item.get("title", ""),
                            "url": story_item.get("url", ""),
                            "source": story_item.get("source", ""),
                        })
            
            # Twitter
            elif item_type == "twitter":
                if "items" in item:
                    for tweet_item in item["items"]:
                        features["twitter"].append({
                            "text": tweet_item.get("text", ""),
                            "url": tweet_item.get("url", ""),
                        })
            
            # Recipes
            elif item_type == "recipes":
                if "items" in item:
                    for recipe_item in item["items"]:
                        features["recipes"].append({
                            "title": recipe_item.get("title", ""),
                            "url": recipe_item.get("url", ""),
                        })
            
            # AI Overview
            elif item_type == "ai_overview":
                features["ai_overview"] = {
                    "text": item.get("text", ""),
                    "sources": item.get("sources", []),
                }
            
            # Related Searches
            elif item_type in ["people_also_search", "related_searches"]:
                if "items" in item:
                    for related_item in item["items"]:
                        features["related_searches"].append({
                            "keyword": related_item.get("title", ""),
                        })
        
        return features
    
    def calculate_visual_position(
        self,
        organic_position: int,
        serp_features: Dict[str, Any],
    ) -> float:
        """
        Calculate visual position accounting for SERP features above organic result.
        
        Each SERP feature adds weight to push down the visual position:
        - Featured snippet: +2 positions
        - AI overview: +3 positions
        - Each PAA question: +0.5 positions
        - Knowledge graph: +1 position
        - Local pack: +3 positions
        - Video carousel: +1 position
        - Image pack: +0.5 positions
        - Shopping results: +1 position
        - Top stories: +2 positions
        
        Args:
            organic_position: Actual organic ranking position
            serp_features: SERP features dictionary from extract_serp_features
            
        Returns:
            Adjusted visual position as float
        """
        visual_position = float(organic_position)
        
        # Featured snippet
        if serp_features.get("featured_snippet"):
            visual_position += 2.0
        
        # AI Overview
        if serp_features.get("ai_overview"):
            visual_position += 3.0
        
        # People Also Ask
        paa_count = len(serp_features.get("people_also_ask", []))
        visual_position += paa_count * 0.5
        
        # Knowledge Graph
        if serp_features.get("knowledge_graph"):
            visual_position += 1.0
        
        # Local Pack
        if serp_features.get("local_pack"):
            visual_position += 3.0
        
        # Video
        if serp_features.get("video"):
            visual_position += 1.0
        
        # Images
        if serp_features.get("image"):
            visual_position += 0.5
        
        # Shopping
        if serp_features.get("shopping"):
            visual_position += 1.0
        
        # Top Stories
        if serp_features.get("top_stories"):
            visual_position += 2.0
        
        return visual_position
    
    async def get_competitor_domains(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
        min_keyword_frequency: int = 2,
        top_n: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Identify competitor domains appearing frequently across keywords.
        
        Args:
            keywords: List of keywords to analyze
            location_code: DataForSEO location code
            language_code: Language code
            min_keyword_frequency: Minimum number of keywords domain must appear in
            top_n: Number of top positions to consider (e.g., top 10)
            
        Returns:
            List of competitor domains with frequency and average position
        """
        # Fetch SERP results
        serp_results = await self.fetch_serp_results(
            keywords=keywords,
            location_code=location_code,
            language_code=language_code,
        )
        
        # Count domain appearances
        domain_stats: Dict[str, Dict[str, Any]] = {}
        
        for serp_result in serp_results:
            organic_results = self.extract_organic_results(serp_result)
            
            # Only consider top N positions
            for result in organic_results[:top_n]:
                domain = result.get("domain", "")
                position = result.get("position", 0)
                
                if not domain:
                    continue
                
                if domain not in domain_stats:
                    domain_stats[domain] = {
                        "domain": domain,
                        "keyword_count": 0,
                        "positions": [],
                        "keywords": [],
                    }
                
                domain_stats[domain]["keyword_count"] += 1
                domain_stats[domain]["positions"].append(position)
                domain_stats[domain]["keywords"].append(serp_result["keyword"])
        
        # Calculate average positions and filter by frequency
        competitors = []
        for domain, stats in domain_stats.items():
            if stats["keyword_count"] >= min_keyword_frequency:
                avg_position = sum(stats["positions"]) / len(stats["positions"])
                competitors.append({
                    "domain": domain,
                    "keyword_count": stats["keyword_count"],
                    "keyword_frequency": stats["keyword_count"] / len(keywords),
                    "avg_position": round(avg_position, 2),
                    "keywords": stats["keywords"],
                })
        
        # Sort by keyword count descending
        competitors.sort(key=lambda x: x["keyword_count"], reverse=True)
        
        logger.info(f"Identified {len(competitors)} competitor domains")
        return competitors
    
    async def analyze_serp_landscape(
        self,
        keywords: List[str],
        user_domain: str,
        location_code: int = 2840,
        language_code: str = "en",
    ) -> Dict[str, Any]:
        """
        Comprehensive SERP landscape analysis for a set of keywords.
        
        Provides:
        - SERP feature displacement analysis
        - Competitor domain mapping
        - Visual vs organic position analysis
        - Click share estimation
        
        Args:
            keywords: List of keywords to analyze
            user_domain: User's domain for position tracking
            location_code: DataForSEO location code
            language_code: Language code
            
        Returns:
            Comprehensive SERP landscape analysis dictionary
        """
        # Fetch SERP results
        serp_results = await self.fetch_serp_results(
            keywords=keywords,
            location_code=location_code,
            language_code=language_code,
        )
        
        analysis = {
            "keywords_analyzed": len(serp_results),
            "serp_feature_displacement": [],
            "competitors": [],
            "total_visual_displacement": 0.0,
            "avg_visual_displacement": 0.0,
        }
        
        total_displacement = 0.0
        displacement_count = 0
        
        # Analyze each keyword
        for serp_result in serp_results:
            keyword = serp_result["keyword"]
            organic_results = self.extract_organic_results(serp_result)
            serp_features = self.extract_serp_features(serp_result)
            
            # Find user's position
            user_position = None
            for result in organic_results:
                if user_domain in result.get("domain", ""):
                    user_position = result.get("position", 0)
                    break
            
            if user_position:
                visual_position = self.calculate_visual_position(
                    user_position,
                    serp_features,
                )
                
                displacement = visual_position - user_position
                
                if displacement > 1.0:  # Significant displacement
                    features_above = []
                    
                    if serp_features.get("featured_snippet"):
                        features_above.append("featured_snippet")
                    if serp_features.get("ai_overview"):
                        features_above.append("ai_overview")
                    if serp_features.get("people_also_ask"):
                        features_above.append(f"paa_x{len(serp_features['people_also_ask'])}")
                    if serp_features.get("knowledge_graph"):
                        features_above.append("knowledge_graph")
                    if serp_features.get("local_pack"):
                        features_above.append("local_pack")
                    if serp_features.get("video"):
                        features_above.append("video")
                    if serp_features.get("shopping"):
                        features_above.append("shopping")
                    if serp_features.get("top_stories"):
                        features_above.append("top_stories")
                    
                    analysis["serp_feature_displacement"].append({
                        "keyword": keyword,
                        "organic_position": user_position,
                        "visual_position": round(visual_position, 1),
                        "displacement": round(displacement, 1),
                        "features_above": features_above,
                    })
                
                total_displacement += displacement
                displacement_count += 1
        
        # Calculate average displacement
        if displacement_count > 0:
            analysis["avg_visual_displacement"] = round(
                total_displacement / displacement_count, 2
            )
        
        # Get competitor domains
        competitors = await self.get_competitor_domains(
            keywords=keywords,
            location_code=location_code,
            language_code=language_code,
        )
        
        # Filter out user's domain
        analysis["competitors"] = [
            c for c in competitors if user_domain not in c["domain"]
        ]
        
        logger.info(f"SERP landscape analysis complete for {len(keywords)} keywords")
        return analysis