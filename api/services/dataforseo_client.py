import os
import asyncio
import logging
from typing import List, Dict, Any, Optional, Set, Tuple
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
import re

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
    - Live SERP results retrieval (Google organic)
    - Keyword difficulty and volume lookups
    - Competitor domain analysis (top ranking domains)
    - SERP feature detection (featured snippets, PAA, knowledge panels, AI Overview, etc.)
    - Batch processing of multiple keywords
    - Response caching via Supabase
    - Visual position calculation (accounting for SERP features)
    
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
    
    # SERP feature type mappings (DataForSEO item types)
    SERP_FEATURE_TYPES = {
        "featured_snippet": ["featured_snippet", "answer_box"],
        "people_also_ask": ["people_also_ask"],
        "knowledge_graph": ["knowledge_graph"],
        "local_pack": ["local_pack", "map"],
        "video": ["video", "video_carousel"],
        "image": ["images"],
        "shopping": ["shopping", "google_shopping"],
        "top_stories": ["top_stories"],
        "twitter": ["twitter"],
        "recipes": ["recipes"],
        "ai_overview": ["ai_overview"],
        "related_searches": ["people_also_search", "related_searches"],
        "hotels_pack": ["hotels_pack"],
        "flights": ["google_flights"],
        "jobs": ["jobs"],
        "events": ["events"],
        "find_results_on": ["find_results_on"],
    }
    
    # Visual position impact per SERP feature (positions pushed down)
    SERP_FEATURE_VISUAL_IMPACT = {
        "featured_snippet": 2.0,
        "knowledge_graph": 0.0,  # Usually on the right side
        "people_also_ask": 0.5,  # Per question
        "local_pack": 3.0,
        "video": 1.5,
        "image": 1.0,
        "shopping": 2.0,
        "top_stories": 2.5,
        "ai_overview": 3.0,
        "twitter": 1.0,
        "recipes": 1.5,
        "hotels_pack": 3.0,
        "flights": 2.0,
        "jobs": 2.0,
        "events": 1.5,
        "find_results_on": 0.5,
        "related_searches": 0.0,  # Usually at bottom
    }
    
    # Rate limiting
    RATE_LIMIT_CALLS_PER_SECOND = 2
    RATE_LIMIT_CALLS_PER_MINUTE = 100
    
    # Retry configuration
    MAX_RETRIES = 3
    RETRY_MIN_WAIT = 1
    RETRY_MAX_WAIT = 10
    
    def __init__(
        self,
        login: Optional[str] = None,
        password: Optional[str] = None,
        cache_enabled: bool = True,
        cache_ttl_hours: int = 24,
    ):
        """
        Initialize DataForSEO API client.
        
        Args:
            login: DataForSEO API login (defaults to DATAFORSEO_LOGIN env var)
            password: DataForSEO API password (defaults to DATAFORSEO_PASSWORD env var)
            cache_enabled: Whether to use caching for API responses
            cache_ttl_hours: Cache TTL in hours
        """
        self.login = login or os.getenv("DATAFORSEO_LOGIN")
        self.password = password or os.getenv("DATAFORSEO_PASSWORD")
        
        if not self.login or not self.password:
            raise DataForSEOAuthError(
                "DataForSEO credentials not provided. Set DATAFORSEO_LOGIN and "
                "DATAFORSEO_PASSWORD environment variables or pass them to the constructor."
            )
        
        self.cache_enabled = cache_enabled
        self.cache_ttl_hours = cache_ttl_hours
        
        # Rate limiting state
        self._call_times_second: List[float] = []
        self._call_times_minute: List[float] = []
        self._rate_limit_lock = asyncio.Lock()
        
        # HTTP client (initialized on first use)
        self._client: Optional[httpx.AsyncClient] = None
        
        # Supabase client for caching (initialized when needed)
        self._supabase = None
    
    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client with authentication"""
        if self._client is None:
            self._client = httpx.AsyncClient(
                auth=(self.login, self.password),
                timeout=httpx.Timeout(30.0),
                limits=httpx.Limits(max_keepalive_connections=5, max_connections=10),
            )
        return self._client
    
    async def close(self):
        """Close HTTP client and cleanup resources"""
        if self._client:
            await self._client.aclose()
            self._client = None
    
    async def __aenter__(self):
        """Async context manager entry"""
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close()
    
    def _get_supabase(self):
        """Lazy initialization of Supabase client"""
        if self._supabase is None and self.cache_enabled:
            try:
                from supabase import create_client
                url = os.getenv("SUPABASE_URL")
                key = os.getenv("SUPABASE_SERVICE_KEY")
                if url and key:
                    self._supabase = create_client(url, key)
                else:
                    logger.warning("Supabase credentials not found, caching disabled")
                    self.cache_enabled = False
            except ImportError:
                logger.warning("Supabase client not installed, caching disabled")
                self.cache_enabled = False
        return self._supabase
    
    async def _enforce_rate_limit(self):
        """Enforce rate limiting before making API calls"""
        async with self._rate_limit_lock:
            now = asyncio.get_event_loop().time()
            
            # Clean old timestamps
            self._call_times_second = [
                t for t in self._call_times_second if now - t < 1.0
            ]
            self._call_times_minute = [
                t for t in self._call_times_minute if now - t < 60.0
            ]
            
            # Check per-second limit
            if len(self._call_times_second) >= self.RATE_LIMIT_CALLS_PER_SECOND:
                wait_time = 1.0 - (now - self._call_times_second[0])
                if wait_time > 0:
                    logger.debug(f"Rate limit: waiting {wait_time:.2f}s")
                    await asyncio.sleep(wait_time)
                    now = asyncio.get_event_loop().time()
                    self._call_times_second = [
                        t for t in self._call_times_second if now - t < 1.0
                    ]
            
            # Check per-minute limit
            if len(self._call_times_minute) >= self.RATE_LIMIT_CALLS_PER_MINUTE:
                wait_time = 60.0 - (now - self._call_times_minute[0])
                if wait_time > 0:
                    logger.debug(f"Rate limit: waiting {wait_time:.2f}s")
                    await asyncio.sleep(wait_time)
                    now = asyncio.get_event_loop().time()
                    self._call_times_minute = [
                        t for t in self._call_times_minute if now - t < 60.0
                    ]
            
            # Record this call
            self._call_times_second.append(now)
            self._call_times_minute.append(now)
    
    def _get_cache_key(self, endpoint: str, params: Dict[str, Any]) -> str:
        """Generate cache key for request"""
        key_data = f"{endpoint}:{json.dumps(params, sort_keys=True)}"
        return hashlib.sha256(key_data.encode()).hexdigest()
    
    async def _get_cached_response(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """Retrieve cached response if available and not expired"""
        if not self.cache_enabled:
            return None
        
        supabase = self._get_supabase()
        if not supabase:
            return None
        
        try:
            result = supabase.table("dataforseo_cache").select("*").eq(
                "cache_key", cache_key
            ).single().execute()
            
            if result.data:
                cached_at = datetime.fromisoformat(result.data["cached_at"])
                if datetime.utcnow() - cached_at < timedelta(hours=self.cache_ttl_hours):
                    logger.debug(f"Cache hit for key {cache_key[:16]}...")
                    return result.data["response_data"]
                else:
                    logger.debug(f"Cache expired for key {cache_key[:16]}...")
        except Exception as e:
            logger.warning(f"Cache retrieval error: {e}")
        
        return None
    
    async def _cache_response(self, cache_key: str, response: Dict[str, Any]):
        """Cache API response"""
        if not self.cache_enabled:
            return
        
        supabase = self._get_supabase()
        if not supabase:
            return
        
        try:
            supabase.table("dataforseo_cache").upsert({
                "cache_key": cache_key,
                "response_data": response,
                "cached_at": datetime.utcnow().isoformat(),
            }).execute()
            logger.debug(f"Cached response for key {cache_key[:16]}...")
        except Exception as e:
            logger.warning(f"Cache storage error: {e}")
    
    @retry(
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential(multiplier=RETRY_MIN_WAIT, max=RETRY_MAX_WAIT),
        retry=retry_if_exception_type((httpx.RequestError, DataForSEORateLimitError)),
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
        Make authenticated request to DataForSEO API with retry logic.
        
        Args:
            endpoint: API endpoint path (e.g., "/serp/google/organic/live/advanced")
            method: HTTP method
            data: Request payload (for POST requests)
            use_cache: Whether to use caching for this request
        
        Returns:
            Parsed JSON response
        
        Raises:
            DataForSEOError: On API errors
            DataForSEORateLimitError: On rate limit errors (triggers retry)
            DataForSEOAuthError: On authentication errors
        """
        # Check cache first
        cache_key = None
        if use_cache and method == "POST" and data:
            cache_key = self._get_cache_key(endpoint, data[0] if data else {})
            cached_response = await self._get_cached_response(cache_key)
            if cached_response:
                return cached_response
        
        # Enforce rate limiting
        await self._enforce_rate_limit()
        
        # Make request
        client = await self._get_client()
        url = f"{self.BASE_URL}{endpoint}"
        
        try:
            if method == "POST":
                response = await client.post(url, json=data)
            else:
                response = await client.get(url)
            
            response.raise_for_status()
            
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                raise DataForSEOAuthError("Authentication failed. Check credentials.")
            elif e.response.status_code == 429:
                raise DataForSEORateLimitError("Rate limit exceeded")
            else:
                raise DataForSEOError(
                    f"API request failed: {e.response.status_code} - {e.response.text}"
                )
        except httpx.RequestError as e:
            raise DataForSEOError(f"Request error: {str(e)}")
        
        # Parse response
        try:
            result = response.json()
        except json.JSONDecodeError as e:
            raise DataForSEOError(f"Invalid JSON response: {str(e)}")
        
        # Check API-level errors
        if result.get("status_code") != 20000:
            error_msg = result.get("status_message", "Unknown error")
            raise DataForSEOError(f"API error: {error_msg}")
        
        # Cache successful response
        if cache_key and use_cache:
            await self._cache_response(cache_key, result)
        
        return result
    
    async def authenticate(self) -> bool:
        """
        Verify authentication by making a test request.
        
        Returns:
            True if authentication is successful
        
        Raises:
            DataForSEOAuthError: If authentication fails
        """
        try:
            await self._make_request("/appendix/user_data", method="GET", use_cache=False)
            logger.info("DataForSEO authentication successful")
            return True
        except DataForSEOAuthError:
            raise
        except Exception as e:
            raise DataForSEOAuthError(f"Authentication check failed: {str(e)}")
    
    def _normalize_domain(self, url_or_domain: str) -> str:
        """Extract clean domain from URL or domain string"""
        # Remove protocol
        domain = re.sub(r"^https?://", "", url_or_domain)
        # Remove www
        domain = re.sub(r"^www\.", "", domain)
        # Remove path
        domain = domain.split("/")[0]
        # Remove trailing dots
        domain = domain.rstrip(".")
        return domain.lower()
    
    def _calculate_visual_position(
        self, organic_rank: int, serp_features_above: List[Dict[str, Any]]
    ) -> float:
        """
        Calculate visual position accounting for SERP features above the result.
        
        Args:
            organic_rank: Organic ranking position (1-100)
            serp_features_above: List of SERP features appearing above this result
        
        Returns:
            Visual position (e.g., 3.5 means visually appears at position 3.5)
        """
        visual_offset = 0.0
        
        for feature in serp_features_above:
            feature_type = feature.get("type", "")
            
            # Determine feature category
            for category, types in self.SERP_FEATURE_TYPES.items():
                if feature_type in types:
                    impact = self.SERP_FEATURE_VISUAL_IMPACT.get(category, 0.0)
                    
                    # Special handling for PAA (count individual questions)
                    if category == "people_also_ask":
                        question_count = len(feature.get("items", []))
                        visual_offset += impact * question_count
                    else:
                        visual_offset += impact
                    break
        
        return organic_rank + visual_offset
    
    def _classify_serp_intent(self, serp_features: List[str], items: List[Dict[str, Any]]) -> str:
        """
        Classify search intent based on SERP composition.
        
        Args:
            serp_features: List of SERP feature types present
            items: All SERP items
        
        Returns:
            Intent classification: 'informational', 'commercial', 'transactional', 'navigational'
        """
        # Count feature types
        has_shopping = any(f in serp_features for f in ["shopping", "google_shopping"])
        has_local = any(f in serp_features for f in ["local_pack", "map"])
        has_knowledge = "knowledge_graph" in serp_features
        has_paa = "people_also_ask" in serp_features
        has_featured_snippet = any(f in serp_features for f in ["featured_snippet", "answer_box"])
        
        # Check if top results are from a single domain (navigational)
        top_domains = []
        for item in items[:3]:
            if item.get("type") == "organic":
                domain = self._normalize_domain(item.get("url", ""))
                top_domains.append(domain)
        
        if len(set(top_domains)) == 1 and has_knowledge:
            return "navigational"
        
        # Shopping/local = transactional
        if has_shopping or has_local:
            return "transactional"
        
        # PAA + featured snippet = informational
        if (has_paa or has_featured_snippet) and not has_shopping:
            return "informational"
        
        # Review/comparison signals = commercial
        organic_titles = [
            item.get("title", "").lower() 
            for item in items[:10] 
            if item.get("type") == "organic"
        ]
        commercial_keywords = ["best", "top", "review", "vs", "compare", "alternative"]
        if any(kw in " ".join(organic_titles) for kw in commercial_keywords):
            return "commercial"
        
        # Default
        return "informational"
    
    async def get_serp_data(
        self,
        keyword: str,
        location_code: int = 2840,  # USA
        language_code: str = "en",
        device: str = "desktop",
        depth: int = 100,
    ) -> Dict[str, Any]:
        """
        Fetch live SERP data for a single keyword.
        
        Args:
            keyword: Search keyword
            location_code: DataForSEO location code (2840 = USA)
            language_code: Language code (e.g., "en")
            device: Device type ("desktop" or "mobile")
            depth: Number of results to retrieve (max 100)
        
        Returns:
            Parsed SERP data with structure:
            {
                "keyword": str,
                "location_code": int,
                "language_code": str,
                "device": str,
                "organic_results": [
                    {
                        "position": int,
                        "url": str,
                        "domain": str,
                        "title": str,
                        "description": str,
                        "visual_position": float,
                    }
                ],
                "serp_features": [
                    {
                        "type": str,
                        "position": int,
                        "data": dict,
                    }
                ],
                "intent_classification": str,
                "total_results": int,
                "check_url": str,
            }
        """
        payload = [{
            "keyword": keyword,
            "location_code": location_code,
            "language_code": language_code,
            "device": device,
            "depth": depth,
        }]
        
        response = await self._make_request(
            "/serp/google/organic/live/advanced",
            method="POST",
            data=payload,
        )
        
        # Extract task results
        tasks = response.get("tasks", [])
        if not tasks or not tasks[0].get("result"):
            raise DataForSEOError("No results returned from API")
        
        task_result = tasks[0]["result"][0]
        items = task_result.get("items", [])
        
        # Parse organic results
        organic_results = []
        serp_features = []
        serp_feature_types = []
        
        for item in items:
            item_type = item.get("type", "")
            
            if item_type == "organic":
                # Find SERP features above this result
                features_above = []
                for prev_item in items:
                    if prev_item.get("rank_absolute", 999) < item.get("rank_absolute", 0):
                        if prev_item.get("type") != "organic":
                            features_above.append(prev_item)
                
                organic_results.append({
                    "position": item.get("rank_absolute", 0),
                    "url": item.get("url", ""),
                    "domain": item.get("domain", ""),
                    "title": item.get("title", ""),
                    "description": item.get("description", ""),
                    "visual_position": self._calculate_visual_position(
                        item.get("rank_absolute", 0),
                        features_above
                    ),
                })
            else:
                # SERP feature
                serp_features.append({
                    "type": item_type,
                    "position": item.get("rank_absolute", 0),
                    "data": item,
                })
                serp_feature_types.append(item_type)
        
        # Classify intent
        intent = self._classify_serp_intent(serp_feature_types, items)
        
        return {
            "keyword": keyword,
            "location_code": location_code,
            "language_code": language_code,
            "device": device,
            "organic_results": organic_results,
            "serp_features": serp_features,
            "intent_classification": intent,
            "total_results": task_result.get("items_count", 0),
            "check_url": task_result.get("check_url", ""),
        }
    
    async def get_competitor_rankings(
        self,
        domain: str,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
        device: str = "desktop",
    ) -> Dict[str, Any]:
        """
        Analyze competitor rankings for a specific domain across multiple keywords.
        
        Args:
            domain: Target domain to analyze
            keywords: List of keywords to check
            location_code: DataForSEO location code
            language_code: Language code
            device: Device type
        
        Returns:
            {
                "domain": str,
                "keywords_analyzed": int,
                "rankings": [
                    {
                        "keyword": str,
                        "position": int or None,
                        "url": str or None,
                        "visual_position": float or None,
                        "serp_features_above": list,
                    }
                ],
                "average_position": float,
                "keywords_ranking": int,
                "top_3_count": int,
                "top_10_count": int,
                "competitor_domains": [  # Other domains found in these SERPs
                    {
                        "domain": str,
                        "appearances": int,
                        "avg_position": float,
                    }
                ],
            }
        """
        normalized_domain = self._normalize_domain(domain)
        rankings = []
        all_competitor_domains = {}
        
        # Process keywords in batches to avoid overwhelming the API
        batch_size = 20
        for i in range(0, len(keywords), batch_size):
            batch = keywords[i:i+batch_size]
            
            # Fetch SERP data for batch
            tasks = []
            for keyword in batch:
                tasks.append(self.get_serp_data(
                    keyword=keyword,
                    location_code=location_code,
                    language_code=language_code,
                    device=device,
                ))
            
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for keyword, result in zip(batch, batch_results):
                if isinstance(result, Exception):
                    logger.warning(f"Error fetching SERP for '{keyword}': {result}")
                    continue
                
                # Find target domain in results
                domain_result = None
                for org_result in result["organic_results"]:
                    if self._normalize_domain(org_result["domain"]) == normalized_domain:
                        domain_result = org_result
                        break
                
                # Collect competitor domains
                for org_result in result["organic_results"][:10]:
                    comp_domain = self._normalize_domain(org_result["domain"])
                    if comp_domain != normalized_domain:
                        if comp_domain not in all_competitor_domains:
                            all_competitor_domains[comp_domain] = []
                        all_competitor_domains[comp_domain].append(org_result["position"])
                
                # Record ranking
                if domain_result:
                    rankings.append({
                        "keyword": keyword,
                        "position": domain_result["position"],
                        "url": domain_result["url"],
                        "visual_position": domain_result["visual_position"],
                        "serp_features_above": [
                            f["type"] for f in result["serp_features"]
                            if f["position"] < domain_result["position"]
                        ],
                    })
                else:
                    rankings.append({
                        "keyword": keyword,
                        "position": None,
                        "url": None,
                        "visual_position": None,
                        "serp_features_above": [],
                    })
        
        # Calculate statistics
        ranked_positions = [r["position"] for r in rankings if r["position"] is not None]
        
        competitor_summary = []
        for comp_domain, positions in all_competitor_domains.items():
            competitor_summary.append({
                "domain": comp_domain,
                "appearances": len(positions),
                "avg_position": sum(positions) / len(positions),
            })
        
        # Sort competitors by appearances
        competitor_summary.sort(key=lambda x: x["appearances"], reverse=True)
        
        return {
            "domain": domain,
            "keywords_analyzed": len(keywords),
            "rankings": rankings,
            "average_position": sum(ranked_positions) / len(ranked_positions) if ranked_positions else None,
            "keywords_ranking": len(ranked_positions),
            "top_3_count": sum(1 for p in ranked_positions if p <= 3),
            "top_10_count": sum(1 for p in ranked_positions if p <= 10),
            "competitor_domains": competitor_summary[:20],  # Top 20 competitors
        }
    
    async def get_keyword_difficulty(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
    ) -> List[Dict[str, Any]]:
        """
        Get keyword difficulty and search volume for multiple keywords.
        
        Args:
            keywords: List of keywords to analyze (max 1000 per request)
            location_code: DataForSEO location code
            language_code: Language code
        
        Returns:
            List of keyword difficulty data:
            [
                {
                    "keyword": str,
                    "difficulty": int,  # 0-100 scale
                    "search_volume": int,
                    "cpc": float,
                    "competition": float,  # 0-1 scale
                    "monthly_searches": [  # Last 12 months
                        {"year": int, "month": int, "search_volume": int}
                    ],
                }
            ]
        """
        if len(keywords) > 1000:
            raise ValueError("Maximum 1000 keywords per request")
        
        payload = [{
            "keywords": keywords,
            "location_code": location_code,
            "language_code": language_code,
        }]
        
        response = await self._make_request(
            "/keywords_data/google_ads/search_volume/live",
            method="POST",
            data=payload,
        )
        
        # Parse results
        tasks = response.get("tasks", [])
        if not tasks or not tasks[0].get("result"):
            raise DataForSEOError("No keyword difficulty results returned")
        
        results = []
        for item in tasks[0]["result"]:
            results.append({
                "keyword": item.get("keyword", ""),
                "difficulty": item.get("keyword_info", {}).get("difficulty", 0),
                "search_volume": item.get("keyword_info", {}).get("search_volume", 0),
                "cpc": item.get("keyword_info", {}).get("cpc", 0.0),
                "competition": item.get("keyword_info", {}).get("competition", 0.0),
                "monthly_searches": item.get("keyword_info", {}).get("monthly_searches", []),
            })
        
        return results
    
    async def batch_serp_fetch(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
        device: str = "desktop",
        max_concurrent: int = 5,
    ) -> List[Dict[str, Any]]:
        """
        Fetch SERP data for multiple keywords with concurrency control.
        
        Args:
            keywords: List of keywords
            location_code: DataForSEO location code
            language_code: Language code
            device: Device type
            max_concurrent: Maximum concurrent requests
        
        Returns:
            List of SERP data dictionaries (same format as get_serp_data)
        """
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def fetch_with_semaphore(keyword):
            async with semaphore:
                try:
                    return await self.get_serp_data(
                        keyword=keyword,
                        location_code=location_code,
                        language_code=language_code,
                        device=device,
                    )
                except Exception as e:
                    logger.error(f"Error fetching SERP for '{keyword}': {e}")
                    return None
        
        tasks = [fetch_with_semaphore(kw) for kw in keywords]
        results = await asyncio.gather(*tasks)
        
        # Filter out None results
        return [r for r in results if r is not None]
    
    async def analyze_serp_features_distribution(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
    ) -> Dict[str, Any]:
        """
        Analyze SERP features across a set of keywords.
        
        Args:
            keywords: List of keywords to analyze
            location_code: DataForSEO location code
            language_code: Language code
        
        Returns:
            {
                "keywords_analyzed": int,
                "feature_frequency": {
                    "featured_snippet": int,
                    "people_also_ask": int,
                    ...
                },
                "feature_percentage": {
                    "featured_snippet": float,
                    ...
                },
                "intent_distribution": {
                    "informational": int,
                    "commercial": int,
                    "transactional": int,
                    "navigational": int,
                },
                "avg_serp_features_per_keyword": float,
            }
        """
        serp_results = await self.batch_serp_fetch(
            keywords=keywords,
            location_code=location_code,
            language_code=language_code,
        )
        
        feature_counts = {}
        intent_counts = {"informational": 0, "commercial": 0, "transactional": 0, "navigational": 0}
        total_features = 0
        
        for result in serp_results:
            # Count features
            for feature in result["serp_features"]:
                feature_type = feature["type"]
                feature_counts[feature_type] = feature_counts.get(feature_type, 0) + 1
            
            total_features += len(result["serp_features"])
            
            # Count intent
            intent = result["intent_classification"]
            intent_counts[intent] += 1
        
        # Calculate percentages
        total_keywords = len(serp_results)
        feature_percentage = {
            feature: (count / total_keywords) * 100
            for feature, count in feature_counts.items()
        }
        
        return {
            "keywords_analyzed": total_keywords,
            "feature_frequency": feature_counts,
            "feature_percentage": feature_percentage,
            "intent_distribution": intent_counts,
            "avg_serp_features_per_keyword": total_features / total_keywords if total_keywords > 0 else 0,
        }