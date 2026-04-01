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
                "Set DATAFORSEO_LOGIN and DATAFORSEO_PASSWORD environment variables "
                "or pass credentials to constructor."
            )
        
        self.timeout = timeout
        self.max_retries = max_retries
        self.supabase = supabase_client
        self.cache_ttl_hours = cache_ttl_hours
        
        # Rate limiting: DataForSEO allows ~2000 requests/hour
        self.rate_limit_semaphore = asyncio.Semaphore(10)  # 10 concurrent requests
        self.request_timestamps: List[float] = []
        self.max_requests_per_minute = 30
        
        self.client: Optional[httpx.AsyncClient] = None
    
    async def __aenter__(self):
        """Async context manager entry"""
        await self.authenticate()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close()
    
    async def authenticate(self):
        """Initialize HTTP client with authentication"""
        if self.client is None:
            self.client = httpx.AsyncClient(
                auth=(self.login, self.password),
                timeout=httpx.Timeout(self.timeout),
                headers={
                    "Content-Type": "application/json",
                }
            )
            logger.info("DataForSEO client authenticated")
    
    async def close(self):
        """Close HTTP client"""
        if self.client:
            await self.client.aclose()
            self.client = None
            logger.info("DataForSEO client closed")
    
    def _generate_cache_key(self, endpoint: str, params: Dict[str, Any]) -> str:
        """Generate cache key from endpoint and parameters"""
        params_str = json.dumps(params, sort_keys=True)
        key_content = f"{endpoint}:{params_str}"
        return hashlib.md5(key_content.encode()).hexdigest()
    
    async def _get_cached_response(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """Retrieve cached response from Supabase"""
        if not self.supabase:
            return None
        
        try:
            result = self.supabase.table("api_cache").select("*").eq(
                "cache_key", cache_key
            ).gte(
                "expires_at", datetime.utcnow().isoformat()
            ).execute()
            
            if result.data and len(result.data) > 0:
                logger.info(f"Cache hit for key: {cache_key}")
                return result.data[0]["response_data"]
        except Exception as e:
            logger.warning(f"Cache retrieval error: {e}")
        
        return None
    
    async def _cache_response(self, cache_key: str, response_data: Dict[str, Any]):
        """Store response in Supabase cache"""
        if not self.supabase:
            return
        
        try:
            expires_at = datetime.utcnow() + timedelta(hours=self.cache_ttl_hours)
            
            self.supabase.table("api_cache").upsert({
                "cache_key": cache_key,
                "endpoint": "dataforseo",
                "response_data": response_data,
                "expires_at": expires_at.isoformat(),
                "created_at": datetime.utcnow().isoformat(),
            }).execute()
            
            logger.info(f"Cached response for key: {cache_key}")
        except Exception as e:
            logger.warning(f"Cache storage error: {e}")
    
    async def _check_rate_limit(self):
        """Ensure we don't exceed rate limits"""
        now = asyncio.get_event_loop().time()
        
        # Remove timestamps older than 1 minute
        self.request_timestamps = [
            ts for ts in self.request_timestamps
            if now - ts < 60
        ]
        
        # Wait if we've hit the rate limit
        if len(self.request_timestamps) >= self.max_requests_per_minute:
            sleep_time = 60 - (now - self.request_timestamps[0])
            if sleep_time > 0:
                logger.warning(f"Rate limit reached, sleeping for {sleep_time:.2f}s")
                await asyncio.sleep(sleep_time)
                self.request_timestamps = []
        
        self.request_timestamps.append(now)
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.NetworkError)),
    )
    async def _make_request(
        self,
        endpoint: str,
        payload: List[Dict[str, Any]],
        use_cache: bool = True,
    ) -> Dict[str, Any]:
        """
        Make API request with rate limiting, retries, and caching.
        
        Args:
            endpoint: API endpoint path (e.g., "/serp/google/organic/live/advanced")
            payload: Request payload (list of task objects)
            use_cache: Whether to use cache
        
        Returns:
            Parsed API response
        
        Raises:
            DataForSEOAuthError: Authentication failed
            DataForSEORateLimitError: Rate limit exceeded
            DataForSEOError: Other API errors
        """
        if not self.client:
            await self.authenticate()
        
        # Check cache
        if use_cache:
            cache_key = self._generate_cache_key(endpoint, payload[0] if payload else {})
            cached = await self._get_cached_response(cache_key)
            if cached:
                return cached
        
        # Rate limiting
        async with self.rate_limit_semaphore:
            await self._check_rate_limit()
            
            url = f"{self.BASE_URL}{endpoint}"
            
            try:
                response = await self.client.post(url, json=payload)
                response.raise_for_status()
                
                data = response.json()
                
                # Check for API-level errors
                if data.get("status_code") == 40101:
                    raise DataForSEOAuthError("Authentication failed")
                elif data.get("status_code") == 50000:
                    raise DataForSEORateLimitError("Rate limit exceeded")
                elif data.get("status_code") != 20000:
                    raise DataForSEOError(
                        f"API error: {data.get('status_message', 'Unknown error')}"
                    )
                
                # Cache successful response
                if use_cache and data.get("tasks"):
                    await self._cache_response(cache_key, data)
                
                return data
                
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 401:
                    raise DataForSEOAuthError("Invalid credentials")
                elif e.response.status_code == 429:
                    raise DataForSEORateLimitError("Rate limit exceeded")
                else:
                    raise DataForSEOError(f"HTTP error: {e}")
            except httpx.TimeoutException:
                logger.warning(f"Request timeout for {endpoint}")
                raise
            except httpx.NetworkError as e:
                logger.warning(f"Network error for {endpoint}: {e}")
                raise
    
    def _parse_serp_features(self, items: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Parse SERP features from result items.
        
        Returns:
            {
                "featured_snippet": bool,
                "people_also_ask": int (count),
                "knowledge_graph": bool,
                "local_pack": bool,
                "video": bool,
                "image": bool,
                "shopping": bool,
                "top_stories": bool,
                "ai_overview": bool,
                "related_searches": bool,
                "features_above_position": int (number of SERP elements before organic #1)
            }
        """
        features = {
            "featured_snippet": False,
            "people_also_ask": 0,
            "knowledge_graph": False,
            "local_pack": False,
            "video": False,
            "image": False,
            "shopping": False,
            "top_stories": False,
            "ai_overview": False,
            "related_searches": False,
            "features_above_position": 0,
        }
        
        first_organic_position = None
        
        for item in items:
            item_type = item.get("type", "")
            rank_absolute = item.get("rank_absolute", 999)
            
            # Track first organic position
            if item_type == "organic" and first_organic_position is None:
                first_organic_position = rank_absolute
            
            # Detect features
            for feature_name, feature_types in self.SERP_FEATURE_TYPES.items():
                if item_type in feature_types:
                    if feature_name == "people_also_ask":
                        features["people_also_ask"] += 1
                    else:
                        features[feature_name] = True
                    
                    # Count features appearing before first organic result
                    if first_organic_position and rank_absolute < first_organic_position:
                        if feature_name == "people_also_ask":
                            features["features_above_position"] += 0.5
                        elif feature_name == "featured_snippet":
                            features["features_above_position"] += 2
                        else:
                            features["features_above_position"] += 1
        
        return features
    
    def _extract_competitors(
        self,
        items: List[Dict[str, Any]],
        exclude_domain: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Extract competitor domains from organic results.
        
        Args:
            items: SERP result items
            exclude_domain: Domain to exclude (user's own site)
        
        Returns:
            List of competitor dictionaries with domain, position, url, title
        """
        competitors = []
        
        for item in items:
            if item.get("type") != "organic":
                continue
            
            domain = item.get("domain")
            if not domain:
                continue
            
            # Exclude user's own domain
            if exclude_domain and exclude_domain.lower() in domain.lower():
                continue
            
            competitors.append({
                "domain": domain,
                "position": item.get("rank_absolute"),
                "url": item.get("url"),
                "title": item.get("title"),
            })
        
        return competitors
    
    async def fetch_serp_results(
        self,
        keywords: List[str],
        location_code: int = 2840,  # United States
        language_code: str = "en",
        device: str = "desktop",
        depth: int = 100,  # Number of results to retrieve
        use_cache: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Fetch live SERP results for multiple keywords.
        
        Args:
            keywords: List of keywords to fetch SERPs for
            location_code: DataForSEO location code (2840 = US)
            language_code: Language code (e.g., "en")
            device: Device type ("desktop" or "mobile")
            depth: Number of results to retrieve per keyword
            use_cache: Whether to use cached results
        
        Returns:
            List of result dictionaries, one per keyword:
            [
                {
                    "keyword": "best crm software",
                    "location_code": 2840,
                    "language_code": "en",
                    "device": "desktop",
                    "serp_features": {...},
                    "organic_results": [...],
                    "competitors": [...],
                    "total_results": 1000000,
                }
            ]
        """
        results = []
        
        # Process keywords in batches (DataForSEO allows multiple tasks per request)
        batch_size = 10
        for i in range(0, len(keywords), batch_size):
            batch = keywords[i:i + batch_size]
            
            payload = []
            for keyword in batch:
                payload.append({
                    "keyword": keyword,
                    "location_code": location_code,
                    "language_code": language_code,
                    "device": device,
                    "depth": depth,
                    "calculate_rectangles": True,  # For SERP feature positioning
                })
            
            try:
                response = await self._make_request(
                    "/serp/google/organic/live/advanced",
                    payload,
                    use_cache=use_cache,
                )
                
                for task in response.get("tasks", []):
                    if task.get("status_code") != 20000:
                        logger.warning(
                            f"Task failed for keyword: {task.get('data', {}).get('keyword')} - "
                            f"{task.get('status_message')}"
                        )
                        continue
                    
                    task_result = task.get("result", [{}])[0]
                    keyword = task_result.get("keyword")
                    items = task_result.get("items", [])
                    
                    # Parse SERP features
                    serp_features = self._parse_serp_features(items)
                    
                    # Extract organic results
                    organic_results = [
                        {
                            "position": item.get("rank_absolute"),
                            "url": item.get("url"),
                            "domain": item.get("domain"),
                            "title": item.get("title"),
                            "description": item.get("description"),
                        }
                        for item in items
                        if item.get("type") == "organic"
                    ]
                    
                    # Extract competitors (top 10 organic results)
                    competitors = self._extract_competitors(items[:20])
                    
                    results.append({
                        "keyword": keyword,
                        "location_code": location_code,
                        "language_code": language_code,
                        "device": device,
                        "serp_features": serp_features,
                        "organic_results": organic_results,
                        "competitors": competitors,
                        "total_results": task_result.get("items_count", 0),
                    })
                
            except DataForSEOError as e:
                logger.error(f"Error fetching SERP results for batch: {e}")
                # Continue with next batch
                continue
        
        return results
    
    async def analyze_keyword_serp(
        self,
        keyword: str,
        location_code: int = 2840,
        language_code: str = "en",
        user_domain: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Comprehensive SERP analysis for a single keyword.
        
        Args:
            keyword: Keyword to analyze
            location_code: DataForSEO location code
            language_code: Language code
            user_domain: User's domain (to calculate position)
        
        Returns:
            {
                "keyword": "best crm software",
                "user_position": 5,
                "user_url": "https://example.com/blog/best-crm",
                "visual_position": 8,  # Accounting for SERP features
                "serp_features": {...},
                "competitors": [...],
                "intent_classification": "commercial",
                "click_share_estimate": 0.08,
            }
        """
        results = await self.fetch_serp_results(
            [keyword],
            location_code=location_code,
            language_code=language_code,
        )
        
        if not results:
            raise DataForSEOError(f"No results returned for keyword: {keyword}")
        
        result = results[0]
        
        # Find user's position
        user_position = None
        user_url = None
        
        if user_domain:
            for item in result["organic_results"]:
                if user_domain.lower() in item["domain"].lower():
                    user_position = item["position"]
                    user_url = item["url"]
                    break
        
        # Calculate visual position (accounting for SERP features)
        visual_position = user_position
        if user_position:
            visual_position = user_position + result["serp_features"]["features_above_position"]
        
        # Classify intent based on SERP composition
        intent = self._classify_serp_intent(result["serp_features"])
        
        # Estimate click share (simplified model)
        click_share = self._estimate_click_share(
            user_position,
            visual_position,
            result["serp_features"],
        ) if user_position else 0.0
        
        return {
            "keyword": keyword,
            "user_position": user_position,
            "user_url": user_url,
            "visual_position": visual_position,
            "serp_features": result["serp_features"],
            "competitors": result["competitors"],
            "intent_classification": intent,
            "click_share_estimate": click_share,
        }
    
    def _classify_serp_intent(self, serp_features: Dict[str, Any]) -> str:
        """
        Classify search intent based on SERP features.
        
        Returns: "informational", "commercial", "transactional", or "navigational"
        """
        # Transactional signals
        if serp_features.get("shopping") or serp_features.get("local_pack"):
            return "transactional"
        
        # Navigational signals
        if serp_features.get("knowledge_graph"):
            return "navigational"
        
        # Informational signals
        if serp_features.get("people_also_ask", 0) >= 3 or serp_features.get("featured_snippet"):
            return "informational"
        
        # Default to commercial
        return "commercial"
    
    def _estimate_click_share(
        self,
        organic_position: Optional[int],
        visual_position: Optional[float],
        serp_features: Dict[str, Any],
    ) -> float:
        """
        Estimate click share based on position and SERP features.
        
        Uses adjusted CTR curves based on research:
        - Position 1 (no features): ~28% CTR
        - Position 1 (with featured snippet above): ~15% CTR
        - Position 3: ~10% CTR
        - Position 5: ~5% CTR
        - Position 10: ~2% CTR
        """
        if not organic_position:
            return 0.0
        
        # Base CTR curve (no SERP features)
        base_ctrs = {
            1: 0.28, 2: 0.15, 3: 0.10, 4: 0.07, 5: 0.05,
            6: 0.04, 7: 0.03, 8: 0.025, 9: 0.022, 10: 0.02,
        }
        
        base_ctr = base_ctrs.get(organic_position, 0.01)
        
        # Adjust for SERP features (displacement factor)
        if visual_position and visual_position > organic_position:
            displacement = visual_position - organic_position
            adjustment_factor = max(0.4, 1 - (displacement * 0.15))
            base_ctr *= adjustment_factor
        
        return round(base_ctr, 4)
    
    async def batch_competitor_analysis(
        self,
        keywords: List[str],
        location_code: int = 2840,
        user_domain: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Analyze competitors across multiple keywords.
        
        Args:
            keywords: List of keywords to analyze
            location_code: DataForSEO location code
            user_domain: User's domain to exclude from competitor list
        
        Returns:
            {
                "keywords_analyzed": 50,
                "competitors": [
                    {
                        "domain": "competitor.com",
                        "keywords_shared": 23,
                        "avg_position": 4.2,
                        "positions": [3, 5, 4, 6, ...],
                        "threat_level": "high"
                    }
                ],
                "serp_feature_summary": {
                    "featured_snippet_count": 12,
                    "paa_avg": 3.4,
                    "ai_overview_count": 8,
                    ...
                }
            }
        """
        results = await self.fetch_serp_results(keywords, location_code=location_code)
        
        # Aggregate competitor data
        competitor_data: Dict[str, Dict[str, Any]] = {}
        
        for result in results:
            for competitor in result["competitors"]:
                domain = competitor["domain"]
                
                if user_domain and user_domain.lower() in domain.lower():
                    continue
                
                if domain not in competitor_data:
                    competitor_data[domain] = {
                        "domain": domain,
                        "keywords_shared": 0,
                        "positions": [],
                    }
                
                competitor_data[domain]["keywords_shared"] += 1
                competitor_data[domain]["positions"].append(competitor["position"])
        
        # Calculate averages and threat levels
        competitors = []
        for domain, data in competitor_data.items():
            avg_position = sum(data["positions"]) / len(data["positions"])
            
            # Threat level: based on # of shared keywords and average position
            if data["keywords_shared"] >= len(keywords) * 0.3 and avg_position <= 5:
                threat_level = "high"
            elif data["keywords_shared"] >= len(keywords) * 0.2 and avg_position <= 8:
                threat_level = "medium"
            else:
                threat_level = "low"
            
            competitors.append({
                "domain": domain,
                "keywords_shared": data["keywords_shared"],
                "avg_position": round(avg_position, 2),
                "positions": data["positions"],
                "threat_level": threat_level,
            })
        
        # Sort by keywords shared (descending)
        competitors.sort(key=lambda x: x["keywords_shared"], reverse=True)
        
        # Aggregate SERP feature data
        serp_feature_summary = {
            "featured_snippet_count": sum(1 for r in results if r["serp_features"]["featured_snippet"]),
            "paa_avg": sum(r["serp_features"]["people_also_ask"] for r in results) / len(results) if results else 0,
            "ai_overview_count": sum(1 for r in results if r["serp_features"]["ai_overview"]),
            "knowledge_graph_count": sum(1 for r in results if r["serp_features"]["knowledge_graph"]),
            "local_pack_count": sum(1 for r in results if r["serp_features"]["local_pack"]),
            "video_count": sum(1 for r in results if r["serp_features"]["video"]),
            "shopping_count": sum(1 for r in results if r["serp_features"]["shopping"]),
        }
        
        return {
            "keywords_analyzed": len(results),
            "competitors": competitors,
            "serp_feature_summary": serp_feature_summary,
        }