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
    
    # Rate limiting configuration
    MAX_REQUESTS_PER_SECOND = 2
    MAX_CONCURRENT_REQUESTS = 5
    BATCH_SIZE = 100  # Max keywords per batch request
    
    # Cache TTL configuration
    SERP_CACHE_TTL_HOURS = 24
    KEYWORD_DATA_CACHE_TTL_HOURS = 168  # 1 week
    
    def __init__(
        self,
        username: Optional[str] = None,
        password: Optional[str] = None,
        supabase_client=None,
        max_concurrent_requests: int = MAX_CONCURRENT_REQUESTS,
        rate_limit_per_second: int = MAX_REQUESTS_PER_SECOND,
    ):
        """
        Initialize DataForSEO client.
        
        Args:
            username: DataForSEO API username (defaults to env var DATAFORSEO_USERNAME)
            password: DataForSEO API password (defaults to env var DATAFORSEO_PASSWORD)
            supabase_client: Supabase client instance for caching
            max_concurrent_requests: Maximum concurrent API requests
            rate_limit_per_second: Maximum requests per second
        """
        self.username = username or os.getenv("DATAFORSEO_USERNAME")
        self.password = password or os.getenv("DATAFORSEO_PASSWORD")
        
        if not self.username or not self.password:
            raise DataForSEOAuthError(
                "DataForSEO credentials not provided. Set DATAFORSEO_USERNAME and "
                "DATAFORSEO_PASSWORD environment variables."
            )
        
        self.supabase = supabase_client
        self.max_concurrent_requests = max_concurrent_requests
        self.rate_limit_per_second = rate_limit_per_second
        
        # Rate limiting semaphore
        self._semaphore = asyncio.Semaphore(max_concurrent_requests)
        self._last_request_times: List[float] = []
        self._rate_limit_lock = asyncio.Lock()
        
        # HTTP client (created in async context)
        self._client: Optional[httpx.AsyncClient] = None
    
    async def __aenter__(self):
        """Async context manager entry"""
        await self.authenticate()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close()
    
    async def authenticate(self):
        """Initialize HTTP client with authentication"""
        if self._client is None:
            self._client = httpx.AsyncClient(
                auth=(self.username, self.password),
                timeout=httpx.Timeout(60.0, connect=10.0),
                limits=httpx.Limits(max_keepalive_connections=10, max_connections=20),
            )
        return self
    
    async def close(self):
        """Close HTTP client"""
        if self._client:
            await self._client.aclose()
            self._client = None
    
    async def _enforce_rate_limit(self):
        """Enforce rate limiting before making requests"""
        async with self._rate_limit_lock:
            now = asyncio.get_event_loop().time()
            
            # Remove requests older than 1 second
            self._last_request_times = [
                t for t in self._last_request_times if now - t < 1.0
            ]
            
            # If at rate limit, wait
            if len(self._last_request_times) >= self.rate_limit_per_second:
                sleep_time = 1.0 - (now - self._last_request_times[0])
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)
                # Clean up old requests after sleeping
                now = asyncio.get_event_loop().time()
                self._last_request_times = [
                    t for t in self._last_request_times if now - t < 1.0
                ]
            
            # Record this request
            self._last_request_times.append(now)
    
    def _get_cache_key(self, endpoint: str, params: Dict[str, Any]) -> str:
        """Generate cache key from endpoint and parameters"""
        # Sort params for consistent keys
        param_str = json.dumps(params, sort_keys=True)
        key_data = f"{endpoint}:{param_str}"
        return hashlib.sha256(key_data.encode()).hexdigest()
    
    async def _get_cached_response(
        self, cache_key: str, ttl_hours: int
    ) -> Optional[Dict[str, Any]]:
        """Retrieve cached response from Supabase if available and fresh"""
        if not self.supabase:
            return None
        
        try:
            result = (
                self.supabase.table("dataforseo_cache")
                .select("response_data, created_at")
                .eq("cache_key", cache_key)
                .maybe_single()
                .execute()
            )
            
            if result.data:
                created_at = datetime.fromisoformat(result.data["created_at"])
                age_hours = (datetime.utcnow() - created_at).total_seconds() / 3600
                
                if age_hours < ttl_hours:
                    logger.info(f"Cache hit for key {cache_key[:12]}... (age: {age_hours:.1f}h)")
                    return result.data["response_data"]
                else:
                    logger.info(f"Cache expired for key {cache_key[:12]}... (age: {age_hours:.1f}h)")
            
            return None
        except Exception as e:
            logger.warning(f"Cache retrieval error: {e}")
            return None
    
    async def _cache_response(self, cache_key: str, response_data: Dict[str, Any]):
        """Store response in Supabase cache"""
        if not self.supabase:
            return
        
        try:
            self.supabase.table("dataforseo_cache").upsert(
                {
                    "cache_key": cache_key,
                    "response_data": response_data,
                    "created_at": datetime.utcnow().isoformat(),
                }
            ).execute()
            logger.info(f"Cached response for key {cache_key[:12]}...")
        except Exception as e:
            logger.warning(f"Cache storage error: {e}")
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((httpx.HTTPError, DataForSEORateLimitError)),
    )
    async def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
        use_cache: bool = True,
        cache_ttl_hours: int = SERP_CACHE_TTL_HOURS,
    ) -> Dict[str, Any]:
        """
        Make authenticated request to DataForSEO API with rate limiting and caching.
        
        Args:
            method: HTTP method (GET or POST)
            endpoint: API endpoint path
            data: Request payload for POST requests
            use_cache: Whether to use caching
            cache_ttl_hours: Cache TTL in hours
            
        Returns:
            API response data
            
        Raises:
            DataForSEOError: On API errors
            DataForSEORateLimitError: On rate limit exceeded
            DataForSEOAuthError: On authentication failure
        """
        if not self._client:
            await self.authenticate()
        
        # Check cache first
        cache_key = None
        if use_cache and data:
            cache_key = self._get_cache_key(endpoint, data)
            cached = await self._get_cached_response(cache_key, cache_ttl_hours)
            if cached:
                return cached
        
        # Enforce rate limiting and concurrency
        await self._enforce_rate_limit()
        
        async with self._semaphore:
            url = f"{self.BASE_URL}{endpoint}"
            
            try:
                if method.upper() == "POST":
                    response = await self._client.post(url, json=data)
                else:
                    response = await self._client.get(url)
                
                # Handle rate limiting
                if response.status_code == 429:
                    logger.warning("Rate limit exceeded, waiting before retry")
                    await asyncio.sleep(2)
                    raise DataForSEORateLimitError("Rate limit exceeded")
                
                # Handle auth errors
                if response.status_code == 401:
                    raise DataForSEOAuthError("Authentication failed")
                
                # Handle other errors
                response.raise_for_status()
                
                result = response.json()
                
                # Check API-level errors
                if result.get("status_code") != 20000:
                    error_msg = result.get("status_message", "Unknown error")
                    raise DataForSEOError(f"API error: {error_msg}")
                
                # Cache successful response
                if use_cache and cache_key:
                    await self._cache_response(cache_key, result)
                
                return result
                
            except httpx.HTTPError as e:
                logger.error(f"HTTP error: {e}")
                raise DataForSEOError(f"Request failed: {e}")
    
    def _extract_serp_features(
        self, items: List[Dict[str, Any]]
    ) -> Tuple[List[str], Dict[str, Any]]:
        """
        Extract SERP features from response items.
        
        Args:
            items: List of SERP items from API response
            
        Returns:
            Tuple of (feature_list, feature_details)
        """
        features = set()
        feature_details = {}
        
        for item in items:
            item_type = item.get("type", "")
            
            # Map item type to feature category
            for feature_name, item_types in self.SERP_FEATURE_TYPES.items():
                if item_type in item_types:
                    features.add(feature_name)
                    
                    # Store feature details
                    if feature_name not in feature_details:
                        feature_details[feature_name] = []
                    
                    feature_details[feature_name].append({
                        "type": item_type,
                        "rank_group": item.get("rank_group"),
                        "rank_absolute": item.get("rank_absolute"),
                        "title": item.get("title", ""),
                        "url": item.get("url", ""),
                    })
        
        return sorted(features), feature_details
    
    def _calculate_visual_position(
        self,
        organic_position: int,
        serp_features: List[str],
        feature_details: Dict[str, Any],
    ) -> float:
        """
        Calculate visual position accounting for SERP features.
        
        Args:
            organic_position: Actual organic ranking position
            serp_features: List of SERP features present
            feature_details: Detailed feature information
            
        Returns:
            Visual position (organic_position + displacement from features)
        """
        displacement = 0.0
        
        for feature in serp_features:
            if feature in self.SERP_FEATURE_VISUAL_IMPACT:
                base_impact = self.SERP_FEATURE_VISUAL_IMPACT[feature]
                
                # For PAA, multiply by number of questions
                if feature == "people_also_ask" and feature in feature_details:
                    paa_count = len(feature_details[feature])
                    displacement += base_impact * paa_count
                else:
                    displacement += base_impact
        
        return organic_position + displacement
    
    def _extract_organic_result(
        self, items: List[Dict[str, Any]], target_domain: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Extract organic result for target domain from SERP items.
        
        Args:
            items: List of SERP items
            target_domain: Domain to find (if None, returns top organic result)
            
        Returns:
            Organic result data or None
        """
        organic_items = [
            item for item in items if item.get("type") == "organic"
        ]
        
        if not organic_items:
            return None
        
        if target_domain:
            # Normalize domain for comparison
            target_domain = target_domain.lower().replace("www.", "")
            
            for item in organic_items:
                url = item.get("url", "")
                if target_domain in url.lower():
                    return {
                        "url": url,
                        "title": item.get("title", ""),
                        "description": item.get("description", ""),
                        "position": item.get("rank_absolute"),
                        "domain": item.get("domain", ""),
                    }
            return None
        else:
            # Return top organic result
            item = organic_items[0]
            return {
                "url": item.get("url", ""),
                "title": item.get("title", ""),
                "description": item.get("description", ""),
                "position": item.get("rank_absolute"),
                "domain": item.get("domain", ""),
            }
    
    def _extract_competitors(
        self,
        items: List[Dict[str, Any]],
        exclude_domain: Optional[str] = None,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Extract competing domains from SERP results.
        
        Args:
            items: List of SERP items
            exclude_domain: Domain to exclude (user's domain)
            limit: Maximum number of competitors to return
            
        Returns:
            List of competitor data
        """
        organic_items = [
            item for item in items if item.get("type") == "organic"
        ]
        
        competitors = []
        seen_domains = set()
        
        if exclude_domain:
            exclude_domain = exclude_domain.lower().replace("www.", "")
        
        for item in organic_items[:limit * 2]:  # Check more items to account for exclusions
            domain = item.get("domain", "").lower().replace("www.", "")
            
            if domain and domain not in seen_domains:
                if not exclude_domain or exclude_domain not in domain:
                    competitors.append({
                        "domain": domain,
                        "url": item.get("url", ""),
                        "title": item.get("title", ""),
                        "position": item.get("rank_absolute"),
                    })
                    seen_domains.add(domain)
                    
                    if len(competitors) >= limit:
                        break
        
        return competitors
    
    async def fetch_serp_results(
        self,
        keywords: List[str],
        location_code: int = 2840,  # United States
        language_code: str = "en",
        device: str = "desktop",
        target_domain: Optional[str] = None,
        use_cache: bool = True,
    ) -> Dict[str, Any]:
        """
        Fetch live SERP results for multiple keywords.
        
        Args:
            keywords: List of keywords to fetch
            location_code: DataForSEO location code (default: 2840 = United States)
            language_code: Language code (default: "en")
            device: Device type ("desktop" or "mobile")
            target_domain: User's domain for position tracking
            use_cache: Whether to use caching
            
        Returns:
            Dict with results per keyword
            
        Example:
            >>> results = await client.fetch_serp_results(
            ...     keywords=["best crm software", "crm for small business"],
            ...     target_domain="example.com"
            ... )
        """
        # Process in batches
        results = {}
        
        for i in range(0, len(keywords), self.BATCH_SIZE):
            batch = keywords[i:i + self.BATCH_SIZE]
            
            # Build request payload
            tasks = []
            for keyword in batch:
                tasks.append({
                    "keyword": keyword,
                    "location_code": location_code,
                    "language_code": language_code,
                    "device": device,
                    "os": "windows" if device == "desktop" else "ios",
                })
            
            payload = tasks
            
            try:
                response = await self._make_request(
                    "POST",
                    "/serp/google/organic/live/advanced",
                    data=payload,
                    use_cache=use_cache,
                    cache_ttl_hours=self.SERP_CACHE_TTL_HOURS,
                )
                
                # Parse results
                for task_result in response.get("tasks", []):
                    if task_result.get("status_code") != 20000:
                        logger.warning(
                            f"Task failed: {task_result.get('status_message')}"
                        )
                        continue
                    
                    result_data = task_result.get("result", [])
                    if not result_data:
                        continue
                    
                    result = result_data[0]
                    keyword = result.get("keyword", "")
                    items = result.get("items", [])
                    
                    # Extract SERP features
                    features, feature_details = self._extract_serp_features(items)
                    
                    # Extract user's position if target_domain provided
                    user_result = None
                    visual_position = None
                    if target_domain:
                        user_result = self._extract_organic_result(items, target_domain)
                        if user_result:
                            visual_position = self._calculate_visual_position(
                                user_result["position"],
                                features,
                                feature_details,
                            )
                    
                    # Extract competitors
                    competitors = self._extract_competitors(items, target_domain)
                    
                    results[keyword] = {
                        "keyword": keyword,
                        "location": result.get("location_name", ""),
                        "total_results": result.get("total_count"),
                        "serp_features": features,
                        "serp_feature_details": feature_details,
                        "user_result": user_result,
                        "visual_position": visual_position,
                        "competitors": competitors,
                        "check_date": result.get("check_url_date", datetime.utcnow().isoformat()),
                    }
                
            except Exception as e:
                logger.error(f"Error fetching SERP results for batch: {e}")
                # Mark keywords as failed
                for keyword in batch:
                    results[keyword] = {"error": str(e)}
        
        return results
    
    async def fetch_keyword_data(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
        use_cache: bool = True,
    ) -> Dict[str, Any]:
        """
        Fetch keyword difficulty, volume, and competition data.
        
        Args:
            keywords: List of keywords
            location_code: DataForSEO location code
            language_code: Language code
            use_cache: Whether to use caching
            
        Returns:
            Dict with keyword data
        """
        results = {}
        
        for i in range(0, len(keywords), self.BATCH_SIZE):
            batch = keywords[i:i + self.BATCH_SIZE]
            
            payload = [{
                "keywords": batch,
                "location_code": location_code,
                "language_code": language_code,
            }]
            
            try:
                response = await self._make_request(
                    "POST",
                    "/keywords_data/google_ads/search_volume/live",
                    data=payload,
                    use_cache=use_cache,
                    cache_ttl_hours=self.KEYWORD_DATA_CACHE_TTL_HOURS,
                )
                
                for task_result in response.get("tasks", []):
                    if task_result.get("status_code") != 20000:
                        continue
                    
                    result_data = task_result.get("result", [])
                    if not result_data:
                        continue
                    
                    for item in result_data:
                        keyword = item.get("keyword", "")
                        results[keyword] = {
                            "keyword": keyword,
                            "search_volume": item.get("search_volume"),
                            "competition": item.get("competition"),
                            "competition_level": item.get("competition_level"),
                            "cpc": item.get("cpc"),
                            "low_top_of_page_bid": item.get("low_top_of_page_bid"),
                            "high_top_of_page_bid": item.get("high_top_of_page_bid"),
                        }
                
            except Exception as e:
                logger.error(f"Error fetching keyword data for batch: {e}")
                for keyword in batch:
                    results[keyword] = {"error": str(e)}
        
        return results
    
    async def analyze_competitor_domains(
        self,
        domains: List[str],
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
    ) -> Dict[str, Any]:
        """
        Analyze competitor domains across multiple keywords.
        
        Args:
            domains: List of competitor domains
            keywords: List of keywords to check
            location_code: DataForSEO location code
            language_code: Language code
            
        Returns:
            Analysis of competitor presence across keywords
        """
        # Fetch SERP results for all keywords
        serp_results = await self.fetch_serp_results(
            keywords=keywords,
            location_code=location_code,
            language_code=language_code,
            use_cache=True,
        )
        
        # Analyze competitor frequency
        competitor_stats = {}
        
        for domain in domains:
            domain_normalized = domain.lower().replace("www.", "")
            competitor_stats[domain] = {
                "domain": domain,
                "keywords_present": 0,
                "average_position": 0,
                "positions": [],
                "keywords": [],
            }
        
        for keyword, data in serp_results.items():
            if "error" in data:
                continue
            
            competitors = data.get("competitors", [])
            
            for comp in competitors:
                comp_domain = comp["domain"].lower().replace("www.", "")
                
                # Check if this is one of our tracked competitors
                for domain in domains:
                    domain_normalized = domain.lower().replace("www.", "")
                    if domain_normalized == comp_domain or domain_normalized in comp_domain:
                        competitor_stats[domain]["keywords_present"] += 1
                        competitor_stats[domain]["positions"].append(comp["position"])
                        competitor_stats[domain]["keywords"].append({
                            "keyword": keyword,
                            "position": comp["position"],
                            "url": comp["url"],
                        })
        
        # Calculate averages
        for domain, stats in competitor_stats.items():
            if stats["positions"]:
                stats["average_position"] = sum(stats["positions"]) / len(stats["positions"])
            stats["presence_rate"] = stats["keywords_present"] / len(keywords) if keywords else 0
        
        # Sort by presence rate
        sorted_competitors = sorted(
            competitor_stats.values(),
            key=lambda x: x["presence_rate"],
            reverse=True,
        )
        
        return {
            "total_keywords_analyzed": len(keywords),
            "competitors": sorted_competitors,
        }
    
    async def batch_process_keywords(
        self,
        keywords: List[str],
        operations: List[str],
        location_code: int = 2840,
        language_code: str = "en",
        target_domain: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Process multiple keywords with multiple operations in parallel.
        
        Args:
            keywords: List of keywords to process
            operations: List of operations ("serp", "keyword_data", or both)
            location_code: DataForSEO location code
            language_code: Language code
            target_domain: User's domain for SERP position tracking
            
        Returns:
            Combined results from all operations
        """
        tasks = []
        
        if "serp" in operations:
            tasks.append(
                self.fetch_serp_results(
                    keywords=keywords,
                    location_code=location_code,
                    language_code=language_code,
                    target_domain=target_domain,
                )
            )
        
        if "keyword_data" in operations:
            tasks.append(
                self.fetch_keyword_data(
                    keywords=keywords,
                    location_code=location_code,
                    language_code=language_code,
                )
            )
        
        results_list = await asyncio.gather(*tasks, return_exceptions=True)
        
        combined_results = {}
        
        for keyword in keywords:
            combined_results[keyword] = {}
        
        for i, operation in enumerate(operations):
            if isinstance(results_list[i], Exception):
                logger.error(f"Operation {operation} failed: {results_list[i]}")
                continue
            
            for keyword, data in results_list[i].items():
                if operation == "serp":
                    combined_results[keyword]["serp"] = data
                elif operation == "keyword_data":
                    combined_results[keyword]["keyword_data"] = data
        
        return combined_results
    
    def classify_serp_intent(
        self, serp_features: List[str], keyword: str
    ) -> Dict[str, Any]:
        """
        Classify search intent based on SERP features and keyword patterns.
        
        Args:
            serp_features: List of SERP features present
            keyword: The search keyword
            
        Returns:
            Intent classification with confidence score
        """
        intent_scores = {
            "informational": 0,
            "commercial": 0,
            "transactional": 0,
            "navigational": 0,
        }
        
        # SERP feature signals
        if "people_also_ask" in serp_features or "knowledge_graph" in serp_features:
            intent_scores["informational"] += 2
        
        if "shopping" in serp_features:
            intent_scores["transactional"] += 3
            intent_scores["commercial"] += 2
        
        if "local_pack" in serp_features:
            intent_scores["transactional"] += 2
        
        if "video" in serp_features:
            intent_scores["informational"] += 1
        
        if "top_stories" in serp_features:
            intent_scores["informational"] += 1
        
        # Keyword pattern signals
        keyword_lower = keyword.lower()
        
        # Informational patterns
        informational_patterns = [
            r'\bhow to\b', r'\bwhat is\b', r'\bwhy\b', r'\bwhen\b',
            r'\bguide\b', r'\btutorial\b', r'\blearn\b', r'\btips\b',
        ]
        for pattern in informational_patterns:
            if re.search(pattern, keyword_lower):
                intent_scores["informational"] += 2
                break
        
        # Commercial patterns
        commercial_patterns = [
            r'\bbest\b', r'\btop\b', r'\breview\b', r'\bcompare\b',
            r'\bvs\b', r'\balternative\b', r'\bcheapest\b', r'\baffordable\b',
        ]
        for pattern in commercial_patterns:
            if re.search(pattern, keyword_lower):
                intent_scores["commercial"] += 2
                break
        
        # Transactional patterns
        transactional_patterns = [
            r'\bbuy\b', r'\bprice\b', r'\bcost\b', r'\bpurchase\b',
            r'\border\b', r'\bdeals\b', r'\bdiscount\b', r'\bcoupon\b',
            r'\bshop\b', r'\bstore\b',
        ]
        for pattern in transactional_patterns:
            if re.search(pattern, keyword_lower):
                intent_scores["transactional"] += 2
                break
        
        # Navigational patterns (brand/site names)
        if re.search(r'\blogin\b|\bsign in\b|\baccount\b', keyword_lower):
            intent_scores["navigational"] += 3
        
        # Determine primary intent
        max_score = max(intent_scores.values())
        if max_score == 0:
            primary_intent = "informational"  # Default
            confidence = 0.3
        else:
            primary_intent = max(intent_scores, key=intent_scores.get)
            total_score = sum(intent_scores.values())
            confidence = max_score / total_score if total_score > 0 else 0
        
        return {
            "primary_intent": primary_intent,
            "confidence": round(confidence, 2),
            "scores": intent_scores,
        }