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
    
    # Rate limiting: DataForSEO allows ~2000 requests per minute for live endpoints
    MAX_REQUESTS_PER_MINUTE = 100  # Conservative limit
    MAX_CONCURRENT_REQUESTS = 10
    
    def __init__(
        self,
        login: Optional[str] = None,
        password: Optional[str] = None,
        cache_enabled: bool = True,
        cache_ttl_hours: int = 24,
    ):
        """
        Initialize DataForSEO client.
        
        Args:
            login: DataForSEO login (defaults to DATAFORSEO_LOGIN env var)
            password: DataForSEO password (defaults to DATAFORSEO_PASSWORD env var)
            cache_enabled: Whether to use response caching
            cache_ttl_hours: Cache TTL in hours
        """
        self.login = login or os.getenv("DATAFORSEO_LOGIN")
        self.password = password or os.getenv("DATAFORSEO_PASSWORD")
        
        if not self.login or not self.password:
            raise DataForSEOAuthError(
                "DataForSEO credentials not provided. "
                "Set DATAFORSEO_LOGIN and DATAFORSEO_PASSWORD environment variables."
            )
        
        self.cache_enabled = cache_enabled
        self.cache_ttl_hours = cache_ttl_hours
        
        # Rate limiting state
        self._request_times: List[float] = []
        self._semaphore = asyncio.Semaphore(self.MAX_CONCURRENT_REQUESTS)
        self._lock = asyncio.Lock()
        
        # HTTP client (initialized on first use)
        self._client: Optional[httpx.AsyncClient] = None
        
        logger.info("DataForSEO client initialized")
    
    async def __aenter__(self):
        """Async context manager entry"""
        await self.authenticate()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close()
    
    async def authenticate(self) -> bool:
        """
        Authenticate with DataForSEO and initialize HTTP client.
        
        Returns:
            True if authentication successful
            
        Raises:
            DataForSEOAuthError: If authentication fails
        """
        try:
            self._client = httpx.AsyncClient(
                auth=(self.login, self.password),
                timeout=httpx.Timeout(30.0, connect=10.0),
                limits=httpx.Limits(max_keepalive_connections=20, max_connections=100),
            )
            
            # Test authentication with a ping
            response = await self._client.get(f"{self.BASE_URL}/serp/google/organic/live/advanced")
            
            if response.status_code == 401:
                raise DataForSEOAuthError("Invalid DataForSEO credentials")
            
            logger.info("DataForSEO authentication successful")
            return True
            
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                raise DataForSEOAuthError("Invalid DataForSEO credentials")
            raise DataForSEOError(f"Authentication failed: {str(e)}")
        except Exception as e:
            raise DataForSEOError(f"Authentication error: {str(e)}")
    
    async def close(self):
        """Close HTTP client connection"""
        if self._client:
            await self._client.aclose()
            self._client = None
            logger.info("DataForSEO client closed")
    
    async def _wait_for_rate_limit(self):
        """
        Implement rate limiting using sliding window.
        Ensures we don't exceed MAX_REQUESTS_PER_MINUTE.
        """
        async with self._lock:
            now = asyncio.get_event_loop().time()
            minute_ago = now - 60
            
            # Remove requests older than 1 minute
            self._request_times = [t for t in self._request_times if t > minute_ago]
            
            # If at limit, wait until oldest request is > 1 minute old
            if len(self._request_times) >= self.MAX_REQUESTS_PER_MINUTE:
                sleep_time = 60 - (now - self._request_times[0]) + 0.1
                if sleep_time > 0:
                    logger.warning(f"Rate limit reached, sleeping {sleep_time:.2f}s")
                    await asyncio.sleep(sleep_time)
                    # Re-clean after sleep
                    now = asyncio.get_event_loop().time()
                    minute_ago = now - 60
                    self._request_times = [t for t in self._request_times if t > minute_ago]
            
            # Record this request
            self._request_times.append(now)
    
    def _generate_cache_key(self, endpoint: str, params: Dict[str, Any]) -> str:
        """Generate cache key for request"""
        # Sort params for consistent hashing
        param_str = json.dumps(params, sort_keys=True)
        key_input = f"{endpoint}:{param_str}"
        return hashlib.sha256(key_input.encode()).hexdigest()
    
    async def _get_cached_response(
        self, cache_key: str
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieve cached response from Supabase if available and not expired.
        
        Args:
            cache_key: Cache key hash
            
        Returns:
            Cached response dict or None
        """
        if not self.cache_enabled:
            return None
        
        try:
            from api.services.supabase_client import get_supabase_client
            
            supabase = get_supabase_client()
            
            result = supabase.table("dataforseo_cache").select("*").eq(
                "cache_key", cache_key
            ).execute()
            
            if result.data and len(result.data) > 0:
                cached = result.data[0]
                created_at = datetime.fromisoformat(
                    cached["created_at"].replace("Z", "+00:00")
                )
                
                # Check if cache is still valid
                if datetime.now(created_at.tzinfo) - created_at < timedelta(
                    hours=self.cache_ttl_hours
                ):
                    logger.info(f"Cache hit for key {cache_key[:16]}...")
                    return cached["response_data"]
                else:
                    logger.info(f"Cache expired for key {cache_key[:16]}...")
            
            return None
            
        except Exception as e:
            logger.warning(f"Cache retrieval error: {str(e)}")
            return None
    
    async def _set_cached_response(
        self, cache_key: str, response_data: Dict[str, Any]
    ):
        """
        Store response in Supabase cache.
        
        Args:
            cache_key: Cache key hash
            response_data: Response data to cache
        """
        if not self.cache_enabled:
            return
        
        try:
            from api.services.supabase_client import get_supabase_client
            
            supabase = get_supabase_client()
            
            # Upsert cache entry
            supabase.table("dataforseo_cache").upsert({
                "cache_key": cache_key,
                "response_data": response_data,
                "created_at": datetime.utcnow().isoformat(),
            }).execute()
            
            logger.info(f"Cached response for key {cache_key[:16]}...")
            
        except Exception as e:
            logger.warning(f"Cache storage error: {str(e)}")
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError)),
        reraise=True,
    )
    async def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[List[Dict[str, Any]]] = None,
        use_cache: bool = True,
    ) -> Dict[str, Any]:
        """
        Make HTTP request to DataForSEO API with rate limiting and retries.
        
        Args:
            method: HTTP method (GET or POST)
            endpoint: API endpoint path
            data: Request payload for POST requests
            use_cache: Whether to use caching
            
        Returns:
            Parsed JSON response
            
        Raises:
            DataForSEORateLimitError: If rate limited
            DataForSEOError: For other API errors
        """
        if not self._client:
            await self.authenticate()
        
        # Check cache for POST requests with data
        cache_key = None
        if use_cache and method == "POST" and data:
            cache_key = self._generate_cache_key(endpoint, {"data": data})
            cached_response = await self._get_cached_response(cache_key)
            if cached_response:
                return cached_response
        
        # Rate limiting and concurrency control
        async with self._semaphore:
            await self._wait_for_rate_limit()
            
            url = f"{self.BASE_URL}/{endpoint}"
            
            try:
                if method == "GET":
                    response = await self._client.get(url)
                elif method == "POST":
                    response = await self._client.post(url, json=data)
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")
                
                response.raise_for_status()
                result = response.json()
                
                # Check for API-level errors
                if result.get("status_code") == 40100:
                    raise DataForSEOAuthError("Authentication failed")
                elif result.get("status_code") == 50000:
                    raise DataForSEORateLimitError("Rate limit exceeded")
                elif result.get("status_code") and result["status_code"] >= 40000:
                    error_msg = result.get("status_message", "Unknown error")
                    raise DataForSEOError(f"API error: {error_msg}")
                
                # Cache successful response
                if cache_key and result.get("status_code") == 20000:
                    await self._set_cached_response(cache_key, result)
                
                return result
                
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:
                    raise DataForSEORateLimitError("Rate limit exceeded")
                elif e.response.status_code == 401:
                    raise DataForSEOAuthError("Authentication failed")
                else:
                    raise DataForSEOError(
                        f"HTTP {e.response.status_code}: {e.response.text}"
                    )
            except httpx.RequestError as e:
                raise DataForSEOError(f"Request error: {str(e)}")
    
    def _parse_serp_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse a single SERP item into standardized format.
        
        Args:
            item: Raw SERP item from DataForSEO
            
        Returns:
            Parsed item dict
        """
        item_type = item.get("type", "")
        
        # Base fields
        parsed = {
            "type": item_type,
            "rank_group": item.get("rank_group"),
            "rank_absolute": item.get("rank_absolute"),
            "position": item.get("position"),
        }
        
        # Organic result
        if item_type == "organic":
            parsed.update({
                "domain": item.get("domain"),
                "url": item.get("url"),
                "title": item.get("title"),
                "description": item.get("description"),
                "breadcrumb": item.get("breadcrumb"),
                "is_image": item.get("is_image", False),
                "is_video": item.get("is_video", False),
                "rating": item.get("rating", {}).get("value") if item.get("rating") else None,
                "timestamp": item.get("timestamp"),
            })
        
        # Featured snippet
        elif "featured_snippet" in item_type or "answer" in item_type:
            parsed.update({
                "domain": item.get("domain"),
                "url": item.get("url"),
                "title": item.get("title"),
                "description": item.get("description"),
                "featured_snippet_type": item.get("featured_snippet", {}).get("type"),
            })
        
        # People Also Ask
        elif "people_also_ask" in item_type:
            parsed.update({
                "questions": [
                    {
                        "question": q.get("title"),
                        "url": q.get("url"),
                        "domain": q.get("domain"),
                    }
                    for q in item.get("items", [])
                ],
            })
        
        # Knowledge graph
        elif "knowledge_graph" in item_type:
            parsed.update({
                "title": item.get("title"),
                "description": item.get("description"),
                "card_type": item.get("card_type"),
                "url": item.get("url"),
            })
        
        # Local pack
        elif "local_pack" in item_type:
            parsed.update({
                "items": [
                    {
                        "title": loc.get("title"),
                        "domain": loc.get("domain"),
                        "rating": loc.get("rating", {}).get("value") if loc.get("rating") else None,
                    }
                    for loc in item.get("items", [])
                ],
            })
        
        # AI Overview
        elif "ai_overview" in item_type:
            parsed.update({
                "text": item.get("text"),
                "expanded_text": item.get("expanded_text"),
                "links": item.get("links", []),
            })
        
        return parsed
    
    def _detect_serp_features(
        self, items: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Detect and categorize SERP features from items list.
        
        Args:
            items: List of SERP items from DataForSEO
            
        Returns:
            Dict with feature counts and details
        """
        features = {
            "present": [],
            "counts": {},
            "details": {},
        }
        
        for item in items:
            item_type = item.get("type", "")
            
            # Categorize by feature type
            for feature_name, type_keywords in self.SERP_FEATURE_TYPES.items():
                if any(kw in item_type for kw in type_keywords):
                    if feature_name not in features["present"]:
                        features["present"].append(feature_name)
                        features["counts"][feature_name] = 0
                        features["details"][feature_name] = []
                    
                    features["counts"][feature_name] += 1
                    features["details"][feature_name].append(
                        self._parse_serp_item(item)
                    )
        
        return features
    
    def _calculate_visual_position(
        self,
        organic_position: int,
        serp_features: Dict[str, Any],
        items: List[Dict[str, Any]],
    ) -> float:
        """
        Calculate visual position accounting for SERP features above the result.
        
        Args:
            organic_position: Organic rank position
            serp_features: Detected SERP features
            items: All SERP items
            
        Returns:
            Visual position (float)
        """
        visual_position = float(organic_position)
        
        # Get the rank_group of the target organic result
        target_rank_group = None
        for item in items:
            if (
                item.get("type") == "organic"
                and item.get("position") == organic_position
            ):
                target_rank_group = item.get("rank_group")
                break
        
        if not target_rank_group:
            return visual_position
        
        # Count features above this position
        for item in items:
            item_rank_group = item.get("rank_group", 999)
            item_type = item.get("type", "")
            
            # Only count features that appear before our result
            if item_rank_group < target_rank_group:
                for feature_name, type_keywords in self.SERP_FEATURE_TYPES.items():
                    if any(kw in item_type for kw in type_keywords):
                        impact = self.SERP_FEATURE_VISUAL_IMPACT.get(feature_name, 0)
                        
                        # For PAA, multiply by number of questions
                        if feature_name == "people_also_ask":
                            paa_count = len(item.get("items", []))
                            visual_position += impact * paa_count
                        else:
                            visual_position += impact
                        break
        
        return visual_position
    
    async def fetch_serp_results(
        self,
        keywords: List[str],
        location_code: int = 2840,  # USA
        language_code: str = "en",
        device: str = "desktop",
        depth: int = 100,
    ) -> Dict[str, Any]:
        """
        Fetch live SERP results for multiple keywords.
        
        Args:
            keywords: List of keywords to query
            location_code: DataForSEO location code (2840 = USA)
            language_code: Language code (en, es, etc.)
            device: Device type (desktop, mobile)
            depth: Number of results to retrieve (max 100)
            
        Returns:
            Dict mapping keywords to SERP data:
            {
                "keyword": {
                    "organic_results": [...],
                    "serp_features": {...},
                    "total_results": int,
                    "timestamp": str,
                }
            }
        """
        # Build task payload
        tasks = []
        for keyword in keywords:
            tasks.append({
                "keyword": keyword,
                "location_code": location_code,
                "language_code": language_code,
                "device": device,
                "depth": depth,
                "calculate_rectangles": False,  # Speeds up response
            })
        
        # Make request
        result = await self._make_request(
            "POST",
            "serp/google/organic/live/advanced",
            data=tasks,
        )
        
        # Parse results
        parsed_results = {}
        
        if result.get("tasks"):
            for task in result["tasks"]:
                if task.get("status_code") != 20000:
                    logger.warning(
                        f"Task failed for keyword: {task.get('data', {}).get('keyword')}"
                    )
                    continue
                
                task_result = task.get("result", [])
                if not task_result:
                    continue
                
                serp_data = task_result[0]
                keyword = serp_data.get("keyword")
                items = serp_data.get("items", [])
                
                # Extract organic results
                organic_results = []
                for item in items:
                    if item.get("type") == "organic":
                        parsed_item = self._parse_serp_item(item)
                        
                        # Calculate visual position
                        position = parsed_item.get("position")
                        if position:
                            serp_features = self._detect_serp_features(items)
                            visual_position = self._calculate_visual_position(
                                position, serp_features, items
                            )
                            parsed_item["visual_position"] = visual_position
                            parsed_item["visual_displacement"] = (
                                visual_position - position
                            )
                        
                        organic_results.append(parsed_item)
                
                # Detect SERP features
                serp_features = self._detect_serp_features(items)
                
                parsed_results[keyword] = {
                    "organic_results": organic_results,
                    "serp_features": serp_features,
                    "total_results": serp_data.get("total_count"),
                    "timestamp": serp_data.get("datetime"),
                    "location": serp_data.get("location_code"),
                    "language": serp_data.get("language_code"),
                }
        
        return parsed_results
    
    async def fetch_keyword_data(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
    ) -> Dict[str, Any]:
        """
        Fetch keyword difficulty, volume, and competition data.
        
        Args:
            keywords: List of keywords
            location_code: DataForSEO location code
            language_code: Language code
            
        Returns:
            Dict mapping keywords to metrics:
            {
                "keyword": {
                    "search_volume": int,
                    "keyword_difficulty": int,
                    "competition": float,
                    "cpc": float,
                    "monthly_searches": [...]
                }
            }
        """
        # Build task payload
        tasks = []
        for keyword in keywords:
            tasks.append({
                "keyword": keyword,
                "location_code": location_code,
                "language_code": language_code,
            })
        
        # Make request to Keywords Data API
        result = await self._make_request(
            "POST",
            "keywords_data/google_ads/search_volume/live",
            data=tasks,
        )
        
        # Parse results
        parsed_results = {}
        
        if result.get("tasks"):
            for task in result["tasks"]:
                if task.get("status_code") != 20000:
                    continue
                
                task_result = task.get("result", [])
                if not task_result:
                    continue
                
                for keyword_data in task_result:
                    keyword = keyword_data.get("keyword")
                    
                    parsed_results[keyword] = {
                        "search_volume": keyword_data.get("search_volume"),
                        "competition": keyword_data.get("competition"),
                        "competition_level": keyword_data.get("competition_level"),
                        "cpc": keyword_data.get("cpc"),
                        "low_top_of_page_bid": keyword_data.get("low_top_of_page_bid"),
                        "high_top_of_page_bid": keyword_data.get("high_top_of_page_bid"),
                        "monthly_searches": keyword_data.get("monthly_searches", []),
                    }
        
        return parsed_results
    
    async def get_competitor_domains(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
        top_n: int = 10,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Extract top competing domains across a set of keywords.
        
        Args:
            keywords: List of keywords to analyze
            location_code: DataForSEO location code
            language_code: Language code
            top_n: Number of top domains to return
            
        Returns:
            Dict with competitor analysis:
            {
                "competitors": [
                    {
                        "domain": "example.com",
                        "keywords_ranked": 45,
                        "avg_position": 3.2,
                        "keywords": ["kw1", "kw2", ...]
                    }
                ]
            }
        """
        # Fetch SERP results
        serp_results = await self.fetch_serp_results(
            keywords=keywords,
            location_code=location_code,
            language_code=language_code,
            depth=top_n,
        )
        
        # Aggregate domain stats
        domain_stats: Dict[str, Dict[str, Any]] = {}
        
        for keyword, data in serp_results.items():
            for result in data.get("organic_results", []):
                domain = result.get("domain")
                position = result.get("position")
                
                if not domain or not position:
                    continue
                
                if domain not in domain_stats:
                    domain_stats[domain] = {
                        "domain": domain,
                        "keywords_ranked": 0,
                        "positions": [],
                        "keywords": [],
                    }
                
                domain_stats[domain]["keywords_ranked"] += 1
                domain_stats[domain]["positions"].append(position)
                domain_stats[domain]["keywords"].append(keyword)
        
        # Calculate averages and sort
        competitors = []
        for domain, stats in domain_stats.items():
            avg_position = sum(stats["positions"]) / len(stats["positions"])
            competitors.append({
                "domain": domain,
                "keywords_ranked": stats["keywords_ranked"],
                "avg_position": round(avg_position, 2),
                "keywords": stats["keywords"],
            })
        
        # Sort by keywords ranked (descending)
        competitors.sort(key=lambda x: x["keywords_ranked"], reverse=True)
        
        return {
            "competitors": competitors[:top_n],
            "total_domains_analyzed": len(domain_stats),
        }
    
    async def batch_process_keywords(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
        batch_size: int = 10,
        include_keyword_data: bool = True,
    ) -> Dict[str, Any]:
        """
        Process large keyword lists in batches with combined SERP and keyword data.
        
        Args:
            keywords: List of keywords to process
            location_code: DataForSEO location code
            language_code: Language code
            batch_size: Number of keywords per batch
            include_keyword_data: Whether to fetch keyword metrics
            
        Returns:
            Combined results dict with SERP and keyword data
        """
        results = {
            "serp_data": {},
            "keyword_data": {},
            "metadata": {
                "total_keywords": len(keywords),
                "batches_processed": 0,
                "errors": [],
            },
        }
        
        # Process in batches
        for i in range(0, len(keywords), batch_size):
            batch = keywords[i:i + batch_size]
            
            try:
                # Fetch SERP data
                serp_data = await self.fetch_serp_results(
                    keywords=batch,
                    location_code=location_code,
                    language_code=language_code,
                )
                results["serp_data"].update(serp_data)
                
                # Fetch keyword data if requested
                if include_keyword_data:
                    keyword_data = await self.fetch_keyword_data(
                        keywords=batch,
                        location_code=location_code,
                        language_code=language_code,
                    )
                    results["keyword_data"].update(keyword_data)
                
                results["metadata"]["batches_processed"] += 1
                logger.info(f"Processed batch {i // batch_size + 1}")
                
            except Exception as e:
                error_msg = f"Batch {i // batch_size + 1} failed: {str(e)}"
                logger.error(error_msg)
                results["metadata"]["errors"].append(error_msg)
        
        return results
    
    def filter_branded_keywords(
        self,
        keywords: List[str],
        brand_terms: List[str],
    ) -> List[str]:
        """
        Filter out branded keywords from a list.
        
        Args:
            keywords: List of keywords to filter
            brand_terms: List of brand terms (domain, company name, etc.)
            
        Returns:
            List of non-branded keywords
        """
        non_branded = []
        
        # Normalize brand terms
        normalized_brands = [
            re.sub(r"[^a-z0-9]", "", term.lower()) for term in brand_terms
        ]
        
        for keyword in keywords:
            normalized_keyword = re.sub(r"[^a-z0-9]", "", keyword.lower())
            
            # Check if any brand term is in the keyword
            is_branded = any(
                brand in normalized_keyword for brand in normalized_brands
            )
            
            if not is_branded:
                non_branded.append(keyword)
        
        return non_branded
    
    def classify_intent(
        self, keyword: str, serp_features: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Classify search intent based on keyword patterns and SERP features.
        
        Args:
            keyword: Keyword to classify
            serp_features: Optional SERP features data
            
        Returns:
            Intent classification: informational, commercial, transactional, navigational
        """
        keyword_lower = keyword.lower()
        
        # Navigational patterns
        navigational_patterns = [
            r"\blogin\b",
            r"\bsign in\b",
            r"\baccount\b",
            r"\bofficial\b",
        ]
        if any(re.search(p, keyword_lower) for p in navigational_patterns):
            return "navigational"
        
        # Transactional patterns
        transactional_patterns = [
            r"\bbuy\b",
            r"\bpurchase\b",
            r"\border\b",
            r"\bcoupon\b",
            r"\bdeal\b",
            r"\bprice\b",
            r"\bcheap\b",
            r"\bdiscount\b",
        ]
        if any(re.search(p, keyword_lower) for p in transactional_patterns):
            return "transactional"
        
        # Commercial investigation patterns
        commercial_patterns = [
            r"\bbest\b",
            r"\btop\b",
            r"\breview\b",
            r"\bcompare\b",
            r"\bvs\b",
            r"\balternative\b",
            r"\boption\b",
        ]
        if any(re.search(p, keyword_lower) for p in commercial_patterns):
            return "commercial"
        
        # Informational patterns
        informational_patterns = [
            r"\bhow\b",
            r"\bwhat\b",
            r"\bwhy\b",
            r"\bwhen\b",
            r"\bwhere\b",
            r"\bguide\b",
            r"\btutorial\b",
            r"\btips\b",
            r"\blearn\b",
        ]
        if any(re.search(p, keyword_lower) for p in informational_patterns):
            return "informational"
        
        # Use SERP features if available
        if serp_features:
            features_present = serp_features.get("present", [])
            
            # Shopping/product features = transactional
            if any(
                f in features_present
                for f in ["shopping", "hotels_pack", "flights"]
            ):
                return "transactional"
            
            # PAA, knowledge graph = informational
            if any(
                f in features_present
                for f in ["people_also_ask", "knowledge_graph"]
            ):
                return "informational"
        
        # Default to informational
        return "informational"