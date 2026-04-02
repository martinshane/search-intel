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
    MAX_REQUESTS_PER_MINUTE = 2000
    MAX_CONCURRENT_REQUESTS = 10
    
    def __init__(
        self,
        username: Optional[str] = None,
        password: Optional[str] = None,
        cache_enabled: bool = True,
        cache_ttl_hours: int = 24,
    ):
        """
        Initialize DataForSEO client.
        
        Args:
            username: DataForSEO API username (defaults to DATAFORSEO_USERNAME env var)
            password: DataForSEO API password (defaults to DATAFORSEO_PASSWORD env var)
            cache_enabled: Whether to cache API responses
            cache_ttl_hours: How long to cache responses (hours)
        """
        self.username = username or os.getenv("DATAFORSEO_USERNAME")
        self.password = password or os.getenv("DATAFORSEO_PASSWORD")
        
        if not self.username or not self.password:
            raise DataForSEOAuthError(
                "DataForSEO credentials not provided. Set DATAFORSEO_USERNAME and "
                "DATAFORSEO_PASSWORD environment variables or pass them to constructor."
            )
        
        self.cache_enabled = cache_enabled
        self.cache_ttl_hours = cache_ttl_hours
        
        # HTTP client with connection pooling
        self.client = httpx.AsyncClient(
            auth=(self.username, self.password),
            timeout=httpx.Timeout(60.0, connect=10.0),
            limits=httpx.Limits(
                max_keepalive_connections=20,
                max_connections=100,
            ),
        )
        
        # Rate limiting state
        self._request_timestamps: List[datetime] = []
        self._semaphore = asyncio.Semaphore(self.MAX_CONCURRENT_REQUESTS)
        self._rate_limit_lock = asyncio.Lock()
        
        # Cache (in-memory for now, will integrate with Supabase)
        self._cache: Dict[str, Tuple[datetime, Any]] = {}
        
        logger.info("DataForSEO client initialized")
    
    async def __aenter__(self):
        """Async context manager entry"""
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close()
    
    async def close(self):
        """Close HTTP client and cleanup resources"""
        await self.client.aclose()
        logger.info("DataForSEO client closed")
    
    def _generate_cache_key(self, endpoint: str, payload: Dict[str, Any]) -> str:
        """Generate cache key from endpoint and payload"""
        # Create deterministic string from payload
        payload_str = json.dumps(payload, sort_keys=True)
        cache_key = f"{endpoint}:{hashlib.md5(payload_str.encode()).hexdigest()}"
        return cache_key
    
    def _get_from_cache(self, cache_key: str) -> Optional[Any]:
        """Get cached response if valid"""
        if not self.cache_enabled:
            return None
        
        if cache_key in self._cache:
            cached_time, cached_data = self._cache[cache_key]
            age = datetime.utcnow() - cached_time
            
            if age < timedelta(hours=self.cache_ttl_hours):
                logger.debug(f"Cache hit for {cache_key} (age: {age})")
                return cached_data
            else:
                # Expired
                del self._cache[cache_key]
                logger.debug(f"Cache expired for {cache_key}")
        
        return None
    
    def _set_cache(self, cache_key: str, data: Any):
        """Store response in cache"""
        if self.cache_enabled:
            self._cache[cache_key] = (datetime.utcnow(), data)
            logger.debug(f"Cached response for {cache_key}")
    
    async def _wait_for_rate_limit(self):
        """Enforce rate limiting"""
        async with self._rate_limit_lock:
            now = datetime.utcnow()
            
            # Remove timestamps older than 1 minute
            self._request_timestamps = [
                ts for ts in self._request_timestamps
                if (now - ts).total_seconds() < 60
            ]
            
            # Check if we're at the limit
            if len(self._request_timestamps) >= self.MAX_REQUESTS_PER_MINUTE:
                # Calculate wait time until oldest request expires
                oldest = self._request_timestamps[0]
                wait_seconds = 60 - (now - oldest).total_seconds()
                
                if wait_seconds > 0:
                    logger.warning(
                        f"Rate limit reached. Waiting {wait_seconds:.2f}s..."
                    )
                    await asyncio.sleep(wait_seconds)
                    # Recursive call to check again
                    return await self._wait_for_rate_limit()
            
            # Record this request
            self._request_timestamps.append(now)
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.NetworkError)),
        reraise=True,
    )
    async def _make_request(
        self,
        endpoint: str,
        payload: List[Dict[str, Any]],
        use_cache: bool = True,
    ) -> Dict[str, Any]:
        """
        Make POST request to DataForSEO API with rate limiting and retries.
        
        Args:
            endpoint: API endpoint (e.g., "/serp/google/organic/live/advanced")
            payload: List of task objects
            use_cache: Whether to use cached responses
            
        Returns:
            API response data
            
        Raises:
            DataForSEOError: On API errors
            DataForSEORateLimitError: On rate limit errors
            DataForSEOAuthError: On authentication errors
        """
        # Check cache
        cache_key = self._generate_cache_key(endpoint, payload[0] if payload else {})
        if use_cache:
            cached = self._get_from_cache(cache_key)
            if cached is not None:
                return cached
        
        # Rate limiting and concurrency control
        async with self._semaphore:
            await self._wait_for_rate_limit()
            
            url = f"{self.BASE_URL}{endpoint}"
            
            try:
                logger.debug(f"POST {url} with {len(payload)} tasks")
                response = await self.client.post(url, json=payload)
                
                # Handle HTTP errors
                if response.status_code == 401:
                    raise DataForSEOAuthError("Authentication failed. Check credentials.")
                elif response.status_code == 429:
                    raise DataForSEORateLimitError("Rate limit exceeded")
                elif response.status_code >= 400:
                    error_msg = f"HTTP {response.status_code}: {response.text}"
                    raise DataForSEOError(error_msg)
                
                data = response.json()
                
                # Check DataForSEO response status
                if data.get("status_code") != 20000:
                    error_msg = data.get("status_message", "Unknown error")
                    raise DataForSEOError(f"API error: {error_msg}")
                
                # Cache successful response
                if use_cache:
                    self._set_cache(cache_key, data)
                
                return data
                
            except httpx.HTTPStatusError as e:
                logger.error(f"HTTP error: {e}")
                raise DataForSEOError(f"HTTP error: {e}")
            except httpx.RequestError as e:
                logger.error(f"Request error: {e}")
                raise DataForSEOError(f"Request error: {e}")
            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error: {e}")
                raise DataForSEOError(f"Invalid JSON response: {e}")
    
    async def authenticate(self) -> bool:
        """
        Verify API credentials.
        
        Returns:
            True if authentication successful
            
        Raises:
            DataForSEOAuthError: If authentication fails
        """
        try:
            # Test with a simple endpoint
            await self._make_request(
                "/appendix/user_data",
                [],
                use_cache=False,
            )
            logger.info("DataForSEO authentication successful")
            return True
        except Exception as e:
            logger.error(f"Authentication failed: {e}")
            raise DataForSEOAuthError(f"Authentication failed: {e}")
    
    async def fetch_serp_live(
        self,
        keyword: str,
        location_code: int = 2840,  # United States
        language_code: str = "en",
        device: str = "desktop",
        depth: int = 100,
    ) -> Dict[str, Any]:
        """
        Fetch live SERP results for a single keyword.
        
        Args:
            keyword: Search query
            location_code: DataForSEO location code (2840 = USA)
            language_code: Language code (en, es, etc.)
            device: Device type (desktop, mobile)
            depth: Number of results to retrieve (max 100)
            
        Returns:
            Parsed SERP data with organic results, features, and competitors
        """
        payload = [
            {
                "keyword": keyword,
                "location_code": location_code,
                "language_code": language_code,
                "device": device,
                "os": "windows" if device == "desktop" else "ios",
                "depth": min(depth, 100),
                "calculate_rectangles": True,  # For SERP feature positioning
            }
        ]
        
        response = await self._make_request(
            "/serp/google/organic/live/advanced",
            payload,
        )
        
        # Parse response
        if not response.get("tasks") or not response["tasks"][0].get("result"):
            logger.warning(f"No results for keyword: {keyword}")
            return self._empty_serp_response(keyword)
        
        task_result = response["tasks"][0]["result"][0]
        
        return self._parse_serp_response(keyword, task_result)
    
    async def fetch_serp_batch(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
        device: str = "desktop",
        depth: int = 100,
        batch_size: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Fetch SERP results for multiple keywords in batches.
        
        Args:
            keywords: List of search queries
            location_code: DataForSEO location code
            language_code: Language code
            device: Device type
            depth: Number of results per keyword
            batch_size: Keywords per API request (max 100)
            
        Returns:
            List of parsed SERP data for each keyword
        """
        results = []
        
        # Process in batches
        for i in range(0, len(keywords), batch_size):
            batch = keywords[i:i + batch_size]
            
            # Create tasks concurrently
            tasks = [
                self.fetch_serp_live(
                    keyword=kw,
                    location_code=location_code,
                    language_code=language_code,
                    device=device,
                    depth=depth,
                )
                for kw in batch
            ]
            
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Handle errors
            for kw, result in zip(batch, batch_results):
                if isinstance(result, Exception):
                    logger.error(f"Error fetching SERP for '{kw}': {result}")
                    results.append(self._empty_serp_response(kw))
                else:
                    results.append(result)
            
            # Small delay between batches
            if i + batch_size < len(keywords):
                await asyncio.sleep(0.5)
        
        return results
    
    def _empty_serp_response(self, keyword: str) -> Dict[str, Any]:
        """Create empty SERP response structure"""
        return {
            "keyword": keyword,
            "organic_results": [],
            "serp_features": [],
            "competitors": [],
            "total_results": 0,
            "serp_feature_summary": {},
            "visual_position_adjustments": {},
        }
    
    def _parse_serp_response(
        self,
        keyword: str,
        task_result: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Parse DataForSEO SERP response into structured format.
        
        Returns:
            {
                "keyword": str,
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
                "competitors": [
                    {
                        "domain": str,
                        "positions": [int],
                        "avg_position": float,
                        "urls": [str],
                    }
                ],
                "total_results": int,
                "serp_feature_summary": {
                    "featured_snippet": bool,
                    "people_also_ask": int,
                    "knowledge_graph": bool,
                    ...
                },
                "visual_position_adjustments": {
                    "position_1": float,  # How much each organic position is pushed down
                    "position_2": float,
                    ...
                }
            }
        """
        items = task_result.get("items", [])
        
        organic_results = []
        serp_features = []
        competitors_map = {}
        serp_feature_summary = {}
        
        # Track SERP features and their positions for visual adjustment
        feature_positions = {}
        
        for item in items:
            item_type = item.get("type", "")
            rank_group = item.get("rank_group")
            rank_absolute = item.get("rank_absolute")
            
            # Organic results
            if item_type == "organic":
                domain = self._extract_domain(item.get("url", ""))
                
                organic_result = {
                    "position": rank_absolute,
                    "url": item.get("url", ""),
                    "domain": domain,
                    "title": item.get("title", ""),
                    "description": item.get("description", ""),
                    "breadcrumb": item.get("breadcrumb", ""),
                }
                
                organic_results.append(organic_result)
                
                # Track competitors
                if domain not in competitors_map:
                    competitors_map[domain] = {
                        "domain": domain,
                        "positions": [],
                        "urls": [],
                    }
                competitors_map[domain]["positions"].append(rank_absolute)
                competitors_map[domain]["urls"].append(item.get("url", ""))
            
            # SERP features
            else:
                feature_type = self._classify_serp_feature(item_type)
                
                if feature_type:
                    serp_feature = {
                        "type": feature_type,
                        "position": rank_absolute or rank_group or 0,
                        "raw_type": item_type,
                        "data": self._extract_feature_data(item_type, item),
                    }
                    
                    serp_features.append(serp_feature)
                    
                    # Track for summary
                    if feature_type == "people_also_ask":
                        count = len(item.get("items", []))
                        serp_feature_summary[feature_type] = (
                            serp_feature_summary.get(feature_type, 0) + count
                        )
                    else:
                        serp_feature_summary[feature_type] = True
                    
                    # Track position for visual adjustment calculation
                    pos = rank_absolute or rank_group or 0
                    if pos > 0:
                        if feature_type not in feature_positions:
                            feature_positions[feature_type] = []
                        feature_positions[feature_type].append(pos)
        
        # Calculate visual position adjustments
        visual_adjustments = self._calculate_visual_adjustments(
            organic_results,
            feature_positions,
        )
        
        # Add visual positions to organic results
        for result in organic_results:
            pos = result["position"]
            result["visual_position"] = pos + visual_adjustments.get(pos, 0)
        
        # Calculate competitor stats
        competitors = []
        for comp_data in competitors_map.values():
            comp_data["avg_position"] = sum(comp_data["positions"]) / len(
                comp_data["positions"]
            )
            competitors.append(comp_data)
        
        # Sort competitors by average position
        competitors.sort(key=lambda x: x["avg_position"])
        
        return {
            "keyword": keyword,
            "organic_results": organic_results,
            "serp_features": serp_features,
            "competitors": competitors,
            "total_results": len(organic_results),
            "serp_feature_summary": serp_feature_summary,
            "visual_position_adjustments": visual_adjustments,
        }
    
    def _classify_serp_feature(self, item_type: str) -> Optional[str]:
        """Classify DataForSEO item type into our SERP feature taxonomy"""
        for feature_name, type_variants in self.SERP_FEATURE_TYPES.items():
            if item_type in type_variants:
                return feature_name
        
        # Direct match if not in mappings
        if item_type in self.SERP_FEATURE_VISUAL_IMPACT:
            return item_type
        
        return None
    
    def _extract_feature_data(
        self,
        item_type: str,
        item: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Extract relevant data from SERP feature item"""
        data = {}
        
        if item_type == "featured_snippet":
            data["description"] = item.get("description", "")
            data["url"] = item.get("url", "")
            data["domain"] = self._extract_domain(item.get("url", ""))
            data["title"] = item.get("title", "")
        
        elif item_type == "people_also_ask":
            questions = item.get("items", [])
            data["questions"] = [
                {
                    "question": q.get("title", ""),
                    "answer": q.get("expanded_element", [{}])[0].get("description", "")
                    if q.get("expanded_element")
                    else "",
                }
                for q in questions
            ]
            data["count"] = len(questions)
        
        elif item_type == "knowledge_graph":
            data["title"] = item.get("title", "")
            data["description"] = item.get("description", "")
            data["url"] = item.get("url", "")
        
        elif item_type == "local_pack":
            locations = item.get("items", [])
            data["count"] = len(locations)
            data["locations"] = [
                {
                    "title": loc.get("title", ""),
                    "address": loc.get("address", ""),
                    "rating": loc.get("rating", {}).get("value"),
                }
                for loc in locations[:3]  # Top 3
            ]
        
        elif item_type == "video":
            data["source"] = item.get("source", "")
            data["title"] = item.get("title", "")
            data["url"] = item.get("url", "")
        
        elif item_type in ["shopping", "google_shopping"]:
            products = item.get("items", [])
            data["count"] = len(products)
        
        elif item_type == "ai_overview":
            data["text"] = item.get("text", "")
            data["expanded_text"] = item.get("expanded_element", [{}])[0].get("text", "")
        
        return data
    
    def _calculate_visual_adjustments(
        self,
        organic_results: List[Dict[str, Any]],
        feature_positions: Dict[str, List[int]],
    ) -> Dict[int, float]:
        """
        Calculate how much each organic position is visually pushed down by SERP features.
        
        Returns:
            Dict mapping organic position -> visual displacement (positions)
        """
        adjustments = {}
        
        for result in organic_results:
            position = result["position"]
            displacement = 0.0
            
            # Check each SERP feature type
            for feature_type, positions in feature_positions.items():
                impact = self.SERP_FEATURE_VISUAL_IMPACT.get(feature_type, 0.0)
                
                # Count how many instances of this feature appear above this organic result
                count_above = sum(1 for fp in positions if fp < position)
                
                # For PAA, multiply by number of questions (stored in summary)
                if feature_type == "people_also_ask" and count_above > 0:
                    # Estimate 4 questions per PAA block (conservative)
                    displacement += impact * count_above * 4
                else:
                    displacement += impact * count_above
            
            adjustments[position] = displacement
        
        return adjustments
    
    def _extract_domain(self, url: str) -> str:
        """Extract domain from URL"""
        if not url:
            return ""
        
        # Remove protocol
        url = re.sub(r"^https?://", "", url)
        # Remove www
        url = re.sub(r"^www\.", "", url)
        # Extract domain (everything before first /)
        domain = url.split("/")[0]
        
        return domain.lower()
    
    async def get_keyword_metrics(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
    ) -> List[Dict[str, Any]]:
        """
        Get keyword difficulty and search volume for multiple keywords.
        
        Args:
            keywords: List of keywords
            location_code: DataForSEO location code
            language_code: Language code
            
        Returns:
            List of keyword metrics:
            [
                {
                    "keyword": str,
                    "search_volume": int,
                    "keyword_difficulty": int (0-100),
                    "cpc": float,
                    "competition": float (0-1),
                }
            ]
        """
        # DataForSEO Keywords Data endpoint
        payload = [
            {
                "keywords": keywords,
                "location_code": location_code,
                "language_code": language_code,
            }
        ]
        
        try:
            response = await self._make_request(
                "/keywords_data/google/search_volume/live",
                payload,
            )
            
            if not response.get("tasks") or not response["tasks"][0].get("result"):
                logger.warning("No keyword metrics returned")
                return []
            
            results = []
            items = response["tasks"][0]["result"][0].get("items", [])
            
            for item in items:
                results.append(
                    {
                        "keyword": item.get("keyword", ""),
                        "search_volume": item.get("search_volume", 0),
                        "keyword_difficulty": item.get("keyword_difficulty", 0),
                        "cpc": item.get("cpc", 0.0),
                        "competition": item.get("competition", 0.0),
                    }
                )
            
            return results
            
        except Exception as e:
            logger.error(f"Error fetching keyword metrics: {e}")
            return []
    
    async def analyze_serp_for_keyword_set(
        self,
        keywords: List[str],
        user_domain: str,
        location_code: int = 2840,
        language_code: str = "en",
    ) -> Dict[str, Any]:
        """
        Analyze SERP landscape for a set of keywords.
        
        Returns comprehensive analysis including:
        - Competitor overlap
        - SERP feature prevalence
        - User's rankings and visual positions
        - CTR opportunities
        - Intent classification
        
        Args:
            keywords: List of keywords to analyze
            user_domain: User's domain (for position detection)
            location_code: Location
            language_code: Language
            
        Returns:
            {
                "keywords_analyzed": int,
                "user_domain": str,
                "competitors": [
                    {
                        "domain": str,
                        "keywords_shared": int,
                        "avg_position": float,
                        "appearances": int,
                    }
                ],
                "serp_features": {
                    "featured_snippet": int,  # Count
                    "people_also_ask": int,
                    ...
                },
                "user_rankings": [
                    {
                        "keyword": str,
                        "position": int,
                        "visual_position": float,
                        "url": str,
                        "displacement": float,
                    }
                ],
                "ctr_opportunities": [
                    {
                        "keyword": str,
                        "current_ctr_estimate": float,
                        "potential_ctr": float,
                        "gain": float,
                    }
                ],
                "intent_distribution": {
                    "informational": int,
                    "commercial": int,
                    "navigational": int,
                    "transactional": int,
                },
            }
        """
        # Fetch SERP data for all keywords
        serp_results = await self.fetch_serp_batch(
            keywords=keywords,
            location_code=location_code,
            language_code=language_code,
        )
        
        # Aggregate analysis
        competitor_map = {}
        serp_feature_counts = {}
        user_rankings = []
        intent_counts = {"informational": 0, "commercial": 0, "navigational": 0, "transactional": 0}
        
        for serp in serp_results:
            keyword = serp["keyword"]
            
            # Competitor tracking
            for comp in serp["competitors"]:
                domain = comp["domain"]
                if domain == user_domain:
                    continue
                
                if domain not in competitor_map:
                    competitor_map[domain] = {
                        "domain": domain,
                        "keywords": set(),
                        "positions": [],
                        "appearances": 0,
                    }
                
                competitor_map[domain]["keywords"].add(keyword)
                competitor_map[domain]["positions"].extend(comp["positions"])
                competitor_map[domain]["appearances"] += 1
            
            # SERP feature tracking
            for feature_type, present in serp["serp_feature_summary"].items():
                if present:
                    if isinstance(present, bool):
                        serp_feature_counts[feature_type] = (
                            serp_feature_counts.get(feature_type, 0) + 1
                        )
                    else:
                        # PAA count
                        serp_feature_counts[feature_type] = (
                            serp_feature_counts.get(feature_type, 0) + present
                        )
            
            # User position tracking
            user_result = None
            for result in serp["organic_results"]:
                if result["domain"] == user_domain:
                    user_result = result
                    break
            
            if user_result:
                user_rankings.append(
                    {
                        "keyword": keyword,
                        "position": user_result["position"],
                        "visual_position": user_result["visual_position"],
                        "url": user_result["url"],
                        "displacement": user_result["visual_position"]
                        - user_result["position"],
                    }
                )
            
            # Intent classification
            intent = self._classify_intent(keyword, serp)
            intent_counts[intent] += 1
        
        # Calculate competitor stats
        competitors = []
        for comp_data in competitor_map.values():
            competitors.append(
                {
                    "domain": comp_data["domain"],
                    "keywords_shared": len(comp_data["keywords"]),
                    "avg_position": sum(comp_data["positions"])
                    / len(comp_data["positions"])
                    if comp_data["positions"]
                    else 0,
                    "appearances": comp_data["appearances"],
                }
            )
        
        # Sort by keyword overlap
        competitors.sort(key=lambda x: x["keywords_shared"], reverse=True)
        
        # Calculate CTR opportunities
        ctr_opportunities = self._calculate_ctr_opportunities(user_rankings)
        
        return {
            "keywords_analyzed": len(keywords),
            "user_domain": user_domain,
            "competitors": competitors[:20],  # Top 20
            "serp_features": serp_feature_counts,
            "user_rankings": user_rankings,
            "ctr_opportunities": ctr_opportunities,
            "intent_distribution": intent_counts,
        }
    
    def _classify_intent(
        self,
        keyword: str,
        serp: Dict[str, Any],
    ) -> str:
        """
        Classify search intent based on keyword and SERP features.
        
        Returns: "informational", "commercial", "navigational", or "transactional"
        """
        keyword_lower = keyword.lower()
        features = serp["serp_feature_summary"]
        
        # Navigational signals
        if features.get("knowledge_graph") or "login" in keyword_lower or "www" in keyword_lower:
            return "navigational"
        
        # Transactional signals
        transactional_words = ["buy", "purchase", "order", "discount", "coupon", "deal", "price", "pricing"]
        if any(word in keyword_lower for word in transactional_words) or features.get("shopping"):
            return "transactional"
        
        # Commercial signals
        commercial_words = ["best", "top", "vs", "review", "comparison", "alternative", "compare"]
        if any(word in keyword_lower for word in commercial_words):
            return "commercial"
        
        # Informational signals (default)
        informational_words = ["how", "what", "why", "when", "where", "guide", "tutorial", "learn"]
        if any(word in keyword_lower for word in informational_words) or features.get("people_also_ask"):
            return "informational"
        
        # Default to informational
        return "informational"
    
    def _calculate_ctr_opportunities(
        self,
        user_rankings: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Calculate CTR improvement opportunities.
        
        Uses position-based CTR curves adjusted for SERP features.
        """
        # Advanced Search CTR curve (desktop, no SERP features)
        base_ctr_curve = {
            1: 0.282,
            2: 0.129,
            3: 0.092,
            4: 0.064,
            5: 0.051,
            6: 0.041,
            7: 0.035,
            8: 0.030,
            9: 0.026,
            10: 0.023,
        }
        
        opportunities = []
        
        for ranking in user_rankings:
            position = ranking["position"]
            visual_position = ranking["visual_position"]
            
            # Base CTR for actual position
            base_ctr = base_ctr_curve.get(position, 0.01)
            
            # Adjusted CTR accounting for visual displacement
            # Visual position reduces CTR exponentially
            visual_penalty = max(0, (visual_position - position) * 0.15)
            current_ctr = base_ctr * (1 - visual_penalty)
            
            # Potential CTR if SERP features removed or if position improved
            potential_positions = []
            
            # Could improve position
            if position > 1:
                potential_positions.append(position - 1)
            if position > 3:
                potential_positions.append(3)
            
            max_gain = 0
            best_scenario = None
            
            for target_pos in potential_positions:
                potential_ctr = base_ctr_curve.get(target_pos, base_ctr)
                gain = potential_ctr - current_ctr
                
                if gain > max_gain:
                    max_gain = gain
                    best_scenario = target_pos
            
            # Also consider removing SERP feature displacement
            if visual_position > position:
                no_displacement_ctr = base_ctr
                displacement_gain = no_displacement_ctr - current_ctr
                
                if displacement_gain > max_gain:
                    max_gain = displacement_gain
                    best_scenario = f"remove_displacement"
            
            if max_gain > 0.01:  # Only flag if >1% CTR gain
                opportunities.append(
                    {
                        "keyword": ranking["keyword"],
                        "current_position": position,
                        "visual_position": visual_position,
                        "current_ctr_estimate": round(current_ctr, 4),
                        "potential_ctr": round(current_ctr + max_gain, 4),
                        "gain": round(max_gain, 4),
                        "scenario": best_scenario,
                    }
                )
        
        # Sort by gain
        opportunities.sort(key=lambda x: x["gain"], reverse=True)
        
        return opportunities

