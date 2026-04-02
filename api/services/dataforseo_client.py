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
        self._request_times: List[float] = []
        self._rate_limit_lock = asyncio.Lock()
        
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
        """Initialize HTTP client with authentication"""
        if self._client is None:
            self._client = httpx.AsyncClient(
                auth=(self.login, self.password),
                timeout=self.timeout,
                headers={
                    "Content-Type": "application/json",
                }
            )
            logger.info("DataForSEO client authenticated")
    
    async def close(self):
        """Close HTTP client"""
        if self._client:
            await self._client.aclose()
            self._client = None
            logger.info("DataForSEO client closed")
    
    async def _wait_for_rate_limit(self):
        """Enforce rate limiting"""
        async with self._rate_limit_lock:
            now = asyncio.get_event_loop().time()
            
            # Remove timestamps older than 1 second
            self._request_times = [
                t for t in self._request_times if now - t < 1.0
            ]
            
            # Wait if we've hit the rate limit
            if len(self._request_times) >= self.rate_limit_per_second:
                sleep_time = 1.0 - (now - self._request_times[0])
                if sleep_time > 0:
                    logger.debug(f"Rate limit reached, sleeping for {sleep_time:.2f}s")
                    await asyncio.sleep(sleep_time)
                    # Clean up old timestamps after sleeping
                    now = asyncio.get_event_loop().time()
                    self._request_times = [
                        t for t in self._request_times if now - t < 1.0
                    ]
            
            # Record this request
            self._request_times.append(asyncio.get_event_loop().time())
    
    def _generate_cache_key(self, endpoint: str, params: Dict[str, Any]) -> str:
        """Generate cache key from endpoint and parameters"""
        # Sort params for consistent hashing
        params_str = json.dumps(params, sort_keys=True)
        combined = f"{endpoint}:{params_str}"
        return hashlib.sha256(combined.encode()).hexdigest()
    
    async def _get_cached_response(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """Retrieve cached response from Supabase"""
        if not self.supabase:
            return None
        
        try:
            result = self.supabase.table("dataforseo_cache").select("*").eq(
                "cache_key", cache_key
            ).single().execute()
            
            if result.data:
                cached_at = datetime.fromisoformat(result.data["cached_at"])
                expires_at = cached_at + timedelta(hours=self.cache_ttl_hours)
                
                if datetime.utcnow() < expires_at:
                    logger.info(f"Cache hit for key {cache_key[:8]}...")
                    return result.data["response_data"]
                else:
                    logger.info(f"Cache expired for key {cache_key[:8]}...")
                    # Delete expired cache
                    self.supabase.table("dataforseo_cache").delete().eq(
                        "cache_key", cache_key
                    ).execute()
        except Exception as e:
            logger.warning(f"Error retrieving cache: {e}")
        
        return None
    
    async def _set_cached_response(self, cache_key: str, response: Dict[str, Any]):
        """Store response in Supabase cache"""
        if not self.supabase:
            return
        
        try:
            self.supabase.table("dataforseo_cache").upsert({
                "cache_key": cache_key,
                "response_data": response,
                "cached_at": datetime.utcnow().isoformat(),
            }).execute()
            logger.info(f"Cached response for key {cache_key[:8]}...")
        except Exception as e:
            logger.warning(f"Error caching response: {e}")
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.ConnectError)),
        reraise=True,
    )
    async def _make_request(
        self, endpoint: str, payload: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Make authenticated request to DataForSEO API with retries.
        
        Args:
            endpoint: API endpoint path (e.g., "/serp/google/organic/live/advanced")
            payload: List of task dictionaries
            
        Returns:
            API response dictionary
            
        Raises:
            DataForSEOAuthError: Authentication failed
            DataForSEORateLimitError: Rate limit exceeded
            DataForSEOError: Other API errors
        """
        if not self._client:
            await self.authenticate()
        
        await self._wait_for_rate_limit()
        
        url = f"{self.BASE_URL}{endpoint}"
        
        try:
            logger.debug(f"Making request to {endpoint} with {len(payload)} tasks")
            response = await self._client.post(url, json=payload)
            
            # Handle authentication errors
            if response.status_code == 401:
                raise DataForSEOAuthError("Authentication failed. Check credentials.")
            
            # Handle rate limiting
            if response.status_code == 429:
                raise DataForSEORateLimitError("Rate limit exceeded")
            
            # Parse response
            response.raise_for_status()
            data = response.json()
            
            # Check for API-level errors
            if data.get("status_code") != 20000:
                error_message = data.get("status_message", "Unknown error")
                raise DataForSEOError(f"API error: {error_message}")
            
            return data
        
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error: {e}")
            raise DataForSEOError(f"HTTP {e.response.status_code}: {e}")
        except httpx.TimeoutException as e:
            logger.error(f"Request timeout: {e}")
            raise
        except httpx.ConnectError as e:
            logger.error(f"Connection error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise DataForSEOError(f"Request failed: {e}")
    
    async def fetch_serp_results(
        self,
        keywords: List[str],
        location_code: int = 2840,  # United States
        language_code: str = "en",
        device: str = "desktop",
        use_cache: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Fetch live SERP results for a batch of keywords.
        
        Args:
            keywords: List of search queries
            location_code: DataForSEO location code (default: 2840 = US)
            language_code: Language code (default: "en")
            device: Device type ("desktop", "mobile")
            use_cache: Whether to use cached results
            
        Returns:
            List of SERP result dictionaries, one per keyword
        """
        results = []
        
        for keyword in keywords:
            # Check cache first
            cache_key = self._generate_cache_key(
                "/serp/google/organic/live/advanced",
                {
                    "keyword": keyword,
                    "location_code": location_code,
                    "language_code": language_code,
                    "device": device,
                }
            )
            
            if use_cache:
                cached = await self._get_cached_response(cache_key)
                if cached:
                    results.append(cached)
                    continue
            
            # Make API request
            payload = [{
                "keyword": keyword,
                "location_code": location_code,
                "language_code": language_code,
                "device": device,
                "os": "windows" if device == "desktop" else "android",
            }]
            
            try:
                response = await self._make_request(
                    "/serp/google/organic/live/advanced", payload
                )
                
                # Extract task result
                if response.get("tasks") and len(response["tasks"]) > 0:
                    task = response["tasks"][0]
                    if task.get("result") and len(task["result"]) > 0:
                        result = task["result"][0]
                        
                        # Cache the result
                        if use_cache:
                            await self._set_cached_response(cache_key, result)
                        
                        results.append(result)
                    else:
                        logger.warning(f"No results for keyword: {keyword}")
                        results.append(None)
                else:
                    logger.warning(f"No tasks in response for keyword: {keyword}")
                    results.append(None)
            
            except Exception as e:
                logger.error(f"Error fetching SERP for keyword '{keyword}': {e}")
                results.append(None)
        
        return results
    
    def extract_competitors(
        self,
        serp_results: List[Dict[str, Any]],
        exclude_domains: Optional[Set[str]] = None,
        min_occurrences: int = 1,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Extract competitor domains from SERP results.
        
        Args:
            serp_results: List of SERP result dictionaries
            exclude_domains: Set of domains to exclude (e.g., user's own domain)
            min_occurrences: Minimum number of keywords a domain must appear in
            
        Returns:
            Dictionary mapping domain to competitor stats:
            {
                "competitor.com": {
                    "occurrences": 15,
                    "avg_position": 4.2,
                    "keywords": ["keyword1", "keyword2", ...],
                    "positions": [3, 5, 4, ...],
                }
            }
        """
        exclude_domains = exclude_domains or set()
        competitors: Dict[str, Dict[str, Any]] = {}
        
        for result in serp_results:
            if not result or "items" not in result:
                continue
            
            keyword = result.get("keyword", "")
            
            # Process organic results
            for item in result.get("items", []):
                if item.get("type") != "organic":
                    continue
                
                url = item.get("url", "")
                domain = item.get("domain", "")
                position = item.get("rank_group", 0)
                
                if not domain or domain in exclude_domains:
                    continue
                
                if domain not in competitors:
                    competitors[domain] = {
                        "occurrences": 0,
                        "keywords": [],
                        "positions": [],
                        "urls": [],
                    }
                
                competitors[domain]["occurrences"] += 1
                competitors[domain]["keywords"].append(keyword)
                competitors[domain]["positions"].append(position)
                competitors[domain]["urls"].append(url)
        
        # Calculate averages and filter by min_occurrences
        filtered_competitors = {}
        for domain, stats in competitors.items():
            if stats["occurrences"] >= min_occurrences:
                stats["avg_position"] = sum(stats["positions"]) / len(stats["positions"])
                stats["min_position"] = min(stats["positions"])
                stats["max_position"] = max(stats["positions"])
                filtered_competitors[domain] = stats
        
        # Sort by occurrences (descending)
        return dict(
            sorted(
                filtered_competitors.items(),
                key=lambda x: x[1]["occurrences"],
                reverse=True,
            )
        )
    
    def detect_serp_features(
        self, serp_results: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Detect SERP features present in results.
        
        Args:
            serp_results: List of SERP result dictionaries
            
        Returns:
            List of dictionaries with SERP feature analysis:
            [
                {
                    "keyword": "best crm software",
                    "features": {
                        "featured_snippet": True,
                        "people_also_ask": True,
                        "paa_count": 4,
                        "knowledge_graph": False,
                        "local_pack": False,
                        "video": True,
                        ...
                    },
                    "visual_displacement": 2.5,  # estimated positions pushed down
                }
            ]
        """
        analyzed = []
        
        for result in serp_results:
            if not result or "items" not in result:
                continue
            
            keyword = result.get("keyword", "")
            features = {
                "featured_snippet": False,
                "people_also_ask": False,
                "paa_count": 0,
                "knowledge_graph": False,
                "local_pack": False,
                "video": False,
                "image": False,
                "shopping": False,
                "top_stories": False,
                "twitter": False,
                "recipes": False,
                "ai_overview": False,
                "related_searches": False,
            }
            
            visual_displacement = 0.0
            
            for item in result.get("items", []):
                item_type = item.get("type", "")
                
                # Featured snippet
                if item_type == "featured_snippet":
                    features["featured_snippet"] = True
                    visual_displacement += 2.0  # Takes significant space
                
                # People Also Ask
                elif item_type == "people_also_ask":
                    features["people_also_ask"] = True
                    # Count individual questions
                    questions = item.get("items", [])
                    paa_count = len(questions)
                    features["paa_count"] = paa_count
                    visual_displacement += paa_count * 0.5
                
                # Knowledge Graph
                elif item_type == "knowledge_graph":
                    features["knowledge_graph"] = True
                    visual_displacement += 1.5
                
                # Local Pack
                elif item_type in ["local_pack", "map"]:
                    features["local_pack"] = True
                    visual_displacement += 3.0  # Local pack is large
                
                # Video
                elif item_type == "video":
                    features["video"] = True
                    visual_displacement += 1.0
                
                # Images
                elif item_type == "images":
                    features["image"] = True
                    visual_displacement += 0.5
                
                # Shopping
                elif item_type in ["shopping", "google_shopping"]:
                    features["shopping"] = True
                    visual_displacement += 1.5
                
                # Top Stories
                elif item_type == "top_stories":
                    features["top_stories"] = True
                    visual_displacement += 2.0
                
                # Twitter
                elif item_type == "twitter":
                    features["twitter"] = True
                    visual_displacement += 1.0
                
                # Recipes
                elif item_type == "recipes":
                    features["recipes"] = True
                    visual_displacement += 1.5
                
                # AI Overview (SGE)
                elif item_type == "ai_overview":
                    features["ai_overview"] = True
                    visual_displacement += 3.0  # AI overviews are large
                
                # Related Searches
                elif item_type in ["people_also_search", "related_searches"]:
                    features["related_searches"] = True
                    # Typically at bottom, doesn't push results down
            
            analyzed.append({
                "keyword": keyword,
                "features": features,
                "visual_displacement": visual_displacement,
            })
        
        return analyzed
    
    def extract_organic_positions(
        self,
        serp_results: List[Dict[str, Any]],
        target_domain: str,
    ) -> List[Dict[str, Any]]:
        """
        Extract organic position data for a target domain.
        
        Args:
            serp_results: List of SERP result dictionaries
            target_domain: Domain to track (e.g., "example.com")
            
        Returns:
            List of position data dictionaries:
            [
                {
                    "keyword": "best crm",
                    "position": 3,
                    "url": "https://example.com/best-crm",
                    "title": "Best CRM Software...",
                    "description": "...",
                    "visual_position": 5.5,  # accounting for SERP features
                    "estimated_ctr": 0.082,
                }
            ]
        """
        positions = []
        
        for result in serp_results:
            if not result or "items" not in result:
                continue
            
            keyword = result.get("keyword", "")
            
            # Find visual displacement from SERP features
            visual_displacement = 0.0
            organic_position = None
            position_data = None
            
            for item in result.get("items", []):
                item_type = item.get("type", "")
                domain = item.get("domain", "")
                
                # Track displacement from features
                if item_type == "featured_snippet":
                    visual_displacement += 2.0
                elif item_type == "people_also_ask":
                    visual_displacement += len(item.get("items", [])) * 0.5
                elif item_type in ["local_pack", "map"]:
                    visual_displacement += 3.0
                elif item_type == "knowledge_graph":
                    visual_displacement += 1.5
                elif item_type == "ai_overview":
                    visual_displacement += 3.0
                elif item_type == "video":
                    visual_displacement += 1.0
                elif item_type in ["shopping", "google_shopping"]:
                    visual_displacement += 1.5
                elif item_type == "top_stories":
                    visual_displacement += 2.0
                
                # Check if this is our target domain
                if item_type == "organic" and domain == target_domain:
                    organic_position = item.get("rank_group", 0)
                    position_data = {
                        "keyword": keyword,
                        "position": organic_position,
                        "url": item.get("url", ""),
                        "title": item.get("title", ""),
                        "description": item.get("description", ""),
                        "visual_position": organic_position + visual_displacement,
                    }
                    break
            
            if position_data:
                # Estimate CTR based on position and SERP features
                # Simple CTR curve: position 1 = 30%, drops exponentially
                base_ctr = self._estimate_ctr(organic_position)
                
                # Adjust for visual displacement
                displacement_penalty = min(visual_displacement * 0.15, 0.8)
                adjusted_ctr = base_ctr * (1 - displacement_penalty)
                
                position_data["estimated_ctr"] = round(adjusted_ctr, 4)
                positions.append(position_data)
        
        return positions
    
    def _estimate_ctr(self, position: int) -> float:
        """
        Estimate CTR based on organic position using industry benchmark curve.
        
        Based on Advanced Web Ranking CTR study data.
        """
        # CTR benchmarks by position (desktop)
        ctr_by_position = {
            1: 0.284,
            2: 0.147,
            3: 0.095,
            4: 0.067,
            5: 0.051,
            6: 0.040,
            7: 0.033,
            8: 0.028,
            9: 0.024,
            10: 0.021,
        }
        
        if position <= 10:
            return ctr_by_position.get(position, 0.021)
        elif position <= 20:
            # Page 2: ~0.01 CTR
            return 0.01
        else:
            # Page 3+: minimal CTR
            return 0.005
    
    async def fetch_batch_serp_data(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
        device: str = "desktop",
        batch_size: int = 10,
        use_cache: bool = True,
    ) -> Dict[str, Any]:
        """
        Fetch SERP data for multiple keywords in batches with full analysis.
        
        Args:
            keywords: List of keywords to analyze
            location_code: DataForSEO location code
            language_code: Language code
            device: Device type
            batch_size: Number of keywords per batch
            use_cache: Whether to use cached results
            
        Returns:
            Dictionary with comprehensive SERP analysis:
            {
                "raw_results": [...],
                "competitors": {...},
                "serp_features": [...],
                "summary": {
                    "total_keywords": 50,
                    "keywords_with_features": 38,
                    "avg_visual_displacement": 2.3,
                    "top_competitors": [...],
                }
            }
        """
        all_results = []
        
        # Process in batches to respect rate limits
        for i in range(0, len(keywords), batch_size):
            batch = keywords[i:i + batch_size]
            logger.info(f"Fetching batch {i // batch_size + 1}: {len(batch)} keywords")
            
            batch_results = await self.fetch_serp_results(
                keywords=batch,
                location_code=location_code,
                language_code=language_code,
                device=device,
                use_cache=use_cache,
            )
            
            all_results.extend(batch_results)
            
            # Small delay between batches
            if i + batch_size < len(keywords):
                await asyncio.sleep(0.5)
        
        # Analyze results
        serp_features = self.detect_serp_features(all_results)
        
        # Summary statistics
        total_keywords = len(keywords)
        keywords_with_features = sum(
            1 for sf in serp_features
            if any(v for k, v in sf["features"].items() if k != "paa_count")
        )
        avg_visual_displacement = sum(
            sf["visual_displacement"] for sf in serp_features
        ) / len(serp_features) if serp_features else 0
        
        return {
            "raw_results": all_results,
            "serp_features": serp_features,
            "summary": {
                "total_keywords": total_keywords,
                "keywords_analyzed": len([r for r in all_results if r]),
                "keywords_with_features": keywords_with_features,
                "avg_visual_displacement": round(avg_visual_displacement, 2),
            }
        }
