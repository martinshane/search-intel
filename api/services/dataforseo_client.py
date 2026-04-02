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
        self._last_request_time = 0
        self._rate_limit_lock = asyncio.Lock()
        
        # HTTP client (initialized in authenticate)
        self._client: Optional[httpx.AsyncClient] = None
        
    async def authenticate(self) -> None:
        """
        Initialize HTTP client with authentication.
        Must be called before making any API requests.
        """
        if self._client is not None:
            return
            
        self._client = httpx.AsyncClient(
            auth=(self.login, self.password),
            timeout=self.timeout,
            headers={
                "Content-Type": "application/json",
            },
        )
        
        # Test authentication
        try:
            response = await self._client.get(f"{self.BASE_URL}/serp/google/locations")
            if response.status_code == 401:
                raise DataForSEOAuthError("Invalid DataForSEO credentials")
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                raise DataForSEOAuthError("Invalid DataForSEO credentials")
            raise DataForSEOError(f"Authentication test failed: {str(e)}")
        except Exception as e:
            raise DataForSEOError(f"Authentication failed: {str(e)}")
            
        logger.info("DataForSEO client authenticated successfully")
    
    async def close(self) -> None:
        """Close HTTP client."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.authenticate()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
    
    async def _rate_limit(self) -> None:
        """Enforce rate limiting."""
        async with self._rate_limit_lock:
            now = asyncio.get_event_loop().time()
            time_since_last = now - self._last_request_time
            min_interval = 1.0 / self.rate_limit_per_second
            
            if time_since_last < min_interval:
                await asyncio.sleep(min_interval - time_since_last)
            
            self._last_request_time = asyncio.get_event_loop().time()
    
    def _generate_cache_key(self, endpoint: str, params: Dict[str, Any]) -> str:
        """Generate cache key from endpoint and parameters."""
        cache_str = json.dumps({"endpoint": endpoint, "params": params}, sort_keys=True)
        return hashlib.sha256(cache_str.encode()).hexdigest()
    
    async def _get_cached_response(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """Retrieve cached response from Supabase."""
        if not self.supabase:
            return None
        
        try:
            result = (
                self.supabase.table("dataforseo_cache")
                .select("*")
                .eq("cache_key", cache_key)
                .single()
                .execute()
            )
            
            if result.data:
                cached_at = datetime.fromisoformat(result.data["cached_at"])
                ttl_delta = timedelta(hours=self.cache_ttl_hours)
                
                if datetime.utcnow() - cached_at < ttl_delta:
                    logger.info(f"Cache hit for key: {cache_key[:16]}...")
                    return result.data["response_data"]
                else:
                    logger.info(f"Cache expired for key: {cache_key[:16]}...")
                    # Delete expired cache
                    self.supabase.table("dataforseo_cache").delete().eq(
                        "cache_key", cache_key
                    ).execute()
            
        except Exception as e:
            logger.warning(f"Cache retrieval failed: {str(e)}")
        
        return None
    
    async def _cache_response(
        self, cache_key: str, response_data: Dict[str, Any]
    ) -> None:
        """Store response in Supabase cache."""
        if not self.supabase:
            return
        
        try:
            self.supabase.table("dataforseo_cache").upsert(
                {
                    "cache_key": cache_key,
                    "response_data": response_data,
                    "cached_at": datetime.utcnow().isoformat(),
                }
            ).execute()
            logger.info(f"Cached response for key: {cache_key[:16]}...")
        except Exception as e:
            logger.warning(f"Cache storage failed: {str(e)}")
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((httpx.HTTPError, DataForSEORateLimitError)),
        reraise=True,
    )
    async def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
        use_cache: bool = True,
    ) -> Dict[str, Any]:
        """
        Make HTTP request to DataForSEO API with retries and caching.
        
        Args:
            method: HTTP method (GET, POST)
            endpoint: API endpoint path
            data: Request payload for POST requests
            use_cache: Whether to use caching
            
        Returns:
            Parsed JSON response
            
        Raises:
            DataForSEOError: On API errors
            DataForSEORateLimitError: On rate limit errors
            DataForSEOAuthError: On authentication errors
        """
        if self._client is None:
            raise DataForSEOError("Client not authenticated. Call authenticate() first.")
        
        # Check cache for GET requests
        cache_key = None
        if use_cache and method == "POST" and data:
            cache_key = self._generate_cache_key(endpoint, data)
            cached = await self._get_cached_response(cache_key)
            if cached:
                return cached
        
        # Rate limiting
        await self._rate_limit()
        
        # Make request
        url = f"{self.BASE_URL}/{endpoint}"
        
        try:
            if method == "GET":
                response = await self._client.get(url)
            elif method == "POST":
                response = await self._client.post(url, json=data)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            # Handle rate limiting
            if response.status_code == 429:
                logger.warning("Rate limit exceeded, retrying...")
                raise DataForSEORateLimitError("Rate limit exceeded")
            
            # Handle authentication errors
            if response.status_code == 401:
                raise DataForSEOAuthError("Authentication failed")
            
            # Handle other HTTP errors
            response.raise_for_status()
            
            result = response.json()
            
            # Check DataForSEO status
            if result.get("status_code") != 20000:
                error_msg = result.get("status_message", "Unknown error")
                raise DataForSEOError(f"API error: {error_msg}")
            
            # Cache successful responses
            if cache_key and use_cache:
                await self._cache_response(cache_key, result)
            
            return result
            
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                raise DataForSEORateLimitError("Rate limit exceeded")
            elif e.response.status_code == 401:
                raise DataForSEOAuthError("Authentication failed")
            else:
                raise DataForSEOError(f"HTTP error: {str(e)}")
        except httpx.HTTPError as e:
            raise DataForSEOError(f"Request failed: {str(e)}")
    
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
            location_code: DataForSEO location code (2840 = US)
            language_code: Language code (en, es, etc.)
            device: Device type (desktop, mobile)
            depth: Number of results to fetch (max 100)
            use_cache: Whether to use caching
            
        Returns:
            List of parsed SERP results, one per keyword
        """
        if not keywords:
            return []
        
        # Build request payload
        tasks = []
        for keyword in keywords:
            payload = {
                "keyword": keyword,
                "location_code": location_code,
                "language_code": language_code,
                "device": device,
                "depth": depth,
            }
            tasks.append(payload)
        
        # Make batch request
        response = await self._make_request(
            "POST",
            "serp/google/organic/live/advanced",
            data=tasks,
            use_cache=use_cache,
        )
        
        # Parse results
        results = []
        for task_result in response.get("tasks", []):
            if task_result.get("status_code") == 20000:
                result_data = task_result.get("result", [{}])[0]
                parsed = self._parse_serp_result(result_data)
                results.append(parsed)
            else:
                logger.warning(
                    f"Task failed: {task_result.get('status_message', 'Unknown error')}"
                )
        
        return results
    
    def _parse_serp_result(self, result_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse raw SERP result into structured format.
        
        Returns:
            {
                "keyword": str,
                "location": str,
                "language": str,
                "total_results": int,
                "organic_results": List[Dict],
                "serp_features": Dict[str, Any],
                "competitors": List[str],
            }
        """
        keyword = result_data.get("keyword", "")
        
        # Extract organic results
        organic_results = []
        items = result_data.get("items", [])
        
        for item in items:
            if item.get("type") == "organic":
                organic_results.append({
                    "position": item.get("rank_group", 0),
                    "url": item.get("url", ""),
                    "domain": item.get("domain", ""),
                    "title": item.get("title", ""),
                    "description": item.get("description", ""),
                    "breadcrumb": item.get("breadcrumb", ""),
                })
        
        # Extract SERP features
        serp_features = self._extract_serp_features(items)
        
        # Extract competitor domains
        competitors = list({
            r["domain"] for r in organic_results if r["domain"]
        })
        
        return {
            "keyword": keyword,
            "location": result_data.get("location_code", ""),
            "language": result_data.get("language_code", ""),
            "total_results": result_data.get("se_results_count", 0),
            "organic_results": organic_results,
            "serp_features": serp_features,
            "competitors": competitors,
        }
    
    def _extract_serp_features(self, items: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Extract and categorize SERP features from items.
        
        Returns:
            {
                "featured_snippet": bool,
                "people_also_ask": {"present": bool, "count": int},
                "knowledge_graph": bool,
                "local_pack": bool,
                "video": bool,
                "image": bool,
                "shopping": bool,
                "top_stories": bool,
                "twitter": bool,
                "recipes": bool,
                "ai_overview": bool,
                "related_searches": bool,
            }
        """
        features = {
            "featured_snippet": False,
            "people_also_ask": {"present": False, "count": 0},
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
        
        paa_count = 0
        
        for item in items:
            item_type = item.get("type", "")
            
            # Map item types to feature categories
            for feature_name, type_variants in self.SERP_FEATURE_TYPES.items():
                if item_type in type_variants:
                    if feature_name == "people_also_ask":
                        paa_count += 1
                        features["people_also_ask"]["present"] = True
                    else:
                        features[feature_name] = True
        
        features["people_also_ask"]["count"] = paa_count
        
        return features
    
    async def analyze_competitors(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
        min_appearances: int = 2,
    ) -> List[Dict[str, Any]]:
        """
        Analyze competitor domains across multiple keywords.
        
        Args:
            keywords: List of keywords to analyze
            location_code: DataForSEO location code
            language_code: Language code
            min_appearances: Minimum keyword appearances to include competitor
            
        Returns:
            List of competitor analysis results:
            [
                {
                    "domain": str,
                    "appearances": int,
                    "avg_position": float,
                    "keywords": List[str],
                    "threat_level": str,  # "high", "medium", "low"
                }
            ]
        """
        # Fetch SERP results
        serp_results = await self.fetch_serp_results(
            keywords=keywords,
            location_code=location_code,
            language_code=language_code,
        )
        
        # Aggregate competitor data
        competitor_data: Dict[str, Dict[str, Any]] = {}
        
        for result in serp_results:
            keyword = result["keyword"]
            
            for organic in result["organic_results"]:
                domain = organic["domain"]
                position = organic["position"]
                
                if domain not in competitor_data:
                    competitor_data[domain] = {
                        "appearances": 0,
                        "positions": [],
                        "keywords": [],
                    }
                
                competitor_data[domain]["appearances"] += 1
                competitor_data[domain]["positions"].append(position)
                competitor_data[domain]["keywords"].append(keyword)
        
        # Filter and compute stats
        competitors = []
        for domain, data in competitor_data.items():
            if data["appearances"] >= min_appearances:
                avg_position = sum(data["positions"]) / len(data["positions"])
                appearance_rate = data["appearances"] / len(keywords)
                
                # Determine threat level
                if appearance_rate >= 0.3 and avg_position <= 5:
                    threat_level = "high"
                elif appearance_rate >= 0.15 and avg_position <= 10:
                    threat_level = "medium"
                else:
                    threat_level = "low"
                
                competitors.append({
                    "domain": domain,
                    "appearances": data["appearances"],
                    "appearance_rate": appearance_rate,
                    "avg_position": round(avg_position, 1),
                    "keywords": data["keywords"],
                    "threat_level": threat_level,
                })
        
        # Sort by appearances descending
        competitors.sort(key=lambda x: x["appearances"], reverse=True)
        
        return competitors
    
    async def analyze_keyword_overlap(
        self,
        user_domain: str,
        competitor_domains: List[str],
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
    ) -> Dict[str, Any]:
        """
        Analyze keyword overlap between user domain and competitors.
        
        Args:
            user_domain: User's domain
            competitor_domains: List of competitor domains to analyze
            keywords: List of keywords to check
            location_code: DataForSEO location code
            language_code: Language code
            
        Returns:
            {
                "total_keywords": int,
                "user_ranking_count": int,
                "overlap_analysis": [
                    {
                        "domain": str,
                        "shared_keywords": int,
                        "user_wins": int,  # user ranks higher
                        "competitor_wins": int,  # competitor ranks higher
                        "keywords": List[Dict],  # detailed comparison
                    }
                ]
            }
        """
        # Fetch SERP results
        serp_results = await self.fetch_serp_results(
            keywords=keywords,
            location_code=location_code,
            language_code=language_code,
        )
        
        # Build domain position map
        domain_positions: Dict[str, Dict[str, int]] = {
            user_domain: {},
        }
        for domain in competitor_domains:
            domain_positions[domain] = {}
        
        for result in serp_results:
            keyword = result["keyword"]
            
            for organic in result["organic_results"]:
                domain = organic["domain"]
                position = organic["position"]
                
                if domain in domain_positions:
                    domain_positions[domain][keyword] = position
        
        # Analyze overlap
        user_ranking_count = len(domain_positions[user_domain])
        
        overlap_analysis = []
        for competitor_domain in competitor_domains:
            shared_keywords = set(domain_positions[user_domain].keys()) & set(
                domain_positions[competitor_domain].keys()
            )
            
            user_wins = 0
            competitor_wins = 0
            keyword_details = []
            
            for keyword in shared_keywords:
                user_pos = domain_positions[user_domain][keyword]
                comp_pos = domain_positions[competitor_domain][keyword]
                
                if user_pos < comp_pos:
                    user_wins += 1
                    winner = "user"
                elif comp_pos < user_pos:
                    competitor_wins += 1
                    winner = "competitor"
                else:
                    winner = "tie"
                
                keyword_details.append({
                    "keyword": keyword,
                    "user_position": user_pos,
                    "competitor_position": comp_pos,
                    "winner": winner,
                    "position_gap": comp_pos - user_pos,
                })
            
            overlap_analysis.append({
                "domain": competitor_domain,
                "shared_keywords": len(shared_keywords),
                "user_wins": user_wins,
                "competitor_wins": competitor_wins,
                "keywords": keyword_details,
            })
        
        return {
            "total_keywords": len(keywords),
            "user_ranking_count": user_ranking_count,
            "overlap_analysis": overlap_analysis,
        }
    
    async def batch_serp_fetch(
        self,
        keywords: List[str],
        batch_size: int = 10,
        location_code: int = 2840,
        language_code: str = "en",
        use_cache: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Fetch SERP results in batches to handle large keyword lists.
        
        Args:
            keywords: List of keywords
            batch_size: Number of keywords per batch
            location_code: DataForSEO location code
            language_code: Language code
            use_cache: Whether to use caching
            
        Returns:
            List of all SERP results
        """
        all_results = []
        
        for i in range(0, len(keywords), batch_size):
            batch = keywords[i:i + batch_size]
            logger.info(f"Fetching batch {i // batch_size + 1}: {len(batch)} keywords")
            
            results = await self.fetch_serp_results(
                keywords=batch,
                location_code=location_code,
                language_code=language_code,
                use_cache=use_cache,
            )
            
            all_results.extend(results)
            
            # Small delay between batches
            if i + batch_size < len(keywords):
                await asyncio.sleep(0.5)
        
        return all_results
    
    def calculate_visual_position(
        self, organic_position: int, serp_features: Dict[str, Any]
    ) -> float:
        """
        Calculate visual position accounting for SERP features.
        
        SERP features push organic results down. This calculates an adjusted
        "visual position" that represents where the result actually appears
        on the page.
        
        Position weights:
        - Featured snippet: +2 positions
        - Each PAA question: +0.5 positions
        - Knowledge graph: +1.5 positions
        - Local pack: +3 positions
        - Video carousel: +2 positions
        - Image pack: +1 position
        - Shopping results: +2 positions
        - Top stories: +1.5 positions
        - AI overview: +3 positions
        
        Args:
            organic_position: Organic ranking position
            serp_features: SERP features dict from _extract_serp_features
            
        Returns:
            Adjusted visual position
        """
        displacement = 0
        
        if serp_features.get("featured_snippet"):
            displacement += 2
        
        if serp_features.get("people_also_ask", {}).get("present"):
            paa_count = serp_features["people_also_ask"]["count"]
            displacement += paa_count * 0.5
        
        if serp_features.get("knowledge_graph"):
            displacement += 1.5
        
        if serp_features.get("local_pack"):
            displacement += 3
        
        if serp_features.get("video"):
            displacement += 2
        
        if serp_features.get("image"):
            displacement += 1
        
        if serp_features.get("shopping"):
            displacement += 2
        
        if serp_features.get("top_stories"):
            displacement += 1.5
        
        if serp_features.get("ai_overview"):
            displacement += 3
        
        return organic_position + displacement