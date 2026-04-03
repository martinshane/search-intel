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
    
    # Rate limiting: DataForSEO allows 2000 API calls per minute
    MAX_REQUESTS_PER_MINUTE = 2000
    MAX_CONCURRENT_REQUESTS = 50
    
    # Cache TTL: 24 hours for SERP results (they change daily)
    CACHE_TTL_SECONDS = 86400
    
    def __init__(
        self,
        username: Optional[str] = None,
        password: Optional[str] = None,
        supabase_client=None,
    ):
        """
        Initialize DataForSEO client.
        
        Args:
            username: DataForSEO API username (or set DATAFORSEO_USERNAME env var)
            password: DataForSEO API password (or set DATAFORSEO_PASSWORD env var)
            supabase_client: Optional Supabase client for caching
        """
        self.username = username or os.getenv("DATAFORSEO_USERNAME")
        self.password = password or os.getenv("DATAFORSEO_PASSWORD")
        
        if not self.username or not self.password:
            raise DataForSEOAuthError(
                "DataForSEO credentials not provided. Set DATAFORSEO_USERNAME and "
                "DATAFORSEO_PASSWORD environment variables or pass them to constructor."
            )
        
        self.supabase = supabase_client
        self.client: Optional[httpx.AsyncClient] = None
        
        # Rate limiting
        self._request_times: List[datetime] = []
        self._rate_limit_lock = asyncio.Lock()
        self._semaphore = asyncio.Semaphore(self.MAX_CONCURRENT_REQUESTS)
        
        # Authentication state
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
        Tests authentication by making a ping request.
        """
        if self._authenticated and self.client:
            return
        
        auth = (self.username, self.password)
        self.client = httpx.AsyncClient(
            auth=auth,
            timeout=httpx.Timeout(30.0, connect=10.0),
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=100),
        )
        
        # Test authentication
        try:
            response = await self.client.get(f"{self.BASE_URL}/serp/google/organic/live/advanced")
            if response.status_code == 401:
                raise DataForSEOAuthError("Invalid DataForSEO credentials")
            self._authenticated = True
            logger.info("DataForSEO client authenticated successfully")
        except httpx.HTTPError as e:
            raise DataForSEOAuthError(f"Failed to authenticate with DataForSEO: {str(e)}")
    
    async def close(self):
        """Close the HTTP client"""
        if self.client:
            await self.client.aclose()
            self.client = None
            self._authenticated = False
    
    async def _wait_for_rate_limit(self):
        """
        Ensure we don't exceed rate limits.
        Implements a sliding window rate limiter.
        """
        async with self._rate_limit_lock:
            now = datetime.utcnow()
            # Remove requests older than 1 minute
            cutoff = now - timedelta(minutes=1)
            self._request_times = [t for t in self._request_times if t > cutoff]
            
            # If we're at the limit, wait until the oldest request expires
            if len(self._request_times) >= self.MAX_REQUESTS_PER_MINUTE:
                oldest = self._request_times[0]
                wait_seconds = 60 - (now - oldest).total_seconds()
                if wait_seconds > 0:
                    logger.warning(f"Rate limit reached, waiting {wait_seconds:.2f}s")
                    await asyncio.sleep(wait_seconds)
                    # Recurse to re-check after waiting
                    return await self._wait_for_rate_limit()
            
            self._request_times.append(now)
    
    def _generate_cache_key(self, endpoint: str, params: Dict[str, Any]) -> str:
        """
        Generate a cache key for a request.
        
        Args:
            endpoint: API endpoint
            params: Request parameters
        
        Returns:
            SHA256 hash of the request
        """
        # Sort params for consistent hashing
        sorted_params = json.dumps(params, sort_keys=True)
        key_string = f"{endpoint}:{sorted_params}"
        return hashlib.sha256(key_string.encode()).hexdigest()
    
    async def _get_cached_response(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve cached response from Supabase.
        
        Args:
            cache_key: Cache key
        
        Returns:
            Cached response dict or None if not found/expired
        """
        if not self.supabase:
            return None
        
        try:
            result = self.supabase.table("dataforseo_cache").select("*").eq("cache_key", cache_key).execute()
            
            if result.data and len(result.data) > 0:
                cached = result.data[0]
                cached_at = datetime.fromisoformat(cached["cached_at"].replace("Z", "+00:00"))
                age_seconds = (datetime.utcnow().replace(tzinfo=cached_at.tzinfo) - cached_at).total_seconds()
                
                if age_seconds < self.CACHE_TTL_SECONDS:
                    logger.info(f"Cache hit for key {cache_key[:16]}... (age: {age_seconds:.0f}s)")
                    return cached["response_data"]
                else:
                    logger.info(f"Cache expired for key {cache_key[:16]}...")
        except Exception as e:
            logger.error(f"Error retrieving from cache: {str(e)}")
        
        return None
    
    async def _set_cached_response(self, cache_key: str, response: Dict[str, Any]):
        """
        Store response in cache.
        
        Args:
            cache_key: Cache key
            response: Response data to cache
        """
        if not self.supabase:
            return
        
        try:
            self.supabase.table("dataforseo_cache").upsert({
                "cache_key": cache_key,
                "response_data": response,
                "cached_at": datetime.utcnow().isoformat(),
            }).execute()
            logger.info(f"Cached response for key {cache_key[:16]}...")
        except Exception as e:
            logger.error(f"Error caching response: {str(e)}")
    
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
        Make an authenticated request to DataForSEO API with retry logic.
        
        Args:
            method: HTTP method (GET or POST)
            endpoint: API endpoint path
            data: Request payload for POST requests
            use_cache: Whether to use caching
        
        Returns:
            API response as dict
        
        Raises:
            DataForSEOError: On API errors
            DataForSEORateLimitError: On rate limit errors
        """
        if not self.client:
            await self.authenticate()
        
        url = f"{self.BASE_URL}/{endpoint.lstrip('/')}"
        
        # Check cache for POST requests (GET requests typically not used for SERP data)
        cache_key = None
        if use_cache and method == "POST" and data:
            cache_key = self._generate_cache_key(endpoint, data)
            cached = await self._get_cached_response(cache_key)
            if cached:
                return cached
        
        # Rate limiting
        await self._wait_for_rate_limit()
        
        async with self._semaphore:
            try:
                if method == "POST":
                    response = await self.client.post(url, json=data)
                else:
                    response = await self.client.get(url)
                
                # Handle rate limiting
                if response.status_code == 429:
                    raise DataForSEORateLimitError("Rate limit exceeded")
                
                # Handle auth errors
                if response.status_code == 401:
                    raise DataForSEOAuthError("Authentication failed")
                
                response.raise_for_status()
                result = response.json()
                
                # DataForSEO wraps responses in a status object
                if result.get("status_code") != 20000:
                    error_msg = result.get("status_message", "Unknown error")
                    raise DataForSEOError(f"API error: {error_msg}")
                
                # Cache successful responses
                if cache_key and use_cache:
                    await self._set_cached_response(cache_key, result)
                
                return result
            
            except httpx.HTTPStatusError as e:
                logger.error(f"HTTP error {e.response.status_code}: {e.response.text}")
                raise DataForSEOError(f"HTTP error: {str(e)}")
            except httpx.HTTPError as e:
                logger.error(f"Request error: {str(e)}")
                raise
    
    async def get_serp_results(
        self,
        keywords: List[str],
        location_code: int = 2840,  # USA
        language_code: str = "en",
        depth: int = 100,  # Number of results to fetch
        device: str = "desktop",
    ) -> List[Dict[str, Any]]:
        """
        Fetch live SERP results for multiple keywords.
        
        Args:
            keywords: List of keywords to fetch
            location_code: DataForSEO location code (2840 = USA)
            language_code: Language code (en, es, etc.)
            depth: Number of results to fetch per keyword (max 700)
            device: Device type (desktop, mobile)
        
        Returns:
            List of result dicts, one per keyword, each containing:
            {
                "keyword": str,
                "location_code": int,
                "language_code": str,
                "se_results_count": int,
                "items_count": int,
                "items": [
                    {
                        "type": str,  # "organic", "paid", "featured_snippet", etc.
                        "rank_group": int,  # Position in SERP
                        "rank_absolute": int,  # Absolute position
                        "position": str,  # Position with SERP feature context
                        "xpath": str,
                        "domain": str,
                        "title": str,
                        "description": str,
                        "url": str,
                        "breadcrumb": str,
                        ...
                    },
                    ...
                ]
            }
        """
        if not keywords:
            return []
        
        # Prepare batch request (DataForSEO supports up to 100 tasks per request)
        tasks = []
        for keyword in keywords[:100]:  # Limit to 100 per batch
            tasks.append({
                "keyword": keyword,
                "location_code": location_code,
                "language_code": language_code,
                "device": device,
                "os": "windows" if device == "desktop" else "ios",
                "depth": depth,
                "calculate_rectangles": True,  # For visual position calculation
            })
        
        endpoint = "serp/google/organic/live/advanced"
        response = await self._make_request("POST", endpoint, data=tasks)
        
        # Parse results
        results = []
        if "tasks" in response:
            for task in response["tasks"]:
                if task.get("status_code") == 20000 and task.get("result"):
                    for result_item in task["result"]:
                        results.append({
                            "keyword": result_item.get("keyword"),
                            "location_code": result_item.get("location_code"),
                            "language_code": result_item.get("language_code"),
                            "se_results_count": result_item.get("se_results_count", 0),
                            "items_count": result_item.get("items_count", 0),
                            "items": result_item.get("items", []),
                        })
        
        return results
    
    def parse_serp_features(self, serp_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse SERP features from a single SERP result.
        
        Args:
            serp_result: Single SERP result dict from get_serp_results()
        
        Returns:
            Dict containing:
            {
                "keyword": str,
                "features_present": [str],  # List of feature types found
                "feature_details": {
                    "featured_snippet": {...},
                    "people_also_ask": {"count": int, "questions": [str]},
                    "knowledge_graph": {...},
                    "local_pack": {"businesses": [...]},
                    "video": {"count": int, "sources": [str]},
                    ...
                },
                "organic_results": [
                    {
                        "rank_absolute": int,
                        "domain": str,
                        "url": str,
                        "title": str,
                        "description": str,
                        "visual_position": float,  # Adjusted for SERP features
                    }
                ],
                "visual_displacement_score": float,  # Total positions pushed down
            }
        """
        keyword = serp_result.get("keyword", "")
        items = serp_result.get("items", [])
        
        features_present: Set[str] = set()
        feature_details: Dict[str, Any] = {}
        organic_results: List[Dict[str, Any]] = []
        visual_displacement = 0.0
        
        paa_questions = []
        video_sources = []
        
        for item in items:
            item_type = item.get("type", "")
            rank_absolute = item.get("rank_absolute", 0)
            
            # Categorize SERP feature
            feature_category = self._categorize_serp_feature(item_type)
            
            if feature_category:
                features_present.add(feature_category)
                
                # Extract feature-specific details
                if feature_category == "featured_snippet":
                    feature_details["featured_snippet"] = {
                        "title": item.get("title", ""),
                        "description": item.get("description", ""),
                        "url": item.get("url", ""),
                        "domain": item.get("domain", ""),
                    }
                    visual_displacement += self.SERP_FEATURE_VISUAL_IMPACT.get(feature_category, 0)
                
                elif feature_category == "people_also_ask":
                    paa_items = item.get("items", [])
                    paa_questions.extend([paa.get("title", "") for paa in paa_items])
                    visual_displacement += self.SERP_FEATURE_VISUAL_IMPACT.get(feature_category, 0) * len(paa_items)
                
                elif feature_category == "knowledge_graph":
                    feature_details["knowledge_graph"] = {
                        "title": item.get("title", ""),
                        "description": item.get("description", ""),
                        "card_id": item.get("card_id", ""),
                    }
                    visual_displacement += self.SERP_FEATURE_VISUAL_IMPACT.get(feature_category, 0)
                
                elif feature_category == "local_pack":
                    feature_details["local_pack"] = {
                        "businesses": [
                            {
                                "title": biz.get("title", ""),
                                "domain": biz.get("domain", ""),
                                "rating": biz.get("rating", {}).get("value"),
                            }
                            for biz in item.get("items", [])
                        ]
                    }
                    visual_displacement += self.SERP_FEATURE_VISUAL_IMPACT.get(feature_category, 0)
                
                elif feature_category == "video":
                    video_items = item.get("items", [])
                    video_sources.extend([v.get("source", "") for v in video_items])
                    visual_displacement += self.SERP_FEATURE_VISUAL_IMPACT.get(feature_category, 0)
                
                elif feature_category == "ai_overview":
                    feature_details["ai_overview"] = {
                        "text": item.get("text", ""),
                        "links_count": len(item.get("links", [])),
                    }
                    visual_displacement += self.SERP_FEATURE_VISUAL_IMPACT.get(feature_category, 0)
                
                else:
                    # Generic feature tracking
                    visual_displacement += self.SERP_FEATURE_VISUAL_IMPACT.get(feature_category, 0)
            
            # Track organic results
            if item_type == "organic":
                organic_results.append({
                    "rank_absolute": rank_absolute,
                    "domain": item.get("domain", ""),
                    "url": item.get("url", ""),
                    "title": item.get("title", ""),
                    "description": item.get("description", ""),
                    "visual_position": rank_absolute + visual_displacement,
                })
        
        # Add aggregated feature details
        if paa_questions:
            feature_details["people_also_ask"] = {
                "count": len(paa_questions),
                "questions": paa_questions[:10],  # Limit to first 10
            }
        
        if video_sources:
            feature_details["video"] = {
                "count": len(video_sources),
                "sources": list(set(video_sources)),  # Unique sources
            }
        
        return {
            "keyword": keyword,
            "features_present": sorted(list(features_present)),
            "feature_details": feature_details,
            "organic_results": organic_results,
            "visual_displacement_score": round(visual_displacement, 2),
        }
    
    def _categorize_serp_feature(self, item_type: str) -> Optional[str]:
        """
        Categorize a DataForSEO item type into a standard SERP feature category.
        
        Args:
            item_type: DataForSEO item type string
        
        Returns:
            Standardized feature category or None if not a feature
        """
        for category, types in self.SERP_FEATURE_TYPES.items():
            if item_type in types:
                return category
        return None
    
    def extract_competitor_data(
        self,
        serp_results: List[Dict[str, Any]],
        user_domain: str,
    ) -> Dict[str, Any]:
        """
        Extract competitor analysis from SERP results.
        
        Args:
            serp_results: List of SERP results from get_serp_results()
            user_domain: The user's domain to exclude from competitors
        
        Returns:
            Dict containing:
            {
                "total_keywords_analyzed": int,
                "user_domain": str,
                "competitors": [
                    {
                        "domain": str,
                        "keywords_shared": int,
                        "avg_position": float,
                        "position_distribution": {
                            "top_3": int,
                            "top_5": int,
                            "top_10": int,
                        },
                        "keywords": [
                            {
                                "keyword": str,
                                "position": int,
                                "url": str,
                                "user_position": int,  # User's position for this keyword
                            }
                        ],
                        "threat_level": str,  # high, medium, low
                    }
                ],
                "user_performance": {
                    "keywords_ranking": int,
                    "avg_position": float,
                    "top_3_count": int,
                    "top_5_count": int,
                    "top_10_count": int,
                },
            }
        """
        # Normalize user domain
        user_domain_normalized = self._normalize_domain(user_domain)
        
        # Track competitor appearances
        competitor_data: Dict[str, Dict[str, Any]] = {}
        user_positions: Dict[str, int] = {}
        total_keywords = len(serp_results)
        
        for serp_result in serp_results:
            keyword = serp_result.get("keyword", "")
            
            for item in serp_result.get("items", []):
                if item.get("type") != "organic":
                    continue
                
                domain = self._normalize_domain(item.get("domain", ""))
                position = item.get("rank_absolute", 999)
                url = item.get("url", "")
                
                if not domain or position > 100:
                    continue
                
                # Track user's position
                if domain == user_domain_normalized:
                    user_positions[keyword] = position
                    continue
                
                # Track competitors
                if domain not in competitor_data:
                    competitor_data[domain] = {
                        "domain": domain,
                        "keywords": [],
                        "positions": [],
                    }
                
                competitor_data[domain]["keywords"].append({
                    "keyword": keyword,
                    "position": position,
                    "url": url,
                    "user_position": user_positions.get(keyword, None),
                })
                competitor_data[domain]["positions"].append(position)
        
        # Calculate competitor metrics
        competitors = []
        for domain, data in competitor_data.items():
            positions = data["positions"]
            keywords_shared = len(positions)
            
            # Only include competitors appearing in at least 10% of keywords or 3+ keywords
            if keywords_shared < max(3, total_keywords * 0.1):
                continue
            
            avg_position = sum(positions) / len(positions)
            
            position_dist = {
                "top_3": sum(1 for p in positions if p <= 3),
                "top_5": sum(1 for p in positions if p <= 5),
                "top_10": sum(1 for p in positions if p <= 10),
            }
            
            # Determine threat level
            threat_level = "low"
            if avg_position <= 5 and keywords_shared >= total_keywords * 0.2:
                threat_level = "high"
            elif avg_position <= 10 and keywords_shared >= total_keywords * 0.15:
                threat_level = "medium"
            
            competitors.append({
                "domain": domain,
                "keywords_shared": keywords_shared,
                "avg_position": round(avg_position, 2),
                "position_distribution": position_dist,
                "keywords": sorted(data["keywords"], key=lambda x: x["position"])[:20],  # Top 20 keywords
                "threat_level": threat_level,
            })
        
        # Sort competitors by threat level and shared keywords
        threat_order = {"high": 0, "medium": 1, "low": 2}
        competitors.sort(key=lambda x: (threat_order[x["threat_level"]], -x["keywords_shared"]))
        
        # Calculate user performance
        user_position_list = list(user_positions.values())
        user_performance = {
            "keywords_ranking": len(user_position_list),
            "avg_position": round(sum(user_position_list) / len(user_position_list), 2) if user_position_list else 0,
            "top_3_count": sum(1 for p in user_position_list if p <= 3),
            "top_5_count": sum(1 for p in user_position_list if p <= 5),
            "top_10_count": sum(1 for p in user_position_list if p <= 10),
        }
        
        return {
            "total_keywords_analyzed": total_keywords,
            "user_domain": user_domain,
            "competitors": competitors[:20],  # Top 20 competitors
            "user_performance": user_performance,
        }
    
    def _normalize_domain(self, domain: str) -> str:
        """
        Normalize a domain by removing www., protocol, and paths.
        
        Args:
            domain: Domain string to normalize
        
        Returns:
            Normalized domain
        """
        if not domain:
            return ""
        
        # Remove protocol
        domain = re.sub(r'^https?://', '', domain)
        
        # Remove www.
        domain = re.sub(r'^www\.', '', domain)
        
        # Remove path
        domain = domain.split('/')[0]
        
        # Remove port
        domain = domain.split(':')[0]
        
        return domain.lower().strip()
    
    async def get_keyword_metrics(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
    ) -> List[Dict[str, Any]]:
        """
        Fetch keyword difficulty and search volume metrics.
        
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
                    "competition": float,  # 0-1
                    "cpc": float,
                    "difficulty": int,  # 0-100
                },
                ...
            ]
        """
        if not keywords:
            return []
        
        # Prepare batch request
        tasks = []
        for keyword in keywords[:1000]:  # Limit to 1000
            tasks.append({
                "keyword": keyword,
                "location_code": location_code,
                "language_code": language_code,
            })
        
        endpoint = "dataforseo_labs/google/keywords_for_keywords/live"
        response = await self._make_request("POST", endpoint, data=tasks)
        
        # Parse results
        results = []
        if "tasks" in response:
            for task in response["tasks"]:
                if task.get("status_code") == 20000 and task.get("result"):
                    for result_item in task["result"]:
                        for item in result_item.get("items", []):
                            results.append({
                                "keyword": item.get("keyword", ""),
                                "search_volume": item.get("keyword_info", {}).get("search_volume", 0),
                                "competition": item.get("keyword_info", {}).get("competition", 0),
                                "cpc": item.get("keyword_info", {}).get("cpc", 0),
                                "difficulty": item.get("keyword_properties", {}).get("keyword_difficulty", 0),
                            })
        
        return results
    
    async def batch_fetch_serp_results(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
        batch_size: int = 100,
        delay_between_batches: float = 1.0,
    ) -> List[Dict[str, Any]]:
        """
        Fetch SERP results for a large list of keywords in batches.
        
        Args:
            keywords: List of keywords
            location_code: DataForSEO location code
            language_code: Language code
            batch_size: Keywords per batch (max 100)
            delay_between_batches: Seconds to wait between batches
        
        Returns:
            Combined list of SERP results
        """
        all_results = []
        
        for i in range(0, len(keywords), batch_size):
            batch = keywords[i:i + batch_size]
            logger.info(f"Fetching SERP results for batch {i // batch_size + 1} ({len(batch)} keywords)")
            
            try:
                results = await self.get_serp_results(
                    keywords=batch,
                    location_code=location_code,
                    language_code=language_code,
                )
                all_results.extend(results)
                
                # Delay between batches to be nice to the API
                if i + batch_size < len(keywords):
                    await asyncio.sleep(delay_between_batches)
            
            except Exception as e:
                logger.error(f"Error fetching batch {i // batch_size + 1}: {str(e)}")
                # Continue with next batch
                continue
        
        logger.info(f"Fetched SERP results for {len(all_results)} keywords total")
        return all_results