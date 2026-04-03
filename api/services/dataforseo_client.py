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
    
    # Rate limits (requests per second)
    RATE_LIMIT_RPS = 2  # DataForSEO standard limit
    RATE_LIMIT_WINDOW = 1.0  # seconds
    
    # Request timeouts
    REQUEST_TIMEOUT = 30.0
    
    # Batch settings
    MAX_BATCH_SIZE = 100  # Max keywords per batch request
    
    def __init__(
        self,
        login: Optional[str] = None,
        password: Optional[str] = None,
        cache_ttl_hours: int = 24,
        supabase_client: Optional[Any] = None,
    ):
        """
        Initialize DataForSEO client.
        
        Args:
            login: DataForSEO API login (default: env DATAFORSEO_LOGIN)
            password: DataForSEO API password (default: env DATAFORSEO_PASSWORD)
            cache_ttl_hours: Hours to cache responses (default: 24)
            supabase_client: Optional Supabase client for caching
        """
        self.login = login or os.getenv("DATAFORSEO_LOGIN")
        self.password = password or os.getenv("DATAFORSEO_PASSWORD")
        self.cache_ttl_hours = cache_ttl_hours
        self.supabase = supabase_client
        
        if not self.login or not self.password:
            raise DataForSEOAuthError(
                "DataForSEO credentials not provided. "
                "Set DATAFORSEO_LOGIN and DATAFORSEO_PASSWORD environment variables."
            )
        
        self.client: Optional[httpx.AsyncClient] = None
        self.authenticated = False
        
        # Rate limiting state
        self._request_times: List[float] = []
        self._rate_limit_lock = asyncio.Lock()
        
        # Request stats
        self.total_requests = 0
        self.cache_hits = 0
        self.cache_misses = 0
    
    async def __aenter__(self):
        """Async context manager entry"""
        await self.authenticate()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close()
    
    async def authenticate(self) -> bool:
        """
        Authenticate with DataForSEO API.
        
        Returns:
            True if authentication successful
            
        Raises:
            DataForSEOAuthError: If authentication fails
        """
        if self.authenticated and self.client:
            return True
        
        try:
            self.client = httpx.AsyncClient(
                auth=(self.login, self.password),
                timeout=self.REQUEST_TIMEOUT,
                limits=httpx.Limits(max_keepalive_connections=5, max_connections=10),
            )
            
            # Test authentication with a simple request
            response = await self._make_request("GET", "/serp/google/locations")
            
            if response.get("status_code") == 20000:
                self.authenticated = True
                logger.info("DataForSEO authentication successful")
                return True
            else:
                raise DataForSEOAuthError(
                    f"Authentication failed: {response.get('status_message')}"
                )
        
        except httpx.HTTPError as e:
            raise DataForSEOAuthError(f"Authentication request failed: {str(e)}")
    
    async def close(self):
        """Close the HTTP client"""
        if self.client:
            await self.client.aclose()
            self.client = None
            self.authenticated = False
    
    async def _enforce_rate_limit(self):
        """Enforce rate limiting using token bucket algorithm"""
        async with self._rate_limit_lock:
            now = asyncio.get_event_loop().time()
            
            # Remove timestamps outside the window
            cutoff = now - self.RATE_LIMIT_WINDOW
            self._request_times = [t for t in self._request_times if t > cutoff]
            
            # Check if we're at the limit
            if len(self._request_times) >= self.RATE_LIMIT_RPS:
                # Calculate wait time
                oldest = self._request_times[0]
                wait_time = self.RATE_LIMIT_WINDOW - (now - oldest)
                if wait_time > 0:
                    logger.debug(f"Rate limit reached, waiting {wait_time:.2f}s")
                    await asyncio.sleep(wait_time)
                    now = asyncio.get_event_loop().time()
            
            # Add current request timestamp
            self._request_times.append(now)
    
    def _get_cache_key(self, endpoint: str, params: Dict[str, Any]) -> str:
        """Generate cache key from endpoint and parameters"""
        # Sort params for consistent hashing
        param_str = json.dumps(params, sort_keys=True)
        key_input = f"{endpoint}:{param_str}"
        return hashlib.sha256(key_input.encode()).hexdigest()
    
    async def _get_cached_response(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """Retrieve cached response from Supabase"""
        if not self.supabase:
            return None
        
        try:
            result = self.supabase.table("dataforseo_cache").select("*").eq(
                "cache_key", cache_key
            ).gte(
                "expires_at", datetime.utcnow().isoformat()
            ).limit(1).execute()
            
            if result.data and len(result.data) > 0:
                self.cache_hits += 1
                logger.debug(f"Cache hit for key: {cache_key[:16]}...")
                return result.data[0]["response_data"]
            
            self.cache_misses += 1
            return None
        
        except Exception as e:
            logger.warning(f"Cache retrieval error: {e}")
            return None
    
    async def _cache_response(
        self, cache_key: str, endpoint: str, response: Dict[str, Any]
    ):
        """Cache response in Supabase"""
        if not self.supabase:
            return
        
        try:
            expires_at = datetime.utcnow() + timedelta(hours=self.cache_ttl_hours)
            
            self.supabase.table("dataforseo_cache").upsert({
                "cache_key": cache_key,
                "endpoint": endpoint,
                "response_data": response,
                "expires_at": expires_at.isoformat(),
                "created_at": datetime.utcnow().isoformat(),
            }).execute()
            
            logger.debug(f"Cached response for key: {cache_key[:16]}...")
        
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
    ) -> Dict[str, Any]:
        """
        Make HTTP request to DataForSEO API with rate limiting and caching.
        
        Args:
            method: HTTP method (GET, POST)
            endpoint: API endpoint path
            data: Request payload for POST requests
            use_cache: Whether to use cache for GET requests
            
        Returns:
            API response as dictionary
            
        Raises:
            DataForSEOError: On API errors
            DataForSEORateLimitError: On rate limit errors
        """
        if not self.authenticated:
            await self.authenticate()
        
        # Check cache for GET requests
        if method == "GET" and use_cache:
            cache_key = self._get_cache_key(endpoint, data or {})
            cached = await self._get_cached_response(cache_key)
            if cached:
                return cached
        
        # Enforce rate limiting
        await self._enforce_rate_limit()
        
        # Make request
        url = f"{self.BASE_URL}{endpoint}"
        
        try:
            if method == "GET":
                response = await self.client.get(url)
            elif method == "POST":
                response = await self.client.post(url, json=data)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            self.total_requests += 1
            
            # Handle HTTP errors
            if response.status_code == 401:
                self.authenticated = False
                raise DataForSEOAuthError("Authentication failed")
            elif response.status_code == 429:
                raise DataForSEORateLimitError("Rate limit exceeded")
            
            response.raise_for_status()
            
            result = response.json()
            
            # Check DataForSEO status code
            if result.get("status_code") != 20000:
                error_msg = result.get("status_message", "Unknown error")
                if result.get("status_code") == 40401:
                    raise DataForSEORateLimitError(f"Rate limit: {error_msg}")
                raise DataForSEOError(f"API error: {error_msg}")
            
            # Cache successful GET responses
            if method == "GET" and use_cache:
                await self._cache_response(cache_key, endpoint, result)
            
            return result
        
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error: {e.response.status_code} - {e.response.text}")
            raise DataForSEOError(f"HTTP {e.response.status_code}: {e.response.text}")
        except httpx.HTTPError as e:
            logger.error(f"Request error: {str(e)}")
            raise
    
    async def fetch_serp_results(
        self,
        keywords: List[str],
        location_code: int = 2840,  # United States
        language_code: str = "en",
        device: str = "desktop",
        depth: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Fetch live SERP results for multiple keywords.
        
        Args:
            keywords: List of keywords to fetch results for
            location_code: DataForSEO location code (default: 2840 = US)
            language_code: Language code (default: "en")
            device: Device type: "desktop" or "mobile"
            depth: Number of results to retrieve (max 100)
            
        Returns:
            List of SERP result dictionaries, one per keyword
            
        Example result structure:
            [
                {
                    "keyword": "best crm software",
                    "location": 2840,
                    "language": "en",
                    "device": "desktop",
                    "serp_features": ["featured_snippet", "people_also_ask"],
                    "serp_feature_details": {...},
                    "organic_results": [
                        {
                            "position": 1,
                            "url": "https://example.com/page",
                            "domain": "example.com",
                            "title": "...",
                            "description": "...",
                            "visual_position": 3.0,  # Accounting for SERP features
                        }
                    ],
                    "top_domains": ["example.com", "competitor.com"],
                    "total_results": 1250000000,
                }
            ]
        """
        if not keywords:
            return []
        
        # Split into batches
        batches = [
            keywords[i : i + self.MAX_BATCH_SIZE]
            for i in range(0, len(keywords), self.MAX_BATCH_SIZE)
        ]
        
        all_results = []
        
        for batch in batches:
            # Prepare batch request
            tasks = [batch]
            post_data = []
            
            for keyword in batch:
                post_data.append({
                    "keyword": keyword,
                    "location_code": location_code,
                    "language_code": language_code,
                    "device": device,
                    "os": "windows" if device == "desktop" else "ios",
                    "depth": depth,
                })
            
            # Make request
            endpoint = "/serp/google/organic/live/advanced"
            response = await self._make_request("POST", endpoint, post_data, use_cache=True)
            
            # Process results
            for task_result in response.get("tasks", []):
                if task_result.get("status_code") != 20000:
                    logger.warning(
                        f"Task failed: {task_result.get('status_message')}"
                    )
                    continue
                
                for item in task_result.get("result", []):
                    parsed = self._parse_serp_result(item)
                    all_results.append(parsed)
        
        return all_results
    
    def _parse_serp_result(self, raw_result: Dict[str, Any]) -> Dict[str, Any]:
        """Parse raw DataForSEO SERP result into structured format"""
        keyword = raw_result.get("keyword", "")
        items = raw_result.get("items", [])
        
        # Extract organic results
        organic_results = []
        serp_features = set()
        serp_feature_details = {}
        
        for item in items:
            item_type = item.get("type", "")
            
            # Check if organic result
            if item_type == "organic":
                organic_results.append({
                    "position": item.get("rank_group", 0),
                    "url": item.get("url", ""),
                    "domain": item.get("domain", ""),
                    "title": item.get("title", ""),
                    "description": item.get("description", ""),
                    "breadcrumb": item.get("breadcrumb", ""),
                })
            
            # Detect SERP features
            for feature_name, item_types in self.SERP_FEATURE_TYPES.items():
                if item_type in item_types:
                    serp_features.add(feature_name)
                    
                    # Store feature details
                    if feature_name not in serp_feature_details:
                        serp_feature_details[feature_name] = []
                    
                    serp_feature_details[feature_name].append({
                        "type": item_type,
                        "position": item.get("rank_absolute", 0),
                        "data": self._extract_feature_data(feature_name, item),
                    })
        
        # Calculate visual positions
        for result in organic_results:
            result["visual_position"] = self._calculate_visual_position(
                result["position"], serp_feature_details, result["position"]
            )
        
        # Extract top domains
        top_domains = list({r["domain"] for r in organic_results if r["domain"]})[:10]
        
        return {
            "keyword": keyword,
            "location": raw_result.get("location_code", 0),
            "language": raw_result.get("language_code", ""),
            "device": raw_result.get("device", ""),
            "serp_features": sorted(list(serp_features)),
            "serp_feature_details": serp_feature_details,
            "organic_results": organic_results,
            "top_domains": top_domains,
            "total_results": raw_result.get("total_count", 0),
            "fetched_at": datetime.utcnow().isoformat(),
        }
    
    def _extract_feature_data(
        self, feature_name: str, item: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Extract relevant data from SERP feature item"""
        if feature_name == "people_also_ask":
            items = item.get("items", [])
            return {
                "questions": [q.get("title", "") for q in items],
                "count": len(items),
            }
        elif feature_name == "featured_snippet":
            return {
                "url": item.get("url", ""),
                "domain": item.get("domain", ""),
                "title": item.get("title", ""),
                "description": item.get("description", ""),
            }
        elif feature_name == "knowledge_graph":
            return {
                "title": item.get("title", ""),
                "description": item.get("description", ""),
            }
        elif feature_name == "ai_overview":
            return {
                "text": item.get("text", ""),
                "source_count": len(item.get("items", [])),
            }
        else:
            return {"type": item.get("type", "")}
    
    def _calculate_visual_position(
        self,
        organic_position: int,
        serp_features: Dict[str, List[Dict[str, Any]]],
        current_position: int,
    ) -> float:
        """
        Calculate visual position accounting for SERP features above the result.
        
        Args:
            organic_position: Organic ranking position
            serp_features: Dictionary of SERP features with positions
            current_position: The specific organic position to calculate for
            
        Returns:
            Visual position (organic position + displacement from features)
        """
        displacement = 0.0
        
        for feature_name, feature_items in serp_features.items():
            impact = self.SERP_FEATURE_VISUAL_IMPACT.get(feature_name, 0.0)
            
            if impact == 0.0:
                continue
            
            for feature_item in feature_items:
                feature_pos = feature_item.get("position", 999)
                
                # If feature appears before or at this organic position
                if feature_pos <= current_position:
                    if feature_name == "people_also_ask":
                        # Each PAA question adds 0.5 positions
                        paa_count = feature_item.get("data", {}).get("count", 1)
                        displacement += impact * paa_count
                    else:
                        displacement += impact
        
        return organic_position + displacement
    
    async def get_competitor_rankings(
        self,
        keywords: List[str],
        user_domain: str,
        location_code: int = 2840,
        language_code: str = "en",
    ) -> Dict[str, Any]:
        """
        Analyze competitor rankings across keywords.
        
        Args:
            keywords: List of keywords to analyze
            user_domain: The user's domain to compare against
            location_code: DataForSEO location code
            language_code: Language code
            
        Returns:
            Dictionary with competitor analysis:
            {
                "competitors": [
                    {
                        "domain": "competitor.com",
                        "keywords_shared": 23,
                        "avg_position": 4.2,
                        "keywords_above_user": 15,
                        "threat_level": "high"
                    }
                ],
                "user_stats": {
                    "keywords_ranking": 18,
                    "avg_position": 8.3,
                    "keywords_top10": 12
                }
            }
        """
        # Fetch SERP results for all keywords
        serp_results = await self.fetch_serp_results(
            keywords=keywords,
            location_code=location_code,
            language_code=language_code,
        )
        
        # Analyze competitor presence
        competitor_data: Dict[str, Dict[str, Any]] = {}
        user_rankings = []
        user_domain_clean = self._clean_domain(user_domain)
        
        for result in serp_results:
            keyword = result["keyword"]
            user_position = None
            
            for organic in result["organic_results"]:
                domain = self._clean_domain(organic["domain"])
                position = organic["position"]
                
                # Track user's position
                if domain == user_domain_clean:
                    user_position = position
                    user_rankings.append(position)
                
                # Track competitors
                if domain != user_domain_clean and position <= 10:
                    if domain not in competitor_data:
                        competitor_data[domain] = {
                            "domain": domain,
                            "keywords": [],
                            "positions": [],
                            "keywords_above_user": 0,
                        }
                    
                    competitor_data[domain]["keywords"].append(keyword)
                    competitor_data[domain]["positions"].append(position)
                    
                    if user_position and position < user_position:
                        competitor_data[domain]["keywords_above_user"] += 1
        
        # Calculate competitor metrics
        competitors = []
        for domain, data in competitor_data.items():
            avg_position = sum(data["positions"]) / len(data["positions"])
            keywords_shared = len(data["keywords"])
            
            # Determine threat level
            if keywords_shared >= len(keywords) * 0.5 and avg_position <= 5:
                threat_level = "high"
            elif keywords_shared >= len(keywords) * 0.3:
                threat_level = "medium"
            else:
                threat_level = "low"
            
            competitors.append({
                "domain": domain,
                "keywords_shared": keywords_shared,
                "avg_position": round(avg_position, 1),
                "keywords_above_user": data["keywords_above_user"],
                "threat_level": threat_level,
            })
        
        # Sort by keywords shared descending
        competitors.sort(key=lambda x: x["keywords_shared"], reverse=True)
        
        # User stats
        user_stats = {
            "keywords_ranking": len(user_rankings),
            "avg_position": round(sum(user_rankings) / len(user_rankings), 1) if user_rankings else None,
            "keywords_top10": len([p for p in user_rankings if p <= 10]),
        }
        
        return {
            "competitors": competitors,
            "user_stats": user_stats,
        }
    
    def _clean_domain(self, domain: str) -> str:
        """Clean and normalize domain name"""
        if not domain:
            return ""
        
        # Remove protocol
        domain = re.sub(r"^https?://", "", domain)
        
        # Remove www
        domain = re.sub(r"^www\.", "", domain)
        
        # Remove path
        domain = domain.split("/")[0]
        
        # Remove port
        domain = domain.split(":")[0]
        
        return domain.lower()
    
    async def get_serp_features_analysis(
        self,
        serp_results: List[Dict[str, Any]],
        user_domain: str,
    ) -> Dict[str, Any]:
        """
        Analyze SERP features and their impact on organic results.
        
        Args:
            serp_results: List of parsed SERP results from fetch_serp_results()
            user_domain: The user's domain
            
        Returns:
            Dictionary with SERP feature analysis:
            {
                "feature_frequency": {"featured_snippet": 12, "people_also_ask": 45},
                "displacement_impact": [
                    {
                        "keyword": "best crm",
                        "organic_position": 3,
                        "visual_position": 8,
                        "displacement": 5,
                        "features_above": ["featured_snippet", "paa_x4"]
                    }
                ],
                "feature_opportunities": [
                    {
                        "keyword": "crm pricing",
                        "feature": "featured_snippet",
                        "current_owner": "competitor.com",
                        "user_position": 4,
                        "recommendation": "Add FAQ schema targeting this query"
                    }
                ]
            }
        """
        feature_frequency: Dict[str, int] = {}
        displacement_impacts = []
        feature_opportunities = []
        user_domain_clean = self._clean_domain(user_domain)
        
        for result in serp_results:
            keyword = result["keyword"]
            
            # Count feature frequency
            for feature in result["serp_features"]:
                feature_frequency[feature] = feature_frequency.get(feature, 0) + 1
            
            # Analyze user's results
            user_result = None
            for organic in result["organic_results"]:
                if self._clean_domain(organic["domain"]) == user_domain_clean:
                    user_result = organic
                    break
            
            if user_result:
                organic_pos = user_result["position"]
                visual_pos = user_result["visual_position"]
                displacement = visual_pos - organic_pos
                
                # If significant displacement (>3 positions)
                if displacement > 3:
                    features_above = []
                    for feature, items in result["serp_feature_details"].items():
                        for item in items:
                            if item["position"] <= organic_pos:
                                if feature == "people_also_ask":
                                    count = item["data"].get("count", 1)
                                    features_above.append(f"paa_x{count}")
                                else:
                                    features_above.append(feature)
                    
                    displacement_impacts.append({
                        "keyword": keyword,
                        "organic_position": organic_pos,
                        "visual_position": round(visual_pos, 1),
                        "displacement": round(displacement, 1),
                        "features_above": features_above,
                    })
                
                # Check for feature opportunities
                if organic_pos <= 10:
                    for feature in ["featured_snippet", "people_also_ask"]:
                        if feature in result["serp_features"]:
                            feature_items = result["serp_feature_details"].get(feature, [])
                            if feature_items:
                                owner_domain = None
                                if feature == "featured_snippet":
                                    owner_domain = feature_items[0]["data"].get("domain", "")
                                
                                if owner_domain and self._clean_domain(owner_domain) != user_domain_clean:
                                    feature_opportunities.append({
                                        "keyword": keyword,
                                        "feature": feature,
                                        "current_owner": owner_domain,
                                        "user_position": organic_pos,
                                        "recommendation": self._get_feature_recommendation(feature),
                                    })
        
        # Sort displacement impacts by displacement descending
        displacement_impacts.sort(key=lambda x: x["displacement"], reverse=True)
        
        return {
            "feature_frequency": feature_frequency,
            "displacement_impact": displacement_impacts[:20],  # Top 20
            "feature_opportunities": feature_opportunities[:10],  # Top 10
        }
    
    def _get_feature_recommendation(self, feature: str) -> str:
        """Get optimization recommendation for SERP feature"""
        recommendations = {
            "featured_snippet": "Add structured FAQ or definition content targeting this query. Use clear question-answer format.",
            "people_also_ask": "Add FAQ schema markup with relevant questions. Create comprehensive content covering related queries.",
            "video": "Create or embed relevant video content. Add VideoObject schema markup.",
            "image": "Add high-quality images with descriptive alt text and schema markup.",
            "local_pack": "Optimize Google Business Profile. Ensure NAP consistency.",
        }
        return recommendations.get(feature, "Optimize content for this SERP feature type.")
    
    async def fetch_keyword_metrics(
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
                    "keyword": "best crm software",
                    "search_volume": 12000,
                    "difficulty": 68,
                    "cpc": 15.23,
                    "competition": 0.85
                }
            ]
        """
        # Split into batches
        batches = [
            keywords[i : i + self.MAX_BATCH_SIZE]
            for i in range(0, len(keywords), self.MAX_BATCH_SIZE)
        ]
        
        all_metrics = []
        
        for batch in batches:
            post_data = []
            for keyword in batch:
                post_data.append({
                    "keyword": keyword,
                    "location_code": location_code,
                    "language_code": language_code,
                })
            
            endpoint = "/keywords_data/google_ads/search_volume/live"
            response = await self._make_request("POST", endpoint, post_data, use_cache=True)
            
            for task_result in response.get("tasks", []):
                if task_result.get("status_code") != 20000:
                    continue
                
                for item in task_result.get("result", []):
                    all_metrics.append({
                        "keyword": item.get("keyword", ""),
                        "search_volume": item.get("search_volume", 0),
                        "competition": item.get("competition", 0.0),
                        "cpc": item.get("cpc", 0.0),
                    })
        
        return all_metrics
    
    async def batch_fetch_with_metrics(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
        include_metrics: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Fetch SERP results and optionally keyword metrics in parallel.
        
        Args:
            keywords: List of keywords
            location_code: DataForSEO location code
            language_code: Language code
            include_metrics: Whether to fetch search volume/difficulty
            
        Returns:
            List of combined SERP and metrics data
        """
        # Fetch SERP results and metrics in parallel
        tasks = [
            self.fetch_serp_results(keywords, location_code, language_code)
        ]
        
        if include_metrics:
            tasks.append(
                self.fetch_keyword_metrics(keywords, location_code, language_code)
            )
        
        results = await asyncio.gather(*tasks)
        serp_results = results[0]
        
        if include_metrics and len(results) > 1:
            metrics = results[1]
            
            # Merge metrics into SERP results
            metrics_map = {m["keyword"]: m for m in metrics}
            
            for serp in serp_results:
                keyword = serp["keyword"]
                if keyword in metrics_map:
                    serp["metrics"] = metrics_map[keyword]
        
        return serp_results
    
    def get_stats(self) -> Dict[str, Any]:
        """Get client usage statistics"""
        cache_hit_rate = (
            self.cache_hits / (self.cache_hits + self.cache_misses)
            if (self.cache_hits + self.cache_misses) > 0
            else 0.0
        )
        
        return {
            "total_requests": self.total_requests,
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "cache_hit_rate": round(cache_hit_rate, 2),
        }