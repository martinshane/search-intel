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
    
    # Position-based CTR curves (baseline, adjusted by SERP features)
    BASELINE_CTR_CURVE = {
        1: 0.394, 2: 0.181, 3: 0.105, 4: 0.072, 5: 0.053,
        6: 0.041, 7: 0.033, 8: 0.027, 9: 0.023, 10: 0.020,
        11: 0.014, 12: 0.012, 13: 0.010, 14: 0.009, 15: 0.008,
        16: 0.007, 17: 0.006, 18: 0.006, 19: 0.005, 20: 0.005,
    }
    
    def __init__(
        self,
        login: Optional[str] = None,
        password: Optional[str] = None,
        timeout: int = 60,
        max_retries: int = 3,
        supabase_client: Optional[Any] = None,
        cache_ttl_hours: int = 24,
        rate_limit_per_second: float = 2.0,
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
            raise DataForSEOAuthError(
                "DataForSEO credentials not found. Set DATAFORSEO_LOGIN and DATAFORSEO_PASSWORD environment variables."
            )
        
        self.timeout = timeout
        self.max_retries = max_retries
        self.supabase = supabase_client
        self.cache_ttl_hours = cache_ttl_hours
        self.rate_limit_per_second = rate_limit_per_second
        
        self._client: Optional[httpx.AsyncClient] = None
        self._auth_validated = False
        self._last_request_time = 0.0
        self._request_lock = asyncio.Lock()
    
    async def __aenter__(self):
        """Async context manager entry"""
        await self.authenticate()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close()
    
    async def authenticate(self):
        """Validate credentials and initialize HTTP client"""
        if self._client is None:
            self._client = httpx.AsyncClient(
                auth=(self.login, self.password),
                timeout=self.timeout,
                headers={
                    "Content-Type": "application/json",
                },
            )
        
        if not self._auth_validated:
            # Test authentication with a cheap API call
            try:
                response = await self._client.get(f"{self.BASE_URL}/serp/google/languages")
                response.raise_for_status()
                data = response.json()
                
                if data.get("status_code") != 20000:
                    raise DataForSEOAuthError(
                        f"Authentication failed: {data.get('status_message', 'Unknown error')}"
                    )
                
                self._auth_validated = True
                logger.info("DataForSEO authentication successful")
            
            except httpx.HTTPStatusError as e:
                raise DataForSEOAuthError(f"Authentication failed: {e}")
    
    async def close(self):
        """Close the HTTP client"""
        if self._client:
            await self._client.aclose()
            self._client = None
            self._auth_validated = False
    
    async def _rate_limit(self):
        """Enforce rate limiting"""
        async with self._request_lock:
            now = asyncio.get_event_loop().time()
            time_since_last = now - self._last_request_time
            min_interval = 1.0 / self.rate_limit_per_second
            
            if time_since_last < min_interval:
                await asyncio.sleep(min_interval - time_since_last)
            
            self._last_request_time = asyncio.get_event_loop().time()
    
    def _generate_cache_key(self, prefix: str, params: Dict[str, Any]) -> str:
        """Generate a cache key from parameters"""
        param_str = json.dumps(params, sort_keys=True)
        hash_suffix = hashlib.md5(param_str.encode()).hexdigest()[:8]
        return f"{prefix}_{hash_suffix}"
    
    async def _get_cached_result(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """Retrieve cached result from Supabase"""
        if not self.supabase:
            return None
        
        try:
            result = self.supabase.table("dataforseo_cache") \
                .select("data, created_at") \
                .eq("cache_key", cache_key) \
                .single() \
                .execute()
            
            if result.data:
                created_at = datetime.fromisoformat(result.data["created_at"].replace("Z", "+00:00"))
                age = datetime.now(created_at.tzinfo) - created_at
                
                if age < timedelta(hours=self.cache_ttl_hours):
                    logger.info(f"Cache hit for key: {cache_key}")
                    return result.data["data"]
                else:
                    logger.info(f"Cache expired for key: {cache_key}")
        
        except Exception as e:
            logger.warning(f"Cache retrieval failed: {e}")
        
        return None
    
    async def _set_cached_result(self, cache_key: str, data: Dict[str, Any]):
        """Store result in Supabase cache"""
        if not self.supabase:
            return
        
        try:
            self.supabase.table("dataforseo_cache").upsert({
                "cache_key": cache_key,
                "data": data,
                "created_at": datetime.utcnow().isoformat(),
            }).execute()
            
            logger.info(f"Cached result for key: {cache_key}")
        
        except Exception as e:
            logger.warning(f"Cache storage failed: {e}")
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.NetworkError)),
    )
    async def _make_request(
        self,
        endpoint: str,
        method: str = "POST",
        data: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """
        Make an authenticated request to DataForSEO API with retries and rate limiting.
        
        Args:
            endpoint: API endpoint path (without base URL)
            method: HTTP method (GET or POST)
            data: Request payload for POST requests
        
        Returns:
            Parsed JSON response
        
        Raises:
            DataForSEOError: On API errors
            DataForSEORateLimitError: On rate limit exceeded
        """
        if not self._client or not self._auth_validated:
            await self.authenticate()
        
        await self._rate_limit()
        
        url = f"{self.BASE_URL}/{endpoint}"
        
        try:
            if method == "POST":
                response = await self._client.post(url, json=data)
            else:
                response = await self._client.get(url)
            
            response.raise_for_status()
            result = response.json()
            
            # Check DataForSEO status code
            status_code = result.get("status_code")
            status_message = result.get("status_message", "Unknown error")
            
            if status_code == 40000:  # Rate limit
                raise DataForSEORateLimitError(f"Rate limit exceeded: {status_message}")
            elif status_code == 40100:  # Authentication failed
                raise DataForSEOAuthError(f"Authentication failed: {status_message}")
            elif status_code != 20000:  # Not OK
                raise DataForSEOError(f"API error ({status_code}): {status_message}")
            
            return result
        
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                raise DataForSEORateLimitError("Rate limit exceeded")
            raise DataForSEOError(f"HTTP error: {e}")
        
        except httpx.TimeoutException:
            logger.warning(f"Request timeout for {endpoint}, retrying...")
            raise
        
        except httpx.NetworkError as e:
            logger.warning(f"Network error for {endpoint}, retrying: {e}")
            raise
    
    def _classify_serp_features(self, items: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Classify and count SERP features from DataForSEO items.
        
        Args:
            items: List of SERP items from DataForSEO response
        
        Returns:
            Dictionary with feature classifications and counts
        """
        features = {
            "featured_snippet": False,
            "people_also_ask": False,
            "people_also_ask_count": 0,
            "knowledge_graph": False,
            "local_pack": False,
            "video": False,
            "image": False,
            "shopping": False,
            "top_stories": False,
            "ai_overview": False,
            "related_searches": False,
            "all_features": [],
        }
        
        for item in items:
            item_type = item.get("type", "")
            
            for feature_name, type_list in self.SERP_FEATURE_TYPES.items():
                if item_type in type_list:
                    features["all_features"].append(item_type)
                    
                    if feature_name == "people_also_ask":
                        features[feature_name] = True
                        # Count number of PAA items
                        if "items" in item:
                            features["people_also_ask_count"] += len(item.get("items", []))
                    else:
                        features[feature_name] = True
        
        return features
    
    def _calculate_visual_position(
        self,
        organic_position: int,
        items: List[Dict[str, Any]],
    ) -> Tuple[float, List[str]]:
        """
        Calculate visual position accounting for SERP features above the result.
        
        Args:
            organic_position: Organic ranking position (1-based)
            items: All SERP items from DataForSEO response
        
        Returns:
            Tuple of (visual_position, list_of_features_above)
        """
        features_above = []
        visual_offset = 0.0
        
        # Find organic result to get its rank_absolute
        target_rank = None
        for item in items:
            if item.get("type") == "organic" and item.get("rank_group") == organic_position:
                target_rank = item.get("rank_absolute", organic_position)
                break
        
        if target_rank is None:
            target_rank = organic_position
        
        # Count features that appear before this rank
        for item in items:
            item_type = item.get("type", "")
            item_rank = item.get("rank_absolute", 999)
            
            # Only count features above the target result
            if item_rank < target_rank:
                for feature_name, type_list in self.SERP_FEATURE_TYPES.items():
                    if item_type in type_list:
                        impact = self.SERP_FEATURE_VISUAL_IMPACT.get(feature_name, 0.5)
                        
                        # For PAA, multiply by number of questions
                        if feature_name == "people_also_ask" and "items" in item:
                            impact *= len(item.get("items", []))
                        
                        visual_offset += impact
                        features_above.append(item_type)
        
        visual_position = organic_position + visual_offset
        return visual_position, features_above
    
    def _extract_competitor_domains(
        self,
        items: List[Dict[str, Any]],
        exclude_domain: Optional[str] = None,
        top_n: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Extract competitor domains from organic results.
        
        Args:
            items: SERP items from DataForSEO response
            exclude_domain: Domain to exclude (user's own domain)
            top_n: Number of top positions to consider
        
        Returns:
            List of competitor domain dictionaries with position info
        """
        competitors = []
        
        for item in items:
            if item.get("type") != "organic":
                continue
            
            position = item.get("rank_group")
            if not position or position > top_n:
                continue
            
            url = item.get("url", "")
            domain = item.get("domain", "")
            
            # Extract domain from URL if not provided
            if not domain and url:
                match = re.search(r"https?://(?:www\.)?([^/]+)", url)
                if match:
                    domain = match.group(1)
            
            # Skip user's own domain
            if exclude_domain and domain and exclude_domain.lower() in domain.lower():
                continue
            
            competitors.append({
                "domain": domain,
                "url": url,
                "position": position,
                "title": item.get("title", ""),
                "description": item.get("description", ""),
            })
        
        return competitors
    
    def _estimate_ctr(
        self,
        position: int,
        features: Dict[str, Any],
        visual_position: float,
    ) -> float:
        """
        Estimate CTR based on position and SERP features.
        
        Uses baseline CTR curve adjusted for SERP feature displacement.
        
        Args:
            position: Organic position
            features: SERP features dictionary
            visual_position: Visual position accounting for features
        
        Returns:
            Estimated CTR (0.0 to 1.0)
        """
        # Get baseline CTR for organic position
        baseline_ctr = self.BASELINE_CTR_CURVE.get(position, 0.003)
        
        # Apply displacement penalty based on visual position shift
        displacement = visual_position - position
        
        if displacement > 0:
            # Each position of displacement reduces CTR by ~30%
            penalty_factor = 0.7 ** displacement
            estimated_ctr = baseline_ctr * penalty_factor
        else:
            estimated_ctr = baseline_ctr
        
        # Additional penalty if AI Overview is present (can take significant clicks)
        if features.get("ai_overview"):
            estimated_ctr *= 0.7
        
        # Additional penalty if featured snippet is present and we're not in position 1
        if features.get("featured_snippet") and position > 1:
            estimated_ctr *= 0.8
        
        return max(0.0, min(1.0, estimated_ctr))
    
    async def fetch_serp_results(
        self,
        keywords: List[str],
        location_code: int = 2840,  # United States
        language_code: str = "en",
        device: str = "desktop",
        depth: int = 100,
        use_cache: bool = True,
    ) -> Dict[str, Any]:
        """
        Fetch live SERP results for multiple keywords.
        
        Args:
            keywords: List of keywords to analyze
            location_code: DataForSEO location code (2840 = US)
            language_code: Language code
            device: Device type (desktop, mobile)
            depth: Number of results to retrieve (max 100)
            use_cache: Whether to use cached results
        
        Returns:
            Dictionary mapping keywords to SERP analysis results
        """
        results = {}
        
        for keyword in keywords:
            cache_key = self._generate_cache_key(
                "serp",
                {
                    "keyword": keyword,
                    "location": location_code,
                    "language": language_code,
                    "device": device,
                }
            )
            
            # Check cache
            if use_cache:
                cached = await self._get_cached_result(cache_key)
                if cached:
                    results[keyword] = cached
                    continue
            
            # Make API request
            try:
                payload = [{
                    "keyword": keyword,
                    "location_code": location_code,
                    "language_code": language_code,
                    "device": device,
                    "depth": depth,
                }]
                
                response = await self._make_request(
                    "serp/google/organic/live/advanced",
                    method="POST",
                    data=payload,
                )
                
                # Parse response
                tasks = response.get("tasks", [])
                if not tasks or not tasks[0].get("result"):
                    logger.warning(f"No SERP results for keyword: {keyword}")
                    results[keyword] = None
                    continue
                
                task_result = tasks[0]["result"][0]
                items = task_result.get("items", [])
                
                # Classify SERP features
                features = self._classify_serp_features(items)
                
                # Extract organic results with visual positions
                organic_results = []
                for item in items:
                    if item.get("type") == "organic":
                        position = item.get("rank_group")
                        if position:
                            visual_pos, features_above = self._calculate_visual_position(
                                position, items
                            )
                            
                            estimated_ctr = self._estimate_ctr(position, features, visual_pos)
                            
                            organic_results.append({
                                "position": position,
                                "visual_position": visual_pos,
                                "url": item.get("url", ""),
                                "domain": item.get("domain", ""),
                                "title": item.get("title", ""),
                                "description": item.get("description", ""),
                                "features_above": features_above,
                                "estimated_ctr": estimated_ctr,
                            })
                
                # Extract competitors
                competitors = self._extract_competitor_domains(items)
                
                keyword_result = {
                    "keyword": keyword,
                    "serp_features": features,
                    "organic_results": organic_results,
                    "competitors": competitors,
                    "total_results": task_result.get("items_count", 0),
                    "timestamp": datetime.utcnow().isoformat(),
                }
                
                results[keyword] = keyword_result
                
                # Cache result
                if use_cache:
                    await self._set_cached_result(cache_key, keyword_result)
            
            except Exception as e:
                logger.error(f"Error fetching SERP for keyword '{keyword}': {e}")
                results[keyword] = None
        
        return results
    
    async def fetch_keyword_data(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
        use_cache: bool = True,
    ) -> Dict[str, Any]:
        """
        Fetch keyword difficulty and search volume data.
        
        Args:
            keywords: List of keywords to analyze
            location_code: DataForSEO location code
            language_code: Language code
            use_cache: Whether to use cached results
        
        Returns:
            Dictionary mapping keywords to keyword metrics
        """
        cache_key = self._generate_cache_key(
            "keywords",
            {
                "keywords": sorted(keywords),
                "location": location_code,
                "language": language_code,
            }
        )
        
        # Check cache
        if use_cache:
            cached = await self._get_cached_result(cache_key)
            if cached:
                return cached
        
        # Make API request
        try:
            payload = [{
                "keywords": keywords,
                "location_code": location_code,
                "language_code": language_code,
            }]
            
            response = await self._make_request(
                "keywords_data/google_ads/search_volume/live",
                method="POST",
                data=payload,
            )
            
            # Parse response
            tasks = response.get("tasks", [])
            if not tasks or not tasks[0].get("result"):
                logger.warning("No keyword data returned")
                return {}
            
            result_data = {}
            for item in tasks[0]["result"]:
                keyword = item.get("keyword")
                if keyword:
                    result_data[keyword] = {
                        "keyword": keyword,
                        "search_volume": item.get("search_volume"),
                        "competition": item.get("competition"),
                        "competition_index": item.get("competition_index"),
                        "cpc": item.get("cpc"),
                        "low_top_of_page_bid": item.get("low_top_of_page_bid"),
                        "high_top_of_page_bid": item.get("high_top_of_page_bid"),
                        "monthly_searches": item.get("monthly_searches", []),
                    }
            
            # Cache result
            if use_cache:
                await self._set_cached_result(cache_key, result_data)
            
            return result_data
        
        except Exception as e:
            logger.error(f"Error fetching keyword data: {e}")
            return {}
    
    async def analyze_competitor_domains(
        self,
        domain: str,
        location_code: int = 2840,
        language_code: str = "en",
        limit: int = 100,
        use_cache: bool = True,
    ) -> Dict[str, Any]:
        """
        Analyze competitor domain performance (organic keywords, traffic estimates).
        
        Args:
            domain: Domain to analyze
            location_code: DataForSEO location code
            language_code: Language code
            limit: Number of keywords to retrieve
            use_cache: Whether to use cached results
        
        Returns:
            Domain analysis with top keywords and metrics
        """
        cache_key = self._generate_cache_key(
            "domain",
            {
                "domain": domain,
                "location": location_code,
                "language": language_code,
            }
        )
        
        # Check cache
        if use_cache:
            cached = await self._get_cached_result(cache_key)
            if cached:
                return cached
        
        # Make API request
        try:
            payload = [{
                "target": domain,
                "location_code": location_code,
                "language_code": language_code,
                "limit": limit,
            }]
            
            response = await self._make_request(
                "dataforseo_labs/google/ranked_keywords/live",
                method="POST",
                data=payload,
            )
            
            # Parse response
            tasks = response.get("tasks", [])
            if not tasks or not tasks[0].get("result"):
                logger.warning(f"No domain data for: {domain}")
                return {}
            
            task_result = tasks[0]["result"][0]
            
            # Extract metrics
            metrics = task_result.get("metrics", {})
            items = task_result.get("items", [])
            
            # Process top keywords
            top_keywords = []
            for item in items[:50]:  # Top 50 keywords
                keyword_data = item.get("keyword_data", {})
                ranked_serp_element = item.get("ranked_serp_element", {})
                
                top_keywords.append({
                    "keyword": keyword_data.get("keyword"),
                    "search_volume": keyword_data.get("keyword_info", {}).get("search_volume"),
                    "position": ranked_serp_element.get("serp_item", {}).get("rank_group"),
                    "etv": item.get("etv"),  # Estimated traffic value
                })
            
            domain_analysis = {
                "domain": domain,
                "metrics": {
                    "organic_keywords_count": metrics.get("organic", {}).get("count"),
                    "organic_etv": metrics.get("organic", {}).get("etv"),
                    "organic_pos_1": metrics.get("organic", {}).get("pos_1"),
                    "organic_pos_2_3": metrics.get("organic", {}).get("pos_2_3"),
                    "organic_pos_4_10": metrics.get("organic", {}).get("pos_4_10"),
                    "organic_pos_11_20": metrics.get("organic", {}).get("pos_11_20"),
                },
                "top_keywords": top_keywords,
                "timestamp": datetime.utcnow().isoformat(),
            }
            
            # Cache result
            if use_cache:
                await self._set_cached_result(cache_key, domain_analysis)
            
            return domain_analysis
        
        except Exception as e:
            logger.error(f"Error analyzing domain '{domain}': {e}")
            return {}
    
    async def batch_serp_analysis(
        self,
        keywords: List[str],
        user_domain: Optional[str] = None,
        location_code: int = 2840,
        language_code: str = "en",
        use_cache: bool = True,
        batch_size: int = 10,
    ) -> Dict[str, Any]:
        """
        Batch process multiple keywords with full SERP analysis.
        
        Includes:
        - SERP features for each keyword
        - User's ranking position (if domain provided)
        - Visual position calculation
        - Competitor analysis
        - CTR estimation
        
        Args:
            keywords: List of keywords to analyze
            user_domain: User's domain (to identify their rankings)
            location_code: DataForSEO location code
            language_code: Language code
            use_cache: Whether to use cached results
            batch_size: Process keywords in batches to avoid overwhelming API
        
        Returns:
            Comprehensive SERP analysis for all keywords
        """
        all_results = {}
        
        # Process in batches
        for i in range(0, len(keywords), batch_size):
            batch = keywords[i:i + batch_size]
            batch_results = await self.fetch_serp_results(
                keywords=batch,
                location_code=location_code,
                language_code=language_code,
                use_cache=use_cache,
            )
            all_results.update(batch_results)
            
            # Small delay between batches
            if i + batch_size < len(keywords):
                await asyncio.sleep(1)
        
        # Aggregate analysis
        total_keywords = len([r for r in all_results.values() if r is not None])
        
        # Find user's rankings if domain provided
        user_rankings = []
        if user_domain:
            for keyword, data in all_results.items():
                if not data:
                    continue
                
                for result in data.get("organic_results", []):
                    if user_domain.lower() in result.get("domain", "").lower():
                        user_rankings.append({
                            "keyword": keyword,
                            "position": result["position"],
                            "visual_position": result["visual_position"],
                            "url": result["url"],
                            "estimated_ctr": result["estimated_ctr"],
                            "features_above": result["features_above"],
                        })
                        break
        
        # Aggregate competitor domains
        competitor_frequency = {}
        for keyword, data in all_results.items():
            if not data:
                continue
            
            for comp in data.get("competitors", []):
                domain = comp["domain"]
                if user_domain and user_domain.lower() in domain.lower():
                    continue
                
                if domain not in competitor_frequency:
                    competitor_frequency[domain] = {
                        "domain": domain,
                        "appearances": 0,
                        "avg_position": 0,
                        "positions": [],
                    }
                
                competitor_frequency[domain]["appearances"] += 1
                competitor_frequency[domain]["positions"].append(comp["position"])
        
        # Calculate average positions for competitors
        top_competitors = []
        for domain, data in competitor_frequency.items():
            data["avg_position"] = sum(data["positions"]) / len(data["positions"])
            data["keyword_count"] = len(data["positions"])
            del data["positions"]  # Remove raw positions list
            top_competitors.append(data)
        
        # Sort by frequency
        top_competitors.sort(key=lambda x: x["appearances"], reverse=True)
        
        # Aggregate SERP features
        feature_stats = {
            "featured_snippet": 0,
            "people_also_ask": 0,
            "knowledge_graph": 0,
            "local_pack": 0,
            "video": 0,
            "image": 0,
            "shopping": 0,
            "top_stories": 0,
            "ai_overview": 0,
        }
        
        for keyword, data in all_results.items():
            if not data:
                continue
            
            features = data.get("serp_features", {})
            for feature in feature_stats.keys():
                if features.get(feature):
                    feature_stats[feature] += 1
        
        return {
            "keywords_analyzed": total_keywords,
            "user_rankings": user_rankings,
            "top_competitors": top_competitors[:20],  # Top 20 competitors
            "serp_feature_prevalence": feature_stats,
            "detailed_results": all_results,
            "summary": {
                "avg_user_position": sum(r["position"] for r in user_rankings) / len(user_rankings) if user_rankings else None,
                "avg_visual_displacement": sum(r["visual_position"] - r["position"] for r in user_rankings) / len(user_rankings) if user_rankings else None,
                "keywords_with_ai_overview": feature_stats["ai_overview"],
                "keywords_with_featured_snippet": feature_stats["featured_snippet"],
            },
        }
