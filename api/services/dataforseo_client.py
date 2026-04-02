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
    
    # Rate limiting parameters
    MAX_REQUESTS_PER_MINUTE = 2000
    MAX_CONCURRENT_REQUESTS = 100
    
    # Retry configuration
    MAX_RETRIES = 3
    RETRY_MIN_WAIT = 1
    RETRY_MAX_WAIT = 10
    
    # Cache TTL
    CACHE_TTL_DAYS = 1
    
    def __init__(
        self,
        login: Optional[str] = None,
        password: Optional[str] = None,
        supabase_client: Optional[Any] = None,
        cache_enabled: bool = True,
    ):
        """
        Initialize DataForSEO client.
        
        Args:
            login: DataForSEO API login (defaults to env var DATAFORSEO_LOGIN)
            password: DataForSEO API password (defaults to env var DATAFORSEO_PASSWORD)
            supabase_client: Supabase client for caching (optional)
            cache_enabled: Whether to use caching
        """
        self.login = login or os.getenv("DATAFORSEO_LOGIN")
        self.password = password or os.getenv("DATAFORSEO_PASSWORD")
        self.supabase_client = supabase_client
        self.cache_enabled = cache_enabled and supabase_client is not None
        
        if not self.login or not self.password:
            raise DataForSEOAuthError("DataForSEO credentials not provided")
        
        self.client: Optional[httpx.AsyncClient] = None
        self.semaphore = asyncio.Semaphore(self.MAX_CONCURRENT_REQUESTS)
        self.request_times: List[float] = []
        
        logger.info("DataForSEO client initialized")
    
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
            auth = httpx.BasicAuth(self.login, self.password)
            self.client = httpx.AsyncClient(
                auth=auth,
                timeout=httpx.Timeout(60.0),
                limits=httpx.Limits(max_keepalive_connections=20, max_connections=100),
            )
            logger.info("DataForSEO client authenticated")
    
    async def close(self):
        """Close HTTP client"""
        if self.client:
            await self.client.aclose()
            self.client = None
            logger.info("DataForSEO client closed")
    
    async def _rate_limit(self):
        """Enforce rate limiting"""
        now = asyncio.get_event_loop().time()
        
        # Remove requests older than 1 minute
        self.request_times = [t for t in self.request_times if now - t < 60]
        
        # If at limit, wait until oldest request is > 1 minute old
        if len(self.request_times) >= self.MAX_REQUESTS_PER_MINUTE:
            sleep_time = 60 - (now - self.request_times[0]) + 0.1
            if sleep_time > 0:
                logger.warning(f"Rate limit reached, sleeping for {sleep_time:.2f}s")
                await asyncio.sleep(sleep_time)
                self.request_times = []
        
        self.request_times.append(now)
    
    def _generate_cache_key(self, endpoint: str, params: Dict[str, Any]) -> str:
        """Generate cache key from endpoint and parameters"""
        # Normalize params for consistent hashing
        normalized = json.dumps(params, sort_keys=True)
        hash_input = f"{endpoint}:{normalized}"
        return hashlib.sha256(hash_input.encode()).hexdigest()
    
    async def _get_from_cache(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """Retrieve data from cache if available and not expired"""
        if not self.cache_enabled:
            return None
        
        try:
            result = self.supabase_client.table("dataforseo_cache").select("*").eq("cache_key", cache_key).single().execute()
            
            if result.data:
                created_at = datetime.fromisoformat(result.data["created_at"].replace("Z", "+00:00"))
                if datetime.utcnow() - created_at.replace(tzinfo=None) < timedelta(days=self.CACHE_TTL_DAYS):
                    logger.info(f"Cache hit for key {cache_key[:8]}...")
                    return result.data["response_data"]
                else:
                    logger.info(f"Cache expired for key {cache_key[:8]}...")
            
            return None
        except Exception as e:
            logger.warning(f"Cache retrieval failed: {e}")
            return None
    
    async def _save_to_cache(self, cache_key: str, data: Dict[str, Any]):
        """Save data to cache"""
        if not self.cache_enabled:
            return
        
        try:
            self.supabase_client.table("dataforseo_cache").upsert({
                "cache_key": cache_key,
                "response_data": data,
                "created_at": datetime.utcnow().isoformat(),
            }).execute()
            logger.info(f"Cached data for key {cache_key[:8]}...")
        except Exception as e:
            logger.warning(f"Cache save failed: {e}")
    
    @retry(
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential(multiplier=1, min=RETRY_MIN_WAIT, max=RETRY_MAX_WAIT),
        retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError)),
        reraise=True,
    )
    async def _make_request(
        self,
        endpoint: str,
        method: str = "POST",
        data: Optional[Any] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Make HTTP request to DataForSEO API with retries and error handling.
        
        Args:
            endpoint: API endpoint path (e.g., "/serp/google/organic/live/advanced")
            method: HTTP method
            data: Request body data
            params: Query parameters
        
        Returns:
            API response data
        
        Raises:
            DataForSEOAuthError: Authentication failed
            DataForSEORateLimitError: Rate limit exceeded
            DataForSEOError: Other API errors
        """
        if not self.client:
            await self.authenticate()
        
        url = f"{self.BASE_URL}{endpoint}"
        
        async with self.semaphore:
            await self._rate_limit()
            
            try:
                if method == "POST":
                    response = await self.client.post(url, json=data, params=params)
                else:
                    response = await self.client.get(url, params=params)
                
                response.raise_for_status()
                
                result = response.json()
                
                # Check for API-level errors
                if result.get("status_code") == 40101:
                    raise DataForSEOAuthError("Authentication failed")
                elif result.get("status_code") == 50000:
                    raise DataForSEORateLimitError("Rate limit exceeded")
                elif result.get("status_code") != 20000:
                    error_msg = result.get("status_message", "Unknown error")
                    raise DataForSEOError(f"API error: {error_msg}")
                
                return result
                
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 401:
                    raise DataForSEOAuthError("Authentication failed")
                elif e.response.status_code == 429:
                    raise DataForSEORateLimitError("Rate limit exceeded")
                else:
                    logger.error(f"HTTP error {e.response.status_code}: {e.response.text}")
                    raise DataForSEOError(f"HTTP error: {e.response.status_code}")
            
            except httpx.RequestError as e:
                logger.error(f"Request error: {e}")
                raise DataForSEOError(f"Request failed: {e}")
    
    def _parse_serp_features(self, items: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Parse SERP items to extract features.
        
        Args:
            items: List of SERP items from DataForSEO response
        
        Returns:
            Dict with feature counts and details
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
            "twitter": False,
            "recipes": False,
            "hotels_pack": False,
            "flights": False,
            "jobs": False,
            "events": False,
            "find_results_on": False,
            "related_searches": False,
        }
        
        feature_details = []
        
        for item in items:
            item_type = item.get("type", "")
            rank_absolute = item.get("rank_absolute", 0)
            
            # Map item type to feature category
            for feature_name, item_types in self.SERP_FEATURE_TYPES.items():
                if item_type in item_types:
                    if feature_name == "people_also_ask":
                        features[feature_name] += 1
                    else:
                        features[feature_name] = True
                    
                    feature_details.append({
                        "type": feature_name,
                        "rank_absolute": rank_absolute,
                        "item_type": item_type,
                        "title": item.get("title", ""),
                        "url": item.get("url", ""),
                    })
                    break
        
        return {
            "features": features,
            "feature_details": feature_details,
        }
    
    def _calculate_visual_position(
        self,
        organic_position: int,
        features_above: List[Dict[str, Any]],
    ) -> float:
        """
        Calculate visual position accounting for SERP features.
        
        Args:
            organic_position: Organic ranking position
            features_above: List of SERP features appearing above this position
        
        Returns:
            Visual position (organic position + displacement from features)
        """
        displacement = 0.0
        
        for feature in features_above:
            feature_type = feature["type"]
            impact = self.SERP_FEATURE_VISUAL_IMPACT.get(feature_type, 0.0)
            
            # For PAA, multiply by count
            if feature_type == "people_also_ask":
                displacement += impact
            else:
                displacement += impact
        
        return organic_position + displacement
    
    def _extract_organic_results(
        self,
        items: List[Dict[str, Any]],
        target_domain: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Extract organic search results from SERP items.
        
        Args:
            items: List of SERP items
            target_domain: Optional domain to highlight (user's domain)
        
        Returns:
            List of organic result dicts with position, URL, title, etc.
        """
        organic_results = []
        
        for item in items:
            if item.get("type") == "organic":
                url = item.get("url", "")
                domain = item.get("domain", "")
                
                # Extract clean domain
                if not domain and url:
                    match = re.search(r"https?://(?:www\.)?([^/]+)", url)
                    if match:
                        domain = match.group(1)
                
                result = {
                    "position": item.get("rank_absolute", 0),
                    "url": url,
                    "domain": domain,
                    "title": item.get("title", ""),
                    "description": item.get("description", ""),
                    "is_target_domain": False,
                }
                
                # Check if this is the target domain
                if target_domain and domain:
                    result["is_target_domain"] = target_domain.lower() in domain.lower()
                
                organic_results.append(result)
        
        return sorted(organic_results, key=lambda x: x["position"])
    
    async def get_serp_data(
        self,
        keyword: str,
        location_code: int = 2840,
        language_code: str = "en",
        device: str = "desktop",
        target_domain: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Get live SERP data for a single keyword.
        
        Args:
            keyword: Search keyword
            location_code: Location code (2840 = United States)
            language_code: Language code
            device: Device type ("desktop", "mobile")
            target_domain: User's domain for position tracking
        
        Returns:
            Dict containing:
            - keyword: Search keyword
            - location_code: Location code used
            - organic_results: List of organic results with positions
            - serp_features: Dict of detected SERP features
            - target_position: Position of target domain (if found)
            - target_visual_position: Visual position accounting for features
            - competitors: List of competing domains in top 10
        """
        endpoint = "/serp/google/organic/live/advanced"
        
        request_data = [{
            "keyword": keyword,
            "location_code": location_code,
            "language_code": language_code,
            "device": device,
            "os": "windows" if device == "desktop" else "ios",
            "depth": 100,  # Get up to 100 results
        }]
        
        # Check cache
        cache_key = self._generate_cache_key(endpoint, request_data[0])
        cached = await self._get_from_cache(cache_key)
        
        if cached:
            return cached
        
        # Make API request
        response = await self._make_request(endpoint, data=request_data)
        
        # Parse response
        if not response.get("tasks") or not response["tasks"][0].get("result"):
            raise DataForSEOError("Invalid response structure")
        
        result = response["tasks"][0]["result"][0]
        items = result.get("items", [])
        
        # Extract organic results
        organic_results = self._extract_organic_results(items, target_domain)
        
        # Parse SERP features
        serp_data = self._parse_serp_features(items)
        
        # Find target domain position
        target_position = None
        target_visual_position = None
        
        for result in organic_results:
            if result["is_target_domain"]:
                target_position = result["position"]
                
                # Calculate visual position
                features_above = [
                    f for f in serp_data["feature_details"]
                    if f["rank_absolute"] < target_position
                ]
                target_visual_position = self._calculate_visual_position(
                    target_position, features_above
                )
                break
        
        # Extract competitors (top 10 domains excluding target)
        competitors = []
        seen_domains = set()
        
        for result in organic_results[:10]:
            if result["domain"] and result["domain"] not in seen_domains:
                if not result["is_target_domain"]:
                    competitors.append({
                        "domain": result["domain"],
                        "position": result["position"],
                        "url": result["url"],
                        "title": result["title"],
                    })
                seen_domains.add(result["domain"])
        
        output = {
            "keyword": keyword,
            "location_code": location_code,
            "language_code": language_code,
            "device": device,
            "organic_results": organic_results,
            "serp_features": serp_data["features"],
            "feature_details": serp_data["feature_details"],
            "target_position": target_position,
            "target_visual_position": target_visual_position,
            "competitors": competitors,
            "total_results": result.get("items_count", 0),
            "retrieved_at": datetime.utcnow().isoformat(),
        }
        
        # Save to cache
        await self._save_to_cache(cache_key, output)
        
        return output
    
    async def get_competitors(
        self,
        domain: str,
        keywords_list: List[str],
        location_code: int = 2840,
        language_code: str = "en",
        top_n: int = 20,
    ) -> Dict[str, Any]:
        """
        Get competitor analysis across multiple keywords.
        
        Args:
            domain: User's domain
            keywords_list: List of keywords to analyze
            location_code: Location code
            language_code: Language code
            top_n: Number of top competitors to return
        
        Returns:
            Dict containing:
            - keywords_analyzed: Number of keywords processed
            - competitors: List of competitor domains with frequency and avg position
            - keyword_overlap: Dict of keyword -> list of competing domains
        """
        # Batch fetch SERP data for all keywords
        tasks = [
            self.get_serp_data(
                keyword=kw,
                location_code=location_code,
                language_code=language_code,
                target_domain=domain,
            )
            for kw in keywords_list
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Aggregate competitor data
        competitor_data: Dict[str, Dict[str, Any]] = {}
        keyword_overlap: Dict[str, List[str]] = {}
        
        successful_results = 0
        
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Failed to fetch SERP data for keyword {keywords_list[i]}: {result}")
                continue
            
            successful_results += 1
            keyword = result["keyword"]
            keyword_overlap[keyword] = []
            
            for competitor in result["competitors"]:
                comp_domain = competitor["domain"]
                keyword_overlap[keyword].append(comp_domain)
                
                if comp_domain not in competitor_data:
                    competitor_data[comp_domain] = {
                        "domain": comp_domain,
                        "keywords_shared": 0,
                        "positions": [],
                        "urls": set(),
                    }
                
                competitor_data[comp_domain]["keywords_shared"] += 1
                competitor_data[comp_domain]["positions"].append(competitor["position"])
                competitor_data[comp_domain]["urls"].add(competitor["url"])
        
        # Calculate aggregate metrics
        competitors = []
        for comp_domain, data in competitor_data.items():
            avg_position = sum(data["positions"]) / len(data["positions"])
            frequency_pct = (data["keywords_shared"] / successful_results) * 100
            
            # Threat level based on frequency and position
            if frequency_pct > 50 and avg_position < 5:
                threat_level = "critical"
            elif frequency_pct > 30 and avg_position < 7:
                threat_level = "high"
            elif frequency_pct > 15:
                threat_level = "medium"
            else:
                threat_level = "low"
            
            competitors.append({
                "domain": comp_domain,
                "keywords_shared": data["keywords_shared"],
                "frequency_pct": round(frequency_pct, 2),
                "avg_position": round(avg_position, 2),
                "best_position": min(data["positions"]),
                "worst_position": max(data["positions"]),
                "threat_level": threat_level,
                "sample_urls": list(data["urls"])[:3],
            })
        
        # Sort by frequency
        competitors.sort(key=lambda x: x["keywords_shared"], reverse=True)
        
        return {
            "domain": domain,
            "keywords_analyzed": successful_results,
            "total_keywords": len(keywords_list),
            "competitors": competitors[:top_n],
            "keyword_overlap": keyword_overlap,
        }
    
    async def get_serp_features(
        self,
        keyword: str,
        location_code: int = 2840,
        language_code: str = "en",
    ) -> Dict[str, Any]:
        """
        Get detailed SERP feature analysis for a keyword.
        
        Args:
            keyword: Search keyword
            location_code: Location code
            language_code: Language code
        
        Returns:
            Dict containing:
            - keyword: Search keyword
            - features_present: Dict of feature -> bool/count
            - feature_details: List of detailed feature info
            - visual_complexity_score: 0-1 score of SERP crowdedness
            - click_likelihood_impact: Estimated CTR impact from features
        """
        serp_data = await self.get_serp_data(
            keyword=keyword,
            location_code=location_code,
            language_code=language_code,
        )
        
        features = serp_data["serp_features"]
        feature_details = serp_data["feature_details"]
        
        # Calculate visual complexity score
        complexity_weights = {
            "featured_snippet": 0.15,
            "ai_overview": 0.20,
            "knowledge_graph": 0.10,
            "local_pack": 0.15,
            "shopping": 0.10,
            "video": 0.08,
            "image": 0.05,
            "people_also_ask": 0.03,  # Per question
            "top_stories": 0.10,
        }
        
        complexity_score = 0.0
        for feature, present in features.items():
            weight = complexity_weights.get(feature, 0.0)
            if feature == "people_also_ask":
                complexity_score += weight * present
            elif present:
                complexity_score += weight
        
        complexity_score = min(1.0, complexity_score)
        
        # Estimate CTR impact (negative correlation with complexity)
        # Base CTR for position 1 = ~30%, reduced by features
        base_ctr = 0.30
        ctr_impact = base_ctr * (1 - complexity_score * 0.6)
        
        return {
            "keyword": keyword,
            "features_present": features,
            "feature_details": feature_details,
            "visual_complexity_score": round(complexity_score, 3),
            "estimated_p1_ctr": round(ctr_impact, 3),
            "click_likelihood_impact": round((ctr_impact - base_ctr) / base_ctr, 3),
        }
    
    async def batch_serp_analysis(
        self,
        keywords: List[str],
        domain: str,
        location_code: int = 2840,
        language_code: str = "en",
        batch_size: int = 50,
    ) -> Dict[str, Any]:
        """
        Perform batch SERP analysis for multiple keywords.
        
        Args:
            keywords: List of keywords to analyze
            domain: User's domain
            location_code: Location code
            language_code: Language code
            batch_size: Number of concurrent requests
        
        Returns:
            Dict containing:
            - total_keywords: Total keywords analyzed
            - successful: Number of successful fetches
            - failed: Number of failed fetches
            - results: List of SERP data for each keyword
            - aggregate_stats: Summary statistics
        """
        results = []
        failed = []
        
        # Process in batches
        for i in range(0, len(keywords), batch_size):
            batch = keywords[i:i + batch_size]
            logger.info(f"Processing batch {i // batch_size + 1}/{(len(keywords) + batch_size - 1) // batch_size}")
            
            tasks = [
                self.get_serp_data(
                    keyword=kw,
                    location_code=location_code,
                    language_code=language_code,
                    target_domain=domain,
                )
                for kw in batch
            ]
            
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for j, result in enumerate(batch_results):
                if isinstance(result, Exception):
                    failed.append({"keyword": batch[j], "error": str(result)})
                else:
                    results.append(result)
            
            # Small delay between batches
            if i + batch_size < len(keywords):
                await asyncio.sleep(1)
        
        # Calculate aggregate statistics
        total_features = {}
        position_data = []
        visual_displacement = []
        
        for result in results:
            # Aggregate features
            for feature, value in result["serp_features"].items():
                if feature not in total_features:
                    total_features[feature] = 0
                if isinstance(value, bool):
                    total_features[feature] += 1 if value else 0
                else:
                    total_features[feature] += value
            
            # Collect position data
            if result["target_position"]:
                position_data.append(result["target_position"])
                if result["target_visual_position"]:
                    displacement = result["target_visual_position"] - result["target_position"]
                    visual_displacement.append(displacement)
        
        # Calculate percentages
        successful = len(results)
        feature_percentages = {
            feature: round((count / successful) * 100, 2)
            for feature, count in total_features.items()
        }
        
        aggregate_stats = {
            "avg_position": round(sum(position_data) / len(position_data), 2) if position_data else None,
            "median_position": sorted(position_data)[len(position_data) // 2] if position_data else None,
            "avg_visual_displacement": round(sum(visual_displacement) / len(visual_displacement), 2) if visual_displacement else 0,
            "feature_frequencies": feature_percentages,
            "keywords_with_target": len(position_data),
            "keywords_without_target": successful - len(position_data),
        }
        
        return {
            "total_keywords": len(keywords),
            "successful": successful,
            "failed": len(failed),
            "failed_keywords": failed,
            "results": results,
            "aggregate_stats": aggregate_stats,
        }