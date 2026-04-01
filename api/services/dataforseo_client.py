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
                "Set DATAFORSEO_LOGIN and DATAFORSEO_PASSWORD environment variables."
            )
        
        self.timeout = timeout
        self.max_retries = max_retries
        self.auth = (self.login, self.password)
        self.supabase = supabase_client
        self.cache_ttl_hours = cache_ttl_hours
        
        # Rate limiting: DataForSEO allows ~2000 API units/minute
        # Conservative semaphore to prevent hammering
        self._semaphore = asyncio.Semaphore(50)
        self._last_request_time = 0
        self._min_request_interval = 0.03  # 30ms between requests
    
    async def authenticate(self) -> bool:
        """
        Test authentication with DataForSEO API.
        
        Returns:
            True if authentication successful
            
        Raises:
            DataForSEOAuthError: If authentication fails
        """
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.BASE_URL}/serp/google/organic/live/advanced",
                    auth=self.auth,
                    timeout=10.0,
                )
                
                if response.status_code == 401:
                    raise DataForSEOAuthError("Invalid DataForSEO credentials")
                
                # Any non-401 response means auth worked
                logger.info("DataForSEO authentication successful")
                return True
                
        except httpx.HTTPError as e:
            logger.error(f"DataForSEO authentication failed: {e}")
            raise DataForSEOAuthError(f"Authentication failed: {e}")
    
    def _generate_cache_key(self, endpoint: str, params: Dict[str, Any]) -> str:
        """Generate cache key for request."""
        # Sort params for consistent hashing
        sorted_params = json.dumps(params, sort_keys=True)
        key_string = f"{endpoint}:{sorted_params}"
        return hashlib.md5(key_string.encode()).hexdigest()
    
    async def _get_cached_response(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """Retrieve cached response from Supabase if available and not expired."""
        if not self.supabase:
            return None
        
        try:
            result = self.supabase.table("dataforseo_cache")\
                .select("*")\
                .eq("cache_key", cache_key)\
                .single()\
                .execute()
            
            if result.data:
                cached_at = datetime.fromisoformat(result.data["cached_at"])
                expiry = cached_at + timedelta(hours=self.cache_ttl_hours)
                
                if datetime.utcnow() < expiry:
                    logger.info(f"Cache hit for key {cache_key}")
                    return result.data["response_data"]
                else:
                    logger.info(f"Cache expired for key {cache_key}")
                    # Delete expired cache
                    self.supabase.table("dataforseo_cache")\
                        .delete()\
                        .eq("cache_key", cache_key)\
                        .execute()
        except Exception as e:
            logger.warning(f"Cache retrieval failed: {e}")
        
        return None
    
    async def _cache_response(self, cache_key: str, response_data: Dict[str, Any]):
        """Cache response in Supabase."""
        if not self.supabase:
            return
        
        try:
            self.supabase.table("dataforseo_cache").upsert({
                "cache_key": cache_key,
                "response_data": response_data,
                "cached_at": datetime.utcnow().isoformat(),
            }).execute()
            logger.info(f"Cached response for key {cache_key}")
        except Exception as e:
            logger.warning(f"Cache storage failed: {e}")
    
    async def _rate_limit(self):
        """Enforce rate limiting between requests."""
        current_time = asyncio.get_event_loop().time()
        time_since_last = current_time - self._last_request_time
        
        if time_since_last < self._min_request_interval:
            await asyncio.sleep(self._min_request_interval - time_since_last)
        
        self._last_request_time = asyncio.get_event_loop().time()
    
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
        use_cache: bool = True,
    ) -> Dict[str, Any]:
        """
        Make authenticated request to DataForSEO API with retry logic.
        
        Args:
            endpoint: API endpoint (relative to BASE_URL)
            method: HTTP method
            data: Request payload
            use_cache: Whether to use caching
            
        Returns:
            API response as dict
            
        Raises:
            DataForSEORateLimitError: If rate limit exceeded
            DataForSEOError: For other API errors
        """
        async with self._semaphore:
            await self._rate_limit()
            
            # Check cache
            cache_key = None
            if use_cache and data:
                cache_key = self._generate_cache_key(endpoint, data[0] if data else {})
                cached = await self._get_cached_response(cache_key)
                if cached:
                    return cached
            
            url = f"{self.BASE_URL}/{endpoint}"
            
            try:
                async with httpx.AsyncClient() as client:
                    if method == "POST":
                        response = await client.post(
                            url,
                            auth=self.auth,
                            json=data,
                            timeout=self.timeout,
                        )
                    else:
                        response = await client.get(
                            url,
                            auth=self.auth,
                            timeout=self.timeout,
                        )
                    
                    # Handle rate limiting
                    if response.status_code == 429:
                        logger.warning("Rate limit exceeded, backing off")
                        raise DataForSEORateLimitError("Rate limit exceeded")
                    
                    # Handle auth errors
                    if response.status_code == 401:
                        raise DataForSEOAuthError("Authentication failed")
                    
                    # Raise for other HTTP errors
                    response.raise_for_status()
                    
                    result = response.json()
                    
                    # Check API-level errors
                    if result.get("status_code") != 20000:
                        error_msg = result.get("status_message", "Unknown error")
                        logger.error(f"DataForSEO API error: {error_msg}")
                        raise DataForSEOError(f"API error: {error_msg}")
                    
                    # Cache successful response
                    if use_cache and cache_key:
                        await self._cache_response(cache_key, result)
                    
                    return result
                    
            except httpx.HTTPStatusError as e:
                logger.error(f"HTTP error: {e.response.status_code} - {e.response.text}")
                raise DataForSEOError(f"HTTP error: {e}")
            except httpx.TimeoutException:
                logger.warning(f"Request timeout for {endpoint}")
                raise
            except httpx.NetworkError as e:
                logger.warning(f"Network error for {endpoint}: {e}")
                raise
    
    async def fetch_serp_results(
        self,
        keywords: List[str],
        location_code: int = 2840,  # United States
        language_code: str = "en",
        device: str = "desktop",
        depth: int = 100,  # Get top 100 results
    ) -> List[Dict[str, Any]]:
        """
        Fetch live SERP results for a list of keywords.
        
        Args:
            keywords: List of keywords to analyze
            location_code: DataForSEO location code (2840 = USA)
            language_code: Language code
            device: Device type (desktop, mobile)
            depth: Number of results to retrieve (max 100)
            
        Returns:
            List of SERP result objects, one per keyword
        """
        tasks = [
            self.fetch_serp_for_keyword(
                keyword=keyword,
                location_code=location_code,
                language_code=language_code,
                device=device,
                depth=depth,
            )
            for keyword in keywords
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out exceptions and log them
        valid_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Failed to fetch SERP for '{keywords[i]}': {result}")
            else:
                valid_results.append(result)
        
        return valid_results
    
    async def fetch_serp_for_keyword(
        self,
        keyword: str,
        location_code: int = 2840,
        language_code: str = "en",
        device: str = "desktop",
        depth: int = 100,
    ) -> Dict[str, Any]:
        """
        Fetch live SERP results for a single keyword.
        
        Args:
            keyword: Keyword to analyze
            location_code: DataForSEO location code
            language_code: Language code
            device: Device type
            depth: Number of results to retrieve
            
        Returns:
            Parsed SERP data including organic results and features
        """
        payload = [{
            "keyword": keyword,
            "location_code": location_code,
            "language_code": language_code,
            "device": device,
            "depth": depth,
            "calculate_rectangles": True,  # For visual position analysis
        }]
        
        response = await self._make_request(
            endpoint="serp/google/organic/live/advanced",
            method="POST",
            data=payload,
        )
        
        # Extract and parse results
        if not response.get("tasks") or not response["tasks"][0].get("result"):
            logger.warning(f"No SERP results for keyword: {keyword}")
            return {
                "keyword": keyword,
                "organic_results": [],
                "serp_features": {},
                "total_results": 0,
            }
        
        task_result = response["tasks"][0]["result"][0]
        
        return self._parse_serp_result(keyword, task_result)
    
    def _parse_serp_result(self, keyword: str, result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse raw SERP result into structured format.
        
        Extracts:
        - Organic results with positions and URLs
        - SERP features (featured snippets, PAA, knowledge panels, etc.)
        - Competitor domains
        - Visual positioning data
        """
        items = result.get("items", [])
        
        organic_results = []
        serp_features = {
            "featured_snippet": None,
            "knowledge_panel": None,
            "local_pack": None,
            "people_also_ask": [],
            "video_carousel": [],
            "image_pack": None,
            "shopping_results": [],
            "top_stories": [],
            "ai_overview": None,
            "reddit_threads": [],
        }
        
        for item in items:
            item_type = item.get("type")
            
            if item_type == "organic":
                organic_results.append({
                    "position": item.get("rank_group", 0),
                    "url": item.get("url"),
                    "domain": item.get("domain"),
                    "title": item.get("title"),
                    "description": item.get("description"),
                    "breadcrumb": item.get("breadcrumb"),
                    "rectangle": item.get("rectangle"),  # Visual position data
                })
            
            elif item_type == "featured_snippet":
                serp_features["featured_snippet"] = {
                    "type": item.get("feature_type"),
                    "url": item.get("url"),
                    "domain": item.get("domain"),
                    "title": item.get("title"),
                    "description": item.get("description"),
                }
            
            elif item_type == "knowledge_panel":
                serp_features["knowledge_panel"] = {
                    "title": item.get("title"),
                    "description": item.get("description"),
                    "url": item.get("url"),
                }
            
            elif item_type == "local_pack":
                serp_features["local_pack"] = {
                    "title": item.get("title"),
                    "items_count": len(item.get("items", [])),
                }
            
            elif item_type == "people_also_ask":
                serp_features["people_also_ask"].append({
                    "question": item.get("title"),
                    "url": item.get("url"),
                    "domain": item.get("domain"),
                })
            
            elif item_type == "video":
                serp_features["video_carousel"].append({
                    "title": item.get("title"),
                    "url": item.get("url"),
                    "source": item.get("source"),
                })
            
            elif item_type == "images":
                serp_features["image_pack"] = {
                    "items_count": len(item.get("items", [])),
                }
            
            elif item_type == "shopping":
                serp_features["shopping_results"].append({
                    "title": item.get("title"),
                    "price": item.get("price"),
                    "url": item.get("url"),
                })
            
            elif item_type == "top_stories":
                serp_features["top_stories"].append({
                    "title": item.get("title"),
                    "url": item.get("url"),
                    "source": item.get("source"),
                })
            
            elif item_type == "ai_overview" or item_type == "google_labs":
                serp_features["ai_overview"] = {
                    "text": item.get("text"),
                    "sources": item.get("links", []),
                }
            
            elif item_type == "discussions_and_forums":
                # Reddit threads and other forum results
                serp_features["reddit_threads"].append({
                    "title": item.get("title"),
                    "url": item.get("url"),
                    "domain": item.get("domain"),
                })
        
        return {
            "keyword": keyword,
            "organic_results": organic_results,
            "serp_features": serp_features,
            "total_results": result.get("items_count", 0),
            "metadata": {
                "check_url": result.get("check_url"),
                "datetime": result.get("datetime"),
            },
        }
    
    def extract_competitors(
        self,
        serp_results: List[Dict[str, Any]],
        top_n: int = 10,
    ) -> Dict[str, Any]:
        """
        Extract and rank competitor domains from SERP results.
        
        Args:
            serp_results: List of SERP results from fetch_serp_results()
            top_n: Number of top competitors to return
            
        Returns:
            Dict with competitor analysis:
            {
                "competitors": [
                    {
                        "domain": "competitor.com",
                        "keywords_shared": 34,
                        "avg_position": 4.2,
                        "positions": [3, 5, 4, 6, ...],
                        "threat_level": "high"
                    }
                ],
                "total_keywords_analyzed": 87,
            }
        """
        domain_data = {}
        total_keywords = len(serp_results)
        
        for serp in serp_results:
            for result in serp.get("organic_results", [])[:10]:  # Top 10 only
                domain = result.get("domain")
                position = result.get("position")
                
                if domain:
                    if domain not in domain_data:
                        domain_data[domain] = {
                            "keywords": [],
                            "positions": [],
                        }
                    
                    domain_data[domain]["keywords"].append(serp.get("keyword"))
                    domain_data[domain]["positions"].append(position)
        
        # Calculate metrics for each competitor
        competitors = []
        for domain, data in domain_data.items():
            keywords_shared = len(data["keywords"])
            avg_position = sum(data["positions"]) / len(data["positions"])
            
            # Threat level based on frequency and average position
            keyword_share = keywords_shared / total_keywords
            if keyword_share > 0.3 and avg_position < 5:
                threat_level = "critical"
            elif keyword_share > 0.2 and avg_position < 7:
                threat_level = "high"
            elif keyword_share > 0.1 or avg_position < 5:
                threat_level = "medium"
            else:
                threat_level = "low"
            
            competitors.append({
                "domain": domain,
                "keywords_shared": keywords_shared,
                "keyword_share": round(keyword_share, 3),
                "avg_position": round(avg_position, 2),
                "positions": data["positions"],
                "threat_level": threat_level,
            })
        
        # Sort by keyword share desc
        competitors.sort(key=lambda x: x["keywords_shared"], reverse=True)
        
        return {
            "competitors": competitors[:top_n],
            "total_keywords_analyzed": total_keywords,
        }
    
    def analyze_serp_features(
        self,
        serp_results: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Analyze SERP features across all keywords.
        
        Returns aggregate statistics about SERP feature prevalence,
        displacement effects, and opportunities.
        
        Returns:
            {
                "feature_prevalence": {
                    "featured_snippet": 0.23,
                    "people_also_ask": 0.67,
                    ...
                },
                "displacement_keywords": [
                    {
                        "keyword": "best crm",
                        "features_above_organic": ["featured_snippet", "paa"],
                        "estimated_visual_displacement": 3.5,
                    }
                ],
                "feature_opportunities": [
                    {
                        "feature": "featured_snippet",
                        "keywords_without": ["crm pricing", ...],
                        "opportunity_count": 12,
                    }
                ]
            }
        """
        total_keywords = len(serp_results)
        feature_counts = {
            "featured_snippet": 0,
            "knowledge_panel": 0,
            "local_pack": 0,
            "people_also_ask": 0,
            "video_carousel": 0,
            "image_pack": 0,
            "shopping_results": 0,
            "top_stories": 0,
            "ai_overview": 0,
            "reddit_threads": 0,
        }
        
        displacement_keywords = []
        keywords_by_feature = {feature: [] for feature in feature_counts.keys()}
        
        for serp in serp_results:
            keyword = serp.get("keyword")
            features = serp.get("serp_features", {})
            
            # Count feature presence
            for feature_name, feature_data in features.items():
                if feature_data:
                    if isinstance(feature_data, list) and len(feature_data) > 0:
                        feature_counts[feature_name] += 1
                        keywords_by_feature[feature_name].append(keyword)
                    elif isinstance(feature_data, dict):
                        feature_counts[feature_name] += 1
                        keywords_by_feature[feature_name].append(keyword)
            
            # Calculate visual displacement
            visual_displacement = 0
            features_above = []
            
            if features.get("featured_snippet"):
                visual_displacement += 2.0
                features_above.append("featured_snippet")
            
            if features.get("ai_overview"):
                visual_displacement += 2.5
                features_above.append("ai_overview")
            
            paa_count = len(features.get("people_also_ask", []))
            if paa_count > 0:
                visual_displacement += paa_count * 0.5
                features_above.append(f"paa_x{paa_count}")
            
            if features.get("local_pack"):
                visual_displacement += 1.5
                features_above.append("local_pack")
            
            if features.get("video_carousel") and len(features["video_carousel"]) > 0:
                visual_displacement += 1.0
                features_above.append("video_carousel")
            
            if features.get("image_pack"):
                visual_displacement += 0.5
                features_above.append("image_pack")
            
            if visual_displacement > 2.0:
                displacement_keywords.append({
                    "keyword": keyword,
                    "features_above_organic": features_above,
                    "estimated_visual_displacement": round(visual_displacement, 1),
                })
        
        # Calculate prevalence
        feature_prevalence = {
            feature: round(count / total_keywords, 3)
            for feature, count in feature_counts.items()
        }
        
        # Identify opportunities (keywords without certain features)
        feature_opportunities = []
        for feature, keywords_with in keywords_by_feature.items():
            keywords_without = [
                s["keyword"] for s in serp_results
                if s["keyword"] not in keywords_with
            ]
            if len(keywords_without) > 0:
                feature_opportunities.append({
                    "feature": feature,
                    "keywords_without": keywords_without,
                    "opportunity_count": len(keywords_without),
                })
        
        return {
            "feature_prevalence": feature_prevalence,
            "displacement_keywords": displacement_keywords,
            "feature_opportunities": feature_opportunities,
            "total_keywords_analyzed": total_keywords,
        }
    
    async def fetch_batch_serp_results(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
        device: str = "desktop",
        batch_size: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Fetch SERP results in batches to manage rate limits and costs.
        
        Args:
            keywords: List of keywords to analyze
            location_code: DataForSEO location code
            language_code: Language code
            device: Device type
            batch_size: Number of keywords per batch
            
        Returns:
            List of all SERP results
        """
        all_results = []
        
        for i in range(0, len(keywords), batch_size):
            batch = keywords[i:i + batch_size]
            logger.info(f"Processing batch {i // batch_size + 1} ({len(batch)} keywords)")
            
            batch_results = await self.fetch_serp_results(
                keywords=batch,
                location_code=location_code,
                language_code=language_code,
                device=device,
            )
            
            all_results.extend(batch_results)
            
            # Small delay between batches
            if i + batch_size < len(keywords):
                await asyncio.sleep(1)
        
        logger.info(f"Completed fetching SERP results for {len(all_results)} keywords")
        return all_results