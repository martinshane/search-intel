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
        Test authentication by making a simple API call.
        
        Returns:
            True if authentication successful
            
        Raises:
            DataForSEOAuthError: If authentication fails
        """
        try:
            async with httpx.AsyncClient(auth=self.auth, timeout=self.timeout) as client:
                response = await client.get(f"{self.BASE_URL}/user_data")
                response.raise_for_status()
                
                data = response.json()
                if data.get("status_code") == 20000:
                    logger.info("DataForSEO authentication successful")
                    return True
                else:
                    raise DataForSEOAuthError(
                        f"Authentication failed: {data.get('status_message', 'Unknown error')}"
                    )
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                raise DataForSEOAuthError("Invalid credentials")
            raise DataForSEOError(f"HTTP error during authentication: {e}")
        except Exception as e:
            raise DataForSEOError(f"Authentication error: {e}")
    
    def _generate_cache_key(self, endpoint: str, params: Dict[str, Any]) -> str:
        """
        Generate cache key from endpoint and parameters.
        
        Args:
            endpoint: API endpoint path
            params: Request parameters
            
        Returns:
            Cache key hash
        """
        # Create a stable string representation of params
        param_str = json.dumps(params, sort_keys=True)
        key_content = f"{endpoint}:{param_str}"
        return hashlib.md5(key_content.encode()).hexdigest()
    
    async def _get_cached_response(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve cached response from Supabase.
        
        Args:
            cache_key: Cache key to look up
            
        Returns:
            Cached response data or None if not found/expired
        """
        if not self.supabase:
            return None
        
        try:
            result = self.supabase.table("dataforseo_cache").select("*").eq("cache_key", cache_key).execute()
            
            if result.data and len(result.data) > 0:
                cached = result.data[0]
                cached_at = datetime.fromisoformat(cached["cached_at"])
                expires_at = cached_at + timedelta(hours=self.cache_ttl_hours)
                
                if datetime.utcnow() < expires_at:
                    logger.info(f"Cache hit for key: {cache_key}")
                    return cached["response_data"]
                else:
                    logger.info(f"Cache expired for key: {cache_key}")
                    # Clean up expired cache entry
                    self.supabase.table("dataforseo_cache").delete().eq("cache_key", cache_key).execute()
            
            return None
        except Exception as e:
            logger.warning(f"Cache retrieval error: {e}")
            return None
    
    async def _set_cached_response(self, cache_key: str, data: Dict[str, Any]) -> None:
        """
        Store response in Supabase cache.
        
        Args:
            cache_key: Cache key
            data: Response data to cache
        """
        if not self.supabase:
            return
        
        try:
            cache_entry = {
                "cache_key": cache_key,
                "response_data": data,
                "cached_at": datetime.utcnow().isoformat()
            }
            
            # Upsert to handle both insert and update
            self.supabase.table("dataforseo_cache").upsert(cache_entry).execute()
            logger.info(f"Cached response for key: {cache_key}")
        except Exception as e:
            logger.warning(f"Cache storage error: {e}")
    
    async def _rate_limit(self) -> None:
        """Apply rate limiting between requests."""
        now = asyncio.get_event_loop().time()
        elapsed = now - self._last_request_time
        
        if elapsed < self._min_request_interval:
            await asyncio.sleep(self._min_request_interval - elapsed)
        
        self._last_request_time = asyncio.get_event_loop().time()
    
    @retry(
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.NetworkError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True
    )
    async def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[List[Dict[str, Any]]] = None,
        use_cache: bool = True
    ) -> Dict[str, Any]:
        """
        Make API request with retries and rate limiting.
        
        Args:
            method: HTTP method (GET or POST)
            endpoint: API endpoint path
            data: Request payload for POST requests
            use_cache: Whether to use caching
            
        Returns:
            API response data
            
        Raises:
            DataForSEORateLimitError: If rate limit exceeded
            DataForSEOError: For other API errors
        """
        # Check cache for POST requests with data
        if use_cache and method == "POST" and data:
            cache_key = self._generate_cache_key(endpoint, data[0] if data else {})
            cached = await self._get_cached_response(cache_key)
            if cached:
                return cached
        
        async with self._semaphore:
            await self._rate_limit()
            
            url = f"{self.BASE_URL}/{endpoint}"
            
            try:
                async with httpx.AsyncClient(auth=self.auth, timeout=self.timeout) as client:
                    if method == "POST":
                        response = await client.post(url, json=data)
                    else:
                        response = await client.get(url)
                    
                    # Check for rate limiting (429 status)
                    if response.status_code == 429:
                        retry_after = int(response.headers.get("Retry-After", 60))
                        logger.warning(f"Rate limit exceeded. Retry after {retry_after}s")
                        raise DataForSEORateLimitError(f"Rate limit exceeded. Retry after {retry_after}s")
                    
                    response.raise_for_status()
                    response_data = response.json()
                    
                    # Check DataForSEO-specific status codes
                    if response_data.get("status_code") != 20000:
                        error_msg = response_data.get("status_message", "Unknown error")
                        logger.error(f"DataForSEO API error: {error_msg}")
                        raise DataForSEOError(f"API error: {error_msg}")
                    
                    # Cache successful POST responses
                    if use_cache and method == "POST" and data:
                        await self._set_cached_response(cache_key, response_data)
                    
                    return response_data
                    
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 401:
                    raise DataForSEOAuthError("Authentication failed")
                elif e.response.status_code == 429:
                    raise DataForSEORateLimitError("Rate limit exceeded")
                else:
                    raise DataForSEOError(f"HTTP error {e.response.status_code}: {e}")
            except httpx.TimeoutException:
                logger.warning(f"Request timeout for {endpoint}")
                raise
            except httpx.NetworkError as e:
                logger.warning(f"Network error for {endpoint}: {e}")
                raise
            except Exception as e:
                raise DataForSEOError(f"Request failed: {e}")
    
    async def fetch_serp_results(
        self,
        keywords: List[str],
        location_code: int = 2840,  # United States
        language_code: str = "en",
        device: str = "desktop",
        depth: int = 100,
        use_cache: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Fetch live SERP results for multiple keywords.
        
        Args:
            keywords: List of keywords to fetch SERPs for
            location_code: DataForSEO location code (default: 2840 = United States)
            language_code: Language code (default: "en")
            device: Device type ("desktop" or "mobile")
            depth: Number of results to fetch (max 100)
            use_cache: Whether to use caching
            
        Returns:
            List of SERP result dictionaries, one per keyword
            
        Example response structure:
            [
                {
                    "keyword": "best crm software",
                    "se_results_count": 1250000000,
                    "items": [
                        {
                            "type": "organic",
                            "rank_group": 1,
                            "rank_absolute": 1,
                            "position": "left",
                            "url": "https://example.com/best-crm",
                            "domain": "example.com",
                            "title": "Best CRM Software...",
                            "description": "Compare top CRM...",
                        }
                    ],
                    "serp_features": ["featured_snippet", "people_also_ask"]
                }
            ]
        """
        # Build request payload
        tasks = []
        for keyword in keywords:
            payload = [{
                "keyword": keyword,
                "location_code": location_code,
                "language_code": language_code,
                "device": device,
                "depth": depth
            }]
            tasks.append(
                self._make_request(
                    "POST",
                    "serp/google/organic/live/advanced",
                    data=payload,
                    use_cache=use_cache
                )
            )
        
        # Execute requests in parallel
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process responses
        results = []
        for i, response in enumerate(responses):
            if isinstance(response, Exception):
                logger.error(f"Error fetching SERP for keyword '{keywords[i]}': {response}")
                results.append({
                    "keyword": keywords[i],
                    "error": str(response),
                    "items": [],
                    "serp_features": []
                })
            else:
                # Extract relevant data from response
                task_result = response.get("tasks", [{}])[0].get("result", [{}])[0]
                
                serp_data = {
                    "keyword": keywords[i],
                    "se_results_count": task_result.get("se_results_count", 0),
                    "items": task_result.get("items", []),
                    "serp_features": self._extract_serp_features(task_result.get("items", []))
                }
                
                results.append(serp_data)
        
        return results
    
    def _extract_serp_features(self, items: List[Dict[str, Any]]) -> List[str]:
        """
        Extract SERP feature types from result items.
        
        Args:
            items: List of SERP result items
            
        Returns:
            List of unique SERP feature types present
        """
        features = set()
        
        for item in items:
            item_type = item.get("type", "")
            
            # Map DataForSEO types to our feature names
            feature_mapping = {
                "featured_snippet": "featured_snippet",
                "answer_box": "featured_snippet",
                "knowledge_graph": "knowledge_panel",
                "local_pack": "local_pack",
                "top_stories": "top_stories",
                "video": "video_carousel",
                "images": "image_pack",
                "shopping": "shopping_results",
                "people_also_ask": "people_also_ask",
                "related_searches": "related_searches",
                "twitter": "twitter_results",
                "app": "app_pack",
                "carousel": "carousel"
            }
            
            if item_type in feature_mapping:
                features.add(feature_mapping[item_type])
            
            # Check for PAA
            if item_type == "people_also_ask":
                features.add("people_also_ask")
        
        return sorted(list(features))
    
    async def fetch_competitor_domains(
        self,
        site_url: str,
        keywords: Optional[List[str]] = None,
        location_code: int = 2840,
        language_code: str = "en",
        use_cache: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Fetch competitor domains for a site based on keyword overlap.
        
        Args:
            site_url: Target site URL (e.g., "example.com")
            keywords: Optional list of keywords to analyze. If None, uses DataForSEO's
                     automatic competitor detection endpoint
            location_code: DataForSEO location code
            language_code: Language code
            use_cache: Whether to use caching
            
        Returns:
            List of competitor domains with metrics
            
        Example response:
            [
                {
                    "domain": "competitor1.com",
                    "keywords_shared": 45,
                    "avg_position": 3.2,
                    "visibility_overlap": 0.68,
                    "common_keywords": ["keyword1", "keyword2", ...]
                }
            ]
        """
        if keywords:
            # Manual approach: fetch SERPs for keywords and analyze domain overlap
            serp_results = await self.fetch_serp_results(
                keywords=keywords,
                location_code=location_code,
                language_code=language_code,
                use_cache=use_cache
            )
            
            return self._analyze_competitor_overlap(site_url, serp_results)
        else:
            # Use DataForSEO's competitors endpoint
            payload = [{
                "target": site_url,
                "location_code": location_code,
                "language_code": language_code
            }]
            
            response = await self._make_request(
                "POST",
                "serp/google/competitors/live",
                data=payload,
                use_cache=use_cache
            )
            
            # Extract competitor data
            task_result = response.get("tasks", [{}])[0].get("result", [{}])[0]
            competitors = []
            
            for item in task_result.get("items", []):
                competitors.append({
                    "domain": item.get("domain"),
                    "keywords_shared": item.get("intersections", 0),
                    "avg_position": item.get("avg_position", 0),
                    "visibility_overlap": item.get("visibility", 0),
                    "etv": item.get("etv", 0)  # Estimated traffic value
                })
            
            return competitors
    
    def _analyze_competitor_overlap(
        self,
        site_url: str,
        serp_results: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Analyze competitor domain overlap from SERP results.
        
        Args:
            site_url: Target site domain
            serp_results: List of SERP results from fetch_serp_results
            
        Returns:
            List of competitor domains with metrics
        """
        from urllib.parse import urlparse
        
        # Normalize target domain
        target_domain = urlparse(site_url).netloc or site_url
        target_domain = target_domain.replace("www.", "")
        
        # Track competitor appearances
        competitor_data = {}
        
        for serp in serp_results:
            keyword = serp["keyword"]
            target_found = False
            target_position = None
            
            for item in serp["items"]:
                if item.get("type") != "organic":
                    continue
                
                domain = item.get("domain", "")
                domain = domain.replace("www.", "")
                
                if domain == target_domain:
                    target_found = True
                    target_position = item.get("rank_absolute", 0)
                    continue
                
                # Track competitor
                if domain not in competitor_data:
                    competitor_data[domain] = {
                        "domain": domain,
                        "keywords_shared": 0,
                        "positions": [],
                        "common_keywords": []
                    }
                
                # Only count if target site also ranks for this keyword
                if target_found:
                    competitor_data[domain]["keywords_shared"] += 1
                    competitor_data[domain]["positions"].append(item.get("rank_absolute", 0))
                    competitor_data[domain]["common_keywords"].append(keyword)
        
        # Calculate metrics
        competitors = []
        for domain, data in competitor_data.items():
            if data["keywords_shared"] > 0:
                competitors.append({
                    "domain": domain,
                    "keywords_shared": data["keywords_shared"],
                    "avg_position": sum(data["positions"]) / len(data["positions"]),
                    "visibility_overlap": data["keywords_shared"] / len(serp_results),
                    "common_keywords": data["common_keywords"]
                })
        
        # Sort by keyword overlap
        competitors.sort(key=lambda x: x["keywords_shared"], reverse=True)
        
        return competitors
    
    async def fetch_keyword_data_batch(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
        use_cache: bool = True
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetch comprehensive data for a batch of keywords in parallel.
        
        Args:
            keywords: List of keywords to analyze
            location_code: DataForSEO location code
            language_code: Language code
            use_cache: Whether to use caching
            
        Returns:
            Dictionary mapping keywords to their full SERP data
        """
        serp_results = await self.fetch_serp_results(
            keywords=keywords,
            location_code=location_code,
            language_code=language_code,
            use_cache=use_cache
        )
        
        # Convert list to dict for easier lookup
        return {result["keyword"]: result for result in serp_results}
    
    async def get_serp_features_for_keywords(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
        use_cache: bool = True
    ) -> Dict[str, List[str]]:
        """
        Get SERP features present for each keyword.
        
        Args:
            keywords: List of keywords
            location_code: DataForSEO location code
            language_code: Language code
            use_cache: Whether to use caching
            
        Returns:
            Dictionary mapping keywords to lists of SERP features
        """
        serp_results = await self.fetch_serp_results(
            keywords=keywords,
            location_code=location_code,
            language_code=language_code,
            use_cache=use_cache
        )
        
        return {
            result["keyword"]: result["serp_features"]
            for result in serp_results
        }
    
    async def close(self) -> None:
        """Clean up resources."""
        # No persistent connections to close with httpx
        pass
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.authenticate()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
