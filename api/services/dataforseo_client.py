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
        self.supabase = supabase_client
        self.cache_ttl_hours = cache_ttl_hours
        
        # Rate limiting: DataForSEO allows 2000 API units/minute for standard accounts
        self._rate_limit_semaphore = asyncio.Semaphore(100)  # Max 100 concurrent requests
        self._request_times: List[float] = []
        self._max_requests_per_minute = 100
        
        self.client: Optional[httpx.AsyncClient] = None
    
    async def __aenter__(self):
        """Async context manager entry"""
        await self.authenticate()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close()
    
    async def authenticate(self):
        """
        Authenticate and initialize HTTP client.
        """
        if self.client is None:
            self.client = httpx.AsyncClient(
                auth=(self.login, self.password),
                timeout=httpx.Timeout(self.timeout),
                limits=httpx.Limits(max_keepalive_connections=20, max_connections=100),
            )
            logger.info("DataForSEO client authenticated")
    
    async def close(self):
        """Close HTTP client"""
        if self.client:
            await self.client.aclose()
            self.client = None
            logger.info("DataForSEO client closed")
    
    async def _enforce_rate_limit(self):
        """
        Enforce rate limiting to stay within API limits.
        Ensures no more than max_requests_per_minute in any 60-second window.
        """
        now = asyncio.get_event_loop().time()
        
        # Remove requests older than 60 seconds
        self._request_times = [t for t in self._request_times if now - t < 60]
        
        # If at limit, wait until oldest request ages out
        if len(self._request_times) >= self._max_requests_per_minute:
            sleep_time = 60 - (now - self._request_times[0]) + 0.1
            if sleep_time > 0:
                logger.warning(f"Rate limit reached, sleeping for {sleep_time:.2f}s")
                await asyncio.sleep(sleep_time)
                # Clean up old requests after sleeping
                now = asyncio.get_event_loop().time()
                self._request_times = [t for t in self._request_times if now - t < 60]
        
        self._request_times.append(now)
    
    def _generate_cache_key(self, endpoint: str, params: Dict[str, Any]) -> str:
        """
        Generate cache key from endpoint and parameters.
        
        Args:
            endpoint: API endpoint
            params: Request parameters
        
        Returns:
            SHA256 hash of endpoint + sorted params
        """
        cache_input = f"{endpoint}:{json.dumps(params, sort_keys=True)}"
        return hashlib.sha256(cache_input.encode()).hexdigest()
    
    async def _get_cached_response(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve cached response from Supabase if available and not expired.
        
        Args:
            cache_key: Cache key
        
        Returns:
            Cached response or None
        """
        if not self.supabase:
            return None
        
        try:
            result = self.supabase.table("dataforseo_cache").select("*").eq("cache_key", cache_key).execute()
            
            if result.data and len(result.data) > 0:
                cached = result.data[0]
                cached_at = datetime.fromisoformat(cached["cached_at"].replace("Z", "+00:00"))
                age_hours = (datetime.now(cached_at.tzinfo) - cached_at).total_seconds() / 3600
                
                if age_hours < self.cache_ttl_hours:
                    logger.info(f"Cache hit for key {cache_key[:16]}... (age: {age_hours:.1f}h)")
                    return cached["response_data"]
                else:
                    logger.info(f"Cache expired for key {cache_key[:16]}... (age: {age_hours:.1f}h)")
            
            return None
        except Exception as e:
            logger.warning(f"Cache retrieval failed: {e}")
            return None
    
    async def _cache_response(self, cache_key: str, response_data: Dict[str, Any]):
        """
        Store response in Supabase cache.
        
        Args:
            cache_key: Cache key
            response_data: Response data to cache
        """
        if not self.supabase:
            return
        
        try:
            self.supabase.table("dataforseo_cache").upsert({
                "cache_key": cache_key,
                "response_data": response_data,
                "cached_at": datetime.utcnow().isoformat(),
            }).execute()
            logger.info(f"Cached response for key {cache_key[:16]}...")
        except Exception as e:
            logger.warning(f"Cache storage failed: {e}")
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((httpx.RequestError, DataForSEORateLimitError)),
    )
    async def _make_request(
        self,
        endpoint: str,
        method: str = "POST",
        data: Optional[List[Dict[str, Any]]] = None,
        use_cache: bool = True,
    ) -> Dict[str, Any]:
        """
        Make HTTP request to DataForSEO API with retry logic.
        
        Args:
            endpoint: API endpoint (e.g., "/serp/google/organic/live/advanced")
            method: HTTP method (POST or GET)
            data: Request payload
            use_cache: Whether to use caching
        
        Returns:
            API response as dict
        
        Raises:
            DataForSEOError: On API errors
            DataForSEORateLimitError: On rate limit errors
            DataForSEOAuthError: On authentication errors
        """
        if not self.client:
            await self.authenticate()
        
        url = f"{self.BASE_URL}{endpoint}"
        
        # Check cache
        if use_cache and method == "POST" and data:
            cache_key = self._generate_cache_key(endpoint, data[0] if data else {})
            cached_response = await self._get_cached_response(cache_key)
            if cached_response:
                return cached_response
        
        # Enforce rate limiting
        async with self._rate_limit_semaphore:
            await self._enforce_rate_limit()
            
            try:
                if method == "POST":
                    response = await self.client.post(url, json=data)
                else:
                    response = await self.client.get(url)
                
                # Handle HTTP errors
                if response.status_code == 401:
                    raise DataForSEOAuthError("Authentication failed. Check credentials.")
                elif response.status_code == 429:
                    raise DataForSEORateLimitError("Rate limit exceeded")
                elif response.status_code >= 400:
                    raise DataForSEOError(
                        f"API request failed with status {response.status_code}: {response.text}"
                    )
                
                result = response.json()
                
                # Check API-level errors
                if result.get("status_code") == 40101:
                    raise DataForSEOAuthError("Invalid credentials")
                elif result.get("status_code") == 50000:
                    raise DataForSEOError(f"API error: {result.get('status_message')}")
                
                # Cache successful response
                if use_cache and method == "POST" and data and result.get("status_code") == 20000:
                    await self._cache_response(cache_key, result)
                
                return result
                
            except httpx.RequestError as e:
                logger.error(f"Request error: {e}")
                raise
    
    async def fetch_serp_results(
        self,
        keywords: List[str],
        location_code: int = 2840,  # USA
        language_code: str = "en",
        device: str = "desktop",
        depth: int = 100,
        use_cache: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Fetch live SERP results for multiple keywords.
        
        Args:
            keywords: List of keywords to query
            location_code: DataForSEO location code (2840 = USA)
            language_code: Language code (e.g., "en")
            device: Device type ("desktop", "mobile", "tablet")
            depth: Number of results to retrieve (max 100)
            use_cache: Whether to use caching
        
        Returns:
            List of SERP result dictionaries, one per keyword
        
        Example response structure:
            [
                {
                    "keyword": "best crm software",
                    "se_results_count": 1234567,
                    "items": [
                        {
                            "type": "organic",
                            "rank_group": 1,
                            "rank_absolute": 1,
                            "position": "left",
                            "domain": "example.com",
                            "title": "...",
                            "url": "...",
                            "description": "..."
                        }
                    ],
                    "serp_features": {
                        "featured_snippet": {...},
                        "people_also_ask": [{...}],
                        ...
                    }
                }
            ]
        """
        if not keywords:
            return []
        
        tasks = []
        for keyword in keywords:
            task_data = [{
                "keyword": keyword,
                "location_code": location_code,
                "language_code": language_code,
                "device": device,
                "depth": depth,
            }]
            
            task = self._make_request(
                endpoint="/serp/google/organic/live/advanced",
                method="POST",
                data=task_data,
                use_cache=use_cache,
            )
            tasks.append(task)
        
        # Execute all requests concurrently
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Parse results
        results = []
        for i, response in enumerate(responses):
            if isinstance(response, Exception):
                logger.error(f"Error fetching SERP for '{keywords[i]}': {response}")
                results.append({
                    "keyword": keywords[i],
                    "error": str(response),
                    "items": [],
                    "serp_features": {},
                })
            elif response.get("status_code") == 20000 and response.get("tasks"):
                task_result = response["tasks"][0]["result"][0] if response["tasks"][0].get("result") else {}
                parsed_result = self._parse_serp_result(keywords[i], task_result)
                results.append(parsed_result)
            else:
                logger.warning(f"Unexpected response for '{keywords[i]}': {response}")
                results.append({
                    "keyword": keywords[i],
                    "error": "Unexpected API response",
                    "items": [],
                    "serp_features": {},
                })
        
        return results
    
    def _parse_serp_result(self, keyword: str, task_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse raw DataForSEO SERP result into structured format.
        
        Args:
            keyword: The keyword queried
            task_result: Raw task result from API
        
        Returns:
            Structured SERP result
        """
        items = task_result.get("items", [])
        
        # Extract organic results
        organic_results = [
            item for item in items
            if item.get("type") == "organic"
        ]
        
        # Extract SERP features
        serp_features = {}
        for feature_name, feature_types in self.SERP_FEATURE_TYPES.items():
            matching_items = [
                item for item in items
                if item.get("type") in feature_types
            ]
            if matching_items:
                serp_features[feature_name] = matching_items
        
        # Count PAA questions
        paa_count = len(serp_features.get("people_also_ask", []))
        
        return {
            "keyword": keyword,
            "se_results_count": task_result.get("se_results_count", 0),
            "items": organic_results,
            "serp_features": serp_features,
            "serp_feature_summary": {
                name: len(items) if isinstance(items, list) else 1
                for name, items in serp_features.items()
            },
            "total_serp_features": len(serp_features),
            "paa_count": paa_count,
            "check_url": task_result.get("check_url"),
            "datetime": task_result.get("datetime"),
        }
    
    async def get_competitor_domains(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
        top_n: int = 10,
    ) -> Dict[str, Any]:
        """
        Identify competitor domains across multiple keywords.
        
        Args:
            keywords: List of keywords to analyze
            location_code: DataForSEO location code
            language_code: Language code
            top_n: Number of top domains to return
        
        Returns:
            Dictionary with competitor analysis:
            {
                "competitors": [
                    {
                        "domain": "competitor.com",
                        "keywords_ranked": 15,
                        "avg_position": 4.2,
                        "appearances": [
                            {"keyword": "...", "position": 3, "url": "..."}
                        ]
                    }
                ],
                "total_keywords_analyzed": 20
            }
        """
        serp_results = await self.fetch_serp_results(
            keywords=keywords,
            location_code=location_code,
            language_code=language_code,
        )
        
        # Aggregate competitor data
        domain_data: Dict[str, Dict[str, Any]] = {}
        
        for result in serp_results:
            keyword = result["keyword"]
            for item in result.get("items", []):
                domain = item.get("domain")
                position = item.get("rank_absolute", 999)
                url = item.get("url", "")
                
                if not domain:
                    continue
                
                if domain not in domain_data:
                    domain_data[domain] = {
                        "domain": domain,
                        "keywords_ranked": 0,
                        "positions": [],
                        "appearances": [],
                    }
                
                domain_data[domain]["keywords_ranked"] += 1
                domain_data[domain]["positions"].append(position)
                domain_data[domain]["appearances"].append({
                    "keyword": keyword,
                    "position": position,
                    "url": url,
                })
        
        # Calculate average positions
        competitors = []
        for domain, data in domain_data.items():
            avg_position = sum(data["positions"]) / len(data["positions"])
            competitors.append({
                "domain": domain,
                "keywords_ranked": data["keywords_ranked"],
                "avg_position": round(avg_position, 2),
                "appearances": data["appearances"],
            })
        
        # Sort by number of keywords ranked (descending)
        competitors.sort(key=lambda x: x["keywords_ranked"], reverse=True)
        
        return {
            "competitors": competitors[:top_n],
            "total_keywords_analyzed": len(keywords),
            "total_unique_domains": len(competitors),
        }
    
    async def analyze_serp_features(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
    ) -> Dict[str, Any]:
        """
        Analyze SERP features across keywords.
        
        Args:
            keywords: List of keywords
            location_code: DataForSEO location code
            language_code: Language code
        
        Returns:
            SERP feature analysis:
            {
                "feature_prevalence": {
                    "featured_snippet": 0.45,  # % of keywords with this feature
                    "people_also_ask": 0.89,
                    ...
                },
                "avg_paa_questions": 4.2,
                "keywords_with_features": [
                    {
                        "keyword": "...",
                        "features": ["featured_snippet", "paa"],
                        "feature_count": 2
                    }
                ]
            }
        """
        serp_results = await self.fetch_serp_results(
            keywords=keywords,
            location_code=location_code,
            language_code=language_code,
        )
        
        total_keywords = len([r for r in serp_results if "error" not in r])
        feature_counts = {feature: 0 for feature in self.SERP_FEATURE_TYPES.keys()}
        paa_counts = []
        keywords_with_features = []
        
        for result in serp_results:
            if "error" in result:
                continue
            
            keyword = result["keyword"]
            features_present = list(result.get("serp_features", {}).keys())
            
            for feature in features_present:
                if feature in feature_counts:
                    feature_counts[feature] += 1
            
            paa_count = result.get("paa_count", 0)
            if paa_count > 0:
                paa_counts.append(paa_count)
            
            keywords_with_features.append({
                "keyword": keyword,
                "features": features_present,
                "feature_count": len(features_present),
                "paa_count": paa_count,
            })
        
        # Calculate prevalence percentages
        feature_prevalence = {
            feature: round(count / total_keywords, 3) if total_keywords > 0 else 0
            for feature, count in feature_counts.items()
        }
        
        avg_paa = round(sum(paa_counts) / len(paa_counts), 2) if paa_counts else 0
        
        return {
            "feature_prevalence": feature_prevalence,
            "avg_paa_questions": avg_paa,
            "total_keywords_analyzed": total_keywords,
            "keywords_with_features": keywords_with_features,
        }
    
    async def get_search_volume(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
    ) -> List[Dict[str, Any]]:
        """
        Get search volume data for keywords.
        
        Args:
            keywords: List of keywords
            location_code: DataForSEO location code
            language_code: Language code
        
        Returns:
            List of keyword volume data:
            [
                {
                    "keyword": "best crm software",
                    "search_volume": 12000,
                    "competition": 0.67,
                    "cpc": 8.45,
                    "trend": [100, 95, 110, ...]  # Monthly trend
                }
            ]
        """
        # Split into batches of 1000 (API limit)
        batch_size = 1000
        batches = [keywords[i:i + batch_size] for i in range(0, len(keywords), batch_size)]
        
        all_results = []
        
        for batch in batches:
            request_data = [{
                "keywords": batch,
                "location_code": location_code,
                "language_code": language_code,
            }]
            
            try:
                response = await self._make_request(
                    endpoint="/keywords_data/google_ads/search_volume/live",
                    method="POST",
                    data=request_data,
                )
                
                if response.get("status_code") == 20000 and response.get("tasks"):
                    result_data = response["tasks"][0].get("result", [])
                    if result_data:
                        all_results.extend(result_data[0].get("items", []))
                
            except Exception as e:
                logger.error(f"Error fetching search volume for batch: {e}")
        
        # Parse results
        parsed_results = []
        for item in all_results:
            parsed_results.append({
                "keyword": item.get("keyword"),
                "search_volume": item.get("search_volume"),
                "competition": item.get("competition"),
                "cpc": item.get("cpc"),
                "monthly_searches": item.get("monthly_searches", []),
            })
        
        return parsed_results
    
    async def batch_process_keywords(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
        batch_size: int = 10,
        delay_between_batches: float = 1.0,
    ) -> Dict[str, Any]:
        """
        Process large keyword lists in batches with rate limiting.
        
        Args:
            keywords: List of keywords to process
            location_code: DataForSEO location code
            language_code: Language code
            batch_size: Keywords per batch
            delay_between_batches: Delay in seconds between batches
        
        Returns:
            Combined results from all batches
        """
        batches = [keywords[i:i + batch_size] for i in range(0, len(keywords), batch_size)]
        
        all_serp_results = []
        
        logger.info(f"Processing {len(keywords)} keywords in {len(batches)} batches")
        
        for i, batch in enumerate(batches):
            logger.info(f"Processing batch {i + 1}/{len(batches)}")
            
            try:
                batch_results = await self.fetch_serp_results(
                    keywords=batch,
                    location_code=location_code,
                    language_code=language_code,
                )
                all_serp_results.extend(batch_results)
                
                # Delay between batches (except for last batch)
                if i < len(batches) - 1:
                    await asyncio.sleep(delay_between_batches)
                    
            except Exception as e:
                logger.error(f"Error processing batch {i + 1}: {e}")
        
        return {
            "total_keywords": len(keywords),
            "successful_queries": len([r for r in all_serp_results if "error" not in r]),
            "failed_queries": len([r for r in all_serp_results if "error" in r]),
            "results": all_serp_results,
        }

