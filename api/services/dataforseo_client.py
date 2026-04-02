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
                "DataForSEO credentials not provided. "
                "Set DATAFORSEO_LOGIN and DATAFORSEO_PASSWORD environment variables."
            )
        
        self.timeout = timeout
        self.max_retries = max_retries
        self.supabase = supabase_client
        self.cache_ttl_hours = cache_ttl_hours
        self.rate_limit_per_second = rate_limit_per_second
        
        # Rate limiting
        self._last_request_time = 0.0
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
        """Initialize and authenticate HTTP client"""
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
    
    async def _rate_limit(self):
        """Enforce rate limiting"""
        async with self._rate_limit_lock:
            now = asyncio.get_event_loop().time()
            time_since_last = now - self._last_request_time
            min_interval = 1.0 / self.rate_limit_per_second
            
            if time_since_last < min_interval:
                sleep_time = min_interval - time_since_last
                await asyncio.sleep(sleep_time)
            
            self._last_request_time = asyncio.get_event_loop().time()
    
    def _generate_cache_key(self, endpoint: str, payload: Dict[str, Any]) -> str:
        """Generate cache key for request"""
        cache_data = {
            "endpoint": endpoint,
            "payload": payload
        }
        cache_str = json.dumps(cache_data, sort_keys=True)
        return hashlib.sha256(cache_str.encode()).hexdigest()
    
    async def _get_cached_response(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """Retrieve cached response from Supabase"""
        if not self.supabase:
            return None
        
        try:
            result = self.supabase.table("dataforseo_cache").select("*").eq("cache_key", cache_key).execute()
            
            if result.data and len(result.data) > 0:
                cached = result.data[0]
                cached_time = datetime.fromisoformat(cached["created_at"])
                
                if datetime.utcnow() - cached_time < timedelta(hours=self.cache_ttl_hours):
                    logger.info(f"Cache hit for key {cache_key[:16]}...")
                    return cached["response_data"]
                else:
                    logger.info(f"Cache expired for key {cache_key[:16]}...")
                    # Delete expired cache
                    self.supabase.table("dataforseo_cache").delete().eq("cache_key", cache_key).execute()
            
            return None
        except Exception as e:
            logger.warning(f"Cache retrieval error: {e}")
            return None
    
    async def _save_cached_response(self, cache_key: str, response_data: Dict[str, Any]):
        """Save response to cache"""
        if not self.supabase:
            return
        
        try:
            self.supabase.table("dataforseo_cache").upsert({
                "cache_key": cache_key,
                "response_data": response_data,
                "created_at": datetime.utcnow().isoformat()
            }).execute()
            logger.info(f"Cached response for key {cache_key[:16]}...")
        except Exception as e:
            logger.warning(f"Cache save error: {e}")
    
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
        use_cache: bool = True
    ) -> Dict[str, Any]:
        """
        Make API request with retries and caching.
        
        Args:
            endpoint: API endpoint path
            payload: Request payload
            use_cache: Whether to use caching
            
        Returns:
            API response data
            
        Raises:
            DataForSEOError: On API errors
            DataForSEORateLimitError: On rate limit exceeded
            DataForSEOAuthError: On authentication failure
        """
        if not self._client:
            await self.authenticate()
        
        # Check cache
        cache_key = None
        if use_cache:
            cache_key = self._generate_cache_key(endpoint, payload)
            cached = await self._get_cached_response(cache_key)
            if cached:
                return cached
        
        # Rate limiting
        await self._rate_limit()
        
        # Make request
        url = f"{self.BASE_URL}{endpoint}"
        
        try:
            logger.debug(f"Making request to {endpoint}")
            response = await self._client.post(url, json=payload)
            
            # Handle authentication errors
            if response.status_code == 401:
                raise DataForSEOAuthError("Authentication failed. Check credentials.")
            
            # Handle rate limiting
            if response.status_code == 429:
                raise DataForSEORateLimitError("Rate limit exceeded")
            
            # Handle other errors
            if response.status_code >= 400:
                error_msg = f"API error {response.status_code}: {response.text}"
                logger.error(error_msg)
                raise DataForSEOError(error_msg)
            
            data = response.json()
            
            # Check API-level errors
            if data.get("status_code") != 20000:
                error_msg = data.get("status_message", "Unknown API error")
                logger.error(f"DataForSEO API error: {error_msg}")
                raise DataForSEOError(error_msg)
            
            # Cache successful response
            if use_cache and cache_key:
                await self._save_cached_response(cache_key, data)
            
            return data
            
        except httpx.TimeoutException as e:
            logger.error(f"Request timeout: {e}")
            raise
        except httpx.NetworkError as e:
            logger.error(f"Network error: {e}")
            raise
        except Exception as e:
            if isinstance(e, (DataForSEOError, DataForSEORateLimitError, DataForSEOAuthError)):
                raise
            logger.error(f"Unexpected error: {e}")
            raise DataForSEOError(f"Unexpected error: {e}")
    
    async def fetch_serp_results(
        self,
        keywords: List[str],
        location_code: int = 2840,  # United States
        language_code: str = "en",
        device: str = "desktop",
        os: str = "windows",
        depth: int = 100,
        use_cache: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Fetch live SERP results for keywords.
        
        Args:
            keywords: List of keywords to fetch
            location_code: DataForSEO location code (2840 = United States)
            language_code: Language code (e.g., "en")
            device: Device type ("desktop" or "mobile")
            os: Operating system
            depth: Number of results to fetch (max 700)
            use_cache: Whether to use caching
            
        Returns:
            List of SERP result dictionaries, one per keyword
        """
        endpoint = "/serp/google/organic/live/advanced"
        
        tasks = []
        for keyword in keywords:
            payload = [{
                "keyword": keyword,
                "location_code": location_code,
                "language_code": language_code,
                "device": device,
                "os": os,
                "depth": depth,
                "calculate_rectangles": True,
            }]
            tasks.append(self._make_request(endpoint, payload, use_cache))
        
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        
        results = []
        for i, response in enumerate(responses):
            if isinstance(response, Exception):
                logger.error(f"Error fetching SERP for '{keywords[i]}': {response}")
                results.append({
                    "keyword": keywords[i],
                    "error": str(response),
                    "items": [],
                    "serp_features": [],
                })
            else:
                parsed = self._parse_serp_response(response, keywords[i])
                results.append(parsed)
        
        return results
    
    def _parse_serp_response(self, response: Dict[str, Any], keyword: str) -> Dict[str, Any]:
        """
        Parse SERP API response into structured format.
        
        Args:
            response: Raw API response
            keyword: Keyword that was queried
            
        Returns:
            Parsed SERP data
        """
        if not response.get("tasks") or len(response["tasks"]) == 0:
            return {
                "keyword": keyword,
                "items": [],
                "serp_features": [],
                "total_results": 0,
            }
        
        task = response["tasks"][0]
        if task.get("status_code") != 20000:
            return {
                "keyword": keyword,
                "error": task.get("status_message", "Unknown error"),
                "items": [],
                "serp_features": [],
            }
        
        result = task.get("result", [{}])[0]
        items = result.get("items", [])
        
        # Extract organic results
        organic_results = []
        serp_features = []
        featured_snippet = None
        people_also_ask = []
        
        for item in items:
            item_type = item.get("type")
            
            if item_type == "organic":
                organic_results.append({
                    "position": item.get("rank_group"),
                    "url": item.get("url"),
                    "domain": item.get("domain"),
                    "title": item.get("title"),
                    "description": item.get("description"),
                    "breadcrumb": item.get("breadcrumb"),
                })
            
            elif item_type == "featured_snippet":
                featured_snippet = {
                    "type": "featured_snippet",
                    "url": item.get("url"),
                    "domain": item.get("domain"),
                    "title": item.get("title"),
                    "description": item.get("description"),
                }
                serp_features.append(featured_snippet)
            
            elif item_type == "people_also_ask":
                for paa_item in item.get("items", []):
                    people_also_ask.append({
                        "question": paa_item.get("title"),
                        "url": paa_item.get("url"),
                    })
                if people_also_ask:
                    serp_features.append({
                        "type": "people_also_ask",
                        "count": len(people_also_ask),
                        "questions": people_also_ask,
                    })
            
            elif item_type == "knowledge_graph":
                serp_features.append({
                    "type": "knowledge_graph",
                    "title": item.get("title"),
                    "description": item.get("description"),
                })
            
            elif item_type == "local_pack":
                serp_features.append({
                    "type": "local_pack",
                    "count": len(item.get("items", [])),
                })
            
            elif item_type in ["video", "video_carousel"]:
                serp_features.append({
                    "type": "video",
                    "count": len(item.get("items", [])),
                })
            
            elif item_type in ["images", "image_pack"]:
                serp_features.append({
                    "type": "image",
                    "count": len(item.get("items", [])),
                })
            
            elif item_type in ["shopping", "google_shopping"]:
                serp_features.append({
                    "type": "shopping",
                    "count": len(item.get("items", [])),
                })
            
            elif item_type == "top_stories":
                serp_features.append({
                    "type": "top_stories",
                    "count": len(item.get("items", [])),
                })
            
            elif item_type == "twitter":
                serp_features.append({
                    "type": "twitter",
                    "count": len(item.get("items", [])),
                })
            
            elif item_type == "recipes":
                serp_features.append({
                    "type": "recipes",
                    "count": len(item.get("items", [])),
                })
            
            elif item_type == "ai_overview":
                serp_features.append({
                    "type": "ai_overview",
                    "text": item.get("text"),
                })
            
            elif item_type in ["people_also_search", "related_searches"]:
                serp_features.append({
                    "type": "related_searches",
                    "count": len(item.get("items", [])),
                })
        
        return {
            "keyword": keyword,
            "total_results": result.get("total_count", 0),
            "organic_results": organic_results,
            "serp_features": serp_features,
            "featured_snippet": featured_snippet,
            "people_also_ask": people_also_ask,
            "check_url": result.get("check_url"),
        }
    
    async def extract_competitors(
        self,
        serp_results: List[Dict[str, Any]],
        user_domain: str,
        top_n: int = 10,
    ) -> Dict[str, Any]:
        """
        Extract competitor analysis from SERP results.
        
        Args:
            serp_results: List of parsed SERP results from fetch_serp_results()
            user_domain: User's domain to exclude from competitors
            top_n: Number of top competitors to return
            
        Returns:
            Competitor analysis dictionary
        """
        domain_appearances = {}
        domain_positions = {}
        keyword_count = 0
        
        for serp in serp_results:
            if serp.get("error"):
                continue
            
            keyword_count += 1
            
            for result in serp.get("organic_results", []):
                domain = result.get("domain")
                position = result.get("position")
                
                if not domain or domain == user_domain:
                    continue
                
                # Track appearances
                if domain not in domain_appearances:
                    domain_appearances[domain] = 0
                    domain_positions[domain] = []
                
                domain_appearances[domain] += 1
                domain_positions[domain].append(position)
        
        # Calculate competitor metrics
        competitors = []
        for domain, appearances in domain_appearances.items():
            positions = domain_positions[domain]
            avg_position = sum(positions) / len(positions)
            appearance_rate = appearances / keyword_count if keyword_count > 0 else 0
            
            # Threat level based on appearance rate and average position
            if appearance_rate > 0.3 and avg_position <= 5:
                threat_level = "high"
            elif appearance_rate > 0.2 or avg_position <= 5:
                threat_level = "medium"
            else:
                threat_level = "low"
            
            competitors.append({
                "domain": domain,
                "keywords_shared": appearances,
                "appearance_rate": round(appearance_rate, 3),
                "avg_position": round(avg_position, 2),
                "best_position": min(positions),
                "worst_position": max(positions),
                "threat_level": threat_level,
            })
        
        # Sort by appearance rate, then by average position
        competitors.sort(key=lambda x: (-x["appearance_rate"], x["avg_position"]))
        
        return {
            "total_keywords_analyzed": keyword_count,
            "total_unique_competitors": len(competitors),
            "top_competitors": competitors[:top_n],
            "all_competitors": competitors,
        }
    
    async def analyze_serp_features(
        self,
        serp_results: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Analyze SERP features across all keywords.
        
        Args:
            serp_results: List of parsed SERP results from fetch_serp_results()
            
        Returns:
            SERP feature analysis
        """
        feature_counts = {}
        keywords_with_features = {}
        total_keywords = 0
        
        for serp in serp_results:
            if serp.get("error"):
                continue
            
            total_keywords += 1
            keyword = serp.get("keyword")
            
            for feature in serp.get("serp_features", []):
                feature_type = feature.get("type")
                
                if feature_type not in feature_counts:
                    feature_counts[feature_type] = 0
                    keywords_with_features[feature_type] = []
                
                feature_counts[feature_type] += 1
                keywords_with_features[feature_type].append(keyword)
        
        # Calculate percentages
        feature_analysis = []
        for feature_type, count in feature_counts.items():
            percentage = (count / total_keywords * 100) if total_keywords > 0 else 0
            feature_analysis.append({
                "feature_type": feature_type,
                "count": count,
                "percentage": round(percentage, 1),
                "keywords": keywords_with_features[feature_type],
            })
        
        # Sort by count descending
        feature_analysis.sort(key=lambda x: -x["count"])
        
        return {
            "total_keywords_analyzed": total_keywords,
            "unique_feature_types": len(feature_counts),
            "features": feature_analysis,
        }
    
    async def calculate_serp_displacement(
        self,
        serp_results: List[Dict[str, Any]],
        user_domain: str,
    ) -> List[Dict[str, Any]]:
        """
        Calculate visual position displacement for user's rankings.
        
        Args:
            serp_results: List of parsed SERP results
            user_domain: User's domain
            
        Returns:
            List of displacement analysis per keyword
        """
        displacement_analysis = []
        
        # Visual position weights
        FEATURE_WEIGHTS = {
            "featured_snippet": 2.0,
            "ai_overview": 2.0,
            "people_also_ask": 0.5,  # per question
            "knowledge_graph": 1.5,
            "local_pack": 2.0,
            "video": 1.5,
            "image": 1.0,
            "shopping": 1.5,
            "top_stories": 1.5,
        }
        
        for serp in serp_results:
            if serp.get("error"):
                continue
            
            keyword = serp.get("keyword")
            
            # Find user's organic position
            user_position = None
            for result in serp.get("organic_results", []):
                if result.get("domain") == user_domain:
                    user_position = result.get("position")
                    break
            
            if user_position is None:
                continue
            
            # Calculate visual displacement
            visual_displacement = 0.0
            features_above = []
            
            for feature in serp.get("serp_features", []):
                feature_type = feature.get("type")
                weight = FEATURE_WEIGHTS.get(feature_type, 1.0)
                
                # For PAA, multiply by question count
                if feature_type == "people_also_ask":
                    count = feature.get("count", 0)
                    weight = weight * count
                
                visual_displacement += weight
                features_above.append({
                    "type": feature_type,
                    "weight": weight,
                })
            
            visual_position = user_position + visual_displacement
            
            # Estimate CTR impact (rough approximation)
            # Assuming ~2% CTR at position 3, ~1% at position 5, etc.
            # Each position displacement reduces CTR by ~0.5%
            estimated_ctr_impact = -0.005 * visual_displacement
            
            displacement_analysis.append({
                "keyword": keyword,
                "organic_position": user_position,
                "visual_displacement": round(visual_displacement, 1),
                "visual_position": round(visual_position, 1),
                "features_above": features_above,
                "estimated_ctr_impact": round(estimated_ctr_impact, 4),
            })
        
        # Sort by displacement magnitude
        displacement_analysis.sort(key=lambda x: -x["visual_displacement"])
        
        return displacement_analysis
    
    async def batch_process_keywords(
        self,
        keywords: List[str],
        batch_size: int = 10,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """
        Process keywords in batches to avoid overwhelming the API.
        
        Args:
            keywords: List of keywords
            batch_size: Number of keywords per batch
            **kwargs: Additional arguments passed to fetch_serp_results()
            
        Returns:
            Combined results from all batches
        """
        all_results = []
        
        for i in range(0, len(keywords), batch_size):
            batch = keywords[i:i + batch_size]
            logger.info(f"Processing batch {i // batch_size + 1} of {(len(keywords) + batch_size - 1) // batch_size}")
            
            batch_results = await self.fetch_serp_results(batch, **kwargs)
            all_results.extend(batch_results)
            
            # Small delay between batches
            if i + batch_size < len(keywords):
                await asyncio.sleep(1)
        
        return all_results
