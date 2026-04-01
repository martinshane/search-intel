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
        Verify authentication credentials by making a test API call.
        
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
                    timeout=self.timeout,
                )
                
                if response.status_code == 401:
                    raise DataForSEOAuthError("Invalid DataForSEO credentials")
                
                if response.status_code == 200:
                    logger.info("DataForSEO authentication successful")
                    return True
                
                response.raise_for_status()
                return True
                
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                raise DataForSEOAuthError("Invalid DataForSEO credentials")
            raise DataForSEOError(f"Authentication failed: {str(e)}")
        except Exception as e:
            raise DataForSEOError(f"Authentication error: {str(e)}")
    
    def _generate_cache_key(self, endpoint: str, params: Dict[str, Any]) -> str:
        """
        Generate a cache key for the request.
        
        Args:
            endpoint: API endpoint
            params: Request parameters
            
        Returns:
            MD5 hash as cache key
        """
        cache_data = {
            "endpoint": endpoint,
            "params": params,
        }
        cache_string = json.dumps(cache_data, sort_keys=True)
        return hashlib.md5(cache_string.encode()).hexdigest()
    
    async def _get_cached_response(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve cached response from Supabase.
        
        Args:
            cache_key: Cache key
            
        Returns:
            Cached response or None if not found/expired
        """
        if not self.supabase:
            return None
        
        try:
            result = self.supabase.table("dataforseo_cache").select("*").eq("cache_key", cache_key).execute()
            
            if not result.data:
                return None
            
            cache_entry = result.data[0]
            cached_at = datetime.fromisoformat(cache_entry["cached_at"])
            expiry = cached_at + timedelta(hours=self.cache_ttl_hours)
            
            if datetime.utcnow() > expiry:
                # Cache expired, delete it
                self.supabase.table("dataforseo_cache").delete().eq("cache_key", cache_key).execute()
                return None
            
            logger.info(f"Cache hit for key: {cache_key}")
            return cache_entry["response_data"]
            
        except Exception as e:
            logger.warning(f"Cache retrieval error: {str(e)}")
            return None
    
    async def _cache_response(self, cache_key: str, response_data: Dict[str, Any]) -> None:
        """
        Cache response in Supabase.
        
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
            logger.info(f"Cached response for key: {cache_key}")
        except Exception as e:
            logger.warning(f"Cache storage error: {str(e)}")
    
    async def _rate_limit(self) -> None:
        """Apply rate limiting between requests"""
        current_time = asyncio.get_event_loop().time()
        time_since_last_request = current_time - self._last_request_time
        
        if time_since_last_request < self._min_request_interval:
            await asyncio.sleep(self._min_request_interval - time_since_last_request)
        
        self._last_request_time = asyncio.get_event_loop().time()
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.HTTPStatusError)),
    )
    async def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[List[Dict[str, Any]]] = None,
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
            API response as dictionary
            
        Raises:
            DataForSEORateLimitError: If rate limit exceeded
            DataForSEOError: For other API errors
        """
        async with self._semaphore:
            await self._rate_limit()
            
            # Check cache for POST requests with data
            cache_key = None
            if use_cache and method == "POST" and data:
                cache_key = self._generate_cache_key(endpoint, data[0] if data else {})
                cached_response = await self._get_cached_response(cache_key)
                if cached_response:
                    return cached_response
            
            url = f"{self.BASE_URL}/{endpoint}"
            
            try:
                async with httpx.AsyncClient() as client:
                    if method == "POST":
                        response = await client.post(
                            url,
                            json=data,
                            auth=self.auth,
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
                    
                    # Handle authentication errors
                    if response.status_code == 401:
                        raise DataForSEOAuthError("Authentication failed")
                    
                    response.raise_for_status()
                    response_data = response.json()
                    
                    # Check for API-level errors
                    if response_data.get("status_code") != 20000:
                        error_msg = response_data.get("status_message", "Unknown error")
                        raise DataForSEOError(f"API error: {error_msg}")
                    
                    # Cache successful response
                    if cache_key:
                        await self._cache_response(cache_key, response_data)
                    
                    return response_data
                    
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:
                    raise DataForSEORateLimitError("Rate limit exceeded")
                elif e.response.status_code == 401:
                    raise DataForSEOAuthError("Authentication failed")
                else:
                    raise DataForSEOError(f"HTTP error: {str(e)}")
            except httpx.TimeoutException:
                logger.warning(f"Request timeout for endpoint: {endpoint}")
                raise
            except Exception as e:
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
            keywords: List of keywords to fetch SERPs for
            location_code: DataForSEO location code (2840 = United States)
            language_code: Language code (e.g., "en")
            device: Device type ("desktop", "mobile", "tablet")
            depth: Number of results to retrieve (max 100)
            use_cache: Whether to use cached results
            
        Returns:
            List of normalized SERP results, one per keyword
            
        Example:
            >>> results = await client.fetch_serp_results(
            ...     keywords=["best crm software", "crm pricing"],
            ...     location_code=2840
            ... )
        """
        tasks = []
        for keyword in keywords:
            task = self._fetch_single_serp(
                keyword=keyword,
                location_code=location_code,
                language_code=language_code,
                device=device,
                depth=depth,
                use_cache=use_cache,
            )
            tasks.append(task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out exceptions and log errors
        valid_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Failed to fetch SERP for keyword '{keywords[i]}': {str(result)}")
            else:
                valid_results.append(result)
        
        return valid_results
    
    async def _fetch_single_serp(
        self,
        keyword: str,
        location_code: int,
        language_code: str,
        device: str,
        depth: int,
        use_cache: bool,
    ) -> Dict[str, Any]:
        """
        Fetch SERP results for a single keyword.
        
        Returns:
            Normalized SERP result dictionary
        """
        payload = [{
            "keyword": keyword,
            "location_code": location_code,
            "language_code": language_code,
            "device": device,
            "depth": depth,
            "calculate_rectangles": True,  # For visual position calculation
        }]
        
        response = await self._make_request(
            method="POST",
            endpoint="serp/google/organic/live/advanced",
            data=payload,
            use_cache=use_cache,
        )
        
        return self._normalize_serp_response(response, keyword)
    
    def _normalize_serp_response(self, response: Dict[str, Any], keyword: str) -> Dict[str, Any]:
        """
        Normalize DataForSEO SERP response into a consistent format.
        
        Args:
            response: Raw DataForSEO API response
            keyword: Original keyword
            
        Returns:
            Normalized SERP data
        """
        if not response.get("tasks") or not response["tasks"][0].get("result"):
            return {
                "keyword": keyword,
                "organic_results": [],
                "serp_features": {},
                "total_results": 0,
                "error": "No results returned",
            }
        
        task_result = response["tasks"][0]["result"][0]
        items = task_result.get("items", [])
        
        # Extract organic results
        organic_results = []
        serp_features = {}
        feature_positions = []
        
        for item in items:
            item_type = item.get("type", "")
            rank_absolute = item.get("rank_absolute", 0)
            
            if item_type == "organic":
                organic_results.append({
                    "position": item.get("rank_group", rank_absolute),
                    "url": item.get("url", ""),
                    "domain": item.get("domain", ""),
                    "title": item.get("title", ""),
                    "description": item.get("description", ""),
                    "is_featured": item.get("is_featured", False),
                    "rank_absolute": rank_absolute,
                })
            else:
                # Track SERP features
                feature_category = self._categorize_serp_feature(item_type)
                if feature_category:
                    if feature_category not in serp_features:
                        serp_features[feature_category] = []
                    
                    serp_features[feature_category].append({
                        "type": item_type,
                        "rank_absolute": rank_absolute,
                        "rectangle": item.get("rectangle", {}),
                    })
                    
                    feature_positions.append(rank_absolute)
        
        # Calculate visual displacement
        avg_feature_position = sum(feature_positions) / len(feature_positions) if feature_positions else 0
        
        return {
            "keyword": keyword,
            "organic_results": organic_results,
            "serp_features": serp_features,
            "total_results": task_result.get("items_count", 0),
            "feature_count": len(feature_positions),
            "avg_feature_position": avg_feature_position,
            "fetched_at": datetime.utcnow().isoformat(),
        }
    
    def _categorize_serp_feature(self, item_type: str) -> Optional[str]:
        """
        Categorize DataForSEO item type into standard SERP feature category.
        
        Args:
            item_type: DataForSEO item type
            
        Returns:
            Standardized feature category or None
        """
        for category, types in self.SERP_FEATURE_TYPES.items():
            if item_type in types:
                return category
        return None
    
    async def analyze_competitors(
        self,
        serp_results: List[Dict[str, Any]],
        user_domain: str,
    ) -> Dict[str, Any]:
        """
        Analyze competitor presence across SERP results.
        
        Args:
            serp_results: List of SERP results from fetch_serp_results
            user_domain: User's domain to exclude from competitor analysis
            
        Returns:
            Competitor analysis with frequency, average positions, overlap
            
        Example:
            >>> serp_results = await client.fetch_serp_results([...])
            >>> analysis = await client.analyze_competitors(serp_results, "mysite.com")
        """
        competitor_data = {}
        total_keywords = len(serp_results)
        
        for serp in serp_results:
            keyword = serp["keyword"]
            for result in serp["organic_results"]:
                domain = result["domain"]
                
                # Skip user's own domain
                if user_domain in domain or domain in user_domain:
                    continue
                
                if domain not in competitor_data:
                    competitor_data[domain] = {
                        "domain": domain,
                        "keywords_appeared": [],
                        "positions": [],
                    }
                
                competitor_data[domain]["keywords_appeared"].append(keyword)
                competitor_data[domain]["positions"].append(result["position"])
        
        # Calculate aggregate metrics
        competitors = []
        for domain, data in competitor_data.items():
            appearance_count = len(data["keywords_appeared"])
            avg_position = sum(data["positions"]) / len(data["positions"])
            
            competitors.append({
                "domain": domain,
                "keywords_shared": appearance_count,
                "keyword_overlap_pct": (appearance_count / total_keywords) * 100,
                "avg_position": round(avg_position, 1),
                "threat_level": self._calculate_threat_level(appearance_count, total_keywords, avg_position),
            })
        
        # Sort by keyword overlap descending
        competitors.sort(key=lambda x: x["keywords_shared"], reverse=True)
        
        return {
            "total_keywords_analyzed": total_keywords,
            "unique_competitors": len(competitors),
            "top_competitors": competitors[:20],  # Top 20 competitors
            "primary_competitors": [c for c in competitors if c["keyword_overlap_pct"] > 20],
        }
    
    def _calculate_threat_level(self, appearance_count: int, total_keywords: int, avg_position: float) -> str:
        """
        Calculate competitor threat level based on overlap and position.
        
        Args:
            appearance_count: Number of keywords competitor appears in
            total_keywords: Total keywords analyzed
            avg_position: Competitor's average position
            
        Returns:
            Threat level: "high", "medium", or "low"
        """
        overlap_pct = (appearance_count / total_keywords) * 100
        
        if overlap_pct > 40 and avg_position < 5:
            return "high"
        elif overlap_pct > 20 and avg_position < 10:
            return "medium"
        else:
            return "low"
    
    async def detect_serp_features(
        self,
        serp_results: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Detect and analyze SERP features across keyword set.
        
        Args:
            serp_results: List of SERP results from fetch_serp_results
            
        Returns:
            SERP feature analysis with prevalence, types, impact
            
        Example:
            >>> analysis = await client.detect_serp_features(serp_results)
        """
        feature_prevalence = {}
        feature_details = []
        total_keywords = len(serp_results)
        
        for serp in serp_results:
            keyword = serp["keyword"]
            features = serp.get("serp_features", {})
            
            for feature_type, occurrences in features.items():
                if feature_type not in feature_prevalence:
                    feature_prevalence[feature_type] = 0
                feature_prevalence[feature_type] += 1
                
                for occurrence in occurrences:
                    feature_details.append({
                        "keyword": keyword,
                        "feature_type": feature_type,
                        "rank_absolute": occurrence["rank_absolute"],
                    })
        
        # Calculate prevalence percentages
        feature_stats = []
        for feature_type, count in feature_prevalence.items():
            feature_stats.append({
                "feature_type": feature_type,
                "keyword_count": count,
                "prevalence_pct": round((count / total_keywords) * 100, 1),
            })
        
        feature_stats.sort(key=lambda x: x["keyword_count"], reverse=True)
        
        return {
            "total_keywords_analyzed": total_keywords,
            "unique_feature_types": len(feature_prevalence),
            "feature_stats": feature_stats,
            "feature_details": feature_details,
        }
    
    async def calculate_visual_displacement(
        self,
        serp_results: List[Dict[str, Any]],
        user_domain: str,
    ) -> List[Dict[str, Any]]:
        """
        Calculate visual displacement for user's rankings due to SERP features.
        
        Args:
            serp_results: List of SERP results from fetch_serp_results
            user_domain: User's domain
            
        Returns:
            List of keywords with displacement analysis
            
        Example:
            >>> displacement = await client.calculate_visual_displacement(serp_results, "mysite.com")
        """
        displacement_analysis = []
        
        for serp in serp_results:
            keyword = serp["keyword"]
            
            # Find user's ranking
            user_position = None
            user_rank_absolute = None
            for result in serp["organic_results"]:
                if user_domain in result["domain"] or result["domain"] in user_domain:
                    user_position = result["position"]
                    user_rank_absolute = result["rank_absolute"]
                    break
            
            if user_position is None:
                continue
            
            # Count features above user's position
            features_above = []
            for feature_type, occurrences in serp.get("serp_features", {}).items():
                for occurrence in occurrences:
                    if occurrence["rank_absolute"] < user_rank_absolute:
                        features_above.append({
                            "type": feature_type,
                            "rank_absolute": occurrence["rank_absolute"],
                        })
            
            # Calculate visual position impact
            # Simplified: each feature adds estimated visual positions
            feature_weights = {
                "featured_snippet": 2.0,
                "people_also_ask": 0.5,  # per PAA box
                "knowledge_graph": 1.5,
                "local_pack": 3.0,
                "video": 1.0,
                "image": 0.5,
                "shopping": 1.5,
                "top_stories": 1.0,
                "ai_overview": 2.5,
            }
            
            visual_displacement = 0
            for feature in features_above:
                weight = feature_weights.get(feature["type"], 1.0)
                visual_displacement += weight
            
            visual_position = user_position + visual_displacement
            
            if visual_displacement > 3:  # Only report significant displacement
                displacement_analysis.append({
                    "keyword": keyword,
                    "organic_position": user_position,
                    "visual_position": round(visual_position, 1),
                    "displacement": round(visual_displacement, 1),
                    "features_above": [f["type"] for f in features_above],
                    "feature_count": len(features_above),
                })
        
        # Sort by displacement descending
        displacement_analysis.sort(key=lambda x: x["displacement"], reverse=True)
        
        return displacement_analysis
    
    async def track_ranking_positions(
        self,
        keywords: List[str],
        user_domain: str,
        location_code: int = 2840,
        language_code: str = "en",
    ) -> List[Dict[str, Any]]:
        """
        Track current ranking positions for a domain across keywords.
        
        Args:
            keywords: List of keywords to track
            user_domain: Domain to track rankings for
            location_code: DataForSEO location code
            language_code: Language code
            
        Returns:
            List of ranking positions per keyword
            
        Example:
            >>> positions = await client.track_ranking_positions(
            ...     keywords=["crm software", "best crm"],
            ...     user_domain="mysite.com"
            ... )
        """
        serp_results = await self.fetch_serp_results(
            keywords=keywords,
            location_code=location_code,
            language_code=language_code,
        )
        
        position_tracking = []
        
        for serp in serp_results:
            keyword = serp["keyword"]
            
            # Find user's position
            user_position = None
            user_url = None
            for result in serp["organic_results"]:
                if user_domain in result["domain"] or result["domain"] in user_domain:
                    user_position = result["position"]
                    user_url = result["url"]
                    break
            
            position_tracking.append({
                "keyword": keyword,
                "position": user_position,
                "url": user_url,
                "is_ranking": user_position is not None,
                "in_top_10": user_position is not None and user_position <= 10,
                "in_top_3": user_position is not None and user_position <= 3,
                "feature_count": serp.get("feature_count", 0),
                "tracked_at": datetime.utcnow().isoformat(),
            })
        
        return position_tracking
    
    async def batch_process_keywords(
        self,
        keywords: List[str],
        user_domain: str,
        location_code: int = 2840,
        language_code: str = "en",
        batch_size: int = 50,
    ) -> Dict[str, Any]:
        """
        Process large keyword lists in batches and generate comprehensive analysis.
        
        Args:
            keywords: List of keywords to analyze
            user_domain: User's domain
            location_code: DataForSEO location code
            language_code: Language code
            batch_size: Number of keywords per batch
            
        Returns:
            Complete SERP landscape analysis including competitors, features, displacement
            
        Example:
            >>> analysis = await client.batch_process_keywords(
            ...     keywords=top_keywords,
            ...     user_domain="mysite.com"
            ... )
        """
        all_serp_results = []
        
        # Process in batches
        for i in range(0, len(keywords), batch_size):
            batch = keywords[i:i + batch_size]
            logger.info(f"Processing batch {i // batch_size + 1}: {len(batch)} keywords")
            
            batch_results = await self.fetch_serp_results(
                keywords=batch,
                location_code=location_code,
                language_code=language_code,
            )
            all_serp_results.extend(batch_results)
            
            # Small delay between batches to be respectful
            if i + batch_size < len(keywords):
                await asyncio.sleep(1)
        
        # Run all analyses
        competitor_analysis = await self.analyze_competitors(all_serp_results, user_domain)
        feature_analysis = await self.detect_serp_features(all_serp_results)
        displacement_analysis = await self.calculate_visual_displacement(all_serp_results, user_domain)
        position_tracking = await self.track_ranking_positions(keywords, user_domain, location_code, language_code)
        
        return {
            "keywords_analyzed": len(keywords),
            "successful_fetches": len(all_serp_results),
            "serp_results": all_serp_results,
            "competitor_analysis": competitor_analysis,
            "feature_analysis": feature_analysis,
            "displacement_analysis": displacement_analysis,
            "position_tracking": position_tracking,
            "analysis_timestamp": datetime.utcnow().isoformat(),
        }