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
                "Set DATAFORSEO_LOGIN and DATAFORSEO_PASSWORD environment variables "
                "or pass them to the constructor."
            )
        
        self.timeout = timeout
        self.max_retries = max_retries
        self.supabase = supabase_client
        self.cache_ttl_hours = cache_ttl_hours
        
        # Rate limiting: DataForSEO allows 2000 requests/minute
        self.rate_limit_requests = 2000
        self.rate_limit_window = 60  # seconds
        self.request_timestamps: List[float] = []
        
        # HTTP client
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
        Initialize HTTP client with authentication.
        
        Raises:
            DataForSEOAuthError: If authentication fails
        """
        try:
            self.client = httpx.AsyncClient(
                auth=(self.login, self.password),
                timeout=self.timeout,
                headers={
                    "Content-Type": "application/json",
                },
            )
            
            # Test authentication with a simple ping
            response = await self.client.get(f"{self.BASE_URL}/serp/google/organic/live/advanced")
            if response.status_code == 401:
                raise DataForSEOAuthError("Invalid DataForSEO credentials")
            
            logger.info("DataForSEO client authenticated successfully")
            
        except httpx.HTTPError as e:
            raise DataForSEOAuthError(f"Authentication failed: {str(e)}")
    
    async def close(self):
        """Close HTTP client"""
        if self.client:
            await self.client.aclose()
            self.client = None
    
    def _check_rate_limit(self):
        """
        Check if we're within rate limits.
        
        Raises:
            DataForSEORateLimitError: If rate limit is exceeded
        """
        now = asyncio.get_event_loop().time()
        
        # Remove timestamps outside the current window
        self.request_timestamps = [
            ts for ts in self.request_timestamps
            if now - ts < self.rate_limit_window
        ]
        
        if len(self.request_timestamps) >= self.rate_limit_requests:
            raise DataForSEORateLimitError(
                f"Rate limit exceeded: {self.rate_limit_requests} requests "
                f"per {self.rate_limit_window} seconds"
            )
        
        self.request_timestamps.append(now)
    
    def _generate_cache_key(self, endpoint: str, params: Dict[str, Any]) -> str:
        """
        Generate a cache key for a request.
        
        Args:
            endpoint: API endpoint
            params: Request parameters
            
        Returns:
            Cache key string
        """
        # Sort params for consistent hashing
        params_str = json.dumps(params, sort_keys=True)
        hash_input = f"{endpoint}:{params_str}"
        return hashlib.sha256(hash_input.encode()).hexdigest()
    
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
            result = self.supabase.table("dataforseo_cache").select("*").eq(
                "cache_key", cache_key
            ).single().execute()
            
            if result.data:
                cached_at = datetime.fromisoformat(result.data["cached_at"])
                expires_at = cached_at + timedelta(hours=self.cache_ttl_hours)
                
                if datetime.utcnow() < expires_at:
                    logger.info(f"Cache hit for key: {cache_key}")
                    return result.data["response_data"]
                else:
                    logger.info(f"Cache expired for key: {cache_key}")
                    # Delete expired cache entry
                    self.supabase.table("dataforseo_cache").delete().eq(
                        "cache_key", cache_key
                    ).execute()
            
        except Exception as e:
            logger.warning(f"Cache retrieval failed: {str(e)}")
        
        return None
    
    async def _cache_response(self, cache_key: str, response_data: Dict[str, Any]):
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
            logger.warning(f"Cache storage failed: {str(e)}")
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((httpx.HTTPError, DataForSEORateLimitError)),
    )
    async def _make_request(
        self,
        endpoint: str,
        payload: List[Dict[str, Any]],
        use_cache: bool = True,
    ) -> Dict[str, Any]:
        """
        Make a request to DataForSEO API with retries and caching.
        
        Args:
            endpoint: API endpoint path (without base URL)
            payload: Request payload
            use_cache: Whether to use caching
            
        Returns:
            API response data
            
        Raises:
            DataForSEOError: If request fails after retries
        """
        if not self.client:
            await self.authenticate()
        
        # Check cache if enabled
        cache_key = self._generate_cache_key(endpoint, payload[0]) if use_cache else None
        if cache_key:
            cached = await self._get_cached_response(cache_key)
            if cached:
                return cached
        
        # Check rate limit
        self._check_rate_limit()
        
        try:
            url = f"{self.BASE_URL}/{endpoint}"
            response = await self.client.post(url, json=payload)
            
            # Handle different response codes
            if response.status_code == 401:
                raise DataForSEOAuthError("Authentication failed")
            elif response.status_code == 429:
                raise DataForSEORateLimitError("Rate limit exceeded")
            elif response.status_code >= 500:
                raise DataForSEOError(f"Server error: {response.status_code}")
            
            response.raise_for_status()
            
            data = response.json()
            
            # Check for API-level errors
            if data.get("status_code") != 20000:
                error_msg = data.get("status_message", "Unknown error")
                raise DataForSEOError(f"API error: {error_msg}")
            
            # Cache successful response
            if cache_key:
                await self._cache_response(cache_key, data)
            
            return data
            
        except httpx.HTTPError as e:
            logger.error(f"HTTP error in DataForSEO request: {str(e)}")
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
            keywords: List of keywords to fetch results for
            location_code: DataForSEO location code (2840 = United States)
            language_code: Language code (e.g., "en")
            device: Device type ("desktop" or "mobile")
            depth: Number of results to retrieve (max 100)
            use_cache: Whether to use caching
            
        Returns:
            List of SERP result dictionaries, one per keyword
            
        Example response structure:
            [
                {
                    "keyword": "best crm software",
                    "location_code": 2840,
                    "language_code": "en",
                    "check_url": "https://www.google.com/search?q=...",
                    "datetime": "2025-01-10 12:34:56 +00:00",
                    "items": [
                        {
                            "type": "organic",
                            "rank_group": 1,
                            "rank_absolute": 1,
                            "position": "left",
                            "url": "https://example.com/page",
                            "domain": "example.com",
                            "title": "Page Title",
                            "description": "Meta description...",
                            ...
                        },
                        {
                            "type": "featured_snippet",
                            ...
                        }
                    ],
                    "serp_features": ["featured_snippet", "people_also_ask", ...],
                }
            ]
        """
        endpoint = "serp/google/organic/live/advanced"
        
        # Build payload for all keywords
        payload = []
        for keyword in keywords:
            payload.append({
                "keyword": keyword,
                "location_code": location_code,
                "language_code": language_code,
                "device": device,
                "depth": depth,
            })
        
        try:
            response = await self._make_request(endpoint, payload, use_cache)
            
            # Parse and structure results
            results = []
            for task in response.get("tasks", []):
                if task.get("status_code") == 20000 and task.get("result"):
                    result_data = task["result"][0]
                    
                    # Extract SERP features present
                    serp_features = self._extract_serp_features(result_data.get("items", []))
                    
                    results.append({
                        "keyword": result_data.get("keyword"),
                        "location_code": result_data.get("location_code"),
                        "language_code": result_data.get("language_code"),
                        "check_url": result_data.get("check_url"),
                        "datetime": result_data.get("datetime"),
                        "items": result_data.get("items", []),
                        "serp_features": serp_features,
                    })
                else:
                    logger.warning(
                        f"Task failed for keyword: {task.get('data', {}).get('keyword')}"
                    )
            
            return results
            
        except Exception as e:
            logger.error(f"Failed to fetch SERP results: {str(e)}")
            raise
    
    def _extract_serp_features(self, items: List[Dict[str, Any]]) -> List[str]:
        """
        Extract SERP features present in the results.
        
        Args:
            items: List of SERP items
            
        Returns:
            List of SERP feature names
        """
        features = set()
        
        for item in items:
            item_type = item.get("type", "")
            
            # Map item types to our standardized feature names
            for feature_name, type_variants in self.SERP_FEATURE_TYPES.items():
                if item_type in type_variants:
                    features.add(feature_name)
        
        return sorted(list(features))
    
    async def analyze_serp_features(
        self,
        serp_results: List[Dict[str, Any]],
        user_domain: str,
    ) -> Dict[str, Any]:
        """
        Analyze SERP features and their impact on organic visibility.
        
        Args:
            serp_results: List of SERP results from fetch_serp_results()
            user_domain: User's domain to analyze positioning
            
        Returns:
            Analysis dictionary with:
                - Feature presence statistics
                - Visual position displacement analysis
                - Click share estimates
        """
        analysis = {
            "keywords_analyzed": len(serp_results),
            "feature_frequency": {},
            "displacement_analysis": [],
            "total_estimated_ctr_loss": 0.0,
        }
        
        # Feature presence counts
        for feature_name in self.SERP_FEATURE_TYPES.keys():
            analysis["feature_frequency"][feature_name] = 0
        
        for result in serp_results:
            keyword = result["keyword"]
            features = result["serp_features"]
            
            # Count feature occurrences
            for feature in features:
                if feature in analysis["feature_frequency"]:
                    analysis["feature_frequency"][feature] += 1
            
            # Find user's organic position
            user_position = None
            user_item = None
            
            for item in result["items"]:
                if item.get("type") == "organic":
                    domain = item.get("domain", "")
                    if domain == user_domain or user_domain in domain:
                        user_position = item.get("rank_absolute")
                        user_item = item
                        break
            
            if user_position:
                # Calculate visual displacement
                visual_position = self._calculate_visual_position(
                    result["items"],
                    user_position
                )
                
                displacement = visual_position - user_position
                
                if displacement > 3:  # Significant displacement threshold
                    # Features above user's listing
                    features_above = []
                    for item in result["items"]:
                        if item.get("rank_absolute", 999) < user_position:
                            item_type = item.get("type")
                            if item_type != "organic":
                                features_above.append(item_type)
                    
                    # Estimate CTR impact
                    # Position-based CTR curve approximation
                    expected_ctr = self._estimate_ctr(user_position)
                    actual_ctr = self._estimate_ctr(visual_position)
                    ctr_loss = expected_ctr - actual_ctr
                    
                    analysis["displacement_analysis"].append({
                        "keyword": keyword,
                        "organic_position": user_position,
                        "visual_position": visual_position,
                        "displacement": displacement,
                        "features_above": features_above,
                        "estimated_ctr_loss": round(ctr_loss, 4),
                    })
                    
                    analysis["total_estimated_ctr_loss"] += ctr_loss
        
        # Convert feature frequency to percentages
        total_keywords = len(serp_results)
        for feature in analysis["feature_frequency"]:
            count = analysis["feature_frequency"][feature]
            analysis["feature_frequency"][feature] = {
                "count": count,
                "percentage": round(count / total_keywords * 100, 1) if total_keywords > 0 else 0,
            }
        
        return analysis
    
    def _calculate_visual_position(
        self,
        items: List[Dict[str, Any]],
        organic_position: int,
    ) -> int:
        """
        Calculate visual position accounting for SERP features.
        
        Args:
            items: SERP items list
            organic_position: Organic rank position
            
        Returns:
            Visual position (how far down the page the result appears)
        """
        # Weight different SERP features by their visual impact
        feature_weights = {
            "featured_snippet": 2.0,
            "knowledge_graph": 1.5,
            "people_also_ask": 0.5,  # per question, will multiply by count
            "local_pack": 2.0,
            "video": 1.0,
            "image": 0.5,
            "shopping": 1.5,
            "top_stories": 1.0,
            "ai_overview": 2.5,
        }
        
        displacement = 0.0
        
        for item in items:
            item_position = item.get("rank_absolute", 999)
            
            # Only count items above the user's position
            if item_position < organic_position:
                item_type = item.get("type", "")
                
                if item_type == "organic":
                    displacement += 1.0
                else:
                    # Check if it matches a feature we track
                    for feature_name, type_variants in self.SERP_FEATURE_TYPES.items():
                        if item_type in type_variants:
                            weight = feature_weights.get(feature_name, 1.0)
                            
                            # PAA boxes can have multiple questions
                            if feature_name == "people_also_ask":
                                paa_count = len(item.get("items", []))
                                displacement += weight * paa_count
                            else:
                                displacement += weight
                            break
        
        return int(organic_position + displacement)
    
    def _estimate_ctr(self, position: int) -> float:
        """
        Estimate CTR based on position using empirical data.
        
        Args:
            position: SERP position
            
        Returns:
            Estimated CTR as decimal (e.g., 0.28 for 28%)
        """
        # Based on Advanced Web Ranking CTR study
        # Desktop, all industries average
        ctr_curve = {
            1: 0.281,
            2: 0.143,
            3: 0.099,
            4: 0.072,
            5: 0.054,
            6: 0.042,
            7: 0.034,
            8: 0.028,
            9: 0.024,
            10: 0.021,
        }
        
        if position in ctr_curve:
            return ctr_curve[position]
        elif position < 1:
            return ctr_curve[1]
        elif position > 10:
            # Exponential decay after position 10
            return 0.021 * (0.8 ** (position - 10))
        else:
            return 0.0
    
    async def analyze_competitors(
        self,
        serp_results: List[Dict[str, Any]],
        user_domain: str,
        min_keyword_overlap: int = 3,
    ) -> Dict[str, Any]:
        """
        Analyze competitor domains across the keyword set.
        
        Args:
            serp_results: List of SERP results from fetch_serp_results()
            user_domain: User's domain
            min_keyword_overlap: Minimum keywords a competitor must appear in
            
        Returns:
            Competitor analysis with:
                - Top competitors by keyword overlap
                - Average positions
                - Threat assessment
        """
        # Track competitor appearances
        competitor_data = {}
        
        for result in serp_results:
            keyword = result["keyword"]
            
            # Extract top 10 organic results
            organic_items = [
                item for item in result["items"]
                if item.get("type") == "organic"
            ][:10]
            
            for item in organic_items:
                domain = item.get("domain", "")
                position = item.get("rank_absolute")
                
                # Skip user's own domain
                if domain == user_domain or user_domain in domain:
                    continue
                
                if domain not in competitor_data:
                    competitor_data[domain] = {
                        "keywords": [],
                        "positions": [],
                    }
                
                competitor_data[domain]["keywords"].append(keyword)
                competitor_data[domain]["positions"].append(position)
        
        # Filter and rank competitors
        competitors = []
        
        for domain, data in competitor_data.items():
            keyword_count = len(data["keywords"])
            
            if keyword_count >= min_keyword_overlap:
                avg_position = sum(data["positions"]) / len(data["positions"])
                
                # Threat level based on keyword overlap and average position
                overlap_pct = keyword_count / len(serp_results) * 100
                
                if overlap_pct > 50 and avg_position < 5:
                    threat_level = "high"
                elif overlap_pct > 30 and avg_position < 8:
                    threat_level = "medium"
                else:
                    threat_level = "low"
                
                competitors.append({
                    "domain": domain,
                    "keywords_shared": keyword_count,
                    "overlap_percentage": round(overlap_pct, 1),
                    "avg_position": round(avg_position, 1),
                    "best_position": min(data["positions"]),
                    "threat_level": threat_level,
                    "sample_keywords": data["keywords"][:5],
                })
        
        # Sort by keyword overlap descending
        competitors.sort(key=lambda x: x["keywords_shared"], reverse=True)
        
        return {
            "total_competitors": len(competitors),
            "competitors": competitors,
            "analysis_summary": {
                "high_threat": len([c for c in competitors if c["threat_level"] == "high"]),
                "medium_threat": len([c for c in competitors if c["threat_level"] == "medium"]),
                "low_threat": len([c for c in competitors if c["threat_level"] == "low"]),
            },
        }
    
    async def track_keyword_positions(
        self,
        keywords: List[str],
        user_domain: str,
        location_code: int = 2840,
        language_code: str = "en",
        use_cache: bool = True,
    ) -> Dict[str, Any]:
        """
        Track keyword positions for a specific domain.
        
        Args:
            keywords: List of keywords to track
            user_domain: Domain to track positions for
            location_code: DataForSEO location code
            language_code: Language code
            use_cache: Whether to use caching
            
        Returns:
            Position tracking data with current rankings and metadata
        """
        # Fetch SERP results
        serp_results = await self.fetch_serp_results(
            keywords=keywords,
            location_code=location_code,
            language_code=language_code,
            use_cache=use_cache,
        )
        
        tracking_data = {
            "tracked_at": datetime.utcnow().isoformat(),
            "domain": user_domain,
            "location_code": location_code,
            "language_code": language_code,
            "keywords": [],
            "summary": {
                "total_keywords": len(keywords),
                "ranking_keywords": 0,
                "top_3": 0,
                "top_10": 0,
                "top_20": 0,
                "not_ranking": 0,
            },
        }
        
        for result in serp_results:
            keyword = result["keyword"]
            
            # Find user's position
            position = None
            url = None
            title = None
            
            for item in result["items"]:
                if item.get("type") == "organic":
                    domain = item.get("domain", "")
                    if domain == user_domain or user_domain in domain:
                        position = item.get("rank_absolute")
                        url = item.get("url")
                        title = item.get("title")
                        break
            
            keyword_data = {
                "keyword": keyword,
                "position": position,
                "url": url,
                "title": title,
                "serp_features": result["serp_features"],
            }
            
            tracking_data["keywords"].append(keyword_data)
            
            # Update summary
            if position:
                tracking_data["summary"]["ranking_keywords"] += 1
                if position <= 3:
                    tracking_data["summary"]["top_3"] += 1
                if position <= 10:
                    tracking_data["summary"]["top_10"] += 1
                if position <= 20:
                    tracking_data["summary"]["top_20"] += 1
            else:
                tracking_data["summary"]["not_ranking"] += 1
        
        return tracking_data
    
    async def batch_process_keywords(
        self,
        keywords: List[str],
        batch_size: int = 100,
        delay_between_batches: float = 1.0,
        **kwargs,
    ) -> List[Dict[str, Any]]:
        """
        Process keywords in batches to manage rate limits and costs.
        
        Args:
            keywords: Full list of keywords
            batch_size: Keywords per batch (DataForSEO accepts up to 100)
            delay_between_batches: Delay in seconds between batches
            **kwargs: Additional arguments to pass to fetch_serp_results()
            
        Returns:
            Combined results from all batches
        """
        all_results = []
        
        # Split into batches
        for i in range(0, len(keywords), batch_size):
            batch = keywords[i:i + batch_size]
            
            logger.info(
                f"Processing batch {i // batch_size + 1} "
                f"({len(batch)} keywords)"
            )
            
            try:
                batch_results = await self.fetch_serp_results(
                    keywords=batch,
                    **kwargs,
                )
                all_results.extend(batch_results)
                
            except Exception as e:
                logger.error(f"Batch processing failed: {str(e)}")
                # Continue with next batch rather than failing completely
                continue
            
            # Delay between batches if not the last one
            if i + batch_size < len(keywords):
                await asyncio.sleep(delay_between_batches)
        
        logger.info(f"Batch processing complete: {len(all_results)} results")
        return all_results
