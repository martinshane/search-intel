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
                "DataForSEO credentials not provided. Set DATAFORSEO_LOGIN and "
                "DATAFORSEO_PASSWORD environment variables or pass them to constructor."
            )
        
        self.timeout = timeout
        self.max_retries = max_retries
        self.supabase = supabase_client
        self.cache_ttl_hours = cache_ttl_hours
        self.rate_limit_per_second = rate_limit_per_second
        
        # Rate limiting
        self._last_request_time = 0.0
        self._request_lock = asyncio.Lock()
        
        # HTTP client (initialized on first request)
        self._client: Optional[httpx.AsyncClient] = None
    
    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client with authentication."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                auth=(self.login, self.password),
                timeout=self.timeout,
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "SearchIntelligenceReport/1.0",
                },
            )
        return self._client
    
    async def close(self):
        """Close HTTP client connection."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
    
    async def __aenter__(self):
        """Async context manager entry."""
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
    
    async def _rate_limit(self):
        """Enforce rate limiting."""
        async with self._request_lock:
            now = asyncio.get_event_loop().time()
            time_since_last = now - self._last_request_time
            min_interval = 1.0 / self.rate_limit_per_second
            
            if time_since_last < min_interval:
                await asyncio.sleep(min_interval - time_since_last)
            
            self._last_request_time = asyncio.get_event_loop().time()
    
    def _generate_cache_key(self, endpoint: str, params: Dict[str, Any]) -> str:
        """Generate cache key from endpoint and parameters."""
        key_str = f"{endpoint}:{json.dumps(params, sort_keys=True)}"
        return hashlib.md5(key_str.encode()).hexdigest()
    
    async def _get_cached_response(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """Retrieve cached response from Supabase if available."""
        if not self.supabase:
            return None
        
        try:
            cutoff_time = datetime.utcnow() - timedelta(hours=self.cache_ttl_hours)
            
            result = self.supabase.table("api_cache").select("*").eq(
                "cache_key", cache_key
            ).gte("created_at", cutoff_time.isoformat()).execute()
            
            if result.data and len(result.data) > 0:
                logger.info(f"Cache hit for key: {cache_key}")
                return result.data[0]["response_data"]
        except Exception as e:
            logger.warning(f"Failed to retrieve cached response: {e}")
        
        return None
    
    async def _cache_response(self, cache_key: str, response: Dict[str, Any]):
        """Cache response to Supabase."""
        if not self.supabase:
            return
        
        try:
            self.supabase.table("api_cache").upsert({
                "cache_key": cache_key,
                "response_data": response,
                "created_at": datetime.utcnow().isoformat(),
                "source": "dataforseo",
            }).execute()
            logger.info(f"Cached response for key: {cache_key}")
        except Exception as e:
            logger.warning(f"Failed to cache response: {e}")
    
    @retry(
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.NetworkError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
    )
    async def _make_request(
        self,
        endpoint: str,
        data: Optional[List[Dict[str, Any]]] = None,
        use_cache: bool = True,
    ) -> Dict[str, Any]:
        """
        Make authenticated request to DataForSEO API with rate limiting and caching.
        
        Args:
            endpoint: API endpoint path (e.g., "/serp/google/organic/live/advanced")
            data: Request payload
            use_cache: Whether to use cached responses
        
        Returns:
            API response dictionary
        
        Raises:
            DataForSEOError: On API errors
            DataForSEORateLimitError: On rate limit exceeded
            DataForSEOAuthError: On authentication failure
        """
        url = f"{self.BASE_URL}{endpoint}"
        
        # Check cache first
        if use_cache and data:
            cache_key = self._generate_cache_key(endpoint, data[0] if data else {})
            cached = await self._get_cached_response(cache_key)
            if cached:
                return cached
        
        # Rate limiting
        await self._rate_limit()
        
        # Make request
        client = await self._get_client()
        
        try:
            if data:
                response = await client.post(url, json=data)
            else:
                response = await client.get(url)
            
            response.raise_for_status()
            
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                raise DataForSEOAuthError("Authentication failed. Check credentials.")
            elif e.response.status_code == 429:
                raise DataForSEORateLimitError("Rate limit exceeded. Retry later.")
            else:
                raise DataForSEOError(f"HTTP {e.response.status_code}: {e.response.text}")
        
        result = response.json()
        
        # DataForSEO API structure: {"status_code": 20000, "tasks": [...]}
        if result.get("status_code") != 20000:
            error_msg = result.get("status_message", "Unknown error")
            raise DataForSEOError(f"API error: {error_msg}")
        
        # Cache successful response
        if use_cache and data:
            await self._cache_response(cache_key, result)
        
        return result
    
    async def authenticate(self) -> bool:
        """
        Test authentication by making a simple API call.
        
        Returns:
            True if authentication successful
        
        Raises:
            DataForSEOAuthError: If authentication fails
        """
        try:
            await self._make_request("/serp/google/locations", use_cache=False)
            logger.info("DataForSEO authentication successful")
            return True
        except DataForSEOAuthError:
            raise
        except Exception as e:
            raise DataForSEOAuthError(f"Authentication test failed: {e}")
    
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
            keywords: List of search keywords
            location_code: DataForSEO location code (2840 = US)
            language_code: Language code (e.g., "en")
            device: Device type ("desktop", "mobile")
            depth: Number of results to fetch (max 100)
            use_cache: Whether to use cached responses
        
        Returns:
            List of standardized SERP result dictionaries, one per keyword
        
        Example:
            >>> results = await client.fetch_serp_results(
            ...     keywords=["best crm", "crm software"],
            ...     location_code=2840
            ... )
            >>> for result in results:
            ...     print(result["keyword"], result["organic_results"][:3])
        """
        tasks = []
        for keyword in keywords:
            task_data = {
                "keyword": keyword,
                "location_code": location_code,
                "language_code": language_code,
                "device": device,
                "depth": depth,
                "calculate_rectangles": True,  # For visual position calculation
            }
            tasks.append(task_data)
        
        response = await self._make_request(
            "/serp/google/organic/live/advanced",
            data=tasks,
            use_cache=use_cache,
        )
        
        results = []
        for task in response.get("tasks", []):
            if task["status_code"] != 20000:
                logger.warning(f"Task failed: {task.get('status_message')}")
                continue
            
            for result_item in task.get("result", []):
                parsed = self._parse_serp_result(result_item)
                results.append(parsed)
        
        return results
    
    def _parse_serp_result(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse and standardize SERP result data.
        
        Returns standardized structure for modules 3, 8, and 11.
        """
        keyword = result.get("keyword", "")
        items = result.get("items", [])
        
        # Extract organic results
        organic_results = []
        for item in items:
            if item.get("type") == "organic":
                organic_results.append({
                    "position": item.get("rank_group", 0),
                    "url": item.get("url", ""),
                    "domain": item.get("domain", ""),
                    "title": item.get("title", ""),
                    "description": item.get("description", ""),
                    "breadcrumb": item.get("breadcrumb", ""),
                })
        
        # Detect SERP features
        serp_features = self._detect_serp_features(items)
        
        # Calculate visual position adjustments
        visual_adjustments = self._calculate_visual_positions(items)
        
        # Extract competitor domains
        competitor_domains = self._extract_competitor_domains(organic_results)
        
        return {
            "keyword": keyword,
            "organic_results": organic_results,
            "serp_features": serp_features,
            "visual_adjustments": visual_adjustments,
            "competitor_domains": competitor_domains,
            "total_results": result.get("items_count", 0),
            "fetch_time": datetime.utcnow().isoformat(),
        }
    
    def _detect_serp_features(self, items: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Detect presence and characteristics of SERP features.
        
        Returns:
            Dictionary mapping feature types to their details
        """
        features = {
            "featured_snippet": None,
            "people_also_ask": [],
            "knowledge_graph": None,
            "local_pack": None,
            "video": [],
            "image": None,
            "shopping": [],
            "top_stories": [],
            "ai_overview": None,
            "related_searches": [],
        }
        
        for item in items:
            item_type = item.get("type", "")
            
            if item_type == "featured_snippet":
                features["featured_snippet"] = {
                    "title": item.get("title", ""),
                    "description": item.get("description", ""),
                    "url": item.get("url", ""),
                    "domain": item.get("domain", ""),
                }
            
            elif item_type == "people_also_ask":
                for expanded_item in item.get("items", []):
                    features["people_also_ask"].append({
                        "question": expanded_item.get("title", ""),
                        "answer": expanded_item.get("description", ""),
                        "url": expanded_item.get("url", ""),
                    })
            
            elif item_type == "knowledge_graph":
                features["knowledge_graph"] = {
                    "title": item.get("title", ""),
                    "description": item.get("description", ""),
                    "type": item.get("card_id", ""),
                }
            
            elif item_type == "local_pack":
                features["local_pack"] = {
                    "count": len(item.get("items", [])),
                }
            
            elif item_type == "video":
                features["video"].append({
                    "title": item.get("title", ""),
                    "source": item.get("source", ""),
                    "url": item.get("url", ""),
                })
            
            elif item_type == "images":
                features["image"] = {
                    "count": len(item.get("items", [])),
                }
            
            elif item_type in ["shopping", "google_shopping"]:
                features["shopping"].append({
                    "title": item.get("title", ""),
                    "price": item.get("price", ""),
                })
            
            elif item_type == "top_stories":
                for story in item.get("items", []):
                    features["top_stories"].append({
                        "title": story.get("title", ""),
                        "source": story.get("source", ""),
                        "url": story.get("url", ""),
                    })
            
            elif item_type == "ai_overview":
                features["ai_overview"] = {
                    "text": item.get("text", ""),
                }
            
            elif item_type in ["people_also_search", "related_searches"]:
                for search in item.get("items", []):
                    features["related_searches"].append(search.get("title", ""))
        
        return features
    
    def _calculate_visual_positions(self, items: List[Dict[str, Any]]) -> Dict[int, int]:
        """
        Calculate visual position adjustments based on SERP features.
        
        Returns:
            Dictionary mapping organic rank_group to visual position
            (accounting for SERP features appearing above)
        """
        adjustments = {}
        visual_offset = 0
        
        for item in items:
            item_type = item.get("type", "")
            rank_group = item.get("rank_group", 0)
            
            # Adjust visual offset based on feature type
            if item_type == "featured_snippet":
                visual_offset += 2  # Featured snippets push down ~2 positions
            elif item_type == "people_also_ask":
                # Each PAA question adds ~0.5 visual positions
                paa_count = len(item.get("items", []))
                visual_offset += paa_count * 0.5
            elif item_type == "knowledge_graph":
                visual_offset += 1.5
            elif item_type == "local_pack":
                visual_offset += 3  # Local pack typically shows 3 results
            elif item_type == "video":
                visual_offset += 1
            elif item_type == "images":
                visual_offset += 0.5
            elif item_type == "shopping":
                visual_offset += 1
            elif item_type == "top_stories":
                visual_offset += 3
            elif item_type == "ai_overview":
                visual_offset += 2
            
            # Map organic positions to visual positions
            if item_type == "organic" and rank_group > 0:
                adjustments[rank_group] = rank_group + int(visual_offset)
        
        return adjustments
    
    def _extract_competitor_domains(
        self, organic_results: List[Dict[str, Any]]
    ) -> List[str]:
        """Extract unique competitor domains from organic results."""
        domains = []
        for result in organic_results:
            domain = result.get("domain", "")
            if domain and domain not in domains:
                domains.append(domain)
        return domains
    
    async def analyze_competitor_rankings(
        self,
        keywords: List[str],
        user_domain: str,
        location_code: int = 2840,
        language_code: str = "en",
        use_cache: bool = True,
    ) -> Dict[str, Any]:
        """
        Analyze competitor domain rankings across keyword set.
        
        Args:
            keywords: List of keywords to analyze
            user_domain: User's domain (to exclude from competitors)
            location_code: DataForSEO location code
            language_code: Language code
            use_cache: Whether to use cached responses
        
        Returns:
            Dictionary with competitor analysis:
            {
                "competitors": [
                    {
                        "domain": "competitor.com",
                        "keywords_shared": 15,
                        "avg_position": 4.2,
                        "keywords": ["keyword1", "keyword2", ...],
                        "threat_level": "high"
                    }
                ],
                "total_keywords_analyzed": 50
            }
        """
        # Fetch SERP results for all keywords
        serp_results = await self.fetch_serp_results(
            keywords=keywords,
            location_code=location_code,
            language_code=language_code,
            use_cache=use_cache,
        )
        
        # Build competitor frequency matrix
        competitor_data: Dict[str, Dict[str, Any]] = {}
        
        for result in serp_results:
            keyword = result["keyword"]
            for domain in result["competitor_domains"]:
                # Skip user's own domain
                if domain.lower() == user_domain.lower():
                    continue
                
                if domain not in competitor_data:
                    competitor_data[domain] = {
                        "keywords": [],
                        "positions": [],
                    }
                
                competitor_data[domain]["keywords"].append(keyword)
                
                # Find position for this domain
                for org_result in result["organic_results"]:
                    if org_result["domain"] == domain:
                        competitor_data[domain]["positions"].append(
                            org_result["position"]
                        )
                        break
        
        # Calculate metrics for each competitor
        competitors = []
        for domain, data in competitor_data.items():
            keywords_shared = len(data["keywords"])
            avg_position = (
                sum(data["positions"]) / len(data["positions"])
                if data["positions"] else 0
            )
            
            # Classify threat level
            threat_level = "low"
            if keywords_shared > len(keywords) * 0.5:
                threat_level = "high"
            elif keywords_shared > len(keywords) * 0.2:
                threat_level = "medium"
            
            competitors.append({
                "domain": domain,
                "keywords_shared": keywords_shared,
                "avg_position": round(avg_position, 2),
                "keywords": data["keywords"],
                "threat_level": threat_level,
            })
        
        # Sort by keywords_shared DESC
        competitors.sort(key=lambda x: x["keywords_shared"], reverse=True)
        
        return {
            "competitors": competitors,
            "total_keywords_analyzed": len(keywords),
        }
    
    def calculate_ctr_with_features(
        self,
        position: int,
        serp_features: Dict[str, Any],
        visual_position: Optional[int] = None,
    ) -> float:
        """
        Calculate estimated CTR for a position, adjusted for SERP features.
        
        Args:
            position: Organic position (1-20)
            serp_features: Dictionary of SERP features present
            visual_position: Calculated visual position (if different from organic)
        
        Returns:
            Estimated CTR as decimal (e.g., 0.15 = 15%)
        """
        # Start with baseline CTR
        base_ctr = self.BASELINE_CTR_CURVE.get(position, 0.005)
        
        # If visual position is significantly lower, use that as base
        if visual_position and visual_position > position + 2:
            base_ctr = self.BASELINE_CTR_CURVE.get(
                min(visual_position, 20), 0.005
            )
        
        # Apply feature-based adjustments
        ctr_multiplier = 1.0
        
        # Featured snippet reduces CTR for position 1
        if serp_features.get("featured_snippet") and position == 1:
            ctr_multiplier *= 0.6
        
        # PAA reduces CTR moderately
        paa_count = len(serp_features.get("people_also_ask", []))
        if paa_count > 0:
            ctr_multiplier *= (1.0 - (paa_count * 0.05))
        
        # Knowledge graph reduces CTR significantly
        if serp_features.get("knowledge_graph"):
            ctr_multiplier *= 0.7
        
        # Local pack dominates for local queries
        if serp_features.get("local_pack") and position <= 5:
            ctr_multiplier *= 0.5
        
        # Video carousel reduces organic CTR
        if serp_features.get("video"):
            ctr_multiplier *= 0.8
        
        # AI overview significantly reduces CTR
        if serp_features.get("ai_overview"):
            ctr_multiplier *= 0.4
        
        return max(base_ctr * ctr_multiplier, 0.001)  # Floor at 0.1%
    
    async def get_organic_ctr_data(
        self,
        keywords: List[str],
        user_domain: str,
        location_code: int = 2840,
        language_code: str = "en",
        use_cache: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Get organic CTR estimates for user's rankings on given keywords.
        
        Args:
            keywords: List of keywords to analyze
            user_domain: User's domain
            location_code: DataForSEO location code
            language_code: Language code
            use_cache: Whether to use cached responses
        
        Returns:
            List of CTR data dictionaries:
            [
                {
                    "keyword": "best crm",
                    "position": 3,
                    "visual_position": 8,
                    "estimated_ctr": 0.045,
                    "estimated_ctr_without_features": 0.105,
                    "ctr_impact": -0.060,
                    "serp_features_present": ["featured_snippet", "people_also_ask"],
                    "url": "https://example.com/page"
                }
            ]
        """
        serp_results = await self.fetch_serp_results(
            keywords=keywords,
            location_code=location_code,
            language_code=language_code,
            use_cache=use_cache,
        )
        
        ctr_data = []
        
        for result in serp_results:
            keyword = result["keyword"]
            serp_features = result["serp_features"]
            visual_adjustments = result["visual_adjustments"]
            
            # Find user's position
            user_position = None
            user_url = None
            
            for org_result in result["organic_results"]:
                if org_result["domain"].lower() == user_domain.lower():
                    user_position = org_result["position"]
                    user_url = org_result["url"]
                    break
            
            if user_position is None:
                # User not ranking for this keyword
                continue
            
            visual_position = visual_adjustments.get(user_position, user_position)
            
            # Calculate CTR with and without features
            estimated_ctr = self.calculate_ctr_with_features(
                user_position, serp_features, visual_position
            )
            baseline_ctr = self.BASELINE_CTR_CURVE.get(user_position, 0.005)
            
            # List active features
            active_features = []
            for feature_type, feature_data in serp_features.items():
                if feature_data:  # Not None or empty list/dict
                    active_features.append(feature_type)
            
            ctr_data.append({
                "keyword": keyword,
                "position": user_position,
                "visual_position": visual_position,
                "estimated_ctr": round(estimated_ctr, 4),
                "estimated_ctr_without_features": round(baseline_ctr, 4),
                "ctr_impact": round(estimated_ctr - baseline_ctr, 4),
                "serp_features_present": active_features,
                "url": user_url,
            })
        
        return ctr_data
    
    async def batch_keyword_analysis(
        self,
        keywords: List[str],
        user_domain: str,
        location_code: int = 2840,
        language_code: str = "en",
        batch_size: int = 10,
        use_cache: bool = True,
    ) -> Dict[str, Any]:
        """
        Comprehensive batch analysis of keywords for modules 3, 8, and 11.
        
        Combines SERP results, competitor analysis, and CTR estimates into
        a single standardized output.
        
        Args:
            keywords: List of keywords to analyze
            user_domain: User's domain
            location_code: DataForSEO location code
            language_code: Language code
            batch_size: Number of keywords to process per batch
            use_cache: Whether to use cached responses
        
        Returns:
            Comprehensive analysis dictionary with all data needed for
            Module 3 (SERP landscape), Module 8 (CTR), and Module 11 (competitors)
        """
        all_results = []
        
        # Process in batches to avoid overwhelming the API
        for i in range(0, len(keywords), batch_size):
            batch = keywords[i:i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}: {len(batch)} keywords")
            
            batch_results = await self.fetch_serp_results(
                keywords=batch,
                location_code=location_code,
                language_code=language_code,
                use_cache=use_cache,
            )
            all_results.extend(batch_results)
            
            # Rate limiting between batches
            if i + batch_size < len(keywords):
                await asyncio.sleep(1)
        
        # Competitor analysis
        competitor_analysis = await self.analyze_competitor_rankings(
            keywords=keywords,
            user_domain=user_domain,
            location_code=location_code,
            language_code=language_code,
            use_cache=use_cache,
        )
        
        # CTR data
        ctr_data = await self.get_organic_ctr_data(
            keywords=keywords,
            user_domain=user_domain,
            location_code=location_code,
            language_code=language_code,
            use_cache=use_cache,
        )
        
        # Aggregate SERP feature statistics
        feature_stats = self._aggregate_feature_stats(all_results)
        
        # Calculate overall click share
        click_share_data = self._calculate_click_share(all_results, user_domain)
        
        return {
            "serp_results": all_results,
            "competitor_analysis": competitor_analysis,
            "ctr_data": ctr_data,
            "feature_statistics": feature_stats,
            "click_share": click_share_data,
            "keywords_analyzed": len(keywords),
            "keywords_ranking": len(ctr_data),
            "analysis_timestamp": datetime.utcnow().isoformat(),
        }
    
    def _aggregate_feature_stats(
        self, serp_results: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Aggregate SERP feature presence statistics across all keywords."""
        feature_counts = {
            feature: 0 for feature in self.SERP_FEATURE_TYPES.keys()
        }
        total = len(serp_results)
        
        for result in serp_results:
            features = result["serp_features"]
            for feature_type, feature_data in features.items():
                if feature_data:  # Present
                    feature_counts[feature_type] += 1
        
        # Calculate percentages
        feature_percentages = {
            feature: round((count / total) * 100, 1)
            for feature, count in feature_counts.items()
        }
        
        return {
            "counts": feature_counts,
            "percentages": feature_percentages,
            "total_keywords": total,
        }
    
    def _calculate_click_share(
        self, serp_results: List[Dict[str, Any]], user_domain: str
    ) -> Dict[str, Any]:
        """Calculate estimated click share across keyword portfolio."""
        total_clicks_available = 0.0
        total_clicks_captured = 0.0
        
        for result in serp_results:
            serp_features = result["serp_features"]
            
            # Estimate total clicks available for this keyword
            # (sum of CTRs for all positions, adjusted for features)
            available = 0.0
            captured = 0.0
            
            for position in range(1, 11):  # Top 10 positions
                ctr = self.calculate_ctr_with_features(
                    position, serp_features, None
                )
                available += ctr
                
                # Check if user ranks at this position
                for org_result in result["organic_results"]:
                    if (org_result["domain"].lower() == user_domain.lower() and
                        org_result["position"] == position):
                        captured += ctr
            
            total_clicks_available += available
            total_clicks_captured += captured
        
        click_share = (
            (total_clicks_captured / total_clicks_available)
            if total_clicks_available > 0 else 0.0
        )
        
        return {
            "click_share": round(click_share, 4),
            "clicks_captured": round(total_clicks_captured, 4),
            "clicks_available": round(total_clicks_available, 4),
            "opportunity": round(total_clicks_available - total_clicks_captured, 4),
        }