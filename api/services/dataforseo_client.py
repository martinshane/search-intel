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
    
    # Rate limiting (requests per second)
    RATE_LIMIT = 2  # Conservative: 2 requests per second
    
    def __init__(
        self,
        login: Optional[str] = None,
        password: Optional[str] = None,
        cache_enabled: bool = True,
        cache_ttl_hours: int = 24,
    ):
        """
        Initialize DataForSEO client.
        
        Args:
            login: DataForSEO login (defaults to DATAFORSEO_LOGIN env var)
            password: DataForSEO password (defaults to DATAFORSEO_PASSWORD env var)
            cache_enabled: Whether to use response caching
            cache_ttl_hours: Cache TTL in hours
        """
        self.login = login or os.getenv("DATAFORSEO_LOGIN")
        self.password = password or os.getenv("DATAFORSEO_PASSWORD")
        
        if not self.login or not self.password:
            raise DataForSEOAuthError(
                "DataForSEO credentials not provided. Set DATAFORSEO_LOGIN and "
                "DATAFORSEO_PASSWORD environment variables or pass to constructor."
            )
        
        self.cache_enabled = cache_enabled
        self.cache_ttl = timedelta(hours=cache_ttl_hours)
        self.authenticated = False
        
        # Rate limiting
        self._last_request_time = None
        self._rate_limit_lock = asyncio.Lock()
        
        # HTTP client
        self.client: Optional[httpx.AsyncClient] = None
    
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
        Tests credentials by making a ping request.
        
        Returns:
            True if authentication successful
            
        Raises:
            DataForSEOAuthError: If authentication fails
        """
        if self.authenticated:
            return True
        
        # Initialize HTTP client
        self.client = httpx.AsyncClient(
            auth=(self.login, self.password),
            timeout=30.0,
            headers={"Content-Type": "application/json"},
        )
        
        try:
            # Test authentication with ping endpoint
            response = await self._make_request("GET", "/appendix/user_data")
            
            if response.get("status_code") == 20000:
                self.authenticated = True
                logger.info("DataForSEO authentication successful")
                
                # Log rate limits if available
                tasks = response.get("tasks", [])
                if tasks and len(tasks) > 0:
                    task_data = tasks[0].get("data", {})
                    rate_limits = task_data.get("limits", {})
                    logger.info(f"DataForSEO rate limits: {rate_limits}")
                
                return True
            else:
                raise DataForSEOAuthError(
                    f"Authentication failed: {response.get('status_message')}"
                )
        
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                raise DataForSEOAuthError("Invalid DataForSEO credentials")
            raise DataForSEOError(f"HTTP error during authentication: {e}")
        
        except Exception as e:
            raise DataForSEOError(f"Authentication failed: {e}")
    
    async def close(self):
        """Close HTTP client"""
        if self.client:
            await self.client.aclose()
            self.client = None
            self.authenticated = False
    
    async def _rate_limit(self):
        """Apply rate limiting between requests"""
        async with self._rate_limit_lock:
            if self._last_request_time:
                elapsed = (datetime.utcnow() - self._last_request_time).total_seconds()
                min_interval = 1.0 / self.RATE_LIMIT
                
                if elapsed < min_interval:
                    wait_time = min_interval - elapsed
                    await asyncio.sleep(wait_time)
            
            self._last_request_time = datetime.utcnow()
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.NetworkError)),
    )
    async def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """
        Make authenticated API request with rate limiting and retries.
        
        Args:
            method: HTTP method (GET, POST)
            endpoint: API endpoint path
            data: Request payload for POST requests
            
        Returns:
            Parsed JSON response
            
        Raises:
            DataForSEORateLimitError: If rate limit exceeded
            DataForSEOError: For other API errors
        """
        if not self.authenticated:
            await self.authenticate()
        
        # Apply rate limiting
        await self._rate_limit()
        
        url = f"{self.BASE_URL}{endpoint}"
        
        try:
            if method.upper() == "POST":
                response = await self.client.post(url, json=data)
            else:
                response = await self.client.get(url)
            
            response.raise_for_status()
            result = response.json()
            
            # Check DataForSEO status code
            status_code = result.get("status_code")
            
            if status_code == 20000:
                return result
            
            elif status_code == 40100:
                raise DataForSEOAuthError("Authentication failed")
            
            elif status_code == 50000:
                raise DataForSEORateLimitError("Rate limit exceeded")
            
            else:
                error_msg = result.get("status_message", "Unknown error")
                raise DataForSEOError(f"API error ({status_code}): {error_msg}")
        
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                raise DataForSEORateLimitError("Rate limit exceeded (HTTP 429)")
            raise DataForSEOError(f"HTTP error: {e}")
        
        except httpx.TimeoutException:
            logger.warning(f"Request timeout for {endpoint}, retrying...")
            raise
        
        except httpx.NetworkError as e:
            logger.warning(f"Network error for {endpoint}, retrying...")
            raise
    
    def _generate_cache_key(self, endpoint: str, params: Dict) -> str:
        """Generate cache key from endpoint and parameters"""
        param_str = json.dumps(params, sort_keys=True)
        combined = f"{endpoint}:{param_str}"
        return hashlib.sha256(combined.encode()).hexdigest()
    
    async def _get_cached_response(
        self,
        cache_key: str,
        supabase_client = None,
    ) -> Optional[Dict]:
        """
        Retrieve cached response from Supabase if available and not expired.
        
        Args:
            cache_key: Cache key
            supabase_client: Supabase client instance
            
        Returns:
            Cached response or None if not found/expired
        """
        if not self.cache_enabled or not supabase_client:
            return None
        
        try:
            result = supabase_client.table("api_cache").select("*").eq(
                "cache_key", cache_key
            ).single().execute()
            
            if result.data:
                cached_at = datetime.fromisoformat(result.data["cached_at"])
                
                if datetime.utcnow() - cached_at < self.cache_ttl:
                    logger.info(f"Cache hit for key {cache_key[:16]}...")
                    return result.data["response_data"]
                else:
                    logger.info(f"Cache expired for key {cache_key[:16]}...")
        
        except Exception as e:
            logger.warning(f"Cache retrieval failed: {e}")
        
        return None
    
    async def _cache_response(
        self,
        cache_key: str,
        response_data: Dict,
        supabase_client = None,
    ):
        """
        Cache API response in Supabase.
        
        Args:
            cache_key: Cache key
            response_data: Response data to cache
            supabase_client: Supabase client instance
        """
        if not self.cache_enabled or not supabase_client:
            return
        
        try:
            supabase_client.table("api_cache").upsert({
                "cache_key": cache_key,
                "response_data": response_data,
                "cached_at": datetime.utcnow().isoformat(),
                "source": "dataforseo",
            }).execute()
            
            logger.info(f"Cached response for key {cache_key[:16]}...")
        
        except Exception as e:
            logger.warning(f"Cache storage failed: {e}")
    
    async def fetch_serp_results(
        self,
        keywords: List[str],
        location_code: int = 2840,  # United States
        language_code: str = "en",
        device: str = "desktop",
        depth: int = 100,
        target_domain: Optional[str] = None,
        supabase_client = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch live SERP results for multiple keywords.
        
        Args:
            keywords: List of keywords to fetch SERPs for
            location_code: DataForSEO location code (2840 = USA)
            language_code: Language code (e.g., "en")
            device: Device type ("desktop" or "mobile")
            depth: Number of results to retrieve (max 700)
            target_domain: Optional target domain to highlight in results
            supabase_client: Optional Supabase client for caching
            
        Returns:
            List of parsed SERP result dictionaries, one per keyword
            
        Example result structure:
            [
                {
                    "keyword": "best crm software",
                    "location": 2840,
                    "language": "en",
                    "serp_features": {
                        "featured_snippet": True,
                        "people_also_ask": 4,
                        "ai_overview": True,
                        ...
                    },
                    "organic_results": [
                        {
                            "position": 1,
                            "url": "https://example.com/page",
                            "domain": "example.com",
                            "title": "...",
                            "description": "...",
                            "is_target": False,
                        },
                        ...
                    ],
                    "target_domain_ranking": {
                        "found": True,
                        "position": 5,
                        "visual_position": 8.5,
                        "url": "https://target.com/page",
                    },
                    "competitors": [
                        {"domain": "competitor1.com", "positions": [1, 3, 7]},
                        ...
                    ],
                    "visual_displacement": 3.5,
                }
            ]
        """
        if not keywords:
            return []
        
        results = []
        
        # Process keywords in batches to respect rate limits
        for keyword in keywords:
            try:
                # Check cache first
                cache_params = {
                    "keyword": keyword,
                    "location": location_code,
                    "language": language_code,
                    "device": device,
                    "depth": depth,
                }
                cache_key = self._generate_cache_key("serp/google/organic", cache_params)
                
                cached = await self._get_cached_response(cache_key, supabase_client)
                if cached:
                    results.append(cached)
                    continue
                
                # Make live request
                payload = [{
                    "keyword": keyword,
                    "location_code": location_code,
                    "language_code": language_code,
                    "device": device,
                    "depth": depth,
                    "calculate_rectangles": True,
                }]
                
                response = await self._make_request(
                    "POST",
                    "/serp/google/organic/live/advanced",
                    data=payload,
                )
                
                # Parse response
                parsed = self._parse_serp_response(
                    response,
                    keyword,
                    target_domain,
                )
                
                results.append(parsed)
                
                # Cache result
                await self._cache_response(cache_key, parsed, supabase_client)
                
                logger.info(f"Fetched SERP for keyword: {keyword}")
            
            except Exception as e:
                logger.error(f"Failed to fetch SERP for '{keyword}': {e}")
                # Return partial data on error
                results.append({
                    "keyword": keyword,
                    "error": str(e),
                    "organic_results": [],
                })
        
        return results
    
    def _parse_serp_response(
        self,
        response: Dict,
        keyword: str,
        target_domain: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Parse DataForSEO SERP response into clean structure.
        
        Args:
            response: Raw API response
            keyword: Keyword searched
            target_domain: Optional target domain to highlight
            
        Returns:
            Parsed SERP data
        """
        parsed = {
            "keyword": keyword,
            "location": None,
            "language": None,
            "serp_features": {},
            "organic_results": [],
            "target_domain_ranking": {
                "found": False,
                "position": None,
                "visual_position": None,
                "url": None,
            },
            "competitors": {},
            "visual_displacement": 0,
            "total_results": 0,
        }
        
        # Extract task data
        tasks = response.get("tasks", [])
        if not tasks or len(tasks) == 0:
            return parsed
        
        task = tasks[0]
        result_data = task.get("result", [])
        if not result_data or len(result_data) == 0:
            return parsed
        
        result = result_data[0]
        
        # Extract metadata
        parsed["location"] = result.get("location_code")
        parsed["language"] = result.get("language_code")
        parsed["total_results"] = result.get("items_count", 0)
        
        # Parse SERP items
        items = result.get("items", [])
        
        organic_position = 0
        visual_position = 0.0
        serp_features = {}
        organic_results = []
        competitor_domains = {}
        
        for item in items:
            item_type = item.get("type", "")
            rank_group = item.get("rank_group")
            rank_absolute = item.get("rank_absolute")
            
            # Identify SERP features
            feature_name = self._identify_serp_feature(item_type)
            if feature_name:
                if feature_name == "people_also_ask":
                    # Count PAA questions
                    paa_items = item.get("items", [])
                    serp_features[feature_name] = len(paa_items) if paa_items else 1
                else:
                    serp_features[feature_name] = True
                
                # Add visual displacement
                visual_impact = self.SERP_FEATURE_VISUAL_IMPACT.get(feature_name, 0)
                if feature_name == "people_also_ask":
                    visual_impact *= serp_features[feature_name]
                visual_position += visual_impact
            
            # Parse organic results
            if item_type == "organic":
                organic_position += 1
                
                url = item.get("url", "")
                domain = item.get("domain", "")
                title = item.get("title", "")
                description = item.get("description", "")
                
                is_target = False
                if target_domain:
                    is_target = self._is_same_domain(domain, target_domain)
                
                organic_result = {
                    "position": organic_position,
                    "rank_absolute": rank_absolute,
                    "url": url,
                    "domain": domain,
                    "title": title,
                    "description": description,
                    "is_target": is_target,
                }
                
                organic_results.append(organic_result)
                
                # Track competitors
                if domain:
                    if domain not in competitor_domains:
                        competitor_domains[domain] = []
                    competitor_domains[domain].append(organic_position)
                
                # Track target domain ranking
                if is_target and not parsed["target_domain_ranking"]["found"]:
                    parsed["target_domain_ranking"] = {
                        "found": True,
                        "position": organic_position,
                        "visual_position": visual_position + organic_position,
                        "url": url,
                    }
        
        parsed["serp_features"] = serp_features
        parsed["organic_results"] = organic_results
        
        # Calculate visual displacement for target domain
        if parsed["target_domain_ranking"]["found"]:
            organic_pos = parsed["target_domain_ranking"]["position"]
            visual_pos = parsed["target_domain_ranking"]["visual_position"]
            parsed["visual_displacement"] = visual_pos - organic_pos
        
        # Format competitors
        competitors = []
        for domain, positions in competitor_domains.items():
            if target_domain and self._is_same_domain(domain, target_domain):
                continue
            
            competitors.append({
                "domain": domain,
                "positions": positions,
                "avg_position": sum(positions) / len(positions),
                "appearances": len(positions),
            })
        
        # Sort by average position
        competitors.sort(key=lambda x: x["avg_position"])
        parsed["competitors"] = competitors[:10]  # Top 10 competitors
        
        return parsed
    
    def _identify_serp_feature(self, item_type: str) -> Optional[str]:
        """
        Identify SERP feature name from DataForSEO item type.
        
        Args:
            item_type: DataForSEO item type
            
        Returns:
            Standardized feature name or None
        """
        for feature_name, type_list in self.SERP_FEATURE_TYPES.items():
            if item_type in type_list:
                return feature_name
        return None
    
    def _is_same_domain(self, domain1: str, domain2: str) -> bool:
        """
        Check if two domains are the same (ignoring www).
        
        Args:
            domain1: First domain
            domain2: Second domain
            
        Returns:
            True if domains match
        """
        if not domain1 or not domain2:
            return False
        
        d1 = domain1.lower().replace("www.", "")
        d2 = domain2.lower().replace("www.", "")
        
        return d1 == d2
    
    async def fetch_keyword_data(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
        supabase_client = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch keyword metrics (search volume, CPC, competition, difficulty).
        
        Args:
            keywords: List of keywords
            location_code: DataForSEO location code
            language_code: Language code
            supabase_client: Optional Supabase client for caching
            
        Returns:
            List of keyword data dictionaries
            
        Example result:
            [
                {
                    "keyword": "best crm software",
                    "search_volume": 8900,
                    "cpc": 12.50,
                    "competition": 0.72,
                    "keyword_difficulty": 58,
                },
                ...
            ]
        """
        if not keywords:
            return []
        
        results = []
        
        # DataForSEO allows up to 1000 keywords per request
        batch_size = 100
        
        for i in range(0, len(keywords), batch_size):
            batch = keywords[i:i + batch_size]
            
            try:
                # Check cache
                cache_params = {
                    "keywords": sorted(batch),
                    "location": location_code,
                    "language": language_code,
                }
                cache_key = self._generate_cache_key("keywords_data", cache_params)
                
                cached = await self._get_cached_response(cache_key, supabase_client)
                if cached:
                    results.extend(cached)
                    continue
                
                # Make live request
                payload = [{
                    "keywords": batch,
                    "location_code": location_code,
                    "language_code": language_code,
                }]
                
                response = await self._make_request(
                    "POST",
                    "/keywords_data/google_ads/search_volume/live",
                    data=payload,
                )
                
                # Parse response
                batch_results = self._parse_keyword_data_response(response)
                results.extend(batch_results)
                
                # Cache results
                await self._cache_response(cache_key, batch_results, supabase_client)
                
                logger.info(f"Fetched keyword data for {len(batch)} keywords")
            
            except Exception as e:
                logger.error(f"Failed to fetch keyword data for batch: {e}")
                # Return partial data
                for kw in batch:
                    results.append({
                        "keyword": kw,
                        "error": str(e),
                    })
        
        return results
    
    def _parse_keyword_data_response(
        self,
        response: Dict,
    ) -> List[Dict[str, Any]]:
        """
        Parse keyword data response.
        
        Args:
            response: Raw API response
            
        Returns:
            List of parsed keyword data
        """
        results = []
        
        tasks = response.get("tasks", [])
        if not tasks:
            return results
        
        task = tasks[0]
        result_data = task.get("result", [])
        if not result_data:
            return results
        
        for item in result_data:
            keyword = item.get("keyword", "")
            
            # Extract metrics
            search_volume = item.get("search_volume")
            cpc = item.get("cpc")
            competition = item.get("competition")
            
            # Keyword difficulty (if available from different endpoint)
            keyword_difficulty = item.get("keyword_difficulty")
            
            results.append({
                "keyword": keyword,
                "search_volume": search_volume,
                "cpc": cpc,
                "competition": competition,
                "keyword_difficulty": keyword_difficulty,
            })
        
        return results
    
    async def fetch_competitor_rankings(
        self,
        target_domain: str,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
        supabase_client = None,
    ) -> Dict[str, Any]:
        """
        Fetch competitor ranking data across multiple keywords.
        Identifies which domains rank most frequently alongside target domain.
        
        Args:
            target_domain: Target domain to analyze
            keywords: List of keywords to check
            location_code: DataForSEO location code
            language_code: Language code
            supabase_client: Optional Supabase client for caching
            
        Returns:
            Competitor analysis dictionary
            
        Example result:
            {
                "target_domain": "target.com",
                "keywords_analyzed": 50,
                "target_rankings": [
                    {"keyword": "best crm", "position": 3, "visual_position": 6.5},
                    ...
                ],
                "top_competitors": [
                    {
                        "domain": "competitor1.com",
                        "keywords_shared": 34,
                        "avg_position": 4.2,
                        "positions_distribution": {1: 2, 2: 5, 3: 8, ...},
                        "threat_level": "high",  # high/medium/low
                    },
                    ...
                ],
            }
        """
        # Fetch SERP results for all keywords
        serp_results = await self.fetch_serp_results(
            keywords=keywords,
            location_code=location_code,
            language_code=language_code,
            target_domain=target_domain,
            supabase_client=supabase_client,
        )
        
        # Aggregate competitor data
        target_rankings = []
        competitor_map = {}
        
        for serp in serp_results:
            keyword = serp.get("keyword")
            
            # Track target rankings
            target_rank = serp.get("target_domain_ranking", {})
            if target_rank.get("found"):
                target_rankings.append({
                    "keyword": keyword,
                    "position": target_rank.get("position"),
                    "visual_position": target_rank.get("visual_position"),
                    "url": target_rank.get("url"),
                })
            
            # Aggregate competitors
            competitors = serp.get("competitors", [])
            for comp in competitors:
                domain = comp["domain"]
                
                if domain not in competitor_map:
                    competitor_map[domain] = {
                        "domain": domain,
                        "keywords": [],
                        "positions": [],
                    }
                
                competitor_map[domain]["keywords"].append(keyword)
                competitor_map[domain]["positions"].extend(comp["positions"])
        
        # Calculate competitor metrics
        top_competitors = []
        
        for domain, data in competitor_map.items():
            positions = data["positions"]
            keywords_shared = len(data["keywords"])
            
            if keywords_shared < 2:  # Filter out one-off appearances
                continue
            
            avg_position = sum(positions) / len(positions)
            
            # Position distribution
            pos_distribution = {}
            for pos in positions:
                pos_distribution[pos] = pos_distribution.get(pos, 0) + 1
            
            # Threat level based on frequency and avg position
            if keywords_shared > len(keywords) * 0.3 and avg_position < 5:
                threat_level = "high"
            elif keywords_shared > len(keywords) * 0.15 and avg_position < 8:
                threat_level = "medium"
            else:
                threat_level = "low"
            
            top_competitors.append({
                "domain": domain,
                "keywords_shared": keywords_shared,
                "avg_position": round(avg_position, 1),
                "positions_distribution": pos_distribution,
                "threat_level": threat_level,
            })
        
        # Sort by keywords_shared DESC
        top_competitors.sort(key=lambda x: x["keywords_shared"], reverse=True)
        
        return {
            "target_domain": target_domain,
            "keywords_analyzed": len(keywords),
            "target_rankings": target_rankings,
            "top_competitors": top_competitors[:20],  # Top 20
        }
    
    async def fetch_backlink_summary(
        self,
        target_domain: str,
        supabase_client = None,
    ) -> Dict[str, Any]:
        """
        Fetch backlink summary for a domain.
        
        Args:
            target_domain: Domain to analyze
            supabase_client: Optional Supabase client for caching
            
        Returns:
            Backlink summary dictionary
            
        Example result:
            {
                "domain": "target.com",
                "total_backlinks": 125000,
                "referring_domains": 3400,
                "referring_ips": 2800,
                "domain_rank": 72,
                "first_seen": "2010-03-15",
                "backlinks_spam_score": 12,
            }
        """
        try:
            # Check cache
            cache_key = self._generate_cache_key("backlinks/summary", {"domain": target_domain})
            cached = await self._get_cached_response(cache_key, supabase_client)
            if cached:
                return cached
            
            # Make live request
            payload = [{
                "target": target_domain,
            }]
            
            response = await self._make_request(
                "POST",
                "/backlinks/summary/live",
                data=payload,
            )
            
            # Parse response
            parsed = self._parse_backlink_summary_response(response, target_domain)
            
            # Cache result
            await self._cache_response(cache_key, parsed, supabase_client)
            
            logger.info(f"Fetched backlink summary for {target_domain}")
            
            return parsed
        
        except Exception as e:
            logger.error(f"Failed to fetch backlink summary for {target_domain}: {e}")
            return {
                "domain": target_domain,
                "error": str(e),
            }
    
    def _parse_backlink_summary_response(
        self,
        response: Dict,
        domain: str,
    ) -> Dict[str, Any]:
        """
        Parse backlink summary response.
        
        Args:
            response: Raw API response
            domain: Target domain
            
        Returns:
            Parsed backlink summary
        """
        result = {
            "domain": domain,
            "total_backlinks": 0,
            "referring_domains": 0,
            "referring_ips": 0,
            "domain_rank": 0,
            "first_seen": None,
            "backlinks_spam_score": 0,
        }
        
        tasks = response.get("tasks", [])
        if not tasks:
            return result
        
        task = tasks[0]
        result_data = task.get("result", [])
        if not result_data:
            return result
        
        data = result_data[0]
        
        # Extract metrics
        result["total_backlinks"] = data.get("backlinks", 0)
        result["referring_domains"] = data.get("referring_domains", 0)
        result["referring_ips"] = data.get("referring_ips", 0)
        result["domain_rank"] = data.get("rank", 0)
        result["first_seen"] = data.get("first_seen")
        result["backlinks_spam_score"] = data.get("backlinks_spam_score", 0)
        
        return result
    
    def calculate_visual_position(
        self,
        organic_position: int,
        serp_features: Dict[str, Any],
    ) -> float:
        """
        Calculate visual position accounting for SERP features above organic result.
        
        Args:
            organic_position: Organic ranking position (1-based)
            serp_features: Dictionary of SERP features present
            
        Returns:
            Visual position (float)
            
        Example:
            >>> calculate_visual_position(3, {
            ...     "featured_snippet": True,
            ...     "people_also_ask": 4,
            ...     "ai_overview": True,
            ... })
            10.0  # Position 3, but visually at position 10
        """
        visual_position = float(organic_position)
        
        for feature, value in serp_features.items():
            impact = self.SERP_FEATURE_VISUAL_IMPACT.get(feature, 0)
            
            if isinstance(value, bool) and value:
                visual_position += impact
            elif isinstance(value, int) and value > 0:
                # For features with counts (e.g., PAA questions)
                visual_position += impact * value
        
        return round(visual_position, 1)
    
    async def batch_serp_analysis(
        self,
        keywords: List[str],
        target_domain: str,
        location_code: int = 2840,
        language_code: str = "en",
        include_keyword_data: bool = True,
        supabase_client = None,
    ) -> Dict[str, Any]:
        """
        Comprehensive batch SERP analysis combining multiple data sources.
        
        This is the primary method for Module 3 (SERP Landscape Analysis).
        
        Args:
            keywords: List of keywords to analyze
            target_domain: Target domain
            location_code: Location code
            language_code: Language code
            include_keyword_data: Whether to fetch search volume/CPC data
            supabase_client: Supabase client for caching
            
        Returns:
            Comprehensive SERP analysis dictionary
        """
        # Fetch SERP results
        logger.info(f"Fetching SERP results for {len(keywords)} keywords...")
        serp_results = await self.fetch_serp_results(
            keywords=keywords,
            location_code=location_code,
            language_code=language_code,
            target_domain=target_domain,
            supabase_client=supabase_client,
        )
        
        # Fetch keyword data if requested
        keyword_data = []
        if include_keyword_data:
            logger.info(f"Fetching keyword metrics for {len(keywords)} keywords...")
            keyword_data = await self.fetch_keyword_data(
                keywords=keywords,
                location_code=location_code,
                language_code=language_code,
                supabase_client=supabase_client,
            )
        
        # Analyze results
        analysis = {
            "target_domain": target_domain,
            "keywords_analyzed": len(keywords),
            "serp_feature_displacement": [],
            "competitors": {},
            "intent_classification": [],
            "total_click_share": 0.0,
            "click_share_opportunity": 0.0,
        }
        
        # Combine SERP and keyword data
        keyword_data_map = {kw["keyword"]: kw for kw in keyword_data}
        
        competitor_map = {}
        total_estimated_clicks = 0
        total_target_clicks = 0
        
        for serp in serp_results:
            keyword = serp.get("keyword")
            kw_metrics = keyword_data_map.get(keyword, {})
            search_volume = kw_metrics.get("search_volume", 0)
            
            # SERP feature displacement
            target_rank = serp.get("target_domain_ranking", {})
            if target_rank.get("found"):
                displacement = serp.get("visual_displacement", 0)
                
                if displacement > 3:
                    features_above = []
                    for feature, value in serp.get("serp_features", {}).items():
                        if value:
                            features_above.append(feature)
                    
                    # Estimate CTR impact
                    organic_pos = target_rank.get("position", 0)
                    visual_pos = target_rank.get("visual_position", 0)
                    
                    # Simplified CTR curve
                    organic_ctr = self._estimate_ctr(organic_pos)
                    visual_ctr = self._estimate_ctr(visual_pos)
                    ctr_impact = organic_ctr - visual_ctr
                    
                    analysis["serp_feature_displacement"].append({
                        "keyword": keyword,
                        "organic_position": organic_pos,
                        "visual_position": visual_pos,
                        "features_above": features_above,
                        "estimated_ctr_impact": round(ctr_impact, 3),
                        "search_volume": search_volume,
                    })
            
            # Aggregate competitors
            for comp in serp.get("competitors", []):
                domain = comp["domain"]
                if domain not in competitor_map:
                    competitor_map[domain] = {
                        "domain": domain,
                        "keywords_shared": 0,
                        "positions": [],
                        "avg_position": 0,
                    }
                competitor_map[domain]["keywords_shared"] += 1
                competitor_map[domain]["positions"].extend(comp["positions"])
            
            # Intent classification (simplified)
            intent = self._classify_serp_intent(serp)
            analysis["intent_classification"].append({
                "keyword": keyword,
                "intent": intent,
                "search_volume": search_volume,
            })
            
            # Click share estimation
            if search_volume:
                total_estimated_clicks += search_volume
                
                if target_rank.get("found"):
                    position = target_rank.get("visual_position", 100)
                    ctr = self._estimate_ctr(position)
                    target_clicks = search_volume * ctr
                    total_target_clicks += target_clicks
        
        # Calculate competitor metrics
        top_competitors = []
        for domain, data in competitor_map.items():
            if data["keywords_shared"] < 2:
                continue
            
            avg_pos = sum(data["positions"]) / len(data["positions"])
            data["avg_position"] = round(avg_pos, 1)
            
            # Threat level
            if data["keywords_shared"] > len(keywords) * 0.2 and avg_pos < 5:
                threat = "high"
            elif data["keywords_shared"] > len(keywords) * 0.1:
                threat = "medium"
            else:
                threat = "low"
            
            top_competitors.append({
                "domain": domain,
                "keywords_shared": data["keywords_shared"],
                "avg_position": data["avg_position"],
                "threat_level": threat,
            })
        
        top_competitors.sort(key=lambda x: x["keywords_shared"], reverse=True)
        analysis["competitors"] = top_competitors[:20]
        
        # Click share
        if total_estimated_clicks > 0:
            analysis["total_click_share"] = round(total_target_clicks / total_estimated_clicks, 3)
            # Opportunity = if all keywords were position 1
            max_clicks = total_estimated_clicks * 0.35  # Assume 35% CTR at position 1
            analysis["click_share_opportunity"] = round(max_clicks / total_estimated_clicks, 3)
        
        return analysis
    
    def _estimate_ctr(self, position: float) -> float:
        """
        Estimate CTR based on position using simplified curve.
        
        Args:
            position: Search position (can be visual position)
            
        Returns:
            Estimated CTR (0-1)
        """
        if position < 1:
            return 0.35
        elif position <= 3:
            return 0.35 * (1 - (position - 1) * 0.15)
        elif position <= 10:
            return 0.05 * (11 - position) / 7
        elif position <= 20:
            return 0.02 * (21 - position) / 10
        else:
            return 0.005
    
    def _classify_serp_intent(self, serp: Dict) -> str:
        """
        Classify search intent based on SERP composition.
        
        Args:
            serp: Parsed SERP data
            
        Returns:
            Intent classification: informational, commercial, transactional, navigational
        """
        features = serp.get("serp_features", {})
        keyword = serp.get("keyword", "").lower()
        
        # Transactional signals
        if features.get("shopping") or features.get("google_shopping"):
            return "transactional"
        
        # Navigational signals
        if features.get("knowledge_graph") and len(keyword.split()) <= 2:
            return "navigational"
        
        # Commercial signals
        if any(word in keyword for word in ["best", "top", "review", "vs", "compare", "alternative"]):
            return "commercial"
        
        # Informational signals
        if features.get("people_also_ask") or features.get("featured_snippet"):
            return "informational"
        
        # Default
        if any(word in keyword for word in ["how", "what", "why", "when", "guide", "tutorial"]):
            return "informational"
        
        return "commercial"