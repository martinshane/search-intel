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
    
    # Rate limiting: DataForSEO allows ~2000 requests/minute
    # We'll be conservative: 20 concurrent requests, 100ms between batches
    MAX_CONCURRENT_REQUESTS = 20
    REQUEST_DELAY_MS = 100
    
    # Cache TTL for SERP results (24 hours)
    CACHE_TTL_HOURS = 24
    
    def __init__(
        self,
        username: Optional[str] = None,
        password: Optional[str] = None,
        supabase_client: Optional[Any] = None,
        enable_cache: bool = True,
    ):
        """
        Initialize DataForSEO client.
        
        Args:
            username: DataForSEO API username (defaults to env DATAFORSEO_USERNAME)
            password: DataForSEO API password (defaults to env DATAFORSEO_PASSWORD)
            supabase_client: Optional Supabase client for caching
            enable_cache: Whether to use caching (default True)
        """
        self.username = username or os.getenv("DATAFORSEO_USERNAME")
        self.password = password or os.getenv("DATAFORSEO_PASSWORD")
        
        if not self.username or not self.password:
            raise DataForSEOAuthError(
                "DataForSEO credentials not provided. Set DATAFORSEO_USERNAME and "
                "DATAFORSEO_PASSWORD environment variables or pass them to constructor."
            )
        
        self.supabase = supabase_client
        self.enable_cache = enable_cache and supabase_client is not None
        
        self._client: Optional[httpx.AsyncClient] = None
        self._semaphore = asyncio.Semaphore(self.MAX_CONCURRENT_REQUESTS)
        self._authenticated = False
    
    async def __aenter__(self):
        """Async context manager entry"""
        await self.authenticate()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close()
    
    async def authenticate(self):
        """
        Authenticate with DataForSEO API and set up HTTP client.
        """
        if self._authenticated and self._client:
            return
        
        auth = httpx.BasicAuth(self.username, self.password)
        
        self._client = httpx.AsyncClient(
            base_url=self.BASE_URL,
            auth=auth,
            timeout=httpx.Timeout(60.0),
            limits=httpx.Limits(
                max_keepalive_connections=self.MAX_CONCURRENT_REQUESTS,
                max_connections=self.MAX_CONCURRENT_REQUESTS * 2,
            ),
        )
        
        # Test authentication
        try:
            response = await self._client.get("/appendix/user_data")
            response.raise_for_status()
            data = response.json()
            
            if data.get("status_code") != 20000:
                raise DataForSEOAuthError(
                    f"Authentication failed: {data.get('status_message', 'Unknown error')}"
                )
            
            self._authenticated = True
            logger.info("Successfully authenticated with DataForSEO API")
            
        except httpx.HTTPStatusError as e:
            raise DataForSEOAuthError(f"Authentication failed: {e}")
        except Exception as e:
            raise DataForSEOAuthError(f"Authentication error: {e}")
    
    async def close(self):
        """Close HTTP client"""
        if self._client:
            await self._client.aclose()
            self._client = None
            self._authenticated = False
    
    def _generate_cache_key(self, endpoint: str, params: Dict[str, Any]) -> str:
        """Generate deterministic cache key from endpoint and params"""
        param_str = json.dumps(params, sort_keys=True)
        hash_obj = hashlib.sha256(f"{endpoint}:{param_str}".encode())
        return f"dataforseo:{hash_obj.hexdigest()}"
    
    async def _get_from_cache(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """Retrieve result from cache if available and not expired"""
        if not self.enable_cache:
            return None
        
        try:
            result = await asyncio.to_thread(
                lambda: self.supabase.table("api_cache")
                .select("data, created_at")
                .eq("cache_key", cache_key)
                .maybe_single()
                .execute()
            )
            
            if not result.data:
                return None
            
            # Check if cache entry is expired
            created_at = datetime.fromisoformat(result.data["created_at"])
            if datetime.utcnow() - created_at > timedelta(hours=self.CACHE_TTL_HOURS):
                # Expired, delete it
                await asyncio.to_thread(
                    lambda: self.supabase.table("api_cache")
                    .delete()
                    .eq("cache_key", cache_key)
                    .execute()
                )
                return None
            
            return result.data["data"]
            
        except Exception as e:
            logger.warning(f"Cache retrieval error: {e}")
            return None
    
    async def _save_to_cache(self, cache_key: str, data: Dict[str, Any]):
        """Save result to cache"""
        if not self.enable_cache:
            return
        
        try:
            await asyncio.to_thread(
                lambda: self.supabase.table("api_cache")
                .upsert({
                    "cache_key": cache_key,
                    "data": data,
                    "created_at": datetime.utcnow().isoformat(),
                })
                .execute()
            )
        except Exception as e:
            logger.warning(f"Cache save error: {e}")
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.NetworkError)),
    )
    async def _make_request(
        self,
        endpoint: str,
        method: str = "GET",
        json_data: Optional[List[Dict[str, Any]]] = None,
        use_cache: bool = True,
    ) -> Dict[str, Any]:
        """
        Make authenticated request to DataForSEO API with rate limiting and retries.
        
        Args:
            endpoint: API endpoint path (e.g., "/serp/google/organic/live/advanced")
            method: HTTP method (GET or POST)
            json_data: JSON payload for POST requests
            use_cache: Whether to use caching for this request
        
        Returns:
            Parsed JSON response
        
        Raises:
            DataForSEOError: On API errors
            DataForSEORateLimitError: On rate limit errors
        """
        if not self._authenticated:
            await self.authenticate()
        
        # Check cache for GET requests
        cache_key = None
        if use_cache and method == "GET":
            cache_key = self._generate_cache_key(endpoint, {})
            cached = await self._get_from_cache(cache_key)
            if cached:
                logger.debug(f"Cache hit for {endpoint}")
                return cached
        elif use_cache and method == "POST" and json_data:
            cache_key = self._generate_cache_key(endpoint, json_data[0] if json_data else {})
            cached = await self._get_from_cache(cache_key)
            if cached:
                logger.debug(f"Cache hit for {endpoint}")
                return cached
        
        # Rate limiting
        async with self._semaphore:
            try:
                if method == "POST":
                    response = await self._client.post(endpoint, json=json_data)
                else:
                    response = await self._client.get(endpoint)
                
                # Add small delay between requests
                await asyncio.sleep(self.REQUEST_DELAY_MS / 1000)
                
                response.raise_for_status()
                data = response.json()
                
                # Check DataForSEO status code
                if data.get("status_code") == 40401:
                    raise DataForSEORateLimitError("Rate limit exceeded")
                elif data.get("status_code") == 40101:
                    raise DataForSEOAuthError("Authentication failed")
                elif data.get("status_code") != 20000:
                    raise DataForSEOError(
                        f"API error: {data.get('status_message', 'Unknown error')} "
                        f"(code: {data.get('status_code')})"
                    )
                
                # Cache successful response
                if use_cache and cache_key:
                    await self._save_to_cache(cache_key, data)
                
                return data
                
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:
                    raise DataForSEORateLimitError("Rate limit exceeded")
                elif e.response.status_code == 401:
                    raise DataForSEOAuthError("Authentication failed")
                else:
                    raise DataForSEOError(f"HTTP error: {e}")
            except httpx.TimeoutException:
                raise DataForSEOError("Request timeout")
            except httpx.NetworkError as e:
                raise DataForSEOError(f"Network error: {e}")
    
    def _extract_serp_features(self, items: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Extract and categorize SERP features from DataForSEO response items.
        
        Returns:
            Dict with:
                - features: List of feature types present
                - feature_counts: Dict of feature type -> count
                - visual_impact: Total visual position impact
                - details: Detailed information about each feature
        """
        features = set()
        feature_counts = {}
        details = []
        
        for item in items:
            item_type = item.get("type", "")
            
            # Categorize item into SERP feature type
            for feature_name, type_list in self.SERP_FEATURE_TYPES.items():
                if item_type in type_list:
                    features.add(feature_name)
                    feature_counts[feature_name] = feature_counts.get(feature_name, 0) + 1
                    
                    # Extract feature-specific details
                    if feature_name == "people_also_ask":
                        details.append({
                            "type": feature_name,
                            "questions": item.get("items", []),
                            "count": len(item.get("items", [])),
                        })
                    elif feature_name == "featured_snippet":
                        details.append({
                            "type": feature_name,
                            "url": item.get("url", ""),
                            "domain": item.get("domain", ""),
                            "title": item.get("title", ""),
                        })
                    elif feature_name == "knowledge_graph":
                        details.append({
                            "type": feature_name,
                            "title": item.get("title", ""),
                            "description": item.get("description", ""),
                        })
                    elif feature_name == "local_pack":
                        details.append({
                            "type": feature_name,
                            "count": len(item.get("items", [])),
                        })
                    else:
                        details.append({
                            "type": feature_name,
                            "rank_absolute": item.get("rank_absolute"),
                        })
                    
                    break
        
        # Calculate total visual impact
        visual_impact = 0.0
        for feature, count in feature_counts.items():
            impact_per_item = self.SERP_FEATURE_VISUAL_IMPACT.get(feature, 0.0)
            if feature == "people_also_ask":
                # Each PAA question adds 0.5 positions
                visual_impact += impact_per_item * count
            else:
                # Most features count once regardless of multiple instances
                visual_impact += impact_per_item
        
        return {
            "features": sorted(list(features)),
            "feature_counts": feature_counts,
            "visual_impact": round(visual_impact, 1),
            "details": details,
        }
    
    def _calculate_visual_position(
        self,
        organic_rank: int,
        items: List[Dict[str, Any]],
    ) -> float:
        """
        Calculate visual position accounting for SERP features above the organic result.
        
        Args:
            organic_rank: The organic ranking position (1-based)
            items: All SERP items from DataForSEO response
        
        Returns:
            Visual position (float)
        """
        visual_offset = 0.0
        
        for item in items:
            item_rank = item.get("rank_absolute", 999)
            
            # Only count features that appear before this organic result
            if item_rank < organic_rank:
                item_type = item.get("type", "")
                
                # Find feature category and add its impact
                for feature_name, type_list in self.SERP_FEATURE_TYPES.items():
                    if item_type in type_list:
                        impact = self.SERP_FEATURE_VISUAL_IMPACT.get(feature_name, 0.0)
                        
                        if feature_name == "people_also_ask":
                            # Count individual questions
                            question_count = len(item.get("items", []))
                            visual_offset += impact * question_count
                        else:
                            visual_offset += impact
                        
                        break
        
        return round(organic_rank + visual_offset, 1)
    
    async def fetch_serp_results(
        self,
        keywords: List[str],
        location_code: int = 2840,  # USA
        language_code: str = "en",
        device: str = "desktop",
        depth: int = 100,  # Number of results to retrieve
    ) -> List[Dict[str, Any]]:
        """
        Fetch live SERP results for multiple keywords.
        
        Args:
            keywords: List of keywords to fetch SERPs for
            location_code: DataForSEO location code (2840 = USA)
            language_code: Language code (e.g., "en")
            device: Device type ("desktop" or "mobile")
            depth: Number of results to retrieve per keyword (max 100)
        
        Returns:
            List of results, one per keyword, each containing:
                - keyword: The keyword
                - organic_results: Top 10 organic results with domain, position, URL
                - serp_features: Detected SERP features
                - total_results: Total number of results
                - location_code: Location code used
        """
        if not keywords:
            return []
        
        # Prepare batch request payload
        tasks = []
        for keyword in keywords:
            task_payload = {
                "keyword": keyword,
                "location_code": location_code,
                "language_code": language_code,
                "device": device,
                "depth": depth,
            }
            tasks.append(task_payload)
        
        # Make request
        endpoint = "/serp/google/organic/live/advanced"
        response = await self._make_request(endpoint, method="POST", json_data=tasks)
        
        # Parse results
        results = []
        for task in response.get("tasks", []):
            if task.get("status_code") != 20000:
                logger.warning(
                    f"Task failed for keyword: {task.get('data', {}).get('keyword', 'unknown')} - "
                    f"{task.get('status_message', 'Unknown error')}"
                )
                continue
            
            for result_item in task.get("result", []):
                keyword = result_item.get("keyword", "")
                items = result_item.get("items", [])
                
                # Extract organic results
                organic_results = []
                for item in items:
                    if item.get("type") == "organic":
                        rank = item.get("rank_group", 0)
                        if rank <= 10:  # Top 10 only
                            visual_pos = self._calculate_visual_position(rank, items)
                            
                            organic_results.append({
                                "position": rank,
                                "visual_position": visual_pos,
                                "url": item.get("url", ""),
                                "domain": item.get("domain", ""),
                                "title": item.get("title", ""),
                                "description": item.get("description", ""),
                                "breadcrumb": item.get("breadcrumb", ""),
                            })
                
                # Sort by position
                organic_results.sort(key=lambda x: x["position"])
                
                # Extract SERP features
                serp_features = self._extract_serp_features(items)
                
                results.append({
                    "keyword": keyword,
                    "organic_results": organic_results,
                    "serp_features": serp_features,
                    "total_results": result_item.get("se_results_count", 0),
                    "location_code": location_code,
                    "language_code": language_code,
                    "device": device,
                    "fetched_at": datetime.utcnow().isoformat(),
                })
        
        return results
    
    async def get_competitor_domains(
        self,
        keywords: List[str],
        location_code: int = 2840,
        min_keyword_overlap: int = 3,
    ) -> List[Dict[str, Any]]:
        """
        Discover competitor domains based on keyword overlap.
        
        Args:
            keywords: List of keywords to analyze
            location_code: DataForSEO location code
            min_keyword_overlap: Minimum number of shared keywords to be considered a competitor
        
        Returns:
            List of competitor domains with:
                - domain: Competitor domain
                - keywords_shared: Number of keywords where they appear in top 10
                - avg_position: Average position across shared keywords
                - keyword_list: List of shared keywords
                - threat_level: "high", "medium", or "low"
        """
        # Fetch SERP results for all keywords
        serp_results = await self.fetch_serp_results(
            keywords=keywords,
            location_code=location_code,
        )
        
        # Build domain frequency map
        domain_keywords: Dict[str, List[Tuple[str, int]]] = {}
        
        for result in serp_results:
            keyword = result["keyword"]
            for organic in result["organic_results"]:
                domain = organic["domain"]
                position = organic["position"]
                
                if domain not in domain_keywords:
                    domain_keywords[domain] = []
                
                domain_keywords[domain].append((keyword, position))
        
        # Filter and aggregate competitors
        competitors = []
        for domain, keyword_positions in domain_keywords.items():
            if len(keyword_positions) < min_keyword_overlap:
                continue
            
            positions = [pos for _, pos in keyword_positions]
            avg_position = sum(positions) / len(positions)
            
            # Determine threat level
            if len(keyword_positions) >= len(keywords) * 0.5 and avg_position <= 5:
                threat_level = "high"
            elif len(keyword_positions) >= len(keywords) * 0.2 and avg_position <= 10:
                threat_level = "medium"
            else:
                threat_level = "low"
            
            competitors.append({
                "domain": domain,
                "keywords_shared": len(keyword_positions),
                "avg_position": round(avg_position, 1),
                "keyword_list": [kw for kw, _ in keyword_positions],
                "threat_level": threat_level,
            })
        
        # Sort by number of shared keywords (descending)
        competitors.sort(key=lambda x: x["keywords_shared"], reverse=True)
        
        return competitors
    
    async def get_domain_overview(
        self,
        domain: str,
        location_code: int = 2840,
    ) -> Dict[str, Any]:
        """
        Get domain overview metrics (if available via DataForSEO).
        
        Note: This uses DataForSEO's domain analytics if you have access.
        For basic implementation, we'll return a placeholder structure.
        
        Args:
            domain: Target domain
            location_code: Location code
        
        Returns:
            Domain metrics including organic keywords, traffic estimates, etc.
        """
        # This would use DataForSEO's domain analytics API
        # For now, return a placeholder structure
        # In production, you'd call /v3/dataforseo_labs/google/domain_overview/live
        
        logger.warning(
            f"Domain overview for {domain} - using placeholder. "
            "Implement full domain analytics API if needed."
        )
        
        return {
            "domain": domain,
            "metrics": {
                "organic_keywords": None,
                "organic_traffic": None,
                "organic_cost": None,
            },
            "note": "Domain overview requires DataForSEO Labs API access",
        }
    
    async def analyze_keyword_batch(
        self,
        keywords: List[str],
        location_code: int = 2840,
        user_domain: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Comprehensive batch analysis for a list of keywords.
        
        Args:
            keywords: List of keywords to analyze
            location_code: Location code
            user_domain: Optional user's domain to check rankings for
        
        Returns:
            Comprehensive analysis including:
                - serp_results: Full SERP data per keyword
                - competitors: Discovered competitors
                - serp_feature_summary: Aggregate SERP feature statistics
                - user_rankings: User's rankings if domain provided
        """
        # Fetch SERP results
        serp_results = await self.fetch_serp_results(
            keywords=keywords,
            location_code=location_code,
        )
        
        # Discover competitors
        competitors = await self.get_competitor_domains(
            keywords=keywords,
            location_code=location_code,
        )
        
        # Aggregate SERP feature statistics
        all_features: Set[str] = set()
        feature_counts = {}
        total_visual_impact = 0.0
        
        for result in serp_results:
            features = result["serp_features"]["features"]
            all_features.update(features)
            
            for feature in features:
                feature_counts[feature] = feature_counts.get(feature, 0) + 1
            
            total_visual_impact += result["serp_features"]["visual_impact"]
        
        serp_feature_summary = {
            "total_keywords": len(keywords),
            "features_present": sorted(list(all_features)),
            "feature_frequency": feature_counts,
            "avg_visual_impact_per_keyword": round(
                total_visual_impact / len(keywords) if keywords else 0,
                2
            ),
        }
        
        # Check user rankings if domain provided
        user_rankings = []
        if user_domain:
            for result in serp_results:
                keyword = result["keyword"]
                user_rank = None
                user_visual_position = None
                
                for organic in result["organic_results"]:
                    if organic["domain"] == user_domain:
                        user_rank = organic["position"]
                        user_visual_position = organic["visual_position"]
                        break
                
                if user_rank:
                    user_rankings.append({
                        "keyword": keyword,
                        "position": user_rank,
                        "visual_position": user_visual_position,
                        "serp_features": result["serp_features"]["features"],
                    })
        
        return {
            "serp_results": serp_results,
            "competitors": competitors,
            "serp_feature_summary": serp_feature_summary,
            "user_rankings": user_rankings if user_domain else None,
        }
    
    async def classify_intent_from_serp(
        self,
        keyword: str,
        serp_result: Dict[str, Any],
    ) -> str:
        """
        Classify search intent based on SERP composition.
        
        Args:
            keyword: The keyword
            serp_result: SERP result from fetch_serp_results
        
        Returns:
            Intent classification: "informational", "commercial", "transactional", "navigational"
        """
        features = serp_result["serp_features"]["features"]
        
        # Navigational signals
        if "knowledge_graph" in features:
            return "navigational"
        
        # Transactional signals
        if "shopping" in features or "google_shopping" in features:
            return "transactional"
        
        # Commercial signals
        if "local_pack" in features:
            return "commercial"
        
        # Check keyword patterns
        keyword_lower = keyword.lower()
        
        transactional_patterns = [
            r'\bbuy\b', r'\bpurchase\b', r'\border\b', r'\bprice\b',
            r'\bcheap\b', r'\bdeal\b', r'\bdiscount\b', r'\bshipping\b'
        ]
        
        commercial_patterns = [
            r'\bbest\b', r'\btop\b', r'\breview\b', r'\bcompare\b',
            r'\bvs\b', r'\balternative\b', r'\baffordable\b'
        ]
        
        informational_patterns = [
            r'\bhow to\b', r'\bwhat is\b', r'\bwhy\b', r'\bguide\b',
            r'\btutorial\b', r'\blearn\b', r'\bexample\b'
        ]
        
        for pattern in transactional_patterns:
            if re.search(pattern, keyword_lower):
                return "transactional"
        
        for pattern in commercial_patterns:
            if re.search(pattern, keyword_lower):
                return "commercial"
        
        for pattern in informational_patterns:
            if re.search(pattern, keyword_lower):
                return "informational"
        
        # Default based on SERP features
        if "people_also_ask" in features or "featured_snippet" in features:
            return "informational"
        
        return "informational"  # Default fallback