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
    
    # Rate limiting: DataForSEO allows high throughput but we'll be conservative
    MAX_REQUESTS_PER_SECOND = 5
    MAX_CONCURRENT_REQUESTS = 10
    
    def __init__(self, cache_client=None):
        """
        Initialize DataForSEO client.
        
        Args:
            cache_client: Optional Supabase client for caching API responses
        """
        self.username = os.getenv("DATAFORSEO_USERNAME")
        self.password = os.getenv("DATAFORSEO_PASSWORD")
        
        if not self.username or not self.password:
            raise DataForSEOAuthError(
                "DataForSEO credentials not found. Set DATAFORSEO_USERNAME and DATAFORSEO_PASSWORD environment variables."
            )
        
        self.cache_client = cache_client
        self.http_client = None
        self._semaphore = asyncio.Semaphore(self.MAX_CONCURRENT_REQUESTS)
        self._rate_limiter = asyncio.Semaphore(self.MAX_REQUESTS_PER_SECOND)
        self._last_request_time = 0.0
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
        Initialize HTTP client with authentication.
        Tests credentials by making a ping request.
        """
        if self._authenticated and self.http_client:
            return
        
        self.http_client = httpx.AsyncClient(
            auth=(self.username, self.password),
            timeout=httpx.Timeout(60.0),
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=50)
        )
        
        # Test authentication
        try:
            response = await self.http_client.get(f"{self.BASE_URL}/serp/google/organic/live/advanced")
            if response.status_code == 401:
                raise DataForSEOAuthError("Invalid DataForSEO credentials")
            self._authenticated = True
            logger.info("DataForSEO client authenticated successfully")
        except httpx.HTTPError as e:
            raise DataForSEOAuthError(f"Authentication failed: {str(e)}")
    
    async def close(self):
        """Close HTTP client"""
        if self.http_client:
            await self.http_client.aclose()
            self._authenticated = False
    
    def _generate_cache_key(self, endpoint: str, params: Dict[str, Any]) -> str:
        """Generate cache key from endpoint and params"""
        key_data = f"{endpoint}:{json.dumps(params, sort_keys=True)}"
        return hashlib.sha256(key_data.encode()).hexdigest()
    
    async def _get_cached_response(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """Retrieve cached response from Supabase"""
        if not self.cache_client:
            return None
        
        try:
            result = await asyncio.to_thread(
                lambda: self.cache_client.table("dataforseo_cache")
                .select("response_data, created_at")
                .eq("cache_key", cache_key)
                .single()
                .execute()
            )
            
            if result.data:
                # Check if cache is still valid (24 hours)
                created_at = datetime.fromisoformat(result.data["created_at"].replace("Z", "+00:00"))
                if datetime.utcnow() - created_at.replace(tzinfo=None) < timedelta(hours=24):
                    logger.info(f"Cache hit for key: {cache_key[:16]}...")
                    return result.data["response_data"]
                else:
                    logger.info(f"Cache expired for key: {cache_key[:16]}...")
        except Exception as e:
            logger.warning(f"Cache retrieval failed: {str(e)}")
        
        return None
    
    async def _cache_response(self, cache_key: str, response_data: Dict[str, Any]):
        """Store response in Supabase cache"""
        if not self.cache_client:
            return
        
        try:
            await asyncio.to_thread(
                lambda: self.cache_client.table("dataforseo_cache")
                .upsert({
                    "cache_key": cache_key,
                    "response_data": response_data,
                    "created_at": datetime.utcnow().isoformat()
                })
                .execute()
            )
            logger.info(f"Cached response for key: {cache_key[:16]}...")
        except Exception as e:
            logger.warning(f"Cache storage failed: {str(e)}")
    
    async def _rate_limit(self):
        """Implement rate limiting"""
        async with self._rate_limiter:
            current_time = asyncio.get_event_loop().time()
            time_since_last = current_time - self._last_request_time
            if time_since_last < (1.0 / self.MAX_REQUESTS_PER_SECOND):
                await asyncio.sleep((1.0 / self.MAX_REQUESTS_PER_SECOND) - time_since_last)
            self._last_request_time = asyncio.get_event_loop().time()
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.NetworkError)),
        reraise=True
    )
    async def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
        use_cache: bool = True
    ) -> Dict[str, Any]:
        """
        Make HTTP request to DataForSEO API with retry logic and caching.
        
        Args:
            method: HTTP method (GET or POST)
            endpoint: API endpoint path
            data: Request payload for POST requests
            use_cache: Whether to use caching
        
        Returns:
            Parsed JSON response
        
        Raises:
            DataForSEORateLimitError: When rate limit is exceeded
            DataForSEOError: For other API errors
        """
        if not self._authenticated:
            await self.authenticate()
        
        # Check cache first
        cache_key = None
        if use_cache and data:
            cache_key = self._generate_cache_key(endpoint, data)
            cached = await self._get_cached_response(cache_key)
            if cached:
                return cached
        
        # Rate limiting
        await self._rate_limit()
        
        async with self._semaphore:
            url = f"{self.BASE_URL}/{endpoint}"
            
            try:
                if method.upper() == "POST":
                    response = await self.http_client.post(url, json=data)
                else:
                    response = await self.http_client.get(url)
                
                # Handle rate limiting
                if response.status_code == 429:
                    logger.warning("Rate limit exceeded, backing off...")
                    raise DataForSEORateLimitError("DataForSEO rate limit exceeded")
                
                # Handle other errors
                if response.status_code >= 400:
                    error_msg = f"DataForSEO API error: {response.status_code} - {response.text}"
                    logger.error(error_msg)
                    raise DataForSEOError(error_msg)
                
                result = response.json()
                
                # Cache successful response
                if use_cache and cache_key and result.get("status_code") == 20000:
                    await self._cache_response(cache_key, result)
                
                return result
                
            except httpx.HTTPError as e:
                logger.error(f"HTTP error during DataForSEO request: {str(e)}")
                raise DataForSEOError(f"Request failed: {str(e)}")
    
    def _classify_serp_features(self, items: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Classify SERP items into feature types and count them.
        
        Args:
            items: List of SERP items from DataForSEO response
        
        Returns:
            Dict with feature counts and details
        """
        features = {}
        feature_details = []
        
        for item in items:
            item_type = item.get("type", "").lower()
            
            # Map to our feature categories
            for feature_name, type_variants in self.SERP_FEATURE_TYPES.items():
                if any(variant in item_type for variant in type_variants):
                    if feature_name not in features:
                        features[feature_name] = 0
                    
                    # Count items (special handling for PAA)
                    if feature_name == "people_also_ask":
                        # Count individual questions
                        questions = item.get("items", [])
                        features[feature_name] += len(questions)
                        feature_details.append({
                            "type": feature_name,
                            "count": len(questions),
                            "rank_group": item.get("rank_group"),
                            "rank_absolute": item.get("rank_absolute")
                        })
                    else:
                        features[feature_name] += 1
                        feature_details.append({
                            "type": feature_name,
                            "rank_group": item.get("rank_group"),
                            "rank_absolute": item.get("rank_absolute")
                        })
                    
                    break
        
        return {
            "feature_counts": features,
            "feature_details": feature_details
        }
    
    def _calculate_visual_position(
        self,
        organic_position: int,
        serp_items: List[Dict[str, Any]]
    ) -> float:
        """
        Calculate visual position accounting for SERP features above the organic result.
        
        Args:
            organic_position: The organic ranking position
            serp_items: All SERP items from the response
        
        Returns:
            Visual position (float)
        """
        visual_displacement = 0.0
        
        for item in serp_items:
            item_rank = item.get("rank_absolute", 999)
            
            # Only count features above our organic position
            if item_rank < organic_position:
                item_type = item.get("type", "").lower()
                
                # Find matching feature type
                for feature_name, type_variants in self.SERP_FEATURE_TYPES.items():
                    if any(variant in item_type for variant in type_variants):
                        impact = self.SERP_FEATURE_VISUAL_IMPACT.get(feature_name, 1.0)
                        
                        # Special handling for PAA (count questions)
                        if feature_name == "people_also_ask":
                            questions = item.get("items", [])
                            visual_displacement += len(questions) * impact
                        else:
                            visual_displacement += impact
                        
                        break
        
        return organic_position + visual_displacement
    
    async def get_serp_data(
        self,
        keyword: str,
        location_code: int = 2840,  # USA
        language_code: str = "en",
        device: str = "desktop",
        depth: int = 100  # Number of results to retrieve
    ) -> Dict[str, Any]:
        """
        Fetch live SERP data for a single keyword.
        
        Args:
            keyword: Search query
            location_code: DataForSEO location code (2840 = USA)
            language_code: Language code (en, es, etc.)
            device: Device type (desktop, mobile, tablet)
            depth: Number of SERP results to retrieve (max 100)
        
        Returns:
            Dict containing:
            - organic_results: List of organic search results
            - serp_features: Detected SERP features
            - total_results: Total number of results
            - keyword_difficulty: If available
            - top_domains: Domains ranking in top 10
        """
        payload = [{
            "keyword": keyword,
            "location_code": location_code,
            "language_code": language_code,
            "device": device,
            "os": "windows" if device == "desktop" else "ios",
            "depth": min(depth, 100)
        }]
        
        response = await self._make_request(
            "POST",
            "serp/google/organic/live/advanced",
            data=payload
        )
        
        if response.get("status_code") != 20000:
            raise DataForSEOError(f"SERP request failed: {response.get('status_message')}")
        
        tasks = response.get("tasks", [])
        if not tasks or not tasks[0].get("result"):
            return {
                "organic_results": [],
                "serp_features": {},
                "total_results": 0,
                "keyword_difficulty": None,
                "top_domains": []
            }
        
        result = tasks[0]["result"][0]
        items = result.get("items", [])
        
        # Extract organic results
        organic_results = []
        all_domains = []
        
        for item in items:
            if item.get("type") == "organic":
                url = item.get("url", "")
                domain = self._extract_domain(url)
                position = item.get("rank_absolute", 0)
                
                # Calculate visual position
                visual_position = self._calculate_visual_position(position, items)
                
                organic_results.append({
                    "position": position,
                    "visual_position": visual_position,
                    "url": url,
                    "domain": domain,
                    "title": item.get("title", ""),
                    "description": item.get("description", ""),
                    "breadcrumb": item.get("breadcrumb", "")
                })
                
                if position <= 10:
                    all_domains.append(domain)
        
        # Classify SERP features
        serp_classification = self._classify_serp_features(items)
        
        # Count domain frequency in top 10
        domain_counts = {}
        for domain in all_domains:
            domain_counts[domain] = domain_counts.get(domain, 0) + 1
        
        top_domains = sorted(
            [{"domain": d, "count": c} for d, c in domain_counts.items()],
            key=lambda x: x["count"],
            reverse=True
        )
        
        return {
            "keyword": keyword,
            "organic_results": organic_results,
            "serp_features": serp_classification["feature_counts"],
            "serp_feature_details": serp_classification["feature_details"],
            "total_results": result.get("items_count", 0),
            "top_domains": top_domains,
            "location_code": location_code,
            "language_code": language_code,
            "device": device,
            "fetched_at": datetime.utcnow().isoformat()
        }
    
    async def get_keyword_metrics(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en"
    ) -> List[Dict[str, Any]]:
        """
        Get keyword metrics including search volume, difficulty, and CPC.
        
        Args:
            keywords: List of keywords to analyze
            location_code: DataForSEO location code
            language_code: Language code
        
        Returns:
            List of dicts with keyword metrics
        """
        # DataForSEO keyword data endpoint
        payload = [{
            "keywords": keywords,
            "location_code": location_code,
            "language_code": language_code
        }]
        
        response = await self._make_request(
            "POST",
            "keywords_data/google_ads/search_volume/live",
            data=payload
        )
        
        if response.get("status_code") != 20000:
            raise DataForSEOError(f"Keyword metrics request failed: {response.get('status_message')}")
        
        tasks = response.get("tasks", [])
        if not tasks or not tasks[0].get("result"):
            return []
        
        results = []
        for item in tasks[0]["result"]:
            results.append({
                "keyword": item.get("keyword"),
                "search_volume": item.get("search_volume"),
                "competition": item.get("competition"),
                "competition_index": item.get("competition_index"),
                "low_top_of_page_bid": item.get("low_top_of_page_bid"),
                "high_top_of_page_bid": item.get("high_top_of_page_bid"),
                "cpc": item.get("cpc"),
                "monthly_searches": item.get("monthly_searches", [])
            })
        
        return results
    
    async def get_competitor_domains(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
        top_n: int = 10
    ) -> Dict[str, Any]:
        """
        Analyze competitor domains across multiple keywords.
        
        Args:
            keywords: List of keywords to analyze
            location_code: DataForSEO location code
            language_code: Language code
            top_n: Number of top competitors to return
        
        Returns:
            Dict containing:
            - competitors: List of competitor domains with frequency and avg position
            - keyword_overlap: Matrix of which competitors appear for which keywords
        """
        # Fetch SERP data for all keywords
        tasks = [
            self.get_serp_data(kw, location_code, language_code)
            for kw in keywords
        ]
        
        serp_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Aggregate competitor data
        competitor_data = {}  # domain -> {keywords: set, positions: list}
        keyword_domain_map = {}  # keyword -> [domains]
        
        for i, result in enumerate(serp_results):
            if isinstance(result, Exception):
                logger.warning(f"Failed to fetch SERP for keyword {keywords[i]}: {str(result)}")
                continue
            
            keyword = keywords[i]
            keyword_domain_map[keyword] = []
            
            for organic in result.get("organic_results", []):
                domain = organic["domain"]
                position = organic["position"]
                
                if domain not in competitor_data:
                    competitor_data[domain] = {
                        "keywords": set(),
                        "positions": [],
                        "urls": []
                    }
                
                competitor_data[domain]["keywords"].add(keyword)
                competitor_data[domain]["positions"].append(position)
                competitor_data[domain]["urls"].append(organic["url"])
                
                if position <= 10:
                    keyword_domain_map[keyword].append(domain)
        
        # Calculate competitor metrics
        competitors = []
        for domain, data in competitor_data.items():
            avg_position = sum(data["positions"]) / len(data["positions"]) if data["positions"] else 0
            keyword_count = len(data["keywords"])
            
            competitors.append({
                "domain": domain,
                "keywords_shared": keyword_count,
                "keyword_overlap_pct": (keyword_count / len(keywords)) * 100 if keywords else 0,
                "avg_position": round(avg_position, 1),
                "best_position": min(data["positions"]) if data["positions"] else None,
                "worst_position": max(data["positions"]) if data["positions"] else None,
                "example_urls": data["urls"][:3]
            })
        
        # Sort by keyword count and take top N
        competitors.sort(key=lambda x: (x["keywords_shared"], -x["avg_position"]), reverse=True)
        top_competitors = competitors[:top_n]
        
        return {
            "competitors": top_competitors,
            "keyword_domain_map": keyword_domain_map,
            "total_keywords_analyzed": len(keywords),
            "total_unique_domains": len(competitor_data)
        }
    
    async def batch_serp_analysis(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
        batch_size: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Fetch SERP data for multiple keywords in batches with rate limiting.
        
        Args:
            keywords: List of keywords to analyze
            location_code: DataForSEO location code
            language_code: Language code
            batch_size: Number of concurrent requests per batch
        
        Returns:
            List of SERP analysis results
        """
        results = []
        
        for i in range(0, len(keywords), batch_size):
            batch = keywords[i:i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}/{(len(keywords) + batch_size - 1)//batch_size}")
            
            tasks = [
                self.get_serp_data(kw, location_code, language_code)
                for kw in batch
            ]
            
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for j, result in enumerate(batch_results):
                if isinstance(result, Exception):
                    logger.error(f"Failed to process keyword '{batch[j]}': {str(result)}")
                    results.append({
                        "keyword": batch[j],
                        "error": str(result),
                        "organic_results": [],
                        "serp_features": {}
                    })
                else:
                    results.append(result)
            
            # Small delay between batches
            if i + batch_size < len(keywords):
                await asyncio.sleep(1)
        
        return results
    
    def _extract_domain(self, url: str) -> str:
        """Extract domain from URL"""
        if not url:
            return ""
        
        # Remove protocol
        domain = re.sub(r'^https?://', '', url)
        # Remove www
        domain = re.sub(r'^www\.', '', domain)
        # Take only domain part (before first /)
        domain = domain.split('/')[0]
        # Remove port if present
        domain = domain.split(':')[0]
        
        return domain.lower()
    
    async def analyze_serp_intent(
        self,
        keyword: str,
        location_code: int = 2840,
        language_code: str = "en"
    ) -> Dict[str, Any]:
        """
        Classify search intent based on SERP features and result types.
        
        Args:
            keyword: Search query to analyze
            location_code: DataForSEO location code
            language_code: Language code
        
        Returns:
            Dict with intent classification and confidence scores
        """
        serp_data = await self.get_serp_data(keyword, location_code, language_code)
        
        features = serp_data.get("serp_features", {})
        organic_results = serp_data.get("organic_results", [])
        
        # Intent scoring
        intent_scores = {
            "informational": 0.0,
            "commercial": 0.0,
            "transactional": 0.0,
            "navigational": 0.0
        }
        
        # SERP feature signals
        if features.get("knowledge_graph", 0) > 0:
            intent_scores["informational"] += 2.0
            intent_scores["navigational"] += 1.0
        
        if features.get("people_also_ask", 0) > 0:
            intent_scores["informational"] += 1.5
        
        if features.get("featured_snippet", 0) > 0:
            intent_scores["informational"] += 1.0
        
        if features.get("shopping", 0) > 0:
            intent_scores["transactional"] += 3.0
        
        if features.get("local_pack", 0) > 0:
            intent_scores["transactional"] += 1.5
        
        if features.get("video", 0) > 0:
            intent_scores["informational"] += 1.0
        
        if features.get("ai_overview", 0) > 0:
            intent_scores["informational"] += 1.5
        
        # Keyword pattern signals
        keyword_lower = keyword.lower()
        
        # Informational patterns
        if any(word in keyword_lower for word in ["how", "what", "why", "when", "where", "guide", "tutorial"]):
            intent_scores["informational"] += 2.0
        
        # Commercial patterns
        if any(word in keyword_lower for word in ["best", "top", "review", "vs", "versus", "comparison", "alternative"]):
            intent_scores["commercial"] += 2.0
        
        # Transactional patterns
        if any(word in keyword_lower for word in ["buy", "price", "cheap", "deal", "discount", "order", "purchase"]):
            intent_scores["transactional"] += 2.0
        
        # Navigational patterns (brand names would require brand list)
        if len(keyword_lower.split()) <= 2 and not any(c in keyword_lower for c in ["?", "how", "what"]):
            intent_scores["navigational"] += 1.0
        
        # Normalize scores
        total_score = sum(intent_scores.values())
        if total_score > 0:
            intent_scores = {k: v / total_score for k, v in intent_scores.items()}
        
        # Determine primary intent
        primary_intent = max(intent_scores.items(), key=lambda x: x[1])
        
        return {
            "keyword": keyword,
            "primary_intent": primary_intent[0],
            "confidence": primary_intent[1],
            "intent_scores": intent_scores,
            "serp_features_present": list(features.keys()),
            "total_organic_results": len(organic_results)
        }