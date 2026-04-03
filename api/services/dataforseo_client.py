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
    
    # Rate limiting: DataForSEO allows 2000 API calls per minute
    RATE_LIMIT_CALLS = 2000
    RATE_LIMIT_PERIOD = 60  # seconds
    
    # Position-based CTR curves (baseline, adjusted by SERP features)
    # Based on Advanced Web Ranking 2023 data
    BASELINE_CTR = {
        1: 0.398, 2: 0.182, 3: 0.105, 4: 0.073, 5: 0.053,
        6: 0.041, 7: 0.033, 8: 0.027, 9: 0.023, 10: 0.020,
        11: 0.017, 12: 0.015, 13: 0.013, 14: 0.012, 15: 0.011,
        16: 0.010, 17: 0.009, 18: 0.008, 19: 0.008, 20: 0.007,
    }
    
    def __init__(self, login: Optional[str] = None, password: Optional[str] = None, cache_client=None):
        """
        Initialize DataForSEO client.
        
        Args:
            login: DataForSEO API login (defaults to DATAFORSEO_LOGIN env var)
            password: DataForSEO API password (defaults to DATAFORSEO_PASSWORD env var)
            cache_client: Optional cache client (Supabase) for response caching
        """
        self.login = login or os.getenv("DATAFORSEO_LOGIN")
        self.password = password or os.getenv("DATAFORSEO_PASSWORD")
        self.cache_client = cache_client
        
        if not self.login or not self.password:
            raise DataForSEOAuthError(
                "DataForSEO credentials not provided. "
                "Set DATAFORSEO_LOGIN and DATAFORSEO_PASSWORD environment variables."
            )
        
        self.client: Optional[httpx.AsyncClient] = None
        self._rate_limit_tokens = self.RATE_LIMIT_CALLS
        self._rate_limit_last_reset = datetime.now()
        self._rate_limit_lock = asyncio.Lock()
    
    async def __aenter__(self):
        """Async context manager entry"""
        await self.authenticate()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close()
    
    async def authenticate(self):
        """Initialize HTTP client with authentication"""
        if self.client is None:
            self.client = httpx.AsyncClient(
                auth=(self.login, self.password),
                timeout=httpx.Timeout(30.0, read=60.0),
                limits=httpx.Limits(max_keepalive_connections=20, max_connections=100),
            )
            logger.info("DataForSEO client authenticated")
    
    async def close(self):
        """Close HTTP client"""
        if self.client:
            await self.client.aclose()
            self.client = None
            logger.info("DataForSEO client closed")
    
    async def _check_rate_limit(self):
        """Check and enforce rate limiting"""
        async with self._rate_limit_lock:
            now = datetime.now()
            elapsed = (now - self._rate_limit_last_reset).total_seconds()
            
            # Reset tokens if period has passed
            if elapsed >= self.RATE_LIMIT_PERIOD:
                self._rate_limit_tokens = self.RATE_LIMIT_CALLS
                self._rate_limit_last_reset = now
            
            # Wait if no tokens available
            if self._rate_limit_tokens <= 0:
                wait_time = self.RATE_LIMIT_PERIOD - elapsed
                if wait_time > 0:
                    logger.warning(f"Rate limit reached, waiting {wait_time:.2f}s")
                    await asyncio.sleep(wait_time)
                    self._rate_limit_tokens = self.RATE_LIMIT_CALLS
                    self._rate_limit_last_reset = datetime.now()
            
            self._rate_limit_tokens -= 1
    
    def _generate_cache_key(self, endpoint: str, params: Dict[str, Any]) -> str:
        """Generate cache key from endpoint and params"""
        cache_str = f"{endpoint}:{json.dumps(params, sort_keys=True)}"
        return hashlib.sha256(cache_str.encode()).hexdigest()
    
    async def _get_cached_response(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """Retrieve cached response if available and not expired"""
        if not self.cache_client:
            return None
        
        try:
            result = self.cache_client.table("dataforseo_cache").select("*").eq("cache_key", cache_key).single().execute()
            
            if result.data:
                expires_at = datetime.fromisoformat(result.data["expires_at"])
                if datetime.now() < expires_at:
                    logger.debug(f"Cache hit for key {cache_key[:16]}...")
                    return result.data["response_data"]
                else:
                    # Delete expired cache entry
                    self.cache_client.table("dataforseo_cache").delete().eq("cache_key", cache_key).execute()
        except Exception as e:
            logger.warning(f"Cache retrieval error: {e}")
        
        return None
    
    async def _cache_response(self, cache_key: str, response_data: Dict[str, Any], ttl_hours: int = 24):
        """Cache API response"""
        if not self.cache_client:
            return
        
        try:
            expires_at = datetime.now() + timedelta(hours=ttl_hours)
            self.cache_client.table("dataforseo_cache").upsert({
                "cache_key": cache_key,
                "response_data": response_data,
                "expires_at": expires_at.isoformat(),
                "created_at": datetime.now().isoformat(),
            }).execute()
            logger.debug(f"Cached response for key {cache_key[:16]}...")
        except Exception as e:
            logger.warning(f"Cache storage error: {e}")
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.NetworkError)),
    )
    async def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[List[Dict[str, Any]]] = None,
        use_cache: bool = True,
        cache_ttl_hours: int = 24,
    ) -> Dict[str, Any]:
        """
        Make HTTP request to DataForSEO API with retries and caching.
        
        Args:
            method: HTTP method (POST, GET)
            endpoint: API endpoint path
            data: Request payload
            use_cache: Whether to use response caching
            cache_ttl_hours: Cache TTL in hours
            
        Returns:
            Parsed JSON response
            
        Raises:
            DataForSEOError: On API errors
            DataForSEORateLimitError: On rate limit errors
            DataForSEOAuthError: On authentication errors
        """
        if not self.client:
            await self.authenticate()
        
        # Check cache first
        cache_key = None
        if use_cache and method == "POST" and data:
            cache_key = self._generate_cache_key(endpoint, data[0] if data else {})
            cached = await self._get_cached_response(cache_key)
            if cached:
                return cached
        
        # Rate limiting
        await self._check_rate_limit()
        
        url = f"{self.BASE_URL}/{endpoint}"
        
        try:
            if method == "POST":
                response = await self.client.post(url, json=data)
            else:
                response = await self.client.get(url)
            
            response.raise_for_status()
            result = response.json()
            
            # Check DataForSEO-specific errors
            if "tasks" in result and result["tasks"]:
                task = result["tasks"][0]
                if task.get("status_code") != 20000:
                    error_msg = task.get("status_message", "Unknown error")
                    if task.get("status_code") == 40101:
                        raise DataForSEOAuthError(f"Authentication failed: {error_msg}")
                    elif task.get("status_code") == 50000:
                        raise DataForSEORateLimitError(f"Rate limit exceeded: {error_msg}")
                    else:
                        raise DataForSEOError(f"API error: {error_msg}")
            
            # Cache successful response
            if use_cache and cache_key and "tasks" in result:
                await self._cache_response(cache_key, result, cache_ttl_hours)
            
            return result
            
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                raise DataForSEOAuthError(f"Authentication failed: {e}")
            elif e.response.status_code == 429:
                raise DataForSEORateLimitError(f"Rate limit exceeded: {e}")
            else:
                raise DataForSEOError(f"HTTP error {e.response.status_code}: {e}")
        except httpx.TimeoutException as e:
            logger.warning(f"Request timeout: {e}")
            raise
        except httpx.NetworkError as e:
            logger.warning(f"Network error: {e}")
            raise
        except Exception as e:
            raise DataForSEOError(f"Unexpected error: {e}")
    
    async def get_serp_results(
        self,
        keywords: List[str],
        location_code: int = 2840,  # United States
        language_code: str = "en",
        device: str = "desktop",
        depth: int = 100,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetch live SERP results for multiple keywords.
        
        Args:
            keywords: List of keywords to fetch SERPs for
            location_code: DataForSEO location code (default: 2840 = US)
            language_code: Language code (default: en)
            device: Device type (desktop, mobile)
            depth: Number of results to fetch per keyword (max 700)
            
        Returns:
            Dictionary mapping keyword to parsed SERP data:
            {
                "keyword": {
                    "organic_results": [...],
                    "serp_features": {...},
                    "total_results": int,
                    "check_url": str
                }
            }
        """
        if not keywords:
            return {}
        
        logger.info(f"Fetching SERP results for {len(keywords)} keywords")
        
        # Prepare batch request (max 100 tasks per request)
        batch_size = 100
        all_results = {}
        
        for i in range(0, len(keywords), batch_size):
            batch = keywords[i:i + batch_size]
            tasks = []
            
            for keyword in batch:
                tasks.append({
                    "keyword": keyword,
                    "location_code": location_code,
                    "language_code": language_code,
                    "device": device,
                    "depth": depth,
                    "calculate_rectangles": True,
                })
            
            try:
                response = await self._make_request(
                    "POST",
                    "serp/google/organic/live/advanced",
                    data=tasks,
                    use_cache=True,
                    cache_ttl_hours=24,
                )
                
                # Parse results
                if "tasks" in response:
                    for task in response["tasks"]:
                        if task.get("status_code") == 20000 and task.get("result"):
                            for result in task["result"]:
                                keyword = result.get("keyword")
                                if keyword:
                                    all_results[keyword] = self._parse_serp_result(result)
                
                logger.info(f"Fetched batch {i // batch_size + 1}/{(len(keywords) - 1) // batch_size + 1}")
                
            except Exception as e:
                logger.error(f"Error fetching SERP batch: {e}")
                # Continue with next batch
        
        return all_results
    
    def _parse_serp_result(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """Parse raw SERP result into clean structure"""
        parsed = {
            "keyword": result.get("keyword"),
            "location_code": result.get("location_code"),
            "language_code": result.get("language_code"),
            "check_url": result.get("check_url"),
            "total_results": result.get("se_results_count", 0),
            "organic_results": [],
            "serp_features": {},
            "serp_items_count": result.get("items_count", 0),
        }
        
        items = result.get("items", [])
        serp_features = {}
        organic_position = 0
        
        for item in items:
            item_type = item.get("type")
            
            # Track organic results
            if item_type == "organic":
                organic_position += 1
                parsed["organic_results"].append({
                    "position": item.get("rank_group"),
                    "organic_position": organic_position,
                    "url": item.get("url"),
                    "domain": item.get("domain"),
                    "title": item.get("title"),
                    "description": item.get("description"),
                    "breadcrumb": item.get("breadcrumb"),
                    "is_amp": item.get("amp_version", False),
                    "rating": item.get("rating"),
                })
            
            # Track SERP features
            else:
                for feature_name, feature_types in self.SERP_FEATURE_TYPES.items():
                    if item_type in feature_types:
                        if feature_name not in serp_features:
                            serp_features[feature_name] = {
                                "present": True,
                                "count": 0,
                                "items": [],
                            }
                        
                        serp_features[feature_name]["count"] += 1
                        serp_features[feature_name]["items"].append({
                            "type": item_type,
                            "title": item.get("title"),
                            "url": item.get("url"),
                            "domain": item.get("domain"),
                            "position": item.get("rank_group"),
                        })
                        
                        # Special handling for PAA
                        if feature_name == "people_also_ask" and "items" in item:
                            paa_questions = [q.get("title") for q in item.get("items", [])]
                            serp_features[feature_name]["questions"] = paa_questions
        
        parsed["serp_features"] = serp_features
        return parsed
    
    async def get_competitor_domains(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
        min_frequency_pct: float = 0.10,
    ) -> List[Dict[str, Any]]:
        """
        Identify competitor domains ranking across multiple keywords.
        
        Args:
            keywords: List of keywords to analyze
            location_code: DataForSEO location code
            language_code: Language code
            min_frequency_pct: Minimum frequency threshold (0-1)
            
        Returns:
            List of competitor domains with stats:
            [
                {
                    "domain": "competitor.com",
                    "keywords_count": 34,
                    "frequency_pct": 0.39,
                    "avg_position": 4.2,
                    "positions": [1, 3, 5, ...],
                    "keywords": ["keyword1", "keyword2", ...]
                }
            ]
        """
        # Fetch SERP results
        serp_results = await self.get_serp_results(
            keywords, location_code, language_code
        )
        
        # Count domain appearances
        domain_stats: Dict[str, Dict[str, Any]] = {}
        
        for keyword, data in serp_results.items():
            for result in data["organic_results"]:
                domain = result["domain"]
                
                if domain not in domain_stats:
                    domain_stats[domain] = {
                        "domain": domain,
                        "keywords": [],
                        "positions": [],
                    }
                
                domain_stats[domain]["keywords"].append(keyword)
                domain_stats[domain]["positions"].append(result["position"])
        
        # Calculate stats and filter
        competitors = []
        total_keywords = len(keywords)
        
        for domain, stats in domain_stats.items():
            frequency_pct = len(stats["keywords"]) / total_keywords
            
            if frequency_pct >= min_frequency_pct:
                competitors.append({
                    "domain": domain,
                    "keywords_count": len(stats["keywords"]),
                    "frequency_pct": frequency_pct,
                    "avg_position": sum(stats["positions"]) / len(stats["positions"]),
                    "min_position": min(stats["positions"]),
                    "max_position": max(stats["positions"]),
                    "keywords": stats["keywords"],
                    "positions": stats["positions"],
                })
        
        # Sort by frequency and avg position
        competitors.sort(key=lambda x: (-x["frequency_pct"], x["avg_position"]))
        
        logger.info(f"Identified {len(competitors)} competitor domains")
        return competitors
    
    async def get_serp_features(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
    ) -> Dict[str, Dict[str, Any]]:
        """
        Extract SERP features for keywords (featured snippets, PAA, etc).
        
        Args:
            keywords: List of keywords to analyze
            location_code: DataForSEO location code
            language_code: Language code
            
        Returns:
            Dictionary mapping keyword to SERP features:
            {
                "keyword": {
                    "featured_snippet": {...},
                    "people_also_ask": {...},
                    "knowledge_graph": {...},
                    ...
                }
            }
        """
        serp_results = await self.get_serp_results(
            keywords, location_code, language_code
        )
        
        features_by_keyword = {}
        
        for keyword, data in serp_results.items():
            features_by_keyword[keyword] = data["serp_features"]
        
        return features_by_keyword
    
    def calculate_visual_position(
        self,
        organic_position: int,
        serp_features: Dict[str, Any],
    ) -> float:
        """
        Calculate visual position accounting for SERP features above organic result.
        
        Args:
            organic_position: Organic ranking position (1-based)
            serp_features: SERP features dict from get_serp_features
            
        Returns:
            Adjusted visual position (higher = further down page)
        """
        visual_position = float(organic_position)
        
        for feature_name, feature_data in serp_features.items():
            if not feature_data.get("present"):
                continue
            
            impact = self.SERP_FEATURE_VISUAL_IMPACT.get(feature_name, 0)
            
            # For PAA, multiply impact by question count
            if feature_name == "people_also_ask":
                count = len(feature_data.get("questions", []))
                visual_position += impact * count
            else:
                visual_position += impact * feature_data.get("count", 1)
        
        return visual_position
    
    def calculate_ctr_opportunity(
        self,
        positions: List[int],
        volumes: List[int],
        serp_features_list: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """
        Calculate CTR opportunity for a list of keyword positions and volumes.
        
        Args:
            positions: List of current organic positions
            volumes: List of search volumes for each keyword
            serp_features_list: Optional list of SERP features per keyword
            
        Returns:
            {
                "current_ctr": 0.042,
                "current_estimated_clicks": 1250,
                "potential_ctr_top5": 0.073,
                "potential_clicks_top5": 2190,
                "click_opportunity": 940,
                "keywords_analyzed": 87
            }
        """
        if len(positions) != len(volumes):
            raise ValueError("positions and volumes must have same length")
        
        if serp_features_list and len(serp_features_list) != len(positions):
            raise ValueError("serp_features_list must match positions length")
        
        total_current_clicks = 0
        total_potential_clicks_top5 = 0
        total_volume = sum(volumes)
        
        for i, (position, volume) in enumerate(zip(positions, volumes)):
            # Get baseline CTR
            current_ctr = self.BASELINE_CTR.get(position, 0.005)
            
            # Adjust for SERP features if provided
            if serp_features_list:
                visual_position = self.calculate_visual_position(
                    position, serp_features_list[i]
                )
                # Reduce CTR proportionally to visual displacement
                if visual_position > position:
                    displacement_factor = position / visual_position
                    current_ctr *= displacement_factor
            
            # Calculate current clicks
            current_clicks = volume * current_ctr
            total_current_clicks += current_clicks
            
            # Calculate potential at top 5
            potential_ctr = sum(self.BASELINE_CTR.get(p, 0.005) for p in range(1, 6)) / 5
            potential_clicks = volume * potential_ctr
            total_potential_clicks_top5 += potential_clicks
        
        return {
            "current_ctr": total_current_clicks / total_volume if total_volume > 0 else 0,
            "current_estimated_clicks": int(total_current_clicks),
            "potential_ctr_top5": total_potential_clicks_top5 / total_volume if total_volume > 0 else 0,
            "potential_clicks_top5": int(total_potential_clicks_top5),
            "click_opportunity": int(total_potential_clicks_top5 - total_current_clicks),
            "keywords_analyzed": len(positions),
            "total_search_volume": total_volume,
        }
    
    def classify_serp_intent(self, serp_features: Dict[str, Any]) -> str:
        """
        Classify search intent based on SERP feature composition.
        
        Args:
            serp_features: SERP features dict
            
        Returns:
            One of: "informational", "commercial", "transactional", "navigational"
        """
        # Transactional signals
        if serp_features.get("shopping", {}).get("present"):
            return "transactional"
        
        # Navigational signals
        if serp_features.get("knowledge_graph", {}).get("present"):
            kg_items = serp_features["knowledge_graph"].get("items", [])
            if kg_items and any("wikipedia" not in item.get("url", "").lower() for item in kg_items):
                return "navigational"
        
        # Commercial signals
        commercial_features = ["video", "image", "local_pack"]
        commercial_count = sum(
            1 for f in commercial_features if serp_features.get(f, {}).get("present")
        )
        if commercial_count >= 2:
            return "commercial"
        
        # Informational signals (default)
        if (serp_features.get("featured_snippet", {}).get("present") or
            serp_features.get("people_also_ask", {}).get("present")):
            return "informational"
        
        # Default to informational
        return "informational"
    
    async def batch_process_keywords(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
        include_competitors: bool = True,
        include_features: bool = True,
        include_ctr: bool = True,
        volumes: Optional[List[int]] = None,
    ) -> Dict[str, Any]:
        """
        Process multiple keywords and return comprehensive analysis.
        
        Args:
            keywords: List of keywords to analyze
            location_code: DataForSEO location code
            language_code: Language code
            include_competitors: Whether to analyze competitors
            include_features: Whether to extract SERP features
            include_ctr: Whether to calculate CTR opportunity
            volumes: Optional search volumes for CTR calculation
            
        Returns:
            {
                "serp_results": {...},
                "competitors": [...],
                "serp_features": {...},
                "ctr_opportunity": {...},
                "summary": {...}
            }
        """
        result = {
            "keywords_analyzed": len(keywords),
            "location_code": location_code,
            "language_code": language_code,
        }
        
        # Fetch SERP results
        serp_results = await self.get_serp_results(keywords, location_code, language_code)
        result["serp_results"] = serp_results
        
        # Extract competitors
        if include_competitors:
            competitors = await self.get_competitor_domains(keywords, location_code, language_code)
            result["competitors"] = competitors
        
        # Extract SERP features
        if include_features:
            features = await self.get_serp_features(keywords, location_code, language_code)
            result["serp_features"] = features
            
            # Classify intent for each keyword
            intent_distribution = {}
            for keyword, feature_data in features.items():
                intent = self.classify_serp_intent(feature_data)
                intent_distribution[intent] = intent_distribution.get(intent, 0) + 1
            
            result["intent_distribution"] = intent_distribution
        
        # Calculate CTR opportunity
        if include_ctr and volumes and len(volumes) == len(keywords):
            positions = []
            serp_features_list = []
            
            for keyword in keywords:
                serp_data = serp_results.get(keyword, {})
                organic = serp_data.get("organic_results", [])
                if organic:
                    positions.append(organic[0]["position"])
                    if include_features:
                        serp_features_list.append(serp_data.get("serp_features", {}))
                else:
                    positions.append(100)  # Not ranking
                    serp_features_list.append({})
            
            ctr_data = self.calculate_ctr_opportunity(
                positions,
                volumes,
                serp_features_list if include_features else None,
            )
            result["ctr_opportunity"] = ctr_data
        
        # Generate summary
        result["summary"] = self._generate_summary(result)
        
        return result
    
    def _generate_summary(self, analysis_result: Dict[str, Any]) -> Dict[str, Any]:
        """Generate summary statistics from analysis result"""
        summary = {
            "keywords_analyzed": analysis_result["keywords_analyzed"],
        }
        
        # SERP feature prevalence
        if "serp_features" in analysis_result:
            feature_counts = {}
            for keyword, features in analysis_result["serp_features"].items():
                for feature_name, feature_data in features.items():
                    if feature_data.get("present"):
                        feature_counts[feature_name] = feature_counts.get(feature_name, 0) + 1
            
            summary["serp_feature_prevalence"] = {
                name: count / analysis_result["keywords_analyzed"]
                for name, count in feature_counts.items()
            }
        
        # Competitor summary
        if "competitors" in analysis_result:
            competitors = analysis_result["competitors"]
            summary["top_competitors"] = competitors[:5]
            summary["total_competitor_domains"] = len(competitors)
        
        # CTR summary
        if "ctr_opportunity" in analysis_result:
            summary["ctr_opportunity"] = analysis_result["ctr_opportunity"]
        
        # Intent distribution
        if "intent_distribution" in analysis_result:
            summary["intent_distribution"] = analysis_result["intent_distribution"]
        
        return summary