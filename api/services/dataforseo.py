import os
import time
import logging
from typing import Dict, List, Optional, Any, Union
from enum import Enum
from dataclasses import dataclass, asdict
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime, timedelta
import json
from collections import defaultdict

logger = logging.getLogger(__name__)


class DataForSEOEndpoint(Enum):
    """Available DataForSEO API endpoints"""
    SERP_LIVE = "/v3/serp/google/organic/live/advanced"
    SERP_TASK_POST = "/v3/serp/google/organic/task_post"
    SERP_TASK_GET = "/v3/serp/google/organic/task_get/advanced/{task_id}"
    KEYWORDS_FOR_SITE = "/v3/dataforseo_labs/google/keywords_for_site/live"
    RANKED_KEYWORDS = "/v3/dataforseo_labs/google/ranked_keywords/live"
    KEYWORD_DIFFICULTY = "/v3/dataforseo_labs/google/bulk_keyword_difficulty/live"
    SERP_COMPETITORS = "/v3/dataforseo_labs/google/serp_competitors/live"
    DOMAIN_RANK_OVERVIEW = "/v3/dataforseo_labs/google/domain_rank_overview/live"
    HISTORICAL_SERPS = "/v3/dataforseo_labs/google/historical_serps/live"


class RateLimitError(Exception):
    """Raised when rate limit is exceeded"""
    pass


class DataForSEOError(Exception):
    """Base exception for DataForSEO API errors"""
    pass


@dataclass
class SERPFeature:
    """Represents a SERP feature on the page"""
    type: str
    position: int
    title: Optional[str] = None
    description: Optional[str] = None
    url: Optional[str] = None
    rank_absolute: Optional[int] = None
    items_count: Optional[int] = None  # For PAA, related searches, etc.
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class OrganicResult:
    """Represents an organic search result"""
    url: str
    domain: str
    title: str
    description: Optional[str]
    position: int
    rank_absolute: int
    is_featured_snippet: bool = False
    breadcrumb: Optional[str] = None
    website_name: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class SERPAnalysis:
    """Complete SERP analysis for a keyword"""
    keyword: str
    location_code: int
    language_code: str
    se_results_count: int
    organic_results: List[OrganicResult]
    serp_features: List[SERPFeature]
    people_also_ask: List[Dict[str, Any]]
    related_searches: List[str]
    visual_position_map: Dict[int, int]  # organic_position -> visual_position
    timestamp: datetime
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "keyword": self.keyword,
            "location_code": self.location_code,
            "language_code": self.language_code,
            "se_results_count": self.se_results_count,
            "organic_results": [r.to_dict() for r in self.organic_results],
            "serp_features": [f.to_dict() for f in self.serp_features],
            "people_also_ask": self.people_also_ask,
            "related_searches": self.related_searches,
            "visual_position_map": self.visual_position_map,
            "timestamp": self.timestamp.isoformat()
        }


@dataclass
class KeywordMetrics:
    """Keyword difficulty and search volume data"""
    keyword: str
    keyword_difficulty: Optional[int]  # 0-100 scale
    search_volume: Optional[int]
    cpc: Optional[float]
    competition: Optional[float]  # 0-1 scale
    monthly_searches: Optional[List[Dict[str, Any]]]  # Historical search volumes
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class CompetitorDomain:
    """Competitor domain ranking data"""
    domain: str
    keywords_count: int
    avg_position: float
    etv: float  # Estimated traffic value
    intersections: int  # Number of shared keywords with target domain
    full_domain_metrics: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class DataForSEOClient:
    """
    Complete DataForSEO API client for Search Intelligence Report.
    
    Provides methods for:
    - SERP data retrieval with full feature extraction
    - Competitor analysis across keyword sets
    - SERP feature displacement calculation
    - CTR opportunity scoring based on SERP composition
    - Keyword difficulty and search volume
    
    Handles authentication, rate limiting, retries, and response parsing.
    """
    
    BASE_URL = "https://api.dataforseo.com"
    
    # Cost per request (approximate, in USD)
    COSTS = {
        "serp_live": 0.002,
        "serp_task": 0.002,
        "keywords_for_site": 0.01,
        "ranked_keywords": 0.01,
        "keyword_difficulty": 0.0025,
        "serp_competitors": 0.01,
        "domain_rank_overview": 0.01,
        "historical_serps": 0.01,
    }
    
    # Visual position weights for SERP features (how much space they take)
    FEATURE_WEIGHTS = {
        "featured_snippet": 2.0,
        "knowledge_panel": 1.5,
        "local_pack": 1.5,
        "people_also_ask": 0.5,  # per question
        "ai_overview": 2.5,
        "video": 1.0,
        "images": 0.5,
        "top_stories": 1.0,
        "shopping_results": 1.0,
        "twitter": 0.5,
        "recipes": 1.0,
        "find_results_on": 0.5,
        "related_searches": 0.3,
        "carousel": 1.0,
    }
    
    def __init__(
        self,
        login: Optional[str] = None,
        password: Optional[str] = None,
        rate_limit_per_minute: int = 2000,
        enable_cache: bool = True
    ):
        """
        Initialize DataForSEO client.
        
        Args:
            login: API login (defaults to DATAFORSEO_LOGIN env var)
            password: API password (defaults to DATAFORSEO_PASSWORD env var)
            rate_limit_per_minute: Max requests per minute
            enable_cache: Whether to cache responses
        """
        self.login = login or os.getenv("DATAFORSEO_LOGIN")
        self.password = password or os.getenv("DATAFORSEO_PASSWORD")
        
        if not self.login or not self.password:
            raise ValueError(
                "DataForSEO credentials not provided. "
                "Set DATAFORSEO_LOGIN and DATAFORSEO_PASSWORD environment variables "
                "or pass credentials to constructor."
            )
        
        self.rate_limit_per_minute = rate_limit_per_minute
        self.enable_cache = enable_cache
        
        # Rate limiting tracking
        self._request_times: List[float] = []
        self._lock = None  # Will be set if threading is needed
        
        # Setup session with retry logic
        self.session = self._create_session()
        
        # Request cache (in-memory for now)
        self._cache: Dict[str, Any] = {}
        
        logger.info("DataForSEO client initialized")
    
    def _create_session(self) -> requests.Session:
        """Create requests session with retry logic"""
        session = requests.Session()
        
        # Configure retries
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Set auth
        session.auth = (self.login, self.password)
        
        # Set headers
        session.headers.update({
            "Content-Type": "application/json",
            "User-Agent": "SearchIntelligenceReport/1.0"
        })
        
        return session
    
    def _check_rate_limit(self):
        """Check and enforce rate limiting"""
        now = time.time()
        
        # Remove requests older than 1 minute
        cutoff = now - 60
        self._request_times = [t for t in self._request_times if t > cutoff]
        
        # Check if we're at the limit
        if len(self._request_times) >= self.rate_limit_per_minute:
            # Calculate how long to wait
            oldest_request = self._request_times[0]
            wait_time = 60 - (now - oldest_request)
            
            if wait_time > 0:
                logger.warning(f"Rate limit reached, waiting {wait_time:.2f}s")
                time.sleep(wait_time)
                # Clear old requests after waiting
                now = time.time()
                cutoff = now - 60
                self._request_times = [t for t in self._request_times if t > cutoff]
        
        # Record this request
        self._request_times.append(now)
    
    def _get_cache_key(self, endpoint: str, payload: Dict) -> str:
        """Generate cache key for request"""
        # Create a deterministic key from endpoint and payload
        payload_str = json.dumps(payload, sort_keys=True)
        return f"{endpoint}:{payload_str}"
    
    def _make_request(
        self,
        endpoint: str,
        payload: List[Dict[str, Any]],
        use_cache: bool = True
    ) -> Dict[str, Any]:
        """
        Make API request to DataForSEO.
        
        Args:
            endpoint: API endpoint path
            payload: Request payload (list of task objects)
            use_cache: Whether to use cached response if available
            
        Returns:
            API response as dict
            
        Raises:
            RateLimitError: If rate limit is exceeded
            DataForSEOError: If API returns error
        """
        # Check cache
        if use_cache and self.enable_cache:
            cache_key = self._get_cache_key(endpoint, payload[0] if payload else {})
            if cache_key in self._cache:
                logger.debug(f"Cache hit for {endpoint}")
                return self._cache[cache_key]
        
        # Check rate limit
        self._check_rate_limit()
        
        # Make request
        url = f"{self.BASE_URL}{endpoint}"
        
        try:
            response = self.session.post(url, json=payload)
            response.raise_for_status()
            
            data = response.json()
            
            # Check for API-level errors
            if data.get("status_code") == 40100:
                raise RateLimitError("DataForSEO rate limit exceeded")
            
            if data.get("status_code") != 20000:
                error_msg = data.get("status_message", "Unknown error")
                raise DataForSEOError(f"API error: {error_msg}")
            
            # Cache successful response
            if use_cache and self.enable_cache:
                self._cache[cache_key] = data
            
            return data
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                raise RateLimitError("HTTP 429: Rate limit exceeded")
            raise DataForSEOError(f"HTTP error: {e}")
        except requests.exceptions.RequestException as e:
            raise DataForSEOError(f"Request failed: {e}")
    
    def get_serp_data(
        self,
        keywords: List[str],
        location_code: int = 2840,  # United States
        language_code: str = "en",
        device: str = "desktop",
        depth: int = 100  # Number of results to retrieve
    ) -> List[SERPAnalysis]:
        """
        Get live SERP data for multiple keywords.
        
        Args:
            keywords: List of keywords to analyze
            location_code: DataForSEO location code (2840 = US)
            language_code: Language code (en, es, etc.)
            device: Device type (desktop, mobile)
            depth: Number of results to retrieve (max 100)
            
        Returns:
            List of SERPAnalysis objects
        """
        logger.info(f"Fetching SERP data for {len(keywords)} keywords")
        
        results = []
        
        # Process in batches of 100 (API limit)
        batch_size = 100
        for i in range(0, len(keywords), batch_size):
            batch = keywords[i:i+batch_size]
            
            # Build payload
            payload = []
            for keyword in batch:
                payload.append({
                    "keyword": keyword,
                    "location_code": location_code,
                    "language_code": language_code,
                    "device": device,
                    "os": "windows" if device == "desktop" else "ios",
                    "depth": depth,
                    "calculate_rectangles": True  # Get position data
                })
            
            # Make request
            response = self._make_request(
                DataForSEOEndpoint.SERP_LIVE.value,
                payload
            )
            
            # Parse results
            for task in response.get("tasks", []):
                if task.get("status_code") == 20000:
                    result_data = task.get("result", [{}])[0]
                    keyword_data = task.get("data", {}).get("keyword", "")
                    
                    analysis = self._parse_serp_response(
                        result_data,
                        keyword_data,
                        location_code,
                        language_code
                    )
                    
                    if analysis:
                        results.append(analysis)
                else:
                    error_msg = task.get("status_message", "Unknown error")
                    logger.error(f"SERP task failed for keyword: {error_msg}")
        
        logger.info(f"Successfully retrieved SERP data for {len(results)} keywords")
        return results
    
    def _parse_serp_response(
        self,
        result: Dict[str, Any],
        keyword: str,
        location_code: int,
        language_code: str
    ) -> Optional[SERPAnalysis]:
        """Parse SERP API response into SERPAnalysis object"""
        if not result:
            return None
        
        # Extract organic results
        organic_results = []
        items = result.get("items", [])
        
        for item in items:
            item_type = item.get("type", "")
            
            if item_type == "organic":
                organic_results.append(OrganicResult(
                    url=item.get("url", ""),
                    domain=item.get("domain", ""),
                    title=item.get("title", ""),
                    description=item.get("description"),
                    position=item.get("rank_group", 0),
                    rank_absolute=item.get("rank_absolute", 0),
                    is_featured_snippet=False,
                    breadcrumb=item.get("breadcrumb"),
                    website_name=item.get("website_name")
                ))
        
        # Extract SERP features
        serp_features = []
        people_also_ask = []
        related_searches = []
        
        for item in items:
            item_type = item.get("type", "")
            rank_absolute = item.get("rank_absolute", 0)
            
            if item_type == "featured_snippet":
                serp_features.append(SERPFeature(
                    type="featured_snippet",
                    position=item.get("rank_group", 0),
                    title=item.get("title"),
                    description=item.get("description"),
                    url=item.get("url"),
                    rank_absolute=rank_absolute
                ))
                # Mark the organic result as featured snippet if URL matches
                for org in organic_results:
                    if org.url == item.get("url"):
                        org.is_featured_snippet = True
            
            elif item_type == "knowledge_panel":
                serp_features.append(SERPFeature(
                    type="knowledge_panel",
                    position=item.get("rank_group", 0),
                    title=item.get("title"),
                    description=item.get("description"),
                    rank_absolute=rank_absolute
                ))
            
            elif item_type == "local_pack":
                serp_features.append(SERPFeature(
                    type="local_pack",
                    position=item.get("rank_group", 0),
                    title="Local Pack",
                    items_count=len(item.get("items", [])),
                    rank_absolute=rank_absolute
                ))
            
            elif item_type == "people_also_ask":
                paa_items = item.get("items", [])
                serp_features.append(SERPFeature(
                    type="people_also_ask",
                    position=item.get("rank_group", 0),
                    title="People Also Ask",
                    items_count=len(paa_items),
                    rank_absolute=rank_absolute
                ))
                # Store PAA questions
                for paa in paa_items:
                    people_also_ask.append({
                        "question": paa.get("title", ""),
                        "url": paa.get("url"),
                        "domain": paa.get("domain")
                    })
            
            elif item_type == "top_stories":
                serp_features.append(SERPFeature(
                    type="top_stories",
                    position=item.get("rank_group", 0),
                    title="Top Stories",
                    items_count=len(item.get("items", [])),
                    rank_absolute=rank_absolute
                ))
            
            elif item_type == "video":
                serp_features.append(SERPFeature(
                    type="video",
                    position=item.get("rank_group", 0),
                    title=item.get("title"),
                    url=item.get("url"),
                    rank_absolute=rank_absolute
                ))
            
            elif item_type == "images":
                serp_features.append(SERPFeature(
                    type="images",
                    position=item.get("rank_group", 0),
                    title="Image Pack",
                    items_count=len(item.get("items", [])),
                    rank_absolute=rank_absolute
                ))
            
            elif item_type == "shopping":
                serp_features.append(SERPFeature(
                    type="shopping_results",
                    position=item.get("rank_group", 0),
                    title="Shopping Results",
                    items_count=len(item.get("items", [])),
                    rank_absolute=rank_absolute
                ))
            
            elif item_type == "related_searches":
                for rel in item.get("items", []):
                    related_searches.append(rel.get("title", ""))
            
            elif item_type == "ai_overview" or item_type == "generative_ai":
                serp_features.append(SERPFeature(
                    type="ai_overview",
                    position=item.get("rank_group", 0),
                    title="AI Overview",
                    description=item.get("text"),
                    rank_absolute=rank_absolute
                ))
            
            elif item_type == "carousel":
                serp_features.append(SERPFeature(
                    type="carousel",
                    position=item.get("rank_group", 0),
                    title="Carousel",
                    items_count=len(item.get("items", [])),
                    rank_absolute=rank_absolute
                ))
            
            elif item_type == "twitter":
                serp_features.append(SERPFeature(
                    type="twitter",
                    position=item.get("rank_group", 0),
                    title="Twitter Results",
                    items_count=len(item.get("items", [])),
                    rank_absolute=rank_absolute
                ))
            
            elif item_type == "find_results_on":
                serp_features.append(SERPFeature(
                    type="find_results_on",
                    position=item.get("rank_group", 0),
                    title="Find Results On",
                    rank_absolute=rank_absolute
                ))
            
            elif item_type == "recipes":
                serp_features.append(SERPFeature(
                    type="recipes",
                    position=item.get("rank_group", 0),
                    title="Recipes",
                    items_count=len(item.get("items", [])),
                    rank_absolute=rank_absolute
                ))
        
        # Calculate visual position map
        visual_position_map = self._calculate_visual_positions(
            organic_results,
            serp_features
        )
        
        return SERPAnalysis(
            keyword=keyword,
            location_code=location_code,
            language_code=language_code,
            se_results_count=result.get("se_results_count", 0),
            organic_results=organic_results,
            serp_features=serp_features,
            people_also_ask=people_also_ask,
            related_searches=related_searches,
            visual_position_map=visual_position_map,
            timestamp=datetime.utcnow()
        )
    
    def _calculate_visual_positions(
        self,
        organic_results: List[OrganicResult],
        serp_features: List[SERPFeature]
    ) -> Dict[int, int]:
        """
        Calculate visual position (accounting for SERP features) for each organic position.
        
        Returns dict mapping organic_position -> visual_position
        """
        visual_map = {}
        
        # Sort all elements by rank_absolute
        all_elements = []
        
        # Add organic results
        for org in organic_results:
            all_elements.append({
                "type": "organic",
                "rank_absolute": org.rank_absolute,
                "position": org.position,
                "weight": 1.0
            })
        
        # Add SERP features
        for feature in serp_features:
            if feature.rank_absolute:
                weight = self.FEATURE_WEIGHTS.get(feature.type, 1.0)
                
                # PAA weight is per question
                if feature.type == "people_also_ask" and feature.items_count:
                    weight = weight * feature.items_count
                
                all_elements.append({
                    "type": "feature",
                    "feature_type": feature.type,
                    "rank_absolute": feature.rank_absolute,
                    "weight": weight
                })
        
        # Sort by rank_absolute
        all_elements.sort(key=lambda x: x["rank_absolute"])
        
        # Calculate visual positions
        visual_position = 0
        for element in all_elements:
            if element["type"] == "organic":
                visual_position += 1
                visual_map[element["position"]] = visual_position
            else:
                # Feature takes up space but doesn't get a position
                visual_position += element["weight"]
        
        return visual_map
    
    def get_keyword_metrics(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en"
    ) -> List[KeywordMetrics]:
        """
        Get keyword difficulty and search volume for keywords.
        
        Args:
            keywords: List of keywords
            location_code: DataForSEO location code
            language_code: Language code
            
        Returns:
            List of KeywordMetrics objects
        """
        logger.info(f"Fetching keyword metrics for {len(keywords)} keywords")
        
        results = []
        
        # Process in batches of 1000 (API limit)
        batch_size = 1000
        for i in range(0, len(keywords), batch_size):
            batch = keywords[i:i+batch_size]
            
            payload = [{
                "keywords": batch,
                "location_code": location_code,
                "language_code": language_code
            }]
            
            response = self._make_request(
                DataForSEOEndpoint.KEYWORD_DIFFICULTY.value,
                payload
            )
            
            for task in response.get("tasks", []):
                if task.get("status_code") == 20000:
                    result_data = task.get("result", [])
                    
                    for item in result_data:
                        keyword_info = item.get("keyword_info", {})
                        
                        results.append(KeywordMetrics(
                            keyword=item.get("keyword", ""),
                            keyword_difficulty=item.get("keyword_difficulty"),
                            search_volume=keyword_info.get("search_volume"),
                            cpc=keyword_info.get("cpc"),
                            competition=keyword_info.get("competition"),
                            monthly_searches=keyword_info.get("monthly_searches", [])
                        ))
        
        logger.info(f"Retrieved metrics for {len(results)} keywords")
        return results
    
    def get_competitor_domains(
        self,
        target_domain: str,
        location_code: int = 2840,
        language_code: str = "en",
        limit: int = 100
    ) -> List[CompetitorDomain]:
        """
        Get competitor domains that rank for similar keywords.
        
        Args:
            target_domain: Domain to analyze
            location_code: DataForSEO location code
            language_code: Language code
            limit: Max number of competitors to return
            
        Returns:
            List of CompetitorDomain objects
        """
        logger.info(f"Fetching competitor data for {target_domain}")
        
        payload = [{
            "target": target_domain,
            "location_code": location_code,
            "language_code": language_code,
            "limit": limit
        }]
        
        response = self._make_request(
            DataForSEOEndpoint.SERP_COMPETITORS.value,
            payload
        )
        
        results = []
        
        for task in response.get("tasks", []):
            if task.get("status_code") == 20000:
                items = task.get("result", [{}])[0].get("items", [])
                
                for item in items:
                    se_results = item.get("se_results_count", 0)
                    avg_pos = item.get("avg_position", 0)
                    
                    results.append(CompetitorDomain(
                        domain=item.get("domain", ""),
                        keywords_count=se_results,
                        avg_position=avg_pos,
                        etv=item.get("etv", 0),
                        intersections=item.get("intersections", 0),
                        full_domain_metrics=item.get("metrics", {})
                    ))
        
        # Sort by intersection count (most relevant competitors first)
        results.sort(key=lambda x: x.intersections, reverse=True)
        
        logger.info(f"Found {len(results)} competitor domains")
        return results
    
    def get_domain_rankings(
        self,
        domain: str,
        location_code: int = 2840,
        language_code: str = "en"
    ) -> Dict[str, Any]:
        """
        Get comprehensive ranking data for a domain.
        
        Args:
            domain: Domain to analyze
            location_code: DataForSEO location code
            language_code: Language code
            
        Returns:
            Dict with ranking metrics
        """
        logger.info(f"Fetching domain ranking overview for {domain}")
        
        payload = [{
            "target": domain,
            "location_code": location_code,
            "language_code": language_code
        }]
        
        response = self._make_request(
            DataForSEOEndpoint.DOMAIN_RANK_OVERVIEW.value,
            payload
        )
        
        for task in response.get("tasks", []):
            if task.get("status_code") == 20000:
                result = task.get("result", [{}])[0]
                metrics = result.get("metrics", {})
                
                return {
                    "domain": domain,
                    "organic_keywords": metrics.get("organic", {}).get("count", 0),
                    "organic_etv": metrics.get("organic", {}).get("etv", 0),
                    "organic_impressions_etv": metrics.get("organic", {}).get("impressions_etv", 0),
                    "organic_estimated_paid_traffic_cost": metrics.get("organic", {}).get("estimated_paid_traffic_cost", 0),
                    "paid_keywords": metrics.get("paid", {}).get("count", 0),
                    "is_new": metrics.get("organic", {}).get("is_new"),
                    "is_up": metrics.get("organic", {}).get("is_up"),
                    "is_down": metrics.get("organic", {}).get("is_down"),
                    "is_lost": metrics.get("organic", {}).get("is_lost")
                }
        
        return {}
    
    def get_ranked_keywords_for_domain(
        self,
        domain: str,
        location_code: int = 2840,
        language_code: str = "en",
        limit: int = 1000,
        filters: Optional[List[Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Get all keywords a domain ranks for.
        
        Args:
            domain: Domain to analyze
            location_code: DataForSEO location code
            language_code: Language code
            limit: Max keywords to return
            filters: Optional filters (e.g., position range, search volume)
            
        Returns:
            List of keyword ranking data dicts
        """
        logger.info(f"Fetching ranked keywords for {domain}")
        
        payload = [{
            "target": domain,
            "location_code": location_code,
            "language_code": language_code,
            "limit": limit
        }]
        
        if filters:
            payload[0]["filters"] = filters
        
        response = self._make_request(
            DataForSEOEndpoint.RANKED_KEYWORDS.value,
            payload
        )
        
        results = []
        
        for task in response.get("tasks", []):
            if task.get("status_code") == 20000:
                items = task.get("result", [{}])[0].get("items", [])
                
                for item in items:
                    keyword_data = item.get("keyword_data", {})
                    ranked_serp_element = item.get("ranked_serp_element", {})
                    
                    results.append({
                        "keyword": keyword_data.get("keyword", ""),
                        "search_volume": keyword_data.get("keyword_info", {}).get("search_volume"),
                        "cpc": keyword_data.get("keyword_info", {}).get("cpc"),
                        "competition": keyword_data.get("keyword_info", {}).get("competition"),
                        "position": ranked_serp_element.get("serp_item", {}).get("rank_absolute"),
                        "url": ranked_serp_element.get("serp_item", {}).get("url"),
                        "etv": ranked_serp_element.get("etv"),
                        "impressions_etv": ranked_serp_element.get("impressions_etv"),
                        "estimated_paid_traffic_cost": ranked_serp_element.get("estimated_paid_traffic_cost")
                    })
        
        logger.info(f"Retrieved {len(results)} ranked keywords for {domain}")
        return results
    
    def analyze_serp_features_displacement(
        self,
        serp_analysis: SERPAnalysis,
        target_url: str
    ) -> Dict[str, Any]:
        """
        Analyze how SERP features affect the visual position of a target URL.
        
        Args:
            serp_analysis: SERPAnalysis object
            target_url: URL to analyze (can be partial match)
            
        Returns:
            Dict with displacement analysis
        """
        # Find the target URL in organic results
        target_result = None
        for result in serp_analysis.organic_results:
            if target_url.lower() in result.url.lower():
                target_result = result
                break
        
        if not target_result:
            return {
                "found": False,
                "error": "Target URL not found in SERP results"
            }
        
        organic_position = target_result.position
        visual_position = serp_analysis.visual_position_map.get(organic_position, organic_position)
        
        # Find features above the target
        features_above = []
        for feature in serp_analysis.serp_features:
            if feature.rank_absolute and feature.rank_absolute < target_result.rank_absolute:
                features_above.append({
                    "type": feature.type,
                    "position": feature.position,
                    "weight": self.FEATURE_WEIGHTS.get(feature.type, 1.0),
                    "title": feature.title,
                    "items_count": feature.items_count
                })
        
        displacement = visual_position - organic_position
        
        # Estimate CTR impact (simplified model)
        # Base CTR by position (desktop, from industry averages)
        position_ctr = {
            1: 0.28, 2: 0.15, 3: 0.11, 4: 0.08, 5: 0.07,
            6: 0.05, 7: 0.04, 8: 0.03, 9: 0.025, 10: 0.02
        }
        
        expected_ctr = position_ctr.get(organic_position, 0.01)
        actual_estimated_ctr = position_ctr.get(int(visual_position), 0.01)
        ctr_impact = actual_estimated_ctr - expected_ctr
        
        return {
            "found": True,
            "keyword": serp_analysis.keyword,
            "target_url": target_result.url,
            "organic_position": organic_position,
            "visual_position": round(visual_position, 1),
            "displacement": round(displacement, 1),
            "features_above": features_above,
            "features_above_count": len(features_above),
            "expected_ctr": round(expected_ctr, 4),
            "estimated_actual_ctr": round(actual_estimated_ctr, 4),
            "estimated_ctr_impact": round(ctr_impact, 4),
            "is_featured_snippet": target_result.is_featured_snippet
        }
    
    def calculate_click_share_opportunity(
        self,
        serp_analyses: List[SERPAnalysis],
        target_domain: str
    ) -> Dict[str, Any]:
        """
        Calculate overall click share and opportunity across multiple keywords.
        
        Args:
            serp_analyses: List of SERPAnalysis objects
            target_domain: Domain to calculate share for
            
        Returns:
            Dict with click share metrics
        """
        total_keywords = len(serp_analyses)
        keywords_ranking = 0
        total_estimated_clicks = 0
        total_possible_clicks = 0
        
        position_ctr = {
            1: 0.28, 2: 0.15, 3: 0.11, 4: 0.08, 5: 0.07,
            6: 0.05, 7: 0.04, 8: 0.03, 9: 0.025, 10: 0.02
        }
        
        keyword_details = []
        
        for serp in serp_analyses:
            # Find target domain in results
            target_result = None
            for result in serp.organic_results:
                if target_domain.lower() in result.domain.lower():
                    target_result = result
                    keywords_ranking += 1
                    break
            
            # Calculate estimated clicks for this keyword
            # (We don't have actual search volume here, so we use position-based estimation)
            if target_result:
                visual_pos = serp.visual_position_map.get(target_result.position, target_result.position)
                estimated_ctr = position_ctr.get(int(visual_pos), 0.01)
                total_estimated_clicks += estimated_ctr
                
                keyword_details.append({
                    "keyword": serp.keyword,
                    "position": target_result.position,
                    "visual_position": round(visual_pos, 1),
                    "estimated_ctr": round(estimated_ctr, 4),
                    "url": target_result.url
                })
            
            # Total possible is if we ranked #1 for everything
            total_possible_clicks += position_ctr[1]
        
        click_share = total_estimated_clicks / total_possible_clicks if total_possible_clicks > 0 else 0
        opportunity = total_possible_clicks - total_estimated_clicks
        
        return {
            "total_keywords_analyzed": total_keywords,
            "keywords_ranking": keywords_ranking,
            "ranking_percentage": round(keywords_ranking / total_keywords * 100, 1) if total_keywords > 0 else 0,
            "estimated_click_share": round(click_share, 4),
            "click_share_percentage": round(click_share * 100, 2),
            "click_share_opportunity": round(opportunity / total_possible_clicks, 4) if total_possible_clicks > 0 else 0,
            "opportunity_percentage": round((opportunity / total_possible_clicks) * 100, 2) if total_possible_clicks > 0 else 0,
            "keyword_details": keyword_details
        }
    
    def get_cost_estimate(
        self,
        num_serp_requests: int = 0,
        num_keywords_difficulty: int = 0,
        num_competitor_requests: int = 0,
        num_domain_overview_requests: int = 0
    ) -> Dict[str, float]:
        """
        Estimate API cost for a report generation.
        
        Args:
            num_serp_requests: Number of SERP requests
            num_keywords_difficulty: Number of keywords for difficulty check
            num_competitor_requests: Number of competitor analysis requests
            num_domain_overview_requests: Number of domain overview requests
            
        Returns:
            Dict with cost breakdown
        """
        costs = {
            "serp": num_serp_requests * self.COSTS["serp_live"],
            "keyword_difficulty": (num_keywords_difficulty / 1000) * self.COSTS["keyword_difficulty"],
            "competitors": num_competitor_requests * self.COSTS["serp_competitors"],
            "domain_overview": num_domain_overview_requests * self.COSTS["domain_rank_overview"],
        }
        
        costs["total"] = sum(costs.values())
        
        return costs
    
    def clear_cache(self):
        """Clear the response cache"""
        self._cache.clear()
        logger.info("Cache cleared")
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        return {
            "cached_requests": len(self._cache),
            "rate_limit_window_requests": len(self._request_times)
        }