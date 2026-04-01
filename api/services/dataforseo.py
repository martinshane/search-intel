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
import base64

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


class DataForSEOService:
    """
    Service wrapper for DataForSEO API
    
    Provides methods for:
    - Authentication using basic auth
    - Fetching SERP data for keywords
    - Parsing SERP features from responses
    - Calculating expected CTR based on position and features
    - Rate limiting and response caching
    """
    
    BASE_URL = "https://api.dataforseo.com"
    DEFAULT_RATE_LIMIT = 2000  # requests per day on free tier
    
    # Industry CTR curves by position (baseline, no SERP features)
    BASELINE_CTR = {
        1: 0.316,
        2: 0.158,
        3: 0.108,
        4: 0.080,
        5: 0.065,
        6: 0.053,
        7: 0.045,
        8: 0.039,
        9: 0.034,
        10: 0.031,
    }
    
    # SERP feature impact multipliers (adjust baseline CTR)
    FEATURE_CTR_IMPACT = {
        "featured_snippet": 0.65,  # Reduces CTR for position 1 by 35%
        "people_also_ask": 0.92,   # Reduces CTR by 8% per PAA above
        "local_pack": 0.70,        # Local pack takes significant clicks
        "knowledge_graph": 0.85,   # KG on right side takes some clicks
        "ai_overview": 0.60,       # AI overview at top drastically reduces CTR
        "video_carousel": 0.88,    # Video carousel takes some clicks
        "image_pack": 0.93,        # Images take fewer clicks
        "shopping_results": 0.80,  # Shopping ads/results compete
        "top_stories": 0.85,       # News box takes clicks
        "related_searches": 1.0,   # At bottom, no impact on organic CTR
        "site_links": 1.15,        # Site links increase CTR for position 1
    }
    
    def __init__(
        self,
        login: Optional[str] = None,
        password: Optional[str] = None,
        rate_limit: int = DEFAULT_RATE_LIMIT,
        cache_ttl_hours: int = 24
    ):
        """
        Initialize DataForSEO service
        
        Args:
            login: DataForSEO API login (defaults to DATAFORSEO_LOGIN env var)
            password: DataForSEO API password (defaults to DATAFORSEO_PASSWORD env var)
            rate_limit: Maximum requests per day
            cache_ttl_hours: How long to cache responses
        """
        self.login = login or os.getenv("DATAFORSEO_LOGIN")
        self.password = password or os.getenv("DATAFORSEO_PASSWORD")
        
        if not self.login or not self.password:
            raise ValueError(
                "DataForSEO credentials not provided. "
                "Set DATAFORSEO_LOGIN and DATAFORSEO_PASSWORD environment variables."
            )
        
        self.rate_limit = rate_limit
        self.cache_ttl = timedelta(hours=cache_ttl_hours)
        
        # Initialize session with retry logic
        self.session = self._create_session()
        
        # Request tracking for rate limiting
        self.request_times: List[datetime] = []
        
        # In-memory cache for responses (session-level)
        self.cache: Dict[str, Dict[str, Any]] = {}
        
        logger.info(f"DataForSEO service initialized with rate limit: {rate_limit}/day")
    
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
        
        return session
    
    def authenticate(self) -> bool:
        """
        Test authentication credentials
        
        Returns:
            True if authentication successful, False otherwise
        """
        try:
            # Use a simple endpoint to test credentials
            url = f"{self.BASE_URL}/v3/appendix/user_data"
            
            auth_string = f"{self.login}:{self.password}"
            encoded_auth = base64.b64encode(auth_string.encode()).decode()
            
            headers = {
                "Authorization": f"Basic {encoded_auth}",
                "Content-Type": "application/json"
            }
            
            response = self.session.get(url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                if data.get("status_code") == 20000:
                    logger.info("DataForSEO authentication successful")
                    return True
                else:
                    logger.error(f"Authentication failed: {data.get('status_message')}")
                    return False
            else:
                logger.error(f"Authentication failed with status {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Authentication error: {str(e)}")
            return False
    
    def _check_rate_limit(self):
        """Check if we're within rate limit"""
        now = datetime.now()
        
        # Remove requests older than 24 hours
        self.request_times = [
            t for t in self.request_times 
            if now - t < timedelta(days=1)
        ]
        
        if len(self.request_times) >= self.rate_limit:
            raise RateLimitError(
                f"Rate limit of {self.rate_limit} requests per day exceeded. "
                f"Next available slot in {24 - (now - self.request_times[0]).total_seconds() / 3600:.1f} hours"
            )
        
        self.request_times.append(now)
    
    def _get_cache_key(self, endpoint: str, params: Dict[str, Any]) -> str:
        """Generate cache key from endpoint and parameters"""
        params_str = json.dumps(params, sort_keys=True)
        return f"{endpoint}:{params_str}"
    
    def _get_from_cache(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """Get response from cache if not expired"""
        if cache_key in self.cache:
            cached = self.cache[cache_key]
            cached_time = datetime.fromisoformat(cached["timestamp"])
            
            if datetime.now() - cached_time < self.cache_ttl:
                logger.debug(f"Cache hit for key: {cache_key}")
                return cached["data"]
            else:
                # Expired, remove from cache
                del self.cache[cache_key]
        
        return None
    
    def _save_to_cache(self, cache_key: str, data: Dict[str, Any]):
        """Save response to cache"""
        self.cache[cache_key] = {
            "timestamp": datetime.now().isoformat(),
            "data": data
        }
    
    def _make_request(
        self,
        endpoint: str,
        method: str = "POST",
        data: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Make authenticated request to DataForSEO API
        
        Args:
            endpoint: API endpoint path
            method: HTTP method (GET or POST)
            data: Request payload for POST requests
            
        Returns:
            API response as dictionary
        """
        # Check cache first
        cache_key = self._get_cache_key(endpoint, data or {})
        cached_response = self._get_from_cache(cache_key)
        if cached_response is not None:
            return cached_response
        
        # Check rate limit
        self._check_rate_limit()
        
        url = f"{self.BASE_URL}{endpoint}"
        
        auth_string = f"{self.login}:{self.password}"
        encoded_auth = base64.b64encode(auth_string.encode()).decode()
        
        headers = {
            "Authorization": f"Basic {encoded_auth}",
            "Content-Type": "application/json"
        }
        
        try:
            if method == "POST":
                response = self.session.post(
                    url,
                    headers=headers,
                    json=data,
                    timeout=60
                )
            else:
                response = self.session.get(
                    url,
                    headers=headers,
                    timeout=60
                )
            
            response.raise_for_status()
            result = response.json()
            
            # Check DataForSEO status code
            if result.get("status_code") != 20000:
                raise DataForSEOError(
                    f"API returned error: {result.get('status_message', 'Unknown error')}"
                )
            
            # Cache successful response
            self._save_to_cache(cache_key, result)
            
            return result
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {str(e)}")
            raise DataForSEOError(f"Request failed: {str(e)}")
    
    def get_serp_data(
        self,
        keyword: str,
        location: Union[str, int] = 2840,  # US by default
        language: str = "en",
        device: str = "desktop",
        depth: int = 100
    ) -> SERPAnalysis:
        """
        Fetch live SERP data for a keyword
        
        Args:
            keyword: Search keyword
            location: Location code or name (default: 2840 = United States)
            language: Language code (default: "en")
            device: Device type ("desktop" or "mobile")
            depth: Number of results to retrieve (max 100)
            
        Returns:
            SERPAnalysis object with parsed SERP data
        """
        logger.info(f"Fetching SERP data for keyword: {keyword}")
        
        # Prepare request
        post_data = [{
            "keyword": keyword,
            "location_code": location if isinstance(location, int) else None,
            "location_name": location if isinstance(location, str) else None,
            "language_code": language,
            "device": device,
            "os": "windows" if device == "desktop" else "ios",
            "depth": depth,
            "calculate_rectangles": True  # For visual position mapping
        }]
        
        # Make request
        response = self._make_request(
            DataForSEOEndpoint.SERP_LIVE.value,
            method="POST",
            data=post_data
        )
        
        # Parse response
        if not response.get("tasks") or not response["tasks"][0].get("result"):
            raise DataForSEOError("No results returned from API")
        
        result = response["tasks"][0]["result"][0]
        
        # Parse SERP features and organic results
        serp_features = self.parse_serp_features(result)
        organic_results = self._parse_organic_results(result)
        people_also_ask = self._parse_people_also_ask(result)
        related_searches = self._parse_related_searches(result)
        
        # Calculate visual position map
        visual_position_map = self._calculate_visual_positions(
            organic_results,
            serp_features
        )
        
        return SERPAnalysis(
            keyword=keyword,
            location_code=result.get("location_code", location),
            language_code=language,
            se_results_count=result.get("se_results_count", 0),
            organic_results=organic_results,
            serp_features=serp_features,
            people_also_ask=people_also_ask,
            related_searches=related_searches,
            visual_position_map=visual_position_map,
            timestamp=datetime.now()
        )
    
    def parse_serp_features(self, serp_result: Dict[str, Any]) -> List[SERPFeature]:
        """
        Parse SERP features from API response
        
        Extracts:
        - Featured snippets
        - People Also Ask boxes
        - Local packs
        - Knowledge graphs/panels
        - AI overviews
        - Video carousels
        - Image packs
        - Shopping results
        - Top stories
        - Site links
        
        Args:
            serp_result: Raw SERP result from API
            
        Returns:
            List of SERPFeature objects
        """
        features = []
        items = serp_result.get("items", [])
        
        for item in items:
            item_type = item.get("type")
            rank_group = item.get("rank_group")
            rank_absolute = item.get("rank_absolute")
            
            if item_type == "featured_snippet":
                features.append(SERPFeature(
                    type="featured_snippet",
                    position=rank_absolute or 0,
                    title=item.get("title"),
                    description=item.get("description"),
                    url=item.get("url"),
                    rank_absolute=rank_absolute
                ))
            
            elif item_type == "people_also_ask":
                paa_items = item.get("items", [])
                features.append(SERPFeature(
                    type="people_also_ask",
                    position=rank_absolute or 0,
                    items_count=len(paa_items),
                    rank_absolute=rank_absolute
                ))
            
            elif item_type == "local_pack":
                local_items = item.get("items", [])
                features.append(SERPFeature(
                    type="local_pack",
                    position=rank_absolute or 0,
                    items_count=len(local_items),
                    rank_absolute=rank_absolute
                ))
            
            elif item_type == "knowledge_graph":
                features.append(SERPFeature(
                    type="knowledge_graph",
                    position=rank_absolute or 0,
                    title=item.get("title"),
                    description=item.get("description"),
                    rank_absolute=rank_absolute
                ))
            
            elif item_type == "ai_overview" or item_type == "google_labs":
                features.append(SERPFeature(
                    type="ai_overview",
                    position=rank_absolute or 0,
                    description=item.get("text"),
                    rank_absolute=rank_absolute
                ))
            
            elif item_type == "video":
                video_items = item.get("items", [])
                features.append(SERPFeature(
                    type="video_carousel",
                    position=rank_absolute or 0,
                    items_count=len(video_items),
                    rank_absolute=rank_absolute
                ))
            
            elif item_type == "images":
                image_items = item.get("items", [])
                features.append(SERPFeature(
                    type="image_pack",
                    position=rank_absolute or 0,
                    items_count=len(image_items),
                    rank_absolute=rank_absolute
                ))
            
            elif item_type == "shopping":
                shopping_items = item.get("items", [])
                features.append(SERPFeature(
                    type="shopping_results",
                    position=rank_absolute or 0,
                    items_count=len(shopping_items),
                    rank_absolute=rank_absolute
                ))
            
            elif item_type == "top_stories":
                stories_items = item.get("items", [])
                features.append(SERPFeature(
                    type="top_stories",
                    position=rank_absolute or 0,
                    items_count=len(stories_items),
                    rank_absolute=rank_absolute
                ))
            
            elif item_type == "related_searches":
                related_items = item.get("items", [])
                features.append(SERPFeature(
                    type="related_searches",
                    position=rank_absolute or 999,  # At bottom
                    items_count=len(related_items),
                    rank_absolute=rank_absolute
                ))
            
            elif item_type == "organic" and item.get("links"):
                # Organic result with site links (position 1 usually)
                features.append(SERPFeature(
                    type="site_links",
                    position=rank_absolute or 0,
                    items_count=len(item.get("links", [])),
                    rank_absolute=rank_absolute
                ))
        
        return features
    
    def _parse_organic_results(self, serp_result: Dict[str, Any]) -> List[OrganicResult]:
        """Parse organic search results from SERP data"""
        organic_results = []
        items = serp_result.get("items", [])
        
        position = 1
        for item in items:
            if item.get("type") == "organic":
                organic_results.append(OrganicResult(
                    url=item.get("url", ""),
                    domain=item.get("domain", ""),
                    title=item.get("title", ""),
                    description=item.get("description"),
                    position=position,
                    rank_absolute=item.get("rank_absolute", position),
                    is_featured_snippet=item.get("is_featured_snippet", False),
                    breadcrumb=item.get("breadcrumb"),
                    website_name=item.get("website_name")
                ))
                position += 1
        
        return organic_results
    
    def _parse_people_also_ask(self, serp_result: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parse People Also Ask questions"""
        paa_list = []
        items = serp_result.get("items", [])
        
        for item in items:
            if item.get("type") == "people_also_ask":
                for paa_item in item.get("items", []):
                    paa_list.append({
                        "question": paa_item.get("title"),
                        "answer": paa_item.get("expanded_element", [{}])[0].get("description"),
                        "source_url": paa_item.get("expanded_element", [{}])[0].get("url")
                    })
        
        return paa_list
    
    def _parse_related_searches(self, serp_result: Dict[str, Any]) -> List[str]:
        """Parse related searches"""
        related = []
        items = serp_result.get("items", [])
        
        for item in items:
            if item.get("type") == "related_searches":
                for related_item in item.get("items", []):
                    related.append(related_item.get("title", ""))
        
        return related
    
    def _calculate_visual_positions(
        self,
        organic_results: List[OrganicResult],
        serp_features: List[SERPFeature]
    ) -> Dict[int, int]:
        """
        Calculate visual position for each organic result
        
        Visual position accounts for SERP features pushing results down.
        Each feature type adds a certain number of "visual positions".
        
        Returns:
            Dictionary mapping organic_position -> visual_position
        """
        # Feature weights (how many positions each feature adds)
        feature_weights = {
            "featured_snippet": 2.0,
            "people_also_ask": 0.5,  # Per question
            "local_pack": 3.0,
            "knowledge_graph": 0,  # Usually on right side, doesn't push down
            "ai_overview": 2.5,
            "video_carousel": 1.5,
            "image_pack": 1.0,
            "shopping_results": 2.0,
            "top_stories": 1.5,
            "site_links": 0.5,
            "related_searches": 0  # At bottom
        }
        
        visual_map = {}
        
        for organic in organic_results:
            visual_offset = 0
            
            # Count features above this organic result
            for feature in serp_features:
                if feature.rank_absolute and feature.rank_absolute < organic.rank_absolute:
                    weight = feature_weights.get(feature.type, 0)
                    
                    # For countable features (PAA, images, etc.), multiply by count
                    if feature.items_count and feature.type in ["people_also_ask"]:
                        visual_offset += weight * feature.items_count
                    else:
                        visual_offset += weight
            
            visual_position = organic.position + visual_offset
            visual_map[organic.position] = int(round(visual_position))
        
        return visual_map
    
    def calculate_expected_ctr(
        self,
        position: int,
        features: List[SERPFeature],
        rank_absolute: Optional[int] = None
    ) -> float:
        """
        Calculate expected CTR for a position given SERP features present
        
        Uses industry baseline CTR curves adjusted by feature impact multipliers.
        
        Args:
            position: Organic position (1-10+)
            features: List of SERP features present
            rank_absolute: Absolute rank (if different from position due to features)
            
        Returns:
            Expected CTR as decimal (e.g., 0.15 = 15%)
        """
        # Get baseline CTR for position
        if position <= 10:
            base_ctr = self.BASELINE_CTR.get(position, 0.025)
        else:
            # Exponential decay for positions beyond 10
            base_ctr = 0.025 * (0.9 ** (position - 10))
        
        # Apply feature impact multipliers
        ctr = base_ctr
        
        # Count features that affect this position
        features_above = [
            f for f in features 
            if f.rank_absolute and rank_absolute and f.rank_absolute < rank_absolute
        ]
        
        for feature in features_above:
            multiplier = self.FEATURE_CTR_IMPACT.get(feature.type, 1.0)
            
            # For PAA, apply multiplier per question (diminishing returns)
            if feature.type == "people_also_ask" and feature.items_count:
                # Each PAA question reduces CTR by 8%, with diminishing returns
                for i in range(feature.items_count):
                    ctr *= (1 - (0.08 * (0.8 ** i)))
            else:
                ctr *= multiplier
        
        # Special case: site links increase CTR for position 1
        has_site_links = any(f.type == "site_links" for f in features if f.position == 1)
        if position == 1 and has_site_links:
            ctr *= self.FEATURE_CTR_IMPACT["site_links"]
        
        return round(ctr, 4)
    
    def get_batch_serp_data(
        self,
        keywords: List[str],
        location: Union[str, int] = 2840,
        language: str = "en",
        device: str = "desktop",
        max_concurrent: int = 5
    ) -> List[SERPAnalysis]:
        """
        Fetch SERP data for multiple keywords with rate limiting
        
        Args:
            keywords: List of keywords to analyze
            location: Location code or name
            language: Language code
            device: Device type
            max_concurrent: Maximum concurrent requests
            
        Returns:
            List of SERPAnalysis objects
        """
        results = []
        
        for i, keyword in enumerate(keywords):
            try:
                logger.info(f"Processing keyword {i+1}/{len(keywords)}: {keyword}")
                
                serp_data = self.get_serp_data(
                    keyword=keyword,
                    location=location,
                    language=language,
                    device=device
                )
                
                results.append(serp_data)
                
                # Rate limiting: sleep between requests if not cached
                if i < len(keywords) - 1:
                    time.sleep(1)  # 1 second between requests
                    
            except RateLimitError as e:
                logger.error(f"Rate limit exceeded: {str(e)}")
                break
            except Exception as e:
                logger.error(f"Error fetching SERP data for '{keyword}': {str(e)}")
                continue
        
        return results
    
    def get_keywords_for_site(
        self,
        target_domain: str,
        location: Union[str, int] = 2840,
        language: str = "en",
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get keywords that a domain ranks for
        
        Args:
            target_domain: Domain to analyze
            location: Location code
            language: Language code
            limit: Maximum number of keywords to return
            
        Returns:
            List of keyword data dictionaries
        """
        logger.info(f"Fetching keywords for domain: {target_domain}")
        
        post_data = [{
            "target": target_domain,
            "location_code": location if isinstance(location, int) else None,
            "location_name": location if isinstance(location, str) else None,
            "language_code": language,
            "limit": limit,
            "filters": [
                ["keyword_data.keyword_info.search_volume", ">", 0]
            ],
            "order_by": ["keyword_data.keyword_info.search_volume,desc"]
        }]
        
        response = self._make_request(
            DataForSEOEndpoint.KEYWORDS_FOR_SITE.value,
            method="POST",
            data=post_data
        )
        
        if not response.get("tasks") or not response["tasks"][0].get("result"):
            return []
        
        items = response["tasks"][0]["result"][0].get("items", [])
        
        keywords = []
        for item in items:
            keyword_data = item.get("keyword_data", {})
            keyword_info = keyword_data.get("keyword_info", {})
            serp_info = item.get("serp_info", {})
            
            keywords.append({
                "keyword": keyword_data.get("keyword"),
                "search_volume": keyword_info.get("search_volume"),
                "competition": keyword_info.get("competition"),
                "cpc": keyword_info.get("cpc"),
                "current_position": serp_info.get("se_results_count"),
                "etv": item.get("etv")  # Estimated traffic value
            })
        
        return keywords
    
    def clear_cache(self):
        """Clear the response cache"""
        self.cache = {}
        logger.info("Cache cleared")
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        return {
            "total_entries": len(self.cache),
            "cache_ttl_hours": self.cache_ttl.total_seconds() / 3600,
            "requests_today": len([
                t for t in self.request_times 
                if datetime.now() - t < timedelta(days=1)
            ]),
            "rate_limit": self.rate_limit,
            "requests_remaining_today": max(
                0,
                self.rate_limit - len([
                    t for t in self.request_times 
                    if datetime.now() - t < timedelta(days=1)
                ])
            )
        }