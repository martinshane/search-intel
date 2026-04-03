"""
DataForSEO API Configuration Module

This module contains all configuration settings for the DataForSEO API integration.
Includes endpoint URLs, default parameters, rate limits, timeouts, response mappings,
and error code definitions for modules 3, 8, and 11.
"""

import os
from typing import Dict, Any, List, Optional, Set
from dataclasses import dataclass, field
from enum import Enum


class DataForSEOEnvironment(Enum):
    """DataForSEO API environment types"""
    PRODUCTION = "production"
    SANDBOX = "sandbox"


class DeviceType(Enum):
    """Supported device types for SERP requests"""
    DESKTOP = "desktop"
    MOBILE = "mobile"
    TABLET = "tablet"


class SearchEngineType(Enum):
    """Supported search engine types"""
    GOOGLE = "google"
    BING = "bing"
    YAHOO = "yahoo"


class SERPFeatureType(Enum):
    """Known SERP feature types for Module 3 analysis"""
    FEATURED_SNIPPET = "featured_snippet"
    PEOPLE_ALSO_ASK = "people_also_ask"
    LOCAL_PACK = "local_pack"
    KNOWLEDGE_PANEL = "knowledge_panel"
    IMAGE_PACK = "images"
    VIDEO_CAROUSEL = "video"
    TOP_STORIES = "top_stories"
    SHOPPING_RESULTS = "shopping"
    RECIPES = "recipes"
    TWITTER = "twitter"
    AI_OVERVIEW = "ai_overview"
    REDDIT_THREADS = "discussions_and_forums"
    SITE_LINKS = "sitelinks"
    RELATED_SEARCHES = "related_searches"
    CAROUSEL = "carousel"
    ORGANIC = "organic"
    PAID = "paid"


class LocationCode:
    """Common location codes for SERP requests"""
    # United States locations
    US_NATIONWIDE = 2840
    US_NEW_YORK = 1023191
    US_LOS_ANGELES = 1023768
    US_CHICAGO = 1023854
    US_HOUSTON = 1024094
    US_PHOENIX = 1023867
    US_PHILADELPHIA = 1023359
    US_SAN_ANTONIO = 1024066
    US_SAN_DIEGO = 1023926
    US_DALLAS = 1024071
    US_SAN_FRANCISCO = 1023768
    US_AUSTIN = 1024153
    US_SEATTLE = 1024053
    US_BOSTON = 1023004
    US_MIAMI = 1023965
    
    # Other major locations
    UK_NATIONWIDE = 2826
    UK_LONDON = 1006886
    CANADA_NATIONWIDE = 2124
    CANADA_TORONTO = 1009247
    AUSTRALIA_NATIONWIDE = 2036
    AUSTRALIA_SYDNEY = 1000543
    GERMANY_NATIONWIDE = 2276
    GERMANY_BERLIN = 1003654
    FRANCE_NATIONWIDE = 2250
    FRANCE_PARIS = 1006094
    INDIA_NATIONWIDE = 2356
    INDIA_DELHI = 1007448
    JAPAN_NATIONWIDE = 2392
    JAPAN_TOKYO = 1009345
    BRAZIL_NATIONWIDE = 2076
    BRAZIL_SAO_PAULO = 1001773
    MEXICO_NATIONWIDE = 2484
    MEXICO_MEXICO_CITY = 1007677
    SPAIN_NATIONWIDE = 2724
    SPAIN_MADRID = 1006607
    ITALY_NATIONWIDE = 2380
    ITALY_ROME = 1006542
    NETHERLANDS_NATIONWIDE = 2528
    NETHERLANDS_AMSTERDAM = 1010562


class LanguageCode:
    """Common language codes for SERP requests"""
    ENGLISH = "en"
    SPANISH = "es"
    FRENCH = "fr"
    GERMAN = "de"
    ITALIAN = "it"
    PORTUGUESE = "pt"
    DUTCH = "nl"
    CHINESE_SIMPLIFIED = "zh-CN"
    CHINESE_TRADITIONAL = "zh-TW"
    JAPANESE = "ja"
    KOREAN = "ko"
    RUSSIAN = "ru"
    ARABIC = "ar"
    HINDI = "hi"
    POLISH = "pl"
    TURKISH = "tr"
    SWEDISH = "sv"
    NORWEGIAN = "no"
    DANISH = "da"
    FINNISH = "fi"


@dataclass
class DataForSEOCredentials:
    """DataForSEO API credentials from environment variables"""
    login: str = field(default_factory=lambda: os.getenv("DATAFORSEO_LOGIN", ""))
    password: str = field(default_factory=lambda: os.getenv("DATAFORSEO_PASSWORD", ""))
    environment: DataForSEOEnvironment = field(
        default_factory=lambda: DataForSEOEnvironment(
            os.getenv("DATAFORSEO_ENVIRONMENT", "production")
        )
    )

    def __post_init__(self):
        if not self.login or not self.password:
            raise ValueError(
                "DATAFORSEO_LOGIN and DATAFORSEO_PASSWORD environment variables must be set. "
                "Please set these in your .env file or environment."
            )
        
        if len(self.login.strip()) == 0 or len(self.password.strip()) == 0:
            raise ValueError(
                "DATAFORSEO_LOGIN and DATAFORSEO_PASSWORD cannot be empty strings"
            )

    @property
    def is_sandbox(self) -> bool:
        return self.environment == DataForSEOEnvironment.SANDBOX

    def validate(self) -> bool:
        """Validate credentials are properly configured"""
        return bool(self.login and self.password)


@dataclass
class APIEndpoints:
    """DataForSEO API endpoint configurations"""
    
    # Base URLs
    BASE_URL: str = "https://api.dataforseo.com"
    SANDBOX_URL: str = "https://sandbox.dataforseo.com"
    
    # Version
    VERSION: str = "v3"
    
    # SERP endpoints (Module 3: SERP Landscape Analysis)
    SERP_GOOGLE_ORGANIC_LIVE: str = "/v3/serp/google/organic/live/advanced"
    SERP_GOOGLE_ORGANIC_TASK_POST: str = "/v3/serp/google/organic/task_post"
    SERP_GOOGLE_ORGANIC_TASK_GET: str = "/v3/serp/google/organic/task_get/advanced/{task_id}"
    SERP_GOOGLE_ORGANIC_TASKS_READY: str = "/v3/serp/google/organic/tasks_ready"
    
    # Domain analytics endpoints (Module 8: Domain Authority)
    DOMAIN_OVERVIEW: str = "/v3/backlinks/summary/live"
    DOMAIN_BACKLINKS: str = "/v3/backlinks/backlinks/live"
    DOMAIN_REFERRING_DOMAINS: str = "/v3/backlinks/referring_domains/live"
    DOMAIN_ANCHORS: str = "/v3/backlinks/anchors/live"
    DOMAIN_PAGES: str = "/v3/backlinks/page_intersection/live"
    
    # Competitor analysis endpoints (Module 11: Competitive Clustering)
    COMPETITORS_DOMAIN: str = "/v3/dataforseo_labs/google/competitors_domain/live"
    KEYWORDS_FOR_SITE: str = "/v3/dataforseo_labs/google/keywords_for_site/live"
    RANKED_KEYWORDS: str = "/v3/dataforseo_labs/google/ranked_keywords/live"
    DOMAIN_INTERSECTION: str = "/v3/dataforseo_labs/google/domain_intersection/live"
    KEYWORD_SUGGESTIONS: str = "/v3/dataforseo_labs/google/keyword_suggestions/live"
    
    # Supporting endpoints
    LOCATIONS: str = "/v3/serp/google/locations"
    LANGUAGES: str = "/v3/serp/google/languages"
    
    def get_base_url(self, use_sandbox: bool = False) -> str:
        """Get the appropriate base URL based on environment"""
        return self.SANDBOX_URL if use_sandbox else self.BASE_URL
    
    def get_full_url(self, endpoint: str, use_sandbox: bool = False) -> str:
        """Construct full URL for an endpoint"""
        base = self.get_base_url(use_sandbox)
        return f"{base}{endpoint}"


@dataclass
class RateLimits:
    """DataForSEO API rate limits and quotas"""
    
    # Rate limits (requests per second)
    MAX_REQUESTS_PER_SECOND: int = 2
    MAX_CONCURRENT_REQUESTS: int = 10
    
    # Batch limits
    MAX_TASKS_PER_BATCH: int = 100
    MAX_KEYWORDS_PER_REQUEST: int = 1000
    
    # Retry configuration
    MAX_RETRIES: int = 3
    RETRY_BACKOFF_FACTOR: float = 2.0  # Exponential backoff
    RETRY_STATUS_CODES: Set[int] = field(default_factory=lambda: {429, 500, 502, 503, 504})
    
    # Timeout configuration (seconds)
    REQUEST_TIMEOUT: int = 30
    LONG_RUNNING_TIMEOUT: int = 300
    
    # Cost controls (USD)
    MAX_COST_PER_REQUEST: float = 1.00
    WARN_COST_THRESHOLD: float = 0.50


@dataclass
class DefaultParameters:
    """Default parameters for various DataForSEO requests"""
    
    # SERP request defaults (Module 3)
    DEFAULT_LOCATION_CODE: int = LocationCode.US_NATIONWIDE
    DEFAULT_LANGUAGE_CODE: str = LanguageCode.ENGLISH
    DEFAULT_DEVICE: str = DeviceType.DESKTOP.value
    DEFAULT_DEPTH: int = 100  # Number of results to return
    DEFAULT_SE_TYPE: str = SearchEngineType.GOOGLE.value
    
    # Domain analytics defaults (Module 8)
    DEFAULT_BACKLINK_LIMIT: int = 1000
    DEFAULT_REFERRING_DOMAIN_LIMIT: int = 500
    DEFAULT_BACKLINK_FILTERS: List[str] = field(default_factory=lambda: ["dofollow", "live"])
    
    # Competitor analysis defaults (Module 11)
    DEFAULT_COMPETITOR_LIMIT: int = 100
    DEFAULT_KEYWORD_INTERSECTION_LIMIT: int = 1000
    DEFAULT_MIN_INTERSECTION: int = 5  # Minimum overlapping keywords
    
    # General settings
    CALCULATE_RECTANGLES: bool = True  # For SERP layout analysis
    LOAD_RESOURCES: bool = False  # Don't load images/CSS/JS
    BROWSER_SCREEN_WIDTH: int = 1920
    BROWSER_SCREEN_HEIGHT: int = 1080
    BROWSER_SCREEN_SCALE_FACTOR: float = 1.0


@dataclass
class SERPFeatureWeights:
    """Visual position weights for different SERP features (Module 3)"""
    
    # How many "organic position equivalents" each feature consumes
    FEATURED_SNIPPET: float = 2.0
    AI_OVERVIEW: float = 3.0
    LOCAL_PACK: float = 1.5
    KNOWLEDGE_PANEL: float = 1.0
    IMAGE_PACK: float = 1.0
    VIDEO_CAROUSEL: float = 1.5
    TOP_STORIES: float = 1.0
    SHOPPING_RESULTS: float = 1.0
    PEOPLE_ALSO_ASK_ITEM: float = 0.5  # Per question
    RELATED_SEARCHES: float = 0.0  # At bottom, doesn't displace
    SITE_LINKS: float = 0.5  # Expands the result
    TWITTER: float = 0.5
    REDDIT_THREADS: float = 1.0
    RECIPES: float = 1.5
    CAROUSEL: float = 1.0
    PAID_AD: float = 0.3  # Ads affect but differently


class SERPFeatureMapping:
    """Mapping between DataForSEO SERP feature types and our internal types"""
    
    FEATURE_MAP: Dict[str, str] = {
        # Exact matches
        "featured_snippet": SERPFeatureType.FEATURED_SNIPPET.value,
        "people_also_ask": SERPFeatureType.PEOPLE_ALSO_ASK.value,
        "local_pack": SERPFeatureType.LOCAL_PACK.value,
        "knowledge_panel": SERPFeatureType.KNOWLEDGE_PANEL.value,
        "images": SERPFeatureType.IMAGE_PACK.value,
        "video": SERPFeatureType.VIDEO_CAROUSEL.value,
        "top_stories": SERPFeatureType.TOP_STORIES.value,
        "shopping": SERPFeatureType.SHOPPING_RESULTS.value,
        "recipes": SERPFeatureType.RECIPES.value,
        "twitter": SERPFeatureType.TWITTER.value,
        "organic": SERPFeatureType.ORGANIC.value,
        "paid": SERPFeatureType.PAID.value,
        
        # Variations and aliases
        "answer_box": SERPFeatureType.FEATURED_SNIPPET.value,
        "map": SERPFeatureType.LOCAL_PACK.value,
        "local_services": SERPFeatureType.LOCAL_PACK.value,
        "knowledge_graph": SERPFeatureType.KNOWLEDGE_PANEL.value,
        "image_pack": SERPFeatureType.IMAGE_PACK.value,
        "video_carousel": SERPFeatureType.VIDEO_CAROUSEL.value,
        "news": SERPFeatureType.TOP_STORIES.value,
        "shopping_carousel": SERPFeatureType.SHOPPING_RESULTS.value,
        "recipe_carousel": SERPFeatureType.RECIPES.value,
        
        # DataForSEO specific types
        "carousel": SERPFeatureType.CAROUSEL.value,
        "sitelinks": SERPFeatureType.SITE_LINKS.value,
        "related_searches": SERPFeatureType.RELATED_SEARCHES.value,
        "find_results_on": SERPFeatureType.RELATED_SEARCHES.value,
        "discussions_and_forums": SERPFeatureType.REDDIT_THREADS.value,
        "ai_overview": SERPFeatureType.AI_OVERVIEW.value,
        "google_ai": SERPFeatureType.AI_OVERVIEW.value,
    }
    
    @classmethod
    def normalize_feature_type(cls, feature_type: str) -> Optional[str]:
        """Normalize a DataForSEO feature type to our internal type"""
        if not feature_type:
            return None
        
        feature_lower = feature_type.lower().strip()
        return cls.FEATURE_MAP.get(feature_lower)
    
    @classmethod
    def get_feature_weight(cls, feature_type: str, count: int = 1) -> float:
        """Get visual position weight for a SERP feature"""
        normalized = cls.normalize_feature_type(feature_type)
        
        if not normalized:
            return 0.5  # Default for unknown features
        
        weights = SERPFeatureWeights()
        
        weight_map = {
            SERPFeatureType.FEATURED_SNIPPET.value: weights.FEATURED_SNIPPET,
            SERPFeatureType.AI_OVERVIEW.value: weights.AI_OVERVIEW,
            SERPFeatureType.LOCAL_PACK.value: weights.LOCAL_PACK,
            SERPFeatureType.KNOWLEDGE_PANEL.value: weights.KNOWLEDGE_PANEL,
            SERPFeatureType.IMAGE_PACK.value: weights.IMAGE_PACK,
            SERPFeatureType.VIDEO_CAROUSEL.value: weights.VIDEO_CAROUSEL,
            SERPFeatureType.TOP_STORIES.value: weights.TOP_STORIES,
            SERPFeatureType.SHOPPING_RESULTS.value: weights.SHOPPING_RESULTS,
            SERPFeatureType.PEOPLE_ALSO_ASK.value: weights.PEOPLE_ALSO_ASK_ITEM * count,
            SERPFeatureType.RELATED_SEARCHES.value: weights.RELATED_SEARCHES,
            SERPFeatureType.SITE_LINKS.value: weights.SITE_LINKS,
            SERPFeatureType.TWITTER.value: weights.TWITTER,
            SERPFeatureType.REDDIT_THREADS.value: weights.REDDIT_THREADS,
            SERPFeatureType.RECIPES.value: weights.RECIPES,
            SERPFeatureType.CAROUSEL.value: weights.CAROUSEL,
            SERPFeatureType.PAID.value: weights.PAID_AD * count,
        }
        
        return weight_map.get(normalized, 0.5)


class ErrorCodes:
    """DataForSEO API error codes and their meanings"""
    
    ERROR_MESSAGES: Dict[int, str] = {
        # 40xxx - Request errors
        40001: "Incorrect login/password",
        40002: "Account suspended",
        40003: "Insufficient funds",
        40004: "Invalid API version",
        40101: "Required parameter missing",
        40102: "Invalid parameter value",
        40103: "Parameter combination not allowed",
        40104: "Unsupported parameter",
        40201: "Location not found",
        40202: "Language not found",
        40301: "Task not found",
        40302: "Task already completed",
        40401: "Rate limit exceeded",
        
        # 50xxx - Server errors
        50001: "Internal server error",
        50002: "Temporary service unavailable",
        50003: "Database error",
        50004: "Timeout error",
    }
    
    @classmethod
    def get_error_message(cls, code: int) -> str:
        """Get human-readable error message for a code"""
        return cls.ERROR_MESSAGES.get(code, f"Unknown error code: {code}")
    
    @classmethod
    def is_retryable(cls, code: int) -> bool:
        """Determine if an error code should trigger a retry"""
        retryable_codes = {50001, 50002, 50003, 50004, 40401}
        return code in retryable_codes


def format_serp_request(
    keyword: str,
    location_code: Optional[int] = None,
    language_code: Optional[str] = None,
    device: Optional[str] = None,
    depth: Optional[int] = None,
    **kwargs
) -> Dict[str, Any]:
    """
    Format a SERP request payload for DataForSEO
    
    Args:
        keyword: Search keyword/query
        location_code: Location code (default: US nationwide)
        language_code: Language code (default: English)
        device: Device type (default: desktop)
        depth: Number of results (default: 100)
        **kwargs: Additional parameters
    
    Returns:
        Formatted request payload
    """
    defaults = DefaultParameters()
    
    payload = {
        "keyword": keyword.strip(),
        "location_code": location_code or defaults.DEFAULT_LOCATION_CODE,
        "language_code": language_code or defaults.DEFAULT_LANGUAGE_CODE,
        "device": device or defaults.DEFAULT_DEVICE,
        "depth": depth or defaults.DEFAULT_DEPTH,
        "se_type": defaults.DEFAULT_SE_TYPE,
        "calculate_rectangles": defaults.CALCULATE_RECTANGLES,
        "browser_screen_width": defaults.BROWSER_SCREEN_WIDTH,
        "browser_screen_height": defaults.BROWSER_SCREEN_HEIGHT,
    }
    
    # Merge additional parameters
    payload.update(kwargs)
    
    return payload


def format_domain_overview_request(
    target: str,
    backlink_filters: Optional[List[str]] = None,
    limit: Optional[int] = None,
    **kwargs
) -> Dict[str, Any]:
    """
    Format a domain overview request for Module 8
    
    Args:
        target: Target domain or URL
        backlink_filters: Filters for backlinks (default: dofollow, live)
        limit: Result limit
        **kwargs: Additional parameters
    
    Returns:
        Formatted request payload
    """
    defaults = DefaultParameters()
    
    payload = {
        "target": target.strip(),
        "filters": backlink_filters or defaults.DEFAULT_BACKLINK_FILTERS,
        "limit": limit or defaults.DEFAULT_BACKLINK_LIMIT,
    }
    
    payload.update(kwargs)
    
    return payload


def format_competitor_request(
    target: str,
    location_code: Optional[int] = None,
    language_code: Optional[str] = None,
    limit: Optional[int] = None,
    **kwargs
) -> Dict[str, Any]:
    """
    Format a competitor analysis request for Module 11
    
    Args:
        target: Target domain
        location_code: Location code
        language_code: Language code
        limit: Number of competitors to return
        **kwargs: Additional parameters
    
    Returns:
        Formatted request payload
    """
    defaults = DefaultParameters()
    
    payload = {
        "target": target.strip(),
        "location_code": location_code or defaults.DEFAULT_LOCATION_CODE,
        "language_code": language_code or defaults.DEFAULT_LANGUAGE_CODE,
        "limit": limit or defaults.DEFAULT_COMPETITOR_LIMIT,
    }
    
    payload.update(kwargs)
    
    return payload


def parse_serp_response(response_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse and normalize a SERP API response
    
    Args:
        response_data: Raw API response
    
    Returns:
        Normalized SERP data structure
    """
    if not response_data or "tasks" not in response_data:
        return {"error": "Invalid response structure", "items": []}
    
    tasks = response_data.get("tasks", [])
    if not tasks:
        return {"error": "No tasks in response", "items": []}
    
    task = tasks[0]
    if task.get("status_code") != 20000:
        error_msg = task.get("status_message", "Unknown error")
        return {"error": error_msg, "items": []}
    
    result = task.get("result", [{}])[0] if task.get("result") else {}
    
    return {
        "keyword": result.get("keyword"),
        "location_code": result.get("location_code"),
        "language_code": result.get("language_code"),
        "device": result.get("se_type"),
        "total_count": result.get("total_count", 0),
        "items_count": result.get("items_count", 0),
        "items": result.get("items", []),
        "se_results_count": result.get("se_results_count"),
    }


def parse_domain_response(response_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse and normalize a domain analytics API response
    
    Args:
        response_data: Raw API response
    
    Returns:
        Normalized domain data structure
    """
    if not response_data or "tasks" not in response_data:
        return {"error": "Invalid response structure", "metrics": {}}
    
    tasks = response_data.get("tasks", [])
    if not tasks:
        return {"error": "No tasks in response", "metrics": {}}
    
    task = tasks[0]
    if task.get("status_code") != 20000:
        error_msg = task.get("status_message", "Unknown error")
        return {"error": error_msg, "metrics": {}}
    
    result = task.get("result", [{}])[0] if task.get("result") else {}
    
    return {
        "target": result.get("target"),
        "metrics": result,
        "items": result.get("items", []),
    }


def parse_competitor_response(response_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse and normalize a competitor analysis API response
    
    Args:
        response_data: Raw API response
    
    Returns:
        Normalized competitor data structure
    """
    if not response_data or "tasks" not in response_data:
        return {"error": "Invalid response structure", "competitors": []}
    
    tasks = response_data.get("tasks", [])
    if not tasks:
        return {"error": "No tasks in response", "competitors": []}
    
    task = tasks[0]
    if task.get("status_code") != 20000:
        error_msg = task.get("status_message", "Unknown error")
        return {"error": error_msg, "competitors": []}
    
    result = task.get("result", [{}])[0] if task.get("result") else {}
    
    return {
        "target": result.get("target"),
        "location_code": result.get("location_code"),
        "language_code": result.get("language_code"),
        "total_count": result.get("total_count", 0),
        "items_count": result.get("items_count", 0),
        "competitors": result.get("items", []),
    }


def extract_serp_features(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Extract and classify SERP features from result items
    
    Args:
        items: List of SERP result items
    
    Returns:
        List of classified SERP features with positions
    """
    features = []
    
    for idx, item in enumerate(items):
        item_type = item.get("type", "")
        normalized_type = SERPFeatureMapping.normalize_feature_type(item_type)
        
        if not normalized_type:
            continue
        
        feature = {
            "type": normalized_type,
            "original_type": item_type,
            "rank_group": item.get("rank_group"),
            "rank_absolute": item.get("rank_absolute"),
            "position": idx + 1,
            "xpath": item.get("xpath"),
        }
        
        # Extract type-specific data
        if normalized_type == SERPFeatureType.PEOPLE_ALSO_ASK.value:
            feature["items"] = item.get("items", [])
            feature["count"] = len(item.get("items", []))
        elif normalized_type == SERPFeatureType.ORGANIC.value:
            feature["url"] = item.get("url")
            feature["domain"] = item.get("domain")
            feature["title"] = item.get("title")
            feature["description"] = item.get("description")
        elif normalized_type == SERPFeatureType.PAID.value:
            feature["url"] = item.get("url")
            feature["domain"] = item.get("domain")
            feature["title"] = item.get("title")
        
        features.append(feature)
    
    return features


def calculate_visual_position(
    organic_position: int,
    features_above: List[Dict[str, Any]]
) -> float:
    """
    Calculate visual position accounting for SERP features
    
    Args:
        organic_position: Organic ranking position
        features_above: List of SERP features appearing above this result
    
    Returns:
        Adjusted visual position
    """
    displacement = 0.0
    
    for feature in features_above:
        feature_type = feature.get("type", "")
        count = feature.get("count", 1)
        weight = SERPFeatureMapping.get_feature_weight(feature_type, count)
        displacement += weight
    
    return organic_position + displacement


# Global configuration instance
CONFIG = {
    "credentials": DataForSEOCredentials(),
    "endpoints": APIEndpoints(),
    "rate_limits": RateLimits(),
    "defaults": DefaultParameters(),
    "feature_weights": SERPFeatureWeights(),
}


def get_config() -> Dict[str, Any]:
    """Get the global configuration object"""
    return CONFIG


def validate_configuration() -> bool:
    """
    Validate that all required configuration is present and valid
    
    Returns:
        True if configuration is valid, raises ValueError otherwise
    """
    config = get_config()
    
    # Validate credentials
    if not config["credentials"].validate():
        raise ValueError("Invalid DataForSEO credentials")
    
    # Validate rate limits are sensible
    rate_limits = config["rate_limits"]
    if rate_limits.MAX_REQUESTS_PER_SECOND <= 0:
        raise ValueError("MAX_REQUESTS_PER_SECOND must be positive")
    
    if rate_limits.REQUEST_TIMEOUT <= 0:
        raise ValueError("REQUEST_TIMEOUT must be positive")
    
    return True