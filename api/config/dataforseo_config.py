"""
DataForSEO API Configuration Module

This module contains all configuration settings for the DataForSEO API integration.
Includes endpoint URLs, default parameters, rate limits, timeouts, response mappings,
and error code definitions.
"""

import os
from typing import Dict, Any, List, Optional
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
    
    # Other major locations
    UK_NATIONWIDE = 2826
    CANADA_NATIONWIDE = 2124
    AUSTRALIA_NATIONWIDE = 2036
    GERMANY_NATIONWIDE = 2276
    FRANCE_NATIONWIDE = 2250
    INDIA_NATIONWIDE = 2356
    JAPAN_NATIONWIDE = 2392
    BRAZIL_NATIONWIDE = 2076
    MEXICO_NATIONWIDE = 2484
    SPAIN_NATIONWIDE = 2724
    ITALY_NATIONWIDE = 2380
    NETHERLANDS_NATIONWIDE = 2528


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
        """Validate credentials are present and non-empty"""
        return bool(self.login and self.password and len(self.login) > 0 and len(self.password) > 0)


@dataclass
class DataForSEOEndpoints:
    """DataForSEO API endpoint configurations"""
    base_url: str = "https://api.dataforseo.com"
    
    # SERP endpoints
    serp_live: str = "/v3/serp/google/organic/live/advanced"
    serp_task_post: str = "/v3/serp/google/organic/task_post"
    serp_task_get: str = "/v3/serp/google/organic/task_get/advanced/{task_id}"
    serp_tasks_ready: str = "/v3/serp/google/organic/tasks_ready"
    
    # Keywords data endpoints
    keywords_for_keywords: str = "/v3/keywords_data/google/keywords_for_keywords/live"
    keywords_for_site: str = "/v3/keywords_data/google/keywords_for_site/live"
    search_volume: str = "/v3/keywords_data/google/search_volume/live"
    
    # Domain analytics endpoints
    domain_overview: str = "/v3/domain_analytics/google/organic/overview/live"
    domain_pages: str = "/v3/domain_analytics/google/organic/pages/live"
    domain_competitors: str = "/v3/domain_analytics/google/organic/competitors/live"
    
    # Account info
    account_info: str = "/v3/appendix/user_data"
    
    def get_full_url(self, endpoint: str) -> str:
        """Construct full URL for an endpoint"""
        return f"{self.base_url}{endpoint}"


@dataclass
class RateLimitConfig:
    """Rate limiting configuration for DataForSEO API"""
    # API rate limits (requests per minute)
    max_requests_per_minute: int = 2000
    max_concurrent_requests: int = 100
    
    # Retry configuration
    max_retries: int = 3
    retry_backoff_factor: float = 2.0  # Exponential backoff: 1s, 2s, 4s
    retry_on_status_codes: List[int] = field(default_factory=lambda: [429, 500, 502, 503, 504])
    
    # Timeout configuration (seconds)
    connect_timeout: int = 10
    read_timeout: int = 60
    
    # Throttling
    min_request_interval: float = 0.03  # 30ms between requests = ~33 req/sec


@dataclass
class CacheConfig:
    """Caching strategy for DataForSEO responses"""
    # Cache TTL (time to live) in seconds
    serp_cache_ttl: int = 86400  # 24 hours for SERP data
    keyword_cache_ttl: int = 604800  # 7 days for keyword data
    domain_cache_ttl: int = 259200  # 3 days for domain analytics
    
    # Cache key prefixes
    serp_cache_prefix: str = "dataforseo:serp"
    keyword_cache_prefix: str = "dataforseo:keyword"
    domain_cache_prefix: str = "dataforseo:domain"
    
    # Cache behavior
    use_cache: bool = True
    refresh_cache_on_error: bool = False  # If API fails, use stale cache
    cache_empty_results: bool = False  # Don't cache empty/null results


@dataclass
class DefaultSERPParameters:
    """Default parameters for SERP API requests"""
    # Location and language defaults
    location_code: int = LocationCode.US_NATIONWIDE
    language_code: str = LanguageCode.ENGLISH
    
    # Device and search settings
    device: str = DeviceType.DESKTOP.value
    os: Optional[str] = None
    depth: int = 100  # Number of results to retrieve
    
    # SERP features to include
    calculate_rectangles: bool = True  # Get position coordinates
    fetch_html: bool = False  # Usually not needed, saves bandwidth
    
    # Search type
    se_type: str = SearchEngineType.GOOGLE.value
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API request"""
        params = {
            "location_code": self.location_code,
            "language_code": self.language_code,
            "device": self.device,
            "depth": self.depth,
            "calculate_rectangles": self.calculate_rectangles,
        }
        
        if self.os:
            params["os"] = self.os
            
        return params


@dataclass
class SERPFeatureTypes:
    """SERP feature types to track"""
    # Organic features
    ORGANIC = "organic"
    
    # Rich features
    FEATURED_SNIPPET = "featured_snippet"
    KNOWLEDGE_GRAPH = "knowledge_graph"
    KNOWLEDGE_PANEL = "knowledge_panel"
    LOCAL_PACK = "local_pack"
    
    # Interactive elements
    PEOPLE_ALSO_ASK = "people_also_ask"
    RELATED_SEARCHES = "related_searches"
    
    # Media
    VIDEO = "video"
    VIDEO_CAROUSEL = "video_carousel"
    IMAGE_PACK = "images_pack"
    
    # Shopping
    SHOPPING = "shopping"
    SHOPPING_CAROUSEL = "shopping_carousel"
    
    # News
    TOP_STORIES = "top_stories"
    NEWS = "news"
    
    # AI and experimental
    AI_OVERVIEW = "ai_overview"
    PERSPECTIVES = "perspectives"
    
    # Social
    TWITTER = "twitter"
    REDDIT_THREADS = "reddit_threads"
    
    # Other
    PAID = "paid"
    SITE_LINKS = "sitelinks"
    RECIPES = "recipes"
    EVENTS = "events"
    JOBS = "jobs"
    
    @classmethod
    def get_all_types(cls) -> List[str]:
        """Get list of all tracked feature types"""
        return [
            getattr(cls, attr) for attr in dir(cls)
            if not attr.startswith('_') and isinstance(getattr(cls, attr), str)
        ]


@dataclass
class ErrorCodes:
    """DataForSEO API error codes and descriptions"""
    # Authentication errors
    ERROR_40101: str = "Authentication failed. Verify credentials."
    ERROR_40102: str = "Low account balance."
    ERROR_40103: str = "User not found."
    
    # Request errors
    ERROR_40001: str = "Invalid request. Check parameters."
    ERROR_40002: str = "Missing required parameter."
    ERROR_40003: str = "Invalid parameter value."
    ERROR_40004: str = "Unsupported parameter."
    
    # Rate limit errors
    ERROR_42901: str = "Too many requests. Rate limit exceeded."
    
    # Task errors
    ERROR_50001: str = "Task not found."
    ERROR_50002: str = "Task processing error."
    ERROR_50003: str = "Task timeout."
    
    # Server errors
    ERROR_50000: str = "Internal server error."
    ERROR_50301: str = "Service temporarily unavailable."
    
    @classmethod
    def get_error_message(cls, error_code: int) -> str:
        """Get human-readable error message for error code"""
        attr_name = f"ERROR_{error_code}"
        return getattr(cls, attr_name, f"Unknown error code: {error_code}")


@dataclass
class CostEstimates:
    """Cost estimates for different API operations"""
    # SERP API costs (USD)
    serp_live_cost_per_request: float = 0.002  # $0.002 per keyword
    serp_task_cost_per_request: float = 0.001  # $0.001 per keyword (async)
    
    # Keywords data costs
    keywords_for_keywords_cost: float = 0.0005
    keywords_for_site_cost: float = 0.005
    search_volume_cost: float = 0.0002
    
    # Domain analytics costs
    domain_overview_cost: float = 0.001
    domain_pages_cost: float = 0.002
    domain_competitors_cost: float = 0.002
    
    # Budget per report (for planning)
    target_keywords_per_report: int = 75
    estimated_cost_per_report: float = 0.15  # $0.15 for 75 keywords
    max_cost_per_report: float = 0.30  # Hard limit


@dataclass
class DataForSEOConfig:
    """Main configuration class combining all settings"""
    credentials: DataForSEOCredentials = field(default_factory=DataForSEOCredentials)
    endpoints: DataForSEOEndpoints = field(default_factory=DataForSEOEndpoints)
    rate_limits: RateLimitConfig = field(default_factory=RateLimitConfig)
    cache: CacheConfig = field(default_factory=CacheConfig)
    default_params: DefaultSERPParameters = field(default_factory=DefaultSERPParameters)
    cost_estimates: CostEstimates = field(default_factory=CostEstimates)
    
    # Feature flags
    enable_caching: bool = True
    enable_rate_limiting: bool = True
    enable_cost_tracking: bool = True
    
    # Logging
    log_api_calls: bool = True
    log_api_responses: bool = False  # Too verbose for production
    
    def __post_init__(self):
        """Validate configuration after initialization"""
        if not self.credentials.validate():
            raise ValueError("Invalid DataForSEO credentials")
    
    @classmethod
    def from_env(cls) -> "DataForSEOConfig":
        """Create configuration from environment variables"""
        return cls()
    
    def get_request_headers(self) -> Dict[str, str]:
        """Get standard headers for API requests"""
        return {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
    
    def get_auth_tuple(self) -> tuple:
        """Get authentication tuple for requests library"""
        return (self.credentials.login, self.credentials.password)
    
    def estimate_request_cost(self, endpoint: str, num_requests: int = 1) -> float:
        """Estimate cost for a given number of requests to an endpoint"""
        cost_map = {
            self.endpoints.serp_live: self.cost_estimates.serp_live_cost_per_request,
            self.endpoints.serp_task_post: self.cost_estimates.serp_task_cost_per_request,
            self.endpoints.keywords_for_keywords: self.cost_estimates.keywords_for_keywords_cost,
            self.endpoints.keywords_for_site: self.cost_estimates.keywords_for_site_cost,
            self.endpoints.search_volume: self.cost_estimates.search_volume_cost,
            self.endpoints.domain_overview: self.cost_estimates.domain_overview_cost,
            self.endpoints.domain_pages: self.cost_estimates.domain_pages_cost,
            self.endpoints.domain_competitors: self.cost_estimates.domain_competitors_cost,
        }
        
        cost_per_request = cost_map.get(endpoint, 0.001)  # Default fallback
        return cost_per_request * num_requests


# Global configuration instance
_config_instance: Optional[DataForSEOConfig] = None


def get_config() -> DataForSEOConfig:
    """Get or create global configuration instance"""
    global _config_instance
    if _config_instance is None:
        _config_instance = DataForSEOConfig.from_env()
    return _config_instance


def reset_config():
    """Reset global configuration (useful for testing)"""
    global _config_instance
    _config_instance = None


# Validation helper
def validate_environment():
    """Validate that all required environment variables are set"""
    required_vars = ["DATAFORSEO_LOGIN", "DATAFORSEO_PASSWORD"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        raise EnvironmentError(
            f"Missing required environment variables: {', '.join(missing_vars)}. "
            f"Please set these in your .env file or environment."
        )
    
    try:
        config = get_config()
        return True
    except Exception as e:
        raise EnvironmentError(f"Configuration validation failed: {str(e)}")
