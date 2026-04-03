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
                "DATAFORSEO_LOGIN and DATAFORSEO_PASSWORD environment variables must be set"
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
    
    # Keywords Data endpoints
    keywords_for_keywords: str = "/v3/keywords_data/google_ads/keywords_for_keywords/live"
    search_volume: str = "/v3/keywords_data/google_ads/search_volume/live"
    keyword_suggestions: str = "/v3/keywords_data/google_ads/keyword_suggestions/live"
    
    # Domain Analytics endpoints
    domain_rank_overview: str = "/v3/domain_analytics/google/overview/live"
    domain_pages: str = "/v3/domain_analytics/google/pages/live"
    
    # Historical SERP data
    serp_history: str = "/v3/serp/google/organic/history"
    
    # Account info
    account_info: str = "/v3/appendix/user_data"
    pricing: str = "/v3/appendix/prices"

    def get_full_url(self, endpoint: str) -> str:
        """Get full URL for an endpoint"""
        return f"{self.base_url}{endpoint}"


@dataclass
class RateLimitConfig:
    """Rate limiting configuration for DataForSEO API"""
    # DataForSEO rate limits (per minute by default)
    max_requests_per_minute: int = 2000
    max_concurrent_requests: int = 200
    
    # Recommended conservative limits for production
    recommended_requests_per_minute: int = 100
    recommended_concurrent_requests: int = 10
    
    # Backoff settings
    initial_backoff_seconds: float = 1.0
    max_backoff_seconds: float = 60.0
    backoff_multiplier: float = 2.0
    max_retries: int = 3
    
    # Batch processing
    batch_delay_seconds: float = 0.1  # Delay between batch requests


@dataclass
class TimeoutConfig:
    """Timeout configuration for API requests"""
    connect_timeout: int = 10  # Connection timeout in seconds
    read_timeout: int = 30  # Read timeout in seconds
    total_timeout: int = 60  # Total request timeout in seconds


@dataclass
class SERPRequestDefaults:
    """Default parameters for SERP requests"""
    location_code: int = LocationCode.US_NATIONWIDE
    language_code: str = LanguageCode.ENGLISH
    device: str = DeviceType.DESKTOP.value
    os: Optional[str] = None
    depth: int = 100  # Number of results to return
    
    # Additional parameters
    calculate_rectangles: bool = True  # For SERP feature positioning
    browser_screen_width: int = 1920
    browser_screen_height: int = 1080
    browser_screen_resolution_ratio: float = 1.0
    
    # Search parameters
    search_param: Optional[str] = None  # Additional search parameters
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API request"""
        params = {
            "location_code": self.location_code,
            "language_code": self.language_code,
            "device": self.device,
            "depth": self.depth,
            "calculate_rectangles": self.calculate_rectangles,
            "browser_screen_width": self.browser_screen_width,
            "browser_screen_height": self.browser_screen_height,
            "browser_screen_resolution_ratio": self.browser_screen_resolution_ratio,
        }
        
        if self.os:
            params["os"] = self.os
        if self.search_param:
            params["search_param"] = self.search_param
            
        return params


@dataclass
class BatchConfig:
    """Configuration for batch processing"""
    # Batch sizes for different operations
    serp_batch_size: int = 20  # Keywords per SERP batch
    keyword_data_batch_size: int = 100  # Keywords per keyword data batch
    
    # Processing delays
    batch_delay_seconds: float = 0.5
    request_delay_seconds: float = 0.1
    
    # Retry configuration for batch processing
    max_batch_retries: int = 2
    retry_failed_items: bool = True


@dataclass
class CostConfig:
    """Cost tracking and budget configuration"""
    # Approximate costs (in USD) - actual costs may vary
    serp_live_cost_per_request: float = 0.002
    serp_task_cost_per_request: float = 0.002
    keywords_for_keywords_cost: float = 0.0006
    search_volume_cost: float = 0.0006
    
    # Budget limits
    max_cost_per_report: float = 1.0  # Maximum $ to spend per report
    enable_cost_tracking: bool = True
    warn_at_cost_threshold: float = 0.8  # Warn at 80% of max cost
    
    def estimate_serp_cost(self, num_keywords: int) -> float:
        """Estimate cost for SERP data pull"""
        return num_keywords * self.serp_live_cost_per_request
    
    def estimate_keyword_data_cost(self, num_keywords: int) -> float:
        """Estimate cost for keyword data pull"""
        return num_keywords * self.keywords_for_keywords_cost


@dataclass
class CacheConfig:
    """Configuration for caching API responses"""
    enable_cache: bool = True
    cache_ttl_hours: int = 24
    
    # Cache keys configuration
    cache_key_prefix: str = "dataforseo"
    serp_cache_key: str = "serp:{keyword}:{location}:{language}:{device}"
    keyword_data_cache_key: str = "kwdata:{keyword}:{location}"
    
    # Cache size limits
    max_cache_size_mb: int = 500
    enable_compression: bool = True


@dataclass
class DataForSEOConfig:
    """Main configuration class for DataForSEO integration"""
    credentials: DataForSEOCredentials = field(default_factory=DataForSEOCredentials)
    endpoints: DataForSEOEndpoints = field(default_factory=DataForSEOEndpoints)
    rate_limits: RateLimitConfig = field(default_factory=RateLimitConfig)
    timeouts: TimeoutConfig = field(default_factory=TimeoutConfig)
    serp_defaults: SERPRequestDefaults = field(default_factory=SERPRequestDefaults)
    batch: BatchConfig = field(default_factory=BatchConfig)
    cost: CostConfig = field(default_factory=CostConfig)
    cache: CacheConfig = field(default_factory=CacheConfig)
    
    # Feature flags
    enable_serp_history: bool = False  # Historical SERP data (if available)
    enable_keyword_suggestions: bool = True
    enable_domain_analytics: bool = False
    
    # Keyword selection for reports
    max_keywords_per_report: int = 100
    min_keyword_impressions: int = 100
    include_branded_keywords: bool = False
    
    # SERP feature tracking
    track_serp_features: List[str] = field(default_factory=lambda: [
        "featured_snippet",
        "people_also_ask",
        "video",
        "local_pack",
        "knowledge_panel",
        "ai_overview",
        "image_pack",
        "shopping_results",
        "top_stories",
        "related_searches",
        "site_links"
    ])
    
    def validate(self) -> bool:
        """Validate entire configuration"""
        if not self.credentials.validate():
            return False
        
        if self.batch.serp_batch_size <= 0:
            raise ValueError("SERP batch size must be positive")
        
        if self.cost.max_cost_per_report <= 0:
            raise ValueError("Max cost per report must be positive")
        
        if self.max_keywords_per_report <= 0:
            raise ValueError("Max keywords per report must be positive")
        
        return True
    
    def get_keyword_limit_for_budget(self, cost_per_keyword: Optional[float] = None) -> int:
        """Calculate maximum keywords that can be processed within budget"""
        if cost_per_keyword is None:
            cost_per_keyword = self.cost.serp_live_cost_per_request
        
        max_by_budget = int(self.cost.max_cost_per_report / cost_per_keyword)
        return min(max_by_budget, self.max_keywords_per_report)


class DataForSEOErrorCodes:
    """DataForSEO API error codes and descriptions"""
    
    ERROR_CODES = {
        # Authentication errors (10xxx)
        10000: "Invalid login or password",
        10001: "Account is blocked",
        10002: "Insufficient funds",
        
        # Request errors (20xxx)
        20000: "Invalid request format",
        20001: "Missing required parameter",
        20002: "Invalid parameter value",
        20003: "Request too large",
        20004: "Too many requests",
        
        # Data errors (30xxx)
        30000: "No data available",
        30001: "Invalid location code",
        30002: "Invalid language code",
        30003: "Invalid keyword",
        
        # System errors (40xxx)
        40000: "Internal server error",
        40001: "Service temporarily unavailable",
        40002: "Timeout",
        
        # Task errors (50xxx)
        50000: "Task not found",
        50001: "Task not ready",
        50002: "Task failed",
    }
    
    @classmethod
    def get_error_message(cls, code: int) -> str:
        """Get error message for error code"""
        return cls.ERROR_CODES.get(code, f"Unknown error code: {code}")
    
    @classmethod
    def is_retriable(cls, code: int) -> bool:
        """Check if error is retriable"""
        retriable_codes = {20004, 40001, 40002}  # Rate limit, unavailable, timeout
        return code in retriable_codes


class ResponseStatusCodes:
    """DataForSEO API response status codes"""
    OK = 20000
    INVALID_REQUEST = 20001
    INSUFFICIENT_FUNDS = 10002
    RATE_LIMIT_EXCEEDED = 20004
    
    SUCCESS_CODES = {20000}
    RETRIABLE_CODES = {20004, 40001, 40002}


def validate_location_code(location_code: int) -> bool:
    """
    Validate location code format.
    DataForSEO location codes are typically 4-7 digit integers.
    """
    return 1000 <= location_code <= 9999999


def validate_language_code(language_code: str) -> bool:
    """
    Validate language code format.
    Should be 2-letter ISO 639-1 code or 5-letter code like 'zh-CN'.
    """
    if not language_code:
        return False
    
    if len(language_code) == 2:
        return language_code.isalpha() and language_code.islower()
    
    if len(language_code) == 5 and language_code[2] == "-":
        lang = language_code[:2]
        country = language_code[3:]
        return lang.isalpha() and lang.islower() and country.isalpha() and country.isupper()
    
    return False


def validate_keyword(keyword: str) -> bool:
    """
    Validate keyword for SERP request.
    Keywords should be non-empty strings with reasonable length.
    """
    if not keyword or not isinstance(keyword, str):
        return False
    
    # Trim whitespace
    keyword = keyword.strip()
    
    # Check length (DataForSEO typically allows up to 255 chars)
    if len(keyword) < 1 or len(keyword) > 255:
        return False
    
    return True


def sanitize_keyword(keyword: str) -> str:
    """
    Sanitize keyword for API request.
    Removes extra whitespace and special characters that might cause issues.
    """
    if not keyword:
        return ""
    
    # Trim and normalize whitespace
    keyword = " ".join(keyword.split())
    
    # Remove null bytes and other problematic characters
    keyword = keyword.replace("\x00", "").replace("\n", " ").replace("\r", " ")
    
    return keyword.strip()


def create_serp_request_payload(
    keyword: str,
    location_code: Optional[int] = None,
    language_code: Optional[str] = None,
    device: Optional[str] = None,
    depth: Optional[int] = None,
    additional_params: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Create a properly formatted SERP request payload.
    
    Args:
        keyword: Search keyword
        location_code: Location code (uses default if None)
        language_code: Language code (uses default if None)
        device: Device type (uses default if None)
        depth: Number of results (uses default if None)
        additional_params: Additional parameters to include
    
    Returns:
        Dictionary formatted for DataForSEO API request
    """
    defaults = SERPRequestDefaults()
    
    payload = {
        "keyword": sanitize_keyword(keyword),
        "location_code": location_code or defaults.location_code,
        "language_code": language_code or defaults.language_code,
        "device": device or defaults.device,
        "depth": depth or defaults.depth,
        "calculate_rectangles": defaults.calculate_rectangles,
        "browser_screen_width": defaults.browser_screen_width,
        "browser_screen_height": defaults.browser_screen_height,
    }
    
    # Add additional parameters if provided
    if additional_params:
        payload.update(additional_params)
    
    return payload


def estimate_api_cost(num_keywords: int, include_keyword_data: bool = True) -> Dict[str, float]:
    """
    Estimate total API cost for a report.
    
    Args:
        num_keywords: Number of keywords to process
        include_keyword_data: Whether to include keyword data API calls
    
    Returns:
        Dictionary with cost breakdown
    """
    cost_config = CostConfig()
    
    serp_cost = cost_config.estimate_serp_cost(num_keywords)
    keyword_cost = cost_config.estimate_keyword_data_cost(num_keywords) if include_keyword_data else 0.0
    
    return {
        "serp_cost": serp_cost,
        "keyword_data_cost": keyword_cost,
        "total_cost": serp_cost + keyword_cost,
        "num_keywords": num_keywords,
        "within_budget": (serp_cost + keyword_cost) <= cost_config.max_cost_per_report
    }


# Global config instance
_global_config: Optional[DataForSEOConfig] = None


def get_config() -> DataForSEOConfig:
    """Get global DataForSEO configuration instance"""
    global _global_config
    if _global_config is None:
        _global_config = DataForSEOConfig()
        _global_config.validate()
    return _global_config


def set_config(config: DataForSEOConfig):
    """Set global DataForSEO configuration instance"""
    global _global_config
    config.validate()
    _global_config = config


def reset_config():
    """Reset global configuration to default"""
    global _global_config
    _global_config = None
