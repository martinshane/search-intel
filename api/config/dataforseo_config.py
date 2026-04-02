import os
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
from enum import Enum


class DataForSEOEnvironment(Enum):
    """DataForSEO API environment types"""
    PRODUCTION = "production"
    SANDBOX = "sandbox"


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
    max_concurrent_requests: int = 100
    
    # Retry configuration
    max_retries: int = 3
    retry_delay_seconds: int = 2
    backoff_multiplier: float = 2.0
    
    # Timeout settings
    request_timeout_seconds: int = 60
    connection_timeout_seconds: int = 10


@dataclass
class DefaultSERPParameters:
    """Default parameters for SERP requests"""
    # Location settings
    location_code: int = 2840  # United States
    location_name: str = "United States"
    language_code: str = "en"
    language_name: str = "English"
    
    # Device settings
    device: str = "desktop"  # Options: desktop, mobile, tablet
    os: Optional[str] = None  # e.g., "windows", "macos", "ios", "android"
    
    # SERP depth
    depth: int = 100  # Number of results to retrieve (max 100)
    
    # Additional parameters
    calculate_rectangles: bool = True  # For SERP feature positioning
    browser_screen_width: int = 1920
    browser_screen_height: int = 1080
    browser_screen_resolution_ratio: int = 1
    
    # Search features
    load_async: bool = False
    search_param: Optional[str] = None  # Additional search parameters (e.g., "&tbs=qdr:m" for last month)

    @staticmethod
    def get_location_codes() -> Dict[str, int]:
        """Common location codes for quick reference"""
        return {
            "United States": 2840,
            "United Kingdom": 2826,
            "Canada": 2124,
            "Australia": 2036,
            "Germany": 2276,
            "France": 2250,
            "Spain": 2724,
            "Italy": 2380,
            "Netherlands": 2528,
            "Sweden": 2752,
            "India": 2356,
            "Japan": 2392,
            "Brazil": 2076,
            "Mexico": 2484,
        }

    @staticmethod
    def get_mobile_parameters() -> Dict[str, Any]:
        """Get default mobile SERP parameters"""
        return {
            "device": "mobile",
            "os": "ios",
            "browser_screen_width": 375,
            "browser_screen_height": 667,
            "browser_screen_resolution_ratio": 2,
        }


@dataclass
class CacheConfig:
    """Cache configuration for SERP data"""
    # Cache TTL settings (in seconds)
    serp_cache_ttl: int = 86400  # 24 hours
    keyword_data_cache_ttl: int = 604800  # 7 days
    domain_analytics_cache_ttl: int = 259200  # 3 days
    
    # Cache storage
    use_database_cache: bool = True
    use_redis_cache: bool = False
    redis_url: Optional[str] = field(
        default_factory=lambda: os.getenv("REDIS_URL")
    )
    
    # Cache key prefixes
    serp_cache_prefix: str = "serp"
    keyword_cache_prefix: str = "keyword"
    domain_cache_prefix: str = "domain"
    
    # Cache size limits
    max_cache_entries: int = 10000
    cache_eviction_policy: str = "lru"  # least recently used

    def get_cache_key(self, prefix: str, *args) -> str:
        """Generate cache key from prefix and arguments"""
        key_parts = [prefix] + [str(arg) for arg in args]
        return ":".join(key_parts)


@dataclass
class DataForSEOBudgetConfig:
    """Budget and cost management for DataForSEO API usage"""
    # Cost per request (approximate, in USD)
    serp_live_cost: float = 0.002
    serp_task_cost: float = 0.002
    keywords_for_keywords_cost: float = 0.001
    search_volume_cost: float = 0.001
    domain_analytics_cost: float = 0.005
    
    # Budget limits
    max_cost_per_report: float = 0.20
    max_keywords_per_report: int = 100
    
    # Cost tracking
    enable_cost_tracking: bool = True
    cost_alert_threshold: float = 0.15  # Alert at 75% of max_cost_per_report
    
    def calculate_report_cost(
        self,
        num_serp_requests: int,
        num_keyword_requests: int = 0,
        num_domain_requests: int = 0
    ) -> float:
        """Calculate estimated cost for a report generation"""
        total_cost = (
            (num_serp_requests * self.serp_live_cost) +
            (num_keyword_requests * self.keywords_for_keywords_cost) +
            (num_domain_requests * self.domain_analytics_cost)
        )
        return round(total_cost, 4)

    def is_within_budget(
        self,
        num_serp_requests: int,
        num_keyword_requests: int = 0,
        num_domain_requests: int = 0
    ) -> bool:
        """Check if planned requests are within budget"""
        estimated_cost = self.calculate_report_cost(
            num_serp_requests,
            num_keyword_requests,
            num_domain_requests
        )
        return estimated_cost <= self.max_cost_per_report


@dataclass
class KeywordSelectionConfig:
    """Configuration for keyword selection and filtering"""
    # Top N keywords to analyze
    top_keywords_count: int = 50
    max_keywords_count: int = 100
    
    # Keyword filtering
    min_impressions: int = 100  # Minimum impressions to consider
    min_clicks: int = 5  # Minimum clicks to consider
    
    # Branded keyword filtering
    filter_branded: bool = True
    brand_match_threshold: float = 0.7  # Fuzzy match threshold for brand detection
    
    # Position change thresholds
    significant_position_change: float = 3.0  # Position change to flag keyword
    position_change_window_days: int = 30
    
    # Keyword categorization
    position_ranges: Dict[str, tuple] = field(default_factory=lambda: {
        "top3": (1, 3),
        "top5": (1, 5),
        "top10": (1, 10),
        "page1": (1, 10),
        "page2": (11, 20),
        "striking_distance": (8, 20),
    })


@dataclass
class DataForSEOConfig:
    """Main configuration class for DataForSEO integration"""
    credentials: DataForSEOCredentials = field(default_factory=DataForSEOCredentials)
    endpoints: DataForSEOEndpoints = field(default_factory=DataForSEOEndpoints)
    rate_limits: RateLimitConfig = field(default_factory=RateLimitConfig)
    default_params: DefaultSERPParameters = field(default_factory=DefaultSERPParameters)
    cache: CacheConfig = field(default_factory=CacheConfig)
    budget: DataForSEOBudgetConfig = field(default_factory=DataForSEOBudgetConfig)
    keyword_selection: KeywordSelectionConfig = field(default_factory=KeywordSelectionConfig)
    
    # Feature flags
    enable_serp_history: bool = False  # Historical SERP data (more expensive)
    enable_domain_analytics: bool = False  # Domain analytics features
    enable_keyword_suggestions: bool = False  # Keyword expansion
    
    # Parallel processing
    max_parallel_requests: int = 10
    batch_size: int = 25  # Keywords per batch
    
    # Error handling
    raise_on_api_error: bool = False  # If False, log errors and continue
    skip_failed_keywords: bool = True  # Continue processing if individual keywords fail

    @classmethod
    def from_env(cls) -> "DataForSEOConfig":
        """Create configuration from environment variables"""
        return cls()

    def validate(self) -> bool:
        """Validate configuration"""
        if not self.credentials.login or not self.credentials.password:
            return False
        
        if self.budget.max_keywords_per_report < self.keyword_selection.top_keywords_count:
            raise ValueError(
                "max_keywords_per_report must be >= top_keywords_count"
            )
        
        return True

    def get_request_headers(self) -> Dict[str, str]:
        """Get headers for DataForSEO API requests"""
        return {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def get_auth(self) -> tuple:
        """Get authentication tuple for requests"""
        return (self.credentials.login, self.credentials.password)


# Global configuration instance
_config: Optional[DataForSEOConfig] = None


def get_dataforseo_config() -> DataForSEOConfig:
    """Get or create global DataForSEO configuration instance"""
    global _config
    if _config is None:
        _config = DataForSEOConfig.from_env()
        _config.validate()
    return _config


def set_dataforseo_config(config: DataForSEOConfig) -> None:
    """Set global DataForSEO configuration instance"""
    global _config
    config.validate()
    _config = config


# Convenience functions for common operations
def get_location_code(location_name: str) -> Optional[int]:
    """Get location code by name"""
    locations = DefaultSERPParameters.get_location_codes()
    return locations.get(location_name)


def get_serp_endpoint_url(endpoint_name: str) -> str:
    """Get full URL for a SERP endpoint"""
    config = get_dataforseo_config()
    endpoint = getattr(config.endpoints, endpoint_name, None)
    if endpoint is None:
        raise ValueError(f"Unknown endpoint: {endpoint_name}")
    return config.endpoints.get_full_url(endpoint)


def calculate_serp_request_cost(num_keywords: int) -> float:
    """Calculate cost for SERP requests"""
    config = get_dataforseo_config()
    return config.budget.calculate_report_cost(num_serp_requests=num_keywords)


def is_request_within_budget(num_keywords: int) -> bool:
    """Check if request is within budget"""
    config = get_dataforseo_config()
    return config.budget.is_within_budget(num_serp_requests=num_keywords)


# Export commonly used classes
__all__ = [
    "DataForSEOConfig",
    "DataForSEOCredentials",
    "DataForSEOEndpoints",
    "RateLimitConfig",
    "DefaultSERPParameters",
    "CacheConfig",
    "DataForSEOBudgetConfig",
    "KeywordSelectionConfig",
    "get_dataforseo_config",
    "set_dataforseo_config",
    "get_location_code",
    "get_serp_endpoint_url",
    "calculate_serp_request_cost",
    "is_request_within_budget",
]