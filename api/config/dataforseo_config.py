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
    device: DeviceType = DeviceType.DESKTOP
    os: Optional[str] = None
    
    # Search settings
    depth: int = 100  # Number of results to retrieve
    calculate_rectangles: bool = True  # For SERP feature positioning
    
    # Content settings
    load_async: bool = True
    browser_screen_width: int = 1920
    browser_screen_height: int = 1080
    browser_screen_resolution_ratio: int = 1


@dataclass
class LocationConfig:
    """Common location configurations"""
    LOCATIONS: Dict[str, Dict[str, Any]] = field(default_factory=lambda: {
        "US": {
            "location_code": 2840,
            "location_name": "United States",
            "language_code": "en",
            "language_name": "English"
        },
        "US_NY": {
            "location_code": 1023191,
            "location_name": "New York,New York,United States",
            "language_code": "en",
            "language_name": "English"
        },
        "US_CA": {
            "location_code": 1023768,
            "location_name": "California,United States",
            "language_code": "en",
            "language_name": "English"
        },
        "UK": {
            "location_code": 2826,
            "location_name": "United Kingdom",
            "language_code": "en",
            "language_name": "English"
        },
        "CA": {
            "location_code": 2124,
            "location_name": "Canada",
            "language_code": "en",
            "language_name": "English"
        },
        "AU": {
            "location_code": 2036,
            "location_name": "Australia",
            "language_code": "en",
            "language_name": "English"
        }
    })

    @classmethod
    def get_location(cls, location_key: str) -> Dict[str, Any]:
        """Get location configuration by key"""
        return cls.LOCATIONS.get(location_key.upper(), cls.LOCATIONS["US"])


@dataclass
class ResponseFieldMappings:
    """Mappings for DataForSEO response fields to internal field names"""
    
    # SERP result fields
    serp_fields: Dict[str, str] = field(default_factory=lambda: {
        "type": "type",
        "rank_group": "rank_group",
        "rank_absolute": "position",
        "domain": "domain",
        "title": "title",
        "description": "description",
        "url": "url",
        "breadcrumb": "breadcrumb",
        "is_image": "has_image",
        "is_video": "has_video",
        "is_featured_snippet": "is_featured_snippet",
        "is_malicious": "is_malicious",
        "is_web_story": "is_web_story",
        "rating": "rating",
        "highlighted": "highlighted_words",
        "links": "sitelinks",
        "faq": "faq",
        "extended_people_also_search": "related_searches"
    })
    
    # SERP feature type mappings
    serp_feature_types: Dict[str, str] = field(default_factory=lambda: {
        "organic": "organic",
        "paid": "paid_ad",
        "featured_snippet": "featured_snippet",
        "knowledge_graph": "knowledge_panel",
        "local_pack": "local_pack",
        "images": "image_pack",
        "videos": "video_carousel",
        "news": "top_stories",
        "shopping": "shopping_results",
        "people_also_ask": "people_also_ask",
        "related_searches": "related_searches",
        "answer_box": "answer_box",
        "carousel": "carousel",
        "twitter": "twitter_results",
        "recipes": "recipes",
        "events": "events",
        "hotels": "hotels",
        "jobs": "jobs",
        "app_pack": "app_pack",
        "flights": "flights",
        "math_solver": "math_solver",
        "dictionary": "dictionary",
        "stocks": "stocks",
        "translation": "translation",
        "weather": "weather"
    })
    
    # Keyword data fields
    keyword_fields: Dict[str, str] = field(default_factory=lambda: {
        "keyword": "keyword",
        "location_code": "location_code",
        "language_code": "language_code",
        "search_partners": "search_partners",
        "competition": "competition",
        "competition_index": "competition_index",
        "search_volume": "search_volume",
        "low_top_of_page_bid": "low_cpc",
        "high_top_of_page_bid": "high_cpc",
        "cpc": "avg_cpc",
        "monthly_searches": "monthly_searches"
    })


@dataclass
class ErrorCodeDefinitions:
    """DataForSEO API error code definitions and handling instructions"""
    
    # Error code mappings
    error_codes: Dict[int, Dict[str, str]] = field(default_factory=lambda: {
        # Success codes
        20000: {
            "message": "Success",
            "severity": "info",
            "action": "continue"
        },
        
        # Client errors (40xxx)
        40001: {
            "message": "Authentication failed - invalid credentials",
            "severity": "critical",
            "action": "check_credentials"
        },
        40002: {
            "message": "Insufficient credits",
            "severity": "critical",
            "action": "add_credits"
        },
        40003: {
            "message": "Invalid request parameters",
            "severity": "error",
            "action": "validate_parameters"
        },
        40004: {
            "message": "Required parameter missing",
            "severity": "error",
            "action": "add_missing_parameter"
        },
        40005: {
            "message": "Invalid parameter value",
            "severity": "error",
            "action": "fix_parameter_value"
        },
        40101: {
            "message": "Invalid location code",
            "severity": "error",
            "action": "check_location_code"
        },
        40102: {
            "message": "Invalid language code",
            "severity": "error",
            "action": "check_language_code"
        },
        40301: {
            "message": "Rate limit exceeded",
            "severity": "warning",
            "action": "retry_with_backoff"
        },
        40401: {
            "message": "Task not found",
            "severity": "error",
            "action": "check_task_id"
        },
        40402: {
            "message": "Task still processing",
            "severity": "info",
            "action": "wait_and_retry"
        },
        
        # Server errors (50xxx)
        50000: {
            "message": "Internal server error",
            "severity": "error",
            "action": "retry_with_backoff"
        },
        50001: {
            "message": "Service temporarily unavailable",
            "severity": "warning",
            "action": "retry_later"
        },
        50002: {
            "message": "Gateway timeout",
            "severity": "warning",
            "action": "retry_with_longer_timeout"
        },
        
        # DataForSEO specific errors (60xxx)
        60000: {
            "message": "Search engine temporarily unavailable",
            "severity": "warning",
            "action": "retry_later"
        },
        60001: {
            "message": "SERP parsing error",
            "severity": "error",
            "action": "report_to_support"
        },
        60002: {
            "message": "Location not supported for this search engine",
            "severity": "error",
            "action": "change_location"
        }
    })
    
    # Retry-able error codes
    retryable_codes: List[int] = field(default_factory=lambda: [
        40301,  # Rate limit exceeded
        40402,  # Task still processing
        50000,  # Internal server error
        50001,  # Service unavailable
        50002,  # Gateway timeout
        60000   # Search engine unavailable
    ])
    
    # Critical error codes (should not retry)
    critical_codes: List[int] = field(default_factory=lambda: [
        40001,  # Authentication failed
        40002,  # Insufficient credits
        40003,  # Invalid request parameters
        40004,  # Required parameter missing
    ])
    
    def get_error_info(self, error_code: int) -> Dict[str, str]:
        """Get error information for a given error code"""
        return self.error_codes.get(error_code, {
            "message": f"Unknown error code: {error_code}",
            "severity": "error",
            "action": "contact_support"
        })
    
    def is_retryable(self, error_code: int) -> bool:
        """Check if an error code is retryable"""
        return error_code in self.retryable_codes
    
    def is_critical(self, error_code: int) -> bool:
        """Check if an error code is critical"""
        return error_code in self.critical_codes


@dataclass
class CostConfig:
    """Cost configuration for DataForSEO API calls"""
    
    # Per-request costs (in USD)
    costs_per_request: Dict[str, float] = field(default_factory=lambda: {
        "serp_live": 0.002,  # $0.002 per live SERP request
        "serp_task_post": 0.002,  # $0.002 per task (same as live)
        "keywords_for_keywords": 0.005,  # $0.005 per keyword batch
        "search_volume": 0.003,  # $0.003 per keyword batch
        "keyword_suggestions": 0.004,  # $0.004 per request
        "domain_rank_overview": 0.01,  # $0.01 per domain
        "domain_pages": 0.015,  # $0.015 per domain with pages
        "serp_history": 0.01  # $0.01 per keyword historical request
    })
    
    # Budget limits per report
    max_budget_per_report: float = 0.50  # $0.50 max per report
    serp_budget_per_report: float = 0.20  # $0.20 for SERP calls
    keyword_budget_per_report: float = 0.15  # $0.15 for keyword data
    domain_budget_per_report: float = 0.10  # $0.10 for domain analytics
    
    def calculate_cost(self, endpoint_type: str, num_requests: int) -> float:
        """Calculate cost for a given number of requests"""
        cost_per_request = self.costs_per_request.get(endpoint_type, 0.0)
        return cost_per_request * num_requests


@dataclass
class CacheConfig:
    """Cache configuration for DataForSEO responses"""
    
    # Cache TTL by endpoint type (in seconds)
    cache_ttl: Dict[str, int] = field(default_factory=lambda: {
        "serp_live": 86400,  # 24 hours - SERPs change daily
        "keywords_for_keywords": 604800,  # 7 days - keyword suggestions relatively stable
        "search_volume": 2592000,  # 30 days - search volume updates monthly
        "domain_rank_overview": 86400,  # 24 hours - domain metrics change daily
        "domain_pages": 86400,  # 24 hours
        "serp_history": 2592000  # 30 days - historical data doesn't change
    })
    
    # Cache key prefixes
    cache_key_prefixes: Dict[str, str] = field(default_factory=lambda: {
        "serp_live": "dataforseo:serp:live:",
        "keywords": "dataforseo:keywords:",
        "search_volume": "dataforseo:search_volume:",
        "domain": "dataforseo:domain:",
        "serp_history": "dataforseo:serp:history:"
    })
    
    def get_cache_key(self, endpoint_type: str, params: Dict[str, Any]) -> str:
        """Generate cache key for a request"""
        import hashlib
        import json
        
        prefix = self.cache_key_prefixes.get(endpoint_type, "dataforseo:")
        params_str = json.dumps(params, sort_keys=True)
        params_hash = hashlib.md5(params_str.encode()).hexdigest()
        
        return f"{prefix}{params_hash}"
    
    def get_ttl(self, endpoint_type: str) -> int:
        """Get cache TTL for an endpoint type"""
        return self.cache_ttl.get(endpoint_type, 86400)


@dataclass
class DataForSEOConfig:
    """Master configuration class combining all DataForSEO settings"""
    
    credentials: DataForSEOCredentials = field(default_factory=DataForSEOCredentials)
    endpoints: DataForSEOEndpoints = field(default_factory=DataForSEOEndpoints)
    rate_limits: RateLimitConfig = field(default_factory=RateLimitConfig)
    default_params: DefaultSERPParameters = field(default_factory=DefaultSERPParameters)
    locations: LocationConfig = field(default_factory=LocationConfig)
    response_mappings: ResponseFieldMappings = field(default_factory=ResponseFieldMappings)
    error_codes: ErrorCodeDefinitions = field(default_factory=ErrorCodeDefinitions)
    costs: CostConfig = field(default_factory=CostConfig)
    cache: CacheConfig = field(default_factory=CacheConfig)
    
    @classmethod
    def from_env(cls) -> "DataForSEOConfig":
        """Create configuration from environment variables"""
        return cls()
    
    def get_auth_header(self) -> Dict[str, str]:
        """Get authentication header for requests"""
        import base64
        
        credentials_str = f"{self.credentials.login}:{self.credentials.password}"
        credentials_bytes = credentials_str.encode('ascii')
        base64_bytes = base64.b64encode(credentials_bytes)
        base64_credentials = base64_bytes.decode('ascii')
        
        return {
            "Authorization": f"Basic {base64_credentials}",
            "Content-Type": "application/json"
        }


# Singleton instance
_config_instance: Optional[DataForSEOConfig] = None


def get_config() -> DataForSEOConfig:
    """Get or create singleton configuration instance"""
    global _config_instance
    
    if _config_instance is None:
        _config_instance = DataForSEOConfig.from_env()
    
    return _config_instance


def reset_config():
    """Reset configuration singleton (mainly for testing)"""
    global _config_instance
    _config_instance = None


# Export commonly used configurations
ENDPOINTS = DataForSEOEndpoints()
RATE_LIMITS = RateLimitConfig()
DEFAULT_PARAMS = DefaultSERPParameters()
LOCATIONS = LocationConfig()
RESPONSE_MAPPINGS = ResponseFieldMappings()
ERROR_CODES = ErrorCodeDefinitions()
COSTS = CostConfig()
CACHE = CacheConfig()