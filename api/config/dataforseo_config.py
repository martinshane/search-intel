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


class TaskPriority(Enum):
    """Task priority levels for DataForSEO requests"""
    STANDARD = 1
    HIGH = 2


@dataclass
class RateLimitConfig:
    """Rate limiting configuration for DataForSEO API"""
    requests_per_second: float = 10.0  # 10 requests/second as per spec
    burst_allowance: int = 20  # Allow burst of 20 requests
    cooldown_seconds: float = 1.0  # Cooldown after hitting limit
    retry_attempts: int = 3
    retry_backoff_base: float = 2.0  # Exponential backoff multiplier


@dataclass
class TimeoutConfig:
    """Timeout configuration for API requests"""
    connect_timeout: int = 10  # seconds
    read_timeout: int = 60  # seconds
    total_timeout: int = 120  # seconds


@dataclass
class EndpointConfig:
    """Configuration for a specific API endpoint"""
    path: str
    method: str = "POST"
    requires_authentication: bool = True
    default_priority: TaskPriority = TaskPriority.STANDARD
    estimated_cost: float = 0.0  # Cost in USD per request
    timeout_override: Optional[int] = None


class DataForSEOConfig:
    """Main configuration class for DataForSEO API"""
    
    # Base URLs
    BASE_URL_PRODUCTION = "https://api.dataforseo.com"
    BASE_URL_SANDBOX = "https://sandbox.dataforseo.com"
    
    # API Version
    API_VERSION = "v3"
    
    # Authentication
    _login: Optional[str] = None
    _password: Optional[str] = None
    _environment: DataForSEOEnvironment = DataForSEOEnvironment.PRODUCTION
    
    # Rate limiting
    rate_limit = RateLimitConfig()
    
    # Timeouts
    timeout = TimeoutConfig()
    
    # Endpoints used in the project
    ENDPOINTS = {
        # Module 3: SERP Landscape Analysis
        "serp_live": EndpointConfig(
            path="/v3/serp/google/organic/live/advanced",
            method="POST",
            estimated_cost=0.002,
            timeout_override=90
        ),
        "serp_task_post": EndpointConfig(
            path="/v3/serp/google/organic/task_post",
            method="POST",
            estimated_cost=0.002
        ),
        "serp_task_get": EndpointConfig(
            path="/v3/serp/google/organic/task_get/{task_id}",
            method="GET",
            estimated_cost=0.0
        ),
        
        # Module 8: Backlink Profile Analysis
        "backlinks_summary": EndpointConfig(
            path="/v3/backlinks/summary/live",
            method="POST",
            estimated_cost=0.001
        ),
        "backlinks_history": EndpointConfig(
            path="/v3/backlinks/history/live",
            method="POST",
            estimated_cost=0.003
        ),
        "backlinks_referring_domains": EndpointConfig(
            path="/v3/backlinks/referring_domains/live",
            method="POST",
            estimated_cost=0.004
        ),
        "backlinks_anchors": EndpointConfig(
            path="/v3/backlinks/anchors/live",
            method="POST",
            estimated_cost=0.002
        ),
        "backlinks_page_intersection": EndpointConfig(
            path="/v3/backlinks/page_intersection/live",
            method="POST",
            estimated_cost=0.006
        ),
        "backlinks_bulk_ranks": EndpointConfig(
            path="/v3/backlinks/bulk_ranks/live",
            method="POST",
            estimated_cost=0.005
        ),
        
        # Module 11: Competitive Gap Analysis
        "keywords_for_site": EndpointConfig(
            path="/v3/dataforseo_labs/google/keywords_for_site/live",
            method="POST",
            estimated_cost=0.01,
            timeout_override=120
        ),
        "ranked_keywords": EndpointConfig(
            path="/v3/dataforseo_labs/google/ranked_keywords/live",
            method="POST",
            estimated_cost=0.01,
            timeout_override=120
        ),
        "keyword_intersection": EndpointConfig(
            path="/v3/dataforseo_labs/google/keyword_intersection/live",
            method="POST",
            estimated_cost=0.015,
            timeout_override=150
        ),
        "competitor_domains": EndpointConfig(
            path="/v3/dataforseo_labs/google/competitors_domain/live",
            method="POST",
            estimated_cost=0.008
        ),
        
        # Utility endpoints
        "locations": EndpointConfig(
            path="/v3/serp/google/locations",
            method="GET",
            requires_authentication=True,
            estimated_cost=0.0
        ),
        "languages": EndpointConfig(
            path="/v3/serp/google/languages",
            method="GET",
            requires_authentication=True,
            estimated_cost=0.0
        ),
    }
    
    # Default request parameters
    DEFAULT_PARAMS = {
        "language_code": LanguageCode.ENGLISH,
        "location_code": LocationCode.US_NATIONWIDE,
        "device": DeviceType.DESKTOP.value,
        "os": "windows",
        "depth": 100,  # Number of results to return
        "priority": TaskPriority.STANDARD.value,
    }
    
    # SERP feature visual weights for Module 3
    # Used to calculate "visual position" - how far down the SERP each element pushes organic results
    SERP_FEATURE_VISUAL_WEIGHTS = {
        SERPFeatureType.FEATURED_SNIPPET: 2.0,
        SERPFeatureType.PEOPLE_ALSO_ASK: 0.5,  # Per question
        SERPFeatureType.LOCAL_PACK: 1.5,
        SERPFeatureType.KNOWLEDGE_PANEL: 0.0,  # Sidebar, doesn't push down
        SERPFeatureType.IMAGE_PACK: 1.0,
        SERPFeatureType.VIDEO_CAROUSEL: 1.5,
        SERPFeatureType.TOP_STORIES: 1.0,
        SERPFeatureType.SHOPPING_RESULTS: 1.5,
        SERPFeatureType.RECIPES: 1.0,
        SERPFeatureType.TWITTER: 0.5,
        SERPFeatureType.AI_OVERVIEW: 2.5,
        SERPFeatureType.REDDIT_THREADS: 1.0,
        SERPFeatureType.SITE_LINKS: 0.0,  # Part of organic listing
        SERPFeatureType.RELATED_SEARCHES: 0.0,  # Bottom of page
        SERPFeatureType.CAROUSEL: 1.0,
        SERPFeatureType.PAID: 0.3,  # Per ad
    }
    
    # Backlink quality thresholds for Module 8
    BACKLINK_QUALITY_THRESHOLDS = {
        "high_quality_rank_min": 1,
        "high_quality_rank_max": 1000000,
        "medium_quality_rank_min": 1000001,
        "medium_quality_rank_max": 10000000,
        "low_quality_rank_min": 10000001,
        "spam_score_threshold": 5,  # Out of 10
        "min_referring_domains_threshold": 100,
        "toxic_ratio_threshold": 0.15,  # 15% toxic links
    }
    
    # Keyword opportunity thresholds for Module 11
    KEYWORD_OPPORTUNITY_THRESHOLDS = {
        "min_search_volume": 100,
        "max_keyword_difficulty": 70,  # Out of 100
        "min_cpc": 0.5,  # USD
        "striking_distance_position_min": 11,
        "striking_distance_position_max": 30,
        "gap_priority_volume_min": 500,
        "gap_priority_position_max": 50,
    }
    
    # Error codes that should trigger retries
    RETRYABLE_ERROR_CODES = {
        40501,  # Rate limit exceeded
        50000,  # Internal server error
        50001,  # Service temporarily unavailable
        50002,  # Gateway timeout
    }
    
    # Error codes that should NOT trigger retries
    NON_RETRYABLE_ERROR_CODES = {
        40000,  # Bad request
        40100,  # Unauthorized
        40101,  # Authentication failed
        40102,  # Insufficient funds
        40103,  # Account suspended
        40301,  # Forbidden
        40400,  # Not found
    }
    
    @classmethod
    def initialize(
        cls,
        login: Optional[str] = None,
        password: Optional[str] = None,
        environment: DataForSEOEnvironment = DataForSEOEnvironment.PRODUCTION
    ) -> None:
        """
        Initialize DataForSEO configuration with credentials.
        
        Args:
            login: DataForSEO API login (defaults to DATAFORSEO_LOGIN env var)
            password: DataForSEO API password (defaults to DATAFORSEO_PASSWORD env var)
            environment: API environment (production or sandbox)
        
        Raises:
            ValueError: If credentials are not provided and not found in environment
        """
        cls._login = login or os.getenv("DATAFORSEO_LOGIN")
        cls._password = password or os.getenv("DATAFORSEO_PASSWORD")
        cls._environment = environment
        
        if not cls._login or not cls._password:
            raise ValueError(
                "DataForSEO credentials not provided. "
                "Set DATAFORSEO_LOGIN and DATAFORSEO_PASSWORD environment variables "
                "or pass credentials to initialize() method."
            )
    
    @classmethod
    def validate_credentials(cls) -> bool:
        """
        Validate that credentials are set.
        
        Returns:
            True if credentials are configured, False otherwise
        """
        return bool(cls._login and cls._password)
    
    @classmethod
    def get_credentials(cls) -> tuple[str, str]:
        """
        Get configured credentials.
        
        Returns:
            Tuple of (login, password)
        
        Raises:
            RuntimeError: If credentials are not initialized
        """
        if not cls.validate_credentials():
            raise RuntimeError(
                "DataForSEO credentials not initialized. "
                "Call DataForSEOConfig.initialize() first."
            )
        return cls._login, cls._password
    
    @classmethod
    def get_base_url(cls) -> str:
        """
        Get the base URL for the configured environment.
        
        Returns:
            Base URL string
        """
        if cls._environment == DataForSEOEnvironment.SANDBOX:
            return cls.BASE_URL_SANDBOX
        return cls.BASE_URL_PRODUCTION
    
    @classmethod
    def get_endpoint_url(cls, endpoint_key: str, **path_params) -> str:
        """
        Get full URL for a specific endpoint.
        
        Args:
            endpoint_key: Key from ENDPOINTS dict
            **path_params: Path parameters to substitute (e.g., task_id)
        
        Returns:
            Full endpoint URL
        
        Raises:
            KeyError: If endpoint_key is not found
        """
        if endpoint_key not in cls.ENDPOINTS:
            raise KeyError(f"Unknown endpoint: {endpoint_key}")
        
        endpoint = cls.ENDPOINTS[endpoint_key]
        path = endpoint.path.format(**path_params) if path_params else endpoint.path
        
        return f"{cls.get_base_url()}{path}"
    
    @classmethod
    def get_endpoint_config(cls, endpoint_key: str) -> EndpointConfig:
        """
        Get configuration for a specific endpoint.
        
        Args:
            endpoint_key: Key from ENDPOINTS dict
        
        Returns:
            EndpointConfig object
        
        Raises:
            KeyError: If endpoint_key is not found
        """
        if endpoint_key not in cls.ENDPOINTS:
            raise KeyError(f"Unknown endpoint: {endpoint_key}")
        
        return cls.ENDPOINTS[endpoint_key]
    
    @classmethod
    def estimate_cost(cls, endpoint_key: str, num_requests: int = 1) -> float:
        """
        Estimate cost for a given number of requests to an endpoint.
        
        Args:
            endpoint_key: Key from ENDPOINTS dict
            num_requests: Number of requests to estimate
        
        Returns:
            Estimated cost in USD
        """
        endpoint = cls.get_endpoint_config(endpoint_key)
        return endpoint.estimated_cost * num_requests
    
    @classmethod
    def is_retryable_error(cls, error_code: int) -> bool:
        """
        Check if an error code should trigger a retry.
        
        Args:
            error_code: DataForSEO API error code
        
        Returns:
            True if error is retryable, False otherwise
        """
        return error_code in cls.RETRYABLE_ERROR_CODES
    
    @classmethod
    def get_location_code(cls, location_name: str) -> Optional[int]:
        """
        Get location code by name (helper method).
        
        Args:
            location_name: Human-readable location name (e.g., "US", "UK", "New York")
        
        Returns:
            Location code if found, None otherwise
        """
        location_map = {
            "US": LocationCode.US_NATIONWIDE,
            "USA": LocationCode.US_NATIONWIDE,
            "United States": LocationCode.US_NATIONWIDE,
            "UK": LocationCode.UK_NATIONWIDE,
            "United Kingdom": LocationCode.UK_NATIONWIDE,
            "Canada": LocationCode.CANADA_NATIONWIDE,
            "Australia": LocationCode.AUSTRALIA_NATIONWIDE,
            "Germany": LocationCode.GERMANY_NATIONWIDE,
            "France": LocationCode.FRANCE_NATIONWIDE,
            "India": LocationCode.INDIA_NATIONWIDE,
            "Japan": LocationCode.JAPAN_NATIONWIDE,
            "Brazil": LocationCode.BRAZIL_NATIONWIDE,
            "Mexico": LocationCode.MEXICO_NATIONWIDE,
            "Spain": LocationCode.SPAIN_NATIONWIDE,
            "Italy": LocationCode.ITALY_NATIONWIDE,
            "Netherlands": LocationCode.NETHERLANDS_NATIONWIDE,
            "New York": LocationCode.US_NEW_YORK,
            "Los Angeles": LocationCode.US_LOS_ANGELES,
            "Chicago": LocationCode.US_CHICAGO,
            "London": LocationCode.UK_LONDON,
            "Toronto": LocationCode.CANADA_TORONTO,
            "Sydney": LocationCode.AUSTRALIA_SYDNEY,
            "Berlin": LocationCode.GERMANY_BERLIN,
            "Paris": LocationCode.FRANCE_PARIS,
            "Delhi": LocationCode.INDIA_DELHI,
            "Tokyo": LocationCode.JAPAN_TOKYO,
        }
        
        return location_map.get(location_name)
    
    @classmethod
    def get_serp_feature_weight(cls, feature_type: SERPFeatureType, count: int = 1) -> float:
        """
        Get visual weight for a SERP feature.
        
        Args:
            feature_type: Type of SERP feature
            count: Number of instances (e.g., number of PAA questions)
        
        Returns:
            Total visual weight (positions pushed down)
        """
        base_weight = cls.SERP_FEATURE_VISUAL_WEIGHTS.get(feature_type, 0.0)
        return base_weight * count


# Convenience function to initialize from environment variables
def init_from_env() -> None:
    """Initialize DataForSEO config from environment variables."""
    DataForSEOConfig.initialize()


# Validation helper
def ensure_initialized() -> None:
    """
    Ensure DataForSEO config is initialized before making API calls.
    
    Raises:
        RuntimeError: If credentials are not configured
    """
    if not DataForSEOConfig.validate_credentials():
        try:
            init_from_env()
        except ValueError as e:
            raise RuntimeError(
                "DataForSEO API credentials not configured. "
                "Set DATAFORSEO_LOGIN and DATAFORSEO_PASSWORD environment variables."
            ) from e
