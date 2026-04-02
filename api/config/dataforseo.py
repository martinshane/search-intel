"""
Configuration module for DataForSEO API integration.

Provides endpoint URLs, default parameters, rate limiting, validation helpers,
and request payload builders for SERP analysis tasks.
"""

import os
from typing import Dict, List, Optional, Any
from datetime import datetime
from enum import Enum


# ============================================================================
# API Configuration
# ============================================================================

class DataForSEOConfig:
    """Main configuration class for DataForSEO API."""
    
    # Base URLs
    BASE_URL = "https://api.dataforseo.com"
    VERSION = "v3"
    
    # API Credentials (from environment)
    API_LOGIN = os.getenv("DATAFORSEO_LOGIN", "")
    API_PASSWORD = os.getenv("DATAFORSEO_PASSWORD", "")
    
    # Rate Limits (requests per second)
    RATE_LIMIT_RPS = 2
    RATE_LIMIT_BURST = 5
    
    # Timeout Settings (seconds)
    TIMEOUT_CONNECT = 10
    TIMEOUT_READ = 60
    TIMEOUT_TOTAL = 120
    
    # Retry Settings
    MAX_RETRIES = 3
    RETRY_BACKOFF_FACTOR = 2
    RETRY_STATUS_CODES = [429, 500, 502, 503, 504]
    
    # Cost Tracking
    COST_PER_SERP_REQUEST = 0.002  # USD per live SERP request
    COST_PER_KEYWORD_DATA_REQUEST = 0.0005  # USD per keyword data request


# ============================================================================
# Endpoint URLs
# ============================================================================

class Endpoints:
    """DataForSEO API endpoint URLs."""
    
    # SERP Endpoints
    SERP_GOOGLE_ORGANIC_LIVE = f"{DataForSEOConfig.BASE_URL}/{DataForSEOConfig.VERSION}/serp/google/organic/live/advanced"
    SERP_GOOGLE_ORGANIC_TASK_POST = f"{DataForSEOConfig.BASE_URL}/{DataForSEOConfig.VERSION}/serp/google/organic/task_post"
    SERP_GOOGLE_ORGANIC_TASK_GET = f"{DataForSEOConfig.BASE_URL}/{DataForSEOConfig.VERSION}/serp/google/organic/task_get/advanced"
    SERP_GOOGLE_ORGANIC_TASKS_READY = f"{DataForSEOConfig.BASE_URL}/{DataForSEOConfig.VERSION}/serp/google/organic/tasks_ready"
    
    # Keyword Data Endpoints
    KEYWORDS_DATA_GOOGLE_SEARCH_VOLUME_LIVE = f"{DataForSEOConfig.BASE_URL}/{DataForSEOConfig.VERSION}/keywords_data/google/search_volume/live"
    KEYWORDS_DATA_GOOGLE_KEYWORDS_FOR_KEYWORDS = f"{DataForSEOConfig.BASE_URL}/{DataForSEOConfig.VERSION}/keywords_data/google/keywords_for_keywords/live"
    
    # Domain Analytics Endpoints
    DOMAIN_ANALYTICS_GOOGLE_OVERVIEW = f"{DataForSEOConfig.BASE_URL}/{DataForSEOConfig.VERSION}/domain_analytics/google/overview/live"
    DOMAIN_ANALYTICS_GOOGLE_COMPETITORS = f"{DataForSEOConfig.BASE_URL}/{DataForSEOConfig.VERSION}/domain_analytics/google/competitors/live"
    
    # On-Page Endpoints
    ON_PAGE_API_TASK_POST = f"{DataForSEOConfig.BASE_URL}/{DataForSEOConfig.VERSION}/on_page/task_post"
    ON_PAGE_API_PAGES = f"{DataForSEOConfig.BASE_URL}/{DataForSEOConfig.VERSION}/on_page/pages"


# ============================================================================
# Location Codes
# ============================================================================

class LocationCodes:
    """Common location codes for SERP requests."""
    
    # Major US Markets
    US_NATIONAL = 2840  # United States
    US_NEW_YORK = 1023191  # New York, NY
    US_LOS_ANGELES = 1023768  # Los Angeles, CA
    US_CHICAGO = 1014221  # Chicago, IL
    US_HOUSTON = 1022183  # Houston, TX
    US_PHOENIX = 1023158  # Phoenix, AZ
    US_PHILADELPHIA = 1023133  # Philadelphia, PA
    US_SAN_ANTONIO = 1026611  # San Antonio, TX
    US_SAN_DIEGO = 1023208  # San Diego, CA
    US_DALLAS = 1019147  # Dallas, TX
    US_SAN_JOSE = 1023208  # San Jose, CA
    
    # International Markets
    UK_NATIONAL = 2826  # United Kingdom
    CANADA_NATIONAL = 2124  # Canada
    AUSTRALIA_NATIONAL = 2036  # Australia
    GERMANY_NATIONAL = 2276  # Germany
    FRANCE_NATIONAL = 2250  # France
    SPAIN_NATIONAL = 2724  # Spain
    ITALY_NATIONAL = 2380  # Italy
    BRAZIL_NATIONAL = 2076  # Brazil
    INDIA_NATIONAL = 2356  # India
    JAPAN_NATIONAL = 2392  # Japan
    
    @classmethod
    def get_default(cls) -> int:
        """Get default location code (US National)."""
        return cls.US_NATIONAL
    
    @classmethod
    def get_location_name(cls, code: int) -> Optional[str]:
        """Get human-readable name for a location code."""
        location_map = {
            2840: "United States",
            1023191: "New York, NY",
            1023768: "Los Angeles, CA",
            1014221: "Chicago, IL",
            1022183: "Houston, TX",
            1023158: "Phoenix, AZ",
            1023133: "Philadelphia, PA",
            1026611: "San Antonio, TX",
            1023208: "San Diego, CA",
            1019147: "Dallas, TX",
            2826: "United Kingdom",
            2124: "Canada",
            2036: "Australia",
            2276: "Germany",
            2250: "France",
            2724: "Spain",
            2380: "Italy",
            2076: "Brazil",
            2356: "India",
            2392: "Japan",
        }
        return location_map.get(code)


# ============================================================================
# Language Codes
# ============================================================================

class LanguageCodes:
    """Common language codes for SERP requests."""
    
    ENGLISH = "en"
    SPANISH = "es"
    FRENCH = "fr"
    GERMAN = "de"
    ITALIAN = "it"
    PORTUGUESE = "pt"
    DUTCH = "nl"
    RUSSIAN = "ru"
    CHINESE_SIMPLIFIED = "zh-CN"
    CHINESE_TRADITIONAL = "zh-TW"
    JAPANESE = "ja"
    KOREAN = "ko"
    ARABIC = "ar"
    HINDI = "hi"
    
    @classmethod
    def get_default(cls) -> str:
        """Get default language code (English)."""
        return cls.ENGLISH


# ============================================================================
# Device Types
# ============================================================================

class DeviceTypes(str, Enum):
    """Device types for SERP requests."""
    
    DESKTOP = "desktop"
    MOBILE = "mobile"
    TABLET = "tablet"
    
    @classmethod
    def get_default(cls) -> str:
        """Get default device type (desktop)."""
        return cls.DESKTOP.value


# ============================================================================
# Search Engine Types
# ============================================================================

class SearchEngineTypes(str, Enum):
    """Search engine types."""
    
    GOOGLE = "google"
    BING = "bing"
    YAHOO = "yahoo"
    
    @classmethod
    def get_default(cls) -> str:
        """Get default search engine (Google)."""
        return cls.GOOGLE.value


# ============================================================================
# SERP Feature Types
# ============================================================================

class SERPFeatureTypes:
    """Types of SERP features that can appear in results."""
    
    FEATURED_SNIPPET = "featured_snippet"
    PEOPLE_ALSO_ASK = "people_also_ask"
    KNOWLEDGE_PANEL = "knowledge_panel"
    LOCAL_PACK = "local_pack"
    VIDEO_CAROUSEL = "video"
    IMAGE_PACK = "images"
    SHOPPING_RESULTS = "shopping"
    TOP_STORIES = "top_stories"
    SITE_LINKS = "sitelinks"
    TWITTER = "twitter"
    AI_OVERVIEW = "ai_overview"
    REDDIT_THREADS = "reddit"
    RELATED_SEARCHES = "related_searches"
    
    # Visual position weights (how many organic positions each feature "costs")
    FEATURE_WEIGHTS = {
        FEATURED_SNIPPET: 2.0,
        PEOPLE_ALSO_ASK: 0.5,  # per question
        KNOWLEDGE_PANEL: 1.5,
        LOCAL_PACK: 3.0,
        VIDEO_CAROUSEL: 1.5,
        IMAGE_PACK: 1.0,
        SHOPPING_RESULTS: 2.0,
        TOP_STORIES: 1.5,
        SITE_LINKS: 0.5,
        TWITTER: 1.0,
        AI_OVERVIEW: 3.0,
        REDDIT_THREADS: 1.0,
        RELATED_SEARCHES: 0.0,  # appears at bottom
    }
    
    @classmethod
    def get_weight(cls, feature_type: str) -> float:
        """Get visual position weight for a feature type."""
        return cls.FEATURE_WEIGHTS.get(feature_type, 0.5)


# ============================================================================
# Default Parameters
# ============================================================================

class DefaultParameters:
    """Default parameter values for API requests."""
    
    # SERP Request Defaults
    SERP_MAX_CRAWL_PAGES = 10  # Only need first page usually
    SERP_DEPTH = 100  # Get up to 100 results
    SERP_CALCULATE_RECTANGLES = False  # Don't need DOM rectangles
    
    # Keyword Data Defaults
    KEYWORD_DATA_LIMIT = 100  # Max keywords per request
    KEYWORD_SEARCH_PARTNERS = False  # Google search only
    
    # Pagination
    MAX_RESULTS_PER_REQUEST = 100
    DEFAULT_OFFSET = 0
    
    # Filtering
    MIN_SEARCH_VOLUME = 10  # Minimum monthly search volume
    MIN_KEYWORD_DIFFICULTY = 0
    MAX_KEYWORD_DIFFICULTY = 100


# ============================================================================
# Helper Functions
# ============================================================================

def validate_credentials() -> bool:
    """
    Validate that DataForSEO API credentials are configured.
    
    Returns:
        bool: True if credentials are present, False otherwise
    """
    return bool(DataForSEOConfig.API_LOGIN and DataForSEOConfig.API_PASSWORD)


def get_auth() -> tuple:
    """
    Get API authentication tuple for requests.
    
    Returns:
        tuple: (username, password) for HTTP Basic Auth
    
    Raises:
        ValueError: If credentials are not configured
    """
    if not validate_credentials():
        raise ValueError(
            "DataForSEO API credentials not configured. "
            "Set DATAFORSEO_LOGIN and DATAFORSEO_PASSWORD environment variables."
        )
    return (DataForSEOConfig.API_LOGIN, DataForSEOConfig.API_PASSWORD)


def build_serp_live_payload(
    keyword: str,
    location_code: Optional[int] = None,
    language_code: Optional[str] = None,
    device: Optional[str] = None,
    depth: Optional[int] = None,
    calculate_rectangles: bool = False,
    **kwargs
) -> List[Dict[str, Any]]:
    """
    Build request payload for live SERP data.
    
    Args:
        keyword: Search query to analyze
        location_code: Geographic location code (default: US National)
        language_code: Language code (default: English)
        device: Device type (default: desktop)
        depth: Number of results to retrieve (default: 100)
        calculate_rectangles: Whether to calculate DOM rectangles
        **kwargs: Additional parameters to include in payload
    
    Returns:
        List containing the request payload dictionary
    """
    payload = {
        "keyword": keyword,
        "location_code": location_code or LocationCodes.get_default(),
        "language_code": language_code or LanguageCodes.get_default(),
        "device": device or DeviceTypes.get_default(),
        "depth": depth or DefaultParameters.SERP_DEPTH,
        "calculate_rectangles": calculate_rectangles,
        "se_type": SearchEngineTypes.get_default(),
    }
    
    # Add any additional parameters
    payload.update(kwargs)
    
    # DataForSEO expects array of task objects
    return [payload]


def build_serp_batch_payload(
    keywords: List[str],
    location_code: Optional[int] = None,
    language_code: Optional[str] = None,
    device: Optional[str] = None,
    **kwargs
) -> List[Dict[str, Any]]:
    """
    Build batch request payload for multiple keywords.
    
    Args:
        keywords: List of search queries to analyze
        location_code: Geographic location code
        language_code: Language code
        device: Device type
        **kwargs: Additional parameters for each task
    
    Returns:
        List of task dictionaries for batch processing
    """
    tasks = []
    for keyword in keywords:
        task = {
            "keyword": keyword,
            "location_code": location_code or LocationCodes.get_default(),
            "language_code": language_code or LanguageCodes.get_default(),
            "device": device or DeviceTypes.get_default(),
            "se_type": SearchEngineTypes.get_default(),
        }
        task.update(kwargs)
        tasks.append(task)
    
    return tasks


def build_keyword_search_volume_payload(
    keywords: List[str],
    location_code: Optional[int] = None,
    language_code: Optional[str] = None,
    search_partners: bool = False,
    **kwargs
) -> List[Dict[str, Any]]:
    """
    Build request payload for keyword search volume data.
    
    Args:
        keywords: List of keywords to get search volume for
        location_code: Geographic location code
        language_code: Language code
        search_partners: Include search partners data
        **kwargs: Additional parameters
    
    Returns:
        List containing the request payload dictionary
    """
    payload = {
        "keywords": keywords,
        "location_code": location_code or LocationCodes.get_default(),
        "language_code": language_code or LanguageCodes.get_default(),
        "search_partners": search_partners,
    }
    
    payload.update(kwargs)
    
    return [payload]


def build_keywords_for_keywords_payload(
    keywords: List[str],
    location_code: Optional[int] = None,
    language_code: Optional[str] = None,
    limit: Optional[int] = None,
    **kwargs
) -> List[Dict[str, Any]]:
    """
    Build request payload for related keywords discovery.
    
    Args:
        keywords: Seed keywords to find related terms for
        location_code: Geographic location code
        language_code: Language code
        limit: Maximum number of results to return
        **kwargs: Additional parameters
    
    Returns:
        List containing the request payload dictionary
    """
    payload = {
        "keywords": keywords,
        "location_code": location_code or LocationCodes.get_default(),
        "language_code": language_code or LanguageCodes.get_default(),
    }
    
    if limit:
        payload["limit"] = limit
    
    payload.update(kwargs)
    
    return [payload]


def build_domain_overview_payload(
    target_domain: str,
    location_code: Optional[int] = None,
    language_code: Optional[str] = None,
    **kwargs
) -> List[Dict[str, Any]]:
    """
    Build request payload for domain overview analytics.
    
    Args:
        target_domain: Domain to analyze
        location_code: Geographic location code
        language_code: Language code
        **kwargs: Additional parameters
    
    Returns:
        List containing the request payload dictionary
    """
    payload = {
        "target": target_domain,
        "location_code": location_code or LocationCodes.get_default(),
        "language_code": language_code or LanguageCodes.get_default(),
    }
    
    payload.update(kwargs)
    
    return [payload]


def build_competitors_payload(
    target_domain: str,
    location_code: Optional[int] = None,
    language_code: Optional[str] = None,
    limit: int = 10,
    **kwargs
) -> List[Dict[str, Any]]:
    """
    Build request payload for competitor discovery.
    
    Args:
        target_domain: Domain to find competitors for
        location_code: Geographic location code
        language_code: Language code
        limit: Maximum number of competitors to return
        **kwargs: Additional parameters
    
    Returns:
        List containing the request payload dictionary
    """
    payload = {
        "target": target_domain,
        "location_code": location_code or LocationCodes.get_default(),
        "language_code": language_code or LanguageCodes.get_default(),
        "limit": limit,
    }
    
    payload.update(kwargs)
    
    return [payload]


def estimate_request_cost(
    num_serp_requests: int = 0,
    num_keyword_requests: int = 0
) -> float:
    """
    Estimate total cost for a set of API requests.
    
    Args:
        num_serp_requests: Number of SERP requests
        num_keyword_requests: Number of keyword data requests
    
    Returns:
        float: Estimated cost in USD
    """
    serp_cost = num_serp_requests * DataForSEOConfig.COST_PER_SERP_REQUEST
    keyword_cost = num_keyword_requests * DataForSEOConfig.COST_PER_KEYWORD_DATA_REQUEST
    
    return serp_cost + keyword_cost


def get_priority_keywords(
    gsc_queries: List[Dict[str, Any]],
    max_keywords: int = 50,
    min_impressions: int = 100,
    brand_terms: Optional[List[str]] = None
) -> List[str]:
    """
    Select priority keywords for SERP analysis from GSC data.
    
    Logic:
    1. Filter out branded queries (if brand_terms provided)
    2. Filter out queries below impression threshold
    3. Sort by impressions DESC
    4. Take top N
    5. Also include any queries with large position changes (>3 spots in 30d)
    
    Args:
        gsc_queries: List of GSC query data dictionaries
        max_keywords: Maximum number of keywords to return
        min_impressions: Minimum impressions threshold
        brand_terms: List of brand terms to filter out (case-insensitive)
    
    Returns:
        List of selected keywords
    """
    filtered_queries = []
    brand_terms_lower = [term.lower() for term in (brand_terms or [])]
    
    for query_data in gsc_queries:
        query = query_data.get("query", "").lower()
        impressions = query_data.get("impressions", 0)
        
        # Filter out branded queries
        if brand_terms_lower:
            is_branded = any(brand in query for brand in brand_terms_lower)
            if is_branded:
                continue
        
        # Filter by impressions
        if impressions < min_impressions:
            continue
        
        filtered_queries.append(query_data)
    
    # Sort by impressions
    filtered_queries.sort(key=lambda x: x.get("impressions", 0), reverse=True)
    
    # Take top N
    priority_keywords = [q["query"] for q in filtered_queries[:max_keywords]]
    
    # TODO: Add logic to include queries with large position changes
    # This would require historical position data from GSC
    
    return priority_keywords


def parse_serp_features(serp_item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse SERP features from DataForSEO response item.
    
    Args:
        serp_item: Single SERP result item from DataForSEO response
    
    Returns:
        Dictionary with parsed feature information:
        {
            "features_present": ["featured_snippet", "people_also_ask", ...],
            "feature_counts": {"people_also_ask": 4, ...},
            "visual_position_offset": 5.5,
            "feature_details": {...}
        }
    """
    features_present = []
    feature_counts = {}
    feature_details = {}
    visual_offset = 0.0
    
    items = serp_item.get("items", [])
    
    for item in items:
        item_type = item.get("type", "")
        
        if item_type == "featured_snippet":
            if SERPFeatureTypes.FEATURED_SNIPPET not in features_present:
                features_present.append(SERPFeatureTypes.FEATURED_SNIPPET)
                visual_offset += SERPFeatureTypes.get_weight(SERPFeatureTypes.FEATURED_SNIPPET)
                feature_details["featured_snippet"] = {
                    "url": item.get("url"),
                    "description": item.get("description")
                }
        
        elif item_type == "people_also_ask":
            if SERPFeatureTypes.PEOPLE_ALSO_ASK not in features_present:
                features_present.append(SERPFeatureTypes.PEOPLE_ALSO_ASK)
            
            questions = item.get("items", [])
            paa_count = len(questions)
            feature_counts["people_also_ask"] = paa_count
            visual_offset += paa_count * SERPFeatureTypes.get_weight(SERPFeatureTypes.PEOPLE_ALSO_ASK)
        
        elif item_type == "knowledge_panel":
            if SERPFeatureTypes.KNOWLEDGE_PANEL not in features_present:
                features_present.append(SERPFeatureTypes.KNOWLEDGE_PANEL)
                visual_offset += SERPFeatureTypes.get_weight(SERPFeatureTypes.KNOWLEDGE_PANEL)
        
        elif item_type == "local_pack":
            if SERPFeatureTypes.LOCAL_PACK not in features_present:
                features_present.append(SERPFeatureTypes.LOCAL_PACK)
                visual_offset += SERPFeatureTypes.get_weight(SERPFeatureTypes.LOCAL_PACK)
        
        elif item_type == "video":
            if SERPFeatureTypes.VIDEO_CAROUSEL not in features_present:
                features_present.append(SERPFeatureTypes.VIDEO_CAROUSEL)
                visual_offset += SERPFeatureTypes.get_weight(SERPFeatureTypes.VIDEO_CAROUSEL)
        
        elif item_type == "images":
            if SERPFeatureTypes.IMAGE_PACK not in features_present:
                features_present.append(SERPFeatureTypes.IMAGE_PACK)
                visual_offset += SERPFeatureTypes.get_weight(SERPFeatureTypes.IMAGE_PACK)
        
        elif item_type == "shopping":
            if SERPFeatureTypes.SHOPPING_RESULTS not in features_present:
                features_present.append(SERPFeatureTypes.SHOPPING_RESULTS)
                visual_offset += SERPFeatureTypes.get_weight(SERPFeatureTypes.SHOPPING_RESULTS)
        
        elif item_type == "top_stories":
            if SERPFeatureTypes.TOP_STORIES not in features_present:
                features_present.append(SERPFeatureTypes.TOP_STORIES)
                visual_offset += SERPFeatureTypes.get_weight(SERPFeatureTypes.TOP_STORIES)
        
        elif item_type == "ai_overview":
            if SERPFeatureTypes.AI_OVERVIEW not in features_present:
                features_present.append(SERPFeatureTypes.AI_OVERVIEW)
                visual_offset += SERPFeatureTypes.get_weight(SERPFeatureTypes.AI_OVERVIEW)
        
        elif item_type == "reddit":
            if SERPFeatureTypes.REDDIT_THREADS not in features_present:
                features_present.append(SERPFeatureTypes.REDDIT_THREADS)
                visual_offset += SERPFeatureTypes.get_weight(SERPFeatureTypes.REDDIT_THREADS)
    
    return {
        "features_present": features_present,
        "feature_counts": feature_counts,
        "visual_position_offset": visual_offset,
        "feature_details": feature_details
    }


def calculate_visual_position(
    organic_rank: int,
    serp_features: Dict[str, Any]
) -> float:
    """
    Calculate visual position accounting for SERP features.
    
    Args:
        organic_rank: Organic ranking position (1-100)
        serp_features: Parsed SERP features from parse_serp_features()
    
    Returns:
        float: Visual position (organic rank + offset from features)
    """
    return organic_rank + serp_features.get("visual_position_offset", 0.0)


def get_intent_from_serp_features(features: List[str]) -> str:
    """
    Classify search intent based on SERP features present.
    
    Args:
        features: List of SERP feature types present
    
    Returns:
        str: Intent classification (informational, commercial, transactional, navigational)
    """
    if SERPFeatureTypes.SHOPPING_RESULTS in features:
        return "transactional"
    
    if SERPFeatureTypes.LOCAL_PACK in features:
        return "transactional"
    
    if SERPFeatureTypes.KNOWLEDGE_PANEL in features and SERPFeatureTypes.SITE_LINKS in features:
        return "navigational"
    
    if SERPFeatureTypes.PEOPLE_ALSO_ASK in features or SERPFeatureTypes.FEATURED_SNIPPET in features:
        return "informational"
    
    if SERPFeatureTypes.VIDEO_CAROUSEL in features or SERPFeatureTypes.IMAGE_PACK in features:
        return "informational"
    
    # Default to commercial if features don't clearly indicate other intent
    return "commercial"


def validate_location_code(location_code: int) -> bool:
    """
    Validate that a location code is recognized.
    
    Args:
        location_code: Location code to validate
    
    Returns:
        bool: True if valid, False otherwise
    """
    return LocationCodes.get_location_name(location_code) is not None


def validate_language_code(language_code: str) -> bool:
    """
    Validate that a language code is recognized.
    
    Args:
        language_code: Language code to validate (e.g., 'en', 'es')
    
    Returns:
        bool: True if valid, False otherwise
    """
    valid_codes = [
        LanguageCodes.ENGLISH,
        LanguageCodes.SPANISH,
        LanguageCodes.FRENCH,
        LanguageCodes.GERMAN,
        LanguageCodes.ITALIAN,
        LanguageCodes.PORTUGUESE,
        LanguageCodes.DUTCH,
        LanguageCodes.RUSSIAN,
        LanguageCodes.CHINESE_SIMPLIFIED,
        LanguageCodes.CHINESE_TRADITIONAL,
        LanguageCodes.JAPANESE,
        LanguageCodes.KOREAN,
        LanguageCodes.ARABIC,
        LanguageCodes.HINDI,
    ]
    return language_code in valid_codes


def get_headers() -> Dict[str, str]:
    """
    Get standard headers for DataForSEO API requests.
    
    Returns:
        Dictionary of HTTP headers
    """
    return {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def format_error_message(response_data: Dict[str, Any]) -> str:
    """
    Format error message from DataForSEO API response.
    
    Args:
        response_data: Response JSON from API
    
    Returns:
        str: Formatted error message
    """
    status_code = response_data.get("status_code", 0)
    status_message = response_data.get("status_message", "Unknown error")
    
    tasks = response_data.get("tasks", [])
    if tasks:
        task_errors = []
        for task in tasks:
            if task.get("status_code") != 20000:
                task_errors.append(
                    f"Task error: {task.get('status_message', 'Unknown')}"
                )
        if task_errors:
            return f"API Error {status_code}: {status_message}. " + "; ".join(task_errors)
    
    return f"API Error {status_code}: {status_message}"

