"""
Configuration settings for Search Intelligence Report API.
Loads from environment variables and validates critical settings.
"""

import os
from typing import Optional
from functools import lru_cache


class Settings:
    """Application settings loaded from environment variables."""
    
    # Application
    APP_NAME: str = "Search Intelligence Report API"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = os.getenv("DEBUG", "false").lower() == "true"
    ENVIRONMENT: str = os.getenv("ENVIRONMENT", "development")
    
    # API
    API_HOST: str = os.getenv("API_HOST", "0.0.0.0")
    API_PORT: int = int(os.getenv("API_PORT", "8000"))
    CORS_ORIGINS: list = os.getenv("CORS_ORIGINS", "*").split(",")
    
    # Database (Supabase)
    DATABASE_URL: str = os.getenv("DATABASE_URL", "")
    SUPABASE_URL: str = os.getenv("SUPABASE_URL", "")
    SUPABASE_KEY: str = os.getenv("SUPABASE_KEY", "")
    SUPABASE_SERVICE_ROLE_KEY: str = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")
    
    # Redis (for job queue and caching)
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    CACHE_TTL_SECONDS: int = int(os.getenv("CACHE_TTL_SECONDS", "86400"))  # 24 hours
    
    # Google OAuth
    GOOGLE_CLIENT_ID: str = os.getenv("GOOGLE_CLIENT_ID", "")
    GOOGLE_CLIENT_SECRET: str = os.getenv("GOOGLE_CLIENT_SECRET", "")
    GOOGLE_REDIRECT_URI: str = os.getenv("GOOGLE_REDIRECT_URI", "")
    GOOGLE_OAUTH_SCOPES: list = [
        "https://www.googleapis.com/auth/webmasters.readonly",
        "https://www.googleapis.com/auth/analytics.readonly",
        "openid",
        "email",
        "profile"
    ]
    
    # Google Search Console API
    GSC_API_URL: str = "https://www.googleapis.com/webmasters/v3"
    GSC_ROWS_PER_REQUEST: int = 25000
    GSC_DATE_RANGE_MONTHS: int = 16
    GSC_RATE_LIMIT_PER_SECOND: int = 10
    GSC_TIMEOUT_SECONDS: int = 30
    
    # Google Analytics 4 API
    GA4_API_URL: str = "https://analyticsdata.googleapis.com/v1beta"
    GA4_DATE_RANGE_MONTHS: int = 16
    GA4_RATE_LIMIT_PER_SECOND: int = 10
    GA4_TIMEOUT_SECONDS: int = 30
    
    # DataForSEO API
    DATAFORSEO_LOGIN: str = os.getenv("DATAFORSEO_LOGIN", "")
    DATAFORSEO_PASSWORD: str = os.getenv("DATAFORSEO_PASSWORD", "")
    DATAFORSEO_API_URL: str = os.getenv(
        "DATAFORSEO_API_URL",
        "https://api.dataforseo.com/v3/"
    )
    DATAFORSEO_RATE_LIMIT: int = int(os.getenv("DATAFORSEO_RATE_LIMIT", "2"))  # requests per second
    DATAFORSEO_TIMEOUT: int = int(os.getenv("DATAFORSEO_TIMEOUT", "60"))  # seconds
    DATAFORSEO_ENABLED: bool = os.getenv("DATAFORSEO_ENABLED", "true").lower() == "true"
    DATAFORSEO_TOP_KEYWORDS_LIMIT: int = int(os.getenv("DATAFORSEO_TOP_KEYWORDS_LIMIT", "50"))
    DATAFORSEO_BUDGET_PER_REPORT: float = float(os.getenv("DATAFORSEO_BUDGET_PER_REPORT", "0.20"))
    
    # LLM (Claude API for narrative generation)
    ANTHROPIC_API_KEY: str = os.getenv("ANTHROPIC_API_KEY", "")
    ANTHROPIC_MODEL: str = os.getenv("ANTHROPIC_MODEL", "claude-3-5-sonnet-20241022")
    ANTHROPIC_MAX_TOKENS: int = int(os.getenv("ANTHROPIC_MAX_TOKENS", "4096"))
    ANTHROPIC_TEMPERATURE: float = float(os.getenv("ANTHROPIC_TEMPERATURE", "0.7"))
    LLM_ENABLED: bool = os.getenv("LLM_ENABLED", "true").lower() == "true"
    
    # Report Generation
    REPORT_TIMEOUT_SECONDS: int = int(os.getenv("REPORT_TIMEOUT_SECONDS", "600"))  # 10 minutes
    REPORT_STORAGE_DAYS: int = int(os.getenv("REPORT_STORAGE_DAYS", "90"))
    MAX_CONCURRENT_REPORTS: int = int(os.getenv("MAX_CONCURRENT_REPORTS", "5"))
    
    # Site Crawl
    CRAWL_MAX_PAGES: int = int(os.getenv("CRAWL_MAX_PAGES", "5000"))
    CRAWL_TIMEOUT_SECONDS: int = int(os.getenv("CRAWL_TIMEOUT_SECONDS", "1800"))  # 30 minutes
    CRAWL_CONCURRENT_REQUESTS: int = int(os.getenv("CRAWL_CONCURRENT_REQUESTS", "8"))
    CRAWL_DELAY_SECONDS: float = float(os.getenv("CRAWL_DELAY_SECONDS", "0.5"))
    CRAWL_USER_AGENT: str = os.getenv(
        "CRAWL_USER_AGENT",
        "SearchIntelligenceBot/1.0 (Report Generation Crawler)"
    )
    
    # Analysis Thresholds
    MIN_CLICKS_FOR_ANALYSIS: int = int(os.getenv("MIN_CLICKS_FOR_ANALYSIS", "10"))
    MIN_IMPRESSIONS_FOR_ANALYSIS: int = int(os.getenv("MIN_IMPRESSIONS_FOR_ANALYSIS", "100"))
    STRIKING_DISTANCE_POSITION_MIN: int = int(os.getenv("STRIKING_DISTANCE_POSITION_MIN", "8"))
    STRIKING_DISTANCE_POSITION_MAX: int = int(os.getenv("STRIKING_DISTANCE_POSITION_MAX", "20"))
    THIN_CONTENT_WORD_COUNT: int = int(os.getenv("THIN_CONTENT_WORD_COUNT", "500"))
    HIGH_BOUNCE_RATE_THRESHOLD: float = float(os.getenv("HIGH_BOUNCE_RATE_THRESHOLD", "0.85"))
    LOW_SESSION_DURATION_THRESHOLD: int = int(os.getenv("LOW_SESSION_DURATION_THRESHOLD", "20"))  # seconds
    
    # ML/Stats Settings
    MSTL_PERIODS: list = [7, 30]  # Weekly and monthly seasonality
    CHANGE_POINT_MIN_SIZE: int = 14  # Minimum segment size for change point detection
    ISOLATION_FOREST_CONTAMINATION: float = 0.1
    CLUSTERING_MIN_SAMPLES: int = 5
    FORECAST_HORIZONS_DAYS: list = [30, 60, 90]
    
    # Logging
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    LOG_FORMAT: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    # Security
    SECRET_KEY: str = os.getenv("SECRET_KEY", "")
    JWT_ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "30"))
    
    def validate(self) -> None:
        """
        Validate that required settings are configured.
        Raises ValueError if critical settings are missing.
        """
        errors = []
        
        # Always required
        if not self.SECRET_KEY:
            errors.append("SECRET_KEY must be set")
        
        if not self.DATABASE_URL and not (self.SUPABASE_URL and self.SUPABASE_KEY):
            errors.append("Either DATABASE_URL or SUPABASE_URL+SUPABASE_KEY must be set")
        
        # Google OAuth required for core functionality
        if not self.GOOGLE_CLIENT_ID:
            errors.append("GOOGLE_CLIENT_ID must be set")
        
        if not self.GOOGLE_CLIENT_SECRET:
            errors.append("GOOGLE_CLIENT_SECRET must be set")
        
        if not self.GOOGLE_REDIRECT_URI:
            errors.append("GOOGLE_REDIRECT_URI must be set")
        
        # DataForSEO validation - only if enabled
        if self.DATAFORSEO_ENABLED:
            if not self.DATAFORSEO_LOGIN:
                errors.append("DATAFORSEO_LOGIN must be set when DataForSEO is enabled")
            
            if not self.DATAFORSEO_PASSWORD:
                errors.append("DATAFORSEO_PASSWORD must be set when DataForSEO is enabled")
            
            if self.DATAFORSEO_RATE_LIMIT < 1:
                errors.append("DATAFORSEO_RATE_LIMIT must be at least 1")
            
            if self.DATAFORSEO_TIMEOUT < 1:
                errors.append("DATAFORSEO_TIMEOUT must be at least 1 second")
            
            if not self.DATAFORSEO_API_URL.startswith("https://"):
                errors.append("DATAFORSEO_API_URL must start with https://")
            
            if not self.DATAFORSEO_API_URL.endswith("/"):
                errors.append("DATAFORSEO_API_URL must end with /")
        
        # LLM validation - only if enabled
        if self.LLM_ENABLED:
            if not self.ANTHROPIC_API_KEY:
                errors.append("ANTHROPIC_API_KEY must be set when LLM is enabled")
        
        # Raise all validation errors at once
        if errors:
            raise ValueError(
                "Configuration validation failed:\n" + "\n".join(f"  - {e}" for e in errors)
            )
    
    def get_dataforseo_credentials(self) -> tuple[str, str]:
        """
        Get DataForSEO credentials.
        
        Returns:
            Tuple of (login, password)
        
        Raises:
            ValueError: If DataForSEO is enabled but credentials are not set
        """
        if not self.DATAFORSEO_ENABLED:
            raise ValueError("DataForSEO is not enabled")
        
        if not self.DATAFORSEO_LOGIN or not self.DATAFORSEO_PASSWORD:
            raise ValueError("DataForSEO credentials not configured")
        
        return (self.DATAFORSEO_LOGIN, self.DATAFORSEO_PASSWORD)
    
    def get_database_url(self) -> str:
        """
        Get the database connection URL.
        Prefers DATABASE_URL, falls back to Supabase URL.
        
        Returns:
            Database connection string
        """
        if self.DATABASE_URL:
            return self.DATABASE_URL
        
        if self.SUPABASE_URL:
            # Convert Supabase URL to direct PostgreSQL connection
            # Supabase URL format: https://xxx.supabase.co
            # Connection format: postgresql://postgres:[PASSWORD]@db.xxx.supabase.co:5432/postgres
            project_ref = self.SUPABASE_URL.replace("https://", "").replace(".supabase.co", "")
            password = self.SUPABASE_SERVICE_ROLE_KEY or self.SUPABASE_KEY
            return f"postgresql://postgres:{password}@db.{project_ref}.supabase.co:5432/postgres"
        
        raise ValueError("No database URL configured")
    
    def is_production(self) -> bool:
        """Check if running in production environment."""
        return self.ENVIRONMENT.lower() == "production"
    
    def is_development(self) -> bool:
        """Check if running in development environment."""
        return self.ENVIRONMENT.lower() == "development"


@lru_cache()
def get_settings() -> Settings:
    """
    Get cached settings instance.
    Validates settings on first call.
    
    Returns:
        Validated Settings instance
    
    Raises:
        ValueError: If settings validation fails
    """
    settings = Settings()
    settings.validate()
    return settings


# Export settings instance for convenience
settings = get_settings()

# Configuration presets for different analysis types
ANALYSIS_PRESETS = {
    "quick": {
        "gsc_date_range_months": 6,
        "ga4_date_range_months": 6,
        "dataforseo_top_keywords": 25,
        "crawl_max_pages": 1000,
        "skip_modules": ["algorithm_updates", "intent_migration"],
    },
    "standard": {
        "gsc_date_range_months": 12,
        "ga4_date_range_months": 12,
        "dataforseo_top_keywords": 50,
        "crawl_max_pages": 3000,
        "skip_modules": [],
    },
    "comprehensive": {
        "gsc_date_range_months": 16,
        "ga4_date_range_months": 16,
        "dataforseo_top_keywords": 100,
        "crawl_max_pages": 5000,
        "skip_modules": [],
    },
}

# Algorithm update sources for Module 6
ALGORITHM_UPDATE_SOURCES = [
    {
        "name": "Google Search Status Dashboard",
        "url": "https://status.search.google.com/",
        "type": "official",
    },
    {
        "name": "Semrush Sensor",
        "url": "https://www.semrush.com/sensor/",
        "type": "tracker",
    },
    {
        "name": "Moz Google Algorithm Update History",
        "url": "https://moz.com/google-algorithm-change",
        "type": "historical",
    },
]

# Position-based CTR curves (baseline, adjusted per SERP feature in Module 3)
DEFAULT_CTR_CURVE = {
    1: 0.284,
    2: 0.147,
    3: 0.094,
    4: 0.067,
    5: 0.051,
    6: 0.041,
    7: 0.034,
    8: 0.029,
    9: 0.025,
    10: 0.022,
}

# SERP feature visual position weights (for Module 3)
SERP_FEATURE_WEIGHTS = {
    "featured_snippet": 2.0,
    "knowledge_panel": 1.5,
    "ai_overview": 2.0,
    "local_pack": 2.0,
    "people_also_ask": 0.5,  # per question
    "video_carousel": 1.0,
    "image_pack": 0.5,
    "shopping_results": 1.0,
    "top_stories": 1.0,
    "twitter_cards": 0.5,
    "reddit_threads": 0.5,
}

# Intent classification patterns (for Module 3 and 7)
INTENT_PATTERNS = {
    "informational": [
        r"\bhow to\b",
        r"\bwhat is\b",
        r"\bwhy\b",
        r"\bguide\b",
        r"\btutorial\b",
        r"\bexamples?\b",
    ],
    "commercial": [
        r"\bbest\b",
        r"\btop\b",
        r"\breview\b",
        r"\bcompare\b",
        r"\bvs\b",
        r"\balternatives?\b",
        r"\boptions?\b",
    ],
    "transactional": [
        r"\bbuy\b",
        r"\bprice\b",
        r"\bcost\b",
        r"\bcheap\b",
        r"\bdeal\b",
        r"\bdiscount\b",
        r"\bcoupon\b",
        r"\bfor sale\b",
    ],
    "navigational": [
        r"\blogin\b",
        r"\bsign in\b",
        r"\bcontact\b",
        r"\bsupport\b",
        r"^[a-z\s]+ (site|website|page)$",
    ],
}

# Trend classification thresholds (for Module 1)
TREND_CLASSIFICATIONS = {
    "strong_growth": 0.05,  # >5% per month
    "growth": 0.01,  # 1-5% per month
    "flat": -0.01,  # -1% to 1% per month
    "decline": -0.05,  # -5% to -1% per month
    "strong_decline": float("-inf"),  # <-5% per month
}

# Page triage buckets (for Module 2)
PAGE_TRIAGE_THRESHOLDS = {
    "growing": 0.1,  # clicks/day slope
    "stable": -0.1,
    "decaying": -0.5,
    "critical": float("-inf"),
}

# Content age quadrants (for Module 4)
CONTENT_AGE_THRESHOLDS = {
    "old_months": 12,  # Content older than this is "old"
    "new_months": 3,  # Content newer than this is "new"
}
