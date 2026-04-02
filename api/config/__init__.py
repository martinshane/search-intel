"""Configuration management for environment variables and settings.

This module provides the central Settings class, APP_VERSION constant,
and the singleton ``settings`` instance used throughout the application.

Previously this lived in api/config.py (a flat module). Because the
api/config/ *directory* also exists (housing dataforseo_config.py),
Python's import system could resolve ``from .config import settings``
to the directory (a namespace package) instead of the .py file,
causing an ImportError on deployment. Moving the code into
api/config/__init__.py makes the directory a proper regular package
and eliminates the ambiguity.
"""

import logging
import os
import re
import uuid
from typing import List, Optional
from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)

# Shared version constant — used by main.py, health.py, and any other module
APP_VERSION = "1.0.0"

# ---------------------------------------------------------------------------
# Generate a stable ephemeral secret if no env var is set.  Logged once at
# import time so operators know they need to configure JWT_SECRET_KEY.
# ---------------------------------------------------------------------------
_EPHEMERAL_SECRET: Optional[str] = None


def _resolve_jwt_secret() -> str:
    """Resolve the JWT signing secret from environment variables.

    Checks in order:
      1. JWT_SECRET_KEY   (preferred — explicit for JWT signing)
      2. SECRET_KEY        (generic fallback)
      3. Generates a random UUID and logs a loud warning.

    The ephemeral fallback keeps the app bootable in dev but any
    production deployment MUST set one of the env vars — otherwise
    every restart invalidates all user sessions.
    """
    global _EPHEMERAL_SECRET

    jwt_key = os.getenv("JWT_SECRET_KEY", "").strip()
    if jwt_key and jwt_key != "your-secret-key-change-in-production":
        return jwt_key

    secret_key = os.getenv("SECRET_KEY", "").strip()
    if secret_key:
        return secret_key

    # Neither env var is set — generate ephemeral key
    if _EPHEMERAL_SECRET is None:
        _EPHEMERAL_SECRET = uuid.uuid4().hex + uuid.uuid4().hex  # 64 hex chars
        logger.warning(
            "\u26a0\ufe0f  No JWT_SECRET_KEY or SECRET_KEY configured! "
            "Using an ephemeral random key — all sessions will be lost on restart. "
            "Set JWT_SECRET_KEY in your environment for persistent authentication."
        )
    return _EPHEMERAL_SECRET


class Settings(BaseSettings):
    """Application settings loaded from environment variables.
    
    All fields that depend on external secrets use Optional[str] = None
    so the app can start even when env vars are missing.  Individual
    endpoints validate that the config they need is present at runtime.
    """
    
    # Application
    app_name: str = "Search Intelligence Report API"
    environment: str = "development"
    debug: bool = False
    api_prefix: str = "/api/v1"
    
    # Server
    host: str = "0.0.0.0"
    port: int = 8000
    
    # Security — optional so app boots without them; endpoints that
    # need auth validate at request time.
    secret_key: str = ""
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 60 * 24 * 7  # 7 days
    
    # CORS — defaults include localhost for dev.  Production origins
    # are added dynamically by get_cors_origins() from FRONTEND_URL,
    # ALLOWED_ORIGINS, and auto-detected Railway patterns.
    cors_origins: list[str] = [
        "http://localhost:3000",
        "http://localhost:5173",
    ]
    
    # FRONTEND_URL — set this in Railway to the web service's public URL.
    # It is automatically appended to CORS allowed origins.
    # Example: https://search-intel-web-production.up.railway.app
    frontend_url: str = ""
    
    # Comma-separated extra origins to allow beyond the defaults.
    # Example: https://clankermarketing.com,https://www.clankermarketing.com
    allowed_origins: str = ""
    
    # Regex patterns for dynamic CORS origin matching (e.g. Railway
    # preview deploys that get unique subdomains).  Evaluated only when
    # the origin is not in the explicit allow-list.
    cors_origin_patterns: list[str] = [
        r"https://.*\.railway\.app$",
        r"https://.*\.vercel\.app$",
    ]
    
    # Google OAuth — optional; auth endpoints check at request time
    google_client_id: str = ""
    google_client_secret: str = ""
    google_redirect_uri: str = ""
    google_auth_uri: str = "https://accounts.google.com/o/oauth2/v2/auth"
    google_token_uri: str = "https://oauth2.googleapis.com/token"
    
    # Google OAuth Scopes
    google_scopes: list[str] = [
        "openid",
        "https://www.googleapis.com/auth/userinfo.email",
        "https://www.googleapis.com/auth/userinfo.profile",
        "https://www.googleapis.com/auth/webmasters.readonly",  # GSC read-only
        "https://www.googleapis.com/auth/analytics.readonly",   # GA4 read-only
    ]
    
    # Supabase — optional; endpoints that use Supabase check at runtime
    supabase_url: str = ""
    supabase_key: str = ""
    supabase_service_role_key: str = ""
    
    # Database encryption key for OAuth tokens
    encryption_key: str = ""
    
    # External APIs
    dataforseo_login: Optional[str] = None
    dataforseo_password: Optional[str] = None
    anthropic_api_key: Optional[str] = None
    
    # API Cache
    api_cache_ttl_hours: int = 24
    
    # Report Generation
    max_keywords_to_analyze: int = 100
    max_pages_to_crawl: int = 5000
    gsc_data_months: int = 16
    
    # Worker
    worker_concurrency: int = 2
    worker_timeout_seconds: int = 300
    
    # Rate Limiting
    rate_limit_per_minute: int = 60
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )

    @property
    def jwt_secret_key(self) -> str:
        """The actual secret used to sign and verify JWT tokens.

        Resolves from JWT_SECRET_KEY -> SECRET_KEY -> ephemeral random.
        All JWT-related code should use this property instead of
        ``self.secret_key`` directly.
        """
        return _resolve_jwt_secret()
    
    def get_cors_origins(self) -> list[str]:
        """Build the full list of allowed CORS origins.
        
        Merges:
          1. cors_origins defaults / env var
          2. FRONTEND_URL (if set)
          3. ALLOWED_ORIGINS (comma-separated)
          4. Hard-coded production domains
        
        Returns a deduplicated, stripped list.
        """
        origins: list[str] = []
        
        # 1. Default / env-var origins
        if isinstance(self.cors_origins, str):
            origins.extend(o.strip() for o in self.cors_origins.split(",") if o.strip())
        else:
            origins.extend(self.cors_origins)
        
        # 2. FRONTEND_URL — auto-added so operators only need one env var
        if self.frontend_url:
            url = self.frontend_url.rstrip("/")
            origins.append(url)
            # Also allow the www variant if it's a bare domain
            if not url.startswith("https://www."):
                www = url.replace("https://", "https://www.", 1)
                origins.append(www)
        
        # 3. ALLOWED_ORIGINS — additional comma-separated origins
        if self.allowed_origins:
            origins.extend(
                o.strip() for o in self.allowed_origins.split(",") if o.strip()
            )
        
        # 4. Hard-coded production domains (always allowed)
        origins.extend([
            "https://clankermarketing.com",
            "https://www.clankermarketing.com",
        ])
        
        # Deduplicate while preserving order
        seen: set[str] = set()
        unique: list[str] = []
        for o in origins:
            o = o.rstrip("/")
            if o and o not in seen:
                seen.add(o)
                unique.append(o)
        return unique
    
    def origin_matches_pattern(self, origin: str) -> bool:
        """Check if an origin matches any of the dynamic CORS patterns.
        
        Used by the CORS middleware to allow Railway preview deploys
        and other dynamic origins without listing them explicitly.
        """
        for pattern in self.cors_origin_patterns:
            try:
                if re.match(pattern, origin):
                    return True
            except re.error:
                continue
        return False
    
    def get_google_scopes(self) -> list[str]:
        """Get Google OAuth scopes as a list, handling comma-separated env var."""
        if isinstance(self.google_scopes, str):
            return [scope.strip() for scope in self.google_scopes.split(",")]
        return self.google_scopes
    
    @property
    def is_production(self) -> bool:
        """Check if running in production environment."""
        return self.environment.lower() == "production"
    
    @property
    def is_development(self) -> bool:
        """Check if running in development environment."""
        return self.environment.lower() == "development"


@lru_cache()
def get_settings() -> Settings:
    """
    Get cached settings instance.
    
    Uses lru_cache to ensure settings are loaded only once
    and reused across the application.
    """
    return Settings()


# Convenience function to get settings instance
settings = get_settings()
