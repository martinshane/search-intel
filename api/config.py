"""Configuration management for environment variables and settings."""

import os
from typing import Optional
from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # Application
    app_name: str = "Search Intelligence Report API"
    environment: str = "development"
    debug: bool = False
    api_prefix: str = "/api/v1"
    
    # Server
    host: str = "0.0.0.0"
    port: int = 8000
    
    # Security
    secret_key: str
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 60 * 24 * 7  # 7 days
    
    # CORS
    cors_origins: list[str] = [
        "http://localhost:3000",
        "http://localhost:5173",
    ]
    
    # Google OAuth
    google_client_id: str
    google_client_secret: str
    google_redirect_uri: str
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
    
    # Supabase
    supabase_url: str
    supabase_key: str
    supabase_service_role_key: str
    
    # Database encryption key for OAuth tokens
    # This should be a 32-byte URL-safe base64-encoded key
    # Generate with: from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())
    encryption_key: str
    
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
    
    def get_cors_origins(self) -> list[str]:
        """Get CORS origins as a list, handling comma-separated env var."""
        if isinstance(self.cors_origins, str):
            return [origin.strip() for origin in self.cors_origins.split(",")]
        return self.cors_origins
    
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
    
    Returns:
        Settings: Application settings instance
    """
    return Settings()


# Convenience function to get settings instance
settings = get_settings()
