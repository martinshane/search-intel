"""
Auth module for handling OAuth2 authentication with Google services.

This module provides authentication functionality for Google Search Console
and Google Analytics 4 APIs, including token management and validation.
"""

from .oauth import (
    get_authorization_url,
    exchange_code_for_token,
    refresh_access_token,
    validate_token,
    revoke_token,
)

from .middleware import require_auth, get_current_user

__all__ = [
    "get_authorization_url",
    "exchange_code_for_token",
    "refresh_access_token",
    "validate_token",
    "revoke_token",
    "require_auth",
    "get_current_user",
]