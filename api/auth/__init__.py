# api/auth/__init__.py
"""
Authentication package for Search Intelligence Report.
Handles Google OAuth flow for GSC and GA4 API access.
"""

from .dependencies import get_current_user  # noqa: F401 — re-export for convenience

__all__ = ["get_current_user"]
