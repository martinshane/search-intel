"""
Rate limiting middleware for the Search Intelligence Report API.

Implements a sliding-window rate limiter with per-IP and per-user
tracking.  Three tiers protect different endpoint categories:

  1. Report generation (POST /reports/create, /reports/generate):
     5 requests per hour per user — prevents burning DataForSEO budget
     and Railway compute.

  2. Auth endpoints (/auth/login, /auth/callback):
     20 requests per minute per IP — standard brute-force protection.

  3. General API (everything else):
     120 requests per minute per IP — generous for normal usage,
     blocks automated scraping.

Storage is in-memory (collections.defaultdict of deques) so it
resets on service restart.  This is acceptable because:
  - Railway services restart infrequently
  - Worst case after restart: limits reset (brief window of no limits)
  - No external dependency (Redis) needed

For multi-instance deployments, swap _storage for a Redis-backed
implementation.
"""

import time
import logging
from collections import defaultdict, deque
from typing import Optional, Tuple

from fastapi import Request, Response, status
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Rate limit tiers
# ---------------------------------------------------------------------------

class RateLimitTier:
    """Defines a rate limit: max_requests within window_seconds."""
    __slots__ = ("name", "max_requests", "window_seconds")

    def __init__(self, name: str, max_requests: int, window_seconds: int):
        self.name = name
        self.max_requests = max_requests
        self.window_seconds = window_seconds


# Tier definitions
TIER_REPORT = RateLimitTier("report_generation", max_requests=5, window_seconds=3600)
TIER_AUTH   = RateLimitTier("auth", max_requests=20, window_seconds=60)
TIER_GENERAL = RateLimitTier("general", max_requests=120, window_seconds=60)

# Path prefixes → tier mapping (checked in order, first match wins)
_TIER_RULES: list[Tuple[str, str, RateLimitTier]] = [
    # (method, path_prefix, tier)
    ("POST", "/api/reports/generate", TIER_REPORT),
    ("POST", "/api/v1/reports/generate", TIER_REPORT),
    ("POST", "/api/reports/create", TIER_REPORT),
    ("POST", "/api/v1/reports/create", TIER_REPORT),
    ("POST", "/reports/generate", TIER_REPORT),
    ("POST", "/reports/create", TIER_REPORT),
    ("GET",  "/api/auth/login", TIER_AUTH),
    ("GET",  "/auth/login", TIER_AUTH),
    ("GET",  "/api/auth/callback", TIER_AUTH),
    ("GET",  "/auth/callback", TIER_AUTH),
    ("GET",  "/api/auth/gsc/authorize", TIER_AUTH),
    ("GET",  "/api/auth/ga4/authorize", TIER_AUTH),
]


def _classify_request(method: str, path: str) -> RateLimitTier:
    """Determine which rate-limit tier a request belongs to."""
    method_upper = method.upper()
    path_lower = path.lower().rstrip("/")
    for rule_method, rule_prefix, tier in _TIER_RULES:
        if method_upper == rule_method and path_lower.startswith(rule_prefix.lower()):
            return tier
    return TIER_GENERAL


# ---------------------------------------------------------------------------
# In-memory sliding window storage
# ---------------------------------------------------------------------------

class _SlidingWindowStore:
    """Thread-safe-ish sliding window counter per key.
    
    Each key maps to a deque of timestamps.  On each check we prune
    expired entries and compare count to the limit.  Memory is bounded
    because old entries are continuously pruned and keys with no recent
    activity are cleaned up periodically.
    """

    def __init__(self):
        self._windows: dict[str, deque] = defaultdict(deque)
        self._last_cleanup = time.monotonic()
        self._cleanup_interval = 300  # prune stale keys every 5 min

    def is_allowed(self, key: str, tier: RateLimitTier) -> Tuple[bool, int, int]:
        """Check if a request is allowed under the given tier.
        
        Returns:
            (allowed, remaining, retry_after_seconds)
        """
        now = time.monotonic()
        window = self._windows[key]
        cutoff = now - tier.window_seconds

        # Prune expired timestamps
        while window and window[0] < cutoff:
            window.popleft()

        if len(window) >= tier.max_requests:
            # Rate limited — calculate retry-after from oldest entry
            retry_after = int(window[0] - cutoff) + 1
            return False, 0, max(retry_after, 1)

        # Allowed — record this request
        window.append(now)
        remaining = tier.max_requests - len(window)

        # Periodic cleanup of stale keys
        if now - self._last_cleanup > self._cleanup_interval:
            self._cleanup(now)

        return True, remaining, 0

    def _cleanup(self, now: float) -> None:
        """Remove keys that have no recent activity."""
        self._last_cleanup = now
        stale_keys = []
        for key, window in self._windows.items():
            if not window or (now - window[-1]) > 7200:  # 2 hours stale
                stale_keys.append(key)
        for key in stale_keys:
            del self._windows[key]
        if stale_keys:
            logger.debug("Rate limiter cleanup: removed %d stale keys", len(stale_keys))


_store = _SlidingWindowStore()


# ---------------------------------------------------------------------------
# Helpers to extract client identity
# ---------------------------------------------------------------------------

def _get_client_ip(request: Request) -> str:
    """Extract the real client IP, respecting X-Forwarded-For behind proxies."""
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        # Take the first IP (original client)
        return forwarded.split(",")[0].strip()
    if request.client:
        return request.client.host
    return "unknown"


def _get_rate_limit_key(request: Request, tier: RateLimitTier) -> str:
    """Build a rate-limit key combining tier, identity, and optionally user ID.
    
    For report generation, we key on user ID (from JWT) if available,
    because that's the resource we're protecting (per-user budget).
    For auth and general, we key on IP.
    """
    ip = _get_client_ip(request)
    
    if tier.name == "report_generation":
        # Try to extract user ID from auth cookie/header for per-user limiting
        # Fall back to IP if not authenticated
        user_id = _extract_user_id_from_request(request)
        if user_id:
            return f"{tier.name}:user:{user_id}"
    
    return f"{tier.name}:ip:{ip}"


def _extract_user_id_from_request(request: Request) -> Optional[str]:
    """Try to extract user ID from JWT token in cookie or Authorization header.
    
    This is a lightweight extraction (no full verification) because the
    actual auth middleware will verify the token downstream.  We just
    need a stable identifier for rate limiting.
    """
    import json
    import base64

    token = None

    # Check cookie first (primary auth method for browser clients)
    token = request.cookies.get("access_token") or request.cookies.get("token")

    # Fall back to Authorization header
    if not token:
        auth_header = request.headers.get("authorization", "")
        if auth_header.lower().startswith("bearer "):
            token = auth_header[7:]

    if not token:
        return None

    try:
        # Decode JWT payload without verification (just for rate limit key)
        parts = token.split(".")
        if len(parts) != 3:
            return None
        # Add padding
        payload_b64 = parts[1] + "=" * (4 - len(parts[1]) % 4)
        payload = json.loads(base64.urlsafe_b64decode(payload_b64))
        return payload.get("sub") or payload.get("user_id") or payload.get("id")
    except Exception:
        return None


# ---------------------------------------------------------------------------
# FastAPI Middleware
# ---------------------------------------------------------------------------

# Paths that are never rate-limited (health checks, docs)
_EXEMPT_PATHS = frozenset({
    "/health",
    "/health/",
    "/",
    "/docs",
    "/docs/",
    "/openapi.json",
    "/redoc",
    "/redoc/",
})


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Sliding-window rate limiter as Starlette/FastAPI middleware.
    
    Adds standard rate-limit headers to every response:
      X-RateLimit-Limit: max requests in the window
      X-RateLimit-Remaining: requests left in the current window
      X-RateLimit-Reset: seconds until the window resets
    
    Returns 429 Too Many Requests with Retry-After header when exceeded.
    """

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        path = request.url.path.rstrip("/")

        # Skip rate limiting for exempt paths
        if path in _EXEMPT_PATHS or path.lower() in _EXEMPT_PATHS:
            return await call_next(request)

        # Classify and check
        tier = _classify_request(request.method, request.url.path)
        key = _get_rate_limit_key(request, tier)
        allowed, remaining, retry_after = _store.is_allowed(key, tier)

        if not allowed:
            logger.warning(
                "Rate limited: %s %s (key=%s, tier=%s)",
                request.method, request.url.path, key, tier.name,
            )
            return JSONResponse(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                content={
                    "error": "Too many requests",
                    "message": _user_message(tier),
                    "retry_after": retry_after,
                },
                headers={
                    "Retry-After": str(retry_after),
                    "X-RateLimit-Limit": str(tier.max_requests),
                    "X-RateLimit-Remaining": "0",
                    "X-RateLimit-Reset": str(retry_after),
                },
            )

        # Process the request normally
        response = await call_next(request)

        # Add rate-limit headers to successful responses
        response.headers["X-RateLimit-Limit"] = str(tier.max_requests)
        response.headers["X-RateLimit-Remaining"] = str(remaining)
        response.headers["X-RateLimit-Reset"] = str(tier.window_seconds)

        return response


def _user_message(tier: RateLimitTier) -> str:
    """Generate a user-friendly rate limit message per tier."""
    if tier.name == "report_generation":
        return (
            "You've reached the report generation limit "
            f"({tier.max_requests} reports per hour). "
            "Please wait before generating another report."
        )
    if tier.name == "auth":
        return (
            "Too many authentication attempts. "
            "Please wait a moment before trying again."
        )
    return (
        "You're making requests too quickly. "
        "Please slow down and try again shortly."
    )
