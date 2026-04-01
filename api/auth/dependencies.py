"""
FastAPI dependencies for authentication.

Provides reusable dependencies for:
- Extracting current user from JWT tokens (Bearer header OR HttpOnly cookie)
- Verifying OAuth tokens
- Requiring authentication on protected routes

JWT configuration is read from the central ``api.config.settings`` object
which resolves the signing key from JWT_SECRET_KEY / SECRET_KEY env vars
(see ``Settings.jwt_secret_key`` for precedence rules).
"""

import logging
from typing import Optional
from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError, jwt
from datetime import datetime, timezone

from ..config import settings
from ..database import get_supabase_client

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# JWT configuration — single source of truth from Settings
# ---------------------------------------------------------------------------
# These module-level references are resolved once at import time (for the
# algorithm) and lazily per call (for the secret, via the property).  Other
# modules that import SECRET_KEY / ALGORITHM from here get consistent values.
# ---------------------------------------------------------------------------

def _get_secret_key() -> str:
    """Return the JWT signing key from settings.

    Using a function rather than a bare module constant ensures the
    ephemeral-key warning in config.py fires at first use, not at
    import time (when logging may not yet be configured).
    """
    return settings.jwt_secret_key

ALGORITHM = settings.algorithm

# Security scheme for bearer token (auto_error=False so we can fall back to cookie)
security = HTTPBearer(auto_error=False)


def _decode_jwt(token: str) -> dict:
    """
    Decode and validate a JWT token string.

    Returns the payload dict on success.
    Raises HTTPException 401 on any failure.
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        payload = jwt.decode(token, _get_secret_key(), algorithms=[ALGORITHM])

        user_id: str = payload.get("sub")
        if user_id is None:
            raise credentials_exception

        exp = payload.get("exp")
        if exp is None:
            raise credentials_exception

        if datetime.fromtimestamp(exp, tz=timezone.utc) < datetime.now(timezone.utc):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token has expired",
                headers={"WWW-Authenticate": "Bearer"},
            )

        return payload

    except JWTError:
        raise credentials_exception


def _extract_token(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials],
) -> Optional[str]:
    """
    Extract JWT token from the request.

    Checks in order:
      1. ``Authorization: Bearer <token>`` header
      2. ``access_token`` HttpOnly cookie

    Returns the raw token string, or None if neither source has one.
    """
    # 1. Bearer header (preferred — explicit is better than implicit)
    if credentials and credentials.credentials:
        return credentials.credentials

    # 2. HttpOnly cookie (set by the /auth/callback redirect)
    cookie_token = request.cookies.get("access_token")
    if cookie_token:
        return cookie_token

    return None


async def get_current_user(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
) -> dict:
    """
    Extract and validate current user from JWT token.

    Accepts JWT from either:
      - ``Authorization: Bearer <token>`` header
      - ``access_token`` HttpOnly cookie (set during OAuth callback)

    Args:
        request: The incoming HTTP request (for cookie access)
        credentials: Optional HTTP Bearer token from request header

    Returns:
        dict: User data including id, email, etc.

    Raises:
        HTTPException: 401 if no token found or token is invalid/expired
    """
    token = _extract_token(request, credentials)
    if token is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated — provide a Bearer token or sign in via OAuth",
            headers={"WWW-Authenticate": "Bearer"},
        )

    payload = _decode_jwt(token)

    user_id = payload.get("sub")

    # Fetch user from database
    supabase = get_supabase_client()

    try:
        response = supabase.table("users").select("*").eq("id", user_id).execute()

        if not response.data or len(response.data) == 0:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="User not found",
                headers={"WWW-Authenticate": "Bearer"},
            )

        return response.data[0]

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}"
        )


async def get_current_user_optional(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(
        HTTPBearer(auto_error=False)
    ),
) -> Optional[dict]:
    """
    Extract current user from JWT token, but don't require authentication.

    Useful for endpoints that have optional authentication (different behavior
    for authenticated vs anonymous users).

    Args:
        request: The incoming HTTP request (for cookie access)
        credentials: Optional HTTP Bearer token from request header

    Returns:
        dict: User data if authenticated, None otherwise
    """
    token = _extract_token(request, credentials)
    if token is None:
        return None

    try:
        payload = _decode_jwt(token)
    except HTTPException:
        return None

    user_id = payload.get("sub")
    if not user_id:
        return None

    try:
        supabase = get_supabase_client()
        response = supabase.table("users").select("*").eq("id", user_id).execute()
        if response.data and len(response.data) > 0:
            return response.data[0]
    except Exception:
        pass

    return None


async def verify_gsc_token(user: dict = Depends(get_current_user)) -> dict:
    """
    Verify that the user has a valid Google Search Console OAuth token.

    Args:
        user: Current authenticated user

    Returns:
        dict: User data (same as input)

    Raises:
        HTTPException: 403 if user hasn't connected GSC or token is invalid
    """
    if not user.get("gsc_token"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Google Search Console not connected. Please complete OAuth flow."
        )

    gsc_token = user.get("gsc_token")

    # Check if token has required fields
    if not isinstance(gsc_token, dict):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid GSC token format. Please reconnect."
        )

    required_fields = ["access_token", "refresh_token", "token_uri"]
    missing_fields = [field for field in required_fields if field not in gsc_token]

    if missing_fields:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"GSC token missing required fields: {', '.join(missing_fields)}. Please reconnect."
        )

    return user


async def verify_ga4_token(user: dict = Depends(get_current_user)) -> dict:
    """
    Verify that the user has a valid Google Analytics 4 OAuth token.

    Args:
        user: Current authenticated user

    Returns:
        dict: User data (same as input)

    Raises:
        HTTPException: 403 if user hasn't connected GA4 or token is invalid
    """
    if not user.get("ga4_token"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Google Analytics 4 not connected. Please complete OAuth flow."
        )

    ga4_token = user.get("ga4_token")

    # Check if token has required fields
    if not isinstance(ga4_token, dict):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid GA4 token format. Please reconnect."
        )

    required_fields = ["access_token", "refresh_token", "token_uri"]
    missing_fields = [field for field in required_fields if field not in ga4_token]

    if missing_fields:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"GA4 token missing required fields: {', '.join(missing_fields)}. Please reconnect."
        )

    return user


async def verify_both_tokens(
    user: dict = Depends(get_current_user)
) -> dict:
    """
    Verify that the user has both valid GSC and GA4 OAuth tokens.

    This is a convenience dependency for endpoints that require both services.

    Args:
        user: Current authenticated user

    Returns:
        dict: User data (same as input)

    Raises:
        HTTPException: 403 if user hasn't connected both services or tokens are invalid
    """
    # Verify GSC token
    await verify_gsc_token(user)

    # Verify GA4 token
    await verify_ga4_token(user)

    return user


def create_access_token(data: dict, expires_delta: Optional[int] = None) -> str:
    """
    Create a new JWT access token.

    Args:
        data: Payload to encode in the token (should include 'sub' for user_id)
        expires_delta: Token expiration time in seconds (default: 30 days)

    Returns:
        str: Encoded JWT token
    """
    from datetime import timedelta

    to_encode = data.copy()

    if expires_delta:
        expire = datetime.now(timezone.utc) + timedelta(seconds=expires_delta)
    else:
        # Default: 30 days
        expire = datetime.now(timezone.utc) + timedelta(days=30)

    to_encode.update({"exp": expire})

    encoded_jwt = jwt.encode(to_encode, _get_secret_key(), algorithm=ALGORITHM)
    return encoded_jwt


async def get_user_by_email(email: str) -> Optional[dict]:
    """
    Fetch user from database by email.

    Helper function used during OAuth callback to find or create users.

    Args:
        email: User email address

    Returns:
        dict: User data if found, None otherwise

    Raises:
        Exception: If database query fails
    """
    supabase = get_supabase_client()

    try:
        response = supabase.table("users").select("*").eq("email", email).execute()

        if response.data and len(response.data) > 0:
            return response.data[0]

        return None

    except Exception as e:
        raise Exception(f"Database error while fetching user: {str(e)}")


async def create_user(email: str, gsc_token: Optional[dict] = None, ga4_token: Optional[dict] = None) -> dict:
    """
    Create a new user in the database.

    Args:
        email: User email address
        gsc_token: Optional GSC OAuth token to store
        ga4_token: Optional GA4 OAuth token to store

    Returns:
        dict: Created user data including generated id

    Raises:
        Exception: If database insert fails
    """
    supabase = get_supabase_client()

    user_data = {
        "email": email,
    }

    if gsc_token:
        user_data["gsc_token"] = gsc_token

    if ga4_token:
        user_data["ga4_token"] = ga4_token

    try:
        response = supabase.table("users").insert(user_data).execute()

        if not response.data or len(response.data) == 0:
            raise Exception("Failed to create user - no data returned")

        return response.data[0]

    except Exception as e:
        raise Exception(f"Database error while creating user: {str(e)}")


async def update_user_tokens(
    user_id: str,
    gsc_token: Optional[dict] = None,
    ga4_token: Optional[dict] = None
) -> dict:
    """
    Update OAuth tokens for an existing user.

    Args:
        user_id: User ID to update
        gsc_token: Optional GSC OAuth token to update
        ga4_token: Optional GA4 OAuth token to update

    Returns:
        dict: Updated user data

    Raises:
        Exception: If database update fails
    """
    supabase = get_supabase_client()

    update_data = {}

    if gsc_token is not None:
        update_data["gsc_token"] = gsc_token

    if ga4_token is not None:
        update_data["ga4_token"] = ga4_token

    if not update_data:
        raise ValueError("At least one token must be provided")

    try:
        response = supabase.table("users").update(update_data).eq("id", user_id).execute()

        if not response.data or len(response.data) == 0:
            raise Exception("Failed to update user tokens - no data returned")

        return response.data[0]

    except Exception as e:
        raise Exception(f"Database error while updating user tokens: {str(e)}")
