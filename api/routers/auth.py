"""
Authentication router for Google OAuth flow.

Public endpoints (no JWT required):
  GET /login     — generate OAuth URL
  GET /callback  — handle Google redirect

Protected / optional-auth endpoints:
  GET  /status          — check auth status + connected services
  GET  /gsc/authorize   — get OAuth URL scoped for GSC connection
  GET  /ga4/authorize   — get OAuth URL scoped for GA4 connection
  GET  /gsc/properties  — list GSC properties for current user
  GET  /ga4/properties  — list GA4 properties for current user
  POST /revoke          — revoke OAuth tokens for current user
"""
from typing import Any, Dict, Optional
from fastapi import APIRouter, Depends, HTTPException, Query

from ..auth.dependencies import get_current_user, get_current_user_optional
from ..auth.oauth import (
    generate_auth_url,
    handle_oauth_callback,
    verify_gsc_access,
    verify_ga4_access,
    revoke_user_tokens,
)

router = APIRouter()


# ---------------------------------------------------------------------------
# Public (unauthenticated) — required for the initial OAuth handshake
# ---------------------------------------------------------------------------

@router.get("/login")
async def login() -> Dict[str, Any]:
    """
    Generate Google OAuth authorization URL.

    Returns URL to redirect user to for Google consent screen.
    """
    try:
        auth_data = generate_auth_url()
        return {
            "authorization_url": auth_data["url"],
            "state": auth_data["state"],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to generate auth URL: {str(e)}")


@router.get("/callback")
async def callback(
    code: str = Query(..., description="Authorization code from Google"),
    state: str = Query(..., description="State parameter for CSRF validation"),
) -> Dict[str, Any]:
    """
    Handle OAuth callback from Google.

    Exchanges authorization code for tokens and stores them.
    Returns a JWT access token for subsequent API calls.
    """
    result = await handle_oauth_callback(code, state)
    return result


# ---------------------------------------------------------------------------
# Auth status — optional JWT (returns unauthenticated info when missing)
# ---------------------------------------------------------------------------

@router.get("/status")
async def auth_status(
    user: Optional[dict] = Depends(get_current_user_optional),
) -> Dict[str, Any]:
    """
    Check authentication status and connected services.

    Returns the user email, which Google services are connected,
    and available property lists.  Works for both authenticated and
    unauthenticated callers — returns ``authenticated: false`` when
    no valid JWT is present.  This is the endpoint the frontend
    polls on page load to decide which UI to show.
    """
    if user is None:
        return {
            "authenticated": False,
            "email": None,
            "gsc_connected": False,
            "ga4_connected": False,
            "gsc_properties": [],
            "ga4_properties": [],
        }

    uid = _uid(user)

    # Determine connected status from stored token fields
    gsc_connected = bool(user.get("gsc_token") or user.get("google_tokens"))
    ga4_connected = bool(user.get("ga4_token") or user.get("google_tokens"))

    # Attempt to list properties (gracefully handle failures)
    gsc_properties: list = []
    ga4_properties: list = []

    if gsc_connected and uid:
        try:
            gsc_data = await verify_gsc_access(uid)
            gsc_properties = gsc_data.get("properties", [])
        except Exception:
            pass  # properties unavailable but still report connected

    if ga4_connected and uid:
        try:
            ga4_data = await verify_ga4_access(uid)
            ga4_properties = ga4_data.get("properties", [])
        except Exception:
            pass

    return {
        "authenticated": True,
        "email": user.get("email", ""),
        "gsc_connected": gsc_connected or len(gsc_properties) > 0,
        "ga4_connected": ga4_connected or len(ga4_properties) > 0,
        "gsc_properties": gsc_properties,
        "ga4_properties": ga4_properties,
    }


# ---------------------------------------------------------------------------
# Authorize shortcuts — used by the frontend connect buttons
# ---------------------------------------------------------------------------

@router.get("/gsc/authorize")
async def gsc_authorize() -> Dict[str, Any]:
    """
    Generate OAuth URL for GSC connection.

    Functionally equivalent to ``/login`` — exists as a distinct path
    so the frontend can call ``/api/auth/gsc/authorize`` directly from
    the "Connect Search Console" button.
    """
    try:
        auth_data = generate_auth_url()
        return {
            "authorization_url": auth_data["url"],
            "state": auth_data["state"],
        }
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to generate GSC auth URL: {str(e)}"
        )


@router.get("/ga4/authorize")
async def ga4_authorize() -> Dict[str, Any]:
    """
    Generate OAuth URL for GA4 connection.

    Functionally equivalent to ``/login`` — exists as a distinct path
    so the frontend can call ``/api/auth/ga4/authorize`` directly from
    the "Connect Analytics" button.
    """
    try:
        auth_data = generate_auth_url()
        return {
            "authorization_url": auth_data["url"],
            "state": auth_data["state"],
        }
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to generate GA4 auth URL: {str(e)}"
        )


# ---------------------------------------------------------------------------
# Protected (JWT required) — user must already be authenticated
# ---------------------------------------------------------------------------

def _uid(user: dict) -> str:
    """Extract canonical user ID from the JWT-decoded user dict."""
    return user.get("sub", user.get("id", user.get("user_id", "")))


@router.get("/gsc/properties")
async def gsc_properties(
    user: dict = Depends(get_current_user),
) -> Dict[str, Any]:
    """List available GSC properties for the authenticated user."""
    uid = _uid(user)
    if not uid:
        raise HTTPException(status_code=401, detail="Cannot determine user identity")
    return await verify_gsc_access(uid)


@router.get("/ga4/properties")
async def ga4_properties(
    user: dict = Depends(get_current_user),
) -> Dict[str, Any]:
    """List available GA4 properties for the authenticated user."""
    uid = _uid(user)
    if not uid:
        raise HTTPException(status_code=401, detail="Cannot determine user identity")
    return await verify_ga4_access(uid)


@router.post("/revoke")
async def revoke_tokens(
    user: dict = Depends(get_current_user),
) -> Dict[str, Any]:
    """Revoke Google OAuth tokens for the authenticated user."""
    uid = _uid(user)
    if not uid:
        raise HTTPException(status_code=401, detail="Cannot determine user identity")
    return await revoke_user_tokens(uid)
