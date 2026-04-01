"""
Authentication router for Google OAuth flow.

Public endpoints (no JWT required):
  GET /auth/login     — generate OAuth URL
  GET /auth/callback  — handle Google redirect

Protected endpoints (JWT required):
  GET  /auth/gsc/properties  — list GSC properties for current user
  GET  /auth/ga4/properties  — list GA4 properties for current user
  POST /auth/revoke          — revoke OAuth tokens for current user
"""
from typing import Any, Dict
from fastapi import APIRouter, Depends, HTTPException, Query

from ..auth.dependencies import get_current_user
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
            "state": auth_data["state"]
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
async def revoke(
    user: dict = Depends(get_current_user),
) -> Dict[str, Any]:
    """Revoke OAuth tokens for the authenticated user."""
    uid = _uid(user)
    if not uid:
        raise HTTPException(status_code=401, detail="Cannot determine user identity")
    return await revoke_user_tokens(uid)
