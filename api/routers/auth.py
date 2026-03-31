"""
Authentication router for Google OAuth flow.
"""
from typing import Any, Dict
from fastapi import APIRouter, HTTPException, Query

from ..auth.oauth import (
    generate_auth_url,
    handle_oauth_callback,
    verify_gsc_access,
    verify_ga4_access,
    revoke_user_tokens,
)

router = APIRouter()


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
    """
    result = await handle_oauth_callback(code, state)
    return result


@router.get("/gsc/properties")
async def gsc_properties(user_id: str = Query(...)) -> Dict[str, Any]:
    """List available GSC properties for a user."""
    return await verify_gsc_access(user_id)


@router.get("/ga4/properties")
async def ga4_properties(user_id: str = Query(...)) -> Dict[str, Any]:
    """List available GA4 properties for a user."""
    return await verify_ga4_access(user_id)


@router.post("/revoke")
async def revoke(user_id: str = Query(...)) -> Dict[str, Any]:
    """Revoke OAuth tokens for a user."""
    return await revoke_user_tokens(user_id)
