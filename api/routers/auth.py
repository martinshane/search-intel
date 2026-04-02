"""
Authentication router for Google OAuth flow.

Public endpoints (no JWT required):
  GET /login     — generate OAuth URL
  GET /callback  — handle Google redirect, issue JWT, redirect to frontend

Protected / optional-auth endpoints:
  GET  /status          — check auth status + connected services
  GET  /gsc/authorize   — get OAuth URL scoped for GSC connection
  GET  /ga4/authorize   — get OAuth URL scoped for GA4 connection
  GET  /gsc/properties  — list GSC properties for current user
  GET  /ga4/properties  — list GA4 properties for current user
  POST /revoke          — revoke OAuth tokens for current user
"""
import logging
import os
from typing import Any, Dict, Optional
from urllib.parse import urlparse

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import RedirectResponse

from ..auth.dependencies import (
    create_access_token,
    get_current_user,
    get_current_user_optional,
)
from ..auth.oauth import (
    generate_auth_url,
    handle_oauth_callback,
    verify_gsc_access,
    verify_ga4_access,
    revoke_user_tokens,
)

logger = logging.getLogger(__name__)

router = APIRouter()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_frontend_url() -> str:
    """
    Determine the frontend URL for post-OAuth redirects.

    Checks, in order:
      1. FRONTEND_URL env var (set in Railway)
      2. Falls back to http://localhost:3000 for local dev
    """
    return (
        os.getenv("FRONTEND_URL", "").rstrip("/")
        or "http://localhost:3000"
    )


def _uid(user: dict) -> str:
    """Extract canonical user ID from the JWT-decoded user dict."""
    return user.get("sub", user.get("id", user.get("user_id", "")))


def _is_cross_origin(api_url: str, frontend_url: str) -> bool:
    """
    Determine whether the API and frontend are cross-origin.

    Two URLs are cross-origin if they differ in scheme, host, or port.
    More importantly for SameSite cookies, two URLs are cross-*site* if
    their registrable domains differ.  On the Public Suffix List,
    ``railway.app`` is a public suffix, so
    ``search-intel-api-xxx.up.railway.app`` and
    ``search-intel-web-xxx.up.railway.app`` are different *sites*.

    We use a conservative check: if the hostnames differ at all, treat
    them as cross-origin and set SameSite=None.
    """
    try:
        api_host = urlparse(api_url).hostname or ""
        fe_host = urlparse(frontend_url).hostname or ""
        # Same host → same-origin → SameSite=Lax is fine
        return api_host.lower() != fe_host.lower()
    except Exception:
        # If we can't parse, assume cross-origin for safety
        return True


def _cookie_params(frontend_url: str) -> Dict[str, Any]:
    """
    Return the correct cookie parameters based on whether the API and
    frontend are cross-origin.

    When cross-origin (separate Railway services, different subdomains):
      - SameSite=None + Secure=True — required for the browser to send
        cookies on cross-origin fetch() with credentials:'include'.
      - This is safe because: cookie is HttpOnly (no JS access), CORS
        restricts which origins can read responses, and JWT validation
        protects against forgery.

    When same-origin (local dev, or custom domain reverse proxy):
      - SameSite=Lax + Secure only on HTTPS — standard cookie security.
    """
    is_https = frontend_url.startswith("https://")

    # Determine the API's own URL for cross-origin comparison
    api_url = os.getenv("API_URL", "") or os.getenv("RAILWAY_PUBLIC_DOMAIN", "")
    if api_url and not api_url.startswith("http"):
        api_url = f"https://{api_url}"

    cross_origin = _is_cross_origin(api_url, frontend_url) if api_url else False

    if cross_origin:
        logger.info(
            "Cross-origin deployment detected (API=%s, frontend=%s) — "
            "using SameSite=None; Secure for auth cookie",
            api_url, frontend_url,
        )
        return {
            "httponly": True,
            "secure": True,          # Required for SameSite=None
            "samesite": "none",      # Allow cross-origin fetch with credentials
            "max_age": 60 * 60 * 24 * 30,  # 30 days
            "path": "/",
        }
    else:
        return {
            "httponly": True,
            "secure": is_https,
            "samesite": "lax",
            "max_age": 60 * 60 * 24 * 30,  # 30 days
            "path": "/",
        }


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
    request: Request,
    code: str = Query(..., description="Authorization code from Google"),
    state: str = Query(..., description="State parameter for CSRF validation"),
):
    """
    Handle OAuth callback from Google.

    1. Exchanges the authorization code for Google OAuth tokens.
    2. Stores encrypted tokens in Supabase.
    3. Generates a JWT access token for the Search Intel API.
    4. Sets the JWT as an HttpOnly cookie (``access_token``).
    5. Redirects the user's browser back to the frontend.

    Cookie SameSite policy:
    - Same-origin (local dev / reverse proxy): SameSite=Lax
    - Cross-origin (separate Railway services): SameSite=None; Secure

    SameSite=None is required because railway.app is on the Public
    Suffix List, making each Railway subdomain a separate "site".
    Without SameSite=None, browsers refuse to send the cookie on
    cross-origin fetch() calls with credentials:'include', silently
    breaking ALL authenticated API requests from the frontend.
    """
    result = await handle_oauth_callback(code, state)

    # Generate a JWT for our API (30-day expiry)
    user_id = str(result.get("user_id", ""))
    email = result.get("email", "")

    if not user_id:
        raise HTTPException(
            status_code=500,
            detail="OAuth succeeded but no user_id was returned",
        )

    jwt_token = create_access_token(
        data={"sub": user_id, "email": email}
    )

    # Redirect to the frontend.  Append ?auth=success so the
    # frontend can show a success toast or re-check /api/auth/status.
    frontend = _get_frontend_url()
    redirect_url = f"{frontend}/?auth=success"

    response = RedirectResponse(url=redirect_url, status_code=302)

    # Set JWT as an HttpOnly cookie with correct SameSite policy
    cookie_params = _cookie_params(frontend)
    response.set_cookie(key="access_token", value=jwt_token, **cookie_params)

    logger.info(
        "OAuth complete for %s (%s) — cookie SameSite=%s, redirecting to %s",
        email, user_id, cookie_params["samesite"], redirect_url,
    )

    return response


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
            raw_gsc = gsc_data.get("properties", [])
            # Normalize to frontend Property interface: {id, name, type}
            # Backend returns: {url, permission_level}
            # Frontend reads:  prop.id (value for <option>), prop.name (display text)
            for prop in raw_gsc:
                site_url = prop.get("url") or prop.get("siteUrl") or ""
                gsc_properties.append({
                    "id": site_url,
                    "name": site_url,
                    "type": "gsc",
                    "permission_level": prop.get("permission_level", ""),
                })
        except Exception:
            pass  # properties unavailable but still report connected

    if ga4_connected and uid:
        try:
            ga4_data = await verify_ga4_access(uid)
            raw_ga4 = ga4_data.get("properties", [])
            # Normalize to frontend Property interface: {id, name, type}
            # Backend returns: {property_id, display_name, parent}
            # Frontend reads:  prop.id (value for <option>), prop.name (display text)
            for prop in raw_ga4:
                prop_id = prop.get("property_id") or prop.get("property") or ""
                display = prop.get("display_name") or prop.get("displayName") or prop_id
                ga4_properties.append({
                    "id": prop_id,
                    "name": display,
                    "type": "ga4",
                    "parent": prop.get("parent", ""),
                })
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


@router.post("/logout")
async def logout() -> Dict[str, Any]:
    """
    Clear the JWT cookie and end the session.

    The frontend can call this to log the user out.  On success,
    subsequent requests will no longer carry the ``access_token``
    cookie and the ``/status`` endpoint will return unauthenticated.

    Uses the same SameSite policy as the callback — if the cookie was
    set with SameSite=None, the delete must also specify SameSite=None
    or the browser won't match the cookie for deletion.
    """
    from fastapi.responses import JSONResponse

    response = JSONResponse(content={"success": True, "message": "Logged out"})

    # Use the same cookie params as setting, for consistent deletion
    frontend = _get_frontend_url()
    cookie_params = _cookie_params(frontend)

    # delete_cookie only accepts: key, path, domain, secure, httponly, samesite
    response.delete_cookie(
        key="access_token",
        path=cookie_params["path"],
        secure=cookie_params["secure"],
        httponly=cookie_params["httponly"],
        samesite=cookie_params["samesite"],
    )
    return response
