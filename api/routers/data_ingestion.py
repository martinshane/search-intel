"""
Data ingestion router — property discovery, connection validation, and data refresh.

Provides endpoints for the frontend to:
1. List GSC properties the user has access to (after OAuth)
2. List GA4 properties the user has access to
3. Validate that a GSC/GA4 property is accessible and has data
4. Check the freshness of cached data for a report
5. Manually trigger a data refresh for a given property

All endpoints require JWT authentication via ``get_current_user``.
"""

import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from api.auth.dependencies import get_current_user

logger = logging.getLogger(__name__)
router = APIRouter()


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------

class PropertyInfo(BaseModel):
    """A single GSC or GA4 property."""
    url: str
    permission_level: Optional[str] = None
    property_type: Optional[str] = None


class PropertiesResponse(BaseModel):
    """Response listing available properties."""
    properties: List[PropertyInfo]
    count: int


class ValidationResponse(BaseModel):
    """Response from a connection validation check."""
    valid: bool
    property_url: str
    message: str
    data_available: bool = False
    earliest_date: Optional[str] = None
    latest_date: Optional[str] = None


class CacheFreshnessResponse(BaseModel):
    """Response describing cache state for a property."""
    property_url: str
    has_cached_data: bool
    last_fetched: Optional[str] = None
    cache_age_hours: Optional[float] = None
    is_stale: bool = True


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_supabase():
    """Get Supabase client via centralized database module."""
    try:
        from api.database import get_supabase_client
        return get_supabase_client()
    except (ValueError, Exception) as e:
        raise HTTPException(status_code=500, detail=f"Supabase not configured: {e}")


def _user_id(user: dict) -> str:
    """Extract the canonical user identifier from the JWT user dict."""
    return user.get("sub", user.get("id", user.get("user_id", "")))


def _get_user_tokens(user_id: str) -> Dict[str, Any]:
    """
    Fetch the user's stored OAuth tokens from Supabase.

    Returns a dict with keys ``gsc_token`` and ``ga4_token`` (each may be
    None if the user hasn't connected that service).
    """
    supabase = _get_supabase()
    try:
        result = (
            supabase.table("users")
            .select("gsc_token, ga4_token")
            .eq("id", user_id)
            .execute()
        )
        if result.data:
            return result.data[0]
    except Exception as exc:
        logger.error("Failed to fetch tokens for user %s: %s", user_id, exc)
    return {}


def _decrypt_token(raw_token: Any) -> Dict[str, Any]:
    """
    Decrypt an OAuth token from its stored (potentially encrypted) form.

    Handles three cases:
    1. Already a plain dict → return as-is.
    2. Encrypted string → decrypt via TokenEncryption.
    3. None / empty → return empty dict.
    """
    if not raw_token:
        return {}

    if isinstance(raw_token, dict):
        return raw_token

    if isinstance(raw_token, str):
        try:
            from api.auth.oauth import encryptor
            return encryptor.decrypt(raw_token)
        except Exception as exc:
            logger.error("Failed to decrypt token: %s", exc)
            return {}

    return {}


def _build_google_credentials(token_data: Dict[str, Any]):
    """Build a google.oauth2.credentials.Credentials object from token dict."""
    from google.oauth2.credentials import Credentials
    return Credentials(
        token=token_data.get("token"),
        refresh_token=token_data.get("refresh_token"),
        token_uri=token_data.get("token_uri", "https://oauth2.googleapis.com/token"),
        client_id=token_data.get("client_id"),
        client_secret=token_data.get("client_secret"),
        scopes=token_data.get("scopes"),
    )


# ---------------------------------------------------------------------------
# GSC property endpoints
# ---------------------------------------------------------------------------

@router.get("/gsc/properties", response_model=PropertiesResponse)
async def list_gsc_properties(
    user: dict = Depends(get_current_user),
) -> Dict[str, Any]:
    """
    List all Google Search Console properties the user has access to.

    Requires the user to have completed GSC OAuth.  Returns a list of
    site URLs with permission levels (siteOwner, siteFullUser, etc.).
    """
    uid = _user_id(user)
    tokens = _get_user_tokens(uid)
    raw_token = tokens.get("gsc_token")
    if not raw_token:
        raise HTTPException(
            status_code=401,
            detail="No GSC token found. Please connect Google Search Console first.",
        )

    token_data = _decrypt_token(raw_token)
    if not token_data:
        raise HTTPException(
            status_code=401,
            detail="Failed to decrypt GSC token. Please re-authenticate.",
        )

    try:
        credentials = _build_google_credentials(token_data)
        from googleapiclient.discovery import build
        service = build("searchconsole", "v1", credentials=credentials)
        sites_response = service.sites().list().execute()

        properties = []
        for site in sites_response.get("siteEntry", []):
            properties.append(PropertyInfo(
                url=site.get("siteUrl", ""),
                permission_level=site.get("permissionLevel"),
            ))

        return {
            "properties": properties,
            "count": len(properties),
        }

    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to list GSC properties for user %s: %s", uid, exc)
        raise HTTPException(
            status_code=502,
            detail=f"Failed to fetch GSC properties: {str(exc)}",
        )


@router.get("/gsc/validate")
async def validate_gsc_property(
    property_url: str = Query(..., description="GSC property URL to validate"),
    user: dict = Depends(get_current_user),
) -> Dict[str, Any]:
    """
    Validate that a specific GSC property is accessible and has data.

    Performs a minimal data request (last 7 days) to verify the connection
    and check data availability without pulling the full 16-month window.
    """
    uid = _user_id(user)
    tokens = _get_user_tokens(uid)
    raw_token = tokens.get("gsc_token")
    if not raw_token:
        raise HTTPException(status_code=401, detail="No GSC token found.")

    token_data = _decrypt_token(raw_token)
    if not token_data:
        raise HTTPException(status_code=401, detail="Failed to decrypt GSC token.")

    try:
        credentials = _build_google_credentials(token_data)
        from googleapiclient.discovery import build
        service = build("searchconsole", "v1", credentials=credentials)

        end_date = datetime.utcnow().date()
        start_date = end_date - timedelta(days=7)

        response = (
            service.searchanalytics()
            .query(
                siteUrl=property_url,
                body={
                    "startDate": start_date.isoformat(),
                    "endDate": end_date.isoformat(),
                    "dimensions": ["date"],
                    "rowLimit": 10,
                },
            )
            .execute()
        )

        rows = response.get("rows", [])
        has_data = len(rows) > 0
        earliest = rows[0]["keys"][0] if rows else None
        latest = rows[-1]["keys"][0] if rows else None

        return {
            "valid": True,
            "property_url": property_url,
            "message": "Property is accessible and has data." if has_data else "Property is accessible but has no recent data.",
            "data_available": has_data,
            "earliest_date": earliest,
            "latest_date": latest,
        }

    except HTTPException:
        raise
    except Exception as exc:
        logger.error("GSC validation failed for %s: %s", property_url, exc)
        return {
            "valid": False,
            "property_url": property_url,
            "message": f"Validation failed: {str(exc)}",
            "data_available": False,
            "earliest_date": None,
            "latest_date": None,
        }


# ---------------------------------------------------------------------------
# GA4 property endpoints
# ---------------------------------------------------------------------------

@router.get("/ga4/properties", response_model=PropertiesResponse)
async def list_ga4_properties(
    user: dict = Depends(get_current_user),
) -> Dict[str, Any]:
    """
    List GA4 properties the user has access to.

    Uses the GA4 Admin API to enumerate accessible properties.
    Falls back to returning an empty list if the Admin API is
    unavailable (some accounts don't grant admin API access).
    """
    uid = _user_id(user)
    tokens = _get_user_tokens(uid)
    raw_token = tokens.get("ga4_token") or tokens.get("gsc_token")
    if not raw_token:
        raise HTTPException(
            status_code=401,
            detail="No GA4 token found. Please connect Google Analytics first.",
        )

    token_data = _decrypt_token(raw_token)
    if not token_data:
        raise HTTPException(status_code=401, detail="Failed to decrypt GA4 token.")

    try:
        credentials = _build_google_credentials(token_data)

        from google.analytics.admin import AnalyticsAdminServiceClient

        admin_client = AnalyticsAdminServiceClient(credentials=credentials)
        accounts = admin_client.list_account_summaries()

        properties = []
        for account in accounts:
            for prop_summary in account.property_summaries:
                properties.append(PropertyInfo(
                    url=prop_summary.property,
                    permission_level=None,
                    property_type="ga4",
                ))

        return {
            "properties": properties,
            "count": len(properties),
        }

    except ImportError:
        logger.warning("google-analytics-admin not installed; GA4 property listing unavailable")
        return {"properties": [], "count": 0}
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to list GA4 properties for user %s: %s", uid, exc)
        raise HTTPException(
            status_code=502,
            detail=f"Failed to fetch GA4 properties: {str(exc)}",
        )


@router.get("/ga4/validate")
async def validate_ga4_property(
    property_id: str = Query(..., description="GA4 property ID (e.g. 'properties/123456')"),
    user: dict = Depends(get_current_user),
) -> Dict[str, Any]:
    """
    Validate that a GA4 property is accessible and has data.

    Performs a minimal data request (last 7 days, sessions only)
    to verify the connection.
    """
    uid = _user_id(user)
    tokens = _get_user_tokens(uid)
    raw_token = tokens.get("ga4_token") or tokens.get("gsc_token")
    if not raw_token:
        raise HTTPException(status_code=401, detail="No GA4 token found.")

    token_data = _decrypt_token(raw_token)
    if not token_data:
        raise HTTPException(status_code=401, detail="Failed to decrypt GA4 token.")

    try:
        from api.ingestion.ga4 import validate_ga4_connection

        credentials = _build_google_credentials(token_data)
        result = validate_ga4_connection(credentials, property_id)

        return {
            "valid": result.get("connected", False),
            "property_url": property_id,
            "message": result.get("message", "Validation complete"),
            "data_available": result.get("has_data", False),
            "earliest_date": result.get("earliest_date"),
            "latest_date": result.get("latest_date"),
        }

    except HTTPException:
        raise
    except Exception as exc:
        logger.error("GA4 validation failed for %s: %s", property_id, exc)
        return {
            "valid": False,
            "property_url": property_id,
            "message": f"Validation failed: {str(exc)}",
            "data_available": False,
            "earliest_date": None,
            "latest_date": None,
        }


# ---------------------------------------------------------------------------
# Connection status — combined GSC + GA4 overview
# ---------------------------------------------------------------------------

@router.get("/status")
async def connection_status(
    user: dict = Depends(get_current_user),
) -> Dict[str, Any]:
    """
    Return the user's current data source connection status.

    Shows whether GSC and GA4 tokens are present (without revealing
    the tokens themselves) so the frontend can render the appropriate
    connect/disconnect UI.
    """
    uid = _user_id(user)
    tokens = _get_user_tokens(uid)

    gsc_connected = bool(tokens.get("gsc_token"))
    ga4_connected = bool(tokens.get("ga4_token"))

    return {
        "user_id": uid,
        "gsc_connected": gsc_connected,
        "ga4_connected": ga4_connected,
        "ready_for_report": gsc_connected,  # GA4 is optional enrichment
        "message": (
            "Ready to generate a report."
            if gsc_connected
            else "Please connect Google Search Console to generate a report."
        ),
    }


# ---------------------------------------------------------------------------
# Cache freshness — check if data needs to be re-fetched
# ---------------------------------------------------------------------------

@router.get("/cache/freshness")
async def check_cache_freshness(
    property_url: str = Query(..., description="GSC property URL to check cache for"),
    user: dict = Depends(get_current_user),
) -> Dict[str, Any]:
    """
    Check how fresh the cached API data is for a given property.

    Looks up the most recent ``api_cache`` entry for the property
    and reports its age.  The frontend uses this to decide whether
    to show a "refresh data" prompt before generating a new report.
    """
    uid = _user_id(user)
    supabase = _get_supabase()

    try:
        result = (
            supabase.table("api_cache")
            .select("created_at")
            .eq("property_url", property_url)
            .order("created_at", desc=True)
            .limit(1)
            .execute()
        )

        if result.data:
            created_at_str = result.data[0]["created_at"]
            # Parse ISO timestamp
            try:
                created_at = datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                created_at = datetime.utcnow() - timedelta(hours=999)

            age = datetime.utcnow() - created_at.replace(tzinfo=None)
            age_hours = age.total_seconds() / 3600
            cache_ttl_hours = int(os.getenv("API_CACHE_TTL_HOURS", "24"))

            return {
                "property_url": property_url,
                "has_cached_data": True,
                "last_fetched": created_at_str,
                "cache_age_hours": round(age_hours, 1),
                "is_stale": age_hours > cache_ttl_hours,
            }
        else:
            return {
                "property_url": property_url,
                "has_cached_data": False,
                "last_fetched": None,
                "cache_age_hours": None,
                "is_stale": True,
            }

    except Exception as exc:
        logger.warning("Failed to check cache freshness for %s: %s", property_url, exc)
        return {
            "property_url": property_url,
            "has_cached_data": False,
            "last_fetched": None,
            "cache_age_hours": None,
            "is_stale": True,
        }
