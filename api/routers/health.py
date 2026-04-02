"""
Health check router.
"""
from typing import Any, Dict
from fastapi import APIRouter

from ..config import APP_VERSION

router = APIRouter()


@router.get("")
@router.get("/")
async def health_check() -> Dict[str, Any]:
    """Basic health check endpoint."""
    return {
        "status": "ok",
        "service": "search-intel-api",
        "version": APP_VERSION,
    }


@router.get("/detailed")
async def detailed_health() -> Dict[str, Any]:
    """Detailed health check with dependency status."""
    deps = {}
    
    # Check Supabase
    try:
        import os
        from api.database import get_supabase_client
        client = get_supabase_client()
        if client:
            # Simple query to verify connection
            client.table("users").select("id").limit(1).execute()
            deps["supabase"] = {"healthy": True}
        else:
            deps["supabase"] = {"healthy": False, "error": "Missing credentials"}
    except Exception as e:
        deps["supabase"] = {"healthy": False, "error": str(e)}
    
    all_healthy = all(d.get("healthy", False) for d in deps.values())
    
    return {
        "status": "ok" if all_healthy else "degraded",
        "service": "search-intel-api",
        "version": APP_VERSION,
        "healthy": all_healthy,
        "dependencies": deps,
    }
