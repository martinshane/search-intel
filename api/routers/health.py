"""
Health check router.
"""
from typing import Any, Dict
from fastapi import APIRouter

router = APIRouter()


@router.get("")
@router.get("/")
async def health_check() -> Dict[str, Any]:
    """Basic health check endpoint."""
    return {
        "status": "ok",
        "service": "search-intel-api",
        "version": "0.1.0"
    }


@router.get("/detailed")
async def detailed_health() -> Dict[str, Any]:
    """Detailed health check with dependency status."""
    deps = {}
    
    # Check Supabase
    try:
        import os
        from supabase import create_client
        url = os.getenv("SUPABASE_URL", "")
        key = os.getenv("SUPABASE_KEY", "")
        if url and key:
            client = create_client(url, key)
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
        "version": "0.1.0",
        "healthy": all_healthy,
        "dependencies": deps
    }
