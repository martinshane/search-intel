"""
Database utilities for the Search Intelligence API.

Provides:
- get_supabase_client(): Returns a configured Supabase client (anon key)
- get_service_role_client(): Returns a Supabase client with service role privileges
- get_db(): FastAPI dependency that yields a Supabase client

**Env-var resolution:**

The codebase historically used several naming conventions for the
Supabase key:

  - SUPABASE_KEY          — used by most routers and database.py
  - SUPABASE_ANON_KEY     — used in .env.example
  - SUPABASE_SERVICE_KEY  — used by cron services

Both client factories now try multiple names in priority order so the
app works regardless of which naming convention the deployment uses.
This eliminates a class of silent-failure bugs where a router would
get an empty key and raise HTTP 500.
"""

import os
import logging
from typing import Generator
from functools import lru_cache

from supabase import create_client, Client

logger = logging.getLogger(__name__)


def _resolve_supabase_url() -> str:
    """Resolve the Supabase project URL from environment."""
    url = os.getenv("SUPABASE_URL", "").strip()
    if not url:
        raise ValueError(
            "Missing SUPABASE_URL environment variable. "
            "Set it to your Supabase project URL (e.g. https://xyz.supabase.co)."
        )
    return url


def _resolve_anon_key() -> str:
    """Resolve the Supabase anon/public key.

    Tries in order:
      1. SUPABASE_KEY       — most common in the codebase
      2. SUPABASE_ANON_KEY  — matches .env.example naming
    """
    for var in ("SUPABASE_KEY", "SUPABASE_ANON_KEY"):
        val = os.getenv(var, "").strip()
        if val:
            return val
    raise ValueError(
        "Missing Supabase anon key. "
        "Set SUPABASE_KEY or SUPABASE_ANON_KEY environment variable."
    )


def _resolve_service_role_key() -> str:
    """Resolve the Supabase service-role key.

    Tries in order:
      1. SUPABASE_SERVICE_ROLE_KEY  — canonical (.env.example, database.py)
      2. SUPABASE_SERVICE_KEY       — used by cron services / program.md
      3. SUPABASE_KEY               — fallback (anon key still works for
         user-owned-row operations with RLS)
      4. SUPABASE_ANON_KEY          — last-resort fallback
    """
    for var in (
        "SUPABASE_SERVICE_ROLE_KEY",
        "SUPABASE_SERVICE_KEY",
        "SUPABASE_KEY",
        "SUPABASE_ANON_KEY",
    ):
        val = os.getenv(var, "").strip()
        if val:
            if var in ("SUPABASE_KEY", "SUPABASE_ANON_KEY"):
                logger.warning(
                    "Using %s as service-role key fallback — set "
                    "SUPABASE_SERVICE_ROLE_KEY for full admin access.",
                    var,
                )
            return val
    raise ValueError(
        "Missing Supabase service role key. "
        "Set SUPABASE_SERVICE_ROLE_KEY environment variable."
    )


@lru_cache()
def get_supabase_client() -> Client:
    """
    Get a cached Supabase client instance (anon key).

    Suitable for user-facing operations where Row Level Security
    should be enforced.  The client is cached via lru_cache so only
    one instance is created per process.

    Returns:
        Client: Configured Supabase client

    Raises:
        ValueError: If required environment variables are missing
    """
    url = _resolve_supabase_url()
    key = _resolve_anon_key()
    logger.info("Initializing Supabase client (anon key)")
    return create_client(url, key)


@lru_cache()
def get_service_role_client() -> Client:
    """
    Get a cached Supabase client using the service role key.

    This client bypasses Row Level Security and should only be used
    for background workers, admin operations, and cron jobs.  Cached
    via lru_cache so only one instance is created per process.

    Returns:
        Client: Supabase client with service role privileges

    Raises:
        ValueError: If required environment variables are missing
    """
    url = _resolve_supabase_url()
    key = _resolve_service_role_key()
    logger.info("Initializing Supabase client (service role key)")
    return create_client(url, key)


def get_db() -> Generator[Client, None, None]:
    """
    FastAPI dependency that provides a Supabase client.

    Usage in FastAPI routes:
        @router.get("/example")
        async def example(db: Client = Depends(get_db)):
            result = db.table("my_table").select("*").execute()

    Yields:
        Client: Supabase client instance
    """
    client = get_supabase_client()
    try:
        yield client
    finally:
        # Supabase client doesn't need explicit cleanup,
        # but this pattern allows future connection pool management
        pass
