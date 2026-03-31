"""
Database utilities for the Search Intelligence API.

Provides:
- get_supabase_client(): Returns a configured Supabase client (used by auth, modules)
- get_db(): FastAPI dependency that yields a Supabase client (used by routes/modules.py)
"""

import os
import logging
from typing import Generator
from functools import lru_cache

from supabase import create_client, Client

logger = logging.getLogger(__name__)


@lru_cache()
def get_supabase_client() -> Client:
    """
    Get a cached Supabase client instance.
    
    Reads SUPABASE_URL and SUPABASE_KEY from environment variables.
    The client is cached via lru_cache so only one instance is created.
    
    Returns:
        Client: Configured Supabase client
        
    Raises:
        ValueError: If required environment variables are missing
    """
    url = os.getenv("SUPABASE_URL", "")
    key = os.getenv("SUPABASE_KEY", "")
    
    if not url or not key:
        raise ValueError(
            "Missing Supabase configuration. "
            "Set SUPABASE_URL and SUPABASE_KEY environment variables."
        )
    
    logger.info("Initializing Supabase client")
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


def get_service_role_client() -> Client:
    """
    Get a Supabase client using the service role key.
    
    This client bypasses Row Level Security and should only
    be used for admin operations (e.g., background workers).
    
    Returns:
        Client: Supabase client with service role privileges
        
    Raises:
        ValueError: If required environment variables are missing
    """
    url = os.getenv("SUPABASE_URL", "")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")
    
    if not url or not key:
        raise ValueError(
            "Missing Supabase service role configuration. "
            "Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY environment variables."
        )
    
    return create_client(url, key)
