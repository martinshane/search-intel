"""
Google OAuth flow implementation with token encryption and Supabase storage.

Handles OAuth2 flow for GSC and GA4 scopes, stores encrypted tokens in Supabase.
"""

import os
import json
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
import secrets
import base64

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2

from supabase import create_client, Client
from fastapi import HTTPException

# Environment variables
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
REDIRECT_URI = os.getenv("OAUTH_REDIRECT_URI", "http://localhost:8000/auth/callback")
ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY")  # Must be set in production
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# OAuth scopes
SCOPES = [
    "https://www.googleapis.com/auth/webmasters.readonly",  # GSC
    "https://www.googleapis.com/auth/analytics.readonly",    # GA4
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile"
]

# Lazy Supabase client — avoids crash when env vars are missing at import time
_supabase_client: Client = None


def _get_supabase() -> Client:
    """Lazy-initialise and return the module-level Supabase client."""
    global _supabase_client
    if _supabase_client is None:
        url = os.getenv("SUPABASE_URL", "")
        key = os.getenv("SUPABASE_KEY", "")
        if not url or not key:
            raise ValueError("Missing SUPABASE_URL or SUPABASE_KEY environment variables")
        _supabase_client = create_client(url, key)
    return _supabase_client


class TokenEncryption:
    """Handle encryption/decryption of OAuth tokens."""
    
    def __init__(self, encryption_key: Optional[str] = None):
        """
        Initialize encryption handler.
        
        Args:
            encryption_key: Base64-encoded Fernet key or None to generate from env
        """
        if encryption_key:
            self.key = encryption_key.encode()
        elif ENCRYPTION_KEY:
            self.key = ENCRYPTION_KEY.encode()
        else:
            # Generate key from a salt (in production, use a proper KMS)
            kdf = PBKDF2(
                algorithm=hashes.SHA256(),
                length=32,
                salt=b"search_intelligence_salt_2024",  # In prod: unique per deployment
                iterations=100000,
            )
            self.key = base64.urlsafe_b64encode(kdf.derive(b"search_intel_secret"))
        
        self.fernet = Fernet(self.key)
    
    def encrypt(self, data: Dict[str, Any]) -> str:
        """Encrypt token data to string."""
        json_str = json.dumps(data)
        encrypted = self.fernet.encrypt(json_str.encode())
        return base64.urlsafe_b64encode(encrypted).decode()
    
    def decrypt(self, encrypted_str: str) -> Dict[str, Any]:
        """Decrypt token data from string."""
        try:
            encrypted = base64.urlsafe_b64decode(encrypted_str.encode())
            decrypted = self.fernet.decrypt(encrypted)
            return json.loads(decrypted.decode())
        except Exception as e:
            raise ValueError(f"Failed to decrypt token: {str(e)}")


encryptor = TokenEncryption()


def get_oauth_flow(state: Optional[str] = None) -> Flow:
    """
    Create OAuth flow instance.
    
    Args:
        state: Optional state parameter for CSRF protection
    
    Returns:
        Configured Flow instance
    """
    if not GOOGLE_CLIENT_ID or not GOOGLE_CLIENT_SECRET:
        raise ValueError("Google OAuth credentials not configured")
    
    client_config = {
        "web": {
            "client_id": GOOGLE_CLIENT_ID,
            "client_secret": GOOGLE_CLIENT_SECRET,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "redirect_uris": [REDIRECT_URI],
        }
    }
    
    flow = Flow.from_client_config(
        client_config=client_config,
        scopes=SCOPES,
        redirect_uri=REDIRECT_URI
    )
    
    if state:
        flow.state = state
    
    return flow


def generate_auth_url() -> Dict[str, str]:
    """
    Generate OAuth authorization URL.
    
    Returns:
        Dict with 'url' and 'state' for CSRF protection
    """
    flow = get_oauth_flow()
    
    # Generate random state for CSRF protection
    state = secrets.token_urlsafe(32)
    
    authorization_url, _ = flow.authorization_url(
        access_type='offline',  # Get refresh token
        include_granted_scopes='true',
        state=state,
        prompt='consent'  # Force consent to get refresh token
    )
    
    return {
        "url": authorization_url,
        "state": state
    }


async def handle_oauth_callback(code: str, state: str) -> Dict[str, Any]:
    """
    Handle OAuth callback and exchange code for tokens.
    
    Args:
        code: Authorization code from Google
        state: State parameter for CSRF validation
    
    Returns:
        Dict with user info and success status
    
    Raises:
        HTTPException: If OAuth flow fails
    """
    try:
        flow = get_oauth_flow(state=state)
        flow.fetch_token(code=code)
        
        credentials = flow.credentials
        
        # Get user info
        user_info = get_user_info(credentials)
        
        # Prepare token data for storage
        token_data = {
            "token": credentials.token,
            "refresh_token": credentials.refresh_token,
            "token_uri": credentials.token_uri,
            "client_id": credentials.client_id,
            "client_secret": credentials.client_secret,
            "scopes": credentials.scopes,
            "expiry": credentials.expiry.isoformat() if credentials.expiry else None
        }
        
        # Encrypt tokens
        encrypted_token = encryptor.encrypt(token_data)
        
        # Store or update user in Supabase
        user_email = user_info.get("email")
        if not user_email:
            raise HTTPException(status_code=400, detail="Failed to get user email")
        
        # Check if user exists
        result = _get_supabase().table("users").select("*").eq("email", user_email).execute()
        
        if result.data:
            # Update existing user
            user_id = result.data[0]["id"]
            _get_supabase().table("users").update({
                "gsc_token": encrypted_token,
                "ga4_token": encrypted_token,  # Same token for both
                "updated_at": datetime.utcnow().isoformat()
            }).eq("id", user_id).execute()
        else:
            # Create new user
            insert_result = _get_supabase().table("users").insert({
                "email": user_email,
                "gsc_token": encrypted_token,
                "ga4_token": encrypted_token,
                "created_at": datetime.utcnow().isoformat()
            }).execute()
            user_id = insert_result.data[0]["id"]
        
        # Verify we have required scopes
        verify_scopes(credentials)
        
        return {
            "success": True,
            "user_id": user_id,
            "email": user_email,
            "user_info": user_info
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"OAuth callback failed: {str(e)}"
        )


def get_user_info(credentials: Credentials) -> Dict[str, Any]:
    """
    Fetch user info from Google using credentials.
    
    Args:
        credentials: Google OAuth2 credentials
    
    Returns:
        Dict with user info (email, name, picture, etc.)
    """
    try:
        service = build('oauth2', 'v2', credentials=credentials)
        user_info = service.userinfo().get().execute()
        return user_info
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"Failed to fetch user info: {str(e)}"
        )


def verify_scopes(credentials: Credentials) -> None:
    """
    Verify that required scopes were granted.
    
    Args:
        credentials: Google OAuth2 credentials
    
    Raises:
        HTTPException: If required scopes missing
    """
    required_scopes = [
        "https://www.googleapis.com/auth/webmasters.readonly",
        "https://www.googleapis.com/auth/analytics.readonly"
    ]
    
    granted_scopes = credentials.scopes or []
    
    missing_scopes = [s for s in required_scopes if s not in granted_scopes]
    
    if missing_scopes:
        raise HTTPException(
            status_code=400,
            detail=f"Missing required scopes: {', '.join(missing_scopes)}"
        )


async def get_user_credentials(user_id: str) -> Credentials:
    """
    Retrieve and decrypt user credentials from Supabase.
    
    Args:
        user_id: User UUID
    
    Returns:
        Google Credentials object
    
    Raises:
        HTTPException: If user not found or token invalid
    """
    try:
        # Fetch user from Supabase
        result = _get_supabase().table("users").select("*").eq("id", user_id).execute()
        
        if not result.data:
            raise HTTPException(status_code=404, detail="User not found")
        
        user = result.data[0]
        
        # Decrypt token
        encrypted_token = user.get("gsc_token") or user.get("ga4_token")
        if not encrypted_token:
            raise HTTPException(
                status_code=401,
                detail="No OAuth token found. Please authenticate."
            )
        
        token_data = encryptor.decrypt(encrypted_token)
        
        # Reconstruct credentials
        credentials = Credentials(
            token=token_data.get("token"),
            refresh_token=token_data.get("refresh_token"),
            token_uri=token_data.get("token_uri"),
            client_id=token_data.get("client_id"),
            client_secret=token_data.get("client_secret"),
            scopes=token_data.get("scopes")
        )
        
        # Set expiry if available
        if token_data.get("expiry"):
            credentials.expiry = datetime.fromisoformat(token_data["expiry"])
        
        # Refresh if expired
        if credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())
            
            # Save refreshed token back to database
            updated_token_data = {
                "token": credentials.token,
                "refresh_token": credentials.refresh_token,
                "token_uri": credentials.token_uri,
                "client_id": credentials.client_id,
                "client_secret": credentials.client_secret,
                "scopes": credentials.scopes,
                "expiry": credentials.expiry.isoformat() if credentials.expiry else None
            }
            
            encrypted_updated = encryptor.encrypt(updated_token_data)
            
            _get_supabase().table("users").update({
                "gsc_token": encrypted_updated,
                "ga4_token": encrypted_updated,
                "updated_at": datetime.utcnow().isoformat()
            }).eq("id", user_id).execute()
        
        return credentials
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve credentials: {str(e)}"
        )


async def revoke_user_tokens(user_id: str) -> Dict[str, bool]:
    """
    Revoke user's OAuth tokens and remove from database.
    
    Args:
        user_id: User UUID
    
    Returns:
        Dict with success status
    """
    try:
        # Get credentials
        credentials = await get_user_credentials(user_id)
        
        # Revoke token with Google
        if credentials.token:
            import requests
            requests.post(
                'https://oauth2.googleapis.com/revoke',
                params={'token': credentials.token},
                headers={'content-type': 'application/x-www-form-urlencoded'}
            )
        
        # Remove tokens from database
        _get_supabase().table("users").update({
            "gsc_token": None,
            "ga4_token": None,
            "updated_at": datetime.utcnow().isoformat()
        }).eq("id", user_id).execute()
        
        return {"success": True}
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to revoke tokens: {str(e)}"
        )


async def verify_gsc_access(user_id: str) -> Dict[str, Any]:
    """
    Verify user has GSC access and list available properties.
    
    Args:
        user_id: User UUID
    
    Returns:
        Dict with available GSC properties
    """
    try:
        credentials = await get_user_credentials(user_id)
        service = build('searchconsole', 'v1', credentials=credentials)
        
        sites_list = service.sites().list().execute()
        
        properties = []
        if 'siteEntry' in sites_list:
            for site in sites_list['siteEntry']:
                properties.append({
                    "url": site.get("siteUrl"),
                    "permission_level": site.get("permissionLevel")
                })
        
        return {
            "has_access": len(properties) > 0,
            "properties": properties
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to verify GSC access: {str(e)}"
        )


async def verify_ga4_access(user_id: str) -> Dict[str, Any]:
    """
    Verify user has GA4 access and list available properties.
    
    Args:
        user_id: User UUID
    
    Returns:
        Dict with available GA4 properties
    """
    try:
        credentials = await get_user_credentials(user_id)
        service = build('analyticsadmin', 'v1beta', credentials=credentials)
        
        # List account summaries
        summaries = service.accountSummaries().list().execute()
        
        properties = []
        if 'accountSummaries' in summaries:
            for account in summaries['accountSummaries']:
                if 'propertySummaries' in account:
                    for prop in account['propertySummaries']:
                        properties.append({
                            "property_id": prop.get("property"),
                            "display_name": prop.get("displayName"),
                            "parent": prop.get("parent")
                        })
        
        return {
            "has_access": len(properties) > 0,
            "properties": properties
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to verify GA4 access: {str(e)}"
        )
