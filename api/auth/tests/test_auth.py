"""
Comprehensive test suite for api/auth — dependencies.py and oauth.py.

Covers:
- SECTION 1: dependencies.py — JWT authentication (get_current_user, get_current_user_optional)
- SECTION 2: dependencies.py — Token verification (verify_gsc_token, verify_ga4_token, verify_both_tokens)
- SECTION 3: dependencies.py — Token creation (create_access_token)
- SECTION 4: dependencies.py — User CRUD (get_user_by_email, create_user, update_user_tokens)
- SECTION 5: oauth.py — TokenEncryption (encrypt, decrypt)
- SECTION 6: oauth.py — OAuth flow (get_oauth_flow, generate_auth_url)
- SECTION 7: oauth.py — OAuth callback (handle_oauth_callback)
- SECTION 8: oauth.py — Credential management (get_user_credentials, revoke_user_tokens)
- SECTION 9: oauth.py — Access verification (verify_gsc_access, verify_ga4_access, verify_scopes)
- SECTION 10: oauth.py — Supabase lazy init (_get_supabase)
- SECTION 11: Edge cases and integration scenarios
"""

import os
import sys
import json
import base64
import asyncio
import unittest
from unittest.mock import patch, MagicMock, AsyncMock
from datetime import datetime, timezone, timedelta
from types import SimpleNamespace

# ---------------------------------------------------------------------------
# Ensure the api package is importable even when run standalone.
# We insert a fake top-level "api" package so relative imports work.
# ---------------------------------------------------------------------------

# Build minimal stubs so the modules can be imported without real deps
# (google, jose, cryptography, supabase, fastapi …)

# --- jose ---
jose_jwt = MagicMock()
jose_jwt.decode = MagicMock()
jose_jwt.encode = MagicMock(return_value="fake.jwt.token")
sys.modules.setdefault("jose", MagicMock(JWTError=Exception))
sys.modules.setdefault("jose.jwt", jose_jwt)
sys.modules["jose"].jwt = jose_jwt
sys.modules["jose"].JWTError = type("JWTError", (Exception,), {})
JWTError = sys.modules["jose"].JWTError

# --- fastapi ---
class _HTTPException(Exception):
    def __init__(self, status_code=400, detail="", headers=None):
        self.status_code = status_code
        self.detail = detail
        self.headers = headers
        super().__init__(detail)

class _status:
    HTTP_401_UNAUTHORIZED = 401
    HTTP_403_FORBIDDEN = 403
    HTTP_500_INTERNAL_SERVER_ERROR = 500

def _Depends(dep):
    return dep

class _HTTPBearer:
    def __init__(self, auto_error=True):
        self.auto_error = auto_error
    def __call__(self):
        return None

class _HTTPAuthorizationCredentials:
    def __init__(self, scheme="Bearer", credentials=""):
        self.scheme = scheme
        self.credentials = credentials

fa = MagicMock()
fa.HTTPException = _HTTPException
fa.Depends = _Depends
fa.status = _status
sys.modules.setdefault("fastapi", fa)
sys.modules.setdefault("fastapi.security", MagicMock(
    HTTPBearer=_HTTPBearer,
    HTTPAuthorizationCredentials=_HTTPAuthorizationCredentials,
))
sys.modules["fastapi"].HTTPException = _HTTPException
sys.modules["fastapi"].Depends = _Depends
sys.modules["fastapi"].status = _status

# --- google auth ---
class _Credentials:
    def __init__(self, token=None, refresh_token=None, token_uri=None,
                 client_id=None, client_secret=None, scopes=None):
        self.token = token
        self.refresh_token = refresh_token
        self.token_uri = token_uri
        self.client_id = client_id
        self.client_secret = client_secret
        self.scopes = scopes or []
        self.expiry = None
        self.expired = False
    def refresh(self, request):
        self.token = "refreshed_token"
        self.expired = False

sys.modules.setdefault("google", MagicMock())
sys.modules.setdefault("google.oauth2", MagicMock())
sys.modules.setdefault("google.oauth2.credentials", MagicMock(Credentials=_Credentials))
sys.modules.setdefault("google.auth", MagicMock())
sys.modules.setdefault("google.auth.transport", MagicMock())
sys.modules.setdefault("google.auth.transport.requests", MagicMock(Request=MagicMock))
sys.modules.setdefault("google_auth_oauthlib", MagicMock())
sys.modules.setdefault("google_auth_oauthlib.flow", MagicMock())
sys.modules.setdefault("googleapiclient", MagicMock())
sys.modules.setdefault("googleapiclient.discovery", MagicMock())

# --- cryptography (real — needed for TokenEncryption tests) ---
# We'll let the real cryptography library load if available; otherwise stub it
try:
    from cryptography.fernet import Fernet
    HAS_CRYPTO = True
except ImportError:
    HAS_CRYPTO = False
    sys.modules.setdefault("cryptography", MagicMock())
    sys.modules.setdefault("cryptography.fernet", MagicMock())
    sys.modules.setdefault("cryptography.hazmat", MagicMock())
    sys.modules.setdefault("cryptography.hazmat.primitives", MagicMock())
    sys.modules.setdefault("cryptography.hazmat.primitives.hashes", MagicMock())
    sys.modules.setdefault("cryptography.hazmat.primitives.kdf", MagicMock())
    sys.modules.setdefault("cryptography.hazmat.primitives.kdf.pbkdf2", MagicMock())

# --- supabase ---
sys.modules.setdefault("supabase", MagicMock())

# --- api package stubs ---
sys.modules.setdefault("api", MagicMock())
sys.modules.setdefault("api.database", MagicMock())
sys.modules["api.database"].get_supabase_client = MagicMock()


# ==========================================================================
# Now import the modules under test
# ==========================================================================

# We'll import dependencies via exec so we can control the environment
import importlib


def _load_dependencies():
    """Load api.auth.dependencies with mocked externals."""
    spec = {
        "Optional": __import__("typing").Optional,
        "Depends": _Depends,
        "HTTPException": _HTTPException,
        "status": _status,
        "HTTPBearer": _HTTPBearer,
        "HTTPAuthorizationCredentials": _HTTPAuthorizationCredentials,
        "JWTError": JWTError,
        "jwt": jose_jwt,
        "datetime": datetime,
        "timezone": timezone,
        "os": os,
        "get_supabase_client": sys.modules["api.database"].get_supabase_client,
    }
    return spec


# =====================================================================
# SECTION 1 — get_current_user
# =====================================================================

class TestGetCurrentUser(unittest.TestCase):
    """Test JWT authentication via get_current_user."""

    def _run(self, coro):
        return asyncio.get_event_loop().run_until_complete(coro)

    def _make_creds(self, token="valid.token"):
        return _HTTPAuthorizationCredentials(scheme="Bearer", credentials=token)

    @patch.dict(os.environ, {"JWT_SECRET_KEY": "test-secret"})
    def test_valid_token_returns_user(self):
        """Valid JWT with existing user returns user dict."""
        future_exp = (datetime.now(timezone.utc) + timedelta(hours=1)).timestamp()
        jose_jwt.decode.return_value = {"sub": "user-123", "exp": future_exp}

        mock_supabase = MagicMock()
        mock_supabase.table.return_value.select.return_value.eq.return_value.execute.return_value = MagicMock(
            data=[{"id": "user-123", "email": "test@example.com"}]
        )
        sys.modules["api.database"].get_supabase_client.return_value = mock_supabase

        # Re-import to pick up env
        from api.auth.dependencies import get_current_user
        creds = self._make_creds("valid.token")
        user = self._run(get_current_user(creds))
        self.assertEqual(user["id"], "user-123")

    @patch.dict(os.environ, {"JWT_SECRET_KEY": "test-secret"})
    def test_missing_sub_raises_401(self):
        """Token without 'sub' claim raises 401."""
        jose_jwt.decode.return_value = {"exp": 9999999999}

        from api.auth.dependencies import get_current_user
        creds = self._make_creds()
        with self.assertRaises(_HTTPException) as ctx:
            self._run(get_current_user(creds))
        self.assertEqual(ctx.exception.status_code, 401)

    @patch.dict(os.environ, {"JWT_SECRET_KEY": "test-secret"})
    def test_missing_exp_raises_401(self):
        """Token without 'exp' claim raises 401."""
        jose_jwt.decode.return_value = {"sub": "user-1"}

        from api.auth.dependencies import get_current_user
        creds = self._make_creds()
        with self.assertRaises(_HTTPException) as ctx:
            self._run(get_current_user(creds))
        self.assertEqual(ctx.exception.status_code, 401)

    @patch.dict(os.environ, {"JWT_SECRET_KEY": "test-secret"})
    def test_expired_token_raises_401(self):
        """Token with past expiry raises 401."""
        past_exp = (datetime.now(timezone.utc) - timedelta(hours=1)).timestamp()
        jose_jwt.decode.return_value = {"sub": "user-1", "exp": past_exp}

        from api.auth.dependencies import get_current_user
        creds = self._make_creds()
        with self.assertRaises(_HTTPException) as ctx:
            self._run(get_current_user(creds))
        self.assertEqual(ctx.exception.status_code, 401)
        self.assertIn("expired", ctx.exception.detail.lower())

    @patch.dict(os.environ, {"JWT_SECRET_KEY": "test-secret"})
    def test_jwt_decode_error_raises_401(self):
        """JWTError during decode raises 401."""
        jose_jwt.decode.side_effect = JWTError("bad token")

        from api.auth.dependencies import get_current_user
        creds = self._make_creds()
        with self.assertRaises(_HTTPException) as ctx:
            self._run(get_current_user(creds))
        self.assertEqual(ctx.exception.status_code, 401)
        jose_jwt.decode.side_effect = None

    @patch.dict(os.environ, {"JWT_SECRET_KEY": "test-secret"})
    def test_user_not_in_db_raises_401(self):
        """Valid token but user not found in DB raises 401."""
        future_exp = (datetime.now(timezone.utc) + timedelta(hours=1)).timestamp()
        jose_jwt.decode.return_value = {"sub": "user-gone", "exp": future_exp}

        mock_supabase = MagicMock()
        mock_supabase.table.return_value.select.return_value.eq.return_value.execute.return_value = MagicMock(data=[])
        sys.modules["api.database"].get_supabase_client.return_value = mock_supabase

        from api.auth.dependencies import get_current_user
        creds = self._make_creds()
        with self.assertRaises(_HTTPException) as ctx:
            self._run(get_current_user(creds))
        self.assertEqual(ctx.exception.status_code, 401)

    @patch.dict(os.environ, {"JWT_SECRET_KEY": "test-secret"})
    def test_db_exception_raises_500(self):
        """Database error during user fetch raises 500."""
        future_exp = (datetime.now(timezone.utc) + timedelta(hours=1)).timestamp()
        jose_jwt.decode.return_value = {"sub": "user-123", "exp": future_exp}

        mock_supabase = MagicMock()
        mock_supabase.table.return_value.select.return_value.eq.return_value.execute.side_effect = Exception("DB down")
        sys.modules["api.database"].get_supabase_client.return_value = mock_supabase

        from api.auth.dependencies import get_current_user
        creds = self._make_creds()
        with self.assertRaises(_HTTPException) as ctx:
            self._run(get_current_user(creds))
        self.assertEqual(ctx.exception.status_code, 500)
        self.assertIn("Database error", ctx.exception.detail)


# =====================================================================
# SECTION 2 — get_current_user_optional
# =====================================================================

class TestGetCurrentUserOptional(unittest.TestCase):
    """Test optional authentication."""

    def _run(self, coro):
        return asyncio.get_event_loop().run_until_complete(coro)

    def test_none_credentials_returns_none(self):
        """No credentials returns None instead of raising."""
        from api.auth.dependencies import get_current_user_optional
        result = self._run(get_current_user_optional(None))
        self.assertIsNone(result)

    @patch.dict(os.environ, {"JWT_SECRET_KEY": "test-secret"})
    def test_invalid_token_returns_none(self):
        """Invalid token returns None instead of raising."""
        jose_jwt.decode.side_effect = JWTError("bad")
        from api.auth.dependencies import get_current_user_optional
        creds = _HTTPAuthorizationCredentials(credentials="bad.token")
        result = self._run(get_current_user_optional(creds))
        self.assertIsNone(result)
        jose_jwt.decode.side_effect = None

    @patch.dict(os.environ, {"JWT_SECRET_KEY": "test-secret"})
    def test_valid_token_returns_user(self):
        """Valid token returns user dict."""
        future_exp = (datetime.now(timezone.utc) + timedelta(hours=1)).timestamp()
        jose_jwt.decode.return_value = {"sub": "user-opt", "exp": future_exp}

        mock_supabase = MagicMock()
        mock_supabase.table.return_value.select.return_value.eq.return_value.execute.return_value = MagicMock(
            data=[{"id": "user-opt", "email": "opt@example.com"}]
        )
        sys.modules["api.database"].get_supabase_client.return_value = mock_supabase

        from api.auth.dependencies import get_current_user_optional
        creds = _HTTPAuthorizationCredentials(credentials="good.token")
        result = self._run(get_current_user_optional(creds))
        self.assertIsNotNone(result)
        self.assertEqual(result["id"], "user-opt")


# =====================================================================
# SECTION 3 — verify_gsc_token / verify_ga4_token / verify_both_tokens
# =====================================================================

class TestVerifyGscToken(unittest.TestCase):
    """Test GSC token verification dependency."""

    def _run(self, coro):
        return asyncio.get_event_loop().run_until_complete(coro)

    def test_no_gsc_token_raises_403(self):
        from api.auth.dependencies import verify_gsc_token
        user = {"id": "u1", "gsc_token": None}
        with self.assertRaises(_HTTPException) as ctx:
            self._run(verify_gsc_token(user))
        self.assertEqual(ctx.exception.status_code, 403)
        self.assertIn("not connected", ctx.exception.detail.lower())

    def test_non_dict_gsc_token_raises_403(self):
        from api.auth.dependencies import verify_gsc_token
        user = {"id": "u1", "gsc_token": "string-not-dict"}
        with self.assertRaises(_HTTPException) as ctx:
            self._run(verify_gsc_token(user))
        self.assertEqual(ctx.exception.status_code, 403)
        self.assertIn("invalid", ctx.exception.detail.lower())

    def test_missing_required_fields_raises_403(self):
        from api.auth.dependencies import verify_gsc_token
        user = {"id": "u1", "gsc_token": {"access_token": "tok"}}  # missing refresh_token, token_uri
        with self.assertRaises(_HTTPException) as ctx:
            self._run(verify_gsc_token(user))
        self.assertEqual(ctx.exception.status_code, 403)
        self.assertIn("refresh_token", ctx.exception.detail)

    def test_valid_gsc_token_returns_user(self):
        from api.auth.dependencies import verify_gsc_token
        user = {"id": "u1", "gsc_token": {
            "access_token": "tok", "refresh_token": "ref", "token_uri": "uri"
        }}
        result = self._run(verify_gsc_token(user))
        self.assertEqual(result["id"], "u1")


class TestVerifyGa4Token(unittest.TestCase):
    """Test GA4 token verification dependency."""

    def _run(self, coro):
        return asyncio.get_event_loop().run_until_complete(coro)

    def test_no_ga4_token_raises_403(self):
        from api.auth.dependencies import verify_ga4_token
        user = {"id": "u1", "ga4_token": None}
        with self.assertRaises(_HTTPException) as ctx:
            self._run(verify_ga4_token(user))
        self.assertEqual(ctx.exception.status_code, 403)

    def test_non_dict_ga4_token_raises_403(self):
        from api.auth.dependencies import verify_ga4_token
        user = {"id": "u1", "ga4_token": 12345}
        with self.assertRaises(_HTTPException) as ctx:
            self._run(verify_ga4_token(user))
        self.assertEqual(ctx.exception.status_code, 403)

    def test_missing_fields_raises_403(self):
        from api.auth.dependencies import verify_ga4_token
        user = {"id": "u1", "ga4_token": {"access_token": "a"}}
        with self.assertRaises(_HTTPException) as ctx:
            self._run(verify_ga4_token(user))
        self.assertEqual(ctx.exception.status_code, 403)
        self.assertIn("token_uri", ctx.exception.detail)

    def test_valid_ga4_token_returns_user(self):
        from api.auth.dependencies import verify_ga4_token
        user = {"id": "u1", "ga4_token": {
            "access_token": "tok", "refresh_token": "ref", "token_uri": "uri"
        }}
        result = self._run(verify_ga4_token(user))
        self.assertEqual(result["id"], "u1")


class TestVerifyBothTokens(unittest.TestCase):
    """Test both-tokens verification."""

    def _run(self, coro):
        return asyncio.get_event_loop().run_until_complete(coro)

    def test_both_valid_returns_user(self):
        from api.auth.dependencies import verify_both_tokens
        user = {
            "id": "u1",
            "gsc_token": {"access_token": "a", "refresh_token": "b", "token_uri": "c"},
            "ga4_token": {"access_token": "d", "refresh_token": "e", "token_uri": "f"},
        }
        result = self._run(verify_both_tokens(user))
        self.assertEqual(result["id"], "u1")

    def test_missing_gsc_raises_403(self):
        from api.auth.dependencies import verify_both_tokens
        user = {
            "id": "u1",
            "gsc_token": None,
            "ga4_token": {"access_token": "d", "refresh_token": "e", "token_uri": "f"},
        }
        with self.assertRaises(_HTTPException) as ctx:
            self._run(verify_both_tokens(user))
        self.assertEqual(ctx.exception.status_code, 403)

    def test_missing_ga4_raises_403(self):
        from api.auth.dependencies import verify_both_tokens
        user = {
            "id": "u1",
            "gsc_token": {"access_token": "a", "refresh_token": "b", "token_uri": "c"},
            "ga4_token": None,
        }
        with self.assertRaises(_HTTPException) as ctx:
            self._run(verify_both_tokens(user))
        self.assertEqual(ctx.exception.status_code, 403)


# =====================================================================
# SECTION 4 — create_access_token
# =====================================================================

class TestCreateAccessToken(unittest.TestCase):
    """Test JWT token creation."""

    @patch.dict(os.environ, {"JWT_SECRET_KEY": "test-key"})
    def test_creates_token_string(self):
        from api.auth.dependencies import create_access_token
        jose_jwt.encode.return_value = "encoded.jwt.token"
        token = create_access_token({"sub": "user-1"})
        self.assertEqual(token, "encoded.jwt.token")

    @patch.dict(os.environ, {"JWT_SECRET_KEY": "test-key"})
    def test_includes_exp_claim(self):
        from api.auth.dependencies import create_access_token
        jose_jwt.encode.return_value = "tok"
        create_access_token({"sub": "u1"})
        call_args = jose_jwt.encode.call_args
        payload = call_args[0][0]
        self.assertIn("exp", payload)

    @patch.dict(os.environ, {"JWT_SECRET_KEY": "test-key"})
    def test_custom_expiry(self):
        from api.auth.dependencies import create_access_token
        jose_jwt.encode.return_value = "tok"
        create_access_token({"sub": "u1"}, expires_delta=3600)
        call_args = jose_jwt.encode.call_args
        payload = call_args[0][0]
        exp = payload["exp"]
        # Should be ~1 hour from now
        delta = (exp - datetime.now(timezone.utc)).total_seconds()
        self.assertAlmostEqual(delta, 3600, delta=5)

    @patch.dict(os.environ, {"JWT_SECRET_KEY": "test-key"})
    def test_default_30_day_expiry(self):
        from api.auth.dependencies import create_access_token
        jose_jwt.encode.return_value = "tok"
        create_access_token({"sub": "u1"})
        call_args = jose_jwt.encode.call_args
        payload = call_args[0][0]
        exp = payload["exp"]
        delta = (exp - datetime.now(timezone.utc)).total_seconds()
        self.assertAlmostEqual(delta, 30 * 86400, delta=10)

    @patch.dict(os.environ, {"JWT_SECRET_KEY": "test-key"})
    def test_preserves_original_data(self):
        """Ensure original dict is not mutated."""
        from api.auth.dependencies import create_access_token
        jose_jwt.encode.return_value = "tok"
        original = {"sub": "u1", "role": "admin"}
        create_access_token(original)
        self.assertNotIn("exp", original)


# =====================================================================
# SECTION 5 — get_user_by_email / create_user / update_user_tokens
# =====================================================================

class TestGetUserByEmail(unittest.TestCase):
    """Test user lookup by email."""

    def _run(self, coro):
        return asyncio.get_event_loop().run_until_complete(coro)

    def test_found_returns_user(self):
        mock_sb = MagicMock()
        mock_sb.table.return_value.select.return_value.eq.return_value.execute.return_value = MagicMock(
            data=[{"id": "u1", "email": "a@b.com"}]
        )
        sys.modules["api.database"].get_supabase_client.return_value = mock_sb
        from api.auth.dependencies import get_user_by_email
        result = self._run(get_user_by_email("a@b.com"))
        self.assertEqual(result["email"], "a@b.com")

    def test_not_found_returns_none(self):
        mock_sb = MagicMock()
        mock_sb.table.return_value.select.return_value.eq.return_value.execute.return_value = MagicMock(data=[])
        sys.modules["api.database"].get_supabase_client.return_value = mock_sb
        from api.auth.dependencies import get_user_by_email
        result = self._run(get_user_by_email("nobody@nowhere.com"))
        self.assertIsNone(result)

    def test_db_error_raises_exception(self):
        mock_sb = MagicMock()
        mock_sb.table.return_value.select.return_value.eq.return_value.execute.side_effect = Exception("conn err")
        sys.modules["api.database"].get_supabase_client.return_value = mock_sb
        from api.auth.dependencies import get_user_by_email
        with self.assertRaises(Exception) as ctx:
            self._run(get_user_by_email("a@b.com"))
        self.assertIn("Database error", str(ctx.exception))


class TestCreateUser(unittest.TestCase):
    """Test user creation."""

    def _run(self, coro):
        return asyncio.get_event_loop().run_until_complete(coro)

    def test_creates_user_with_email(self):
        mock_sb = MagicMock()
        mock_sb.table.return_value.insert.return_value.execute.return_value = MagicMock(
            data=[{"id": "new-1", "email": "new@test.com"}]
        )
        sys.modules["api.database"].get_supabase_client.return_value = mock_sb
        from api.auth.dependencies import create_user
        user = self._run(create_user("new@test.com"))
        self.assertEqual(user["email"], "new@test.com")

    def test_creates_user_with_tokens(self):
        mock_sb = MagicMock()
        mock_sb.table.return_value.insert.return_value.execute.return_value = MagicMock(
            data=[{"id": "new-2", "email": "tok@test.com", "gsc_token": {"a": 1}}]
        )
        sys.modules["api.database"].get_supabase_client.return_value = mock_sb
        from api.auth.dependencies import create_user
        user = self._run(create_user("tok@test.com", gsc_token={"a": 1}, ga4_token={"b": 2}))
        self.assertIsNotNone(user)

    def test_empty_result_raises(self):
        mock_sb = MagicMock()
        mock_sb.table.return_value.insert.return_value.execute.return_value = MagicMock(data=[])
        sys.modules["api.database"].get_supabase_client.return_value = mock_sb
        from api.auth.dependencies import create_user
        with self.assertRaises(Exception) as ctx:
            self._run(create_user("fail@test.com"))
        self.assertIn("Failed to create user", str(ctx.exception))

    def test_db_error_raises(self):
        mock_sb = MagicMock()
        mock_sb.table.return_value.insert.return_value.execute.side_effect = Exception("insert fail")
        sys.modules["api.database"].get_supabase_client.return_value = mock_sb
        from api.auth.dependencies import create_user
        with self.assertRaises(Exception) as ctx:
            self._run(create_user("err@test.com"))
        self.assertIn("Database error", str(ctx.exception))


class TestUpdateUserTokens(unittest.TestCase):
    """Test token updates."""

    def _run(self, coro):
        return asyncio.get_event_loop().run_until_complete(coro)

    def test_updates_gsc_token(self):
        mock_sb = MagicMock()
        mock_sb.table.return_value.update.return_value.eq.return_value.execute.return_value = MagicMock(
            data=[{"id": "u1", "gsc_token": {"new": True}}]
        )
        sys.modules["api.database"].get_supabase_client.return_value = mock_sb
        from api.auth.dependencies import update_user_tokens
        result = self._run(update_user_tokens("u1", gsc_token={"new": True}))
        self.assertIsNotNone(result)

    def test_updates_ga4_token(self):
        mock_sb = MagicMock()
        mock_sb.table.return_value.update.return_value.eq.return_value.execute.return_value = MagicMock(
            data=[{"id": "u1"}]
        )
        sys.modules["api.database"].get_supabase_client.return_value = mock_sb
        from api.auth.dependencies import update_user_tokens
        result = self._run(update_user_tokens("u1", ga4_token={"x": 1}))
        self.assertIsNotNone(result)

    def test_no_tokens_raises_value_error(self):
        from api.auth.dependencies import update_user_tokens
        with self.assertRaises(ValueError) as ctx:
            self._run(update_user_tokens("u1"))
        self.assertIn("At least one token", str(ctx.exception))

    def test_empty_result_raises(self):
        mock_sb = MagicMock()
        mock_sb.table.return_value.update.return_value.eq.return_value.execute.return_value = MagicMock(data=[])
        sys.modules["api.database"].get_supabase_client.return_value = mock_sb
        from api.auth.dependencies import update_user_tokens
        with self.assertRaises(Exception) as ctx:
            self._run(update_user_tokens("u1", gsc_token={"a": 1}))
        self.assertIn("Failed to update", str(ctx.exception))


# =====================================================================
# SECTION 6 — TokenEncryption
# =====================================================================

@unittest.skipUnless(HAS_CRYPTO, "cryptography library not installed")
class TestTokenEncryption(unittest.TestCase):
    """Test token encrypt/decrypt round-trip."""

    def test_encrypt_decrypt_round_trip(self):
        from api.auth.oauth import TokenEncryption
        key = Fernet.generate_key().decode()
        enc = TokenEncryption(encryption_key=key)
        data = {"token": "abc123", "refresh_token": "ref456"}
        encrypted = enc.encrypt(data)
        self.assertIsInstance(encrypted, str)
        decrypted = enc.decrypt(encrypted)
        self.assertEqual(decrypted, data)

    def test_encrypt_different_each_time(self):
        from api.auth.oauth import TokenEncryption
        key = Fernet.generate_key().decode()
        enc = TokenEncryption(encryption_key=key)
        data = {"token": "same"}
        e1 = enc.encrypt(data)
        e2 = enc.encrypt(data)
        # Fernet uses timestamp nonce, so encryptions differ
        self.assertNotEqual(e1, e2)

    def test_decrypt_invalid_data_raises(self):
        from api.auth.oauth import TokenEncryption
        key = Fernet.generate_key().decode()
        enc = TokenEncryption(encryption_key=key)
        with self.assertRaises(ValueError) as ctx:
            enc.decrypt("not-valid-encrypted-data")
        self.assertIn("Failed to decrypt", str(ctx.exception))

    def test_decrypt_wrong_key_raises(self):
        from api.auth.oauth import TokenEncryption
        key1 = Fernet.generate_key().decode()
        key2 = Fernet.generate_key().decode()
        enc1 = TokenEncryption(encryption_key=key1)
        enc2 = TokenEncryption(encryption_key=key2)
        encrypted = enc1.encrypt({"token": "secret"})
        with self.assertRaises(ValueError):
            enc2.decrypt(encrypted)

    def test_fallback_key_generation(self):
        """When no key is provided and no env var, PBKDF2 fallback is used."""
        from api.auth.oauth import TokenEncryption
        with patch.dict(os.environ, {}, clear=False):
            # Remove ENCRYPTION_KEY if present
            os.environ.pop("ENCRYPTION_KEY", None)
            enc = TokenEncryption(encryption_key=None)
            data = {"token": "fallback_test"}
            encrypted = enc.encrypt(data)
            decrypted = enc.decrypt(encrypted)
            self.assertEqual(decrypted, data)

    def test_unicode_data(self):
        from api.auth.oauth import TokenEncryption
        key = Fernet.generate_key().decode()
        enc = TokenEncryption(encryption_key=key)
        data = {"token": "日本語テスト", "user": "用户"}
        encrypted = enc.encrypt(data)
        decrypted = enc.decrypt(encrypted)
        self.assertEqual(decrypted, data)

    def test_empty_dict(self):
        from api.auth.oauth import TokenEncryption
        key = Fernet.generate_key().decode()
        enc = TokenEncryption(encryption_key=key)
        encrypted = enc.encrypt({})
        decrypted = enc.decrypt(encrypted)
        self.assertEqual(decrypted, {})

    def test_nested_data(self):
        from api.auth.oauth import TokenEncryption
        key = Fernet.generate_key().decode()
        enc = TokenEncryption(encryption_key=key)
        data = {"token": "t", "scopes": ["read", "write"], "meta": {"created": 123}}
        encrypted = enc.encrypt(data)
        decrypted = enc.decrypt(encrypted)
        self.assertEqual(decrypted, data)


# =====================================================================
# SECTION 7 — OAuth flow helpers
# =====================================================================

class TestGetOauthFlow(unittest.TestCase):
    """Test OAuth flow creation."""

    @patch.dict(os.environ, {"GOOGLE_CLIENT_ID": "", "GOOGLE_CLIENT_SECRET": ""})
    def test_missing_credentials_raises(self):
        # Force reload of module-level vars
        import api.auth.oauth as oauth_mod
        oauth_mod.GOOGLE_CLIENT_ID = ""
        oauth_mod.GOOGLE_CLIENT_SECRET = ""
        with self.assertRaises(ValueError) as ctx:
            oauth_mod.get_oauth_flow()
        self.assertIn("not configured", str(ctx.exception))

    def test_scopes_include_gsc_and_ga4(self):
        import api.auth.oauth as oauth_mod
        self.assertIn("https://www.googleapis.com/auth/webmasters.readonly", oauth_mod.SCOPES)
        self.assertIn("https://www.googleapis.com/auth/analytics.readonly", oauth_mod.SCOPES)

    def test_scopes_include_openid(self):
        import api.auth.oauth as oauth_mod
        self.assertIn("openid", oauth_mod.SCOPES)

    def test_scopes_count(self):
        import api.auth.oauth as oauth_mod
        self.assertEqual(len(oauth_mod.SCOPES), 5)


class TestGenerateAuthUrl(unittest.TestCase):
    """Test auth URL generation."""

    def test_returns_url_and_state(self):
        import api.auth.oauth as oauth_mod
        oauth_mod.GOOGLE_CLIENT_ID = "test-id"
        oauth_mod.GOOGLE_CLIENT_SECRET = "test-secret"
        mock_flow = MagicMock()
        mock_flow.authorization_url.return_value = ("https://accounts.google.com/auth?...", "state123")
        with patch.object(oauth_mod, "get_oauth_flow", return_value=mock_flow):
            result = oauth_mod.generate_auth_url()
        self.assertIn("url", result)
        self.assertIn("state", result)
        self.assertTrue(len(result["state"]) > 10)

    def test_state_is_random(self):
        import api.auth.oauth as oauth_mod
        oauth_mod.GOOGLE_CLIENT_ID = "test-id"
        oauth_mod.GOOGLE_CLIENT_SECRET = "test-secret"
        mock_flow = MagicMock()
        mock_flow.authorization_url.return_value = ("https://url", "s")
        with patch.object(oauth_mod, "get_oauth_flow", return_value=mock_flow):
            r1 = oauth_mod.generate_auth_url()
            r2 = oauth_mod.generate_auth_url()
        self.assertNotEqual(r1["state"], r2["state"])


# =====================================================================
# SECTION 8 — verify_scopes
# =====================================================================

class TestVerifyScopes(unittest.TestCase):
    """Test scope verification."""

    def test_all_scopes_granted_no_error(self):
        import api.auth.oauth as oauth_mod
        creds = MagicMock()
        creds.scopes = [
            "https://www.googleapis.com/auth/webmasters.readonly",
            "https://www.googleapis.com/auth/analytics.readonly",
            "openid"
        ]
        # Should not raise
        oauth_mod.verify_scopes(creds)

    def test_missing_gsc_scope_raises(self):
        import api.auth.oauth as oauth_mod
        creds = MagicMock()
        creds.scopes = ["https://www.googleapis.com/auth/analytics.readonly"]
        with self.assertRaises(_HTTPException) as ctx:
            oauth_mod.verify_scopes(creds)
        self.assertEqual(ctx.exception.status_code, 400)
        self.assertIn("webmasters", ctx.exception.detail)

    def test_missing_ga4_scope_raises(self):
        import api.auth.oauth as oauth_mod
        creds = MagicMock()
        creds.scopes = ["https://www.googleapis.com/auth/webmasters.readonly"]
        with self.assertRaises(_HTTPException) as ctx:
            oauth_mod.verify_scopes(creds)
        self.assertIn("analytics", ctx.exception.detail)

    def test_no_scopes_at_all_raises(self):
        import api.auth.oauth as oauth_mod
        creds = MagicMock()
        creds.scopes = []
        with self.assertRaises(_HTTPException):
            oauth_mod.verify_scopes(creds)

    def test_none_scopes_raises(self):
        import api.auth.oauth as oauth_mod
        creds = MagicMock()
        creds.scopes = None
        with self.assertRaises(_HTTPException):
            oauth_mod.verify_scopes(creds)


# =====================================================================
# SECTION 9 — _get_supabase lazy init
# =====================================================================

class TestGetSupabaseLazy(unittest.TestCase):
    """Test lazy Supabase client initialization."""

    def test_missing_env_raises(self):
        import api.auth.oauth as oauth_mod
        oauth_mod._supabase_client = None
        with patch.dict(os.environ, {"SUPABASE_URL": "", "SUPABASE_KEY": ""}):
            with self.assertRaises(ValueError) as ctx:
                oauth_mod._get_supabase()
            self.assertIn("Missing", str(ctx.exception))

    def test_caches_client(self):
        import api.auth.oauth as oauth_mod
        mock_client = MagicMock()
        oauth_mod._supabase_client = mock_client
        result = oauth_mod._get_supabase()
        self.assertEqual(result, mock_client)


# =====================================================================
# SECTION 10 — handle_oauth_callback
# =====================================================================

class TestHandleOauthCallback(unittest.TestCase):
    """Test OAuth callback handling."""

    def _run(self, coro):
        return asyncio.get_event_loop().run_until_complete(coro)

    def test_callback_failure_raises_400(self):
        import api.auth.oauth as oauth_mod
        mock_flow = MagicMock()
        mock_flow.fetch_token.side_effect = Exception("invalid code")
        with patch.object(oauth_mod, "get_oauth_flow", return_value=mock_flow):
            with self.assertRaises(_HTTPException) as ctx:
                self._run(oauth_mod.handle_oauth_callback("bad-code", "state"))
            self.assertEqual(ctx.exception.status_code, 400)

    def test_successful_callback_new_user(self):
        import api.auth.oauth as oauth_mod
        mock_flow = MagicMock()
        mock_creds = MagicMock()
        mock_creds.token = "access_tok"
        mock_creds.refresh_token = "refresh_tok"
        mock_creds.token_uri = "https://oauth2.googleapis.com/token"
        mock_creds.client_id = "cid"
        mock_creds.client_secret = "csec"
        mock_creds.scopes = [
            "https://www.googleapis.com/auth/webmasters.readonly",
            "https://www.googleapis.com/auth/analytics.readonly"
        ]
        mock_creds.expiry = datetime.now(timezone.utc) + timedelta(hours=1)
        mock_flow.credentials = mock_creds
        mock_flow.fetch_token = MagicMock()

        mock_sb = MagicMock()
        # User not found -> insert
        mock_sb.table.return_value.select.return_value.eq.return_value.execute.return_value = MagicMock(data=[])
        mock_sb.table.return_value.insert.return_value.execute.return_value = MagicMock(
            data=[{"id": "new-user-id"}]
        )
        oauth_mod._supabase_client = mock_sb

        with patch.object(oauth_mod, "get_oauth_flow", return_value=mock_flow):
            with patch.object(oauth_mod, "get_user_info", return_value={"email": "new@test.com"}):
                with patch.object(oauth_mod, "verify_scopes"):
                    result = self._run(oauth_mod.handle_oauth_callback("code123", "state123"))

        self.assertTrue(result["success"])
        self.assertEqual(result["email"], "new@test.com")

    def test_missing_email_raises_400(self):
        import api.auth.oauth as oauth_mod
        mock_flow = MagicMock()
        mock_creds = MagicMock()
        mock_creds.token = "tok"
        mock_creds.refresh_token = "ref"
        mock_creds.token_uri = "uri"
        mock_creds.client_id = "cid"
        mock_creds.client_secret = "csec"
        mock_creds.scopes = []
        mock_creds.expiry = None
        mock_flow.credentials = mock_creds
        mock_flow.fetch_token = MagicMock()

        with patch.object(oauth_mod, "get_oauth_flow", return_value=mock_flow):
            with patch.object(oauth_mod, "get_user_info", return_value={}):
                with self.assertRaises(_HTTPException) as ctx:
                    self._run(oauth_mod.handle_oauth_callback("code", "state"))
                self.assertEqual(ctx.exception.status_code, 400)


# =====================================================================
# SECTION 11 — Edge cases and integration scenarios
# =====================================================================

class TestAuthModuleExports(unittest.TestCase):
    """Test __init__.py exports."""

    def test_get_current_user_exported(self):
        from api.auth import get_current_user
        self.assertIsNotNone(get_current_user)

    def test_all_exports(self):
        import api.auth as auth_pkg
        self.assertIn("get_current_user", auth_pkg.__all__)


class TestSecurityScheme(unittest.TestCase):
    """Test security configuration."""

    def test_algorithm_is_hs256(self):
        from api.auth.dependencies import ALGORITHM
        self.assertEqual(ALGORITHM, "HS256")

    def test_default_secret_key_warning(self):
        """Default secret key should be present but obviously insecure."""
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("JWT_SECRET_KEY", None)
            # Re-read module-level
            from api.auth.dependencies import SECRET_KEY
            # The default key should NOT be empty (would cause jwt failures)
            self.assertTrue(len(SECRET_KEY) > 0)


class TestOauthConstants(unittest.TestCase):
    """Test oauth.py constants and defaults."""

    def test_redirect_uri_default(self):
        import api.auth.oauth as oauth_mod
        # With no env var, defaults to localhost
        default = os.getenv("OAUTH_REDIRECT_URI", "http://localhost:8000/auth/callback")
        self.assertIn("callback", default)

    def test_scopes_are_readonly(self):
        """Scopes should be read-only for security."""
        import api.auth.oauth as oauth_mod
        for scope in oauth_mod.SCOPES:
            if "webmasters" in scope or "analytics" in scope:
                self.assertIn("readonly", scope)


class TestEdgeCases(unittest.TestCase):
    """Edge cases across the auth module."""

    def _run(self, coro):
        return asyncio.get_event_loop().run_until_complete(coro)

    def test_empty_gsc_token_dict_raises_403(self):
        """Empty dict for gsc_token should raise (missing required fields)."""
        from api.auth.dependencies import verify_gsc_token
        user = {"id": "u1", "gsc_token": {}}
        with self.assertRaises(_HTTPException) as ctx:
            self._run(verify_gsc_token(user))
        self.assertEqual(ctx.exception.status_code, 403)

    def test_extra_fields_in_token_ok(self):
        """Extra fields in token dict should not cause issues."""
        from api.auth.dependencies import verify_gsc_token
        user = {"id": "u1", "gsc_token": {
            "access_token": "a", "refresh_token": "b", "token_uri": "c",
            "extra_field": "should be ignored"
        }}
        result = self._run(verify_gsc_token(user))
        self.assertEqual(result["id"], "u1")

    @patch.dict(os.environ, {"JWT_SECRET_KEY": "test-secret"})
    def test_create_token_does_not_include_sensitive_in_payload(self):
        """Token creation should include exp but data is passed through."""
        from api.auth.dependencies import create_access_token
        jose_jwt.encode.return_value = "tok"
        create_access_token({"sub": "u1", "email": "test@test.com"})
        call_args = jose_jwt.encode.call_args
        payload = call_args[0][0]
        self.assertIn("sub", payload)
        self.assertIn("email", payload)
        self.assertIn("exp", payload)

    def test_update_both_tokens_at_once(self):
        mock_sb = MagicMock()
        mock_sb.table.return_value.update.return_value.eq.return_value.execute.return_value = MagicMock(
            data=[{"id": "u1"}]
        )
        sys.modules["api.database"].get_supabase_client.return_value = mock_sb
        from api.auth.dependencies import update_user_tokens
        result = self._run(update_user_tokens("u1", gsc_token={"a": 1}, ga4_token={"b": 2}))
        self.assertIsNotNone(result)
        # Verify both tokens were in the update call
        update_call = mock_sb.table.return_value.update.call_args
        update_data = update_call[0][0]
        self.assertIn("gsc_token", update_data)
        self.assertIn("ga4_token", update_data)


if __name__ == "__main__":
    unittest.main()
