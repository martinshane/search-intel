"""
Comprehensive test suite for api/routers/ — auth, health, and reports routers.

Tests cover:
- Health router: basic health check, detailed health check
- Auth router: login, callback, GSC/GA4 properties, token revocation
- Reports router: helpers (_user_id, _get_supabase, _require_completed, _fetch_module_results),
  CRUD (create, get, list), PDF export, email delivery, consulting CTAs, consulting services
"""

import ast
import os
import sys
import json
import uuid
import unittest
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock
from datetime import datetime, timezone, timedelta


# ---------------------------------------------------------------------------
# 1. Health Router Tests
# ---------------------------------------------------------------------------


class TestHealthCheck(unittest.TestCase):
    """Test basic health check endpoint."""

    def _import_health_router(self):
        with patch.dict(os.environ, {"ENVIRONMENT": "test"}):
            from api.routers.health import health_check
            return health_check

    @patch("api.routers.health.APP_VERSION", "1.0.0")
    def test_returns_ok_status(self):
        import asyncio
        health_check = self._import_health_router()
        result = asyncio.get_event_loop().run_until_complete(health_check())
        self.assertEqual(result["status"], "ok")

    @patch("api.routers.health.APP_VERSION", "1.0.0")
    def test_returns_service_name(self):
        import asyncio
        health_check = self._import_health_router()
        result = asyncio.get_event_loop().run_until_complete(health_check())
        self.assertEqual(result["service"], "search-intel-api")

    @patch("api.routers.health.APP_VERSION", "2.5.0")
    def test_returns_version(self):
        import asyncio
        health_check = self._import_health_router()
        result = asyncio.get_event_loop().run_until_complete(health_check())
        self.assertEqual(result["version"], "2.5.0")

    @patch("api.routers.health.APP_VERSION", "1.0.0")
    def test_has_three_keys(self):
        import asyncio
        health_check = self._import_health_router()
        result = asyncio.get_event_loop().run_until_complete(health_check())
        self.assertEqual(set(result.keys()), {"status", "service", "version"})


class TestDetailedHealth(unittest.TestCase):
    """Test detailed health check endpoint."""

    @patch("api.routers.health.APP_VERSION", "1.0.0")
    @patch.dict(os.environ, {"SUPABASE_URL": "", "SUPABASE_KEY": ""})
    def test_missing_credentials(self):
        import asyncio
        from api.routers.health import detailed_health
        result = asyncio.get_event_loop().run_until_complete(detailed_health())
        self.assertIn("dependencies", result)
        self.assertIn("supabase", result["dependencies"])

    @patch("api.routers.health.APP_VERSION", "1.0.0")
    @patch.dict(os.environ, {"SUPABASE_URL": "", "SUPABASE_KEY": ""})
    def test_degraded_without_supabase(self):
        import asyncio
        from api.routers.health import detailed_health
        result = asyncio.get_event_loop().run_until_complete(detailed_health())
        # Missing credentials → not healthy
        dep = result["dependencies"]["supabase"]
        self.assertFalse(dep["healthy"])

    @patch("api.routers.health.APP_VERSION", "1.0.0")
    @patch.dict(os.environ, {"SUPABASE_URL": "https://test.supabase.co", "SUPABASE_KEY": "test-key"})
    @patch("api.routers.health.create_client")
    def test_healthy_with_supabase(self, mock_create):
        import asyncio
        mock_client = MagicMock()
        mock_client.table.return_value.select.return_value.limit.return_value.execute.return_value = MagicMock(data=[{"id": 1}])
        mock_create.return_value = mock_client
        from api.routers.health import detailed_health
        result = asyncio.get_event_loop().run_until_complete(detailed_health())
        self.assertTrue(result["dependencies"]["supabase"]["healthy"])
        self.assertTrue(result["healthy"])
        self.assertEqual(result["status"], "ok")

    @patch("api.routers.health.APP_VERSION", "1.0.0")
    @patch.dict(os.environ, {"SUPABASE_URL": "https://test.supabase.co", "SUPABASE_KEY": "test-key"})
    @patch("api.routers.health.create_client")
    def test_supabase_exception(self, mock_create):
        import asyncio
        mock_create.side_effect = Exception("Connection refused")
        from api.routers.health import detailed_health
        result = asyncio.get_event_loop().run_until_complete(detailed_health())
        self.assertFalse(result["dependencies"]["supabase"]["healthy"])
        self.assertIn("Connection refused", result["dependencies"]["supabase"]["error"])

    @patch("api.routers.health.APP_VERSION", "1.0.0")
    @patch.dict(os.environ, {"SUPABASE_URL": "", "SUPABASE_KEY": ""})
    def test_response_has_required_keys(self):
        import asyncio
        from api.routers.health import detailed_health
        result = asyncio.get_event_loop().run_until_complete(detailed_health())
        for key in ("status", "service", "version", "healthy", "dependencies"):
            self.assertIn(key, result)


# ---------------------------------------------------------------------------
# 2. Auth Router Tests
# ---------------------------------------------------------------------------


class TestAuthLogin(unittest.TestCase):
    """Test /auth/login endpoint."""

    @patch("api.routers.auth.generate_auth_url")
    def test_login_success(self, mock_gen):
        import asyncio
        mock_gen.return_value = {"url": "https://accounts.google.com/o/oauth2", "state": "abc123"}
        from api.routers.auth import login
        result = asyncio.get_event_loop().run_until_complete(login())
        self.assertIn("authorization_url", result)
        self.assertIn("state", result)
        self.assertEqual(result["authorization_url"], "https://accounts.google.com/o/oauth2")
        self.assertEqual(result["state"], "abc123")

    @patch("api.routers.auth.generate_auth_url")
    def test_login_exception_raises_500(self, mock_gen):
        import asyncio
        from fastapi import HTTPException
        mock_gen.side_effect = Exception("No client ID")
        from api.routers.auth import login
        with self.assertRaises(HTTPException) as ctx:
            asyncio.get_event_loop().run_until_complete(login())
        self.assertEqual(ctx.exception.status_code, 500)
        self.assertIn("No client ID", ctx.exception.detail)


class TestAuthCallback(unittest.TestCase):
    """Test /auth/callback endpoint."""

    @patch("api.routers.auth.handle_oauth_callback")
    def test_callback_success(self, mock_cb):
        import asyncio
        mock_cb.return_value = {"success": True, "user_id": "u1", "email": "a@b.com"}
        from api.routers.auth import callback
        result = asyncio.get_event_loop().run_until_complete(callback(code="code123", state="state456"))
        self.assertTrue(result["success"])
        mock_cb.assert_called_once_with("code123", "state456")

    @patch("api.routers.auth.handle_oauth_callback")
    def test_callback_passes_params(self, mock_cb):
        import asyncio
        mock_cb.return_value = {}
        from api.routers.auth import callback
        asyncio.get_event_loop().run_until_complete(callback(code="XYZ", state="ABC"))
        mock_cb.assert_called_once_with("XYZ", "ABC")


class TestAuthGscProperties(unittest.TestCase):
    """Test /auth/gsc/properties endpoint."""

    @patch("api.routers.auth.verify_gsc_access")
    def test_gsc_properties(self, mock_verify):
        import asyncio
        mock_verify.return_value = {"has_access": True, "properties": [{"url": "sc-domain:example.com"}]}
        from api.routers.auth import gsc_properties
        result = asyncio.get_event_loop().run_until_complete(gsc_properties(user_id="u1"))
        self.assertTrue(result["has_access"])
        mock_verify.assert_called_once_with("u1")


class TestAuthGa4Properties(unittest.TestCase):
    """Test /auth/ga4/properties endpoint."""

    @patch("api.routers.auth.verify_ga4_access")
    def test_ga4_properties(self, mock_verify):
        import asyncio
        mock_verify.return_value = {"has_access": True, "properties": []}
        from api.routers.auth import ga4_properties
        result = asyncio.get_event_loop().run_until_complete(ga4_properties(user_id="u2"))
        mock_verify.assert_called_once_with("u2")


class TestAuthRevoke(unittest.TestCase):
    """Test /auth/revoke endpoint."""

    @patch("api.routers.auth.revoke_user_tokens")
    def test_revoke(self, mock_revoke):
        import asyncio
        mock_revoke.return_value = {"success": True}
        from api.routers.auth import revoke
        result = asyncio.get_event_loop().run_until_complete(revoke(user_id="u3"))
        self.assertTrue(result["success"])
        mock_revoke.assert_called_once_with("u3")


# ---------------------------------------------------------------------------
# 3. Reports Router — Helper Tests
# ---------------------------------------------------------------------------


class TestUserId(unittest.TestCase):
    """Test _user_id helper."""

    def test_sub_key(self):
        from api.routers.reports import _user_id
        self.assertEqual(_user_id({"sub": "abc"}), "abc")

    def test_id_key_fallback(self):
        from api.routers.reports import _user_id
        self.assertEqual(_user_id({"id": "def"}), "def")

    def test_user_id_key_fallback(self):
        from api.routers.reports import _user_id
        self.assertEqual(_user_id({"user_id": "ghi"}), "ghi")

    def test_sub_takes_priority(self):
        from api.routers.reports import _user_id
        self.assertEqual(_user_id({"sub": "s", "id": "i", "user_id": "u"}), "s")

    def test_empty_dict(self):
        from api.routers.reports import _user_id
        self.assertEqual(_user_id({}), "")

    def test_none_sub(self):
        from api.routers.reports import _user_id
        # dict.get("sub", ...) returns None if key exists with None value
        result = _user_id({"sub": None, "id": "fallback"})
        # sub exists but is None — get returns None
        self.assertIsNone(result)


class TestRequireCompleted(unittest.TestCase):
    """Test _require_completed helper."""

    def test_completed_status(self):
        from api.routers.reports import _require_completed
        # Should not raise
        _require_completed({"status": "completed"})

    def test_done_status(self):
        from api.routers.reports import _require_completed
        _require_completed({"status": "done"})

    def test_ready_status(self):
        from api.routers.reports import _require_completed
        _require_completed({"status": "ready"})

    def test_queued_raises(self):
        from api.routers.reports import _require_completed
        from fastapi import HTTPException
        with self.assertRaises(HTTPException) as ctx:
            _require_completed({"status": "queued"})
        self.assertEqual(ctx.exception.status_code, 400)
        self.assertIn("queued", ctx.exception.detail)

    def test_running_raises(self):
        from api.routers.reports import _require_completed
        from fastapi import HTTPException
        with self.assertRaises(HTTPException):
            _require_completed({"status": "running"})

    def test_custom_action_in_message(self):
        from api.routers.reports import _require_completed
        from fastapi import HTTPException
        with self.assertRaises(HTTPException) as ctx:
            _require_completed({"status": "pending"}, action="PDF export")
        self.assertIn("PDF export", ctx.exception.detail)

    def test_no_status_key(self):
        from api.routers.reports import _require_completed
        from fastapi import HTTPException
        with self.assertRaises(HTTPException):
            _require_completed({})


class TestGetSupabase(unittest.TestCase):
    """Test _get_supabase helper."""

    @patch.dict(os.environ, {"SUPABASE_URL": "", "SUPABASE_KEY": ""})
    def test_missing_url_raises(self):
        from api.routers.reports import _get_supabase
        from fastapi import HTTPException
        with self.assertRaises(HTTPException) as ctx:
            _get_supabase()
        self.assertEqual(ctx.exception.status_code, 500)

    @patch.dict(os.environ, {"SUPABASE_URL": "https://x.supabase.co", "SUPABASE_KEY": "key123"})
    @patch("api.routers.reports.create_client")
    def test_returns_client(self, mock_create):
        mock_client = MagicMock()
        mock_create.return_value = mock_client
        from api.routers.reports import _get_supabase
        result = _get_supabase()
        self.assertEqual(result, mock_client)
        mock_create.assert_called_once_with("https://x.supabase.co", "key123")


class TestFetchModuleResults(unittest.TestCase):
    """Test _fetch_module_results helper."""

    @patch("api.routers.reports._get_supabase")
    def test_returns_dict_keyed_by_module_number(self, mock_sb):
        mock_client = MagicMock()
        mock_client.table.return_value.select.return_value.eq.return_value.order.return_value.execute.return_value = MagicMock(
            data=[
                {"module_number": 1, "results": {"summary": "ok"}},
                {"module_number": 5, "results": {"summary": "good"}},
            ]
        )
        mock_sb.return_value = mock_client
        from api.routers.reports import _fetch_module_results
        result = _fetch_module_results("report-123")
        self.assertIn(1, result)
        self.assertIn(5, result)
        self.assertEqual(result[1]["summary"], "ok")

    @patch("api.routers.reports._get_supabase")
    def test_empty_results(self, mock_sb):
        mock_client = MagicMock()
        mock_client.table.return_value.select.return_value.eq.return_value.order.return_value.execute.return_value = MagicMock(data=[])
        mock_sb.return_value = mock_client
        from api.routers.reports import _fetch_module_results
        result = _fetch_module_results("report-456")
        self.assertEqual(result, {})

    @patch("api.routers.reports._get_supabase")
    def test_none_data(self, mock_sb):
        mock_client = MagicMock()
        mock_client.table.return_value.select.return_value.eq.return_value.order.return_value.execute.return_value = MagicMock(data=None)
        mock_sb.return_value = mock_client
        from api.routers.reports import _fetch_module_results
        result = _fetch_module_results("report-789")
        self.assertEqual(result, {})

    @patch("api.routers.reports._get_supabase")
    def test_skips_none_module_number(self, mock_sb):
        mock_client = MagicMock()
        mock_client.table.return_value.select.return_value.eq.return_value.order.return_value.execute.return_value = MagicMock(
            data=[{"module_number": None, "results": {"x": 1}}, {"module_number": 2, "results": {"y": 2}}]
        )
        mock_sb.return_value = mock_client
        from api.routers.reports import _fetch_module_results
        result = _fetch_module_results("r")
        self.assertNotIn(None, result)
        self.assertIn(2, result)

    @patch("api.routers.reports._get_supabase")
    def test_skips_empty_results(self, mock_sb):
        mock_client = MagicMock()
        mock_client.table.return_value.select.return_value.eq.return_value.order.return_value.execute.return_value = MagicMock(
            data=[{"module_number": 3, "results": None}]
        )
        mock_sb.return_value = mock_client
        from api.routers.reports import _fetch_module_results
        result = _fetch_module_results("r")
        self.assertNotIn(3, result)

    @patch("api.routers.reports._get_supabase")
    def test_non_dict_results_become_empty(self, mock_sb):
        mock_client = MagicMock()
        mock_client.table.return_value.select.return_value.eq.return_value.order.return_value.execute.return_value = MagicMock(
            data=[{"module_number": 4, "results": "just a string"}]
        )
        mock_sb.return_value = mock_client
        from api.routers.reports import _fetch_module_results
        result = _fetch_module_results("r")
        self.assertEqual(result[4], {})

    @patch("api.routers.reports._get_supabase")
    def test_db_exception_raises_http(self, mock_sb):
        from fastapi import HTTPException
        mock_client = MagicMock()
        mock_client.table.return_value.select.return_value.eq.return_value.order.return_value.execute.side_effect = Exception("DB down")
        mock_sb.return_value = mock_client
        from api.routers.reports import _fetch_module_results
        with self.assertRaises(HTTPException) as ctx:
            _fetch_module_results("r")
        self.assertEqual(ctx.exception.status_code, 500)


class TestGetOwnedReport(unittest.TestCase):
    """Test _get_owned_report helper."""

    @patch("api.routers.reports._get_supabase")
    def test_report_found_and_owned(self, mock_sb):
        import asyncio
        mock_client = MagicMock()
        mock_client.table.return_value.select.return_value.eq.return_value.execute.return_value = MagicMock(
            data=[{"id": "r1", "user_id": "u1", "status": "completed"}]
        )
        mock_sb.return_value = mock_client
        from api.routers.reports import _get_owned_report
        result = asyncio.get_event_loop().run_until_complete(_get_owned_report("r1", {"sub": "u1"}))
        self.assertEqual(result["id"], "r1")

    @patch("api.routers.reports._get_supabase")
    def test_report_not_found(self, mock_sb):
        import asyncio
        from fastapi import HTTPException
        mock_client = MagicMock()
        mock_client.table.return_value.select.return_value.eq.return_value.execute.return_value = MagicMock(data=[])
        mock_sb.return_value = mock_client
        from api.routers.reports import _get_owned_report
        with self.assertRaises(HTTPException) as ctx:
            asyncio.get_event_loop().run_until_complete(_get_owned_report("r1", {"sub": "u1"}))
        self.assertEqual(ctx.exception.status_code, 404)

    @patch("api.routers.reports._get_supabase")
    def test_report_wrong_owner(self, mock_sb):
        import asyncio
        from fastapi import HTTPException
        mock_client = MagicMock()
        mock_client.table.return_value.select.return_value.eq.return_value.execute.return_value = MagicMock(
            data=[{"id": "r1", "user_id": "u1", "status": "completed"}]
        )
        mock_sb.return_value = mock_client
        from api.routers.reports import _get_owned_report
        with self.assertRaises(HTTPException) as ctx:
            asyncio.get_event_loop().run_until_complete(_get_owned_report("r1", {"sub": "u2"}))
        self.assertEqual(ctx.exception.status_code, 403)

    @patch("api.routers.reports._get_supabase")
    def test_db_error_raises_500(self, mock_sb):
        import asyncio
        from fastapi import HTTPException
        mock_client = MagicMock()
        mock_client.table.return_value.select.return_value.eq.return_value.execute.side_effect = Exception("Timeout")
        mock_sb.return_value = mock_client
        from api.routers.reports import _get_owned_report
        with self.assertRaises(HTTPException) as ctx:
            asyncio.get_event_loop().run_until_complete(_get_owned_report("r1", {"sub": "u1"}))
        self.assertEqual(ctx.exception.status_code, 500)


# ---------------------------------------------------------------------------
# 4. Reports Router — CRUD Endpoint Tests
# ---------------------------------------------------------------------------


class TestCreateReport(unittest.TestCase):
    """Test POST /reports/create endpoint."""

    @patch("api.routers.reports._get_supabase")
    @patch("api.routers.reports.uuid.uuid4")
    def test_create_success(self, mock_uuid, mock_sb):
        import asyncio
        from api.routers.reports import create_report, ReportRequest
        mock_uuid.return_value = uuid.UUID("12345678-1234-5678-1234-567812345678")
        mock_client = MagicMock()
        mock_client.table.return_value.insert.return_value.execute.return_value = MagicMock(data=[{"id": "12345678-1234-5678-1234-567812345678"}])
        mock_sb.return_value = mock_client
        req = ReportRequest(gsc_property="sc-domain:example.com", domain="example.com")
        result = asyncio.get_event_loop().run_until_complete(create_report(req, {"sub": "u1"}))
        self.assertEqual(result["status"], "queued")
        self.assertEqual(result["report_id"], "12345678-1234-5678-1234-567812345678")
        self.assertIn("message", result)

    @patch("api.routers.reports._get_supabase")
    def test_create_db_error(self, mock_sb):
        import asyncio
        from api.routers.reports import create_report, ReportRequest
        from fastapi import HTTPException
        mock_client = MagicMock()
        mock_client.table.return_value.insert.return_value.execute.side_effect = Exception("Insert failed")
        mock_sb.return_value = mock_client
        req = ReportRequest(gsc_property="sc-domain:test.com", domain="test.com")
        with self.assertRaises(HTTPException) as ctx:
            asyncio.get_event_loop().run_until_complete(create_report(req, {"sub": "u1"}))
        self.assertEqual(ctx.exception.status_code, 500)


class TestGetReport(unittest.TestCase):
    """Test GET /reports/{report_id} endpoint."""

    @patch("api.routers.reports._get_owned_report")
    def test_get_report_returns_data(self, mock_owned):
        import asyncio
        mock_owned.return_value = {"id": "r1", "domain": "example.com", "status": "completed"}
        from api.routers.reports import get_report
        result = asyncio.get_event_loop().run_until_complete(get_report("r1", {"sub": "u1"}))
        self.assertEqual(result["id"], "r1")
        mock_owned.assert_called_once_with("r1", {"sub": "u1"})


class TestListMyReports(unittest.TestCase):
    """Test GET /reports/user/me endpoint."""

    @patch("api.routers.reports._get_supabase")
    def test_list_reports(self, mock_sb):
        import asyncio
        mock_client = MagicMock()
        mock_client.table.return_value.select.return_value.eq.return_value.order.return_value.execute.return_value = MagicMock(
            data=[{"id": "r1", "domain": "a.com"}, {"id": "r2", "domain": "b.com"}]
        )
        mock_sb.return_value = mock_client
        from api.routers.reports import list_my_reports
        result = asyncio.get_event_loop().run_until_complete(list_my_reports({"sub": "u1"}))
        self.assertEqual(len(result), 2)

    @patch("api.routers.reports._get_supabase")
    def test_list_empty(self, mock_sb):
        import asyncio
        mock_client = MagicMock()
        mock_client.table.return_value.select.return_value.eq.return_value.order.return_value.execute.return_value = MagicMock(data=None)
        mock_sb.return_value = mock_client
        from api.routers.reports import list_my_reports
        result = asyncio.get_event_loop().run_until_complete(list_my_reports({"sub": "u1"}))
        self.assertEqual(result, [])

    @patch("api.routers.reports._get_supabase")
    def test_list_db_error(self, mock_sb):
        import asyncio
        from fastapi import HTTPException
        mock_client = MagicMock()
        mock_client.table.return_value.select.return_value.eq.return_value.order.return_value.execute.side_effect = Exception("DB error")
        mock_sb.return_value = mock_client
        from api.routers.reports import list_my_reports
        with self.assertRaises(HTTPException) as ctx:
            asyncio.get_event_loop().run_until_complete(list_my_reports({"sub": "u1"}))
        self.assertEqual(ctx.exception.status_code, 500)


# ---------------------------------------------------------------------------
# 5. Reports Router — PDF Export Tests
# ---------------------------------------------------------------------------


class TestExportPdf(unittest.TestCase):
    """Test GET /reports/{report_id}/pdf endpoint."""

    @patch("api.routers.reports._fetch_module_results")
    @patch("api.routers.reports._get_owned_report")
    def test_pdf_export_success(self, mock_owned, mock_modules):
        import asyncio
        mock_owned.return_value = {"id": "r1", "user_id": "u1", "status": "completed", "domain": "example.com"}
        mock_modules.return_value = {1: {"summary": "ok"}}
        with patch("api.services.pdf_export.generate_pdf_report", return_value=b"%PDF-1.4 fake"):
            from api.routers.reports import export_report_pdf
            result = asyncio.get_event_loop().run_until_complete(export_report_pdf("r1", {"sub": "u1"}))
            self.assertEqual(result.media_type, "application/pdf")
            self.assertIn(b"PDF", result.body)

    @patch("api.routers.reports._fetch_module_results")
    @patch("api.routers.reports._get_owned_report")
    def test_pdf_not_completed(self, mock_owned, mock_modules):
        import asyncio
        from fastapi import HTTPException
        mock_owned.return_value = {"id": "r1", "user_id": "u1", "status": "queued"}
        from api.routers.reports import export_report_pdf
        with self.assertRaises(HTTPException) as ctx:
            asyncio.get_event_loop().run_until_complete(export_report_pdf("r1", {"sub": "u1"}))
        self.assertEqual(ctx.exception.status_code, 400)

    @patch("api.routers.reports._fetch_module_results")
    @patch("api.routers.reports._get_owned_report")
    def test_pdf_no_modules(self, mock_owned, mock_modules):
        import asyncio
        from fastapi import HTTPException
        mock_owned.return_value = {"id": "r1", "user_id": "u1", "status": "completed"}
        mock_modules.return_value = {}
        from api.routers.reports import export_report_pdf
        with self.assertRaises(HTTPException) as ctx:
            asyncio.get_event_loop().run_until_complete(export_report_pdf("r1", {"sub": "u1"}))
        self.assertEqual(ctx.exception.status_code, 400)

    @patch("api.routers.reports._fetch_module_results")
    @patch("api.routers.reports._get_owned_report")
    def test_pdf_filename_contains_domain(self, mock_owned, mock_modules):
        import asyncio
        mock_owned.return_value = {"id": "r1", "user_id": "u1", "status": "completed", "domain": "my.site.com"}
        mock_modules.return_value = {1: {"summary": "ok"}}
        with patch("api.services.pdf_export.generate_pdf_report", return_value=b"%PDF"):
            from api.routers.reports import export_report_pdf
            result = asyncio.get_event_loop().run_until_complete(export_report_pdf("r1", {"sub": "u1"}))
            disp = result.headers.get("content-disposition", "")
            self.assertIn("my_site_com", disp)

    @patch("api.routers.reports._fetch_module_results")
    @patch("api.routers.reports._get_owned_report")
    def test_pdf_import_error(self, mock_owned, mock_modules):
        import asyncio
        from fastapi import HTTPException
        mock_owned.return_value = {"id": "r1", "user_id": "u1", "status": "completed"}
        mock_modules.return_value = {1: {"summary": "ok"}}
        with patch.dict("sys.modules", {"api.services.pdf_export": None}):
            from api.routers.reports import export_report_pdf
            with self.assertRaises((HTTPException, ImportError, TypeError)):
                asyncio.get_event_loop().run_until_complete(export_report_pdf("r1", {"sub": "u1"}))


# ---------------------------------------------------------------------------
# 6. Reports Router — Email Delivery Tests
# ---------------------------------------------------------------------------


class TestEmailReport(unittest.TestCase):
    """Test POST /reports/{report_id}/email endpoint."""

    @patch("api.routers.reports._get_supabase")
    @patch("api.routers.reports._fetch_module_results")
    @patch("api.routers.reports._get_owned_report")
    def test_email_success(self, mock_owned, mock_modules, mock_sb):
        import asyncio
        mock_owned.return_value = {"id": "r1", "user_id": "u1", "status": "completed", "domain": "example.com"}
        mock_modules.return_value = {1: {"summary": "ok"}}
        mock_client = MagicMock()
        mock_sb.return_value = mock_client
        with patch("api.services.pdf_export.generate_pdf_report", return_value=b"%PDF"), \
             patch("api.services.email_delivery.send_report_email", new_callable=AsyncMock) as mock_send:
            mock_send.return_value = {"success": True, "provider": "smtp"}
            from api.routers.reports import email_report, EmailRequest
            req = EmailRequest(to_email="test@example.com")
            result = asyncio.get_event_loop().run_until_complete(email_report("r1", req, {"sub": "u1"}))
            self.assertTrue(result["success"])
            self.assertEqual(result["to_email"], "test@example.com")

    @patch("api.routers.reports._fetch_module_results")
    @patch("api.routers.reports._get_owned_report")
    def test_email_not_completed(self, mock_owned, mock_modules):
        import asyncio
        from fastapi import HTTPException
        mock_owned.return_value = {"id": "r1", "user_id": "u1", "status": "running"}
        from api.routers.reports import email_report, EmailRequest
        req = EmailRequest(to_email="test@example.com")
        with self.assertRaises(HTTPException) as ctx:
            asyncio.get_event_loop().run_until_complete(email_report("r1", req, {"sub": "u1"}))
        self.assertEqual(ctx.exception.status_code, 400)

    @patch("api.routers.reports._fetch_module_results")
    @patch("api.routers.reports._get_owned_report")
    def test_email_no_modules(self, mock_owned, mock_modules):
        import asyncio
        from fastapi import HTTPException
        mock_owned.return_value = {"id": "r1", "user_id": "u1", "status": "completed"}
        mock_modules.return_value = {}
        from api.routers.reports import email_report, EmailRequest
        req = EmailRequest(to_email="test@example.com")
        with self.assertRaises(HTTPException) as ctx:
            asyncio.get_event_loop().run_until_complete(email_report("r1", req, {"sub": "u1"}))
        self.assertEqual(ctx.exception.status_code, 400)

    @patch("api.routers.reports._get_supabase")
    @patch("api.routers.reports._fetch_module_results")
    @patch("api.routers.reports._get_owned_report")
    def test_email_delivery_failure(self, mock_owned, mock_modules, mock_sb):
        import asyncio
        from fastapi import HTTPException
        mock_owned.return_value = {"id": "r1", "user_id": "u1", "status": "completed", "domain": "t.com"}
        mock_modules.return_value = {1: {"summary": "ok"}}
        with patch("api.services.pdf_export.generate_pdf_report", return_value=b"%PDF"), \
             patch("api.services.email_delivery.send_report_email", new_callable=AsyncMock) as mock_send:
            mock_send.return_value = {"success": False, "error": "SMTP timeout"}
            from api.routers.reports import email_report, EmailRequest
            req = EmailRequest(to_email="fail@example.com")
            with self.assertRaises(HTTPException) as ctx:
                asyncio.get_event_loop().run_until_complete(email_report("r1", req, {"sub": "u1"}))
            self.assertEqual(ctx.exception.status_code, 502)


class TestEmailStatus(unittest.TestCase):
    """Test GET /reports/email/status endpoint."""

    def test_email_status_configured(self):
        import asyncio
        with patch("api.services.email_delivery.check_email_config", new_callable=AsyncMock) as mock_check:
            mock_check.return_value = {"configured": True, "provider": "sendgrid"}
            from api.routers.reports import email_status
            result = asyncio.get_event_loop().run_until_complete(email_status({"sub": "u1"}))
            self.assertTrue(result["configured"])

    def test_email_status_exception(self):
        import asyncio
        with patch("api.services.email_delivery.check_email_config", new_callable=AsyncMock) as mock_check:
            mock_check.side_effect = Exception("Config error")
            from api.routers.reports import email_status
            result = asyncio.get_event_loop().run_until_complete(email_status({"sub": "u1"}))
            self.assertFalse(result["configured"])


# ---------------------------------------------------------------------------
# 7. Reports Router — Consulting CTA Tests
# ---------------------------------------------------------------------------


class TestGetReportCtas(unittest.TestCase):
    """Test GET /reports/{report_id}/ctas endpoint."""

    @patch("api.routers.reports._fetch_module_results")
    @patch("api.routers.reports._get_owned_report")
    def test_ctas_success(self, mock_owned, mock_modules):
        import asyncio
        mock_owned.return_value = {"id": "r1", "user_id": "u1", "status": "completed"}
        mock_modules.return_value = {1: {"summary": "declining"}}
        with patch("api.services.consulting_ctas.generate_report_ctas") as mock_ctas:
            mock_ctas.return_value = {"ctas": [{"module": 1, "headline": "Fix it"}], "executive_cta": {}}
            from api.routers.reports import get_report_ctas
            result = asyncio.get_event_loop().run_until_complete(get_report_ctas("r1", {"sub": "u1"}, max_ctas=3))
            self.assertIn("ctas", result)
            mock_ctas.assert_called_once_with(mock_modules.return_value, max_ctas=3)

    @patch("api.routers.reports._fetch_module_results")
    @patch("api.routers.reports._get_owned_report")
    def test_ctas_not_completed(self, mock_owned, mock_modules):
        import asyncio
        from fastapi import HTTPException
        mock_owned.return_value = {"id": "r1", "user_id": "u1", "status": "queued"}
        from api.routers.reports import get_report_ctas
        with self.assertRaises(HTTPException) as ctx:
            asyncio.get_event_loop().run_until_complete(get_report_ctas("r1", {"sub": "u1"}, max_ctas=5))
        self.assertEqual(ctx.exception.status_code, 400)


class TestListConsultingServices(unittest.TestCase):
    """Test GET /reports/consulting/services endpoint."""

    def test_services_success(self):
        import asyncio
        with patch("api.services.consulting_ctas.get_available_services") as mock_svc, \
             patch("api.services.consulting_ctas.CONTACT_URL", "https://contact"), \
             patch("api.services.consulting_ctas.BOOKING_URL", "https://booking"), \
             patch("api.services.consulting_ctas.AUDIT_URL", "https://audit"):
            mock_svc.return_value = [{"name": "SEO Audit"}]
            from api.routers.reports import list_consulting_services
            result = asyncio.get_event_loop().run_until_complete(list_consulting_services({"sub": "u1"}))
            self.assertIn("services", result)
            self.assertIn("contact_url", result)
            self.assertIn("booking_url", result)
            self.assertIn("audit_url", result)


# ---------------------------------------------------------------------------
# 8. Request/Response Model Tests
# ---------------------------------------------------------------------------


class TestReportRequest(unittest.TestCase):
    """Test ReportRequest Pydantic model."""

    def test_required_fields(self):
        from api.routers.reports import ReportRequest
        req = ReportRequest(gsc_property="sc-domain:test.com", domain="test.com")
        self.assertEqual(req.gsc_property, "sc-domain:test.com")
        self.assertEqual(req.domain, "test.com")
        self.assertIsNone(req.ga4_property)

    def test_optional_ga4(self):
        from api.routers.reports import ReportRequest
        req = ReportRequest(gsc_property="x", domain="d", ga4_property="properties/12345")
        self.assertEqual(req.ga4_property, "properties/12345")

    def test_missing_gsc_raises(self):
        from api.routers.reports import ReportRequest
        from pydantic import ValidationError
        with self.assertRaises(ValidationError):
            ReportRequest(domain="test.com")

    def test_missing_domain_raises(self):
        from api.routers.reports import ReportRequest
        from pydantic import ValidationError
        with self.assertRaises(ValidationError):
            ReportRequest(gsc_property="x")


class TestEmailRequest(unittest.TestCase):
    """Test EmailRequest Pydantic model."""

    def test_required_fields(self):
        from api.routers.reports import EmailRequest
        req = EmailRequest(to_email="user@example.com")
        self.assertEqual(req.to_email, "user@example.com")
        self.assertIsNone(req.subject)

    def test_optional_subject(self):
        from api.routers.reports import EmailRequest
        req = EmailRequest(to_email="a@b.com", subject="My Report")
        self.assertEqual(req.subject, "My Report")


class TestReportResponse(unittest.TestCase):
    """Test ReportResponse Pydantic model."""

    def test_all_fields(self):
        from api.routers.reports import ReportResponse
        resp = ReportResponse(report_id="r1", status="queued", message="Created")
        self.assertEqual(resp.report_id, "r1")


class TestEmailResponse(unittest.TestCase):
    """Test EmailResponse Pydantic model."""

    def test_all_fields(self):
        from api.routers.reports import EmailResponse
        resp = EmailResponse(success=True, message="Sent", provider="smtp", to_email="a@b.com")
        self.assertTrue(resp.success)

    def test_optional_fields(self):
        from api.routers.reports import EmailResponse
        resp = EmailResponse(success=False, message="Failed")
        self.assertIsNone(resp.provider)
        self.assertIsNone(resp.to_email)


# ---------------------------------------------------------------------------
# 9. Router Registration Tests
# ---------------------------------------------------------------------------


class TestRouterRegistration(unittest.TestCase):
    """Test that routers have correct routes registered."""

    def test_health_router_has_routes(self):
        from api.routers.health import router
        paths = [r.path for r in router.routes]
        self.assertIn("", paths)
        self.assertIn("/", paths)
        self.assertIn("/detailed", paths)

    def test_auth_router_has_routes(self):
        from api.routers.auth import router
        paths = [r.path for r in router.routes]
        self.assertIn("/login", paths)
        self.assertIn("/callback", paths)
        self.assertIn("/gsc/properties", paths)
        self.assertIn("/ga4/properties", paths)
        self.assertIn("/revoke", paths)

    def test_reports_router_has_routes(self):
        from api.routers.reports import router
        paths = [r.path for r in router.routes]
        self.assertIn("/create", paths)
        self.assertIn("/{report_id}", paths)
        self.assertIn("/user/me", paths)
        self.assertIn("/{report_id}/pdf", paths)
        self.assertIn("/{report_id}/email", paths)
        self.assertIn("/email/status", paths)
        self.assertIn("/{report_id}/ctas", paths)
        self.assertIn("/consulting/services", paths)

    def test_auth_routes_are_get_or_post(self):
        from api.routers.auth import router
        for route in router.routes:
            if hasattr(route, 'methods'):
                for method in route.methods:
                    self.assertIn(method, {"GET", "POST"})


# ---------------------------------------------------------------------------
# 10. Edge Cases
# ---------------------------------------------------------------------------


class TestEdgeCases(unittest.TestCase):
    """Edge case tests across all routers."""

    def test_user_id_with_integer_values(self):
        from api.routers.reports import _user_id
        result = _user_id({"sub": 12345})
        self.assertEqual(result, 12345)

    def test_require_completed_with_none_status(self):
        from api.routers.reports import _require_completed
        from fastapi import HTTPException
        with self.assertRaises(HTTPException):
            _require_completed({"status": None})

    @patch("api.routers.reports._get_supabase")
    def test_fetch_module_results_int_conversion(self, mock_sb):
        mock_client = MagicMock()
        mock_client.table.return_value.select.return_value.eq.return_value.order.return_value.execute.return_value = MagicMock(
            data=[{"module_number": "7", "results": {"x": 1}}]
        )
        mock_sb.return_value = mock_client
        from api.routers.reports import _fetch_module_results
        result = _fetch_module_results("r")
        self.assertIn(7, result)

    def test_health_router_module_docstring(self):
        import api.routers.health as h
        self.assertIsNotNone(h.__doc__)

    def test_auth_router_module_docstring(self):
        import api.routers.auth as a
        self.assertIsNotNone(a.__doc__)

    def test_reports_router_module_docstring(self):
        import api.routers.reports as r
        self.assertIsNotNone(r.__doc__)


if __name__ == "__main__":
    unittest.main()
