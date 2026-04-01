"""
Comprehensive test suite for api/services/email_delivery.py

Tests all functions:
- _get_config (env-based configuration)
- _build_html_body (branded HTML email template)
- _build_plain_body (plain-text email template)
- _esc (HTML entity escaping)
- _send_via_smtp (SMTP provider)
- _send_via_sendgrid (SendGrid provider)
- _send_sendgrid_urllib (urllib fallback for SendGrid)
- _send_via_ses (AWS SES provider)
- send_report_email (public API — provider dispatch)
- check_email_config (config verification)
"""

import asyncio
import base64
import json
import os
import smtplib
import unittest
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from typing import Any, Dict
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock


# ---------------------------------------------------------------------------
# We import the module under test.  Because the file lives inside the
# ``api.services`` package we need to handle the import carefully in a
# standalone test runner context.
# ---------------------------------------------------------------------------
import importlib, sys, types

# Ensure the api and api.services packages exist on sys.modules so the
# relative-style import inside email_delivery resolves.
for pkg in ("api", "api.services"):
    if pkg not in sys.modules:
        sys.modules[pkg] = types.ModuleType(pkg)

# Now import the module directly from its file path.
import importlib.util

_spec = importlib.util.spec_from_file_location(
    "api.services.email_delivery",
    os.path.join(os.path.dirname(__file__), "..", "email_delivery.py"),
)
email_delivery = importlib.util.module_from_spec(_spec)
sys.modules["api.services.email_delivery"] = email_delivery
_spec.loader.exec_module(email_delivery)

# Convenience aliases
_get_config = email_delivery._get_config
_build_html_body = email_delivery._build_html_body
_build_plain_body = email_delivery._build_plain_body
_esc = email_delivery._esc
_send_via_smtp = email_delivery._send_via_smtp
_send_via_sendgrid = email_delivery._send_via_sendgrid
_send_sendgrid_urllib = email_delivery._send_sendgrid_urllib
_send_via_ses = email_delivery._send_via_ses
send_report_email = email_delivery.send_report_email
check_email_config = email_delivery.check_email_config


def _run(coro):
    """Helper to run an async coroutine synchronously in tests."""
    return asyncio.get_event_loop().run_until_complete(coro)


# ===================================================================
# 1. _get_config
# ===================================================================


class TestGetConfig(unittest.TestCase):
    """Test environment-based configuration loading."""

    @patch.dict(os.environ, {}, clear=True)
    def test_defaults(self):
        cfg = _get_config()
        self.assertEqual(cfg["provider"], "smtp")
        self.assertEqual(cfg["from_email"], "reports@clankermarketing.com")
        self.assertEqual(cfg["from_name"], "Search Intelligence")
        self.assertEqual(cfg["smtp_port"], 587)
        self.assertTrue(cfg["smtp_use_tls"])
        self.assertEqual(cfg["aws_region"], "us-east-1")

    @patch.dict(os.environ, {"EMAIL_PROVIDER": "sendgrid", "SENDGRID_API_KEY": "SG.test123"})
    def test_sendgrid_provider(self):
        cfg = _get_config()
        self.assertEqual(cfg["provider"], "sendgrid")
        self.assertEqual(cfg["sendgrid_api_key"], "SG.test123")

    @patch.dict(os.environ, {"EMAIL_PROVIDER": "SES", "AWS_ACCESS_KEY_ID": "AK", "AWS_SECRET_ACCESS_KEY": "SK"})
    def test_ses_provider_case_insensitive(self):
        cfg = _get_config()
        self.assertEqual(cfg["provider"], "ses")
        self.assertEqual(cfg["aws_access_key"], "AK")
        self.assertEqual(cfg["aws_secret_key"], "SK")

    @patch.dict(os.environ, {"SMTP_HOST": "mail.example.com", "SMTP_PORT": "465", "SMTP_USE_TLS": "false"})
    def test_smtp_custom(self):
        cfg = _get_config()
        self.assertEqual(cfg["smtp_host"], "mail.example.com")
        self.assertEqual(cfg["smtp_port"], 465)
        self.assertFalse(cfg["smtp_use_tls"])

    @patch.dict(os.environ, {"EMAIL_FROM": "noreply@example.com", "EMAIL_FROM_NAME": "My App"})
    def test_custom_sender(self):
        cfg = _get_config()
        self.assertEqual(cfg["from_email"], "noreply@example.com")
        self.assertEqual(cfg["from_name"], "My App")

    @patch.dict(os.environ, {"SMTP_USE_TLS": "TRUE"})
    def test_tls_true_uppercase(self):
        cfg = _get_config()
        self.assertTrue(cfg["smtp_use_tls"])

    @patch.dict(os.environ, {"SMTP_USE_TLS": "False"})
    def test_tls_false_mixed_case(self):
        cfg = _get_config()
        self.assertFalse(cfg["smtp_use_tls"])

    @patch.dict(os.environ, {"SMTP_USERNAME": "user", "SMTP_PASSWORD": "pass"})
    def test_smtp_credentials(self):
        cfg = _get_config()
        self.assertEqual(cfg["smtp_username"], "user")
        self.assertEqual(cfg["smtp_password"], "pass")


# ===================================================================
# 2. _esc (HTML escaping)
# ===================================================================


class TestEsc(unittest.TestCase):
    """Test HTML entity escaping."""

    def test_ampersand(self):
        self.assertEqual(_esc("A&B"), "A&amp;B")

    def test_less_than(self):
        self.assertEqual(_esc("<script>"), "&lt;script&gt;")

    def test_greater_than(self):
        self.assertEqual(_esc("a>b"), "a&gt;b")

    def test_double_quote(self):
        self.assertEqual(_esc('say "hi"'), "say &quot;hi&quot;")

    def test_no_escape_needed(self):
        self.assertEqual(_esc("hello world"), "hello world")

    def test_all_entities(self):
        self.assertEqual(_esc('&<>"'), "&amp;&lt;&gt;&quot;")

    def test_non_string_input(self):
        self.assertEqual(_esc(123), "123")

    def test_empty_string(self):
        self.assertEqual(_esc(""), "")

    def test_unicode(self):
        self.assertEqual(_esc("日本語"), "日本語")

    def test_already_escaped(self):
        # Double escaping should still work correctly
        self.assertEqual(_esc("&amp;"), "&amp;amp;")


# ===================================================================
# 3. _build_html_body
# ===================================================================


class TestBuildHtmlBody(unittest.TestCase):
    """Test branded HTML email body generation."""

    def test_contains_domain(self):
        html = _build_html_body({"domain": "example.com"})
        self.assertIn("example.com", html)

    def test_contains_report_id(self):
        html = _build_html_body({"domain": "x.com", "id": "RPT-42"})
        self.assertIn("RPT-42", html)

    def test_contains_date_from_created_at(self):
        html = _build_html_body({"domain": "x.com", "created_at": "2026-03-15T12:00:00Z"})
        self.assertIn("2026-03-15", html)

    def test_default_date_when_no_created_at(self):
        html = _build_html_body({"domain": "x.com"})
        today = datetime.utcnow().strftime("%Y-%m-%d")
        self.assertIn(today, html)

    def test_contains_cta_link(self):
        html = _build_html_body({"domain": "x.com"})
        self.assertIn("clankermarketing.com/contact", html)

    def test_contains_branding(self):
        html = _build_html_body({"domain": "x.com"})
        self.assertIn("Clanker Marketing", html)

    def test_html_escapes_domain(self):
        html = _build_html_body({"domain": '<script>alert("xss")</script>'})
        self.assertNotIn("<script>", html)
        self.assertIn("&lt;script&gt;", html)

    def test_is_valid_html(self):
        html = _build_html_body({"domain": "test.com"})
        self.assertIn("<!DOCTYPE html>", html)
        self.assertIn("</html>", html)

    def test_contains_all_modules_mention(self):
        html = _build_html_body({"domain": "test.com"})
        self.assertIn("12", html)
        self.assertIn("module", html.lower())

    def test_default_domain(self):
        html = _build_html_body({})
        self.assertIn("your website", html)

    def test_short_created_at(self):
        html = _build_html_body({"domain": "x.com", "created_at": "short"})
        today = datetime.utcnow().strftime("%Y-%m-%d")
        self.assertIn(today, html)

    def test_numeric_report_id(self):
        html = _build_html_body({"domain": "x.com", "id": 999})
        self.assertIn("999", html)


# ===================================================================
# 4. _build_plain_body
# ===================================================================


class TestBuildPlainBody(unittest.TestCase):
    """Test plain-text email body generation."""

    def test_contains_domain(self):
        text = _build_plain_body({"domain": "example.com"})
        self.assertIn("example.com", text)

    def test_contains_date(self):
        text = _build_plain_body({"domain": "x.com", "created_at": "2026-01-20T00:00:00Z"})
        self.assertIn("2026-01-20", text)

    def test_default_date(self):
        text = _build_plain_body({"domain": "x.com"})
        today = datetime.utcnow().strftime("%Y-%m-%d")
        self.assertIn(today, text)

    def test_contains_cta(self):
        text = _build_plain_body({"domain": "x.com"})
        self.assertIn("clankermarketing.com/contact", text)

    def test_contains_branding(self):
        text = _build_plain_body({"domain": "x.com"})
        self.assertIn("Clanker Marketing", text)

    def test_default_domain(self):
        text = _build_plain_body({})
        self.assertIn("your website", text)

    def test_mentions_12_modules(self):
        text = _build_plain_body({"domain": "x.com"})
        self.assertIn("12 modules", text)

    def test_no_html_tags(self):
        text = _build_plain_body({"domain": "x.com"})
        self.assertNotIn("<", text)


# ===================================================================
# 5. _send_via_smtp
# ===================================================================


class TestSendViaSmtp(unittest.TestCase):
    """Test SMTP email sending."""

    def _base_config(self, **overrides):
        cfg = {
            "from_email": "test@example.com",
            "from_name": "Test",
            "smtp_host": "smtp.example.com",
            "smtp_port": 587,
            "smtp_username": "user",
            "smtp_password": "pass",
            "smtp_use_tls": True,
        }
        cfg.update(overrides)
        return cfg

    @patch("smtplib.SMTP")
    def test_success_with_tls(self, mock_smtp_cls):
        mock_server = MagicMock()
        mock_smtp_cls.return_value = mock_server
        cfg = self._base_config()
        result = _run(_send_via_smtp(cfg, "to@ex.com", "Subj", "<h1>Hi</h1>", "Hi", b"%PDF", "r.pdf"))
        self.assertTrue(result["success"])
        self.assertEqual(result["provider"], "smtp")
        mock_server.starttls.assert_called_once()
        mock_server.login.assert_called_once_with("user", "pass")
        mock_server.sendmail.assert_called_once()
        mock_server.quit.assert_called_once()

    @patch("smtplib.SMTP")
    def test_success_without_tls(self, mock_smtp_cls):
        mock_server = MagicMock()
        mock_smtp_cls.return_value = mock_server
        cfg = self._base_config(smtp_use_tls=False)
        result = _run(_send_via_smtp(cfg, "to@ex.com", "Subj", "<h1>Hi</h1>", "Hi"))
        self.assertTrue(result["success"])
        mock_server.starttls.assert_not_called()

    @patch("smtplib.SMTP")
    def test_no_auth_when_username_empty(self, mock_smtp_cls):
        mock_server = MagicMock()
        mock_smtp_cls.return_value = mock_server
        cfg = self._base_config(smtp_username="", smtp_password="")
        result = _run(_send_via_smtp(cfg, "to@ex.com", "Subj", "<h1>Hi</h1>", "Hi"))
        self.assertTrue(result["success"])
        mock_server.login.assert_not_called()

    def test_missing_smtp_host(self):
        cfg = self._base_config(smtp_host="")
        result = _run(_send_via_smtp(cfg, "to@ex.com", "Subj", "html", "txt"))
        self.assertFalse(result["success"])
        self.assertIn("not configured", result["error"])

    @patch("smtplib.SMTP")
    def test_smtp_exception(self, mock_smtp_cls):
        mock_smtp_cls.side_effect = smtplib.SMTPException("Connection refused")
        cfg = self._base_config()
        result = _run(_send_via_smtp(cfg, "to@ex.com", "Subj", "html", "txt"))
        self.assertFalse(result["success"])
        self.assertIn("Connection refused", result["error"])

    @patch("smtplib.SMTP")
    def test_no_pdf_attachment(self, mock_smtp_cls):
        mock_server = MagicMock()
        mock_smtp_cls.return_value = mock_server
        cfg = self._base_config()
        result = _run(_send_via_smtp(cfg, "to@ex.com", "Subj", "html", "txt", None, None))
        self.assertTrue(result["success"])
        # sendmail should still be called
        mock_server.sendmail.assert_called_once()

    @patch("smtplib.SMTP")
    def test_sendmail_args(self, mock_smtp_cls):
        mock_server = MagicMock()
        mock_smtp_cls.return_value = mock_server
        cfg = self._base_config()
        _run(_send_via_smtp(cfg, "recipient@test.com", "Test Subject", "html", "txt"))
        args = mock_server.sendmail.call_args
        self.assertEqual(args[0][0], "test@example.com")
        self.assertEqual(args[0][1], ["recipient@test.com"])


# ===================================================================
# 6. _send_via_sendgrid
# ===================================================================


class TestSendViaSendgrid(unittest.TestCase):
    """Test SendGrid email sending."""

    def _base_config(self, **overrides):
        cfg = {
            "from_email": "test@example.com",
            "from_name": "Test",
            "sendgrid_api_key": "SG.testkey123",
        }
        cfg.update(overrides)
        return cfg

    @patch("httpx.AsyncClient")
    def test_success(self, mock_client_cls):
        mock_resp = MagicMock()
        mock_resp.status_code = 202
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_resp)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client_cls.return_value = mock_client
        cfg = self._base_config()
        result = _run(_send_via_sendgrid(cfg, "to@ex.com", "Subj", "html", "txt"))
        self.assertTrue(result["success"])
        self.assertEqual(result["provider"], "sendgrid")
        self.assertEqual(result["status_code"], 202)

    @patch("httpx.AsyncClient")
    def test_with_pdf_attachment(self, mock_client_cls):
        mock_resp = MagicMock()
        mock_resp.status_code = 202
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_resp)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client_cls.return_value = mock_client
        cfg = self._base_config()
        result = _run(_send_via_sendgrid(cfg, "to@ex.com", "Subj", "html", "txt", b"%PDF-data", "report.pdf"))
        self.assertTrue(result["success"])
        # Verify the payload included attachments
        call_args = mock_client.post.call_args
        payload = call_args[1]["json"]
        self.assertIn("attachments", payload)
        self.assertEqual(payload["attachments"][0]["filename"], "report.pdf")

    @patch("httpx.AsyncClient")
    def test_error_status(self, mock_client_cls):
        mock_resp = MagicMock()
        mock_resp.status_code = 400
        mock_resp.text = "Bad Request"
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_resp)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client_cls.return_value = mock_client
        cfg = self._base_config()
        result = _run(_send_via_sendgrid(cfg, "to@ex.com", "Subj", "html", "txt"))
        self.assertFalse(result["success"])
        self.assertEqual(result["status_code"], 400)

    def test_missing_api_key(self):
        cfg = self._base_config(sendgrid_api_key="")
        result = _run(_send_via_sendgrid(cfg, "to@ex.com", "Subj", "html", "txt"))
        self.assertFalse(result["success"])
        self.assertIn("not configured", result["error"])

    @patch("httpx.AsyncClient")
    def test_exception_handling(self, mock_client_cls):
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(side_effect=Exception("Network error"))
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client_cls.return_value = mock_client
        cfg = self._base_config()
        result = _run(_send_via_sendgrid(cfg, "to@ex.com", "Subj", "html", "txt"))
        self.assertFalse(result["success"])
        self.assertIn("Network error", result["error"])

    @patch("httpx.AsyncClient")
    def test_status_200_success(self, mock_client_cls):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_resp)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client_cls.return_value = mock_client
        cfg = self._base_config()
        result = _run(_send_via_sendgrid(cfg, "to@ex.com", "Subj", "html", "txt"))
        self.assertTrue(result["success"])

    @patch("httpx.AsyncClient")
    def test_status_201_success(self, mock_client_cls):
        mock_resp = MagicMock()
        mock_resp.status_code = 201
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_resp)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client_cls.return_value = mock_client
        cfg = self._base_config()
        result = _run(_send_via_sendgrid(cfg, "to@ex.com", "Subj", "html", "txt"))
        self.assertTrue(result["success"])


# ===================================================================
# 7. _send_sendgrid_urllib
# ===================================================================


class TestSendSendgridUrllib(unittest.TestCase):
    """Test SendGrid urllib fallback."""

    def _base_config(self, **overrides):
        cfg = {
            "from_email": "test@example.com",
            "from_name": "Test",
            "sendgrid_api_key": "SG.testkey123",
        }
        cfg.update(overrides)
        return cfg

    @patch("urllib.request.urlopen")
    def test_success(self, mock_urlopen):
        mock_resp = MagicMock()
        mock_resp.status = 202
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp
        cfg = self._base_config()
        result = _run(_send_sendgrid_urllib(cfg, "to@ex.com", "Subj", "html", "txt"))
        self.assertTrue(result["success"])
        self.assertEqual(result["provider"], "sendgrid")

    @patch("urllib.request.urlopen")
    def test_with_pdf(self, mock_urlopen):
        mock_resp = MagicMock()
        mock_resp.status = 202
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp
        cfg = self._base_config()
        result = _run(_send_sendgrid_urllib(cfg, "to@ex.com", "Subj", "html", "txt", b"%PDF", "r.pdf"))
        self.assertTrue(result["success"])

    @patch("urllib.request.urlopen")
    def test_non_success_status(self, mock_urlopen):
        mock_resp = MagicMock()
        mock_resp.status = 500
        mock_resp.read.return_value = b"Internal Server Error"
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp
        cfg = self._base_config()
        result = _run(_send_sendgrid_urllib(cfg, "to@ex.com", "Subj", "html", "txt"))
        self.assertFalse(result["success"])
        self.assertEqual(result["status_code"], 500)

    @patch("urllib.request.urlopen")
    def test_exception(self, mock_urlopen):
        mock_urlopen.side_effect = Exception("Timeout")
        cfg = self._base_config()
        result = _run(_send_sendgrid_urllib(cfg, "to@ex.com", "Subj", "html", "txt"))
        self.assertFalse(result["success"])
        self.assertIn("Timeout", result["error"])


# ===================================================================
# 8. _send_via_ses
# ===================================================================


class TestSendViaSes(unittest.TestCase):
    """Test AWS SES email sending."""

    def _base_config(self, **overrides):
        cfg = {
            "from_email": "test@example.com",
            "from_name": "Test",
            "aws_access_key": "AKIATEST",
            "aws_secret_key": "secret123",
            "aws_region": "us-east-1",
        }
        cfg.update(overrides)
        return cfg

    @patch("boto3.client")
    def test_success(self, mock_boto3_client):
        mock_ses = MagicMock()
        mock_ses.send_raw_email.return_value = {"MessageId": "msg-123"}
        mock_boto3_client.return_value = mock_ses
        cfg = self._base_config()
        result = _run(_send_via_ses(cfg, "to@ex.com", "Subj", "html", "txt", b"%PDF", "r.pdf"))
        self.assertTrue(result["success"])
        self.assertEqual(result["provider"], "ses")
        self.assertEqual(result["message_id"], "msg-123")

    @patch("boto3.client")
    def test_no_pdf(self, mock_boto3_client):
        mock_ses = MagicMock()
        mock_ses.send_raw_email.return_value = {"MessageId": "msg-456"}
        mock_boto3_client.return_value = mock_ses
        cfg = self._base_config()
        result = _run(_send_via_ses(cfg, "to@ex.com", "Subj", "html", "txt"))
        self.assertTrue(result["success"])

    def test_missing_credentials(self):
        cfg = self._base_config(aws_access_key="", aws_secret_key="")
        result = _run(_send_via_ses(cfg, "to@ex.com", "Subj", "html", "txt"))
        self.assertFalse(result["success"])
        self.assertIn("not configured", result["error"])

    def test_missing_secret_key(self):
        cfg = self._base_config(aws_secret_key="")
        result = _run(_send_via_ses(cfg, "to@ex.com", "Subj", "html", "txt"))
        self.assertFalse(result["success"])

    @patch("boto3.client")
    def test_ses_exception(self, mock_boto3_client):
        mock_ses = MagicMock()
        mock_ses.send_raw_email.side_effect = Exception("SES rate limit")
        mock_boto3_client.return_value = mock_ses
        cfg = self._base_config()
        result = _run(_send_via_ses(cfg, "to@ex.com", "Subj", "html", "txt"))
        self.assertFalse(result["success"])
        self.assertIn("SES rate limit", result["error"])

    @patch("boto3.client")
    def test_region_passed(self, mock_boto3_client):
        mock_ses = MagicMock()
        mock_ses.send_raw_email.return_value = {"MessageId": "x"}
        mock_boto3_client.return_value = mock_ses
        cfg = self._base_config(aws_region="eu-west-1")
        _run(_send_via_ses(cfg, "to@ex.com", "Subj", "html", "txt"))
        mock_boto3_client.assert_called_once_with(
            "ses",
            region_name="eu-west-1",
            aws_access_key_id="AKIATEST",
            aws_secret_access_key="secret123",
        )

    @patch("boto3.client")
    def test_no_message_id_in_response(self, mock_boto3_client):
        mock_ses = MagicMock()
        mock_ses.send_raw_email.return_value = {}
        mock_boto3_client.return_value = mock_ses
        cfg = self._base_config()
        result = _run(_send_via_ses(cfg, "to@ex.com", "Subj", "html", "txt"))
        self.assertTrue(result["success"])
        self.assertEqual(result["message_id"], "")


# ===================================================================
# 9. send_report_email (public API)
# ===================================================================


class TestSendReportEmail(unittest.TestCase):
    """Test the main public API dispatcher."""

    @patch.dict(os.environ, {"EMAIL_PROVIDER": "smtp", "SMTP_HOST": "mail.test.com"})
    @patch("smtplib.SMTP")
    def test_dispatches_to_smtp(self, mock_smtp_cls):
        mock_server = MagicMock()
        mock_smtp_cls.return_value = mock_server
        report = {"domain": "test.com", "id": 1}
        result = _run(send_report_email("user@ex.com", report))
        self.assertTrue(result["success"])
        self.assertEqual(result["to_email"], "user@ex.com")
        self.assertEqual(result["domain"], "test.com")
        self.assertEqual(result["report_id"], 1)

    @patch.dict(os.environ, {"EMAIL_PROVIDER": "sendgrid", "SENDGRID_API_KEY": "SG.key"})
    @patch("httpx.AsyncClient")
    def test_dispatches_to_sendgrid(self, mock_client_cls):
        mock_resp = MagicMock()
        mock_resp.status_code = 202
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_resp)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client_cls.return_value = mock_client
        report = {"domain": "test.com", "id": 2}
        result = _run(send_report_email("user@ex.com", report))
        self.assertTrue(result["success"])
        self.assertEqual(result["provider"], "sendgrid")

    @patch.dict(os.environ, {"EMAIL_PROVIDER": "ses", "AWS_ACCESS_KEY_ID": "AK", "AWS_SECRET_ACCESS_KEY": "SK"})
    @patch("boto3.client")
    def test_dispatches_to_ses(self, mock_boto3_client):
        mock_ses = MagicMock()
        mock_ses.send_raw_email.return_value = {"MessageId": "msg-1"}
        mock_boto3_client.return_value = mock_ses
        report = {"domain": "test.com", "id": 3}
        result = _run(send_report_email("user@ex.com", report))
        self.assertTrue(result["success"])
        self.assertEqual(result["provider"], "ses")

    @patch.dict(os.environ, {"EMAIL_PROVIDER": "smtp", "SMTP_HOST": "mail.test.com"})
    @patch("smtplib.SMTP")
    def test_auto_generates_subject(self, mock_smtp_cls):
        mock_server = MagicMock()
        mock_smtp_cls.return_value = mock_server
        report = {"domain": "example.org", "id": 1}
        result = _run(send_report_email("user@ex.com", report))
        self.assertTrue(result["success"])

    @patch.dict(os.environ, {"EMAIL_PROVIDER": "smtp", "SMTP_HOST": "mail.test.com"})
    @patch("smtplib.SMTP")
    def test_custom_subject(self, mock_smtp_cls):
        mock_server = MagicMock()
        mock_smtp_cls.return_value = mock_server
        report = {"domain": "x.com", "id": 1}
        result = _run(send_report_email("user@ex.com", report, subject="Custom Subject"))
        self.assertTrue(result["success"])

    @patch.dict(os.environ, {"EMAIL_PROVIDER": "smtp", "SMTP_HOST": "mail.test.com"})
    @patch("smtplib.SMTP")
    def test_auto_generates_pdf_filename(self, mock_smtp_cls):
        mock_server = MagicMock()
        mock_smtp_cls.return_value = mock_server
        report = {"domain": "hello.world", "id": 1}
        result = _run(send_report_email("user@ex.com", report, pdf_bytes=b"%PDF-test"))
        self.assertTrue(result["success"])

    @patch.dict(os.environ, {"EMAIL_PROVIDER": "smtp", "SMTP_HOST": "mail.test.com"})
    @patch("smtplib.SMTP")
    def test_custom_pdf_filename(self, mock_smtp_cls):
        mock_server = MagicMock()
        mock_smtp_cls.return_value = mock_server
        report = {"domain": "x.com", "id": 1}
        result = _run(send_report_email("user@ex.com", report, pdf_bytes=b"data", pdf_filename="custom.pdf"))
        self.assertTrue(result["success"])

    @patch.dict(os.environ, {"EMAIL_PROVIDER": "smtp", "SMTP_HOST": "mail.test.com"})
    @patch("smtplib.SMTP")
    def test_result_enriched_with_metadata(self, mock_smtp_cls):
        mock_server = MagicMock()
        mock_smtp_cls.return_value = mock_server
        report = {"domain": "enriched.com", "id": 99}
        result = _run(send_report_email("user@ex.com", report))
        self.assertEqual(result["to_email"], "user@ex.com")
        self.assertEqual(result["report_id"], 99)
        self.assertEqual(result["domain"], "enriched.com")

    @patch.dict(os.environ, {"EMAIL_PROVIDER": "smtp", "SMTP_HOST": "mail.test.com"})
    @patch("smtplib.SMTP")
    def test_default_domain_in_report(self, mock_smtp_cls):
        mock_server = MagicMock()
        mock_smtp_cls.return_value = mock_server
        report = {"id": 1}  # No domain key
        result = _run(send_report_email("user@ex.com", report))
        self.assertTrue(result["success"])
        self.assertEqual(result["domain"], "report")


# ===================================================================
# 10. check_email_config
# ===================================================================


class TestCheckEmailConfig(unittest.TestCase):
    """Test email configuration verification."""

    @patch.dict(os.environ, {"EMAIL_PROVIDER": "smtp", "SMTP_HOST": "mail.example.com"})
    def test_smtp_configured(self):
        result = _run(check_email_config())
        self.assertTrue(result["configured"])
        self.assertEqual(result["provider"], "smtp")

    @patch.dict(os.environ, {"EMAIL_PROVIDER": "smtp"}, clear=True)
    def test_smtp_not_configured(self):
        result = _run(check_email_config())
        self.assertFalse(result["configured"])

    @patch.dict(os.environ, {"EMAIL_PROVIDER": "sendgrid", "SENDGRID_API_KEY": "SG.test"})
    def test_sendgrid_configured(self):
        result = _run(check_email_config())
        self.assertTrue(result["configured"])
        self.assertEqual(result["provider"], "sendgrid")

    @patch.dict(os.environ, {"EMAIL_PROVIDER": "sendgrid"}, clear=True)
    def test_sendgrid_not_configured(self):
        result = _run(check_email_config())
        self.assertFalse(result["configured"])

    @patch.dict(os.environ, {"EMAIL_PROVIDER": "ses", "AWS_ACCESS_KEY_ID": "AK", "AWS_SECRET_ACCESS_KEY": "SK"})
    def test_ses_configured(self):
        result = _run(check_email_config())
        self.assertTrue(result["configured"])
        self.assertEqual(result["provider"], "ses")

    @patch.dict(os.environ, {"EMAIL_PROVIDER": "ses", "AWS_ACCESS_KEY_ID": "AK"}, clear=True)
    def test_ses_missing_secret(self):
        result = _run(check_email_config())
        self.assertFalse(result["configured"])

    @patch.dict(os.environ, {"EMAIL_PROVIDER": "smtp", "SMTP_HOST": "x.com", "EMAIL_FROM": "me@x.com"})
    def test_from_email_returned(self):
        result = _run(check_email_config())
        self.assertEqual(result["from_email"], "me@x.com")


# ===================================================================
# 11. Edge Cases
# ===================================================================


class TestEdgeCases(unittest.TestCase):
    """Miscellaneous edge-case tests."""

    def test_esc_with_none(self):
        result = _esc(None)
        self.assertEqual(result, "None")

    def test_html_body_with_empty_dict(self):
        html = _build_html_body({})
        self.assertIn("your website", html)
        self.assertIn("<!DOCTYPE html>", html)

    def test_plain_body_with_empty_dict(self):
        text = _build_plain_body({})
        self.assertIn("your website", text)

    def test_html_body_unicode_domain(self):
        html = _build_html_body({"domain": "日本語.jp"})
        self.assertIn("日本語.jp", html)

    def test_plain_body_unicode_domain(self):
        text = _build_plain_body({"domain": "日本語.jp"})
        self.assertIn("日本語.jp", text)

    @patch.dict(os.environ, {"EMAIL_PROVIDER": "smtp"}, clear=True)
    def test_send_report_with_no_smtp_host(self):
        report = {"domain": "fail.com", "id": 1}
        result = _run(send_report_email("user@ex.com", report))
        self.assertFalse(result["success"])

    def test_html_body_xss_in_report_id(self):
        html = _build_html_body({"domain": "x.com", "id": '<img src=x onerror="alert(1)">'})
        self.assertNotIn("onerror", html)

    @patch.dict(os.environ, {"EMAIL_PROVIDER": "smtp", "SMTP_HOST": "mail.test.com"})
    @patch("smtplib.SMTP")
    def test_very_large_pdf(self, mock_smtp_cls):
        mock_server = MagicMock()
        mock_smtp_cls.return_value = mock_server
        report = {"domain": "big.com", "id": 1}
        big_pdf = b"X" * (10 * 1024 * 1024)  # 10MB
        result = _run(send_report_email("user@ex.com", report, pdf_bytes=big_pdf))
        self.assertTrue(result["success"])

    @patch.dict(os.environ, {"SMTP_PORT": "notanumber"})
    def test_invalid_port_raises(self):
        with self.assertRaises(ValueError):
            _get_config()

    def test_build_html_body_integer_created_at(self):
        # created_at is an int — not a string, should fall back to today
        html = _build_html_body({"domain": "x.com", "created_at": 12345})
        today = datetime.utcnow().strftime("%Y-%m-%d")
        self.assertIn(today, html)


if __name__ == "__main__":
    unittest.main()
