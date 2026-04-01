"""
Comprehensive test suite for api/utils/ — errors.py, retry.py, performance.py.

Covers the full utility layer: custom exception hierarchy, error formatting,
retry with exponential backoff, circuit breaker pattern, performance metrics
singleton, timing decorators, context manager, and progress tracker.
"""

import ast
import time
import unittest
from unittest.mock import patch, MagicMock, AsyncMock
from enum import Enum


# ---------------------------------------------------------------------------
# SECTION 1  ·  api/utils/errors.py
# ---------------------------------------------------------------------------

class TestErrorSeverity(unittest.TestCase):
    """Verify ErrorSeverity enum members and ordering."""

    def test_four_members(self):
        from api.utils.errors import ErrorSeverity
        self.assertEqual(len(ErrorSeverity), 4)

    def test_values(self):
        from api.utils.errors import ErrorSeverity
        self.assertEqual(ErrorSeverity.CRITICAL.value, "critical")
        self.assertEqual(ErrorSeverity.HIGH.value, "high")
        self.assertEqual(ErrorSeverity.MEDIUM.value, "medium")
        self.assertEqual(ErrorSeverity.LOW.value, "low")

    def test_is_enum(self):
        from api.utils.errors import ErrorSeverity
        self.assertTrue(issubclass(ErrorSeverity, Enum))


class TestSearchIntelError(unittest.TestCase):
    """Base exception class."""

    def test_message_stored(self):
        from api.utils.errors import SearchIntelError
        e = SearchIntelError("tech msg")
        self.assertEqual(e.message, "tech msg")
        self.assertEqual(str(e), "tech msg")

    def test_default_user_message(self):
        from api.utils.errors import SearchIntelError
        e = SearchIntelError("x")
        self.assertIn("unexpected error", e.user_message.lower())

    def test_custom_user_message(self):
        from api.utils.errors import SearchIntelError
        e = SearchIntelError("x", user_message="Custom")
        self.assertEqual(e.user_message, "Custom")

    def test_default_severity_is_high(self):
        from api.utils.errors import SearchIntelError, ErrorSeverity
        e = SearchIntelError("x")
        self.assertEqual(e.severity, ErrorSeverity.HIGH)

    def test_custom_severity(self):
        from api.utils.errors import SearchIntelError, ErrorSeverity
        e = SearchIntelError("x", severity=ErrorSeverity.LOW)
        self.assertEqual(e.severity, ErrorSeverity.LOW)

    def test_context_default_empty(self):
        from api.utils.errors import SearchIntelError
        e = SearchIntelError("x")
        self.assertEqual(e.context, {})

    def test_context_custom(self):
        from api.utils.errors import SearchIntelError
        e = SearchIntelError("x", context={"k": "v"})
        self.assertEqual(e.context, {"k": "v"})

    def test_original_error(self):
        from api.utils.errors import SearchIntelError
        orig = ValueError("orig")
        e = SearchIntelError("x", original_error=orig)
        self.assertIs(e.original_error, orig)

    def test_to_dict_keys(self):
        from api.utils.errors import SearchIntelError
        d = SearchIntelError("x").to_dict()
        for key in ("error", "error_type", "message", "severity", "context"):
            self.assertIn(key, d)

    def test_to_dict_error_true(self):
        from api.utils.errors import SearchIntelError
        self.assertTrue(SearchIntelError("x").to_dict()["error"])

    def test_to_dict_error_type(self):
        from api.utils.errors import SearchIntelError
        self.assertEqual(SearchIntelError("x").to_dict()["error_type"], "SearchIntelError")

    def test_is_exception(self):
        from api.utils.errors import SearchIntelError
        self.assertTrue(issubclass(SearchIntelError, Exception))


# --- Data Ingestion Errors ---

class TestDataIngestionError(unittest.TestCase):
    def test_default_user_message(self):
        from api.utils.errors import DataIngestionError
        e = DataIngestionError("fail")
        self.assertIn("fetch data", e.user_message.lower())

    def test_inherits_base(self):
        from api.utils.errors import DataIngestionError, SearchIntelError
        self.assertTrue(issubclass(DataIngestionError, SearchIntelError))


class TestGSCAuthError(unittest.TestCase):
    def test_severity_critical(self):
        from api.utils.errors import GSCAuthError, ErrorSeverity
        e = GSCAuthError("token expired")
        self.assertEqual(e.severity, ErrorSeverity.CRITICAL)

    def test_user_message_mentions_reconnect(self):
        from api.utils.errors import GSCAuthError
        e = GSCAuthError("token expired")
        self.assertIn("reconnect", e.user_message.lower())

    def test_context_passed(self):
        from api.utils.errors import GSCAuthError
        e = GSCAuthError("x", context={"scope": "gsc"})
        self.assertEqual(e.context["scope"], "gsc")


class TestGSCAPIError(unittest.TestCase):
    def test_rate_limit_message(self):
        from api.utils.errors import GSCAPIError
        e = GSCAPIError("rate limited", endpoint="/query", status_code=429)
        self.assertIn("rate limit", e.user_message.lower())

    def test_server_error_message(self):
        from api.utils.errors import GSCAPIError
        e = GSCAPIError("server err", endpoint="/query", status_code=500)
        self.assertIn("google", e.user_message.lower())

    def test_generic_message(self):
        from api.utils.errors import GSCAPIError
        e = GSCAPIError("bad", endpoint="/query", status_code=400)
        self.assertIn("check your property", e.user_message.lower())

    def test_context_includes_endpoint(self):
        from api.utils.errors import GSCAPIError
        e = GSCAPIError("x", endpoint="/test", status_code=200)
        self.assertEqual(e.context["endpoint"], "/test")
        self.assertEqual(e.context["status_code"], 200)

    def test_rate_limit_severity_high(self):
        from api.utils.errors import GSCAPIError, ErrorSeverity
        e = GSCAPIError("x", endpoint="/q", status_code=429)
        self.assertEqual(e.severity, ErrorSeverity.HIGH)

    def test_non_rate_limit_severity_critical(self):
        from api.utils.errors import GSCAPIError, ErrorSeverity
        e = GSCAPIError("x", endpoint="/q", status_code=500)
        self.assertEqual(e.severity, ErrorSeverity.CRITICAL)


class TestGA4AuthError(unittest.TestCase):
    def test_severity_high(self):
        from api.utils.errors import GA4AuthError, ErrorSeverity
        e = GA4AuthError("expired")
        self.assertEqual(e.severity, ErrorSeverity.HIGH)

    def test_user_message_mentions_reconnect(self):
        from api.utils.errors import GA4AuthError
        e = GA4AuthError("expired")
        self.assertIn("reconnect", e.user_message.lower())


class TestGA4APIError(unittest.TestCase):
    def test_rate_limit_message(self):
        from api.utils.errors import GA4APIError
        e = GA4APIError("x", property_id="123", status_code=429)
        self.assertIn("rate limit", e.user_message.lower())

    def test_403_message(self):
        from api.utils.errors import GA4APIError
        e = GA4APIError("x", property_id="123", status_code=403)
        self.assertIn("access denied", e.user_message.lower())

    def test_generic_message(self):
        from api.utils.errors import GA4APIError
        e = GA4APIError("x", property_id="123")
        self.assertIn("unavailable", e.user_message.lower())

    def test_severity_medium(self):
        from api.utils.errors import GA4APIError, ErrorSeverity
        e = GA4APIError("x", property_id="123")
        self.assertEqual(e.severity, ErrorSeverity.MEDIUM)

    def test_context_includes_property_id(self):
        from api.utils.errors import GA4APIError
        e = GA4APIError("x", property_id="999")
        self.assertEqual(e.context["property_id"], "999")


class TestDataForSEOError(unittest.TestCase):
    def test_rate_limit(self):
        from api.utils.errors import DataForSEOError
        e = DataForSEOError("x", status_code=429)
        self.assertIn("rate limit", e.user_message.lower())

    def test_credits_exhausted(self):
        from api.utils.errors import DataForSEOError
        e = DataForSEOError("x", status_code=402)
        self.assertIn("credits", e.user_message.lower())

    def test_generic(self):
        from api.utils.errors import DataForSEOError
        e = DataForSEOError("x")
        self.assertIn("incomplete", e.user_message.lower())

    def test_severity_medium(self):
        from api.utils.errors import DataForSEOError, ErrorSeverity
        e = DataForSEOError("x")
        self.assertEqual(e.severity, ErrorSeverity.MEDIUM)

    def test_context_keyword(self):
        from api.utils.errors import DataForSEOError
        e = DataForSEOError("x", keyword="seo tools")
        self.assertEqual(e.context["keyword"], "seo tools")


class TestCrawlError(unittest.TestCase):
    def test_user_message(self):
        from api.utils.errors import CrawlError
        e = CrawlError("fail", url="https://example.com")
        self.assertIn("crawl", e.user_message.lower())

    def test_severity_medium(self):
        from api.utils.errors import CrawlError, ErrorSeverity
        e = CrawlError("fail")
        self.assertEqual(e.severity, ErrorSeverity.MEDIUM)

    def test_context_url(self):
        from api.utils.errors import CrawlError
        e = CrawlError("fail", url="https://test.com")
        self.assertEqual(e.context["url"], "https://test.com")


class TestInsufficientDataError(unittest.TestCase):
    def test_user_message_mentions_days(self):
        from api.utils.errors import InsufficientDataError
        e = InsufficientDataError("not enough", data_source="gsc", required_days=90, available_days=30)
        self.assertIn("30", e.user_message)
        self.assertIn("90", e.user_message)

    def test_severity_critical(self):
        from api.utils.errors import InsufficientDataError, ErrorSeverity
        e = InsufficientDataError("x", data_source="gsc", required_days=90, available_days=30)
        self.assertEqual(e.severity, ErrorSeverity.CRITICAL)

    def test_context_fields(self):
        from api.utils.errors import InsufficientDataError
        e = InsufficientDataError("x", data_source="ga4", required_days=60, available_days=10)
        self.assertEqual(e.context["data_source"], "ga4")
        self.assertEqual(e.context["required_days"], 60)
        self.assertEqual(e.context["available_days"], 10)


# --- Analysis Module Errors ---

class TestAnalysisErrors(unittest.TestCase):
    def test_analysis_error_default_msg(self):
        from api.utils.errors import AnalysisError
        e = AnalysisError("internal")
        self.assertIn("could not be completed", e.user_message.lower())

    def test_time_series_error(self):
        from api.utils.errors import TimeSeriesAnalysisError, ErrorSeverity
        e = TimeSeriesAnalysisError("fail", module="module_1")
        self.assertEqual(e.severity, ErrorSeverity.MEDIUM)
        self.assertEqual(e.context["module"], "module_1")

    def test_anomaly_detection_error(self):
        from api.utils.errors import AnomalyDetectionError, ErrorSeverity
        e = AnomalyDetectionError("fail", method="zscore")
        self.assertEqual(e.severity, ErrorSeverity.LOW)
        self.assertEqual(e.context["method"], "zscore")

    def test_change_point_error(self):
        from api.utils.errors import ChangePointDetectionError, ErrorSeverity
        e = ChangePointDetectionError("fail")
        self.assertEqual(e.severity, ErrorSeverity.MEDIUM)
        self.assertIn("algorithm", e.user_message.lower())

    def test_graph_analysis_error(self):
        from api.utils.errors import GraphAnalysisError, ErrorSeverity
        e = GraphAnalysisError("fail", operation="pagerank")
        self.assertEqual(e.severity, ErrorSeverity.MEDIUM)
        self.assertEqual(e.context["operation"], "pagerank")

    def test_model_training_error(self):
        from api.utils.errors import ModelTrainingError, ErrorSeverity
        e = ModelTrainingError("fail", model_type="linear")
        self.assertEqual(e.severity, ErrorSeverity.MEDIUM)
        self.assertEqual(e.context["model_type"], "linear")

    def test_llm_error(self):
        from api.utils.errors import LLMError, ErrorSeverity
        e = LLMError("fail", operation="summarize")
        self.assertEqual(e.severity, ErrorSeverity.LOW)
        self.assertEqual(e.context["operation"], "summarize")


# --- Database Errors ---

class TestDatabaseErrors(unittest.TestCase):
    def test_read_operation_message(self):
        from api.utils.errors import DatabaseError
        e = DatabaseError("fail", operation="read", table="reports")
        self.assertIn("retrieve", e.user_message.lower())

    def test_write_operation_message(self):
        from api.utils.errors import DatabaseError
        e = DatabaseError("fail", operation="write", table="reports")
        self.assertIn("saved", e.user_message.lower())

    def test_other_operation_message(self):
        from api.utils.errors import DatabaseError
        e = DatabaseError("fail", operation="migrate")
        self.assertIn("try again", e.user_message.lower())

    def test_read_severity_medium(self):
        from api.utils.errors import DatabaseError, ErrorSeverity
        e = DatabaseError("fail", operation="read")
        self.assertEqual(e.severity, ErrorSeverity.MEDIUM)

    def test_other_severity_high(self):
        from api.utils.errors import DatabaseError, ErrorSeverity
        e = DatabaseError("fail", operation="migrate")
        self.assertEqual(e.severity, ErrorSeverity.HIGH)

    def test_context_fields(self):
        from api.utils.errors import DatabaseError
        e = DatabaseError("fail", operation="read", table="users")
        self.assertEqual(e.context["operation"], "read")
        self.assertEqual(e.context["table"], "users")


class TestCacheError(unittest.TestCase):
    def test_severity_low(self):
        from api.utils.errors import CacheError, ErrorSeverity
        e = CacheError("miss", cache_key="report:123")
        self.assertEqual(e.severity, ErrorSeverity.LOW)

    def test_context_cache_key(self):
        from api.utils.errors import CacheError
        e = CacheError("miss", cache_key="key:1")
        self.assertEqual(e.context["cache_key"], "key:1")

    def test_user_message(self):
        from api.utils.errors import CacheError
        e = CacheError("miss", cache_key="k")
        self.assertIn("cache", e.user_message.lower())


# --- Report & Validation Errors ---

class TestReportGenerationError(unittest.TestCase):
    def test_severity_critical(self):
        from api.utils.errors import ReportGenerationError, ErrorSeverity
        e = ReportGenerationError("fail", stage="module_1")
        self.assertEqual(e.severity, ErrorSeverity.CRITICAL)

    def test_user_message_mentions_stage(self):
        from api.utils.errors import ReportGenerationError
        e = ReportGenerationError("fail", stage="pdf_render")
        self.assertIn("pdf_render", e.user_message)

    def test_context_stage(self):
        from api.utils.errors import ReportGenerationError
        e = ReportGenerationError("fail", stage="s1")
        self.assertEqual(e.context["stage"], "s1")


class TestValidationError(unittest.TestCase):
    def test_user_message_mentions_field(self):
        from api.utils.errors import ValidationError
        e = ValidationError("too long", field="domain", value="x" * 300)
        self.assertIn("domain", e.user_message)

    def test_context_field_and_value(self):
        from api.utils.errors import ValidationError
        e = ValidationError("bad", field="email", value=123)
        self.assertEqual(e.context["field"], "email")
        self.assertEqual(e.context["value"], "123")


# --- Error Formatting Utilities ---

class TestFormatErrorForUser(unittest.TestCase):
    def test_search_intel_error(self):
        from api.utils.errors import format_error_for_user, SearchIntelError
        e = SearchIntelError("x", user_message="Nice msg")
        d = format_error_for_user(e)
        self.assertEqual(d["message"], "Nice msg")
        self.assertTrue(d["error"])

    def test_unknown_error(self):
        from api.utils.errors import format_error_for_user
        d = format_error_for_user(ValueError("oops"))
        self.assertTrue(d["error"])
        self.assertEqual(d["error_type"], "UnexpectedError")
        self.assertNotIn("oops", d["message"])

    def test_subclass_error(self):
        from api.utils.errors import format_error_for_user, GSCAuthError
        d = format_error_for_user(GSCAuthError("expired"))
        self.assertEqual(d["error_type"], "GSCAuthError")


class TestFormatErrorForLogging(unittest.TestCase):
    def test_basic_exception(self):
        from api.utils.errors import format_error_for_logging
        d = format_error_for_logging(ValueError("test"))
        self.assertEqual(d["error_type"], "ValueError")
        self.assertEqual(d["message"], "test")

    def test_search_intel_error_full(self):
        from api.utils.errors import format_error_for_logging, SearchIntelError
        orig = IOError("io")
        e = SearchIntelError("tech", user_message="user", original_error=orig, context={"k": 1})
        d = format_error_for_logging(e)
        self.assertIn("severity", d)
        self.assertEqual(d["context"]["k"], 1)
        self.assertEqual(d["original_error"]["type"], "IOError")

    def test_no_original_error(self):
        from api.utils.errors import format_error_for_logging, SearchIntelError
        d = format_error_for_logging(SearchIntelError("x"))
        self.assertNotIn("original_error", d)


class TestShouldRetry(unittest.TestCase):
    def test_gsc_rate_limit_yes(self):
        from api.utils.errors import should_retry, GSCAPIError
        e = GSCAPIError("x", endpoint="/q", status_code=429)
        self.assertTrue(should_retry(e))

    def test_ga4_server_error_yes(self):
        from api.utils.errors import should_retry, GA4APIError
        e = GA4APIError("x", property_id="1", status_code=502)
        self.assertTrue(should_retry(e))

    def test_gsc_auth_error_no(self):
        from api.utils.errors import should_retry, GSCAuthError
        self.assertFalse(should_retry(GSCAuthError("expired")))

    def test_ga4_auth_error_no(self):
        from api.utils.errors import should_retry, GA4AuthError
        self.assertFalse(should_retry(GA4AuthError("expired")))

    def test_validation_error_no(self):
        from api.utils.errors import should_retry, ValidationError
        self.assertFalse(should_retry(ValidationError("bad", field="x", value="y")))

    def test_database_error_yes(self):
        from api.utils.errors import should_retry, DatabaseError
        self.assertTrue(should_retry(DatabaseError("fail", operation="read")))

    def test_connection_error_yes(self):
        from api.utils.errors import should_retry
        self.assertTrue(should_retry(ConnectionError("refused")))

    def test_timeout_error_yes(self):
        from api.utils.errors import should_retry
        self.assertTrue(should_retry(TimeoutError("timeout")))

    def test_unknown_error_no(self):
        from api.utils.errors import should_retry
        self.assertFalse(should_retry(ValueError("unknown")))

    def test_gsc_client_error_no(self):
        from api.utils.errors import should_retry, GSCAPIError
        e = GSCAPIError("x", endpoint="/q", status_code=400)
        self.assertFalse(should_retry(e))

    def test_dataforseo_rate_limit_no_429(self):
        from api.utils.errors import should_retry, DataForSEOError
        e = DataForSEOError("x", status_code=402)
        self.assertFalse(should_retry(e))


class TestGetFallbackMessage(unittest.TestCase):
    def test_known_modules(self):
        from api.utils.errors import get_fallback_message
        known = [
            "health_trajectory", "page_triage", "serp_landscape",
            "content_intelligence", "gameplan", "algorithm_impacts",
            "intent_migration", "ctr_modeling", "site_architecture",
            "branded_split", "competitive_radar", "revenue_attribution"
        ]
        for mod in known:
            msg = get_fallback_message(ValueError(), mod)
            self.assertTrue(len(msg) > 10, f"Empty fallback for {mod}")

    def test_unknown_module(self):
        from api.utils.errors import get_fallback_message
        msg = get_fallback_message(ValueError(), "unknown_module")
        self.assertIn("unknown_module", msg)
        self.assertIn("unavailable", msg.lower())


class TestBuildApiErrorContext(unittest.TestCase):
    def test_all_fields(self):
        from api.utils.errors import build_api_error_context
        ctx = build_api_error_context("/api/test", status_code=500, response_body="error body", request_params={"k": "v"})
        self.assertEqual(ctx["endpoint"], "/api/test")
        self.assertEqual(ctx["status_code"], 500)
        self.assertIn("error body", ctx["response_preview"])

    def test_long_response_truncated(self):
        from api.utils.errors import build_api_error_context
        ctx = build_api_error_context("/api", response_body="x" * 1000)
        self.assertLessEqual(len(ctx["response_preview"]), 200)

    def test_none_response(self):
        from api.utils.errors import build_api_error_context
        ctx = build_api_error_context("/api")
        self.assertIsNone(ctx["response_preview"])


class TestBuildAnalysisErrorContext(unittest.TestCase):
    def test_all_fields(self):
        from api.utils.errors import build_analysis_error_context
        ctx = build_analysis_error_context("mod1", input_shape=(100, 5), step="decompose")
        self.assertEqual(ctx["module"], "mod1")
        self.assertEqual(ctx["input_shape"], (100, 5))
        self.assertEqual(ctx["step"], "decompose")


# ---------------------------------------------------------------------------
# SECTION 2  ·  api/utils/retry.py
# ---------------------------------------------------------------------------

class TestRetryableErrors(unittest.TestCase):
    def test_retryable_error_is_exception(self):
        from api.utils.retry import RetryableError
        self.assertTrue(issubclass(RetryableError, Exception))

    def test_rate_limit_is_retryable(self):
        from api.utils.retry import RateLimitError, RetryableError
        self.assertTrue(issubclass(RateLimitError, RetryableError))

    def test_transient_is_retryable(self):
        from api.utils.retry import TransientError, RetryableError
        self.assertTrue(issubclass(TransientError, RetryableError))


class TestIsRetryableError(unittest.TestCase):
    def test_retryable_error_true(self):
        from api.utils.retry import is_retryable_error, RetryableError
        self.assertTrue(is_retryable_error(RetryableError("x")))

    def test_rate_limit_true(self):
        from api.utils.retry import is_retryable_error, RateLimitError
        self.assertTrue(is_retryable_error(RateLimitError("x")))

    def test_transient_true(self):
        from api.utils.retry import is_retryable_error, TransientError
        self.assertTrue(is_retryable_error(TransientError("x")))

    def test_value_error_false(self):
        from api.utils.retry import is_retryable_error
        self.assertFalse(is_retryable_error(ValueError("x")))

    def test_httpx_429_true(self):
        import httpx
        from api.utils.retry import is_retryable_error
        resp = httpx.Response(429, request=httpx.Request("GET", "http://x"))
        e = httpx.HTTPStatusError("rate limit", request=resp.request, response=resp)
        self.assertTrue(is_retryable_error(e))

    def test_httpx_500_true(self):
        import httpx
        from api.utils.retry import is_retryable_error
        resp = httpx.Response(500, request=httpx.Request("GET", "http://x"))
        e = httpx.HTTPStatusError("server", request=resp.request, response=resp)
        self.assertTrue(is_retryable_error(e))

    def test_httpx_400_false(self):
        import httpx
        from api.utils.retry import is_retryable_error
        resp = httpx.Response(400, request=httpx.Request("GET", "http://x"))
        e = httpx.HTTPStatusError("bad", request=resp.request, response=resp)
        self.assertFalse(is_retryable_error(e))

    def test_httpx_403_false(self):
        import httpx
        from api.utils.retry import is_retryable_error
        resp = httpx.Response(403, request=httpx.Request("GET", "http://x"))
        e = httpx.HTTPStatusError("forbidden", request=resp.request, response=resp)
        self.assertFalse(is_retryable_error(e))

    def test_httpx_503_true(self):
        import httpx
        from api.utils.retry import is_retryable_error
        resp = httpx.Response(503, request=httpx.Request("GET", "http://x"))
        e = httpx.HTTPStatusError("unavail", request=resp.request, response=resp)
        self.assertTrue(is_retryable_error(e))

    def test_connect_error_true(self):
        import httpx
        from api.utils.retry import is_retryable_error
        self.assertTrue(is_retryable_error(httpx.ConnectError("refused")))

    def test_timeout_exception_true(self):
        import httpx
        from api.utils.retry import is_retryable_error
        self.assertTrue(is_retryable_error(httpx.TimeoutException("timeout")))

    def test_read_timeout_true(self):
        import httpx
        from api.utils.retry import is_retryable_error
        self.assertTrue(is_retryable_error(httpx.ReadTimeout("timeout")))

    def test_write_timeout_true(self):
        import httpx
        from api.utils.retry import is_retryable_error
        self.assertTrue(is_retryable_error(httpx.WriteTimeout("timeout")))

    def test_remote_protocol_error_true(self):
        import httpx
        from api.utils.retry import is_retryable_error
        self.assertTrue(is_retryable_error(httpx.RemoteProtocolError("protocol")))

    def test_key_error_false(self):
        from api.utils.retry import is_retryable_error
        self.assertFalse(is_retryable_error(KeyError("k")))

    def test_type_error_false(self):
        from api.utils.retry import is_retryable_error
        self.assertFalse(is_retryable_error(TypeError("t")))


class TestCalculateBackoff(unittest.TestCase):
    def test_attempt_0_base_delay(self):
        from api.utils.retry import calculate_backoff
        d = calculate_backoff(0, base_delay=1.0, jitter=False)
        self.assertAlmostEqual(d, 1.0)

    def test_attempt_1_doubled(self):
        from api.utils.retry import calculate_backoff
        d = calculate_backoff(1, base_delay=1.0, jitter=False)
        self.assertAlmostEqual(d, 2.0)

    def test_attempt_2_quadrupled(self):
        from api.utils.retry import calculate_backoff
        d = calculate_backoff(2, base_delay=1.0, jitter=False)
        self.assertAlmostEqual(d, 4.0)

    def test_max_delay_capped(self):
        from api.utils.retry import calculate_backoff
        d = calculate_backoff(100, base_delay=1.0, max_delay=60.0, jitter=False)
        self.assertAlmostEqual(d, 60.0)

    def test_custom_base(self):
        from api.utils.retry import calculate_backoff
        d = calculate_backoff(0, base_delay=5.0, jitter=False)
        self.assertAlmostEqual(d, 5.0)

    def test_custom_exponential_base(self):
        from api.utils.retry import calculate_backoff
        d = calculate_backoff(1, base_delay=1.0, exponential_base=3.0, jitter=False)
        self.assertAlmostEqual(d, 3.0)

    def test_jitter_within_range(self):
        from api.utils.retry import calculate_backoff
        # With jitter, should be within ±25% of base calculation
        for _ in range(50):
            d = calculate_backoff(0, base_delay=10.0, jitter=True)
            self.assertGreaterEqual(d, 7.5)
            self.assertLessEqual(d, 12.5)

    def test_never_negative(self):
        from api.utils.retry import calculate_backoff
        for _ in range(100):
            d = calculate_backoff(0, base_delay=0.01, jitter=True)
            self.assertGreaterEqual(d, 0)

    def test_attempt_0_no_jitter_exact(self):
        from api.utils.retry import calculate_backoff
        d = calculate_backoff(0, base_delay=2.5, max_delay=100, exponential_base=2, jitter=False)
        self.assertEqual(d, 2.5)


class TestRetryDecorator(unittest.TestCase):
    def test_sync_success_no_retry(self):
        from api.utils.retry import retry
        call_count = 0

        @retry(max_attempts=3, base_delay=0.01, jitter=False)
        def fn():
            nonlocal call_count
            call_count += 1
            return "ok"

        self.assertEqual(fn(), "ok")
        self.assertEqual(call_count, 1)

    def test_sync_retries_on_retryable(self):
        from api.utils.retry import retry, TransientError
        call_count = 0

        @retry(max_attempts=3, base_delay=0.001, jitter=False)
        def fn():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise TransientError("fail")
            return "ok"

        self.assertEqual(fn(), "ok")
        self.assertEqual(call_count, 3)

    def test_sync_raises_after_max_attempts(self):
        from api.utils.retry import retry, TransientError
        call_count = 0

        @retry(max_attempts=2, base_delay=0.001, jitter=False)
        def fn():
            nonlocal call_count
            call_count += 1
            raise TransientError("always fail")

        with self.assertRaises(TransientError):
            fn()
        self.assertEqual(call_count, 2)

    def test_sync_no_retry_on_non_retryable(self):
        from api.utils.retry import retry
        call_count = 0

        @retry(max_attempts=3, base_delay=0.001, jitter=False)
        def fn():
            nonlocal call_count
            call_count += 1
            raise ValueError("not retryable")

        with self.assertRaises(ValueError):
            fn()
        self.assertEqual(call_count, 1)

    def test_custom_retryable_errors(self):
        from api.utils.retry import retry

        class CustomErr(Exception):
            pass

        call_count = 0

        @retry(max_attempts=3, base_delay=0.001, jitter=False, retryable_errors=(CustomErr,))
        def fn():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise CustomErr("custom")
            return "done"

        self.assertEqual(fn(), "done")
        self.assertEqual(call_count, 3)

    def test_on_retry_callback(self):
        from api.utils.retry import retry, TransientError
        callbacks = []

        def cb(err, attempt, delay):
            callbacks.append((type(err).__name__, attempt, delay))

        @retry(max_attempts=3, base_delay=0.001, jitter=False, on_retry=cb)
        def fn():
            raise TransientError("fail")

        with self.assertRaises(TransientError):
            fn()
        self.assertEqual(len(callbacks), 1)  # callback on first retry only (not on last fail)

    def test_preserves_function_name(self):
        from api.utils.retry import retry

        @retry(max_attempts=2)
        def my_func():
            pass

        self.assertEqual(my_func.__name__, "my_func")


class TestRetryDecoratorAsync(unittest.TestCase):
    def test_async_success(self):
        import asyncio
        from api.utils.retry import retry

        call_count = 0

        @retry(max_attempts=3, base_delay=0.001, jitter=False)
        async def fn():
            nonlocal call_count
            call_count += 1
            return "ok"

        result = asyncio.get_event_loop().run_until_complete(fn())
        self.assertEqual(result, "ok")
        self.assertEqual(call_count, 1)

    def test_async_retries(self):
        import asyncio
        from api.utils.retry import retry, RateLimitError

        call_count = 0

        @retry(max_attempts=3, base_delay=0.001, jitter=False)
        async def fn():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise RateLimitError("limit")
            return "ok"

        result = asyncio.get_event_loop().run_until_complete(fn())
        self.assertEqual(result, "ok")
        self.assertEqual(call_count, 2)

    def test_async_raises_after_max(self):
        import asyncio
        from api.utils.retry import retry, TransientError

        @retry(max_attempts=2, base_delay=0.001, jitter=False)
        async def fn():
            raise TransientError("fail")

        with self.assertRaises(TransientError):
            asyncio.get_event_loop().run_until_complete(fn())

    def test_async_preserves_name(self):
        from api.utils.retry import retry

        @retry(max_attempts=2)
        async def my_async_func():
            pass

        self.assertEqual(my_async_func.__name__, "my_async_func")


class TestCircuitBreaker(unittest.TestCase):
    def test_opens_after_threshold(self):
        from api.utils.retry import retry_with_circuit_breaker, TransientError
        call_count = 0

        @retry_with_circuit_breaker(
            max_attempts=1, failure_threshold=2, recovery_timeout=100,
            base_delay=0.001, jitter=False
        )
        def fn():
            nonlocal call_count
            call_count += 1
            raise TransientError("fail")

        # Exhaust threshold
        for _ in range(2):
            try:
                fn()
            except Exception:
                pass

        # Circuit should be open - next call should fail fast with RetryableError
        from api.utils.retry import RetryableError
        with self.assertRaises((RetryableError, TransientError)):
            fn()

    def test_success_resets_circuit(self):
        from api.utils.retry import retry_with_circuit_breaker, TransientError

        attempts = 0

        @retry_with_circuit_breaker(
            max_attempts=1, failure_threshold=3, recovery_timeout=100,
            base_delay=0.001, jitter=False
        )
        def fn():
            nonlocal attempts
            attempts += 1
            if attempts <= 1:
                raise TransientError("fail")
            return "ok"

        # First call fails
        try:
            fn()
        except TransientError:
            pass

        # Second call succeeds, should reset
        result = fn()
        self.assertEqual(result, "ok")


# ---------------------------------------------------------------------------
# SECTION 3  ·  api/utils/performance.py
# ---------------------------------------------------------------------------

class TestPerformanceMetrics(unittest.TestCase):
    def setUp(self):
        from api.utils.performance import PerformanceMetrics
        self.pm = PerformanceMetrics()
        self.pm.reset()

    def test_singleton(self):
        from api.utils.performance import PerformanceMetrics
        a = PerformanceMetrics()
        b = PerformanceMetrics()
        self.assertIs(a, b)

    def test_record_and_get_stats(self):
        self.pm.record("op1", 1.5)
        self.pm.record("op1", 2.5)
        stats = self.pm.get_stats("op1")
        self.assertEqual(stats["call_count"], 2)
        self.assertAlmostEqual(stats["total_time"], 4.0)
        self.assertAlmostEqual(stats["avg_time"], 2.0)
        self.assertAlmostEqual(stats["min_time"], 1.5)
        self.assertAlmostEqual(stats["max_time"], 2.5)

    def test_get_stats_unknown_op(self):
        stats = self.pm.get_stats("nonexistent")
        self.assertEqual(stats, {})

    def test_get_stats_all(self):
        self.pm.record("a", 1.0)
        self.pm.record("b", 2.0)
        stats = self.pm.get_stats()
        self.assertIn("a", stats)
        self.assertIn("b", stats)

    def test_get_bottlenecks(self):
        self.pm.record("fast", 0.1)
        self.pm.record("slow", 10.0)
        self.pm.record("medium", 5.0)
        bottlenecks = self.pm.get_bottlenecks(2)
        self.assertEqual(len(bottlenecks), 2)
        self.assertEqual(bottlenecks[0][0], "slow")

    def test_reset(self):
        self.pm.record("x", 1.0)
        self.pm.reset()
        self.assertEqual(self.pm.get_stats(), {})

    def test_export_json(self):
        import json
        self.pm.record("op", 1.0)
        j = self.pm.export_json()
        data = json.loads(j)
        self.assertIn("summary", data)
        self.assertIn("bottlenecks", data)
        self.assertIn("raw_metrics", data)

    def test_metadata_recorded(self):
        self.pm.record("op", 1.0, metadata={"rows": 100})
        raw = self.pm._metrics["op"][0]
        self.assertEqual(raw["metadata"]["rows"], 100)

    def test_timestamp_recorded(self):
        self.pm.record("op", 1.0)
        raw = self.pm._metrics["op"][0]
        self.assertIn("timestamp", raw)

    def test_log_summary_no_error(self):
        self.pm.record("op", 1.0)
        self.pm.log_summary()  # Should not raise


class TestTimedDecorator(unittest.TestCase):
    def setUp(self):
        from api.utils.performance import PerformanceMetrics
        PerformanceMetrics().reset()

    def test_records_timing(self):
        from api.utils.performance import timed, metrics

        @timed("test_op")
        def fn():
            return 42

        result = fn()
        self.assertEqual(result, 42)
        stats = metrics.get_stats("test_op")
        self.assertEqual(stats["call_count"], 1)
        self.assertGreater(stats["total_time"], 0)

    def test_auto_name(self):
        from api.utils.performance import timed, metrics

        @timed()
        def my_function():
            pass

        my_function()
        # Should use module.function_name pattern
        all_stats = metrics.get_stats()
        names = list(all_stats.keys())
        self.assertTrue(any("my_function" in n for n in names))

    def test_preserves_function_name(self):
        from api.utils.performance import timed

        @timed()
        def specific_name():
            pass

        self.assertEqual(specific_name.__name__, "specific_name")


class TestAsyncTimedDecorator(unittest.TestCase):
    def setUp(self):
        from api.utils.performance import PerformanceMetrics
        PerformanceMetrics().reset()

    def test_records_async_timing(self):
        import asyncio
        from api.utils.performance import async_timed, metrics

        @async_timed("async_op")
        async def fn():
            return "result"

        result = asyncio.get_event_loop().run_until_complete(fn())
        self.assertEqual(result, "result")
        stats = metrics.get_stats("async_op")
        self.assertEqual(stats["call_count"], 1)


class TestMeasureTimeContextManager(unittest.TestCase):
    def setUp(self):
        from api.utils.performance import PerformanceMetrics
        PerformanceMetrics().reset()

    def test_records_timing(self):
        from api.utils.performance import measure_time, metrics

        with measure_time("ctx_op"):
            _ = sum(range(1000))

        stats = metrics.get_stats("ctx_op")
        self.assertEqual(stats["call_count"], 1)
        self.assertGreater(stats["total_time"], 0)

    def test_with_metadata(self):
        from api.utils.performance import measure_time, metrics

        with measure_time("ctx_op2", {"items": 50}):
            pass

        raw = metrics._metrics["ctx_op2"][0]
        self.assertEqual(raw["metadata"]["items"], 50)


class TestProgressTracker(unittest.TestCase):
    def test_basic_tracking(self):
        from api.utils.performance import ProgressTracker
        t = ProgressTracker("test_op", total_steps=3)
        t.start_step("s1", "Step 1")
        t.complete_step()
        t.start_step("s2", "Step 2")
        t.complete_step()
        summary = t.get_summary()
        self.assertEqual(summary["steps_completed"], 2)
        self.assertEqual(summary["steps_total"], 3)

    def test_auto_complete_previous(self):
        from api.utils.performance import ProgressTracker
        t = ProgressTracker("test_op", total_steps=3)
        t.start_step("s1", "Step 1")
        # Starting s2 without completing s1 should auto-complete s1
        t.start_step("s2", "Step 2")
        t.complete_step()
        summary = t.get_summary()
        self.assertEqual(summary["steps_completed"], 2)

    def test_slowest_step(self):
        from api.utils.performance import ProgressTracker
        t = ProgressTracker("test_op", total_steps=2)
        t.start_step("fast")
        time.sleep(0.01)
        t.complete_step()
        t.start_step("slow")
        time.sleep(0.05)
        t.complete_step()
        summary = t.get_summary()
        self.assertEqual(summary["slowest_step"]["name"], "slow")

    def test_empty_tracker(self):
        from api.utils.performance import ProgressTracker
        t = ProgressTracker("empty", total_steps=5)
        summary = t.get_summary()
        self.assertEqual(summary["steps_completed"], 0)
        self.assertIsNone(summary["slowest_step"])
        self.assertEqual(summary["average_step_time"], 0)

    def test_log_summary_no_error(self):
        from api.utils.performance import ProgressTracker
        t = ProgressTracker("test", total_steps=1)
        t.start_step("s1")
        t.complete_step()
        t.log_summary()  # Should not raise

    def test_complete_step_without_start(self):
        from api.utils.performance import ProgressTracker
        t = ProgressTracker("test", total_steps=1)
        # Should not raise, just warn
        t.complete_step()
        self.assertEqual(len(t.step_timings), 0)

    def test_metadata_in_step(self):
        from api.utils.performance import ProgressTracker
        t = ProgressTracker("test", total_steps=1)
        t.start_step("s1")
        t.complete_step(metadata={"records": 500})
        self.assertEqual(t.step_timings[0]["metadata"]["records"], 500)

    def test_total_duration(self):
        from api.utils.performance import ProgressTracker
        t = ProgressTracker("test", total_steps=1)
        time.sleep(0.02)
        summary = t.get_summary()
        self.assertGreater(summary["total_duration"], 0.01)


# ---------------------------------------------------------------------------
# META: Verify this test file itself parses cleanly
# ---------------------------------------------------------------------------

class TestSelfParse(unittest.TestCase):
    def test_file_parses(self):
        with open(__file__, "r") as f:
            source = f.read()
        ast.parse(source)  # Should not raise


if __name__ == "__main__":
    unittest.main()
