"""
Comprehensive unit tests for api/worker/pipeline.py — the analysis pipeline
coordinator that orchestrates all 12 analysis modules.

Tests cover:
  - Data conversion helpers (_ensure_dataframe, _crawl_dict_to_page_dataframe,
    _normalize_serp_data, _get_module_data)
  - Module dependency checking
  - Module input preparation for all 12 modules
  - Error handling and graceful degradation
  - Pipeline execution order (modules 1-4, 6-12, then gameplan 5 last)
  - Report data assembly from pipeline results
  - Progress callback invocation
  - Partial report generation when modules fail

All tests use mock module functions to avoid heavy ML/stats dependencies.
"""

import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch, call
from typing import Any, Dict, List, Optional

import pandas as pd
import numpy as np

from api.worker.pipeline import (
    AnalysisPipeline,
    ModuleError,
    ModuleResult,
    PipelineResult,
    ProgressCallback,
    _crawl_dict_to_page_dataframe,
    _ensure_dataframe,
    _get_module_data,
    _normalize_serp_data,
    run_analysis_pipeline,
)


# ---------------------------------------------------------------------------
# Fixtures: reusable test data
# ---------------------------------------------------------------------------

@pytest.fixture
def daily_data():
    """16 months of synthetic daily GSC data for Module 1."""
    dates = pd.date_range("2024-10-01", periods=480, freq="D")
    return pd.DataFrame({
        "date": dates,
        "clicks": np.random.randint(80, 150, size=480),
        "impressions": np.random.randint(800, 1500, size=480),
    })


@pytest.fixture
def page_daily_data():
    """Per-page daily data for Module 2."""
    pages = [f"/page-{i}" for i in range(20)]
    rows = []
    for page in pages:
        for day in pd.date_range("2025-01-01", periods=90, freq="D"):
            rows.append({
                "page": page,
                "date": day,
                "clicks": np.random.randint(1, 50),
                "impressions": np.random.randint(10, 500),
                "ctr": round(np.random.uniform(0.01, 0.15), 3),
                "position": round(np.random.uniform(1, 30), 1),
            })
    return pd.DataFrame(rows)


@pytest.fixture
def ga4_landing_data():
    """GA4 landing page engagement data for Module 2."""
    return pd.DataFrame({
        "page": [f"/page-{i}" for i in range(20)],
        "sessions": np.random.randint(10, 500, size=20),
        "bounce_rate": np.random.uniform(0.3, 0.9, size=20),
        "avg_session_duration": np.random.uniform(10, 300, size=20),
    })


@pytest.fixture
def gsc_page_summary():
    """Page-level aggregates from GSC."""
    return pd.DataFrame({
        "page": [f"/page-{i}" for i in range(20)],
        "clicks": np.random.randint(10, 500, size=20),
        "impressions": np.random.randint(100, 5000, size=20),
        "ctr": np.random.uniform(0.01, 0.15, size=20),
        "position": np.random.uniform(1, 30, size=20),
    })


@pytest.fixture
def gsc_keyword_data():
    """Keyword performance data from GSC."""
    return pd.DataFrame({
        "query": [f"keyword {i}" for i in range(50)],
        "clicks": np.random.randint(1, 200, size=50),
        "impressions": np.random.randint(10, 2000, size=50),
        "ctr": np.random.uniform(0.01, 0.15, size=50),
        "position": np.random.uniform(1, 50, size=50),
    })


@pytest.fixture
def serp_data_wrapper():
    """DataForSEO SERP data in the wrapper dict format."""
    results = []
    for i in range(10):
        results.append({
            "keyword": f"keyword {i}",
            "success": True,
            "data": {
                "keyword": f"keyword {i}",
                "serp_features": {
                    "featured_snippet": i % 3 == 0,
                    "people_also_ask": 3 if i % 2 == 0 else 0,
                    "video_carousel": i % 5 == 0,
                    "local_pack": False,
                    "knowledge_panel": i % 4 == 0,
                    "ai_overview": i % 2 == 0,
                    "image_pack": False,
                    "shopping_results": False,
                },
                "organic_results": [
                    {
                        "position": j + 1,
                        "url": f"https://example{j}.com/page",
                        "domain": f"example{j}.com",
                        "title": f"Result {j + 1}",
                        "is_user_result": j == 2,
                    }
                    for j in range(10)
                ],
                "user_position": 3,
                "user_url": "https://example2.com/page",
                "visual_position": 7.5,
                "competitors": [
                    {"domain": "competitor1.com", "position": 1},
                    {"domain": "competitor2.com", "position": 2},
                ],
            },
        })
    # Add one failed result to test filtering
    results.append({
        "keyword": "failed keyword",
        "success": False,
        "data": {},
    })
    return {
        "total_keywords_requested": 11,
        "successful_fetches": 10,
        "results": results,
        "spending": {"total": 0.02},
    }


@pytest.fixture
def crawl_data():
    """Crawler output dict with pages and link graph."""
    pages = []
    for i in range(15):
        pages.append({
            "url": f"https://example.com/page-{i}",
            "title": f"Page {i} Title",
            "h1": f"Page {i} Heading",
            "meta_description": f"Description for page {i}",
            "word_count": 500 + i * 100,
            "canonical": f"https://example.com/page-{i}",
            "schema_types": ["Article"] if i % 2 == 0 else [],
            "internal_links": [f"https://example.com/page-{(i+1) % 15}"],
        })
    return {
        "pages": pages,
        "link_graph": {
            f"https://example.com/page-{i}": [
                f"https://example.com/page-{(i+1) % 15}"
            ]
            for i in range(15)
        },
        "sitemap_urls": [f"https://example.com/page-{i}" for i in range(15)],
        "stats": {
            "pages_crawled": 15,
            "total_internal_links": 15,
            "crawl_time_seconds": 3.2,
        },
    }


@pytest.fixture
def full_data_context(
    daily_data, page_daily_data, ga4_landing_data, gsc_page_summary,
    gsc_keyword_data, serp_data_wrapper, crawl_data,
):
    """Complete data context with all data types present."""
    return {
        "gsc_daily_data": daily_data,
        "gsc_page_daily_data": page_daily_data,
        "gsc_page_summary": gsc_page_summary,
        "gsc_keyword_data": gsc_keyword_data,
        "gsc_query_page_data": pd.DataFrame({
            "query": [f"keyword {i}" for i in range(30)],
            "page": [f"/page-{i % 20}" for i in range(30)],
            "clicks": np.random.randint(1, 100, size=30),
            "impressions": np.random.randint(10, 1000, size=30),
        }),
        "gsc_query_date_data": pd.DataFrame({
            "query": [f"keyword {i % 10}" for i in range(100)],
            "date": pd.date_range("2025-01-01", periods=100, freq="D"),
            "clicks": np.random.randint(1, 50, size=100),
            "impressions": np.random.randint(10, 500, size=100),
        }),
        "ga4_landing_pages": ga4_landing_data,
        "ga4_conversions": pd.DataFrame({
            "event_name": ["purchase", "signup", "download"],
            "count": [100, 500, 250],
        }),
        "ga4_engagement_data": pd.DataFrame({
            "sessions": [1000],
            "bounce_rate": [0.45],
        }),
        "ga4_ecommerce": pd.DataFrame({
            "transaction_id": [f"txn-{i}" for i in range(5)],
            "revenue": [50.0, 120.0, 75.0, 200.0, 30.0],
        }),
        "serp_data": serp_data_wrapper,
        "crawl_data": crawl_data,
        "internal_link_graph": crawl_data,
        "sitemap_urls": crawl_data["sitemap_urls"],
        "brand_terms": ["example", "example.com"],
        "domain": "example.com",
        "gsc_query_data": gsc_keyword_data,
    }


def _mock_module_success(name: str, output: Optional[Dict] = None):
    """Create a mock module function that returns a simple dict."""
    def _func(**kwargs):
        return output or {f"{name}_result": True, "summary": f"{name} completed"}
    return _func


def _mock_module_failure(name: str, error_msg: str = "Test error"):
    """Create a mock module function that raises an exception."""
    def _func(**kwargs):
        raise ValueError(f"{name}: {error_msg}")
    return _func


# ===================================================================
# Tests: _ensure_dataframe helper
# ===================================================================


class TestEnsureDataframe:
    """Tests for the _ensure_dataframe helper."""

    def test_none_returns_none(self):
        assert _ensure_dataframe(None, "test") is None

    def test_dataframe_passthrough(self):
        df = pd.DataFrame({"a": [1, 2]})
        result = _ensure_dataframe(df, "test")
        assert result is df

    def test_list_of_dicts_converts(self):
        data = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
        result = _ensure_dataframe(data, "test")
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert list(result.columns) == ["a", "b"]

    def test_empty_list_returns_empty_dataframe(self):
        result = _ensure_dataframe([], "test")
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_dict_with_rows_key(self):
        data = {"rows": [{"x": 1}, {"x": 2}]}
        result = _ensure_dataframe(data, "test")
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2

    def test_dict_with_data_key(self):
        data = {"data": [{"x": 10}]}
        result = _ensure_dataframe(data, "test")
        assert isinstance(result, pd.DataFrame)
        assert result.iloc[0]["x"] == 10

    def test_dict_with_results_key(self):
        data = {"results": [{"y": 5}]}
        result = _ensure_dataframe(data, "test")
        assert isinstance(result, pd.DataFrame)
        assert result.iloc[0]["y"] == 5

    def test_unrecognised_type_passthrough(self):
        """Non-convertible types are returned as-is."""
        result = _ensure_dataframe("some string", "test")
        assert result == "some string"


# ===================================================================
# Tests: _crawl_dict_to_page_dataframe helper
# ===================================================================


class TestCrawlDictToPageDataframe:
    """Tests for crawl data dict → DataFrame conversion."""

    def test_none_returns_none(self):
        assert _crawl_dict_to_page_dataframe(None) is None

    def test_dataframe_passthrough(self):
        df = pd.DataFrame({"url": ["/a"], "word_count": [100]})
        result = _crawl_dict_to_page_dataframe(df)
        assert result is df

    def test_non_dict_returns_none(self):
        assert _crawl_dict_to_page_dataframe("not a dict") is None

    def test_empty_pages_returns_none(self):
        assert _crawl_dict_to_page_dataframe({"pages": []}) is None

    def test_missing_pages_key_returns_none(self):
        assert _crawl_dict_to_page_dataframe({"other": "data"}) is None

    def test_valid_crawl_data(self, crawl_data):
        result = _crawl_dict_to_page_dataframe(crawl_data)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 15
        assert "url" in result.columns
        assert "word_count" in result.columns
        assert "title" in result.columns
        assert "h1" in result.columns

    def test_missing_columns_get_defaults(self):
        """Pages without word_count/last_modified get default values."""
        data = {"pages": [{"url": "/page-1", "title": "Test"}]}
        result = _crawl_dict_to_page_dataframe(data)
        assert isinstance(result, pd.DataFrame)
        assert "word_count" in result.columns
        assert "last_modified" in result.columns
        assert result.iloc[0]["word_count"] == 0


# ===================================================================
# Tests: _normalize_serp_data helper
# ===================================================================


class TestNormalizeSerpData:
    """Tests for DataForSEO wrapper dict → flat list conversion."""

    def test_none_returns_empty_list(self):
        assert _normalize_serp_data(None) == []

    def test_list_passthrough(self):
        data = [{"keyword": "test", "organic_results": []}]
        result = _normalize_serp_data(data)
        assert result is data

    def test_non_dict_non_list_returns_empty(self):
        assert _normalize_serp_data(42) == []

    def test_missing_results_key_returns_empty(self):
        assert _normalize_serp_data({"other": "data"}) == []

    def test_filters_failed_results(self, serp_data_wrapper):
        result = _normalize_serp_data(serp_data_wrapper)
        # 10 successful + 1 failed → should get 10
        assert len(result) == 10

    def test_normalized_entries_have_required_keys(self, serp_data_wrapper):
        result = _normalize_serp_data(serp_data_wrapper)
        for entry in result:
            assert "keyword" in entry
            assert "user_domain" in entry
            assert "organic_results" in entry
            assert "serp_features" in entry
            # Promoted top-level feature keys
            assert "featured_snippet" in entry
            assert "knowledge_panel" in entry
            assert "ai_overview" in entry
            assert "people_also_ask" in entry

    def test_featured_snippet_promoted(self, serp_data_wrapper):
        result = _normalize_serp_data(serp_data_wrapper)
        # Keyword 0 has featured_snippet=True (i % 3 == 0)
        entry_0 = result[0]
        assert entry_0["featured_snippet"] is not None
        assert entry_0["featured_snippet"]["position"] == 0

    def test_user_domain_extracted(self, serp_data_wrapper):
        result = _normalize_serp_data(serp_data_wrapper)
        # is_user_result is True for position 3 (j==2, domain=example2.com)
        for entry in result:
            assert entry["user_domain"] == "example2.com"

    def test_paa_promoted_as_list(self, serp_data_wrapper):
        result = _normalize_serp_data(serp_data_wrapper)
        # Keyword 0 has people_also_ask=3 (i % 2 == 0)
        entry_0 = result[0]
        assert isinstance(entry_0["people_also_ask"], list)
        assert len(entry_0["people_also_ask"]) == 3


# ===================================================================
# Tests: _get_module_data helper
# ===================================================================


class TestGetModuleData:
    """Tests for extracting data from completed module results."""

    def test_returns_data_on_success(self):
        results = {
            "health_trajectory": ModuleResult(
                module_name="health_trajectory",
                status="success",
                data={"trend": "up"},
            )
        }
        assert _get_module_data(results, "health_trajectory") == {"trend": "up"}

    def test_returns_none_on_failure(self):
        results = {
            "health_trajectory": ModuleResult(
                module_name="health_trajectory",
                status="failed",
                data=None,
            )
        }
        assert _get_module_data(results, "health_trajectory") is None

    def test_returns_none_for_missing_module(self):
        assert _get_module_data({}, "health_trajectory") is None

    def test_returns_none_on_skipped(self):
        results = {
            "gameplan": ModuleResult(
                module_name="gameplan",
                status="skipped",
                data=None,
            )
        }
        assert _get_module_data(results, "gameplan") is None


# ===================================================================
# Tests: AnalysisPipeline — dependency checking
# ===================================================================


class TestPipelineDependencies:
    """Tests for module dependency resolution."""

    def test_no_deps_always_runnable(self):
        pipeline = AnalysisPipeline()
        can_run, reason = pipeline._check_dependencies("health_trajectory", {})
        assert can_run is True
        assert reason is None

    def test_gameplan_requires_health_and_triage(self):
        pipeline = AnalysisPipeline()
        # Missing both deps
        can_run, reason = pipeline._check_dependencies("gameplan", {})
        assert can_run is False
        assert "health_trajectory" in reason

    def test_gameplan_satisfied_with_both_deps(self):
        pipeline = AnalysisPipeline()
        completed = {
            "health_trajectory": ModuleResult(
                module_name="health_trajectory", status="success", data={}
            ),
            "page_triage": ModuleResult(
                module_name="page_triage", status="success", data={}
            ),
        }
        can_run, reason = pipeline._check_dependencies("gameplan", completed)
        assert can_run is True

    def test_gameplan_blocked_if_dep_failed(self):
        pipeline = AnalysisPipeline()
        completed = {
            "health_trajectory": ModuleResult(
                module_name="health_trajectory", status="failed", data=None
            ),
            "page_triage": ModuleResult(
                module_name="page_triage", status="success", data={}
            ),
        }
        can_run, reason = pipeline._check_dependencies("gameplan", completed)
        assert can_run is False
        assert "failed" in reason


# ===================================================================
# Tests: AnalysisPipeline — execution order
# ===================================================================


class TestPipelineExecutionOrder:
    """Tests that modules run in the correct order."""

    def test_gameplan_runs_last(self):
        """Gameplan (Module 5) must run after all other modules."""
        pipeline = AnalysisPipeline()
        names = [name for name, _ in pipeline.modules]
        assert names[-1] == "gameplan"

    def test_twelve_modules_registered(self):
        pipeline = AnalysisPipeline()
        assert len(pipeline.modules) == 12

    def test_module_numbers_map_all_twelve(self):
        pipeline = AnalysisPipeline()
        assert len(pipeline.MODULE_NUMBERS) == 12
        assert set(pipeline.MODULE_NUMBERS.values()) == set(range(1, 13))


# ===================================================================
# Tests: AnalysisPipeline — full execution with mocks
# ===================================================================


class TestPipelineExecution:
    """Tests for full pipeline execution with mocked module functions."""

    def _make_pipeline_with_mocks(self, module_overrides=None):
        """Create a pipeline with all modules replaced by mocks."""
        pipeline = AnalysisPipeline()
        overrides = module_overrides or {}
        new_modules = []
        for name, func in pipeline.modules:
            if name in overrides:
                new_modules.append((name, overrides[name]))
            else:
                new_modules.append((name, _mock_module_success(name)))
        pipeline.modules = new_modules
        return pipeline

    def test_all_modules_succeed(self, full_data_context):
        pipeline = self._make_pipeline_with_mocks()
        result = pipeline.execute(full_data_context)
        assert result.status == "complete"
        assert len(result.modules) == 12
        assert all(m.status == "success" for m in result.modules)
        assert len(result.errors) == 0

    def test_single_module_failure_produces_partial(self, full_data_context):
        pipeline = self._make_pipeline_with_mocks({
            "serp_landscape": _mock_module_failure("serp_landscape"),
        })
        result = pipeline.execute(full_data_context)
        assert result.status == "partial"
        assert sum(1 for m in result.modules if m.status == "success") == 11
        assert sum(1 for m in result.modules if m.status == "failed") == 1
        assert len(result.errors) == 1

    def test_all_modules_fail_produces_failed(self, full_data_context):
        overrides = {}
        pipeline = AnalysisPipeline()
        for name, _ in pipeline.modules:
            overrides[name] = _mock_module_failure(name)
        pipeline = self._make_pipeline_with_mocks(overrides)
        result = pipeline.execute(full_data_context)
        # Gameplan is skipped (dep failure), rest are failed
        failed = sum(1 for m in result.modules if m.status == "failed")
        skipped = sum(1 for m in result.modules if m.status == "skipped")
        assert result.status == "failed"
        assert failed + skipped == 12

    def test_gameplan_skipped_when_deps_fail(self, full_data_context):
        pipeline = self._make_pipeline_with_mocks({
            "health_trajectory": _mock_module_failure("health_trajectory"),
        })
        result = pipeline.execute(full_data_context)
        gameplan = [m for m in result.modules if m.module_name == "gameplan"][0]
        assert gameplan.status == "skipped"
        assert gameplan.error is not None
        assert "DependencyNotMet" in gameplan.error.error_type

    def test_progress_callback_invoked(self, full_data_context):
        pipeline = self._make_pipeline_with_mocks()
        callback = MagicMock()
        result = pipeline.execute(full_data_context, progress_callback=callback)
        assert callback.call_count == 12
        # Verify callback receives module_name and ModuleResult
        for c in callback.call_args_list:
            args = c[0]
            assert isinstance(args[0], str)
            assert isinstance(args[1], ModuleResult)

    def test_callback_failure_does_not_abort_pipeline(self, full_data_context):
        pipeline = self._make_pipeline_with_mocks()
        callback = MagicMock(side_effect=RuntimeError("callback exploded"))
        result = pipeline.execute(full_data_context, progress_callback=callback)
        # Pipeline should still complete all modules despite callback errors
        assert result.status == "complete"
        assert len(result.modules) == 12

    def test_results_in_section_order(self, full_data_context):
        """Results must be in report section order (1-12), not execution order."""
        pipeline = self._make_pipeline_with_mocks()
        result = pipeline.execute(full_data_context)
        expected_order = [
            "health_trajectory", "page_triage", "serp_landscape",
            "content_intelligence", "gameplan", "algorithm_impact",
            "intent_migration", "technical_health", "site_architecture",
            "branded_split", "competitive_threats", "revenue_attribution",
        ]
        actual_order = [m.module_name for m in result.modules]
        assert actual_order == expected_order

    def test_execution_time_tracked(self, full_data_context):
        pipeline = self._make_pipeline_with_mocks()
        result = pipeline.execute(full_data_context)
        assert result.total_execution_time >= 0
        for m in result.modules:
            assert m.execution_time_seconds >= 0


# ===================================================================
# Tests: AnalysisPipeline — report data generation
# ===================================================================


class TestGetReportData:
    """Tests for converting PipelineResult → report JSON structure."""

    def test_complete_report_structure(self, full_data_context):
        pipeline = self._make_pipeline_with_mocks()
        result = pipeline.execute(full_data_context)
        report = pipeline.get_report_data(result)

        assert "metadata" in report
        assert "sections" in report
        assert "errors" in report

        assert report["metadata"]["status"] == "complete"
        assert "generated_at" in report["metadata"]
        assert "execution_time_seconds" in report["metadata"]
        assert "completion_message" in report["metadata"]

        assert len(report["sections"]) == 12

    def test_successful_section_has_data(self, full_data_context):
        pipeline = self._make_pipeline_with_mocks()
        result = pipeline.execute(full_data_context)
        report = pipeline.get_report_data(result)

        section = report["sections"]["health_trajectory"]
        assert section["status"] == "success"
        assert "data" in section

    def test_failed_section_has_error(self, full_data_context):
        pipeline = self._make_pipeline_with_mocks({
            "serp_landscape": _mock_module_failure("serp_landscape"),
        })
        result = pipeline.execute(full_data_context)
        report = pipeline.get_report_data(result)

        section = report["sections"]["serp_landscape"]
        assert section["status"] == "failed"
        assert "error" in section
        assert "type" in section["error"]
        assert "message" in section["error"]

    def test_errors_list_populated(self, full_data_context):
        pipeline = self._make_pipeline_with_mocks({
            "content_intelligence": _mock_module_failure("content_intelligence"),
        })
        result = pipeline.execute(full_data_context)
        report = pipeline.get_report_data(result)

        assert len(report["errors"]) >= 1
        error = report["errors"][0]
        assert "module_name" in error
        assert "error_type" in error
        assert "user_message" in error

    def _make_pipeline_with_mocks(self, module_overrides=None):
        pipeline = AnalysisPipeline()
        overrides = module_overrides or {}
        new_modules = []
        for name, func in pipeline.modules:
            if name in overrides:
                new_modules.append((name, overrides[name]))
            else:
                new_modules.append((name, _mock_module_success(name)))
        pipeline.modules = new_modules
        return pipeline


# ===================================================================
# Tests: AnalysisPipeline — module input preparation
# ===================================================================


class TestModuleInputPreparation:
    """Tests that _prepare_module_inputs correctly maps data for each module."""

    def test_health_trajectory_inputs(self, full_data_context):
        pipeline = AnalysisPipeline()
        inputs = pipeline._prepare_module_inputs(
            "health_trajectory", full_data_context, {}
        )
        assert "df" in inputs
        assert isinstance(inputs["df"], pd.DataFrame)

    def test_page_triage_inputs(self, full_data_context):
        pipeline = AnalysisPipeline()
        inputs = pipeline._prepare_module_inputs(
            "page_triage", full_data_context, {}
        )
        assert "page_daily_data" in inputs
        assert "ga4_landing_data" in inputs
        assert "gsc_page_summary" in inputs

    def test_serp_landscape_inputs(self, full_data_context):
        pipeline = AnalysisPipeline()
        inputs = pipeline._prepare_module_inputs(
            "serp_landscape", full_data_context, {}
        )
        assert "serp_data" in inputs
        assert isinstance(inputs["serp_data"], list)
        assert "gsc_keyword_data" in inputs

    def test_content_intelligence_inputs(self, full_data_context):
        pipeline = AnalysisPipeline()
        inputs = pipeline._prepare_module_inputs(
            "content_intelligence", full_data_context, {}
        )
        assert "gsc_query_page" in inputs
        assert "page_data" in inputs
        assert isinstance(inputs["page_data"], pd.DataFrame)
        assert "ga4_engagement" in inputs

    def test_gameplan_inputs_with_all_modules(self, full_data_context):
        completed = {}
        for name in [
            "health_trajectory", "page_triage", "serp_landscape",
            "content_intelligence", "algorithm_impact", "intent_migration",
            "technical_health", "site_architecture", "branded_split",
            "competitive_threats", "revenue_attribution",
        ]:
            completed[name] = ModuleResult(
                module_name=name, status="success",
                data={f"{name}_data": True},
            )
        pipeline = AnalysisPipeline()
        inputs = pipeline._prepare_module_inputs(
            "gameplan", full_data_context, completed
        )
        assert "health" in inputs
        assert "triage" in inputs
        assert "serp" in inputs
        assert "content" in inputs
        assert "algorithm" in inputs
        assert "revenue" in inputs

    def test_algorithm_impact_inputs(self, full_data_context):
        completed = {
            "health_trajectory": ModuleResult(
                module_name="health_trajectory", status="success",
                data={"change_points": [{"date": "2025-03-01", "magnitude": -0.1}]},
            )
        }
        pipeline = AnalysisPipeline()
        inputs = pipeline._prepare_module_inputs(
            "algorithm_impact", full_data_context, completed
        )
        assert "daily_data" in inputs
        assert "change_points_from_module1" in inputs
        assert inputs["change_points_from_module1"] is not None

    def test_intent_migration_inputs(self, full_data_context):
        pipeline = AnalysisPipeline()
        inputs = pipeline._prepare_module_inputs(
            "intent_migration", full_data_context, {}
        )
        assert "query_timeseries" in inputs
        assert "serp_data" in inputs
        assert isinstance(inputs["serp_data"], list)

    def test_technical_health_inputs(self, full_data_context):
        pipeline = AnalysisPipeline()
        inputs = pipeline._prepare_module_inputs(
            "technical_health", full_data_context, {}
        )
        assert "gsc_coverage" in inputs
        assert "crawl_technical" in inputs

    def test_site_architecture_inputs(self, full_data_context):
        pipeline = AnalysisPipeline()
        inputs = pipeline._prepare_module_inputs(
            "site_architecture", full_data_context, {}
        )
        assert "link_graph" in inputs
        assert "page_performance" in inputs
        assert "sitemap_urls" in inputs
        assert "query_data" in inputs

    def test_branded_split_inputs(self, full_data_context):
        pipeline = AnalysisPipeline()
        inputs = pipeline._prepare_module_inputs(
            "branded_split", full_data_context, {}
        )
        assert "gsc_query_data" in inputs
        assert "brand_terms" in inputs
        assert inputs["brand_terms"] == ["example", "example.com"]

    def test_competitive_threats_inputs(self, full_data_context):
        pipeline = AnalysisPipeline()
        inputs = pipeline._prepare_module_inputs(
            "competitive_threats", full_data_context, {}
        )
        assert "serp_data" in inputs
        assert isinstance(inputs["serp_data"], list)
        assert "gsc_data" in inputs
        assert "user_domain" in inputs
        assert inputs["user_domain"] == "example.com"

    def test_revenue_attribution_inputs(self, full_data_context):
        pipeline = AnalysisPipeline()
        inputs = pipeline._prepare_module_inputs(
            "revenue_attribution", full_data_context, {}
        )
        assert "gsc_data" in inputs
        assert "ga4_conversions" in inputs
        assert "ga4_engagement" in inputs
        assert "ga4_ecommerce" in inputs


# ===================================================================
# Tests: AnalysisPipeline — error categorisation
# ===================================================================


class TestErrorCategorisation:
    """Tests for user-friendly error classification."""

    def test_insufficient_data_error(self):
        pipeline = AnalysisPipeline()
        err = ValueError("Insufficient data to compute trend")
        cat = pipeline._categorize_error(err, "health_trajectory")
        assert cat == "insufficient_data"

    def test_api_failure_error(self):
        pipeline = AnalysisPipeline()
        err = ConnectionError("API connection refused")
        cat = pipeline._categorize_error(err, "serp_landscape")
        assert cat == "api_failure"

    def test_generic_processing_error(self):
        pipeline = AnalysisPipeline()
        err = TypeError("unhashable type: 'list'")
        cat = pipeline._categorize_error(err, "content_intelligence")
        assert cat == "processing_error"

    def test_module_error_has_user_message(self):
        pipeline = AnalysisPipeline()
        err = ValueError("Not enough rows")
        error_obj = pipeline._create_module_error(
            "health_trajectory", err, "traceback..."
        )
        assert isinstance(error_obj, ModuleError)
        assert error_obj.user_message != ""
        assert "30 days" in error_obj.user_message  # insufficient_data message


# ===================================================================
# Tests: run_analysis_pipeline convenience function
# ===================================================================


class TestRunAnalysisPipeline:
    """Tests for the top-level convenience function."""

    @patch("api.worker.pipeline.AnalysisPipeline")
    def test_returns_report_data(self, MockPipeline):
        mock_instance = MagicMock()
        MockPipeline.return_value = mock_instance
        mock_result = MagicMock()
        mock_instance.execute.return_value = mock_result
        mock_instance.get_report_data.return_value = {"metadata": {}, "sections": {}}

        report = run_analysis_pipeline({"gsc_daily_data": None})
        assert "metadata" in report
        assert "sections" in report
        mock_instance.execute.assert_called_once()
        mock_instance.get_report_data.assert_called_once_with(mock_result)

    @patch("api.worker.pipeline.AnalysisPipeline")
    def test_passes_progress_callback(self, MockPipeline):
        mock_instance = MagicMock()
        MockPipeline.return_value = mock_instance
        mock_instance.execute.return_value = MagicMock()
        mock_instance.get_report_data.return_value = {}

        callback = MagicMock()
        run_analysis_pipeline({}, progress_callback=callback)
        mock_instance.execute.assert_called_once_with(
            {}, progress_callback=callback
        )


# ===================================================================
# Tests: PipelineResult and ModuleResult dataclasses
# ===================================================================


class TestDataclasses:
    """Tests for the pipeline data structures."""

    def test_module_result_defaults(self):
        r = ModuleResult(module_name="test", status="success")
        assert r.data is None
        assert r.error is None
        assert r.execution_time_seconds == 0.0

    def test_module_error_timestamp(self):
        e = ModuleError(
            module_name="test",
            error_type="ValueError",
            error_message="bad input",
            traceback="...",
        )
        assert e.timestamp is not None
        assert e.user_message == ""

    def test_pipeline_result_timestamp(self):
        r = PipelineResult(
            status="complete",
            modules=[],
            errors=[],
            total_execution_time=1.5,
        )
        assert r.completed_at is not None
        assert r.total_execution_time == 1.5


# ===================================================================
# Tests: Partial report message generation
# ===================================================================


class TestPartialReportMessage:
    """Tests for the user-facing report completion message."""

    def test_all_successful_message(self):
        pipeline = AnalysisPipeline()
        msg = pipeline._generate_partial_report_message(12, 0, 0)
        assert "All analysis sections completed successfully" in msg

    def test_partial_message_with_failures(self):
        pipeline = AnalysisPipeline()
        msg = pipeline._generate_partial_report_message(10, 2, 0)
        assert "10 of 12" in msg
        assert "2 section(s) encountered errors" in msg

    def test_partial_message_with_skips(self):
        pipeline = AnalysisPipeline()
        msg = pipeline._generate_partial_report_message(9, 1, 2)
        assert "9 of 12" in msg
        assert "1 section(s) encountered errors" in msg
        assert "2 section(s) were skipped" in msg


# ===================================================================
# Tests: Edge cases
# ===================================================================


class TestEdgeCases:
    """Tests for boundary conditions and edge cases."""

    def test_empty_data_context(self):
        """Pipeline should handle completely empty data gracefully."""
        pipeline = AnalysisPipeline()
        # Replace modules with mocks that accept any kwargs
        pipeline.modules = [
            (name, _mock_module_success(name))
            for name, _ in pipeline.modules
        ]
        result = pipeline.execute({})
        # Should complete (with mocks) even with no data
        assert result.status == "complete"

    def test_site_architecture_fallback_to_crawl_data(self):
        """Module 9 should use crawl_data when internal_link_graph is missing."""
        pipeline = AnalysisPipeline()
        data_context = {
            "crawl_data": {
                "pages": [{"url": "/a"}],
                "link_graph": {"/a": ["/b"]},
                "sitemap_urls": ["/a", "/b"],
                "stats": {},
            },
            "gsc_page_summary": pd.DataFrame({
                "page": ["/a"],
                "clicks": [10],
                "impressions": [100],
                "position": [5.0],
            }),
            "gsc_keyword_data": pd.DataFrame({
                "query": ["test"],
                "clicks": [5],
            }),
        }
        inputs = pipeline._prepare_module_inputs(
            "site_architecture", data_context, {}
        )
        assert inputs["link_graph"] is not None
        assert inputs["sitemap_urls"] == ["/a", "/b"]

    def test_serp_data_none_produces_empty_list_for_modules(self):
        """When serp_data is None, SERP-consuming modules get empty list."""
        pipeline = AnalysisPipeline()
        data_context = {"serp_data": None, "gsc_keyword_data": None}
        inputs = pipeline._prepare_module_inputs(
            "serp_landscape", data_context, {}
        )
        assert inputs["serp_data"] == []

    def test_branded_split_default_brand_terms(self):
        """brand_terms defaults to empty list when missing."""
        pipeline = AnalysisPipeline()
        data_context = {"gsc_query_data": None}
        inputs = pipeline._prepare_module_inputs(
            "branded_split", data_context, {}
        )
        assert inputs["brand_terms"] == []
