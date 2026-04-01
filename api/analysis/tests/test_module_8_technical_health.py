"""
Tests for Module 8: Technical Health Analysis

Tests the analyze_technical_health() function and all internal helpers.
Covers: Core Web Vitals assessment, indexing coverage analysis, mobile
usability analysis, crawl error classification, technical debt scoring,
and summary generation.
"""

import pytest

from api.analysis.module_8_technical_health import (
    analyze_technical_health,
    _classify_cwv_value,
    _score_cwv_metric,
    _analyze_core_web_vitals,
    _cwv_recommendation,
    _analyze_indexing_coverage,
    _analyze_mobile_usability,
    _analyze_crawl_errors,
    _compute_technical_debt_score,
    _score_to_grade,
    CWV_THRESHOLDS,
    CWV_SCORE_WEIGHTS,
    INDEX_SEVERITY,
    MOBILE_SEVERITY,
)


# ---------------------------------------------------------------------------
# Fixtures — test data generators
# ---------------------------------------------------------------------------

def _make_cwv_data(lcp=2400, inp=180, cls_val=0.08, pages=None):
    """Generate synthetic CWV data."""
    data = {
        "metrics": {
            "lcp": {"p75": lcp, "good_pct": 0.72},
            "inp": {"p75": inp, "good_pct": 0.85},
            "cls": {"p75": cls_val, "good_pct": 0.90},
        },
    }
    if pages:
        data["pages"] = pages
    return data


def _make_coverage_data(valid=1200, warning=30, excluded=450, error=12, issues=None):
    """Generate synthetic indexing coverage data."""
    return {
        "summary": {
            "valid": valid,
            "warning": warning,
            "excluded": excluded,
            "error": error,
        },
        "issues": issues or [],
    }


def _make_mobile_data(pages_with_issues=45, total_pages=1200, issues=None):
    """Generate synthetic mobile usability data."""
    return {
        "summary": {
            "pages_with_issues": pages_with_issues,
            "total_pages": total_pages,
        },
        "issues": issues or [],
    }


def _make_crawl_data(pages=None):
    """Generate synthetic crawl data."""
    if pages is None:
        pages = [
            {
                "url": f"https://example.com/page-{i}",
                "status_code": 200,
                "redirect_chain": [],
                "canonical": f"https://example.com/page-{i}",
                "meta_robots": "index,follow",
                "h1": [f"Page {i}"],
                "title": f"Page {i} | Example",
                "meta_description": f"Description for page {i}",
                "internal_links_in": 5,
                "internal_links_out": 12,
                "external_links_out": 3,
                "load_time_ms": 800,
                "content_length": 45000,
                "word_count": 800,
                "has_schema": True,
                "schema_types": ["Article"],
                "images_without_alt": 0,
                "broken_links": [],
                "mixed_content": False,
            }
            for i in range(10)
        ]
    return {"pages": pages}


# ===========================================================================
# 1. Constants Validation
# ===========================================================================

class TestConstants:
    """Validate module constants are well-formed."""

    def test_cwv_thresholds_has_core_metrics(self):
        for m in ("lcp", "inp", "cls"):
            assert m in CWV_THRESHOLDS
            assert "good" in CWV_THRESHOLDS[m]
            assert "poor" in CWV_THRESHOLDS[m]
            assert CWV_THRESHOLDS[m]["good"] < CWV_THRESHOLDS[m]["poor"]

    def test_cwv_score_weights_sum_to_one(self):
        assert abs(sum(CWV_SCORE_WEIGHTS.values()) - 1.0) < 0.001

    def test_index_severity_values_valid(self):
        valid_levels = {"critical", "high", "medium", "low"}
        for reason, level in INDEX_SEVERITY.items():
            assert level in valid_levels, f"{reason} has invalid severity {level}"

    def test_mobile_severity_values_valid(self):
        valid_levels = {"critical", "high", "medium", "low"}
        for issue_type, level in MOBILE_SEVERITY.items():
            assert level in valid_levels, f"{issue_type} has invalid severity {level}"

    def test_index_severity_nonempty(self):
        assert len(INDEX_SEVERITY) > 0

    def test_mobile_severity_nonempty(self):
        assert len(MOBILE_SEVERITY) > 0


# ===========================================================================
# 2. _classify_cwv_value Tests
# ===========================================================================

class TestClassifyCwvValue:
    """Test CWV metric classification."""

    def test_good_lcp(self):
        assert _classify_cwv_value("lcp", 2000) == "good"

    def test_good_at_threshold(self):
        assert _classify_cwv_value("lcp", 2500) == "good"

    def test_needs_improvement_lcp(self):
        assert _classify_cwv_value("lcp", 3500) == "needs_improvement"

    def test_poor_lcp(self):
        assert _classify_cwv_value("lcp", 5000) == "poor"

    def test_poor_at_threshold(self):
        # At exactly the poor threshold, it's needs_improvement (value <= poor)
        assert _classify_cwv_value("lcp", 4000) == "needs_improvement"

    def test_good_inp(self):
        assert _classify_cwv_value("inp", 100) == "good"

    def test_poor_inp(self):
        assert _classify_cwv_value("inp", 600) == "poor"

    def test_good_cls(self):
        assert _classify_cwv_value("cls", 0.05) == "good"

    def test_needs_improvement_cls(self):
        assert _classify_cwv_value("cls", 0.15) == "needs_improvement"

    def test_poor_cls(self):
        assert _classify_cwv_value("cls", 0.30) == "poor"

    def test_unknown_metric(self):
        assert _classify_cwv_value("unknown_metric", 100) == "unknown"

    def test_zero_value(self):
        assert _classify_cwv_value("lcp", 0) == "good"

    def test_fid_classification(self):
        assert _classify_cwv_value("fid", 50) == "good"
        assert _classify_cwv_value("fid", 200) == "needs_improvement"
        assert _classify_cwv_value("fid", 400) == "poor"


# ===========================================================================
# 3. _score_cwv_metric Tests
# ===========================================================================

class TestScoreCwvMetric:
    """Test CWV metric scoring (0-100)."""

    def test_perfect_score_at_good(self):
        assert _score_cwv_metric("lcp", 2500) == 100.0

    def test_perfect_score_below_good(self):
        assert _score_cwv_metric("lcp", 1000) == 100.0

    def test_zero_score_at_double_poor(self):
        assert _score_cwv_metric("lcp", 8000) == 0.0

    def test_zero_score_above_double_poor(self):
        assert _score_cwv_metric("lcp", 10000) == 0.0

    def test_midrange_score(self):
        # Between good (2500) and poor (4000), score should be 40-100
        score = _score_cwv_metric("lcp", 3250)
        assert 40 < score < 100

    def test_between_poor_and_double(self):
        # Between poor (4000) and 2*poor (8000), score should be 0-40
        score = _score_cwv_metric("lcp", 6000)
        assert 0 < score < 40

    def test_unknown_metric_returns_50(self):
        assert _score_cwv_metric("fake_metric", 100) == 50.0

    def test_score_monotonically_decreasing(self):
        """Higher values should give lower scores."""
        prev = 100.0
        for val in [0, 1000, 2500, 3000, 4000, 6000, 8000, 10000]:
            score = _score_cwv_metric("lcp", val)
            assert score <= prev
            prev = score

    def test_cls_scoring(self):
        # CLS good = 0.1, poor = 0.25
        assert _score_cwv_metric("cls", 0.05) == 100.0
        score_mid = _score_cwv_metric("cls", 0.15)
        assert 40 < score_mid < 100
        assert _score_cwv_metric("cls", 0.50) == 0.0

    def test_inp_scoring(self):
        assert _score_cwv_metric("inp", 100) == 100.0
        assert _score_cwv_metric("inp", 1000) == 0.0


# ===========================================================================
# 4. _cwv_recommendation Tests
# ===========================================================================

class TestCwvRecommendation:
    """Test CWV recommendation generation."""

    def test_poor_lcp_recommendation(self):
        rec = _cwv_recommendation("lcp", 5000, "poor")
        assert "Critical" in rec
        assert "Largest Contentful Paint" in rec
        assert "5000" in rec

    def test_needs_improvement_lcp(self):
        rec = _cwv_recommendation("lcp", 3000, "needs_improvement")
        assert "Moderate" in rec
        assert "Largest Contentful Paint" in rec

    def test_poor_inp_recommendation(self):
        rec = _cwv_recommendation("inp", 600, "poor")
        assert "Critical" in rec
        assert "Interaction to Next Paint" in rec

    def test_poor_cls_recommendation(self):
        rec = _cwv_recommendation("cls", 0.30, "poor")
        assert "Critical" in rec
        assert "Cumulative Layout Shift" in rec

    def test_unknown_metric_fallback(self):
        rec = _cwv_recommendation("unknown", 999, "poor")
        assert "Critical" in rec
        assert "unknown" in rec


# ===========================================================================
# 5. _analyze_core_web_vitals Tests
# ===========================================================================

class TestAnalyzeCoreWebVitals:
    """Test CWV analysis."""

    def test_none_input(self):
        result = _analyze_core_web_vitals(None)
        assert result["overall_score"] == 0.0
        assert result["pass"] is False
        assert len(result["recommendations"]) > 0

    def test_empty_dict_input(self):
        result = _analyze_core_web_vitals({})
        assert result["overall_score"] == 0.0

    def test_good_metrics(self):
        data = _make_cwv_data(lcp=2000, inp=150, cls_val=0.05)
        result = _analyze_core_web_vitals(data)
        assert result["pass"] is True
        assert result["overall_score"] == 100.0
        for m in ("lcp", "inp", "cls"):
            assert result["metrics"][m]["classification"] == "good"

    def test_poor_metrics(self):
        data = _make_cwv_data(lcp=5000, inp=600, cls_val=0.30)
        result = _analyze_core_web_vitals(data)
        assert result["pass"] is False
        assert result["overall_score"] < 50
        assert len(result["recommendations"]) >= 3

    def test_mixed_metrics(self):
        data = _make_cwv_data(lcp=2000, inp=600, cls_val=0.05)
        result = _analyze_core_web_vitals(data)
        assert result["pass"] is False  # inp is poor
        assert result["overall_score"] < 100

    def test_flat_data_format(self):
        """Test fallback when data is flat (not nested in metrics key)."""
        data = {"lcp": 2000, "inp": 150, "cls": 0.05}
        result = _analyze_core_web_vitals(data)
        assert result["overall_score"] > 0

    def test_metric_as_scalar(self):
        """Test when metric value is a plain number, not a dict."""
        data = {"metrics": {"lcp": 2000, "inp": 150, "cls": 0.05}}
        result = _analyze_core_web_vitals(data)
        assert result["metrics"]["lcp"]["p75"] == 2000.0

    def test_metric_with_value_key(self):
        """Test when metric dict uses 'value' key instead of 'p75'."""
        data = {"metrics": {"lcp": {"value": 2000}, "inp": {"value": 150}, "cls": {"value": 0.05}}}
        result = _analyze_core_web_vitals(data)
        assert result["metrics"]["lcp"]["p75"] == 2000.0

    def test_no_data_metric(self):
        """Metric present but no p75 or value."""
        data = {"metrics": {"lcp": {}, "inp": {"p75": 150}, "cls": {"p75": 0.05}}}
        result = _analyze_core_web_vitals(data)
        assert result["metrics"]["lcp"]["status"] == "no_data"

    def test_page_level_analysis(self):
        pages = [
            {"url": "/fast", "lcp": 1500, "inp": 100, "cls": 0.02},
            {"url": "/slow", "lcp": 6000, "inp": 700, "cls": 0.40},
        ]
        data = _make_cwv_data(pages=pages)
        result = _analyze_core_web_vitals(data)
        assert len(result["page_level"]) == 2
        # Poor pages should be first
        assert result["page_level"][0]["worst_classification"] == "poor"

    def test_page_level_capped_at_30(self):
        pages = [{"url": f"/p-{i}", "lcp": 5000} for i in range(50)]
        data = _make_cwv_data(pages=pages)
        result = _analyze_core_web_vitals(data)
        assert len(result["page_level"]) <= 30

    def test_good_pct_preserved(self):
        data = _make_cwv_data(lcp=2000, inp=150, cls_val=0.05)
        result = _analyze_core_web_vitals(data)
        assert result["metrics"]["lcp"]["good_pct"] == 0.72

    def test_pass_ignores_no_data_metrics(self):
        """Pass should not be blocked by metrics with no data."""
        data = {"metrics": {"lcp": {"p75": 2000}, "cls": {"p75": 0.05}}}
        result = _analyze_core_web_vitals(data)
        # inp has no data, so pass should be based on lcp + cls only
        assert result["pass"] is True

    def test_page_level_good_pages(self):
        pages = [{"url": "/ok", "lcp": 1500, "inp": 100, "cls": 0.02}]
        data = _make_cwv_data(pages=pages)
        result = _analyze_core_web_vitals(data)
        assert result["page_level"][0]["worst_classification"] == "good"
        assert result["page_level"][0]["worst_metric"] is None

    def test_page_with_page_path_key(self):
        pages = [{"page_path": "/alt-key", "lcp": 5000}]
        data = _make_cwv_data(pages=pages)
        result = _analyze_core_web_vitals(data)
        assert result["page_level"][0]["url"] == "/alt-key"


# ===========================================================================
# 6. _analyze_indexing_coverage Tests
# ===========================================================================

class TestAnalyzeIndexingCoverage:
    """Test indexing coverage analysis."""

    def test_none_input(self):
        result = _analyze_indexing_coverage(None)
        assert result["index_ratio"] == 0.0
        assert len(result["recommendations"]) > 0

    def test_empty_dict(self):
        result = _analyze_indexing_coverage({})
        assert result["summary"]["total"] == 0

    def test_healthy_coverage(self):
        data = _make_coverage_data(valid=900, warning=10, excluded=80, error=10)
        result = _analyze_indexing_coverage(data)
        assert result["index_ratio"] == 0.9
        assert result["summary"]["valid"] == 900
        assert result["summary"]["total"] == 1000

    def test_low_index_ratio_recommendation(self):
        data = _make_coverage_data(valid=20, warning=5, excluded=50, error=25)
        result = _analyze_indexing_coverage(data)
        assert result["index_ratio"] < 0.5
        recs = " ".join(result["recommendations"])
        assert "indexed" in recs.lower()

    def test_errors_generate_recommendation(self):
        data = _make_coverage_data(error=15)
        result = _analyze_indexing_coverage(data)
        recs = " ".join(result["recommendations"])
        assert "15" in recs

    def test_issues_classified_by_severity(self):
        issues = [
            {"reason": "server_error", "count": 5, "urls": ["/a"]},
            {"reason": "noindex", "count": 20, "urls": ["/b"]},
            {"reason": "alternate_page", "count": 100, "urls": ["/c"]},
        ]
        data = _make_coverage_data(issues=issues)
        result = _analyze_indexing_coverage(data)
        assert len(result["issues_by_severity"]["critical"]) == 1
        assert len(result["issues_by_severity"]["high"]) == 1
        assert len(result["issues_by_severity"]["low"]) == 1

    def test_top_issues_sorted_by_count(self):
        issues = [
            {"reason": "noindex", "count": 10},
            {"reason": "server_error", "count": 50},
            {"reason": "alternate_page", "count": 5},
        ]
        data = _make_coverage_data(issues=issues)
        result = _analyze_indexing_coverage(data)
        assert result["top_issues"][0]["count"] == 50

    def test_top_issues_capped_at_15(self):
        issues = [{"reason": "noindex", "count": i} for i in range(20)]
        data = _make_coverage_data(issues=issues)
        result = _analyze_indexing_coverage(data)
        assert len(result["top_issues"]) <= 15

    def test_crawled_not_indexed_recommendation(self):
        issues = [{"reason": "crawled_not_indexed", "count": 25}]
        data = _make_coverage_data(issues=issues)
        result = _analyze_indexing_coverage(data)
        recs = " ".join(result["recommendations"])
        assert "crawled but not indexed" in recs.lower()

    def test_sample_urls_capped_at_5(self):
        issues = [{"reason": "noindex", "count": 10, "urls": [f"/p{i}" for i in range(10)]}]
        data = _make_coverage_data(issues=issues)
        result = _analyze_indexing_coverage(data)
        for issue in result["top_issues"]:
            assert len(issue["sample_urls"]) <= 5

    def test_unknown_reason_defaults_to_low(self):
        issues = [{"reason": "some_new_reason", "count": 5}]
        data = _make_coverage_data(issues=issues)
        result = _analyze_indexing_coverage(data)
        assert result["issues_by_severity"]["low"][0]["reason"] == "some_new_reason"

    def test_zero_total_no_division_error(self):
        data = _make_coverage_data(valid=0, warning=0, excluded=0, error=0)
        result = _analyze_indexing_coverage(data)
        assert result["index_ratio"] == 0.0


# ===========================================================================
# 7. _analyze_mobile_usability Tests
# ===========================================================================

class TestAnalyzeMobileUsability:
    """Test mobile usability analysis."""

    def test_none_input(self):
        result = _analyze_mobile_usability(None)
        assert result["mobile_friendly_pct"] == 100.0
        assert len(result["recommendations"]) > 0

    def test_empty_dict(self):
        result = _analyze_mobile_usability({})
        assert result["total_pages"] == 0

    def test_no_issues(self):
        data = _make_mobile_data(pages_with_issues=0, total_pages=1000)
        result = _analyze_mobile_usability(data)
        assert result["mobile_friendly_pct"] == 100.0
        assert result["pages_with_issues"] == 0

    def test_some_issues(self):
        data = _make_mobile_data(pages_with_issues=100, total_pages=1000)
        result = _analyze_mobile_usability(data)
        assert result["mobile_friendly_pct"] == 90.0

    def test_critical_issues_recommendation(self):
        issues = [{"type": "viewport_not_set", "count": 10}]
        data = _make_mobile_data(pages_with_issues=10, total_pages=100, issues=issues)
        result = _analyze_mobile_usability(data)
        recs = " ".join(result["recommendations"])
        assert "viewport_not_set" in recs

    def test_low_mobile_pct_recommendation(self):
        data = _make_mobile_data(pages_with_issues=200, total_pages=1000)
        result = _analyze_mobile_usability(data)
        assert result["mobile_friendly_pct"] == 80.0
        recs = " ".join(result["recommendations"])
        assert "80.0%" in recs

    def test_issues_sorted_by_severity(self):
        issues = [
            {"type": "text_too_small", "count": 30},  # medium
            {"type": "viewport_not_set", "count": 5},  # critical
            {"type": "content_wider_than_screen", "count": 10},  # high
        ]
        data = _make_mobile_data(pages_with_issues=45, total_pages=1000, issues=issues)
        result = _analyze_mobile_usability(data)
        assert result["issues"][0]["severity"] == "critical"
        assert result["issues"][1]["severity"] == "high"

    def test_unknown_issue_type_defaults_to_low(self):
        issues = [{"type": "new_mobile_issue", "count": 5}]
        data = _make_mobile_data(pages_with_issues=5, total_pages=100, issues=issues)
        result = _analyze_mobile_usability(data)
        assert result["issues"][0]["severity"] == "low"

    def test_sample_urls_capped_at_5(self):
        issues = [{"type": "text_too_small", "count": 10, "urls": [f"/p{i}" for i in range(10)]}]
        data = _make_mobile_data(pages_with_issues=10, total_pages=100, issues=issues)
        result = _analyze_mobile_usability(data)
        assert len(result["issues"][0]["sample_urls"]) <= 5

    def test_zero_total_pages(self):
        data = _make_mobile_data(pages_with_issues=0, total_pages=0)
        result = _analyze_mobile_usability(data)
        assert result["mobile_friendly_pct"] == 100.0


# ===========================================================================
# 8. _analyze_crawl_errors Tests
# ===========================================================================

class TestAnalyzeCrawlErrors:
    """Test crawl error analysis."""

    def test_none_input(self):
        result = _analyze_crawl_errors(None)
        assert result["total_pages_crawled"] == 0
        assert len(result["recommendations"]) > 0

    def test_empty_dict(self):
        result = _analyze_crawl_errors({})
        assert result["total_pages_crawled"] == 0

    def test_empty_pages_list(self):
        result = _analyze_crawl_errors({"pages": []})
        assert result["total_pages_crawled"] == 0
        assert "zero pages" in result["recommendations"][0].lower()

    def test_healthy_crawl(self):
        data = _make_crawl_data()
        result = _analyze_crawl_errors(data)
        assert result["total_pages_crawled"] == 10
        assert result["status_code_distribution"][200] == 10
        assert len(result["broken_links"]) == 0

    def test_status_code_distribution(self):
        pages = [
            {"url": "/ok", "status_code": 200, "title": "OK", "h1": ["OK"], "meta_description": "desc"},
            {"url": "/not-found", "status_code": 404, "title": "NF", "h1": ["NF"], "meta_description": "desc"},
            {"url": "/error", "status_code": 500, "title": "Err", "h1": ["Err"], "meta_description": "desc"},
            {"url": "/redirect", "status_code": 301, "title": "R", "h1": ["R"], "meta_description": "desc"},
        ]
        result = _analyze_crawl_errors({"pages": pages})
        assert result["status_code_distribution"][200] == 1
        assert result["status_code_distribution"][404] == 1
        assert result["status_code_distribution"][500] == 1

    def test_error_pages_recommendation(self):
        pages = [
            {"url": f"/p{i}", "status_code": 404, "title": "T", "h1": ["H"], "meta_description": "d"}
            for i in range(5)
        ]
        result = _analyze_crawl_errors({"pages": pages})
        recs = " ".join(result["recommendations"])
        assert "4xx/5xx" in recs

    def test_redirect_chains_detected(self):
        pages = [
            {
                "url": "/page",
                "status_code": 200,
                "redirect_chain": ["/a", "/b", "/c"],
                "title": "T",
                "h1": ["H"],
                "meta_description": "d",
            }
        ]
        result = _analyze_crawl_errors({"pages": pages})
        assert len(result["redirect_issues"]) == 1
        assert result["redirect_issues"][0]["hops"] == 3

    def test_long_redirect_chains_recommendation(self):
        pages = [
            {
                "url": f"/page-{i}",
                "status_code": 200,
                "redirect_chain": ["/a", "/b", "/c", "/d"],
                "title": "T",
                "h1": ["H"],
                "meta_description": "d",
            }
            for i in range(3)
        ]
        result = _analyze_crawl_errors({"pages": pages})
        recs = " ".join(result["recommendations"])
        assert "redirect chains" in recs.lower()

    def test_redirect_issues_capped_at_20(self):
        pages = [
            {
                "url": f"/p{i}",
                "status_code": 200,
                "redirect_chain": ["/a", "/b", "/c"],
                "title": "T",
                "h1": ["H"],
                "meta_description": "d",
            }
            for i in range(30)
        ]
        result = _analyze_crawl_errors({"pages": pages})
        assert len(result["redirect_issues"]) <= 20

    def test_canonical_mismatch_detected(self):
        pages = [
            {
                "url": "https://example.com/page",
                "status_code": 200,
                "canonical": "https://example.com/other-page",
                "title": "T",
                "h1": ["H"],
                "meta_description": "d",
            }
        ]
        result = _analyze_crawl_errors({"pages": pages})
        assert len(result["canonical_issues"]) == 1

    def test_canonical_match_no_issue(self):
        pages = [
            {
                "url": "https://example.com/page",
                "status_code": 200,
                "canonical": "https://example.com/page",
                "title": "T",
                "h1": ["H"],
                "meta_description": "d",
            }
        ]
        result = _analyze_crawl_errors({"pages": pages})
        assert len(result["canonical_issues"]) == 0

    def test_canonical_mismatch_only_200(self):
        """Canonical mismatch should only flag 200-status pages."""
        pages = [
            {
                "url": "https://example.com/page",
                "status_code": 301,
                "canonical": "https://example.com/other",
                "title": "T",
                "h1": ["H"],
                "meta_description": "d",
            }
        ]
        result = _analyze_crawl_errors({"pages": pages})
        assert len(result["canonical_issues"]) == 0

    def test_missing_title(self):
        pages = [{"url": "/p", "status_code": 200, "title": "", "h1": ["H"], "meta_description": "d"}]
        result = _analyze_crawl_errors({"pages": pages})
        assert "/p" in result["missing_meta"]["no_title"]

    def test_missing_description(self):
        pages = [{"url": "/p", "status_code": 200, "title": "T", "h1": ["H"], "meta_description": ""}]
        result = _analyze_crawl_errors({"pages": pages})
        assert "/p" in result["missing_meta"]["no_description"]

    def test_missing_h1(self):
        pages = [{"url": "/p", "status_code": 200, "title": "T", "h1": [], "meta_description": "d"}]
        result = _analyze_crawl_errors({"pages": pages})
        assert "/p" in result["missing_meta"]["no_h1"]

    def test_missing_title_recommendation(self):
        pages = [
            {"url": f"/p{i}", "status_code": 200, "title": "", "h1": ["H"], "meta_description": "d"}
            for i in range(10)
        ]
        result = _analyze_crawl_errors({"pages": pages})
        recs = " ".join(result["recommendations"])
        assert "title" in recs.lower()

    def test_missing_description_recommendation(self):
        pages = [
            {"url": f"/p{i}", "status_code": 200, "title": "T", "h1": ["H"], "meta_description": ""}
            for i in range(15)
        ]
        result = _analyze_crawl_errors({"pages": pages})
        recs = " ".join(result["recommendations"])
        assert "meta description" in recs.lower()

    def test_broken_links_detected(self):
        pages = [
            {
                "url": "/source",
                "status_code": 200,
                "broken_links": ["/dead1", "/dead2"],
                "title": "T",
                "h1": ["H"],
                "meta_description": "d",
            }
        ]
        result = _analyze_crawl_errors({"pages": pages})
        assert len(result["broken_links"]) == 2

    def test_broken_links_deduplicated(self):
        pages = [
            {
                "url": "/source",
                "status_code": 200,
                "broken_links": ["/dead", "/dead"],
                "title": "T",
                "h1": ["H"],
                "meta_description": "d",
            }
        ]
        result = _analyze_crawl_errors({"pages": pages})
        assert len(result["broken_links"]) == 1

    def test_broken_links_capped_at_30(self):
        pages = [
            {
                "url": f"/src{i}",
                "status_code": 200,
                "broken_links": [f"/dead{i}"],
                "title": "T",
                "h1": ["H"],
                "meta_description": "d",
            }
            for i in range(40)
        ]
        result = _analyze_crawl_errors({"pages": pages})
        assert len(result["broken_links"]) <= 30

    def test_broken_links_recommendation(self):
        pages = [
            {
                "url": f"/src{i}",
                "status_code": 200,
                "broken_links": [f"/dead{i}"],
                "title": "T",
                "h1": ["H"],
                "meta_description": "d",
            }
            for i in range(10)
        ]
        result = _analyze_crawl_errors({"pages": pages})
        recs = " ".join(result["recommendations"])
        assert "broken" in recs.lower()

    def test_slow_pages_detected(self):
        pages = [
            {"url": "/slow", "status_code": 200, "load_time_ms": 5000, "title": "T", "h1": ["H"], "meta_description": "d"}
        ]
        result = _analyze_crawl_errors({"pages": pages})
        assert len(result["performance_issues"]) == 1
        assert result["performance_issues"][0]["load_time_ms"] == 5000

    def test_slow_pages_sorted_desc(self):
        pages = [
            {"url": "/slow1", "status_code": 200, "load_time_ms": 4000, "title": "T", "h1": ["H"], "meta_description": "d"},
            {"url": "/slow2", "status_code": 200, "load_time_ms": 8000, "title": "T", "h1": ["H"], "meta_description": "d"},
        ]
        result = _analyze_crawl_errors({"pages": pages})
        assert result["performance_issues"][0]["load_time_ms"] == 8000

    def test_performance_issues_capped_at_20(self):
        pages = [
            {"url": f"/p{i}", "status_code": 200, "load_time_ms": 5000, "title": "T", "h1": ["H"], "meta_description": "d"}
            for i in range(30)
        ]
        result = _analyze_crawl_errors({"pages": pages})
        assert len(result["performance_issues"]) <= 20

    def test_schema_coverage(self):
        pages = [
            {"url": "/with", "status_code": 200, "has_schema": True, "schema_types": ["Article", "FAQ"], "title": "T", "h1": ["H"], "meta_description": "d"},
            {"url": "/without", "status_code": 200, "has_schema": False, "title": "T", "h1": ["H"], "meta_description": "d"},
        ]
        result = _analyze_crawl_errors({"pages": pages})
        assert result["schema_coverage"]["with_schema"] == 1
        assert result["schema_coverage"]["without_schema"] == 1
        assert "Article" in result["schema_coverage"]["types"]

    def test_low_schema_coverage_recommendation(self):
        pages = [
            {"url": f"/p{i}", "status_code": 200, "has_schema": False, "title": "T", "h1": ["H"], "meta_description": "d"}
            for i in range(10)
        ]
        result = _analyze_crawl_errors({"pages": pages})
        recs = " ".join(result["recommendations"])
        assert "schema" in recs.lower() or "structured data" in recs.lower()

    def test_images_without_alt(self):
        pages = [
            {"url": "/p1", "status_code": 200, "images_without_alt": 5, "title": "T", "h1": ["H"], "meta_description": "d"},
            {"url": "/p2", "status_code": 200, "images_without_alt": 0, "title": "T", "h1": ["H"], "meta_description": "d"},
        ]
        result = _analyze_crawl_errors({"pages": pages})
        assert len(result["accessibility_issues"]) == 1
        assert result["accessibility_issues"][0]["count"] == 5

    def test_slow_pages_recommendation(self):
        pages = [
            {"url": f"/p{i}", "status_code": 200, "load_time_ms": 5000, "title": "T", "h1": ["H"], "meta_description": "d"}
            for i in range(3)
        ]
        result = _analyze_crawl_errors({"pages": pages})
        recs = " ".join(result["recommendations"])
        assert "3 seconds" in recs or "3 pages" in recs.lower() or "load" in recs.lower()

    def test_missing_meta_capped_at_20(self):
        pages = [
            {"url": f"/p{i}", "status_code": 200, "title": "", "h1": [], "meta_description": ""}
            for i in range(30)
        ]
        result = _analyze_crawl_errors({"pages": pages})
        assert len(result["missing_meta"]["no_title"]) <= 20
        assert len(result["missing_meta"]["no_description"]) <= 20
        assert len(result["missing_meta"]["no_h1"]) <= 20


# ===========================================================================
# 9. _score_to_grade Tests
# ===========================================================================

class TestScoreToGrade:
    """Test grade assignment."""

    def test_grade_a(self):
        assert _score_to_grade(95) == "A"
        assert _score_to_grade(90) == "A"

    def test_grade_b(self):
        assert _score_to_grade(85) == "B"
        assert _score_to_grade(80) == "B"

    def test_grade_c(self):
        assert _score_to_grade(70) == "C"
        assert _score_to_grade(65) == "C"

    def test_grade_d(self):
        assert _score_to_grade(55) == "D"
        assert _score_to_grade(50) == "D"

    def test_grade_f(self):
        assert _score_to_grade(40) == "F"
        assert _score_to_grade(0) == "F"

    def test_boundary_values(self):
        assert _score_to_grade(89.9) == "B"
        assert _score_to_grade(79.9) == "C"
        assert _score_to_grade(64.9) == "D"
        assert _score_to_grade(49.9) == "F"


# ===========================================================================
# 10. _compute_technical_debt_score Tests
# ===========================================================================

class TestComputeTechnicalDebtScore:
    """Test composite debt scoring."""

    def test_perfect_scores(self):
        cwv = {"overall_score": 100.0, "pass": True}
        indexing = {"index_ratio": 1.0, "issues_by_severity": {"critical": [], "high": []}}
        mobile = {"mobile_friendly_pct": 100.0, "issues": []}
        crawl = {
            "total_pages_crawled": 100,
            "status_code_distribution": {200: 100},
            "broken_links": [],
            "redirect_issues": [],
            "missing_meta": {"no_title": []},
        }
        result = _compute_technical_debt_score(cwv, indexing, mobile, crawl)
        assert result["total_score"] >= 95
        assert result["grade"] == "A"

    def test_terrible_scores(self):
        cwv = {"overall_score": 0.0, "pass": False}
        indexing = {
            "index_ratio": 0.1,
            "issues_by_severity": {
                "critical": [{"reason": "x"}] * 5,
                "high": [{"reason": "y"}] * 5,
            },
        }
        mobile = {"mobile_friendly_pct": 50.0, "issues": [{"severity": "critical"}] * 3}
        crawl = {
            "total_pages_crawled": 100,
            "status_code_distribution": {404: 50, 200: 50},
            "broken_links": list(range(20)),
            "redirect_issues": list(range(10)),
            "missing_meta": {"no_title": list(range(15))},
        }
        result = _compute_technical_debt_score(cwv, indexing, mobile, crawl)
        assert result["total_score"] < 40
        assert result["grade"] in ("D", "F")

    def test_dimensions_present(self):
        cwv = {"overall_score": 50.0, "pass": False}
        indexing = {"index_ratio": 0.5, "issues_by_severity": {"critical": [], "high": []}}
        mobile = {"mobile_friendly_pct": 80.0, "issues": []}
        crawl = {
            "total_pages_crawled": 50,
            "status_code_distribution": {200: 50},
            "broken_links": [],
            "redirect_issues": [],
            "missing_meta": {"no_title": []},
        }
        result = _compute_technical_debt_score(cwv, indexing, mobile, crawl)
        dims = result["dimensions"]
        assert "core_web_vitals" in dims
        assert "indexing_coverage" in dims
        assert "mobile_usability" in dims
        assert "crawl_health" in dims

    def test_max_scores_correct(self):
        cwv = {"overall_score": 100.0, "pass": True}
        indexing = {"index_ratio": 1.0, "issues_by_severity": {"critical": [], "high": []}}
        mobile = {"mobile_friendly_pct": 100.0, "issues": []}
        crawl = {
            "total_pages_crawled": 10,
            "status_code_distribution": {200: 10},
            "broken_links": [],
            "redirect_issues": [],
            "missing_meta": {"no_title": []},
        }
        result = _compute_technical_debt_score(cwv, indexing, mobile, crawl)
        dims = result["dimensions"]
        assert dims["core_web_vitals"]["max"] == 30
        assert dims["indexing_coverage"]["max"] == 25
        assert dims["mobile_usability"]["max"] == 20
        assert dims["crawl_health"]["max"] == 25

    def test_no_crawl_data_neutral(self):
        cwv = {"overall_score": 100.0, "pass": True}
        indexing = {"index_ratio": 1.0, "issues_by_severity": {"critical": [], "high": []}}
        mobile = {"mobile_friendly_pct": 100.0, "issues": []}
        crawl = {
            "total_pages_crawled": 0,
            "status_code_distribution": {},
            "broken_links": [],
            "redirect_issues": [],
            "missing_meta": {"no_title": []},
        }
        result = _compute_technical_debt_score(cwv, indexing, mobile, crawl)
        assert result["dimensions"]["crawl_health"]["raw"] == 50.0

    def test_critical_mobile_penalty(self):
        cwv = {"overall_score": 100.0, "pass": True}
        indexing = {"index_ratio": 1.0, "issues_by_severity": {"critical": [], "high": []}}
        mobile_good = {"mobile_friendly_pct": 100.0, "issues": []}
        mobile_bad = {"mobile_friendly_pct": 100.0, "issues": [{"severity": "critical"}] * 3}
        crawl = {
            "total_pages_crawled": 10,
            "status_code_distribution": {200: 10},
            "broken_links": [],
            "redirect_issues": [],
            "missing_meta": {"no_title": []},
        }
        score_good = _compute_technical_debt_score(cwv, indexing, mobile_good, crawl)
        score_bad = _compute_technical_debt_score(cwv, indexing, mobile_bad, crawl)
        assert score_bad["total_score"] < score_good["total_score"]

    def test_indexing_issues_penalty(self):
        cwv = {"overall_score": 100.0, "pass": True}
        indexing_clean = {"index_ratio": 0.9, "issues_by_severity": {"critical": [], "high": []}}
        indexing_bad = {
            "index_ratio": 0.9,
            "issues_by_severity": {
                "critical": [{"reason": "x"}] * 3,
                "high": [{"reason": "y"}] * 5,
            },
        }
        mobile = {"mobile_friendly_pct": 100.0, "issues": []}
        crawl = {
            "total_pages_crawled": 10,
            "status_code_distribution": {200: 10},
            "broken_links": [],
            "redirect_issues": [],
            "missing_meta": {"no_title": []},
        }
        clean = _compute_technical_debt_score(cwv, indexing_clean, mobile, crawl)
        bad = _compute_technical_debt_score(cwv, indexing_bad, mobile, crawl)
        assert bad["total_score"] < clean["total_score"]


# ===========================================================================
# 11. Full Pipeline — analyze_technical_health Tests
# ===========================================================================

class TestAnalyzeTechnicalHealth:
    """Test the main entry point."""

    def test_all_none_inputs(self):
        result = analyze_technical_health()
        assert "summary" in result
        assert "technical_score" in result
        assert "core_web_vitals" in result
        assert "indexing_coverage" in result
        assert "mobile_usability" in result
        assert "crawl_health" in result
        assert "all_recommendations" in result
        assert "priority_fixes" in result

    def test_output_schema_keys(self):
        result = analyze_technical_health()
        assert isinstance(result["summary"], str)
        assert isinstance(result["technical_score"], dict)
        assert isinstance(result["all_recommendations"], list)
        assert isinstance(result["priority_fixes"], list)

    def test_with_good_data(self):
        cwv = _make_cwv_data(lcp=2000, inp=150, cls_val=0.05)
        coverage = _make_coverage_data(valid=900, warning=10, excluded=80, error=10)
        mobile = _make_mobile_data(pages_with_issues=0, total_pages=1000)
        crawl = _make_crawl_data()
        result = analyze_technical_health(cwv, coverage, mobile, crawl)
        assert result["technical_score"]["grade"] in ("A", "B")
        assert "PASSING" in result["summary"]

    def test_with_poor_data(self):
        cwv = _make_cwv_data(lcp=6000, inp=700, cls_val=0.35)
        coverage = _make_coverage_data(valid=100, warning=5, excluded=500, error=50)
        mobile = _make_mobile_data(
            pages_with_issues=200,
            total_pages=500,
            issues=[{"type": "viewport_not_set", "count": 50}],
        )
        pages = [
            {"url": f"/p{i}", "status_code": 404, "title": "", "h1": [], "meta_description": "", "broken_links": ["/dead"], "load_time_ms": 5000, "has_schema": False}
            for i in range(20)
        ]
        crawl = {"pages": pages}
        result = analyze_technical_health(cwv, coverage, mobile, crawl)
        assert result["technical_score"]["grade"] in ("D", "F")
        assert "FAILING" in result["summary"]
        assert len(result["all_recommendations"]) > 3

    def test_priority_fixes_capped_at_5(self):
        cwv = _make_cwv_data(lcp=6000, inp=700, cls_val=0.35)
        coverage = _make_coverage_data(valid=10, warning=5, excluded=500, error=50, issues=[
            {"reason": "crawled_not_indexed", "count": 100},
        ])
        mobile = _make_mobile_data(
            pages_with_issues=200,
            total_pages=500,
            issues=[{"type": "viewport_not_set", "count": 50}],
        )
        pages = [
            {"url": f"/p{i}", "status_code": 404, "title": "", "h1": [], "meta_description": "", "broken_links": ["/dead"], "load_time_ms": 5000, "has_schema": False}
            for i in range(20)
        ]
        crawl = {"pages": pages}
        result = analyze_technical_health(cwv, coverage, mobile, crawl)
        assert len(result["priority_fixes"]) <= 5

    def test_summary_includes_score(self):
        result = analyze_technical_health()
        assert "Technical Health Score" in result["summary"]
        assert "Grade" in result["summary"]

    def test_summary_includes_indexing(self):
        coverage = _make_coverage_data(valid=500, warning=10, excluded=200, error=5)
        result = analyze_technical_health(gsc_coverage=coverage)
        assert "Indexing" in result["summary"]
        assert "500" in result["summary"]

    def test_summary_mobile_no_issues(self):
        mobile = _make_mobile_data(pages_with_issues=0, total_pages=1000)
        result = analyze_technical_health(gsc_mobile=mobile)
        assert "No issues detected" in result["summary"]

    def test_summary_mobile_with_issues(self):
        mobile = _make_mobile_data(pages_with_issues=10, total_pages=1000)
        result = analyze_technical_health(gsc_mobile=mobile)
        assert "10 pages with issues" in result["summary"]

    def test_summary_crawl_info(self):
        crawl = _make_crawl_data()
        result = analyze_technical_health(crawl_technical=crawl)
        assert "10 pages analysed" in result["summary"]

    def test_all_recommendations_aggregated(self):
        cwv = _make_cwv_data(lcp=6000, inp=700, cls_val=0.35)
        coverage = _make_coverage_data(error=20)
        result = analyze_technical_health(ga4_cwv_data=cwv, gsc_coverage=coverage)
        # Should have CWV recs + indexing recs at minimum
        assert len(result["all_recommendations"]) >= 4


# ===========================================================================
# 12. Edge Cases
# ===========================================================================

class TestEdgeCases:
    """Edge case coverage."""

    def test_page_with_no_optional_fields(self):
        """Crawl page with only url and status_code."""
        pages = [{"url": "/minimal", "status_code": 200}]
        result = _analyze_crawl_errors({"pages": pages})
        assert result["total_pages_crawled"] == 1

    def test_page_with_none_title(self):
        pages = [{"url": "/p", "status_code": 200, "title": None, "h1": None, "meta_description": None}]
        result = _analyze_crawl_errors({"pages": pages})
        # None title should be treated as missing
        assert "/p" in result["missing_meta"]["no_title"]

    def test_whitespace_only_title(self):
        pages = [{"url": "/p", "status_code": 200, "title": "   ", "h1": ["H"], "meta_description": "d"}]
        result = _analyze_crawl_errors({"pages": pages})
        assert "/p" in result["missing_meta"]["no_title"]

    def test_non_dict_cwv_input(self):
        result = _analyze_core_web_vitals("not a dict")
        assert result["overall_score"] == 0.0
        assert len(result["recommendations"]) > 0

    def test_non_dict_coverage_input(self):
        result = _analyze_indexing_coverage("not a dict")
        assert result["index_ratio"] == 0.0

    def test_non_dict_mobile_input(self):
        result = _analyze_mobile_usability("not a dict")
        assert result["mobile_friendly_pct"] == 100.0

    def test_non_dict_crawl_input(self):
        result = _analyze_crawl_errors("not a dict")
        assert result["total_pages_crawled"] == 0

    def test_very_large_page_count(self):
        """Crawl with 300 pages to test caps."""
        pages = [
            {
                "url": f"/p{i}",
                "status_code": 200,
                "title": "T",
                "h1": ["H"],
                "meta_description": "d",
                "has_schema": True,
                "schema_types": ["Article"],
            }
            for i in range(300)
        ]
        result = _analyze_crawl_errors({"pages": pages})
        assert result["total_pages_crawled"] == 300

    def test_cwv_data_with_extra_metrics(self):
        """CWV data includes fcp and ttfb — should not crash."""
        data = {
            "metrics": {
                "lcp": {"p75": 2000},
                "inp": {"p75": 150},
                "cls": {"p75": 0.05},
                "fcp": {"p75": 1500},
                "ttfb": {"p75": 600},
            }
        }
        result = _analyze_core_web_vitals(data)
        assert result["pass"] is True
        # fcp/ttfb are not in the main 3 metrics loop but shouldn't cause errors

    def test_debt_score_always_0_to_100(self):
        """Debt score should never exceed range."""
        result = analyze_technical_health()
        assert 0 <= result["technical_score"]["total_score"] <= 100

    def test_redirect_chain_single_hop_not_issue(self):
        """A single-hop redirect chain should NOT be flagged."""
        pages = [
            {
                "url": "/page",
                "status_code": 200,
                "redirect_chain": ["/a"],
                "title": "T",
                "h1": ["H"],
                "meta_description": "d",
            }
        ]
        result = _analyze_crawl_errors({"pages": pages})
        assert len(result["redirect_issues"]) == 0

    def test_zero_load_time_not_flagged(self):
        pages = [
            {"url": "/p", "status_code": 200, "load_time_ms": 0, "title": "T", "h1": ["H"], "meta_description": "d"}
        ]
        result = _analyze_crawl_errors({"pages": pages})
        assert len(result["performance_issues"]) == 0

    def test_schema_types_capped_at_10(self):
        pages = [
            {
                "url": f"/p{i}",
                "status_code": 200,
                "has_schema": True,
                "schema_types": [f"Type{i}"],
                "title": "T",
                "h1": ["H"],
                "meta_description": "d",
            }
            for i in range(20)
        ]
        result = _analyze_crawl_errors({"pages": pages})
        assert len(result["schema_coverage"]["types"]) <= 10
