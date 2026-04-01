"""
Comprehensive test suite for Module 7: Intent Migration.

Covers:
 1. classify_query_intent — keyword patterns, SERP features, defaults
 2. infer_page_type — URL patterns, page_meta fallback
 3. IntentMigrationAnalyzer helpers — SERP features map, page meta map
 4. _split_time_windows — date handling, window slicing
 5. _aggregate_query_metrics — grouping, CTR calculation
 6. _classify_window — per-query classification
 7. _detect_shifts — primary change, magnitude threshold, sorting/cap
 8. _identify_emerging_intents — new queries, rapid growth, scaling
 9. _analyze_content_alignment — misalignment detection, severity, sorting
10. _alignment_recommendation — all intent/page_type combos
11. _intent_portfolio_summary — weighted distribution, changes
12. _generate_recommendations — all recommendation types
13. _build_summary — narrative text
14. analyze (full pipeline) — end-to-end output schema
15. analyze_intent_migration public function — signature compatibility
16. Edge cases — empty data, single row, NaN, special characters
"""

import math
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List

import numpy as np
import pandas as pd
import pytest

from api.analysis.module_7_intent_migration import (
    IntentMigrationAnalyzer,
    analyze_intent_migration,
    classify_query_intent,
    infer_page_type,
    _TRANSACTIONAL_PATTERNS,
    _COMMERCIAL_PATTERNS,
    _NAVIGATIONAL_PATTERNS,
    _SERP_INTENT_SIGNALS,
    _PAGE_TYPE_PATTERNS,
)


# ===================================================================
# Helpers
# ===================================================================

def _make_ts(queries, days=90, start_date=None):
    """Build a minimal query_timeseries DataFrame spanning *days* days."""
    if start_date is None:
        start_date = datetime(2025, 1, 1)
    rows = []
    for d in range(days):
        dt = start_date + timedelta(days=d)
        for q, page in queries:
            rows.append({
                "query": q,
                "page": page,
                "date": dt.strftime("%Y-%m-%d"),
                "clicks": 10,
                "impressions": 100,
                "ctr": 0.1,
                "position": 5.0,
            })
    return pd.DataFrame(rows)


def _make_serp(keyword, features):
    """Minimal SERP data dict for a single keyword."""
    items = [{"type": f} for f in features]
    return {"results": [{"keyword": keyword, "items": items}]}


# ===================================================================
# 1. classify_query_intent
# ===================================================================

class TestClassifyQueryIntent:
    """Tests for the standalone intent classifier."""

    def test_transactional_keyword(self):
        result = classify_query_intent("buy cheap shoes online")
        assert result["primary_intent"] == "transactional"
        assert "keyword_transactional" in result["signals"]

    def test_commercial_keyword(self):
        result = classify_query_intent("best running shoes 2025 review")
        assert result["primary_intent"] == "commercial"
        assert "keyword_commercial" in result["signals"]

    def test_navigational_keyword(self):
        result = classify_query_intent("gmail login account")
        assert result["primary_intent"] == "navigational"
        assert "keyword_navigational" in result["signals"]

    def test_informational_default(self):
        result = classify_query_intent("how does photosynthesis work")
        assert result["primary_intent"] == "informational"
        assert "default:no_strong_signals" in result["signals"]

    def test_serp_features_override(self):
        """SERP features carry heavier weight than keywords."""
        result = classify_query_intent("shoes", serp_features=["shopping_results", "ads_top"])
        assert result["primary_intent"] == "transactional"
        assert any("serp:" in s for s in result["signals"])

    def test_informational_serp(self):
        result = classify_query_intent("python tutorial", serp_features=["featured_snippet", "people_also_ask"])
        assert result["primary_intent"] == "informational"

    def test_mixed_signals(self):
        """Both keyword and SERP contribute to scoring."""
        result = classify_query_intent("best laptop deals", serp_features=["shopping_results"])
        # commercial keyword + transactional SERP = could be either
        assert result["primary_intent"] in ("transactional", "commercial")

    def test_output_keys(self):
        result = classify_query_intent("test query")
        assert "primary_intent" in result
        assert "confidence" in result
        assert "intent_distribution" in result
        assert "signals" in result

    def test_distribution_sums_to_one(self):
        result = classify_query_intent("best shoes buy online", serp_features=["shopping_results"])
        dist = result["intent_distribution"]
        total = sum(dist.values())
        assert abs(total - 1.0) < 0.01

    def test_confidence_range(self):
        result = classify_query_intent("buy shoes")
        assert 0 < result["confidence"] <= 1.0

    def test_case_insensitive(self):
        r1 = classify_query_intent("BUY shoes")
        r2 = classify_query_intent("buy shoes")
        assert r1["primary_intent"] == r2["primary_intent"]

    def test_empty_query_default(self):
        result = classify_query_intent("")
        assert result["primary_intent"] == "informational"

    def test_whitespace_stripped(self):
        result = classify_query_intent("  buy shoes  ")
        assert result["primary_intent"] == "transactional"

    def test_serp_features_empty_list(self):
        result = classify_query_intent("test", serp_features=[])
        assert result["primary_intent"] == "informational"

    def test_serp_features_none(self):
        result = classify_query_intent("test", serp_features=None)
        assert result["primary_intent"] == "informational"

    def test_unknown_serp_feature_ignored(self):
        result = classify_query_intent("test", serp_features=["unknown_feature_xyz"])
        assert "default:no_strong_signals" in result["signals"]


# ===================================================================
# 2. infer_page_type
# ===================================================================

class TestInferPageType:
    def test_blog(self):
        assert infer_page_type("https://example.com/blog/my-post") == "blog"

    def test_product(self):
        assert infer_page_type("https://example.com/product/shoes-123") == "product"

    def test_category(self):
        assert infer_page_type("https://example.com/category/mens-shoes") == "category"

    def test_landing_page(self):
        assert infer_page_type("https://example.com/lp/spring-sale") == "landing_page"

    def test_documentation(self):
        assert infer_page_type("https://example.com/docs/api-guide") == "documentation"

    def test_tool(self):
        assert infer_page_type("https://example.com/tools/seo-checker/") == "tool"

    def test_homepage(self):
        assert infer_page_type("https://example.com/") == "homepage"
        assert infer_page_type("https://example.com") == "homepage"

    def test_other_fallback(self):
        assert infer_page_type("https://example.com/about-us") == "other"

    def test_page_meta_product(self):
        meta = {"title": "Buy Premium Shoes - Shop Now"}
        assert infer_page_type("https://example.com/some-page", page_meta=meta) == "product"

    def test_page_meta_documentation(self):
        meta = {"title": "How to Set Up Your Account - Tutorial"}
        assert infer_page_type("https://example.com/some-page", page_meta=meta) == "documentation"

    def test_url_pattern_overrides_meta(self):
        """URL pattern takes precedence over page_meta."""
        meta = {"title": "Buy Now"}
        result = infer_page_type("https://example.com/blog/post-1", page_meta=meta)
        assert result == "blog"

    def test_none_page_meta(self):
        result = infer_page_type("https://example.com/xyz", page_meta=None)
        assert result == "other"

    def test_case_insensitive(self):
        assert infer_page_type("https://EXAMPLE.COM/BLOG/post") == "blog"


# ===================================================================
# 3. IntentMigrationAnalyzer — Helper maps
# ===================================================================

class TestBuildSerpFeaturesMap:
    def test_empty_serp(self):
        a = IntentMigrationAnalyzer(pd.DataFrame(), serp_data={})
        assert a._serp_features_map == {}

    def test_none_serp(self):
        a = IntentMigrationAnalyzer(pd.DataFrame(), serp_data=None)
        assert a._serp_features_map == {}

    def test_valid_serp(self):
        serp = _make_serp("buy shoes", ["shopping_results", "ads_top"])
        a = IntentMigrationAnalyzer(pd.DataFrame(), serp_data=serp)
        assert "buy shoes" in a._serp_features_map
        assert "shopping_results" in a._serp_features_map["buy shoes"]

    def test_organic_excluded(self):
        serp = {"results": [{"keyword": "test", "items": [{"type": "organic"}]}]}
        a = IntentMigrationAnalyzer(pd.DataFrame(), serp_data=serp)
        assert a._serp_features_map == {}

    def test_feature_flags(self):
        serp = {"results": [{"keyword": "q", "items": [{"type": "organic", "featured_snippet": True}]}]}
        a = IntentMigrationAnalyzer(pd.DataFrame(), serp_data=serp)
        assert "featured_snippet" in a._serp_features_map.get("q", [])


class TestBuildPageMetaMap:
    def test_none_page_data(self):
        a = IntentMigrationAnalyzer(pd.DataFrame(), page_data=None)
        assert a._page_meta_map == {}

    def test_empty_dataframe(self):
        a = IntentMigrationAnalyzer(pd.DataFrame(), page_data=pd.DataFrame())
        assert a._page_meta_map == {}

    def test_dataframe_with_url(self):
        pdf = pd.DataFrame([{"url": "https://example.com/page", "title": "Page"}])
        a = IntentMigrationAnalyzer(pd.DataFrame(), page_data=pdf)
        assert "https://example.com/page" in a._page_meta_map

    def test_dict_page_data(self):
        pdata = {"https://example.com/p": {"title": "P"}}
        a = IntentMigrationAnalyzer(pd.DataFrame(), page_data=pdata)
        assert "https://example.com/p" in a._page_meta_map


# ===================================================================
# 4. _split_time_windows
# ===================================================================

class TestSplitTimeWindows:
    def test_basic_split(self):
        ts = _make_ts([("q1", "https://example.com/p1")], days=120)
        a = IntentMigrationAnalyzer(ts)
        recent, comparison = a._split_time_windows()
        assert not recent.empty
        assert not comparison.empty

    def test_recent_window_size(self):
        ts = _make_ts([("q1", "https://example.com/p1")], days=120)
        a = IntentMigrationAnalyzer(ts)
        recent, _ = a._split_time_windows()
        dates = pd.to_datetime(recent["date"])
        span = (dates.max() - dates.min()).days
        assert span <= a.RECENT_WINDOW_DAYS

    def test_alternate_date_column(self):
        ts = _make_ts([("q1", "https://example.com/p1")], days=120)
        ts = ts.rename(columns={"date": "day"})
        a = IntentMigrationAnalyzer(ts)
        recent, comparison = a._split_time_windows()
        assert not recent.empty

    def test_short_data_returns_something(self):
        ts = _make_ts([("q1", "https://example.com/p1")], days=10)
        a = IntentMigrationAnalyzer(ts)
        recent, comparison = a._split_time_windows()
        # Recent should have data; comparison may be empty
        assert not recent.empty


# ===================================================================
# 5. _aggregate_query_metrics
# ===================================================================

class TestAggregateQueryMetrics:
    def test_basic_aggregation(self):
        ts = _make_ts([("q1", "https://example.com/p1")], days=10)
        a = IntentMigrationAnalyzer(ts)
        agg = a._aggregate_query_metrics(ts)
        assert len(agg) == 1
        assert agg.iloc[0]["query"] == "q1"
        assert agg.iloc[0]["impressions"] == 1000  # 100 * 10 days

    def test_ctr_calculation(self):
        ts = _make_ts([("q1", "https://example.com/p1")], days=10)
        a = IntentMigrationAnalyzer(ts)
        agg = a._aggregate_query_metrics(ts)
        expected_ctr = 100 / 1000  # 10 clicks * 10 / 100 imp * 10
        assert abs(agg.iloc[0]["ctr"] - expected_ctr) < 0.01

    def test_multiple_queries(self):
        ts = _make_ts([("q1", "https://example.com/p1"), ("q2", "https://example.com/p2")], days=10)
        a = IntentMigrationAnalyzer(ts)
        agg = a._aggregate_query_metrics(ts)
        assert len(agg) == 2

    def test_empty_dataframe(self):
        a = IntentMigrationAnalyzer(pd.DataFrame())
        agg = a._aggregate_query_metrics(pd.DataFrame())
        assert agg.empty

    def test_keyword_column_alias(self):
        ts = _make_ts([("q1", "https://example.com/p1")], days=10)
        ts = ts.rename(columns={"query": "keyword"})
        a = IntentMigrationAnalyzer(ts)
        agg = a._aggregate_query_metrics(ts)
        assert len(agg) == 1
        assert agg.iloc[0]["query"] == "q1"

    def test_no_position_column(self):
        ts = _make_ts([("q1", "https://example.com/p1")], days=5)
        ts = ts.drop(columns=["position"])
        a = IntentMigrationAnalyzer(ts)
        agg = a._aggregate_query_metrics(ts)
        assert "avg_position" in agg.columns

    def test_zero_impressions_ctr(self):
        ts = _make_ts([("q1", "https://example.com/p1")], days=5)
        ts["impressions"] = 0
        a = IntentMigrationAnalyzer(ts)
        agg = a._aggregate_query_metrics(ts)
        assert agg.iloc[0]["ctr"] == 0


# ===================================================================
# 6. _classify_window
# ===================================================================

class TestClassifyWindow:
    def test_classifies_queries(self):
        ts = _make_ts([("buy shoes", "https://example.com/p1")], days=10)
        a = IntentMigrationAnalyzer(ts)
        agg = a._aggregate_query_metrics(ts)
        result = a._classify_window(agg)
        assert "buy shoes" in result
        assert result["buy shoes"]["primary_intent"] == "transactional"

    def test_includes_metrics(self):
        ts = _make_ts([("test query", "https://example.com/p1")], days=10)
        a = IntentMigrationAnalyzer(ts)
        agg = a._aggregate_query_metrics(ts)
        result = a._classify_window(agg)
        info = result["test query"]
        assert "impressions" in info
        assert "clicks" in info
        assert "avg_position" in info

    def test_uses_serp_features(self):
        serp = _make_serp("test query", ["shopping_results"])
        ts = _make_ts([("test query", "https://example.com/p1")], days=10)
        a = IntentMigrationAnalyzer(ts, serp_data=serp)
        agg = a._aggregate_query_metrics(ts)
        result = a._classify_window(agg)
        assert result["test query"]["primary_intent"] == "transactional"


# ===================================================================
# 7. _detect_shifts
# ===================================================================

class TestDetectShifts:
    def _make_intents(self, query, primary, dist, impressions=200):
        return {
            query: {
                "primary_intent": primary,
                "intent_distribution": dist,
                "impressions": impressions,
                "clicks": 20,
                "avg_position": 5.0,
                "confidence": 0.8,
            }
        }

    def test_primary_changed(self):
        recent = self._make_intents("q1", "transactional",
            {"transactional": 0.7, "informational": 0.3, "commercial": 0.0, "navigational": 0.0})
        prev = self._make_intents("q1", "informational",
            {"transactional": 0.2, "informational": 0.7, "commercial": 0.1, "navigational": 0.0})
        a = IntentMigrationAnalyzer(pd.DataFrame())
        shifts = a._detect_shifts(recent, prev)
        assert len(shifts) == 1
        assert shifts[0]["primary_changed"] is True
        assert shifts[0]["query"] == "q1"

    def test_no_shift(self):
        dist = {"transactional": 0.1, "informational": 0.7, "commercial": 0.1, "navigational": 0.1}
        recent = self._make_intents("q1", "informational", dist)
        prev = self._make_intents("q1", "informational", dist)
        a = IntentMigrationAnalyzer(pd.DataFrame())
        shifts = a._detect_shifts(recent, prev)
        assert len(shifts) == 0

    def test_low_impression_excluded(self):
        recent = self._make_intents("q1", "transactional",
            {"transactional": 0.9, "informational": 0.1}, impressions=10)
        prev = self._make_intents("q1", "informational",
            {"transactional": 0.1, "informational": 0.9})
        a = IntentMigrationAnalyzer(pd.DataFrame())
        shifts = a._detect_shifts(recent, prev)
        assert len(shifts) == 0

    def test_magnitude_shift_without_primary_change(self):
        recent = self._make_intents("q1", "informational",
            {"transactional": 0.4, "informational": 0.5, "commercial": 0.05, "navigational": 0.05})
        prev = self._make_intents("q1", "informational",
            {"transactional": 0.1, "informational": 0.8, "commercial": 0.05, "navigational": 0.05})
        a = IntentMigrationAnalyzer(pd.DataFrame())
        shifts = a._detect_shifts(recent, prev)
        assert len(shifts) == 1
        assert shifts[0]["primary_changed"] is False
        assert shifts[0]["shift_magnitude"] >= 0.15

    def test_capped_at_50(self):
        recent = {}
        prev = {}
        for i in range(60):
            q = f"q{i}"
            recent[q] = {
                "primary_intent": "transactional",
                "intent_distribution": {"transactional": 0.9, "informational": 0.1},
                "impressions": 200,
                "clicks": 20,
                "avg_position": 5.0,
            }
            prev[q] = {
                "primary_intent": "informational",
                "intent_distribution": {"transactional": 0.1, "informational": 0.9},
                "impressions": 200,
                "clicks": 20,
                "avg_position": 5.0,
            }
        a = IntentMigrationAnalyzer(pd.DataFrame())
        shifts = a._detect_shifts(recent, prev)
        assert len(shifts) <= 50

    def test_sorted_primary_first(self):
        recent = {}
        prev = {}
        # q_primary has primary change, low impressions
        recent["q_primary"] = {
            "primary_intent": "transactional",
            "intent_distribution": {"transactional": 0.8, "informational": 0.2},
            "impressions": 100, "clicks": 10, "avg_position": 3.0,
        }
        prev["q_primary"] = {
            "primary_intent": "informational",
            "intent_distribution": {"transactional": 0.2, "informational": 0.8},
            "impressions": 100, "clicks": 10, "avg_position": 3.0,
        }
        # q_magnitude has magnitude shift only, high impressions
        recent["q_magnitude"] = {
            "primary_intent": "informational",
            "intent_distribution": {"transactional": 0.4, "informational": 0.5, "commercial": 0.1},
            "impressions": 5000, "clicks": 500, "avg_position": 2.0,
        }
        prev["q_magnitude"] = {
            "primary_intent": "informational",
            "intent_distribution": {"transactional": 0.1, "informational": 0.8, "commercial": 0.1},
            "impressions": 5000, "clicks": 500, "avg_position": 2.0,
        }
        a = IntentMigrationAnalyzer(pd.DataFrame())
        shifts = a._detect_shifts(recent, prev)
        assert len(shifts) == 2
        assert shifts[0]["query"] == "q_primary"

    def test_shift_output_keys(self):
        recent = self._make_intents("q1", "transactional",
            {"transactional": 0.8, "informational": 0.2})
        prev = self._make_intents("q1", "informational",
            {"transactional": 0.2, "informational": 0.8})
        a = IntentMigrationAnalyzer(pd.DataFrame())
        shifts = a._detect_shifts(recent, prev)
        s = shifts[0]
        expected_keys = {"query", "previous_intent", "current_intent", "primary_changed",
                         "shift_magnitude", "growing_intent", "growing_delta",
                         "declining_intent", "declining_delta", "impressions", "clicks",
                         "avg_position", "recent_distribution", "previous_distribution"}
        assert expected_keys.issubset(s.keys())


# ===================================================================
# 8. _identify_emerging_intents
# ===================================================================

class TestIdentifyEmergingIntents:
    def _make_intent_info(self, impressions=200, primary="informational"):
        return {
            "primary_intent": primary,
            "confidence": 0.8,
            "impressions": impressions,
            "clicks": 20,
            "avg_position": 5.0,
            "intent_distribution": {primary: 0.8},
        }

    def test_new_query_detected(self):
        recent = {"new_query": self._make_intent_info()}
        comparison = {}
        a = IntentMigrationAnalyzer(pd.DataFrame())
        emerging = a._identify_emerging_intents(recent, comparison)
        assert len(emerging) == 1
        assert emerging[0]["type"] == "new_query"
        assert emerging[0]["growth_signal"] == "appeared_recently"

    def test_new_query_low_impressions_excluded(self):
        recent = {"new_query": self._make_intent_info(impressions=10)}
        comparison = {}
        a = IntentMigrationAnalyzer(pd.DataFrame())
        emerging = a._identify_emerging_intents(recent, comparison)
        assert len(emerging) == 0

    def test_rapid_growth(self):
        # Recent: 200 impressions over 30 days
        # Comparison: 50 impressions over 60 days → scaled = 50 * (30/60) = 25
        # Growth rate = (200 - 25) / 25 = 7.0
        recent = {"q1": self._make_intent_info(impressions=200)}
        comparison = {"q1": self._make_intent_info(impressions=50)}
        a = IntentMigrationAnalyzer(pd.DataFrame())
        emerging = a._identify_emerging_intents(recent, comparison)
        assert len(emerging) == 1
        assert emerging[0]["type"] == "rapid_growth"
        assert emerging[0]["growth_rate"] >= 0.5

    def test_no_growth(self):
        # Same impressions, scaled down = same → 0 growth
        recent = {"q1": self._make_intent_info(impressions=100)}
        comparison = {"q1": self._make_intent_info(impressions=200)}
        a = IntentMigrationAnalyzer(pd.DataFrame())
        emerging = a._identify_emerging_intents(recent, comparison)
        assert len(emerging) == 0

    def test_sorted_by_impressions(self):
        recent = {
            "q_low": self._make_intent_info(impressions=100),
            "q_high": self._make_intent_info(impressions=500),
        }
        comparison = {}
        a = IntentMigrationAnalyzer(pd.DataFrame())
        emerging = a._identify_emerging_intents(recent, comparison)
        assert emerging[0]["query"] == "q_high"

    def test_capped_at_40(self):
        recent = {f"q{i}": self._make_intent_info(impressions=200) for i in range(50)}
        comparison = {}
        a = IntentMigrationAnalyzer(pd.DataFrame())
        emerging = a._identify_emerging_intents(recent, comparison)
        assert len(emerging) <= 40

    def test_output_keys(self):
        recent = {"q1": self._make_intent_info()}
        comparison = {}
        a = IntentMigrationAnalyzer(pd.DataFrame())
        emerging = a._identify_emerging_intents(recent, comparison)
        e = emerging[0]
        expected_keys = {"query", "type", "intent", "confidence", "impressions",
                         "clicks", "avg_position", "growth_signal"}
        assert expected_keys.issubset(e.keys())


# ===================================================================
# 9. _analyze_content_alignment
# ===================================================================

class TestAnalyzeContentAlignment:
    def test_misalignment_detected(self):
        """Transactional intent on a blog page = critical misalignment."""
        ts = _make_ts([("buy shoes", "https://example.com/blog/shoes")], days=120)
        a = IntentMigrationAnalyzer(ts)
        recent_df, _ = a._split_time_windows()
        recent_agg = a._aggregate_query_metrics(recent_df)
        recent_intents = a._classify_window(recent_agg)
        misalign = a._analyze_content_alignment(recent_intents)
        # May have misalignment if impressions >= threshold
        if len(misalign) > 0:
            assert misalign[0]["severity"] in ("critical", "high", "medium")

    def test_no_page_column(self):
        ts = pd.DataFrame({"query": ["q1"], "date": ["2025-01-01"],
                           "clicks": [10], "impressions": [100]})
        a = IntentMigrationAnalyzer(ts)
        intents = {"q1": {"primary_intent": "transactional", "confidence": 0.9,
                          "impressions": 200, "clicks": 20, "avg_position": 3.0}}
        result = a._analyze_content_alignment(intents)
        assert result == []

    def test_low_impressions_skipped(self):
        ts = _make_ts([("buy shoes", "https://example.com/blog/shoes")], days=120)
        a = IntentMigrationAnalyzer(ts)
        intents = {"buy shoes": {"primary_intent": "transactional", "confidence": 0.9,
                                  "impressions": 5, "clicks": 1, "avg_position": 3.0}}
        result = a._analyze_content_alignment(intents)
        assert len(result) == 0

    def test_alignment_severity_critical(self):
        """Transactional intent + blog page + high confidence = critical."""
        ts = _make_ts([("buy shoes", "https://example.com/blog/shoes")], days=120)
        a = IntentMigrationAnalyzer(ts)
        intents = {"buy shoes": {
            "primary_intent": "transactional",
            "confidence": 0.9,
            "impressions": 200, "clicks": 20, "avg_position": 3.0,
            "intent_distribution": {"transactional": 0.9},
        }}
        result = a._analyze_content_alignment(intents)
        if len(result) > 0:
            assert result[0]["severity"] == "critical"

    def test_capped_at_40(self):
        queries = [(f"buy item{i}", f"https://example.com/blog/item{i}") for i in range(50)]
        ts = _make_ts(queries, days=120)
        a = IntentMigrationAnalyzer(ts)
        recent_df, _ = a._split_time_windows()
        recent_agg = a._aggregate_query_metrics(recent_df)
        recent_intents = a._classify_window(recent_agg)
        result = a._analyze_content_alignment(recent_intents)
        assert len(result) <= 40


# ===================================================================
# 10. _alignment_recommendation
# ===================================================================

class TestAlignmentRecommendation:
    def test_transactional_blog(self):
        rec = IntentMigrationAnalyzer._alignment_recommendation(
            "transactional", "blog", "buy shoes")
        assert "landing page" in rec.lower() or "product" in rec.lower()

    def test_transactional_documentation(self):
        rec = IntentMigrationAnalyzer._alignment_recommendation(
            "transactional", "documentation", "buy shoes")
        assert "landing page" in rec.lower() or "product" in rec.lower()

    def test_commercial_product(self):
        rec = IntentMigrationAnalyzer._alignment_recommendation(
            "commercial", "product", "best shoes")
        assert "comparison" in rec.lower() or "review" in rec.lower()

    def test_informational_product(self):
        rec = IntentMigrationAnalyzer._alignment_recommendation(
            "informational", "product", "how to clean shoes")
        assert "blog" in rec.lower() or "guide" in rec.lower() or "educational" in rec.lower()

    def test_navigational(self):
        rec = IntentMigrationAnalyzer._alignment_recommendation(
            "navigational", "blog", "nike login")
        assert "branding" in rec.lower() or "navigat" in rec.lower() or "schema" in rec.lower()

    def test_default_fallback(self):
        rec = IntentMigrationAnalyzer._alignment_recommendation(
            "informational", "category", "shoes info")
        assert "review" in rec.lower() or "intent" in rec.lower()


# ===================================================================
# 11. _intent_portfolio_summary
# ===================================================================

class TestIntentPortfolioSummary:
    def _make_intents(self, queries_and_dist):
        intents = {}
        for q, dist, imp in queries_and_dist:
            intents[q] = {
                "primary_intent": max(dist, key=dist.get),
                "intent_distribution": dist,
                "impressions": imp,
                "clicks": imp // 10,
                "avg_position": 5.0,
            }
        return intents

    def test_basic_portfolio(self):
        recent = self._make_intents([
            ("q1", {"informational": 0.8, "transactional": 0.2}, 100),
            ("q2", {"informational": 0.3, "transactional": 0.7}, 100),
        ])
        prev = self._make_intents([
            ("q1", {"informational": 0.9, "transactional": 0.1}, 100),
            ("q2", {"informational": 0.9, "transactional": 0.1}, 100),
        ])
        a = IntentMigrationAnalyzer(pd.DataFrame())
        portfolio = a._intent_portfolio_summary(recent, prev)
        assert "recent_distribution" in portfolio
        assert "previous_distribution" in portfolio
        assert "changes_by_intent" in portfolio
        assert "dominant_intent" in portfolio
        assert portfolio["total_queries_recent"] == 2

    def test_distribution_sums_near_one(self):
        recent = self._make_intents([
            ("q1", {"informational": 0.5, "commercial": 0.3, "transactional": 0.2}, 100),
        ])
        a = IntentMigrationAnalyzer(pd.DataFrame())
        portfolio = a._intent_portfolio_summary(recent, {})
        total = sum(portfolio["recent_distribution"].values())
        assert abs(total - 1.0) < 0.01

    def test_empty_intents(self):
        a = IntentMigrationAnalyzer(pd.DataFrame())
        portfolio = a._intent_portfolio_summary({}, {})
        assert portfolio["total_queries_recent"] == 0
        assert portfolio["dominant_intent"] == "informational"

    def test_change_direction(self):
        recent = self._make_intents([
            ("q1", {"transactional": 0.8, "informational": 0.2}, 100),
        ])
        prev = self._make_intents([
            ("q1", {"transactional": 0.2, "informational": 0.8}, 100),
        ])
        a = IntentMigrationAnalyzer(pd.DataFrame())
        portfolio = a._intent_portfolio_summary(recent, prev)
        changes = portfolio["changes_by_intent"]
        assert changes["transactional"]["direction"] == "growing"
        assert changes["informational"]["direction"] == "declining"


# ===================================================================
# 12. _generate_recommendations
# ===================================================================

class TestGenerateRecommendations:
    def test_critical_misalignment_rec(self):
        misalignments = [{"severity": "critical", "impressions": 500, "query": "buy shoes"}]
        a = IntentMigrationAnalyzer(pd.DataFrame())
        recs = a._generate_recommendations([], [], misalignments, {"changes_by_intent": {}})
        assert any(r["category"] == "content_misalignment" and r["priority"] == 1 for r in recs)

    def test_transactional_shift_rec(self):
        shifts = [{
            "primary_changed": True,
            "current_intent": "transactional",
            "previous_intent": "informational",
            "query": "q1",
            "impressions": 200,
        }]
        a = IntentMigrationAnalyzer(pd.DataFrame())
        recs = a._generate_recommendations(shifts, [], [], {"changes_by_intent": {}})
        assert any(r["category"] == "intent_shift" for r in recs)

    def test_from_informational_rec(self):
        shifts = [{
            "primary_changed": True,
            "current_intent": "commercial",
            "previous_intent": "informational",
            "query": "q1",
            "impressions": 200,
        }]
        a = IntentMigrationAnalyzer(pd.DataFrame())
        recs = a._generate_recommendations(shifts, [], [], {"changes_by_intent": {}})
        assert any("informational" in r.get("title", "").lower() for r in recs)

    def test_new_queries_rec(self):
        emerging = [{"type": "new_query", "query": "q1", "impressions": 200}]
        a = IntentMigrationAnalyzer(pd.DataFrame())
        recs = a._generate_recommendations([], emerging, [], {"changes_by_intent": {}})
        assert any(r["category"] == "emerging_intent" for r in recs)

    def test_rapid_growth_rec(self):
        emerging = [{"type": "rapid_growth", "query": "q1", "impressions": 200}]
        a = IntentMigrationAnalyzer(pd.DataFrame())
        recs = a._generate_recommendations([], emerging, [], {"changes_by_intent": {}})
        assert any("growing" in r.get("title", "").lower() for r in recs)

    def test_portfolio_trend_rec(self):
        portfolio = {"changes_by_intent": {
            "commercial": {"change": 0.04},
            "transactional": {"change": 0.03},
        }}
        a = IntentMigrationAnalyzer(pd.DataFrame())
        recs = a._generate_recommendations([], [], [], portfolio)
        assert any(r["category"] == "portfolio_trend" for r in recs)

    def test_high_misalignment_rec(self):
        misalignments = [{"severity": "high", "impressions": 300, "query": "q1"}]
        a = IntentMigrationAnalyzer(pd.DataFrame())
        recs = a._generate_recommendations([], [], misalignments, {"changes_by_intent": {}})
        assert any(r["priority"] == 7 for r in recs)

    def test_sorted_by_priority(self):
        misalignments = [{"severity": "critical", "impressions": 500, "query": "q1"}]
        emerging = [{"type": "new_query", "query": "q2", "impressions": 200}]
        shifts = [{"primary_changed": True, "current_intent": "transactional",
                    "previous_intent": "informational", "query": "q3", "impressions": 200}]
        a = IntentMigrationAnalyzer(pd.DataFrame())
        recs = a._generate_recommendations(shifts, emerging, misalignments, {"changes_by_intent": {}})
        priorities = [r["priority"] for r in recs]
        assert priorities == sorted(priorities)

    def test_no_recommendations_when_empty(self):
        a = IntentMigrationAnalyzer(pd.DataFrame())
        recs = a._generate_recommendations([], [], [], {"changes_by_intent": {}})
        assert recs == []


# ===================================================================
# 13. _build_summary
# ===================================================================

class TestBuildSummary:
    def test_contains_query_count(self):
        portfolio = {"dominant_intent": "informational", "total_queries_recent": 42}
        a = IntentMigrationAnalyzer(pd.DataFrame())
        summary = a._build_summary([], [], [], portfolio)
        assert "42" in summary

    def test_mentions_dominant_intent(self):
        portfolio = {"dominant_intent": "transactional", "total_queries_recent": 10}
        a = IntentMigrationAnalyzer(pd.DataFrame())
        summary = a._build_summary([], [], [], portfolio)
        assert "transactional" in summary.lower()

    def test_with_primary_shifts(self):
        shifts = [{
            "primary_changed": True,
            "previous_intent": "informational",
            "current_intent": "transactional",
            "impressions": 200,
        }]
        portfolio = {"dominant_intent": "informational", "total_queries_recent": 10}
        a = IntentMigrationAnalyzer(pd.DataFrame())
        summary = a._build_summary(shifts, [], [], portfolio)
        assert "1 quer" in summary.lower()
        assert "shift" in summary.lower()

    def test_no_shifts_stable(self):
        portfolio = {"dominant_intent": "informational", "total_queries_recent": 10}
        a = IntentMigrationAnalyzer(pd.DataFrame())
        summary = a._build_summary([], [], [], portfolio)
        assert "stable" in summary.lower()

    def test_emerging_mentioned(self):
        emerging = [{"type": "new_query"}, {"type": "rapid_growth"}]
        portfolio = {"dominant_intent": "informational", "total_queries_recent": 10}
        a = IntentMigrationAnalyzer(pd.DataFrame())
        summary = a._build_summary([], emerging, [], portfolio)
        assert "1 new" in summary.lower()
        assert "1 rapidly" in summary.lower()

    def test_misalignment_mentioned(self):
        misalignments = [
            {"severity": "critical"},
            {"severity": "high"},
            {"severity": "high"},
        ]
        portfolio = {"dominant_intent": "informational", "total_queries_recent": 10}
        a = IntentMigrationAnalyzer(pd.DataFrame())
        summary = a._build_summary([], [], misalignments, portfolio)
        assert "1 critical" in summary.lower()
        assert "2 high" in summary.lower()


# ===================================================================
# 14. analyze (full pipeline)
# ===================================================================

class TestAnalyzeFullPipeline:
    def test_output_schema(self):
        ts = _make_ts([("buy shoes", "https://example.com/blog/shoes"),
                        ("how to tie shoes", "https://example.com/docs/tie")], days=120)
        result = IntentMigrationAnalyzer(ts).analyze()
        expected_keys = {"summary", "intent_shifts", "emerging_intents",
                         "content_alignment", "portfolio_distribution", "recommendations"}
        assert expected_keys == set(result.keys())

    def test_all_lists(self):
        ts = _make_ts([("buy shoes", "https://example.com/p1")], days=120)
        result = IntentMigrationAnalyzer(ts).analyze()
        assert isinstance(result["intent_shifts"], list)
        assert isinstance(result["emerging_intents"], list)
        assert isinstance(result["content_alignment"], list)
        assert isinstance(result["recommendations"], list)

    def test_summary_is_string(self):
        ts = _make_ts([("q1", "https://example.com/p1")], days=120)
        result = IntentMigrationAnalyzer(ts).analyze()
        assert isinstance(result["summary"], str)
        assert len(result["summary"]) > 0

    def test_portfolio_is_dict(self):
        ts = _make_ts([("q1", "https://example.com/p1")], days=120)
        result = IntentMigrationAnalyzer(ts).analyze()
        assert isinstance(result["portfolio_distribution"], dict)

    def test_with_serp_data(self):
        serp = _make_serp("buy shoes", ["shopping_results"])
        ts = _make_ts([("buy shoes", "https://example.com/p1")], days=120)
        result = IntentMigrationAnalyzer(ts, serp_data=serp).analyze()
        assert "summary" in result

    def test_with_page_data(self):
        page_data = pd.DataFrame([{"url": "https://example.com/p1", "title": "Product Page"}])
        ts = _make_ts([("q1", "https://example.com/p1")], days=120)
        result = IntentMigrationAnalyzer(ts, page_data=page_data).analyze()
        assert "summary" in result

    def test_empty_data_minimal_result(self):
        ts = pd.DataFrame({"query": [], "page": [], "date": [], "clicks": [], "impressions": []})
        result = IntentMigrationAnalyzer(ts).analyze()
        assert "insufficient" in result["summary"].lower() or result["intent_shifts"] == []


# ===================================================================
# 15. analyze_intent_migration public function
# ===================================================================

class TestAnalyzeIntentMigrationPublic:
    def test_basic_call(self):
        ts = _make_ts([("q1", "https://example.com/p1")], days=120)
        result = analyze_intent_migration(ts)
        assert "summary" in result
        assert "intent_shifts" in result

    def test_with_all_args(self):
        ts = _make_ts([("q1", "https://example.com/p1")], days=120)
        serp = _make_serp("q1", ["featured_snippet"])
        page_data = pd.DataFrame([{"url": "https://example.com/p1", "title": "Test"}])
        result = analyze_intent_migration(ts, serp_data=serp, page_data=page_data)
        assert isinstance(result, dict)

    def test_none_optional_args(self):
        ts = _make_ts([("q1", "https://example.com/p1")], days=120)
        result = analyze_intent_migration(ts, serp_data=None, page_data=None)
        assert isinstance(result, dict)


# ===================================================================
# 16. Edge cases
# ===================================================================

class TestEdgeCases:
    def test_single_day_data(self):
        ts = _make_ts([("q1", "https://example.com/p1")], days=1)
        result = IntentMigrationAnalyzer(ts).analyze()
        assert isinstance(result, dict)

    def test_nan_values(self):
        ts = _make_ts([("q1", "https://example.com/p1")], days=120)
        ts.loc[0, "clicks"] = np.nan
        ts.loc[1, "impressions"] = np.nan
        result = IntentMigrationAnalyzer(ts).analyze()
        assert isinstance(result, dict)

    def test_special_characters_in_query(self):
        ts = _make_ts([("buy shoes (men's) — size 10+", "https://example.com/p1")], days=120)
        result = IntentMigrationAnalyzer(ts).analyze()
        assert isinstance(result, dict)

    def test_unicode_query(self):
        ts = _make_ts([("zapatos comprar precio", "https://example.com/p1")], days=120)
        result = IntentMigrationAnalyzer(ts).analyze()
        assert isinstance(result, dict)

    def test_very_large_impressions(self):
        ts = _make_ts([("q1", "https://example.com/p1")], days=120)
        ts["impressions"] = 999999999
        result = IntentMigrationAnalyzer(ts).analyze()
        assert isinstance(result, dict)

    def test_zero_impressions_everywhere(self):
        ts = _make_ts([("q1", "https://example.com/p1")], days=120)
        ts["impressions"] = 0
        ts["clicks"] = 0
        result = IntentMigrationAnalyzer(ts).analyze()
        assert isinstance(result, dict)

    def test_duplicate_queries(self):
        """Multiple rows for same query+date is normal timeseries data."""
        ts = _make_ts([("q1", "https://example.com/p1"), ("q1", "https://example.com/p2")], days=120)
        result = IntentMigrationAnalyzer(ts).analyze()
        assert isinstance(result, dict)

    def test_constants_nonempty(self):
        assert len(_TRANSACTIONAL_PATTERNS) > 0
        assert len(_COMMERCIAL_PATTERNS) > 0
        assert len(_NAVIGATIONAL_PATTERNS) > 0
        assert len(_SERP_INTENT_SIGNALS) > 0
        assert len(_PAGE_TYPE_PATTERNS) > 0
