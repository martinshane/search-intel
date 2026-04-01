"""
Comprehensive test suite for api/analysis/module_10_branded_split.py

Module 10: Branded vs Non-Branded Split — brand query dependency analysis.
Tests cover all helper functions, BrandedSplitAnalyzer methods, the public
analyze_branded_split() API, and edge cases.
"""

import math
import re
import unittest
from unittest.mock import patch

# ---------------------------------------------------------------------------
# Import the module under test
# ---------------------------------------------------------------------------
from api.analysis.module_10_branded_split import (
    BrandedSplitAnalyzer,
    _compile_brand_patterns,
    _is_branded,
    _normalise_brand_terms,
    _pct_change,
    _safe_div,
    analyze_branded_split,
)


# ===================================================================
# 1. _normalise_brand_terms
# ===================================================================
class TestNormaliseBrandTerms(unittest.TestCase):
    """Tests for _normalise_brand_terms helper."""

    def test_none_returns_empty(self):
        self.assertEqual(_normalise_brand_terms(None), [])

    def test_empty_list_returns_empty(self):
        self.assertEqual(_normalise_brand_terms([]), [])

    def test_single_term_lowered(self):
        result = _normalise_brand_terms(["Acme"])
        self.assertIn("acme", result)

    def test_whitespace_stripped(self):
        result = _normalise_brand_terms(["  Acme  "])
        self.assertIn("acme", result)

    def test_multi_word_adds_variants(self):
        result = _normalise_brand_terms(["Acme Corp"])
        self.assertIn("acme corp", result)
        self.assertIn("acmecorp", result)
        self.assertIn("acme-corp", result)

    def test_deduplication(self):
        result = _normalise_brand_terms(["Acme", "acme", "ACME"])
        self.assertEqual(result.count("acme"), 1)

    def test_empty_strings_excluded(self):
        result = _normalise_brand_terms(["", "  ", "Acme"])
        self.assertNotIn("", result)
        self.assertIn("acme", result)

    def test_variant_deduplication(self):
        result = _normalise_brand_terms(["Acme Corp", "acmecorp"])
        # "acmecorp" should appear only once even though it's both a variant and explicit
        self.assertEqual(result.count("acmecorp"), 1)


# ===================================================================
# 2. _compile_brand_patterns
# ===================================================================
class TestCompileBrandPatterns(unittest.TestCase):
    """Tests for _compile_brand_patterns helper."""

    def test_empty_returns_empty(self):
        self.assertEqual(_compile_brand_patterns([]), [])

    def test_single_term_returns_pattern(self):
        patterns = _compile_brand_patterns(["acme"])
        self.assertEqual(len(patterns), 1)
        self.assertIsInstance(patterns[0], re.Pattern)

    def test_pattern_matches_at_start(self):
        patterns = _compile_brand_patterns(["acme"])
        self.assertTrue(patterns[0].search("acme widget"))

    def test_pattern_matches_at_end(self):
        patterns = _compile_brand_patterns(["acme"])
        self.assertTrue(patterns[0].search("buy acme"))

    def test_pattern_matches_with_hyphen(self):
        patterns = _compile_brand_patterns(["acme"])
        self.assertTrue(patterns[0].search("best-acme-tools"))

    def test_pattern_case_insensitive(self):
        patterns = _compile_brand_patterns(["acme"])
        self.assertTrue(patterns[0].search("ACME widget"))

    def test_special_regex_chars_escaped(self):
        # Should not crash on regex-special characters
        patterns = _compile_brand_patterns(["acme.co"])
        self.assertEqual(len(patterns), 1)
        self.assertTrue(patterns[0].search("acme.co pricing"))
        # The dot should be literal, not wildcard
        self.assertFalse(patterns[0].search("acmexco pricing"))


# ===================================================================
# 3. _is_branded
# ===================================================================
class TestIsBranded(unittest.TestCase):
    """Tests for _is_branded helper."""

    def setUp(self):
        self.patterns = _compile_brand_patterns(["acme", "widgetco"])

    def test_branded_query(self):
        self.assertTrue(_is_branded("acme pricing", self.patterns))

    def test_non_branded_query(self):
        self.assertFalse(_is_branded("best widgets 2025", self.patterns))

    def test_case_insensitive(self):
        self.assertTrue(_is_branded("ACME Support", self.patterns))

    def test_whitespace_stripped(self):
        self.assertTrue(_is_branded("  acme  ", self.patterns))

    def test_empty_query_no_match(self):
        self.assertFalse(_is_branded("", self.patterns))

    def test_empty_patterns_no_match(self):
        self.assertFalse(_is_branded("acme", []))

    def test_second_brand_matches(self):
        self.assertTrue(_is_branded("widgetco reviews", self.patterns))


# ===================================================================
# 4. _safe_div
# ===================================================================
class TestSafeDiv(unittest.TestCase):
    """Tests for _safe_div helper."""

    def test_normal_division(self):
        self.assertEqual(_safe_div(10, 5), 2.0)

    def test_zero_denominator_returns_default(self):
        self.assertEqual(_safe_div(10, 0), 0.0)

    def test_custom_default(self):
        self.assertEqual(_safe_div(10, 0, default=-1.0), -1.0)

    def test_zero_numerator(self):
        self.assertEqual(_safe_div(0, 5), 0.0)


# ===================================================================
# 5. _pct_change
# ===================================================================
class TestPctChange(unittest.TestCase):
    """Tests for _pct_change helper."""

    def test_positive_change(self):
        self.assertEqual(_pct_change(100, 150), 50.0)

    def test_negative_change(self):
        self.assertEqual(_pct_change(200, 100), -50.0)

    def test_no_change(self):
        self.assertEqual(_pct_change(100, 100), 0.0)

    def test_zero_old_returns_none(self):
        self.assertIsNone(_pct_change(0, 100))

    def test_rounding(self):
        result = _pct_change(3, 4)
        self.assertAlmostEqual(result, 33.33, places=2)


# ===================================================================
# 6. BrandedSplitAnalyzer.__init__
# ===================================================================
class TestBrandedSplitAnalyzerInit(unittest.TestCase):
    """Tests for BrandedSplitAnalyzer constructor."""

    def test_list_input(self):
        a = BrandedSplitAnalyzer([{"query": "a"}], ["brand"])
        self.assertEqual(len(a.raw_data), 1)

    def test_non_list_input_becomes_empty(self):
        a = BrandedSplitAnalyzer("not a list", ["brand"])
        self.assertEqual(a.raw_data, [])

    def test_none_input_becomes_empty(self):
        a = BrandedSplitAnalyzer(None, ["brand"])
        self.assertEqual(a.raw_data, [])

    def test_brand_terms_normalised(self):
        a = BrandedSplitAnalyzer([], ["Acme Corp"])
        self.assertIn("acme corp", a.brand_terms)
        self.assertIn("acmecorp", a.brand_terms)

    def test_none_brand_terms(self):
        a = BrandedSplitAnalyzer([], None)
        self.assertEqual(a.brand_terms, [])
        self.assertEqual(a.brand_patterns, [])

    def test_initial_buckets_empty(self):
        a = BrandedSplitAnalyzer([], ["brand"])
        self.assertEqual(a.branded_rows, [])
        self.assertEqual(a.non_branded_rows, [])


# ===================================================================
# 7. _classify_queries
# ===================================================================
class TestClassifyQueries(unittest.TestCase):
    """Tests for BrandedSplitAnalyzer._classify_queries."""

    def test_branded_classified(self):
        data = [{"query": "acme pricing", "clicks": 10}]
        a = BrandedSplitAnalyzer(data, ["acme"])
        a._classify_queries()
        self.assertEqual(len(a.branded_rows), 1)
        self.assertEqual(len(a.non_branded_rows), 0)

    def test_non_branded_classified(self):
        data = [{"query": "best widgets", "clicks": 5}]
        a = BrandedSplitAnalyzer(data, ["acme"])
        a._classify_queries()
        self.assertEqual(len(a.branded_rows), 0)
        self.assertEqual(len(a.non_branded_rows), 1)

    def test_mixed_classification(self):
        data = [
            {"query": "acme pricing", "clicks": 10},
            {"query": "best widgets", "clicks": 5},
            {"query": "acme reviews", "clicks": 3},
        ]
        a = BrandedSplitAnalyzer(data, ["acme"])
        a._classify_queries()
        self.assertEqual(len(a.branded_rows), 2)
        self.assertEqual(len(a.non_branded_rows), 1)

    def test_keys_format_query(self):
        data = [{"keys": ["acme stuff"], "clicks": 1}]
        a = BrandedSplitAnalyzer(data, ["acme"])
        a._classify_queries()
        self.assertEqual(len(a.branded_rows), 1)

    def test_empty_query_skipped(self):
        data = [{"query": "", "clicks": 1}]
        a = BrandedSplitAnalyzer(data, ["acme"])
        a._classify_queries()
        self.assertEqual(len(a.branded_rows), 0)
        self.assertEqual(len(a.non_branded_rows), 0)

    def test_no_brand_patterns_all_non_branded(self):
        data = [{"query": "acme", "clicks": 1}]
        a = BrandedSplitAnalyzer(data, [])
        a._classify_queries()
        self.assertEqual(len(a.branded_rows), 0)
        self.assertEqual(len(a.non_branded_rows), 1)

    def test_query_field_added(self):
        data = [{"query": "acme pricing", "clicks": 10}]
        a = BrandedSplitAnalyzer(data, ["acme"])
        a._classify_queries()
        self.assertEqual(a.branded_rows[0]["_query"], "acme pricing")


# ===================================================================
# 8. _aggregate_segment
# ===================================================================
class TestAggregateSegment(unittest.TestCase):
    """Tests for BrandedSplitAnalyzer._aggregate_segment."""

    def _make_analyzer(self):
        return BrandedSplitAnalyzer([], ["brand"])

    def test_basic_aggregation(self):
        a = self._make_analyzer()
        rows = [
            {"_query": "q1", "clicks": 10, "impressions": 100, "position": 3.0, "page": "/a"},
            {"_query": "q2", "clicks": 5, "impressions": 50, "position": 7.0, "page": "/b"},
        ]
        result = a._aggregate_segment(rows)
        self.assertEqual(result["total_clicks"], 15)
        self.assertEqual(result["total_impressions"], 150)
        self.assertEqual(result["unique_queries"], 2)
        self.assertEqual(result["unique_pages"], 2)
        self.assertEqual(result["avg_position"], 5.0)
        self.assertEqual(result["avg_ctr_pct"], 10.0)

    def test_empty_rows(self):
        a = self._make_analyzer()
        result = a._aggregate_segment([])
        self.assertEqual(result["total_clicks"], 0)
        self.assertEqual(result["total_impressions"], 0)
        self.assertEqual(result["unique_queries"], 0)
        self.assertIsNone(result["avg_position"])

    def test_no_position_returns_none(self):
        a = self._make_analyzer()
        rows = [{"_query": "q1", "clicks": 10, "impressions": 100}]
        result = a._aggregate_segment(rows)
        self.assertIsNone(result["avg_position"])

    def test_url_key_for_page(self):
        a = self._make_analyzer()
        rows = [{"_query": "q1", "clicks": 1, "impressions": 10, "url": "/x"}]
        result = a._aggregate_segment(rows)
        self.assertEqual(result["unique_pages"], 1)

    def test_duplicate_queries_counted_once(self):
        a = self._make_analyzer()
        rows = [
            {"_query": "q1", "clicks": 5, "impressions": 50},
            {"_query": "q1", "clicks": 3, "impressions": 30},
        ]
        result = a._aggregate_segment(rows)
        self.assertEqual(result["unique_queries"], 1)
        self.assertEqual(result["total_clicks"], 8)


# ===================================================================
# 9. _assess_brand_dependency
# ===================================================================
class TestAssessBrandDependency(unittest.TestCase):
    """Tests for BrandedSplitAnalyzer._assess_brand_dependency."""

    def _make_analyzer(self):
        return BrandedSplitAnalyzer([], ["brand"])

    def test_critical_risk(self):
        a = self._make_analyzer()
        branded = {"total_clicks": 800, "total_impressions": 8000, "unique_queries": 90}
        non_branded = {"total_clicks": 200, "total_impressions": 2000, "unique_queries": 10}
        result = a._assess_brand_dependency(branded, non_branded)
        self.assertEqual(result["risk_level"], "critical")
        self.assertGreaterEqual(result["dependency_score"], 70)

    def test_low_risk(self):
        a = self._make_analyzer()
        branded = {"total_clicks": 100, "total_impressions": 1000, "unique_queries": 10}
        non_branded = {"total_clicks": 900, "total_impressions": 9000, "unique_queries": 90}
        result = a._assess_brand_dependency(branded, non_branded)
        self.assertEqual(result["risk_level"], "low")
        self.assertLess(result["dependency_score"], 30)

    def test_moderate_risk(self):
        a = self._make_analyzer()
        branded = {"total_clicks": 350, "total_impressions": 3500, "unique_queries": 35}
        non_branded = {"total_clicks": 650, "total_impressions": 6500, "unique_queries": 65}
        result = a._assess_brand_dependency(branded, non_branded)
        self.assertEqual(result["risk_level"], "moderate")

    def test_high_risk(self):
        a = self._make_analyzer()
        branded = {"total_clicks": 550, "total_impressions": 5500, "unique_queries": 55}
        non_branded = {"total_clicks": 450, "total_impressions": 4500, "unique_queries": 45}
        result = a._assess_brand_dependency(branded, non_branded)
        self.assertEqual(result["risk_level"], "high")

    def test_output_keys(self):
        a = self._make_analyzer()
        branded = {"total_clicks": 500, "total_impressions": 5000, "unique_queries": 50}
        non_branded = {"total_clicks": 500, "total_impressions": 5000, "unique_queries": 50}
        result = a._assess_brand_dependency(branded, non_branded)
        expected_keys = {
            "dependency_score", "risk_level", "risk_label",
            "branded_click_share_pct", "branded_impression_share_pct",
            "non_branded_click_share_pct", "non_branded_impression_share_pct",
        }
        self.assertEqual(set(result.keys()), expected_keys)

    def test_shares_sum_to_100(self):
        a = self._make_analyzer()
        branded = {"total_clicks": 300, "total_impressions": 4000, "unique_queries": 30}
        non_branded = {"total_clicks": 700, "total_impressions": 6000, "unique_queries": 70}
        result = a._assess_brand_dependency(branded, non_branded)
        self.assertAlmostEqual(
            result["branded_click_share_pct"] + result["non_branded_click_share_pct"], 100, places=1
        )
        self.assertAlmostEqual(
            result["branded_impression_share_pct"] + result["non_branded_impression_share_pct"], 100, places=1
        )

    def test_zero_total_clicks(self):
        a = self._make_analyzer()
        branded = {"total_clicks": 0, "total_impressions": 0, "unique_queries": 0}
        non_branded = {"total_clicks": 0, "total_impressions": 0, "unique_queries": 0}
        result = a._assess_brand_dependency(branded, non_branded)
        self.assertEqual(result["branded_click_share_pct"], 0.0)
        self.assertEqual(result["risk_level"], "low")


# ===================================================================
# 10. _top_queries
# ===================================================================
class TestTopQueries(unittest.TestCase):
    """Tests for BrandedSplitAnalyzer._top_queries."""

    def _make_analyzer(self):
        return BrandedSplitAnalyzer([], ["brand"])

    def test_sorted_by_clicks(self):
        a = self._make_analyzer()
        rows = [
            {"_query": "low", "clicks": 1, "impressions": 100},
            {"_query": "high", "clicks": 100, "impressions": 1000},
            {"_query": "mid", "clicks": 50, "impressions": 500},
        ]
        result = a._top_queries(rows)
        self.assertEqual(result[0]["query"], "high")
        self.assertEqual(result[-1]["query"], "low")

    def test_limit_respected(self):
        a = self._make_analyzer()
        rows = [{"_query": f"q{i}", "clicks": i, "impressions": i * 10} for i in range(50)]
        result = a._top_queries(rows, limit=5)
        self.assertEqual(len(result), 5)

    def test_aggregates_same_query(self):
        a = self._make_analyzer()
        rows = [
            {"_query": "q1", "clicks": 10, "impressions": 100, "page": "/a"},
            {"_query": "q1", "clicks": 5, "impressions": 50, "page": "/b"},
        ]
        result = a._top_queries(rows)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["clicks"], 15)
        self.assertEqual(result[0]["page_count"], 2)

    def test_ctr_calculated(self):
        a = self._make_analyzer()
        rows = [{"_query": "q1", "clicks": 10, "impressions": 200}]
        result = a._top_queries(rows)
        self.assertEqual(result[0]["ctr_pct"], 5.0)

    def test_output_keys(self):
        a = self._make_analyzer()
        rows = [{"_query": "q1", "clicks": 10, "impressions": 200, "position": 5.0, "page": "/a"}]
        result = a._top_queries(rows)
        expected = {"query", "clicks", "impressions", "avg_position", "ctr_pct", "page_count"}
        self.assertEqual(set(result[0].keys()), expected)

    def test_empty_rows(self):
        a = self._make_analyzer()
        result = a._top_queries([])
        self.assertEqual(result, [])

    def test_position_averaged(self):
        a = self._make_analyzer()
        rows = [
            {"_query": "q1", "clicks": 5, "impressions": 50, "position": 3.0},
            {"_query": "q1", "clicks": 5, "impressions": 50, "position": 7.0},
        ]
        result = a._top_queries(rows)
        self.assertEqual(result[0]["avg_position"], 5.0)


# ===================================================================
# 11. _find_non_branded_opportunities
# ===================================================================
class TestFindNonBrandedOpportunities(unittest.TestCase):
    """Tests for BrandedSplitAnalyzer._find_non_branded_opportunities."""

    def test_low_impression_excluded(self):
        a = BrandedSplitAnalyzer([], ["brand"])
        a.non_branded_rows = [
            {"_query": "small", "clicks": 0, "impressions": 10, "position": 15.0},
        ]
        result = a._find_non_branded_opportunities()
        self.assertEqual(len(result), 0)

    def test_top3_position_excluded(self):
        a = BrandedSplitAnalyzer([], ["brand"])
        a.non_branded_rows = [
            {"_query": "top", "clicks": 50, "impressions": 500, "position": 2.0},
        ]
        result = a._find_non_branded_opportunities()
        self.assertEqual(len(result), 0)

    def test_striking_distance(self):
        a = BrandedSplitAnalyzer([], ["brand"])
        a.non_branded_rows = [
            {"_query": "mid", "clicks": 5, "impressions": 200, "position": 12.0},
        ]
        result = a._find_non_branded_opportunities()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["opportunity_type"], "striking_distance")

    def test_long_tail_type(self):
        a = BrandedSplitAnalyzer([], ["brand"])
        a.non_branded_rows = [
            {"_query": "far", "clicks": 1, "impressions": 100, "position": 50.0},
        ]
        result = a._find_non_branded_opportunities()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["opportunity_type"], "long_tail")

    def test_sorted_by_priority(self):
        a = BrandedSplitAnalyzer([], ["brand"])
        a.non_branded_rows = [
            {"_query": "low_pri", "clicks": 1, "impressions": 60, "position": 5.0},
            {"_query": "high_pri", "clicks": 1, "impressions": 5000, "position": 15.0},
        ]
        result = a._find_non_branded_opportunities()
        if len(result) >= 2:
            self.assertGreaterEqual(result[0]["priority_score"], result[1]["priority_score"])

    def test_capped_at_40(self):
        a = BrandedSplitAnalyzer([], ["brand"])
        a.non_branded_rows = [
            {"_query": f"q{i}", "clicks": 1, "impressions": 100, "position": 10.0}
            for i in range(60)
        ]
        result = a._find_non_branded_opportunities()
        self.assertLessEqual(len(result), 40)

    def test_output_keys(self):
        a = BrandedSplitAnalyzer([], ["brand"])
        a.non_branded_rows = [
            {"_query": "q1", "clicks": 5, "impressions": 200, "position": 10.0},
        ]
        result = a._find_non_branded_opportunities()
        expected = {"query", "clicks", "impressions", "avg_position", "ctr_pct", "priority_score", "opportunity_type"}
        self.assertEqual(set(result[0].keys()), expected)

    def test_empty_non_branded(self):
        a = BrandedSplitAnalyzer([], ["brand"])
        a.non_branded_rows = []
        result = a._find_non_branded_opportunities()
        self.assertEqual(result, [])


# ===================================================================
# 12. _analyze_trends
# ===================================================================
class TestAnalyzeTrends(unittest.TestCase):
    """Tests for BrandedSplitAnalyzer._analyze_trends."""

    def test_no_dates_returns_unavailable(self):
        a = BrandedSplitAnalyzer([], ["brand"])
        a.branded_rows = [{"_query": "q1", "clicks": 10}]
        a.non_branded_rows = []
        result = a._analyze_trends()
        self.assertFalse(result["available"])

    def test_with_dates_available(self):
        a = BrandedSplitAnalyzer([], ["brand"])
        a.branded_rows = [
            {"_query": "q1", "clicks": 10, "impressions": 100, "date": "2025-01-06"},
        ]
        a.non_branded_rows = [
            {"_query": "q2", "clicks": 5, "impressions": 50, "date": "2025-01-06"},
        ]
        result = a._analyze_trends()
        self.assertTrue(result["available"])
        self.assertGreater(result["weeks_analyzed"], 0)

    def test_timeline_structure(self):
        a = BrandedSplitAnalyzer([], ["brand"])
        a.branded_rows = [
            {"_query": "q1", "clicks": 10, "impressions": 100, "date": "2025-01-06"},
        ]
        a.non_branded_rows = []
        result = a._analyze_trends()
        self.assertIn("timeline", result)
        if result["timeline"]:
            entry = result["timeline"][0]
            self.assertIn("week", entry)
            self.assertIn("branded_clicks", entry)
            self.assertIn("non_branded_clicks", entry)
            self.assertIn("branded_click_share_pct", entry)

    def test_trend_direction_increasing(self):
        a = BrandedSplitAnalyzer([], ["brand"])
        # Early: branded low, late: branded high
        a.branded_rows = []
        a.non_branded_rows = []
        for i in range(12):
            week_date = f"2025-{1 + i // 4:02d}-{(i % 4) * 7 + 1:02d}"
            if i < 4:
                a.branded_rows.append({"_query": "b", "clicks": 10, "impressions": 100, "date": week_date})
                a.non_branded_rows.append({"_query": "nb", "clicks": 90, "impressions": 900, "date": week_date})
            else:
                a.branded_rows.append({"_query": "b", "clicks": 80, "impressions": 800, "date": week_date})
                a.non_branded_rows.append({"_query": "nb", "clicks": 20, "impressions": 200, "date": week_date})
        result = a._analyze_trends()
        self.assertTrue(result["available"])
        if result.get("trend", {}).get("trend_direction"):
            self.assertEqual(result["trend"]["trend_direction"], "increasing_brand_dependency")

    def test_keys_format_date(self):
        a = BrandedSplitAnalyzer([], ["brand"])
        a.branded_rows = [
            {"_query": "q1", "clicks": 10, "impressions": 100, "keys": ["2025-01-06", "q1"]},
        ]
        a.non_branded_rows = []
        result = a._analyze_trends()
        self.assertTrue(result["available"])

    def test_empty_both_rows(self):
        a = BrandedSplitAnalyzer([], ["brand"])
        a.branded_rows = []
        a.non_branded_rows = []
        result = a._analyze_trends()
        self.assertFalse(result["available"])


# ===================================================================
# 13. _page_brand_dependency
# ===================================================================
class TestPageBrandDependency(unittest.TestCase):
    """Tests for BrandedSplitAnalyzer._page_brand_dependency."""

    def test_high_dependency_page(self):
        a = BrandedSplitAnalyzer([], ["brand"])
        a.branded_rows = [{"_query": "brand", "clicks": 90, "impressions": 900, "page": "/home"}]
        a.non_branded_rows = [{"_query": "other", "clicks": 10, "impressions": 100, "page": "/home"}]
        result = a._page_brand_dependency()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["dependency_level"], "high")
        self.assertGreaterEqual(result[0]["branded_share_pct"], 80)

    def test_low_dependency_page(self):
        a = BrandedSplitAnalyzer([], ["brand"])
        a.branded_rows = [{"_query": "brand", "clicks": 10, "impressions": 100, "page": "/blog"}]
        a.non_branded_rows = [{"_query": "other", "clicks": 90, "impressions": 900, "page": "/blog"}]
        result = a._page_brand_dependency()
        self.assertEqual(result[0]["dependency_level"], "low")

    def test_moderate_dependency(self):
        a = BrandedSplitAnalyzer([], ["brand"])
        a.branded_rows = [{"_query": "brand", "clicks": 60, "impressions": 600, "page": "/about"}]
        a.non_branded_rows = [{"_query": "other", "clicks": 40, "impressions": 400, "page": "/about"}]
        result = a._page_brand_dependency()
        self.assertEqual(result[0]["dependency_level"], "moderate")

    def test_low_traffic_excluded(self):
        a = BrandedSplitAnalyzer([], ["brand"])
        a.branded_rows = [{"_query": "brand", "clicks": 3, "impressions": 30, "page": "/tiny"}]
        a.non_branded_rows = [{"_query": "other", "clicks": 2, "impressions": 20, "page": "/tiny"}]
        result = a._page_brand_dependency()
        self.assertEqual(len(result), 0)

    def test_sorted_by_branded_share(self):
        a = BrandedSplitAnalyzer([], ["brand"])
        a.branded_rows = [
            {"_query": "b", "clicks": 90, "impressions": 900, "page": "/high"},
            {"_query": "b", "clicks": 50, "impressions": 500, "page": "/mid"},
        ]
        a.non_branded_rows = [
            {"_query": "nb", "clicks": 10, "impressions": 100, "page": "/high"},
            {"_query": "nb", "clicks": 50, "impressions": 500, "page": "/mid"},
        ]
        result = a._page_brand_dependency()
        self.assertGreaterEqual(result[0]["branded_share_pct"], result[1]["branded_share_pct"])

    def test_capped_at_30(self):
        a = BrandedSplitAnalyzer([], ["brand"])
        a.branded_rows = [
            {"_query": "b", "clicks": 50, "impressions": 500, "page": f"/p{i}"} for i in range(40)
        ]
        a.non_branded_rows = [
            {"_query": "nb", "clicks": 50, "impressions": 500, "page": f"/p{i}"} for i in range(40)
        ]
        result = a._page_brand_dependency()
        self.assertLessEqual(len(result), 30)

    def test_url_key_fallback(self):
        a = BrandedSplitAnalyzer([], ["brand"])
        a.branded_rows = [{"_query": "b", "clicks": 50, "impressions": 500, "url": "/alt"}]
        a.non_branded_rows = [{"_query": "nb", "clicks": 50, "impressions": 500, "url": "/alt"}]
        result = a._page_brand_dependency()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["page"], "/alt")

    def test_output_keys(self):
        a = BrandedSplitAnalyzer([], ["brand"])
        a.branded_rows = [{"_query": "b", "clicks": 80, "impressions": 800, "page": "/p"}]
        a.non_branded_rows = [{"_query": "nb", "clicks": 20, "impressions": 200, "page": "/p"}]
        result = a._page_brand_dependency()
        expected = {"page", "total_clicks", "branded_clicks", "non_branded_clicks", "branded_share_pct", "dependency_level"}
        self.assertEqual(set(result[0].keys()), expected)


# ===================================================================
# 14. _detect_brand_cannibalization
# ===================================================================
class TestDetectBrandCannibalization(unittest.TestCase):
    """Tests for BrandedSplitAnalyzer._detect_brand_cannibalization."""

    def test_no_cannibalization(self):
        a = BrandedSplitAnalyzer([], ["brand"])
        a.branded_rows = [
            {"_query": "brand pricing", "clicks": 10, "impressions": 100, "page": "/pricing", "position": 1.0},
        ]
        result = a._detect_brand_cannibalization()
        self.assertEqual(len(result), 0)

    def test_cannibalization_detected(self):
        a = BrandedSplitAnalyzer([], ["brand"])
        a.branded_rows = [
            {"_query": "brand pricing", "clicks": 10, "impressions": 100, "page": "/pricing", "position": 1.0},
            {"_query": "brand pricing", "clicks": 5, "impressions": 80, "page": "/about", "position": 5.0},
        ]
        result = a._detect_brand_cannibalization()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["query"], "brand pricing")
        self.assertEqual(result[0]["page_count"], 2)

    def test_sorted_by_impressions(self):
        a = BrandedSplitAnalyzer([], ["brand"])
        a.branded_rows = [
            {"_query": "q1", "clicks": 5, "impressions": 50, "page": "/a", "position": 1.0},
            {"_query": "q1", "clicks": 5, "impressions": 50, "page": "/b", "position": 2.0},
            {"_query": "q2", "clicks": 10, "impressions": 500, "page": "/c", "position": 1.0},
            {"_query": "q2", "clicks": 10, "impressions": 500, "page": "/d", "position": 2.0},
        ]
        result = a._detect_brand_cannibalization()
        self.assertEqual(result[0]["query"], "q2")

    def test_capped_at_20(self):
        a = BrandedSplitAnalyzer([], ["brand"])
        a.branded_rows = []
        for i in range(30):
            a.branded_rows.append({"_query": f"q{i}", "clicks": 5, "impressions": 50, "page": "/a", "position": 1.0})
            a.branded_rows.append({"_query": f"q{i}", "clicks": 5, "impressions": 50, "page": "/b", "position": 2.0})
        result = a._detect_brand_cannibalization()
        self.assertLessEqual(len(result), 20)

    def test_pages_capped_at_5(self):
        a = BrandedSplitAnalyzer([], ["brand"])
        a.branded_rows = [
            {"_query": "q1", "clicks": 1, "impressions": 10, "page": f"/p{i}", "position": float(i)}
            for i in range(8)
        ]
        result = a._detect_brand_cannibalization()
        self.assertLessEqual(len(result[0]["pages"]), 5)

    def test_pages_sorted_by_clicks(self):
        a = BrandedSplitAnalyzer([], ["brand"])
        a.branded_rows = [
            {"_query": "q1", "clicks": 20, "impressions": 200, "page": "/winner", "position": 1.0},
            {"_query": "q1", "clicks": 1, "impressions": 100, "page": "/loser", "position": 5.0},
        ]
        result = a._detect_brand_cannibalization()
        self.assertEqual(result[0]["pages"][0]["page"], "/winner")

    def test_empty_branded_rows(self):
        a = BrandedSplitAnalyzer([], ["brand"])
        a.branded_rows = []
        result = a._detect_brand_cannibalization()
        self.assertEqual(result, [])


# ===================================================================
# 15. _generate_recommendations
# ===================================================================
class TestGenerateRecommendations(unittest.TestCase):
    """Tests for BrandedSplitAnalyzer._generate_recommendations."""

    def _make_analyzer(self):
        return BrandedSplitAnalyzer([], ["brand"])

    def test_critical_dependency_rec(self):
        a = self._make_analyzer()
        dep = {"risk_level": "critical", "branded_click_share_pct": 85}
        result = a._generate_recommendations(dep, [], {"trend": {}}, [], [])
        categories = [r["category"] for r in result]
        self.assertIn("brand_dependency", categories)

    def test_high_dependency_rec(self):
        a = self._make_analyzer()
        dep = {"risk_level": "high", "branded_click_share_pct": 60}
        result = a._generate_recommendations(dep, [], {"trend": {}}, [], [])
        categories = [r["category"] for r in result]
        self.assertIn("brand_dependency", categories)

    def test_no_rec_for_low_dependency(self):
        a = self._make_analyzer()
        dep = {"risk_level": "low", "branded_click_share_pct": 15}
        result = a._generate_recommendations(dep, [], {"trend": {}}, [], [])
        categories = [r["category"] for r in result]
        self.assertNotIn("brand_dependency", categories)

    def test_opportunities_rec(self):
        a = self._make_analyzer()
        dep = {"risk_level": "low", "branded_click_share_pct": 15}
        opps = [{"query": f"q{i}", "impressions": 100} for i in range(10)]
        result = a._generate_recommendations(dep, opps, {"trend": {}}, [], [])
        categories = [r["category"] for r in result]
        self.assertIn("non_branded_growth", categories)

    def test_trend_warning_rec(self):
        a = self._make_analyzer()
        dep = {"risk_level": "low", "branded_click_share_pct": 15}
        trends = {"trend": {
            "trend_direction": "increasing_brand_dependency",
            "branded_share_change_pp": 5.0,
            "non_branded_click_growth_pct": -10,
        }}
        result = a._generate_recommendations(dep, [], trends, [], [])
        categories = [r["category"] for r in result]
        self.assertIn("trend_warning", categories)

    def test_positive_trend_rec(self):
        a = self._make_analyzer()
        dep = {"risk_level": "low", "branded_click_share_pct": 15}
        trends = {"trend": {
            "trend_direction": "decreasing_brand_dependency",
            "non_branded_click_growth_pct": 30,
        }}
        result = a._generate_recommendations(dep, [], trends, [], [])
        categories = [r["category"] for r in result]
        self.assertIn("positive_trend", categories)

    def test_page_dependency_rec(self):
        a = self._make_analyzer()
        dep = {"risk_level": "low", "branded_click_share_pct": 15}
        page_dep = [{"dependency_level": "high"}, {"dependency_level": "high"}]
        result = a._generate_recommendations(dep, [], {"trend": {}}, page_dep, [])
        categories = [r["category"] for r in result]
        self.assertIn("page_dependency", categories)

    def test_cannibalization_rec(self):
        a = self._make_analyzer()
        dep = {"risk_level": "low", "branded_click_share_pct": 15}
        cannib = [{"query": "q1"}]
        result = a._generate_recommendations(dep, [], {"trend": {}}, [], cannib)
        categories = [r["category"] for r in result]
        self.assertIn("brand_cannibalization", categories)

    def test_low_brand_awareness_rec(self):
        a = self._make_analyzer()
        dep = {"risk_level": "low", "branded_click_share_pct": 5}
        result = a._generate_recommendations(dep, [], {"trend": {}}, [], [])
        categories = [r["category"] for r in result]
        self.assertIn("brand_awareness", categories)

    def test_sorted_by_priority(self):
        a = self._make_analyzer()
        dep = {"risk_level": "critical", "branded_click_share_pct": 85}
        opps = [{"query": f"q{i}", "impressions": 100} for i in range(5)]
        page_dep = [{"dependency_level": "high"}]
        cannib = [{"query": "q1"}]
        result = a._generate_recommendations(dep, opps, {"trend": {}}, page_dep, cannib)
        priority_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
        for i in range(len(result) - 1):
            self.assertLessEqual(
                priority_order.get(result[i]["priority"], 4),
                priority_order.get(result[i + 1]["priority"], 4),
            )

    def test_rec_has_required_keys(self):
        a = self._make_analyzer()
        dep = {"risk_level": "critical", "branded_click_share_pct": 85}
        result = a._generate_recommendations(dep, [], {"trend": {}}, [], [])
        for rec in result:
            self.assertIn("category", rec)
            self.assertIn("priority", rec)
            self.assertIn("title", rec)
            self.assertIn("detail", rec)
            self.assertIn("impact", rec)


# ===================================================================
# 16. _build_summary
# ===================================================================
class TestBuildSummary(unittest.TestCase):
    """Tests for BrandedSplitAnalyzer._build_summary."""

    def _make_analyzer(self):
        return BrandedSplitAnalyzer([], ["brand"])

    def _default_args(self):
        branded_agg = {"total_clicks": 300, "unique_queries": 30, "avg_position": 2.5, "avg_ctr_pct": 15.0}
        non_branded_agg = {"total_clicks": 700, "unique_queries": 70, "avg_position": 12.3, "avg_ctr_pct": 3.5}
        dependency = {
            "branded_click_share_pct": 30.0,
            "non_branded_click_share_pct": 70.0,
            "dependency_score": 35.0,
            "risk_level": "moderate",
            "risk_label": "Healthy brand presence",
        }
        return branded_agg, non_branded_agg, dependency

    def test_returns_string(self):
        a = self._make_analyzer()
        branded_agg, non_branded_agg, dependency = self._default_args()
        result = a._build_summary(branded_agg, non_branded_agg, dependency, [], {"trend": {}}, [])
        self.assertIsInstance(result, str)

    def test_mentions_total_clicks(self):
        a = self._make_analyzer()
        branded_agg, non_branded_agg, dependency = self._default_args()
        result = a._build_summary(branded_agg, non_branded_agg, dependency, [], {"trend": {}}, [])
        self.assertIn("1,000", result)

    def test_mentions_dependency_score(self):
        a = self._make_analyzer()
        branded_agg, non_branded_agg, dependency = self._default_args()
        result = a._build_summary(branded_agg, non_branded_agg, dependency, [], {"trend": {}}, [])
        self.assertIn("35.0", result)

    def test_mentions_risk_level(self):
        a = self._make_analyzer()
        branded_agg, non_branded_agg, dependency = self._default_args()
        result = a._build_summary(branded_agg, non_branded_agg, dependency, [], {"trend": {}}, [])
        self.assertIn("moderate", result)

    def test_mentions_opportunities(self):
        a = self._make_analyzer()
        branded_agg, non_branded_agg, dependency = self._default_args()
        opps = [{"query": "q1"}] * 5
        result = a._build_summary(branded_agg, non_branded_agg, dependency, opps, {"trend": {}}, [])
        self.assertIn("5", result)
        self.assertIn("opportunities", result.lower())

    def test_mentions_recommendations(self):
        a = self._make_analyzer()
        branded_agg, non_branded_agg, dependency = self._default_args()
        recs = [{"title": "r"}] * 3
        result = a._build_summary(branded_agg, non_branded_agg, dependency, [], {"trend": {}}, recs)
        self.assertIn("3", result)
        self.assertIn("recommendation", result.lower())

    def test_trend_direction_mentioned(self):
        a = self._make_analyzer()
        branded_agg, non_branded_agg, dependency = self._default_args()
        trends = {"trend": {"trend_direction": "stable"}}
        result = a._build_summary(branded_agg, non_branded_agg, dependency, [], trends, [])
        self.assertIn("stable", result.lower())


# ===================================================================
# 17. Full pipeline — analyze()
# ===================================================================
class TestAnalyzeFullPipeline(unittest.TestCase):
    """Tests for BrandedSplitAnalyzer.analyze() full pipeline."""

    def _make_data(self):
        return [
            {"query": "acme pricing", "clicks": 50, "impressions": 500, "position": 1.5, "page": "/pricing"},
            {"query": "acme reviews", "clicks": 30, "impressions": 300, "position": 2.0, "page": "/reviews"},
            {"query": "best widget tools", "clicks": 20, "impressions": 800, "position": 8.0, "page": "/blog/widgets"},
            {"query": "how to use widgets", "clicks": 10, "impressions": 600, "position": 12.0, "page": "/blog/guide"},
            {"query": "widget comparison 2025", "clicks": 5, "impressions": 400, "position": 15.0, "page": "/blog/compare"},
        ]

    def test_output_schema(self):
        result = BrandedSplitAnalyzer(self._make_data(), ["acme"]).analyze()
        expected_keys = {
            "summary", "brand_terms_used", "branded_pct", "non_branded_growth",
            "segments", "brand_dependency", "top_branded_queries", "top_non_branded_queries",
            "non_branded_opportunities", "trends", "page_brand_dependency",
            "brand_cannibalization", "recommendations",
        }
        self.assertTrue(expected_keys.issubset(set(result.keys())))

    def test_no_data_error(self):
        result = BrandedSplitAnalyzer([], ["acme"]).analyze()
        self.assertEqual(result["error"], "no_data")
        self.assertIsNone(result["branded_pct"])

    def test_no_brand_terms_error(self):
        result = BrandedSplitAnalyzer(self._make_data(), []).analyze()
        self.assertEqual(result["error"], "no_brand_terms")

    def test_no_brand_terms_none_error(self):
        result = BrandedSplitAnalyzer(self._make_data(), None).analyze()
        self.assertEqual(result["error"], "no_brand_terms")

    def test_branded_pct_numeric(self):
        result = BrandedSplitAnalyzer(self._make_data(), ["acme"]).analyze()
        self.assertIsInstance(result["branded_pct"], (int, float))
        self.assertGreater(result["branded_pct"], 0)

    def test_segments_present(self):
        result = BrandedSplitAnalyzer(self._make_data(), ["acme"]).analyze()
        self.assertIn("branded", result["segments"])
        self.assertIn("non_branded", result["segments"])

    def test_summary_is_string(self):
        result = BrandedSplitAnalyzer(self._make_data(), ["acme"]).analyze()
        self.assertIsInstance(result["summary"], str)
        self.assertGreater(len(result["summary"]), 50)

    def test_brand_terms_tracked(self):
        result = BrandedSplitAnalyzer(self._make_data(), ["Acme"]).analyze()
        self.assertIn("acme", result["brand_terms_used"])

    def test_recommendations_list(self):
        result = BrandedSplitAnalyzer(self._make_data(), ["acme"]).analyze()
        self.assertIsInstance(result["recommendations"], list)

    def test_top_queries_lists(self):
        result = BrandedSplitAnalyzer(self._make_data(), ["acme"]).analyze()
        self.assertIsInstance(result["top_branded_queries"], list)
        self.assertIsInstance(result["top_non_branded_queries"], list)


# ===================================================================
# 18. Public API — analyze_branded_split()
# ===================================================================
class TestAnalyzeBrandedSplit(unittest.TestCase):
    """Tests for the public analyze_branded_split function."""

    def test_basic_call(self):
        data = [{"query": "brand stuff", "clicks": 10, "impressions": 100}]
        result = analyze_branded_split(data, brand_terms=["brand"])
        self.assertIn("summary", result)

    def test_no_data(self):
        result = analyze_branded_split([], brand_terms=["brand"])
        self.assertEqual(result["error"], "no_data")

    def test_none_data(self):
        result = analyze_branded_split(None, brand_terms=["brand"])
        self.assertEqual(result["error"], "no_data")

    def test_no_brand_terms(self):
        data = [{"query": "stuff", "clicks": 10, "impressions": 100}]
        result = analyze_branded_split(data, brand_terms=None)
        self.assertEqual(result["error"], "no_brand_terms")

    def test_dict_input_treated_as_empty(self):
        result = analyze_branded_split({"not": "a list"}, brand_terms=["brand"])
        self.assertEqual(result["error"], "no_data")


# ===================================================================
# 19. Edge cases
# ===================================================================
class TestEdgeCases(unittest.TestCase):
    """Edge case and boundary tests."""

    def test_unicode_queries(self):
        data = [{"query": "ácme café", "clicks": 10, "impressions": 100, "page": "/café"}]
        result = analyze_branded_split(data, brand_terms=["ácme"])
        self.assertIn("summary", result)
        self.assertNotEqual(result.get("error"), "no_data")

    def test_very_large_dataset(self):
        data = [
            {"query": f"q{i}", "clicks": i, "impressions": i * 10, "position": float(i % 50) + 1, "page": f"/p{i % 100}"}
            for i in range(1000)
        ]
        data.append({"query": "brand main", "clicks": 500, "impressions": 5000, "position": 1.0, "page": "/home"})
        result = analyze_branded_split(data, brand_terms=["brand"])
        self.assertIn("summary", result)
        self.assertNotEqual(result.get("error"), "no_data")

    def test_zero_impressions_ctr(self):
        data = [{"query": "brand zero", "clicks": 0, "impressions": 0, "page": "/z"}]
        result = analyze_branded_split(data, brand_terms=["brand"])
        self.assertIn("segments", result)
        self.assertEqual(result["segments"]["branded"]["avg_ctr_pct"], 0.0)

    def test_no_page_field(self):
        data = [{"query": "brand test", "clicks": 10, "impressions": 100}]
        result = analyze_branded_split(data, brand_terms=["brand"])
        self.assertIn("segments", result)

    def test_special_chars_in_brand(self):
        data = [{"query": "c++ coding", "clicks": 10, "impressions": 100}]
        result = analyze_branded_split(data, brand_terms=["c++"])
        self.assertIn("summary", result)

    def test_single_row_branded(self):
        data = [{"query": "myco", "clicks": 100, "impressions": 1000, "position": 1.0, "page": "/"}]
        result = analyze_branded_split(data, brand_terms=["myco"])
        self.assertEqual(result["branded_pct"], 100.0)
        self.assertEqual(result["segments"]["non_branded"]["total_clicks"], 0)

    def test_single_row_non_branded(self):
        data = [{"query": "generic term", "clicks": 100, "impressions": 1000, "position": 1.0, "page": "/"}]
        result = analyze_branded_split(data, brand_terms=["myco"])
        self.assertEqual(result["branded_pct"], 0.0)

    def test_multiple_brand_terms(self):
        data = [
            {"query": "alpha pricing", "clicks": 10, "impressions": 100},
            {"query": "beta reviews", "clicks": 10, "impressions": 100},
            {"query": "generic stuff", "clicks": 10, "impressions": 100},
        ]
        result = analyze_branded_split(data, brand_terms=["alpha", "beta"])
        # Both alpha and beta should be branded
        branded_clicks = result["segments"]["branded"]["total_clicks"]
        self.assertEqual(branded_clicks, 20)

    def test_position_zero_excluded_from_avg(self):
        data = [
            {"query": "brand q", "clicks": 10, "impressions": 100, "position": 0, "page": "/a"},
            {"query": "brand q2", "clicks": 10, "impressions": 100, "position": 5.0, "page": "/b"},
        ]
        result = analyze_branded_split(data, brand_terms=["brand"])
        # position=0 is falsy, so should be excluded from avg
        self.assertEqual(result["segments"]["branded"]["avg_position"], 5.0)

    def test_trend_with_dates(self):
        data = []
        for i in range(30):
            day = f"2025-01-{i + 1:02d}"
            data.append({"query": "brand daily", "clicks": 10, "impressions": 100, "date": day, "page": "/"})
            data.append({"query": "generic daily", "clicks": 20, "impressions": 200, "date": day, "page": "/blog"})
        result = analyze_branded_split(data, brand_terms=["brand"])
        self.assertTrue(result["trends"]["available"])
        self.assertGreater(result["trends"]["weeks_analyzed"], 0)

    def test_non_branded_growth_none_without_dates(self):
        data = [
            {"query": "brand q", "clicks": 50, "impressions": 500, "page": "/"},
            {"query": "other q", "clicks": 50, "impressions": 500, "page": "/blog"},
        ]
        result = analyze_branded_split(data, brand_terms=["brand"])
        self.assertIsNone(result["non_branded_growth"])


if __name__ == "__main__":
    unittest.main()
