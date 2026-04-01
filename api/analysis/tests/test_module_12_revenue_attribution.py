"""
Comprehensive test suite for Module 12: Revenue Attribution & ROI Modeling.

Tests cover:
  1. Constants — CTR benchmarks completeness and ordering
  2. _expected_ctr — interpolation, boundary, edge cases
  3. _page_key — URL normalisation
  4. _normalise_gsc — DataFrame, list, None, invalid
  5. _build_page_map — various page key variants, DataFrame, None
  6. _aggregate_by_page — basic aggregation, empty, weighted avg position
  7. _aggregate_by_query — basic aggregation, page dedup, empty
  8. _compute_page_revenue — ecommerce, conversion, default value, no data
  9. _compute_query_revenue — click-share attribution, empty, no revenue pages
 10. _revenue_at_risk — risk factors, severity, sorting, empty
 11. _position_improvement_roi — scenarios, filtering, priority score, empty
 12. _conversion_funnel_analysis — tiers, leaks, overall metrics
 13. _revenue_concentration — Pareto analysis, risk levels
 14. _generate_recommendations — all recommendation categories, sorting
 15. _build_summary — narrative content, mentions key metrics
 16. Full pipeline analyze() — output schema, no data, full data
 17. Public API estimate_revenue_attribution — basic, no data
 18. Edge cases — unicode, large dataset, zero values, special chars
"""
import math
import unittest
from unittest.mock import MagicMock

from api.analysis.module_12_revenue_attribution import (
    RevenueAttributionAnalyzer,
    _CTR_BENCHMARKS,
    _expected_ctr,
    _page_key,
    estimate_revenue_attribution,
)


# ---------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------
def _gsc_row(query="test", page="https://example.com/page", clicks=10,
             impressions=100, position=5.0, ctr=0.1):
    return {"query": query, "page": page, "clicks": clicks,
            "impressions": impressions, "position": position, "ctr": ctr}


def _conv_row(page="/page", conversions=5, conversion_rate=0.05):
    return {"page": page, "conversions": conversions,
            "conversion_rate": conversion_rate}


def _eng_row(page="/page", sessions=100, bounce_rate=0.5,
             avg_session_duration=60, pages_per_session=3):
    return {"page": page, "sessions": sessions, "bounce_rate": bounce_rate,
            "avg_session_duration": avg_session_duration,
            "pages_per_session": pages_per_session}


def _ecom_row(page="/page", revenue=500, transactions=10, avg_order_value=50):
    return {"page": page, "revenue": revenue, "transactions": transactions,
            "avg_order_value": avg_order_value}


def _make_analyzer(gsc=None, conv=None, eng=None, ecom=None):
    return RevenueAttributionAnalyzer(
        gsc_data=gsc or [], ga4_conversions=conv,
        ga4_engagement=eng, ga4_ecommerce=ecom,
    )


# ===================================================================
# 1. Constants
# ===================================================================
class TestCTRBenchmarks(unittest.TestCase):
    def test_has_20_entries(self):
        self.assertEqual(len(_CTR_BENCHMARKS), 20)

    def test_position_1_highest(self):
        self.assertEqual(max(_CTR_BENCHMARKS.values()), _CTR_BENCHMARKS[1])

    def test_all_positive(self):
        for pos, ctr in _CTR_BENCHMARKS.items():
            self.assertGreater(ctr, 0, f"Position {pos} CTR must be positive")

    def test_monotonically_decreasing(self):
        for i in range(1, 20):
            self.assertGreaterEqual(
                _CTR_BENCHMARKS[i], _CTR_BENCHMARKS[i + 1],
                f"CTR at position {i} should be >= position {i+1}",
            )

    def test_keys_are_1_to_20(self):
        self.assertEqual(set(_CTR_BENCHMARKS.keys()), set(range(1, 21)))


# ===================================================================
# 2. _expected_ctr
# ===================================================================
class TestExpectedCtr(unittest.TestCase):
    def test_position_1(self):
        self.assertEqual(_expected_ctr(1), _CTR_BENCHMARKS[1])

    def test_position_20(self):
        self.assertEqual(_expected_ctr(20), _CTR_BENCHMARKS[20])

    def test_below_1(self):
        self.assertEqual(_expected_ctr(0.5), _CTR_BENCHMARKS[1])

    def test_above_20(self):
        self.assertEqual(_expected_ctr(25), _CTR_BENCHMARKS[20])

    def test_interpolation_midpoint(self):
        result = _expected_ctr(1.5)
        expected = _CTR_BENCHMARKS[1] * 0.5 + _CTR_BENCHMARKS[2] * 0.5
        self.assertAlmostEqual(result, expected, places=6)

    def test_interpolation_quarter(self):
        result = _expected_ctr(3.25)
        expected = _CTR_BENCHMARKS[3] * 0.75 + _CTR_BENCHMARKS[4] * 0.25
        self.assertAlmostEqual(result, expected, places=6)

    def test_exact_integer_position(self):
        for pos in range(1, 21):
            self.assertAlmostEqual(
                _expected_ctr(float(pos)), _CTR_BENCHMARKS[pos], places=6,
            )

    def test_returns_float(self):
        self.assertIsInstance(_expected_ctr(5.5), float)


# ===================================================================
# 3. _page_key
# ===================================================================
class TestPageKey(unittest.TestCase):
    def test_full_url(self):
        self.assertEqual(_page_key("https://example.com/blog/post"), "/blog/post")

    def test_trailing_slash_stripped(self):
        self.assertEqual(_page_key("https://example.com/page/"), "/page")

    def test_root_url(self):
        self.assertEqual(_page_key("https://example.com/"), "/")

    def test_root_no_slash(self):
        self.assertEqual(_page_key("https://example.com"), "/")

    def test_lowercase(self):
        self.assertEqual(_page_key("https://example.com/Blog/Post"), "/blog/post")

    def test_query_params_preserved(self):
        # urlparse keeps query, but path extraction strips them
        result = _page_key("https://example.com/page?q=1")
        self.assertEqual(result, "/page")

    def test_fragment_stripped(self):
        result = _page_key("https://example.com/page#section")
        self.assertEqual(result, "/page")

    def test_path_only(self):
        result = _page_key("/some/path/")
        self.assertEqual(result, "/some/path")

    def test_empty_string(self):
        result = _page_key("")
        self.assertIsInstance(result, str)

    def test_with_port(self):
        result = _page_key("https://example.com:8080/page")
        self.assertEqual(result, "/page")


# ===================================================================
# 4. _normalise_gsc
# ===================================================================
class TestNormaliseGsc(unittest.TestCase):
    def test_none_returns_empty_list(self):
        self.assertEqual(RevenueAttributionAnalyzer._normalise_gsc(None), [])

    def test_list_passthrough(self):
        data = [{"query": "a"}]
        self.assertEqual(RevenueAttributionAnalyzer._normalise_gsc(data), data)

    def test_dataframe_like(self):
        mock_df = MagicMock()
        mock_df.to_dict.return_value = [{"query": "x"}]
        result = RevenueAttributionAnalyzer._normalise_gsc(mock_df)
        self.assertEqual(result, [{"query": "x"}])
        mock_df.to_dict.assert_called_once_with("records")

    def test_non_list_non_df_returns_empty(self):
        self.assertEqual(RevenueAttributionAnalyzer._normalise_gsc("bad"), [])

    def test_empty_list(self):
        self.assertEqual(RevenueAttributionAnalyzer._normalise_gsc([]), [])

    def test_dict_returns_empty(self):
        self.assertEqual(RevenueAttributionAnalyzer._normalise_gsc({"a": 1}), [])


# ===================================================================
# 5. _build_page_map
# ===================================================================
class TestBuildPageMap(unittest.TestCase):
    def test_none_returns_empty(self):
        self.assertEqual(RevenueAttributionAnalyzer._build_page_map(None), {})

    def test_list_with_page_key(self):
        data = [{"page": "https://example.com/a", "conversions": 5}]
        result = RevenueAttributionAnalyzer._build_page_map(data)
        self.assertIn("/a", result)
        self.assertEqual(result["/a"]["conversions"], 5)

    def test_page_path_key(self):
        data = [{"page_path": "/b", "conversions": 3}]
        result = RevenueAttributionAnalyzer._build_page_map(data)
        self.assertIn("/b", result)

    def test_landing_page_key(self):
        data = [{"landing_page": "/c", "revenue": 100}]
        result = RevenueAttributionAnalyzer._build_page_map(data)
        self.assertIn("/c", result)

    def test_url_key(self):
        data = [{"url": "https://example.com/d", "sessions": 10}]
        result = RevenueAttributionAnalyzer._build_page_map(data)
        self.assertIn("/d", result)

    def test_empty_page_skipped(self):
        data = [{"conversions": 5}]  # no page key
        result = RevenueAttributionAnalyzer._build_page_map(data)
        self.assertEqual(result, {})

    def test_dataframe_like(self):
        mock_df = MagicMock()
        mock_df.to_dict.return_value = [{"page": "/x", "revenue": 10}]
        result = RevenueAttributionAnalyzer._build_page_map(mock_df)
        self.assertIn("/x", result)

    def test_non_list_returns_empty(self):
        self.assertEqual(RevenueAttributionAnalyzer._build_page_map("bad"), {})

    def test_multiple_rows(self):
        data = [
            {"page": "/a", "conversions": 1},
            {"page": "/b", "conversions": 2},
        ]
        result = RevenueAttributionAnalyzer._build_page_map(data)
        self.assertEqual(len(result), 2)


# ===================================================================
# 6. _aggregate_by_page
# ===================================================================
class TestAggregateByPage(unittest.TestCase):
    def test_basic_aggregation(self):
        rows = [
            _gsc_row(query="a", page="https://example.com/p1", clicks=10, impressions=100, position=3.0),
            _gsc_row(query="b", page="https://example.com/p1", clicks=5, impressions=50, position=5.0),
        ]
        a = _make_analyzer(gsc=rows)
        result = a._aggregate_by_page()
        self.assertIn("/p1", result)
        self.assertEqual(result["/p1"]["clicks"], 15)
        self.assertEqual(result["/p1"]["impressions"], 150)
        self.assertEqual(result["/p1"]["query_count"], 2)

    def test_weighted_avg_position(self):
        rows = [
            _gsc_row(page="https://example.com/p", clicks=5, impressions=100, position=2.0),
            _gsc_row(page="https://example.com/p", clicks=5, impressions=100, position=8.0),
        ]
        a = _make_analyzer(gsc=rows)
        result = a._aggregate_by_page()
        # weighted avg = (2*100 + 8*100) / 200 = 5.0
        self.assertAlmostEqual(result["/p"]["avg_position"], 5.0, places=1)

    def test_queries_sorted_by_clicks(self):
        rows = [
            _gsc_row(query="low", page="https://example.com/p", clicks=1),
            _gsc_row(query="high", page="https://example.com/p", clicks=100),
        ]
        a = _make_analyzer(gsc=rows)
        result = a._aggregate_by_page()
        self.assertEqual(result["/p"]["queries"][0]["query"], "high")

    def test_empty_gsc(self):
        a = _make_analyzer(gsc=[])
        result = a._aggregate_by_page()
        self.assertEqual(result, {})

    def test_no_page_skipped(self):
        rows = [{"query": "test", "clicks": 10, "impressions": 100, "position": 5}]
        a = _make_analyzer(gsc=rows)
        result = a._aggregate_by_page()
        self.assertEqual(result, {})

    def test_ctr_computed(self):
        rows = [_gsc_row(page="https://example.com/p", clicks=20, impressions=200)]
        a = _make_analyzer(gsc=rows)
        result = a._aggregate_by_page()
        self.assertAlmostEqual(result["/p"]["ctr"], 0.1, places=4)

    def test_multiple_pages(self):
        rows = [
            _gsc_row(page="https://example.com/a"),
            _gsc_row(page="https://example.com/b"),
        ]
        a = _make_analyzer(gsc=rows)
        result = a._aggregate_by_page()
        self.assertEqual(len(result), 2)


# ===================================================================
# 7. _aggregate_by_query
# ===================================================================
class TestAggregateByQuery(unittest.TestCase):
    def test_basic_aggregation(self):
        rows = [
            _gsc_row(query="term", page="https://example.com/a", clicks=10, impressions=100),
            _gsc_row(query="term", page="https://example.com/b", clicks=5, impressions=50),
        ]
        a = _make_analyzer(gsc=rows)
        result = a._aggregate_by_query()
        self.assertIn("term", result)
        self.assertEqual(result["term"]["clicks"], 15)
        self.assertEqual(result["term"]["page_count"], 2)

    def test_pages_deduped(self):
        rows = [
            _gsc_row(query="term", page="https://example.com/a"),
            _gsc_row(query="term", page="https://example.com/a"),
        ]
        a = _make_analyzer(gsc=rows)
        result = a._aggregate_by_query()
        self.assertEqual(result["term"]["page_count"], 1)

    def test_pages_capped_at_5(self):
        rows = [
            _gsc_row(query="term", page=f"https://example.com/p{i}")
            for i in range(10)
        ]
        a = _make_analyzer(gsc=rows)
        result = a._aggregate_by_query()
        self.assertLessEqual(len(result["term"]["pages"]), 5)

    def test_empty_query_skipped(self):
        rows = [{"query": "", "page": "https://example.com/a", "clicks": 10,
                 "impressions": 100, "position": 5}]
        a = _make_analyzer(gsc=rows)
        result = a._aggregate_by_query()
        self.assertNotIn("", result)

    def test_empty_gsc(self):
        a = _make_analyzer(gsc=[])
        result = a._aggregate_by_query()
        self.assertEqual(result, {})

    def test_query_key_present(self):
        rows = [_gsc_row(query="test")]
        a = _make_analyzer(gsc=rows)
        result = a._aggregate_by_query()
        self.assertEqual(result["test"]["query"], "test")


# ===================================================================
# 8. _compute_page_revenue
# ===================================================================
class TestComputePageRevenue(unittest.TestCase):
    def test_ecommerce_revenue(self):
        gsc = [_gsc_row(page="https://example.com/shop")]
        ecom = [_ecom_row(page="/shop", revenue=500, transactions=10, avg_order_value=50)]
        a = _make_analyzer(gsc=gsc, ecom=ecom)
        page_agg = a._aggregate_by_page()
        result = a._compute_page_revenue(page_agg)
        self.assertEqual(result[0]["revenue"], 500)
        self.assertEqual(result[0]["revenue_source"], "ecommerce_actual")

    def test_conversion_estimated_with_aov(self):
        gsc = [_gsc_row(page="https://example.com/p")]
        conv = [_conv_row(page="/p", conversions=10, conversion_rate=0.1)]
        ecom = [_ecom_row(page="/p", revenue=0, transactions=0, avg_order_value=100)]
        a = _make_analyzer(gsc=gsc, conv=conv, ecom=ecom)
        page_agg = a._aggregate_by_page()
        result = a._compute_page_revenue(page_agg)
        self.assertEqual(result[0]["revenue"], 1000)
        self.assertEqual(result[0]["revenue_source"], "conversion_estimated")

    def test_conversion_default_value(self):
        gsc = [_gsc_row(page="https://example.com/p")]
        conv = [_conv_row(page="/p", conversions=5)]
        a = _make_analyzer(gsc=gsc, conv=conv)
        page_agg = a._aggregate_by_page()
        result = a._compute_page_revenue(page_agg)
        self.assertEqual(result[0]["revenue"], 250)  # 5 * 50
        self.assertEqual(result[0]["revenue_source"], "conversion_default_value")

    def test_no_conversion_data(self):
        gsc = [_gsc_row(page="https://example.com/p")]
        a = _make_analyzer(gsc=gsc)
        page_agg = a._aggregate_by_page()
        result = a._compute_page_revenue(page_agg)
        self.assertEqual(result[0]["revenue"], 0)
        self.assertEqual(result[0]["revenue_source"], "no_conversion_data")

    def test_sorted_by_revenue_desc(self):
        gsc = [
            _gsc_row(page="https://example.com/low"),
            _gsc_row(page="https://example.com/high"),
        ]
        ecom = [
            _ecom_row(page="/low", revenue=10),
            _ecom_row(page="/high", revenue=1000),
        ]
        a = _make_analyzer(gsc=gsc, ecom=ecom)
        page_agg = a._aggregate_by_page()
        result = a._compute_page_revenue(page_agg)
        self.assertEqual(result[0]["page"], "/high")

    def test_revenue_per_click(self):
        gsc = [_gsc_row(page="https://example.com/p", clicks=10)]
        ecom = [_ecom_row(page="/p", revenue=100)]
        a = _make_analyzer(gsc=gsc, ecom=ecom)
        page_agg = a._aggregate_by_page()
        result = a._compute_page_revenue(page_agg)
        self.assertEqual(result[0]["revenue_per_click"], 10.0)

    def test_output_keys(self):
        gsc = [_gsc_row(page="https://example.com/p")]
        a = _make_analyzer(gsc=gsc)
        page_agg = a._aggregate_by_page()
        result = a._compute_page_revenue(page_agg)
        expected_keys = {"page", "clicks", "impressions", "avg_position", "ctr",
                         "query_count", "top_queries", "revenue", "revenue_source",
                         "conversions", "conversion_rate", "sessions", "bounce_rate",
                         "avg_session_duration", "revenue_per_click"}
        self.assertTrue(expected_keys.issubset(set(result[0].keys())))

    def test_engagement_data_included(self):
        gsc = [_gsc_row(page="https://example.com/p")]
        eng = [_eng_row(page="/p", sessions=200, bounce_rate=0.6)]
        a = _make_analyzer(gsc=gsc, eng=eng)
        page_agg = a._aggregate_by_page()
        result = a._compute_page_revenue(page_agg)
        self.assertEqual(result[0]["sessions"], 200)
        self.assertAlmostEqual(result[0]["bounce_rate"], 0.6, places=4)


# ===================================================================
# 9. _compute_query_revenue
# ===================================================================
class TestComputeQueryRevenue(unittest.TestCase):
    def test_click_share_attribution(self):
        gsc = [
            _gsc_row(query="a", page="https://example.com/p", clicks=50, impressions=500),
            _gsc_row(query="b", page="https://example.com/p", clicks=50, impressions=500),
        ]
        ecom = [_ecom_row(page="/p", revenue=1000)]
        a = _make_analyzer(gsc=gsc, ecom=ecom)
        page_agg = a._aggregate_by_page()
        query_agg = a._aggregate_by_query()
        page_rev = a._compute_page_revenue(page_agg)
        result = a._compute_query_revenue(query_agg, page_rev)
        # Each query gets 50% of page revenue
        revs = {r["query"]: r["attributed_revenue"] for r in result}
        self.assertAlmostEqual(revs["a"], 500, places=0)
        self.assertAlmostEqual(revs["b"], 500, places=0)

    def test_sorted_by_attributed_revenue_desc(self):
        gsc = [
            _gsc_row(query="low", page="https://example.com/p", clicks=10),
            _gsc_row(query="high", page="https://example.com/p", clicks=90),
        ]
        ecom = [_ecom_row(page="/p", revenue=1000)]
        a = _make_analyzer(gsc=gsc, ecom=ecom)
        page_agg = a._aggregate_by_page()
        query_agg = a._aggregate_by_query()
        page_rev = a._compute_page_revenue(page_agg)
        result = a._compute_query_revenue(query_agg, page_rev)
        self.assertEqual(result[0]["query"], "high")

    def test_capped_at_50(self):
        gsc = [
            _gsc_row(query=f"q{i}", page="https://example.com/p", clicks=1)
            for i in range(60)
        ]
        a = _make_analyzer(gsc=gsc)
        page_agg = a._aggregate_by_page()
        query_agg = a._aggregate_by_query()
        page_rev = a._compute_page_revenue(page_agg)
        result = a._compute_query_revenue(query_agg, page_rev)
        self.assertLessEqual(len(result), 50)

    def test_no_revenue_pages(self):
        gsc = [_gsc_row(query="test", page="https://example.com/p")]
        a = _make_analyzer(gsc=gsc)
        page_agg = a._aggregate_by_page()
        query_agg = a._aggregate_by_query()
        page_rev = a._compute_page_revenue(page_agg)
        result = a._compute_query_revenue(query_agg, page_rev)
        self.assertEqual(result[0]["attributed_revenue"], 0)

    def test_output_keys(self):
        gsc = [_gsc_row(query="test", page="https://example.com/p")]
        a = _make_analyzer(gsc=gsc)
        page_agg = a._aggregate_by_page()
        query_agg = a._aggregate_by_query()
        page_rev = a._compute_page_revenue(page_agg)
        result = a._compute_query_revenue(query_agg, page_rev)
        expected_keys = {"query", "clicks", "impressions", "avg_position",
                         "ctr", "page_count", "attributed_revenue", "revenue_per_click"}
        self.assertEqual(set(result[0].keys()), expected_keys)


# ===================================================================
# 10. _revenue_at_risk
# ===================================================================
class TestRevenueAtRisk(unittest.TestCase):
    def _page_entry(self, page="/p", revenue=1000, bounce_rate=0.5,
                    avg_session_duration=60, avg_position=5, ctr=0.1,
                    query_count=10, clicks=100, top_queries=None):
        return {
            "page": page, "revenue": revenue, "bounce_rate": bounce_rate,
            "avg_session_duration": avg_session_duration,
            "avg_position": avg_position, "ctr": ctr,
            "query_count": query_count, "clicks": clicks,
            "top_queries": top_queries or ["q1", "q2"],
            "revenue_per_click": round(revenue / max(clicks, 1), 2),
        }

    def test_high_bounce_rate_risk(self):
        pages = [self._page_entry(bounce_rate=0.85)]
        a = _make_analyzer()
        result = a._revenue_at_risk(pages)
        self.assertTrue(len(result) > 0)
        self.assertIn("high_bounce_rate", result[0]["risk_factors"])

    def test_elevated_bounce_rate(self):
        pages = [self._page_entry(bounce_rate=0.70)]
        a = _make_analyzer()
        result = a._revenue_at_risk(pages)
        self.assertIn("elevated_bounce_rate", result[0]["risk_factors"])

    def test_very_low_engagement(self):
        pages = [self._page_entry(avg_session_duration=10)]
        a = _make_analyzer()
        result = a._revenue_at_risk(pages)
        self.assertIn("very_low_engagement", result[0]["risk_factors"])

    def test_low_engagement(self):
        pages = [self._page_entry(avg_session_duration=20)]
        a = _make_analyzer()
        result = a._revenue_at_risk(pages)
        self.assertIn("low_engagement", result[0]["risk_factors"])

    def test_deep_position(self):
        pages = [self._page_entry(avg_position=18, ctr=0.001)]
        a = _make_analyzer()
        result = a._revenue_at_risk(pages)
        factors = result[0]["risk_factors"]
        self.assertIn("deep_position", factors)

    def test_declining_position(self):
        pages = [self._page_entry(avg_position=12, ctr=0.001)]
        a = _make_analyzer()
        result = a._revenue_at_risk(pages)
        self.assertIn("declining_position", result[0]["risk_factors"])

    def test_ctr_below_benchmark(self):
        # Position 5 expected CTR ~0.095, set actual to half
        pages = [self._page_entry(avg_position=5, ctr=0.03)]
        a = _make_analyzer()
        result = a._revenue_at_risk(pages)
        factors = result[0]["risk_factors"]
        self.assertTrue(
            "ctr_significantly_below_benchmark" in factors or
            "ctr_below_benchmark" in factors,
        )

    def test_query_concentration(self):
        pages = [self._page_entry(query_count=1, clicks=100)]
        a = _make_analyzer()
        result = a._revenue_at_risk(pages)
        self.assertIn("query_concentration", result[0]["risk_factors"])

    def test_severity_critical(self):
        pages = [self._page_entry(bounce_rate=0.9, avg_session_duration=5,
                                  avg_position=16, ctr=0.001)]
        a = _make_analyzer()
        result = a._revenue_at_risk(pages)
        self.assertEqual(result[0]["severity"], "critical")

    def test_zero_revenue_excluded(self):
        pages = [self._page_entry(revenue=0, bounce_rate=0.9)]
        a = _make_analyzer()
        result = a._revenue_at_risk(pages)
        self.assertEqual(len(result), 0)

    def test_capped_at_30(self):
        pages = [
            self._page_entry(page=f"/p{i}", revenue=100, bounce_rate=0.9)
            for i in range(50)
        ]
        a = _make_analyzer()
        result = a._revenue_at_risk(pages)
        self.assertLessEqual(len(result), 30)

    def test_sorted_by_revenue_times_risk(self):
        pages = [
            self._page_entry(page="/high_rev", revenue=10000, bounce_rate=0.85),
            self._page_entry(page="/low_rev", revenue=10, bounce_rate=0.85),
        ]
        a = _make_analyzer()
        result = a._revenue_at_risk(pages)
        self.assertEqual(result[0]["page"], "/high_rev")

    def test_output_keys(self):
        pages = [self._page_entry(bounce_rate=0.9)]
        a = _make_analyzer()
        result = a._revenue_at_risk(pages)
        expected_keys = {"page", "revenue", "revenue_per_click", "risk_score",
                         "severity", "risk_factors", "clicks", "avg_position",
                         "bounce_rate", "top_queries"}
        self.assertTrue(expected_keys.issubset(set(result[0].keys())))

    def test_no_risk_factors_excluded(self):
        # Good metrics: low bounce, high engagement, good position, good CTR
        pages = [self._page_entry(bounce_rate=0.3, avg_session_duration=120,
                                  avg_position=2, ctr=0.3, query_count=20)]
        a = _make_analyzer()
        result = a._revenue_at_risk(pages)
        self.assertEqual(len(result), 0)


# ===================================================================
# 11. _position_improvement_roi
# ===================================================================
class TestPositionImprovementRoi(unittest.TestCase):
    def _query_entry(self, query="test", avg_position=8, clicks=50,
                     impressions=500, revenue_per_click=5.0,
                     attributed_revenue=250):
        return {
            "query": query, "avg_position": avg_position, "clicks": clicks,
            "impressions": impressions, "ctr": round(clicks / max(impressions, 1), 4),
            "page_count": 1, "attributed_revenue": attributed_revenue,
            "revenue_per_click": revenue_per_click,
        }

    def test_generates_scenarios(self):
        queries = [self._query_entry(avg_position=8)]
        a = _make_analyzer()
        result = a._position_improvement_roi(queries)
        self.assertTrue(len(result) > 0)
        self.assertTrue(len(result[0]["scenarios"]) > 0)

    def test_position_under_4_excluded(self):
        queries = [self._query_entry(avg_position=2)]
        a = _make_analyzer()
        result = a._position_improvement_roi(queries)
        self.assertEqual(len(result), 0)

    def test_position_over_30_excluded(self):
        queries = [self._query_entry(avg_position=35)]
        a = _make_analyzer()
        result = a._position_improvement_roi(queries)
        self.assertEqual(len(result), 0)

    def test_low_impressions_excluded(self):
        queries = [self._query_entry(impressions=30)]
        a = _make_analyzer()
        result = a._position_improvement_roi(queries)
        self.assertEqual(len(result), 0)

    def test_scenarios_have_targets(self):
        queries = [self._query_entry(avg_position=10)]
        a = _make_analyzer()
        result = a._position_improvement_roi(queries)
        targets = {s["target"] for s in result[0]["scenarios"]}
        self.assertTrue(targets.issubset({"position_1", "top_3", "top_5"}))

    def test_best_scenario_selected(self):
        queries = [self._query_entry(avg_position=10)]
        a = _make_analyzer()
        result = a._position_improvement_roi(queries)
        # Position 1 should yield most additional revenue
        self.assertEqual(result[0]["best_scenario_target"], "position_1")

    def test_capped_at_40(self):
        queries = [
            self._query_entry(query=f"q{i}", avg_position=10, impressions=1000)
            for i in range(50)
        ]
        a = _make_analyzer()
        result = a._position_improvement_roi(queries)
        self.assertLessEqual(len(result), 40)

    def test_sorted_by_priority_score(self):
        queries = [
            self._query_entry(query="low", avg_position=10, impressions=100, revenue_per_click=1),
            self._query_entry(query="high", avg_position=10, impressions=10000, revenue_per_click=50),
        ]
        a = _make_analyzer()
        result = a._position_improvement_roi(queries)
        self.assertEqual(result[0]["query"], "high")

    def test_output_keys(self):
        queries = [self._query_entry(avg_position=10)]
        a = _make_analyzer()
        result = a._position_improvement_roi(queries)
        expected = {"query", "current_position", "clicks", "impressions",
                    "current_revenue", "revenue_per_click", "scenarios",
                    "best_scenario_revenue", "best_scenario_target", "priority_score"}
        self.assertEqual(set(result[0].keys()), expected)

    def test_empty_input(self):
        a = _make_analyzer()
        result = a._position_improvement_roi([])
        self.assertEqual(result, [])

    def test_position_exactly_4(self):
        queries = [self._query_entry(avg_position=4, impressions=500)]
        a = _make_analyzer()
        result = a._position_improvement_roi(queries)
        # Position 4 is eligible (>= 4)
        self.assertTrue(len(result) > 0)


# ===================================================================
# 12. _conversion_funnel_analysis
# ===================================================================
class TestConversionFunnelAnalysis(unittest.TestCase):
    def _pages(self):
        return [
            {"page": "/high", "clicks": 200, "revenue": 5000, "conversions": 50,
             "conversion_rate": 0.05, "sessions": 400, "bounce_rate": 0.3,
             "avg_session_duration": 120},
            {"page": "/mid", "clicks": 100, "revenue": 500, "conversions": 5,
             "conversion_rate": 0.02, "sessions": 200, "bounce_rate": 0.5,
             "avg_session_duration": 60},
            {"page": "/traffic", "clicks": 50, "revenue": 0, "conversions": 0,
             "conversion_rate": 0, "sessions": 80, "bounce_rate": 0.7,
             "avg_session_duration": 30},
            {"page": "/low", "clicks": 5, "revenue": 0, "conversions": 0,
             "conversion_rate": 0, "sessions": 8, "bounce_rate": 0.8,
             "avg_session_duration": 10},
        ]

    def test_total_clicks(self):
        a = _make_analyzer()
        result = a._conversion_funnel_analysis(self._pages())
        self.assertEqual(result["total_clicks"], 355)

    def test_total_revenue(self):
        a = _make_analyzer()
        result = a._conversion_funnel_analysis(self._pages())
        self.assertEqual(result["total_revenue"], 5500)

    def test_tiers_high_value(self):
        a = _make_analyzer()
        result = a._conversion_funnel_analysis(self._pages())
        self.assertEqual(result["tiers"]["high_value"]["count"], 1)

    def test_tiers_mid_value(self):
        a = _make_analyzer()
        result = a._conversion_funnel_analysis(self._pages())
        self.assertEqual(result["tiers"]["mid_value"]["count"], 1)

    def test_tiers_traffic_only(self):
        a = _make_analyzer()
        result = a._conversion_funnel_analysis(self._pages())
        # clicks > 20 and revenue == 0
        self.assertEqual(result["tiers"]["traffic_only"]["count"], 1)

    def test_tiers_low_traffic(self):
        a = _make_analyzer()
        result = a._conversion_funnel_analysis(self._pages())
        self.assertEqual(result["tiers"]["low_traffic"]["count"], 1)

    def test_funnel_leak_high_traffic_no_conversions(self):
        pages = [
            {"page": "/leak", "clicks": 100, "revenue": 0, "conversions": 0,
             "conversion_rate": 0, "sessions": 100, "bounce_rate": 0.8,
             "avg_session_duration": 10},
        ]
        a = _make_analyzer()
        result = a._conversion_funnel_analysis(pages)
        self.assertTrue(len(result["funnel_leaks"]) > 0)
        self.assertEqual(result["funnel_leaks"][0]["issue"], "high_traffic_no_conversions")

    def test_funnel_leak_high_bounce_despite_conversions(self):
        pages = [
            {"page": "/bouncy", "clicks": 50, "revenue": 100, "conversions": 5,
             "conversion_rate": 0.05, "sessions": 100, "bounce_rate": 0.85,
             "avg_session_duration": 15},
        ]
        a = _make_analyzer()
        result = a._conversion_funnel_analysis(pages)
        self.assertTrue(len(result["funnel_leaks"]) > 0)
        self.assertEqual(result["funnel_leaks"][0]["issue"], "high_bounce_despite_conversions")

    def test_leaks_capped_at_20(self):
        pages = [
            {"page": f"/leak{i}", "clicks": 100, "revenue": 0, "conversions": 0,
             "conversion_rate": 0, "sessions": 100, "bounce_rate": 0.9,
             "avg_session_duration": 5}
            for i in range(30)
        ]
        a = _make_analyzer()
        result = a._conversion_funnel_analysis(pages)
        self.assertLessEqual(len(result["funnel_leaks"]), 20)

    def test_overall_conversion_rate(self):
        a = _make_analyzer()
        result = a._conversion_funnel_analysis(self._pages())
        # 55 conversions / 688 sessions
        expected = 55 / 688
        self.assertAlmostEqual(result["overall_conversion_rate"], round(expected, 4), places=4)

    def test_output_keys(self):
        a = _make_analyzer()
        result = a._conversion_funnel_analysis(self._pages())
        expected = {"total_clicks", "total_revenue", "total_conversions",
                    "overall_conversion_rate", "avg_revenue_per_click",
                    "tiers", "funnel_leaks"}
        self.assertEqual(set(result.keys()), expected)


# ===================================================================
# 13. _revenue_concentration
# ===================================================================
class TestRevenueConcentration(unittest.TestCase):
    def test_basic_concentration(self):
        pages = [
            {"page": "/big", "revenue": 8000, "clicks": 100},
            {"page": "/small1", "revenue": 1000, "clicks": 50},
            {"page": "/small2", "revenue": 1000, "clicks": 50},
        ]
        queries = [
            {"query": "q1", "attributed_revenue": 5000},
            {"query": "q2", "attributed_revenue": 3000},
            {"query": "q3", "attributed_revenue": 2000},
        ]
        a = _make_analyzer()
        result = a._revenue_concentration(pages, queries)
        self.assertIn("pages_for_80_pct_revenue", result)
        self.assertIn("page_concentration_risk", result)

    def test_critical_concentration(self):
        pages = [{"page": f"/p{i}", "revenue": 10000 if i == 0 else 10, "clicks": 50}
                 for i in range(20)]
        queries = [{"query": "q", "attributed_revenue": 100}]
        a = _make_analyzer()
        result = a._revenue_concentration(pages, queries)
        # 1 page out of 20 = 5% -> critical
        self.assertEqual(result["page_concentration_risk"], "critical")

    def test_low_concentration(self):
        pages = [{"page": f"/p{i}", "revenue": 100, "clicks": 50} for i in range(10)]
        queries = [{"query": f"q{i}", "attributed_revenue": 100} for i in range(10)]
        a = _make_analyzer()
        result = a._revenue_concentration(pages, queries)
        # 80% revenue needs 8 of 10 pages = 0.8 -> low
        self.assertEqual(result["page_concentration_risk"], "low")

    def test_top_5_pages_share(self):
        pages = [{"page": f"/p{i}", "revenue": 100, "clicks": 10} for i in range(10)]
        queries = [{"query": "q", "attributed_revenue": 100}]
        a = _make_analyzer()
        result = a._revenue_concentration(pages, queries)
        self.assertAlmostEqual(result["top_5_pages_revenue_share"], 0.5, places=2)

    def test_output_keys(self):
        pages = [{"page": "/p", "revenue": 100, "clicks": 10}]
        queries = [{"query": "q", "attributed_revenue": 100}]
        a = _make_analyzer()
        result = a._revenue_concentration(pages, queries)
        expected = {"pages_for_80_pct_revenue", "total_revenue_pages",
                    "page_concentration_ratio", "page_concentration_risk",
                    "queries_for_80_pct_revenue", "total_revenue_queries",
                    "query_concentration_ratio", "top_5_pages_revenue_share",
                    "top_10_queries_revenue_share"}
        self.assertEqual(set(result.keys()), expected)

    def test_all_zero_revenue(self):
        pages = [{"page": "/p", "revenue": 0, "clicks": 10}]
        queries = [{"query": "q", "attributed_revenue": 0}]
        a = _make_analyzer()
        # Should not crash
        result = a._revenue_concentration(pages, queries)
        self.assertIsInstance(result, dict)


# ===================================================================
# 14. _generate_recommendations
# ===================================================================
class TestGenerateRecommendations(unittest.TestCase):
    def test_critical_risk_recommendation(self):
        at_risk = [{"severity": "critical", "revenue": 5000, "page": "/p"}]
        a = _make_analyzer()
        recs = a._generate_recommendations([], [], at_risk, [], {}, {})
        self.assertTrue(any(r["category"] == "protect_revenue" for r in recs))

    def test_position_improvement_recommendation(self):
        roi = [{"best_scenario_revenue": 1000, "query": "q"}]
        a = _make_analyzer()
        recs = a._generate_recommendations([], [], [], roi, {}, {})
        self.assertTrue(any(r["category"] == "position_improvement" for r in recs))

    def test_funnel_leak_recommendation(self):
        funnel = {"funnel_leaks": [{"potential_revenue": 500, "page": "/p"}],
                  "tiers": {"traffic_only": {"count": 0, "clicks": 0}}}
        a = _make_analyzer()
        recs = a._generate_recommendations([], [], [], [], funnel, {})
        self.assertTrue(any(r["category"] == "funnel_optimization" for r in recs))

    def test_diversification_recommendation(self):
        concentration = {"page_concentration_risk": "critical",
                         "pages_for_80_pct_revenue": 2,
                         "total_revenue_pages": 50}
        a = _make_analyzer()
        recs = a._generate_recommendations([], [], [], [], {}, concentration)
        self.assertTrue(any(r["category"] == "diversification" for r in recs))

    def test_traffic_only_recommendation(self):
        funnel = {"funnel_leaks": [],
                  "tiers": {"traffic_only": {"count": 10, "clicks": 500}}}
        a = _make_analyzer()
        recs = a._generate_recommendations([], [], [], [], funnel, {})
        self.assertTrue(any(r["category"] == "conversion_expansion" for r in recs))

    def test_high_rpc_expansion_recommendation(self):
        query_revenue = [
            {"revenue_per_click": 5.0, "clicks": 20, "query": "high_rpc"},
        ]
        a = _make_analyzer()
        recs = a._generate_recommendations([], query_revenue, [], [], {}, {})
        self.assertTrue(any(r["category"] == "high_value_expansion" for r in recs))

    def test_sorted_by_priority(self):
        at_risk = [{"severity": "critical", "revenue": 5000, "page": "/p"}]
        roi = [{"best_scenario_revenue": 1000, "query": "q"}]
        funnel = {"funnel_leaks": [{"potential_revenue": 500, "page": "/l"}],
                  "tiers": {"traffic_only": {"count": 0, "clicks": 0}}}
        a = _make_analyzer()
        recs = a._generate_recommendations([], [], at_risk, roi, funnel, {})
        priorities = [r["priority"] for r in recs]
        self.assertEqual(priorities, sorted(priorities))

    def test_empty_all(self):
        a = _make_analyzer()
        recs = a._generate_recommendations([], [], [], [], {}, {})
        self.assertEqual(recs, [])


# ===================================================================
# 15. _build_summary
# ===================================================================
class TestBuildSummary(unittest.TestCase):
    def _sample_data(self):
        page_rev = [{"revenue": 1000, "clicks": 100}]
        query_rev = [{"attributed_revenue": 500}]
        at_risk = [{"revenue": 200}]
        roi = [{"best_scenario_revenue": 300}]
        funnel = {"total_revenue": 1000, "total_clicks": 500,
                  "total_conversions": 25, "overall_conversion_rate": 0.05,
                  "avg_revenue_per_click": 2.0}
        concentration = {"page_concentration_risk": "moderate",
                         "pages_for_80_pct_revenue": 5}
        recs = [{"priority": 1}]
        return page_rev, query_rev, at_risk, roi, funnel, concentration, recs

    def test_returns_string(self):
        a = _make_analyzer()
        result = a._build_summary(*self._sample_data())
        self.assertIsInstance(result, str)

    def test_mentions_revenue(self):
        a = _make_analyzer()
        result = a._build_summary(*self._sample_data())
        self.assertIn("$1,000", result)

    def test_mentions_clicks(self):
        a = _make_analyzer()
        result = a._build_summary(*self._sample_data())
        self.assertIn("500", result)

    def test_mentions_conversions(self):
        a = _make_analyzer()
        result = a._build_summary(*self._sample_data())
        self.assertIn("25", result)

    def test_mentions_at_risk(self):
        a = _make_analyzer()
        result = a._build_summary(*self._sample_data())
        self.assertIn("risk", result.lower())

    def test_mentions_recommendations_count(self):
        a = _make_analyzer()
        result = a._build_summary(*self._sample_data())
        self.assertIn("1 strategic recommendation", result)

    def test_critical_concentration_mentioned(self):
        page_rev, query_rev, at_risk, roi, funnel, _, recs = self._sample_data()
        concentration = {"page_concentration_risk": "critical",
                         "pages_for_80_pct_revenue": 2}
        a = _make_analyzer()
        result = a._build_summary(page_rev, query_rev, at_risk, roi, funnel, concentration, recs)
        self.assertIn("critical", result.lower())


# ===================================================================
# 16. Full pipeline analyze()
# ===================================================================
class TestAnalyze(unittest.TestCase):
    def test_no_data(self):
        a = _make_analyzer(gsc=[])
        result = a.analyze()
        self.assertIn("Insufficient", result["summary"])
        self.assertEqual(result["revenue_by_page"], [])
        self.assertEqual(result["recommendations"], [])

    def test_output_schema(self):
        gsc = [_gsc_row(query="test", page="https://example.com/p")]
        a = _make_analyzer(gsc=gsc)
        result = a.analyze()
        expected_keys = {"summary", "revenue_by_page", "top_converting_queries",
                         "revenue_at_risk", "position_improvement_roi",
                         "conversion_funnel", "revenue_concentration",
                         "recommendations", "data_quality"}
        self.assertEqual(set(result.keys()), expected_keys)

    def test_data_quality_fields(self):
        gsc = [_gsc_row()]
        a = _make_analyzer(gsc=gsc)
        result = a.analyze()
        dq = result["data_quality"]
        self.assertIn("has_ecommerce_data", dq)
        self.assertIn("has_conversion_data", dq)
        self.assertIn("gsc_rows_analyzed", dq)
        self.assertEqual(dq["gsc_rows_analyzed"], 1)

    def test_full_data(self):
        gsc = [
            _gsc_row(query="buy shoes", page="https://example.com/shop",
                     clicks=100, impressions=1000, position=5),
            _gsc_row(query="shoe reviews", page="https://example.com/blog",
                     clicks=50, impressions=800, position=8),
        ]
        conv = [_conv_row(page="/shop", conversions=20)]
        eng = [
            _eng_row(page="/shop", sessions=200, bounce_rate=0.3),
            _eng_row(page="/blog", sessions=100, bounce_rate=0.6),
        ]
        ecom = [_ecom_row(page="/shop", revenue=5000, transactions=20, avg_order_value=250)]
        a = _make_analyzer(gsc=gsc, conv=conv, eng=eng, ecom=ecom)
        result = a.analyze()
        self.assertTrue(len(result["revenue_by_page"]) > 0)
        self.assertTrue(len(result["top_converting_queries"]) > 0)
        self.assertIsInstance(result["summary"], str)

    def test_revenue_by_page_capped_at_50(self):
        gsc = [_gsc_row(query=f"q{i}", page=f"https://example.com/p{i}")
               for i in range(60)]
        a = _make_analyzer(gsc=gsc)
        result = a.analyze()
        self.assertLessEqual(len(result["revenue_by_page"]), 50)

    def test_has_ecommerce_flag(self):
        gsc = [_gsc_row()]
        ecom = [_ecom_row()]
        a = _make_analyzer(gsc=gsc, ecom=ecom)
        result = a.analyze()
        self.assertTrue(result["data_quality"]["has_ecommerce_data"])

    def test_no_ecommerce_flag(self):
        gsc = [_gsc_row()]
        a = _make_analyzer(gsc=gsc)
        result = a.analyze()
        self.assertFalse(result["data_quality"]["has_ecommerce_data"])


# ===================================================================
# 17. Public API estimate_revenue_attribution
# ===================================================================
class TestEstimateRevenueAttribution(unittest.TestCase):
    def test_basic(self):
        gsc = [_gsc_row()]
        result = estimate_revenue_attribution(gsc)
        self.assertIn("summary", result)
        self.assertIn("revenue_by_page", result)

    def test_no_data(self):
        result = estimate_revenue_attribution(None)
        self.assertIn("Insufficient", result["summary"])

    def test_with_all_data(self):
        gsc = [_gsc_row(page="https://example.com/p")]
        conv = [_conv_row(page="/p")]
        eng = [_eng_row(page="/p")]
        ecom = [_ecom_row(page="/p")]
        result = estimate_revenue_attribution(gsc, conv, eng, ecom)
        self.assertTrue(result["data_quality"]["has_ecommerce_data"])
        self.assertTrue(result["data_quality"]["has_conversion_data"])

    def test_empty_list(self):
        result = estimate_revenue_attribution([])
        self.assertIn("Insufficient", result["summary"])


# ===================================================================
# 18. Edge cases
# ===================================================================
class TestEdgeCases(unittest.TestCase):
    def test_unicode_query(self):
        gsc = [_gsc_row(query="café résumé", page="https://example.com/p")]
        result = estimate_revenue_attribution(gsc)
        self.assertIn("summary", result)

    def test_unicode_url(self):
        gsc = [_gsc_row(page="https://example.com/über/café")]
        result = estimate_revenue_attribution(gsc)
        self.assertTrue(len(result["revenue_by_page"]) > 0)

    def test_special_chars_in_query(self):
        gsc = [_gsc_row(query="what is a <div> tag?")]
        result = estimate_revenue_attribution(gsc)
        self.assertIn("summary", result)

    def test_very_large_dataset(self):
        gsc = [_gsc_row(query=f"q{i}", page=f"https://example.com/p{i % 20}",
                        clicks=i + 1, impressions=(i + 1) * 10, position=i % 20 + 1)
               for i in range(500)]
        ecom = [_ecom_row(page=f"/p{i}", revenue=i * 100) for i in range(20)]
        result = estimate_revenue_attribution(gsc, ga4_ecommerce=ecom)
        self.assertLessEqual(len(result["revenue_by_page"]), 50)
        self.assertLessEqual(len(result["top_converting_queries"]), 50)

    def test_zero_clicks(self):
        gsc = [_gsc_row(clicks=0, impressions=100)]
        result = estimate_revenue_attribution(gsc)
        self.assertIn("summary", result)

    def test_zero_impressions(self):
        gsc = [_gsc_row(clicks=0, impressions=0)]
        result = estimate_revenue_attribution(gsc)
        self.assertIn("summary", result)

    def test_negative_values_handled(self):
        gsc = [_gsc_row(clicks=-5, impressions=100)]
        result = estimate_revenue_attribution(gsc)
        self.assertIn("summary", result)

    def test_url_key_variant(self):
        gsc = [{"query": "test", "url": "https://example.com/via-url",
                "clicks": 10, "impressions": 100, "position": 5, "ctr": 0.1}]
        result = estimate_revenue_attribution(gsc)
        self.assertTrue(len(result["revenue_by_page"]) > 0)

    def test_mixed_page_key_formats_in_ga4(self):
        gsc = [_gsc_row(page="https://example.com/p")]
        conv = [{"page_path": "/p", "conversions": 5, "conversion_rate": 0.05}]
        result = estimate_revenue_attribution(gsc, ga4_conversions=conv)
        page = result["revenue_by_page"][0]
        self.assertEqual(page["conversions"], 5)

    def test_duplicate_pages_in_gsc(self):
        gsc = [
            _gsc_row(query="a", page="https://example.com/p", clicks=10),
            _gsc_row(query="b", page="https://EXAMPLE.com/P", clicks=5),
        ]
        result = estimate_revenue_attribution(gsc)
        # Both should normalise to /p
        self.assertEqual(len(result["revenue_by_page"]), 1)
        self.assertEqual(result["revenue_by_page"][0]["clicks"], 15)

    def test_position_improvement_with_revenue(self):
        gsc = [_gsc_row(query="mid rank", page="https://example.com/p",
                        clicks=30, impressions=500, position=10)]
        ecom = [_ecom_row(page="/p", revenue=300)]
        result = estimate_revenue_attribution(gsc, ga4_ecommerce=ecom)
        # Query at position 10 with revenue should generate ROI opportunities
        self.assertTrue(len(result["position_improvement_roi"]) > 0)


if __name__ == "__main__":
    unittest.main()
