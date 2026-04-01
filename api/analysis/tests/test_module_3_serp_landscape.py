"""
Comprehensive test suite for api/analysis/module_3_serp_landscape.py
(Module 3 — SERP Landscape Analysis).

Tests cover:
  1. Input validation (empty/None input)
  2. Output schema (all required keys present)
  3. Helper functions (_extract_domain, _classify_keyword_intent, etc.)
  4. SERP feature displacement analysis
  5. Competitor mapping & threat scoring
  6. Intent classification & mismatch detection
  7. Click-share estimation
  8. SERP feature summary aggregation
  9. Full pipeline integration
  10. Edge cases
"""

import unittest
from unittest.mock import patch
from collections import Counter

from api.analysis.module_3_serp_landscape import (
    analyze_serp_landscape,
    _extract_domain,
    _is_user_domain,
    _find_user_result,
    _features_above_position,
    _visual_position,
    _classify_keyword_intent,
    _infer_page_type,
    _is_intent_mismatch,
    _mismatch_recommendation,
    _serp_feature_summary,
    _analyze_displacement,
    _analyze_competitors,
    _analyze_intents,
    _estimate_click_share,
    _empty_result,
    SERP_FEATURE_WEIGHTS,
    GENERIC_CTR_BY_POSITION,
)


# ---------------------------------------------------------------------------
# Test data builders
# ---------------------------------------------------------------------------

def _make_serp(keyword="test keyword", user_domain="example.com",
               organic_results=None, featured_snippet=None,
               knowledge_panel=None, ai_overview=None,
               local_pack=None, people_also_ask=None,
               video_results=None, images_pack=None,
               shopping_results=None):
    """Build a minimal SERP dict for testing."""
    if organic_results is None:
        organic_results = [
            {"url": f"https://www.{user_domain}/page", "position": 3},
            {"url": "https://competitor-a.com/page", "position": 1},
            {"url": "https://competitor-b.com/page", "position": 2},
        ]
    return {
        "keyword": keyword,
        "user_domain": user_domain,
        "organic_results": organic_results,
        "featured_snippet": featured_snippet,
        "knowledge_panel": knowledge_panel,
        "ai_overview": ai_overview,
        "local_pack": local_pack,
        "people_also_ask": people_also_ask or [],
        "video_results": video_results,
        "images_pack": images_pack,
        "shopping_results": shopping_results,
    }


def _make_gsc_df(rows):
    """Build a pandas DataFrame mimicking GSC keyword data."""
    import pandas as pd
    return pd.DataFrame(rows)


# ===================================================================
# 1. Input validation
# ===================================================================

class TestInputValidation(unittest.TestCase):
    """analyze_serp_landscape must handle empty / None input gracefully."""

    def test_none_input(self):
        result = analyze_serp_landscape(None)
        self.assertEqual(result["keywords_analyzed"], 0)

    def test_empty_list(self):
        result = analyze_serp_landscape([])
        self.assertEqual(result["keywords_analyzed"], 0)

    def test_empty_returns_full_schema(self):
        result = analyze_serp_landscape([])
        expected_keys = {
            "keywords_analyzed", "serp_feature_displacement",
            "serp_feature_summary", "competitors", "intent_analysis",
            "click_share", "summary",
        }
        self.assertEqual(set(result.keys()), expected_keys)


# ===================================================================
# 2. Output schema
# ===================================================================

class TestOutputSchema(unittest.TestCase):
    """Full run must produce all required top-level and nested keys."""

    @classmethod
    def setUpClass(cls):
        cls.serp_data = [_make_serp()]
        cls.result = analyze_serp_landscape(cls.serp_data)

    def test_top_level_keys(self):
        expected = {
            "keywords_analyzed", "serp_feature_displacement",
            "serp_feature_summary", "competitors", "intent_analysis",
            "click_share", "summary",
        }
        self.assertEqual(set(self.result.keys()), expected)

    def test_summary_keys(self):
        summary = self.result["summary"]
        expected = {
            "keywords_analyzed", "keywords_with_significant_displacement",
            "avg_visual_displacement", "primary_competitors_count",
            "total_click_share", "click_opportunity_size",
            "dominant_intent", "intent_mismatches_found",
        }
        self.assertEqual(set(summary.keys()), expected)

    def test_intent_analysis_keys(self):
        ia = self.result["intent_analysis"]
        self.assertIn("intent_distribution", ia)
        self.assertIn("intent_mismatches", ia)

    def test_click_share_keys(self):
        cs = self.result["click_share"]
        expected = {
            "total_click_share", "current_monthly_clicks",
            "potential_monthly_clicks", "click_opportunity",
            "keyword_breakdown",
        }
        self.assertEqual(set(cs.keys()), expected)

    def test_keywords_analyzed_count(self):
        self.assertEqual(self.result["keywords_analyzed"], 1)


# ===================================================================
# 3. Helper: _extract_domain
# ===================================================================

class TestExtractDomain(unittest.TestCase):

    def test_simple_url(self):
        self.assertEqual(_extract_domain("https://example.com/path"), "example.com")

    def test_www_prefix_stripped(self):
        self.assertEqual(_extract_domain("https://www.example.com/path"), "example.com")

    def test_subdomain_preserved(self):
        self.assertEqual(_extract_domain("https://blog.example.com"), "blog.example.com")

    def test_invalid_url(self):
        self.assertEqual(_extract_domain("not a url"), "")

    def test_empty_string(self):
        self.assertEqual(_extract_domain(""), "")

    def test_http_scheme(self):
        self.assertEqual(_extract_domain("http://example.com"), "example.com")


# ===================================================================
# 4. Helper: _is_user_domain / _find_user_result
# ===================================================================

class TestUserDomainHelpers(unittest.TestCase):

    def test_is_user_domain_match(self):
        serp = {"user_domain": "example.com"}
        self.assertTrue(_is_user_domain("example.com", serp))

    def test_is_user_domain_www_prefix(self):
        serp = {"user_domain": "www.example.com"}
        self.assertTrue(_is_user_domain("example.com", serp))

    def test_is_user_domain_no_match(self):
        serp = {"user_domain": "other.com"}
        self.assertFalse(_is_user_domain("example.com", serp))

    def test_find_user_result_found(self):
        serp = _make_serp(user_domain="example.com")
        result = _find_user_result(serp)
        self.assertIsNotNone(result)
        self.assertEqual(result["position"], 3)

    def test_find_user_result_not_found(self):
        serp = _make_serp(user_domain="notfound.com")
        result = _find_user_result(serp)
        self.assertIsNone(result)


# ===================================================================
# 5. SERP feature displacement
# ===================================================================

class TestDisplacement(unittest.TestCase):

    def test_no_features_no_displacement(self):
        serp = _make_serp()
        results = _analyze_displacement([serp])
        # Position 3 with no SERP features => visual_pos ≈ 3 => displacement < 2 threshold
        self.assertEqual(len(results), 0)

    def test_heavy_features_causes_displacement(self):
        serp = _make_serp(
            featured_snippet={"position": 1, "text": "answer"},
            ai_overview={"position": 0},
            local_pack={"position": 2},
        )
        results = _analyze_displacement([serp])
        # User at pos 3 with featured_snippet(2.0)+ai_overview(2.5)+local_pack(3.0)=7.5 displacement
        self.assertGreater(len(results), 0)
        self.assertGreater(results[0]["displacement"], 2)

    def test_displacement_sorted_descending(self):
        serps = [
            _make_serp(keyword="low", featured_snippet={"position": 1, "text": "x"}),
            _make_serp(
                keyword="high",
                featured_snippet={"position": 1, "text": "x"},
                ai_overview={"position": 0},
                local_pack={"position": 2},
            ),
        ]
        results = _analyze_displacement(serps)
        if len(results) >= 2:
            self.assertGreaterEqual(results[0]["displacement"], results[1]["displacement"])

    def test_displacement_capped_at_50(self):
        serps = [
            _make_serp(
                keyword=f"kw_{i}",
                featured_snippet={"position": 1, "text": "x"},
                ai_overview={"position": 0},
                local_pack={"position": 2},
            )
            for i in range(60)
        ]
        results = _analyze_displacement(serps)
        self.assertLessEqual(len(results), 50)


# ===================================================================
# 6. Helper: _visual_position and _features_above_position
# ===================================================================

class TestVisualPosition(unittest.TestCase):

    def test_no_features_equals_organic(self):
        self.assertEqual(_visual_position(3, []), 3)

    def test_single_feature(self):
        vp = _visual_position(3, ["featured_snippet"])
        self.assertEqual(vp, 3 + SERP_FEATURE_WEIGHTS["featured_snippet"])

    def test_multiple_features(self):
        features = ["featured_snippet", "ai_overview"]
        vp = _visual_position(1, features)
        expected = 1 + SERP_FEATURE_WEIGHTS["featured_snippet"] + SERP_FEATURE_WEIGHTS["ai_overview"]
        self.assertEqual(vp, expected)

    def test_unknown_feature_uses_default_weight(self):
        vp = _visual_position(1, ["unknown_feature"])
        self.assertEqual(vp, 1.5)  # default weight 0.5

    def test_features_above_position_empty_serp(self):
        serp = _make_serp()
        features = _features_above_position(serp, 3)
        self.assertEqual(features, [])

    def test_features_above_position_with_snippet(self):
        serp = _make_serp(featured_snippet={"position": 1, "text": "x"})
        features = _features_above_position(serp, 3)
        self.assertIn("featured_snippet", features)

    def test_features_above_position_snippet_below(self):
        serp = _make_serp(featured_snippet={"position": 5, "text": "x"})
        features = _features_above_position(serp, 3)
        self.assertNotIn("featured_snippet", features)


# ===================================================================
# 7. Intent classification
# ===================================================================

class TestIntentClassification(unittest.TestCase):

    def test_navigational_login(self):
        self.assertEqual(_classify_keyword_intent("gmail login", {}), "navigational")

    def test_navigational_sign_in(self):
        self.assertEqual(_classify_keyword_intent("facebook sign in", {}), "navigational")

    def test_transactional_buy(self):
        self.assertEqual(_classify_keyword_intent("buy running shoes", {}), "transactional")

    def test_transactional_price(self):
        self.assertEqual(_classify_keyword_intent("iphone 15 price", {}), "transactional")

    def test_commercial_best(self):
        self.assertEqual(_classify_keyword_intent("best laptop 2024", {}), "commercial")

    def test_commercial_vs(self):
        self.assertEqual(_classify_keyword_intent("mac vs windows", {}), "commercial")

    def test_informational_how(self):
        self.assertEqual(_classify_keyword_intent("how to tie a tie", {}), "informational")

    def test_informational_what(self):
        self.assertEqual(_classify_keyword_intent("what is photosynthesis", {}), "informational")

    def test_transactional_via_shopping_serp(self):
        serp = {"shopping_results": [{"price": "$10"}]}
        self.assertEqual(_classify_keyword_intent("wireless earbuds", serp), "transactional")

    def test_informational_via_knowledge_panel(self):
        serp = {"knowledge_panel": {"title": "X"}, "people_also_ask": []}
        self.assertEqual(_classify_keyword_intent("python programming", serp), "informational")

    def test_default_is_informational(self):
        self.assertEqual(_classify_keyword_intent("random thing", {}), "informational")


# ===================================================================
# 8. Page type inference & intent mismatch
# ===================================================================

class TestPageTypeAndMismatch(unittest.TestCase):

    def test_blog_page(self):
        self.assertEqual(_infer_page_type("https://example.com/blog/post-1"), "blog")

    def test_product_page(self):
        self.assertEqual(_infer_page_type("https://example.com/product/widget"), "product")

    def test_category_page(self):
        self.assertEqual(_infer_page_type("https://example.com/category/shoes"), "category")

    def test_homepage(self):
        self.assertEqual(_infer_page_type("https://example.com/"), "homepage")

    def test_other_page(self):
        self.assertEqual(_infer_page_type("https://example.com/a/b/c/d"), "other")

    def test_mismatch_transactional_blog(self):
        self.assertTrue(_is_intent_mismatch("transactional", "blog"))

    def test_mismatch_commercial_blog(self):
        self.assertTrue(_is_intent_mismatch("commercial", "blog"))

    def test_mismatch_informational_product(self):
        self.assertTrue(_is_intent_mismatch("informational", "product"))

    def test_no_mismatch_informational_blog(self):
        self.assertFalse(_is_intent_mismatch("informational", "blog"))

    def test_no_mismatch_transactional_product(self):
        self.assertFalse(_is_intent_mismatch("transactional", "product"))

    def test_recommendation_transactional_blog(self):
        rec = _mismatch_recommendation("transactional", "blog")
        self.assertIn("product", rec.lower())

    def test_recommendation_informational_product(self):
        rec = _mismatch_recommendation("informational", "product")
        self.assertIn("educational", rec.lower())


# ===================================================================
# 9. Competitor analysis
# ===================================================================

class TestCompetitorAnalysis(unittest.TestCase):

    def test_single_serp_finds_competitors(self):
        serp = _make_serp()
        competitors = _analyze_competitors([serp])
        domains = [c["domain"] for c in competitors]
        self.assertIn("competitor-a.com", domains)
        self.assertIn("competitor-b.com", domains)

    def test_user_domain_excluded(self):
        serp = _make_serp(user_domain="example.com")
        competitors = _analyze_competitors([serp])
        domains = [c["domain"] for c in competitors]
        self.assertNotIn("example.com", domains)

    def test_threat_level_assigned(self):
        serp = _make_serp()
        competitors = _analyze_competitors([serp])
        for c in competitors:
            self.assertIn(c["threat_level"], ("critical", "high", "medium", "low"))

    def test_high_overlap_high_position_is_critical(self):
        # Create 10 SERPs where competitor-a always ranks #1
        serps = []
        for i in range(10):
            serps.append(_make_serp(
                keyword=f"kw_{i}",
                user_domain="example.com",
                organic_results=[
                    {"url": "https://competitor-a.com/p", "position": 1},
                    {"url": "https://example.com/p", "position": 5},
                ],
            ))
        competitors = _analyze_competitors(serps)
        top = next(c for c in competitors if c["domain"] == "competitor-a.com")
        self.assertEqual(top["overlap_percentage"], 100.0)
        self.assertIn(top["threat_level"], ("critical", "high"))

    def test_max_20_competitors(self):
        organic = [{"url": f"https://comp-{i}.com/p", "position": i + 1} for i in range(25)]
        organic.append({"url": "https://example.com/p", "position": 26})
        serps = [_make_serp(organic_results=organic)]
        competitors = _analyze_competitors(serps)
        self.assertLessEqual(len(competitors), 20)

    def test_empty_serp_data(self):
        competitors = _analyze_competitors([])
        self.assertEqual(competitors, [])


# ===================================================================
# 10. Intent analysis (full)
# ===================================================================

class TestIntentAnalysis(unittest.TestCase):

    def test_distribution_sums_to_one(self):
        serps = [
            _make_serp(keyword="buy shoes"),
            _make_serp(keyword="how to tie shoes"),
            _make_serp(keyword="best running shoes"),
        ]
        result = _analyze_intents(serps)
        total = sum(result["intent_distribution"].values())
        self.assertAlmostEqual(total, 1.0, places=2)

    def test_mismatches_detected(self):
        # User ranks a blog page for a transactional keyword
        serp = _make_serp(
            keyword="buy running shoes",
            user_domain="example.com",
            organic_results=[
                {"url": "https://www.example.com/blog/running-shoes-guide", "position": 3},
            ],
        )
        result = _analyze_intents([serp])
        self.assertGreater(len(result["intent_mismatches"]), 0)
        mismatch = result["intent_mismatches"][0]
        self.assertEqual(mismatch["serp_intent"], "transactional")
        self.assertEqual(mismatch["page_type"], "blog")

    def test_no_mismatch_for_aligned_content(self):
        serp = _make_serp(
            keyword="how to run faster",
            user_domain="example.com",
            organic_results=[
                {"url": "https://www.example.com/blog/run-faster", "position": 2},
            ],
        )
        result = _analyze_intents([serp])
        self.assertEqual(len(result["intent_mismatches"]), 0)


# ===================================================================
# 11. Click-share estimation
# ===================================================================

class TestClickShare(unittest.TestCase):

    def test_with_matching_gsc_data(self):
        import pandas as pd
        serps = [_make_serp(keyword="test query")]
        gsc_df = pd.DataFrame([{
            "query": "test query",
            "impressions": 1000,
            "clicks": 50,
            "position": 3,
        }])
        result = _estimate_click_share(serps, gsc_df)
        self.assertGreater(result["total_click_share"], 0)
        self.assertEqual(result["current_monthly_clicks"], 50)
        self.assertGreater(result["potential_monthly_clicks"], 0)

    def test_no_gsc_data(self):
        serps = [_make_serp(keyword="orphan query")]
        result = _estimate_click_share(serps, None)
        self.assertEqual(result["total_click_share"], 0)
        self.assertEqual(result["current_monthly_clicks"], 0)

    def test_empty_gsc_dataframe(self):
        import pandas as pd
        serps = [_make_serp(keyword="query")]
        result = _estimate_click_share(serps, pd.DataFrame())
        self.assertEqual(result["current_monthly_clicks"], 0)

    def test_heavy_features_reduce_potential(self):
        import pandas as pd
        serp = _make_serp(
            keyword="test query",
            featured_snippet={"position": 1, "text": "x"},
            ai_overview={"position": 0},
            local_pack={"position": 2},
        )
        gsc_df = pd.DataFrame([{
            "query": "test query",
            "impressions": 1000,
            "clicks": 50,
            "position": 3,
        }])
        result_heavy = _estimate_click_share([serp], gsc_df)

        serp_clean = _make_serp(keyword="test query")
        result_clean = _estimate_click_share([serp_clean], gsc_df)

        # Heavy features should increase click share (same clicks / lower potential)
        self.assertGreaterEqual(
            result_heavy["total_click_share"],
            result_clean["total_click_share"],
        )

    def test_breakdown_sorted_by_clicks(self):
        import pandas as pd
        serps = [
            _make_serp(keyword="high clicks"),
            _make_serp(keyword="low clicks"),
        ]
        gsc_df = pd.DataFrame([
            {"query": "high clicks", "impressions": 5000, "clicks": 500, "position": 1},
            {"query": "low clicks", "impressions": 1000, "clicks": 10, "position": 8},
        ])
        result = _estimate_click_share(serps, gsc_df)
        breakdown = result["keyword_breakdown"]
        if len(breakdown) >= 2:
            self.assertGreaterEqual(breakdown[0]["clicks"], breakdown[1]["clicks"])


# ===================================================================
# 12. SERP feature summary
# ===================================================================

class TestSerpFeatureSummary(unittest.TestCase):

    def test_counts_features(self):
        serps = [
            _make_serp(featured_snippet={"text": "x"}),
            _make_serp(featured_snippet={"text": "y"}, ai_overview={"text": "z"}),
        ]
        summary = _serp_feature_summary(serps)
        self.assertEqual(summary["feature_prevalence"]["featured_snippet"]["count"], 2)
        self.assertEqual(summary["feature_prevalence"]["ai_overview"]["count"], 1)

    def test_percentages(self):
        serps = [_make_serp(featured_snippet={"text": "x"}) for _ in range(4)]
        summary = _serp_feature_summary(serps)
        self.assertEqual(summary["feature_prevalence"]["featured_snippet"]["pct"], 100.0)

    def test_sample_keywords_capped_at_5(self):
        serps = [
            _make_serp(keyword=f"kw_{i}", knowledge_panel={"title": "x"})
            for i in range(10)
        ]
        summary = _serp_feature_summary(serps)
        self.assertLessEqual(len(summary["feature_sample_keywords"]["knowledge_panel"]), 5)

    def test_empty_serps(self):
        summary = _serp_feature_summary([])
        self.assertEqual(summary["feature_prevalence"], {})

    def test_no_features_serp(self):
        summary = _serp_feature_summary([_make_serp()])
        self.assertEqual(summary["feature_prevalence"], {})


# ===================================================================
# 13. Full pipeline integration
# ===================================================================

class TestFullPipeline(unittest.TestCase):

    def test_single_keyword(self):
        result = analyze_serp_landscape([_make_serp()])
        self.assertEqual(result["keywords_analyzed"], 1)
        self.assertIsInstance(result["competitors"], list)

    def test_multiple_keywords(self):
        serps = [_make_serp(keyword=f"kw_{i}") for i in range(5)]
        result = analyze_serp_landscape(serps)
        self.assertEqual(result["keywords_analyzed"], 5)

    def test_with_gsc_data(self):
        import pandas as pd
        serps = [_make_serp(keyword="my query")]
        gsc_df = pd.DataFrame([{
            "query": "my query",
            "impressions": 2000,
            "clicks": 100,
            "position": 2,
        }])
        result = analyze_serp_landscape(serps, gsc_df)
        self.assertGreater(result["click_share"]["current_monthly_clicks"], 0)

    def test_summary_dominant_intent(self):
        serps = [
            _make_serp(keyword="how to cook pasta"),
            _make_serp(keyword="what is machine learning"),
            _make_serp(keyword="buy headphones"),
        ]
        result = analyze_serp_landscape(serps)
        self.assertIn(result["summary"]["dominant_intent"],
                       ("informational", "commercial", "transactional", "navigational"))

    def test_rich_serp_with_all_features(self):
        serp = _make_serp(
            keyword="best laptops 2024",
            featured_snippet={"position": 0, "text": "Top picks"},
            knowledge_panel={"title": "Laptop"},
            ai_overview={"text": "AI says..."},
            local_pack={"position": 2},
            people_also_ask=[
                {"question": "q1", "position": 4},
                {"question": "q2", "position": 5},
            ],
            video_results=[{"url": "https://youtube.com/v1", "position": 6}],
            images_pack={"position": 7},
            shopping_results=[{"price": "$999", "position": 1}],
        )
        result = analyze_serp_landscape([serp])
        self.assertEqual(result["keywords_analyzed"], 1)
        # Should have feature prevalence entries
        fp = result["serp_feature_summary"]["feature_prevalence"]
        self.assertGreater(len(fp), 0)


# ===================================================================
# 14. Edge cases
# ===================================================================

class TestEdgeCases(unittest.TestCase):

    def test_serp_with_no_organic_results(self):
        serp = _make_serp(organic_results=[])
        result = analyze_serp_landscape([serp])
        self.assertEqual(result["keywords_analyzed"], 1)
        self.assertEqual(len(result["serp_feature_displacement"]), 0)
        self.assertEqual(len(result["competitors"]), 0)

    def test_user_not_ranking(self):
        serp = _make_serp(
            user_domain="mysite.com",
            organic_results=[
                {"url": "https://other.com/a", "position": 1},
                {"url": "https://another.com/b", "position": 2},
            ],
        )
        result = analyze_serp_landscape([serp])
        self.assertEqual(len(result["serp_feature_displacement"]), 0)

    def test_duplicate_keywords(self):
        serps = [_make_serp(keyword="same") for _ in range(3)]
        result = analyze_serp_landscape(serps)
        self.assertEqual(result["keywords_analyzed"], 3)

    def test_malformed_url_in_organic(self):
        serp = _make_serp(organic_results=[
            {"url": "not-a-url", "position": 1},
            {"url": "https://example.com/page", "position": 2},
        ])
        # Should not raise
        result = analyze_serp_landscape([serp])
        self.assertEqual(result["keywords_analyzed"], 1)

    def test_missing_position_in_organic(self):
        serp = _make_serp(organic_results=[
            {"url": "https://example.com/page"},
        ])
        # Should handle gracefully
        result = analyze_serp_landscape([serp])
        self.assertIsNotNone(result)


# ===================================================================
# 15. Constants validation
# ===================================================================

class TestConstants(unittest.TestCase):

    def test_ctr_positions_1_through_10(self):
        for i in range(1, 11):
            self.assertIn(i, GENERIC_CTR_BY_POSITION)

    def test_ctr_decreasing(self):
        for i in range(1, 10):
            self.assertGreater(
                GENERIC_CTR_BY_POSITION[i],
                GENERIC_CTR_BY_POSITION[i + 1],
            )

    def test_feature_weights_positive(self):
        for name, weight in SERP_FEATURE_WEIGHTS.items():
            self.assertGreater(weight, 0, f"{name} weight should be positive")

    def test_empty_result_helper(self):
        empty = _empty_result()
        self.assertEqual(empty["keywords_analyzed"], 0)
        self.assertEqual(empty["serp_feature_displacement"], [])
        self.assertEqual(empty["competitors"], [])


if __name__ == "__main__":
    unittest.main()
