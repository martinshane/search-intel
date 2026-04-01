"""
Comprehensive test suite for Module 11: Competitive Threats —
competitor identification, keyword vulnerability, emerging threat detection,
competitive pressure scoring, and content velocity estimation.
"""

import math
import unittest
from unittest.mock import patch
from collections import defaultdict

# ---------------------------------------------------------------------------
# Import module under test
# ---------------------------------------------------------------------------
import importlib, types, sys, textwrap, pathlib

# We test by importing the module source directly since it's in a remote repo.
# The tests assume the module is importable as api.analysis.module_11_competitive_threats
# For test runner convenience we also support running standalone.

try:
    from api.analysis.module_11_competitive_threats import (
        GENERIC_CTR_BY_POSITION,
        THREAT_CRITICAL_OVERLAP_PCT,
        THREAT_CRITICAL_AVG_POS,
        THREAT_HIGH_OVERLAP_PCT,
        THREAT_HIGH_AVG_POS,
        THREAT_MEDIUM_OVERLAP_PCT,
        VULNERABILITY_POSITION_DIFF,
        VULNERABILITY_HIGH_IMPRESSIONS,
        _extract_domain,
        _normalize_domain,
        _is_user_url,
        _find_user_result,
        _find_competitor_result,
        _keyword_cluster,
        _profile_competitors,
        _assess_keyword_vulnerability,
        _detect_emerging_threats,
        _estimate_content_velocity,
        _analyze_competitive_pressure,
        _generate_recommendations,
        _vulnerability_recommendation,
        _emerging_signal_label,
        _empty_result,
        analyze_competitive_threats,
    )
except ImportError:
    pass  # Will be tested via direct calls


# ===========================================================================
# Helper factories
# ===========================================================================

def _serp(keyword, organic_results=None):
    """Build a minimal SERP dict."""
    return {
        "keyword": keyword,
        "organic_results": organic_results or [],
    }


def _result(url, position, snippet="", title=""):
    """Build a minimal organic result dict."""
    return {"url": url, "position": position, "snippet": snippet, "title": title}


def _make_serp_data(keywords, user_domain="example.com", comp_domains=None, user_pos=5, comp_pos=3):
    """Generate a batch of SERPs with user and competitor results."""
    comp_domains = comp_domains or ["competitor.com"]
    serps = []
    for kw in keywords:
        results = []
        for cd in comp_domains:
            results.append(_result(f"https://{cd}/{kw.replace(' ', '-')}", comp_pos))
        results.append(_result(f"https://{user_domain}/{kw.replace(' ', '-')}", user_pos))
        results.sort(key=lambda r: r["position"])
        serps.append(_serp(kw, results))
    return serps


# ===========================================================================
# 1. Constants
# ===========================================================================

class TestConstants(unittest.TestCase):
    """Verify that module-level constants are sensible."""

    def test_ctr_by_position_has_10_entries(self):
        self.assertEqual(len(GENERIC_CTR_BY_POSITION), 10)

    def test_ctr_position_1_highest(self):
        self.assertEqual(max(GENERIC_CTR_BY_POSITION, key=GENERIC_CTR_BY_POSITION.get), 1)

    def test_ctr_values_positive(self):
        for pos, ctr in GENERIC_CTR_BY_POSITION.items():
            self.assertGreater(ctr, 0)

    def test_ctr_monotonically_decreasing(self):
        for i in range(1, 10):
            self.assertGreater(GENERIC_CTR_BY_POSITION[i], GENERIC_CTR_BY_POSITION[i + 1])

    def test_threat_thresholds_ordered(self):
        self.assertGreater(THREAT_CRITICAL_OVERLAP_PCT, THREAT_HIGH_OVERLAP_PCT)
        self.assertGreater(THREAT_HIGH_OVERLAP_PCT, THREAT_MEDIUM_OVERLAP_PCT)

    def test_threat_avg_pos_thresholds(self):
        self.assertLess(THREAT_CRITICAL_AVG_POS, THREAT_HIGH_AVG_POS)

    def test_vulnerability_thresholds_positive(self):
        self.assertGreater(VULNERABILITY_POSITION_DIFF, 0)
        self.assertGreater(VULNERABILITY_HIGH_IMPRESSIONS, 0)


# ===========================================================================
# 2. _extract_domain
# ===========================================================================

class TestExtractDomain(unittest.TestCase):

    def test_basic_url(self):
        self.assertEqual(_extract_domain("https://example.com/page"), "example.com")

    def test_www_stripped(self):
        self.assertEqual(_extract_domain("https://www.example.com/page"), "example.com")

    def test_subdomain_preserved(self):
        self.assertEqual(_extract_domain("https://blog.example.com/page"), "blog.example.com")

    def test_empty_string(self):
        self.assertEqual(_extract_domain(""), "")

    def test_invalid_url(self):
        result = _extract_domain("not-a-url")
        self.assertIsInstance(result, str)

    def test_with_port(self):
        result = _extract_domain("https://example.com:8080/page")
        self.assertIn("example.com", result)

    def test_uppercase_normalized(self):
        self.assertEqual(_extract_domain("https://EXAMPLE.COM/page"), "example.com")

    def test_http_url(self):
        self.assertEqual(_extract_domain("http://example.com/page"), "example.com")

    def test_trailing_slash(self):
        self.assertEqual(_extract_domain("https://example.com/"), "example.com")


# ===========================================================================
# 3. _normalize_domain
# ===========================================================================

class TestNormalizeDomain(unittest.TestCase):

    def test_basic(self):
        self.assertEqual(_normalize_domain("example.com"), "example.com")

    def test_www_stripped(self):
        self.assertEqual(_normalize_domain("www.example.com"), "example.com")

    def test_trailing_slash_stripped(self):
        self.assertEqual(_normalize_domain("example.com/"), "example.com")

    def test_uppercase(self):
        self.assertEqual(_normalize_domain("EXAMPLE.COM"), "example.com")

    def test_whitespace_stripped(self):
        self.assertEqual(_normalize_domain("  example.com  "), "example.com")

    def test_www_with_slash(self):
        self.assertEqual(_normalize_domain("www.example.com/"), "example.com")


# ===========================================================================
# 4. _is_user_url
# ===========================================================================

class TestIsUserUrl(unittest.TestCase):

    def test_matching(self):
        self.assertTrue(_is_user_url("https://example.com/page", "example.com"))

    def test_www_matching(self):
        self.assertTrue(_is_user_url("https://www.example.com/page", "example.com"))

    def test_non_matching(self):
        self.assertFalse(_is_user_url("https://other.com/page", "example.com"))

    def test_user_domain_with_www(self):
        self.assertTrue(_is_user_url("https://example.com/page", "www.example.com"))

    def test_subdomain_no_match(self):
        self.assertFalse(_is_user_url("https://blog.example.com/page", "example.com"))


# ===========================================================================
# 5. _find_user_result
# ===========================================================================

class TestFindUserResult(unittest.TestCase):

    def test_found(self):
        serp = _serp("test", [_result("https://example.com/p", 3)])
        r = _find_user_result(serp, "example.com")
        self.assertIsNotNone(r)
        self.assertEqual(r["position"], 3)

    def test_not_found(self):
        serp = _serp("test", [_result("https://other.com/p", 1)])
        self.assertIsNone(_find_user_result(serp, "example.com"))

    def test_www_variant(self):
        serp = _serp("test", [_result("https://www.example.com/p", 2)])
        r = _find_user_result(serp, "example.com")
        self.assertIsNotNone(r)

    def test_empty_organic(self):
        serp = _serp("test", [])
        self.assertIsNone(_find_user_result(serp, "example.com"))

    def test_no_organic_key(self):
        serp = {"keyword": "test"}
        self.assertIsNone(_find_user_result(serp, "example.com"))


# ===========================================================================
# 6. _find_competitor_result
# ===========================================================================

class TestFindCompetitorResult(unittest.TestCase):

    def test_found(self):
        serp = _serp("test", [_result("https://comp.com/p", 2)])
        r = _find_competitor_result(serp, "comp.com")
        self.assertIsNotNone(r)
        self.assertEqual(r["position"], 2)

    def test_not_found(self):
        serp = _serp("test", [_result("https://other.com/p", 1)])
        self.assertIsNone(_find_competitor_result(serp, "comp.com"))

    def test_www_variant(self):
        serp = _serp("test", [_result("https://www.comp.com/p", 5)])
        r = _find_competitor_result(serp, "comp.com")
        self.assertIsNotNone(r)


# ===========================================================================
# 7. _keyword_cluster
# ===========================================================================

class TestKeywordCluster(unittest.TestCase):

    def test_single_long_word(self):
        self.assertEqual(_keyword_cluster("marketing"), "marketing")

    def test_picks_longest_word(self):
        # Returns first word >3 chars
        result = _keyword_cluster("how to optimize seo")
        self.assertIn(result, ["optimize"])

    def test_short_words_only(self):
        # All <=3 chars, returns first word
        self.assertEqual(_keyword_cluster("how to do"), "how")

    def test_empty_string(self):
        self.assertEqual(_keyword_cluster(""), "other")

    def test_case_insensitive(self):
        result = _keyword_cluster("SEO Marketing Tips")
        self.assertEqual(result, "marketing")

    def test_single_short_word(self):
        self.assertEqual(_keyword_cluster("seo"), "seo")


# ===========================================================================
# 8. _profile_competitors
# ===========================================================================

class TestProfileCompetitors(unittest.TestCase):

    def test_basic_profiling(self):
        serps = [
            _serp("kw1", [
                _result("https://comp.com/a", 1),
                _result("https://example.com/a", 3),
            ]),
            _serp("kw2", [
                _result("https://comp.com/b", 2),
                _result("https://example.com/b", 4),
            ]),
        ]
        profiles = _profile_competitors(serps, "example.com")
        self.assertEqual(len(profiles), 1)
        self.assertEqual(profiles[0]["domain"], "comp.com")
        self.assertEqual(profiles[0]["keywords_shared"], 2)

    def test_user_excluded(self):
        serps = [_serp("kw1", [_result("https://example.com/a", 1)])]
        profiles = _profile_competitors(serps, "example.com")
        self.assertEqual(len(profiles), 0)

    def test_overlap_percentage(self):
        serps = [
            _serp("kw1", [_result("https://comp.com/a", 1)]),
            _serp("kw2", []),
        ]
        profiles = _profile_competitors(serps, "example.com")
        self.assertEqual(profiles[0]["overlap_percentage"], 50.0)

    def test_position_distribution(self):
        serps = [
            _serp("kw1", [_result("https://comp.com/a", 1), _result("https://example.com/x", 5)]),
            _serp("kw2", [_result("https://comp.com/b", 7), _result("https://example.com/y", 8)]),
            _serp("kw3", [_result("https://comp.com/c", 15), _result("https://example.com/z", 20)]),
        ]
        profiles = _profile_competitors(serps, "example.com")
        dist = profiles[0]["position_distribution"]
        self.assertEqual(dist["top_3"], 1)
        self.assertEqual(dist["pos_4_10"], 1)
        self.assertEqual(dist["pos_11_plus"], 1)

    def test_rank_1_keywords(self):
        serps = [
            _serp("seo tips", [_result("https://comp.com/a", 1)]),
            _serp("seo guide", [_result("https://comp.com/b", 5)]),
        ]
        profiles = _profile_competitors(serps, "example.com")
        self.assertIn("seo tips", profiles[0]["rank_1_keywords"])
        self.assertNotIn("seo guide", profiles[0]["rank_1_keywords"])

    def test_head_to_head_win_rate(self):
        serps = [
            _serp("kw1", [
                _result("https://comp.com/a", 2),
                _result("https://example.com/a", 5),
            ]),
            _serp("kw2", [
                _result("https://comp.com/b", 8),
                _result("https://example.com/b", 3),
            ]),
        ]
        profiles = _profile_competitors(serps, "example.com")
        # comp wins kw1, loses kw2
        self.assertAlmostEqual(profiles[0]["head_to_head_win_rate"], 0.5, places=2)
        self.assertEqual(profiles[0]["head_to_head_contests"], 2)

    def test_threat_level_critical(self):
        # Need overlap > 40% and avg_pos < 5
        serps = _make_serp_data(
            [f"keyword{i}" for i in range(3)],
            user_domain="example.com",
            comp_domains=["critical.com"],
            user_pos=10,
            comp_pos=2,
        )
        profiles = _profile_competitors(serps, "example.com")
        self.assertEqual(profiles[0]["threat_level"], "critical")

    def test_threat_level_low(self):
        # Need overlap < 20% and avg_pos >= 5
        serps = [
            _serp("kw1", [
                _result("https://comp.com/a", 12),
                _result("https://example.com/a", 5),
            ]),
        ] + [_serp(f"kw{i}", [_result("https://example.com/x", 3)]) for i in range(10)]
        profiles = _profile_competitors(serps, "example.com")
        if profiles:
            self.assertEqual(profiles[0]["threat_level"], "low")

    def test_capped_at_25(self):
        serps = []
        for i in range(30):
            serps.append(_serp(f"kw{i}", [
                _result(f"https://comp{i}.com/a", 3),
                _result("https://example.com/a", 5),
            ]))
        profiles = _profile_competitors(serps, "example.com")
        self.assertLessEqual(len(profiles), 25)

    def test_sorted_by_overlap_then_position(self):
        serps = [
            _serp("kw1", [_result("https://a.com/x", 2), _result("https://b.com/y", 1)]),
            _serp("kw2", [_result("https://a.com/x2", 3), _result("https://b.com/y2", 4)]),
            _serp("kw3", [_result("https://a.com/x3", 5)]),
        ]
        profiles = _profile_competitors(serps, "example.com")
        # a.com has 3 keywords, b.com has 2
        self.assertEqual(profiles[0]["domain"], "a.com")

    def test_unique_urls_counted(self):
        serps = [
            _serp("kw1", [_result("https://comp.com/page1", 1)]),
            _serp("kw2", [_result("https://comp.com/page2", 2)]),
            _serp("kw3", [_result("https://comp.com/page1", 3)]),  # duplicate URL
        ]
        profiles = _profile_competitors(serps, "example.com")
        self.assertEqual(profiles[0]["unique_urls_seen"], 2)

    def test_empty_serp_data(self):
        profiles = _profile_competitors([], "example.com")
        self.assertEqual(profiles, [])

    def test_output_keys(self):
        serps = [_serp("kw1", [_result("https://comp.com/a", 1)])]
        profiles = _profile_competitors(serps, "example.com")
        expected_keys = {
            "domain", "keywords_shared", "overlap_percentage", "avg_position",
            "position_distribution", "rank_1_keywords", "unique_urls_seen",
            "head_to_head_win_rate", "head_to_head_contests", "threat_level",
        }
        self.assertTrue(expected_keys.issubset(set(profiles[0].keys())))


# ===========================================================================
# 9. _assess_keyword_vulnerability
# ===========================================================================

class TestAssessKeywordVulnerability(unittest.TestCase):

    def test_basic_vulnerability(self):
        serps = [
            _serp("kw1", [
                _result("https://comp.com/a", 2),
                _result("https://example.com/a", 8),
            ]),
        ]
        result = _assess_keyword_vulnerability(serps, None, "example.com")
        self.assertGreater(result["total_vulnerable"], 0)

    def test_not_ranking_vulnerability(self):
        serps = [
            _serp("kw1", [_result("https://comp.com/a", 1)]),
        ]
        # Without GSC data showing impressions > 50, not_ranking may not trigger
        result = _assess_keyword_vulnerability(serps, None, "example.com")
        # The keyword won't appear because impressions=0 < 50
        self.assertEqual(result["total_vulnerable"], 0)

    def test_not_ranking_with_impressions(self):
        try:
            import pandas as pd
            gsc = pd.DataFrame([{"query": "kw1", "impressions": 1000, "clicks": 50, "position": 15}])
            serps = [_serp("kw1", [_result("https://comp.com/a", 1)])]
            result = _assess_keyword_vulnerability(serps, gsc, "example.com")
            self.assertGreater(result["total_vulnerable"], 0)
            vuln = result["vulnerable_keywords"][0]
            self.assertEqual(vuln["vulnerability"], "not_ranking")
            self.assertEqual(vuln["risk_level"], "critical")
        except ImportError:
            self.skipTest("pandas not available")

    def test_defended_keywords(self):
        serps = [
            _serp("kw1", [_result("https://example.com/a", 1)]),
        ]
        result = _assess_keyword_vulnerability(serps, None, "example.com")
        self.assertEqual(result["defended_keywords_count"], 1)

    def test_vulnerability_output_keys(self):
        serps = [_serp("kw1", [
            _result("https://comp.com/a", 1),
            _result("https://example.com/a", 8),
        ])]
        result = _assess_keyword_vulnerability(serps, None, "example.com")
        expected_keys = {
            "vulnerable_keywords", "defended_keywords_count", "total_vulnerable",
            "critical_count", "high_count", "medium_count",
        }
        self.assertTrue(expected_keys.issubset(set(result.keys())))

    def test_capped_at_40(self):
        serps = []
        for i in range(50):
            serps.append(_serp(f"kw{i}", [
                _result(f"https://comp.com/{i}", 1),
                _result(f"https://example.com/{i}", 15),
            ]))
        result = _assess_keyword_vulnerability(serps, None, "example.com")
        self.assertLessEqual(len(result["vulnerable_keywords"]), 40)

    def test_sorted_by_risk_then_impressions(self):
        try:
            import pandas as pd
            gsc = pd.DataFrame([
                {"query": "kw1", "impressions": 100, "clicks": 5, "position": 8},
                {"query": "kw2", "impressions": 1000, "clicks": 50, "position": 12},
            ])
            serps = [
                _serp("kw1", [_result("https://comp.com/a", 1), _result("https://example.com/a", 8)]),
                _serp("kw2", [_result("https://comp.com/b", 1), _result("https://example.com/b", 12)]),
            ]
            result = _assess_keyword_vulnerability(serps, gsc, "example.com")
            if len(result["vulnerable_keywords"]) >= 2:
                # Higher impressions / worse risk should come first
                first = result["vulnerable_keywords"][0]
                self.assertGreaterEqual(first["impressions"], 100)
        except ImportError:
            self.skipTest("pandas not available")

    def test_no_vulnerability_when_user_top3(self):
        serps = [_serp("kw1", [
            _result("https://example.com/a", 2),
            _result("https://comp.com/a", 5),
        ])]
        result = _assess_keyword_vulnerability(serps, None, "example.com")
        self.assertEqual(result["total_vulnerable"], 0)

    def test_empty_serp_data(self):
        result = _assess_keyword_vulnerability([], None, "example.com")
        self.assertEqual(result["total_vulnerable"], 0)
        self.assertEqual(result["defended_keywords_count"], 0)


# ===========================================================================
# 10. _vulnerability_recommendation
# ===========================================================================

class TestVulnerabilityRecommendation(unittest.TestCase):

    def test_deep_position(self):
        rec = _vulnerability_recommendation(25, 10, 3)
        self.assertIn("dedicated content strategy", rec)

    def test_major_gap(self):
        rec = _vulnerability_recommendation(10, 8, 2)
        self.assertIn("Major position gap", rec)

    def test_significant_gap(self):
        rec = _vulnerability_recommendation(8, 5, 2)
        self.assertIn("Significant gap", rec)

    def test_many_competitors_ahead(self):
        rec = _vulnerability_recommendation(8, 2, 6)
        self.assertIn("6 competitors", rec)

    def test_incremental(self):
        rec = _vulnerability_recommendation(5, 2, 2)
        self.assertIn("Incremental", rec)

    def test_returns_string(self):
        self.assertIsInstance(_vulnerability_recommendation(10, 5, 3), str)


# ===========================================================================
# 11. _detect_emerging_threats
# ===========================================================================

class TestDetectEmergingThreats(unittest.TestCase):

    def test_basic_detection(self):
        # Create a competitor with many unique URLs and fresh signals
        serps = []
        for i in range(10):
            serps.append(_serp(f"keyword{i}", [
                _result(f"https://newcomer.com/page{i}", i + 5,
                        snippet="Updated 2026 guide", title="Latest tips 2026"),
                _result(f"https://example.com/p{i}", 3),
            ]))
        threats = _detect_emerging_threats(serps, "example.com")
        domains = [t["domain"] for t in threats]
        self.assertIn("newcomer.com", domains)

    def test_user_excluded(self):
        serps = [_serp("kw1", [_result("https://example.com/a", 1)])]
        threats = _detect_emerging_threats(serps, "example.com")
        domains = [t["domain"] for t in threats]
        self.assertNotIn("example.com", domains)

    def test_minimum_keyword_count(self):
        # Fewer than 3 keywords = excluded
        serps = [
            _serp("kw1", [_result("https://tiny.com/a", 5)]),
            _serp("kw2", [_result("https://tiny.com/b", 6)]),
        ]
        threats = _detect_emerging_threats(serps, "example.com")
        domains = [t["domain"] for t in threats]
        self.assertNotIn("tiny.com", domains)

    def test_output_keys(self):
        serps = []
        for i in range(5):
            serps.append(_serp(f"kw{i}", [
                _result(f"https://emerging.com/page{i}", 12,
                        snippet="new 2026 content", title="Latest 2025"),
            ]))
        threats = _detect_emerging_threats(serps, "example.com")
        if threats:
            expected = {
                "domain", "keywords_present", "overlap_percentage", "avg_position",
                "unique_urls", "url_diversity_ratio", "fresh_content_pct",
                "emerging_threat_score", "signal",
            }
            self.assertTrue(expected.issubset(set(threats[0].keys())))

    def test_capped_at_15(self):
        serps = []
        for i in range(5):
            results = []
            for j in range(20):
                results.append(_result(
                    f"https://domain{j}.com/page{i}", j + 1,
                    snippet="updated 2026", title="latest new"
                ))
            serps.append(_serp(f"keyword{i}", results))
        threats = _detect_emerging_threats(serps, "example.com")
        self.assertLessEqual(len(threats), 15)

    def test_sorted_by_score_descending(self):
        serps = []
        for i in range(10):
            serps.append(_serp(f"kw{i}", [
                _result(f"https://big.com/p{i}", 8, snippet="2026 updated"),
                _result(f"https://small.com/p{i}", 15),
            ]))
        threats = _detect_emerging_threats(serps, "example.com")
        if len(threats) >= 2:
            for i in range(len(threats) - 1):
                self.assertGreaterEqual(
                    threats[i]["emerging_threat_score"],
                    threats[i + 1]["emerging_threat_score"]
                )

    def test_empty_serp_data(self):
        threats = _detect_emerging_threats([], "example.com")
        self.assertEqual(threats, [])

    def test_fresh_signals_detected(self):
        serps = []
        for i in range(5):
            serps.append(_serp(f"kw{i}", [
                _result(f"https://fresh.com/p{i}", 8,
                        snippet="updated for 2026", title="new guide"),
            ]))
        threats = _detect_emerging_threats(serps, "example.com")
        if threats:
            fresh_threat = [t for t in threats if t["domain"] == "fresh.com"]
            if fresh_threat:
                self.assertGreater(fresh_threat[0]["fresh_content_pct"], 0)


# ===========================================================================
# 12. _emerging_signal_label
# ===========================================================================

class TestEmergingSignalLabel(unittest.TestCase):

    def test_high_content_velocity(self):
        self.assertEqual(_emerging_signal_label(5, 40, 0.5), "high_content_velocity")

    def test_broad_content_expansion(self):
        self.assertEqual(_emerging_signal_label(5, 10, 0.8), "broad_content_expansion")

    def test_new_market_entrant(self):
        self.assertEqual(_emerging_signal_label(15, 10, 0.3), "new_market_entrant")

    def test_growing_presence(self):
        self.assertEqual(_emerging_signal_label(5, 10, 0.3), "growing_presence")

    def test_priority_fresh_over_diversity(self):
        # fresh_pct > 30 takes priority even if url_diversity > 0.7
        self.assertEqual(_emerging_signal_label(5, 50, 0.9), "high_content_velocity")


# ===========================================================================
# 13. _estimate_content_velocity
# ===========================================================================

class TestEstimateContentVelocity(unittest.TestCase):

    def test_basic_velocity(self):
        serps = [
            _serp("kw1", [
                _result("https://comp.com/a", 1),
                _result("https://comp.com/b", 2),
                _result("https://example.com/x", 3),
            ]),
        ]
        result = _estimate_content_velocity(serps, "example.com")
        self.assertEqual(result["user_unique_pages"], 1)
        self.assertGreater(len(result["competitor_velocity"]), 0)

    def test_vs_user_ratio(self):
        serps = [
            _serp("kw1", [
                _result("https://comp.com/a", 1),
                _result("https://comp.com/b", 2),
                _result("https://example.com/x", 3),
            ]),
            _serp("kw2", [
                _result("https://comp.com/c", 1),
                _result("https://example.com/x", 4),  # same URL
            ]),
        ]
        result = _estimate_content_velocity(serps, "example.com")
        # comp has 3 unique URLs, user has 1
        comp_entry = result["competitor_velocity"][0]
        self.assertEqual(comp_entry["unique_ranking_pages"], 3)
        self.assertEqual(comp_entry["vs_user_ratio"], 3.0)

    def test_assessment_significantly_more(self):
        serps = [
            _serp("kw1", [
                _result("https://comp.com/a", 1),
                _result("https://comp.com/b", 2),
                _result("https://comp.com/c", 3),
                _result("https://example.com/x", 4),
            ]),
        ]
        result = _estimate_content_velocity(serps, "example.com")
        self.assertEqual(result["competitor_velocity"][0]["assessment"], "significantly_more")

    def test_assessment_fewer(self):
        serps = [
            _serp("kw1", [
                _result("https://comp.com/a", 5),
                _result("https://example.com/a", 1),
                _result("https://example.com/b", 2),
                _result("https://example.com/c", 3),
                _result("https://example.com/d", 4),
            ]),
        ]
        result = _estimate_content_velocity(serps, "example.com")
        if result["competitor_velocity"]:
            self.assertEqual(result["competitor_velocity"][0]["assessment"], "fewer")

    def test_user_content_gap(self):
        serps = []
        for i in range(5):
            serps.append(_serp(f"kw{i}", [
                _result(f"https://comp1.com/p{i}", 1),
                _result(f"https://comp2.com/p{i}", 2),
                _result(f"https://comp3.com/p{i}", 3),
                _result("https://example.com/only", 8),
            ]))
        result = _estimate_content_velocity(serps, "example.com")
        self.assertGreater(result["user_content_gap"], 0)

    def test_capped_at_15(self):
        serps = []
        for i in range(5):
            results = [_result("https://example.com/x", 10)]
            for j in range(20):
                results.append(_result(f"https://domain{j}.com/p{i}", j + 1))
            results.sort(key=lambda r: r["position"])
            serps.append(_serp(f"kw{i}", results[:10]))
        result = _estimate_content_velocity(serps, "example.com")
        self.assertLessEqual(len(result["competitor_velocity"]), 15)

    def test_empty_serp(self):
        result = _estimate_content_velocity([], "example.com")
        self.assertEqual(result["user_unique_pages"], 0)
        self.assertEqual(result["competitor_velocity"], [])

    def test_zero_user_pages_inf_ratio(self):
        serps = [_serp("kw1", [_result("https://comp.com/a", 1)])]
        result = _estimate_content_velocity(serps, "example.com")
        if result["competitor_velocity"]:
            self.assertEqual(result["competitor_velocity"][0]["vs_user_ratio"], float("inf"))

    def test_output_keys(self):
        serps = [_serp("kw1", [
            _result("https://comp.com/a", 1),
            _result("https://example.com/x", 2),
        ])]
        result = _estimate_content_velocity(serps, "example.com")
        self.assertIn("user_unique_pages", result)
        self.assertIn("competitor_velocity", result)
        self.assertIn("user_content_gap", result)


# ===========================================================================
# 14. _analyze_competitive_pressure
# ===========================================================================

class TestAnalyzeCompetitivePressure(unittest.TestCase):

    def test_basic_pressure(self):
        serps = _make_serp_data(
            ["marketing tips", "marketing guide", "marketing strategy"],
            comp_domains=["comp1.com", "comp2.com"],
            user_pos=8,
            comp_pos=3,
        )
        pressure = _analyze_competitive_pressure(serps, "example.com")
        self.assertGreater(len(pressure), 0)

    def test_single_keyword_cluster_excluded(self):
        serps = [_serp("solo keyword", [_result("https://comp.com/a", 1)])]
        pressure = _analyze_competitive_pressure(serps, "example.com")
        # Clusters with < 2 keywords are excluded
        self.assertEqual(len(pressure), 0)

    def test_pressure_score_range(self):
        serps = _make_serp_data(
            [f"topic word{i}" for i in range(5)],
            comp_domains=[f"comp{j}.com" for j in range(10)],
            user_pos=15,
            comp_pos=2,
        )
        pressure = _analyze_competitive_pressure(serps, "example.com")
        for p in pressure:
            self.assertGreaterEqual(p["pressure_score"], 0)
            self.assertLessEqual(p["pressure_score"], 100)

    def test_pressure_levels(self):
        valid_levels = {"critical", "high", "moderate", "low"}
        serps = _make_serp_data(
            [f"content word{i}" for i in range(5)],
            comp_domains=["comp.com"],
            user_pos=5,
            comp_pos=3,
        )
        pressure = _analyze_competitive_pressure(serps, "example.com")
        for p in pressure:
            self.assertIn(p["pressure_level"], valid_levels)

    def test_capped_at_20(self):
        serps = []
        for i in range(30):
            cluster_word = f"cluster{i:02d}word"
            serps.extend(_make_serp_data(
                [f"{cluster_word} sub1", f"{cluster_word} sub2"],
                comp_domains=["comp.com"],
                user_pos=8,
                comp_pos=2,
            ))
        pressure = _analyze_competitive_pressure(serps, "example.com")
        self.assertLessEqual(len(pressure), 20)

    def test_sorted_by_pressure_descending(self):
        serps = _make_serp_data(
            [f"important word{i}" for i in range(5)],
            comp_domains=[f"comp{j}.com" for j in range(10)],
            user_pos=20,
            comp_pos=1,
        )
        pressure = _analyze_competitive_pressure(serps, "example.com")
        if len(pressure) >= 2:
            for i in range(len(pressure) - 1):
                self.assertGreaterEqual(
                    pressure[i]["pressure_score"],
                    pressure[i + 1]["pressure_score"]
                )

    def test_output_keys(self):
        serps = _make_serp_data(
            ["testing word1", "testing word2"],
            comp_domains=["comp.com"],
            user_pos=8,
            comp_pos=3,
        )
        pressure = _analyze_competitive_pressure(serps, "example.com")
        if pressure:
            expected = {
                "cluster", "keyword_count", "sample_keywords", "avg_user_position",
                "avg_competitor_position", "unique_competitors", "user_outside_top3_pct",
                "pressure_score", "pressure_level",
            }
            self.assertTrue(expected.issubset(set(pressure[0].keys())))

    def test_empty_serp_data(self):
        pressure = _analyze_competitive_pressure([], "example.com")
        self.assertEqual(pressure, [])

    def test_sample_keywords_capped(self):
        serps = _make_serp_data(
            [f"cluster word{i}" for i in range(10)],
            comp_domains=["comp.com"],
            user_pos=8,
            comp_pos=3,
        )
        pressure = _analyze_competitive_pressure(serps, "example.com")
        for p in pressure:
            self.assertLessEqual(len(p["sample_keywords"]), 5)


# ===========================================================================
# 15. _generate_recommendations
# ===========================================================================

class TestGenerateRecommendations(unittest.TestCase):

    def _make_args(self, **overrides):
        defaults = {
            "competitors": [],
            "vulnerability": {
                "vulnerable_keywords": [],
                "defended_keywords_count": 0,
                "total_vulnerable": 0,
                "critical_count": 0,
                "high_count": 0,
                "medium_count": 0,
            },
            "emerging": [],
            "content_velocity": {
                "user_unique_pages": 5,
                "competitor_velocity": [],
                "user_content_gap": 0,
            },
            "pressure": [],
        }
        defaults.update(overrides)
        return defaults

    def test_critical_competitor_rec(self):
        args = self._make_args(competitors=[
            {"domain": "threat.com", "threat_level": "critical"},
        ])
        recs = _generate_recommendations(**args)
        self.assertTrue(any("threat.com" in r["recommendation"] for r in recs))

    def test_vulnerable_keywords_rec(self):
        args = self._make_args(vulnerability={
            "vulnerable_keywords": [],
            "defended_keywords_count": 0,
            "total_vulnerable": 5,
            "critical_count": 3,
            "high_count": 2,
            "medium_count": 0,
        })
        recs = _generate_recommendations(**args)
        self.assertTrue(any(r["category"] == "keyword_defence" for r in recs))

    def test_not_ranking_rec(self):
        args = self._make_args(vulnerability={
            "vulnerable_keywords": [
                {"vulnerability": "not_ranking", "keyword": "test"},
            ],
            "defended_keywords_count": 0,
            "total_vulnerable": 1,
            "critical_count": 0,
            "high_count": 0,
            "medium_count": 1,
        })
        recs = _generate_recommendations(**args)
        content_gap_recs = [r for r in recs if r["category"] == "content_gap"]
        self.assertGreater(len(content_gap_recs), 0)

    def test_emerging_threats_rec(self):
        args = self._make_args(emerging=[
            {"domain": "new.com", "signal": "high_content_velocity"},
        ])
        recs = _generate_recommendations(**args)
        self.assertTrue(any(r["category"] == "emerging_threats" for r in recs))

    def test_content_velocity_gap_rec(self):
        args = self._make_args(content_velocity={
            "user_unique_pages": 5,
            "competitor_velocity": [],
            "user_content_gap": 5,
        })
        recs = _generate_recommendations(**args)
        self.assertTrue(any(r["category"] == "content_velocity" for r in recs))

    def test_critical_pressure_clusters_rec(self):
        args = self._make_args(pressure=[
            {"cluster": "seo", "pressure_level": "critical"},
        ])
        recs = _generate_recommendations(**args)
        self.assertTrue(any(r["category"] == "cluster_defence" for r in recs))

    def test_sorted_by_priority(self):
        args = self._make_args(
            competitors=[{"domain": "x.com", "threat_level": "critical"}],
            vulnerability={
                "vulnerable_keywords": [{"vulnerability": "not_ranking"}],
                "defended_keywords_count": 0,
                "total_vulnerable": 5,
                "critical_count": 3,
                "high_count": 2,
                "medium_count": 0,
            },
            emerging=[{"domain": "new.com", "signal": "high_content_velocity"}],
        )
        recs = _generate_recommendations(**args)
        priorities = [r["priority"] for r in recs]
        priority_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
        numeric = [priority_order.get(p, 9) for p in priorities]
        self.assertEqual(numeric, sorted(numeric))

    def test_capped_at_10(self):
        args = self._make_args(
            competitors=[{"domain": f"comp{i}.com", "threat_level": "critical"} for i in range(5)],
            vulnerability={
                "vulnerable_keywords": [{"vulnerability": "not_ranking"}] * 5,
                "defended_keywords_count": 0,
                "total_vulnerable": 10,
                "critical_count": 5,
                "high_count": 5,
                "medium_count": 0,
            },
            emerging=[{"domain": f"e{i}.com", "signal": "high_content_velocity"} for i in range(5)],
            content_velocity={"user_unique_pages": 1, "competitor_velocity": [], "user_content_gap": 10},
            pressure=[{"cluster": f"c{i}", "pressure_level": "critical"} for i in range(5)],
        )
        recs = _generate_recommendations(**args)
        self.assertLessEqual(len(recs), 10)

    def test_empty_all(self):
        args = self._make_args()
        recs = _generate_recommendations(**args)
        self.assertEqual(recs, [])

    def test_rec_output_keys(self):
        args = self._make_args(competitors=[
            {"domain": "x.com", "threat_level": "critical"},
        ])
        recs = _generate_recommendations(**args)
        if recs:
            self.assertIn("priority", recs[0])
            self.assertIn("category", recs[0])
            self.assertIn("recommendation", recs[0])


# ===========================================================================
# 16. analyze_competitive_threats (public API)
# ===========================================================================

class TestAnalyzeCompetitiveThreats(unittest.TestCase):

    def test_no_serp_data(self):
        result = analyze_competitive_threats(None, None, "example.com")
        self.assertEqual(result["keywords_analyzed"], 0)

    def test_no_user_domain(self):
        result = analyze_competitive_threats([_serp("kw1", [])], None, None)
        self.assertEqual(result["keywords_analyzed"], 0)

    def test_empty_serp_list(self):
        result = analyze_competitive_threats([], None, "example.com")
        self.assertEqual(result["keywords_analyzed"], 0)

    def test_basic_analysis(self):
        serps = _make_serp_data(
            ["seo tips", "seo guide", "seo tools"],
            comp_domains=["comp1.com", "comp2.com"],
            user_pos=5,
            comp_pos=2,
        )
        result = analyze_competitive_threats(serps, None, "example.com")
        self.assertEqual(result["keywords_analyzed"], 3)
        self.assertGreater(len(result["competitor_profiles"]), 0)

    def test_output_schema(self):
        serps = [_serp("kw1", [
            _result("https://comp.com/a", 1),
            _result("https://example.com/a", 3),
        ])]
        result = analyze_competitive_threats(serps, None, "example.com")
        expected_keys = {
            "keywords_analyzed", "competitor_profiles", "keyword_vulnerability",
            "emerging_threats", "content_velocity", "competitive_pressure",
            "recommendations", "summary",
        }
        self.assertTrue(expected_keys.issubset(set(result.keys())))

    def test_summary_is_string(self):
        serps = _make_serp_data(["kw1", "kw2"], comp_domains=["comp.com"])
        result = analyze_competitive_threats(serps, None, "example.com")
        self.assertIsInstance(result["summary"], str)
        self.assertGreater(len(result["summary"]), 0)

    def test_summary_mentions_keywords(self):
        serps = _make_serp_data(["kw1", "kw2"], comp_domains=["comp.com"])
        result = analyze_competitive_threats(serps, None, "example.com")
        self.assertIn("2", result["summary"])

    def test_with_gsc_data(self):
        try:
            import pandas as pd
            gsc = pd.DataFrame([
                {"query": "kw1", "impressions": 500, "clicks": 25, "position": 5, "ctr": 0.05},
            ])
            serps = [_serp("kw1", [
                _result("https://comp.com/a", 1),
                _result("https://example.com/a", 5),
            ])]
            result = analyze_competitive_threats(serps, gsc, "example.com")
            self.assertEqual(result["keywords_analyzed"], 1)
        except ImportError:
            self.skipTest("pandas not available")

    def test_competitor_profiles_is_list(self):
        serps = _make_serp_data(["kw1"], comp_domains=["comp.com"])
        result = analyze_competitive_threats(serps, None, "example.com")
        self.assertIsInstance(result["competitor_profiles"], list)

    def test_recommendations_is_list(self):
        serps = _make_serp_data(["kw1"], comp_domains=["comp.com"])
        result = analyze_competitive_threats(serps, None, "example.com")
        self.assertIsInstance(result["recommendations"], list)


# ===========================================================================
# 17. _empty_result
# ===========================================================================

class TestEmptyResult(unittest.TestCase):

    def test_schema(self):
        result = _empty_result()
        expected_keys = {
            "keywords_analyzed", "competitor_profiles", "keyword_vulnerability",
            "emerging_threats", "content_velocity", "competitive_pressure",
            "recommendations", "summary",
        }
        self.assertEqual(expected_keys, set(result.keys()))

    def test_keywords_analyzed_zero(self):
        self.assertEqual(_empty_result()["keywords_analyzed"], 0)

    def test_lists_empty(self):
        result = _empty_result()
        self.assertEqual(result["competitor_profiles"], [])
        self.assertEqual(result["emerging_threats"], [])
        self.assertEqual(result["competitive_pressure"], [])
        self.assertEqual(result["recommendations"], [])

    def test_vulnerability_sub_keys(self):
        vuln = _empty_result()["keyword_vulnerability"]
        self.assertEqual(vuln["total_vulnerable"], 0)
        self.assertEqual(vuln["critical_count"], 0)

    def test_content_velocity_sub_keys(self):
        cv = _empty_result()["content_velocity"]
        self.assertEqual(cv["user_unique_pages"], 0)
        self.assertEqual(cv["user_content_gap"], 0)

    def test_summary_is_string(self):
        self.assertIsInstance(_empty_result()["summary"], str)


# ===========================================================================
# 18. Edge cases
# ===========================================================================

class TestEdgeCases(unittest.TestCase):

    def test_unicode_domains(self):
        serps = [_serp("kw1", [_result("https://München.de/page", 1)])]
        profiles = _profile_competitors(serps, "example.com")
        # Should not crash
        self.assertIsInstance(profiles, list)

    def test_special_chars_in_keyword(self):
        serps = [_serp("what is C++ programming?", [
            _result("https://comp.com/a", 1),
            _result("https://example.com/a", 3),
        ])]
        result = analyze_competitive_threats(serps, None, "example.com")
        self.assertEqual(result["keywords_analyzed"], 1)

    def test_duplicate_competitor_urls(self):
        serps = [
            _serp("kw1", [_result("https://comp.com/same", 1)]),
            _serp("kw2", [_result("https://comp.com/same", 2)]),
        ]
        profiles = _profile_competitors(serps, "example.com")
        self.assertEqual(profiles[0]["unique_urls_seen"], 1)

    def test_very_large_serp_dataset(self):
        serps = _make_serp_data(
            [f"keyword{i}" for i in range(100)],
            comp_domains=[f"comp{j}.com" for j in range(5)],
            user_pos=8,
            comp_pos=3,
        )
        result = analyze_competitive_threats(serps, None, "example.com")
        self.assertEqual(result["keywords_analyzed"], 100)

    def test_no_organic_results_key(self):
        serps = [{"keyword": "test"}]
        # Should handle gracefully
        profiles = _profile_competitors(serps, "example.com")
        self.assertEqual(profiles, [])

    def test_empty_url_in_results(self):
        serps = [_serp("kw1", [_result("", 1)])]
        profiles = _profile_competitors(serps, "example.com")
        # Empty domain should be filtered
        self.assertEqual(profiles, [])

    def test_position_zero(self):
        serps = [_serp("kw1", [
            _result("https://comp.com/a", 0),
            _result("https://example.com/a", 3),
        ])]
        profiles = _profile_competitors(serps, "example.com")
        self.assertIsInstance(profiles, list)

    def test_mixed_http_https(self):
        serps = [
            _serp("kw1", [_result("http://comp.com/a", 1)]),
            _serp("kw2", [_result("https://comp.com/b", 2)]),
        ]
        profiles = _profile_competitors(serps, "example.com")
        self.assertEqual(len(profiles), 1)
        self.assertEqual(profiles[0]["keywords_shared"], 2)

    def test_www_user_domain(self):
        serps = [_serp("kw1", [
            _result("https://www.example.com/a", 1),
            _result("https://comp.com/b", 2),
        ])]
        profiles = _profile_competitors(serps, "www.example.com")
        # example.com (user) should be excluded
        self.assertEqual(len(profiles), 1)
        self.assertEqual(profiles[0]["domain"], "comp.com")

    def test_only_top_20_results_considered_in_profiling(self):
        results = [_result(f"https://comp.com/p{i}", i) for i in range(1, 25)]
        serps = [_serp("kw1", results)]
        profiles = _profile_competitors(serps, "example.com")
        if profiles:
            # Only first 20 results are considered
            self.assertLessEqual(profiles[0]["keywords_shared"], 1)

    def test_content_velocity_only_top10(self):
        results = [_result(f"https://comp.com/p{i}", i) for i in range(1, 15)]
        serps = [_serp("kw1", results)]
        velocity = _estimate_content_velocity(serps, "example.com")
        # Only top 10 organic results considered
        if velocity["competitor_velocity"]:
            self.assertLessEqual(velocity["competitor_velocity"][0]["unique_ranking_pages"], 10)


if __name__ == "__main__":
    unittest.main()
