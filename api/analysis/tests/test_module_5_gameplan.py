"""
Comprehensive test suite for Module 5: The Gameplan — Prioritized Action Plan.

Tests cover:
1. Input validation — empty/None inputs, partial module data
2. Output schema — all required keys present, correct types
3. Critical fixes extraction — decay pages, CTR anomalies, cannibalization
4. Quick wins extraction — striking distance, SERP features, content refresh
5. Strategic plays extraction — consolidation, refresh programs, algorithm recovery, intent shifts
6. Structural improvements — internal linking, seasonal calendar, monitoring
7. Recovery/growth potential calculations
8. Narrative generation — fallback narrative structure and content
9. Helper functions — instruction generators, summarizers
10. Full pipeline — realistic multi-module inputs
11. Edge cases — empty modules, missing keys, zero values
"""

import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime

# Import module under test
from api.analysis.module_5_gameplan import (
    generate_gameplan,
    _extract_critical_fixes,
    _extract_quick_wins,
    _extract_strategic_plays,
    _extract_structural_improvements,
    _calculate_recovery_potential,
    _calculate_growth_potential,
    _generate_narrative_with_fallback,
    _generate_fallback_narrative,
    _summarize_health,
    _generate_critical_page_instructions,
    _generate_cannibalization_instructions,
    _generate_striking_distance_instructions,
    _generate_serp_feature_instructions,
    _generate_consolidation_instructions,
    _generate_algorithm_recovery_instructions,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _minimal_health():
    return {
        "overall_direction": "growth",
        "trend_slope_pct_per_month": 3.2,
        "health_score": 72,
        "seasonality": {},
    }


def _minimal_triage():
    return {"pages": [], "summary": {}}


def _minimal_serp():
    return {"serp_feature_displacement": [], "summary": {}}


def _minimal_content():
    return {
        "cannibalization_clusters": [],
        "striking_distance": [],
        "thin_content_pages": [],
        "update_priority_matrix": {},
    }


def _critical_triage():
    """Triage with critical pages and CTR anomalies."""
    return {
        "pages": [
            {
                "url": "/blog/important-page",
                "bucket": "critical",
                "current_monthly_clicks": 500,
                "trend_slope": -0.8,
                "ctr_anomaly": True,
                "engagement_flag": "low_engagement",
                "ctr_actual": 0.02,
                "ctr_expected": 0.06,
                "impressions": 20000,
            },
            {
                "url": "/blog/small-page",
                "bucket": "critical",
                "current_monthly_clicks": 20,
                "trend_slope": -0.3,
            },
            {
                "url": "/blog/high-ctr-anomaly",
                "bucket": "decaying",
                "current_monthly_clicks": 300,
                "ctr_anomaly": True,
                "ctr_actual": 0.01,
                "ctr_expected": 0.05,
                "impressions": 15000,
                "engagement_flag": "low_engagement",
            },
        ],
        "summary": {},
    }


def _content_with_cannibalization():
    return {
        "cannibalization_clusters": [
            {
                "pages": ["/page-a", "/page-b"],
                "shared_queries": 12,
                "total_impressions_affected": 8000,
                "recommendation": "consolidate",
                "keep_page": "/page-a",
            },
            {
                "pages": ["/page-c", "/page-d"],
                "shared_queries": 3,
                "total_impressions_affected": 1000,
                "recommendation": "differentiate",
            },
        ],
        "striking_distance": [
            {
                "query": "best seo tool",
                "current_position": 12,
                "landing_page": "/tools",
                "estimated_click_gain_if_top5": 200,
                "intent": "commercial",
            },
            {
                "query": "how to audit site",
                "current_position": 18,
                "landing_page": "/guides/audit",
                "estimated_click_gain_if_top5": 80,
                "intent": "informational",
            },
            {
                "query": "tiny keyword",
                "current_position": 14,
                "landing_page": "/tiny",
                "estimated_click_gain_if_top5": 10,
            },
        ],
        "thin_content_pages": [],
        "update_priority_matrix": {
            "urgent_update": [
                {"url": f"/old-{i}", "current_monthly_clicks": 100}
                for i in range(8)
            ]
        },
    }


def _serp_with_displacement():
    return {
        "serp_feature_displacement": [
            {
                "keyword": "seo software",
                "estimated_ctr_impact": -0.05,
                "impressions": 10000,
                "features_above": ["featured_snippet", "people_also_ask"],
            },
            {
                "keyword": "minor keyword",
                "estimated_ctr_impact": -0.01,
                "impressions": 500,
                "features_above": [],
            },
        ],
        "summary": {},
    }


def _algorithm_data():
    return {
        "updates_impacting_site": [
            {
                "update_name": "Core Update March 2026",
                "recovery_status": "not_recovered",
                "click_change_pct": -12,
                "common_characteristics": ["thin_content", "no_schema"],
            },
            {
                "update_name": "Helpful Content Update",
                "recovery_status": "not_recovered",
                "click_change_pct": -8,
                "common_characteristics": ["thin_content"],
            },
            {
                "update_name": "Old Update",
                "recovery_status": "recovered",
                "click_change_pct": -3,
            },
        ]
    }


def _intent_data():
    return {
        "migrations": [
            {"keyword": "seo tool", "traffic_impact_pct": -15},
            {"keyword": "rank tracker", "traffic_impact_pct": -20},
            {"keyword": "minor shift", "traffic_impact_pct": -2},
        ]
    }


def _architecture_data():
    return {
        "orphan_pages": ["/orphan-1", "/orphan-2", "/orphan-3"],
        "hub_opportunities": [
            {"topic": "SEO guides"},
            {"topic": "Analytics tutorials"},
        ],
    }


def _branded_data():
    return {"branded_ratio": 0.35}


def _revenue_data():
    return {"revenue_at_risk_90d": 12500}


# ===========================================================================
# 1. Input Validation
# ===========================================================================


class TestInputValidation(unittest.TestCase):
    """Gameplan should handle empty / None inputs gracefully."""

    def test_all_minimal_inputs(self):
        result = generate_gameplan(
            _minimal_health(), _minimal_triage(),
            _minimal_serp(), _minimal_content()
        )
        self.assertIsInstance(result, dict)
        self.assertIn("critical", result)
        self.assertIn("quick_wins", result)

    def test_empty_dicts(self):
        result = generate_gameplan({}, {}, {}, {})
        self.assertIsInstance(result, dict)
        self.assertEqual(result["critical"], [])
        self.assertEqual(result["quick_wins"], [])

    def test_optional_modules_none(self):
        result = generate_gameplan(
            _minimal_health(), _minimal_triage(),
            _minimal_serp(), _minimal_content(),
            algorithm=None, intent=None, ctr=None,
            architecture=None, branded=None,
            competitive=None, revenue=None,
        )
        self.assertIsInstance(result, dict)

    def test_optional_modules_provided(self):
        result = generate_gameplan(
            _minimal_health(), _critical_triage(),
            _serp_with_displacement(), _content_with_cannibalization(),
            algorithm=_algorithm_data(),
            intent=_intent_data(),
            architecture=_architecture_data(),
            branded=_branded_data(),
            revenue=_revenue_data(),
        )
        self.assertIsInstance(result, dict)
        self.assertIn("narrative", result)


# ===========================================================================
# 2. Output Schema
# ===========================================================================


class TestOutputSchema(unittest.TestCase):
    """Verify top-level keys and types."""

    def setUp(self):
        self.result = generate_gameplan(
            _minimal_health(), _critical_triage(),
            _serp_with_displacement(), _content_with_cannibalization(),
            algorithm=_algorithm_data(),
            intent=_intent_data(),
            architecture=_architecture_data(),
            branded=_branded_data(),
            revenue=_revenue_data(),
        )

    def test_required_keys(self):
        for key in [
            "critical", "quick_wins", "strategic", "structural",
            "total_estimated_monthly_click_recovery",
            "total_estimated_monthly_click_growth",
            "narrative", "generated_at",
        ]:
            self.assertIn(key, self.result, f"Missing key: {key}")

    def test_lists_types(self):
        self.assertIsInstance(self.result["critical"], list)
        self.assertIsInstance(self.result["quick_wins"], list)
        self.assertIsInstance(self.result["strategic"], list)
        self.assertIsInstance(self.result["structural"], list)

    def test_numeric_fields(self):
        self.assertIsInstance(self.result["total_estimated_monthly_click_recovery"], int)
        self.assertIsInstance(self.result["total_estimated_monthly_click_growth"], int)

    def test_narrative_is_string(self):
        self.assertIsInstance(self.result["narrative"], str)
        self.assertGreater(len(self.result["narrative"]), 0)

    def test_generated_at_is_iso(self):
        datetime.fromisoformat(self.result["generated_at"])


# ===========================================================================
# 3. Critical Fixes Extraction
# ===========================================================================


class TestCriticalFixes(unittest.TestCase):

    def test_empty_triage(self):
        result = _extract_critical_fixes({}, {"pages": []}, {"cannibalization_clusters": []})
        self.assertEqual(result, [])

    def test_critical_page_above_threshold(self):
        result = _extract_critical_fixes({}, _critical_triage(), _minimal_content())
        types = [a["type"] for a in result]
        self.assertIn("critical_page_rescue", types)

    def test_critical_page_below_threshold_excluded(self):
        """Pages with < 100 clicks should NOT be in critical."""
        result = _extract_critical_fixes({}, _critical_triage(), _minimal_content())
        for action in result:
            if action["type"] == "critical_page_rescue":
                self.assertGreater(action["current_clicks"], 100)

    def test_ctr_anomaly_extracted(self):
        result = _extract_critical_fixes({}, _critical_triage(), _minimal_content())
        types = [a["type"] for a in result]
        self.assertIn("ctr_optimization", types)

    def test_ctr_optimization_has_impact(self):
        result = _extract_critical_fixes({}, _critical_triage(), _minimal_content())
        ctr_actions = [a for a in result if a["type"] == "ctr_optimization"]
        for action in ctr_actions:
            self.assertGreater(action["impact"], 0)

    def test_cannibalization_above_threshold(self):
        result = _extract_critical_fixes({}, _minimal_triage(), _content_with_cannibalization())
        types = [a["type"] for a in result]
        self.assertIn("cannibalization_fix", types)

    def test_cannibalization_below_threshold_excluded(self):
        """Clusters with < 5000 impressions should NOT be critical."""
        result = _extract_critical_fixes({}, _minimal_triage(), _content_with_cannibalization())
        for action in result:
            if action["type"] == "cannibalization_fix":
                self.assertGreater(action["impact"], 0)

    def test_sorted_by_impact(self):
        result = _extract_critical_fixes({}, _critical_triage(), _content_with_cannibalization())
        impacts = [a.get("impact", 0) for a in result]
        self.assertEqual(impacts, sorted(impacts, reverse=True))

    def test_capped_at_10(self):
        # Create many critical pages
        pages = [
            {"url": f"/page-{i}", "bucket": "critical",
             "current_monthly_clicks": 200, "trend_slope": -0.5}
            for i in range(20)
        ]
        triage = {"pages": pages}
        result = _extract_critical_fixes({}, triage, _minimal_content())
        self.assertLessEqual(len(result), 10)


# ===========================================================================
# 4. Quick Wins Extraction
# ===========================================================================


class TestQuickWins(unittest.TestCase):

    def test_empty_inputs(self):
        result = _extract_quick_wins(
            {"pages": []}, {"serp_feature_displacement": []},
            {"striking_distance": []}, None
        )
        self.assertEqual(result, [])

    def test_striking_distance_included(self):
        result = _extract_quick_wins(
            _minimal_triage(), _minimal_serp(),
            _content_with_cannibalization(), None
        )
        types = [a["type"] for a in result]
        self.assertIn("striking_distance", types)

    def test_small_striking_distance_excluded(self):
        """Keywords with < 50 click gain should be excluded."""
        result = _extract_quick_wins(
            _minimal_triage(), _minimal_serp(),
            _content_with_cannibalization(), None
        )
        for action in result:
            if action["type"] == "striking_distance":
                self.assertGreater(action["impact"], 50)

    def test_serp_feature_included(self):
        result = _extract_quick_wins(
            _minimal_triage(), _serp_with_displacement(),
            _minimal_content(), None
        )
        types = [a["type"] for a in result]
        self.assertIn("serp_feature_optimization", types)

    def test_minor_serp_impact_excluded(self):
        """SERP features with CTR impact < 3% should be excluded."""
        result = _extract_quick_wins(
            _minimal_triage(), _serp_with_displacement(),
            _minimal_content(), None
        )
        for action in result:
            if action["type"] == "serp_feature_optimization":
                self.assertGreater(action["impact"], 0)

    def test_content_refresh_for_decaying_pages(self):
        triage = {
            "pages": [
                {
                    "url": "/decaying",
                    "bucket": "decaying",
                    "current_monthly_clicks": 100,
                    "engagement_flag": "low_engagement",
                }
            ]
        }
        result = _extract_quick_wins(triage, _minimal_serp(), _minimal_content(), None)
        types = [a["type"] for a in result]
        self.assertIn("content_refresh", types)

    def test_priority_score_added(self):
        result = _extract_quick_wins(
            _minimal_triage(), _serp_with_displacement(),
            _content_with_cannibalization(), None
        )
        for action in result:
            self.assertIn("priority_score", action)
            self.assertIsInstance(action["priority_score"], (int, float))

    def test_sorted_by_priority_score(self):
        result = _extract_quick_wins(
            _minimal_triage(), _serp_with_displacement(),
            _content_with_cannibalization(), None
        )
        scores = [a["priority_score"] for a in result]
        self.assertEqual(scores, sorted(scores, reverse=True))

    def test_capped_at_15(self):
        content = {
            "cannibalization_clusters": [],
            "striking_distance": [
                {
                    "query": f"kw-{i}",
                    "current_position": 12,
                    "landing_page": f"/p-{i}",
                    "estimated_click_gain_if_top5": 100 + i,
                    "intent": "informational",
                }
                for i in range(25)
            ],
            "thin_content_pages": [],
            "update_priority_matrix": {},
        }
        result = _extract_quick_wins(_minimal_triage(), _minimal_serp(), content, None)
        self.assertLessEqual(len(result), 15)


# ===========================================================================
# 5. Strategic Plays Extraction
# ===========================================================================


class TestStrategicPlays(unittest.TestCase):

    def test_empty_inputs(self):
        result = _extract_strategic_plays(
            _minimal_content(), _minimal_serp(), None, None, None
        )
        self.assertEqual(result, [])

    def test_consolidation_project_detected(self):
        result = _extract_strategic_plays(
            _content_with_cannibalization(), _minimal_serp(),
            None, None, None
        )
        types = [a["type"] for a in result]
        # The cluster with recommendation "consolidate" and > 1000 impressions
        self.assertIn("consolidation_project", types)

    def test_content_refresh_program_with_many_urgent(self):
        result = _extract_strategic_plays(
            _content_with_cannibalization(), _minimal_serp(),
            None, None, None
        )
        types = [a["type"] for a in result]
        self.assertIn("content_refresh_program", types)

    def test_algorithm_recovery_detected(self):
        result = _extract_strategic_plays(
            _minimal_content(), _minimal_serp(),
            _algorithm_data(), None, None
        )
        types = [a["type"] for a in result]
        self.assertIn("algorithm_recovery", types)

    def test_recovered_updates_excluded(self):
        """Only unrecovered updates with > 5% impact should trigger actions."""
        algo = {
            "updates_impacting_site": [
                {"recovery_status": "recovered", "click_change_pct": -20}
            ]
        }
        result = _extract_strategic_plays(
            _minimal_content(), _minimal_serp(), algo, None, None
        )
        types = [a["type"] for a in result]
        self.assertNotIn("algorithm_recovery", types)

    def test_intent_realignment_detected(self):
        result = _extract_strategic_plays(
            _minimal_content(), _minimal_serp(),
            None, _intent_data(), None
        )
        types = [a["type"] for a in result]
        self.assertIn("intent_realignment", types)

    def test_minor_intent_shift_excluded(self):
        intent = {"migrations": [{"keyword": "kw", "traffic_impact_pct": -3}]}
        result = _extract_strategic_plays(
            _minimal_content(), _minimal_serp(), None, intent, None
        )
        types = [a["type"] for a in result]
        self.assertNotIn("intent_realignment", types)

    def test_sorted_by_impact(self):
        result = _extract_strategic_plays(
            _content_with_cannibalization(), _minimal_serp(),
            _algorithm_data(), _intent_data(), None
        )
        impacts = [a.get("impact", 0) for a in result]
        self.assertEqual(impacts, sorted(impacts, reverse=True))

    def test_capped_at_8(self):
        result = _extract_strategic_plays(
            _content_with_cannibalization(), _minimal_serp(),
            _algorithm_data(), _intent_data(), None
        )
        self.assertLessEqual(len(result), 8)


# ===========================================================================
# 6. Structural Improvements
# ===========================================================================


class TestStructuralImprovements(unittest.TestCase):

    def test_always_has_competitive_monitoring(self):
        result = _extract_structural_improvements(None, {}, None)
        types = [a["type"] for a in result]
        self.assertIn("competitive_monitoring", types)

    def test_orphan_pages_detected(self):
        result = _extract_structural_improvements(
            _architecture_data(), _minimal_health(), None
        )
        types = [a["type"] for a in result]
        self.assertIn("internal_linking", types)

    def test_hub_opportunities_detected(self):
        result = _extract_structural_improvements(
            _architecture_data(), _minimal_health(), None
        )
        types = [a["type"] for a in result]
        self.assertIn("hub_development", types)

    def test_seasonal_calendar_with_cycle(self):
        health = {
            "seasonality": {
                "monthly_cycle": True,
                "best_day": "Tuesday",
                "cycle_description": "Weekly peaks on Tuesdays",
            }
        }
        result = _extract_structural_improvements(None, health, None)
        types = [a["type"] for a in result]
        self.assertIn("seasonal_calendar", types)

    def test_no_seasonal_without_cycle(self):
        result = _extract_structural_improvements(
            None, _minimal_health(), None
        )
        types = [a["type"] for a in result]
        self.assertNotIn("seasonal_calendar", types)

    def test_brand_monitoring_when_branded_provided(self):
        result = _extract_structural_improvements(
            None, _minimal_health(), _branded_data()
        )
        types = [a["type"] for a in result]
        self.assertIn("brand_monitoring", types)

    def test_no_brand_monitoring_without_branded(self):
        result = _extract_structural_improvements(
            None, _minimal_health(), None
        )
        types = [a["type"] for a in result]
        self.assertNotIn("brand_monitoring", types)


# ===========================================================================
# 7. Recovery & Growth Potential Calculations
# ===========================================================================


class TestPotentialCalculations(unittest.TestCase):

    def test_recovery_from_critical(self):
        critical = [{"impact": 100}, {"impact": 200}]
        total = _calculate_recovery_potential(critical, [], {})
        self.assertEqual(total, 300)

    def test_recovery_includes_ctr_and_refresh_quick_wins(self):
        quick_wins = [
            {"type": "ctr_optimization", "impact": 50},
            {"type": "content_refresh", "impact": 30},
            {"type": "striking_distance", "impact": 999},
        ]
        total = _calculate_recovery_potential([], quick_wins, {})
        self.assertEqual(total, 80)  # only ctr + refresh, not striking_distance

    def test_growth_from_striking_distance(self):
        quick_wins = [
            {"type": "striking_distance", "impact": 200},
            {"type": "ctr_optimization", "impact": 100},
        ]
        total = _calculate_growth_potential(quick_wins, [], {}, {})
        self.assertEqual(total, 200)  # only striking_distance

    def test_growth_from_strategic(self):
        strategic = [{"impact": 500}, {"impact": "ongoing"}]
        total = _calculate_growth_potential([], strategic, {}, {})
        self.assertEqual(total, 500)  # only int impacts

    def test_zero_when_empty(self):
        self.assertEqual(_calculate_recovery_potential([], [], {}), 0)
        self.assertEqual(_calculate_growth_potential([], [], {}, {}), 0)


# ===========================================================================
# 8. Fallback Narrative
# ===========================================================================


class TestFallbackNarrative(unittest.TestCase):

    def test_contains_health_summary(self):
        data = {
            "health_summary": "Strong growth trajectory at +5.0% per month",
            "critical_count": 3,
            "quick_wins_count": 10,
            "strategic_count": 4,
            "total_recovery": 2000,
            "total_growth": 5000,
            "top_critical": [{"type": "critical_page_rescue", "impact": 500}],
            "top_quick_wins": [],
            "branded_ratio": None,
            "revenue_at_risk": None,
            "algorithm_impacts": [],
        }
        narrative = _generate_fallback_narrative(data)
        self.assertIn("Strong growth trajectory", narrative)

    def test_mentions_critical_count(self):
        data = {
            "health_summary": "Declining",
            "critical_count": 5,
            "quick_wins_count": 8,
            "strategic_count": 2,
            "total_recovery": 1000,
            "total_growth": 3000,
            "top_critical": [{"type": "ctr_optimization", "impact": 200}],
            "top_quick_wins": [],
            "branded_ratio": None,
            "revenue_at_risk": None,
            "algorithm_impacts": [],
        }
        narrative = _generate_fallback_narrative(data)
        self.assertIn("5 critical issues", narrative)

    def test_no_critical_text(self):
        data = {
            "health_summary": "Stable",
            "critical_count": 0,
            "quick_wins_count": 5,
            "strategic_count": 1,
            "total_recovery": 0,
            "total_growth": 100,
            "top_critical": [],
            "top_quick_wins": [],
            "branded_ratio": None,
            "revenue_at_risk": None,
            "algorithm_impacts": [],
        }
        narrative = _generate_fallback_narrative(data)
        self.assertIn("No critical issues", narrative)

    def test_algorithm_impact_mentioned(self):
        data = {
            "health_summary": "Declining",
            "critical_count": 2,
            "quick_wins_count": 5,
            "strategic_count": 2,
            "total_recovery": 500,
            "total_growth": 1000,
            "top_critical": [],
            "top_quick_wins": [],
            "branded_ratio": None,
            "revenue_at_risk": None,
            "algorithm_impacts": [{"name": "Core Update"}],
        }
        narrative = _generate_fallback_narrative(data)
        self.assertIn("algorithm", narrative.lower())

    def test_total_upside_mentioned(self):
        data = {
            "health_summary": "Growing",
            "critical_count": 1,
            "quick_wins_count": 3,
            "strategic_count": 2,
            "total_recovery": 2000,
            "total_growth": 3000,
            "top_critical": [{"type": "ctr", "impact": 100}],
            "top_quick_wins": [],
            "branded_ratio": None,
            "revenue_at_risk": None,
            "algorithm_impacts": [],
        }
        narrative = _generate_fallback_narrative(data)
        self.assertIn("5,000", narrative)

    @patch.dict("os.environ", {}, clear=True)
    def test_narrative_with_fallback_no_api_key(self):
        """Without ANTHROPIC_API_KEY, should fall back to template."""
        data = {
            "health_summary": "Stable",
            "critical_count": 0,
            "quick_wins_count": 2,
            "strategic_count": 1,
            "total_recovery": 100,
            "total_growth": 200,
            "top_critical": [],
            "top_quick_wins": [],
            "branded_ratio": None,
            "revenue_at_risk": None,
            "algorithm_impacts": [],
        }
        narrative = _generate_narrative_with_fallback(data)
        self.assertIsInstance(narrative, str)
        self.assertGreater(len(narrative), 0)


# ===========================================================================
# 9. Helper / Instruction Functions
# ===========================================================================


class TestHealthSummary(unittest.TestCase):

    def test_strong_growth(self):
        s = _summarize_health({"overall_direction": "strong_growth", "trend_slope_pct_per_month": 8.0})
        self.assertIn("Strong growth", s)

    def test_growth(self):
        s = _summarize_health({"overall_direction": "growth", "trend_slope_pct_per_month": 3.0})
        self.assertIn("Growing", s)

    def test_flat(self):
        s = _summarize_health({"overall_direction": "flat", "trend_slope_pct_per_month": 0.2})
        self.assertIn("Stable", s)

    def test_decline(self):
        s = _summarize_health({"overall_direction": "decline", "trend_slope_pct_per_month": -4.0})
        self.assertIn("Declining", s)

    def test_strong_decline(self):
        s = _summarize_health({"overall_direction": "strong_decline", "trend_slope_pct_per_month": -15.0})
        self.assertIn("Significant decline", s)

    def test_unknown(self):
        s = _summarize_health({"overall_direction": "unknown"})
        self.assertIn("unclear", s)

    def test_empty_dict(self):
        s = _summarize_health({})
        self.assertIsInstance(s, str)


class TestInstructionGenerators(unittest.TestCase):

    def test_critical_page_ctr_anomaly(self):
        page = {"ctr_anomaly": True, "engagement_flag": "ok", "trend_slope": -0.1}
        instructions = _generate_critical_page_instructions(page)
        self.assertIn("title tag", instructions.lower())

    def test_critical_page_low_engagement(self):
        page = {"engagement_flag": "low_engagement", "trend_slope": -0.1}
        instructions = _generate_critical_page_instructions(page)
        self.assertIn("content quality", instructions.lower())

    def test_critical_page_steep_decline(self):
        page = {"trend_slope": -0.8}
        instructions = _generate_critical_page_instructions(page)
        self.assertIn("fresh content", instructions.lower())

    def test_critical_page_default(self):
        page = {}
        instructions = _generate_critical_page_instructions(page)
        self.assertIn("audit", instructions.lower())

    def test_cannibalization_consolidate(self):
        cluster = {"recommendation": "consolidate", "keep_page": "/main", "pages": ["/main", "/dup"]}
        instructions = _generate_cannibalization_instructions(cluster)
        self.assertIn("Consolidate", instructions)
        self.assertIn("/main", instructions)

    def test_cannibalization_differentiate(self):
        cluster = {"recommendation": "differentiate", "pages": ["/a", "/b"]}
        instructions = _generate_cannibalization_instructions(cluster)
        self.assertIn("Differentiate", instructions)

    def test_cannibalization_canonical(self):
        cluster = {"recommendation": "canonical", "keep_page": "/main", "pages": ["/main", "/dup"]}
        instructions = _generate_cannibalization_instructions(cluster)
        self.assertIn("canonical", instructions.lower())

    def test_striking_distance_commercial(self):
        opp = {"intent": "commercial", "current_position": 12}
        instructions = _generate_striking_distance_instructions(opp)
        self.assertIn("comparison", instructions.lower())

    def test_striking_distance_informational(self):
        opp = {"intent": "informational", "current_position": 12}
        instructions = _generate_striking_distance_instructions(opp)
        self.assertIn("how-to", instructions.lower())

    def test_striking_distance_high_position(self):
        opp = {"intent": "informational", "current_position": 18}
        instructions = _generate_striking_distance_instructions(opp)
        self.assertIn("internal links", instructions.lower())

    def test_serp_feature_featured_snippet(self):
        data = {"features_above": ["featured_snippet"]}
        instructions = _generate_serp_feature_instructions(data)
        self.assertIn("featured snippet", instructions.lower())

    def test_serp_feature_paa(self):
        data = {"features_above": ["people_also_ask"]}
        instructions = _generate_serp_feature_instructions(data)
        self.assertIn("FAQ", instructions)

    def test_serp_feature_video(self):
        data = {"features_above": ["video"]}
        instructions = _generate_serp_feature_instructions(data)
        self.assertIn("video", instructions.lower())

    def test_serp_feature_local(self):
        data = {"features_above": ["local_pack"]}
        instructions = _generate_serp_feature_instructions(data)
        self.assertIn("local", instructions.lower())

    def test_serp_feature_empty(self):
        data = {"features_above": []}
        instructions = _generate_serp_feature_instructions(data)
        self.assertIn("Optimize", instructions)

    def test_consolidation_instructions(self):
        clusters = [
            {"pages": ["/a", "/b"]},
            {"pages": ["/c", "/d", "/e"]},
        ]
        instructions = _generate_consolidation_instructions(clusters)
        self.assertIn("2 query clusters", instructions)
        self.assertIn("5 pages", instructions)

    def test_algorithm_recovery_thin_content(self):
        updates = [{"common_characteristics": ["thin_content"]}]
        instructions = _generate_algorithm_recovery_instructions(updates)
        self.assertIn("content depth", instructions.lower())

    def test_algorithm_recovery_no_schema(self):
        updates = [{"common_characteristics": ["no_schema"]}]
        instructions = _generate_algorithm_recovery_instructions(updates)
        self.assertIn("schema", instructions.lower())

    def test_algorithm_recovery_generic(self):
        updates = [{"common_characteristics": ["other_issue"]}]
        instructions = _generate_algorithm_recovery_instructions(updates)
        self.assertIn("E-E-A-T", instructions)

    def test_algorithm_recovery_empty_characteristics(self):
        updates = [{}]
        instructions = _generate_algorithm_recovery_instructions(updates)
        self.assertIsInstance(instructions, str)


# ===========================================================================
# 10. Full Pipeline Integration
# ===========================================================================


class TestFullPipeline(unittest.TestCase):
    """Test with realistic multi-module inputs."""

    def setUp(self):
        self.result = generate_gameplan(
            _minimal_health(),
            _critical_triage(),
            _serp_with_displacement(),
            _content_with_cannibalization(),
            algorithm=_algorithm_data(),
            intent=_intent_data(),
            architecture=_architecture_data(),
            branded=_branded_data(),
            revenue=_revenue_data(),
        )

    def test_has_critical_actions(self):
        self.assertGreater(len(self.result["critical"]), 0)

    def test_has_quick_wins(self):
        self.assertGreater(len(self.result["quick_wins"]), 0)

    def test_has_strategic_actions(self):
        self.assertGreater(len(self.result["strategic"]), 0)

    def test_has_structural_improvements(self):
        self.assertGreater(len(self.result["structural"]), 0)

    def test_recovery_is_positive(self):
        self.assertGreater(self.result["total_estimated_monthly_click_recovery"], 0)

    def test_growth_is_positive(self):
        self.assertGreater(self.result["total_estimated_monthly_click_growth"], 0)

    def test_narrative_is_multi_paragraph(self):
        self.assertGreater(len(self.result["narrative"].split("\n")), 1)

    def test_critical_action_fields(self):
        for action in self.result["critical"]:
            self.assertIn("type", action)
            self.assertIn("action", action)
            self.assertIn("impact", action)

    def test_quick_win_action_fields(self):
        for action in self.result["quick_wins"]:
            self.assertIn("type", action)
            self.assertIn("action", action)
            self.assertIn("impact", action)
            self.assertIn("priority_score", action)


# ===========================================================================
# 11. Edge Cases
# ===========================================================================


class TestEdgeCases(unittest.TestCase):

    def test_triage_pages_missing_keys(self):
        """Pages missing optional keys should not crash."""
        triage = {"pages": [{"url": "/test", "bucket": "critical", "current_monthly_clicks": 200}]}
        result = _extract_critical_fixes({}, triage, _minimal_content())
        self.assertIsInstance(result, list)

    def test_zero_impressions_cannibalization(self):
        content = {
            "cannibalization_clusters": [
                {"pages": ["/a", "/b"], "total_impressions_affected": 0, "shared_queries": 0}
            ],
            "striking_distance": [],
            "thin_content_pages": [],
            "update_priority_matrix": {},
        }
        result = _extract_critical_fixes({}, _minimal_triage(), content)
        # Should not include zero-impression clusters
        cannibal_actions = [a for a in result if a["type"] == "cannibalization_fix"]
        self.assertEqual(len(cannibal_actions), 0)

    def test_negative_ctr_impact(self):
        """Negative values should not produce negative impacts."""
        serp = {
            "serp_feature_displacement": [
                {"keyword": "kw", "estimated_ctr_impact": 0.05, "impressions": 1000, "features_above": []}
            ]
        }
        result = _extract_quick_wins(_minimal_triage(), serp, _minimal_content(), None)
        for action in result:
            self.assertGreaterEqual(action.get("impact", 0), 0)

    def test_all_modules_empty_dicts(self):
        result = generate_gameplan({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})
        self.assertIsInstance(result, dict)
        self.assertEqual(result["critical"], [])
        self.assertEqual(result["quick_wins"], [])

    def test_very_large_numbers(self):
        triage = {
            "pages": [
                {
                    "url": "/huge",
                    "bucket": "critical",
                    "current_monthly_clicks": 10_000_000,
                    "trend_slope": -1.0,
                }
            ]
        }
        result = _extract_critical_fixes({}, triage, _minimal_content())
        self.assertGreater(len(result), 0)
        self.assertIsInstance(result[0]["impact"], int)

    def test_special_characters_in_urls(self):
        triage = {
            "pages": [
                {
                    "url": "/blog/page?q=hello%20world&lang=en#section",
                    "bucket": "critical",
                    "current_monthly_clicks": 200,
                }
            ]
        }
        result = _extract_critical_fixes({}, triage, _minimal_content())
        self.assertIsInstance(result, list)


if __name__ == "__main__":
    unittest.main()
