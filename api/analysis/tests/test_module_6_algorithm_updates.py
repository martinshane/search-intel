"""
Comprehensive test suite for Module 6: Algorithm Update Impact Analysis.

Tests cover:
1. Input validation — empty/None inputs, dict-to-DataFrame conversion
2. Output schema — all required top-level keys present
3. AlgorithmUpdate / ImpactAssessment dataclasses
4. _find_matching_update — window matching, no match, exact match
5. _assess_update_impact — pre/post windows, impact direction, edge cases
6. _find_affected_pages — no page data, multiple pages, top_n cap, sorting
7. _identify_common_characteristics — thin content, schema, backlinks, freshness, content type
8. _assess_recovery_status — ongoing, recovered, partial, not_recovered, unknown
9. _calculate_vulnerability — no impacts, all negative, mixed, factors
10. _generate_recommendation — no impacts, no negatives, various characteristics
11. _build_update_timeline — empty data, within/outside window
12. _build_summary — no impacts, mixed positive/negative, not_recovered
13. _impact_to_dict — serialization correctness
14. analyze_algorithm_impacts (public entry) — full pipeline, dict input, empty input
15. Edge cases — NaN values, single day data, zero clicks/impressions
"""

import unittest
from datetime import datetime, timedelta
from unittest.mock import patch
from typing import Dict, Any, List

import pandas as pd
import numpy as np

# We test via direct import of the module contents
import sys, os, importlib, types

# Build a minimal package structure so the module can be imported
# We parse and exec the module code directly to avoid needing the full package
MODULE_CODE_PATH = None  # Will be loaded from string

# ---------------------------------------------------------------------------
# Direct import simulation: parse module code into a testable namespace
# ---------------------------------------------------------------------------

_MODULE_SOURCE = open(os.path.join(os.path.dirname(__file__), "module_6_source.py")).read() if os.path.exists(os.path.join(os.path.dirname(__file__), "module_6_source.py")) else None

# Instead, define everything inline by importing from the actual module text.
# For CI we rely on the repo structure; for this standalone test we'll
# re-implement a lightweight import.

# We'll use exec-based import to avoid package dependency issues:

def _load_module_from_source(source_code: str, module_name: str = "module_6"):
    """Load module from source code string."""
    mod = types.ModuleType(module_name)
    mod.__file__ = "<test>"
    exec(compile(source_code, "<test>", "exec"), mod.__dict__)
    return mod


# The actual source is fetched from the repo; for the test file we
# import directly assuming the package is on sys.path.
# This test is designed to work when run from the repo root:
#   python -m pytest api/analysis/tests/test_module_6_algorithm_updates.py

try:
    from api.analysis.module_6_algorithm_updates import (
        AlgorithmUpdate,
        ImpactAssessment,
        AlgorithmImpactAnalyzer,
        KNOWN_ALGORITHM_UPDATES,
        analyze_algorithm_impacts,
    )
except ImportError:
    # Fallback: try relative import (when run as part of the package)
    try:
        from ..module_6_algorithm_updates import (
            AlgorithmUpdate,
            ImpactAssessment,
            AlgorithmImpactAnalyzer,
            KNOWN_ALGORITHM_UPDATES,
            analyze_algorithm_impacts,
        )
    except (ImportError, ValueError):
        raise ImportError(
            "Cannot import module_6_algorithm_updates. "
            "Run tests from the repo root: python -m pytest api/analysis/tests/"
        )


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

def _make_daily_data(
    start_date: datetime,
    days: int = 60,
    base_clicks: float = 100,
    base_impressions: float = 1000,
    base_position: float = 15.0,
    click_trend: float = 0.0,
) -> pd.DataFrame:
    """Generate synthetic daily aggregate data."""
    dates = [start_date + timedelta(days=i) for i in range(days)]
    np.random.seed(42)
    clicks = [max(0, base_clicks + click_trend * i + np.random.normal(0, 5)) for i in range(days)]
    impressions = [max(0, base_impressions + np.random.normal(0, 50)) for i in range(days)]
    positions = [max(1, base_position + np.random.normal(0, 0.5)) for i in range(days)]
    return pd.DataFrame({
        "date": dates,
        "clicks": clicks,
        "impressions": impressions,
        "position": positions,
    })


def _make_page_daily_data(
    start_date: datetime,
    days: int = 60,
    pages: list = None,
) -> pd.DataFrame:
    """Generate synthetic per-page daily data."""
    if pages is None:
        pages = ["/page-a", "/page-b", "/page-c"]
    rows = []
    np.random.seed(42)
    for page in pages:
        for i in range(days):
            rows.append({
                "date": start_date + timedelta(days=i),
                "page": page,
                "clicks": max(0, 20 + np.random.normal(0, 3)),
                "impressions": max(0, 200 + np.random.normal(0, 20)),
                "position": max(1, 10 + np.random.normal(0, 1)),
            })
    return pd.DataFrame(rows)


def _make_page_metadata(pages: list = None) -> pd.DataFrame:
    """Generate synthetic page metadata."""
    if pages is None:
        pages = ["/page-a", "/page-b", "/page-c"]
    return pd.DataFrame({
        "page": pages,
        "word_count": [300, 1500, 800],
        "has_schema": [False, True, False],
        "content_type": ["blog", "blog", "product"],
        "backlink_count": [2, 50, 3],
        "last_modified": [
            (datetime.now() - timedelta(days=400)).isoformat(),
            (datetime.now() - timedelta(days=30)).isoformat(),
            (datetime.now() - timedelta(days=200)).isoformat(),
        ],
    })


def _make_analyzer(updates=None) -> AlgorithmImpactAnalyzer:
    """Create an analyzer with known or default updates."""
    if updates is None:
        updates = KNOWN_ALGORITHM_UPDATES
    return AlgorithmImpactAnalyzer(updates)


# ---------------------------------------------------------------------------
# 1. Input validation
# ---------------------------------------------------------------------------

class TestInputValidation(unittest.TestCase):
    """Test input handling for analyze_algorithm_impacts."""

    def test_none_daily_data(self):
        result = analyze_algorithm_impacts(None)
        self.assertIn("summary", result)
        self.assertEqual(result["updates_impacting_site"], [])
        self.assertEqual(result["vulnerability_score"], 0.0)

    def test_empty_dataframe(self):
        result = analyze_algorithm_impacts(pd.DataFrame())
        self.assertEqual(result["updates_impacting_site"], [])
        self.assertIn("Insufficient", result["summary"])

    def test_dict_input_conversion(self):
        """Dict input should be converted to DataFrame."""
        data = {
            "date": [datetime(2025, 6, 1) + timedelta(days=i) for i in range(30)],
            "clicks": [100] * 30,
            "impressions": [1000] * 30,
            "position": [10.0] * 30,
        }
        result = analyze_algorithm_impacts(data)
        self.assertIn("summary", result)
        # Should not raise

    def test_empty_change_points(self):
        """Empty change points should not crash."""
        daily = _make_daily_data(datetime(2025, 1, 1), days=60)
        result = analyze_algorithm_impacts(daily, change_points_from_module1=[])
        self.assertIsInstance(result, dict)

    def test_none_change_points(self):
        daily = _make_daily_data(datetime(2025, 1, 1), days=60)
        result = analyze_algorithm_impacts(daily, change_points_from_module1=None)
        self.assertIsInstance(result, dict)


# ---------------------------------------------------------------------------
# 2. Output schema
# ---------------------------------------------------------------------------

class TestOutputSchema(unittest.TestCase):
    """Ensure output dict has all required keys."""

    def setUp(self):
        self.daily = _make_daily_data(datetime(2025, 1, 1), days=120)
        self.result = analyze_algorithm_impacts(self.daily)

    def test_top_level_keys(self):
        required = {
            "summary", "updates_impacting_site", "vulnerability_score",
            "vulnerability_factors", "recommendation", "unexplained_changes",
            "total_updates_in_period", "updates_with_site_impact", "update_timeline",
        }
        self.assertTrue(required.issubset(set(self.result.keys())))

    def test_vulnerability_score_range(self):
        self.assertGreaterEqual(self.result["vulnerability_score"], 0.0)
        self.assertLessEqual(self.result["vulnerability_score"], 1.0)

    def test_lists_are_lists(self):
        self.assertIsInstance(self.result["updates_impacting_site"], list)
        self.assertIsInstance(self.result["vulnerability_factors"], list)
        self.assertIsInstance(self.result["unexplained_changes"], list)
        self.assertIsInstance(self.result["update_timeline"], list)

    def test_summary_is_string(self):
        self.assertIsInstance(self.result["summary"], str)
        self.assertTrue(len(self.result["summary"]) > 0)

    def test_counts_are_ints(self):
        self.assertIsInstance(self.result["total_updates_in_period"], int)
        self.assertIsInstance(self.result["updates_with_site_impact"], int)


# ---------------------------------------------------------------------------
# 3. Dataclasses
# ---------------------------------------------------------------------------

class TestDataclasses(unittest.TestCase):
    """Test AlgorithmUpdate and ImpactAssessment dataclasses."""

    def test_algorithm_update_creation(self):
        au = AlgorithmUpdate(
            date=datetime(2025, 3, 13),
            name="March 2025 Core Update",
            type="core",
            source="Google",
            description="A core update",
        )
        self.assertEqual(au.name, "March 2025 Core Update")
        self.assertEqual(au.type, "core")

    def test_algorithm_update_default_description(self):
        au = AlgorithmUpdate(
            date=datetime(2025, 1, 1), name="Test", type="spam", source="Google"
        )
        self.assertIsNone(au.description)

    def test_impact_assessment_creation(self):
        ia = ImpactAssessment(
            update_name="Test", update_date=datetime(2025, 1, 1),
            update_type="core", site_impact="negative",
            click_change_pct=-15.0, impression_change_pct=-10.0,
            position_change_avg=2.5, pages_most_affected=[],
            common_characteristics=["thin_content"],
            recovery_status="not_recovered", days_since_update=90,
        )
        self.assertEqual(ia.site_impact, "negative")
        self.assertEqual(ia.click_change_pct, -15.0)


# ---------------------------------------------------------------------------
# 4. _find_matching_update
# ---------------------------------------------------------------------------

class TestFindMatchingUpdate(unittest.TestCase):
    """Test change-point-to-update matching."""

    def setUp(self):
        self.updates = [
            AlgorithmUpdate(datetime(2025, 3, 13), "March 2025 Core", "core", "Google"),
            AlgorithmUpdate(datetime(2025, 6, 5), "June 2025 Spam", "spam", "Google"),
        ]
        self.analyzer = AlgorithmImpactAnalyzer(self.updates)

    def test_exact_match(self):
        result = self.analyzer._find_matching_update(datetime(2025, 3, 13))
        self.assertIsNotNone(result)
        self.assertEqual(result.name, "March 2025 Core")

    def test_within_window(self):
        result = self.analyzer._find_matching_update(datetime(2025, 3, 16))
        self.assertIsNotNone(result)
        self.assertEqual(result.name, "March 2025 Core")

    def test_outside_window(self):
        result = self.analyzer._find_matching_update(datetime(2025, 4, 1))
        self.assertIsNone(result)

    def test_boundary_7_days(self):
        result = self.analyzer._find_matching_update(datetime(2025, 3, 20))
        self.assertIsNotNone(result)

    def test_boundary_8_days(self):
        result = self.analyzer._find_matching_update(datetime(2025, 3, 21))
        self.assertIsNone(result)

    def test_custom_window(self):
        result = self.analyzer._find_matching_update(
            datetime(2025, 3, 25), window_days=14
        )
        self.assertIsNotNone(result)


# ---------------------------------------------------------------------------
# 5. _assess_update_impact
# ---------------------------------------------------------------------------

class TestAssessUpdateImpact(unittest.TestCase):
    """Test impact assessment for a single update."""

    def setUp(self):
        self.update = AlgorithmUpdate(
            datetime(2025, 6, 5), "June 2025 Spam", "spam", "Google"
        )
        self.analyzer = _make_analyzer([self.update])

    def test_positive_impact(self):
        """Post-update clicks much higher → positive."""
        dates = [datetime(2025, 5, 20) + timedelta(days=i) for i in range(40)]
        clicks = [50] * 16 + [150] * 24  # Jump at update date
        daily = pd.DataFrame({
            "date": dates, "clicks": clicks,
            "impressions": [1000] * 40, "position": [10.0] * 40,
        })
        impact = self.analyzer._assess_update_impact(
            self.update, {"date": "2025-06-05"}, daily, None, None
        )
        self.assertEqual(impact.site_impact, "positive")
        self.assertGreater(impact.click_change_pct, 5)

    def test_negative_impact(self):
        """Post-update clicks much lower → negative."""
        dates = [datetime(2025, 5, 20) + timedelta(days=i) for i in range(40)]
        clicks = [150] * 16 + [50] * 24
        daily = pd.DataFrame({
            "date": dates, "clicks": clicks,
            "impressions": [1000] * 40, "position": [10.0] * 40,
        })
        impact = self.analyzer._assess_update_impact(
            self.update, {"date": "2025-06-05"}, daily, None, None
        )
        self.assertEqual(impact.site_impact, "negative")
        self.assertLess(impact.click_change_pct, -5)

    def test_neutral_impact(self):
        """Small change → neutral."""
        dates = [datetime(2025, 5, 20) + timedelta(days=i) for i in range(40)]
        clicks = [100] * 40
        daily = pd.DataFrame({
            "date": dates, "clicks": clicks,
            "impressions": [1000] * 40, "position": [10.0] * 40,
        })
        impact = self.analyzer._assess_update_impact(
            self.update, {"date": "2025-06-05"}, daily, None, None
        )
        self.assertEqual(impact.site_impact, "neutral")

    def test_no_pre_data(self):
        """No data before update → zero changes."""
        dates = [datetime(2025, 6, 5) + timedelta(days=i) for i in range(20)]
        daily = pd.DataFrame({
            "date": dates, "clicks": [100] * 20,
            "impressions": [1000] * 20, "position": [10.0] * 20,
        })
        impact = self.analyzer._assess_update_impact(
            self.update, {"date": "2025-06-05"}, daily, None, None
        )
        self.assertEqual(impact.click_change_pct, 0.0)
        self.assertEqual(impact.impression_change_pct, 0.0)

    def test_impact_has_correct_fields(self):
        daily = _make_daily_data(datetime(2025, 5, 20), days=40)
        impact = self.analyzer._assess_update_impact(
            self.update, {"date": "2025-06-05"}, daily, None, None
        )
        self.assertIsInstance(impact, ImpactAssessment)
        self.assertEqual(impact.update_name, "June 2025 Spam")
        self.assertEqual(impact.update_type, "spam")
        self.assertIsInstance(impact.days_since_update, int)


# ---------------------------------------------------------------------------
# 6. _find_affected_pages
# ---------------------------------------------------------------------------

class TestFindAffectedPages(unittest.TestCase):
    """Test per-page impact detection."""

    def setUp(self):
        self.analyzer = _make_analyzer()
        self.update_date = datetime(2025, 6, 5)

    def test_no_page_data(self):
        result = self.analyzer._find_affected_pages(None, self.update_date)
        self.assertEqual(result, [])

    def test_empty_page_data(self):
        result = self.analyzer._find_affected_pages(pd.DataFrame(), self.update_date)
        self.assertEqual(result, [])

    def test_pages_detected(self):
        page_data = _make_page_daily_data(datetime(2025, 5, 20), days=40)
        result = self.analyzer._find_affected_pages(page_data, self.update_date)
        self.assertIsInstance(result, list)
        for item in result:
            self.assertIn("page", item)
            self.assertIn("click_change", item)
            self.assertIn("click_change_pct", item)

    def test_sorted_by_abs_click_change(self):
        page_data = _make_page_daily_data(datetime(2025, 5, 20), days=40)
        result = self.analyzer._find_affected_pages(page_data, self.update_date)
        if len(result) > 1:
            for i in range(len(result) - 1):
                self.assertGreaterEqual(
                    abs(result[i]["click_change"]),
                    abs(result[i + 1]["click_change"]),
                )

    def test_capped_at_top_n(self):
        pages = [f"/page-{i}" for i in range(20)]
        page_data = _make_page_daily_data(datetime(2025, 5, 20), days=40, pages=pages)
        result = self.analyzer._find_affected_pages(page_data, self.update_date, top_n=5)
        self.assertLessEqual(len(result), 5)

    def test_no_pre_or_post_data_for_page(self):
        """Pages with data only before or after update get skipped."""
        rows = []
        for i in range(14):
            rows.append({
                "date": datetime(2025, 5, 20) + timedelta(days=i),
                "page": "/only-pre",
                "clicks": 10, "impressions": 100, "position": 5.0,
            })
        page_data = pd.DataFrame(rows)
        result = self.analyzer._find_affected_pages(page_data, self.update_date)
        # /only-pre has no post data, should be empty or skipped
        page_names = [p["page"] for p in result]
        self.assertNotIn("/only-pre", page_names)


# ---------------------------------------------------------------------------
# 7. _identify_common_characteristics
# ---------------------------------------------------------------------------

class TestIdentifyCommonCharacteristics(unittest.TestCase):
    """Test identification of vulnerability patterns."""

    def setUp(self):
        self.analyzer = _make_analyzer()

    def test_no_metadata(self):
        result = self.analyzer._identify_common_characteristics(
            [{"page": "/a", "click_change": -10}], None
        )
        self.assertEqual(result, [])

    def test_empty_affected_pages(self):
        meta = _make_page_metadata()
        result = self.analyzer._identify_common_characteristics([], meta)
        self.assertEqual(result, [])

    def test_thin_content_detected(self):
        affected = [{"page": "/page-a", "click_change": -10}]
        meta = pd.DataFrame({
            "page": ["/page-a"],
            "word_count": [300],
            "has_schema": [False],
        })
        result = self.analyzer._identify_common_characteristics(affected, meta)
        self.assertIn("thin_content", result)

    def test_short_content_detected(self):
        affected = [{"page": "/page-a", "click_change": -10}]
        meta = pd.DataFrame({
            "page": ["/page-a"],
            "word_count": [700],
        })
        result = self.analyzer._identify_common_characteristics(affected, meta)
        self.assertIn("short_content", result)

    def test_no_schema_detected(self):
        affected = [{"page": "/p1", "click_change": -5}]
        meta = pd.DataFrame({
            "page": ["/p1"],
            "has_schema": [False],
        })
        result = self.analyzer._identify_common_characteristics(affected, meta)
        self.assertIn("no_schema", result)

    def test_low_backlinks_detected(self):
        affected = [{"page": "/p1", "click_change": -5}]
        meta = pd.DataFrame({
            "page": ["/p1"],
            "backlink_count": [2],
        })
        result = self.analyzer._identify_common_characteristics(affected, meta)
        self.assertIn("low_backlinks", result)

    def test_content_type_concentration(self):
        affected = [
            {"page": "/p1", "click_change": -5},
            {"page": "/p2", "click_change": -8},
            {"page": "/p3", "click_change": -3},
        ]
        meta = pd.DataFrame({
            "page": ["/p1", "/p2", "/p3"],
            "content_type": ["blog", "blog", "blog"],
        })
        result = self.analyzer._identify_common_characteristics(affected, meta)
        self.assertTrue(any("content_type" in c for c in result))

    def test_positive_pages_ignored(self):
        """Only negative click_change pages analyzed."""
        affected = [{"page": "/p1", "click_change": 50}]
        meta = pd.DataFrame({
            "page": ["/p1"],
            "word_count": [100],
            "has_schema": [False],
        })
        result = self.analyzer._identify_common_characteristics(affected, meta)
        self.assertEqual(result, [])

    def test_outdated_content_detected(self):
        affected = [{"page": "/old", "click_change": -10}]
        meta = pd.DataFrame({
            "page": ["/old"],
            "last_modified": [(datetime.now() - timedelta(days=500)).isoformat()],
        })
        result = self.analyzer._identify_common_characteristics(affected, meta)
        self.assertIn("outdated_content", result)

    def test_empty_metadata_dataframe(self):
        affected = [{"page": "/p1", "click_change": -5}]
        meta = pd.DataFrame()
        result = self.analyzer._identify_common_characteristics(affected, meta)
        self.assertEqual(result, [])


# ---------------------------------------------------------------------------
# 8. _assess_recovery_status
# ---------------------------------------------------------------------------

class TestAssessRecoveryStatus(unittest.TestCase):
    """Test recovery status assessment."""

    def setUp(self):
        self.analyzer = _make_analyzer()

    def test_ongoing_if_recent(self):
        """Updates < 30 days ago → ongoing."""
        daily = _make_daily_data(datetime.now() - timedelta(days=30), days=30)
        status = self.analyzer._assess_recovery_status(
            daily, datetime.now() - timedelta(days=10), -20.0
        )
        self.assertEqual(status, "ongoing")

    def test_unknown_if_no_recent_data(self):
        """No recent data → unknown."""
        daily = _make_daily_data(datetime(2024, 1, 1), days=30)
        status = self.analyzer._assess_recovery_status(
            daily, datetime(2024, 1, 15), -20.0
        )
        self.assertEqual(status, "unknown")

    def test_recovered_negative_impact(self):
        """If recent clicks are back to pre-update levels → recovered."""
        update_date = datetime.now() - timedelta(days=60)
        # Pre-update: 100 clicks; recent: ~100 clicks
        dates = []
        clicks = []
        for i in range(90):
            d = update_date - timedelta(days=14) + timedelta(days=i)
            dates.append(d)
            clicks.append(100)
        daily = pd.DataFrame({
            "date": dates, "clicks": clicks,
            "impressions": [1000] * 90, "position": [10.0] * 90,
        })
        status = self.analyzer._assess_recovery_status(daily, update_date, -20.0)
        self.assertEqual(status, "recovered")

    def test_not_recovered(self):
        """Recent clicks still far below pre-update → not_recovered."""
        update_date = datetime.now() - timedelta(days=60)
        dates = []
        clicks = []
        for i in range(90):
            d = update_date - timedelta(days=14) + timedelta(days=i)
            dates.append(d)
            if d < update_date:
                clicks.append(200)
            else:
                clicks.append(50)
        daily = pd.DataFrame({
            "date": dates, "clicks": clicks,
            "impressions": [1000] * 90, "position": [10.0] * 90,
        })
        status = self.analyzer._assess_recovery_status(daily, update_date, -75.0)
        self.assertEqual(status, "not_recovered")


# ---------------------------------------------------------------------------
# 9. _calculate_vulnerability
# ---------------------------------------------------------------------------

class TestCalculateVulnerability(unittest.TestCase):
    """Test vulnerability score calculation."""

    def setUp(self):
        self.analyzer = _make_analyzer()
        self.daily = _make_daily_data(datetime(2025, 1, 1), days=60)

    def test_no_impacts_zero_score(self):
        score, factors = self.analyzer._calculate_vulnerability([], self.daily)
        self.assertEqual(score, 0.0)
        self.assertEqual(factors, [])

    def test_all_negative_impacts(self):
        impacts = [
            ImpactAssessment(
                "Test", datetime(2025, 3, 13), "core", "negative",
                -25.0, -20.0, 3.0, [], ["thin_content"],
                "not_recovered", 90,
            ),
            ImpactAssessment(
                "Test2", datetime(2025, 6, 5), "spam", "negative",
                -30.0, -25.0, 4.0, [], ["thin_content"],
                "not_recovered", 60,
            ),
        ]
        score, factors = self.analyzer._calculate_vulnerability(impacts, self.daily)
        self.assertGreater(score, 0.5)
        self.assertIn("frequent_negative_impacts", factors)
        self.assertIn("severe_impact_history", factors)
        self.assertIn("poor_recovery_rate", factors)

    def test_mixed_impacts(self):
        impacts = [
            ImpactAssessment(
                "Neg", datetime(2025, 3, 13), "core", "negative",
                -10.0, -5.0, 1.0, [], [], "recovered", 90,
            ),
            ImpactAssessment(
                "Pos", datetime(2025, 6, 5), "core", "positive",
                15.0, 10.0, -1.0, [], [], "recovered", 60,
            ),
        ]
        score, factors = self.analyzer._calculate_vulnerability(impacts, self.daily)
        self.assertGreaterEqual(score, 0.0)
        self.assertLessEqual(score, 1.0)

    def test_high_volatility_factor(self):
        """High CV in daily clicks → traffic volatility factor."""
        volatile = self.daily.copy()
        np.random.seed(99)
        volatile["clicks"] = np.random.uniform(10, 500, len(volatile))
        impacts = [
            ImpactAssessment(
                "Neg", datetime(2025, 3, 13), "core", "negative",
                -10.0, -5.0, 1.0, [], [], "recovered", 90,
            ),
        ]
        score, factors = self.analyzer._calculate_vulnerability(impacts, volatile)
        self.assertIn("high_traffic_volatility", factors)

    def test_recurring_characteristics(self):
        impacts = [
            ImpactAssessment(
                "N1", datetime(2025, 3, 13), "core", "negative",
                -10.0, -5.0, 1.0, [], ["thin_content", "no_schema"],
                "recovered", 90,
            ),
            ImpactAssessment(
                "N2", datetime(2025, 6, 5), "spam", "negative",
                -15.0, -10.0, 2.0, [], ["thin_content"],
                "recovered", 60,
            ),
        ]
        score, factors = self.analyzer._calculate_vulnerability(impacts, self.daily)
        self.assertTrue(any("recurring_issues" in f for f in factors))


# ---------------------------------------------------------------------------
# 10. _generate_recommendation
# ---------------------------------------------------------------------------

class TestGenerateRecommendation(unittest.TestCase):
    """Test recommendation generation."""

    def setUp(self):
        self.analyzer = _make_analyzer()

    def test_no_impacts(self):
        rec = self.analyzer._generate_recommendation([], 0.0, [])
        self.assertIn("No significant", rec)

    def test_no_negative_impacts(self):
        impacts = [
            ImpactAssessment(
                "Pos", datetime(2025, 3, 13), "core", "positive",
                15.0, 10.0, -1.0, [], [], "recovered", 90,
            ),
        ]
        rec = self.analyzer._generate_recommendation(impacts, 0.1, [])
        self.assertIn("resilience", rec)

    def test_high_vulnerability(self):
        impacts = [
            ImpactAssessment(
                "Neg", datetime(2025, 3, 13), "core", "negative",
                -25.0, -20.0, 3.0, [], ["thin_content"],
                "not_recovered", 90,
            ),
        ]
        rec = self.analyzer._generate_recommendation(impacts, 0.8, ["severe_impact_history"])
        self.assertIn("HIGH VULNERABILITY", rec)

    def test_moderate_vulnerability(self):
        impacts = [
            ImpactAssessment(
                "Neg", datetime(2025, 3, 13), "core", "negative",
                -10.0, -5.0, 1.0, [], ["no_schema"],
                "partial_recovery", 90,
            ),
        ]
        rec = self.analyzer._generate_recommendation(impacts, 0.5, [])
        self.assertIn("MODERATE VULNERABILITY", rec)

    def test_thin_content_recommendation(self):
        impacts = [
            ImpactAssessment(
                "Neg", datetime(2025, 3, 13), "core", "negative",
                -20.0, -15.0, 2.0, [], ["thin_content"],
                "not_recovered", 90,
            ),
        ]
        rec = self.analyzer._generate_recommendation(impacts, 0.6, [])
        self.assertIn("content depth", rec)

    def test_no_schema_recommendation(self):
        impacts = [
            ImpactAssessment(
                "Neg", datetime(2025, 3, 13), "core", "negative",
                -20.0, -15.0, 2.0, [], ["no_schema"],
                "not_recovered", 90,
            ),
        ]
        rec = self.analyzer._generate_recommendation(impacts, 0.6, [])
        self.assertIn("structured data", rec)

    def test_low_backlinks_recommendation(self):
        impacts = [
            ImpactAssessment(
                "Neg", datetime(2025, 3, 13), "core", "negative",
                -20.0, -15.0, 2.0, [], ["low_backlinks"],
                "not_recovered", 90,
            ),
        ]
        rec = self.analyzer._generate_recommendation(impacts, 0.6, [])
        self.assertIn("authority", rec)

    def test_outdated_content_recommendation(self):
        impacts = [
            ImpactAssessment(
                "Neg", datetime(2025, 3, 13), "core", "negative",
                -20.0, -15.0, 2.0, [], ["outdated_content"],
                "not_recovered", 90,
            ),
        ]
        rec = self.analyzer._generate_recommendation(impacts, 0.6, [])
        self.assertIn("freshness", rec)

    def test_not_recovered_mention(self):
        impacts = [
            ImpactAssessment(
                "Neg", datetime(2025, 3, 13), "core", "negative",
                -20.0, -15.0, 2.0, [], [],
                "not_recovered", 90,
            ),
        ]
        rec = self.analyzer._generate_recommendation(impacts, 0.6, [])
        self.assertIn("not recovered", rec)


# ---------------------------------------------------------------------------
# 11. _build_update_timeline
# ---------------------------------------------------------------------------

class TestBuildUpdateTimeline(unittest.TestCase):
    """Test timeline generation."""

    def setUp(self):
        self.analyzer = _make_analyzer()

    def test_empty_data(self):
        result = self.analyzer._build_update_timeline(pd.DataFrame(columns=["date", "clicks"]))
        self.assertEqual(result, [])

    def test_updates_within_window(self):
        daily = _make_daily_data(datetime(2025, 1, 1), days=365)
        timeline = self.analyzer._build_update_timeline(daily)
        self.assertIsInstance(timeline, list)
        for entry in timeline:
            self.assertIn("date", entry)
            self.assertIn("name", entry)
            self.assertIn("type", entry)

    def test_sorted_descending(self):
        daily = _make_daily_data(datetime(2025, 1, 1), days=365)
        timeline = self.analyzer._build_update_timeline(daily)
        if len(timeline) > 1:
            for i in range(len(timeline) - 1):
                self.assertGreaterEqual(timeline[i]["date"], timeline[i + 1]["date"])

    def test_updates_outside_window_excluded(self):
        """Data only in 2025 Jan-Feb → no 2023 updates."""
        daily = _make_daily_data(datetime(2025, 1, 1), days=30)
        timeline = self.analyzer._build_update_timeline(daily)
        for entry in timeline:
            date = datetime.fromisoformat(entry["date"])
            self.assertGreaterEqual(date, datetime(2025, 1, 1))


# ---------------------------------------------------------------------------
# 12. _build_summary
# ---------------------------------------------------------------------------

class TestBuildSummary(unittest.TestCase):
    """Test summary string generation."""

    def setUp(self):
        self.analyzer = _make_analyzer()

    def test_no_impacts(self):
        summary = self.analyzer._build_summary([], 0.0)
        self.assertIn("No algorithm update impacts", summary)

    def test_mixed_impacts(self):
        impacts = [
            ImpactAssessment(
                "Neg", datetime(2025, 3, 13), "core", "negative",
                -20.0, -15.0, 2.0, [], [], "not_recovered", 90,
            ),
            ImpactAssessment(
                "Pos", datetime(2025, 6, 5), "core", "positive",
                15.0, 10.0, -1.0, [], [], "recovered", 60,
            ),
        ]
        summary = self.analyzer._build_summary(impacts, 0.5)
        self.assertIn("2 algorithm update(s)", summary)
        self.assertIn("negative", summary)
        self.assertIn("positive", summary)
        self.assertIn("moderate", summary)

    def test_not_recovered_mentioned(self):
        impacts = [
            ImpactAssessment(
                "Neg", datetime(2025, 3, 13), "core", "negative",
                -20.0, -15.0, 2.0, [], [], "not_recovered", 90,
            ),
        ]
        summary = self.analyzer._build_summary(impacts, 0.7)
        self.assertIn("not yet recovered", summary)
        self.assertIn("high", summary)

    def test_low_vulnerability_label(self):
        impacts = [
            ImpactAssessment(
                "Pos", datetime(2025, 3, 13), "core", "positive",
                10.0, 5.0, -0.5, [], [], "recovered", 90,
            ),
        ]
        summary = self.analyzer._build_summary(impacts, 0.2)
        self.assertIn("low", summary)


# ---------------------------------------------------------------------------
# 13. _impact_to_dict
# ---------------------------------------------------------------------------

class TestImpactToDict(unittest.TestCase):
    """Test serialization of ImpactAssessment."""

    def test_all_keys_present(self):
        analyzer = _make_analyzer()
        impact = ImpactAssessment(
            "Test", datetime(2025, 3, 13), "core", "negative",
            -15.0, -10.0, 2.5, [{"page": "/a"}], ["thin_content"],
            "not_recovered", 90,
        )
        d = analyzer._impact_to_dict(impact)
        expected_keys = {
            "update_name", "date", "update_type", "site_impact",
            "click_change_pct", "impression_change_pct", "position_change_avg",
            "pages_most_affected", "common_characteristics",
            "recovery_status", "days_since_update",
        }
        self.assertEqual(set(d.keys()), expected_keys)

    def test_date_is_iso_string(self):
        analyzer = _make_analyzer()
        impact = ImpactAssessment(
            "Test", datetime(2025, 3, 13), "core", "neutral",
            0.0, 0.0, 0.0, [], [], "ongoing", 10,
        )
        d = analyzer._impact_to_dict(impact)
        self.assertIsInstance(d["date"], str)
        datetime.fromisoformat(d["date"])  # Should not raise


# ---------------------------------------------------------------------------
# 14. Full pipeline (analyze_algorithm_impacts)
# ---------------------------------------------------------------------------

class TestFullPipeline(unittest.TestCase):
    """Test the public entry point end-to-end."""

    def test_with_change_points_matching_updates(self):
        """Change points near known updates should be matched."""
        daily = _make_daily_data(datetime(2025, 2, 1), days=120)
        change_points = [
            {"date": "2025-03-14"},  # 1 day after March 2025 Core Update
            {"date": "2025-06-06"},  # 1 day after June 2025 Spam Update
        ]
        result = analyze_algorithm_impacts(daily, change_points)
        self.assertGreater(len(result["updates_impacting_site"]), 0)

    def test_with_page_data(self):
        daily = _make_daily_data(datetime(2025, 2, 1), days=120)
        page_data = _make_page_daily_data(datetime(2025, 2, 1), days=120)
        page_meta = _make_page_metadata()
        result = analyze_algorithm_impacts(
            daily, page_daily_data=page_data, page_metadata=page_meta
        )
        self.assertIn("updates_impacting_site", result)

    def test_dict_page_data_conversion(self):
        """Dict page_daily_data and page_metadata should convert to DataFrame."""
        daily = _make_daily_data(datetime(2025, 2, 1), days=60)
        page_dict = {
            "date": [datetime(2025, 3, 1)] * 3,
            "page": ["/a", "/b", "/c"],
            "clicks": [10, 20, 30],
            "impressions": [100, 200, 300],
            "position": [5.0, 10.0, 15.0],
        }
        meta_dict = {
            "page": ["/a", "/b", "/c"],
            "word_count": [500, 1000, 1500],
        }
        result = analyze_algorithm_impacts(
            daily, page_daily_data=page_dict, page_metadata=meta_dict
        )
        self.assertIsInstance(result, dict)

    def test_unexplained_changes(self):
        """Change points not near any update go to unexplained_changes."""
        daily = _make_daily_data(datetime(2025, 2, 1), days=60)
        change_points = [
            {"date": "2025-02-15"},  # Not near any known update
        ]
        result = analyze_algorithm_impacts(daily, change_points)
        self.assertGreater(len(result["unexplained_changes"]), 0)


# ---------------------------------------------------------------------------
# 15. Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases(unittest.TestCase):
    """Edge case and robustness tests."""

    def test_single_day_data(self):
        daily = pd.DataFrame({
            "date": [datetime(2025, 3, 13)],
            "clicks": [100],
            "impressions": [1000],
            "position": [10.0],
        })
        result = analyze_algorithm_impacts(daily)
        self.assertIsInstance(result, dict)

    def test_zero_clicks_impressions(self):
        daily = _make_daily_data(datetime(2025, 1, 1), days=60, base_clicks=0, base_impressions=0)
        daily["clicks"] = 0
        daily["impressions"] = 0
        result = analyze_algorithm_impacts(daily)
        self.assertIsInstance(result, dict)

    def test_string_date_in_change_point(self):
        daily = _make_daily_data(datetime(2025, 2, 1), days=60)
        change_points = [{"date": "2025-03-14"}]
        result = analyze_algorithm_impacts(daily, change_points)
        self.assertIsInstance(result, dict)

    def test_timestamp_key_in_change_point(self):
        daily = _make_daily_data(datetime(2025, 2, 1), days=60)
        change_points = [{"timestamp": "2025-03-14"}]
        result = analyze_algorithm_impacts(daily, change_points)
        self.assertIsInstance(result, dict)

    def test_known_updates_sorted_descending(self):
        analyzer = _make_analyzer()
        dates = [u.date for u in analyzer.algorithm_updates]
        for i in range(len(dates) - 1):
            self.assertGreaterEqual(dates[i], dates[i + 1])

    def test_known_updates_nonempty(self):
        self.assertGreater(len(KNOWN_ALGORITHM_UPDATES), 10)

    def test_large_dataset(self):
        """2 years of data should still work."""
        daily = _make_daily_data(datetime(2024, 1, 1), days=730)
        result = analyze_algorithm_impacts(daily)
        self.assertIsInstance(result, dict)
        self.assertGreater(result["total_updates_in_period"], 0)


if __name__ == "__main__":
    unittest.main()
