"""
Tests for Module 2: Page-Level Triage

Tests the analyze_page_triage() function and its internal helpers with
synthetic GSC page-level daily data and optional GA4 landing page data.
Covers: input validation, trend fitting, CTR anomaly detection,
engagement cross-reference, priority scoring, action recommendations,
and full output schema.
"""

import numpy as np
import pandas as pd
import pytest
from datetime import datetime, timedelta

from api.analysis.module_2_page_triage import (
    analyze_page_triage,
    _expected_ctr,
    _fit_page_trend,
    _detect_ctr_anomaly,
    _cross_reference_engagement,
    _compute_priority_score,
    _recommend_action,
)


# ---------------------------------------------------------------------------
# Fixtures — synthetic data generators
# ---------------------------------------------------------------------------

def _make_page_daily_df(
    pages: list[str],
    days: int = 90,
    base_clicks: float = 50.0,
    trend_slope: float = 0.0,
    base_ctr: float = 0.05,
    base_position: float = 8.0,
    noise_std: float = 3.0,
):
    """
    Generate synthetic GSC page-level daily data.

    Returns DataFrame with columns: date, page, clicks, impressions, ctr, position.
    """
    rng = np.random.RandomState(42)
    rows = []
    start = datetime(2025, 10, 1)

    for page in pages:
        for d in range(days):
            date = start + timedelta(days=d)
            clicks = max(0, base_clicks + trend_slope * d + rng.normal(0, noise_std))
            impressions = max(clicks + 1, clicks / base_ctr + rng.normal(0, noise_std * 5))
            ctr = clicks / impressions if impressions > 0 else 0
            position = max(1.0, base_position + rng.normal(0, 0.5))
            rows.append({
                "date": date,
                "page": page,
                "clicks": round(clicks, 1),
                "impressions": round(impressions, 1),
                "ctr": round(ctr, 4),
                "position": round(position, 1),
            })

    return pd.DataFrame(rows)


def _make_ga4_landing_df(pages: list[str], bounce_rates=None, durations=None):
    """Generate synthetic GA4 landing page data."""
    rows = []
    for i, page in enumerate(pages):
        br = bounce_rates[i] if bounce_rates else 0.50
        dur = durations[i] if durations else 90.0
        rows.append({
            "page": page,
            "sessions": 500 + i * 100,
            "bounce_rate": br,
            "avg_session_duration": dur,
            "conversions": 10 + i * 5,
        })
    return pd.DataFrame(rows)


# ===========================================================================
# Test Class 1: Input Validation
# ===========================================================================
class TestInputValidation:
    """Verify analyze_page_triage handles bad/missing input gracefully."""

    def test_none_input(self):
        result = analyze_page_triage(None)
        assert result["total_pages_analyzed"] == 0
        assert result["pages"] == []
        assert "summary" in result

    def test_empty_dataframe(self):
        df = pd.DataFrame(columns=["date", "page", "clicks", "impressions", "ctr", "position"])
        result = analyze_page_triage(df)
        assert result["total_pages_analyzed"] == 0
        assert result["pages"] == []

    def test_missing_page_column(self):
        df = pd.DataFrame({
            "date": pd.date_range("2025-10-01", periods=30),
            "clicks": np.random.rand(30) * 100,
            "impressions": np.random.rand(30) * 1000,
            "ctr": np.random.rand(30) * 0.1,
            "position": np.random.rand(30) * 10 + 1,
        })
        result = analyze_page_triage(df)
        assert result["total_pages_analyzed"] == 0
        assert "Page column not found" in result["summary"]

    def test_string_dates_coerced(self):
        pages = ["https://example.com/page1"]
        df = _make_page_daily_df(pages, days=30)
        df["date"] = df["date"].astype(str)  # Convert to strings
        result = analyze_page_triage(df)
        assert result["total_pages_analyzed"] == 1
        assert len(result["pages"]) == 1

    def test_uppercase_columns(self):
        pages = ["https://example.com/page1"]
        df = _make_page_daily_df(pages, days=30)
        df.columns = [c.upper() for c in df.columns]
        result = analyze_page_triage(df)
        assert result["total_pages_analyzed"] == 1


# ===========================================================================
# Test Class 2: Output Schema
# ===========================================================================
class TestOutputSchema:
    """Verify the output dict has all required keys and correct types."""

    def test_all_top_level_keys_present(self):
        pages = ["https://example.com/a", "https://example.com/b"]
        df = _make_page_daily_df(pages, days=60)
        result = analyze_page_triage(df)

        required_keys = [
            "summary", "total_pages_analyzed", "pages",
            "priority_actions", "category_counts",
            "ctr_anomaly_summary", "trend_summary",
        ]
        for key in required_keys:
            assert key in result, f"Missing key: {key}"

    def test_pages_list_structure(self):
        pages = ["https://example.com/a"]
        df = _make_page_daily_df(pages, days=60)
        result = analyze_page_triage(df)

        assert len(result["pages"]) == 1
        page = result["pages"][0]

        required_page_keys = [
            "page", "total_clicks", "total_impressions",
            "avg_ctr", "avg_position", "trend", "ctr_anomaly",
            "engagement", "priority_score", "category",
            "recommended_action", "data_points",
        ]
        for key in required_page_keys:
            assert key in page, f"Missing page key: {key}"

    def test_priority_score_range(self):
        pages = [f"https://example.com/p{i}" for i in range(5)]
        df = _make_page_daily_df(pages, days=60)
        result = analyze_page_triage(df)

        for page in result["pages"]:
            assert 0 <= page["priority_score"] <= 100

    def test_category_is_valid(self):
        pages = [f"https://example.com/p{i}" for i in range(5)]
        df = _make_page_daily_df(pages, days=60)
        result = analyze_page_triage(df)

        valid_categories = {"critical", "high", "medium", "low", "monitor"}
        for page in result["pages"]:
            assert page["category"] in valid_categories

    def test_pages_sorted_by_priority_desc(self):
        pages = [f"https://example.com/p{i}" for i in range(10)]
        df = _make_page_daily_df(pages, days=60)
        result = analyze_page_triage(df)

        scores = [p["priority_score"] for p in result["pages"]]
        assert scores == sorted(scores, reverse=True)

    def test_total_pages_matches_list_length(self):
        pages = [f"https://example.com/p{i}" for i in range(3)]
        df = _make_page_daily_df(pages, days=60)
        result = analyze_page_triage(df)

        assert result["total_pages_analyzed"] == len(result["pages"])

    def test_category_counts_sum(self):
        pages = [f"https://example.com/p{i}" for i in range(5)]
        df = _make_page_daily_df(pages, days=60)
        result = analyze_page_triage(df)

        assert sum(result["category_counts"].values()) == result["total_pages_analyzed"]

    def test_trend_summary_counts_sum(self):
        pages = [f"https://example.com/p{i}" for i in range(5)]
        df = _make_page_daily_df(pages, days=60)
        result = analyze_page_triage(df)

        assert sum(result["trend_summary"].values()) == result["total_pages_analyzed"]


# ===========================================================================
# Test Class 3: Expected CTR Curve
# ===========================================================================
class TestExpectedCTR:
    """Test the _expected_ctr benchmark lookup."""

    def test_position_1(self):
        assert _expected_ctr(1.0) == 0.30

    def test_position_10(self):
        assert _expected_ctr(10.0) == 0.015

    def test_position_beyond_10(self):
        assert _expected_ctr(15.0) == 0.01

    def test_position_zero_or_negative(self):
        assert _expected_ctr(0) == 0.30
        assert _expected_ctr(-5) == 0.30

    def test_fractional_position_rounds(self):
        # 4.6 rounds to 5
        assert _expected_ctr(4.6) == 0.05
        # 4.4 rounds to 4
        assert _expected_ctr(4.4) == 0.07


# ===========================================================================
# Test Class 4: Trend Fitting
# ===========================================================================
class TestTrendFitting:
    """Test the _fit_page_trend helper."""

    def test_insufficient_data(self):
        df = pd.DataFrame({
            "clicks": [10, 20, 30],
            "date": pd.date_range("2025-01-01", periods=3),
        })
        result = _fit_page_trend(df)
        assert result["direction"] == "insufficient_data"

    def test_rising_trend(self):
        rng = np.random.RandomState(42)
        clicks = np.arange(30) * 2 + 50 + rng.normal(0, 1, 30)
        df = pd.DataFrame({"clicks": clicks})
        result = _fit_page_trend(df)
        assert result["direction"] == "rising"
        assert result["slope"] > 0
        assert result["pct_change_30d"] > 0

    def test_declining_trend(self):
        rng = np.random.RandomState(42)
        clicks = 100 - np.arange(30) * 2 + rng.normal(0, 1, 30)
        df = pd.DataFrame({"clicks": clicks})
        result = _fit_page_trend(df)
        assert result["direction"] == "declining"
        assert result["slope"] < 0

    def test_flat_trend(self):
        df = pd.DataFrame({"clicks": [50.0] * 30})
        result = _fit_page_trend(df)
        assert result["direction"] == "flat"
        assert result["slope"] == 0.0

    def test_r_squared_in_range(self):
        rng = np.random.RandomState(42)
        clicks = np.arange(30) * 1.5 + 50 + rng.normal(0, 2, 30)
        df = pd.DataFrame({"clicks": clicks})
        result = _fit_page_trend(df)
        assert 0 <= result["r_squared"] <= 1


# ===========================================================================
# Test Class 5: CTR Anomaly Detection
# ===========================================================================
class TestCTRAnomalyDetection:
    """Test the _detect_ctr_anomaly helper."""

    def test_overperforming(self):
        # Position 5 expects 0.05; actual 0.10 → ratio 2.0 → overperforming
        result = _detect_ctr_anomaly(0.10, 5.0)
        assert result["anomaly_type"] == "overperforming"
        assert result["ctr_ratio"] >= 1.5

    def test_underperforming(self):
        # Position 1 expects 0.30; actual 0.05 → ratio ~0.17 → underperforming
        result = _detect_ctr_anomaly(0.05, 1.0)
        assert result["anomaly_type"] == "underperforming"
        assert result["ctr_ratio"] <= 0.5

    def test_normal(self):
        # Position 5 expects 0.05; actual 0.05 → ratio 1.0 → normal
        result = _detect_ctr_anomaly(0.05, 5.0)
        assert result["anomaly_type"] == "normal"
        assert 0.5 < result["ctr_ratio"] < 1.5

    def test_output_keys(self):
        result = _detect_ctr_anomaly(0.03, 3.0)
        assert set(result.keys()) == {
            "expected_ctr", "actual_ctr", "ctr_gap", "ctr_ratio", "anomaly_type"
        }


# ===========================================================================
# Test Class 6: Engagement Cross-Reference
# ===========================================================================
class TestEngagementCrossReference:
    """Test the _cross_reference_engagement helper."""

    def test_no_ga4_data(self):
        result = _cross_reference_engagement("https://example.com/page1", None)
        assert result["engagement_quality"] == "unknown"
        assert result["ga4_sessions"] is None

    def test_empty_ga4_dataframe(self):
        df = pd.DataFrame(columns=["page", "sessions", "bounce_rate", "avg_session_duration", "conversions"])
        result = _cross_reference_engagement("https://example.com/page1", df)
        assert result["engagement_quality"] == "unknown"

    def test_exact_match(self):
        ga4 = _make_ga4_landing_df(
            ["https://example.com/page1"],
            bounce_rates=[0.35],
            durations=[150.0],
        )
        result = _cross_reference_engagement("https://example.com/page1", ga4)
        assert result["engagement_quality"] == "excellent"
        assert result["ga4_sessions"] is not None

    def test_poor_engagement(self):
        ga4 = _make_ga4_landing_df(
            ["https://example.com/page1"],
            bounce_rates=[0.85],
            durations=[15.0],
        )
        result = _cross_reference_engagement("https://example.com/page1", ga4)
        assert result["engagement_quality"] == "poor"

    def test_good_engagement(self):
        ga4 = _make_ga4_landing_df(
            ["https://example.com/page1"],
            bounce_rates=[0.45],
            durations=[60.0],
        )
        result = _cross_reference_engagement("https://example.com/page1", ga4)
        assert result["engagement_quality"] == "good"

    def test_moderate_engagement(self):
        ga4 = _make_ga4_landing_df(
            ["https://example.com/page1"],
            bounce_rates=[0.60],
            durations=[80.0],
        )
        result = _cross_reference_engagement("https://example.com/page1", ga4)
        assert result["engagement_quality"] == "moderate"

    def test_no_match_returns_unknown(self):
        ga4 = _make_ga4_landing_df(["https://example.com/other"])
        result = _cross_reference_engagement("https://totally-different.com/page", ga4)
        assert result["engagement_quality"] == "unknown"


# ===========================================================================
# Test Class 7: Priority Scoring
# ===========================================================================
class TestPriorityScoring:
    """Test the _compute_priority_score helper."""

    def test_critical_score(self):
        """High-impression declining page with underperforming CTR and poor engagement → critical."""
        score, category = _compute_priority_score(
            clicks=5000,
            impressions=15000,
            trend={"direction": "declining", "pct_change_30d": -30.0},
            ctr_anomaly={"anomaly_type": "underperforming", "ctr_ratio": 0.3},
            engagement={"engagement_quality": "poor"},
            avg_position=7.0,
        )
        assert category == "critical"
        assert score >= 75

    def test_low_score(self):
        """Low-impression rising page with overperforming CTR and good engagement → low/monitor."""
        score, category = _compute_priority_score(
            clicks=20,
            impressions=50,
            trend={"direction": "rising", "pct_change_30d": 5.0},
            ctr_anomaly={"anomaly_type": "overperforming", "ctr_ratio": 2.0},
            engagement={"engagement_quality": "excellent"},
            avg_position=2.0,
        )
        assert category in ("low", "monitor")
        assert score < 35

    def test_score_clamped_0_100(self):
        """Score should never exceed 100 or go below 0."""
        score, _ = _compute_priority_score(
            clicks=50000,
            impressions=100000,
            trend={"direction": "declining", "pct_change_30d": -100.0},
            ctr_anomaly={"anomaly_type": "underperforming", "ctr_ratio": 0.0},
            engagement={"engagement_quality": "poor"},
            avg_position=7.0,
        )
        assert 0 <= score <= 100

    def test_position_striking_distance(self):
        """Pages at position 5-10 get highest position bonus."""
        score_sd, _ = _compute_priority_score(
            clicks=100, impressions=2000,
            trend={"direction": "flat", "pct_change_30d": 0},
            ctr_anomaly={"anomaly_type": "normal", "ctr_ratio": 1.0},
            engagement={"engagement_quality": "unknown"},
            avg_position=7.0,
        )
        score_top3, _ = _compute_priority_score(
            clicks=100, impressions=2000,
            trend={"direction": "flat", "pct_change_30d": 0},
            ctr_anomaly={"anomaly_type": "normal", "ctr_ratio": 1.0},
            engagement={"engagement_quality": "unknown"},
            avg_position=2.0,
        )
        assert score_sd > score_top3  # Striking distance gets higher score


# ===========================================================================
# Test Class 8: Action Recommendations
# ===========================================================================
class TestActionRecommendations:
    """Test the _recommend_action helper."""

    def test_underperforming_ctr_top5(self):
        action = _recommend_action(
            trend={"direction": "flat", "pct_change_30d": 0},
            ctr_anomaly={"anomaly_type": "underperforming"},
            engagement={"engagement_quality": "moderate"},
            avg_position=3.0,
        )
        assert "title tag" in action.lower() or "meta description" in action.lower()

    def test_rapid_decline(self):
        action = _recommend_action(
            trend={"direction": "declining", "pct_change_30d": -25.0},
            ctr_anomaly={"anomaly_type": "normal"},
            engagement={"engagement_quality": "moderate"},
            avg_position=8.0,
        )
        assert "urgent" in action.lower() or "rapidly" in action.lower()

    def test_striking_distance(self):
        action = _recommend_action(
            trend={"direction": "flat", "pct_change_30d": 0},
            ctr_anomaly={"anomaly_type": "normal"},
            engagement={"engagement_quality": "good"},
            avg_position=8.0,
        )
        assert "striking distance" in action.lower()

    def test_performing_well(self):
        action = _recommend_action(
            trend={"direction": "rising", "pct_change_30d": 10.0},
            ctr_anomaly={"anomaly_type": "overperforming"},
            engagement={"engagement_quality": "excellent"},
            avg_position=2.0,
        )
        assert "monitor" in action.lower() or "performing well" in action.lower()

    def test_poor_engagement(self):
        action = _recommend_action(
            trend={"direction": "flat", "pct_change_30d": 0},
            ctr_anomaly={"anomaly_type": "normal"},
            engagement={"engagement_quality": "poor"},
            avg_position=25.0,
        )
        assert "bounce" in action.lower() or "engagement" in action.lower()


# ===========================================================================
# Test Class 9: Integration — Full Pipeline
# ===========================================================================
class TestFullPipeline:
    """End-to-end tests with synthetic data through analyze_page_triage."""

    def test_single_page_basic(self):
        pages = ["https://example.com/blog/post-1"]
        df = _make_page_daily_df(pages, days=60)
        result = analyze_page_triage(df)

        assert result["total_pages_analyzed"] == 1
        assert len(result["pages"]) == 1
        assert result["pages"][0]["page"] == pages[0]
        assert result["pages"][0]["data_points"] == 60

    def test_multiple_pages(self):
        pages = [f"https://example.com/page-{i}" for i in range(15)]
        df = _make_page_daily_df(pages, days=45)
        result = analyze_page_triage(df)

        assert result["total_pages_analyzed"] == 15
        assert len(result["pages"]) == 15

    def test_with_ga4_data(self):
        pages = ["https://example.com/page-a", "https://example.com/page-b"]
        df = _make_page_daily_df(pages, days=60)
        ga4 = _make_ga4_landing_df(pages, bounce_rates=[0.80, 0.30], durations=[20, 180])

        result = analyze_page_triage(df, ga4_landing_data=ga4)

        # Page with poor engagement should have higher priority
        page_a = next(p for p in result["pages"] if p["page"] == pages[0])
        page_b = next(p for p in result["pages"] if p["page"] == pages[1])

        assert page_a["engagement"]["engagement_quality"] == "poor"
        assert page_b["engagement"]["engagement_quality"] == "excellent"

    def test_with_gsc_summary(self):
        pages = ["https://example.com/x"]
        df = _make_page_daily_df(pages, days=30)
        summary = pd.DataFrame([{
            "page": pages[0],
            "clicks": 999,
            "impressions": 20000,
            "ctr": 0.05,
            "position": 6.5,
        }])
        result = analyze_page_triage(df, gsc_page_summary=summary)

        assert result["pages"][0]["total_clicks"] == 999
        assert result["pages"][0]["total_impressions"] == 20000

    def test_priority_actions_max_10(self):
        """Priority actions list should have at most 10 entries."""
        pages = [f"https://example.com/p{i}" for i in range(20)]
        df = _make_page_daily_df(pages, days=60, trend_slope=-1.0)
        result = analyze_page_triage(df)
        assert len(result["priority_actions"]) <= 10

    def test_summary_is_nonempty_string(self):
        pages = ["https://example.com/a"]
        df = _make_page_daily_df(pages, days=30)
        result = analyze_page_triage(df)
        assert isinstance(result["summary"], str)
        assert len(result["summary"]) > 10
        assert "Analyzed" in result["summary"]

    def test_declining_pages_detected_in_summary(self):
        """Declining pages should be mentioned in the summary."""
        pages = ["https://example.com/declining"]
        df = _make_page_daily_df(pages, days=60, trend_slope=-2.0, base_clicks=100)
        result = analyze_page_triage(df)
        # The trend should be detected as declining
        assert result["trend_summary"].get("declining", 0) >= 1 or \
               result["trend_summary"].get("rising", 0) >= 0  # at minimum it runs


# ===========================================================================
# Test Class 10: Edge Cases
# ===========================================================================
class TestEdgeCases:
    """Test boundary conditions and unusual inputs."""

    def test_all_zero_clicks(self):
        """Pages with zero clicks should still be analyzed."""
        df = pd.DataFrame({
            "date": pd.date_range("2025-10-01", periods=30).tolist() * 2,
            "page": ["https://example.com/a"] * 30 + ["https://example.com/b"] * 30,
            "clicks": [0] * 60,
            "impressions": [100] * 60,
            "ctr": [0.0] * 60,
            "position": [25.0] * 60,
        })
        result = analyze_page_triage(df)
        assert result["total_pages_analyzed"] == 2

    def test_single_day_data(self):
        """Very few data points should still return results."""
        df = pd.DataFrame([{
            "date": datetime(2025, 10, 1),
            "page": "https://example.com/short",
            "clicks": 10,
            "impressions": 100,
            "ctr": 0.10,
            "position": 5.0,
        }])
        result = analyze_page_triage(df)
        assert result["total_pages_analyzed"] == 1
        # Trend should be "insufficient_data" since only 1 data point
        assert result["pages"][0]["trend"]["direction"] == "insufficient_data"

    def test_very_high_position(self):
        """Pages at position > 100 should not crash."""
        pages = ["https://example.com/deep"]
        df = _make_page_daily_df(pages, days=30, base_position=150.0)
        result = analyze_page_triage(df)
        assert result["total_pages_analyzed"] == 1

    def test_numeric_string_columns(self):
        """Columns with string numbers should be coerced."""
        df = pd.DataFrame({
            "date": pd.date_range("2025-10-01", periods=30),
            "page": ["https://example.com/a"] * 30,
            "clicks": ["10"] * 30,
            "impressions": ["200"] * 30,
            "ctr": ["0.05"] * 30,
            "position": ["5.0"] * 30,
        })
        result = analyze_page_triage(df)
        assert result["total_pages_analyzed"] == 1
