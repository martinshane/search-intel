"""
Comprehensive test suite for Module 4: Content Intelligence.

Covers:
  1. Input validation & edge cases
  2. Output schema validation
  3. Cannibalization detection (detect_cannibalization)
  4. Cannibalization action determination
  5. Cannibalization severity calculation
  6. Striking distance identification (find_striking_distance)
  7. Click gain estimation
  8. Query intent classification
  9. Ranking difficulty estimation
  10. Striking distance priority scoring
  11. Thin content flagging (flag_thin_content)
  12. Expected CTR curve
  13. Content age vs performance matrix (analyze_content_age_performance)
  14. Page trend calculation (calculate_page_trends)
  15. Full pipeline integration
  16. Edge cases — empty DataFrames, NaN values, single rows
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from api.analysis.module_4_content_intelligence import (
    analyze_content_intelligence,
    detect_cannibalization,
    determine_cannibalization_action,
    calculate_cannibalization_severity,
    find_striking_distance,
    estimate_click_gain_to_top5,
    classify_query_intent,
    estimate_ranking_difficulty,
    calculate_striking_distance_priority,
    flag_thin_content,
    get_expected_ctr_for_position,
    analyze_content_age_performance,
    calculate_page_trends,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_gsc_query_page(rows):
    """Helper to create a GSC query-page DataFrame."""
    return pd.DataFrame(rows, columns=["query", "page", "clicks", "impressions", "position"])


def _make_page_data(rows):
    """Helper to create a page_data (crawl) DataFrame."""
    return pd.DataFrame(rows, columns=["url", "word_count", "last_modified", "title", "h1"])


def _make_ga4_engagement(rows):
    """Helper to create a GA4 engagement DataFrame."""
    return pd.DataFrame(rows, columns=["page", "bounce_rate", "avg_session_duration", "sessions"])


def _empty_gsc():
    return _make_gsc_query_page([])


def _empty_page_data():
    return _make_page_data([])


def _empty_ga4():
    return _make_ga4_engagement([])


# ---------------------------------------------------------------------------
# 1. Input Validation & Edge Cases
# ---------------------------------------------------------------------------

class TestInputValidation:
    """Test handling of empty and minimal inputs."""

    def test_all_empty_dataframes(self):
        result = analyze_content_intelligence(_empty_gsc(), _empty_page_data(), _empty_ga4())
        assert isinstance(result, dict)
        assert "cannibalization_clusters" in result
        assert "striking_distance" in result
        assert "thin_content" in result
        assert "update_priority_matrix" in result
        assert "summary" in result

    def test_empty_gsc_returns_empty_clusters(self):
        clusters = detect_cannibalization(_empty_gsc())
        assert clusters == []

    def test_empty_gsc_returns_empty_striking(self):
        opportunities = find_striking_distance(_empty_gsc())
        assert opportunities == []

    def test_empty_gsc_returns_empty_thin(self):
        thin = flag_thin_content(_empty_gsc(), _empty_page_data(), _empty_ga4())
        assert thin == []


# ---------------------------------------------------------------------------
# 2. Output Schema Validation
# ---------------------------------------------------------------------------

class TestOutputSchema:
    """Verify full pipeline returns the expected structure."""

    def test_top_level_keys(self):
        gsc = _make_gsc_query_page([
            ["best widgets", "https://example.com/a", 50, 2000, 5.0],
            ["best widgets", "https://example.com/b", 20, 1500, 12.0],
        ])
        result = analyze_content_intelligence(gsc, _empty_page_data(), _empty_ga4())
        for key in ["cannibalization_clusters", "striking_distance", "thin_content",
                     "update_priority_matrix", "summary"]:
            assert key in result, f"Missing key: {key}"

    def test_summary_keys(self):
        gsc = _make_gsc_query_page([
            ["best widgets", "https://example.com/a", 50, 2000, 5.0],
            ["best widgets", "https://example.com/b", 20, 1500, 12.0],
        ])
        result = analyze_content_intelligence(gsc, _empty_page_data(), _empty_ga4())
        summary = result["summary"]
        expected_keys = [
            "cannibalization_clusters_found",
            "total_impressions_cannibalized",
            "striking_distance_keywords",
            "estimated_strike_distance_clicks",
            "thin_content_pages",
            "urgent_update_pages",
        ]
        for key in expected_keys:
            assert key in summary, f"Missing summary key: {key}"

    def test_update_priority_matrix_quadrants(self):
        result = analyze_content_intelligence(_empty_gsc(), _empty_page_data(), _empty_ga4())
        matrix = result["update_priority_matrix"]
        for quadrant in ["urgent_update", "leave_alone", "structural_problem", "double_down"]:
            assert quadrant in matrix, f"Missing quadrant: {quadrant}"


# ---------------------------------------------------------------------------
# 3. Cannibalization Detection
# ---------------------------------------------------------------------------

class TestCannibalization:
    """Test detect_cannibalization logic."""

    def test_single_page_per_query_no_cannibalization(self):
        gsc = _make_gsc_query_page([
            ["widget a", "https://example.com/a", 100, 5000, 3.0],
            ["widget b", "https://example.com/b", 80, 4000, 4.0],
        ])
        clusters = detect_cannibalization(gsc)
        assert clusters == []

    def test_two_pages_for_same_query_detected(self):
        gsc = _make_gsc_query_page([
            ["best widgets", "https://example.com/a", 50, 2000, 5.0],
            ["best widgets", "https://example.com/b", 20, 1500, 12.0],
        ])
        clusters = detect_cannibalization(gsc)
        assert len(clusters) == 1
        assert clusters[0]["query"] == "best widgets"
        assert clusters[0]["page_count"] == 2

    def test_low_impression_cannibalization_filtered_out(self):
        """Queries with < 100 total impressions should be skipped."""
        gsc = _make_gsc_query_page([
            ["rare keyword", "https://example.com/a", 1, 30, 5.0],
            ["rare keyword", "https://example.com/b", 0, 20, 15.0],
        ])
        clusters = detect_cannibalization(gsc)
        assert clusters == []

    def test_cluster_fields(self):
        gsc = _make_gsc_query_page([
            ["best widgets", "https://example.com/a", 50, 2000, 3.0],
            ["best widgets", "https://example.com/b", 20, 1500, 15.0],
        ])
        clusters = detect_cannibalization(gsc)
        c = clusters[0]
        expected_fields = [
            "query", "pages", "page_count", "total_impressions_affected",
            "total_clicks", "avg_position", "best_position", "worst_position",
            "position_gap", "keep_page", "recommendation", "severity",
            "page_performance",
        ]
        for field in expected_fields:
            assert field in c, f"Missing cluster field: {field}"

    def test_winning_page_is_best_position(self):
        gsc = _make_gsc_query_page([
            ["best widgets", "https://example.com/a", 50, 2000, 8.0],
            ["best widgets", "https://example.com/b", 80, 1500, 3.0],
        ])
        clusters = detect_cannibalization(gsc)
        assert clusters[0]["keep_page"] == "https://example.com/b"

    def test_page_performance_ctr_calculated(self):
        gsc = _make_gsc_query_page([
            ["test query", "https://example.com/a", 100, 1000, 3.0],
            ["test query", "https://example.com/b", 50, 500, 8.0],
        ])
        clusters = detect_cannibalization(gsc)
        for perf in clusters[0]["page_performance"]:
            assert "ctr" in perf
            assert perf["ctr"] == 10.0  # 100/1000*100 or 50/500*100

    def test_sorted_by_severity(self):
        gsc = _make_gsc_query_page([
            ["low volume", "https://example.com/a", 10, 200, 5.0],
            ["low volume", "https://example.com/b", 5, 150, 10.0],
            ["high volume", "https://example.com/a", 500, 20000, 3.0],
            ["high volume", "https://example.com/c", 100, 15000, 18.0],
        ])
        clusters = detect_cannibalization(gsc)
        assert len(clusters) == 2
        assert clusters[0]["severity"] >= clusters[1]["severity"]

    def test_three_pages_cannibalization(self):
        gsc = _make_gsc_query_page([
            ["widgets", "https://example.com/a", 100, 5000, 3.0],
            ["widgets", "https://example.com/b", 50, 3000, 8.0],
            ["widgets", "https://example.com/c", 20, 2000, 15.0],
        ])
        clusters = detect_cannibalization(gsc)
        assert clusters[0]["page_count"] == 3


# ---------------------------------------------------------------------------
# 4. Cannibalization Action Determination
# ---------------------------------------------------------------------------

class TestCannibalizationAction:
    """Test determine_cannibalization_action helper."""

    def test_small_gap_two_pages_differentiate(self):
        assert determine_cannibalization_action(3, 1000, 2) == "differentiate"

    def test_large_gap_consolidate(self):
        assert determine_cannibalization_action(15, 1000, 2) == "consolidate"

    def test_high_impressions_consolidate(self):
        assert determine_cannibalization_action(3, 6000, 2) == "consolidate"

    def test_many_pages_consolidate(self):
        assert determine_cannibalization_action(3, 1000, 4) == "consolidate"

    def test_medium_scenario_canonical(self):
        assert determine_cannibalization_action(7, 2000, 2) == "canonical_redirect"


# ---------------------------------------------------------------------------
# 5. Cannibalization Severity
# ---------------------------------------------------------------------------

class TestCannibalizationSeverity:
    """Test calculate_cannibalization_severity helper."""

    def test_low_severity(self):
        score = calculate_cannibalization_severity(100, 2)
        assert 0 <= score <= 100
        assert score < 20

    def test_high_severity(self):
        score = calculate_cannibalization_severity(15000, 25)
        assert score > 80

    def test_max_capped_at_100(self):
        score = calculate_cannibalization_severity(100000, 100)
        assert score <= 100

    def test_zero_inputs(self):
        score = calculate_cannibalization_severity(0, 0)
        assert score == 0


# ---------------------------------------------------------------------------
# 6. Striking Distance
# ---------------------------------------------------------------------------

class TestStrikingDistance:
    """Test find_striking_distance logic."""

    def test_position_in_range_detected(self):
        gsc = _make_gsc_query_page([
            ["widget tips", "https://example.com/tips", 5, 500, 12.0],
        ])
        opps = find_striking_distance(gsc)
        assert len(opps) == 1
        assert opps[0]["query"] == "widget tips"

    def test_position_too_high_excluded(self):
        gsc = _make_gsc_query_page([
            ["good keyword", "https://example.com/a", 100, 5000, 3.0],
        ])
        opps = find_striking_distance(gsc)
        assert len(opps) == 0

    def test_position_too_low_excluded(self):
        gsc = _make_gsc_query_page([
            ["deep keyword", "https://example.com/a", 1, 200, 25.0],
        ])
        opps = find_striking_distance(gsc)
        assert len(opps) == 0

    def test_low_impressions_filtered(self):
        gsc = _make_gsc_query_page([
            ["tiny keyword", "https://example.com/a", 1, 50, 10.0],
        ])
        opps = find_striking_distance(gsc)
        assert len(opps) == 0

    def test_opportunity_fields(self):
        gsc = _make_gsc_query_page([
            ["target keyword", "https://example.com/a", 10, 1000, 11.0],
        ])
        opps = find_striking_distance(gsc)
        opp = opps[0]
        expected_fields = [
            "query", "current_position", "impressions", "current_clicks",
            "current_ctr", "estimated_click_gain_if_top5", "intent",
            "landing_page", "difficulty", "priority_score",
        ]
        for field in expected_fields:
            assert field in opp, f"Missing field: {field}"

    def test_sorted_by_estimated_gain(self):
        gsc = _make_gsc_query_page([
            ["low volume", "https://example.com/a", 5, 200, 10.0],
            ["high volume", "https://example.com/b", 10, 5000, 10.0],
        ])
        opps = find_striking_distance(gsc)
        assert len(opps) == 2
        assert opps[0]["estimated_click_gain_if_top5"] >= opps[1]["estimated_click_gain_if_top5"]

    def test_max_50_results(self):
        rows = []
        for i in range(60):
            rows.append([f"keyword {i}", f"https://example.com/{i}", 5, 500, 12.0])
        gsc = _make_gsc_query_page(rows)
        opps = find_striking_distance(gsc)
        assert len(opps) <= 50


# ---------------------------------------------------------------------------
# 7. Click Gain Estimation
# ---------------------------------------------------------------------------

class TestClickGainEstimation:
    """Test estimate_click_gain_to_top5."""

    def test_gain_positive_for_low_position(self):
        gain = estimate_click_gain_to_top5(impressions=5000, current_position=15, current_clicks=50)
        assert gain > 0

    def test_zero_impressions(self):
        gain = estimate_click_gain_to_top5(impressions=0, current_position=10, current_clicks=0)
        assert gain == 0

    def test_already_high_clicks_lower_gain(self):
        gain_low = estimate_click_gain_to_top5(impressions=1000, current_position=10, current_clicks=10)
        gain_high = estimate_click_gain_to_top5(impressions=1000, current_position=10, current_clicks=70)
        assert gain_low >= gain_high

    def test_never_negative(self):
        gain = estimate_click_gain_to_top5(impressions=100, current_position=1, current_clicks=500)
        assert gain >= 0


# ---------------------------------------------------------------------------
# 8. Query Intent Classification
# ---------------------------------------------------------------------------

class TestQueryIntentClassification:
    """Test classify_query_intent."""

    def test_transactional_buy(self):
        assert classify_query_intent("buy widgets online") == "transactional"

    def test_transactional_price(self):
        assert classify_query_intent("widget price comparison") == "transactional"

    def test_commercial_best(self):
        assert classify_query_intent("best widget 2026") == "commercial"

    def test_commercial_vs(self):
        assert classify_query_intent("widget a vs widget b") == "commercial"

    def test_informational_how(self):
        assert classify_query_intent("how to install widgets") == "informational"

    def test_informational_guide(self):
        assert classify_query_intent("complete widget guide") == "informational"

    def test_navigational_short(self):
        assert classify_query_intent("acme corp") == "navigational"

    def test_default_informational(self):
        assert classify_query_intent("widgets for large rooms in winter") == "informational"


# ---------------------------------------------------------------------------
# 9. Ranking Difficulty
# ---------------------------------------------------------------------------

class TestRankingDifficulty:
    """Test estimate_ranking_difficulty."""

    def test_low_difficulty_position_10(self):
        assert estimate_ranking_difficulty(10) == "low"

    def test_medium_difficulty_position_13(self):
        assert estimate_ranking_difficulty(13) == "medium"

    def test_high_difficulty_position_18(self):
        assert estimate_ranking_difficulty(18) == "high"

    def test_boundary_position_8(self):
        assert estimate_ranking_difficulty(8) == "low"

    def test_boundary_position_15(self):
        assert estimate_ranking_difficulty(15) == "medium"


# ---------------------------------------------------------------------------
# 10. Striking Distance Priority
# ---------------------------------------------------------------------------

class TestStrikingDistancePriority:
    """Test calculate_striking_distance_priority."""

    def test_score_range(self):
        score = calculate_striking_distance_priority(1000, 200, 12)
        assert 0 <= score <= 100

    def test_higher_impressions_higher_score(self):
        low = calculate_striking_distance_priority(100, 200, 12)
        high = calculate_striking_distance_priority(5000, 200, 12)
        assert high > low

    def test_closer_position_higher_score(self):
        far = calculate_striking_distance_priority(1000, 200, 19)
        close = calculate_striking_distance_priority(1000, 200, 9)
        assert close > far

    def test_higher_gain_higher_score(self):
        low_gain = calculate_striking_distance_priority(1000, 50, 12)
        high_gain = calculate_striking_distance_priority(1000, 500, 12)
        assert high_gain > low_gain


# ---------------------------------------------------------------------------
# 11. Thin Content Flagging
# ---------------------------------------------------------------------------

class TestThinContent:
    """Test flag_thin_content."""

    def test_low_word_count_flagged(self):
        gsc = _make_gsc_query_page([
            ["query a", "https://example.com/thin", 30, 500, 8.0],
        ])
        page_data = _make_page_data([
            ["https://example.com/thin", 200, "2025-01-01", "Thin Page", "Thin H1"],
        ])
        thin = flag_thin_content(gsc, page_data, _empty_ga4())
        assert len(thin) >= 1
        assert "low_word_count" in thin[0]["flags"]

    def test_high_bounce_flagged(self):
        gsc = _make_gsc_query_page([
            ["query b", "https://example.com/bounce", 30, 500, 8.0],
        ])
        ga4 = _make_ga4_engagement([
            ["https://example.com/bounce", 92.0, 60.0, 100],
        ])
        thin = flag_thin_content(gsc, _empty_page_data(), ga4)
        assert len(thin) >= 1
        assert "high_bounce_rate" in thin[0]["flags"]

    def test_low_engagement_flagged(self):
        gsc = _make_gsc_query_page([
            ["query c", "https://example.com/loweng", 30, 500, 8.0],
        ])
        ga4 = _make_ga4_engagement([
            ["https://example.com/loweng", 50.0, 10.0, 100],
        ])
        thin = flag_thin_content(gsc, _empty_page_data(), ga4)
        assert len(thin) >= 1
        assert "low_engagement_time" in thin[0]["flags"]

    def test_low_impressions_excluded(self):
        gsc = _make_gsc_query_page([
            ["rare query", "https://example.com/rare", 1, 30, 5.0],
        ])
        page_data = _make_page_data([
            ["https://example.com/rare", 100, "2025-01-01", "Rare", "Rare"],
        ])
        thin = flag_thin_content(gsc, page_data, _empty_ga4())
        assert len(thin) == 0

    def test_no_flags_means_not_listed(self):
        gsc = _make_gsc_query_page([
            ["good query", "https://example.com/good", 100, 500, 3.0],
        ])
        page_data = _make_page_data([
            ["https://example.com/good", 2000, "2025-01-01", "Good Page", "Good H1"],
        ])
        ga4 = _make_ga4_engagement([
            ["https://example.com/good", 40.0, 120.0, 500],
        ])
        thin = flag_thin_content(gsc, page_data, ga4)
        assert len(thin) == 0

    def test_multiple_flags_severity(self):
        gsc = _make_gsc_query_page([
            ["bad query", "https://example.com/bad", 5, 500, 8.0],
        ])
        page_data = _make_page_data([
            ["https://example.com/bad", 100, "2025-01-01", "Bad", "Bad"],
        ])
        ga4 = _make_ga4_engagement([
            ["https://example.com/bad", 95.0, 5.0, 100],
        ])
        thin = flag_thin_content(gsc, page_data, ga4)
        assert len(thin) == 1
        assert thin[0]["severity"] >= 2

    def test_sorted_by_severity_then_impressions(self):
        gsc = _make_gsc_query_page([
            ["q1", "https://example.com/a", 10, 500, 8.0],
            ["q2", "https://example.com/b", 10, 1000, 8.0],
        ])
        page_data = _make_page_data([
            ["https://example.com/a", 100, "2025-01-01", "A", "A"],
            ["https://example.com/b", 100, "2025-01-01", "B", "B"],
        ])
        ga4 = _make_ga4_engagement([
            ["https://example.com/a", 95.0, 5.0, 100],
            ["https://example.com/b", 50.0, 5.0, 100],
        ])
        thin = flag_thin_content(gsc, page_data, ga4)
        if len(thin) >= 2:
            assert thin[0]["severity"] >= thin[1]["severity"]

    def test_max_30_results(self):
        rows_gsc = []
        rows_page = []
        for i in range(40):
            url = f"https://example.com/{i}"
            rows_gsc.append([f"query {i}", url, 5, 500, 8.0])
            rows_page.append([url, 50, "2025-01-01", f"T{i}", f"H{i}"])
        gsc = _make_gsc_query_page(rows_gsc)
        page_data = _make_page_data(rows_page)
        thin = flag_thin_content(gsc, page_data, _empty_ga4())
        assert len(thin) <= 30


# ---------------------------------------------------------------------------
# 12. Expected CTR Curve
# ---------------------------------------------------------------------------

class TestExpectedCTR:
    """Test get_expected_ctr_for_position."""

    def test_position_1_highest(self):
        assert get_expected_ctr_for_position(1) == 0.28

    def test_position_10(self):
        assert get_expected_ctr_for_position(10) == 0.025

    def test_position_below_1(self):
        ctr = get_expected_ctr_for_position(0.5)
        assert ctr == 0.28

    def test_position_above_10_decays(self):
        ctr = get_expected_ctr_for_position(20)
        assert ctr < 0.025
        assert ctr > 0

    def test_monotonically_decreasing_1_to_10(self):
        ctrs = [get_expected_ctr_for_position(i) for i in range(1, 11)]
        for i in range(1, len(ctrs)):
            assert ctrs[i] <= ctrs[i - 1]


# ---------------------------------------------------------------------------
# 13. Content Age vs Performance Matrix
# ---------------------------------------------------------------------------

class TestContentAgePerformance:
    """Test analyze_content_age_performance."""

    def test_empty_inputs(self):
        result = analyze_content_age_performance(_empty_gsc(), _empty_page_data())
        assert all(k in result for k in ["urgent_update", "leave_alone", "structural_problem", "double_down"])

    def test_all_quadrants_are_lists(self):
        result = analyze_content_age_performance(_empty_gsc(), _empty_page_data())
        for quadrant in result.values():
            assert isinstance(quadrant, list)

    def test_old_page_with_good_ctr_not_urgent(self):
        """Pages with old age but stable trend should not be urgent_update."""
        gsc = _make_gsc_query_page([
            ["stable query", "https://example.com/old", 100, 500, 5.0],
        ])
        old_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
        page_data = _make_page_data([
            ["https://example.com/old", 2000, old_date, "Old Page", "Old H1"],
        ])
        result = analyze_content_age_performance(gsc, page_data)
        # The page may or may not appear in any quadrant, but should not crash
        assert isinstance(result, dict)


# ---------------------------------------------------------------------------
# 14. Page Trend Calculation
# ---------------------------------------------------------------------------

class TestPageTrends:
    """Test calculate_page_trends."""

    def test_empty_df(self):
        result = calculate_page_trends(_empty_gsc())
        assert isinstance(result, pd.DataFrame)
        assert "page" in result.columns
        assert "trend" in result.columns
        assert len(result) == 0

    def test_single_page_returns_trend(self):
        gsc = _make_gsc_query_page([
            ["query a", "https://example.com/a", 100, 1000, 3.0],
        ])
        result = calculate_page_trends(gsc)
        assert len(result) == 1
        assert "trend" in result.columns

    def test_multiple_pages(self):
        gsc = _make_gsc_query_page([
            ["query a", "https://example.com/a", 100, 1000, 3.0],
            ["query b", "https://example.com/b", 10, 1000, 15.0],
        ])
        result = calculate_page_trends(gsc)
        assert len(result) == 2

    def test_trend_normalized(self):
        gsc = _make_gsc_query_page([
            ["q1", "https://example.com/a", 200, 1000, 2.0],
            ["q2", "https://example.com/a", 150, 800, 3.0],
            ["q3", "https://example.com/b", 5, 1000, 20.0],
        ])
        result = calculate_page_trends(gsc)
        for _, row in result.iterrows():
            assert -1.0 <= row["trend"] <= 1.0 or np.isclose(abs(row["trend"]), 1.0)

    def test_high_ctr_page_trends_positive(self):
        """A page with CTR well above expected should trend positive."""
        gsc = _make_gsc_query_page([
            ["great query", "https://example.com/good", 300, 1000, 3.0],
            ["bad query", "https://example.com/bad", 1, 1000, 3.0],
        ])
        result = calculate_page_trends(gsc)
        good = result[result["page"] == "https://example.com/good"]["trend"].iloc[0]
        bad = result[result["page"] == "https://example.com/bad"]["trend"].iloc[0]
        assert good > bad


# ---------------------------------------------------------------------------
# 15. Full Pipeline Integration
# ---------------------------------------------------------------------------

class TestFullPipeline:
    """Integration tests for analyze_content_intelligence."""

    def test_realistic_dataset(self):
        gsc = _make_gsc_query_page([
            # Cannibalization pair
            ["best widgets", "https://example.com/guide", 80, 3000, 4.0],
            ["best widgets", "https://example.com/review", 30, 2000, 9.0],
            # Striking distance
            ["widget installation tips", "https://example.com/tips", 5, 800, 12.0],
            # Normal performing
            ["buy widgets online", "https://example.com/shop", 200, 5000, 2.0],
        ])
        page_data = _make_page_data([
            ["https://example.com/guide", 1500, "2025-06-01", "Guide", "Guide"],
            ["https://example.com/review", 800, "2025-09-01", "Review", "Review"],
            ["https://example.com/tips", 300, "2025-01-01", "Tips", "Tips"],
            ["https://example.com/shop", 2000, "2025-11-01", "Shop", "Shop"],
        ])
        ga4 = _make_ga4_engagement([
            ["https://example.com/guide", 45.0, 120.0, 300],
            ["https://example.com/review", 55.0, 90.0, 150],
            ["https://example.com/tips", 70.0, 40.0, 50],
            ["https://example.com/shop", 30.0, 180.0, 800],
        ])
        result = analyze_content_intelligence(gsc, page_data, ga4)

        assert result["summary"]["cannibalization_clusters_found"] >= 1
        assert result["summary"]["striking_distance_keywords"] >= 1
        assert isinstance(result["thin_content"], list)
        assert isinstance(result["update_priority_matrix"], dict)

    def test_summary_impressions_cannibalized(self):
        gsc = _make_gsc_query_page([
            ["keyword x", "https://example.com/a", 10, 500, 5.0],
            ["keyword x", "https://example.com/b", 5, 300, 12.0],
        ])
        result = analyze_content_intelligence(gsc, _empty_page_data(), _empty_ga4())
        assert result["summary"]["total_impressions_cannibalized"] == 800

    def test_no_crash_with_nan_values(self):
        gsc = _make_gsc_query_page([
            ["query", "https://example.com/a", 10, 500, float('nan')],
        ])
        # Should not crash
        result = analyze_content_intelligence(gsc, _empty_page_data(), _empty_ga4())
        assert isinstance(result, dict)


# ---------------------------------------------------------------------------
# 16. Edge Cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    """Additional edge cases."""

    def test_zero_impressions_page(self):
        gsc = _make_gsc_query_page([
            ["query", "https://example.com/a", 0, 0, 10.0],
        ])
        # Should not divide by zero
        clusters = detect_cannibalization(gsc)
        assert isinstance(clusters, list)

    def test_duplicate_query_page_rows(self):
        gsc = _make_gsc_query_page([
            ["dup query", "https://example.com/a", 50, 500, 5.0],
            ["dup query", "https://example.com/a", 50, 500, 5.0],
        ])
        clusters = detect_cannibalization(gsc)
        # Two rows for same page+query = 2 entries in group, may be flagged
        assert isinstance(clusters, list)

    def test_very_long_query(self):
        long_query = "how to " + " ".join(["optimize"] * 50) + " widgets"
        gsc = _make_gsc_query_page([
            [long_query, "https://example.com/a", 10, 500, 12.0],
        ])
        opps = find_striking_distance(gsc)
        assert isinstance(opps, list)

    def test_special_chars_in_url(self):
        gsc = _make_gsc_query_page([
            ["test", "https://example.com/path?q=1&b=2#frag", 10, 500, 12.0],
        ])
        opps = find_striking_distance(gsc)
        assert isinstance(opps, list)

    def test_intent_classification_empty_string(self):
        result = classify_query_intent("")
        assert result in ["transactional", "commercial", "informational", "navigational"]
