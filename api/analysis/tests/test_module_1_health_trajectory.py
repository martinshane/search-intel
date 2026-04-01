"""
Tests for Module 1: Health & Trajectory Analysis

Tests the analyze_health_trajectory() function with synthetic GSC data.
Covers: input validation, trend detection, seasonality, change points,
anomalies, projections, health scoring, and summary generation.
"""

import numpy as np
import pandas as pd
import pytest
from datetime import datetime, timedelta

from api.analysis.module_1_health_trajectory import analyze_health_trajectory


# ---------------------------------------------------------------------------
# Fixtures — synthetic data generators
# ---------------------------------------------------------------------------

def _make_daily_df(days=90, base_clicks=100, trend_slope=0.0,
                   weekly_pattern=True, noise_std=5.0,
                   anomaly_days=None, step_change_day=None, step_magnitude=0):
    """
    Generate synthetic GSC daily data.
    
    Args:
        days: number of days of data
        base_clicks: starting daily clicks
        trend_slope: clicks added per day (positive = growing)
        weekly_pattern: if True, add Mon-Fri higher / Sat-Sun lower pattern
        noise_std: Gaussian noise standard deviation
        anomaly_days: list of day indices to inject extreme spikes
        step_change_day: day index where a level shift occurs
        step_magnitude: size of the level shift
    """
    dates = [datetime(2025, 1, 1) + timedelta(days=i) for i in range(days)]
    clicks = []
    
    for i in range(days):
        val = base_clicks + trend_slope * i
        
        # Weekly seasonality
        if weekly_pattern:
            dow = dates[i].weekday()
            if dow < 5:  # Mon-Fri
                val *= 1.15
            else:  # Sat-Sun
                val *= 0.65
        
        # Step change
        if step_change_day is not None and i >= step_change_day:
            val += step_magnitude
        
        # Noise
        val += np.random.normal(0, noise_std)
        val = max(0, val)
        clicks.append(val)
    
    # Inject anomalies
    if anomaly_days:
        for ad in anomaly_days:
            if 0 <= ad < days:
                clicks[ad] = clicks[ad] * 5  # 5x spike
    
    impressions = [c * np.random.uniform(15, 25) for c in clicks]
    ctrs = [c / imp if imp > 0 else 0 for c, imp in zip(clicks, impressions)]
    positions = [np.random.uniform(8, 25) for _ in range(days)]
    
    return pd.DataFrame({
        "date": dates,
        "clicks": clicks,
        "impressions": impressions,
        "ctr": ctrs,
        "position": positions,
    })


# ---------------------------------------------------------------------------
# Test: Input validation
# ---------------------------------------------------------------------------

class TestInputValidation:
    """Tests for edge cases and input handling."""

    def test_insufficient_data_returns_none_score(self):
        """Less than 14 days should return health_score=None."""
        df = _make_daily_df(days=10)
        result = analyze_health_trajectory(df)
        assert result["health_score"] is None
        assert "Insufficient data" in result["summary"]
        assert result["change_points"] == []
        assert result["anomalies"] == []

    def test_empty_dataframe(self):
        """Empty DataFrame should handle gracefully."""
        df = pd.DataFrame(columns=["date", "clicks", "impressions", "ctr", "position"])
        result = analyze_health_trajectory(df)
        assert result["health_score"] is None

    def test_missing_optional_columns(self):
        """Should handle missing impressions/ctr/position by filling zeros."""
        dates = [datetime(2025, 1, 1) + timedelta(days=i) for i in range(60)]
        df = pd.DataFrame({"date": dates, "clicks": np.random.randint(50, 150, 60)})
        result = analyze_health_trajectory(df)
        assert result["health_score"] is not None
        assert isinstance(result["health_score"], int)

    def test_string_date_column(self):
        """Date column as strings should be parsed correctly."""
        dates = [(datetime(2025, 1, 1) + timedelta(days=i)).strftime("%Y-%m-%d")
                 for i in range(60)]
        df = pd.DataFrame({
            "date": dates,
            "clicks": np.random.randint(50, 150, 60),
            "impressions": np.random.randint(1000, 3000, 60),
            "ctr": np.random.uniform(0.02, 0.08, 60),
            "position": np.random.uniform(10, 30, 60),
        })
        result = analyze_health_trajectory(df)
        assert result["health_score"] is not None

    def test_uppercase_columns(self):
        """Column names with different casing should be normalized."""
        df = _make_daily_df(days=60)
        df.columns = [c.upper() for c in df.columns]
        result = analyze_health_trajectory(df)
        assert result["health_score"] is not None

    def test_missing_date_column_raises(self):
        """DataFrame without a date column should handle the error."""
        df = pd.DataFrame({"clicks": [10, 20, 30], "impressions": [100, 200, 300]})
        result = analyze_health_trajectory(df)
        # Should catch the error internally and return error summary
        assert "error" in result.get("summary", "").lower() or result["health_score"] is None


# ---------------------------------------------------------------------------
# Test: Output schema
# ---------------------------------------------------------------------------

class TestOutputSchema:
    """Verify the output dict has all expected keys and correct types."""

    def test_all_keys_present(self):
        """Result should contain all required top-level keys."""
        df = _make_daily_df(days=90)
        result = analyze_health_trajectory(df)
        required_keys = [
            "summary", "health_score", "trend", "seasonality",
            "change_points", "anomalies", "projection", "metrics_summary"
        ]
        for key in required_keys:
            assert key in result, f"Missing key: {key}"

    def test_health_score_range(self):
        """Health score should be 0-100."""
        df = _make_daily_df(days=90)
        result = analyze_health_trajectory(df)
        score = result["health_score"]
        assert isinstance(score, int)
        assert 0 <= score <= 100

    def test_trend_structure(self):
        """Trend dict should have direction, slope, r_squared."""
        df = _make_daily_df(days=90)
        result = analyze_health_trajectory(df)
        trend = result["trend"]
        assert trend["direction"] in ("growing", "flat", "declining")
        assert isinstance(trend["slope"], float)
        assert isinstance(trend["r_squared"], float)
        assert 0 <= trend["r_squared"] <= 1.0

    def test_seasonality_structure(self):
        """Seasonality dict should have weekly_pattern bool."""
        df = _make_daily_df(days=90)
        result = analyze_health_trajectory(df)
        seas = result["seasonality"]
        assert isinstance(seas["weekly_pattern"], bool)
        if seas["weekly_pattern"]:
            assert seas["day_of_week_index"] is not None
            assert seas["peak_day"] is not None

    def test_change_points_are_list_of_dicts(self):
        """Change points should be a list of dicts with date, metric, direction."""
        df = _make_daily_df(days=90, step_change_day=45, step_magnitude=80)
        result = analyze_health_trajectory(df)
        cps = result["change_points"]
        assert isinstance(cps, list)
        for cp in cps:
            assert "date" in cp
            assert "metric" in cp
            assert "direction" in cp
            assert cp["direction"] in ("up", "down")

    def test_anomalies_structure(self):
        """Anomalies should have date, metric, value, z_score, type."""
        df = _make_daily_df(days=90, anomaly_days=[30, 60])
        result = analyze_health_trajectory(df)
        anomalies = result["anomalies"]
        assert isinstance(anomalies, list)
        for a in anomalies:
            assert "date" in a
            assert "metric" in a
            assert "z_score" in a
            assert "type" in a
            assert a["type"] in ("spike", "drop")

    def test_projection_structure(self):
        """Projection should have method, forecast, totals."""
        df = _make_daily_df(days=90)
        result = analyze_health_trajectory(df)
        proj = result["projection"]
        assert proj["method"] == "linear_trend_with_weekly_seasonality"
        assert isinstance(proj["forecast"], list)
        assert len(proj["forecast"]) == 7  # First 7 days only
        assert proj["forecast_days"] == 30

    def test_metrics_summary_structure(self):
        """Metrics summary should have current/prior periods and changes."""
        df = _make_daily_df(days=90)
        result = analyze_health_trajectory(df)
        ms = result["metrics_summary"]
        assert "current_period" in ms
        assert "prior_period" in ms
        assert "changes" in ms
        assert "clicks" in ms["current_period"]


# ---------------------------------------------------------------------------
# Test: Trend detection
# ---------------------------------------------------------------------------

class TestTrendDetection:
    """Verify trend direction is correctly identified."""

    def test_growing_trend_detected(self):
        """Strong upward slope should be classified as growing."""
        np.random.seed(42)
        df = _make_daily_df(days=90, base_clicks=50, trend_slope=2.0,
                            weekly_pattern=False, noise_std=3)
        result = analyze_health_trajectory(df)
        assert result["trend"]["direction"] == "growing"
        assert result["trend"]["slope"] > 0

    def test_declining_trend_detected(self):
        """Strong downward slope should be classified as declining."""
        np.random.seed(42)
        df = _make_daily_df(days=90, base_clicks=200, trend_slope=-2.0,
                            weekly_pattern=False, noise_std=3)
        result = analyze_health_trajectory(df)
        assert result["trend"]["direction"] == "declining"
        assert result["trend"]["slope"] < 0

    def test_flat_trend_detected(self):
        """No slope should be classified as flat."""
        np.random.seed(42)
        df = _make_daily_df(days=90, base_clicks=100, trend_slope=0.0,
                            weekly_pattern=False, noise_std=2)
        result = analyze_health_trajectory(df)
        assert result["trend"]["direction"] == "flat"


# ---------------------------------------------------------------------------
# Test: Seasonality detection
# ---------------------------------------------------------------------------

class TestSeasonalityDetection:
    """Verify weekly patterns are detected."""

    def test_weekly_pattern_detected(self):
        """Data with weekday/weekend pattern should detect weekly seasonality."""
        np.random.seed(42)
        df = _make_daily_df(days=90, weekly_pattern=True, noise_std=2)
        result = analyze_health_trajectory(df)
        seas = result["seasonality"]
        assert seas["weekly_pattern"] is True
        # Weekdays should be higher than weekends
        dow = seas["day_of_week_index"]
        weekday_avg = np.mean([dow["Monday"], dow["Tuesday"], dow["Wednesday"],
                               dow["Thursday"], dow["Friday"]])
        weekend_avg = np.mean([dow["Saturday"], dow["Sunday"]])
        assert weekday_avg > weekend_avg

    def test_no_weekly_pattern_when_uniform(self):
        """Uniform data across all days should not detect seasonality."""
        np.random.seed(42)
        df = _make_daily_df(days=90, weekly_pattern=False, noise_std=1)
        result = analyze_health_trajectory(df)
        # With very low noise and no pattern, weekly_pattern should be False
        assert result["seasonality"]["weekly_pattern"] is False


# ---------------------------------------------------------------------------
# Test: Anomaly detection
# ---------------------------------------------------------------------------

class TestAnomalyDetection:
    """Verify anomalies are flagged on injected spikes."""

    def test_spike_anomalies_detected(self):
        """Injected 5x spikes should be flagged as anomalies."""
        np.random.seed(42)
        df = _make_daily_df(days=90, base_clicks=100, noise_std=5,
                            weekly_pattern=False, anomaly_days=[40, 70])
        result = analyze_health_trajectory(df)
        anomalies = result["anomalies"]
        # Should detect at least the injected spikes
        spike_anomalies = [a for a in anomalies if a["type"] == "spike"]
        assert len(spike_anomalies) >= 1, "Should detect at least one injected spike"

    def test_clean_data_few_anomalies(self):
        """Clean data with low noise should have very few or no anomalies."""
        np.random.seed(42)
        df = _make_daily_df(days=90, base_clicks=100, noise_std=1,
                            weekly_pattern=False)
        result = analyze_health_trajectory(df)
        assert len(result["anomalies"]) <= 2


# ---------------------------------------------------------------------------
# Test: Health score
# ---------------------------------------------------------------------------

class TestHealthScore:
    """Verify health scoring logic."""

    def test_growing_site_scores_high(self):
        """A growing site with no anomalies should score high."""
        np.random.seed(42)
        df = _make_daily_df(days=90, base_clicks=100, trend_slope=1.5,
                            weekly_pattern=False, noise_std=3)
        result = analyze_health_trajectory(df)
        assert result["health_score"] >= 60

    def test_declining_site_scores_lower(self):
        """A declining site should score lower than a growing one."""
        np.random.seed(42)
        df_grow = _make_daily_df(days=90, base_clicks=100, trend_slope=1.5,
                                  weekly_pattern=False, noise_std=3)
        np.random.seed(42)
        df_decline = _make_daily_df(days=90, base_clicks=200, trend_slope=-2.0,
                                     weekly_pattern=False, noise_std=3)
        grow_score = analyze_health_trajectory(df_grow)["health_score"]
        decline_score = analyze_health_trajectory(df_decline)["health_score"]
        assert grow_score > decline_score


# ---------------------------------------------------------------------------
# Test: Projection
# ---------------------------------------------------------------------------

class TestProjection:
    """Verify forward projections are reasonable."""

    def test_growing_site_projects_higher(self):
        """A growing trend should project more clicks than current."""
        np.random.seed(42)
        df = _make_daily_df(days=90, base_clicks=100, trend_slope=2.0,
                            weekly_pattern=False, noise_std=3)
        result = analyze_health_trajectory(df)
        proj = result["projection"]
        assert proj["projected_change_pct"] is not None
        assert proj["projected_change_pct"] > 0

    def test_forecast_has_correct_dates(self):
        """Forecast dates should start the day after last data point."""
        np.random.seed(42)
        df = _make_daily_df(days=90)
        last_date = df["date"].max()
        result = analyze_health_trajectory(df)
        forecast = result["projection"]["forecast"]
        assert len(forecast) == 7
        first_forecast_date = datetime.strptime(forecast[0]["date"], "%Y-%m-%d")
        expected = last_date + timedelta(days=1)
        assert first_forecast_date.date() == expected.date()


# ---------------------------------------------------------------------------
# Test: Summary generation
# ---------------------------------------------------------------------------

class TestSummary:
    """Verify human-readable summaries are generated."""

    def test_summary_is_non_empty_string(self):
        """Summary should always be a non-empty string."""
        df = _make_daily_df(days=90)
        result = analyze_health_trajectory(df)
        assert isinstance(result["summary"], str)
        assert len(result["summary"]) > 20

    def test_summary_mentions_score(self):
        """Summary should reference the health score."""
        df = _make_daily_df(days=90)
        result = analyze_health_trajectory(df)
        assert "score" in result["summary"].lower() or "/100" in result["summary"]


# ---------------------------------------------------------------------------
# Run with: pytest api/analysis/tests/test_module_1_health_trajectory.py -v
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
