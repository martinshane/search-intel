"""
Pytest configuration and shared fixtures for synthetic data generation.

This module provides reusable fixtures for generating realistic test data
across all module tests. The synthetic data mimics 16 months of daily GSC metrics
with controlled characteristics for testing specific scenarios.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple


@pytest.fixture
def date_range_16_months():
    """Generate 16 months of daily dates ending today."""
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=16 * 30)  # Approximate 16 months
    return pd.date_range(start=start_date, end=end_date, freq='D')


@pytest.fixture
def baseline_daily_data(date_range_16_months):
    """
    Generate baseline synthetic daily GSC data with realistic patterns.
    
    Returns DataFrame with columns: date, clicks, impressions, ctr, position
    Pattern: stable baseline with weekly seasonality and monthly cycles.
    """
    dates = date_range_16_months
    n_days = len(dates)
    
    # Base trend (flat)
    base_clicks = 1000
    
    # Weekly seasonality (weekday vs weekend pattern)
    weekly_pattern = np.array([1.0, 1.05, 1.08, 1.10, 1.07, 0.85, 0.75])  # Mon-Sun
    weekly_seasonal = np.tile(weekly_pattern, n_days // 7 + 1)[:n_days]
    
    # Monthly seasonality (slight spike at month start)
    day_of_month = np.array([d.day for d in dates])
    monthly_seasonal = 1.0 + 0.15 * np.exp(-day_of_month / 5)
    
    # Combine components with noise
    np.random.seed(42)
    noise = np.random.normal(1.0, 0.05, n_days)
    clicks = base_clicks * weekly_seasonal * monthly_seasonal * noise
    clicks = np.maximum(clicks, 0).astype(int)
    
    # Generate impressions (CTR around 5%)
    base_ctr = 0.05
    ctr_noise = np.random.normal(1.0, 0.1, n_days)
    ctr = base_ctr * ctr_noise
    ctr = np.clip(ctr, 0.01, 0.30)
    impressions = (clicks / ctr).astype(int)
    
    # Position (stable around 8.0)
    position = np.random.normal(8.0, 0.5, n_days)
    position = np.clip(position, 1.0, 100.0)
    
    return pd.DataFrame({
        'date': dates,
        'clicks': clicks,
        'impressions': impressions,
        'ctr': ctr,
        'position': position
    })


@pytest.fixture
def growing_daily_data(date_range_16_months):
    """
    Generate synthetic data with strong growth trend (+5% per month).
    """
    dates = date_range_16_months
    n_days = len(dates)
    
    # Growth trend: 5% per month = ~0.165% per day compounded
    day_indices = np.arange(n_days)
    growth_factor = (1.05 ** (1/30)) ** day_indices
    base_clicks = 800
    
    # Weekly seasonality
    weekly_pattern = np.array([1.0, 1.05, 1.08, 1.10, 1.07, 0.85, 0.75])
    weekly_seasonal = np.tile(weekly_pattern, n_days // 7 + 1)[:n_days]
    
    # Noise
    np.random.seed(43)
    noise = np.random.normal(1.0, 0.08, n_days)
    
    clicks = base_clicks * growth_factor * weekly_seasonal * noise
    clicks = np.maximum(clicks, 0).astype(int)
    
    # CTR slightly improving with rankings
    base_ctr = 0.045
    ctr_improvement = 1.0 + 0.002 * day_indices / 30  # Slight CTR improvement
    ctr = base_ctr * ctr_improvement * np.random.normal(1.0, 0.08, n_days)
    ctr = np.clip(ctr, 0.01, 0.30)
    impressions = (clicks / ctr).astype(int)
    
    # Position improving (going down in number)
    position = 12.0 - 0.01 * day_indices  # Slowly improving from 12 to ~7
    position = position + np.random.normal(0, 0.3, n_days)
    position = np.clip(position, 1.0, 100.0)
    
    return pd.DataFrame({
        'date': dates,
        'clicks': clicks,
        'impressions': impressions,
        'ctr': ctr,
        'position': position
    })


@pytest.fixture
def declining_daily_data(date_range_16_months):
    """
    Generate synthetic data with decline trend (-3% per month).
    """
    dates = date_range_16_months
    n_days = len(dates)
    
    # Decline trend: -3% per month
    day_indices = np.arange(n_days)
    decline_factor = (0.97 ** (1/30)) ** day_indices
    base_clicks = 1200
    
    # Weekly seasonality
    weekly_pattern = np.array([1.0, 1.05, 1.08, 1.10, 1.07, 0.85, 0.75])
    weekly_seasonal = np.tile(weekly_pattern, n_days // 7 + 1)[:n_days]
    
    # Noise
    np.random.seed(44)
    noise = np.random.normal(1.0, 0.06, n_days)
    
    clicks = base_clicks * decline_factor * weekly_seasonal * noise
    clicks = np.maximum(clicks, 0).astype(int)
    
    # CTR declining
    base_ctr = 0.055
    ctr_decline = 1.0 - 0.001 * day_indices / 30
    ctr = base_ctr * ctr_decline * np.random.normal(1.0, 0.08, n_days)
    ctr = np.clip(ctr, 0.01, 0.30)
    impressions = (clicks / ctr).astype(int)
    
    # Position worsening (going up in number)
    position = 6.0 + 0.015 * day_indices  # Worsening from 6 to ~13
    position = position + np.random.normal(0, 0.4, n_days)
    position = np.clip(position, 1.0, 100.0)
    
    return pd.DataFrame({
        'date': dates,
        'clicks': clicks,
        'impressions': impressions,
        'ctr': ctr,
        'position': position
    })


@pytest.fixture
def data_with_changepoint(date_range_16_months):
    """
    Generate data with a clear change point (algorithm update impact).
    
    Pattern: stable for 10 months, then sudden drop of 25% at month 10,
    followed by partial recovery.
    """
    dates = date_range_16_months
    n_days = len(dates)
    
    # Change point at day ~300 (10 months in)
    changepoint_day = 300
    
    # Base pattern before changepoint
    base_clicks = 1500
    
    # Weekly seasonality
    weekly_pattern = np.array([1.0, 1.05, 1.08, 1.10, 1.07, 0.85, 0.75])
    weekly_seasonal = np.tile(weekly_pattern, n_days // 7 + 1)[:n_days]
    
    # Apply changepoint: 25% drop, then slow recovery
    changepoint_effect = np.ones(n_days)
    changepoint_effect[changepoint_day:] = 0.75  # 25% drop
    
    # Slow recovery after changepoint (recover 10% over remaining days)
    recovery_days = n_days - changepoint_day
    if recovery_days > 0:
        recovery = np.linspace(0, 0.10, recovery_days)
        changepoint_effect[changepoint_day:] += recovery
    
    # Noise
    np.random.seed(45)
    noise = np.random.normal(1.0, 0.05, n_days)
    
    clicks = base_clicks * weekly_seasonal * changepoint_effect * noise
    clicks = np.maximum(clicks, 0).astype(int)
    
    # CTR relatively stable
    ctr = np.random.normal(0.048, 0.004, n_days)
    ctr = np.clip(ctr, 0.01, 0.30)
    impressions = (clicks / ctr).astype(int)
    
    # Position worsens at changepoint
    position = np.ones(n_days) * 5.5
    position[changepoint_day:] += 3.0  # Jump from 5.5 to 8.5
    position = position + np.random.normal(0, 0.3, n_days)
    position = np.clip(position, 1.0, 100.0)
    
    return pd.DataFrame({
        'date': dates,
        'clicks': clicks,
        'impressions': impressions,
        'ctr': ctr,
        'position': position
    }), changepoint_day


@pytest.fixture
def data_with_multiple_changepoints(date_range_16_months):
    """
    Generate data with multiple change points for testing detection robustness.
    
    Pattern:
    - Stable baseline (months 0-4)
    - Growth period (months 4-8): +15%
    - Algorithm hit (month 10): -20%
    - Recovery (months 11-16): gradual recovery to baseline
    """
    dates = date_range_16_months
    n_days = len(dates)
    
    # Define changepoints
    cp1_day = 120  # ~4 months: start growth
    cp2_day = 300  # ~10 months: algorithm hit
    
    base_clicks = 1000
    
    # Weekly seasonality
    weekly_pattern = np.array([1.0, 1.05, 1.08, 1.10, 1.07, 0.85, 0.75])
    weekly_seasonal = np.tile(weekly_pattern, n_days // 7 + 1)[:n_days]
    
    # Build trend with multiple levels
    trend_effect = np.ones(n_days)
    
    # Period 1: baseline (days 0-120)
    trend_effect[:cp1_day] = 1.0
    
    # Period 2: growth (days 120-300)
    growth_period = cp2_day - cp1_day
    trend_effect[cp1_day:cp2_day] = 1.0 + 0.15 * np.linspace(0, 1, growth_period)
    
    # Period 3: algorithm hit (days 300+)
    trend_effect[cp2_day:] = 1.15 * 0.80  # Drop to 92% of baseline
    
    # Gradual recovery in period 3
    recovery_days = n_days - cp2_day
    if recovery_days > 0:
        recovery = np.linspace(0, 0.08, recovery_days)  # Recover 8% over remaining time
        trend_effect[cp2_day:] += recovery
    
    # Noise
    np.random.seed(46)
    noise = np.random.normal(1.0, 0.06, n_days)
    
    clicks = base_clicks * weekly_seasonal * trend_effect * noise
    clicks = np.maximum(clicks, 0).astype(int)
    
    # CTR
    ctr = np.random.normal(0.050, 0.005, n_days)
    ctr = np.clip(ctr, 0.01, 0.30)
    impressions = (clicks / ctr).astype(int)
    
    # Position changes at changepoints
    position = np.ones(n_days) * 9.0
    position[cp1_day:cp2_day] -= 2.0  # Improves during growth
    position[cp2_day:] += 2.5  # Worsens after algorithm hit
    position = position + np.random.normal(0, 0.4, n_days)
    position = np.clip(position, 1.0, 100.0)
    
    return pd.DataFrame({
        'date': dates,
        'clicks': clicks,
        'impressions': impressions,
        'ctr': ctr,
        'position': position
    }), [cp1_day, cp2_day]


@pytest.fixture
def data_with_anomalies(date_range_16_months):
    """
    Generate data with clear anomalies (one-off spikes/drops).
    
    Pattern: stable baseline with 3 anomalies:
    - Day 100: spike (+150%)
    - Day 250: drop (-60%)
    - Day 400: spike (+120%)
    """
    dates = date_range_16_months
    n_days = len(dates)
    
    base_clicks = 1200
    
    # Weekly seasonality
    weekly_pattern = np.array([1.0, 1.05, 1.08, 1.10, 1.07, 0.85, 0.75])
    weekly_seasonal = np.tile(weekly_pattern, n_days // 7 + 1)[:n_days]
    
    # Noise
    np.random.seed(47)
    noise = np.random.normal(1.0, 0.05, n_days)
    
    clicks = base_clicks * weekly_seasonal * noise
    
    # Insert anomalies
    anomaly_days = []
    if 100 < n_days:
        clicks[100] *= 2.5  # +150% spike
        anomaly_days.append(100)
    if 250 < n_days:
        clicks[250] *= 0.4  # -60% drop
        anomaly_days.append(250)
    if 400 < n_days:
        clicks[400] *= 2.2  # +120% spike
        anomaly_days.append(400)
    
    clicks = np.maximum(clicks, 0).astype(int)
    
    # CTR
    ctr = np.random.normal(0.047, 0.004, n_days)
    ctr = np.clip(ctr, 0.01, 0.30)
    impressions = (clicks / ctr).astype(int)
    
    # Position stable
    position = np.random.normal(7.5, 0.4, n_days)
    position = np.clip(position, 1.0, 100.0)
    
    return pd.DataFrame({
        'date': dates,
        'clicks': clicks,
        'impressions': impressions,
        'ctr': ctr,
        'position': position
    }), anomaly_days


@pytest.fixture
def flat_stable_data(date_range_16_months):
    """
    Generate perfectly flat data for testing edge cases.
    
    No trend, minimal seasonality, minimal noise.
    """
    dates = date_range_16_months
    n_days = len(dates)
    
    base_clicks = 1000
    
    # Very weak weekly seasonality
    weekly_pattern = np.array([1.0, 1.01, 1.02, 1.02, 1.01, 0.99, 0.98])
    weekly_seasonal = np.tile(weekly_pattern, n_days // 7 + 1)[:n_days]
    
    # Minimal noise
    np.random.seed(48)
    noise = np.random.normal(1.0, 0.02, n_days)
    
    clicks = base_clicks * weekly_seasonal * noise
    clicks = np.maximum(clicks, 0).astype(int)
    
    # Stable CTR
    ctr = np.random.normal(0.050, 0.001, n_days)
    ctr = np.clip(ctr, 0.01, 0.30)
    impressions = (clicks / ctr).astype(int)
    
    # Stable position
    position = np.random.normal(8.0, 0.2, n_days)
    position = np.clip(position, 1.0, 100.0)
    
    return pd.DataFrame({
        'date': dates,
        'clicks': clicks,
        'impressions': impressions,
        'ctr': ctr,
        'position': position
    })


@pytest.fixture
def high_volatility_data(date_range_16_months):
    """
    Generate data with high volatility but no clear trend.
    
    For testing robustness of decomposition and changepoint detection.
    """
    dates = date_range_16_months
    n_days = len(dates)
    
    base_clicks = 1000
    
    # Strong weekly seasonality
    weekly_pattern = np.array([1.0, 1.15, 1.25, 1.30, 1.20, 0.70, 0.60])
    weekly_seasonal = np.tile(weekly_pattern, n_days // 7 + 1)[:n_days]
    
    # High noise
    np.random.seed(49)
    noise = np.random.normal(1.0, 0.20, n_days)  # 20% standard deviation
    
    clicks = base_clicks * weekly_seasonal * noise
    clicks = np.maximum(clicks, 0).astype(int)
    
    # Volatile CTR
    ctr = np.random.normal(0.050, 0.010, n_days)
    ctr = np.clip(ctr, 0.01, 0.30)
    impressions = (clicks / ctr).astype(int)
    
    # Volatile position
    position = np.random.normal(8.0, 1.5, n_days)
    position = np.clip(position, 1.0, 100.0)
    
    return pd.DataFrame({
        'date': dates,
        'clicks': clicks,
        'impressions': impressions,
        'ctr': ctr,
        'position': position
    })


@pytest.fixture
def expected_module1_output_schema():
    """
    Define the expected output schema for Module 1 (Health & Trajectory).
    
    Used for validation in tests to ensure output matches spec exactly.
    """
    return {
        "overall_direction": str,  # "strong_growth", "growth", "flat", "decline", "strong_decline"
        "trend_slope_pct_per_month": float,
        "change_points": list,  # List[{"date": str, "magnitude": float, "direction": str}]
        "seasonality": dict,  # {"best_day": str, "worst_day": str, "monthly_cycle": bool, "cycle_description": str}
        "anomalies": list,  # List[{"date": str, "type": str, "magnitude": float}]
        "forecast": dict  # {"30d": {...}, "60d": {...}, "90d": {...}}
    }


def validate_module1_output(output: dict, schema: dict) -> Tuple[bool, List[str]]:
    """
    Validate Module 1 output against expected schema.
    
    Args:
        output: The module output to validate
        schema: Expected schema dictionary
        
    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    errors = []
    
    # Check top-level keys
    required_keys = set(schema.keys())
    actual_keys = set(output.keys())
    
    missing_keys = required_keys - actual_keys
    if missing_keys:
        errors.append(f"Missing required keys: {missing_keys}")
    
    # Check types for present keys
    for key in required_keys & actual_keys:
        expected_type = schema[key]
        actual_value = output[key]
        
        if not isinstance(actual_value, expected_type):
            errors.append(f"Key '{key}' has wrong type: expected {expected_type}, got {type(actual_value)}")
    
    # Validate overall_direction values
    if "overall_direction" in output:
        valid_directions = {"strong_growth", "growth", "flat", "decline", "strong_decline"}
        if output["overall_direction"] not in valid_directions:
            errors.append(f"Invalid overall_direction: {output['overall_direction']}. Must be one of {valid_directions}")
    
    # Validate change_points structure
    if "change_points" in output and isinstance(output["change_points"], list):
        for i, cp in enumerate(output["change_points"]):
            if not isinstance(cp, dict):
                errors.append(f"change_points[{i}] is not a dict")
                continue
            
            required_cp_keys = {"date", "magnitude", "direction"}
            missing_cp_keys = required_cp_keys - set(cp.keys())
            if missing_cp_keys:
                errors.append(f"change_points[{i}] missing keys: {missing_cp_keys}")
    
    # Validate seasonality structure
    if "seasonality" in output and isinstance(output["seasonality"], dict):
        required_season_keys = {"best_day", "worst_day", "monthly_cycle", "cycle_description"}
        missing_season_keys = required_season_keys - set(output["seasonality"].keys())
        if missing_season_keys:
            errors.append(f"seasonality missing keys: {missing_season_keys}")
    
    # Validate anomalies structure
    if "anomalies" in output and isinstance(output["anomalies"], list):
        for i, anom in enumerate(output["anomalies"]):
            if not isinstance(anom, dict):
                errors.append(f"anomalies[{i}] is not a dict")
                continue
            
            required_anom_keys = {"date", "type", "magnitude"}
            missing_anom_keys = required_anom_keys - set(anom.keys())
            if missing_anom_keys:
                errors.append(f"anomalies[{i}] missing keys: {missing_anom_keys}")
    
    # Validate forecast structure
    if "forecast" in output and isinstance(output["forecast"], dict):
        required_forecast_periods = {"30d", "60d", "90d"}
        missing_periods = required_forecast_periods - set(output["forecast"].keys())
        if missing_periods:
            errors.append(f"forecast missing periods: {missing_periods}")
        
        for period in required_forecast_periods & set(output["forecast"].keys()):
            forecast_period = output["forecast"][period]
            if not isinstance(forecast_period, dict):
                errors.append(f"forecast[{period}] is not a dict")
                continue
            
            required_forecast_keys = {"clicks", "ci_low", "ci_high"}
            missing_forecast_keys = required_forecast_keys - set(forecast_