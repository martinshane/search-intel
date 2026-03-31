"""
Module 1: Health & Trajectory Analysis

Enhanced with robustness fixes for real-world noisy GSC data:
- Added validation checks for data quality
- Improved change point detection sensitivity
- Better handling of sparse data periods
- Graceful degradation when insufficient data
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import logging

# Statistical libraries
from scipy import stats
from scipy.optimize import curve_fit
from statsmodels.tsa.seasonal import MSTL
import ruptures as rpt

# Matrix profile for pattern detection
try:
    import stumpy
    STUMPY_AVAILABLE = True
except ImportError:
    STUMPY_AVAILABLE = False
    logging.warning("STUMPY not available - pattern detection will be limited")

logger = logging.getLogger(__name__)


def analyze_health_trajectory(daily_data: pd.DataFrame, min_days: int = 90) -> Dict[str, Any]:
    """
    Comprehensive health and trajectory analysis with robustness enhancements.
    
    Args:
        daily_data: DataFrame with columns ['date', 'clicks', 'impressions']
        min_days: Minimum days of data required for analysis
        
    Returns:
        Dictionary containing health metrics, trends, seasonality, anomalies, and forecasts
    """
    try:
        # Validation and preprocessing
        validated_data = _validate_and_prepare_data(daily_data, min_days)
        if validated_data is None:
            return _insufficient_data_response()
        
        # Extract time series
        clicks_series = validated_data['clicks'].values
        impressions_series = validated_data['impressions'].values
        dates = validated_data['date'].values
        
        # 1. Decomposition for trend and seasonality
        decomposition_result = _perform_decomposition(clicks_series, dates)
        
        # 2. Trend analysis
        trend_analysis = _analyze_trend(
            decomposition_result['trend'],
            dates,
            clicks_series
        )
        
        # 3. Change point detection
        change_points = _detect_change_points(
            decomposition_result['trend'],
            dates,
            clicks_series
        )
        
        # 4. Seasonality analysis
        seasonality_analysis = _analyze_seasonality(
            decomposition_result,
            validated_data
        )
        
        # 5. Anomaly detection
        anomalies = _detect_anomalies(
            decomposition_result['residual'],
            dates,
            clicks_series
        )
        
        # 6. Pattern detection with STUMPY (if available)
        patterns = _detect_patterns(clicks_series, dates)
        
        # 7. Forecast
        forecast = _generate_forecast(
            clicks_series,
            decomposition_result,
            trend_analysis
        )
        
        # 8. Overall health score
        health_score = _calculate_health_score(
            trend_analysis,
            change_points,
            anomalies
        )
        
        return {
            "status": "success",
            "data_quality": {
                "days_analyzed": len(validated_data),
                "data_completeness": float(validated_data['clicks'].notna().sum() / len(validated_data)),
                "date_range": {
                    "start": str(dates[0]),
                    "end": str(dates[-1])
                }
            },
            "overall_direction": trend_analysis['direction'],
            "trend_slope_pct_per_month": trend_analysis['slope_pct_per_month'],
            "health_score": health_score,
            "change_points": change_points,
            "seasonality": seasonality_analysis,
            "anomalies": anomalies,
            "patterns": patterns,
            "forecast": forecast,
            "summary_metrics": {
                "current_avg_daily_clicks": float(np.mean(clicks_series[-30:])),
                "vs_90d_ago": _calculate_period_change(clicks_series, 90),
                "vs_180d_ago": _calculate_period_change(clicks_series, 180),
                "volatility": float(np.std(clicks_series) / np.mean(clicks_series))
            }
        }
        
    except Exception as e:
        logger.error(f"Error in health trajectory analysis: {str(e)}", exc_info=True)
        return {
            "status": "error",
            "error": str(e),
            "message": "Unable to complete analysis due to data issues"
        }


def _validate_and_prepare_data(df: pd.DataFrame, min_days: int) -> Optional[pd.DataFrame]:
    """
    Validate and prepare data with enhanced robustness.
    """
    if df is None or len(df) == 0:
        logger.warning("No data provided")
        return None
    
    # Make a copy to avoid modifying original
    data = df.copy()
    
    # Ensure required columns
    required_cols = ['date', 'clicks', 'impressions']
    if not all(col in data.columns for col in required_cols):
        logger.error(f"Missing required columns. Have: {data.columns.tolist()}")
        return None
    
    # Convert date to datetime
    if not pd.api.types.is_datetime64_any_dtype(data['date']):
        data['date'] = pd.to_datetime(data['date'], errors='coerce')
    
    # Remove rows with invalid dates
    data = data.dropna(subset=['date'])
    
    # Sort by date
    data = data.sort_values('date').reset_index(drop=True)
    
    # Check for minimum data requirement
    if len(data) < min_days:
        logger.warning(f"Insufficient data: {len(data)} days < {min_days} required")
        return None
    
    # Fill missing values in clicks/impressions with 0 (common in GSC data)
    data['clicks'] = pd.to_numeric(data['clicks'], errors='coerce').fillna(0)
    data['impressions'] = pd.to_numeric(data['impressions'], errors='coerce').fillna(0)
    
    # Remove outliers (extreme values that would skew analysis)
    # Using IQR method but only for extreme outliers (10x IQR)
    for col in ['clicks', 'impressions']:
        Q1 = data[col].quantile(0.25)
        Q3 = data[col].quantile(0.75)
        IQR = Q3 - Q1
        extreme_upper = Q3 + 10 * IQR
        
        outlier_count = (data[col] > extreme_upper).sum()
        if outlier_count > 0:
            logger.info(f"Capping {outlier_count} extreme outliers in {col}")
            data.loc[data[col] > extreme_upper, col] = extreme_upper
    
    return data


def _insufficient_data_response() -> Dict[str, Any]:
    """
    Return a graceful response when data is insufficient.
    """
    return {
        "status": "insufficient_data",
        "message": "Not enough data to perform meaningful analysis. Need at least 90 days of data.",
        "overall_direction": "unknown",
        "trend_slope_pct_per_month": 0.0,
        "health_score": None,
        "change_points": [],
        "seasonality": {},
        "anomalies": [],
        "patterns": {},
        "forecast": {}
    }


def _perform_decomposition(series: np.ndarray, dates: np.ndarray) -> Dict[str, np.ndarray]:
    """
    Perform MSTL decomposition with enhanced error handling for noisy data.
    """
    try:
        # MSTL requires at least 2 full periods of data
        # For weekly (7) and monthly (30) periods, need at least 60 days
        if len(series) < 60:
            logger.warning("Series too short for MSTL, using simple moving average")
            return _simple_decomposition(series)
        
        # Convert to pandas Series for MSTL
        ts = pd.Series(series, index=pd.DatetimeIndex(dates))
        
        # Handle zeros and very low variance
        if np.std(series) < 1e-6:
            logger.warning("Series has near-zero variance")
            return _simple_decomposition(series)
        
        # MSTL with multiple seasonal periods
        # periods: 7 (weekly), 30 (monthly) - adjusted for data availability
        periods = []
        if len(series) >= 14:
            periods.append(7)
        if len(series) >= 60:
            periods.append(30)
        
        if not periods:
            return _simple_decomposition(series)
        
        mstl = MSTL(ts, periods=periods, stl_kwargs={'seasonal': 7})
        result = mstl.fit()
        
        return {
            'trend': result.trend.values,
            'seasonal': result.seasonal.values,
            'residual': result.resid.values,
            'observed': series
        }
        
    except Exception as e:
        logger.warning(f"MSTL decomposition failed: {e}, using fallback")
        return _simple_decomposition(series)


def _simple_decomposition(series: np.ndarray) -> Dict[str, np.ndarray]:
    """
    Fallback decomposition using simple moving averages.
    """
    # Trend: 7-day moving average
    window = min(7, len(series) // 3)
    trend = pd.Series(series).rolling(window=window, center=True).mean().fillna(method='bfill').fillna(method='ffill').values
    
    # Residual: observed - trend
    residual = series - trend
    
    # Seasonal: simple day-of-week pattern if enough data
    seasonal = np.zeros_like(series)
    if len(series) >= 14:
        for i in range(7):
            mask = np.arange(len(series)) % 7 == i
            if mask.sum() > 0:
                seasonal[mask] = np.mean(residual[mask])
    
    return {
        'trend': trend,
        'seasonal': seasonal,
        'residual': residual - seasonal,
        'observed': series
    }


def _analyze_trend(trend: np.ndarray, dates: np.ndarray, original_series: np.ndarray) -> Dict[str, Any]:
    """
    Analyze trend component with improved robustness.
    """
    # Filter out NaN values from trend
    valid_mask = ~np.isnan(trend)
    if valid_mask.sum() < 30:
        logger.warning("Too many NaN values in trend")
        return {
            'direction': 'unknown',
            'slope_pct_per_month': 0.0,
            'slope': 0.0,
            'confidence': 0.0
        }
    
    clean_trend = trend[valid_mask]
    clean_x = np.arange(len(trend))[valid_mask]
    
    # Linear regression on trend
    slope, intercept, r_value, p_value, std_err = stats.linregress(clean_x, clean_trend)
    
    # Convert slope to percentage change per month
    if len(clean_trend) > 0 and np.mean(clean_trend) > 0:
        days_in_data = len(clean_trend)
        avg_value = np.mean(clean_trend)
        slope_pct_per_month = (slope * 30 / avg_value) * 100
    else:
        slope_pct_per_month = 0.0
    
    # Classify direction with refined thresholds
    if slope_pct_per_month > 5:
        direction = "strong_growth"
    elif slope_pct_per_month > 1:
        direction = "growth"
    elif slope_pct_per_month > -1:
        direction = "flat"
    elif slope_pct_per_month > -5:
        direction = "declining"
    else:
        direction = "strong_decline"
    
    return {
        'direction': direction,
        'slope_pct_per_month': round(slope_pct_per_month, 2),
        'slope': float(slope),
        'confidence': float(r_value ** 2),  # R-squared as confidence measure
        'p_value': float(p_value)
    }


def _detect_change_points(trend: np.ndarray, dates: np.ndarray, original_series: np.ndarray) -> List[Dict[str, Any]]:
    """
    Detect change points with improved sensitivity for noisy data.
    """
    change_points = []
    
    try:
        # Filter valid data
        valid_mask = ~np.isnan(trend)
        if valid_mask.sum() < 30:
            return change_points
        
        clean_trend = trend[valid_mask]
        clean_dates = dates[valid_mask]
        
        # Use Pelt algorithm with adjusted penalty
        # Lower penalty = more sensitive to changes
        # Penalty scales with data variance
        signal_std = np.std(clean_trend)
        if signal_std < 1e-6:
            return change_points
        
        # Normalize signal for better change point detection
        normalized_signal = (clean_trend - np.mean(clean_trend)) / signal_std
        
        # Pelt algorithm
        algo = rpt.Pelt(model="rbf", min_size=14, jump=1).fit(normalized_signal)
        
        # Adjusted penalty based on data length and variance
        penalty_value = 3 * np.log(len(normalized_signal))
        detected_bkps = algo.predict(pen=penalty_value)
        
        # Convert breakpoints to change point events
        for i, bkp in enumerate(detected_bkps[:-1]):  # Last one is always end of series
            if bkp >= len(clean_trend) or bkp == 0:
                continue
            
            # Calculate magnitude of change
            window = 14  # 2 weeks before and after
            before_start = max(0, bkp - window)
            after_end = min(len(clean_trend), bkp + window)
            
            before_mean = np.mean(clean_trend[before_start:bkp])
            after_mean = np.mean(clean_trend[bkp:after_end])
            
            if before_mean > 0:
                magnitude = (after_mean - before_mean) / before_mean
            else:
                magnitude = 0.0
            
            # Only report significant changes (>10% change)
            if abs(magnitude) > 0.10:
                direction = "increase" if magnitude > 0 else "drop"
                
                change_points.append({
                    "date": str(clean_dates[bkp]),
                    "magnitude": round(magnitude, 3),
                    "direction": direction,
                    "before_avg": round(float(before_mean), 1),
                    "after_avg": round(float(after_mean), 1)
                })
        
        # Sort by magnitude (most significant first)
        change_points.sort(key=lambda x: abs(x['magnitude']), reverse=True)
        
        # Limit to top 5 most significant
        return change_points[:5]
        
    except Exception as e:
        logger.warning(f"Change point detection failed: {e}")
        return []


def _analyze_seasonality(decomposition: Dict[str, np.ndarray], data: pd.DataFrame) -> Dict[str, Any]:
    """
    Analyze seasonality patterns with better handling of sparse data.
    """
    try:
        seasonal = decomposition['seasonal']
        
        # Day-of-week analysis
        data_copy = data.copy()
        data_copy['day_of_week'] = pd.to_datetime(data_copy['date']).dt.day_name()
        data_copy['seasonal_component'] = seasonal[:len(data_copy)]
        
        dow_avg = data_copy.groupby('day_of_week')['clicks'].mean()
        
        if len(dow_avg) == 7:
            best_day = dow_avg.idxmax()
            worst_day = dow_avg.idxmin()
            dow_variation = (dow_avg.max() - dow_avg.min()) / dow_avg.mean()
        else:
            best_day = "Unknown"
            worst_day = "Unknown"
            dow_variation = 0.0
        
        # Monthly cycle detection (simplified)
        data_copy['day_of_month'] = pd.to_datetime(data_copy['date']).dt.day
        monthly_pattern = data_copy.groupby('day_of_month')['clicks'].mean()
        
        # Check if first week is significantly higher
        if len(monthly_pattern) >= 28:
            first_week = monthly_pattern[1:8].mean()
            rest_of_month = monthly_pattern[8:].mean()
            
            if rest_of_month > 0:
                monthly_spike = (first_week - rest_of_month) / rest_of_month
                has_monthly_cycle = monthly_spike > 0.15
                cycle_description = f"{abs(monthly_spike)*100:.0f}% traffic spike first week of month" if has_monthly_cycle else "No strong monthly pattern"
            else:
                has_monthly_cycle = False
                cycle_description = "Insufficient data for monthly pattern"
        else:
            has_monthly_cycle = False
            cycle_description = "Insufficient data for monthly pattern"
        
        return {
            "best_day": best_day,
            "worst_day": worst_day,
            "day_of_week_variation_pct": round(dow_variation * 100, 1),
            "monthly_cycle": has_monthly_cycle,
            "cycle_description": cycle_description
        }
        
    except Exception as e:
        logger.warning(f"Seasonality analysis failed: {e}")
        return {
            "best_day": "Unknown",
            "worst_day": "Unknown",
            "day_of_week_variation_pct": 0.0,
            "monthly_cycle": False,
            "cycle_description": "Analysis failed"
        }


def _detect_anomalies(residual: np.ndarray, dates: np.ndarray, original_series: np.ndarray) -> List[Dict[str, Any]]:
    """
    Detect anomalies using statistical methods with improved noise handling.
    """
    anomalies = []
    
    try:
        # Filter valid residuals
        valid_mask = ~np.isnan(residual)
        if valid_mask.sum() < 30:
            return anomalies
        
        clean_residual = residual[valid_mask]
        clean_dates = dates[valid_mask]
        clean_original = original_series[valid_mask]
        
        # Use modified Z-score for anomaly detection (more robust than standard Z-score)
        median = np.median(clean_residual)
        mad = np.median(np.abs(clean_residual - median))
        
        if mad < 1e-6:
            return anomalies
        
        modified_z_scores = 0.6745 * (clean_residual - median) / mad
        
        # Anomalies are points with |modified_z_score| > 3.5
        anomaly_mask = np.abs(modified_z_scores) > 3.5
        anomaly_indices = np.where(anomaly_mask)[0]
        
        for idx in anomaly_indices:
            magnitude = float(modified_z_scores[idx])
            anomaly_type = "spike" if magnitude > 0 else "drop"
            
            anomalies.append({
                "date": str(clean_dates[idx]),
                "type": anomaly_type,
                "magnitude": round(magnitude, 2),
                "value": round(float(clean_original[idx]), 1),
                "expected": round(float(clean_original[idx] - clean_residual[idx]), 1)
            })
        
        # Sort by absolute magnitude
        anomalies.sort(key=lambda x: abs(x['magnitude']), reverse=True)
        
        # Limit to top 10
        return anomalies[:10]
        
    except Exception as e:
        logger.warning(f"Anomaly detection failed: {e}")
        return []


def _detect_patterns(series: np.ndarray, dates: np.ndarray) -> Dict[str, Any]:
    """
    Detect recurring patterns using matrix profile (if available).
    """
    if not STUMPY_AVAILABLE:
        return {
            "motifs_found": False,
            "message": "Pattern detection not available"
        }
    
    try:
        # Need at least 60 days for meaningful pattern detection
        if len(series) < 60:
            return {"motifs_found": False, "message": "Insufficient data"}
        
        # Filter out zeros and NaN
        valid_mask = ~np.isnan(series) & (series > 0)
        if valid_mask.sum() < 60:
            return {"motifs_found": False, "message": "Too many missing values"}
        
        clean_series = series[valid_mask]
        
        # Window size: 7 days (weekly patterns)
        m = 7
        if len(clean_series) < 2 * m:
            return {"motifs_found": False, "message": "Series too short for pattern detection"}
        
        # Compute matrix profile
        mp = stumpy.stump(clean_series, m=m)
        
        # Find motifs (similar patterns)
        motif_idx = np.argsort(mp[:, 0])[:3]  # Top 3 motifs
        
        motifs = []
        for idx in motif_idx:
            if mp[idx, 0] < np.inf:
                motifs.append({
                    "position": int(idx),
                    "distance": float(mp[idx, 0]),
                    "pattern_length": m
                })
        
        return {
            "motifs_found": len(motifs) > 0,
            "motifs": motifs,
            "message": f"Found {len(motifs)} recurring patterns"
        }
        
    except Exception as e:
        logger.warning(f"Pattern detection failed: {e}")
        return {"motifs_found": False, "message": "Pattern detection failed"}


def _generate_forecast(series: np.ndarray, decomposition: Dict[str, np.ndarray], trend_analysis: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generate forecast with confidence intervals.
    Uses simple trend projection with seasonal adjustment.
    """
    try:
        # Need at least 60 days for meaningful forecast
        if len(series) < 60:
            return {}
        
        trend = decomposition['trend']
        valid_mask = ~np.isnan(trend)
        
        if valid_mask.sum() < 60:
            return {}
        
        clean_trend = trend[valid_mask]
        
        # Use last 90 days for projection
        recent_trend = clean_trend[-90:]
        x = np.arange(len(recent_trend))
        
        # Fit linear model
        slope, intercept, r_value, _, std_err = stats.linregress(x, recent_trend)
        
        # Calculate confidence intervals (wider for longer horizons)
        def forecast_with_ci(days_ahead: int):
            # Project trend
            x_future = len(recent_trend) - 1 + days_ahead
            trend_forecast = slope * x_future