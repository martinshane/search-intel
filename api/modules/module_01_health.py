"""
Module 1: Health & Trajectory Analysis
MSTL decomposition, PELT change point detection, STUMPY matrix profile, ARIMA forecasting
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import logging

# Time series decomposition and forecasting
from statsmodels.tsa.seasonal import MSTL
from statsmodels.tsa.arima.model import ARIMA
from scipy import stats
from scipy.optimize import curve_fit

# Change point detection
import ruptures as rpt

# Matrix profile for anomaly/motif detection
import stumpy

logger = logging.getLogger(__name__)


def analyze_health_trajectory(daily_data: pd.DataFrame) -> Dict[str, Any]:
    """
    Complete Module 1 implementation: Health & Trajectory Analysis
    
    Performs:
    1. MSTL decomposition (trend, weekly/monthly seasonality, residuals)
    2. Trend direction classification and slope calculation
    3. PELT change point detection on trend component
    4. STUMPY matrix profile analysis on residuals (motifs + discords)
    5. ARIMA forecasting (30/60/90 day projections)
    
    Args:
        daily_data: DataFrame with columns ['date', 'clicks', 'impressions']
                   Must be sorted by date ascending, daily granularity
    
    Returns:
        Dict matching spec schema with keys:
        - overall_direction: str
        - trend_slope_pct_per_month: float
        - change_points: List[Dict]
        - seasonality: Dict
        - anomalies: List[Dict]
        - forecast: Dict
    """
    
    try:
        # Validate and prepare data
        df = _prepare_data(daily_data)
        
        if len(df) < 90:
            logger.warning(f"Insufficient data for full analysis: {len(df)} days (need 90+)")
            return _minimal_analysis(df)
        
        # 1. MSTL Decomposition
        decomposition = _perform_mstl_decomposition(df)
        
        # 2. Trend Analysis
        trend_analysis = _analyze_trend(decomposition['trend'], df)
        
        # 3. Change Point Detection
        change_points = _detect_change_points(decomposition['trend'], df)
        
        # 4. Seasonality Analysis
        seasonality_analysis = _analyze_seasonality(
            decomposition['seasonal_weekly'],
            decomposition['seasonal_monthly'],
            df
        )
        
        # 5. Anomaly Detection (matrix profile on residuals)
        anomalies = _detect_anomalies(decomposition['resid'], df)
        
        # 6. Forecasting
        forecast = _generate_forecast(df, decomposition)
        
        # Assemble final result
        result = {
            "overall_direction": trend_analysis['direction'],
            "trend_slope_pct_per_month": trend_analysis['slope_pct_per_month'],
            "change_points": change_points,
            "seasonality": seasonality_analysis,
            "anomalies": anomalies,
            "forecast": forecast,
            "metadata": {
                "days_analyzed": len(df),
                "date_range": {
                    "start": df['date'].min().isoformat(),
                    "end": df['date'].max().isoformat()
                },
                "avg_daily_clicks": float(df['clicks'].mean()),
                "total_clicks_period": int(df['clicks'].sum())
            }
        }
        
        logger.info(f"Module 1 analysis complete: {trend_analysis['direction']} trend, "
                   f"{len(change_points)} change points detected")
        
        return result
        
    except Exception as e:
        logger.error(f"Module 1 analysis failed: {str(e)}", exc_info=True)
        raise


def _prepare_data(daily_data: pd.DataFrame) -> pd.DataFrame:
    """Validate and prepare input data"""
    
    df = daily_data.copy()
    
    # Ensure required columns
    required_cols = ['date', 'clicks', 'impressions']
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    
    # Convert date to datetime
    df['date'] = pd.to_datetime(df['date'])
    
    # Sort by date
    df = df.sort_values('date').reset_index(drop=True)
    
    # Fill missing dates with 0 clicks/impressions
    date_range = pd.date_range(start=df['date'].min(), end=df['date'].max(), freq='D')
    df = df.set_index('date').reindex(date_range, fill_value=0).reset_index()
    df.columns = ['date', 'clicks', 'impressions'] if len(df.columns) == 3 else ['date'] + list(df.columns[1:])
    
    # Ensure numeric types
    df['clicks'] = pd.to_numeric(df['clicks'], errors='coerce').fillna(0)
    df['impressions'] = pd.to_numeric(df['impressions'], errors='coerce').fillna(0)
    
    return df


def _perform_mstl_decomposition(df: pd.DataFrame) -> Dict[str, pd.Series]:
    """
    Perform MSTL decomposition with weekly and monthly periods
    
    Returns dict with: trend, seasonal_weekly, seasonal_monthly, resid
    """
    
    # Use clicks as the primary metric
    ts = df['clicks'].values
    
    # MSTL requires at least 2 full periods of the longest seasonality
    min_length = 2 * 30  # 60 days minimum
    if len(ts) < min_length:
        logger.warning(f"Data too short for MSTL ({len(ts)} days), using simpler decomposition")
        return _simple_decomposition(df)
    
    try:
        # MSTL with weekly (7) and monthly (30) periods
        # windows parameter: 7 means "odd integer closest to 7"
        mstl = MSTL(ts, periods=[7, 30], windows=[7, 31])
        result = mstl.fit()
        
        return {
            'trend': pd.Series(result.trend, index=df.index),
            'seasonal_weekly': pd.Series(result.seasonal[:, 0], index=df.index),
            'seasonal_monthly': pd.Series(result.seasonal[:, 1], index=df.index),
            'resid': pd.Series(result.resid, index=df.index)
        }
        
    except Exception as e:
        logger.warning(f"MSTL decomposition failed: {str(e)}, using fallback")
        return _simple_decomposition(df)


def _simple_decomposition(df: pd.DataFrame) -> Dict[str, pd.Series]:
    """Fallback decomposition for short time series"""
    
    # Simple trend via rolling mean
    window = min(30, len(df) // 3)
    trend = df['clicks'].rolling(window=window, center=True).mean().fillna(method='bfill').fillna(method='ffill')
    
    # Detrend
    detrended = df['clicks'] - trend
    
    # Weekly seasonality via day-of-week averages
    df_temp = df.copy()
    df_temp['dow'] = pd.to_datetime(df_temp['date']).dt.dayofweek
    df_temp['detrended'] = detrended
    dow_avg = df_temp.groupby('dow')['detrended'].mean()
    seasonal_weekly = df_temp['dow'].map(dow_avg)
    
    # Residual
    resid = detrended - seasonal_weekly
    
    return {
        'trend': trend,
        'seasonal_weekly': seasonal_weekly,
        'seasonal_monthly': pd.Series(0, index=df.index),  # No monthly component
        'resid': resid
    }


def _analyze_trend(trend: pd.Series, df: pd.DataFrame) -> Dict[str, Any]:
    """
    Analyze trend component: fit linear regression, classify direction
    
    Returns:
        direction: strong_growth, growth, flat, decline, strong_decline
        slope_pct_per_month: percentage change per month
    """
    
    # Remove NaN values
    valid_mask = ~trend.isna()
    y = trend[valid_mask].values
    x = np.arange(len(y))
    
    if len(y) < 2:
        return {
            'direction': 'insufficient_data',
            'slope_pct_per_month': 0.0,
            'slope_absolute': 0.0,
            'r_squared': 0.0
        }
    
    # Linear regression
    slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)
    
    # Convert slope to percentage change per month
    # slope is in clicks/day, convert to clicks/month then to percentage
    baseline = np.median(y)
    if baseline > 0:
        slope_per_month = slope * 30  # 30 days
        slope_pct_per_month = (slope_per_month / baseline) * 100
    else:
        slope_pct_per_month = 0.0
    
    # Classify direction
    if slope_pct_per_month > 5:
        direction = 'strong_growth'
    elif slope_pct_per_month > 1:
        direction = 'growth'
    elif slope_pct_per_month > -1:
        direction = 'flat'
    elif slope_pct_per_month > -5:
        direction = 'decline'
    else:
        direction = 'strong_decline'
    
    return {
        'direction': direction,
        'slope_pct_per_month': round(slope_pct_per_month, 2),
        'slope_absolute': round(slope, 4),
        'r_squared': round(r_value ** 2, 3),
        'p_value': round(p_value, 4)
    }


def _detect_change_points(trend: pd.Series, df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Detect change points in trend using PELT algorithm
    
    Returns list of change points with date, magnitude, direction
    """
    
    # Remove NaN
    valid_mask = ~trend.isna()
    signal = trend[valid_mask].values
    dates = df.loc[valid_mask, 'date'].values
    
    if len(signal) < 30:
        logger.warning("Insufficient data for change point detection")
        return []
    
    try:
        # PELT algorithm (Pruned Exact Linear Time)
        # pen parameter controls sensitivity (higher = fewer change points)
        # Start with adaptive penalty based on data variance
        penalty = 3 * np.var(signal)
        
        algo = rpt.Pelt(model="rbf", min_size=7, jump=1).fit(signal)
        change_point_indices = algo.predict(pen=penalty)
        
        # Remove the final index (always included by ruptures)
        if change_point_indices and change_point_indices[-1] == len(signal):
            change_point_indices = change_point_indices[:-1]
        
        change_points = []
        
        for idx in change_point_indices:
            if idx <= 0 or idx >= len(signal) - 1:
                continue
            
            # Calculate magnitude as difference in local means before/after
            window = 7  # 1 week window
            before_start = max(0, idx - window)
            after_end = min(len(signal), idx + window)
            
            mean_before = np.mean(signal[before_start:idx])
            mean_after = np.mean(signal[idx:after_end])
            
            magnitude = (mean_after - mean_before) / mean_before if mean_before != 0 else 0
            
            # Determine direction
            if magnitude > 0.05:
                direction = 'rise'
            elif magnitude < -0.05:
                direction = 'drop'
            else:
                direction = 'shift'
            
            change_points.append({
                'date': pd.Timestamp(dates[idx]).isoformat(),
                'magnitude': round(magnitude, 3),
                'direction': direction,
                'index': int(idx)
            })
        
        # Sort by absolute magnitude, keep top 5 most significant
        change_points.sort(key=lambda x: abs(x['magnitude']), reverse=True)
        change_points = change_points[:5]
        
        # Re-sort by date
        change_points.sort(key=lambda x: x['date'])
        
        # Remove index (internal use only)
        for cp in change_points:
            del cp['index']
        
        logger.info(f"Detected {len(change_points)} significant change points")
        return change_points
        
    except Exception as e:
        logger.warning(f"Change point detection failed: {str(e)}")
        return []


def _analyze_seasonality(
    seasonal_weekly: pd.Series,
    seasonal_monthly: pd.Series,
    df: pd.DataFrame
) -> Dict[str, Any]:
    """
    Analyze seasonality patterns
    
    Returns:
        best_day: day of week with highest traffic
        worst_day: day of week with lowest traffic
        monthly_cycle: bool
        cycle_description: str
    """
    
    df_temp = df.copy()
    df_temp['dow'] = pd.to_datetime(df_temp['date']).dt.dayofweek
    df_temp['seasonal_weekly'] = seasonal_weekly.values
    
    # Day of week analysis
    dow_effects = df_temp.groupby('dow')['seasonal_weekly'].mean()
    
    day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    best_dow = int(dow_effects.idxmax())
    worst_dow = int(dow_effects.idxmin())
    
    best_day = day_names[best_dow]
    worst_day = day_names[worst_dow]
    
    # Check for significant weekly pattern
    weekly_range = dow_effects.max() - dow_effects.min()
    weekly_mean = abs(seasonal_weekly.mean())
    has_weekly_pattern = weekly_range > (0.1 * df['clicks'].mean())
    
    # Monthly cycle detection
    monthly_strength = seasonal_monthly.abs().mean()
    has_monthly_cycle = monthly_strength > (0.05 * df['clicks'].mean())
    
    # Describe monthly cycle if present
    cycle_description = None
    if has_monthly_cycle:
        # Find peak day of month
        df_temp['dom'] = pd.to_datetime(df_temp['date']).dt.day
        df_temp['seasonal_monthly'] = seasonal_monthly.values
        dom_effects = df_temp.groupby('dom')['seasonal_monthly'].mean()
        peak_dom = int(dom_effects.idxmax())
        peak_magnitude = (dom_effects.max() / df['clicks'].mean()) * 100
        
        if peak_dom <= 7:
            cycle_description = f"{abs(peak_magnitude):.0f}% traffic spike first week of each month"
        elif peak_dom >= 23:
            cycle_description = f"{abs(peak_magnitude):.0f}% traffic spike last week of each month"
        else:
            cycle_description = f"{abs(peak_magnitude):.0f}% traffic variation by time of month"
    
    return {
        'best_day': best_day,
        'worst_day': worst_day,
        'monthly_cycle': has_monthly_cycle,
        'cycle_description': cycle_description,
        'weekly_pattern_strength': round(weekly_range / df['clicks'].mean(), 3) if df['clicks'].mean() > 0 else 0
    }


def _detect_anomalies(resid: pd.Series, df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Detect anomalies using STUMPY matrix profile on residuals
    
    Returns list of anomalies (discords) with date, type, magnitude
    """
    
    # Remove NaN
    valid_mask = ~resid.isna()
    signal = resid[valid_mask].values
    dates = df.loc[valid_mask, 'date'].values
    
    if len(signal) < 30:
        logger.warning("Insufficient data for anomaly detection")
        return []
    
    try:
        # Normalize residuals
        signal_norm = (signal - np.mean(signal)) / (np.std(signal) + 1e-8)
        
        # Matrix profile with window = 7 days
        window_size = min(7, len(signal) // 4)
        if window_size < 3:
            window_size = 3
        
        # Compute matrix profile
        mp = stumpy.stump(signal_norm, m=window_size)
        
        # Extract matrix profile values and indices
        mp_values = mp[:, 0]  # First column is the matrix profile
        
        # Discords are subsequences with highest matrix profile values
        # (most dissimilar to all other subsequences)
        
        # Find top discords (above 95th percentile)
        threshold = np.percentile(mp_values, 95)
        discord_indices = np.where(mp_values > threshold)[0]
        
        anomalies = []
        
        # Group nearby indices to avoid duplicate detections
        if len(discord_indices) > 0:
            groups = []
            current_group = [discord_indices[0]]
            
            for idx in discord_indices[1:]:
                if idx - current_group[-1] <= window_size:
                    current_group.append(idx)
                else:
                    groups.append(current_group)
                    current_group = [idx]
            groups.append(current_group)
            
            # Take the peak from each group
            for group in groups:
                peak_idx = group[np.argmax([mp_values[i] for i in group])]
                
                # Calculate magnitude as standardized residual at this point
                magnitude = signal_norm[peak_idx]
                
                anomalies.append({
                    'date': pd.Timestamp(dates[peak_idx]).isoformat(),
                    'type': 'discord',
                    'magnitude': round(magnitude, 3),
                    'severity': 'high' if abs(magnitude) > 3 else 'medium'
                })
        
        # Sort by absolute magnitude
        anomalies.sort(key=lambda x: abs(x['magnitude']), reverse=True)
        
        # Keep top 10 most significant
        anomalies = anomalies[:10]
        
        # Re-sort by date
        anomalies.sort(key=lambda x: x['date'])
        
        logger.info(f"Detected {len(anomalies)} anomalies via matrix profile")
        return anomalies
        
    except Exception as e:
        logger.warning(f"Anomaly detection failed: {str(e)}")
        return []


def _generate_forecast(df: pd.DataFrame, decomposition: Dict[str, pd.Series]) -> Dict[str, Any]:
    """
    Generate 30/60/90 day forecasts using ARIMA
    
    Returns dict with 30d, 60d, 90d forecasts with confidence intervals
    """
    
    try:
        # Use clicks for forecasting
        ts = df['clicks'].values
        
        # Fit ARIMA model
        # Auto-select order using AIC (start with common ARIMA(1,1,1))
        # For speed, use fixed order rather than auto_arima
        model = ARIMA(ts, order=(1, 1, 1), seasonal_order=(0, 0, 0, 0))
        fitted = model.fit()
        
        # Forecast 90 days ahead
        forecast_result = fitted.forecast(steps=90, alpha=0.05)  # 95% CI
        
        # Get confidence intervals
        forecast_df = fitted.get_forecast(steps=90).summary_frame(alpha=0.05)
        
        # Extract predictions and intervals
        predictions = forecast_result.values if hasattr(forecast_result, 'values') else forecast_result
        ci_low = forecast_df['mean_ci_lower'].values
        ci_high = forecast_df['mean_ci_upper'].values
        
        # Format results for 30/60/90 day horizons
        forecast = {
            '30d': {
                'clicks': int(round(predictions[29])),
                'ci_low': int(round(ci_low[29])),
                'ci_high': int(round(ci_high[29]))
            },
            '60d': {
                'clicks': int(round(predictions[59])),
                'ci_low': int(round(ci_low[59])),
                'ci_high': int(round(ci_high[59]))
            },
            '90d': {
                'clicks': int(round(predictions[89])),
                'ci_low': int(round(ci_low[89])),
                'ci_high': int(round(ci_high[89]))
            }
        }
        
        # Add forecast trend
        forecast['trend'] = 'improving' if predictions[89] > predictions[0] else 'declining'
        forecast['confidence'] = 'medium'  # Could calculate based on CI width
        
        logger.info(f"Generated ARIMA forecast: 90d = {forecast['90d']['clicks']} clicks")
        return forecast
        
    except Exception as e:
        logger.warning(f"ARIMA forecasting failed: {str(e)}, using simple projection")
        return _simple_forecast(df)


def _simple_forecast(df: pd.DataFrame) -> Dict[str, Any]:
    """Fallback forecast using linear extrapolation"""
    
    # Simple linear trend on last 30 days
    recent = df.tail(30)['clicks'].values
    x = np.arange(len(recent))
    
    if len(recent) < 2:
        # No data for forecast
        current_avg = df['clicks'].mean()
        return {
            '30d': {'clicks': int(current_avg), 'ci_low': 0, 'ci_high': int(current_avg * 2)},
            '60d': {'clicks': int(current_avg), 'ci_low': 0, 'ci_high': int(current_avg * 2)},
            '90d': {'clicks': int(current_avg), 'ci_low': 0, 'ci_high': int(current_avg * 2)},
            'trend': 'unknown',
            'confidence': 'low'
        }
    
    slope, intercept = np.polyfit(x, recent, 1)
    
    # Project forward
    pred_30 = slope * (len(recent) + 30) + intercept
    pred_60 = slope * (len(recent) + 60) + intercept
    pred_90 = slope * (len(recent) + 90) + intercept
    
    # Simple CI based on recent variance
    std = np.std(recent)
    
    return {
        '30d': {
            'clicks': max(0, int(round(pred_30))),
            'ci_low': max(0, int(round(pred_30 - 2 * std))),