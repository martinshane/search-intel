"""
Module 01: Health & Trajectory Analysis
Analyzes overall site health and traffic trajectory using time-series decomposition,
change point detection, pattern discovery, and forecasting.
Error handling strategy:
- Graceful fallbacks for insufficient data (< 30 days)
- Decomposition failures fall back to simple linear regression
- Missing forecasts when statistical models fail
- Always return valid schema with partial results + error flags
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import logging

logger = logging.getLogger(__name__)

# Conditional imports with fallbacks
try:
    from statsmodels.tsa.seasonal import MSTL
    HAS_MSTL = True
except ImportError:
    HAS_MSTL = False
    logger.warning("statsmodels not available - will use fallback decomposition")

try:
    import stumpy
    HAS_STUMPY = True
except ImportError:
    HAS_STUMPY = False
    logger.warning("stumpy not available - pattern detection disabled")

try:
    import ruptures as rpt
    HAS_RUPTURES = True
except ImportError:
    HAS_RUPTURES = False
    logger.warning("ruptures not available - using scipy for change points")

try:
    from scipy import stats, signal
    from scipy.optimize import curve_fit
    HAS_SCIPY = True
except ImportError:
    HAS_SCIPY = False
    logger.warning("scipy not available - some features disabled")

try:
    from statsmodels.tsa.arima.model import ARIMA
    from statsmodels.tsa.statespace.sarimax import SARIMAX
    HAS_ARIMA = True
except ImportError:
    HAS_ARIMA = False
    logger.warning("ARIMA not available - will use simple extrapolation")


def _validate_input_data(daily_data: pd.DataFrame) -> tuple[bool, Optional[str]]:
    """
    Validate input data meets minimum requirements.
    Returns:
        (is_valid, error_message)
    """
    if daily_data is None or daily_data.empty:
        return False, "No data provided"

    if len(daily_data) < 30:
        return False, f"Insufficient data: {len(daily_data)} days (minimum 30 required)"

    required_cols = ['date', 'clicks']
    missing_cols = [col for col in required_cols if col not in daily_data.columns]
    if missing_cols:
        return False, f"Missing required columns: {missing_cols}"

    if daily_data['clicks'].isna().all():
        return False, "All click values are null"

    return True, None


def _simple_trend_analysis(daily_data: pd.DataFrame) -> Dict[str, Any]:
    """
    Fallback trend analysis using simple linear regression.
    Used when MSTL decomposition fails or insufficient data.
    """
    try:
        # Prepare data
        df = daily_data.copy()
        df = df.sort_values('date')
        df['days_since_start'] = (df['date'] - df['date'].min()).dt.days

        # Fill missing values with forward fill then backward fill
        df['clicks'] = df['clicks'].fillna(method='ffill').fillna(method='bfill').fillna(0)

        # Simple linear regression
        if HAS_SCIPY:
            slope, intercept, r_value, p_value, std_err = stats.linregress(
                df['days_since_start'],
                df['clicks']
            )
        else:
            # Fallback to numpy polyfit
            coeffs = np.polyfit(df['days_since_start'], df['clicks'], 1)
            slope, intercept = coeffs[0], coeffs[1]
            r_value = 0.0

        # Calculate trend direction
        avg_clicks = df['clicks'].mean()
        if avg_clicks > 0:
            monthly_change_pct = (slope * 30 / avg_clicks) * 100
        else:
            monthly_change_pct = 0.0

        # Classify direction
        if monthly_change_pct > 5:
            direction = "strong_growth"
        elif monthly_change_pct > 1:
            direction = "growth"
        elif monthly_change_pct > -1:
            direction = "flat"
        elif monthly_change_pct > -5:
            direction = "decline"
        else:
            direction = "strong_decline"

        # Simple day-of-week seasonality
        df['day_of_week'] = df['date'].dt.day_name()
        dow_avg = df.groupby('day_of_week')['clicks'].mean()
        best_day = dow_avg.idxmax() if not dow_avg.empty else "Unknown"
        worst_day = dow_avg.idxmin() if not dow_avg.empty else "Unknown"

        return {
            "trend_component": df['clicks'].tolist(),
            "seasonal_component": None,
            "residual_component": None,
            "slope": float(slope),
            "direction": direction,
            "monthly_change_pct": float(monthly_change_pct),
            "best_day": best_day,
            "worst_day": worst_day,
            "method": "simple_linear_regression"
        }
    except Exception as e:
        logger.error(f"Simple trend analysis failed: {e}")
        return {
            "trend_component": None,
            "seasonal_component": None,
            "residual_component": None,
            "slope": 0.0,
            "direction": "unknown",
            "monthly_change_pct": 0.0,
            "best_day": "Unknown",
            "worst_day": "Unknown",
            "method": "failed",
            "error": str(e)
        }


def _decompose_time_series(daily_data: pd.DataFrame) -> Dict[str, Any]:
    """
    Decompose time series using MSTL or fallback to simple analysis.
    """
    if not HAS_MSTL:
        logger.info("MSTL not available, using simple trend analysis")
        return _simple_trend_analysis(daily_data)

    try:
        df = daily_data.copy()
        df = df.sort_values('date')
        df = df.set_index('date')

        # Fill missing values
        df['clicks'] = df['clicks'].fillna(method='ffill').fillna(method='bfill').fillna(0)

        # Ensure we have enough data for decomposition
        if len(df) < 60:  # Need at least 2 months for weekly + monthly seasonality
            logger.info(f"Insufficient data for MSTL ({len(df)} days), using simple analysis")
            return _simple_trend_analysis(daily_data)

        # MSTL decomposition with weekly and monthly periods
        mstl = MSTL(df['clicks'], periods=[7, 30], stl_kwargs={'seasonal_deg': 0})
        result = mstl.fit()

        # Extract components
        trend = result.trend
        seasonal = result.seasonal
        resid = result.resid

        # Calculate trend slope
        trend_values = trend.dropna().values
        if len(trend_values) > 1:
            x = np.arange(len(trend_values))
            slope = np.polyfit(x, trend_values, 1)[0]
            avg_clicks = trend_values.mean()
            if avg_clicks > 0:
                monthly_change_pct = (slope * 30 / avg_clicks) * 100
            else:
                monthly_change_pct = 0.0
        else:
            slope = 0.0
            monthly_change_pct = 0.0

        # Classify direction
        if monthly_change_pct > 5:
            direction = "strong_growth"
        elif monthly_change_pct > 1:
            direction = "growth"
        elif monthly_change_pct > -1:
            direction = "flat"
        elif monthly_change_pct > -5:
            direction = "decline"
        else:
            direction = "strong_decline"

        # Day of week analysis from seasonal component
        df_temp = df.reset_index()
        df_temp['day_of_week'] = df_temp['date'].dt.day_name()
        df_temp['seasonal'] = seasonal.values if len(seasonal) == len(df_temp) else 0
        dow_seasonal = df_temp.groupby('day_of_week')['seasonal'].mean()
        best_day = dow_seasonal.idxmax() if not dow_seasonal.empty else "Unknown"
        worst_day = dow_seasonal.idxmin() if not dow_seasonal.empty else "Unknown"

        return {
            "trend_component": trend.tolist(),
            "seasonal_component": seasonal.tolist(),
            "residual_component": resid.tolist(),
            "slope": float(slope),
            "direction": direction,
            "monthly_change_pct": float(monthly_change_pct),
            "best_day": best_day,
            "worst_day": worst_day,
            "method": "mstl"
        }
    except Exception as e:
        logger.warning(f"MSTL decomposition failed: {e}, falling back to simple analysis")
        return _simple_trend_analysis(daily_data)


def _detect_change_points(trend_data: List[float], dates: pd.Series) -> List[Dict[str, Any]]:
    """
    Detect change points in trend component using ruptures or scipy fallback.
    """
    if trend_data is None or len(trend_data) < 30:
        return []

    try:
        trend_array = np.array([x for x in trend_data if x is not None and not np.isnan(x)])
        if len(trend_array) < 30:
            return []

        change_points = []

        if HAS_RUPTURES:
            # Use PELT algorithm for change point detection
            try:
                algo = rpt.Pelt(model="rbf", min_size=7, jump=1).fit(trend_array)
                result = algo.predict(pen=10)

                # Convert indices to change points with magnitude
                for idx in result[:-1]:  # Last point is always end of series
                    if idx > 0 and idx < len(trend_array) - 1:
                        magnitude = (trend_array[idx] - trend_array[idx-1]) / (trend_array[idx-1] + 1)
                        direction = "increase" if magnitude > 0 else "drop"

                        # Map back to date
                        date_idx = min(idx, len(dates) - 1)
                        change_date = dates.iloc[date_idx]

                        change_points.append({
                            "date": change_date.strftime("%Y-%m-%d") if hasattr(change_date, 'strftime') else str(change_date),
                            "magnitude": float(magnitude),
                            "direction": direction
                        })
            except Exception as e:
                logger.warning(f"PELT algorithm failed: {e}, using scipy fallback")

        # Fallback to simple threshold-based detection if ruptures failed or not available
        if not change_points and HAS_SCIPY:
            # Use find_peaks on absolute differences
            diffs = np.diff(trend_array)
            abs_diffs = np.abs(diffs)
            threshold = np.percentile(abs_diffs, 90)  # Top 10% of changes

            peaks, _ = signal.find_peaks(abs_diffs, height=threshold, distance=7)

            for idx in peaks:
                if idx < len(trend_array) - 1:
                    magnitude = diffs[idx] / (trend_array[idx] + 1)
                    direction = "increase" if magnitude > 0 else "drop"

                    date_idx = min(idx, len(dates) - 1)
                    change_date = dates.iloc[date_idx]

                    change_points.append({
                        "date": change_date.strftime("%Y-%m-%d") if hasattr(change_date, 'strftime') else str(change_date),
                        "magnitude": float(magnitude),
                        "direction": direction
                    })

        # Sort by magnitude and keep top 5 most significant
        change_points.sort(key=lambda x: abs(x['magnitude']), reverse=True)
        return change_points[:5]

    except Exception as e:
        logger.error(f"Change point detection failed: {e}")
        return []


def _detect_patterns(residual_data: List[float]) -> Dict[str, Any]:
    """
    Detect recurring patterns and anomalies using STUMPY or fallback.
    """
    if not HAS_STUMPY or residual_data is None:
        return {
            "motifs": [],
            "anomalies": [],
            "method": "disabled"
        }

    try:
        resid_array = np.array([x for x in residual_data if x is not None and not np.isnan(x)])

        if len(resid_array) < 50:  # Need sufficient data for pattern detection
            return {
                "motifs": [],
                "anomalies": [],
                "method": "insufficient_data"
            }

        # Compute matrix profile with window of 7 days
        m = 7
        if len(resid_array) > m:
            mp = stumpy.stump(resid_array, m=m)

            # Find motifs (recurring patterns) - lowest matrix profile values
            motif_idx = np.argsort(mp[:, 0])[:3]  # Top 3 motifs

            # Find discords (anomalies) - highest matrix profile values
            discord_idx = np.argsort(mp[:, 0])[-3:]  # Top 3 anomalies

            motifs = [{"index": int(idx), "score": float(mp[idx, 0])} for idx in motif_idx]
            anomalies = [{"index": int(idx), "score": float(mp[idx, 0])} for idx in discord_idx]

            return {
                "motifs": motifs,
                "anomalies": anomalies,
                "method": "stumpy"
            }
        else:
            return {
                "motifs": [],
                "anomalies": [],
                "method": "insufficient_data"
            }
    except Exception as e:
        logger.error(f"Pattern detection failed: {e}")
        return {
            "motifs": [],
            "anomalies": [],
            "method": "failed",
            "error": str(e)
        }


def _forecast_trend(daily_data: pd.DataFrame, decomposition: Dict[str, Any]) -> Dict[str, Dict[str, float]]:
    """
    Forecast future trend using ARIMA or simple extrapolation.
    """
    try:
        df = daily_data.copy()
        df = df.sort_values('date')
        df['clicks'] = df['clicks'].fillna(method='ffill').fillna(method='bfill').fillna(0)
        clicks_series = df['clicks'].values

        if len(clicks_series) < 30:
            return _simple_forecast(clicks_series, decomposition.get('slope', 0))

        if HAS_ARIMA:
            try:
                # Try ARIMA forecasting
                model = ARIMA(clicks_series, order=(1, 1, 1))
                fitted = model.fit()

                # Forecast 30, 60, 90 days
                forecast_30 = fitted.forecast(steps=30)
                forecast_60 = fitted.forecast(steps=60)
                forecast_90 = fitted.forecast(steps=90)

                # Get confidence intervals (approximate)
                std_err = np.std(fitted.resid) if hasattr(fitted, 'resid') else np.std(clicks_series) * 0.1

                return {
                    "30d": {
                        "clicks": float(forecast_30[-1]),
                        "ci_low": float(forecast_30[-1] - 1.96 * std_err),
                        "ci_high": float(forecast_30[-1] + 1.96 * std_err)
                    },
                    "60d": {
                        "clicks": float(forecast_60[-1]),
                        "ci_low": float(forecast_60[-1] - 1.96 * std_err * 1.5),
                        "ci_high": float(forecast_60[-1] + 1.96 * std_err * 1.5)
                    },
                    "90d": {
                        "clicks": float(forecast_90[-1]),
                        "ci_low": float(forecast_90[-1] - 1.96 * std_err * 2),
                        "ci_high": float(forecast_90[-1] + 1.96 * std_err * 2)
                    },
                    "method": "arima"
                }
            except Exception as e:
                logger.warning(f"ARIMA forecasting failed: {e}, using simple extrapolation")
                return _simple_forecast(clicks_series, decomposition.get('slope', 0))
        else:
            return _simple_forecast(clicks_series, decomposition.get('slope', 0))

    except Exception as e:
        logger.error(f"Forecasting failed: {e}")
        return _simple_forecast([0], 0)


def _simple_forecast(clicks_series: np.ndarray, slope: float) -> Dict[str, Dict[str, float]]:
    """
    Simple linear extrapolation forecast fallback.
    """
    try:
        current_avg = np.mean(clicks_series[-30:]) if len(clicks_series) >= 30 else np.mean(clicks_series)
        std_dev = np.std(clicks_series[-30:]) if len(clicks_series) >= 30 else np.std(clicks_series)

        if std_dev == 0 or np.isnan(std_dev):
            std_dev = current_avg * 0.1  # Assume 10% variation

        forecast_30 = current_avg + (slope * 30)
        forecast_60 = current_avg + (slope * 60)
        forecast_90 = current_avg + (slope * 90)

        return {
            "30d": {
                "clicks": float(max(0, forecast_30)),
                "ci_low": float(max(0, forecast_30 - 1.96 * std_dev)),
                "ci_high": float(forecast_30 + 1.96 * std_dev)
            },
            "60d": {
                "clicks": float(max(0, forecast_60)),
                "ci_low": float(max(0, forecast_60 - 1.96 * std_dev * 1.5)),
                "ci_high": float(forecast_60 + 1.96 * std_dev * 1.5)
            },
            "90d": {
                "clicks": float(max(0, forecast_90)),
                "ci_low": float(max(0, forecast_90 - 1.96 * std_dev * 2)),
                "ci_high": float(forecast_90 + 1.96 * std_dev * 2)
            },
            "method": "linear_extrapolation"
        }
    except Exception as e:
        logger.error(f"Simple forecast failed: {e}")
        return {
            "30d": {"clicks": 0, "ci_low": 0, "ci_high": 0},
            "60d": {"clicks": 0, "ci_low": 0, "ci_high": 0},
            "90d": {"clicks": 0, "ci_low": 0, "ci_high": 0},
            "method": "failed"
        }


def analyze_health_trajectory(daily_data: pd.DataFrame) -> dict:
    """
    Analyze overall site health and traffic trajectory.

    Args:
        daily_data: DataFrame with columns ['date', 'clicks', 'impressions']
                    Date should be datetime, 16 months of daily data

    Returns:
        Dictionary with health metrics, trend analysis, change points,
        seasonality patterns, anomalies, and forecast.
        Always returns valid schema even with partial failures.
    """
    # Validate input
    is_valid, error_msg = _validate_input_data(daily_data)
    if not is_valid:
        logger.error(f"Invalid input data: {error_msg}")
        return {
            "overall_direction": "unknown",
            "trend_slope_pct_per_month": 0.0,
            "change_points": [],
            "seasonality": {
                "best_day": "Unknown",
                "worst_day": "Unknown",
                "monthly_cycle": False,
                "cycle_description": "Insufficient data for analysis"
            },
            "anomalies": [],
            "forecast": {
                "30d": {"clicks": 0, "ci_low": 0, "ci_high": 0},
                "60d": {"clicks": 0, "ci_low": 0, "ci_high": 0},
                "90d": {"clicks": 0, "ci_low": 0, "ci_high": 0}
            },
            "data_quality": {
                "sufficient_data": False,
                "days_analyzed": len(daily_data) if daily_data is not None else 0,
                "error": error_msg
            }
        }

    try:
        # 1. Decompose time series
        logger.info("Starting time series decomposition")
        decomposition = _decompose_time_series(daily_data)

        # 2. Detect change points
        logger.info("Detecting change points")
        change_points = _detect_change_points(
            decomposition.get('trend_component'),
            daily_data.sort_values('date')['date']
        )

        # 3. Detect patterns and anomalies
        logger.info("Detecting patterns and anomalies")
        patterns = _detect_patterns(decomposition.get('residual_component'))

        # Map anomalies to dates
        anomalies_with_dates = []
        if patterns.get('anomalies'):
            df_sorted = daily_data.sort_values('date')
            for anomaly in patterns['anomalies']:
                idx = anomaly['index']
                if idx < len(df_sorted):
                    anomalies_with_dates.append({
                        "date": df_sorted.iloc[idx]['date'].strftime("%Y-%m-%d"),
                        "type": "discord",
                        "magnitude": anomaly['score']
                    })

        # 4. Forecast future trend
        logger.info("Generating forecast")
        forecast = _forecast_trend(daily_data, decomposition)

        # 5. Determine monthly cycle presence
        has_monthly_cycle = False
        cycle_description = "No significant monthly pattern detected"

        if decomposition.get('seasonal_component') is not None:
            seasonal = decomposition['seasonal_component']
            # Simple heuristic: check if there's meaningful variation in seasonal component
            if len(seasonal) > 30:
                seasonal_array = np.array(seasonal)
                # Check for ~30-day periodicity by looking at autocorrelation
                seasonal_std = np.std(seasonal_array)
                seasonal_range = np.max(seasonal_array) - np.min(seasonal_array)
                avg_clicks = daily_data['clicks'].mean()

                if avg_clicks > 0 and seasonal_range / avg_clicks > 0.1:
                    has_monthly_cycle = True
                    cycle_description = (
                        f"Monthly cycle detected with amplitude of "
                        f"{seasonal_range:.0f} clicks "
                        f"({seasonal_range / avg_clicks * 100:.1f}% of average)"
                    )
                else:
                    cycle_description = (
                        "Seasonal component present but monthly variation is minimal "
                        f"({seasonal_range / max(avg_clicks, 1) * 100:.1f}% of average)"
                    )

        # 6. Assemble final result
        result = {
            "overall_direction": decomposition.get("direction", "unknown"),
            "trend_slope_pct_per_month": round(decomposition.get("monthly_change_pct", 0.0), 2),
            "change_points": change_points,
            "seasonality": {
                "best_day": decomposition.get("best_day", "Unknown"),
                "worst_day": decomposition.get("worst_day", "Unknown"),
                "monthly_cycle": has_monthly_cycle,
                "cycle_description": cycle_description
            },
            "anomalies": anomalies_with_dates,
            "forecast": forecast,
            "data_quality": {
                "sufficient_data": True,
                "days_analyzed": len(daily_data),
                "decomposition_method": decomposition.get("method", "unknown"),
                "pattern_method": patterns.get("method", "unknown")
            }
        }

        logger.info(
            f"Health analysis complete: direction={result['overall_direction']}, "
            f"slope={result['trend_slope_pct_per_month']}%/mo, "
            f"change_points={len(change_points)}, anomalies={len(anomalies_with_dates)}"
        )

        return result

    except Exception as e:
        logger.error(f"Health trajectory analysis failed: {e}", exc_info=True)
        return {
            "overall_direction": "error",
            "trend_slope_pct_per_month": 0.0,
            "change_points": [],
            "seasonality": {
                "best_day": "Unknown",
                "worst_day": "Unknown",
                "monthly_cycle": False,
                "cycle_description": "Analysis failed"
            },
            "anomalies": [],
            "forecast": {
                "30d": {"clicks": 0, "ci_low": 0, "ci_high": 0},
                "60d": {"clicks": 0, "ci_low": 0, "ci_high": 0},
                "90d": {"clicks": 0, "ci_low": 0, "ci_high": 0}
            },
            "data_quality": {
                "sufficient_data": False,
                "days_analyzed": len(daily_data) if daily_data is not None else 0,
                "error": str(e)
            }
        }
