"""
Module 1: Health & Trajectory Analysis

Performs time series decomposition, change point detection, anomaly detection,
and forecasting on site-wide GSC traffic data.

Dependencies:
- statsmodels (MSTL decomposition)
- ruptures (change point detection via PELT)
- stumpy (matrix profile for motifs/discords)
- scipy (curve fitting)
- numpy, pandas
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import logging

try:
    from statsmodels.tsa.seasonal import MSTL
    from statsmodels.tsa.arima.model import ARIMA
except ImportError:
    MSTL = None
    ARIMA = None

try:
    import ruptures as rpt
except ImportError:
    rpt = None

try:
    import stumpy
except ImportError:
    stumpy = None

from scipy import stats
from scipy.optimize import curve_fit

logger = logging.getLogger(__name__)


class HealthTrajectoryAnalyzer:
    """Analyzes site health and traffic trajectory using advanced time series methods."""
    
    def __init__(self):
        self.required_days = 90  # Minimum days needed for meaningful analysis
        
    def analyze(self, daily_data: pd.DataFrame) -> Dict[str, Any]:
        """
        Main analysis entry point.
        
        Args:
            daily_data: DataFrame with columns ['date', 'clicks', 'impressions', 'ctr', 'position']
                       Must have at least 90 days of data, ideally 12-16 months
        
        Returns:
            Dictionary with health and trajectory analysis results
        """
        try:
            # Validate input
            if daily_data is None or len(daily_data) == 0:
                return self._empty_result("No data provided")
            
            if len(daily_data) < self.required_days:
                return self._empty_result(f"Insufficient data: {len(daily_data)} days (need {self.required_days})")
            
            # Ensure date column is datetime and sorted
            daily_data = daily_data.copy()
            daily_data['date'] = pd.to_datetime(daily_data['date'])
            daily_data = daily_data.sort_values('date').reset_index(drop=True)
            
            # Check for required columns
            required_cols = ['date', 'clicks', 'impressions']
            missing_cols = [col for col in required_cols if col not in daily_data.columns]
            if missing_cols:
                return self._empty_result(f"Missing required columns: {missing_cols}")
            
            # Fill any gaps in date range
            daily_data = self._fill_date_gaps(daily_data)
            
            results = {
                "data_range": {
                    "start_date": daily_data['date'].min().strftime('%Y-%m-%d'),
                    "end_date": daily_data['date'].max().strftime('%Y-%m-%d'),
                    "days": len(daily_data)
                }
            }
            
            # 1. Decomposition
            decomposition = self._decompose_time_series(daily_data['clicks'].values)
            results['decomposition'] = decomposition
            
            # 2. Trend analysis
            trend_analysis = self._analyze_trend(decomposition.get('trend'))
            results.update(trend_analysis)
            
            # 3. Change point detection
            change_points = self._detect_change_points(
                daily_data['clicks'].values,
                daily_data['date'].values
            )
            results['change_points'] = change_points
            
            # 4. Seasonality analysis
            seasonality = self._analyze_seasonality(
                decomposition.get('seasonal_weekly'),
                decomposition.get('seasonal_monthly'),
                daily_data['date'].values
            )
            results['seasonality'] = seasonality
            
            # 5. Anomaly detection (motifs and discords)
            anomalies = self._detect_anomalies(
                decomposition.get('resid'),
                daily_data['date'].values
            )
            results['anomalies'] = anomalies
            
            # 6. Forecast
            forecast = self._generate_forecast(
                daily_data['clicks'].values,
                decomposition
            )
            results['forecast'] = forecast
            
            return results
            
        except Exception as e:
            logger.error(f"Error in health trajectory analysis: {str(e)}", exc_info=True)
            return self._empty_result(f"Analysis error: {str(e)}")
    
    def _fill_date_gaps(self, df: pd.DataFrame) -> pd.DataFrame:
        """Fill any missing dates in the time series with zeros."""
        date_range = pd.date_range(start=df['date'].min(), end=df['date'].max(), freq='D')
        full_df = pd.DataFrame({'date': date_range})
        merged = full_df.merge(df, on='date', how='left')
        
        # Fill missing numeric values with 0
        numeric_cols = ['clicks', 'impressions', 'ctr', 'position']
        for col in numeric_cols:
            if col in merged.columns:
                merged[col] = merged[col].fillna(0)
        
        return merged
    
    def _decompose_time_series(self, data: np.ndarray) -> Dict[str, Any]:
        """
        Perform MSTL decomposition with weekly and monthly seasonality.
        
        Returns:
            Dict with trend, seasonal components, and residuals
        """
        if MSTL is None:
            logger.warning("statsmodels not available, skipping decomposition")
            return {
                'trend': data,
                'seasonal_weekly': np.zeros_like(data),
                'seasonal_monthly': np.zeros_like(data),
                'resid': np.zeros_like(data)
            }
        
        try:
            # MSTL expects periods as a list
            # 7 for weekly, 30 for monthly cycles
            mstl = MSTL(data, periods=[7, 30], stl_kwargs={'seasonal': 7})
            result = mstl.fit()
            
            # Extract components
            trend = result.trend
            seasonal = result.seasonal
            resid = result.resid
            
            # Separate seasonal components (first is weekly, second is monthly)
            seasonal_weekly = seasonal[:, 0] if seasonal.shape[1] > 0 else np.zeros_like(data)
            seasonal_monthly = seasonal[:, 1] if seasonal.shape[1] > 1 else np.zeros_like(data)
            
            return {
                'trend': trend,
                'seasonal_weekly': seasonal_weekly,
                'seasonal_monthly': seasonal_monthly,
                'resid': resid
            }
            
        except Exception as e:
            logger.warning(f"MSTL decomposition failed: {str(e)}, using simple moving average")
            # Fallback: simple trend extraction via moving average
            window = min(30, len(data) // 4)
            trend = pd.Series(data).rolling(window=window, center=True).mean().fillna(method='bfill').fillna(method='ffill').values
            return {
                'trend': trend,
                'seasonal_weekly': np.zeros_like(data),
                'seasonal_monthly': np.zeros_like(data),
                'resid': data - trend
            }
    
    def _analyze_trend(self, trend: Optional[np.ndarray]) -> Dict[str, Any]:
        """
        Analyze trend component to determine direction and magnitude.
        
        Returns:
            Dict with overall_direction, trend_slope_pct_per_month, etc.
        """
        if trend is None or len(trend) == 0:
            return {
                'overall_direction': 'unknown',
                'trend_slope_pct_per_month': 0.0,
                'trend_confidence': 0.0
            }
        
        # Fit linear regression on trend
        x = np.arange(len(trend))
        
        # Remove any NaN values
        valid_mask = ~np.isnan(trend)
        if valid_mask.sum() < 10:
            return {
                'overall_direction': 'insufficient_data',
                'trend_slope_pct_per_month': 0.0,
                'trend_confidence': 0.0
            }
        
        x_valid = x[valid_mask]
        trend_valid = trend[valid_mask]
        
        slope, intercept, r_value, p_value, std_err = stats.linregress(x_valid, trend_valid)
        
        # Convert slope to % change per month
        # slope is per day, multiply by 30 for monthly
        # divide by mean to get percentage
        mean_traffic = np.mean(trend_valid)
        if mean_traffic > 0:
            slope_pct_per_month = (slope * 30 / mean_traffic) * 100
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
            'overall_direction': direction,
            'trend_slope_pct_per_month': round(slope_pct_per_month, 2),
            'trend_confidence': round(r_value ** 2, 3),  # R² value
            'trend_p_value': round(p_value, 4)
        }
    
    def _detect_change_points(self, data: np.ndarray, dates: np.ndarray) -> List[Dict[str, Any]]:
        """
        Detect structural breaks in the time series using PELT algorithm.
        
        Returns:
            List of change points with date and magnitude
        """
        if rpt is None:
            logger.warning("ruptures library not available, skipping change point detection")
            return []
        
        try:
            # Use PELT with rbf kernel
            algo = rpt.Pelt(model="rbf", min_size=7, jump=1).fit(data)
            
            # Detect with penalty tuned to avoid over-segmentation
            penalty_value = np.log(len(data)) * np.var(data)
            change_indices = algo.predict(pen=penalty_value)
            
            # Remove the last index (end of series)
            change_indices = [idx for idx in change_indices if idx < len(data)]
            
            change_points = []
            for idx in change_indices:
                if idx > 0 and idx < len(data):
                    # Calculate magnitude as relative change
                    before_mean = np.mean(data[max(0, idx-14):idx])
                    after_mean = np.mean(data[idx:min(len(data), idx+14)])
                    
                    if before_mean > 0:
                        magnitude = (after_mean - before_mean) / before_mean
                    else:
                        magnitude = 0.0
                    
                    change_points.append({
                        'date': pd.Timestamp(dates[idx]).strftime('%Y-%m-%d'),
                        'magnitude': round(magnitude, 3),
                        'direction': 'increase' if magnitude > 0 else 'drop'
                    })
            
            # Sort by absolute magnitude and keep top 5
            change_points.sort(key=lambda x: abs(x['magnitude']), reverse=True)
            return change_points[:5]
            
        except Exception as e:
            logger.warning(f"Change point detection failed: {str(e)}")
            return []
    
    def _analyze_seasonality(
        self,
        weekly_seasonal: Optional[np.ndarray],
        monthly_seasonal: Optional[np.ndarray],
        dates: np.ndarray
    ) -> Dict[str, Any]:
        """
        Analyze seasonal components to identify patterns.
        
        Returns:
            Dict with best/worst days, monthly cycles, etc.
        """
        result = {
            'best_day': None,
            'worst_day': None,
            'monthly_cycle': False,
            'cycle_description': None
        }
        
        if weekly_seasonal is not None and len(weekly_seasonal) >= 7:
            # Reshape into weeks and average each day of week
            # Pad to make divisible by 7
            pad_length = (7 - len(weekly_seasonal) % 7) % 7
            padded = np.pad(weekly_seasonal, (0, pad_length), mode='edge')
            weeks = padded.reshape(-1, 7)
            day_averages = np.mean(weeks, axis=0)
            
            days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            best_idx = np.argmax(day_averages)
            worst_idx = np.argmin(day_averages)
            
            result['best_day'] = days[best_idx]
            result['worst_day'] = days[worst_idx]
        
        if monthly_seasonal is not None and len(monthly_seasonal) >= 30:
            # Check if monthly seasonal component is significant
            monthly_std = np.std(monthly_seasonal)
            if monthly_std > np.std(weekly_seasonal) * 0.5 if weekly_seasonal is not None else monthly_std > 0:
                result['monthly_cycle'] = True
                
                # Find peak within month (approx)
                # Take last 30 days as representative month
                last_month = monthly_seasonal[-30:]
                peak_day = np.argmax(last_month) + 1
                magnitude = (np.max(last_month) - np.mean(last_month)) / np.mean(np.abs(last_month)) if np.mean(np.abs(last_month)) > 0 else 0
                
                if magnitude > 0.1:
                    result['cycle_description'] = f"Traffic spike around day {peak_day} of month ({magnitude*100:.0f}% above average)"
        
        return result
    
    def _detect_anomalies(self, residuals: Optional[np.ndarray], dates: np.ndarray) -> List[Dict[str, Any]]:
        """
        Use STUMPY matrix profile to detect anomalies (discords).
        
        Returns:
            List of anomalies with date, type, and magnitude
        """
        if stumpy is None or residuals is None or len(residuals) < 30:
            return []
        
        try:
            # Use a window size of 7 days for pattern matching
            window_size = min(7, len(residuals) // 4)
            if window_size < 3:
                return []
            
            # Compute matrix profile
            mp = stumpy.stump(residuals, m=window_size)
            
            # Find top discords (anomalies)
            # Discord index is where matrix profile distance is highest
            discord_indices = np.argsort(mp[:, 0])[-5:]  # Top 5 discords
            
            anomalies = []
            for idx in discord_indices:
                if idx >= 0 and idx < len(dates):
                    # Calculate magnitude as z-score
                    window = residuals[max(0, idx-window_size):min(len(residuals), idx+window_size)]
                    magnitude = (residuals[idx] - np.mean(window)) / (np.std(window) + 1e-6)
                    
                    if abs(magnitude) > 2:  # At least 2 standard deviations
                        anomalies.append({
                            'date': pd.Timestamp(dates[idx]).strftime('%Y-%m-%d'),
                            'type': 'discord',
                            'magnitude': round(magnitude, 2)
                        })
            
            return anomalies
            
        except Exception as e:
            logger.warning(f"Anomaly detection failed: {str(e)}")
            return []
    
    def _generate_forecast(self, data: np.ndarray, decomposition: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate 30/60/90 day forecast using ARIMA on detrended/deseasonalized data.
        
        Returns:
            Dict with forecasts and confidence intervals
        """
        if ARIMA is None or len(data) < 60:
            # Simple baseline forecast using recent average
            recent_mean = np.mean(data[-30:])
            return {
                '30d': {'clicks': int(recent_mean * 30), 'ci_low': None, 'ci_high': None},
                '60d': {'clicks': int(recent_mean * 60), 'ci_low': None, 'ci_high': None},
                '90d': {'clicks': int(recent_mean * 90), 'ci_low': None, 'ci_high': None}
            }
        
        try:
            # Fit ARIMA on the data
            # Use auto order selection with reasonable bounds
            model = ARIMA(data, order=(1, 1, 1), seasonal_order=(1, 0, 1, 7))
            fitted = model.fit()
            
            # Forecast 90 days ahead
            forecast_result = fitted.forecast(steps=90)
            forecast_values = forecast_result
            
            # Get confidence intervals (approximate if not available)
            # Use last 30 days std as proxy for uncertainty
            recent_std = np.std(data[-30:])
            
            result = {}
            for days, label in [(30, '30d'), (60, '60d'), (90, '90d')]:
                forecast_mean = np.mean(forecast_values[:days])
                
                # Confidence intervals widen with time
                uncertainty = recent_std * np.sqrt(days / 30)
                
                result[label] = {
                    'clicks': int(forecast_mean * days),
                    'ci_low': int((forecast_mean - 1.96 * uncertainty) * days),
                    'ci_high': int((forecast_mean + 1.96 * uncertainty) * days)
                }
            
            return result
            
        except Exception as e:
            logger.warning(f"ARIMA forecast failed: {str(e)}, using simple projection")
            # Fallback to trend-based projection
            trend = decomposition.get('trend')
            if trend is not None and len(trend) > 30:
                recent_trend = trend[-30:]
                x = np.arange(len(recent_trend))
                slope, intercept = np.polyfit(x, recent_trend, 1)
                
                result = {}
                for days, label in [(30, '30d'), (60, '60d'), (90, '90d')]:
                    projected = intercept + slope * (len(recent_trend) + days)
                    uncertainty = np.std(recent_trend) * np.sqrt(days / 30)
                    
                    result[label] = {
                        'clicks': int(max(0, projected * days)),
                        'ci_low': int(max(0, (projected - 1.96 * uncertainty) * days)),
                        'ci_high': int(max(0, (projected + 1.96 * uncertainty) * days))
                    }
                
                return result
            
            # Ultimate fallback
            recent_mean = np.mean(data[-30:])
            return {
                '30d': {'clicks': int(recent_mean * 30), 'ci_low': None, 'ci_high': None},
                '60d': {'clicks': int(recent_mean * 60), 'ci_low': None, 'ci_high': None},
                '90d': {'clicks': int(recent_mean * 90), 'ci_low': None, 'ci_high': None}
            }
    
    def _empty_result(self, reason: str) -> Dict[str, Any]:
        """Return empty result structure with error message."""
        return {
            'error': reason,
            'overall_direction': 'unknown',
            'trend_slope_pct_per_month': 0.0,
            'change_points': [],
            'seasonality': {
                'best_day': None,
                'worst_day': None,
                'monthly_cycle': False,
                'cycle_description': None
            },
            'anomalies': [],
            'forecast': {
                '30d': {'clicks': None, 'ci_low': None, 'ci_high': None},
                '60d': {'clicks': None, 'ci_low': None, 'ci_high': None},
                '90d': {'clicks': None, 'ci_low': None, 'ci_high': None}
            }
        }


def analyze_health_trajectory(daily_data: pd.DataFrame) -> Dict[str, Any]:
    """
    Main entry point for Module 1 analysis.
    
    Args:
        daily_data: DataFrame with GSC daily time series data
        
    Returns:
        Dict containing health and trajectory analysis results
    """
    analyzer = HealthTrajectoryAnalyzer()
    return analyzer.analyze(daily_data)
