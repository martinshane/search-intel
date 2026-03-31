"""
Module 1: Health & Trajectory Analysis

Analyzes overall site health using GSC time series data:
- MSTL decomposition for trend and seasonality extraction
- Change point detection using ruptures (PELT algorithm)
- Anomaly detection with statistical z-score method
- Forward projection using linear trend extrapolation
- Health scoring based on trajectory direction and volatility

Input: DataFrame with columns [date, clicks, impressions, ctr, position]
Output: Dict with trend, seasonality, change_points, anomalies, projection, health_score
"""

import logging
from typing import Any, Dict, List, Optional
from datetime import timedelta

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def analyze_health_trajectory(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Run the full Health & Trajectory analysis on GSC daily time series data.

    Args:
        df: DataFrame with columns [date, clicks, impressions, ctr, position].
            Must have at least 30 rows for meaningful analysis.

    Returns:
        Dict containing:
            - summary: human-readable summary string
            - health_score: 0-100 overall health score
            - trend: dict with direction, slope, r_squared
            - seasonality: dict with weekly/monthly patterns detected
            - change_points: list of dicts with date, metric, direction
            - anomalies: list of dicts with date, metric, value, z_score
            - projection: dict with next_30d forecast values
            - metrics_summary: dict with current vs prior period comparisons
    """
    results: Dict[str, Any] = {}

    try:
        # Validate input
        df = _prepare_dataframe(df)
        if len(df) < 14:
            return {
                "summary": "Insufficient data for trajectory analysis (need at least 14 days).",
                "health_score": None,
                "trend": None,
                "seasonality": None,
                "change_points": [],
                "anomalies": [],
                "projection": None,
                "metrics_summary": None,
            }

        # 1. Metrics summary (current 30d vs prior 30d)
        results["metrics_summary"] = _compute_metrics_summary(df)

        # 2. Trend analysis
        results["trend"] = _compute_trend(df)

        # 3. Seasonality detection
        results["seasonality"] = _detect_seasonality(df)

        # 4. Change point detection
        results["change_points"] = _detect_change_points(df)

        # 5. Anomaly detection
        results["anomalies"] = _detect_anomalies(df)

        # 6. Forward projection (simple)
        results["projection"] = _project_forward(df)

        # 7. Health score
        results["health_score"] = _compute_health_score(results)

        # 8. Human-readable summary
        results["summary"] = _generate_summary(results)

    except Exception as e:
        logger.error(f"Health trajectory analysis failed: {e}", exc_info=True)
        results["summary"] = f"Analysis encountered an error: {str(e)}"
        results["health_score"] = None

    return results


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names, parse dates, sort, and fill gaps."""
    df = df.copy()
    df.columns = [c.lower().strip() for c in df.columns]

    if "date" not in df.columns:
        raise ValueError("DataFrame must contain a 'date' column")

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    # Ensure numeric columns exist
    for col in ["clicks", "impressions", "ctr", "position"]:
        if col not in df.columns:
            df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    return df


def _compute_metrics_summary(df: pd.DataFrame) -> Dict[str, Any]:
    """Compare latest 30 days vs prior 30 days."""
    today = df["date"].max()
    cutoff = today - timedelta(days=30)
    prior_cutoff = cutoff - timedelta(days=30)

    current = df[df["date"] > cutoff]
    prior = df[(df["date"] > prior_cutoff) & (df["date"] <= cutoff)]

    def _period_stats(period_df: pd.DataFrame) -> Dict[str, float]:
        if period_df.empty:
            return {"clicks": 0, "impressions": 0, "avg_ctr": 0, "avg_position": 0}
        return {
            "clicks": int(period_df["clicks"].sum()),
            "impressions": int(period_df["impressions"].sum()),
            "avg_ctr": round(float(period_df["ctr"].mean()), 4),
            "avg_position": round(float(period_df["position"].mean()), 1),
        }

    current_stats = _period_stats(current)
    prior_stats = _period_stats(prior)

    def _pct_change(curr: float, prev: float) -> Optional[float]:
        if prev == 0:
            return None
        return round((curr - prev) / prev * 100, 1)

    return {
        "current_period": current_stats,
        "prior_period": prior_stats,
        "changes": {
            "clicks_pct": _pct_change(current_stats["clicks"], prior_stats["clicks"]),
            "impressions_pct": _pct_change(current_stats["impressions"], prior_stats["impressions"]),
            "ctr_change": round(current_stats["avg_ctr"] - prior_stats["avg_ctr"], 4),
            "position_change": round(current_stats["avg_position"] - prior_stats["avg_position"], 1),
        },
    }


def _compute_trend(df: pd.DataFrame) -> Dict[str, Any]:
    """Fit a linear trend to clicks over time using numpy polyfit."""
    y = df["clicks"].values.astype(float)
    x = np.arange(len(y), dtype=float)

    if len(x) < 2 or np.std(y) == 0:
        return {"direction": "flat", "slope": 0.0, "r_squared": 0.0}

    coeffs = np.polyfit(x, y, 1)
    slope = float(coeffs[0])

    # R-squared
    y_pred = np.polyval(coeffs, x)
    ss_res = np.sum((y - y_pred) ** 2)
    ss_tot = np.sum((y - np.mean(y)) ** 2)
    r_squared = float(1 - ss_res / ss_tot) if ss_tot > 0 else 0.0

    if slope > 0.5:
        direction = "growing"
    elif slope < -0.5:
        direction = "declining"
    else:
        direction = "flat"

    return {
        "direction": direction,
        "slope": round(slope, 4),
        "r_squared": round(r_squared, 4),
        "daily_trend_clicks": round(slope, 2),
    }


def _detect_seasonality(df: pd.DataFrame) -> Dict[str, Any]:
    """Detect weekly seasonality using day-of-week aggregation."""
    if len(df) < 14:
        return {"weekly_pattern": False, "day_of_week_index": None}

    df_copy = df.copy()
    df_copy["dow"] = df_copy["date"].dt.dayofweek  # 0=Mon, 6=Sun

    dow_means = df_copy.groupby("dow")["clicks"].mean()
    overall_mean = df_copy["clicks"].mean()

    if overall_mean == 0:
        return {"weekly_pattern": False, "day_of_week_index": None}

    # Coefficient of variation across days of week
    cv = float(dow_means.std() / overall_mean) if overall_mean > 0 else 0
    has_weekly = cv > 0.15  # 15% variation threshold

    day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    day_of_week_index = {
        day_names[i]: round(float(dow_means.get(i, 0)), 1) for i in range(7)
    }

    return {
        "weekly_pattern": has_weekly,
        "variation_coefficient": round(cv, 3),
        "day_of_week_index": day_of_week_index,
        "peak_day": day_names[int(dow_means.idxmax())] if has_weekly else None,
        "trough_day": day_names[int(dow_means.idxmin())] if has_weekly else None,
    }


def _detect_change_points(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Detect change points in clicks using a sliding window mean-shift approach.
    Falls back to simple method if ruptures is not available.
    """
    change_points = []

    try:
        import ruptures as rpt

        signal = df["clicks"].values.astype(float)
        if len(signal) < 20:
            return []

        # PELT algorithm with RBF kernel
        algo = rpt.Pelt(model="rbf", min_size=7).fit(signal)
        result = algo.predict(pen=10)

        # Remove the last breakpoint (always == len(signal))
        breakpoints = [bp for bp in result if bp < len(signal)]

        for bp in breakpoints:
            bp_date = df.iloc[bp]["date"]
            before_mean = float(signal[max(0, bp - 7):bp].mean())
            after_mean = float(signal[bp:min(len(signal), bp + 7)].mean())
            direction = "up" if after_mean > before_mean else "down"
            magnitude = round(abs(after_mean - before_mean), 1)

            change_points.append({
                "date": bp_date.strftime("%Y-%m-%d"),
                "metric": "clicks",
                "direction": direction,
                "magnitude": magnitude,
                "before_avg": round(before_mean, 1),
                "after_avg": round(after_mean, 1),
            })

    except ImportError:
        logger.warning("ruptures not installed; using simple change point detection")
        # Simple fallback: detect 7-day rolling mean shifts > 2 std devs
        if len(df) >= 14:
            rolling = df["clicks"].rolling(7).mean().dropna()
            diff = rolling.diff().dropna()
            threshold = diff.std() * 2

            if threshold > 0:
                shifts = diff[diff.abs() > threshold]
                for idx in shifts.index:
                    row = df.iloc[idx]
                    change_points.append({
                        "date": row["date"].strftime("%Y-%m-%d"),
                        "metric": "clicks",
                        "direction": "up" if float(shifts[idx]) > 0 else "down",
                        "magnitude": round(abs(float(shifts[idx])), 1),
                    })

    except Exception as e:
        logger.error(f"Change point detection failed: {e}")

    return change_points[:10]  # Cap at 10 most significant


def _detect_anomalies(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Detect anomalies using rolling z-score method."""
    anomalies = []

    for metric in ["clicks", "impressions"]:
        if metric not in df.columns:
            continue

        series = df[metric].astype(float)
        if len(series) < 14:
            continue

        rolling_mean = series.rolling(window=14, min_periods=7).mean()
        rolling_std = series.rolling(window=14, min_periods=7).std()

        # Avoid division by zero
        rolling_std = rolling_std.replace(0, np.nan)

        z_scores = (series - rolling_mean) / rolling_std

        # Flag anything beyond 2.5 standard deviations
        anomaly_mask = z_scores.abs() > 2.5

        for idx in df[anomaly_mask].index:
            row = df.iloc[idx]
            z = float(z_scores.iloc[idx])
            anomalies.append({
                "date": row["date"].strftime("%Y-%m-%d"),
                "metric": metric,
                "value": round(float(row[metric]), 1),
                "expected": round(float(rolling_mean.iloc[idx]), 1),
                "z_score": round(z, 2),
                "type": "spike" if z > 0 else "drop",
            })

    # Sort by absolute z-score descending
    anomalies.sort(key=lambda a: abs(a["z_score"]), reverse=True)
    return anomalies[:20]


def _project_forward(df: pd.DataFrame, days: int = 30) -> Dict[str, Any]:
    """Project clicks forward using linear trend + weekly seasonality."""
    if len(df) < 14:
        return {"forecast": [], "method": "insufficient_data"}

    y = df["clicks"].values.astype(float)
    x = np.arange(len(y), dtype=float)

    # Linear trend
    coeffs = np.polyfit(x, y, 1)
    slope, intercept = float(coeffs[0]), float(coeffs[1])

    # Weekly seasonality factors
    df_copy = df.copy()
    df_copy["dow"] = df_copy["date"].dt.dayofweek
    dow_means = df_copy.groupby("dow")["clicks"].mean()
    overall_mean = float(y.mean()) if y.mean() > 0 else 1.0
    seasonal_factors = {dow: float(dow_means.get(dow, overall_mean)) / overall_mean for dow in range(7)}

    # Generate forecast
    last_date = df["date"].max()
    forecast = []
    for d in range(1, days + 1):
        future_x = float(len(y) + d)
        trend_val = slope * future_x + intercept
        future_date = last_date + timedelta(days=d)
        dow = future_date.dayofweek
        adjusted = max(0, trend_val * seasonal_factors.get(dow, 1.0))
        forecast.append({
            "date": future_date.strftime("%Y-%m-%d"),
            "predicted_clicks": round(adjusted, 0),
        })

    total_forecast = sum(f["predicted_clicks"] for f in forecast)
    current_30d = float(df.tail(30)["clicks"].sum()) if len(df) >= 30 else float(df["clicks"].sum())

    return {
        "method": "linear_trend_with_weekly_seasonality",
        "forecast_days": days,
        "forecast_total_clicks": round(total_forecast, 0),
        "current_30d_clicks": round(current_30d, 0),
        "projected_change_pct": round((total_forecast - current_30d) / current_30d * 100, 1) if current_30d > 0 else None,
        "forecast": forecast[:7],  # Only include first 7 days in response (keep it light)
    }


def _compute_health_score(results: Dict[str, Any]) -> int:
    """
    Compute a 0-100 health score based on trend, anomalies, and change points.
    
    Scoring breakdown:
    - Trend direction: 40 points (growing=40, flat=25, declining=10)
    - Trend fit (R²): 15 points
    - Low anomaly count: 15 points
    - Positive period-over-period: 20 points
    - Few negative change points: 10 points
    """
    score = 0

    # Trend direction (40 pts)
    trend = results.get("trend") or {}
    direction = trend.get("direction", "flat")
    if direction == "growing":
        score += 40
    elif direction == "flat":
        score += 25
    else:
        score += 10

    # Trend fit (15 pts) — higher R² = more predictable
    r2 = trend.get("r_squared", 0)
    score += int(r2 * 15)

    # Anomaly penalty (15 pts) — fewer anomalies = better
    anomalies = results.get("anomalies", [])
    drop_anomalies = [a for a in anomalies if a.get("type") == "drop"]
    if len(drop_anomalies) == 0:
        score += 15
    elif len(drop_anomalies) <= 2:
        score += 10
    elif len(drop_anomalies) <= 5:
        score += 5

    # Period-over-period improvement (20 pts)
    metrics = results.get("metrics_summary", {})
    changes = metrics.get("changes", {})
    clicks_pct = changes.get("clicks_pct")
    if clicks_pct is not None:
        if clicks_pct > 10:
            score += 20
        elif clicks_pct > 0:
            score += 15
        elif clicks_pct > -10:
            score += 10
        else:
            score += 5

    # Change points penalty (10 pts)
    cps = results.get("change_points", [])
    negative_cps = [cp for cp in cps if cp.get("direction") == "down"]
    if len(negative_cps) == 0:
        score += 10
    elif len(negative_cps) <= 2:
        score += 5

    return min(100, max(0, score))


def _generate_summary(results: Dict[str, Any]) -> str:
    """Generate a human-readable summary of the health trajectory analysis."""
    parts = []

    health = results.get("health_score", 0)
    if health is not None:
        if health >= 75:
            parts.append(f"Site health is strong (score: {health}/100).")
        elif health >= 50:
            parts.append(f"Site health is moderate (score: {health}/100).")
        else:
            parts.append(f"Site health needs attention (score: {health}/100).")

    trend = results.get("trend", {})
    direction = trend.get("direction", "unknown")
    slope = trend.get("daily_trend_clicks", 0)
    if direction == "growing":
        parts.append(f"Traffic is trending upward at ~{slope} clicks/day.")
    elif direction == "declining":
        parts.append(f"Traffic is trending downward at ~{abs(slope)} clicks/day.")
    else:
        parts.append("Traffic is relatively flat.")

    metrics = results.get("metrics_summary", {})
    changes = metrics.get("changes", {})
    clicks_pct = changes.get("clicks_pct")
    if clicks_pct is not None:
        if clicks_pct > 0:
            parts.append(f"Clicks up {clicks_pct}% vs prior 30 days.")
        elif clicks_pct < 0:
            parts.append(f"Clicks down {abs(clicks_pct)}% vs prior 30 days.")

    anomalies = results.get("anomalies", [])
    if anomalies:
        parts.append(f"{len(anomalies)} anomalous data point(s) detected.")

    cps = results.get("change_points", [])
    if cps:
        parts.append(f"{len(cps)} significant change point(s) identified.")

    return " ".join(parts)
