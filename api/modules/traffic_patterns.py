import os
from flask import Blueprint, jsonify, request
from functools import wraps
import jwt
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from scipy import stats
from typing import Dict, List, Any, Tuple
import json

from db import get_db_connection

traffic_patterns_bp = Blueprint('traffic_patterns', __name__)

SECRET_KEY = os.environ.get('SECRET_KEY', 'dev-secret-key')

def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get('Authorization')
        if not token:
            return jsonify({'error': 'Token is missing'}), 401
        try:
            if token.startswith('Bearer '):
                token = token[7:]
            data = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
            request.user_id = data['user_id']
        except Exception as e:
            return jsonify({'error': 'Token is invalid'}), 401
        return f(*args, **kwargs)
    return decorated

def calculate_hourly_patterns(daily_data: pd.DataFrame) -> Dict[str, Any]:
    """
    Analyze hourly traffic patterns if hourly data is available.
    Returns peak hours, off-peak hours, and hourly distribution.
    """
    if 'hour' not in daily_data.columns or daily_data.empty:
        return {
            'available': False,
            'peak_hours': [],
            'off_peak_hours': [],
            'hourly_distribution': [],
            'hour_volatility': 0.0
        }
    
    hourly_avg = daily_data.groupby('hour').agg({
        'clicks': 'mean',
        'impressions': 'mean'
    }).reset_index()
    
    hourly_avg = hourly_avg.sort_values('hour')
    
    # Calculate z-scores to identify peak and off-peak hours
    hourly_avg['clicks_zscore'] = stats.zscore(hourly_avg['clicks'])
    
    peak_hours = hourly_avg[hourly_avg['clicks_zscore'] > 0.5]['hour'].tolist()
    off_peak_hours = hourly_avg[hourly_avg['clicks_zscore'] < -0.5]['hour'].tolist()
    
    hourly_distribution = []
    for _, row in hourly_avg.iterrows():
        hourly_distribution.append({
            'hour': int(row['hour']),
            'avg_clicks': float(row['clicks']),
            'avg_impressions': float(row['impressions']),
            'click_share': float(row['clicks'] / hourly_avg['clicks'].sum()) if hourly_avg['clicks'].sum() > 0 else 0
        })
    
    hour_volatility = float(hourly_avg['clicks'].std() / hourly_avg['clicks'].mean()) if hourly_avg['clicks'].mean() > 0 else 0
    
    return {
        'available': True,
        'peak_hours': [int(h) for h in peak_hours],
        'off_peak_hours': [int(h) for h in off_peak_hours],
        'hourly_distribution': hourly_distribution,
        'hour_volatility': hour_volatility
    }

def calculate_daily_patterns(daily_data: pd.DataFrame) -> Dict[str, Any]:
    """
    Analyze day-of-week patterns.
    Returns best/worst days, daily distribution, and weekly volatility.
    """
    if 'day_of_week' not in daily_data.columns or daily_data.empty:
        daily_data['day_of_week'] = pd.to_datetime(daily_data['date']).dt.dayofweek
    
    day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    
    daily_avg = daily_data.groupby('day_of_week').agg({
        'clicks': 'mean',
        'impressions': 'mean',
        'ctr': 'mean',
        'position': 'mean'
    }).reset_index()
    
    daily_avg = daily_avg.sort_values('day_of_week')
    
    best_day_idx = daily_avg['clicks'].idxmax()
    worst_day_idx = daily_avg['clicks'].idxmin()
    
    best_day = {
        'day': day_names[int(daily_avg.loc[best_day_idx, 'day_of_week'])],
        'avg_clicks': float(daily_avg.loc[best_day_idx, 'clicks']),
        'index': int(daily_avg.loc[best_day_idx, 'day_of_week'])
    }
    
    worst_day = {
        'day': day_names[int(daily_avg.loc[worst_day_idx, 'day_of_week'])],
        'avg_clicks': float(daily_avg.loc[worst_day_idx, 'clicks']),
        'index': int(daily_avg.loc[worst_day_idx, 'day_of_week'])
    }
    
    daily_distribution = []
    for _, row in daily_avg.iterrows():
        day_idx = int(row['day_of_week'])
        daily_distribution.append({
            'day': day_names[day_idx],
            'day_index': day_idx,
            'avg_clicks': float(row['clicks']),
            'avg_impressions': float(row['impressions']),
            'avg_ctr': float(row['ctr']),
            'avg_position': float(row['position']),
            'click_share': float(row['clicks'] / daily_avg['clicks'].sum()) if daily_avg['clicks'].sum() > 0 else 0
        })
    
    day_volatility = float(daily_avg['clicks'].std() / daily_avg['clicks'].mean()) if daily_avg['clicks'].mean() > 0 else 0
    
    # Check for weekend vs weekday pattern
    weekday_avg = daily_data[daily_data['day_of_week'] < 5]['clicks'].mean()
    weekend_avg = daily_data[daily_data['day_of_week'] >= 5]['clicks'].mean()
    
    weekend_effect = 'positive' if weekend_avg > weekday_avg * 1.1 else 'negative' if weekend_avg < weekday_avg * 0.9 else 'neutral'
    weekend_difference_pct = float(((weekend_avg - weekday_avg) / weekday_avg * 100)) if weekday_avg > 0 else 0
    
    return {
        'best_day': best_day,
        'worst_day': worst_day,
        'daily_distribution': daily_distribution,
        'day_volatility': day_volatility,
        'weekend_effect': weekend_effect,
        'weekend_difference_pct': weekend_difference_pct,
        'weekday_avg_clicks': float(weekday_avg),
        'weekend_avg_clicks': float(weekend_avg)
    }

def calculate_monthly_patterns(daily_data: pd.DataFrame) -> Dict[str, Any]:
    """
    Analyze month-level patterns and trends.
    Returns monthly aggregations, growth rates, and best/worst months.
    """
    daily_data['month'] = pd.to_datetime(daily_data['date']).dt.to_period('M')
    
    monthly_agg = daily_data.groupby('month').agg({
        'clicks': 'sum',
        'impressions': 'sum',
        'ctr': 'mean',
        'position': 'mean'
    }).reset_index()
    
    monthly_agg['month_str'] = monthly_agg['month'].astype(str)
    monthly_agg = monthly_agg.sort_values('month')
    
    # Calculate month-over-month growth rates
    monthly_agg['clicks_growth_pct'] = monthly_agg['clicks'].pct_change() * 100
    monthly_agg['impressions_growth_pct'] = monthly_agg['impressions'].pct_change() * 100
    
    monthly_distribution = []
    for _, row in monthly_agg.iterrows():
        monthly_distribution.append({
            'month': str(row['month_str']),
            'total_clicks': int(row['clicks']),
            'total_impressions': int(row['impressions']),
            'avg_ctr': float(row['ctr']),
            'avg_position': float(row['position']),
            'clicks_growth_pct': float(row['clicks_growth_pct']) if not pd.isna(row['clicks_growth_pct']) else None,
            'impressions_growth_pct': float(row['impressions_growth_pct']) if not pd.isna(row['impressions_growth_pct']) else None
        })
    
    # Identify best and worst months
    if len(monthly_agg) > 0:
        best_month_idx = monthly_agg['clicks'].idxmax()
        worst_month_idx = monthly_agg['clicks'].idxmin()
        
        best_month = {
            'month': str(monthly_agg.loc[best_month_idx, 'month_str']),
            'total_clicks': int(monthly_agg.loc[best_month_idx, 'clicks'])
        }
        
        worst_month = {
            'month': str(monthly_agg.loc[worst_month_idx, 'month_str']),
            'total_clicks': int(monthly_agg.loc[worst_month_idx, 'clicks'])
        }
    else:
        best_month = {'month': None, 'total_clicks': 0}
        worst_month = {'month': None, 'total_clicks': 0}
    
    # Calculate overall monthly trend
    if len(monthly_agg) > 1:
        x = np.arange(len(monthly_agg))
        y = monthly_agg['clicks'].values
        slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)
        
        avg_monthly_clicks = float(monthly_agg['clicks'].mean())
        trend_direction = 'growing' if slope > 0 else 'declining'
        trend_strength = abs(float(slope / avg_monthly_clicks * 100)) if avg_monthly_clicks > 0 else 0
        
        month_volatility = float(monthly_agg['clicks'].std() / avg_monthly_clicks) if avg_monthly_clicks > 0 else 0
    else:
        trend_direction = 'insufficient_data'
        trend_strength = 0
        month_volatility = 0
        avg_monthly_clicks = 0
    
    return {
        'monthly_distribution': monthly_distribution,
        'best_month': best_month,
        'worst_month': worst_month,
        'trend_direction': trend_direction,
        'trend_strength': trend_strength,
        'month_volatility': month_volatility,
        'avg_monthly_clicks': avg_monthly_clicks
    }

def detect_seasonal_patterns(daily_data: pd.DataFrame) -> Dict[str, Any]:
    """
    Detect recurring seasonal patterns using autocorrelation and decomposition.
    Returns identified cycles and their characteristics.
    """
    if len(daily_data) < 90:
        return {
            'detected': False,
            'cycles': [],
            'seasonal_strength': 0.0,
            'description': 'Insufficient data for seasonal analysis'
        }
    
    daily_data = daily_data.sort_values('date')
    clicks_series = daily_data['clicks'].values
    
    cycles = []
    
    # Check for weekly cycle (7 days)
    if len(clicks_series) >= 28:
        weekly_mean = []
        for i in range(0, len(clicks_series) - 7, 7):
            weekly_mean.append(np.mean(clicks_series[i:i+7]))
        
        if len(weekly_mean) > 1:
            weekly_std = np.std(weekly_mean)
            weekly_avg = np.mean(weekly_mean)
            weekly_cv = weekly_std / weekly_avg if weekly_avg > 0 else 0
            
            if weekly_cv < 0.3:  # Consistent weekly pattern
                cycles.append({
                    'period_days': 7,
                    'period_name': 'weekly',
                    'strength': float(1 - weekly_cv),
                    'description': 'Consistent weekly traffic cycle detected'
                })
    
    # Check for monthly cycle (30 days)
    if len(clicks_series) >= 90:
        monthly_chunks = []
        for i in range(0, len(clicks_series), 30):
            if i + 30 <= len(clicks_series):
                monthly_chunks.append(np.mean(clicks_series[i:i+30]))
        
        if len(monthly_chunks) > 2:
            monthly_std = np.std(monthly_chunks)
            monthly_avg = np.mean(monthly_chunks)
            monthly_cv = monthly_std / monthly_avg if monthly_avg > 0 else 0
            
            # Check for increasing/decreasing pattern
            monthly_trend = np.polyfit(range(len(monthly_chunks)), monthly_chunks, 1)[0]
            
            if abs(monthly_trend) > monthly_avg * 0.05:
                cycles.append({
                    'period_days': 30,
                    'period_name': 'monthly',
                    'strength': float(abs(monthly_trend) / monthly_avg) if monthly_avg > 0 else 0,
                    'description': f'Monthly {"growth" if monthly_trend > 0 else "decline"} pattern detected'
                })
    
    # Check for quarterly patterns (90 days)
    if len(clicks_series) >= 270:
        quarterly_chunks = []
        for i in range(0, len(clicks_series), 90):
            if i + 90 <= len(clicks_series):
                quarterly_chunks.append(np.mean(clicks_series[i:i+90]))
        
        if len(quarterly_chunks) > 1:
            q_range = max(quarterly_chunks) - min(quarterly_chunks)
            q_avg = np.mean(quarterly_chunks)
            
            if q_range > q_avg * 0.2:
                cycles.append({
                    'period_days': 90,
                    'period_name': 'quarterly',
                    'strength': float(q_range / q_avg) if q_avg > 0 else 0,
                    'description': 'Quarterly seasonal variation detected'
                })
    
    # Calculate overall seasonal strength
    if cycles:
        seasonal_strength = float(np.mean([c['strength'] for c in cycles]))
        detected = True
        
        # Generate description
        if len(cycles) == 1:
            description = cycles[0]['description']
        else:
            cycle_names = [c['period_name'] for c in cycles]
            description = f"Multiple seasonal patterns detected: {', '.join(cycle_names)}"
    else:
        seasonal_strength = 0.0
        detected = False
        description = 'No significant seasonal patterns detected'
    
    return {
        'detected': detected,
        'cycles': cycles,
        'seasonal_strength': seasonal_strength,
        'description': description
    }

def calculate_traffic_volatility(daily_data: pd.DataFrame) -> Dict[str, Any]:
    """
    Calculate various volatility metrics for traffic patterns.
    Returns volatility scores and stability indicators.
    """
    daily_data = daily_data.sort_values('date')
    clicks = daily_data['clicks'].values
    
    if len(clicks) < 7:
        return {
            'overall_volatility': 0.0,
            'trend_stability': 0.0,
            'day_to_day_volatility': 0.0,
            'week_over_week_volatility': 0.0,
            'stability_score': 0.0,
            'classification': 'insufficient_data'
        }
    
    # Overall coefficient of variation
    overall_volatility = float(np.std(clicks) / np.mean(clicks)) if np.mean(clicks) > 0 else 0
    
    # Day-to-day percentage changes
    daily_changes = np.diff(clicks) / clicks[:-1]
    daily_changes = daily_changes[np.isfinite(daily_changes)]
    day_to_day_volatility = float(np.std(daily_changes)) if len(daily_changes) > 0 else 0
    
    # Week-over-week volatility
    if len(clicks) >= 14:
        weekly_sums = []
        for i in range(0, len(clicks) - 7, 7):
            weekly_sums.append(np.sum(clicks[i:i+7]))
        
        if len(weekly_sums) > 1:
            weekly_changes = np.diff(weekly_sums) / np.array(weekly_sums[:-1])
            weekly_changes = weekly_changes[np.isfinite(weekly_changes)]
            week_over_week_volatility = float(np.std(weekly_changes)) if len(weekly_changes) > 0 else 0
        else:
            week_over_week_volatility = 0.0
    else:
        week_over_week_volatility = 0.0
    
    # Trend stability (detrended volatility)
    if len(clicks) > 7:
        x = np.arange(len(clicks))
        z = np.polyfit(x, clicks, 1)
        p = np.poly1d(z)
        trend_line = p(x)
        residuals = clicks - trend_line
        trend_stability = float(np.std(residuals) / np.mean(clicks)) if np.mean(clicks) > 0 else 0
    else:
        trend_stability = overall_volatility
    
    # Calculate composite stability score (0-1, higher is more stable)
    # Invert volatility metrics so higher = more stable
    volatility_components = [
        1 / (1 + overall_volatility),
        1 / (1 + day_to_day_volatility),
        1 / (1 + week_over_week_volatility),
        1 / (1 + trend_stability)
    ]
    stability_score = float(np.mean(volatility_components))
    
    # Classification
    if stability_score > 0.75:
        classification = 'highly_stable'
    elif stability_score > 0.6:
        classification = 'stable'
    elif stability_score > 0.4:
        classification = 'moderate'
    elif stability_score > 0.25:
        classification = 'volatile'
    else:
        classification = 'highly_volatile'
    
    return {
        'overall_volatility': overall_volatility,
        'trend_stability': trend_stability,
        'day_to_day_volatility': day_to_day_volatility,
        'week_over_week_volatility': week_over_week_volatility,
        'stability_score': stability_score,
        'classification': classification
    }

def identify_traffic_anomalies(daily_data: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Identify significant traffic anomalies (spikes and drops).
    Uses statistical thresholds based on historical patterns.
    """
    if len(daily_data) < 14:
        return []
    
    daily_data = daily_data.sort_values('date')
    
    # Calculate rolling statistics
    daily_data['rolling_mean'] = daily_data['clicks'].rolling(window=7, center=True).mean()
    daily_data['rolling_std'] = daily_data['clicks'].rolling(window=7, center=True).std()
    
    # Calculate z-scores
    daily_data['z_score'] = (daily_data['clicks'] - daily_data['rolling_mean']) / daily_data['rolling_std']
    
    anomalies = []
    
    # Identify anomalies (|z-score| > 2.5)
    anomaly_rows = daily_data[abs(daily_data['z_score']) > 2.5]
    
    for _, row in anomaly_rows.iterrows():
        if pd.isna(row['z_score']) or pd.isna(row['rolling_mean']):
            continue
        
        anomaly_type = 'spike' if row['z_score'] > 0 else 'drop'
        magnitude = float(abs(row['z_score']))
        
        expected_clicks = float(row['rolling_mean'])
        actual_clicks = int(row['clicks'])
        difference = actual_clicks - expected_clicks
        difference_pct = float((difference / expected_clicks * 100)) if expected_clicks > 0 else 0
        
        anomalies.append({
            'date': str(row['date']),
            'type': anomaly_type,
            'magnitude': magnitude,
            'actual_clicks': actual_clicks,
            'expected_clicks': expected_clicks,
            'difference': float(difference),
            'difference_pct': difference_pct,
            'severity': 'high' if magnitude > 3.5 else 'medium'
        })
    
    # Sort by magnitude
    anomalies.sort(key=lambda x: x['magnitude'], reverse=True)
    
    return anomalies[:20]  # Return top 20 anomalies

@traffic_patterns_bp.route('/reports/<report_id>/modules/traffic-patterns', methods=['GET'])
@token_required
def get_traffic_patterns(report_id):
    """
    Get Module 2 traffic pattern analysis data for a report.
    
    Returns:
        - Hourly patterns (if available)
        - Daily patterns (day of week)
        - Monthly patterns and trends
        - Seasonal pattern detection
        - Traffic volatility metrics
        - Anomaly detection
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Verify report exists and user has access
        cur.execute("""
            SELECT r.id, r.status, r.site_url
            FROM reports r
            WHERE r.id = %s AND r.user_id = %s
        """, (report_id, request.user_id))
        
        report = cur.fetchone()
        
        if not report:
            cur.close()
            conn.close()
            return jsonify({'error': 'Report not found'}), 404
        
        report_status = report[1]
        
        if report_status == 'pending' or report_status == 'processing':
            cur.close()
            conn.close()
            return jsonify({
                'status': 'processing',
                'message': 'Traffic pattern analysis is still being generated'
            }), 202
        
        # Check if module data exists
        cur.execute("""
            SELECT module_data, created_at, updated_at
            FROM report_modules
            WHERE report_id = %s AND module_name = 'traffic_patterns'
        """, (report_id,))
        
        module_row = cur.fetchone()
        
        if module_row and module_row[0]:
            cur.close()
            conn.close()
            
            return jsonify({
                'report_id': report_id,
                'module_name': 'traffic_patterns',
                'data': module_row[0],
                'created_at': module_row[1].isoformat() if module_row[1] else None,
                'updated_at': module_row[2].isoformat() if module_row[2] else None
            }), 200
        
        # If no cached data, generate it from raw GSC data
        cur.execute("""
            SELECT data_type, data_value, date_pulled
            FROM gsc_data
            WHERE report_id = %s AND data_type = 'daily_performance'
            ORDER BY date_pulled DESC
            LIMIT 1
        """, (report_id,))
        
        gsc_row = cur.fetchone()
        
        if not gsc_row:
            cur.close()
            conn.close()
            return jsonify({
                'error': 'No GSC data available for traffic pattern analysis'
            }), 404
        
        # Parse daily performance data
        daily_data_raw = gsc_row[1]
        
        if isinstance(daily_data_raw, str):
            daily_data_raw = json.loads(daily_data_raw)
        
        # Convert to DataFrame
        if 'rows' in daily_data_raw:
            rows = daily_data_raw['rows']
        else:
            rows = daily_data_raw
        
        daily_records = []
        for row in rows:
            if 'keys' in row:
                date_str = row['keys'][0]
            else:
                date_str = row.get('date')
            
            daily_records.append({
                'date': pd.to_datetime(date_str),
                'clicks': row.get('clicks', 0),
                'impressions': row.get('impressions', 0),
                'ctr': row.get('ctr', 0),
                'position': row.get('position', 0)
            })
        
        if not daily_records:
            cur.close()
            conn.close()
            return jsonify({
                'error': 'No valid daily data found'
            }), 404
        
        daily_df = pd.DataFrame(daily_records)
        daily_df['day_of_week'] = daily_df['date'].dt.dayofweek
        
        # Perform all analyses
        hourly_patterns = calculate_hourly_patterns(daily_df)
        daily_patterns = calculate_daily_patterns(daily_df)
        monthly_patterns = calculate_monthly_patterns(daily_df)
        seasonal_patterns = detect_seasonal_patterns(daily_df)
        volatility_metrics = calculate_traffic_volatility(daily_df)
        anomalies = identify_traffic_anomalies(daily_df)
        
        # Compile results
        results = {
            'hourly_patterns': hourly_patterns,
            'daily_patterns': daily_patterns,
            'monthly_patterns': monthly_patterns,
            'seasonal_patterns': seasonal_patterns,
            'volatility_metrics': volatility_metrics,
            'anomalies': anomalies,
            'data_range': {
                'start_date': str(daily_df['date'].min().date()),
                'end_date': str(daily_df['date'].max().date()),
                'total_days': len(daily_df)
            },
            'summary': {
                'total_clicks': int(daily_df['clicks'].sum()),
                'total_impressions': int(daily_df['impressions'].sum()),
                'avg_daily_clicks': float(daily_df['clicks'].mean()),
                'avg_ctr': float(daily_df['ctr'].mean()),
                'avg_position': float(daily_df['position'].mean())
            }
        }
        
        # Cache the results
        cur.execute("""
            INSERT INTO report_modules (report_id, module_name, module_data, created_at, updated_at)
            VALUES (%s, %s, %s, NOW(), NOW())
            ON CONFLICT (report_id, module_name)
            DO UPDATE SET module_data = EXCLUDED.module_data, updated_at = NOW()
        """, (report_id, 'traffic_patterns', json.dumps(results)))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'report_id': report_id,
            'module_name': 'traffic_patterns',
            'data': results,
            'created_at': datetime.utcnow().isoformat(),
            'updated_at': datetime.utcnow().isoformat()
        }), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
