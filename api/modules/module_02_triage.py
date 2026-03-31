"""
Module 02: Page-Level Triage
Optimized for performance using vectorization and batch operations.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from scipy import stats
from sklearn.ensemble import IsolationForest
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


def analyze_page_triage(
    page_daily_data: pd.DataFrame,
    ga4_landing_data: pd.DataFrame,
    gsc_page_summary: pd.DataFrame
) -> Dict:
    """
    Analyze page-level performance trends and identify priority pages for action.
    
    Optimized implementation using vectorized operations instead of iterrows.
    
    Args:
        page_daily_data: DataFrame with columns [page, date, clicks, impressions, ctr, position]
        ga4_landing_data: DataFrame with columns [page, sessions, bounce_rate, avg_session_duration, conversions]
        gsc_page_summary: DataFrame with columns [page, total_clicks, total_impressions, avg_ctr, avg_position]
    
    Returns:
        Dictionary with page analysis results and summary statistics
    """
    logger.info("Starting page-level triage analysis")
    
    # Ensure date column is datetime
    page_daily_data['date'] = pd.to_datetime(page_daily_data['date'])
    
    # 1. Per-page trend fitting (vectorized)
    logger.info("Computing per-page trends")
    trend_results = _compute_page_trends_vectorized(page_daily_data)
    
    # 2. CTR anomaly detection
    logger.info("Detecting CTR anomalies")
    ctr_anomalies = _detect_ctr_anomalies_vectorized(gsc_page_summary)
    
    # 3. Engagement cross-reference
    logger.info("Cross-referencing with engagement data")
    engagement_flags = _flag_low_engagement_vectorized(ga4_landing_data)
    
    # 4. Merge all analysis results
    logger.info("Merging analysis results")
    page_analysis = _merge_page_analysis(
        trend_results,
        ctr_anomalies,
        engagement_flags,
        gsc_page_summary
    )
    
    # 5. Priority scoring
    logger.info("Computing priority scores")
    page_analysis = _compute_priority_scores_vectorized(page_analysis)
    
    # 6. Generate summary statistics
    summary = _generate_summary_stats(page_analysis)
    
    logger.info(f"Analyzed {len(page_analysis)} pages")
    
    return {
        "pages": page_analysis.to_dict('records'),
        "summary": summary
    }


def _compute_page_trends_vectorized(page_daily_data: pd.DataFrame) -> pd.DataFrame:
    """
    Compute trend slopes for all pages using vectorized operations.
    
    Returns DataFrame with columns: [page, trend_slope, days_with_data, projected_page1_loss_date]
    """
    # Filter pages with sufficient data (> 30 days)
    page_counts = page_daily_data.groupby('page').size()
    valid_pages = page_counts[page_counts > 30].index
    df = page_daily_data[page_daily_data['page'].isin(valid_pages)].copy()
    
    if len(df) == 0:
        return pd.DataFrame(columns=['page', 'trend_slope', 'days_with_data', 'bucket', 'projected_page1_loss_date'])
    
    # Convert date to numeric for regression (days since min date)
    min_date = df['date'].min()
    df['days_since_start'] = (df['date'] - min_date).dt.days
    
    # Group by page and compute linear regression for each
    def compute_slope(group):
        X = group['days_since_start'].values
        y = group['clicks'].values
        
        if len(X) < 2:
            return pd.Series({
                'trend_slope': 0.0,
                'days_with_data': len(X),
                'current_position': group['position'].mean(),
                'current_clicks': group['clicks'].tail(30).mean()  # Last 30 days average
            })
        
        # Linear regression: slope of best fit line
        slope, intercept, r_value, p_value, std_err = stats.linregress(X, y)
        
        # Project days until position falls below 10 (page 1 threshold)
        current_position = group['position'].tail(30).mean()
        position_slope = 0.0
        
        # Compute position slope if we have position data
        if 'position' in group.columns and not group['position'].isna().all():
            X_pos = group['days_since_start'].values
            y_pos = group['position'].values
            valid_idx = ~np.isnan(y_pos)
            if valid_idx.sum() >= 2:
                pos_slope, _, _, _, _ = stats.linregress(X_pos[valid_idx], y_pos[valid_idx])
                position_slope = pos_slope
        
        return pd.Series({
            'trend_slope': slope,
            'days_with_data': len(X),
            'current_position': current_position,
            'current_clicks': group['clicks'].tail(30).mean(),
            'position_slope': position_slope
        })
    
    trends = df.groupby('page').apply(compute_slope).reset_index()
    
    # Bucket pages based on trend slope (vectorized)
    conditions = [
        trends['trend_slope'] > 0.1,
        (trends['trend_slope'] >= -0.1) & (trends['trend_slope'] <= 0.1),
        (trends['trend_slope'] >= -0.5) & (trends['trend_slope'] < -0.1),
        trends['trend_slope'] < -0.5
    ]
    choices = ['growing', 'stable', 'decaying', 'critical']
    trends['bucket'] = np.select(conditions, choices, default='stable')
    
    # Project page 1 loss date for decaying/critical pages (vectorized)
    def project_loss_date(row):
        if row['bucket'] in ['decaying', 'critical'] and row['position_slope'] > 0:
            # Days until position reaches 10
            days_until_loss = (10 - row['current_position']) / row['position_slope']
            if 0 < days_until_loss < 365:  # Within next year
                loss_date = datetime.now() + timedelta(days=days_until_loss)
                return loss_date.strftime('%Y-%m-%d')
        return None
    
    trends['projected_page1_loss_date'] = trends.apply(project_loss_date, axis=1)
    
    return trends


def _detect_ctr_anomalies_vectorized(gsc_page_summary: pd.DataFrame) -> pd.DataFrame:
    """
    Detect CTR anomalies using Isolation Forest within position groups.
    Fully vectorized implementation.
    
    Returns DataFrame with columns: [page, ctr_anomaly, ctr_expected, ctr_actual]
    """
    df = gsc_page_summary.copy()
    
    # Round position to group similar positions
    df['position_group'] = np.round(df['avg_position'])
    
    # Initialize result columns
    df['ctr_anomaly'] = False
    df['ctr_expected'] = df['avg_ctr']
    
    # Group by position and detect anomalies within each group
    anomaly_results = []
    
    for position_group, group in df.groupby('position_group'):
        if len(group) < 5:  # Need at least 5 samples for isolation forest
            group_result = group[['page']].copy()
            group_result['ctr_anomaly'] = False
            group_result['ctr_expected'] = group['avg_ctr'].values
            anomaly_results.append(group_result)
            continue
        
        # Compute expected CTR as median of the position group
        expected_ctr = group['avg_ctr'].median()
        
        # Use Isolation Forest to detect anomalies
        X = group[['avg_ctr']].values
        iso_forest = IsolationForest(contamination=0.1, random_state=42)
        predictions = iso_forest.fit_predict(X)
        
        # Create result for this group
        group_result = group[['page']].copy()
        group_result['ctr_anomaly'] = predictions == -1  # -1 indicates anomaly
        group_result['ctr_expected'] = expected_ctr
        
        # Only flag as anomaly if CTR is below expected (underperformance)
        group_result['ctr_anomaly'] = (
            group_result['ctr_anomaly'] & 
            (group['avg_ctr'].values < expected_ctr)
        )
        
        anomaly_results.append(group_result)
    
    # Combine all groups
    anomaly_df = pd.concat(anomaly_results, ignore_index=True)
    
    # Merge with original data to get actual CTR
    result = anomaly_df.merge(
        df[['page', 'avg_ctr']], 
        on='page', 
        how='left'
    )
    result.rename(columns={'avg_ctr': 'ctr_actual'}, inplace=True)
    
    return result[['page', 'ctr_anomaly', 'ctr_expected', 'ctr_actual']]


def _flag_low_engagement_vectorized(ga4_landing_data: pd.DataFrame) -> pd.DataFrame:
    """
    Flag pages with low engagement metrics using vectorized operations.
    
    Returns DataFrame with columns: [page, engagement_flag, bounce_rate, avg_session_duration]
    """
    if len(ga4_landing_data) == 0:
        return pd.DataFrame(columns=['page', 'engagement_flag', 'bounce_rate', 'avg_session_duration'])
    
    df = ga4_landing_data.copy()
    
    # Vectorized engagement flagging
    low_engagement = (
        (df['bounce_rate'] > 0.80) | 
        (df['avg_session_duration'] < 30)
    )
    
    df['engagement_flag'] = np.where(low_engagement, 'low_engagement', 'normal')
    
    return df[['page', 'engagement_flag', 'bounce_rate', 'avg_session_duration']]


def _merge_page_analysis(
    trend_results: pd.DataFrame,
    ctr_anomalies: pd.DataFrame,
    engagement_flags: pd.DataFrame,
    gsc_page_summary: pd.DataFrame
) -> pd.DataFrame:
    """
    Merge all analysis components into a single DataFrame.
    """
    # Start with trend results
    merged = trend_results.copy()
    
    # Add CTR anomaly data
    merged = merged.merge(ctr_anomalies, on='page', how='left')
    
    # Add engagement flags
    merged = merged.merge(engagement_flags, on='page', how='left')
    
    # Add summary data (total clicks, impressions)
    merged = merged.merge(
        gsc_page_summary[['page', 'total_clicks', 'total_impressions', 'avg_position']],
        on='page',
        how='left',
        suffixes=('', '_summary')
    )
    
    # Use summary avg_position if current_position is missing
    merged['current_position'] = merged['current_position'].fillna(merged['avg_position'])
    
    # Calculate monthly clicks from current clicks (30-day average)
    merged['current_monthly_clicks'] = merged['current_clicks'] * 30
    
    # Fill NaN values
    merged['ctr_anomaly'] = merged['ctr_anomaly'].fillna(False)
    merged['engagement_flag'] = merged['engagement_flag'].fillna('unknown')
    
    return merged


def _compute_priority_scores_vectorized(page_analysis: pd.DataFrame) -> pd.DataFrame:
    """
    Compute priority scores for all pages using vectorized operations.
    
    Priority score = (current_monthly_clicks × abs(decay_rate)) × recoverability_factor
    """
    df = page_analysis.copy()
    
    # Calculate decay rate (trend slope)
    df['decay_rate'] = -df['trend_slope']  # Negative slope = decay
    
    # Recoverability factor based on:
    # 1. Current position (easier from position 8 than 25)
    # 2. Whether it's a CTR problem (easy fix)
    # 3. Bucket severity
    
    # Position factor: scale from 1.0 (position 1-10) to 0.2 (position 50+)
    position_factor = np.clip(1.2 - (df['current_position'] / 50), 0.2, 1.0)
    
    # CTR problem factor: 1.5x if CTR anomaly (easy fix)
    ctr_factor = np.where(df['ctr_anomaly'] == True, 1.5, 1.0)
    
    # Bucket severity factor
    bucket_factors = {'critical': 2.0, 'decaying': 1.5, 'stable': 0.5, 'growing': 0.2}
    bucket_factor = df['bucket'].map(bucket_factors).fillna(1.0)
    
    # Combined recoverability factor
    recoverability_factor = position_factor * ctr_factor * bucket_factors.get(df['bucket'].iloc[0], 1.0)
    
    # Vectorized recoverability calculation
    df['recoverability_factor'] = df.apply(
        lambda row: (
            position_factor[row.name] * 
            ctr_factor[row.name] * 
            bucket_factors.get(row['bucket'], 1.0)
        ),
        axis=1
    )
    
    # Priority score
    df['priority_score'] = (
        df['current_monthly_clicks'].fillna(0) * 
        df['decay_rate'].abs() * 
        df['recoverability_factor']
    )
    
    # Determine recommended action (vectorized)
    def get_recommendation(row):
        if row['ctr_anomaly']:
            return 'title_rewrite'
        elif row['engagement_flag'] == 'low_engagement':
            return 'content_refresh'
        elif row['bucket'] == 'critical':
            return 'urgent_content_update'
        elif row['bucket'] == 'decaying':
            return 'content_optimization'
        elif row['bucket'] == 'growing':
            return 'double_down'
        else:
            return 'monitor'
    
    df['recommended_action'] = df.apply(get_recommendation, axis=1)
    
    # Sort by priority score descending
    df = df.sort_values('priority_score', ascending=False)
    
    return df


def _generate_summary_stats(page_analysis: pd.DataFrame) -> Dict:
    """
    Generate summary statistics from page analysis.
    """
    # Count pages by bucket
    bucket_counts = page_analysis['bucket'].value_counts().to_dict()
    
    # Total recoverable clicks (from decaying + critical pages)
    recoverable = page_analysis[
        page_analysis['bucket'].isin(['decaying', 'critical'])
    ]['current_monthly_clicks'].sum()
    
    return {
        'total_pages_analyzed': len(page_analysis),
        'growing': bucket_counts.get('growing', 0),
        'stable': bucket_counts.get('stable', 0),
        'decaying': bucket_counts.get('decaying', 0),
        'critical': bucket_counts.get('critical', 0),
        'total_recoverable_clicks_monthly': int(recoverable)
    }
