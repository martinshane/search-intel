"""
Module 2: Page-Level Triage

Analyzes per-page performance trends, CTR anomalies, and engagement patterns
to identify pages that need immediate attention.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from sklearn.ensemble import IsolationForest
from scipy import stats
import logging

logger = logging.getLogger(__name__)


def analyze_page_triage(
    page_daily_data: pd.DataFrame,
    ga4_landing_data: pd.DataFrame,
    gsc_page_summary: pd.DataFrame
) -> Dict[str, Any]:
    """
    Perform comprehensive page-level triage analysis.
    
    Args:
        page_daily_data: GSC per-page daily time series with columns:
            - page (str): URL
            - date (datetime): Date
            - clicks (int): Daily clicks
            - impressions (int): Daily impressions
            - ctr (float): Click-through rate
            - position (float): Average position
        ga4_landing_data: GA4 landing page engagement data with columns:
            - landing_page (str): URL (may need normalization)
            - sessions (int): Total sessions
            - bounce_rate (float): Bounce rate (0-1)
            - avg_session_duration (float): Average session duration in seconds
            - conversions (int): Total conversions
        gsc_page_summary: GSC page-level summary with columns:
            - page (str): URL
            - clicks (int): Total clicks in period
            - impressions (int): Total impressions
            - ctr (float): Average CTR
            - position (float): Average position
    
    Returns:
        Dictionary containing:
            - pages: List of analyzed pages with trend analysis
            - summary: Aggregate statistics
            - ctr_anomalies: Pages with unusual CTR patterns
            - engagement_flags: Pages with engagement issues
    """
    try:
        # Validate input data
        if page_daily_data.empty:
            raise ValueError("page_daily_data is empty")
        
        # 1. Per-page trend fitting
        logger.info("Starting per-page trend analysis")
        page_trends = _analyze_page_trends(page_daily_data)
        
        # 2. CTR anomaly detection
        logger.info("Detecting CTR anomalies")
        ctr_anomalies = _detect_ctr_anomalies(gsc_page_summary)
        
        # 3. Engagement cross-reference
        logger.info("Cross-referencing engagement data")
        engagement_flags = _analyze_engagement(ga4_landing_data)
        
        # 4. Priority scoring
        logger.info("Calculating priority scores")
        prioritized_pages = _calculate_priority_scores(
            page_trends,
            ctr_anomalies,
            engagement_flags,
            gsc_page_summary
        )
        
        # 5. Generate summary statistics
        summary = _generate_summary(prioritized_pages)
        
        return {
            "pages": prioritized_pages,
            "summary": summary,
            "ctr_anomalies": ctr_anomalies,
            "engagement_flags": engagement_flags,
            "generated_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error in page triage analysis: {str(e)}")
        raise


def _analyze_page_trends(page_daily_data: pd.DataFrame) -> pd.DataFrame:
    """
    Fit linear regression to each page's click trend and classify trajectory.
    
    Returns DataFrame with columns:
        - page
        - trend_slope (clicks/day change rate)
        - trend_r_squared
        - bucket (growing, stable, decaying, critical)
        - projected_page1_loss_date (if applicable)
    """
    # Minimum days of data required for trend analysis
    MIN_DAYS = 30
    
    # Group by page and calculate trends
    trends = []
    
    for page, group in page_daily_data.groupby('page'):
        if len(group) < MIN_DAYS:
            continue
        
        # Sort by date
        group = group.sort_values('date')
        
        # Create numeric date index (days since start)
        group['day_index'] = (group['date'] - group['date'].min()).dt.days
        
        # Fit linear regression on clicks
        if group['clicks'].sum() == 0:
            # Skip pages with no clicks
            continue
        
        X = group['day_index'].values.reshape(-1, 1)
        y = group['clicks'].values
        
        # Handle edge cases
        if len(X) < 2 or np.std(y) == 0:
            continue
        
        slope, intercept, r_value, p_value, std_err = stats.linregress(
            X.flatten(), y
        )
        
        # Calculate current metrics
        current_clicks = group['clicks'].tail(30).sum()  # Last 30 days
        current_position = group['position'].tail(30).mean()
        
        # Project when page might fall below position 10 (page 1 boundary)
        projected_loss_date = None
        if slope < 0 and current_position < 10:
            # Estimate days until position > 10
            # This is simplified - in production would use position trend too
            position_slope = 0
            if len(group) >= 30:
                pos_X = group['day_index'].tail(30).values.reshape(-1, 1)
                pos_y = group['position'].tail(30).values
                if np.std(pos_y) > 0:
                    pos_slope, _, _, _, _ = stats.linregress(
                        pos_X.flatten(), pos_y
                    )
            
            if pos_slope > 0:
                days_to_loss = (10 - current_position) / pos_slope
                if 0 < days_to_loss < 365:
                    projected_loss_date = (
                        datetime.utcnow() + timedelta(days=days_to_loss)
                    ).date().isoformat()
        
        # Classify bucket based on slope
        if slope > 0.1:
            bucket = "growing"
        elif slope >= -0.1:
            bucket = "stable"
        elif slope >= -0.5:
            bucket = "decaying"
        else:
            bucket = "critical"
        
        trends.append({
            'page': page,
            'trend_slope': slope,
            'trend_r_squared': r_value ** 2,
            'bucket': bucket,
            'current_monthly_clicks': int(current_clicks),
            'current_position': round(current_position, 1),
            'projected_page1_loss_date': projected_loss_date
        })
    
    return pd.DataFrame(trends)


def _detect_ctr_anomalies(gsc_page_summary: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Use Isolation Forest to detect pages with anomalously low CTR
    within their position group.
    
    Returns list of anomalous pages with expected vs actual CTR.
    """
    anomalies = []
    
    # Filter pages with meaningful impressions
    min_impressions = 100
    filtered = gsc_page_summary[
        gsc_page_summary['impressions'] >= min_impressions
    ].copy()
    
    if len(filtered) < 10:
        logger.warning("Not enough pages for CTR anomaly detection")
        return anomalies
    
    # Round position to nearest integer for grouping
    filtered['position_group'] = filtered['position'].round()
    
    # Analyze each position group
    for position, group in filtered.groupby('position_group'):
        if len(group) < 5:
            continue
        
        # Calculate expected CTR as median of the group
        expected_ctr = group['ctr'].median()
        
        # Use Isolation Forest to find outliers
        X = group[['ctr']].values
        
        try:
            clf = IsolationForest(
                contamination=0.1,
                random_state=42,
                n_estimators=100
            )
            predictions = clf.fit_predict(X)
            
            # Get anomalies (prediction = -1)
            anomaly_mask = predictions == -1
            anomalous_pages = group[anomaly_mask]
            
            for _, page_row in anomalous_pages.iterrows():
                # Only flag if CTR is below expected (not above)
                if page_row['ctr'] < expected_ctr:
                    anomalies.append({
                        'page': page_row['page'],
                        'position': round(page_row['position'], 1),
                        'expected_ctr': round(expected_ctr, 4),
                        'actual_ctr': round(page_row['ctr'], 4),
                        'ctr_gap': round(expected_ctr - page_row['ctr'], 4),
                        'impressions': int(page_row['impressions']),
                        'potential_clicks': int(
                            page_row['impressions'] * (expected_ctr - page_row['ctr'])
                        )
                    })
        except Exception as e:
            logger.warning(
                f"Could not detect anomalies for position {position}: {str(e)}"
            )
            continue
    
    # Sort by potential clicks descending
    anomalies.sort(key=lambda x: x['potential_clicks'], reverse=True)
    
    return anomalies


def _analyze_engagement(ga4_landing_data: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Flag pages with high search traffic but poor engagement metrics.
    
    Returns list of pages with engagement issues.
    """
    engagement_flags = []
    
    if ga4_landing_data.empty:
        logger.warning("No GA4 landing page data available")
        return engagement_flags
    
    # Define thresholds
    HIGH_BOUNCE_THRESHOLD = 0.80
    LOW_SESSION_THRESHOLD = 30  # seconds
    MIN_SESSIONS = 50
    
    # Filter to pages with meaningful traffic
    filtered = ga4_landing_data[
        ga4_landing_data['sessions'] >= MIN_SESSIONS
    ].copy()
    
    for _, row in filtered.iterrows():
        flags = []
        
        if row['bounce_rate'] > HIGH_BOUNCE_THRESHOLD:
            flags.append('high_bounce')
        
        if row['avg_session_duration'] < LOW_SESSION_THRESHOLD:
            flags.append('low_engagement')
        
        if flags:
            engagement_flags.append({
                'page': row['landing_page'],
                'sessions': int(row['sessions']),
                'bounce_rate': round(row['bounce_rate'], 3),
                'avg_session_duration': round(row['avg_session_duration'], 1),
                'flags': flags,
                'severity': 'high' if len(flags) > 1 else 'medium'
            })
    
    # Sort by sessions descending (prioritize high-traffic pages)
    engagement_flags.sort(key=lambda x: x['sessions'], reverse=True)
    
    return engagement_flags


def _calculate_priority_scores(
    page_trends: pd.DataFrame,
    ctr_anomalies: List[Dict[str, Any]],
    engagement_flags: List[Dict[str, Any]],
    gsc_page_summary: pd.DataFrame
) -> List[Dict[str, Any]]:
    """
    Calculate priority scores for each page and generate recommendations.
    
    Priority score formula:
    score = (current_monthly_clicks × abs(decay_rate)) × recoverability_factor
    
    recoverability_factor based on:
    - How recently decay started (recent = easier)
    - Current position (easier to recover from #8 than #25)
    - Whether it's a CTR problem (easy fix) vs position problem (hard fix)
    """
    # Create lookup dictionaries
    ctr_anomaly_dict = {a['page']: a for a in ctr_anomalies}
    engagement_dict = {e['page']: e for e in engagement_flags}
    
    prioritized = []
    
    for _, row in page_trends.iterrows():
        page = row['page']
        
        # Get CTR anomaly info if exists
        ctr_anomaly = ctr_anomaly_dict.get(page)
        ctr_flag = ctr_anomaly is not None
        
        # Get engagement info if exists
        engagement = engagement_dict.get(page)
        engagement_flag = engagement['flags'][0] if engagement else None
        
        # Calculate recoverability factor
        recoverability = 1.0
        
        # Position factor (easier to recover from higher positions)
        if row['current_position'] <= 10:
            recoverability *= 1.5
        elif row['current_position'] <= 20:
            recoverability *= 1.2
        else:
            recoverability *= 0.8
        
        # CTR problem is easier to fix than position problem
        if ctr_flag:
            recoverability *= 1.3
        
        # Recent decay is easier to recover
        if row['bucket'] in ['decaying', 'critical']:
            if row['trend_r_squared'] > 0.5:  # Clear trend
                recoverability *= 1.2
        
        # Calculate priority score
        priority_score = (
            row['current_monthly_clicks'] * 
            abs(row['trend_slope']) * 
            recoverability
        )
        
        # Determine recommended action
        recommended_action = _determine_recommended_action(
            row, ctr_flag, engagement_flag
        )
        
        prioritized.append({
            'url': page,
            'bucket': row['bucket'],
            'current_monthly_clicks': row['current_monthly_clicks'],
            'current_position': row['current_position'],
            'trend_slope': round(row['trend_slope'], 3),
            'trend_r_squared': round(row['trend_r_squared'], 3),
            'projected_page1_loss_date': row.get('projected_page1_loss_date'),
            'ctr_anomaly': ctr_flag,
            'ctr_expected': ctr_anomaly['expected_ctr'] if ctr_anomaly else None,
            'ctr_actual': ctr_anomaly['actual_ctr'] if ctr_anomaly else None,
            'potential_click_gain_ctr': (
                ctr_anomaly['potential_clicks'] if ctr_anomaly else 0
            ),
            'engagement_flag': engagement_flag,
            'priority_score': round(priority_score, 2),
            'recommended_action': recommended_action
        })
    
    # Sort by priority score descending
    prioritized.sort(key=lambda x: x['priority_score'], reverse=True)
    
    return prioritized


def _determine_recommended_action(
    page_row: pd.Series,
    has_ctr_anomaly: bool,
    engagement_flag: Optional[str]
) -> str:
    """
    Determine the recommended action based on page characteristics.
    """
    if has_ctr_anomaly:
        return "title_meta_rewrite"
    
    if engagement_flag == 'high_bounce':
        return "content_intent_alignment"
    
    if engagement_flag == 'low_engagement':
        return "content_depth_expansion"
    
    if page_row['bucket'] == 'critical':
        if page_row['current_position'] > 20:
            return "comprehensive_content_overhaul"
        else:
            return "technical_seo_audit"
    
    if page_row['bucket'] == 'decaying':
        if page_row['current_position'] <= 10:
            return "content_refresh_and_internal_links"
        else:
            return "content_expansion_and_optimization"
    
    if page_row['bucket'] == 'growing':
        return "accelerate_with_internal_links"
    
    return "monitor"


def _generate_summary(prioritized_pages: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Generate summary statistics across all analyzed pages.
    """
    if not prioritized_pages:
        return {
            "total_pages_analyzed": 0,
            "growing": 0,
            "stable": 0,
            "decaying": 0,
            "critical": 0,
            "total_recoverable_clicks_monthly": 0
        }
    
    # Count by bucket
    bucket_counts = {}
    for page in prioritized_pages:
        bucket = page['bucket']
        bucket_counts[bucket] = bucket_counts.get(bucket, 0) + 1
    
    # Calculate total recoverable clicks
    # Only count decaying and critical pages
    recoverable_clicks = sum(
        page['potential_click_gain_ctr']
        for page in prioritized_pages
        if page['bucket'] in ['decaying', 'critical'] and 
        page['potential_click_gain_ctr'] > 0
    )
    
    return {
        "total_pages_analyzed": len(prioritized_pages),
        "growing": bucket_counts.get('growing', 0),
        "stable": bucket_counts.get('stable', 0),
        "decaying": bucket_counts.get('decaying', 0),
        "critical": bucket_counts.get('critical', 0),
        "total_recoverable_clicks_monthly": int(recoverable_clicks),
        "pages_with_ctr_issues": sum(
            1 for p in prioritized_pages if p['ctr_anomaly']
        ),
        "pages_with_engagement_issues": sum(
            1 for p in prioritized_pages if p['engagement_flag']
        )
    }
