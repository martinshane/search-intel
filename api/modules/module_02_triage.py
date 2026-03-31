"""
Module 2: Page-Level Triage

Analyzes per-page performance trends, detects CTR anomalies, flags engagement issues,
and prioritizes pages for action based on decay rate and recoverability.

Refined to handle:
- Minimum data requirements (pages with insufficient history)
- Improved decay rate calculation for irregular traffic patterns
- Better handling of pages with seasonal or volatile traffic
"""

import pandas as pd
import numpy as np
from scipy import stats
from sklearn.ensemble import IsolationForest
from typing import Dict, List, Any, Optional
import warnings
warnings.filterwarnings('ignore')


def analyze_page_triage(
    page_daily_data: pd.DataFrame,
    ga4_landing_data: Optional[pd.DataFrame] = None,
    gsc_page_summary: Optional[pd.DataFrame] = None
) -> Dict[str, Any]:
    """
    Performs page-level triage analysis.
    
    Args:
        page_daily_data: DataFrame with columns [page, date, clicks, impressions, ctr, position]
        ga4_landing_data: DataFrame with columns [page, bounce_rate, avg_session_duration, sessions]
        gsc_page_summary: DataFrame with columns [page, total_clicks, total_impressions, avg_position]
    
    Returns:
        Dictionary containing:
        - pages: List of page analysis objects with trend, CTR anomaly, engagement flags
        - summary: Aggregated statistics across all pages
    """
    
    if page_daily_data is None or len(page_daily_data) == 0:
        return _empty_result()
    
    # Ensure date column is datetime
    page_daily_data = page_daily_data.copy()
    page_daily_data['date'] = pd.to_datetime(page_daily_data['date'])
    
    # Analyze each page
    pages = []
    for page_url in page_daily_data['page'].unique():
        page_data = page_daily_data[page_daily_data['page'] == page_url].sort_values('date')
        
        # Get GA4 data for this page if available
        ga4_data = None
        if ga4_landing_data is not None and len(ga4_landing_data) > 0:
            ga4_match = ga4_landing_data[ga4_landing_data['page'] == page_url]
            if len(ga4_match) > 0:
                ga4_data = ga4_match.iloc[0]
        
        # Get GSC summary for this page if available
        gsc_summary = None
        if gsc_page_summary is not None and len(gsc_page_summary) > 0:
            gsc_match = gsc_page_summary[gsc_page_summary['page'] == page_url]
            if len(gsc_match) > 0:
                gsc_summary = gsc_match.iloc[0]
        
        page_analysis = _analyze_single_page(page_url, page_data, ga4_data, gsc_summary)
        if page_analysis is not None:
            pages.append(page_analysis)
    
    # Detect CTR anomalies across all pages
    pages = _detect_ctr_anomalies(pages)
    
    # Calculate priority scores
    pages = _calculate_priority_scores(pages)
    
    # Sort by priority score descending
    pages.sort(key=lambda x: x.get('priority_score', 0), reverse=True)
    
    # Generate summary statistics
    summary = _generate_summary(pages)
    
    return {
        'pages': pages,
        'summary': summary
    }


def _analyze_single_page(
    page_url: str,
    page_data: pd.DataFrame,
    ga4_data: Optional[pd.Series],
    gsc_summary: Optional[pd.Series]
) -> Optional[Dict[str, Any]]:
    """
    Analyzes a single page's performance trends and flags issues.
    
    Returns None if page doesn't meet minimum data requirements.
    """
    
    # Minimum data requirements: at least 14 days of data
    if len(page_data) < 14:
        return None
    
    # Filter out days with zero clicks AND zero impressions (likely data gaps)
    page_data = page_data[(page_data['clicks'] > 0) | (page_data['impressions'] > 0)].copy()
    
    if len(page_data) < 14:
        return None
    
    # Calculate current monthly metrics (last 30 days)
    last_30_days = page_data.tail(30)
    current_monthly_clicks = last_30_days['clicks'].sum()
    current_monthly_impressions = last_30_days['impressions'].sum()
    
    # Require minimum traffic to be worth analyzing
    if current_monthly_clicks < 5 and current_monthly_impressions < 100:
        return None
    
    # Calculate average position
    avg_position = page_data['position'].mean() if 'position' in page_data.columns else None
    
    # Calculate trend with improved handling of irregular patterns
    trend_result = _calculate_trend(page_data)
    
    # Determine bucket based on trend
    bucket = _determine_bucket(trend_result['slope'], trend_result['confidence'])
    
    # Calculate projected page 1 loss date if decaying
    projected_loss_date = None
    if bucket in ['decaying', 'critical'] and avg_position is not None and avg_position <= 15:
        projected_loss_date = _calculate_projected_loss_date(
            page_data, avg_position, trend_result['slope']
        )
    
    # Check engagement issues from GA4 data
    engagement_flag = None
    if ga4_data is not None:
        engagement_flag = _check_engagement_issues(ga4_data)
    
    # Calculate actual CTR
    actual_ctr = current_monthly_clicks / current_monthly_impressions if current_monthly_impressions > 0 else 0
    
    result = {
        'url': page_url,
        'bucket': bucket,
        'current_monthly_clicks': int(current_monthly_clicks),
        'current_monthly_impressions': int(current_monthly_impressions),
        'trend_slope': round(trend_result['slope'], 4),
        'trend_confidence': round(trend_result['confidence'], 3),
        'trend_r_squared': round(trend_result['r_squared'], 3),
        'avg_position': round(avg_position, 1) if avg_position is not None else None,
        'actual_ctr': round(actual_ctr, 4),
        'engagement_flag': engagement_flag,
        'projected_page1_loss_date': projected_loss_date,
        'days_of_data': len(page_data),
        'data_regularity': trend_result['data_regularity']
    }
    
    # Add GA4 metrics if available
    if ga4_data is not None:
        result['bounce_rate'] = round(float(ga4_data.get('bounce_rate', 0)), 3)
        result['avg_session_duration'] = round(float(ga4_data.get('avg_session_duration', 0)), 1)
        result['sessions'] = int(ga4_data.get('sessions', 0))
    
    return result


def _calculate_trend(page_data: pd.DataFrame) -> Dict[str, float]:
    """
    Calculates trend with improved handling for irregular traffic patterns.
    
    Uses weighted regression to emphasize recent data and applies
    outlier-resistant techniques for volatile traffic.
    """
    
    # Convert dates to numeric (days since first date)
    page_data = page_data.copy()
    page_data['days'] = (page_data['date'] - page_data['date'].min()).dt.days
    
    # Check data regularity (gaps in time series)
    total_days_span = page_data['days'].max() - page_data['days'].min() + 1
    data_regularity = len(page_data) / total_days_span if total_days_span > 0 else 0
    
    clicks = page_data['clicks'].values
    days = page_data['days'].values
    
    # Handle zero-inflated data (many days with zero clicks)
    non_zero_ratio = np.sum(clicks > 0) / len(clicks)
    
    if non_zero_ratio < 0.3:  # Very sparse data
        # Use only non-zero days for trend calculation
        non_zero_mask = clicks > 0
        if np.sum(non_zero_mask) < 7:
            # Not enough non-zero data points
            return {
                'slope': 0.0,
                'confidence': 0.0,
                'r_squared': 0.0,
                'data_regularity': data_regularity
            }
        clicks = clicks[non_zero_mask]
        days = days[non_zero_mask]
    
    # Apply smoothing for volatile data
    if len(clicks) > 7:
        # Use 7-day rolling average to reduce noise
        smoothed = pd.Series(clicks).rolling(window=min(7, len(clicks)), min_periods=1).mean().values
    else:
        smoothed = clicks
    
    # Weight recent data more heavily (exponential decay)
    # More recent = higher weight
    weights = np.exp(np.linspace(-1, 0, len(days)))
    weights = weights / weights.sum()  # Normalize
    
    try:
        # Weighted linear regression on smoothed data
        # Using log(clicks + 1) for better handling of exponential decay patterns
        y = np.log1p(smoothed)  # log(1 + clicks) to handle zeros
        
        # Fit weighted regression
        W = np.diag(weights)
        X = np.column_stack([np.ones(len(days)), days])
        XtW = X.T @ W
        beta = np.linalg.lstsq(XtW @ X, XtW @ y, rcond=None)[0]
        
        # Convert log-space slope back to clicks/day
        # Slope in log space represents relative growth rate
        log_slope = beta[1]
        
        # Calculate predictions and R²
        y_pred = X @ beta
        ss_res = np.sum(weights * (y - y_pred) ** 2)
        ss_tot = np.sum(weights * (y - np.average(y, weights=weights)) ** 2)
        r_squared = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0
        
        # Convert relative growth rate to absolute clicks/day
        # This is approximate: exp(log_slope) - 1 gives daily % change
        # Multiply by average clicks to get absolute change
        avg_clicks = np.mean(smoothed)
        daily_change_pct = np.exp(log_slope) - 1
        slope = avg_clicks * daily_change_pct
        
        # Confidence based on R² and data regularity
        confidence = r_squared * data_regularity
        
        return {
            'slope': slope,
            'confidence': max(0, min(1, confidence)),
            'r_squared': max(0, min(1, r_squared)),
            'data_regularity': data_regularity
        }
        
    except (np.linalg.LinAlgError, ValueError):
        # Regression failed, return neutral trend
        return {
            'slope': 0.0,
            'confidence': 0.0,
            'r_squared': 0.0,
            'data_regularity': data_regularity
        }


def _determine_bucket(slope: float, confidence: float) -> str:
    """
    Classifies page into bucket based on trend slope and confidence.
    
    Only assigns strong buckets (growing/critical) when confidence is high.
    Uses more conservative thresholds for irregular data.
    """
    
    # Require minimum confidence to classify as non-stable
    min_confidence = 0.3
    
    if confidence < min_confidence:
        return 'stable'
    
    # Thresholds in clicks per day
    if slope > 0.5:
        return 'growing'
    elif slope > 0.1:
        return 'stable'
    elif slope > -0.3:
        return 'stable'
    elif slope > -1.0:
        return 'decaying'
    else:
        return 'critical'


def _calculate_projected_loss_date(
    page_data: pd.DataFrame,
    current_position: float,
    slope: float
) -> Optional[str]:
    """
    Projects when page will fall below position 10 (off page 1).
    
    Returns None if page is already off page 1 or trend is not declining.
    """
    
    if current_position > 10 or slope >= 0:
        return None
    
    # Estimate position decay rate from click decay rate
    # Rough approximation: each click lost corresponds to position drop
    # This is highly simplified; real relationship is complex
    
    # Get recent position trend if available
    if 'position' in page_data.columns:
        recent_data = page_data.tail(30)
        if len(recent_data) > 7:
            days = (recent_data['date'] - recent_data['date'].min()).dt.days.values
            positions = recent_data['position'].values
            
            try:
                slope_pos, _, _, _, _ = stats.linregress(days, positions)
                
                if slope_pos > 0:  # Position is increasing (getting worse)
                    # Days until position reaches 10
                    days_until_loss = (10 - current_position) / slope_pos
                    
                    if 0 < days_until_loss < 365:  # Only if within next year
                        loss_date = page_data['date'].max() + pd.Timedelta(days=int(days_until_loss))
                        return loss_date.strftime('%Y-%m-%d')
            except:
                pass
    
    return None


def _check_engagement_issues(ga4_data: pd.Series) -> Optional[str]:
    """
    Flags pages with poor engagement metrics from GA4 data.
    """
    
    bounce_rate = float(ga4_data.get('bounce_rate', 0))
    avg_session = float(ga4_data.get('avg_session_duration', 0))
    
    if bounce_rate > 0.8 and avg_session < 30:
        return 'low_engagement'
    elif bounce_rate > 0.7 and avg_session < 45:
        return 'moderate_engagement_issues'
    
    return None


def _detect_ctr_anomalies(pages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Detects CTR anomalies using position-grouped isolation forest.
    
    Pages with anomalously low CTR for their position are flagged.
    """
    
    if len(pages) < 10:  # Need sufficient pages for anomaly detection
        for page in pages:
            page['ctr_anomaly'] = False
            page['ctr_expected'] = None
        return pages
    
    # Group pages by position (rounded to nearest integer)
    position_groups = {}
    for page in pages:
        if page['avg_position'] is not None:
            pos_bucket = round(page['avg_position'])
            if pos_bucket not in position_groups:
                position_groups[pos_bucket] = []
            position_groups[pos_bucket].append(page)
    
    # Run isolation forest within each position group
    for pos_bucket, group_pages in position_groups.items():
        if len(group_pages) < 5:  # Need minimum pages per group
            for page in group_pages:
                page['ctr_anomaly'] = False
                page['ctr_expected'] = None
            continue
        
        # Extract CTR values
        ctrs = np.array([p['actual_ctr'] for p in group_pages]).reshape(-1, 1)
        
        # Handle case where all CTRs are identical or very similar
        if np.std(ctrs) < 0.001:
            for page in group_pages:
                page['ctr_anomaly'] = False
                page['ctr_expected'] = page['actual_ctr']
            continue
        
        try:
            # Fit isolation forest
            iso_forest = IsolationForest(contamination=0.1, random_state=42)
            predictions = iso_forest.fit_predict(ctrs)
            
            # Calculate expected CTR (median of the group)
            expected_ctr = np.median(ctrs)
            
            # Flag anomalies (predictions == -1) that are BELOW expected
            for i, page in enumerate(group_pages):
                page['ctr_expected'] = float(expected_ctr)
                page['ctr_anomaly'] = bool(predictions[i] == -1 and page['actual_ctr'] < expected_ctr)
        except:
            # Isolation forest failed, skip anomaly detection for this group
            for page in group_pages:
                page['ctr_anomaly'] = False
                page['ctr_expected'] = None
    
    return pages


def _calculate_priority_scores(pages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Calculates priority score for each page and recommends action.
    
    Priority = (current_monthly_clicks × abs(decay_rate)) × recoverability_factor
    """
    
    for page in pages:
        clicks = page['current_monthly_clicks']
        slope = page['trend_slope']
        confidence = page['trend_confidence']
        bucket = page['bucket']
        position = page.get('avg_position')
        ctr_anomaly = page.get('ctr_anomaly', False)
        engagement_flag = page.get('engagement_flag')
        
        # Base impact: clicks at risk
        if slope < 0:
            # Monthly decay estimate
            monthly_decay = abs(slope) * 30
            base_impact = clicks * (monthly_decay / max(clicks, 1))
        else:
            base_impact = 0
        
        # Recoverability factor
        recoverability = 1.0
        
        # Easier to recover if CTR issue (just rewrite title)
        if ctr_anomaly:
            recoverability *= 2.0
        
        # Easier to recover from better positions
        if position is not None:
            if position <= 5:
                recoverability *= 1.5
            elif position <= 10:
                recoverability *= 1.2
            elif position > 20:
                recoverability *= 0.5
        
        # Recent decay is more recoverable
        if bucket == 'decaying':
            recoverability *= 1.3
        elif bucket == 'critical':
            recoverability *= 1.1  # Critical is harder (decay is advanced)
        
        # Low confidence reduces priority
        recoverability *= confidence
        
        # Calculate final score
        priority_score = base_impact * recoverability
        
        # Determine recommended action
        recommended_action = _determine_recommended_action(page)
        
        page['priority_score'] = round(priority_score, 2)
        page['recommended_action'] = recommended_action
    
    return pages


def _determine_recommended_action(page: Dict[str, Any]) -> str:
    """
    Determines the recommended action for a page based on its issues.
    """
    
    ctr_anomaly = page.get('ctr_anomaly', False)
    engagement_flag = page.get('engagement_flag')
    bucket = page['bucket']
    
    if ctr_anomaly:
        return 'title_rewrite'
    elif engagement_flag == 'low_engagement':
        return 'content_mismatch_review'
    elif bucket == 'critical':
        return 'urgent_content_refresh'
    elif bucket == 'decaying':
        return 'content_update'
    elif bucket == 'growing':
        return 'amplify_with_links'
    else:
        return 'monitor'


def _generate_summary(pages: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Generates summary statistics across all analyzed pages.
    """
    
    total_pages = len(pages)
    
    # Count by bucket
    bucket_counts = {
        'growing': 0,
        'stable': 0,
        'decaying': 0,
        'critical': 0
    }
    
    for page in pages:
        bucket = page['bucket']
        if bucket in bucket_counts:
            bucket_counts[bucket] += 1
    
    # Calculate total recoverable clicks (from decaying + critical pages)
    recoverable_clicks = sum(
        page['current_monthly_clicks']
        for page in pages
        if page['bucket'] in ['decaying', 'critical']
    )
    
    # Count pages with specific issues
    ctr_anomaly_count = sum(1 for p in pages if p.get('ctr_anomaly', False))
    engagement_issue_count = sum(1 for p in pages if p.get('engagement_flag') is not None)
    
    return {
        'total_pages_analyzed': total_pages,
        'growing': bucket_counts['growing'],
        'stable': bucket_counts['stable'],
        'decaying': bucket_counts['decaying'],
        'critical': bucket_counts['critical'],
        'total_recoverable_clicks_monthly': recoverable_clicks,
        'pages_with_ctr_anomalies': ctr_anomaly_count,
        'pages_with_engagement_issues': engagement_issue_count
    }


def _empty_result() -> Dict[str, Any]:
    """
    Returns empty result structure when no data is available.
    """
    return {
        'pages': [],
        'summary': {
            'total_pages_analyzed': 0,
            'growing': 0,
            'stable': 0,
            'decaying': 0,
            'critical': 0,
            'total_recoverable_clicks_monthly': 0,
            'pages_with_ctr_anomalies': 0,
            'pages_with_engagement_issues': 0
        }
    }
