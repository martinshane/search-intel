"""
Module 2: Page-Level Triage
Identifies pages by trajectory (growing/stable/decaying/critical),
detects CTR anomalies, flags engagement issues, and prioritizes actions.
"""

import pandas as pd
import numpy as np
from scipy import stats
from sklearn.ensemble import IsolationForest
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import logging

logger = logging.getLogger(__name__)


def analyze_page_triage(
    page_daily_data: pd.DataFrame,
    ga4_landing_data: Optional[pd.DataFrame],
    gsc_page_summary: pd.DataFrame
) -> Dict[str, Any]:
    """
    Analyze page-level performance with fallbacks for missing data.
    
    Args:
        page_daily_data: GSC per-page daily time series (url, date, clicks, impressions, ctr, position)
        ga4_landing_data: GA4 landing page engagement (url, sessions, bounce_rate, avg_session_duration)
        gsc_page_summary: GSC page summary stats (url, total_clicks, total_impressions, avg_ctr, avg_position)
    
    Returns:
        dict with page analysis results
    """
    logger.info("Starting page-level triage analysis")
    
    try:
        # Validate required data
        if page_daily_data is None or page_daily_data.empty:
            logger.error("No page daily data available")
            return _get_empty_result("No page performance data available")
        
        if gsc_page_summary is None or gsc_page_summary.empty:
            logger.error("No page summary data available")
            return _get_empty_result("No page summary data available")
        
        # Handle missing GA4 data gracefully
        has_ga4 = ga4_landing_data is not None and not ga4_landing_data.empty
        if not has_ga4:
            logger.warning("GA4 data not available - engagement analysis will be limited")
        
        # Ensure required columns exist
        required_daily_cols = ['url', 'date', 'clicks']
        missing_cols = [col for col in required_daily_cols if col not in page_daily_data.columns]
        if missing_cols:
            logger.error(f"Missing required columns in page_daily_data: {missing_cols}")
            return _get_empty_result(f"Missing required data columns: {missing_cols}")
        
        # Convert date column to datetime
        try:
            page_daily_data['date'] = pd.to_datetime(page_daily_data['date'])
        except Exception as e:
            logger.error(f"Error converting dates: {e}")
            return _get_empty_result("Invalid date format in data")
        
        # Step 1: Per-page trend fitting
        logger.info("Analyzing page trends")
        page_trends = _analyze_page_trends(page_daily_data)
        
        # Step 2: CTR anomaly detection
        logger.info("Detecting CTR anomalies")
        ctr_anomalies = _detect_ctr_anomalies(gsc_page_summary)
        
        # Step 3: Engagement cross-reference (if GA4 available)
        logger.info("Analyzing engagement metrics")
        engagement_flags = _analyze_engagement(page_trends, ga4_landing_data) if has_ga4 else {}
        
        # Step 4: Priority scoring
        logger.info("Computing priority scores")
        prioritized_pages = _compute_priority_scores(
            page_trends,
            ctr_anomalies,
            engagement_flags,
            gsc_page_summary
        )
        
        # Generate summary statistics
        summary = _generate_summary(prioritized_pages)
        
        result = {
            "pages": prioritized_pages,
            "summary": summary,
            "data_quality": {
                "has_ga4_data": has_ga4,
                "total_pages_in_dataset": len(page_daily_data['url'].unique()),
                "pages_with_sufficient_history": len([p for p in page_trends if p.get('has_sufficient_data', False)]),
                "date_range_days": (page_daily_data['date'].max() - page_daily_data['date'].min()).days
            }
        }
        
        logger.info(f"Triage analysis complete: {len(prioritized_pages)} pages analyzed")
        return result
        
    except Exception as e:
        logger.error(f"Error in page triage analysis: {e}", exc_info=True)
        return _get_empty_result(f"Analysis failed: {str(e)}")


def _get_empty_result(reason: str) -> Dict[str, Any]:
    """Return empty result structure with error message."""
    return {
        "pages": [],
        "summary": {
            "total_pages_analyzed": 0,
            "growing": 0,
            "stable": 0,
            "decaying": 0,
            "critical": 0,
            "total_recoverable_clicks_monthly": 0,
            "error": reason
        },
        "data_quality": {
            "has_ga4_data": False,
            "total_pages_in_dataset": 0,
            "pages_with_sufficient_history": 0,
            "date_range_days": 0
        }
    }


def _analyze_page_trends(page_daily_data: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Fit linear regression on daily clicks for each page.
    Returns list of page trend data with fallbacks for insufficient history.
    """
    page_trends = []
    
    for url in page_daily_data['url'].unique():
        try:
            page_data = page_daily_data[page_daily_data['url'] == url].copy()
            page_data = page_data.sort_values('date')
            
            # Check for sufficient data
            if len(page_data) < 7:
                # Insufficient history - provide limited analysis
                page_trends.append({
                    'url': url,
                    'has_sufficient_data': False,
                    'days_of_data': len(page_data),
                    'bucket': 'insufficient_data',
                    'trend_slope': 0.0,
                    'current_monthly_clicks': page_data['clicks'].sum() * (30 / len(page_data)) if len(page_data) > 0 else 0,
                    'insufficient_history': True
                })
                continue
            
            # Convert dates to numeric for regression (days since first observation)
            page_data['days_since_start'] = (page_data['date'] - page_data['date'].min()).dt.days
            
            # Handle missing or zero clicks
            if page_data['clicks'].sum() == 0:
                page_trends.append({
                    'url': url,
                    'has_sufficient_data': True,
                    'days_of_data': len(page_data),
                    'bucket': 'no_traffic',
                    'trend_slope': 0.0,
                    'current_monthly_clicks': 0,
                    'no_traffic': True
                })
                continue
            
            # Fit linear regression
            try:
                X = page_data['days_since_start'].values.reshape(-1, 1)
                y = page_data['clicks'].values
                
                slope, intercept, r_value, p_value, std_err = stats.linregress(
                    page_data['days_since_start'].values,
                    y
                )
                
            except Exception as e:
                logger.warning(f"Regression failed for {url}: {e}")
                slope = 0.0
                intercept = page_data['clicks'].mean() if len(page_data) > 0 else 0
                p_value = 1.0
            
            # Calculate current monthly clicks (last 30 days or available data)
            recent_data = page_data[page_data['date'] >= page_data['date'].max() - timedelta(days=30)]
            current_monthly_clicks = recent_data['clicks'].sum()
            
            # If less than 30 days, extrapolate
            if len(recent_data) < 30 and len(recent_data) > 0:
                current_monthly_clicks = recent_data['clicks'].sum() * (30 / len(recent_data))
            
            # Classify bucket based on slope
            if slope > 0.1:
                bucket = 'growing'
            elif slope < -0.5:
                bucket = 'critical'
            elif slope < -0.1:
                bucket = 'decaying'
            else:
                bucket = 'stable'
            
            # Project date when page might fall below position 10 (page 1 threshold)
            avg_position = page_data['position'].mean() if 'position' in page_data.columns else None
            projected_loss_date = None
            
            if avg_position is not None and slope < -0.05 and current_monthly_clicks > 0:
                try:
                    # Rough projection: if losing X clicks/day, when do we hit critical threshold?
                    days_to_critical = abs((current_monthly_clicks * 0.5) / (slope * 30)) if slope < 0 else None
                    if days_to_critical and days_to_critical < 365:
                        projected_loss_date = (datetime.now() + timedelta(days=days_to_critical)).strftime('%Y-%m-%d')
                except:
                    pass
            
            page_trends.append({
                'url': url,
                'has_sufficient_data': True,
                'days_of_data': len(page_data),
                'bucket': bucket,
                'trend_slope': round(slope, 4),
                'trend_intercept': round(intercept, 2),
                'trend_r_squared': round(r_value ** 2, 3) if 'r_value' in locals() else 0,
                'trend_p_value': round(p_value, 4) if 'p_value' in locals() else 1.0,
                'current_monthly_clicks': round(current_monthly_clicks, 1),
                'projected_page1_loss_date': projected_loss_date,
                'avg_position': round(avg_position, 1) if avg_position is not None else None
            })
            
        except Exception as e:
            logger.warning(f"Error analyzing trend for {url}: {e}")
            # Add page with error flag
            page_trends.append({
                'url': url,
                'has_sufficient_data': False,
                'bucket': 'analysis_error',
                'trend_slope': 0.0,
                'current_monthly_clicks': 0,
                'error': str(e)
            })
    
    return page_trends


def _detect_ctr_anomalies(gsc_page_summary: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    """
    Detect pages with anomalously low CTR using Isolation Forest.
    Groups by position and flags outliers within each group.
    """
    if 'avg_position' not in gsc_page_summary.columns or 'avg_ctr' not in gsc_page_summary.columns:
        logger.warning("Missing position or CTR columns for anomaly detection")
        return {}
    
    anomalies = {}
    
    try:
        # Filter to pages with meaningful data
        valid_pages = gsc_page_summary[
            (gsc_page_summary['total_impressions'] >= 100) &
            (gsc_page_summary['avg_position'] <= 20)
        ].copy()
        
        if len(valid_pages) < 10:
            logger.warning(f"Only {len(valid_pages)} pages with sufficient data for CTR anomaly detection")
            return {}
        
        # Round position to group similar positions
        valid_pages['position_group'] = valid_pages['avg_position'].round(0)
        
        # Detect anomalies within each position group
        for position in valid_pages['position_group'].unique():
            try:
                group_pages = valid_pages[valid_pages['position_group'] == position]
                
                # Need at least 5 pages in group for meaningful anomaly detection
                if len(group_pages) < 5:
                    continue
                
                # Prepare features for Isolation Forest
                X = group_pages[['avg_ctr']].values
                
                # Fit Isolation Forest
                iso_forest = IsolationForest(
                    contamination=0.1,
                    random_state=42,
                    n_estimators=100
                )
                predictions = iso_forest.fit_predict(X)
                
                # Flag anomalies (prediction = -1)
                anomaly_mask = predictions == -1
                
                # Calculate expected CTR (median of the group)
                expected_ctr = group_pages['avg_ctr'].median()
                
                # Record anomalies
                for idx, row in group_pages[anomaly_mask].iterrows():
                    anomalies[row['url']] = {
                        'ctr_anomaly': True,
                        'ctr_expected': round(expected_ctr, 4),
                        'ctr_actual': round(row['avg_ctr'], 4),
                        'ctr_deficit': round(expected_ctr - row['avg_ctr'], 4),
                        'position_group': int(position)
                    }
                    
            except Exception as e:
                logger.warning(f"Error detecting anomalies for position group {position}: {e}")
                continue
        
        logger.info(f"Detected {len(anomalies)} CTR anomalies")
        
    except Exception as e:
        logger.error(f"Error in CTR anomaly detection: {e}")
    
    return anomalies


def _analyze_engagement(
    page_trends: List[Dict[str, Any]],
    ga4_landing_data: Optional[pd.DataFrame]
) -> Dict[str, str]:
    """
    Cross-reference with GA4 engagement metrics.
    Returns dict of url -> engagement_flag.
    """
    if ga4_landing_data is None or ga4_landing_data.empty:
        return {}
    
    engagement_flags = {}
    
    try:
        # Ensure required columns exist
        required_cols = ['url', 'sessions']
        if not all(col in ga4_landing_data.columns for col in required_cols):
            logger.warning("GA4 data missing required columns")
            return {}
        
        # Create URL mapping for matching
        url_map = {}
        for _, row in ga4_landing_data.iterrows():
            url_map[row['url']] = row
        
        # Check each page's engagement
        for page in page_trends:
            url = page['url']
            
            # Try exact match first
            ga4_data = url_map.get(url)
            
            # Try normalized URL matching if exact fails
            if ga4_data is None:
                normalized_url = url.rstrip('/').split('?')[0]
                for ga4_url, ga4_row in url_map.items():
                    if ga4_url.rstrip('/').split('?')[0] == normalized_url:
                        ga4_data = ga4_row
                        break
            
            if ga4_data is None:
                continue
            
            # Check for engagement issues
            bounce_rate = ga4_data.get('bounce_rate', 0)
            avg_session = ga4_data.get('avg_session_duration', 0)
            sessions = ga4_data.get('sessions', 0)
            
            # Only flag if we have meaningful session data
            if sessions < 10:
                continue
            
            # Flag low engagement
            if bounce_rate > 0.80 or avg_session < 30:
                engagement_flags[url] = 'low_engagement'
            elif bounce_rate > 0.70 or avg_session < 45:
                engagement_flags[url] = 'moderate_engagement_concern'
        
        logger.info(f"Flagged {len(engagement_flags)} pages with engagement issues")
        
    except Exception as e:
        logger.error(f"Error analyzing engagement: {e}")
    
    return engagement_flags


def _compute_priority_scores(
    page_trends: List[Dict[str, Any]],
    ctr_anomalies: Dict[str, Dict[str, Any]],
    engagement_flags: Dict[str, str],
    gsc_page_summary: pd.DataFrame
) -> List[Dict[str, Any]]:
    """
    Combine all signals into priority-scored page list.
    """
    prioritized = []
    
    # Create lookup for page summary data
    summary_map = {}
    for _, row in gsc_page_summary.iterrows():
        summary_map[row['url']] = row
    
    for page in page_trends:
        url = page['url']
        
        # Skip pages without sufficient data
        if not page.get('has_sufficient_data', False):
            continue
        
        # Get CTR anomaly data
        ctr_data = ctr_anomalies.get(url, {})
        
        # Get engagement flag
        engagement_flag = engagement_flags.get(url)
        
        # Get summary stats
        summary = summary_map.get(url, {})
        
        # Calculate recoverability factor
        recoverability = _calculate_recoverability(page, ctr_data, summary)
        
        # Calculate priority score
        monthly_clicks = page.get('current_monthly_clicks', 0)
        decay_rate = abs(page.get('trend_slope', 0))
        
        priority_score = (monthly_clicks * decay_rate * recoverability)
        
        # Boost score for CTR anomalies (easy wins)
        if ctr_data.get('ctr_anomaly'):
            priority_score *= 1.5
        
        # Determine recommended action
        recommended_action = _determine_action(page, ctr_data, engagement_flag)
        
        # Compile page result
        page_result = {
            'url': url,
            'bucket': page.get('bucket', 'unknown'),
            'current_monthly_clicks': round(monthly_clicks, 1),
            'trend_slope': page.get('trend_slope', 0),
            'projected_page1_loss_date': page.get('projected_page1_loss_date'),
            'priority_score': round(priority_score, 1),
            'recommended_action': recommended_action,
            'recoverability_factor': round(recoverability, 2)
        }
        
        # Add CTR anomaly data if present
        if ctr_data:
            page_result.update({
                'ctr_anomaly': ctr_data.get('ctr_anomaly', False),
                'ctr_expected': ctr_data.get('ctr_expected'),
                'ctr_actual': ctr_data.get('ctr_actual'),
            })
        
        # Add engagement flag if present
        if engagement_flag:
            page_result['engagement_flag'] = engagement_flag
        
        # Add position data if available
        if page.get('avg_position'):
            page_result['avg_position'] = page['avg_position']
        
        prioritized.append(page_result)
    
    # Sort by priority score descending
    prioritized.sort(key=lambda x: x['priority_score'], reverse=True)
    
    return prioritized


def _calculate_recoverability(
    page: Dict[str, Any],
    ctr_data: Dict[str, Any],
    summary: Dict[str, Any]
) -> float:
    """
    Calculate how recoverable a page is based on various factors.
    Returns a multiplier (0.1 to 2.0).
    """
    recoverability = 1.0
    
    # Position-based recoverability (easier to recover from #8 than #25)
    avg_position = page.get('avg_position')
    if avg_position:
        if avg_position <= 10:
            recoverability *= 1.5  # Page 1 - high recoverability
        elif avg_position <= 20:
            recoverability *= 1.2  # Page 2 - moderate
        else:
            recoverability *= 0.7  # Page 3+ - harder
    
    # CTR anomaly = easy fix
    if ctr_data.get('ctr_anomaly'):
        recoverability *= 1.8
    
    # Recent decay is more recoverable than long-term decline
    bucket = page.get('bucket')
    if bucket == 'critical':
        recoverability *= 1.3  # Urgent but recoverable if acted on now
    elif bucket == 'decaying':
        recoverability *= 1.1
    
    # Clamp to reasonable range
    return max(0.1, min(2.0, recoverability))


def _determine_action(
    page: Dict[str, Any],
    ctr_data: Dict[str, Any],
    engagement_flag: Optional[str]
) -> str:
    """
    Determine recommended action based on page signals.
    """
    # CTR anomaly is highest priority and easiest fix
    if ctr_data.get('ctr_anomaly'):
        return 'title_rewrite'
    
    # Engagement issues suggest content mismatch
    if engagement_flag == 'low_engagement':
        return 'content_refresh'
    
    # Bucket-based recommendations
    bucket = page.get('bucket', '')
    
    if bucket == 'critical':
        return 'urgent_investigation'
    elif bucket == 'decaying':
        if page.get('avg_position', 999) > 10:
            return 'content_expansion'
        else:
            return 'internal_links'
    elif bucket == 'stable':
        return 'monitor'
    elif bucket == 'growing':
        return 'double_down'
    
    return 'review'


def _generate_summary(prioritized_pages: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Generate summary statistics across all pages.
    """
    bucket_counts = {
        'growing': 0,
        'stable': 0,
        'decaying': 0,
        'critical': 0
    }
    
    total_recoverable = 0.0
    
    for page in prioritized_pages:
        bucket = page.get('bucket', 'unknown')
        if bucket in bucket_counts:
            bucket_counts[bucket] += 1
        
        # Sum recoverable clicks from decaying/critical pages
        if bucket in ['decaying', 'critical']:
            monthly_clicks = page.get('current_monthly_clicks', 0)
            decay_rate = abs(page.get('trend_slope', 0))
            # Estimate recoverable as portion of current clicks at risk
            total_recoverable += monthly_clicks * min(decay_rate * 30, 0.8)
    
    return {
        'total_pages_analyzed': len(prioritized_pages),
        'growing': bucket_counts['growing'],
        'stable': bucket_counts['stable'],
        'decaying': bucket_counts['decaying'],
        'critical': bucket_counts['critical'],
        'total_recoverable_clicks_monthly': round(total_recoverable, 1)
    }
