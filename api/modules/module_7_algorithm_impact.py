import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import pandas as pd
import numpy as np
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class AlgorithmUpdate:
    """Represents a known Google algorithm update"""
    name: str
    date: datetime
    type: str  # 'core', 'helpful_content', 'spam', 'product_reviews', etc.


# Curated list of major Google algorithm updates (2023-2024)
ALGORITHM_UPDATES = [
    # 2023 Updates
    AlgorithmUpdate("February 2023 Product Reviews Update", datetime(2023, 2, 21), "product_reviews"),
    AlgorithmUpdate("March 2023 Core Update", datetime(2023, 3, 15), "core"),
    AlgorithmUpdate("April 2023 Reviews Update", datetime(2023, 4, 12), "product_reviews"),
    AlgorithmUpdate("August 2023 Core Update", datetime(2023, 8, 22), "core"),
    AlgorithmUpdate("September 2023 Helpful Content Update", datetime(2023, 9, 14), "helpful_content"),
    AlgorithmUpdate("October 2023 Core Update", datetime(2023, 10, 5), "core"),
    AlgorithmUpdate("November 2023 Core Update", datetime(2023, 11, 2), "core"),
    
    # 2024 Updates
    AlgorithmUpdate("March 2024 Core Update", datetime(2024, 3, 5), "core"),
    AlgorithmUpdate("April 2024 Reviews Update", datetime(2024, 4, 9), "product_reviews"),
    AlgorithmUpdate("June 2024 Spam Update", datetime(2024, 6, 20), "spam"),
    AlgorithmUpdate("August 2024 Core Update", datetime(2024, 8, 15), "core"),
    AlgorithmUpdate("November 2024 Core Update", datetime(2024, 11, 11), "core"),
    
    # 2025 Updates (placeholder for recent/upcoming)
    AlgorithmUpdate("March 2025 Core Update", datetime(2025, 3, 6), "core"),
]


def analyze_algorithm_impacts(daily_data: pd.DataFrame, lookback_months: int = 18) -> Dict:
    """
    Analyze the impact of Google algorithm updates on site performance.
    
    Args:
        daily_data: DataFrame with columns ['date', 'clicks', 'impressions', 'ctr', 'position']
        lookback_months: Number of months to analyze (default 18)
    
    Returns:
        Dictionary containing affected updates, metrics changes, and severity scores
    """
    try:
        # Validate input data
        if daily_data is None or daily_data.empty:
            logger.error("No daily data provided for algorithm impact analysis")
            return {
                "error": "No data available",
                "affected_updates": [],
                "summary": {
                    "total_updates_analyzed": 0,
                    "updates_with_negative_impact": 0,
                    "updates_with_positive_impact": 0,
                    "updates_with_no_impact": 0
                }
            }
        
        # Ensure date column is datetime
        if 'date' not in daily_data.columns:
            logger.error("Required 'date' column not found in daily_data")
            return {"error": "Missing required 'date' column", "affected_updates": []}
        
        daily_data = daily_data.copy()
        daily_data['date'] = pd.to_datetime(daily_data['date'])
        daily_data = daily_data.sort_values('date')
        
        # Validate required columns
        required_cols = ['clicks', 'impressions', 'ctr', 'position']
        missing_cols = [col for col in required_cols if col not in daily_data.columns]
        if missing_cols:
            logger.error(f"Missing required columns: {missing_cols}")
            return {
                "error": f"Missing required columns: {', '.join(missing_cols)}",
                "affected_updates": []
            }
        
        # Filter data to lookback period
        end_date = daily_data['date'].max()
        start_date = end_date - timedelta(days=lookback_months * 30)
        
        if start_date > daily_data['date'].max():
            logger.warning("Start date is after the latest data point")
            return {
                "error": "Insufficient data for requested lookback period",
                "affected_updates": [],
                "requested_lookback_months": lookback_months,
                "available_data_days": len(daily_data)
            }
        
        filtered_data = daily_data[daily_data['date'] >= start_date].copy()
        
        if len(filtered_data) < 30:
            logger.warning(f"Insufficient data points: {len(filtered_data)}")
            return {
                "error": "Insufficient data points for analysis",
                "affected_updates": [],
                "data_points_available": len(filtered_data),
                "minimum_required": 30
            }
        
        # Get relevant algorithm updates within the date range
        relevant_updates = [
            update for update in ALGORITHM_UPDATES
            if start_date <= update.date <= end_date
        ]
        
        if not relevant_updates:
            logger.info(f"No algorithm updates found in the date range {start_date.date()} to {end_date.date()}")
            return {
                "affected_updates": [],
                "summary": {
                    "total_updates_analyzed": 0,
                    "updates_with_negative_impact": 0,
                    "updates_with_positive_impact": 0,
                    "updates_with_no_impact": 0,
                    "date_range_analyzed": {
                        "start": start_date.strftime("%Y-%m-%d"),
                        "end": end_date.strftime("%Y-%m-%d")
                    }
                }
            }
        
        logger.info(f"Analyzing {len(relevant_updates)} algorithm updates")
        
        # Analyze each update
        affected_updates = []
        
        for update in relevant_updates:
            try:
                impact_analysis = _analyze_single_update(filtered_data, update)
                if impact_analysis:
                    affected_updates.append(impact_analysis)
            except Exception as e:
                logger.error(f"Error analyzing update {update.name}: {str(e)}")
                continue
        
        # Calculate summary statistics
        summary = _calculate_summary(affected_updates, relevant_updates, start_date, end_date)
        
        return {
            "affected_updates": affected_updates,
            "summary": summary,
            "date_range_analyzed": {
                "start": start_date.strftime("%Y-%m-%d"),
                "end": end_date.strftime("%Y-%m-%d")
            }
        }
    
    except Exception as e:
        logger.error(f"Error in algorithm impact analysis: {str(e)}")
        return {
            "error": f"Analysis failed: {str(e)}",
            "affected_updates": []
        }


def _analyze_single_update(data: pd.DataFrame, update: AlgorithmUpdate) -> Optional[Dict]:
    """
    Analyze the impact of a single algorithm update.
    
    Args:
        data: Daily metrics DataFrame
        update: AlgorithmUpdate object
    
    Returns:
        Dictionary with impact analysis or None if insufficient data
    """
    try:
        # Define time windows
        pre_window_days = 30  # 30 days before update
        post_window_days = 30  # 30 days after update
        short_pre_window_days = 7  # 7 days before update
        short_post_window_days = 7  # 7 days after update
        
        update_date = update.date
        
        # Calculate window dates
        pre_30_start = update_date - timedelta(days=pre_window_days)
        pre_30_end = update_date - timedelta(days=1)
        
        post_30_start = update_date
        post_30_end = update_date + timedelta(days=post_window_days - 1)
        
        pre_7_start = update_date - timedelta(days=short_pre_window_days)
        pre_7_end = update_date - timedelta(days=1)
        
        post_7_start = update_date
        post_7_end = update_date + timedelta(days=short_post_window_days - 1)
        
        # Extract data for each window
        pre_30_data = data[(data['date'] >= pre_30_start) & (data['date'] <= pre_30_end)]
        post_30_data = data[(data['date'] >= post_30_start) & (data['date'] <= post_30_end)]
        
        pre_7_data = data[(data['date'] >= pre_7_start) & (data['date'] <= pre_7_end)]
        post_7_data = data[(data['date'] >= post_7_start) & (data['date'] <= post_7_end)]
        
        # Check if we have sufficient data for both windows
        if len(pre_30_data) < 14 or len(post_30_data) < 14:
            logger.debug(f"Insufficient data for update {update.name}: pre={len(pre_30_data)}, post={len(post_30_data)}")
            return None
        
        # Calculate 30-day metrics
        metrics_30d = _calculate_window_metrics(pre_30_data, post_30_data)
        
        # Calculate 7-day metrics if sufficient data
        metrics_7d = None
        if len(pre_7_data) >= 5 and len(post_7_data) >= 5:
            metrics_7d = _calculate_window_metrics(pre_7_data, post_7_data)
        
        # Calculate severity score
        severity_score = _calculate_severity_score(metrics_30d)
        
        # Determine impact classification
        impact_classification = _classify_impact(metrics_30d, severity_score)
        
        # Check for significant changes
        significant_changes = _identify_significant_changes(metrics_30d)
        
        return {
            "update_name": update.name,
            "update_date": update.date.strftime("%Y-%m-%d"),
            "update_type": update.type,
            "impact_classification": impact_classification,
            "severity_score": round(severity_score, 2),
            "metrics_changes_30d": metrics_30d,
            "metrics_changes_7d": metrics_7d,
            "significant_changes": significant_changes,
            "data_quality": {
                "pre_window_days": len(pre_30_data),
                "post_window_days": len(post_30_data),
                "pre_window_complete": len(pre_30_data) >= pre_window_days * 0.8,
                "post_window_complete": len(post_30_data) >= post_window_days * 0.8
            }
        }
    
    except Exception as e:
        logger.error(f"Error analyzing update {update.name}: {str(e)}")
        return None


def _calculate_window_metrics(pre_data: pd.DataFrame, post_data: pd.DataFrame) -> Dict:
    """
    Calculate metrics changes between pre and post windows.
    
    Args:
        pre_data: Pre-update window data
        post_data: Post-update window data
    
    Returns:
        Dictionary with metrics changes
    """
    metrics = {}
    
    # Clicks
    pre_clicks = pre_data['clicks'].sum()
    post_clicks = post_data['clicks'].sum()
    clicks_change_pct = _calculate_percentage_change(pre_clicks, post_clicks)
    
    metrics['clicks'] = {
        'pre': int(pre_clicks),
        'post': int(post_clicks),
        'change_pct': round(clicks_change_pct, 2),
        'change_absolute': int(post_clicks - pre_clicks)
    }
    
    # Impressions
    pre_impressions = pre_data['impressions'].sum()
    post_impressions = post_data['impressions'].sum()
    impressions_change_pct = _calculate_percentage_change(pre_impressions, post_impressions)
    
    metrics['impressions'] = {
        'pre': int(pre_impressions),
        'post': int(post_impressions),
        'change_pct': round(impressions_change_pct, 2),
        'change_absolute': int(post_impressions - pre_impressions)
    }
    
    # CTR (average)
    pre_ctr = pre_data['ctr'].mean()
    post_ctr = post_data['ctr'].mean()
    ctr_change_pct = _calculate_percentage_change(pre_ctr, post_ctr)
    
    metrics['ctr'] = {
        'pre': round(pre_ctr, 4),
        'post': round(post_ctr, 4),
        'change_pct': round(ctr_change_pct, 2),
        'change_absolute': round(post_ctr - pre_ctr, 4)
    }
    
    # Position (average)
    pre_position = pre_data['position'].mean()
    post_position = post_data['position'].mean()
    position_change = post_position - pre_position
    position_change_pct = _calculate_percentage_change(pre_position, post_position)
    
    metrics['position'] = {
        'pre': round(pre_position, 2),
        'post': round(post_position, 2),
        'change': round(position_change, 2),
        'change_pct': round(position_change_pct, 2)
    }
    
    return metrics


def _calculate_percentage_change(pre_value: float, post_value: float) -> float:
    """
    Calculate percentage change between two values.
    
    Args:
        pre_value: Value before change
        post_value: Value after change
    
    Returns:
        Percentage change (can be positive or negative)
    """
    if pre_value == 0:
        if post_value == 0:
            return 0.0
        return 100.0 if post_value > 0 else -100.0
    
    return ((post_value - pre_value) / pre_value) * 100


def _calculate_severity_score(metrics: Dict) -> float:
    """
    Calculate a severity score for the algorithm update impact.
    
    Score is 0-100, where:
    - 0-20: Minimal impact
    - 21-40: Minor impact
    - 41-60: Moderate impact
    - 61-80: Significant impact
    - 81-100: Severe impact
    
    Args:
        metrics: Dictionary with metrics changes
    
    Returns:
        Severity score (0-100)
    """
    # Weight each metric by importance
    weights = {
        'clicks': 0.40,
        'impressions': 0.25,
        'ctr': 0.20,
        'position': 0.15
    }
    
    scores = {}
    
    # Clicks impact (drops are worse than gains)
    clicks_change = metrics['clicks']['change_pct']
    if clicks_change < 0:
        # Negative impact: scale more aggressively
        scores['clicks'] = min(abs(clicks_change) * 1.5, 100)
    else:
        # Positive impact: scale less aggressively
        scores['clicks'] = min(clicks_change * 0.8, 100)
    
    # Impressions impact
    impressions_change = metrics['impressions']['change_pct']
    if impressions_change < 0:
        scores['impressions'] = min(abs(impressions_change) * 1.2, 100)
    else:
        scores['impressions'] = min(impressions_change * 0.6, 100)
    
    # CTR impact
    ctr_change = metrics['ctr']['change_pct']
    if ctr_change < 0:
        scores['ctr'] = min(abs(ctr_change) * 2.0, 100)
    else:
        scores['ctr'] = min(ctr_change * 1.0, 100)
    
    # Position impact (worse position = higher score)
    position_change = metrics['position']['change']
    if position_change > 0:  # Position worsened (higher number)
        scores['position'] = min(position_change * 10, 100)
    else:  # Position improved
        scores['position'] = min(abs(position_change) * 5, 100)
    
    # Calculate weighted score
    severity_score = sum(scores[metric] * weights[metric] for metric in weights.keys())
    
    return min(severity_score, 100)


def _classify_impact(metrics: Dict, severity_score: float) -> str:
    """
    Classify the impact of an algorithm update.
    
    Args:
        metrics: Dictionary with metrics changes
        severity_score: Calculated severity score
    
    Returns:
        Impact classification string
    """
    clicks_change = metrics['clicks']['change_pct']
    
    # Determine primary direction
    if clicks_change < -20:
        direction = "significant_negative"
    elif clicks_change < -5:
        direction = "negative"
    elif clicks_change > 30:
        direction = "significant_positive"
    elif clicks_change > 5:
        direction = "positive"
    else:
        direction = "neutral"
    
    # Combine with severity for classification
    if severity_score > 60:
        if direction in ["significant_negative", "negative"]:
            return "severe_negative_impact"
        elif direction in ["significant_positive", "positive"]:
            return "major_positive_impact"
    elif severity_score > 40:
        if direction in ["significant_negative", "negative"]:
            return "moderate_negative_impact"
        elif direction in ["significant_positive", "positive"]:
            return "moderate_positive_impact"
    elif severity_score > 20:
        if direction == "significant_negative":
            return "minor_negative_impact"
        elif direction == "significant_positive":
            return "minor_positive_impact"
        else:
            return "minimal_impact"
    
    return "no_significant_impact"


def _identify_significant_changes(metrics: Dict) -> List[Dict]:
    """
    Identify metrics with significant changes (drops >20% or gains >30%).
    
    Args:
        metrics: Dictionary with metrics changes
    
    Returns:
        List of significant change dictionaries
    """
    significant_changes = []
    
    # Check clicks
    if metrics['clicks']['change_pct'] < -20:
        significant_changes.append({
            'metric': 'clicks',
            'type': 'significant_drop',
            'change_pct': metrics['clicks']['change_pct'],
            'severity': 'high' if metrics['clicks']['change_pct'] < -40 else 'moderate'
        })
    elif metrics['clicks']['change_pct'] > 30:
        significant_changes.append({
            'metric': 'clicks',
            'type': 'significant_gain',
            'change_pct': metrics['clicks']['change_pct'],
            'severity': 'high' if metrics['clicks']['change_pct'] > 50 else 'moderate'
        })
    
    # Check impressions
    if metrics['impressions']['change_pct'] < -20:
        significant_changes.append({
            'metric': 'impressions',
            'type': 'significant_drop',
            'change_pct': metrics['impressions']['change_pct'],
            'severity': 'high' if metrics['impressions']['change_pct'] < -40 else 'moderate'
        })
    elif metrics['impressions']['change_pct'] > 30:
        significant_changes.append({
            'metric': 'impressions',
            'type': 'significant_gain',
            'change_pct': metrics['impressions']['change_pct'],
            'severity': 'high' if metrics['impressions']['change_pct'] > 50 else 'moderate'
        })
    
    # Check CTR
    if metrics['ctr']['change_pct'] < -20:
        significant_changes.append({
            'metric': 'ctr',
            'type': 'significant_drop',
            'change_pct': metrics['ctr']['change_pct'],
            'severity': 'high' if metrics['ctr']['change_pct'] < -40 else 'moderate'
        })
    elif metrics['ctr']['change_pct'] > 30:
        significant_changes.append({
            'metric': 'ctr',
            'type': 'significant_gain',
            'change_pct': metrics['ctr']['change_pct'],
            'severity': 'high' if metrics['ctr']['change_pct'] > 50 else 'moderate'
        })
    
    # Check position (use absolute change, not percentage)
    position_change = metrics['position']['change']
    if position_change > 5:  # Worsened by more than 5 positions
        significant_changes.append({
            'metric': 'position',
            'type': 'significant_drop',
            'change': position_change,
            'severity': 'high' if position_change > 10 else 'moderate'
        })
    elif position_change < -5:  # Improved by more than 5 positions
        significant_changes.append({
            'metric': 'position',
            'type': 'significant_gain',
            'change': position_change,
            'severity': 'high' if position_change < -10 else 'moderate'
        })
    
    return significant_changes


def _calculate_summary(affected_updates: List[Dict], all_updates: List[AlgorithmUpdate], 
                       start_date: datetime, end_date: datetime) -> Dict:
    """
    Calculate summary statistics for all analyzed updates.
    
    Args:
        affected_updates: List of analyzed update dictionaries
        all_updates: List of all AlgorithmUpdate objects in range
        start_date: Analysis start date
        end_date: Analysis end date
    
    Returns:
        Summary statistics dictionary
    """
    negative_count = sum(1 for u in affected_updates 
                        if 'negative' in u.get('impact_classification', ''))
    positive_count = sum(1 for u in affected_updates 
                        if 'positive' in u.get('impact_classification', ''))
    neutral_count = sum(1 for u in affected_updates 
                       if u.get('impact_classification', '') in ['no_significant_impact', 'minimal_impact'])
    
    # Calculate average severity for negative impacts
    negative_severities = [u['severity_score'] for u in affected_updates 
                          if 'negative' in u.get('impact_classification', '')]
    avg_negative_severity = np.mean(negative_severities) if negative_severities else 0
    
    # Find most impactful update
    most_impactful = None
    if affected_updates:
        most_impactful = max(affected_updates, key=lambda u: u['severity_score'])
    
    # Calculate total clicks impact
    total_clicks_change = sum(u['metrics_changes_30d']['clicks']['change_absolute'] 
                             for u in affected_updates if 'metrics_changes_30d' in u)
    
    summary = {
        'total_updates_analyzed': len(all_updates),
        'updates_with_data': len(affected_updates),
        'updates_with_negative_impact': negative_count,
        'updates_with_positive_impact': positive_count,
        'updates_with_no_impact': neutral_count,
        'average_negative_severity': round(avg_negative_severity, 2),
        'total_clicks_change': int(total_clicks_change),
        'date_range': {
            'start': start_date.strftime("%Y-%m-%d"),
            'end': end_date.strftime("%Y-%m-%d")
        }
    }
    
    if most_impactful:
        summary['most_impactful_update'] = {
            'name': most_impactful['update_name'],
            'date': most_impactful['update_date'],
            'severity_score': most_impactful['severity_score'],
            'impact_classification': most_impactful['impact_classification']
        }
    
    # Breakdown by update type
    type_breakdown = {}
    for update in affected_updates:
        update_type = update.get('update_type', 'unknown')
        if update_type not in type_breakdown:
            type_breakdown[update_type] = {
                'count': 0,
                'negative': 0,
                'positive': 0,
                'neutral': 0
            }
        type_breakdown[update_type]['count'] += 1
        
        classification = update.get('impact_classification', '')
        if 'negative' in classification:
            type_breakdown[update_type]['negative'] += 1
        elif 'positive' in classification:
            type_breakdown[update_type]['positive'] += 1
        else:
            type_breakdown[update_type]['neutral'] += 1
    
    summary['update_type_breakdown'] = type_breakdown
    
    return summary


def get_algorithm_update_dates(start_date: datetime, end_date: datetime) -> List[Dict]:
    """
    Get list of algorithm updates within a date range.
    
    Args:
        start_date: Start of date range
        end_date: End of date range
    
    Returns:
        List of algorithm update dictionaries
    """
    updates_in_range = [
        {
            'name': update.name,
            'date': update.date.strftime("%Y-%m-%d"),
            'type': update.type
        }
        for update in ALGORITHM_UPDATES
        if start_date <= update.date <= end_date
    ]
    
    return sorted(updates_in_range, key=lambda x: x['date'])
