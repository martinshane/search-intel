"""
Module 10: Branded vs Non-Branded Health Analysis

Analyzes the split between branded and non-branded search traffic,
assessing dependency risk and growth opportunities.
"""

from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from rapidfuzz import fuzz
from scipy import stats


def analyze_branded_split(
    gsc_query_data: pd.DataFrame,
    brand_terms: List[str],
    health_branded: Optional[Dict[str, Any]] = None,
    health_non_branded: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Analyze branded vs non-branded search traffic health.
    
    Args:
        gsc_query_data: DataFrame with columns [query, date, clicks, impressions, ctr, position]
        brand_terms: List of brand name variations (e.g., ["acme", "acme corp", "acmecorp"])
        health_branded: Optional pre-computed health analysis for branded queries
        health_non_branded: Optional pre-computed health analysis for non-branded queries
    
    Returns:
        {
            "branded_ratio": float,
            "dependency_level": str,
            "branded_trend": dict,
            "non_branded_trend": dict,
            "non_branded_opportunity": dict,
            "classification_summary": dict,
            "branded_queries": list,
            "non_branded_queries": list
        }
    """
    try:
        # Step 1: Classify queries as branded, non-branded, or competitor-branded
        classification_results = _classify_queries(gsc_query_data, brand_terms)
        
        # Step 2: Split data by classification
        branded_data = gsc_query_data[
            gsc_query_data['query'].isin(classification_results['branded'])
        ].copy()
        
        non_branded_data = gsc_query_data[
            gsc_query_data['query'].isin(classification_results['non_branded'])
        ].copy()
        
        # Step 3: Calculate branded ratio and dependency level
        total_clicks = gsc_query_data['clicks'].sum()
        branded_clicks = branded_data['clicks'].sum()
        non_branded_clicks = non_branded_data['clicks'].sum()
        
        branded_ratio = branded_clicks / total_clicks if total_clicks > 0 else 0.0
        dependency_level = _assess_dependency_level(branded_ratio)
        
        # Step 4: Analyze trends separately
        branded_trend = _analyze_segment_trend(branded_data) if not branded_data.empty else None
        non_branded_trend = _analyze_segment_trend(non_branded_data) if not non_branded_data.empty else None
        
        # Override with pre-computed health analyses if provided
        if health_branded:
            branded_trend = health_branded
        if health_non_branded:
            non_branded_trend = health_non_branded
        
        # Step 5: Calculate non-branded opportunity
        non_branded_opportunity = _calculate_non_branded_opportunity(
            non_branded_data,
            non_branded_trend
        )
        
        # Step 6: Prepare top queries for each category
        branded_top_queries = _get_top_queries(branded_data, limit=20)
        non_branded_top_queries = _get_top_queries(non_branded_data, limit=20)
        
        return {
            "branded_ratio": round(branded_ratio, 3),
            "dependency_level": dependency_level,
            "branded_trend": branded_trend,
            "non_branded_trend": non_branded_trend,
            "non_branded_opportunity": non_branded_opportunity,
            "classification_summary": {
                "total_queries": len(gsc_query_data['query'].unique()),
                "branded_queries": len(classification_results['branded']),
                "non_branded_queries": len(classification_results['non_branded']),
                "competitor_branded_queries": len(classification_results['competitor_branded']),
                "branded_clicks": int(branded_clicks),
                "non_branded_clicks": int(non_branded_clicks),
                "branded_impressions": int(branded_data['impressions'].sum()),
                "non_branded_impressions": int(non_branded_data['impressions'].sum())
            },
            "branded_queries": branded_top_queries,
            "non_branded_queries": non_branded_top_queries,
            "strategic_recommendation": _generate_strategic_recommendation(
                branded_ratio,
                branded_trend,
                non_branded_trend,
                non_branded_opportunity
            )
        }
    
    except Exception as e:
        return {
            "error": str(e),
            "branded_ratio": 0.0,
            "dependency_level": "unknown",
            "branded_trend": None,
            "non_branded_trend": None,
            "non_branded_opportunity": None,
            "classification_summary": {},
            "branded_queries": [],
            "non_branded_queries": []
        }


def _classify_queries(
    gsc_query_data: pd.DataFrame,
    brand_terms: List[str]
) -> Dict[str, List[str]]:
    """
    Classify queries into branded, non-branded, or competitor-branded.
    
    Uses fuzzy matching to handle misspellings and variations.
    """
    branded = []
    non_branded = []
    competitor_branded = []
    
    # Common competitor indicators (expand based on niche)
    competitor_indicators = [
        'vs', 'versus', 'alternative', 'competitor', 'compare',
        'instead of', 'better than', 'similar to'
    ]
    
    unique_queries = gsc_query_data['query'].unique()
    
    for query in unique_queries:
        query_lower = query.lower()
        
        # Check for brand match using fuzzy matching
        is_branded = False
        for brand_term in brand_terms:
            brand_lower = brand_term.lower()
            
            # Exact substring match
            if brand_lower in query_lower:
                is_branded = True
                break
            
            # Fuzzy match for misspellings (threshold of 85)
            if fuzz.partial_ratio(brand_lower, query_lower) >= 85:
                is_branded = True
                break
        
        if is_branded:
            # Check if it's a competitor comparison query
            if any(indicator in query_lower for indicator in competitor_indicators):
                competitor_branded.append(query)
            else:
                branded.append(query)
        else:
            non_branded.append(query)
    
    return {
        "branded": branded,
        "non_branded": non_branded,
        "competitor_branded": competitor_branded
    }


def _assess_dependency_level(branded_ratio: float) -> str:
    """
    Assess the site's dependency on branded traffic.
    """
    if branded_ratio > 0.90:
        return "critical"
    elif branded_ratio > 0.70:
        return "high"
    elif branded_ratio > 0.50:
        return "balanced"
    else:
        return "discovery_driven"


def _analyze_segment_trend(segment_data: pd.DataFrame) -> Dict[str, Any]:
    """
    Analyze trend for a specific segment (branded or non-branded).
    
    Simplified version - in production, this would call the full Module 1 analysis.
    """
    if segment_data.empty:
        return {
            "direction": "unknown",
            "slope": 0.0,
            "trend_pct_per_month": 0.0
        }
    
    # Group by date and sum clicks
    daily_data = segment_data.groupby('date').agg({
        'clicks': 'sum',
        'impressions': 'sum'
    }).reset_index()
    
    daily_data = daily_data.sort_values('date')
    
    if len(daily_data) < 30:
        return {
            "direction": "insufficient_data",
            "slope": 0.0,
            "trend_pct_per_month": 0.0
        }
    
    # Simple linear regression on clicks
    daily_data['day_index'] = range(len(daily_data))
    
    if daily_data['clicks'].sum() == 0:
        return {
            "direction": "no_traffic",
            "slope": 0.0,
            "trend_pct_per_month": 0.0
        }
    
    slope, intercept, r_value, p_value, std_err = stats.linregress(
        daily_data['day_index'],
        daily_data['clicks']
    )
    
    # Convert daily slope to monthly percentage change
    avg_clicks = daily_data['clicks'].mean()
    if avg_clicks > 0:
        trend_pct_per_month = (slope * 30 / avg_clicks) * 100
    else:
        trend_pct_per_month = 0.0
    
    # Classify direction
    if trend_pct_per_month > 5:
        direction = "strong_growth"
    elif trend_pct_per_month > 1:
        direction = "growth"
    elif trend_pct_per_month > -1:
        direction = "stable"
    elif trend_pct_per_month > -5:
        direction = "declining"
    else:
        direction = "strong_decline"
    
    return {
        "direction": direction,
        "slope": round(slope, 4),
        "trend_pct_per_month": round(trend_pct_per_month, 2),
        "r_squared": round(r_value ** 2, 3),
        "current_monthly_clicks": int(daily_data['clicks'].tail(30).sum())
    }


def _calculate_non_branded_opportunity(
    non_branded_data: pd.DataFrame,
    non_branded_trend: Optional[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Calculate the opportunity size in non-branded search.
    """
    if non_branded_data.empty:
        return {
            "current_monthly_clicks": 0,
            "potential_monthly_clicks": 0,
            "gap": 0,
            "months_to_meaningful_at_current_rate": None,
            "months_to_meaningful_with_actions": None
        }
    
    # Calculate current monthly clicks
    recent_30d = non_branded_data[
        non_branded_data['date'] >= (non_branded_data['date'].max() - timedelta(days=30))
    ]
    current_monthly_clicks = int(recent_30d['clicks'].sum())
    
    # Calculate potential clicks (top 3 CTR assumption)
    total_impressions = recent_30d['impressions'].sum()
    
    # Estimate potential CTR if ranking in top 3 for all queries
    # Conservative estimate: 15% CTR for position 1-3 average
    target_ctr = 0.15
    potential_monthly_clicks = int(total_impressions * target_ctr)
    
    gap = potential_monthly_clicks - current_monthly_clicks
    
    # Calculate time to meaningful (>20% of total traffic)
    months_to_meaningful_current = None
    months_to_meaningful_accelerated = None
    
    if non_branded_trend and non_branded_trend.get('trend_pct_per_month', 0) > 0:
        current_growth_rate = non_branded_trend['trend_pct_per_month'] / 100
        
        if current_growth_rate > 0 and current_monthly_clicks > 0:
            # Calculate months needed to reach 20% of potential
            target_clicks = potential_monthly_clicks * 0.20
            
            if current_monthly_clicks < target_clicks:
                months_to_meaningful_current = np.log(target_clicks / current_monthly_clicks) / np.log(1 + current_growth_rate)
                months_to_meaningful_current = int(np.ceil(months_to_meaningful_current))
                
                # With actions, assume 3x growth acceleration
                accelerated_growth_rate = current_growth_rate * 3
                months_to_meaningful_accelerated = np.log(target_clicks / current_monthly_clicks) / np.log(1 + accelerated_growth_rate)
                months_to_meaningful_accelerated = int(np.ceil(months_to_meaningful_accelerated))
    
    return {
        "current_monthly_clicks": current_monthly_clicks,
        "potential_monthly_clicks": potential_monthly_clicks,
        "gap": gap,
        "current_ctr": round(recent_30d['clicks'].sum() / recent_30d['impressions'].sum(), 4) if recent_30d['impressions'].sum() > 0 else 0,
        "target_ctr": target_ctr,
        "months_to_meaningful_at_current_rate": months_to_meaningful_current,
        "months_to_meaningful_with_actions": months_to_meaningful_accelerated
    }


def _get_top_queries(segment_data: pd.DataFrame, limit: int = 20) -> List[Dict[str, Any]]:
    """
    Get top queries by clicks for a segment.
    """
    if segment_data.empty:
        return []
    
    query_summary = segment_data.groupby('query').agg({
        'clicks': 'sum',
        'impressions': 'sum',
        'position': 'mean'
    }).reset_index()
    
    query_summary['ctr'] = query_summary['clicks'] / query_summary['impressions']
    query_summary = query_summary.sort_values('clicks', ascending=False).head(limit)
    
    return [
        {
            "query": row['query'],
            "clicks": int(row['clicks']),
            "impressions": int(row['impressions']),
            "ctr": round(row['ctr'], 4),
            "avg_position": round(row['position'], 1)
        }
        for _, row in query_summary.iterrows()
    ]


def _generate_strategic_recommendation(
    branded_ratio: float,
    branded_trend: Optional[Dict[str, Any]],
    non_branded_trend: Optional[Dict[str, Any]],
    non_branded_opportunity: Optional[Dict[str, Any]]
) -> str:
    """
    Generate strategic recommendation based on branded/non-branded analysis.
    """
    recommendations = []
    
    # Dependency assessment
    if branded_ratio > 0.90:
        recommendations.append(
            "CRITICAL: Your site is heavily dependent on branded search (>90% of traffic). "
            "This creates significant business risk. Prioritize non-branded content development immediately."
        )
    elif branded_ratio > 0.70:
        recommendations.append(
            "Your site has high branded dependency (>70% of traffic). "
            "Focus on diversifying traffic sources through non-branded SEO initiatives."
        )
    elif branded_ratio < 0.50:
        recommendations.append(
            "Healthy SEO: Your site is discovery-driven with <50% branded traffic. "
            "Continue investing in non-branded content to sustain growth."
        )
    
    # Trend-based recommendations
    if branded_trend and non_branded_trend:
        branded_direction = branded_trend.get('direction', 'unknown')
        non_branded_direction = non_branded_trend.get('direction', 'unknown')
        
        if non_branded_direction in ['strong_growth', 'growth']:
            recommendations.append(
                f"Non-branded traffic is growing at {non_branded_trend.get('trend_pct_per_month', 0)}%/month. "
                "Double down on what's working - audit top-performing non-branded content and replicate the strategy."
            )
        elif non_branded_direction in ['declining', 'strong_decline']:
            recommendations.append(
                f"Non-branded traffic is declining at {abs(non_branded_trend.get('trend_pct_per_month', 0))}%/month. "
                "This requires immediate attention - review algorithm updates and competitive movements."
            )
        
        if branded_direction in ['declining', 'strong_decline']:
            recommendations.append(
                "Branded traffic is declining - this could indicate brand health issues beyond SEO. "
                "Review brand awareness campaigns and customer satisfaction."
            )
    
    # Opportunity sizing
    if non_branded_opportunity and non_branded_opportunity.get('gap', 0) > 0:
        gap = non_branded_opportunity['gap']
        current = non_branded_opportunity['current_monthly_clicks']
        
        if gap > current * 2:
            recommendations.append(
                f"Large opportunity: You're capturing only {current:,} of {non_branded_opportunity['potential_monthly_clicks']:,} "
                f"potential monthly clicks from non-branded search ({gap:,} click gap). "
                "Focus on improving positions for high-impression queries."
            )
    
    if not recommendations:
        return "Your branded/non-branded split is balanced. Continue current SEO strategy."
    
    return " ".join(recommendations)
