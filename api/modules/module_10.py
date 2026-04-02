"""
Module 10: Revenue Impact Analysis

Calculates organic traffic value using industry CPC benchmarks, estimates conversions
based on GA4 conversion data, projects revenue impact of ranking improvements,
identifies high-value pages by traffic × conversion rate.

Integrates with:
- Module 1 (GA4 data)
- GSC query analytics data
- Industry CPC benchmarks
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


# Industry average CPC benchmarks by search intent category (USD)
CPC_BENCHMARKS = {
    "transactional": 3.50,
    "commercial": 2.80,
    "informational": 0.75,
    "navigational": 1.20,
    "local": 2.40,
    "brand": 0.45
}

# Conservative conversion rate benchmarks by industry
CONVERSION_RATE_BENCHMARKS = {
    "ecommerce": 0.02,
    "saas": 0.03,
    "lead_gen": 0.05,
    "content": 0.01,
    "local": 0.04,
    "default": 0.025
}


def classify_query_intent(query: str, position: float, page_url: str) -> str:
    """
    Classify search query intent based on patterns.
    
    Args:
        query: Search query text
        position: Average ranking position
        page_url: Landing page URL
        
    Returns:
        Intent category: transactional, commercial, informational, navigational, local, brand
    """
    query_lower = query.lower()
    url_lower = page_url.lower()
    
    # Brand/navigational signals
    if position <= 3 and any(term in query_lower for term in ['login', 'sign in', 'account', 'portal']):
        return "navigational"
    
    # Transactional intent
    transactional_terms = [
        'buy', 'purchase', 'order', 'shop', 'price', 'cost', 'discount',
        'deal', 'coupon', 'cheap', 'affordable', 'online', 'store'
    ]
    if any(term in query_lower for term in transactional_terms):
        return "transactional"
    
    if '/product' in url_lower or '/shop' in url_lower or '/buy' in url_lower:
        return "transactional"
    
    # Commercial investigation
    commercial_terms = [
        'best', 'top', 'vs', 'versus', 'review', 'comparison', 'compare',
        'alternative', 'recommend', 'which', 'should i'
    ]
    if any(term in query_lower for term in commercial_terms):
        return "commercial"
    
    # Local intent
    local_terms = ['near me', 'nearby', 'local', 'in [city]', 'directions', 'hours', 'open']
    if any(term in query_lower for term in local_terms):
        return "local"
    
    # Informational intent
    informational_terms = [
        'how to', 'what is', 'why', 'when', 'where', 'guide', 'tutorial',
        'learn', 'meaning', 'definition', 'tips', 'ideas', 'examples'
    ]
    if any(term in query_lower for term in informational_terms):
        return "informational"
    
    if '/blog' in url_lower or '/article' in url_lower or '/guide' in url_lower:
        return "informational"
    
    # Default to informational if position > 10 (typically informational SERPs)
    if position > 10:
        return "informational"
    
    return "commercial"


def estimate_query_cpc(query: str, intent: str, impressions: int) -> float:
    """
    Estimate CPC for a query based on intent and search volume.
    
    High-volume queries typically have higher competition and CPC.
    """
    base_cpc = CPC_BENCHMARKS[intent]
    
    # Volume multiplier (higher volume = higher competition)
    if impressions > 10000:
        volume_multiplier = 1.5
    elif impressions > 5000:
        volume_multiplier = 1.3
    elif impressions > 1000:
        volume_multiplier = 1.1
    else:
        volume_multiplier = 1.0
    
    # Query length multiplier (longer tail = lower CPC)
    word_count = len(query.split())
    if word_count >= 5:
        length_multiplier = 0.7
    elif word_count >= 3:
        length_multiplier = 0.85
    else:
        length_multiplier = 1.0
    
    return base_cpc * volume_multiplier * length_multiplier


def calculate_traffic_value(
    gsc_query_data: pd.DataFrame,
    time_period_days: int = 30
) -> Dict[str, Any]:
    """
    Calculate total organic traffic value using CPC benchmarks.
    
    Args:
        gsc_query_data: DataFrame with columns: query, clicks, impressions, position, page
        time_period_days: Number of days in analysis period
        
    Returns:
        Dictionary with traffic value metrics
    """
    if gsc_query_data.empty:
        return {
            "total_traffic_value": 0,
            "traffic_value_monthly": 0,
            "avg_click_value": 0,
            "queries_analyzed": 0
        }
    
    # Classify intent and estimate CPC for each query
    gsc_query_data['intent'] = gsc_query_data.apply(
        lambda row: classify_query_intent(row['query'], row['position'], row.get('page', '')),
        axis=1
    )
    
    gsc_query_data['estimated_cpc'] = gsc_query_data.apply(
        lambda row: estimate_query_cpc(row['query'], row['intent'], row['impressions']),
        axis=1
    )
    
    # Calculate traffic value
    gsc_query_data['traffic_value'] = gsc_query_data['clicks'] * gsc_query_data['estimated_cpc']
    
    total_value = gsc_query_data['traffic_value'].sum()
    monthly_value = total_value * (30 / time_period_days)
    total_clicks = gsc_query_data['clicks'].sum()
    avg_click_value = total_value / total_clicks if total_clicks > 0 else 0
    
    # Value by intent
    value_by_intent = gsc_query_data.groupby('intent').agg({
        'traffic_value': 'sum',
        'clicks': 'sum',
        'query': 'count'
    }).to_dict('index')
    
    # Top value queries
    top_queries = gsc_query_data.nlargest(20, 'traffic_value')[
        ['query', 'clicks', 'position', 'intent', 'estimated_cpc', 'traffic_value']
    ].to_dict('records')
    
    return {
        "total_traffic_value": round(total_value, 2),
        "traffic_value_monthly": round(monthly_value, 2),
        "avg_click_value": round(avg_click_value, 2),
        "queries_analyzed": len(gsc_query_data),
        "value_by_intent": {
            intent: {
                "total_value": round(data['traffic_value'], 2),
                "clicks": int(data['clicks']),
                "query_count": int(data['query']),
                "avg_click_value": round(data['traffic_value'] / data['clicks'], 2) if data['clicks'] > 0 else 0
            }
            for intent, data in value_by_intent.items()
        },
        "top_value_queries": [
            {
                "query": q['query'],
                "clicks": int(q['clicks']),
                "position": round(q['position'], 1),
                "intent": q['intent'],
                "estimated_cpc": round(q['estimated_cpc'], 2),
                "traffic_value": round(q['traffic_value'], 2)
            }
            for q in top_queries
        ]
    }


def estimate_conversions(
    page_data: pd.DataFrame,
    ga4_conversion_data: Optional[Dict[str, Any]] = None,
    industry: str = "default"
) -> Dict[str, Any]:
    """
    Estimate conversions and revenue based on GA4 data or industry benchmarks.
    
    Args:
        page_data: DataFrame with columns: page, clicks, sessions, bounce_rate, avg_session_duration
        ga4_conversion_data: GA4 conversion metrics if available
        industry: Industry type for benchmark conversion rates
        
    Returns:
        Dictionary with conversion estimates
    """
    if page_data.empty:
        return {
            "estimated_conversions": 0,
            "conversion_rate": 0,
            "pages_analyzed": 0
        }
    
    # Use GA4 conversion data if available, otherwise use benchmarks
    if ga4_conversion_data and 'conversion_rate' in ga4_conversion_data:
        base_conversion_rate = ga4_conversion_data['conversion_rate']
        actual_conversions = ga4_conversion_data.get('total_conversions', 0)
    else:
        base_conversion_rate = CONVERSION_RATE_BENCHMARKS.get(industry, CONVERSION_RATE_BENCHMARKS['default'])
        actual_conversions = None
    
    # Adjust conversion rate per page based on engagement signals
    def calculate_page_conversion_rate(row):
        cr = base_conversion_rate
        
        # Bounce rate adjustment
        bounce = row.get('bounce_rate', 0.5)
        if bounce < 0.3:
            cr *= 1.3  # Low bounce = higher quality traffic
        elif bounce > 0.7:
            cr *= 0.6  # High bounce = lower quality traffic
        
        # Session duration adjustment
        duration = row.get('avg_session_duration', 60)
        if duration > 180:  # > 3 minutes
            cr *= 1.2
        elif duration < 30:  # < 30 seconds
            cr *= 0.5
        
        return cr
    
    page_data['estimated_conversion_rate'] = page_data.apply(calculate_page_conversion_rate, axis=1)
    page_data['estimated_conversions'] = page_data['clicks'] * page_data['estimated_conversion_rate']
    
    total_conversions = page_data['estimated_conversions'].sum()
    total_clicks = page_data['clicks'].sum()
    overall_conversion_rate = total_conversions / total_clicks if total_clicks > 0 else 0
    
    # High-value pages (traffic × conversion rate)
    page_data['page_value_score'] = page_data['clicks'] * page_data['estimated_conversion_rate']
    high_value_pages = page_data.nlargest(20, 'page_value_score')[
        ['page', 'clicks', 'estimated_conversion_rate', 'estimated_conversions', 'page_value_score']
    ].to_dict('records')
    
    return {
        "estimated_conversions": round(total_conversions, 1),
        "estimated_conversion_rate": round(overall_conversion_rate, 4),
        "actual_conversions": actual_conversions,
        "pages_analyzed": len(page_data),
        "high_value_pages": [
            {
                "page": p['page'],
                "clicks": int(p['clicks']),
                "estimated_conversion_rate": round(p['estimated_conversion_rate'], 4),
                "estimated_conversions": round(p['estimated_conversions'], 1),
                "value_score": round(p['page_value_score'], 2)
            }
            for p in high_value_pages
        ]
    }


def project_revenue_impact(
    gsc_query_data: pd.DataFrame,
    position_improvements: List[Dict[str, Any]],
    avg_order_value: Optional[float] = None,
    conversion_rate: float = 0.025
) -> Dict[str, Any]:
    """
    Project revenue impact of ranking improvements.
    
    Args:
        gsc_query_data: DataFrame with query performance data
        position_improvements: List of potential position improvements from other modules
        avg_order_value: Average order value (if applicable)
        conversion_rate: Site conversion rate
        
    Returns:
        Dictionary with revenue projections
    """
    if gsc_query_data.empty or not position_improvements:
        return {
            "total_revenue_opportunity": 0,
            "opportunities_analyzed": 0
        }
    
    # CTR by position (industry averages)
    position_ctr = {
        1: 0.316, 2: 0.158, 3: 0.107, 4: 0.074, 5: 0.057,
        6: 0.046, 7: 0.039, 8: 0.033, 9: 0.029, 10: 0.025,
        11: 0.020, 12: 0.017, 13: 0.015, 14: 0.013, 15: 0.012
    }
    
    def get_ctr_for_position(pos: float) -> float:
        if pos < 1:
            return position_ctr[1]
        pos_int = min(int(round(pos)), 15)
        return position_ctr.get(pos_int, 0.01)
    
    opportunities = []
    
    for improvement in position_improvements:
        query = improvement.get('query', '')
        current_position = improvement.get('current_position', 0)
        target_position = improvement.get('target_position', 0)
        
        # Find query in data
        query_data = gsc_query_data[gsc_query_data['query'] == query]
        if query_data.empty:
            continue
        
        row = query_data.iloc[0]
        impressions = row['impressions']
        current_clicks = row['clicks']
        
        # Calculate projected clicks at new position
        current_ctr = get_ctr_for_position(current_position)
        target_ctr = get_ctr_for_position(target_position)
        
        projected_clicks = impressions * target_ctr
        click_gain = projected_clicks - current_clicks
        
        if click_gain <= 0:
            continue
        
        # Calculate traffic value
        intent = classify_query_intent(query, target_position, row.get('page', ''))
        estimated_cpc = estimate_query_cpc(query, intent, impressions)
        traffic_value_gain = click_gain * estimated_cpc
        
        # Calculate revenue impact
        projected_conversions = click_gain * conversion_rate
        revenue_impact = 0
        if avg_order_value:
            revenue_impact = projected_conversions * avg_order_value
        
        opportunities.append({
            "query": query,
            "current_position": round(current_position, 1),
            "target_position": target_position,
            "current_clicks": int(current_clicks),
            "projected_clicks": round(projected_clicks, 1),
            "click_gain": round(click_gain, 1),
            "traffic_value_gain_monthly": round(traffic_value_gain, 2),
            "projected_conversions_monthly": round(projected_conversions, 2),
            "revenue_impact_monthly": round(revenue_impact, 2) if revenue_impact else None,
            "effort_estimate": improvement.get('effort', 'medium'),
            "roi_score": round(traffic_value_gain / max(improvement.get('effort_score', 5), 1), 2)
        })
    
    # Sort by ROI score
    opportunities.sort(key=lambda x: x['roi_score'], reverse=True)
    
    total_click_gain = sum(o['click_gain'] for o in opportunities)
    total_traffic_value = sum(o['traffic_value_gain_monthly'] for o in opportunities)
    total_conversions = sum(o['projected_conversions_monthly'] for o in opportunities)
    total_revenue = sum(o['revenue_impact_monthly'] for o in opportunities if o['revenue_impact_monthly'])
    
    return {
        "total_click_gain_monthly": round(total_click_gain, 1),
        "total_traffic_value_gain_monthly": round(total_traffic_value, 2),
        "total_projected_conversions_monthly": round(total_conversions, 2),
        "total_revenue_opportunity_monthly": round(total_revenue, 2) if total_revenue else None,
        "opportunities_analyzed": len(opportunities),
        "top_opportunities": opportunities[:20]
    }


def analyze_revenue_impact(
    gsc_query_data: pd.DataFrame,
    page_performance_data: pd.DataFrame,
    ga4_data: Optional[Dict[str, Any]] = None,
    position_improvements: Optional[List[Dict[str, Any]]] = None,
    config: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Main analysis function for Module 10: Revenue Impact.
    
    Args:
        gsc_query_data: GSC query-level performance data
        page_performance_data: Page-level performance with engagement metrics
        ga4_data: GA4 conversion and revenue data
        position_improvements: Potential ranking improvements from other modules
        config: Configuration with industry type, AOV, etc.
        
    Returns:
        Complete revenue impact analysis results
    """
    try:
        logger.info("Starting revenue impact analysis")
        
        config = config or {}
        industry = config.get('industry', 'default')
        avg_order_value = config.get('avg_order_value')
        time_period_days = config.get('time_period_days', 30)
        
        # 1. Calculate traffic value
        logger.info("Calculating traffic value")
        traffic_value = calculate_traffic_value(gsc_query_data, time_period_days)
        
        # 2. Extract conversion data from GA4
        ga4_conversion_data = None
        if ga4_data:
            ga4_conversion_data = {
                'conversion_rate': ga4_data.get('conversion_rate', 0),
                'total_conversions': ga4_data.get('total_conversions', 0),
                'total_revenue': ga4_data.get('total_revenue', 0)
            }
        
        # 3. Estimate conversions
        logger.info("Estimating conversions")
        conversion_estimates = estimate_conversions(
            page_performance_data,
            ga4_conversion_data,
            industry
        )
        
        # 4. Project revenue impact of improvements
        revenue_projections = {}
        if position_improvements:
            logger.info("Projecting revenue impact of ranking improvements")
            conversion_rate = conversion_estimates.get('estimated_conversion_rate', 0.025)
            revenue_projections = project_revenue_impact(
                gsc_query_data,
                position_improvements,
                avg_order_value,
                conversion_rate
            )
        
        # 5. Calculate current revenue attribution (if AOV available)
        current_revenue_attribution = None
        if avg_order_value and conversion_estimates['estimated_conversions'] > 0:
            current_revenue_attribution = {
                "estimated_monthly_conversions": conversion_estimates['estimated_conversions'],
                "estimated_monthly_revenue": round(
                    conversion_estimates['estimated_conversions'] * avg_order_value, 2
                ),
                "avg_order_value": avg_order_value
            }
        
        # 6. Calculate ROI metrics
        total_opportunity = revenue_projections.get('total_revenue_opportunity_monthly', 0)
        current_value = traffic_value['traffic_value_monthly']
        
        roi_summary = {
            "current_organic_traffic_value_monthly": current_value,
            "potential_traffic_value_gain_monthly": revenue_projections.get(
                'total_traffic_value_gain_monthly', 0
            ),
            "potential_revenue_gain_monthly": total_opportunity,
            "roi_multiplier": round(
                (current_value + revenue_projections.get('total_traffic_value_gain_monthly', 0)) / current_value, 2
            ) if current_value > 0 else 0
        }
        
        result = {
            "traffic_value": traffic_value,
            "conversion_estimates": conversion_estimates,
            "revenue_projections": revenue_projections,
            "current_revenue_attribution": current_revenue_attribution,
            "roi_summary": roi_summary,
            "metadata": {
                "analysis_date": datetime.utcnow().isoformat(),
                "time_period_days": time_period_days,
                "industry": industry,
                "avg_order_value": avg_order_value,
                "queries_analyzed": len(gsc_query_data),
                "pages_analyzed": len(page_performance_data)
            }
        }
        
        logger.info("Revenue impact analysis complete")
        return result
        
    except Exception as e:
        logger.error(f"Error in revenue impact analysis: {str(e)}", exc_info=True)
        raise


def format_revenue_report(analysis_results: Dict[str, Any]) -> Dict[str, Any]:
    """
    Format revenue analysis results for report generation.
    
    Args:
        analysis_results: Raw analysis results from analyze_revenue_impact
        
    Returns:
        Formatted report data ready for JSON serialization
    """
    traffic_value = analysis_results['traffic_value']
    conversions = analysis_results['conversion_estimates']
    projections = analysis_results['revenue_projections']
    roi = analysis_results['roi_summary']
    
    # Executive summary
    summary = {
        "current_monthly_traffic_value": traffic_value['traffic_value_monthly'],
        "estimated_monthly_conversions": conversions['estimated_conversions'],
        "potential_monthly_revenue_gain": projections.get('total_revenue_opportunity_monthly', 0),
        "top_opportunity_count": len(projections.get('top_opportunities', [])),
        "roi_multiplier": roi['roi_multiplier']
    }
    
    # Key insights
    insights = []
    
    # Traffic value insights
    if traffic_value['traffic_value_monthly'] > 10000:
        insights.append({
            "type": "high_value",
            "message": f"Your organic traffic generates ${traffic_value['traffic_value_monthly']:,.0f}/month in equivalent ad value",
            "severity": "positive"
        })
    
    # Conversion insights
    if conversions['estimated_conversion_rate'] < 0.02:
        insights.append({
            "type": "low_conversion",
            "message": "Conversion rate is below industry average - consider CRO initiatives",
            "severity": "warning"
        })
    
    # Revenue opportunity insights
    if projections.get('total_revenue_opportunity_monthly', 0) > 5000:
        insights.append({
            "type": "high_opportunity",
            "message": f"${projections['total_revenue_opportunity_monthly']:,.0f}/month revenue opportunity identified",
            "severity": "positive"
        })
    
    # High-value page insights
    high_value_pages = conversions.get('high_value_pages', [])
    if high_value_pages:
        top_page = high_value_pages[0]
        insights.append({
            "type": "top_page",
            "message": f"Top revenue page: {top_page['page']} ({top_page['estimated_conversions']:.1f} conversions/month)",
            "severity": "info"
        })
    
    return {
        "summary": summary,
        "insights": insights,
        "traffic_value_breakdown": traffic_value['value_by_intent'],
        "top_value_queries": traffic_value['top_value_queries'][:10],
        "high_value_pages": high_value_pages[:10],
        "top_revenue_opportunities": projections.get('top_opportunities', [])[:10],
        "roi_summary": roi,
        "metadata": analysis_results['metadata']
    }


# Convenience wrapper for module execution
def run_module(
    gsc_data: Dict[str, Any],
    ga4_data: Optional[Dict[str, Any]] = None,
    position_improvements: Optional[List[Dict[str, Any]]] = None,
    config: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Execute Module 10 analysis with provided data.
    
    Args:
        gsc_data: GSC data dictionary with 'queries' and 'pages' DataFrames
        ga4_data: GA4 data dictionary with conversion metrics
        position_improvements: List of ranking improvement opportunities
        config: Analysis configuration
        
    Returns:
        Formatted analysis results
    """
    gsc_query_data = gsc_data.get('queries', pd.DataFrame())
    page_performance_data = gsc_data.get('pages', pd.DataFrame())
    
    # Ensure required columns exist
    if not gsc_query_data.empty:
        required_cols = ['query', 'clicks', 'impressions', 'position']
        if not all(col in gsc_query_data.columns for col in required_cols):
            raise ValueError(f"GSC query data missing required columns: {required_cols}")
    
    # Run analysis
    results = analyze_revenue_impact(
        gsc_query_data,
        page_performance_data,
        ga4_data,
        position_improvements,
        config
    )
    
    # Format for report
    return format_revenue_report(results)
