"""
Module 12: Revenue Attribution

Estimates revenue impact of search performance and potential opportunities.
Maps GSC clicks to GA4 conversions and calculates position-to-revenue projections.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


def estimate_revenue_attribution(
    gsc_data: pd.DataFrame,
    ga4_conversions: pd.DataFrame,
    ga4_engagement: pd.DataFrame,
    ga4_ecommerce: Optional[pd.DataFrame] = None,
    contextual_ctr_model: Optional[Any] = None,
    decaying_pages: Optional[List[Dict[str, Any]]] = None,
    gameplan_actions: Optional[List[Dict[str, Any]]] = None
) -> Dict[str, Any]:
    """
    Estimate revenue attribution from organic search and ROI of recommended actions.
    
    Args:
        gsc_data: GSC performance data by page
        ga4_conversions: GA4 conversion data by landing page
        ga4_engagement: GA4 engagement metrics by landing page
        ga4_ecommerce: Optional ecommerce transaction data
        contextual_ctr_model: Trained CTR model from Module 8
        decaying_pages: Pages in decay from Module 2
        gameplan_actions: Recommended actions from Module 5
    
    Returns:
        Dictionary containing revenue attribution analysis and ROI projections
    """
    
    try:
        logger.info("Starting revenue attribution analysis")
        
        # Calculate click-to-conversion mapping
        conversion_mapping = _calculate_conversion_mapping(
            gsc_data, ga4_conversions, ga4_engagement, ga4_ecommerce
        )
        
        # Build position-to-revenue model
        position_revenue_model = _build_position_revenue_model(
            gsc_data, conversion_mapping, contextual_ctr_model
        )
        
        # Calculate total search-attributed revenue
        total_revenue = _calculate_total_search_revenue(
            gsc_data, conversion_mapping
        )
        
        # Estimate revenue at risk from decaying pages
        revenue_at_risk = _calculate_revenue_at_risk(
            decaying_pages, conversion_mapping
        )
        
        # Calculate top revenue keywords
        top_keywords = _identify_top_revenue_keywords(
            gsc_data, conversion_mapping, position_revenue_model
        )
        
        # Estimate ROI of recommended actions
        action_roi = _calculate_action_roi(
            gameplan_actions, conversion_mapping, position_revenue_model
        )
        
        result = {
            "total_search_attributed_revenue_monthly": total_revenue,
            "revenue_at_risk_90d": revenue_at_risk,
            "top_revenue_keywords": top_keywords,
            "action_roi": action_roi,
            "conversion_mapping": conversion_mapping,
            "position_revenue_projections": position_revenue_model,
            "metadata": {
                "generated_at": datetime.utcnow().isoformat(),
                "has_ecommerce_data": ga4_ecommerce is not None,
                "pages_analyzed": len(gsc_data) if gsc_data is not None else 0,
                "keywords_analyzed": len(gsc_data.get("query", [])) if gsc_data is not None else 0
            }
        }
        
        logger.info(f"Revenue attribution complete. Total monthly revenue: ${total_revenue:,.2f}")
        return result
        
    except Exception as e:
        logger.error(f"Error in revenue attribution analysis: {str(e)}")
        raise


def _calculate_conversion_mapping(
    gsc_data: pd.DataFrame,
    ga4_conversions: pd.DataFrame,
    ga4_engagement: pd.DataFrame,
    ga4_ecommerce: Optional[pd.DataFrame]
) -> Dict[str, Dict[str, float]]:
    """
    Map landing pages to conversion rates and revenue metrics.
    """
    
    if gsc_data is None or ga4_conversions is None or ga4_engagement is None:
        return {}
    
    mapping = {}
    
    # Group GSC data by page
    if isinstance(gsc_data, pd.DataFrame):
        gsc_by_page = gsc_data.groupby('page').agg({
            'clicks': 'sum',
            'impressions': 'sum'
        }).reset_index()
    else:
        return {}
    
    # Match with GA4 conversion data
    for _, row in gsc_by_page.iterrows():
        page = row['page']
        
        # Find matching GA4 data (handle URL normalization)
        ga4_match = ga4_conversions[ga4_conversions['landing_page'] == page]
        engagement_match = ga4_engagement[ga4_engagement['landing_page'] == page]
        
        if len(ga4_match) > 0:
            conversions = ga4_match['conversions'].iloc[0]
            sessions = ga4_match['sessions'].iloc[0] if 'sessions' in ga4_match.columns else engagement_match['sessions'].iloc[0] if len(engagement_match) > 0 else 0
            
            conversion_rate = conversions / sessions if sessions > 0 else 0.0
            
            # Calculate average order value if ecommerce data available
            avg_order_value = 0.0
            if ga4_ecommerce is not None:
                ecommerce_match = ga4_ecommerce[ga4_ecommerce['landing_page'] == page]
                if len(ecommerce_match) > 0:
                    total_revenue = ecommerce_match['revenue'].iloc[0]
                    transactions = ecommerce_match['transactions'].iloc[0]
                    avg_order_value = total_revenue / transactions if transactions > 0 else 0.0
            
            # If no ecommerce data, use a default conversion value (can be configured)
            if avg_order_value == 0.0 and conversion_rate > 0:
                avg_order_value = 100.0  # Default value per conversion
            
            mapping[page] = {
                'conversion_rate': conversion_rate,
                'avg_order_value': avg_order_value,
                'monthly_clicks': row['clicks'],
                'monthly_conversions': conversions,
                'monthly_revenue': conversions * avg_order_value
            }
    
    return mapping


def _build_position_revenue_model(
    gsc_data: pd.DataFrame,
    conversion_mapping: Dict[str, Dict[str, float]],
    contextual_ctr_model: Optional[Any]
) -> Dict[str, List[Dict[str, float]]]:
    """
    Model revenue impact of position changes for each keyword.
    """
    
    if gsc_data is None or not conversion_mapping:
        return {}
    
    model = {}
    
    # Use default CTR curve if no contextual model provided
    default_ctr_curve = {
        1: 0.316, 2: 0.158, 3: 0.082, 4: 0.051, 5: 0.036,
        6: 0.027, 7: 0.021, 8: 0.017, 9: 0.014, 10: 0.012
    }
    
    # Group by query to analyze keyword-level revenue
    if 'query' in gsc_data.columns and 'page' in gsc_data.columns:
        query_page = gsc_data.groupby(['query', 'page']).agg({
            'position': 'mean',
            'impressions': 'sum'
        }).reset_index()
        
        for _, row in query_page.iterrows():
            query = row['query']
            page = row['page']
            current_position = row['position']
            impressions = row['impressions']
            
            # Get conversion metrics for the landing page
            page_metrics = conversion_mapping.get(page, {})
            conversion_rate = page_metrics.get('conversion_rate', 0.0)
            avg_order_value = page_metrics.get('avg_order_value', 0.0)
            
            if conversion_rate == 0.0 or avg_order_value == 0.0:
                continue
            
            # Calculate revenue at different positions
            position_projections = []
            for position in range(1, 11):
                # Estimate CTR at this position
                if contextual_ctr_model:
                    # Use contextual model if available
                    estimated_ctr = default_ctr_curve.get(position, 0.01)
                else:
                    estimated_ctr = default_ctr_curve.get(position, 0.01)
                
                # Calculate projected metrics
                projected_clicks = impressions * estimated_ctr
                projected_conversions = projected_clicks * conversion_rate
                projected_revenue = projected_conversions * avg_order_value
                
                position_projections.append({
                    'position': position,
                    'estimated_ctr': estimated_ctr,
                    'projected_clicks': round(projected_clicks, 1),
                    'projected_conversions': round(projected_conversions, 2),
                    'projected_revenue': round(projected_revenue, 2)
                })
            
            model[query] = {
                'current_position': round(current_position, 1),
                'landing_page': page,
                'impressions': impressions,
                'conversion_rate': conversion_rate,
                'avg_order_value': avg_order_value,
                'projections': position_projections
            }
    
    return model


def _calculate_total_search_revenue(
    gsc_data: pd.DataFrame,
    conversion_mapping: Dict[str, Dict[str, float]]
) -> float:
    """
    Calculate total monthly revenue attributed to organic search.
    """
    
    if not conversion_mapping:
        return 0.0
    
    total_revenue = sum(
        metrics['monthly_revenue']
        for metrics in conversion_mapping.values()
    )
    
    return round(total_revenue, 2)


def _calculate_revenue_at_risk(
    decaying_pages: Optional[List[Dict[str, Any]]],
    conversion_mapping: Dict[str, Dict[str, float]]
) -> float:
    """
    Estimate revenue at risk from decaying pages over 90 days.
    """
    
    if not decaying_pages or not conversion_mapping:
        return 0.0
    
    revenue_at_risk = 0.0
    
    for page_data in decaying_pages:
        if page_data.get('bucket') in ['decaying', 'critical']:
            page = page_data.get('url')
            trend_slope = page_data.get('trend_slope', 0.0)
            
            if page in conversion_mapping and trend_slope < 0:
                current_monthly_revenue = conversion_mapping[page]['monthly_revenue']
                
                # Project revenue loss over 90 days (3 months)
                # Assuming linear decay: current_revenue * |slope| * 3
                projected_loss = current_monthly_revenue * abs(trend_slope) * 3
                revenue_at_risk += projected_loss
    
    return round(revenue_at_risk, 2)


def _identify_top_revenue_keywords(
    gsc_data: pd.DataFrame,
    conversion_mapping: Dict[str, Dict[str, float]],
    position_revenue_model: Dict[str, List[Dict[str, float]]]
) -> List[Dict[str, Any]]:
    """
    Identify top keywords by current and potential revenue.
    """
    
    if not position_revenue_model:
        return []
    
    top_keywords = []
    
    for query, model_data in position_revenue_model.items():
        current_position = model_data['current_position']
        projections = model_data['projections']
        
        # Find current revenue
        current_proj = next(
            (p for p in projections if abs(p['position'] - current_position) < 1),
            projections[min(int(current_position) - 1, len(projections) - 1)]
        )
        
        # Find top 3 position revenue
        top3_proj = projections[2]  # Position 3 (index 2)
        
        current_revenue = current_proj['projected_revenue']
        potential_revenue = top3_proj['projected_revenue']
        gap = potential_revenue - current_revenue
        
        if current_revenue > 0 or gap > 0:
            top_keywords.append({
                'keyword': query,
                'current_position': current_position,
                'current_revenue_monthly': round(current_revenue, 2),
                'potential_revenue_if_top3': round(potential_revenue, 2),
                'gap': round(gap, 2),
                'landing_page': model_data['landing_page'],
                'impressions': model_data['impressions']
            })
    
    # Sort by gap (opportunity size)
    top_keywords.sort(key=lambda x: x['gap'], reverse=True)
    
    return top_keywords[:20]  # Return top 20


def _calculate_action_roi(
    gameplan_actions: Optional[List[Dict[str, Any]]],
    conversion_mapping: Dict[str, Dict[str, float]],
    position_revenue_model: Dict[str, List[Dict[str, float]]]
) -> Dict[str, float]:
    """
    Calculate ROI estimates for recommended actions.
    """
    
    if not gameplan_actions:
        return {
            'critical_fixes_monthly_value': 0.0,
            'quick_wins_monthly_value': 0.0,
            'strategic_plays_monthly_value': 0.0,
            'total_opportunity': 0.0
        }
    
    roi = {
        'critical_fixes_monthly_value': 0.0,
        'quick_wins_monthly_value': 0.0,
        'strategic_plays_monthly_value': 0.0,
        'structural_improvements_monthly_value': 0.0
    }
    
    for action in gameplan_actions:
        category = action.get('category', 'quick_wins')  # critical, quick_wins, strategic, structural
        
        # Estimate revenue impact based on action type and affected pages/keywords
        impact_clicks = action.get('impact', 0)  # clicks/month recoverable
        
        # Convert clicks to revenue using average conversion metrics
        if conversion_mapping:
            avg_conversion_rate = np.mean([
                m['conversion_rate'] for m in conversion_mapping.values()
            ])
            avg_order_value = np.mean([
                m['avg_order_value'] for m in conversion_mapping.values()
                if m['avg_order_value'] > 0
            ])
            
            revenue_impact = impact_clicks * avg_conversion_rate * avg_order_value
        else:
            # Fallback estimate: $1 per click (conservative)
            revenue_impact = impact_clicks * 1.0
        
        # Categorize
        if category == 'critical':
            roi['critical_fixes_monthly_value'] += revenue_impact
        elif category == 'quick_wins':
            roi['quick_wins_monthly_value'] += revenue_impact
        elif category == 'strategic':
            roi['strategic_plays_monthly_value'] += revenue_impact
        else:
            roi['structural_improvements_monthly_value'] += revenue_impact
    
    # Round all values
    for key in roi:
        roi[key] = round(roi[key], 2)
    
    roi['total_opportunity'] = sum(roi.values())
    
    return roi


# Placeholder implementation for development
def analyze_revenue_attribution_placeholder() -> Dict[str, Any]:
    """
    Placeholder implementation returning mock data structure.
    """
    return {
        "total_search_attributed_revenue_monthly": 0.0,
        "revenue_at_risk_90d": 0.0,
        "top_revenue_keywords": [],
        "action_roi": {
            "critical_fixes_monthly_value": 0.0,
            "quick_wins_monthly_value": 0.0,
            "strategic_plays_monthly_value": 0.0,
            "total_opportunity": 0.0
        },
        "metadata": {
            "generated_at": datetime.utcnow().isoformat(),
            "status": "placeholder"
        }
    }
