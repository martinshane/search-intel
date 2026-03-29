"""
Module 4: Content Intelligence

Analyzes content cannibalization, striking distance opportunities,
thin content flags, and content age vs performance matrix.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
from collections import defaultdict
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


def analyze_content_intelligence(
    gsc_query_page: pd.DataFrame,
    page_data: pd.DataFrame,
    ga4_engagement: pd.DataFrame
) -> Dict[str, Any]:
    """
    Comprehensive content intelligence analysis combining:
    1. Cannibalization detection
    2. Striking distance opportunities
    3. Thin content flagging
    4. Content age vs performance matrix
    
    Args:
        gsc_query_page: DataFrame with columns [query, page, clicks, impressions, position]
        page_data: DataFrame from crawl with [url, word_count, last_modified, title, h1]
        ga4_engagement: DataFrame with [page, bounce_rate, avg_session_duration, sessions]
    
    Returns:
        Dictionary with cannibalization_clusters, striking_distance, thin_content,
        update_priority_matrix
    """
    try:
        logger.info("Starting content intelligence analysis")
        
        # 1. Cannibalization detection
        cannibalization = detect_cannibalization(gsc_query_page)
        
        # 2. Striking distance opportunities
        striking_distance = find_striking_distance(gsc_query_page)
        
        # 3. Thin content flagging
        thin_content = flag_thin_content(gsc_query_page, page_data, ga4_engagement)
        
        # 4. Content age vs performance matrix
        update_matrix = analyze_content_age_performance(
            gsc_query_page, page_data
        )
        
        result = {
            "cannibalization_clusters": cannibalization,
            "striking_distance": striking_distance,
            "thin_content": thin_content,
            "update_priority_matrix": update_matrix,
            "summary": {
                "cannibalization_clusters_found": len(cannibalization),
                "total_impressions_cannibalized": sum(
                    c["total_impressions_affected"] for c in cannibalization
                ),
                "striking_distance_keywords": len(striking_distance),
                "estimated_strike_distance_clicks": sum(
                    sd["estimated_click_gain_if_top5"] for sd in striking_distance
                ),
                "thin_content_pages": len(thin_content),
                "urgent_update_pages": len(update_matrix.get("urgent_update", []))
            }
        }
        
        logger.info(f"Content intelligence analysis complete: {result['summary']}")
        return result
        
    except Exception as e:
        logger.error(f"Error in content intelligence analysis: {str(e)}", exc_info=True)
        raise


def detect_cannibalization(gsc_query_page: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Detect keyword cannibalization: queries ranking for multiple pages.
    
    Args:
        gsc_query_page: DataFrame with query, page, clicks, impressions, position
    
    Returns:
        List of cannibalization clusters with affected queries and pages
    """
    try:
        clusters = []
        
        # Group by query to find those with multiple pages
        query_groups = gsc_query_page.groupby('query')
        
        for query, group in query_groups:
            if len(group) < 2:
                continue
            
            # Multiple pages ranking for this query
            pages = group['page'].tolist()
            total_impressions = group['impressions'].sum()
            
            # Skip low-volume cannibalization
            if total_impressions < 100:
                continue
            
            # Calculate metrics
            avg_position = group['position'].mean()
            best_position = group['position'].min()
            worst_position = group['position'].max()
            position_gap = worst_position - best_position
            
            # Determine winning page (best position or most clicks)
            group_sorted = group.sort_values(['position', 'clicks'], ascending=[True, False])
            winning_page = group_sorted.iloc[0]['page']
            
            # Determine recommendation
            recommendation = determine_cannibalization_action(
                position_gap, total_impressions, len(pages)
            )
            
            clusters.append({
                "query": query,
                "query_group": query,  # Could cluster similar queries
                "pages": pages,
                "page_count": len(pages),
                "total_impressions_affected": int(total_impressions),
                "total_clicks": int(group['clicks'].sum()),
                "avg_position": round(avg_position, 1),
                "best_position": round(best_position, 1),
                "worst_position": round(worst_position, 1),
                "position_gap": round(position_gap, 1),
                "keep_page": winning_page,
                "recommendation": recommendation,
                "severity": calculate_cannibalization_severity(
                    total_impressions, position_gap
                ),
                "page_performance": [
                    {
                        "url": row['page'],
                        "position": round(row['position'], 1),
                        "clicks": int(row['clicks']),
                        "impressions": int(row['impressions']),
                        "ctr": round(row['clicks'] / row['impressions'] * 100, 2)
                            if row['impressions'] > 0 else 0
                    }
                    for _, row in group.iterrows()
                ]
            })
        
        # Sort by severity
        clusters.sort(key=lambda x: x['severity'], reverse=True)
        
        logger.info(f"Found {len(clusters)} cannibalization clusters")
        return clusters
        
    except Exception as e:
        logger.error(f"Error detecting cannibalization: {str(e)}", exc_info=True)
        return []


def determine_cannibalization_action(
    position_gap: float,
    impressions: float,
    page_count: int
) -> str:
    """Determine recommended action for cannibalization."""
    if position_gap < 5 and page_count == 2:
        return "differentiate"
    elif position_gap > 10:
        return "consolidate"
    elif impressions > 5000:
        return "consolidate"
    elif page_count > 3:
        return "consolidate"
    else:
        return "canonical_redirect"


def calculate_cannibalization_severity(
    impressions: float,
    position_gap: float
) -> float:
    """Calculate severity score for cannibalization (0-100)."""
    # Higher impressions and larger position gap = more severe
    impression_factor = min(impressions / 10000, 1.0) * 50
    gap_factor = min(position_gap / 20, 1.0) * 50
    return round(impression_factor + gap_factor, 1)


def find_striking_distance(gsc_query_page: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Find keywords in striking distance (positions 8-20) worth targeting.
    
    Args:
        gsc_query_page: DataFrame with query, page, clicks, impressions, position
    
    Returns:
        List of striking distance opportunities
    """
    try:
        # Filter for striking distance positions
        striking = gsc_query_page[
            (gsc_query_page['position'] >= 8) &
            (gsc_query_page['position'] <= 20) &
            (gsc_query_page['impressions'] >= 100)  # Minimum volume threshold
        ].copy()
        
        if striking.empty:
            logger.info("No striking distance keywords found")
            return []
        
        # Calculate potential click gain
        striking['estimated_click_gain_if_top5'] = striking.apply(
            lambda row: estimate_click_gain_to_top5(
                row['impressions'],
                row['position'],
                row['clicks']
            ),
            axis=1
        )
        
        # Classify intent from query patterns
        striking['intent'] = striking['query'].apply(classify_query_intent)
        
        # Sort by potential gain
        striking = striking.sort_values('estimated_click_gain_if_top5', ascending=False)
        
        opportunities = []
        for _, row in striking.iterrows():
            opportunities.append({
                "query": row['query'],
                "current_position": round(row['position'], 1),
                "impressions": int(row['impressions']),
                "current_clicks": int(row['clicks']),
                "current_ctr": round(row['clicks'] / row['impressions'] * 100, 2)
                    if row['impressions'] > 0 else 0,
                "estimated_click_gain_if_top5": int(row['estimated_click_gain_if_top5']),
                "intent": row['intent'],
                "landing_page": row['page'],
                "difficulty": estimate_ranking_difficulty(row['position']),
                "priority_score": calculate_striking_distance_priority(
                    row['impressions'],
                    row['estimated_click_gain_if_top5'],
                    row['position']
                )
            })
        
        logger.info(f"Found {len(opportunities)} striking distance opportunities")
        return opportunities[:50]  # Limit to top 50
        
    except Exception as e:
        logger.error(f"Error finding striking distance: {str(e)}", exc_info=True)
        return []


def estimate_click_gain_to_top5(
    impressions: float,
    current_position: float,
    current_clicks: float
) -> float:
    """
    Estimate additional clicks if keyword moves to top 5.
    Uses simplified CTR curve assumptions.
    """
    # Generic CTR benchmarks by position
    ctr_benchmarks = {
        1: 0.28, 2: 0.15, 3: 0.11, 4: 0.08, 5: 0.06,
        8: 0.04, 10: 0.025, 15: 0.015, 20: 0.01
    }
    
    # Estimate current CTR based on position
    current_ctr = current_clicks / impressions if impressions > 0 else 0
    
    # Estimate CTR if in top 5 (use position 4 as target)
    target_ctr = ctr_benchmarks.get(4, 0.08)
    
    # Estimated click gain
    potential_clicks = impressions * target_ctr
    gain = max(0, potential_clicks - current_clicks)
    
    return gain


def classify_query_intent(query: str) -> str:
    """
    Classify query intent based on keyword patterns.
    Simple rule-based classification.
    """
    query_lower = query.lower()
    
    # Transactional signals
    transactional_keywords = ['buy', 'price', 'cost', 'cheap', 'deal', 'discount', 'coupon', 'order', 'purchase']
    if any(kw in query_lower for kw in transactional_keywords):
        return "transactional"
    
    # Commercial investigation signals
    commercial_keywords = ['best', 'top', 'review', 'vs', 'versus', 'compare', 'alternative']
    if any(kw in query_lower for kw in commercial_keywords):
        return "commercial"
    
    # Informational signals
    informational_keywords = ['how', 'what', 'why', 'when', 'guide', 'tutorial', 'tips', 'learn']
    if any(kw in query_lower for kw in informational_keywords):
        return "informational"
    
    # Navigational (branded) - harder to detect without brand context
    if len(query.split()) <= 2 and not any(kw in query_lower for kw in informational_keywords + commercial_keywords):
        return "navigational"
    
    return "informational"  # Default


def estimate_ranking_difficulty(current_position: float) -> str:
    """Estimate difficulty of reaching top 5 from current position."""
    if current_position <= 10:
        return "low"
    elif current_position <= 15:
        return "medium"
    else:
        return "high"


def calculate_striking_distance_priority(
    impressions: float,
    estimated_gain: float,
    position: float
) -> float:
    """Calculate priority score for striking distance keyword (0-100)."""
    # Higher impressions = more valuable
    impression_score = min(impressions / 5000, 1.0) * 40
    
    # Higher estimated gain = higher priority
    gain_score = min(estimated_gain / 500, 1.0) * 40
    
    # Closer to page 1 = easier win
    position_score = max(0, (20 - position) / 12) * 20
    
    return round(impression_score + gain_score + position_score, 1)


def flag_thin_content(
    gsc_query_page: pd.DataFrame,
    page_data: pd.DataFrame,
    ga4_engagement: pd.DataFrame
) -> List[Dict[str, Any]]:
    """
    Flag pages with thin content that have search visibility.
    
    Args:
        gsc_query_page: GSC performance data
        page_data: Crawl data with word counts
        ga4_engagement: GA4 engagement metrics
    
    Returns:
        List of thin content pages with flags
    """
    try:
        # Aggregate GSC data by page
        page_performance = gsc_query_page.groupby('page').agg({
            'impressions': 'sum',
            'clicks': 'sum',
            'position': 'mean'
        }).reset_index()
        
        # Merge with page data
        if not page_data.empty and 'url' in page_data.columns:
            page_performance = page_performance.merge(
                page_data[['url', 'word_count']],
                left_on='page',
                right_on='url',
                how='left'
            )
        else:
            page_performance['word_count'] = np.nan
        
        # Merge with GA4 engagement
        if not ga4_engagement.empty:
            page_performance = page_performance.merge(
                ga4_engagement[['page', 'bounce_rate', 'avg_session_duration', 'sessions']],
                on='page',
                how='left'
            )
        else:
            page_performance['bounce_rate'] = np.nan
            page_performance['avg_session_duration'] = np.nan
            page_performance['sessions'] = np.nan
        
        thin_pages = []
        
        for _, row in page_performance.iterrows():
            flags = []
            
            # Must have meaningful search visibility
            if row['impressions'] < 100:
                continue
            
            # Thin content by word count
            if pd.notna(row.get('word_count')) and row['word_count'] < 500:
                flags.append("low_word_count")
            
            # High bounce rate
            if pd.notna(row.get('bounce_rate')) and row['bounce_rate'] > 85:
                flags.append("high_bounce_rate")
            
            # Low engagement time
            if pd.notna(row.get('avg_session_duration')) and row['avg_session_duration'] < 20:
                flags.append("low_engagement_time")
            
            # CTR below expected for position
            if row['impressions'] > 0:
                actual_ctr = row['clicks'] / row['impressions']
                expected_ctr = get_expected_ctr_for_position(row['position'])
                
                if actual_ctr < expected_ctr * 0.6:  # 40% below expected
                    flags.append("low_ctr")
            
            if flags:
                thin_pages.append({
                    "url": row['page'],
                    "impressions": int(row['impressions']),
                    "clicks": int(row['clicks']),
                    "position": round(row['position'], 1),
                    "word_count": int(row['word_count']) if pd.notna(row.get('word_count')) else None,
                    "bounce_rate": round(row['bounce_rate'], 1) if pd.notna(row.get('bounce_rate')) else None,
                    "avg_session_duration": round(row['avg_session_duration'], 1) if pd.notna(row.get('avg_session_duration')) else None,
                    "flags": flags,
                    "severity": len(flags),
                    "recommended_action": "content_expansion" if "low_word_count" in flags else "content_rewrite"
                })
        
        # Sort by severity and impressions
        thin_pages.sort(key=lambda x: (x['severity'], x['impressions']), reverse=True)
        
        logger.info(f"Flagged {len(thin_pages)} thin content pages")
        return thin_pages[:30]  # Limit to top 30
        
    except Exception as e:
        logger.error(f"Error flagging thin content: {str(e)}", exc_info=True)
        return []


def get_expected_ctr_for_position(position: float) -> float:
    """Get expected CTR for a given position."""
    ctr_curve = {
        1: 0.28, 2: 0.15, 3: 0.11, 4: 0.08, 5: 0.06,
        6: 0.05, 7: 0.04, 8: 0.04, 9: 0.03, 10: 0.025
    }
    
    # Linear interpolation for positions not in the dict
    pos_int = int(position)
    if pos_int in ctr_curve:
        return ctr_curve[pos_int]
    elif pos_int < 1:
        return ctr_curve[1]
    elif pos_int > 10:
        return 0.025 * (10 / pos_int)  # Decay curve
    else:
        # Interpolate between two nearest positions
        lower = ctr_curve.get(pos_int, 0.025)
        upper = ctr_curve.get(pos_int + 1, 0.025)
        fraction = position - pos_int
        return lower + (upper - lower) * fraction


def analyze_content_age_performance(
    gsc_query_page: pd.DataFrame,
    page_data: pd.DataFrame
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Create content age vs performance matrix.
    Quadrants: urgent_update, leave_alone, structural_problem, double_down
    
    Args:
        gsc_query_page: GSC performance data
        page_data: Crawl data with last_modified dates
    
    Returns:
        Dictionary with pages categorized by quadrant
    """
    try:
        # Aggregate performance by page
        page_performance = gsc_query_page.groupby('page').agg({
            'impressions': 'sum',
            'clicks': 'sum',
            'position': 'mean'
        }).reset_index()
        
        # Add time series analysis for trajectory
        # Calculate per-page trend (simplified - would use actual time series in production)
        page_trends = calculate_page_trends(gsc_query_page)
        page_performance = page_performance.merge(
            page_trends,
            on='page',
            how='left'
        )
        
        # Merge with page data for age
        if not page_data.empty and 'url' in page_data.columns and 'last_modified' in page_data.columns:
            page_performance = page_performance.merge(
                page_data[['url', 'last_modified']],
                left_on='page',
                right_on='url',
                how='left'
            )
            
            # Calculate age in days
            now = datetime.now()
            page_performance['age_days'] = page_performance['last_modified'].apply(
                lambda x: (now - pd.to_datetime(x)).days if pd.notna(x) else None
            )
        else:
            page_performance['age_days'] = np.nan
        
        # Categorize into quadrants
        quadrants = {
            "urgent_update": [],
            "leave_alone": [],
            "structural_problem": [],
            "double_down": []
        }
        
        for _, row in page_performance.iterrows():
            # Skip if missing data
            if pd.isna(row.get('age_days')) or pd.isna(row.get('trend')):
                continue
            
            # Skip low-volume pages
            if row['impressions'] < 100:
                continue
            
            # Categorize by age and trend
            is_old = row['age_days'] > 180  # 6+ months old
            is_decaying = row['trend'] < -0.1  # Negative trend
            is_growing = row['trend'] > 0.1  # Positive trend
            
            page_info = {
                "url": row['page'],
                "age_days": int(row['age_days']),
                "impressions": int(row['impressions']),
                "clicks": int(row['clicks']),
                "position": round(row['position'], 1),
                "trend": round(row['trend'], 3),
                "last_modified": row.get('last_modified')
            }
            
            if is_old and is_decaying:
                page_info['quadrant'] = "urgent_update"
                page_info['action'] = "Content refresh needed - traffic declining"
                quadrants['urgent_update'].append(page_info)
            elif is_old and not is_decaying and not is_growing:
                page_info['quadrant'] = "leave_alone"
                page_info['action'] = "Evergreen content - stable performance"
                quadrants['leave_alone'].append(page_info)
            elif not is_old and is_decaying:
                page_info['quadrant'] = "structural_problem"
                page_info['action'] = "New content declining - wrong intent or targeting"
                quadrants['structural_problem'].append(page_info)
            elif not is_old and is_growing:
                page_info['quadrant'] = "double_down"
                page_info['action'] = "New content growing - add internal links and promotion"
                quadrants['double_down'].append(page_info)
        
        # Sort each quadrant by impressions
        for quadrant in quadrants:
            quadrants[quadrant].sort(key=lambda x: x['impressions'], reverse=True)
        
        logger.info(f"Content age matrix: urgent_update={len(quadrants['urgent_update'])}, "
                   f"leave_alone={len(quadrants['leave_alone'])}, "
                   f"structural_problem={len(quadrants['structural_problem'])}, "
                   f"double_down={len(quadrants['double_down'])}")
        
        return quadrants
        
    except Exception as e:
        logger.error(f"Error in content age analysis: {str(e)}", exc_info=True)
        return {
            "urgent_update": [],
            "leave_alone": [],
            "structural_problem": [],
            "double_down": []
        }


def calculate_page_trends(gsc_query_page: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate trend for each page (simplified version).
    In production, this would use actual time series data.
    