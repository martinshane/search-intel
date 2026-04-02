# api/modules/module_5.py
# Page-Level CTR Analysis Module

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


def calculate_expected_ctr(position: float) -> float:
    """
    Calculate expected CTR based on position using industry-standard CTR curve.
    
    Based on aggregated industry data:
    - Position 1: ~28-35% CTR
    - Position 2: ~15-18% CTR
    - Position 3: ~10-12% CTR
    - Position 4-10: exponential decay
    - Position 11+: < 2% CTR
    
    Args:
        position: Average search position
        
    Returns:
        Expected CTR as decimal (e.g., 0.28 for 28%)
    """
    if position <= 0:
        return 0.0
    
    # CTR curve model (exponential decay)
    # Formula: CTR = a * e^(-b * position) + c
    # Calibrated to industry benchmarks
    a = 0.35
    b = 0.25
    c = 0.005
    
    expected_ctr = a * np.exp(-b * (position - 1)) + c
    
    # Cap at realistic maximums
    if position <= 1:
        expected_ctr = min(expected_ctr, 0.35)
    
    return max(0.0, min(1.0, expected_ctr))


def calculate_ctr_performance_ratio(actual_ctr: float, expected_ctr: float) -> float:
    """
    Calculate performance ratio: actual CTR / expected CTR.
    
    Ratio interpretation:
    - > 1.2: Outperforming (great title/snippet)
    - 0.8-1.2: Normal performance
    - 0.5-0.8: Underperforming (optimization opportunity)
    - < 0.5: Severely underperforming (urgent fix needed)
    
    Args:
        actual_ctr: Observed CTR
        expected_ctr: Expected CTR based on position
        
    Returns:
        Performance ratio
    """
    if expected_ctr == 0:
        return 1.0 if actual_ctr == 0 else 0.0
    
    return actual_ctr / expected_ctr


def classify_ctr_performance(ratio: float) -> str:
    """
    Classify CTR performance based on ratio.
    
    Args:
        ratio: actual_ctr / expected_ctr
        
    Returns:
        Performance classification string
    """
    if ratio >= 1.2:
        return "outperforming"
    elif ratio >= 0.8:
        return "normal"
    elif ratio >= 0.5:
        return "underperforming"
    else:
        return "severely_underperforming"


def calculate_ctr_opportunity(
    impressions: int,
    actual_ctr: float,
    expected_ctr: float
) -> Dict[str, float]:
    """
    Calculate potential click gain from improving CTR to expected level.
    
    Args:
        impressions: Monthly impressions
        actual_ctr: Current CTR
        expected_ctr: Expected CTR at current position
        
    Returns:
        Dictionary with opportunity metrics
    """
    current_clicks = impressions * actual_ctr
    potential_clicks = impressions * expected_ctr
    click_opportunity = potential_clicks - current_clicks
    
    # Also calculate opportunity if we reach 120% of expected (best-in-class)
    best_case_ctr = min(expected_ctr * 1.2, 1.0)
    best_case_clicks = impressions * best_case_ctr
    best_case_opportunity = best_case_clicks - current_clicks
    
    return {
        "current_clicks": round(current_clicks, 1),
        "expected_clicks": round(potential_clicks, 1),
        "click_opportunity": round(max(0, click_opportunity), 1),
        "best_case_clicks": round(best_case_clicks, 1),
        "best_case_opportunity": round(max(0, best_case_opportunity), 1),
        "improvement_potential_pct": round((click_opportunity / max(current_clicks, 1)) * 100, 1)
    }


def identify_title_snippet_issues(
    page_url: str,
    ctr_ratio: float,
    position: float,
    impressions: int
) -> Optional[Dict[str, Any]]:
    """
    Identify likely title/snippet issues based on CTR underperformance.
    
    Args:
        page_url: Page URL
        ctr_ratio: CTR performance ratio
        position: Average position
        impressions: Monthly impressions
        
    Returns:
        Issue diagnosis if found, None otherwise
    """
    if ctr_ratio >= 0.8:
        return None
    
    # Only flag if meaningful traffic (enough impressions to be statistically significant)
    if impressions < 100:
        return None
    
    # Only flag if position is reasonable (top 20)
    if position > 20:
        return None
    
    severity = "high" if ctr_ratio < 0.5 else "medium"
    
    # Diagnosis based on position and CTR ratio
    if position <= 3 and ctr_ratio < 0.5:
        issue_type = "weak_title"
        description = "Ranking in top 3 but CTR is severely low - title likely not compelling"
    elif position <= 5 and ctr_ratio < 0.6:
        issue_type = "title_mismatch"
        description = "Top 5 position with poor CTR - title may not match search intent"
    elif position <= 10 and ctr_ratio < 0.7:
        issue_type = "snippet_optimization"
        description = "Page 1 ranking with below-average CTR - optimize title and meta description"
    else:
        issue_type = "general_ctr_issue"
        description = f"Position {position:.1f} with {int(ctr_ratio*100)}% of expected CTR"
    
    return {
        "issue_type": issue_type,
        "severity": severity,
        "description": description,
        "recommended_action": get_ctr_improvement_action(issue_type, position)
    }


def get_ctr_improvement_action(issue_type: str, position: float) -> str:
    """
    Get recommended action for CTR improvement.
    
    Args:
        issue_type: Type of CTR issue
        position: Current position
        
    Returns:
        Recommended action string
    """
    actions = {
        "weak_title": "Rewrite title to be more compelling and benefit-focused. Consider adding year, numbers, or power words.",
        "title_mismatch": "Align title more closely with search intent. Analyze competing titles in SERPs.",
        "snippet_optimization": "Optimize meta description to better highlight value proposition. Ensure title has primary keyword.",
        "general_ctr_issue": "Review and improve title tag and meta description. Consider adding schema markup for rich snippets."
    }
    
    base_action = actions.get(issue_type, "Optimize title and meta description for better CTR")
    
    if position <= 5:
        base_action += " Priority: HIGH - you're ranking well, CTR optimization is quick win."
    
    return base_action


def analyze_ctr_distribution(page_data: pd.DataFrame) -> Dict[str, Any]:
    """
    Analyze overall CTR distribution across all pages.
    
    Args:
        page_data: DataFrame with page performance data
        
    Returns:
        Distribution statistics
    """
    if page_data.empty:
        return {
            "total_pages": 0,
            "performance_breakdown": {},
            "average_ctr_ratio": 0,
            "median_ctr_ratio": 0
        }
    
    # Calculate performance ratios for all pages
    page_data['ctr_ratio'] = page_data.apply(
        lambda row: calculate_ctr_performance_ratio(row['ctr'], row['expected_ctr']),
        axis=1
    )
    
    page_data['performance_class'] = page_data['ctr_ratio'].apply(classify_ctr_performance)
    
    # Count pages in each performance category
    performance_breakdown = page_data['performance_class'].value_counts().to_dict()
    
    # Calculate aggregate statistics
    avg_ratio = page_data['ctr_ratio'].mean()
    median_ratio = page_data['ctr_ratio'].median()
    
    # Weighted average (by impressions)
    if 'impressions' in page_data.columns:
        weighted_avg_ratio = (
            (page_data['ctr_ratio'] * page_data['impressions']).sum() / 
            page_data['impressions'].sum()
        )
    else:
        weighted_avg_ratio = avg_ratio
    
    return {
        "total_pages": len(page_data),
        "performance_breakdown": performance_breakdown,
        "average_ctr_ratio": round(avg_ratio, 3),
        "median_ctr_ratio": round(median_ratio, 3),
        "weighted_average_ctr_ratio": round(weighted_avg_ratio, 3),
        "pages_outperforming": performance_breakdown.get("outperforming", 0),
        "pages_normal": performance_breakdown.get("normal", 0),
        "pages_underperforming": performance_breakdown.get("underperforming", 0),
        "pages_severely_underperforming": performance_breakdown.get("severely_underperforming", 0)
    }


def analyze_page_ctr(
    gsc_page_data: pd.DataFrame,
    min_impressions: int = 100,
    days: int = 90
) -> Dict[str, Any]:
    """
    Main function: Analyze CTR performance for all pages.
    
    Identifies:
    - Top performing pages by CTR (overperforming expectations)
    - Underperforming pages (high impressions, low CTR)
    - CTR optimization opportunities
    - Overall CTR distribution
    
    Args:
        gsc_page_data: DataFrame with columns [page, clicks, impressions, ctr, position, date]
        min_impressions: Minimum impressions to include in analysis (default 100)
        days: Number of days to analyze (default 90)
        
    Returns:
        Dictionary with structured CTR analysis results
    """
    logger.info(f"Starting page-level CTR analysis for {len(gsc_page_data)} rows")
    
    # Filter to recent time period if date column exists
    if 'date' in gsc_page_data.columns:
        gsc_page_data['date'] = pd.to_datetime(gsc_page_data['date'])
        cutoff_date = datetime.now() - timedelta(days=days)
        gsc_page_data = gsc_page_data[gsc_page_data['date'] >= cutoff_date]
        logger.info(f"Filtered to last {days} days: {len(gsc_page_data)} rows")
    
    # Aggregate by page (sum clicks/impressions, average position)
    page_summary = gsc_page_data.groupby('page').agg({
        'clicks': 'sum',
        'impressions': 'sum',
        'position': 'mean'
    }).reset_index()
    
    # Calculate actual CTR
    page_summary['ctr'] = page_summary['clicks'] / page_summary['impressions'].replace(0, 1)
    
    # Filter to pages with meaningful traffic
    page_summary = page_summary[page_summary['impressions'] >= min_impressions]
    logger.info(f"Pages with >= {min_impressions} impressions: {len(page_summary)}")
    
    if page_summary.empty:
        logger.warning("No pages meet minimum impression threshold")
        return {
            "summary": {
                "total_pages_analyzed": 0,
                "total_impressions": 0,
                "total_clicks": 0,
                "average_ctr": 0,
                "total_click_opportunity": 0
            },
            "top_performers": [],
            "underperformers": [],
            "ctr_opportunities": [],
            "distribution": {}
        }
    
    # Calculate expected CTR for each page based on position
    page_summary['expected_ctr'] = page_summary['position'].apply(calculate_expected_ctr)
    page_summary['ctr_ratio'] = page_summary.apply(
        lambda row: calculate_ctr_performance_ratio(row['ctr'], row['expected_ctr']),
        axis=1
    )
    page_summary['performance_class'] = page_summary['ctr_ratio'].apply(classify_ctr_performance)
    
    # Calculate opportunities
    opportunities = page_summary.apply(
        lambda row: calculate_ctr_opportunity(
            row['impressions'],
            row['ctr'],
            row['expected_ctr']
        ),
        axis=1
    )
    
    page_summary['click_opportunity'] = [opp['click_opportunity'] for opp in opportunities]
    page_summary['improvement_potential_pct'] = [opp['improvement_potential_pct'] for opp in opportunities]
    
    # Identify top performers (overperforming CTR expectations)
    top_performers = page_summary[page_summary['ctr_ratio'] >= 1.2].copy()
    top_performers = top_performers.nlargest(20, 'ctr_ratio')
    
    top_performers_list = []
    for _, row in top_performers.iterrows():
        top_performers_list.append({
            "page": row['page'],
            "position": round(row['position'], 1),
            "impressions": int(row['impressions']),
            "clicks": int(row['clicks']),
            "ctr": round(row['ctr'], 4),
            "expected_ctr": round(row['expected_ctr'], 4),
            "ctr_ratio": round(row['ctr_ratio'], 2),
            "performance_class": row['performance_class']
        })
    
    # Identify underperformers (high impressions, low CTR ratio)
    underperformers = page_summary[
        (page_summary['ctr_ratio'] < 0.8) & 
        (page_summary['impressions'] >= min_impressions * 2)
    ].copy()
    underperformers = underperformers.nlargest(30, 'click_opportunity')
    
    underperformers_list = []
    for _, row in underperformers.iterrows():
        issue = identify_title_snippet_issues(
            row['page'],
            row['ctr_ratio'],
            row['position'],
            row['impressions']
        )
        
        underperformers_list.append({
            "page": row['page'],
            "position": round(row['position'], 1),
            "impressions": int(row['impressions']),
            "clicks": int(row['clicks']),
            "ctr": round(row['ctr'], 4),
            "expected_ctr": round(row['expected_ctr'], 4),
            "ctr_ratio": round(row['ctr_ratio'], 2),
            "click_opportunity": round(row['click_opportunity'], 1),
            "improvement_potential_pct": round(row['improvement_potential_pct'], 1),
            "performance_class": row['performance_class'],
            "issue": issue
        })
    
    # Top opportunities (sorted by click_opportunity)
    opportunities_list = page_summary[page_summary['click_opportunity'] > 0].copy()
    opportunities_list = opportunities_list.nlargest(50, 'click_opportunity')
    
    ctr_opportunities = []
    for _, row in opportunities_list.iterrows():
        ctr_opportunities.append({
            "page": row['page'],
            "position": round(row['position'], 1),
            "impressions": int(row['impressions']),
            "current_clicks": int(row['clicks']),
            "potential_clicks": int(row['clicks'] + row['click_opportunity']),
            "click_opportunity": round(row['click_opportunity'], 1),
            "improvement_potential_pct": round(row['improvement_potential_pct'], 1),
            "ctr": round(row['ctr'], 4),
            "expected_ctr": round(row['expected_ctr'], 4),
            "ctr_ratio": round(row['ctr_ratio'], 2),
            "recommended_action": get_ctr_improvement_action(
                "general_ctr_issue" if row['ctr_ratio'] >= 0.5 else "weak_title",
                row['position']
            )
        })
    
    # Overall distribution analysis
    distribution = analyze_ctr_distribution(page_summary)
    
    # Summary statistics
    total_click_opportunity = page_summary['click_opportunity'].sum()
    
    summary = {
        "total_pages_analyzed": len(page_summary),
        "total_impressions": int(page_summary['impressions'].sum()),
        "total_clicks": int(page_summary['clicks'].sum()),
        "average_ctr": round(page_summary['ctr'].mean(), 4),
        "average_position": round(page_summary['position'].mean(), 1),
        "total_click_opportunity": round(total_click_opportunity, 1),
        "pages_with_opportunity": len(opportunities_list),
        "analysis_period_days": days
    }
    
    logger.info(f"CTR analysis complete. Total click opportunity: {total_click_opportunity}")
    
    return {
        "summary": summary,
        "top_performers": top_performers_list,
        "underperformers": underperformers_list,
        "ctr_opportunities": ctr_opportunities,
        "distribution": distribution,
        "metadata": {
            "module": "page_ctr_analysis",
            "version": "1.0",
            "generated_at": datetime.now().isoformat(),
            "min_impressions_threshold": min_impressions,
            "analysis_period_days": days
        }
    }


# Example usage and testing
if __name__ == "__main__":
    # Test with sample data
    sample_data = pd.DataFrame({
        'page': ['/page1', '/page2', '/page3', '/page4', '/page5'] * 30,
        'date': pd.date_range(start='2024-01-01', periods=150, freq='D'),
        'clicks': [100, 50, 200, 30, 150] * 30,
        'impressions': [5000, 2000, 10000, 3000, 8000] * 30,
        'position': [3.2, 8.5, 1.8, 12.3, 4.7] * 30
    })
    
    result = analyze_page_ctr(sample_data, min_impressions=1000)
    
    print("=== Page-Level CTR Analysis Results ===")
    print(f"\nSummary:")
    for key, value in result['summary'].items():
        print(f"  {key}: {value}")
    
    print(f"\nTop Performers: {len(result['top_performers'])}")
    if result['top_performers']:
        print(f"  Best: {result['top_performers'][0]['page']} (CTR ratio: {result['top_performers'][0]['ctr_ratio']})")
    
    print(f"\nUnderperformers: {len(result['underperformers'])}")
    if result['underperformers']:
        print(f"  Worst: {result['underperformers'][0]['page']} (CTR ratio: {result['underperformers'][0]['ctr_ratio']})")
    
    print(f"\nCTR Opportunities: {len(result['ctr_opportunities'])}")
    if result['ctr_opportunities']:
        top_opp = result['ctr_opportunities'][0]
        print(f"  Top: {top_opp['page']} ({top_opp['click_opportunity']} clicks potential)")