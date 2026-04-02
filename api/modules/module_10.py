"""
Module 10: Conversion Opportunity Scanner

Analyzes GA4 conversion data to identify high-traffic, low-conversion pages.
Calculates conversion rates, compares against benchmarks, scores opportunities
based on traffic volume and conversion gap, provides actionable CRO recommendations.

Integrates with:
- Module 1 (GA4 data)
- GSC page analytics data
- Internal benchmark database
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
import logging
from collections import defaultdict

logger = logging.getLogger(__name__)


# Industry conversion rate benchmarks by page type
CONVERSION_BENCHMARKS = {
    "product": {
        "excellent": 0.05,
        "good": 0.03,
        "average": 0.02,
        "poor": 0.01
    },
    "category": {
        "excellent": 0.03,
        "good": 0.02,
        "average": 0.015,
        "poor": 0.008
    },
    "landing": {
        "excellent": 0.08,
        "good": 0.05,
        "average": 0.03,
        "poor": 0.015
    },
    "blog": {
        "excellent": 0.015,
        "good": 0.01,
        "average": 0.005,
        "poor": 0.002
    },
    "home": {
        "excellent": 0.04,
        "good": 0.025,
        "average": 0.015,
        "poor": 0.008
    },
    "default": {
        "excellent": 0.04,
        "good": 0.025,
        "average": 0.015,
        "poor": 0.008
    }
}


def classify_page_type(url: str, page_data: Optional[Dict] = None) -> str:
    """
    Classify page type based on URL patterns and metadata.
    
    Args:
        url: Page URL
        page_data: Optional page metadata from crawl
        
    Returns:
        Page type: product, category, landing, blog, home, default
    """
    url_lower = url.lower()
    
    # Home page
    if url_lower.rstrip('/').endswith(('.com', '.net', '.org', '.io')) or url_lower.endswith('/'):
        return "home"
    
    # Product pages
    product_patterns = ['/product/', '/p/', '/item/', '/buy/', '/shop/', '-p-', '/products/']
    if any(pattern in url_lower for pattern in product_patterns):
        return "product"
    
    # Category/collection pages
    category_patterns = ['/category/', '/collection/', '/c/', '/categories/', '/shop/']
    if any(pattern in url_lower for pattern in category_patterns):
        return "category"
    
    # Blog/content pages
    blog_patterns = ['/blog/', '/article/', '/post/', '/news/', '/guide/', '/tutorial/']
    if any(pattern in url_lower for pattern in blog_patterns):
        return "blog"
    
    # Landing pages (often have shorter URLs or specific patterns)
    landing_patterns = ['/lp/', '/landing/', '/promo/', '/offer/', '/campaign/']
    if any(pattern in url_lower for pattern in landing_patterns):
        return "landing"
    
    # Check metadata if available
    if page_data:
        title = page_data.get('title', '').lower()
        if 'buy' in title or 'shop' in title:
            return "product"
        if 'blog' in title or 'article' in title:
            return "blog"
    
    return "default"


def calculate_conversion_rate(conversions: int, sessions: int) -> float:
    """Calculate conversion rate with zero-division handling."""
    if sessions == 0:
        return 0.0
    return conversions / sessions


def get_benchmark_for_page(page_type: str, metric: str = "average") -> float:
    """
    Get conversion rate benchmark for a page type.
    
    Args:
        page_type: Type of page
        metric: Benchmark level (excellent, good, average, poor)
        
    Returns:
        Benchmark conversion rate
    """
    benchmarks = CONVERSION_BENCHMARKS.get(page_type, CONVERSION_BENCHMARKS["default"])
    return benchmarks.get(metric, benchmarks["average"])


def calculate_opportunity_score(
    traffic: int,
    current_cr: float,
    benchmark_cr: float,
    avg_order_value: Optional[float] = None,
    position: Optional[float] = None
) -> float:
    """
    Calculate opportunity score for a page based on traffic volume and conversion gap.
    
    Score formula:
    - Base score = traffic × (benchmark_cr - current_cr) × 1000
    - Multiplied by AOV factor if available
    - Adjusted by position factor (easier wins at higher positions)
    
    Args:
        traffic: Monthly sessions/visits
        current_cr: Current conversion rate
        benchmark_cr: Benchmark conversion rate
        avg_order_value: Average order/conversion value (optional)
        position: Average search position (optional)
        
    Returns:
        Opportunity score (0-100+)
    """
    if benchmark_cr <= current_cr:
        return 0.0
    
    # Base score: traffic × conversion gap
    conversion_gap = benchmark_cr - current_cr
    base_score = traffic * conversion_gap * 1000
    
    # Apply AOV multiplier (higher value conversions = higher priority)
    if avg_order_value:
        aov_factor = min(avg_order_value / 100, 5.0)  # Cap at 5x multiplier
        base_score *= aov_factor
    
    # Position factor (easier to improve high-ranking pages)
    if position:
        if position <= 5:
            position_factor = 1.5
        elif position <= 10:
            position_factor = 1.2
        elif position <= 20:
            position_factor = 1.0
        else:
            position_factor = 0.8
        base_score *= position_factor
    
    # Normalize to 0-100 scale (with potential for >100 for exceptional opportunities)
    return min(base_score / 100, 150)


def calculate_potential_lift(
    traffic: int,
    current_cr: float,
    benchmark_cr: float,
    avg_order_value: Optional[float] = None
) -> Dict[str, Any]:
    """
    Calculate potential conversion and revenue lift if benchmark is achieved.
    
    Args:
        traffic: Monthly sessions
        current_cr: Current conversion rate
        benchmark_cr: Target benchmark conversion rate
        avg_order_value: Average order value (optional)
        
    Returns:
        Dict with lift calculations
    """
    current_conversions = traffic * current_cr
    potential_conversions = traffic * benchmark_cr
    conversion_lift = potential_conversions - current_conversions
    
    result = {
        "current_conversions": round(current_conversions, 1),
        "potential_conversions": round(potential_conversions, 1),
        "conversion_lift": round(conversion_lift, 1),
        "conversion_lift_pct": round((conversion_lift / max(current_conversions, 1)) * 100, 1)
    }
    
    if avg_order_value:
        current_revenue = current_conversions * avg_order_value
        potential_revenue = potential_conversions * avg_order_value
        revenue_lift = potential_revenue - current_revenue
        
        result.update({
            "current_revenue": round(current_revenue, 2),
            "potential_revenue": round(potential_revenue, 2),
            "revenue_lift": round(revenue_lift, 2),
            "revenue_lift_pct": round((revenue_lift / max(current_revenue, 1)) * 100, 1)
        })
    
    return result


def generate_cro_recommendations(
    page_type: str,
    current_cr: float,
    benchmark_cr: float,
    engagement_metrics: Dict[str, Any],
    traffic_source: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Generate actionable CRO recommendations based on page analysis.
    
    Args:
        page_type: Type of page
        current_cr: Current conversion rate
        benchmark_cr: Benchmark conversion rate
        engagement_metrics: Engagement data (bounce rate, time on page, etc.)
        traffic_source: Primary traffic source (optional)
        
    Returns:
        List of prioritized recommendations
    """
    recommendations = []
    
    # Calculate conversion gap severity
    if benchmark_cr > 0:
        gap_pct = ((benchmark_cr - current_cr) / benchmark_cr) * 100
    else:
        gap_pct = 0
    
    bounce_rate = engagement_metrics.get('bounce_rate', 0)
    avg_session_duration = engagement_metrics.get('avg_session_duration', 0)
    pages_per_session = engagement_metrics.get('pages_per_session', 0)
    
    # High bounce rate recommendations
    if bounce_rate > 70:
        recommendations.append({
            "category": "User Experience",
            "issue": "High Bounce Rate",
            "recommendation": "Improve above-the-fold content and value proposition clarity. Visitors are leaving too quickly.",
            "priority": "high",
            "estimated_impact": "20-30% conversion lift",
            "actions": [
                "Add clear, compelling headline that matches search intent",
                "Place primary CTA above the fold",
                "Reduce page load time (target < 2.5s)",
                "Ensure mobile responsiveness",
                "Add trust signals (testimonials, ratings, security badges)"
            ]
        })
    
    # Low engagement recommendations
    if avg_session_duration < 30 and page_type in ['product', 'landing']:
        recommendations.append({
            "category": "Content Quality",
            "issue": "Low Time on Page",
            "recommendation": "Content is not engaging visitors. Enhance information architecture and persuasive elements.",
            "priority": "high",
            "estimated_impact": "15-25% conversion lift",
            "actions": [
                "Add compelling product/service descriptions",
                "Include high-quality images or videos",
                "Add social proof (reviews, testimonials, case studies)",
                "Use bullet points and scannable formatting",
                "Address common objections proactively"
            ]
        })
    
    # Low pages per session (for non-landing pages)
    if pages_per_session < 1.5 and page_type not in ['product', 'landing']:
        recommendations.append({
            "category": "Navigation",
            "issue": "Low Pages Per Session",
            "recommendation": "Visitors aren't exploring further. Improve internal linking and navigation paths.",
            "priority": "medium",
            "estimated_impact": "10-15% conversion lift",
            "actions": [
                "Add relevant internal links to related content/products",
                "Implement 'You might also like' recommendations",
                "Add clear next-step CTAs",
                "Improve main navigation visibility",
                "Use breadcrumbs for better site structure understanding"
            ]
        })
    
    # Page-type specific recommendations
    if page_type == "product":
        if current_cr < get_benchmark_for_page("product", "poor"):
            recommendations.append({
                "category": "Product Page Optimization",
                "issue": "Below-Average Product Conversion",
                "recommendation": "Product page is underperforming. Optimize for conversion best practices.",
                "priority": "high",
                "estimated_impact": "30-50% conversion lift",
                "actions": [
                    "Add multiple high-quality product images with zoom",
                    "Include detailed specifications and dimensions",
                    "Add customer reviews and ratings",
                    "Show stock status and urgency indicators",
                    "Offer multiple payment options",
                    "Add clear return/shipping policies",
                    "Implement size guides or product finders",
                    "Show related products and upsells"
                ]
            })
    
    elif page_type == "landing":
        if current_cr < get_benchmark_for_page("landing", "average"):
            recommendations.append({
                "category": "Landing Page Optimization",
                "issue": "Low Landing Page Conversion",
                "recommendation": "Landing page not effectively converting traffic. Optimize conversion funnel.",
                "priority": "critical",
                "estimated_impact": "40-60% conversion lift",
                "actions": [
                    "Simplify form fields (reduce friction)",
                    "Add compelling headline matching ad/link copy",
                    "Use directional cues pointing to CTA",
                    "Remove navigation to reduce exits",
                    "Add urgency elements (limited time offers)",
                    "A/B test different CTA button colors/text",
                    "Add live chat for immediate support",
                    "Show guarantees and risk-reversal"
                ]
            })
    
    elif page_type == "blog":
        if current_cr < get_benchmark_for_page("blog", "average"):
            recommendations.append({
                "category": "Content Monetization",
                "issue": "Low Content Conversion",
                "recommendation": "Blog content not effectively leading to conversions. Improve conversion paths.",
                "priority": "medium",
                "estimated_impact": "15-25% conversion lift",
                "actions": [
                    "Add contextual CTAs within content",
                    "Create content upgrades (downloadable resources)",
                    "Use exit-intent popups for email capture",
                    "Add 'Start here' guides for new visitors",
                    "Link to product/service pages naturally",
                    "Add end-of-post CTAs with clear value proposition",
                    "Implement sticky sidebar CTAs",
                    "Create content-to-conversion funnels"
                ]
            })
    
    # Traffic source specific recommendations
    if traffic_source == "organic":
        recommendations.append({
            "category": "Search Intent Alignment",
            "issue": "Organic Traffic Optimization",
            "recommendation": "Ensure page content matches search intent for converting organic visitors.",
            "priority": "high",
            "estimated_impact": "20-30% conversion lift",
            "actions": [
                "Review top keywords and ensure content alignment",
                "Match headline to primary search query",
                "Answer visitor questions immediately",
                "Add FAQ section for common queries",
                "Optimize meta description as 'ad copy'",
                "Add schema markup for rich snippets",
                "Ensure page load speed is optimized"
            ]
        })
    
    # General large gap recommendation
    if gap_pct > 50:
        recommendations.append({
            "category": "Comprehensive Audit",
            "issue": "Significant Conversion Gap",
            "recommendation": "Large gap between performance and benchmark suggests multiple optimization opportunities.",
            "priority": "critical",
            "estimated_impact": "50-100% conversion lift potential",
            "actions": [
                "Conduct comprehensive CRO audit",
                "Implement user testing or session recordings",
                "Analyze heatmaps and click patterns",
                "Survey visitors to understand barriers",
                "Review complete conversion funnel",
                "Consider professional CRO consultant",
                "Implement systematic A/B testing program"
            ]
        })
    
    # Sort by priority
    priority_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    recommendations.sort(key=lambda x: priority_order.get(x["priority"], 3))
    
    return recommendations


def analyze_conversion_funnel(
    page_data: pd.DataFrame,
    conversion_events: List[Dict[str, Any]],
    funnel_steps: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Analyze conversion funnel to identify drop-off points.
    
    Args:
        page_data: Page-level data with session counts
        conversion_events: List of conversion event data
        funnel_steps: Optional list of funnel step page patterns
        
    Returns:
        Funnel analysis with drop-off rates
    """
    if not funnel_steps:
        funnel_steps = ["landing", "product", "cart", "checkout", "confirmation"]
    
    funnel_data = []
    
    for i, step in enumerate(funnel_steps):
        step_pages = page_data[page_data['url'].str.contains(step, case=False, na=False)]
        
        if not step_pages.empty:
            total_sessions = step_pages['sessions'].sum()
            
            # For final step, count conversions
            if i == len(funnel_steps) - 1:
                conversions = sum(e['count'] for e in conversion_events if step in e.get('page', ''))
            else:
                conversions = 0
            
            funnel_data.append({
                "step": step,
                "step_number": i + 1,
                "sessions": int(total_sessions),
                "conversions": int(conversions)
            })
    
    # Calculate drop-off rates
    for i in range(len(funnel_data) - 1):
        current_sessions = funnel_data[i]['sessions']
        next_sessions = funnel_data[i + 1]['sessions']
        
        if current_sessions > 0:
            drop_off_rate = ((current_sessions - next_sessions) / current_sessions) * 100
            funnel_data[i]['drop_off_rate'] = round(drop_off_rate, 1)
            funnel_data[i]['continuation_rate'] = round(100 - drop_off_rate, 1)
    
    # Overall funnel conversion rate
    if funnel_data and funnel_data[0]['sessions'] > 0:
        overall_cr = (funnel_data[-1]['conversions'] / funnel_data[0]['sessions']) * 100
    else:
        overall_cr = 0
    
    return {
        "funnel_steps": funnel_data,
        "overall_conversion_rate": round(overall_cr, 2),
        "biggest_drop_off_step": max(funnel_data[:-1], key=lambda x: x.get('drop_off_rate', 0)) if len(funnel_data) > 1 else None
    }


def segment_opportunities(
    opportunities: List[Dict[str, Any]],
    segment_by: str = "page_type"
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Segment conversion opportunities by specified dimension.
    
    Args:
        opportunities: List of opportunity dicts
        segment_by: Dimension to segment by (page_type, traffic_tier, etc.)
        
    Returns:
        Dict of segmented opportunities
    """
    segments = defaultdict(list)
    
    for opp in opportunities:
        key = opp.get(segment_by, "unknown")
        segments[key].append(opp)
    
    # Sort each segment by opportunity score
    for key in segments:
        segments[key].sort(key=lambda x: x.get('opportunity_score', 0), reverse=True)
    
    return dict(segments)


def process(
    ga4_data: Dict[str, Any],
    gsc_data: Dict[str, Any],
    page_data: Optional[Dict[str, Any]] = None,
    config: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Main processing function for Module 10: Conversion Opportunity Scanner.
    
    Analyzes GA4 conversion data to identify high-traffic, low-conversion pages,
    calculates opportunity scores, and generates actionable CRO recommendations.
    
    Args:
        ga4_data: GA4 analytics data including landing pages and conversions
        gsc_data: Google Search Console data for position context
        page_data: Optional crawl data for page metadata
        config: Optional configuration parameters
        
    Returns:
        Dict containing conversion opportunity analysis
    """
    logger.info("Starting Module 10: Conversion Opportunity Scanner")
    
    try:
        # Extract configuration
        config = config or {}
        min_sessions = config.get('min_sessions', 100)  # Minimum sessions to analyze
        avg_order_value = config.get('avg_order_value', None)  # Optional AOV
        target_benchmark = config.get('target_benchmark', 'good')  # Target benchmark level
        
        # Extract GA4 landing page data
        landing_pages = ga4_data.get('landing_pages', [])
        if not landing_pages:
            logger.warning("No GA4 landing page data available")
            return {
                "error": "No landing page data available",
                "opportunities": [],
                "summary": {}
            }
        
        # Convert to DataFrame
        df = pd.DataFrame(landing_pages)
        
        # Filter to pages with minimum traffic
        df = df[df['sessions'] >= min_sessions].copy()
        
        if df.empty:
            logger.warning(f"No pages with >= {min_sessions} sessions")
            return {
                "error": f"No pages with sufficient traffic (>= {min_sessions} sessions)",
                "opportunities": [],
                "summary": {}
            }
        
        # Classify page types
        df['page_type'] = df['page'].apply(classify_page_type)
        
        # Calculate conversion rates
        df['conversion_rate'] = df.apply(
            lambda row: calculate_conversion_rate(
                row.get('conversions', 0),
                row.get('sessions', 0)
            ),
            axis=1
        )
        
        # Get benchmark conversion rates
        df['benchmark_cr'] = df.apply(
            lambda row: get_benchmark_for_page(row['page_type'], target_benchmark),
            axis=1
        )
        
        # Calculate conversion gap
        df['conversion_gap'] = df['benchmark_cr'] - df['conversion_rate']
        
        # Filter to pages below benchmark
        opportunities_df = df[df['conversion_gap'] > 0].copy()
        
        # Merge with GSC position data if available
        gsc_pages = gsc_data.get('pages', [])
        if gsc_pages:
            gsc_df = pd.DataFrame(gsc_pages)
            if 'page' in gsc_df.columns and 'position' in gsc_df.columns:
                opportunities_df = opportunities_df.merge(
                    gsc_df[['page', 'position']],
                    on='page',
                    how='left'
                )
        
        # Calculate opportunity scores
        opportunities_df['opportunity_score'] = opportunities_df.apply(
            lambda row: calculate_opportunity_score(
                row['sessions'],
                row['conversion_rate'],
                row['benchmark_cr'],
                avg_order_value,
                row.get('position', None)
            ),
            axis=1
        )
        
        # Calculate potential lift
        lift_data = opportunities_df.apply(
            lambda row: pd.Series(calculate_potential_lift(
                row['sessions'],
                row['conversion_rate'],
                row['benchmark_cr'],
                avg_order_value
            )),
            axis=1
        )
        opportunities_df = pd.concat([opportunities_df, lift_data], axis=1)
        
        # Sort by opportunity score
        opportunities_df = opportunities_df.sort_values('opportunity_score', ascending=False)
        
        # Generate recommendations for top opportunities
        top_opportunities = []
        
        for idx, row in opportunities_df.head(20).iterrows():
            engagement_metrics = {
                'bounce_rate': row.get('bounce_rate', 0) * 100 if row.get('bounce_rate', 0) <= 1 else row.get('bounce_rate', 0),
                'avg_session_duration': row.get('avg_session_duration', 0),
                'pages_per_session': row.get('pages_per_session', 1)
            }
            
            recommendations = generate_cro_recommendations(
                row['page_type'],
                row['conversion_rate'],
                row['benchmark_cr'],
                engagement_metrics,
                row.get('traffic_source', None)
            )
            
            opportunity = {
                "page": row['page'],
                "page_type": row['page_type'],
                "monthly_sessions": int(row['sessions']),
                "current_conversion_rate": round(row['conversion_rate'] * 100, 2),
                "benchmark_conversion_rate": round(row['benchmark_cr'] * 100, 2),
                "conversion_gap_pct": round((row['conversion_gap'] / row['benchmark_cr']) * 100, 1),
                "opportunity_score": round(row['opportunity_score'], 1),
                "current_conversions": row['current_conversions'],
                "potential_conversions": row['potential_conversions'],
                "conversion_lift": row['conversion_lift'],
                "conversion_lift_pct": row['conversion_lift_pct'],
                "engagement_metrics": {
                    "bounce_rate": round(engagement_metrics['bounce_rate'], 1),
                    "avg_session_duration": round(engagement_metrics['avg_session_duration'], 1),
                    "pages_per_session": round(engagement_metrics['pages_per_session'], 2)
                },
                "recommendations": recommendations[:3]  # Top 3 recommendations
            }
            
            # Add revenue data if AOV provided
            if avg_order_value and 'revenue_lift' in row:
                opportunity['current_revenue'] = row['current_revenue']
                opportunity['potential_revenue'] = row['potential_revenue']
                opportunity['revenue_lift'] = row['revenue_lift']
                opportunity['revenue_lift_pct'] = row['revenue_lift_pct']
            
            # Add position if available
            if 'position' in row and pd.notna(row['position']):
                opportunity['avg_position'] = round(row['position'], 1)
            
            top_opportunities.append(opportunity)
        
        # Calculate summary statistics
        total_sessions = int(opportunities_df['sessions'].sum())
        total_current_conversions = opportunities_df['current_conversions'].sum()
        total_potential_conversions = opportunities_df['potential_conversions'].sum()
        total_conversion_lift = total_potential_conversions - total_current_conversions
        
        summary = {
            "total_pages_analyzed": len(df),
            "pages_below_benchmark": len(opportunities_df),
            "total_monthly_sessions": total_sessions,
            "total_current_conversions": round(total_current_conversions, 1),
            "total_potential_conversions": round(total_potential_conversions, 1),
            "total_conversion_lift": round(total_conversion_lift, 1),
            "total_conversion_lift_pct": round((total_conversion_lift / max(total_current_conversions, 1)) * 100, 1),
            "avg_current_conversion_rate": round(df['conversion_rate'].mean() * 100, 2),
            "avg_benchmark_conversion_rate": round(df['benchmark_cr'].mean() * 100, 2)
        }
        
        # Add revenue summary if AOV provided
        if avg_order_value:
            total_current_revenue = total_current_conversions * avg_order_value
            total_potential_revenue = total_potential_conversions * avg_order_value
            total_revenue_lift = total_potential_revenue - total_current_revenue
            
            summary.update({
                "total_current_revenue": round(total_current_revenue, 2),
                "total_potential_revenue": round(total_potential_revenue, 2),
                "total_revenue_lift": round(total_revenue_lift, 2),
                "total_revenue_lift_pct": round((total_revenue_lift / max(total_current_revenue, 1)) * 100, 1)
            })
        
        # Segment opportunities by page type
        page_type_breakdown = opportunities_df.groupby('page_type').agg({
            'sessions': 'sum',
            'conversion_lift': 'sum',
            'opportunity_score': 'mean'
        }).to_dict('index')
        
        for page_type, stats in page_type_breakdown.items():
            page_type_breakdown[page_type] = {
                'total_sessions': int(stats['sessions']),
                'total_conversion_lift': round(stats['conversion_lift'], 1),
                'avg_opportunity_score': round(stats['opportunity_score'], 1),
                'page_count': len(opportunities_df[opportunities_df['page_type'] == page_type])
            }
        
        # Identify quick wins (high score, low complexity)
        quick_wins = []
        for opp in top_opportunities[:10]:
            # Quick win criteria: high opportunity score and simpler fixes
            if opp['opportunity_score'] > 50:
                primary_issue = opp['recommendations'][0]['issue'] if opp['recommendations'] else ""
                if any(term in primary_issue.lower() for term in ['bounce', 'time', 'cta', 'headline']):
                    quick_wins.append({
                        "page": opp['page'],
                        "opportunity_score": opp['opportunity_score'],
                        "conversion_lift": opp['conversion_lift'],
                        "primary_fix": opp['recommendations'][0]['recommendation'] if opp['recommendations'] else "Optimize conversion elements"
                    })
        
        # Overall insights
        insights = []
        
        # Insight: Overall conversion performance
        avg_gap = opportunities_df['conversion_gap'].mean()
        if avg_gap > 0.01:
            insights.append({
                "type": "overall_performance",
                "severity": "high" if avg_gap > 0.02 else "medium",
                "insight": f"Site-wide conversion rates are {round(avg_gap * 100, 1)}% below industry benchmarks on average",
                "recommendation": "Systematic CRO program needed across multiple page types"
            })
        
        # Insight: Page type with biggest opportunity
        if page_type_breakdown:
            biggest_opportunity_type = max(
                page_type_breakdown.items(),
                key=lambda x: x[1]['total_conversion_lift']
            )
            insights.append({
                "type": "page_type_priority",
                "severity": "high",
                "insight": f"{biggest_opportunity_type[0].title()} pages have the largest conversion opportunity",
                "recommendation": f"Focus initial CRO efforts on {biggest_opportunity_type[0]} pages for maximum impact"
            })
        
        # Insight: High bounce rates
        high_bounce_pages = opportunities_df[
            opportunities_df['bounce_rate'] > 0.7
        ] if 'bounce_rate' in opportunities_df.columns else pd.DataFrame()
        
        if not high_bounce_pages.empty:
            insights.append({
                "type": "engagement_issue",
                "severity": "high",
                "insight": f"{len(high_bounce_pages)} high-traffic pages have bounce rates above 70%",
                "recommendation": "Improve above-the-fold content and value proposition clarity"
            })
        
        # Insight: Quick wins available
        if quick_wins:
            total_quick_win_lift = sum(qw['conversion_lift'] for qw in quick_wins)
            insights.append({
                "type": "quick_wins",
                "severity": "medium",
                "insight": f"{len(quick_wins)} pages have simple optimization opportunities",
                "recommendation": f"Address these quick wins first for {round(total_quick_win_lift, 0)} potential additional conversions/month"
            })
        
        result = {
            "summary": summary,
            "opportunities": top_opportunities,
            "page_type_breakdown": page_type_breakdown,
            "quick_wins": quick_wins[:5],  # Top 5 quick wins
            "insights": insights,
            "analysis_params": {
                "min_sessions_threshold": min_sessions,
                "target_benchmark": target_benchmark,
                "avg_order_value": avg_order_value,
                "pages_analyzed": len(df),
                "date_range": ga4_data.get('date_range', 'unknown')
            }
        }
        
        logger.info(f"Module 10 complete: {len(top_opportunities)} opportunities identified")
        return result
        
    except Exception as e:
        logger.error(f"Error in Module 10: {str(e)}", exc_info=True)
        return {
            "error": str(e),
            "opportunities": [],
            "summary": {}
        }
