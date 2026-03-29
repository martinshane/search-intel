"""
Module 5: The Gameplan — Synthesize all prior modules into a prioritized action list.

This module takes the outputs from Modules 1-4 and generates a comprehensive,
prioritized gameplan with critical fixes, quick wins, strategic plays, and
structural improvements. Each action includes specific instructions, estimated
impact, effort level, and dependencies.
"""

import logging
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
from enum import Enum

logger = logging.getLogger(__name__)


class EffortLevel(str, Enum):
    """Effort level for an action item."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class Priority(str, Enum):
    """Priority category for actions."""
    CRITICAL = "critical"
    QUICK_WIN = "quick_win"
    STRATEGIC = "strategic"
    STRUCTURAL = "structural"


@dataclass
class ActionItem:
    """Represents a single action item in the gameplan."""
    action: str
    page_or_keyword: str
    what_to_do: str
    impact_monthly_clicks: int
    effort: EffortLevel
    priority: Priority
    dependencies: List[str]
    estimated_timeframe: str  # e.g., "this week", "this month", "Q1 2026"
    metrics: Dict[str, Any]  # Supporting data for the recommendation


@dataclass
class GameplanOutput:
    """Complete gameplan output structure."""
    critical: List[ActionItem]
    quick_wins: List[ActionItem]
    strategic: List[ActionItem]
    structural: List[ActionItem]
    total_estimated_monthly_click_recovery: int
    total_estimated_monthly_click_growth: int
    narrative: str
    summary_stats: Dict[str, Any]


def generate_gameplan(
    health: Dict[str, Any],
    triage: Dict[str, Any],
    serp: Optional[Dict[str, Any]] = None,
    content: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Synthesize all prior modules into a prioritized action list.
    
    Args:
        health: Output from Module 1 (Health & Trajectory)
        triage: Output from Module 2 (Page-Level Triage)
        serp: Output from Module 3 (SERP Landscape Analysis) - optional in MVP
        content: Output from Module 4 (Content Intelligence) - optional in MVP
    
    Returns:
        dict: Structured gameplan with categorized actions, impact estimates, and narrative
    
    The gameplan is organized into four priority tiers:
    
    1. Critical fixes (do this week):
       - Pages in "critical" decay bucket with > 100 clicks/month
       - CTR anomalies on high-impression keywords (title rewrites)
       - Cannibalization causing both pages to underperform
    
    2. Quick wins (do this month):
       - Striking distance keywords needing minor content updates
       - SERP feature optimization (add FAQ schema for PAA keywords)
       - Internal link additions to boost decaying pages
    
    3. Strategic plays (this quarter):
       - Content gaps worth filling (new pages to create)
       - Consolidation projects (merge cannibalizing pages)
       - Content refreshes for "urgent update" quadrant pages
    
    4. Structural improvements (ongoing):
       - Internal link architecture changes
       - Seasonal content calendar based on identified cycles
       - Competitor monitoring priorities
    """
    logger.info("Starting gameplan generation")
    
    critical_actions = []
    quick_win_actions = []
    strategic_actions = []
    structural_actions = []
    
    total_recovery = 0
    total_growth = 0
    
    # Extract critical pages from triage
    critical_pages = [
        p for p in triage.get("pages", [])
        if p.get("bucket") == "critical" and p.get("current_monthly_clicks", 0) > 100
    ]
    
    # Process critical fixes
    critical_actions.extend(_generate_critical_fixes(critical_pages, triage))
    total_recovery += sum(
        action.impact_monthly_clicks 
        for action in critical_actions
    )
    
    # Process quick wins
    quick_win_actions.extend(_generate_quick_wins(triage, health, serp, content))
    total_growth += sum(
        action.impact_monthly_clicks 
        for action in quick_win_actions
    )
    
    # Process strategic plays
    strategic_actions.extend(_generate_strategic_plays(triage, health, content))
    total_growth += sum(
        action.impact_monthly_clicks 
        for action in strategic_actions
    )
    
    # Process structural improvements
    structural_actions.extend(_generate_structural_improvements(health, triage))
    
    # Generate narrative summary
    narrative = _generate_narrative(
        health=health,
        triage=triage,
        critical_count=len(critical_actions),
        quick_win_count=len(quick_win_actions),
        strategic_count=len(strategic_actions),
        total_recovery=total_recovery,
        total_growth=total_growth
    )
    
    # Build summary statistics
    summary_stats = {
        "total_actions": len(critical_actions) + len(quick_win_actions) + 
                        len(strategic_actions) + len(structural_actions),
        "critical_actions": len(critical_actions),
        "quick_win_actions": len(quick_win_actions),
        "strategic_actions": len(strategic_actions),
        "structural_actions": len(structural_actions),
        "total_pages_affected": len(set(
            action.page_or_keyword 
            for action in critical_actions + quick_win_actions + strategic_actions
            if action.page_or_keyword
        )),
        "avg_effort_critical": _calculate_avg_effort(critical_actions),
        "avg_effort_quick_wins": _calculate_avg_effort(quick_win_actions)
    }
    
    gameplan = GameplanOutput(
        critical=[asdict(a) for a in critical_actions],
        quick_wins=[asdict(a) for a in quick_win_actions],
        strategic=[asdict(a) for a in strategic_actions],
        structural=[asdict(a) for a in structural_actions],
        total_estimated_monthly_click_recovery=total_recovery,
        total_estimated_monthly_click_growth=total_growth,
        narrative=narrative,
        summary_stats=summary_stats
    )
    
    logger.info(
        f"Gameplan generated: {len(critical_actions)} critical, "
        f"{len(quick_win_actions)} quick wins, {len(strategic_actions)} strategic, "
        f"{len(structural_actions)} structural"
    )
    
    return asdict(gameplan)


def _generate_critical_fixes(
    critical_pages: List[Dict[str, Any]],
    triage: Dict[str, Any]
) -> List[ActionItem]:
    """Generate critical fix action items."""
    actions = []
    
    # Sort by priority score (already calculated in triage)
    sorted_pages = sorted(
        critical_pages,
        key=lambda p: p.get("priority_score", 0),
        reverse=True
    )
    
    for page in sorted_pages[:10]:  # Limit to top 10 most critical
        url = page.get("url", "")
        current_clicks = page.get("current_monthly_clicks", 0)
        trend_slope = page.get("trend_slope", 0)
        ctr_anomaly = page.get("ctr_anomaly", False)
        engagement_flag = page.get("engagement_flag")
        
        # Estimate recoverable clicks (if decay stops)
        # Assume recovery to stable state = current clicks + 30 days of projected loss
        projected_monthly_loss = abs(trend_slope * 30)
        recoverable_clicks = int(projected_monthly_loss)
        
        # Determine primary issue and action
        if ctr_anomaly:
            action = ActionItem(
                action=f"Fix CTR anomaly on {url}",
                page_or_keyword=url,
                what_to_do=(
                    f"Rewrite title and meta description. Current CTR is "
                    f"{page.get('ctr_actual', 0):.1%} vs expected "
                    f"{page.get('ctr_expected', 0):.1%}. Focus on making the "
                    f"snippet more compelling and accurately reflecting page content."
                ),
                impact_monthly_clicks=recoverable_clicks,
                effort=EffortLevel.LOW,
                priority=Priority.CRITICAL,
                dependencies=[],
                estimated_timeframe="this week",
                metrics={
                    "current_ctr": page.get("ctr_actual"),
                    "expected_ctr": page.get("ctr_expected"),
                    "current_position": page.get("average_position"),
                    "monthly_impressions": page.get("monthly_impressions")
                }
            )
        elif engagement_flag == "low_engagement":
            action = ActionItem(
                action=f"Fix content mismatch on {url}",
                page_or_keyword=url,
                what_to_do=(
                    f"This page has high search traffic but poor engagement "
                    f"(high bounce rate or low session duration). Review search "
                    f"queries driving traffic and ensure content matches user intent. "
                    f"May need content rewrite or better internal linking to relevant pages."
                ),
                impact_monthly_clicks=recoverable_clicks,
                effort=EffortLevel.MEDIUM,
                priority=Priority.CRITICAL,
                dependencies=[],
                estimated_timeframe="this week",
                metrics={
                    "current_monthly_clicks": current_clicks,
                    "decay_rate": trend_slope,
                    "engagement_flag": engagement_flag
                }
            )
        else:
            action = ActionItem(
                action=f"Halt critical decay on {url}",
                page_or_keyword=url,
                what_to_do=(
                    f"This page is losing {int(abs(trend_slope))} clicks/day. "
                    f"Investigate ranking drops: check for technical issues "
                    f"(indexing, site speed), review recent content changes, "
                    f"analyze competitor movement. May need content refresh, "
                    f"more internal links, or addressing quality issues."
                ),
                impact_monthly_clicks=recoverable_clicks,
                effort=EffortLevel.HIGH,
                priority=Priority.CRITICAL,
                dependencies=[],
                estimated_timeframe="this week",
                metrics={
                    "current_monthly_clicks": current_clicks,
                    "decay_rate": trend_slope,
                    "days_until_page1_loss": page.get("days_until_page1_loss")
                }
            )
        
        actions.append(action)
    
    # Add CTR anomaly fixes for high-impression keywords
    for page in triage.get("pages", []):
        if (page.get("ctr_anomaly") and 
            page.get("monthly_impressions", 0) > 1000 and
            page not in critical_pages):  # Not already in critical
            
            potential_clicks = int(
                page.get("monthly_impressions", 0) * 
                (page.get("ctr_expected", 0) - page.get("ctr_actual", 0))
            )
            
            if potential_clicks > 50:  # Meaningful impact
                action = ActionItem(
                    action=f"Fix high-impression CTR anomaly: {page.get('url')}",
                    page_or_keyword=page.get("url", ""),
                    what_to_do=(
                        f"High impression volume ({page.get('monthly_impressions'):,}) "
                        f"but underperforming CTR. Rewrite title and description to be "
                        f"more compelling. Current CTR: {page.get('ctr_actual', 0):.1%}, "
                        f"Expected: {page.get('ctr_expected', 0):.1%}."
                    ),
                    impact_monthly_clicks=potential_clicks,
                    effort=EffortLevel.LOW,
                    priority=Priority.CRITICAL,
                    dependencies=[],
                    estimated_timeframe="this week",
                    metrics={
                        "monthly_impressions": page.get("monthly_impressions"),
                        "current_ctr": page.get("ctr_actual"),
                        "expected_ctr": page.get("ctr_expected"),
                        "potential_click_gain": potential_clicks
                    }
                )
                actions.append(action)
    
    return actions


def _generate_quick_wins(
    triage: Dict[str, Any],
    health: Dict[str, Any],
    serp: Optional[Dict[str, Any]],
    content: Optional[Dict[str, Any]]
) -> List[ActionItem]:
    """Generate quick win action items."""
    actions = []
    
    # Striking distance opportunities (from content module if available)
    if content and "striking_distance" in content:
        for opportunity in content["striking_distance"][:15]:  # Top 15
            estimated_gain = opportunity.get("estimated_click_gain_if_top5", 0)
            if estimated_gain > 20:  # Meaningful threshold
                action = ActionItem(
                    action=f"Boost striking distance keyword: {opportunity.get('query')}",
                    page_or_keyword=opportunity.get("query", ""),
                    what_to_do=(
                        f"Currently ranking at position {opportunity.get('current_position', 0):.1f}. "
                        f"Add 300-500 words of depth to {opportunity.get('landing_page')}. "
                        f"Focus on answering related questions around this {opportunity.get('intent')} query. "
                        f"Add internal links from related pages."
                    ),
                    impact_monthly_clicks=estimated_gain,
                    effort=EffortLevel.LOW,
                    priority=Priority.QUICK_WIN,
                    dependencies=[],
                    estimated_timeframe="this month",
                    metrics={
                        "current_position": opportunity.get("current_position"),
                        "monthly_impressions": opportunity.get("impressions"),
                        "intent": opportunity.get("intent"),
                        "landing_page": opportunity.get("landing_page")
                    }
                )
                actions.append(action)
    
    # Pages with declining but not yet critical status
    decaying_pages = [
        p for p in triage.get("pages", [])
        if p.get("bucket") == "decaying" and p.get("current_monthly_clicks", 0) > 50
    ]
    
    for page in sorted(decaying_pages, key=lambda p: p.get("priority_score", 0), reverse=True)[:10]:
        # Estimate impact of stopping decay
        potential_clicks = int(abs(page.get("trend_slope", 0)) * 30)
        
        action = ActionItem(
            action=f"Stabilize decaying page: {page.get('url')}",
            page_or_keyword=page.get("url", ""),
            what_to_do=(
                f"Add 5-10 internal links from high-authority pages to this URL. "
                f"Refresh content with current information (2026 data, updated examples). "
                f"Ensure page is mobile-friendly and loads quickly."
            ),
            impact_monthly_clicks=potential_clicks,
            effort=EffortLevel.LOW,
            priority=Priority.QUICK_WIN,
            dependencies=[],
            estimated_timeframe="this month",
            metrics={
                "current_monthly_clicks": page.get("current_monthly_clicks"),
                "trend_slope": page.get("trend_slope"),
                "bucket": page.get("bucket")
            }
        )
        actions.append(action)
    
    # SERP feature opportunities (if SERP data available)
    if serp and "feature_opportunities" in serp:
        for opp in serp["feature_opportunities"][:5]:  # Top 5 opportunities
            if opp.get("estimated_click_gain", 0) > 50:
                feature_type = opp.get("feature", "")
                action = ActionItem(
                    action=f"Capture {feature_type} for: {opp.get('keyword')}",
                    page_or_keyword=opp.get("keyword", ""),
                    what_to_do=_get_feature_instructions(feature_type, opp),
                    impact_monthly_clicks=opp.get("estimated_click_gain", 0),
                    effort=EffortLevel.LOW,
                    priority=Priority.QUICK_WIN,
                    dependencies=[],
                    estimated_timeframe="this month",
                    metrics={
                        "feature_type": feature_type,
                        "current_holder": opp.get("current_holder"),
                        "difficulty": opp.get("difficulty")
                    }
                )
                actions.append(action)
    
    return actions


def _generate_strategic_plays(
    triage: Dict[str, Any],
    health: Dict[str, Any],
    content: Optional[Dict[str, Any]]
) -> List[ActionItem]:
    """Generate strategic play action items."""
    actions = []
    
    # Cannibalization fixes (from content module)
    if content and "cannibalization_clusters" in content:
        for cluster in content["cannibalization_clusters"]:
            if cluster.get("total_impressions_affected", 0) > 500:
                action = ActionItem(
                    action=f"Resolve cannibalization: {cluster.get('query_group')}",
                    page_or_keyword=cluster.get("query_group", ""),
                    what_to_do=(
                        f"Recommendation: {cluster.get('recommendation')}. "
                        f"Pages involved: {', '.join(cluster.get('pages', []))}. "
                        f"Keep: {cluster.get('keep_page')}. "
                        f"Either consolidate content or differentiate intent clearly."
                    ),
                    impact_monthly_clicks=int(cluster.get("total_impressions_affected", 0) * 0.05),
                    effort=EffortLevel.HIGH,
                    priority=Priority.STRATEGIC,
                    dependencies=[],
                    estimated_timeframe="this quarter",
                    metrics={
                        "pages_affected": cluster.get("pages", []),
                        "shared_queries": cluster.get("shared_queries"),
                        "impressions_affected": cluster.get("total_impressions_affected")
                    }
                )
                actions.append(action)
    
    # Content refreshes for urgent update quadrant
    if content and "update_priority_matrix" in content:
        urgent_updates = content["update_priority_matrix"].get("urgent_update", [])
        for page in urgent_updates[:5]:  # Top 5
            action = ActionItem(
                action=f"Urgent content refresh: {page.get('url')}",
                page_or_keyword=page.get("url", ""),
                what_to_do=(
                    f"This older page is decaying. Full content refresh needed: "
                    f"update statistics, replace outdated examples, add new sections "
                    f"covering recent developments. Consider updating publish date after refresh."
                ),
                impact_monthly_clicks=page.get("estimated_recovery", 0),
                effort=EffortLevel.HIGH,
                priority=Priority.STRATEGIC,
                dependencies=[],
                estimated_timeframe="this quarter",
                metrics={
                    "page_age": page.get("age_months"),
                    "current_clicks": page.get("current_monthly_clicks"),
                    "decay_rate": page.get("trend_slope")
                }
            )
            actions.append(action)
    
    # Thin content expansion
    if content and "thin_content" in content:
        for page in content["thin_content"][:5]:
            if page.get("monthly_impressions", 0) > 500:
                action = ActionItem(
                    action=f"Expand thin content: {page.get('url')}",
                    page_or_keyword=page.get("url", ""),
                    what_to_do=(
                        f"Current word count: {page.get('word_count')}. "
                        f"Expand to at least 1,200 words. Add: examples, case studies, "
                        f"FAQ section, comparison tables, visual content. "
                        f"Focus on comprehensiveness over keyword density."
                    ),
                    impact_monthly_clicks=int(page.get("monthly_impressions", 0) * 0.03),
                    effort=EffortLevel.MEDIUM,
                    priority=Priority.STRATEGIC,
                    dependencies=[],
                    estimated_timeframe="this quarter",
                    metrics={
                        "word_count": page.get("word_count"),
                        "monthly_impressions": page.get("monthly_impressions"),
                        "bounce_rate": page.get("bounce_rate")
                    }
                )
                actions.append(action)
    
    return actions


def _generate_structural_improvements(
    health: Dict[str, Any],
    triage: Dict[str, Any]
) -> List[ActionItem]:
    """Generate structural improvement action items."""
    actions = []
    
    # Seasonality-based content calendar
    if health.get("seasonality", {}).get("monthly_cycle"):
        action = ActionItem(
            action="Implement seasonal content calendar",
            page_or_keyword="site-wide",
            what_to_do=(
                f"Traffic pattern shows {health['seasonality'].get('cycle_description')}. "
                f"Plan content publication and promotion campaigns around this cycle. "
                f"Best performing day: {health['seasonality'].get('best_day')}. "
                f"Schedule high-priority launches accordingly."
            ),
            impact_monthly_clicks=0,  # Indirect impact
            effort=EffortLevel.LOW,
            priority=Priority.STRUCTURAL,
            dependencies=[],
            estimated_timeframe="ongoing",
            metrics={
                "best_day": health['seasonality'].get('best_day'),
                "worst_day": health['seasonality'].get('worst_day'),
                "cycle_description": health['seasonality'].get('cycle_description')
            }
        )
        actions.append(action)
    
    # Monitoring and alerting
    action = ActionItem(
        action="Set up traffic monitoring alerts",
        page_or_keyword="site-wide",
        what_to_do=(
            "Based on historical volatility, set up alerts for: "
            "1) Daily traffic drops > 15% (may indicate technical issues or penalties), "
            "2) Individual page traffic drops > 30% week-over-week, "
            "3) CTR drops on top keywords. "
            "Use GSC API to pull daily data and compare to 7-day and 28-day averages."
        ),
        impact_monthly_clicks=0,  # Defensive
        effort=EffortLevel.MEDIUM,
        priority=Priority.STRUCTURAL,
        dependencies=[],
        estimated_timeframe="this month",
        metrics={
            "change_points_detected": len(health.get("change_points", [])),
            "historical_volatility": health.get("trend_slope_pct_per_month")
        }
    )
    actions.append(action)
    
    # Regular content audits
    total_pages = triage.get("summary", {}).get("total_pages_analyzed", 0)
    if total_pages > 50:
        action = ActionItem(
            action="Establish quarterly content audit process",
            page_or_keyword="site-wide",
            what