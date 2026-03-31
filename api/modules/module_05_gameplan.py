"""
Module 05: The Gameplan
Synthesizes all prior modules into a prioritized action list with narrative generation.
Handles edge cases where certain analysis sections have no actionable items.
"""

import logging
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class EffortLevel(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class ActionCategory(str, Enum):
    CRITICAL = "critical"
    QUICK_WINS = "quick_wins"
    STRATEGIC = "strategic"
    STRUCTURAL = "structural"


@dataclass
class ActionItem:
    """Represents a single recommended action."""
    action: str
    category: ActionCategory
    page_or_keyword: Optional[str]
    impact_monthly_clicks: float
    effort: EffortLevel
    dependencies: List[str] = field(default_factory=list)
    rationale: str = ""
    priority_score: float = 0.0


def generate_gameplan(
    health: Dict[str, Any],
    triage: Dict[str, Any],
    serp: Dict[str, Any],
    content: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Synthesize all prior modules into a prioritized action list.
    Gracefully handles missing or empty analysis sections.

    Args:
        health: Output from Module 1 (Health & Trajectory)
        triage: Output from Module 2 (Page-Level Triage)
        serp: Output from Module 3 (SERP Landscape Analysis)
        content: Output from Module 4 (Content Intelligence)

    Returns:
        Dictionary with categorized actions, impact estimates, and narrative
    """
    logger.info("Starting gameplan generation")

    # Initialize action lists
    critical_actions = []
    quick_wins = []
    strategic_actions = []
    structural_actions = []

    # Track total impact
    total_recovery = 0.0
    total_growth = 0.0

    # Generate actions from each module
    critical_from_triage, recovery_triage = _extract_critical_from_triage(triage)
    critical_actions.extend(critical_from_triage)
    total_recovery += recovery_triage

    critical_from_content, recovery_content = _extract_critical_from_content(content)
    critical_actions.extend(critical_from_content)
    total_recovery += recovery_content

    quick_from_content, growth_content = _extract_quick_wins_from_content(content)
    quick_wins.extend(quick_from_content)
    total_growth += growth_content

    quick_from_serp, growth_serp = _extract_quick_wins_from_serp(serp)
    quick_wins.extend(quick_from_serp)
    total_growth += growth_serp

    strategic_from_content, growth_strategic = _extract_strategic_from_content(content)
    strategic_actions.extend(strategic_from_content)
    total_growth += growth_strategic

    structural_from_health = _extract_structural_from_health(health)
    structural_actions.extend(structural_from_health)

    structural_from_content = _extract_structural_from_content(content)
    structural_actions.extend(structural_from_content)

    # Sort each category by priority score (descending)
    critical_actions.sort(key=lambda x: x.priority_score, reverse=True)
    quick_wins.sort(key=lambda x: x.priority_score, reverse=True)
    strategic_actions.sort(key=lambda x: x.priority_score, reverse=True)
    structural_actions.sort(key=lambda x: x.priority_score, reverse=True)

    # Generate narrative
    narrative = _generate_narrative(
        health=health,
        triage=triage,
        serp=serp,
        content=content,
        critical_count=len(critical_actions),
        quick_wins_count=len(quick_wins),
        strategic_count=len(strategic_actions),
        total_recovery=total_recovery,
        total_growth=total_growth
    )

    # Convert action items to dictionaries
    result = {
        "critical": [_action_to_dict(a) for a in critical_actions],
        "quick_wins": [_action_to_dict(a) for a in quick_wins],
        "strategic": [_action_to_dict(a) for a in strategic_actions],
        "structural": [_action_to_dict(a) for a in structural_actions],
        "total_estimated_monthly_click_recovery": round(total_recovery, 0),
        "total_estimated_monthly_click_growth": round(total_growth, 0),
        "narrative": narrative,
        "summary": {
            "critical_actions": len(critical_actions),
            "quick_wins": len(quick_wins),
            "strategic_plays": len(strategic_actions),
            "structural_improvements": len(structural_actions),
            "total_actions": len(critical_actions) + len(quick_wins) + len(strategic_actions) + len(structural_actions)
        }
    }

    logger.info(f"Gameplan generated: {result['summary']['total_actions']} total actions")
    return result


def _extract_critical_from_triage(triage: Dict[str, Any]) -> tuple[List[ActionItem], float]:
    """Extract critical actions from page triage data."""
    actions = []
    total_recovery = 0.0

    if not triage or "pages" not in triage:
        logger.warning("No triage data available")
        return actions, total_recovery

    pages = triage.get("pages", [])

    for page in pages:
        bucket = page.get("bucket", "")
        priority = page.get("priority_score", 0)
        monthly_clicks = page.get("current_monthly_clicks", 0)

        # Critical: pages in "critical" decay bucket with significant traffic
        if bucket == "critical" and monthly_clicks > 100:
            action_text = _generate_page_action_text(page)
            actions.append(ActionItem(
                action=action_text,
                category=ActionCategory.CRITICAL,
                page_or_keyword=page.get("url", ""),
                impact_monthly_clicks=monthly_clicks * 0.7,  # Assume 70% recovery potential
                effort=_determine_effort_level(page),
                priority_score=priority,
                rationale=f"Page losing {abs(page.get('trend_slope', 0)):.2f} clicks/day with {monthly_clicks} monthly clicks at risk"
            ))
            total_recovery += monthly_clicks * 0.7

        # CTR anomalies on high-impression pages
        elif page.get("ctr_anomaly") and monthly_clicks > 50:
            expected_ctr = page.get("ctr_expected", 0)
            actual_ctr = page.get("ctr_actual", 0)
            potential_gain = monthly_clicks * (expected_ctr / actual_ctr - 1) if actual_ctr > 0 else monthly_clicks * 0.5

            actions.append(ActionItem(
                action=f"Rewrite title and meta description for {page.get('url', 'page')} to improve CTR from {actual_ctr*100:.1f}% to expected {expected_ctr*100:.1f}%",
                category=ActionCategory.CRITICAL,
                page_or_keyword=page.get("url", ""),
                impact_monthly_clicks=potential_gain,
                effort=EffortLevel.LOW,
                priority_score=priority,
                rationale=f"CTR is {(1 - actual_ctr/expected_ctr)*100:.0f}% below expected for position {page.get('position', 'unknown')}"
            ))
            total_recovery += potential_gain

    return actions, total_recovery


def _extract_critical_from_content(content: Dict[str, Any]) -> tuple[List[ActionItem], float]:
    """Extract critical actions from content intelligence data."""
    actions = []
    total_recovery = 0.0

    if not content:
        logger.warning("No content data available")
        return actions, total_recovery

    # Cannibalization issues
    cannibalization = content.get("cannibalization_clusters", [])
    for cluster in cannibalization:
        impressions = cluster.get("total_impressions_affected", 0)
        if impressions > 1000:  # High-impact cannibalization
            recommendation = cluster.get("recommendation", "review")
            keep_page = cluster.get("keep_page", "")
            pages = cluster.get("pages", [])

            if recommendation == "consolidate":
                action_text = f"Consolidate {len(pages)} cannibalizing pages into {keep_page}"
                impact = impressions * 0.3  # Conservative 30% recovery
            elif recommendation == "differentiate":
                action_text = f"Differentiate content for {len(pages)} pages targeting similar queries"
                impact = impressions * 0.2
            else:
                action_text = f"Resolve cannibalization for {cluster.get('query_group', 'query group')}"
                impact = impressions * 0.15

            actions.append(ActionItem(
                action=action_text,
                category=ActionCategory.CRITICAL,
                page_or_keyword=cluster.get("query_group", ""),
                impact_monthly_clicks=impact,
                effort=EffortLevel.HIGH if recommendation == "consolidate" else EffortLevel.MEDIUM,
                priority_score=impressions / 100,  # Higher impressions = higher priority
                rationale=f"Cannibalization affecting {impressions:,} monthly impressions across {len(pages)} pages"
            ))
            total_recovery += impact

    return actions, total_recovery


def _extract_quick_wins_from_content(content: Dict[str, Any]) -> tuple[List[ActionItem], float]:
    """Extract quick win actions from content intelligence data."""
    actions = []
    total_growth = 0.0

    if not content:
        return actions, total_growth

    # Striking distance opportunities
    striking_distance = content.get("striking_distance", [])
    for opportunity in striking_distance[:15]:  # Top 15 opportunities
        keyword = opportunity.get("query", "")
        current_pos = opportunity.get("current_position", 0)
        impressions = opportunity.get("impressions", 0)
        click_gain = opportunity.get("estimated_click_gain_if_top5", 0)

        if click_gain > 50:  # Meaningful opportunity
            actions.append(ActionItem(
                action=f"Optimize '{keyword}' from position {current_pos:.1f} to top 5",
                category=ActionCategory.QUICK_WINS,
                page_or_keyword=keyword,
                impact_monthly_clicks=click_gain,
                effort=EffortLevel.LOW if current_pos < 12 else EffortLevel.MEDIUM,
                priority_score=click_gain,
                rationale=f"Currently position {current_pos:.1f} with {impressions:,} monthly impressions"
            ))
            total_growth += click_gain

    # Thin content expansion
    thin_content = content.get("thin_content", [])
    for page in thin_content[:10]:  # Top 10 thin pages
        url = page.get("url", "")
        impressions = page.get("impressions", 0)
        if impressions > 500:
            estimated_gain = impressions * 0.15  # 15% improvement from content expansion

            actions.append(ActionItem(
                action=f"Expand thin content on {url} (currently {page.get('word_count', 0)} words)",
                category=ActionCategory.QUICK_WINS,
                page_or_keyword=url,
                impact_monthly_clicks=estimated_gain,
                effort=EffortLevel.MEDIUM,
                priority_score=impressions / 100,
                rationale=f"High impressions ({impressions:,}/mo) but thin content with poor engagement"
            ))
            total_growth += estimated_gain

    return actions, total_growth


def _extract_quick_wins_from_serp(serp: Dict[str, Any]) -> tuple[List[ActionItem], float]:
    """Extract quick win actions from SERP landscape data."""
    actions = []
    total_growth = 0.0

    if not serp:
        return actions, total_growth

    # SERP feature opportunities
    feature_opportunities = serp.get("serp_feature_displacement", [])
    for item in feature_opportunities[:10]:
        keyword = item.get("keyword", "")
        impact = item.get("estimated_ctr_impact", 0)
        features = item.get("features_above", [])

        if abs(impact) > 0.02:  # > 2% CTR impact
            # Determine what action to take based on features
            if "featured_snippet" in features:
                action_text = f"Add FAQ schema to target featured snippet for '{keyword}'"
                effort = EffortLevel.LOW
            elif any("paa" in str(f).lower() for f in features):
                action_text = f"Optimize for People Also Ask boxes on '{keyword}'"
                effort = EffortLevel.MEDIUM
            elif "ai_overview" in features:
                action_text = f"Optimize content for AI Overview inclusion on '{keyword}'"
                effort = EffortLevel.MEDIUM
            else:
                action_text = f"Improve SERP visibility for '{keyword}' (multiple features present)"
                effort = EffortLevel.MEDIUM

            # Estimate click gain (rough approximation)
            estimated_clicks = abs(impact) * 1000  # Scale CTR impact to clicks

            actions.append(ActionItem(
                action=action_text,
                category=ActionCategory.QUICK_WINS,
                page_or_keyword=keyword,
                impact_monthly_clicks=estimated_clicks,
                effort=effort,
                priority_score=abs(impact) * 100,
                rationale=f"SERP features reducing effective CTR by {abs(impact)*100:.1f}%"
            ))
            total_growth += estimated_clicks

    return actions, total_growth


def _extract_strategic_from_content(content: Dict[str, Any]) -> tuple[List[ActionItem], float]:
    """Extract strategic actions from content intelligence data."""
    actions = []
    total_growth = 0.0

    if not content:
        return actions, total_growth

    # Content update priorities
    update_matrix = content.get("update_priority_matrix", {})

    # Urgent updates (old + decaying)
    urgent_updates = update_matrix.get("urgent_update", [])
    for page in urgent_updates[:20]:
        url = page.get("url", "")
        monthly_clicks = page.get("current_monthly_clicks", 0)
        estimated_recovery = monthly_clicks * 0.5  # 50% recovery potential

        actions.append(ActionItem(
            action=f"Comprehensive content refresh for {url}",
            category=ActionCategory.STRATEGIC,
            page_or_keyword=url,
            impact_monthly_clicks=estimated_recovery,
            effort=EffortLevel.HIGH,
            priority_score=monthly_clicks,
            rationale=f"Old content ({page.get('age_days', 0)} days) with declining performance"
        ))
        total_growth += estimated_recovery

    # Double-down opportunities (new + growing)
    double_down = update_matrix.get("double_down", [])
    for page in double_down[:10]:
        url = page.get("url", "")
        monthly_clicks = page.get("current_monthly_clicks", 0)
        growth_rate = page.get("growth_rate", 0)
        estimated_acceleration = monthly_clicks * growth_rate * 2  # Double the growth

        actions.append(ActionItem(
            action=f"Amplify growing content: add internal links and backlinks to {url}",
            category=ActionCategory.STRATEGIC,
            page_or_keyword=url,
            impact_monthly_clicks=estimated_acceleration,
            effort=EffortLevel.MEDIUM,
            priority_score=monthly_clicks * growth_rate,
            rationale=f"New content showing strong growth trajectory (+{growth_rate*100:.1f}%/mo)"
        ))
        total_growth += estimated_acceleration

    return actions, total_growth


def _extract_structural_from_health(health: Dict[str, Any]) -> List[ActionItem]:
    """Extract structural improvements from health & trajectory data."""
    actions = []

    if not health:
        return actions

    # Seasonality-based content calendar
    seasonality = health.get("seasonality", {})
    if seasonality.get("monthly_cycle"):
        cycle_desc = seasonality.get("cycle_description", "")
        actions.append(ActionItem(
            action=f"Implement seasonal content calendar: {cycle_desc}",
            category=ActionCategory.STRUCTURAL,
            page_or_keyword=None,
            impact_monthly_clicks=0,  # Indirect impact
            effort=EffortLevel.MEDIUM,
            priority_score=10,
            rationale="Clear monthly traffic patterns detected"
        ))

    # Address overall trajectory
    direction = health.get("overall_direction", "")
    if direction in ["declining", "strong_decline"]:
        slope = health.get("trend_slope_pct_per_month", 0)
        actions.append(ActionItem(
            action=f"Investigate root cause of {abs(slope):.1f}% monthly decline across site",
            category=ActionCategory.STRUCTURAL,
            page_or_keyword=None,
            impact_monthly_clicks=0,
            effort=EffortLevel.HIGH,
            priority_score=abs(slope) * 10,
            rationale="Site-wide declining trend requires systemic intervention"
        ))

    return actions


def _extract_structural_from_content(content: Dict[str, Any]) -> List[ActionItem]:
    """Extract structural improvements from content intelligence data."""
    actions = []

    if not content:
        return actions

    # Cannibalization suggests need for content strategy
    cannibalization_count = len(content.get("cannibalization_clusters", []))
    if cannibalization_count > 5:
        actions.append(ActionItem(
            action=f"Develop content architecture strategy to prevent cannibalization ({cannibalization_count} clusters found)",
            category=ActionCategory.STRUCTURAL,
            page_or_keyword=None,
            impact_monthly_clicks=0,
            effort=EffortLevel.HIGH,
            priority_score=cannibalization_count,
            rationale="Widespread cannibalization indicates systematic content planning issues"
        ))

    # Thin content suggests quality standards needed
    thin_count = len(content.get("thin_content", []))
    if thin_count > 10:
        actions.append(ActionItem(
            action=f"Establish content quality standards ({thin_count} thin pages identified)",
            category=ActionCategory.STRUCTURAL,
            page_or_keyword=None,
            impact_monthly_clicks=0,
            effort=EffortLevel.MEDIUM,
            priority_score=thin_count / 2,
            rationale="Multiple thin content pages suggest need for quality guidelines"
        ))

    return actions


def _generate_page_action_text(page: Dict[str, Any]) -> str:
    """Generate specific action text for a page based on its issues."""
    url = page.get("url", "page")
    recommended_action = page.get("recommended_action", "review")

    action_templates = {
        "title_rewrite": f"Rewrite title tag for {url} to improve CTR",
        "content_expansion": f"Expand content on {url} (add 500+ words)",
        "technical_fix": f"Fix technical issues on {url}",
        "internal_links": f"Add internal links pointing to {url}",
        "content_refresh": f"Update and refresh outdated content on {url}",
        "consolidate": f"Consolidate {url} with competing page",
        "review": f"Review declining performance on {url}"
    }

    return action_templates.get(recommended_action, f"Address issues on {url}")


def _determine_effort_level(page: Dict[str, Any]) -> EffortLevel:
    """Determine effort level based on page characteristics."""
    recommended_action = page.get("recommended_action", "review")

    effort_map = {
        "title_rewrite": EffortLevel.LOW,
        "internal_links": EffortLevel.LOW,
        "content_expansion": EffortLevel.MEDIUM,
        "content_refresh": EffortLevel.MEDIUM,
        "technical_fix": EffortLevel.MEDIUM,
        "consolidate": EffortLevel.HIGH,
        "review": EffortLevel.MEDIUM
    }

    return effort_map.get(recommended_action, EffortLevel.MEDIUM)


def _action_to_dict(action: ActionItem) -> Dict[str, Any]:
    """Convert ActionItem to dictionary for JSON serialization."""
    return {
        "action": action.action,
        "page_or_keyword": action.page_or_keyword,
        "impact_monthly_clicks": round(action.impact_monthly_clicks, 0),
        "effort": action.effort.value,
        "dependencies": action.dependencies,
        "rationale": action.rationale,
        "priority_score": round(action.priority_score, 2)
    }


def _generate_narrative(
    health: Dict[str, Any],
    triage: Dict[str, Any],
    serp: Dict[str, Any],
    content: Dict[str, Any],
    critical_count: int,
    quick_wins_count: int,
    strategic_count: int,
    total_recovery: float,
    total_growth: float
) -> str:
    """
    Generate human-readable narrative for the gameplan.
    Handles edge cases where sections have no data or no actionable items.
    """
    narrative_parts = []

    # Opening: overall health assessment
    direction = health.get("overall_direction", "unknown") if health else "unknown"
    slope = health.get("trend_slope_pct_per_month", 0) if health else 0

    if direction in ["declining", "strong_decline"]:
        narrative_parts.append(
            f"Your site is currently declining at {abs(slope):.1f}% per month. "
            f"This analysis has identified {critical_count + quick_wins_count + strategic_count} "
            f"concrete actions to reverse this trend."
        )
    elif direction in ["growth", "strong_growth"]:
        narrative_parts.append(
            f"Your site is growing at {slope:.1f}% per month. "
            f"This analysis has identified {critical_count + quick_wins_count + strategic_count} "
            f"opportunities to accelerate that growth further."
        )
    elif direction == "flat":
        narrative_parts.append(
            f"Your site traffic is currently flat. "
            f"This analysis has identified {critical_count + quick_wins_count + strategic_count} "
            f"opportunities to break through to growth."
        )
    else:
        narrative_parts.append(
            f"Based on your search performance data, this analysis has identified "
            f"{critical_count + quick_wins_count + strategic_count} prioritized actions."
        )

    # Critical actions section
    if critical_count > 0:
        narrative_parts.append(
            f"\n\n**Critical Actions (This Week):** "
            f"There are {critical_count} urgent issues that need immediate attention. "
            f"These represent {total_recovery:,.0f} clicks per month at risk. "
        )

        # Add context from triage if available
        if triage and "summary" in triage:
            critical_pages = triage["summary"].get("critical", 0)
            if critical