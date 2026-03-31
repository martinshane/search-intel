"""
Module 05: The Gameplan — Prioritized Action Plan

Synthesizes outputs from all prior modules into a prioritized, actionable roadmap.
Includes fallback narrative generation when Claude API fails.
"""

import anthropic
import json
import os
from typing import Dict, Any, List, Optional
from datetime import datetime


def generate_gameplan(
    health: Dict[str, Any],
    triage: Dict[str, Any],
    serp: Dict[str, Any],
    content: Dict[str, Any],
    algorithm: Optional[Dict[str, Any]] = None,
    intent: Optional[Dict[str, Any]] = None,
    ctr: Optional[Dict[str, Any]] = None,
    architecture: Optional[Dict[str, Any]] = None,
    branded: Optional[Dict[str, Any]] = None,
    competitive: Optional[Dict[str, Any]] = None,
    revenue: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Synthesize all module outputs into a prioritized action plan.
    
    Args:
        health: Output from Module 1 (Health & Trajectory)
        triage: Output from Module 2 (Page-Level Triage)
        serp: Output from Module 3 (SERP Landscape)
        content: Output from Module 4 (Content Intelligence)
        algorithm: Optional output from Module 6
        intent: Optional output from Module 7
        ctr: Optional output from Module 8
        architecture: Optional output from Module 9
        branded: Optional output from Module 10
        competitive: Optional output from Module 11
        revenue: Optional output from Module 12
    
    Returns:
        Dictionary with prioritized actions and narrative synthesis
    """
    
    # Build prioritized action lists
    critical = _extract_critical_fixes(health, triage, content)
    quick_wins = _extract_quick_wins(triage, serp, content, ctr)
    strategic = _extract_strategic_plays(content, serp, algorithm, intent, branded)
    structural = _extract_structural_improvements(architecture, health, branded)
    
    # Calculate aggregate impact estimates
    total_recovery = _calculate_recovery_potential(critical, quick_wins, triage)
    total_growth = _calculate_growth_potential(quick_wins, strategic, content, serp)
    
    # Prepare structured data for LLM synthesis
    synthesis_data = {
        "health_summary": _summarize_health(health),
        "critical_count": len(critical),
        "quick_wins_count": len(quick_wins),
        "strategic_count": len(strategic),
        "total_recovery": total_recovery,
        "total_growth": total_growth,
        "top_critical": critical[:3] if critical else [],
        "top_quick_wins": quick_wins[:5] if quick_wins else [],
        "branded_ratio": branded.get("branded_ratio") if branded else None,
        "revenue_at_risk": revenue.get("revenue_at_risk_90d") if revenue else None,
        "algorithm_impacts": algorithm.get("updates_impacting_site", []) if algorithm else []
    }
    
    # Generate narrative with fallback
    narrative = _generate_narrative_with_fallback(synthesis_data)
    
    return {
        "critical": critical,
        "quick_wins": quick_wins,
        "strategic": strategic,
        "structural": structural,
        "total_estimated_monthly_click_recovery": total_recovery,
        "total_estimated_monthly_click_growth": total_growth,
        "narrative": narrative,
        "generated_at": datetime.utcnow().isoformat()
    }


def _extract_critical_fixes(
    health: Dict[str, Any],
    triage: Dict[str, Any],
    content: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """Extract urgent issues requiring immediate attention."""
    critical = []
    
    # Critical decaying pages (from triage)
    if triage.get("pages"):
        for page in triage["pages"]:
            if page.get("bucket") == "critical" and page.get("current_monthly_clicks", 0) > 100:
                critical.append({
                    "action": f"Emergency intervention required for {page['url']}",
                    "type": "critical_decay",
                    "page": page["url"],
                    "detail": f"Page is in critical decay with {page['current_monthly_clicks']} monthly clicks. "
                             f"Current trend slope: {page.get('trend_slope', 0):.2f} clicks/day. "
                             f"Projected to fall below page 1 by {page.get('projected_page1_loss_date', 'soon')}.",
                    "impact": page.get("current_monthly_clicks", 0),
                    "effort": "high",
                    "instructions": _generate_critical_page_instructions(page),
                    "priority_score": page.get("priority_score", 0)
                })
    
    # CTR anomalies on high-traffic keywords
    if triage.get("pages"):
        for page in triage["pages"]:
            if page.get("ctr_anomaly") and page.get("current_monthly_clicks", 0) > 50:
                expected_ctr = page.get("ctr_expected", 0)
                actual_ctr = page.get("ctr_actual", 0)
                ctr_gap = (expected_ctr - actual_ctr) * 100
                potential_clicks = page.get("current_monthly_clicks", 0) * (expected_ctr / actual_ctr if actual_ctr > 0 else 2)
                
                critical.append({
                    "action": f"Fix CTR anomaly for {page['url']}",
                    "type": "ctr_fix",
                    "page": page["url"],
                    "detail": f"CTR is {ctr_gap:.1f}% below expected. "
                             f"Expected: {expected_ctr*100:.1f}%, Actual: {actual_ctr*100:.1f}%. "
                             f"This indicates a title/meta description problem.",
                    "impact": int(potential_clicks - page.get("current_monthly_clicks", 0)),
                    "effort": "low",
                    "instructions": [
                        "Rewrite title tag to be more compelling and include target keyword",
                        "Update meta description to better match search intent",
                        "Review SERP preview to ensure it stands out from competitors",
                        "A/B test different title formats if possible"
                    ],
                    "priority_score": (potential_clicks - page.get("current_monthly_clicks", 0)) * 2
                })
    
    # Severe cannibalization
    if content.get("cannibalization_clusters"):
        for cluster in content["cannibalization_clusters"]:
            if cluster.get("total_impressions_affected", 0) > 5000:
                critical.append({
                    "action": f"Resolve cannibalization for: {cluster.get('query_group', 'query group')}",
                    "type": "cannibalization",
                    "detail": f"{len(cluster.get('pages', []))} pages competing for {cluster.get('shared_queries', 0)} queries. "
                             f"Total impressions affected: {cluster.get('total_impressions_affected', 0)}. "
                             f"Recommendation: {cluster.get('recommendation', 'consolidate')}.",
                    "pages": cluster.get("pages", []),
                    "impact": int(cluster.get("total_impressions_affected", 0) * 0.05),  # Estimate 5% click gain
                    "effort": "medium",
                    "instructions": _generate_cannibalization_instructions(cluster),
                    "priority_score": cluster.get("total_impressions_affected", 0) * 0.05
                })
    
    # Sort by priority score descending
    critical.sort(key=lambda x: x.get("priority_score", 0), reverse=True)
    
    return critical


def _extract_quick_wins(
    triage: Dict[str, Any],
    serp: Dict[str, Any],
    content: Dict[str, Any],
    ctr: Optional[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """Extract low-effort, high-impact opportunities."""
    quick_wins = []
    
    # Striking distance keywords
    if content.get("striking_distance"):
        for keyword_opp in content["striking_distance"][:10]:  # Top 10
            current_pos = keyword_opp.get("current_position", 20)
            impressions = keyword_opp.get("impressions", 0)
            click_gain = keyword_opp.get("estimated_click_gain_if_top5", 0)
            
            if click_gain > 50:  # Worth pursuing
                quick_wins.append({
                    "action": f"Boost '{keyword_opp.get('query', 'keyword')}' from position {current_pos:.1f} to top 5",
                    "type": "striking_distance",
                    "keyword": keyword_opp.get("query"),
                    "page": keyword_opp.get("landing_page"),
                    "detail": f"Currently at position {current_pos:.1f} with {impressions} monthly impressions. "
                             f"Moving to top 5 could gain {click_gain} clicks/month.",
                    "impact": click_gain,
                    "effort": "low" if current_pos <= 12 else "medium",
                    "instructions": _generate_striking_distance_instructions(keyword_opp),
                    "priority_score": click_gain
                })
    
    # SERP feature opportunities
    if ctr and ctr.get("feature_opportunities"):
        for feature_opp in ctr["feature_opportunities"][:5]:  # Top 5
            if feature_opp.get("estimated_click_gain", 0) > 100:
                quick_wins.append({
                    "action": f"Capture {feature_opp.get('feature', 'SERP feature')} for '{feature_opp.get('keyword')}'",
                    "type": "serp_feature",
                    "keyword": feature_opp.get("keyword"),
                    "feature": feature_opp.get("feature"),
                    "detail": f"Current holder: {feature_opp.get('current_holder', 'competitor')}. "
                             f"Estimated impact: +{feature_opp.get('estimated_click_gain', 0)} clicks/month. "
                             f"Difficulty: {feature_opp.get('difficulty', 'medium')}.",
                    "impact": feature_opp.get("estimated_click_gain", 0),
                    "effort": "low" if feature_opp.get("difficulty") == "low" else "medium",
                    "instructions": _generate_serp_feature_instructions(feature_opp),
                    "priority_score": feature_opp.get("estimated_click_gain", 0)
                })
    
    # Low-hanging CTR improvements
    if triage.get("pages"):
        for page in triage["pages"]:
            if (page.get("ctr_anomaly") and 
                page.get("current_monthly_clicks", 0) > 20 and 
                page.get("current_monthly_clicks", 0) <= 100):  # Mid-range traffic
                
                expected_ctr = page.get("ctr_expected", 0)
                actual_ctr = page.get("ctr_actual", 0)
                potential_gain = page.get("current_monthly_clicks", 0) * (expected_ctr / actual_ctr - 1) if actual_ctr > 0 else 0
                
                if potential_gain > 20:
                    quick_wins.append({
                        "action": f"Quick title optimization for {page['url']}",
                        "type": "ctr_optimization",
                        "page": page["url"],
                        "detail": f"Simple title/meta fix could gain {int(potential_gain)} clicks/month.",
                        "impact": int(potential_gain),
                        "effort": "low",
                        "instructions": [
                            "Rewrite title tag with stronger benefit statement",
                            "Add current year if time-sensitive content",
                            "Test emotional triggers or numbers in title"
                        ],
                        "priority_score": potential_gain * 1.5  # Bonus for low effort
                    })
    
    # Internal link additions
    if content.get("thin_content"):
        for thin_page in content["thin_content"][:5]:  # Top 5
            if thin_page.get("impressions", 0) > 500:
                quick_wins.append({
                    "action": f"Add internal links to {thin_page.get('url')}",
                    "type": "internal_linking",
                    "page": thin_page.get("url"),
                    "detail": f"Page has {thin_page.get('impressions', 0)} impressions but may lack authority. "
                             f"Quick internal link additions from related high-authority pages.",
                    "impact": int(thin_page.get("impressions", 0) * 0.02),  # Conservative 2% click gain
                    "effort": "low",
                    "instructions": [
                        "Identify 3-5 high-authority pages with related content",
                        "Add contextual internal links with descriptive anchor text",
                        "Ensure links are naturally placed within content body"
                    ],
                    "priority_score": thin_page.get("impressions", 0) * 0.02
                })
    
    # Sort by priority score descending
    quick_wins.sort(key=lambda x: x.get("priority_score", 0), reverse=True)
    
    return quick_wins


def _extract_strategic_plays(
    content: Dict[str, Any],
    serp: Dict[str, Any],
    algorithm: Optional[Dict[str, Any]],
    intent: Optional[Dict[str, Any]],
    branded: Optional[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """Extract longer-term strategic initiatives."""
    strategic = []
    
    # Content consolidation projects
    if content.get("cannibalization_clusters"):
        for cluster in content["cannibalization_clusters"]:
            if cluster.get("recommendation") == "consolidate":
                strategic.append({
                    "action": f"Content consolidation project: {cluster.get('query_group', 'query group')}",
                    "type": "consolidation",
                    "detail": f"Merge {len(cluster.get('pages', []))} competing pages into one authoritative resource.",
                    "pages": cluster.get("pages", []),
                    "impact": int(cluster.get("total_impressions_affected", 0) * 0.08),
                    "effort": "high",
                    "timeline": "this_quarter",
                    "instructions": _generate_consolidation_instructions(cluster),
                    "priority_score": cluster.get("total_impressions_affected", 0) * 0.08
                })
    
    # Content refresh campaigns
    if content.get("update_priority_matrix", {}).get("urgent_update"):
        urgent_updates = content["update_priority_matrix"]["urgent_update"][:10]
        if urgent_updates:
            total_impact = sum(page.get("current_monthly_clicks", 0) for page in urgent_updates)
            strategic.append({
                "action": f"Content refresh campaign: {len(urgent_updates)} aging pages",
                "type": "content_refresh",
                "detail": f"Systematic update of {len(urgent_updates)} pages showing age-related decay. "
                         f"Combined current traffic: {total_impact} clicks/month.",
                "pages": [page.get("url") for page in urgent_updates],
                "impact": int(total_impact * 0.15),  # 15% recovery estimate
                "effort": "high",
                "timeline": "this_quarter",
                "instructions": [
                    "Update statistics and data to current year",
                    "Add new examples and case studies",
                    "Expand thin sections with more detail",
                    "Update images and screenshots",
                    "Add FAQ sections for common questions",
                    "Refresh meta descriptions and titles"
                ],
                "priority_score": total_impact * 0.15
            })
    
    # Non-branded growth strategy
    if branded and branded.get("branded_ratio", 0) > 0.7:
        non_branded_opportunity = branded.get("non_branded_opportunity", {})
        gap = non_branded_opportunity.get("gap", 0)
        
        if gap > 500:
            strategic.append({
                "action": "Non-branded traffic growth initiative",
                "type": "non_branded_growth",
                "detail": f"Your site is {branded.get('branded_ratio', 0)*100:.0f}% dependent on branded search. "
                         f"Non-branded opportunity: {gap} clicks/month. "
                         f"Time to meaningful non-branded traffic: {non_branded_opportunity.get('months_to_meaningful_with_actions', 12)} months.",
                "impact": int(gap * 0.3),  # 30% of gap achievable in quarter
                "effort": "high",
                "timeline": "this_quarter",
                "instructions": [
                    "Audit top non-branded keywords for content gaps",
                    "Create comprehensive guides for high-volume informational queries",
                    "Build comparison pages for commercial keywords",
                    "Implement FAQ schema for featured snippet opportunities",
                    "Launch monthly content calendar targeting non-branded terms",
                    "Build topical clusters around core non-branded themes"
                ],
                "priority_score": gap * 0.3
            })
    
    # Competitor response strategy
    if serp.get("competitors"):
        top_competitor = serp["competitors"][0] if serp["competitors"] else None
        if top_competitor and top_competitor.get("keywords_shared", 0) > 20:
            strategic.append({
                "action": f"Competitive response: {top_competitor.get('domain')}",
                "type": "competitive_strategy",
                "detail": f"Primary competitor appears in {top_competitor.get('keywords_shared', 0)} of your keywords "
                         f"at avg position {top_competitor.get('avg_position', 0):.1f}. "
                         f"Develop targeted response strategy.",
                "impact": int(top_competitor.get("keywords_shared", 0) * 50),  # Rough estimate
                "effort": "high",
                "timeline": "this_quarter",
                "instructions": [
                    f"Conduct content gap analysis vs {top_competitor.get('domain')}",
                    "Identify their top-performing content formats",
                    "Find keywords where you're close behind and prioritize",
                    "Analyze their backlink strategy",
                    "Monitor for new content launches and respond quickly"
                ],
                "priority_score": top_competitor.get("keywords_shared", 0) * 50
            })
    
    # Algorithm recovery (if recent negative impact)
    if algorithm and algorithm.get("updates_impacting_site"):
        for update in algorithm["updates_impacting_site"]:
            if (update.get("site_impact") == "negative" and 
                update.get("recovery_status") == "not_recovered" and
                abs(update.get("click_change_pct", 0)) > 10):
                
                strategic.append({
                    "action": f"Algorithm recovery: {update.get('update_name')}",
                    "type": "algorithm_recovery",
                    "detail": f"{update.get('update_name')} caused {update.get('click_change_pct', 0):.1f}% traffic drop. "
                             f"Most affected: {', '.join(update.get('pages_most_affected', [])[:3])}. "
                             f"Common issues: {', '.join(update.get('common_characteristics', []))}.",
                    "impact": int(abs(update.get("click_change_pct", 0)) * 100),  # Rough monthly click estimate
                    "effort": "high",
                    "timeline": "this_quarter",
                    "instructions": _generate_algorithm_recovery_instructions(update),
                    "priority_score": abs(update.get("click_change_pct", 0)) * 100
                })
    
    # Sort by priority score descending
    strategic.sort(key=lambda x: x.get("priority_score", 0), reverse=True)
    
    return strategic


def _extract_structural_improvements(
    architecture: Optional[Dict[str, Any]],
    health: Dict[str, Any],
    branded: Optional[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """Extract ongoing structural/architectural improvements."""
    structural = []
    
    # Internal link architecture fixes
    if architecture:
        authority_flow = architecture.get("authority_flow_to_conversion", 0)
        if authority_flow < 0.1:  # Less than 10% reaching conversion pages
            structural.append({
                "action": "Internal link architecture overhaul",
                "type": "link_architecture",
                "detail": f"Only {authority_flow*100:.1f}% of internal link authority reaches conversion pages. "
                         f"Restructure to improve authority distribution.",
                "timeline": "ongoing",
                "instructions": [
                    "Map out conversion funnel and key pages",
                    "Implement hub-and-spoke model from homepage to hubs to conversion pages",
                    "Add contextual links from blog content to related product/service pages",
                    "Remove or nofollow unnecessary footer/sidebar links",
                    "Create internal linking guidelines for content team"
                ]
            })
        
        # Orphan page fixes
        orphan_pages = architecture.get("orphan_pages", [])
        if len(orphan_pages) > 10:
            structural.append({
                "action": f"Connect {len(orphan_pages)} orphaned pages",
                "type": "orphan_resolution",
                "detail": f"{len(orphan_pages)} pages have no internal links but appear in GSC data.",
                "timeline": "ongoing",
                "instructions": [
                    "Review each orphan page for value",
                    "Delete or 301 redirect low-value pages",
                    "Add internal links to valuable orphan pages",
                    "Add to sitemap if not already included",
                    "Create process to prevent future orphan pages"
                ]
            })
        
        # Link insertion recommendations
        link_recs = architecture.get("link_recommendations", [])[:10]
        if link_recs:
            structural.append({
                "action": "Implement high-value internal link recommendations",
                "type": "link_insertion",
                "detail": f"{len(link_recs)} specific link placements identified with measurable impact.",
                "timeline": "ongoing",
                "recommendations": link_recs,
                "instructions": [
                    "Implement recommended links in priority order",
                    "Use suggested anchor text but ensure natural flow",
                    "Place links contextually within content body",
                    "Monitor impact on target page rankings",
                    "Continue identifying new opportunities monthly"
                ]
            })
    
    # Seasonal content calendar
    if health.get("seasonality", {}).get("monthly_cycle"):
        seasonal_desc = health["seasonality"].get("cycle_description", "")
        structural.append({
            "action": "Implement seasonal content calendar",
            "type": "seasonal_optimization",
            "detail": f"Site shows clear seasonal pattern: {seasonal_desc}. "
                     f"Optimize content publication timing.",
            "timeline": "ongoing",
            "instructions": [
                "Schedule content updates before peak periods",
                "Prepare seasonal campaigns 2-4 weeks