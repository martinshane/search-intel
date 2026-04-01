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
    serp: Optional[Dict[str, Any]] = None,
    content: Optional[Dict[str, Any]] = None,
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

    Only health (Module 1) and triage (Module 2) are required.  All other
    module outputs are optional enrichments — the gameplan produces a useful
    action plan with just GSC-based health + triage data, and becomes
    progressively richer as more module outputs are available.

    Args:
        health: Output from Module 1 (Health & Trajectory) — REQUIRED
        triage: Output from Module 2 (Page-Level Triage) — REQUIRED
        serp: Optional output from Module 3 (SERP Landscape)
        content: Optional output from Module 4 (Content Intelligence)
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
    """
    Extract critical fixes (do this week).
    
    Criteria:
    - Pages in "critical" decay bucket with > 100 clicks/month
    - CTR anomalies on high-impression keywords
    - Cannibalization causing both pages to underperform
    """
    critical_actions = []
    
    # 1. Critical decaying pages
    if triage and "pages" in triage:
        for page in triage["pages"]:
            if (page.get("bucket") == "critical" and 
                page.get("current_monthly_clicks", 0) > 100):
                
                action = {
                    "type": "critical_page_rescue",
                    "page": page["url"],
                    "current_clicks": page["current_monthly_clicks"],
                    "trend_slope": page.get("trend_slope", 0),
                    "action": _generate_critical_page_instructions(page),
                    "impact": int(page["current_monthly_clicks"] * 0.6),
                    "effort": "medium",
                    "deadline": "7 days"
                }
                critical_actions.append(action)
    
    # 2. High-value CTR anomalies
    if triage and "pages" in triage:
        for page in triage["pages"]:
            if (page.get("ctr_anomaly") and 
                page.get("current_monthly_clicks", 0) > 200):
                
                expected_clicks = page.get("ctr_expected", 0) * page.get("impressions", 0)
                actual_clicks = page.get("ctr_actual", 0) * page.get("impressions", 0)
                potential_gain = expected_clicks - actual_clicks
                
                if potential_gain > 50:
                    action = {
                        "type": "ctr_optimization",
                        "page": page["url"],
                        "current_ctr": page.get("ctr_actual", 0),
                        "expected_ctr": page.get("ctr_expected", 0),
                        "action": "Rewrite title tag and meta description to improve CTR. Current CTR is significantly below expected for position.",
                        "impact": int(potential_gain * 30),
                        "effort": "low",
                        "deadline": "3 days"
                    }
                    critical_actions.append(action)
    
    # 3. High-impact cannibalization
    if content and "cannibalization_clusters" in content:
        for cluster in content["cannibalization_clusters"]:
            if cluster.get("total_impressions_affected", 0) > 5000:
                action = {
                    "type": "cannibalization_fix",
                    "pages": cluster["pages"],
                    "queries_affected": cluster.get("shared_queries", 0),
                    "action": _generate_cannibalization_instructions(cluster),
                    "impact": int(cluster["total_impressions_affected"] * 0.05),
                    "effort": "high",
                    "deadline": "14 days"
                }
                critical_actions.append(action)
    
    # Sort by impact descending
    critical_actions.sort(key=lambda x: x.get("impact", 0), reverse=True)
    
    return critical_actions[:10]


def _extract_quick_wins(
    triage: Dict[str, Any],
    serp: Dict[str, Any],
    content: Dict[str, Any],
    ctr: Optional[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Extract quick wins (do this month).
    
    Criteria:
    - Striking distance keywords (positions 8-20)
    - SERP feature optimization opportunities
    - Minor content updates for decaying pages
    """
    quick_wins = []
    
    # 1. Striking distance keywords
    if content and "striking_distance" in content:
        for keyword_opp in content["striking_distance"]:
            if keyword_opp.get("estimated_click_gain_if_top5", 0) > 50:
                action = {
                    "type": "striking_distance",
                    "keyword": keyword_opp["query"],
                    "current_position": keyword_opp["current_position"],
                    "page": keyword_opp["landing_page"],
                    "action": _generate_striking_distance_instructions(keyword_opp),
                    "impact": keyword_opp["estimated_click_gain_if_top5"],
                    "effort": "medium",
                    "timeframe": "30 days"
                }
                quick_wins.append(action)
    
    # 2. SERP feature opportunities
    if serp and "serp_feature_displacement" in serp:
        for keyword_data in serp["serp_feature_displacement"]:
            if abs(keyword_data.get("estimated_ctr_impact", 0)) > 0.03:
                action = {
                    "type": "serp_feature_optimization",
                    "keyword": keyword_data["keyword"],
                    "features_to_target": keyword_data.get("features_above", []),
                    "action": _generate_serp_feature_instructions(keyword_data),
                    "impact": int(keyword_data.get("impressions", 0) * abs(keyword_data.get("estimated_ctr_impact", 0))),
                    "effort": "low",
                    "timeframe": "14 days"
                }
                quick_wins.append(action)
    
    # 3. Decaying pages with low effort fixes
    if triage and "pages" in triage:
        for page in triage["pages"]:
            if (page.get("bucket") == "decaying" and 
                page.get("current_monthly_clicks", 0) > 50 and
                page.get("engagement_flag") == "low_engagement"):
                
                action = {
                    "type": "content_refresh",
                    "page": page["url"],
                    "action": "Update content to better match search intent. Add recent statistics, examples, or case studies. Current content shows low engagement.",
                    "impact": int(page["current_monthly_clicks"] * 0.3),
                    "effort": "medium",
                    "timeframe": "21 days"
                }
                quick_wins.append(action)
    
    # Sort by impact/effort ratio
    for action in quick_wins:
        effort_multiplier = {"low": 1, "medium": 2, "high": 4}
        action["priority_score"] = action.get("impact", 0) / effort_multiplier.get(action.get("effort", "medium"), 2)
    
    quick_wins.sort(key=lambda x: x.get("priority_score", 0), reverse=True)
    
    return quick_wins[:15]


def _extract_strategic_plays(
    content: Dict[str, Any],
    serp: Dict[str, Any],
    algorithm: Optional[Dict[str, Any]],
    intent: Optional[Dict[str, Any]],
    branded: Optional[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Extract strategic plays (this quarter).
    
    Criteria:
    - Content gap opportunities
    - Major consolidation projects
    - Content refreshes for aged content
    - Algorithm recovery initiatives
    """
    strategic_actions = []
    
    # 1. Major consolidation projects
    if content and "cannibalization_clusters" in content:
        clusters_to_consolidate = [
            c for c in content["cannibalization_clusters"]
            if c.get("recommendation") == "consolidate" and
            c.get("total_impressions_affected", 0) > 1000
        ]
        
        if clusters_to_consolidate:
            total_impact = sum(c.get("total_impressions_affected", 0) for c in clusters_to_consolidate)
            action = {
                "type": "consolidation_project",
                "clusters_count": len(clusters_to_consolidate),
                "action": _generate_consolidation_instructions(clusters_to_consolidate),
                "impact": int(total_impact * 0.08),
                "effort": "high",
                "timeframe": "90 days"
            }
            strategic_actions.append(action)
    
    # 2. Content refresh program (aged content)
    if content and "update_priority_matrix" in content:
        urgent_updates = content["update_priority_matrix"].get("urgent_update", [])
        if len(urgent_updates) > 5:
            total_clicks = sum(p.get("current_monthly_clicks", 0) for p in urgent_updates[:10])
            action = {
                "type": "content_refresh_program",
                "pages_count": len(urgent_updates),
                "action": f"Systematically refresh {len(urgent_updates)} aged pages showing decay. Prioritize pages with highest current traffic. Update statistics, add new sections, improve formatting.",
                "impact": int(total_clicks * 0.4),
                "effort": "high",
                "timeframe": "90 days"
            }
            strategic_actions.append(action)
    
    # 3. Algorithm recovery
    if algorithm and "updates_impacting_site" in algorithm:
        unrecovered_updates = [
            u for u in algorithm["updates_impacting_site"]
            if u.get("recovery_status") == "not_recovered" and
            abs(u.get("click_change_pct", 0)) > 5
        ]
        
        if unrecovered_updates:
            action = {
                "type": "algorithm_recovery",
                "updates_affected": len(unrecovered_updates),
                "action": _generate_algorithm_recovery_instructions(unrecovered_updates),
                "impact": int(sum(abs(u.get("click_change_pct", 0)) for u in unrecovered_updates) * 100),
                "effort": "high",
                "timeframe": "90 days"
            }
            strategic_actions.append(action)
    
    # 4. Intent migration response
    if intent and "migrations" in intent:
        significant_migrations = [
            m for m in intent["migrations"]
            if m.get("traffic_impact_pct", 0) < -10
        ]
        
        if significant_migrations:
            action = {
                "type": "intent_realignment",
                "keywords_affected": len(significant_migrations),
                "action": f"Realign content for {len(significant_migrations)} keywords showing intent shifts. Focus on matching new SERP patterns and user expectations.",
                "impact": int(sum(abs(m.get("traffic_impact_pct", 0)) for m in significant_migrations) * 50),
                "effort": "high",
                "timeframe": "60 days"
            }
            strategic_actions.append(action)
    
    # Sort by impact
    strategic_actions.sort(key=lambda x: x.get("impact", 0), reverse=True)
    
    return strategic_actions[:8]


def _extract_structural_improvements(
    architecture: Optional[Dict[str, Any]],
    health: Dict[str, Any],
    branded: Optional[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Extract structural improvements (ongoing).
    
    Criteria:
    - Internal linking architecture changes
    - Seasonal content calendar
    - Monitoring and alerting setup
    """
    structural_actions = []
    
    # 1. Internal linking improvements
    if architecture and "orphan_pages" in architecture:
        orphan_count = len(architecture["orphan_pages"])
        if orphan_count > 0:
            action = {
                "type": "internal_linking",
                "action": f"Integrate {orphan_count} orphan pages into internal link structure. Add contextual links from related content.",
                "impact": "ongoing",
                "effort": "medium"
            }
            structural_actions.append(action)
    
    if architecture and "hub_opportunities" in architecture:
        if architecture["hub_opportunities"]:
            action = {
                "type": "hub_development",
                "action": f"Develop {len(architecture['hub_opportunities'])} content hubs to strengthen topical authority. Create pillar pages and strengthen internal linking.",
                "impact": "ongoing",
                "effort": "high"
            }
            structural_actions.append(action)
    
    # 2. Seasonal calendar
    if health and "seasonality" in health:
        if health["seasonality"].get("monthly_cycle"):
            action = {
                "type": "seasonal_calendar",
                "action": f"Implement content publishing calendar aligned with traffic patterns. Best day: {health['seasonality'].get('best_day', 'Unknown')}. {health['seasonality'].get('cycle_description', '')}",
                "impact": "ongoing",
                "effort": "low"
            }
            structural_actions.append(action)
    
    # 3. Branded search monitoring
    if branded:
        action = {
            "type": "brand_monitoring",
            "action": f"Set up alerts for branded search performance. Current branded ratio: {branded.get('branded_ratio', 0):.1%}. Monitor for reputation issues and brand strength changes.",
            "impact": "ongoing",
            "effort": "low"
        }
        structural_actions.append(action)
    
    # 4. Competitive monitoring
    action = {
        "type": "competitive_monitoring",
        "action": "Establish quarterly competitive analysis routine. Track competitor movements on priority keywords and identify new threats early.",
        "impact": "ongoing",
        "effort": "low"
    }
    structural_actions.append(action)
    
    return structural_actions


def _generate_critical_page_instructions(page: Dict[str, Any]) -> str:
    """Generate specific instructions for critical page rescue."""
    instructions = []
    
    if page.get("ctr_anomaly"):
        instructions.append("Rewrite title tag and meta description")
    
    if page.get("engagement_flag") == "low_engagement":
        instructions.append("Improve content quality and search intent match")
    
    if page.get("trend_slope", 0) < -0.5:
        instructions.append("Add fresh content, update statistics, improve comprehensiveness")
    
    if not instructions:
        instructions.append("Comprehensive content audit and optimization needed")
    
    return ". ".join(instructions) + "."


def _generate_cannibalization_instructions(cluster: Dict[str, Any]) -> str:
    """Generate specific instructions for cannibalization fix."""
    recommendation = cluster.get("recommendation", "consolidate")
    keep_page = cluster.get("keep_page", cluster["pages"][0])
    
    if recommendation == "consolidate":
        return f"Consolidate content into {keep_page}. Set up 301 redirects from other pages. Ensure comprehensive coverage of all queries."
    elif recommendation == "differentiate":
        return f"Differentiate pages by targeting distinct search intents. Update titles and content to clearly signal different purposes."
    else:
        return f"Set canonical tag on duplicate pages pointing to {keep_page}."


def _generate_striking_distance_instructions(keyword_opp: Dict[str, Any]) -> str:
    """Generate specific instructions for striking distance keyword."""
    intent = keyword_opp.get("intent", "informational")
    current_pos = keyword_opp.get("current_position", 15)
    
    instructions = []
    
    if intent == "commercial":
        instructions.append("Add comparison tables and product/service details")
    elif intent == "informational":
        instructions.append("Expand with comprehensive how-to content and examples")
    
    if current_pos > 15:
        instructions.append("Build internal links from related content")
    else:
        instructions.append("Optimize existing content depth and structure")
    
    instructions.append("Add relevant schema markup")
    
    return ". ".join(instructions) + "."


def _generate_serp_feature_instructions(keyword_data: Dict[str, Any]) -> str:
    """Generate specific instructions for SERP feature optimization."""
    features = keyword_data.get("features_above", [])
    instructions = []
    
    for feature in features:
        if "featured_snippet" in feature.lower():
            instructions.append("Format content for featured snippet (use clear definitions, lists, tables)")
        elif "paa" in feature.lower() or "people_also_ask" in feature.lower():
            instructions.append("Add FAQ schema and answer related questions in content")
        elif "video" in feature.lower():
            instructions.append("Consider adding video content and video schema")
        elif "local" in feature.lower():
            instructions.append("Optimize local SEO signals (NAP, local schema)")
    
    if not instructions:
        instructions.append("Optimize for SERP features present on this keyword")
    
    return ". ".join(instructions) + "."


def _generate_consolidation_instructions(clusters: List[Dict[str, Any]]) -> str:
    """Generate instructions for consolidation project."""
    total_pages = sum(len(c.get("pages", [])) for c in clusters)
    
    return f"Consolidation project for {len(clusters)} query clusters affecting {total_pages} pages. For each cluster: 1) Choose strongest page as consolidation target, 2) Merge unique content from other pages, 3) Set up 301 redirects, 4) Update internal links, 5) Monitor rankings for 30 days post-consolidation."


def _generate_algorithm_recovery_instructions(updates: List[Dict[str, Any]]) -> str:
    """Generate instructions for algorithm recovery."""
    common_characteristics = []
    for update in updates:
        chars = update.get("common_characteristics", [])
        common_characteristics.extend(chars)
    
    # Find most common characteristics
    from collections import Counter
    char_counts = Counter(common_characteristics)
    top_chars = [char for char, count in char_counts.most_common(3)]
    
    if "thin_content" in top_chars:
        return "Algorithm recovery focused on content depth. Expand thin pages with comprehensive information, examples, and expert insights. Target 1500+ words for key pages."
    elif "no_schema" in top_chars:
        return "Algorithm recovery focused on structured data. Implement comprehensive schema markup across affected pages. Focus on Article, FAQ, and HowTo schemas."
    else:
        return f"Algorithm recovery plan: Analyze {len(updates)} update impacts and address common patterns. Focus on E-E-A-T signals, content quality, and user experience improvements."


def _summarize_health(health: Dict[str, Any]) -> str:
    """Generate concise health summary."""
    direction = health.get("overall_direction", "unknown")
    slope = health.get("trend_slope_pct_per_month", 0)
    
    if direction == "strong_growth":
        return f"Strong growth trajectory at {slope:+.1f}% per month"
    elif direction == "growth":
        return f"Growing at {slope:+.1f}% per month"
    elif direction == "flat":
        return "Stable performance with flat trend"
    elif direction == "decline":
        return f"Declining at {slope:.1f}% per month"
    elif direction == "strong_decline":
        return f"Significant decline at {slope:.1f}% per month"
    else:
        return "Performance trend unclear"


def _calculate_recovery_potential(
    critical: List[Dict[str, Any]],
    quick_wins: List[Dict[str, Any]],
    triage: Dict[str, Any]
) -> int:
    """Calculate total estimated monthly click recovery."""
    total = 0
    
    # From critical actions
    for action in critical:
        total += action.get("impact", 0)
    
    # From quick wins (count only non-growth actions)
    for action in quick_wins:
        if action.get("type") in ["ctr_optimization", "content_refresh"]:
            total += action.get("impact", 0)
    
    return int(total)


def _calculate_growth_potential(
    quick_wins: List[Dict[str, Any]],
    strategic: List[Dict[str, Any]],
    content: Dict[str, Any],
    serp: Dict[str, Any]
) -> int:
    """Calculate total estimated monthly click growth from new opportunities."""
    total = 0
    
    # From striking distance keywords
    for action in quick_wins:
        if action.get("type") == "striking_distance":
            total += action.get("impact", 0)
    
    # From strategic plays
    for action in strategic:
        if isinstance(action.get("impact"), int):
            total += action.get("impact", 0)
    
    return int(total)


def _generate_narrative_with_fallback(synthesis_data: Dict[str, Any]) -> str:
    """
    Generate executive narrative using Claude API with fallback to template-based generation.
    """
    try:
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            return _generate_fallback_narrative(synthesis_data)
        
        client = anthropic.Anthropic(api_key=api_key)
        
        prompt = f"""You are writing the executive summary for a Search Intelligence Report. 

Based on this data, write a compelling 3-paragraph narrative:

Site Health: {synthesis_data['health_summary']}
Critical Issues: {synthesis_data['critical_count']}
Quick Win Opportunities: {synthesis_data['quick_wins_count']}
Strategic Initiatives: {synthesis_data['strategic_count']}
Estimated Monthly Click Recovery: {synthesis_data['total_recovery']:,}
Estimated Monthly Click Growth Potential: {synthesis_data['total_growth']:,}

Top Critical Issues:
{json.dumps(synthesis_data['top_critical'], indent=2)}

Top Quick Wins:
{json.dumps(synthesis_data['top_quick_wins'], indent=2)}

Write in a direct, consultant-grade tone. No fluff. Focus on:
1. Current state and trajectory
2. Immediate priorities and their impact
3. Strategic roadmap and total opportunity

Keep it under 300 words."""

        message = client.messages.create(
            model="claude-3-5-sonnet-20241022",
            max_tokens=1024,
            messages=[
                {"role": "user", "content": prompt}
            ]
        )
        
        return message.content[0].text
        
    except Exception as e:
        print(f"Claude API failed, using fallback narrative: {e}")
        return _generate_fallback_narrative(synthesis_data)


def _generate_fallback_narrative(synthesis_data: Dict[str, Any]) -> str:
    """Generate narrative using templates when API fails."""
    
    health = synthesis_data['health_summary']
    critical_count = synthesis_data['critical_count']
    quick_wins_count = synthesis_data['quick_wins_count']
    total_recovery = synthesis_data['total_recovery']
    total_growth = synthesis_data['total_growth']
    
    # Paragraph 1: Current state
    p1 = f"{health}. "
    
    if critical_count > 0:
        p1 += f"Our analysis identified {critical_count} critical issues requiring immediate attention. "
    else:
        p1 += "No critical issues detected. "
    
    if synthesis_data.get('algorithm_impacts'):
        p1 += f"Recent algorithm updates have impacted performance. "
    
    # Paragraph 2: Immediate priorities
    p2 = f"We've identified {quick_wins_count} quick-win opportunities "
    p2 += f"that can recover an estimated {total_recovery:,} clicks per month. "
    
    if synthesis_data['top_critical']:
        top_issue = synthesis_data['top_critical'][0]
        p2 += f"The highest priority is {top_issue.get('type', 'optimization').replace('_', ' ')}, "
        p2 += f"which affects {top_issue.get('impact', 0):,} monthly clicks. "
    
    # Paragraph 3: Strategic roadmap
    p3 = f"Beyond immediate fixes, we've mapped {synthesis_data['strategic_count']} strategic initiatives "
    p3 += f"with a combined growth potential of {total_growth:,} monthly clicks. "
    
    if total_recovery + total_growth > 1000:
        p3 += f"Total upside opportunity: {total_recovery + total_growth:,} clicks per month."
    
    return f"{p1}\n\n{p2}\n\n{p3}"

