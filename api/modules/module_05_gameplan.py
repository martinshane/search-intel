"""
Module 5: The Gameplan

Synthesizes outputs from Modules 1-4 into a prioritized action list with
critical/quick_wins/strategic/structural categories. Uses Claude API for
narrative generation.
"""

import anthropic
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
import os


@dataclass
class Action:
    """Represents a single action item in the gameplan."""
    action: str
    page_or_keyword: str
    impact_monthly_clicks: int
    effort: str  # "low", "medium", "high"
    category: str  # For tracking which module this came from
    dependencies: Optional[List[str]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result = asdict(self)
        if result['dependencies'] is None:
            result['dependencies'] = []
        return result


class GameplanGenerator:
    """Generates prioritized gameplan from module outputs."""
    
    def __init__(self, anthropic_api_key: Optional[str] = None):
        """Initialize with optional Claude API key."""
        self.anthropic_api_key = anthropic_api_key or os.getenv("ANTHROPIC_API_KEY")
        if self.anthropic_api_key:
            self.client = anthropic.Anthropic(api_key=self.anthropic_api_key)
        else:
            self.client = None
    
    def generate_gameplan(
        self,
        health: Dict[str, Any],
        triage: Dict[str, Any],
        serp: Optional[Dict[str, Any]],
        content: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Synthesize all prior modules into a prioritized action list.
        
        Args:
            health: Output from Module 1 (Health & Trajectory)
            triage: Output from Module 2 (Page-Level Triage)
            serp: Output from Module 3 (SERP Landscape Analysis) - optional
            content: Output from Module 4 (Content Intelligence)
        
        Returns:
            Dictionary with critical/quick_wins/strategic/structural action lists,
            impact estimates, and narrative.
        """
        # Initialize action lists
        critical = []
        quick_wins = []
        strategic = []
        structural = []
        
        # 1. CRITICAL FIXES (do this week)
        critical.extend(self._extract_critical_fixes(triage, content))
        
        # 2. QUICK WINS (do this month)
        quick_wins.extend(self._extract_quick_wins(triage, content, serp))
        
        # 3. STRATEGIC PLAYS (this quarter)
        strategic.extend(self._extract_strategic_plays(health, triage, content, serp))
        
        # 4. STRUCTURAL IMPROVEMENTS (ongoing)
        structural.extend(self._extract_structural_improvements(health, content))
        
        # Calculate total impact
        total_recovery = sum(a['impact_monthly_clicks'] for a in critical + quick_wins)
        total_growth = sum(a['impact_monthly_clicks'] for a in strategic)
        
        # Generate narrative using Claude API
        narrative = self._generate_narrative(
            health=health,
            triage=triage,
            serp=serp,
            content=content,
            critical_count=len(critical),
            quick_wins_count=len(quick_wins),
            strategic_count=len(strategic),
            total_recovery=total_recovery,
            total_growth=total_growth
        )
        
        return {
            "critical": critical,
            "quick_wins": quick_wins,
            "strategic": strategic,
            "structural": structural,
            "total_estimated_monthly_click_recovery": total_recovery,
            "total_estimated_monthly_click_growth": total_growth,
            "narrative": narrative,
            "summary": {
                "total_actions": len(critical) + len(quick_wins) + len(strategic) + len(structural),
                "critical_actions": len(critical),
                "quick_win_actions": len(quick_wins),
                "strategic_actions": len(strategic),
                "structural_actions": len(structural)
            }
        }
    
    def _extract_critical_fixes(
        self,
        triage: Dict[str, Any],
        content: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Extract critical fixes from triage and content modules."""
        actions = []
        
        # Critical decay pages (>100 clicks/month)
        for page in triage.get('pages', []):
            if page.get('bucket') == 'critical' and page.get('current_monthly_clicks', 0) > 100:
                action = Action(
                    action=f"Emergency intervention for critically decaying page. "
                           f"Current position {page.get('avg_position', 'N/A')}, "
                           f"losing {abs(page.get('trend_slope', 0)):.2f} clicks/day. "
                           f"Recommended: {page.get('recommended_action', 'content refresh')}",
                    page_or_keyword=page['url'],
                    impact_monthly_clicks=int(page['current_monthly_clicks'] * 0.5),  # Assume 50% recovery
                    effort="medium",
                    category="critical_decay"
                )
                actions.append(action.to_dict())
        
        # CTR anomalies on high-impression keywords (easy title rewrites)
        for page in triage.get('pages', []):
            if page.get('ctr_anomaly') and page.get('current_monthly_clicks', 0) > 50:
                expected_ctr = page.get('ctr_expected', 0)
                actual_ctr = page.get('ctr_actual', 0)
                ctr_gap = expected_ctr - actual_ctr
                potential_clicks = int(page.get('impressions_monthly', 0) * ctr_gap)
                
                action = Action(
                    action=f"Rewrite title tag and meta description. Currently achieving "
                           f"{actual_ctr:.1%} CTR vs expected {expected_ctr:.1%}. "
                           f"Low-effort, high-impact fix.",
                    page_or_keyword=page['url'],
                    impact_monthly_clicks=potential_clicks,
                    effort="low",
                    category="ctr_optimization"
                )
                actions.append(action.to_dict())
        
        # Cannibalization causing both pages to underperform
        for cluster in content.get('cannibalization_clusters', []):
            if cluster.get('total_impressions_affected', 0) > 1000:
                action = Action(
                    action=f"Resolve cannibalization between {len(cluster['pages'])} pages "
                           f"competing for '{cluster['query_group']}'. "
                           f"Recommendation: {cluster['recommendation']}. "
                           f"Keep: {cluster.get('keep_page', 'TBD')}",
                    page_or_keyword=", ".join(cluster['pages']),
                    impact_monthly_clicks=int(cluster['total_impressions_affected'] * 0.05),
                    effort="medium",
                    category="cannibalization",
                    dependencies=None
                )
                actions.append(action.to_dict())
        
        # Sort by impact and return top items
        actions.sort(key=lambda x: x['impact_monthly_clicks'], reverse=True)
        return actions[:10]  # Limit to top 10 critical items
    
    def _extract_quick_wins(
        self,
        triage: Dict[str, Any],
        content: Dict[str, Any],
        serp: Optional[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Extract quick win opportunities."""
        actions = []
        
        # Striking distance keywords (positions 8-20, high impressions)
        for keyword_data in content.get('striking_distance', []):
            if keyword_data.get('impressions', 0) > 500:
                action = Action(
                    action=f"Optimize for '{keyword_data['query']}' - currently position "
                           f"{keyword_data['current_position']:.1f}. Minor content update "
                           f"could push to top 5. Intent: {keyword_data.get('intent', 'unknown')}",
                    page_or_keyword=keyword_data.get('landing_page', 'N/A'),
                    impact_monthly_clicks=keyword_data.get('estimated_click_gain_if_top5', 0),
                    effort="low",
                    category="striking_distance"
                )
                actions.append(action.to_dict())
        
        # SERP feature opportunities (if SERP data available)
        if serp:
            for opportunity in serp.get('feature_opportunities', []):
                if opportunity.get('estimated_click_gain', 0) > 100:
                    feature_type = opportunity['feature']
                    action_map = {
                        'featured_snippet': 'Add FAQ schema and restructure content with clear Q&A format',
                        'people_also_ask': 'Add FAQ schema targeting these questions',
                        'video_carousel': 'Create and embed video content',
                        'image_pack': 'Optimize images with descriptive alt text and proper sizing'
                    }
                    
                    action = Action(
                        action=f"Capture {feature_type} for '{opportunity['keyword']}'. "
                               f"{action_map.get(feature_type, 'Optimize for this SERP feature')}. "
                               f"Current holder: {opportunity.get('current_holder', 'N/A')}",
                        page_or_keyword=opportunity['keyword'],
                        impact_monthly_clicks=opportunity['estimated_click_gain'],
                        effort=opportunity.get('difficulty', 'medium'),
                        category="serp_feature"
                    )
                    actions.append(action.to_dict())
        
        # Thin content expansions (quick impact)
        for page_data in content.get('thin_content', [])[:5]:  # Top 5 only
            if page_data.get('impressions', 0) > 300:
                action = Action(
                    action=f"Expand thin content. Current word count: {page_data.get('word_count', 0)}. "
                           f"Target: 1500+ words. Add depth, examples, FAQs.",
                    page_or_keyword=page_data['url'],
                    impact_monthly_clicks=int(page_data.get('impressions', 0) * 0.02),
                    effort="medium",
                    category="content_expansion"
                )
                actions.append(action.to_dict())
        
        # Sort by effort (prioritize low effort) then impact
        actions.sort(key=lambda x: (
            0 if x['effort'] == 'low' else 1 if x['effort'] == 'medium' else 2,
            -x['impact_monthly_clicks']
        ))
        return actions[:15]  # Top 15 quick wins
    
    def _extract_strategic_plays(
        self,
        health: Dict[str, Any],
        triage: Dict[str, Any],
        content: Dict[str, Any],
        serp: Optional[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Extract strategic plays for the quarter."""
        actions = []
        
        # Content refresh for "urgent update" quadrant
        urgent_updates = content.get('update_priority_matrix', {}).get('urgent_update', [])
        for page_data in urgent_updates[:10]:
            action = Action(
                action=f"Comprehensive content refresh for aging, decaying page. "
                       f"Last updated: {page_data.get('last_modified', 'unknown')}. "
                       f"Update statistics, add new sections, refresh examples.",
                page_or_keyword=page_data['url'],
                impact_monthly_clicks=int(page_data.get('current_clicks', 0) * 0.3),
                effort="high",
                category="content_refresh"
            )
            actions.append(action.to_dict())
        
        # Consolidation projects
        for cluster in content.get('cannibalization_clusters', []):
            if cluster.get('recommendation') == 'consolidate':
                action = Action(
                    action=f"Consolidation project: merge {len(cluster['pages'])} pages "
                           f"into single authoritative resource. Set up 301 redirects. "
                           f"Combine best content from each page.",
                    page_or_keyword=cluster.get('keep_page', 'TBD'),
                    impact_monthly_clicks=int(cluster.get('total_impressions_affected', 0) * 0.08),
                    effort="high",
                    category="consolidation",
                    dependencies=[f"Content audit of {p}" for p in cluster['pages']]
                )
                actions.append(action.to_dict())
        
        # New content creation for gaps
        # Look for high-impression keywords without corresponding pages
        if serp:
            for keyword_data in serp.get('keywords_analyzed', [])[:20]:
                # Placeholder logic - in real implementation, would check if keyword
                # has a dedicated page or is being captured by generic page
                if keyword_data.get('intent') in ['commercial', 'transactional']:
                    action = Action(
                        action=f"Create dedicated page for '{keyword_data.get('keyword', 'N/A')}' "
                               f"targeting {keyword_data.get('intent')} intent. "
                               f"Current search volume suggests significant opportunity.",
                        page_or_keyword=f"NEW: {keyword_data.get('keyword', 'N/A')}",
                        impact_monthly_clicks=500,  # Conservative estimate for new content
                        effort="high",
                        category="content_gap"
                    )
                    actions.append(action.to_dict())
        
        # Double-down opportunities (new + growing content)
        double_down = content.get('update_priority_matrix', {}).get('double_down', [])
        for page_data in double_down[:5]:
            action = Action(
                action=f"Double down on momentum: increase internal links, build backlinks, "
                       f"expand related content. Page is new and already growing.",
                page_or_keyword=page_data['url'],
                impact_monthly_clicks=int(page_data.get('current_clicks', 0) * 0.5),
                effort="medium",
                category="momentum"
            )
            actions.append(action.to_dict())
        
        actions.sort(key=lambda x: x['impact_monthly_clicks'], reverse=True)
        return actions[:12]  # Top 12 strategic plays
    
    def _extract_structural_improvements(
        self,
        health: Dict[str, Any],
        content: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Extract ongoing structural improvements."""
        actions = []
        
        # Seasonal content calendar
        seasonality = health.get('seasonality', {})
        if seasonality.get('monthly_cycle'):
            action = Action(
                action=f"Implement seasonal content calendar. Traffic pattern shows "
                       f"{seasonality.get('cycle_description', 'recurring patterns')}. "
                       f"Best day: {seasonality.get('best_day', 'N/A')}, "
                       f"worst day: {seasonality.get('worst_day', 'N/A')}. "
                       f"Time content publication and promotions accordingly.",
                page_or_keyword="Site-wide",
                impact_monthly_clicks=0,  # Efficiency gain, not direct traffic
                effort="low",
                category="scheduling"
            )
            actions.append(action.to_dict())
        
        # Internal linking strategy
        action = Action(
            action="Systematic internal linking audit and improvement. "
                   "Review all high-authority pages and ensure they link to conversion pages. "
                   "Add contextual internal links to starved high-potential pages.",
            page_or_keyword="Site-wide",
            impact_monthly_clicks=0,  # Supports other actions
            effort="medium",
            category="internal_linking"
        )
        actions.append(action.to_dict())
        
        # Content update schedule
        action = Action(
            action="Establish quarterly content review schedule. Identify pages that are "
                   "6+ months old and schedule for freshness updates. Prevent future decay.",
            page_or_keyword="Site-wide",
            impact_monthly_clicks=0,
            effort="low",
            category="maintenance"
        )
        actions.append(action.to_dict())
        
        # Template optimization
        action = Action(
            action="Audit site templates for CTR optimization opportunities. "
                   "Ensure all pages have optimal title tag length, compelling meta descriptions, "
                   "and schema markup where applicable.",
            page_or_keyword="Site-wide",
            impact_monthly_clicks=0,
            effort="medium",
            category="technical_seo"
        )
        actions.append(action.to_dict())
        
        return actions
    
    def _generate_narrative(
        self,
        health: Dict[str, Any],
        triage: Dict[str, Any],
        serp: Optional[Dict[str, Any]],
        content: Dict[str, Any],
        critical_count: int,
        quick_wins_count: int,
        strategic_count: int,
        total_recovery: int,
        total_growth: int
    ) -> str:
        """
        Generate human-readable narrative summary using Claude API.
        Falls back to template-based narrative if API is unavailable.
        """
        if not self.client:
            return self._generate_template_narrative(
                health, triage, serp, content,
                critical_count, quick_wins_count, strategic_count,
                total_recovery, total_growth
            )
        
        # Prepare context for Claude
        context = self._prepare_llm_context(
            health, triage, serp, content,
            critical_count, quick_wins_count, strategic_count,
            total_recovery, total_growth
        )
        
        prompt = f"""You are a senior SEO consultant generating an executive summary for a client's search intelligence report.

Based on the analysis data below, write a compelling 3-4 paragraph narrative that:
1. Opens with the current state and trajectory (be direct about problems)
2. Quantifies the opportunity (recovery + growth potential)
3. Outlines the recommended approach (critical → quick wins → strategic)
4. Ends with a clear call to action

Tone: Professional consultant, no fluff, numbers-driven, actionable.
Length: 250-350 words.

ANALYSIS DATA:
{context}

Write the narrative now:"""

        try:
            message = self.client.messages.create(
                model="claude-3-5-sonnet-20241022",
                max_tokens=1024,
                messages=[
                    {"role": "user", "content": prompt}
                ]
            )
            
            narrative = message.content[0].text.strip()
            return narrative
            
        except Exception as e:
            print(f"Claude API error: {e}")
            return self._generate_template_narrative(
                health, triage, serp, content,
                critical_count, quick_wins_count, strategic_count,
                total_recovery, total_growth
            )
    
    def _prepare_llm_context(
        self,
        health: Dict[str, Any],
        triage: Dict[str, Any],
        serp: Optional[Dict[str, Any]],
        content: Dict[str, Any],
        critical_count: int,
        quick_wins_count: int,
        strategic_count: int,
        total_recovery: int,
        total_growth: int
    ) -> str:
        """Prepare structured context for LLM."""
        context_parts = []
        
        # Overall trajectory
        direction = health.get('overall_direction', 'unknown')
        slope = health.get('trend_slope_pct_per_month', 0)
        context_parts.append(f"TRAJECTORY: {direction}, {slope:+.1f}% per month")
        
        # Forecast
        forecast_30d = health.get('forecast', {}).get('30d', {})
        if forecast_30d:
            context_parts.append(
                f"FORECAST: {forecast_30d.get('clicks', 0):,} clicks in 30 days "
                f"(±{forecast_30d.get('ci_high', 0) - forecast_30d.get('clicks', 0):,})"
            )
        
        # Page buckets
        summary = triage.get('summary', {})
        context_parts.append(
            f"PAGES: {summary.get('critical', 0)} critical, "
            f"{summary.get('decaying', 0)} decaying, "
            f"{summary.get('stable', 0)} stable, "
            f"{summary.get('growing', 0)} growing"
        )
        
        # Recoverable clicks
        recoverable = summary.get('total_recoverable_clicks_monthly', 0)
        context_parts.append(f"RECOVERABLE: {recoverable:,} clicks/month")
        
        # Cannibalization
        cannibalization_count = len(content.get('cannibalization_clusters', []))
        if cannibalization_count > 0:
            context_parts.append(f"CANNIBALIZATION: {cannibalization_count} clusters found")
        
        # Striking distance
        striking_distance_count = len(content.get('striking_distance', []))
        context_parts.append(f"STRIKING DISTANCE: {striking_distance_count} keywords positions 8-20")
        
        # SERP insights
        if serp:
            displacement_count = len(serp.get('serp_feature_displacement', []))
            if displacement_count > 0:
                context_parts.append(f"SERP DISPLACEMENT: {displacement_count} keywords affected by features")
        
        # Action summary
        context_parts.append(
            f"GAMEPLAN: {critical_count} critical fixes, "
            f"{quick_wins_count} quick wins, "
            f"{strategic_count} strategic plays"
        )
        
        context_parts.append(
            f"OPPORTUNITY: {total_recovery:,} clicks/month recovery + "
            f"{total_growth:,} clicks/month growth potential"
        )
        
        return "\n".join(context_parts)
    
    def _generate_template_narrative(
        self,
        health: Dict[str, Any],
        triage: Dict[str, Any],
        serp: Optional[Dict[str, Any]],
        content: Dict[str, Any],
        critical_count: int,
        quick_wins_count: int,
        strategic_count: int,
        total_recovery: int,
        total_growth: int
    ) -> str:
        """Generate narrative from template when Claude API unavailable."""
        direction = health.get('overall_direction', 'stable')
        slope = health.get('trend_slope_pct_per_month', 0)
        
        summary = triage.get('summary', {})
        critical_pages = summary.get('critical', 0)
        decaying_pages = summary.get('decaying', 0)
        
        # Opening - current state
        if direction in ['declining', 'strong_decline']:
            opening = (
                f"Your site is currently declining at {abs(slope):.1f}% per month. "
                f"We've identified {critical_pages} pages in critical decay and "
                f"{