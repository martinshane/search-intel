"""
Module 10: Internal Linking Analysis

Analyzes internal link structure from sitemaps and crawl data to identify:
1. Orphan pages (no internal links)
2. Over-linked pages (diluting link equity)
3. Under-linked opportunities (high-value pages needing more links)
4. Strategic linking recommendations

Returns a linking_score (0-100) and actionable recommendations.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
from collections import defaultdict, Counter
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class InternalLinkingAnalyzer:
    """Analyzes internal link structure and provides recommendations."""
    
    # Thresholds for classification
    ORPHAN_MAX_INBOUND = 0
    OVERLINKED_MIN_INBOUND = 100  # Pages with > 100 internal links may be over-linked
    UNDERLINKED_VALUE_THRESHOLD = 100  # Monthly clicks threshold for "high-value"
    UNDERLINKED_MAX_INBOUND = 5  # High-value pages with < 5 links are under-linked
    
    def __init__(self, internal_links_data: pd.DataFrame, 
                 page_performance_data: pd.DataFrame,
                 crawl_data: Optional[pd.DataFrame] = None):
        """
        Initialize the analyzer.
        
        Args:
            internal_links_data: DataFrame with columns [from_url, to_url, anchor_text]
            page_performance_data: DataFrame with GSC metrics [url, clicks, impressions, ctr, position]
            crawl_data: Optional DataFrame with crawl metadata [url, word_count, title, etc.]
        """
        self.internal_links_data = internal_links_data
        self.page_performance_data = page_performance_data
        self.crawl_data = crawl_data
        
        # Build link graph structures
        self.outbound_links = defaultdict(list)  # from_url -> [to_url]
        self.inbound_links = defaultdict(list)   # to_url -> [from_url]
        self.anchor_texts = defaultdict(list)    # to_url -> [anchor_text]
        
        self._build_link_graph()
    
    def _build_link_graph(self):
        """Build internal link graph structures from raw data."""
        if self.internal_links_data.empty:
            logger.warning("No internal links data available")
            return
        
        for _, row in self.internal_links_data.iterrows():
            from_url = row['from_url']
            to_url = row['to_url']
            anchor = row.get('anchor_text', '')
            
            self.outbound_links[from_url].append(to_url)
            self.inbound_links[to_url].append(from_url)
            if anchor:
                self.anchor_texts[to_url].append(anchor)
    
    def _normalize_url(self, url: str) -> str:
        """Normalize URL for consistent matching."""
        # Remove trailing slashes, lowercase, remove fragments
        url = url.lower().rstrip('/')
        if '#' in url:
            url = url.split('#')[0]
        return url
    
    def _get_page_value_score(self, url: str) -> float:
        """
        Calculate page value based on current performance.
        
        Higher score = more valuable page that should receive more internal links.
        
        Score factors:
        - Monthly clicks (primary)
        - Average position (better position = easier to improve)
        - CTR performance
        - Impressions (search demand)
        """
        normalized_url = self._normalize_url(url)
        
        # Find matching page in performance data
        page_data = self.page_performance_data[
            self.page_performance_data['url'].apply(self._normalize_url) == normalized_url
        ]
        
        if page_data.empty:
            return 0.0
        
        page = page_data.iloc[0]
        
        clicks = page.get('clicks', 0)
        impressions = page.get('impressions', 0)
        position = page.get('position', 100)
        ctr = page.get('ctr', 0)
        
        # Calculate value score (0-100 scale)
        # Primary factor: clicks (weighted heavily)
        click_score = min(clicks / 10, 50)  # Up to 50 points, 1000 clicks = max
        
        # Position bonus: pages on page 1-2 are easier to improve
        position_score = max(0, 20 - position) if position <= 20 else 0  # Up to 20 points
        
        # Impressions factor: high demand
        impression_score = min(impressions / 1000, 20)  # Up to 20 points
        
        # CTR factor: pages performing well deserve more links
        expected_ctr = self._get_expected_ctr(position)
        ctr_performance = (ctr / expected_ctr) if expected_ctr > 0 else 1.0
        ctr_score = min(ctr_performance * 10, 10)  # Up to 10 points
        
        total_score = click_score + position_score + impression_score + ctr_score
        
        return min(total_score, 100)
    
    def _get_expected_ctr(self, position: float) -> float:
        """Get expected CTR based on position using industry benchmarks."""
        if position <= 1:
            return 0.35
        elif position <= 2:
            return 0.25
        elif position <= 3:
            return 0.18
        elif position <= 5:
            return 0.10
        elif position <= 10:
            return 0.05
        elif position <= 20:
            return 0.02
        else:
            return 0.01
    
    def identify_orphan_pages(self) -> List[Dict[str, Any]]:
        """
        Identify orphan pages (pages with no internal links pointing to them).
        
        Returns:
            List of orphan page dictionaries with metadata and recommendations
        """
        orphans = []
        
        # Get all URLs from performance data
        all_pages = set(self.page_performance_data['url'].apply(self._normalize_url))
        
        # Get all URLs that have inbound links
        linked_pages = set(self._normalize_url(url) for url in self.inbound_links.keys())
        
        # Orphans = pages in GSC but not in internal link graph
        orphan_urls = all_pages - linked_pages
        
        for url in orphan_urls:
            # Get performance data
            page_data = self.page_performance_data[
                self.page_performance_data['url'].apply(self._normalize_url) == url
            ]
            
            if page_data.empty:
                continue
            
            page = page_data.iloc[0]
            clicks = page.get('clicks', 0)
            impressions = page.get('impressions', 0)
            position = page.get('position', 100)
            
            # Calculate opportunity score (orphans with traffic are high priority)
            opportunity_score = self._get_page_value_score(url)
            
            # Determine severity
            if clicks > 50:
                severity = "critical"
            elif clicks > 10:
                severity = "high"
            elif impressions > 100:
                severity = "medium"
            else:
                severity = "low"
            
            orphans.append({
                "url": page['url'],
                "monthly_clicks": clicks,
                "impressions": impressions,
                "avg_position": round(position, 1),
                "opportunity_score": round(opportunity_score, 1),
                "severity": severity,
                "recommendation": self._generate_orphan_recommendation(page, opportunity_score)
            })
        
        # Sort by opportunity score descending
        orphans.sort(key=lambda x: x['opportunity_score'], reverse=True)
        
        return orphans
    
    def _generate_orphan_recommendation(self, page: pd.Series, opportunity_score: float) -> str:
        """Generate specific recommendation for an orphan page."""
        clicks = page.get('clicks', 0)
        position = page.get('position', 100)
        
        if opportunity_score > 50:
            return f"URGENT: Add internal links from related high-authority pages. This orphan is ranking #{int(position)} and getting {int(clicks)} clicks despite no internal links."
        elif opportunity_score > 20:
            return f"Add 3-5 contextual internal links from topically relevant pages to boost rankings from position #{int(position)}."
        elif clicks > 0:
            return f"Add at least 2 internal links to maintain current rankings and prevent decay."
        else:
            return "Evaluate if this page should be indexed. If yes, add internal links from main navigation or related content."
    
    def identify_overlinked_pages(self) -> List[Dict[str, Any]]:
        """
        Identify pages with excessive inbound internal links.
        
        Over-linked pages may indicate:
        1. Unnecessary site-wide footer/header links
        2. Automated internal linking gone wrong
        3. Link equity dilution
        
        Returns:
            List of over-linked page dictionaries with analysis
        """
        overlinked = []
        
        for url, inbound_list in self.inbound_links.items():
            inbound_count = len(inbound_list)
            
            if inbound_count < self.OVERLINKED_MIN_INBOUND:
                continue
            
            # Get performance data
            normalized_url = self._normalize_url(url)
            page_data = self.page_performance_data[
                self.page_performance_data['url'].apply(self._normalize_url) == normalized_url
            ]
            
            if page_data.empty:
                # Page has links but no GSC data (may be non-indexable)
                clicks = 0
                position = 100
                page_url = url
            else:
                page = page_data.iloc[0]
                clicks = page.get('clicks', 0)
                position = page.get('position', 100)
                page_url = page['url']
            
            # Analyze anchor text diversity
            anchors = self.anchor_texts.get(url, [])
            unique_anchors = len(set(anchors))
            anchor_diversity = unique_anchors / len(anchors) if anchors else 0
            
            # Analyze link sources
            unique_sources = len(set(inbound_list))
            source_diversity = unique_sources / len(inbound_list)
            
            # Detect site-wide links (same source repeated many times)
            source_counts = Counter(inbound_list)
            max_source_count = max(source_counts.values()) if source_counts else 0
            is_sitewide = max_source_count > (inbound_count * 0.5)
            
            # Calculate dilution risk
            # High risk if: many links, low diversity, poor performance
            dilution_risk = "low"
            if inbound_count > 500 and anchor_diversity < 0.1:
                dilution_risk = "critical"
            elif inbound_count > 300 and (anchor_diversity < 0.2 or is_sitewide):
                dilution_risk = "high"
            elif inbound_count > 200:
                dilution_risk = "medium"
            
            overlinked.append({
                "url": page_url,
                "inbound_links_count": inbound_count,
                "unique_sources": unique_sources,
                "source_diversity": round(source_diversity, 2),
                "anchor_diversity": round(anchor_diversity, 2),
                "is_sitewide_link": is_sitewide,
                "monthly_clicks": clicks,
                "avg_position": round(position, 1),
                "dilution_risk": dilution_risk,
                "recommendation": self._generate_overlinked_recommendation(
                    inbound_count, anchor_diversity, is_sitewide, dilution_risk
                )
            })
        
        # Sort by dilution risk (critical first) then by inbound count
        risk_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
        overlinked.sort(key=lambda x: (risk_order[x['dilution_risk']], -x['inbound_links_count']))
        
        return overlinked
    
    def _generate_overlinked_recommendation(self, count: int, diversity: float, 
                                           is_sitewide: bool, risk: str) -> str:
        """Generate specific recommendation for an over-linked page."""
        if risk == "critical":
            return f"CRITICAL: Remove site-wide footer/header links. {count} links with only {diversity:.0%} anchor diversity is diluting link equity."
        elif is_sitewide:
            return f"Remove site-wide links or use nofollow. {count} links from same template are not providing value."
        elif risk == "high":
            return f"Audit and remove low-value internal links. Focus on keeping only the most contextually relevant {int(count * 0.2)} links."
        elif diversity < 0.3:
            return f"Diversify anchor text and reduce automated internal linking. Current diversity: {diversity:.0%}."
        else:
            return f"Monitor link growth. While {count} links is high, diversity metrics are acceptable."
    
    def identify_underlinked_opportunities(self) -> List[Dict[str, Any]]:
        """
        Identify high-value pages that should receive more internal links.
        
        These are pages that:
        1. Have strong performance (clicks, good position)
        2. Have relatively few inbound internal links
        3. Could rank even better with more internal link equity
        
        Returns:
            List of under-linked opportunity dictionaries
        """
        opportunities = []
        
        for _, page in self.page_performance_data.iterrows():
            url = page['url']
            normalized_url = self._normalize_url(url)
            
            clicks = page.get('clicks', 0)
            impressions = page.get('impressions', 0)
            position = page.get('position', 100)
            
            # Only consider pages with meaningful traffic
            if clicks < self.UNDERLINKED_VALUE_THRESHOLD:
                continue
            
            # Count inbound links
            inbound_count = len(self.inbound_links.get(url, []))
            
            # Only flag if significantly under-linked
            if inbound_count >= self.UNDERLINKED_MAX_INBOUND:
                continue
            
            # Calculate opportunity metrics
            value_score = self._get_page_value_score(url)
            
            # Calculate potential impact
            # Pages on page 1-2 with few links have highest improvement potential
            if position <= 10:
                potential_ranking_gain = 3  # Could move up 3 spots
            elif position <= 20:
                potential_ranking_gain = 5
            else:
                potential_ranking_gain = 2
            
            # Estimate click gain from ranking improvement
            current_ctr = self._get_expected_ctr(position)
            improved_ctr = self._get_expected_ctr(max(1, position - potential_ranking_gain))
            estimated_click_gain = impressions * (improved_ctr - current_ctr)
            
            # Find potential link sources
            suggested_sources = self._find_link_sources(url, page)
            
            opportunities.append({
                "url": url,
                "monthly_clicks": clicks,
                "impressions": impressions,
                "current_position": round(position, 1),
                "current_inbound_links": inbound_count,
                "value_score": round(value_score, 1),
                "potential_ranking_gain": potential_ranking_gain,
                "estimated_monthly_click_gain": round(estimated_click_gain),
                "priority": self._calculate_opportunity_priority(
                    value_score, estimated_click_gain, position
                ),
                "suggested_link_count": self._suggest_link_count(position, inbound_count),
                "suggested_sources": suggested_sources[:5],  # Top 5 sources
                "recommendation": self._generate_underlinked_recommendation(
                    position, inbound_count, estimated_click_gain
                )
            })
        
        # Sort by priority score descending
        opportunities.sort(key=lambda x: (
            {"critical": 0, "high": 1, "medium": 2, "low": 3}[x['priority']],
            -x['estimated_monthly_click_gain']
        ))
        
        return opportunities
    
    def _calculate_opportunity_priority(self, value_score: float, 
                                       click_gain: float, position: float) -> str:
        """Calculate priority level for under-linked opportunity."""
        if value_score > 60 and click_gain > 100 and position <= 10:
            return "critical"
        elif value_score > 40 and click_gain > 50:
            return "high"
        elif click_gain > 20:
            return "medium"
        else:
            return "low"
    
    def _suggest_link_count(self, position: float, current_links: int) -> int:
        """Suggest optimal number of internal links to add."""
        if position <= 5:
            target = 10  # Top 5 pages should have 10+ links
        elif position <= 10:
            target = 8
        elif position <= 20:
            target = 5
        else:
            target = 3
        
        return max(1, target - current_links)
    
    def _find_link_sources(self, target_url: str, page_data: pd.Series) -> List[Dict[str, Any]]:
        """
        Find potential pages to link from to the target URL.
        
        Ideal source pages:
        1. Topically related (similar keywords)
        2. Have higher authority (more inbound links)
        3. Already perform well
        4. Not already linking to target
        """
        sources = []
        normalized_target = self._normalize_url(target_url)
        
        # Get pages already linking to target
        existing_sources = set(self._normalize_url(url) for url in 
                              self.inbound_links.get(target_url, []))
        
        # Get target page keywords (from GSC data if available)
        # For simplicity, we'll use high-performing pages as potential sources
        
        for _, potential_source in self.page_performance_data.iterrows():
            source_url = potential_source['url']
            normalized_source = self._normalize_url(source_url)
            
            # Skip if already linking
            if normalized_source in existing_sources:
                continue
            
            # Skip if same page
            if normalized_source == normalized_target:
                continue
            
            source_clicks = potential_source.get('clicks', 0)
            source_position = potential_source.get('position', 100)
            
            # Prefer high-performing pages as sources
            if source_clicks < 10:
                continue
            
            # Count outbound links from this source
            outbound_count = len(self.outbound_links.get(source_url, []))
            
            # Calculate source quality score
            inbound_to_source = len(self.inbound_links.get(source_url, []))
            authority_score = min(inbound_to_source / 10, 10)  # Up to 10 points
            performance_score = min(source_clicks / 10, 10)  # Up to 10 points
            position_score = max(0, 20 - source_position) if source_position <= 20 else 0
            
            # Penalize if source already has many outbound links
            outbound_penalty = min(outbound_count / 10, 5)
            
            quality_score = authority_score + performance_score + position_score - outbound_penalty
            
            sources.append({
                "url": source_url,
                "quality_score": round(quality_score, 1),
                "monthly_clicks": source_clicks,
                "current_outbound_links": outbound_count,
                "reason": self._explain_source_suggestion(quality_score, source_clicks, source_position)
            })
        
        # Sort by quality score descending
        sources.sort(key=lambda x: x['quality_score'], reverse=True)
        
        return sources
    
    def _explain_source_suggestion(self, quality_score: float, 
                                   clicks: float, position: float) -> str:
        """Explain why this page is a good link source."""
        if quality_score > 20:
            return f"High-authority page (position #{int(position)}, {int(clicks)} clicks/month)"
        elif quality_score > 15:
            return f"Strong performing page with good linking potential"
        elif clicks > 100:
            return f"High-traffic page that could pass link equity"
        else:
            return "Contextually relevant page"
    
    def _generate_underlinked_recommendation(self, position: float, 
                                            current_links: int, click_gain: float) -> str:
        """Generate specific recommendation for an under-linked page."""
        if position <= 5 and current_links < 3:
            return f"URGENT: Add 5-7 contextual internal links from high-authority pages. Could gain ~{int(click_gain)} clicks/month with better link support."
        elif position <= 10:
            return f"Add 3-5 internal links from related content to boost from position #{int(position)} to top 5. Estimated gain: {int(click_gain)} clicks/month."
        elif position <= 20:
            return f"Add internal links to push from page 2 (#{int(position)}) to page 1. Target 5+ contextual links."
        else:
            return f"Add internal links as part of broader optimization strategy. Estimated click gain: {int(click_gain)}/month."
    
    def calculate_linking_score(self, orphans: List[Dict], 
                                overlinked: List[Dict],
                                underlinked: List[Dict]) -> float:
        """
        Calculate overall internal linking health score (0-100).
        
        Score factors:
        - Orphan pages (negative impact, especially high-traffic orphans)
        - Over-linked pages (negative impact based on dilution risk)
        - Under-linked opportunities (negative impact, unrealized potential)
        - Overall link graph health
        
        Higher score = healthier internal linking structure
        """
        score = 100.0
        
        # Penalty for orphans
        if orphans:
            total_orphan_clicks = sum(o['monthly_clicks'] for o in orphans)
            critical_orphans = len([o for o in orphans if o['severity'] == 'critical'])
            
            # Deduct up to 30 points for orphans
            orphan_penalty = min(30, (len(orphans) / 10) * 10 + critical_orphans * 5)
            score -= orphan_penalty
        
        # Penalty for over-linked pages
        if overlinked:
            critical_overlinked = len([o for o in overlinked if o['dilution_risk'] == 'critical'])
            high_risk = len([o for o in overlinked if o['dilution_risk'] == 'high'])
            
            # Deduct up to 25 points for over-linking issues
            overlink_penalty = min(25, critical_overlinked * 10 + high_risk * 5)
            score -= overlink_penalty
        
        # Penalty for under-linked opportunities
        if underlinked:
            critical_underlinked = len([o for o in underlinked if o['priority'] == 'critical'])
            total_unrealized_clicks = sum(o['estimated_monthly_click_gain'] for o in underlinked)
            
            # Deduct up to 25 points for missed opportunities
            underlink_penalty = min(25, (total_unrealized_clicks / 100) + critical_underlinked * 5)
            score -= underlink_penalty
        
        # Bonus for good link distribution
        if self.inbound_links:
            link_counts = [len(links) for links in self.inbound_links.values()]
            avg_links = np.mean(link_counts)
            std_links = np.std(link_counts)
            
            # Good distribution: average 3-10 links per page, low variance
            if 3 <= avg_links <= 10 and std_links < avg_links:
                score += 5
        
        # Ensure score stays in 0-100 range
        return max(0, min(100, score))
    
    def generate_summary_insights(self, orphans: List[Dict], 
                                 overlinked: List[Dict],
                                 underlinked: List[Dict],
                                 linking_score: float) -> Dict[str, Any]:
        """Generate high-level summary insights about internal linking."""
        total_pages_analyzed = len(set(self.page_performance_data['url']))
        total_links = len(self.internal_links_data) if not self.internal_links_data.empty else 0
        
        # Calculate metrics
        pages_with_links = len(self.inbound_links)
        avg_inbound = np.mean([len(links) for links in self.inbound_links.values()]) if self.inbound_links else 0
        
        # Identify most common issues
        issues = []
        if len(orphans) > total_pages_analyzed * 0.1:
            issues.append("high_orphan_rate")
        if any(o['severity'] == 'critical' for o in orphans):
            issues.append("critical_orphans")
        if any(o['dilution_risk'] == 'critical' for o in overlinked):
            issues.append("link_equity_dilution")
        if len(underlinked) > 10:
            issues.append("many_missed_opportunities")
        
        # Calculate total opportunity
        total_recoverable_clicks = sum(o['monthly_clicks'] for o in orphans if o['monthly_clicks'] > 0)
        total_potential_clicks = sum(o['estimated_monthly_click_gain'] for o in underlinked)
        
        # Generate health assessment
        if linking_score >= 80:
            health = "excellent"
            summary = "Internal linking structure is strong with minimal issues."
        elif linking_score >= 60:
            health = "good"
            summary = "Internal linking is functional but has opportunities for improvement."
        elif linking_score >= 40:
            health = "fair"
            summary = "Internal linking structure has significant issues that should be addressed."
        else:
            health = "poor"
            summary = "Internal linking requires urgent attention. Multiple critical issues detected."
        
        return {
            "total_pages_analyzed": total_pages_analyzed,
            "total_internal_links": total_links,
            "pages_with_inbound_links": pages_with_links,
            "avg_inbound_links_per_page": round(avg_inbound, 1),
            "orphan_page_count": len(orphans),
            "overlinked_page_count": len(overlinked),
            "underlinked_opportunity_count": len(underlinked),
            "linking_health": health,
            "linking_score": round(linking_score, 1),
            "primary_issues": issues,
            "total_recoverable_clicks_monthly": round(total_recoverable_clicks),
            "total_potential_clicks_monthly": round(total_potential_clicks),
            "summary": summary,
            "top_priority_action": self._get_top_priority_action(orphans, overlinked, underlinked)
        }
    
    def _get_top_priority_action(self, orphans: List[Dict], 
                                 overlinked: List[Dict],
                                 underlinked: List[Dict]) -> str:
        """Identify the single most important action to take."""
        # Check for critical orphans
        critical_orphans = [o for o in orphans if o['severity'] == 'critical']
        if critical_orphans:
            top_orphan = max(critical_orphans, key=lambda x: x['monthly_clicks'])
            return f"Add internal links to orphan page: {top_orphan['url']} ({int(top_orphan['monthly_clicks'])} clicks/month at risk)"
        
        # Check for critical over-linking
        critical_overlinked = [o for o in overlinked if o['dilution_risk'] == 'critical']
        if critical_overlinked:
            return f"Remove site-wide links from {critical_overlinked[0]['url']} ({critical_overlinked[0]['inbound_links_count']} excessive links)"
        
        # Check for high-value under-linked pages
        critical_underlinked = [o for o in underlinked if o['priority'] == 'critical']
        if critical_underlinked:
            top_opp = critical_underlinked[0]
            return f"Add internal links to high-value page: {top_opp['url']} (potential +{int(top_opp['estimated_monthly_click_gain'])} clicks/month)"
        
        # Default action
        if orphans:
            return f"Address {len(orphans)} orphan pages to improve site connectivity"
        elif underlinked:
            return f"Optimize internal linking for {len(underlinked)} under-linked pages"
        else:
            return "Maintain current internal linking structure"


def analyze_internal_linking(internal_links_data: pd.DataFrame,
                            page_performance_data: pd.DataFrame,
                            crawl_data: Optional[pd.DataFrame] = None) -> Dict[str, Any]:
    """
    Main entry point for Module 10 analysis.
    
    Args:
        internal_links_data: DataFrame with internal link graph [from_url, to_url, anchor_text]
        page_performance_data: GSC performance data [url, clicks, impressions, ctr, position]
        crawl_data: Optional crawl metadata
    
    Returns:
        Dictionary with:
        - orphan_pages: List of pages with no internal links
        - over_linked_pages: List of pages with excessive links
        - under_linked_opportunities: List of high-value pages needing more links
        - linking_score: Overall health score (0-100)
        - summary: High-level insights and metrics
        - recommendations: Top priority actions
    """
    try:
        logger.info("Starting Module 10: Internal Linking Analysis")
        
        # Initialize analyzer
        analyzer = InternalLinkingAnalyzer(
            internal_links_data=internal_links_data,
            page_performance_data=page_performance_data,
            crawl_data=crawl_data
        )
        
        # Run analyses
        logger.info("Identifying orphan pages...")
        orphans = analyzer.identify_orphan_pages()
        
        logger.info("Identifying over-linked pages...")
        overlinked = analyzer.identify_overlinked_pages()
        
        logger.info("Identifying under-linked opportunities...")
        underlinked = analyzer.identify_underlinked_opportunities()
        
        # Calculate overall score
        logger.info("Calculating linking health score...")
        linking_score = analyzer.calculate_linking_score(orphans, overlinked, underlinked)
        
        # Generate summary
        summary = analyzer.generate_summary_insights(orphans, overlinked, underlinked, linking_score)
        
        # Compile results
        results = {
            "orphan_pages": orphans[:50],  # Limit to top 50
            "over_linked_pages": overlinked[:30],  # Limit to top 30
            "under_linked_opportunities": underlinked[:50],  # Limit to top 50
            "linking_score": round(linking_score, 1),
            "summary": summary,
            "analysis_timestamp": datetime.utcnow().isoformat(),
            "module": "internal_linking"
        }
        
        logger.info(f"Module 10 complete. Linking score: {linking_score:.1f}")
        return results
        
    except Exception as e:
        logger.error(f"Error in Module 10 analysis: {str(e)}", exc_info=True)
        raise


# Example usage for testing
if __name__ == "__main__":
    # Create sample data
    sample_links = pd.DataFrame({
        'from_url': ['/home', '/home', '/blog', '/products', '/home'] * 20,
        'to_url': ['/blog', '/products', '/blog-post-1', '/product-1', '/about'] * 20,
        'anchor_text': ['Blog', 'Products', 'Read more', 'View product', 'About us'] * 20
    })
    
    sample_performance = pd.DataFrame({
        'url': ['/blog-post-1', '/product-1', '/orphan-page', '/overlinked'],
        'clicks': [150, 300, 80, 50],
        'impressions': [5000, 8000, 1000, 2000],
        'ctr': [0.03, 0.0375, 0.08, 0.025],
        'position': [8.5, 5.2, 15.0, 20.0]
    })
    
    results = analyze_internal_linking(sample_links, sample_performance)
    print(f"Linking Score: {results['linking_score']}")
    print(f"Orphan Pages: {len(results['orphan_pages'])}")
    print(f"Over-linked Pages: {len(results['over_linked_pages'])}")
    print(f"Under-linked Opportunities: {len(results['under_linked_opportunities'])}")
