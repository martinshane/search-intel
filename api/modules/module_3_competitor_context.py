"""
Module 3: Competitor Context Analysis

Analyzes competitive landscape using DataForSEO SERP data to identify:
- Top competitors by keyword overlap
- Competitive density and market positioning
- Content gaps and opportunities
- Strategic competitive insights
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from collections import defaultdict, Counter
import pandas as pd
import numpy as np
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class CompetitorMetrics:
    """Metrics for a single competitor"""
    domain: str
    keywords_shared: int
    total_impressions_shared: int
    avg_position: float
    position_better_count: int  # Times they rank higher
    position_worse_count: int  # Times we rank higher
    overlap_percentage: float
    threat_level: str  # low, medium, high, critical


@dataclass
class KeywordOverlap:
    """Overlap analysis for a specific keyword"""
    keyword: str
    user_position: int
    user_url: str
    competitor_domains: List[Dict[str, Any]]  # domain, position, url
    impressions: int
    clicks: int
    competitive_density: float  # 0-1 score


class CompetitorContextAnalyzer:
    """Analyzes competitive landscape from SERP data"""
    
    def __init__(self, serp_data: List[Dict], gsc_keyword_data: pd.DataFrame, 
                 user_domain: str, config: Optional[Dict] = None):
        """
        Initialize analyzer
        
        Args:
            serp_data: Live SERP results from DataForSEO
            gsc_keyword_data: GSC performance data by keyword
            user_domain: The user's domain
            config: Configuration options
        """
        self.serp_data = serp_data
        self.gsc_data = gsc_keyword_data
        self.user_domain = self._normalize_domain(user_domain)
        self.config = config or {}
        
        # Analysis parameters
        self.min_overlap_threshold = self.config.get('min_overlap_threshold', 3)
        self.top_n_competitors = self.config.get('top_n_competitors', 10)
        self.analyze_top_positions = self.config.get('analyze_top_positions', 20)
        
    def _normalize_domain(self, domain: str) -> str:
        """Normalize domain to consistent format"""
        domain = domain.lower()
        domain = domain.replace('https://', '').replace('http://', '')
        domain = domain.replace('www.', '')
        domain = domain.split('/')[0]
        return domain
    
    def _extract_domain_from_url(self, url: str) -> str:
        """Extract and normalize domain from URL"""
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            domain = parsed.netloc or parsed.path
            return self._normalize_domain(domain)
        except Exception as e:
            logger.warning(f"Error parsing URL {url}: {e}")
            return ""
    
    def _calculate_competitive_density(self, serp_result: Dict, 
                                      user_position: Optional[int]) -> float:
        """
        Calculate competitive density score for a keyword
        
        Factors:
        - Number of strong domains in top 10
        - Presence of high-authority sites (news, gov, edu)
        - Domain diversity vs dominance
        - SERP feature competition
        """
        try:
            organic_results = serp_result.get('items', [])[:10]
            
            if not organic_results:
                return 0.0
            
            scores = []
            
            # Factor 1: Number of unique domains in top 10
            domains = [self._extract_domain_from_url(r.get('url', '')) 
                      for r in organic_results]
            unique_domains = len(set(d for d in domains if d))
            domain_diversity_score = unique_domains / 10.0
            scores.append(domain_diversity_score)
            
            # Factor 2: High-authority domain presence
            authority_domains = ['.gov', '.edu', '.org']
            authority_count = sum(1 for d in domains 
                                if any(d.endswith(ext) for ext in authority_domains))
            authority_score = min(authority_count / 3.0, 1.0)
            scores.append(authority_score)
            
            # Factor 3: Major publisher presence (news, big brands)
            major_publishers = [
                'nytimes.com', 'wsj.com', 'forbes.com', 'bloomberg.com',
                'reuters.com', 'bbc.com', 'cnn.com', 'wikipedia.org',
                'amazon.com', 'youtube.com'
            ]
            major_pub_count = sum(1 for d in domains 
                                 if any(pub in d for pub in major_publishers))
            major_pub_score = min(major_pub_count / 3.0, 1.0)
            scores.append(major_pub_score)
            
            # Factor 4: Domain concentration (inverse - lower is more competitive)
            domain_counts = Counter(domains)
            max_domain_count = max(domain_counts.values()) if domain_counts else 1
            concentration_score = max_domain_count / len(organic_results)
            concentration_penalty = 1.0 - concentration_score
            scores.append(concentration_penalty)
            
            # Factor 5: SERP features that increase competition
            features = serp_result.get('features', {})
            competitive_features = [
                'featured_snippet', 'knowledge_panel', 'local_pack',
                'shopping_results', 'top_stories', 'video_carousel'
            ]
            feature_count = sum(1 for f in competitive_features 
                              if features.get(f))
            feature_score = min(feature_count / 3.0, 1.0)
            scores.append(feature_score)
            
            # Factor 6: Position stability (if user is present)
            # Higher positions = more competition
            if user_position and 1 <= user_position <= 20:
                position_score = 1.0 - ((user_position - 1) / 20.0)
                scores.append(position_score)
            
            # Weighted average
            weights = [0.2, 0.15, 0.15, 0.2, 0.15, 0.15]
            if len(scores) < len(weights):
                weights = weights[:len(scores)]
            
            density = np.average(scores, weights=weights)
            return float(np.clip(density, 0.0, 1.0))
            
        except Exception as e:
            logger.error(f"Error calculating competitive density: {e}")
            return 0.5
    
    def _identify_competitors(self) -> Dict[str, CompetitorMetrics]:
        """
        Identify top competitors by analyzing SERP overlap
        
        Returns dictionary mapping domain -> CompetitorMetrics
        """
        competitor_data = defaultdict(lambda: {
            'keywords': [],
            'positions': [],
            'user_positions': [],
            'impressions': [],
            'better_count': 0,
            'worse_count': 0
        })
        
        total_keywords_analyzed = 0
        
        for serp_result in self.serp_data:
            try:
                keyword = serp_result.get('keyword', '')
                if not keyword:
                    continue
                
                total_keywords_analyzed += 1
                
                # Get GSC data for this keyword
                gsc_row = self.gsc_data[
                    self.gsc_data['query'].str.lower() == keyword.lower()
                ]
                
                impressions = 0
                user_position = None
                
                if not gsc_row.empty:
                    impressions = int(gsc_row.iloc[0].get('impressions', 0))
                    user_position = float(gsc_row.iloc[0].get('position', 999))
                
                # Extract organic results
                organic_results = serp_result.get('items', [])[:self.analyze_top_positions]
                
                for result in organic_results:
                    url = result.get('url', '')
                    domain = self._extract_domain_from_url(url)
                    position = result.get('rank_absolute', 999)
                    
                    if not domain or domain == self.user_domain:
                        continue
                    
                    competitor_data[domain]['keywords'].append(keyword)
                    competitor_data[domain]['positions'].append(position)
                    competitor_data[domain]['impressions'].append(impressions)
                    
                    if user_position:
                        competitor_data[domain]['user_positions'].append(user_position)
                        if position < user_position:
                            competitor_data[domain]['better_count'] += 1
                        else:
                            competitor_data[domain]['worse_count'] += 1
                            
            except Exception as e:
                logger.error(f"Error processing SERP result: {e}")
                continue
        
        # Convert to CompetitorMetrics objects
        competitors = {}
        
        for domain, data in competitor_data.items():
            if len(data['keywords']) < self.min_overlap_threshold:
                continue
            
            keywords_shared = len(set(data['keywords']))
            total_impressions = sum(data['impressions'])
            avg_position = np.mean(data['positions']) if data['positions'] else 999
            
            overlap_pct = (keywords_shared / total_keywords_analyzed * 100) \
                         if total_keywords_analyzed > 0 else 0
            
            # Determine threat level
            if overlap_pct > 50 and avg_position < 5:
                threat_level = 'critical'
            elif overlap_pct > 30 and avg_position < 10:
                threat_level = 'high'
            elif overlap_pct > 15 or avg_position < 15:
                threat_level = 'medium'
            else:
                threat_level = 'low'
            
            competitors[domain] = CompetitorMetrics(
                domain=domain,
                keywords_shared=keywords_shared,
                total_impressions_shared=total_impressions,
                avg_position=float(avg_position),
                position_better_count=data['better_count'],
                position_worse_count=data['worse_count'],
                overlap_percentage=float(overlap_pct),
                threat_level=threat_level
            )
        
        return competitors
    
    def _analyze_keyword_overlaps(self, 
                                  competitors: Dict[str, CompetitorMetrics]) -> List[KeywordOverlap]:
        """
        Analyze detailed keyword-level overlap with competitors
        """
        keyword_overlaps = []
        
        for serp_result in self.serp_data:
            try:
                keyword = serp_result.get('keyword', '')
                if not keyword:
                    continue
                
                # Get GSC data
                gsc_row = self.gsc_data[
                    self.gsc_data['query'].str.lower() == keyword.lower()
                ]
                
                impressions = 0
                clicks = 0
                user_position = None
                user_url = None
                
                if not gsc_row.empty:
                    impressions = int(gsc_row.iloc[0].get('impressions', 0))
                    clicks = int(gsc_row.iloc[0].get('clicks', 0))
                    user_position = float(gsc_row.iloc[0].get('position', 999))
                    user_url = gsc_row.iloc[0].get('page', '')
                
                # Extract competitor positions
                organic_results = serp_result.get('items', [])[:20]
                competitor_domains_for_kw = []
                
                for result in organic_results:
                    url = result.get('url', '')
                    domain = self._extract_domain_from_url(url)
                    position = result.get('rank_absolute', 999)
                    
                    if domain == self.user_domain:
                        if not user_position:
                            user_position = position
                        if not user_url:
                            user_url = url
                        continue
                    
                    if domain in competitors:
                        competitor_domains_for_kw.append({
                            'domain': domain,
                            'position': position,
                            'url': url,
                            'threat_level': competitors[domain].threat_level
                        })
                
                # Calculate competitive density
                density = self._calculate_competitive_density(serp_result, user_position)
                
                if competitor_domains_for_kw:
                    keyword_overlaps.append(KeywordOverlap(
                        keyword=keyword,
                        user_position=int(user_position) if user_position else 999,
                        user_url=user_url or '',
                        competitor_domains=competitor_domains_for_kw,
                        impressions=impressions,
                        clicks=clicks,
                        competitive_density=float(density)
                    ))
                    
            except Exception as e:
                logger.error(f"Error analyzing keyword overlap: {e}")
                continue
        
        return keyword_overlaps
    
    def _identify_content_gaps(self, 
                               competitors: Dict[str, CompetitorMetrics],
                               keyword_overlaps: List[KeywordOverlap]) -> List[Dict[str, Any]]:
        """
        Identify content gaps - keywords where competitors rank but user doesn't
        """
        gaps = []
        
        # Keywords where user has poor/no ranking but competitors do well
        for overlap in keyword_overlaps:
            if overlap.user_position > 20 or overlap.user_position == 999:
                # User doesn't rank well for this keyword
                
                # Check if high-threat competitors rank well
                high_threat_comps = [
                    c for c in overlap.competitor_domains
                    if c['position'] <= 10 and c['threat_level'] in ['high', 'critical']
                ]
                
                if high_threat_comps and overlap.impressions > 100:
                    # This is a gap worth addressing
                    gaps.append({
                        'keyword': overlap.keyword,
                        'impressions': overlap.impressions,
                        'user_position': overlap.user_position,
                        'competitors_ranking': len(high_threat_comps),
                        'best_competitor_position': min(c['position'] for c in high_threat_comps),
                        'competitive_density': overlap.competitive_density,
                        'opportunity_score': self._calculate_gap_opportunity_score(
                            overlap.impressions,
                            overlap.competitive_density,
                            len(high_threat_comps)
                        ),
                        'top_ranking_domains': [
                            {'domain': c['domain'], 'position': c['position']}
                            for c in high_threat_comps[:3]
                        ]
                    })
        
        # Sort by opportunity score
        gaps.sort(key=lambda x: x['opportunity_score'], reverse=True)
        
        return gaps[:50]  # Return top 50 gaps
    
    def _calculate_gap_opportunity_score(self, impressions: int, 
                                        density: float, 
                                        num_competitors: int) -> float:
        """
        Calculate opportunity score for a content gap
        
        Higher score = better opportunity
        """
        # High impressions = high opportunity
        impression_score = min(np.log1p(impressions) / 10.0, 1.0)
        
        # Lower density = easier to compete
        density_score = 1.0 - density
        
        # More competitors = validated opportunity but harder to win
        competitor_factor = min(num_competitors / 5.0, 1.0)
        
        # Weighted combination
        score = (
            impression_score * 0.5 +
            density_score * 0.3 +
            competitor_factor * 0.2
        ) * 100
        
        return float(score)
    
    def _calculate_market_positioning(self,
                                     competitors: Dict[str, CompetitorMetrics],
                                     keyword_overlaps: List[KeywordOverlap]) -> Dict[str, Any]:
        """
        Calculate overall market positioning metrics
        """
        total_keywords = len(keyword_overlaps)
        
        if total_keywords == 0:
            return {
                'total_keywords_analyzed': 0,
                'avg_competitive_density': 0.0,
                'market_position': 'unknown',
                'share_of_voice': 0.0,
                'position_distribution': {}
            }
        
        # Average competitive density
        avg_density = np.mean([k.competitive_density for k in keyword_overlaps])
        
        # Position distribution
        position_bins = {
            'top_3': 0,
            'top_5': 0,
            'top_10': 0,
            'page_2': 0,
            'beyond': 0
        }
        
        total_impressions = 0
        user_impressions = 0
        
        for overlap in keyword_overlaps:
            pos = overlap.user_position
            
            if pos <= 3:
                position_bins['top_3'] += 1
            elif pos <= 5:
                position_bins['top_5'] += 1
            elif pos <= 10:
                position_bins['top_10'] += 1
            elif pos <= 20:
                position_bins['page_2'] += 1
            else:
                position_bins['beyond'] += 1
            
            # Share of voice calculation
            total_impressions += overlap.impressions
            if pos <= 20:
                # Estimate CTR based on position
                ctr = self._estimate_ctr(pos, overlap.competitive_density)
                user_impressions += overlap.impressions * ctr
        
        share_of_voice = (user_impressions / total_impressions * 100) \
                        if total_impressions > 0 else 0.0
        
        # Determine market position
        top_10_pct = (position_bins['top_3'] + position_bins['top_5'] + 
                     position_bins['top_10']) / total_keywords * 100
        
        if top_10_pct > 60:
            market_position = 'leader'
        elif top_10_pct > 40:
            market_position = 'strong_contender'
        elif top_10_pct > 20:
            market_position = 'emerging'
        else:
            market_position = 'challenger'
        
        return {
            'total_keywords_analyzed': total_keywords,
            'avg_competitive_density': float(avg_density),
            'market_position': market_position,
            'share_of_voice': float(share_of_voice),
            'position_distribution': {
                'top_3_count': position_bins['top_3'],
                'top_3_pct': float(position_bins['top_3'] / total_keywords * 100),
                'top_5_count': position_bins['top_5'],
                'top_5_pct': float(position_bins['top_5'] / total_keywords * 100),
                'top_10_count': position_bins['top_10'],
                'top_10_pct': float(position_bins['top_10'] / total_keywords * 100),
                'page_2_count': position_bins['page_2'],
                'page_2_pct': float(position_bins['page_2'] / total_keywords * 100),
                'beyond_page_2_count': position_bins['beyond'],
                'beyond_page_2_pct': float(position_bins['beyond'] / total_keywords * 100)
            }
        }
    
    def _estimate_ctr(self, position: int, density: float) -> float:
        """
        Estimate CTR based on position and competitive density
        
        Uses advanced CTR model that accounts for SERP features
        """
        # Base CTR curve (from AWR study)
        base_ctrs = {
            1: 0.27, 2: 0.15, 3: 0.11, 4: 0.08, 5: 0.07,
            6: 0.06, 7: 0.05, 8: 0.04, 9: 0.04, 10: 0.03,
            11: 0.02, 12: 0.02, 13: 0.02, 14: 0.01, 15: 0.01
        }
        
        base_ctr = base_ctrs.get(position, 0.01 * (16 - min(position, 20)) / 5)
        
        # Adjust for competitive density
        # Higher density = more SERP features = lower CTR
        density_penalty = 1.0 - (density * 0.4)
        
        adjusted_ctr = base_ctr * density_penalty
        
        return float(max(adjusted_ctr, 0.001))
    
    def _generate_strategic_insights(self,
                                    competitors: Dict[str, CompetitorMetrics],
                                    keyword_overlaps: List[KeywordOverlap],
                                    content_gaps: List[Dict],
                                    market_positioning: Dict) -> List[Dict[str, Any]]:
        """
        Generate strategic insights from competitive analysis
        """
        insights = []
        
        # Insight 1: Dominant competitor threat
        if competitors:
            top_competitor = max(competitors.values(), 
                                key=lambda c: c.overlap_percentage)
            
            if top_competitor.overlap_percentage > 30:
                insights.append({
                    'type': 'dominant_competitor',
                    'priority': 'high',
                    'title': f'High Overlap with {top_competitor.domain}',
                    'description': (
                        f'{top_competitor.domain} appears in {top_competitor.overlap_percentage:.1f}% '
                        f'of your keyword set. They rank better than you in '
                        f'{top_competitor.position_better_count} keywords and have an average '
                        f'position of {top_competitor.avg_position:.1f}.'
                    ),
                    'recommendation': (
                        'Conduct detailed content comparison with this competitor. '
                        'Identify their content strengths and gaps in your coverage.'
                    ),
                    'impact': 'high'
                })
        
        # Insight 2: Market positioning
        market_pos = market_positioning['market_position']
        if market_pos in ['challenger', 'emerging']:
            insights.append({
                'type': 'market_position',
                'priority': 'medium',
                'title': f'Market Position: {market_pos.replace("_", " ").title()}',
                'description': (
                    f'Only {market_positioning["position_distribution"]["top_10_pct"]:.1f}% '
                    f'of your keywords rank in top 10. Your share of voice is '
                    f'{market_positioning["share_of_voice"]:.1f}%.'
                ),
                'recommendation': (
                    'Focus on moving page 2 keywords to page 1. Prioritize keywords '
                    'with lower competitive density first for quicker wins.'
                ),
                'impact': 'high'
            })
        
        # Insight 3: High-density competitive landscape
        avg_density = market_positioning['avg_competitive_density']
        if avg_density > 0.7:
            insights.append({
                'type': 'competitive_density',
                'priority': 'high',
                'title': 'Highly Competitive Keyword Set',
                'description': (
                    f'Average competitive density is {avg_density:.2f}, indicating a '
                    f'challenging SERP landscape with many authority sites and SERP features.'
                ),
                'recommendation': (
                    'Consider targeting related long-tail keywords with lower density. '
                    'Invest in comprehensive content that can compete with authority sites.'
                ),
                'impact': 'strategic'
            })
        
        # Insight 4: Content gap opportunities
        if content_gaps:
            high_value_gaps = [g for g in content_gaps[:10] 
                              if g['opportunity_score'] > 50]
            
            if high_value_gaps:
                total_gap_impressions = sum(g['impressions'] for g in high_value_gaps)
                
                insights.append({
                    'type': 'content_gaps',
                    'priority': 'high',
                    'title': f'{len(high_value_gaps)} High-Value Content Gaps Identified',
                    'description': (
                        f'Found {len(high_value_gaps)} keywords with {total_gap_impressions:,} '
                        f'monthly impressions where competitors rank but you don\'t. '
                        f'Top opportunity: "{high_value_gaps[0]["keyword"]}" '
                        f'({high_value_gaps[0]["impressions"]:,} impressions).'
                    ),
                    'recommendation': (
                        'Create targeted content for these gap keywords. Start with the '
                        'highest opportunity scores and lowest competitive density.'
                    ),
                    'impact': 'high',
                    'estimated_traffic_opportunity': int(total_gap_impressions * 0.15)
                })
        
        # Insight 5: Win rate analysis
        if competitors:
            total_better = sum(c.position_better_count for c in competitors.values())
            total_worse = sum(c.position_worse_count for c in competitors.values())
            total_comparisons = total_better + total_worse
            
            if total_comparisons > 0:
                win_rate = (total_worse / total_comparisons) * 100
                
                if win_rate < 40:
                    insights.append({
                        'type': 'win_rate',
                        'priority': 'medium',
                        'title': f'Low Win Rate: {win_rate:.1f}%',
                        'description': (
                            f'When ranking for the same keywords, competitors rank higher '
                            f'than you {100-win_rate:.1f}% of the time.'
                        ),
                        'recommendation': (
                            'Analyze content quality gaps. Consider comprehensive content '
                            'refreshes, better internal linking, and E-E-A-T improvements.'
                        ),
                        'impact': 'medium'
                    })
        
        # Insight 6: Diversification opportunity
        if len(competitors) < 3:
            insights.append({
                'type': 'market_concentration',
                'priority': 'low',
                'title': 'Limited Competitive Overlap',
                'description': (
                    f'Only {len(competitors)} competitors appear frequently in your SERPs. '
                    f'This may indicate a niche opportunity or limited keyword coverage.'
                ),
                'recommendation': (
                    'Expand keyword research to identify adjacent topics and opportunities. '
                    'Look for ways to broaden your topical authority.'
                ),
                'impact': 'strategic'
            })
        
        return insights
    
    def analyze(self) -> Dict[str, Any]:
        """
        Run complete competitor context analysis
        
        Returns structured data matching report specification
        """
        try:
            logger.info("Starting competitor context analysis")
            
            # Step 1: Identify competitors
            logger.info("Identifying competitors from SERP data")
            competitors = self._identify_competitors()
            
            # Step 2: Analyze keyword overlaps
            logger.info(f"Analyzing keyword overlaps with {len(competitors)} competitors")
            keyword_overlaps = self._analyze_keyword_overlaps(competitors)
            
            # Step 3: Identify content gaps
            logger.info("Identifying content gaps")
            content_gaps = self._identify_content_gaps(competitors, keyword_overlaps)
            
            # Step 4: Calculate market positioning
            logger.info("Calculating market positioning metrics")
            market_positioning = self._calculate_market_positioning(
                competitors, keyword_overlaps
            )
            
            # Step 5: Generate strategic insights
            logger.info("Generating strategic insights")
            insights = self._generate_strategic_insights(
                competitors, keyword_overlaps, content_gaps, market_positioning
            )
            
            # Sort competitors by threat level and overlap
            top_competitors = sorted(
                competitors.values(),
                key=lambda c: (
                    {'critical': 4, 'high': 3, 'medium': 2, 'low': 1}[c.threat_level],
                    c.overlap_percentage
                ),
                reverse=True
            )[:self.top_n_competitors]
            
            # Format results
            results = {
                'competitors': [
                    {
                        'domain': c.domain,
                        'keywords_shared': c.keywords_shared,
                        'overlap_percentage': round(c.overlap_percentage, 1),
                        'total_impressions_shared': c.total_impressions_shared,
                        'avg_position': round(c.avg_position, 1),
                        'position_better_count': c.position_better_count,
                        'position_worse_count': c.position_worse_count,
                        'win_rate': round(
                            (c.position_worse_count / 
                             (c.position_better_count + c.position_worse_count) * 100)
                            if (c.position_better_count + c.position_worse_count) > 0
                            else 0, 1
                        ),
                        'threat_level': c.threat_level
                    }
                    for c in top_competitors
                ],
                'market_positioning': market_positioning,
                'content_gaps': content_gaps[:20],  # Top 20 gaps
                'competitive_keywords': [
                    {
                        'keyword': overlap.keyword,
                        'user_position': overlap.user_position,
                        'user_url': overlap.user_url,
                        'impressions': overlap.impressions,
                        'clicks': overlap.clicks,
                        'competitive_density': round(overlap.competitive_density, 2),
                        'competitor_count': len(overlap.competitor_domains),
                        'top_competitors': overlap.competitor_domains[:5]
                    }
                    for overlap in sorted(
                        keyword_overlaps,
                        key=lambda x: x.competitive_density * np.log1p(x.impressions),
                        reverse=True
                    )[:30]  # Top 30 most competitive keywords
                ],
                'insights': insights,
                'summary': {
                    'total_competitors_identified': len(competitors),
                    'total_keywords_analyzed': len(keyword_overlaps),
                    'avg_competitive_density': round(
                        market_positioning['avg_competitive_density'], 2
                    ),
                    'content_gaps_identified': len(content_gaps),
                    'total_gap_impressions': sum(g['impressions'] for g in content_gaps),
                    'estimated_gap_opportunity_clicks': int(
                        sum(g['impressions'] for g in content_gaps) * 0.15
                    ),
                    'market_position': market_positioning['market_position'],
                    'share_of_voice_pct': round(
                        market_positioning['share_of_voice'], 1
                    )
                }
            }
            
            logger.info(
                f"Competitor analysis complete: {len(top_competitors)} competitors, "
                f"{len(content_gaps)} gaps, {len(insights)} insights"
            )
            
            return results
            
        except Exception as e:
            logger.error(f"Error in competitor context analysis: {e}", exc_info=True)
            raise


def analyze_competitor_context(serp_data: List[Dict], 
                               gsc_keyword_data: pd.DataFrame,
                               user_domain: str,
                               config: Optional[Dict] = None) -> Dict[str, Any]:
    """
    Main entry point for competitor context analysis
    
    Args:
        serp_data: Live SERP results from DataForSEO
        gsc_keyword_data: GSC performance data by keyword with columns:
                         query, impressions, clicks, position, page
        user_domain: The user's domain (e.g., 'example.com')
        config: Optional configuration parameters
    
    Returns:
        Dictionary containing:
        - competitors: List of competitor domains with overlap metrics
        - market_positioning: Overall market position analysis
        - content_gaps: Identified keyword opportunities
        - competitive_keywords: Most competitive keywords in portfolio
        - insights: Strategic recommendations
        - summary: High-level metrics
    """
    analyzer = CompetitorContextAnalyzer(
        serp_data=serp_data,
        gsc_keyword_data=gsc_keyword_data,
        user_domain=user_domain,
        config=config
    )
    
    return analyzer.analyze()
