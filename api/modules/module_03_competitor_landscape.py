"""
Module 3: SERP Landscape & Competitor Analysis

Analyzes the competitive landscape in organic search by:
1. Identifying top organic competitors (domains ranking for same keywords)
2. Calculating competitor overlap scores
3. Determining keyword gaps (what competitors rank for that target doesn't)
4. Enriching with live SERP position data via DataForSEO
5. Analyzing SERP feature displacement

Returns structured data for frontend visualization including:
- Competitor domains with overlap metrics
- Opportunity keywords
- SERP feature impact analysis
- Click share estimation
"""

import logging
from typing import Dict, List, Any, Optional, Set, Tuple
from collections import defaultdict
from datetime import datetime
import pandas as pd
import numpy as np
from dataclasses import dataclass, asdict

logger = logging.getLogger(__name__)


@dataclass
class CompetitorDomain:
    """Represents a competitor domain with metrics."""
    domain: str
    shared_keywords: int
    overlap_score: float
    avg_position: float
    threat_level: str
    keywords_they_win: int
    avg_position_gap: float
    estimated_monthly_clicks: int


@dataclass
class KeywordGap:
    """Represents a keyword opportunity where competitors rank but target doesn't."""
    keyword: str
    search_volume: Optional[int]
    competitor_domains: List[str]
    best_competitor_position: float
    difficulty_estimate: str
    intent_type: str
    estimated_opportunity: int


@dataclass
class SERPFeatureImpact:
    """Represents SERP feature impact on a keyword."""
    keyword: str
    organic_position: float
    visual_position: float
    features_above: List[str]
    estimated_ctr_impact: float
    current_ctr: Optional[float]
    expected_ctr: Optional[float]


@dataclass
class CompetitorAnalysisResult:
    """Complete result from competitor landscape analysis."""
    competitors: List[CompetitorDomain]
    keyword_gaps: List[KeywordGap]
    serp_displacement: List[SERPFeatureImpact]
    total_click_share: float
    click_share_opportunity: float
    keywords_analyzed: int
    summary: Dict[str, Any]
    metadata: Dict[str, Any]


class CompetitorLandscapeAnalyzer:
    """Analyzes competitive landscape in organic search."""
    
    # Position-based CTR curves (baseline, no SERP features)
    BASELINE_CTR = {
        1: 0.316, 2: 0.158, 3: 0.100, 4: 0.071, 5: 0.053,
        6: 0.042, 7: 0.034, 8: 0.028, 9: 0.024, 10: 0.020,
        11: 0.015, 12: 0.012, 13: 0.010, 14: 0.008, 15: 0.007,
        16: 0.006, 17: 0.005, 18: 0.005, 19: 0.004, 20: 0.004
    }
    
    # SERP feature impact on CTR (multipliers and position displacement)
    SERP_FEATURE_IMPACT = {
        'featured_snippet': {'ctr_steal': 0.40, 'visual_displacement': 2.0},
        'ai_overview': {'ctr_steal': 0.35, 'visual_displacement': 3.0},
        'knowledge_panel': {'ctr_steal': 0.25, 'visual_displacement': 1.5},
        'local_pack': {'ctr_steal': 0.30, 'visual_displacement': 3.0},
        'people_also_ask': {'ctr_steal': 0.05, 'visual_displacement': 0.5},  # per question
        'video_carousel': {'ctr_steal': 0.15, 'visual_displacement': 2.0},
        'image_pack': {'ctr_steal': 0.10, 'visual_displacement': 1.5},
        'shopping_results': {'ctr_steal': 0.20, 'visual_displacement': 2.5},
        'top_stories': {'ctr_steal': 0.12, 'visual_displacement': 2.0},
        'twitter': {'ctr_steal': 0.08, 'visual_displacement': 1.5},
        'reddit_threads': {'ctr_steal': 0.10, 'visual_displacement': 1.0}
    }
    
    # Intent classification patterns
    INTENT_PATTERNS = {
        'informational': ['how to', 'what is', 'why', 'guide', 'tutorial', 'learn'],
        'commercial': ['best', 'top', 'review', 'compare', 'vs', 'alternative'],
        'transactional': ['buy', 'price', 'cheap', 'deal', 'discount', 'coupon', 'order'],
        'navigational': []  # Determined by brand name presence
    }
    
    def __init__(self, target_domain: str, brand_terms: Optional[List[str]] = None):
        """
        Initialize analyzer.
        
        Args:
            target_domain: The domain being analyzed
            brand_terms: Optional list of brand terms to filter out
        """
        self.target_domain = self._normalize_domain(target_domain)
        self.brand_terms = [term.lower() for term in (brand_terms or [])]
        self.brand_terms.append(self.target_domain.replace('www.', '').split('.')[0])
    
    def analyze(
        self,
        gsc_keyword_data: pd.DataFrame,
        serp_data: Optional[List[Dict[str, Any]]] = None,
        min_impressions: int = 50,
        top_n_keywords: int = 100
    ) -> CompetitorAnalysisResult:
        """
        Perform complete competitor landscape analysis.
        
        Args:
            gsc_keyword_data: DataFrame with columns: query, clicks, impressions, position, ctr
            serp_data: Optional list of SERP results from DataForSEO
            min_impressions: Minimum impressions to include keyword
            top_n_keywords: Number of top keywords to analyze
            
        Returns:
            CompetitorAnalysisResult with all analysis data
        """
        start_time = datetime.now()
        logger.info(f"Starting competitor landscape analysis for {self.target_domain}")
        
        # Filter and prepare keyword data
        filtered_keywords = self._prepare_keyword_data(
            gsc_keyword_data, min_impressions, top_n_keywords
        )
        
        if filtered_keywords.empty:
            logger.warning("No keywords found matching criteria")
            return self._empty_result()
        
        # Extract competitors from SERP data
        if serp_data:
            competitors_map, keyword_positions = self._extract_competitors_from_serp(
                serp_data, filtered_keywords
            )
        else:
            logger.warning("No SERP data provided, competitor analysis will be limited")
            competitors_map = {}
            keyword_positions = {}
        
        # Calculate competitor overlap and metrics
        competitors = self._calculate_competitor_metrics(
            competitors_map, keyword_positions, filtered_keywords
        )
        
        # Identify keyword gaps
        keyword_gaps = self._identify_keyword_gaps(
            competitors_map, keyword_positions, filtered_keywords
        )
        
        # Analyze SERP feature displacement
        serp_displacement = []
        if serp_data:
            serp_displacement = self._analyze_serp_displacement(
                serp_data, filtered_keywords
            )
        
        # Calculate click share
        total_click_share, click_share_opportunity = self._calculate_click_share(
            filtered_keywords, serp_data
        )
        
        # Generate summary
        summary = self._generate_summary(
            competitors, keyword_gaps, serp_displacement,
            total_click_share, click_share_opportunity
        )
        
        metadata = {
            'analysis_date': datetime.now().isoformat(),
            'target_domain': self.target_domain,
            'keywords_analyzed': len(filtered_keywords),
            'serp_data_available': serp_data is not None,
            'processing_time_seconds': (datetime.now() - start_time).total_seconds()
        }
        
        logger.info(f"Competitor analysis completed in {metadata['processing_time_seconds']:.2f}s")
        
        return CompetitorAnalysisResult(
            competitors=competitors,
            keyword_gaps=keyword_gaps,
            serp_displacement=serp_displacement,
            total_click_share=total_click_share,
            click_share_opportunity=click_share_opportunity,
            keywords_analyzed=len(filtered_keywords),
            summary=summary,
            metadata=metadata
        )
    
    def _prepare_keyword_data(
        self,
        gsc_data: pd.DataFrame,
        min_impressions: int,
        top_n: int
    ) -> pd.DataFrame:
        """Filter and prepare keyword data for analysis."""
        df = gsc_data.copy()
        
        # Ensure required columns exist
        required_cols = ['query', 'clicks', 'impressions', 'position']
        if not all(col in df.columns for col in required_cols):
            missing = [col for col in required_cols if col not in df.columns]
            logger.error(f"Missing required columns: {missing}")
            return pd.DataFrame()
        
        # Add CTR if not present
        if 'ctr' not in df.columns:
            df['ctr'] = df['clicks'] / df['impressions'].replace(0, 1)
        
        # Filter out branded queries
        if self.brand_terms:
            brand_mask = df['query'].str.lower().apply(
                lambda q: not any(brand in q for brand in self.brand_terms)
            )
            df = df[brand_mask]
        
        # Filter by impressions
        df = df[df['impressions'] >= min_impressions]
        
        # Sort by impressions and take top N
        df = df.nlargest(top_n, 'impressions')
        
        logger.info(f"Prepared {len(df)} keywords for analysis (filtered from {len(gsc_data)})")
        
        return df.reset_index(drop=True)
    
    def _extract_competitors_from_serp(
        self,
        serp_data: List[Dict[str, Any]],
        keywords_df: pd.DataFrame
    ) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, List[Dict[str, Any]]]]:
        """
        Extract competitor domains and their positions from SERP data.
        
        Returns:
            Tuple of (competitors_map, keyword_positions)
            - competitors_map: {domain: {keyword: position, ...}}
            - keyword_positions: {keyword: [{domain, position, url, ...}, ...]}
        """
        competitors_map = defaultdict(dict)
        keyword_positions = {}
        
        keyword_set = set(keywords_df['query'].str.lower())
        
        for serp_result in serp_data:
            keyword = serp_result.get('keyword', '').lower()
            
            if keyword not in keyword_set:
                continue
            
            organic_results = serp_result.get('organic_results', [])
            positions_for_keyword = []
            
            for result in organic_results[:20]:  # Top 20 positions
                url = result.get('url', '')
                domain = self._normalize_domain(self._extract_domain(url))
                position = result.get('position', 999)
                
                if not domain or domain == self.target_domain:
                    continue
                
                positions_for_keyword.append({
                    'domain': domain,
                    'position': position,
                    'url': url,
                    'title': result.get('title', ''),
                    'snippet': result.get('snippet', '')
                })
                
                # Record this position for the competitor
                if keyword not in competitors_map[domain]:
                    competitors_map[domain][keyword] = position
                else:
                    # Keep best position if multiple results
                    competitors_map[domain][keyword] = min(
                        competitors_map[domain][keyword], position
                    )
            
            keyword_positions[keyword] = positions_for_keyword
        
        logger.info(f"Extracted {len(competitors_map)} competitor domains from SERP data")
        
        return dict(competitors_map), keyword_positions
    
    def _calculate_competitor_metrics(
        self,
        competitors_map: Dict[str, Dict[str, Any]],
        keyword_positions: Dict[str, List[Dict[str, Any]]],
        keywords_df: pd.DataFrame
    ) -> List[CompetitorDomain]:
        """Calculate metrics for each competitor domain."""
        if not competitors_map:
            return []
        
        keyword_to_target_position = dict(zip(
            keywords_df['query'].str.lower(),
            keywords_df['position']
        ))
        
        keyword_to_impressions = dict(zip(
            keywords_df['query'].str.lower(),
            keywords_df['impressions']
        ))
        
        competitors = []
        
        for domain, keyword_positions_map in competitors_map.items():
            shared_keywords = len(keyword_positions_map)
            
            # Calculate overlap score (0-1)
            # Based on: keyword overlap % + position competitiveness
            overlap_pct = shared_keywords / len(keywords_df)
            
            positions = list(keyword_positions_map.values())
            avg_position = np.mean(positions)
            
            # Count keywords where competitor beats target
            keywords_they_win = 0
            position_gaps = []
            
            for kw, comp_pos in keyword_positions_map.items():
                target_pos = keyword_to_target_position.get(kw)
                if target_pos and comp_pos < target_pos:
                    keywords_they_win += 1
                    position_gaps.append(target_pos - comp_pos)
            
            avg_position_gap = np.mean(position_gaps) if position_gaps else 0
            
            # Overlap score combines frequency and position quality
            position_quality = 1 / (1 + avg_position / 10)  # Normalize to 0-1
            overlap_score = (overlap_pct * 0.6 + position_quality * 0.4)
            
            # Estimate monthly clicks based on positions and search volumes
            estimated_clicks = 0
            for kw, pos in keyword_positions_map.items():
                impressions = keyword_to_impressions.get(kw, 0)
                ctr = self.BASELINE_CTR.get(int(pos), 0.001)
                estimated_clicks += impressions * ctr
            
            # Determine threat level
            if overlap_score > 0.5 and keywords_they_win > shared_keywords * 0.5:
                threat_level = 'critical'
            elif overlap_score > 0.3 or keywords_they_win > shared_keywords * 0.3:
                threat_level = 'high'
            elif overlap_score > 0.15:
                threat_level = 'medium'
            else:
                threat_level = 'low'
            
            competitors.append(CompetitorDomain(
                domain=domain,
                shared_keywords=shared_keywords,
                overlap_score=round(overlap_score, 3),
                avg_position=round(avg_position, 1),
                threat_level=threat_level,
                keywords_they_win=keywords_they_win,
                avg_position_gap=round(avg_position_gap, 1),
                estimated_monthly_clicks=int(estimated_clicks)
            ))
        
        # Sort by overlap score
        competitors.sort(key=lambda x: x.overlap_score, reverse=True)
        
        logger.info(f"Calculated metrics for {len(competitors)} competitors")
        
        return competitors[:50]  # Return top 50
    
    def _identify_keyword_gaps(
        self,
        competitors_map: Dict[str, Dict[str, Any]],
        keyword_positions: Dict[str, List[Dict[str, Any]]],
        keywords_df: pd.DataFrame
    ) -> List[KeywordGap]:
        """Identify keywords where competitors rank but target doesn't."""
        if not keyword_positions:
            return []
        
        # Get keywords from GSC where target already ranks
        target_keywords = set(keywords_df['query'].str.lower())
        
        # Collect all keywords where competitors rank
        # (In production, this would come from competitor keyword data via DataForSEO)
        # For now, use the SERP data we have
        
        gaps = []
        
        # For each keyword in our SERP data
        for keyword, positions in keyword_positions.items():
            # Check if any strong competitors rank well
            competitor_domains = []
            best_position = 999
            
            for pos_data in positions:
                domain = pos_data['domain']
                position = pos_data['position']
                
                # Consider it a gap if competitor is in top 10
                if position <= 10:
                    competitor_domains.append(domain)
                    best_position = min(best_position, position)
            
            # Only consider if we have competitors ranking AND target isn't ranking well
            target_position = keywords_df[
                keywords_df['query'].str.lower() == keyword
            ]['position'].values
            
            if not len(competitor_domains):
                continue
            
            # Gap if: target doesn't rank at all, or ranks poorly (>20)
            is_gap = (
                len(target_position) == 0 or 
                target_position[0] > 20
            )
            
            if is_gap and best_position <= 10:
                # Estimate difficulty based on best competitor position
                if best_position <= 3:
                    difficulty = 'high'
                elif best_position <= 7:
                    difficulty = 'medium'
                else:
                    difficulty = 'low'
                
                # Classify intent
                intent = self._classify_intent(keyword)
                
                # Estimate opportunity (rough heuristic)
                # Better positions = higher opportunity
                opportunity_score = int((11 - best_position) * 50)
                
                gaps.append(KeywordGap(
                    keyword=keyword,
                    search_volume=None,  # Would come from DataForSEO keyword data
                    competitor_domains=competitor_domains[:5],
                    best_competitor_position=round(best_position, 1),
                    difficulty_estimate=difficulty,
                    intent_type=intent,
                    estimated_opportunity=opportunity_score
                ))
        
        # Sort by opportunity
        gaps.sort(key=lambda x: x.estimated_opportunity, reverse=True)
        
        logger.info(f"Identified {len(gaps)} keyword gap opportunities")
        
        return gaps[:100]  # Return top 100
    
    def _analyze_serp_displacement(
        self,
        serp_data: List[Dict[str, Any]],
        keywords_df: pd.DataFrame
    ) -> List[SERPFeatureImpact]:
        """Analyze how SERP features displace organic results."""
        displacement_results = []
        
        keyword_to_position = dict(zip(
            keywords_df['query'].str.lower(),
            keywords_df['position']
        ))
        
        keyword_to_ctr = dict(zip(
            keywords_df['query'].str.lower(),
            keywords_df['ctr']
        ))
        
        for serp_result in serp_data:
            keyword = serp_result.get('keyword', '').lower()
            
            if keyword not in keyword_to_position:
                continue
            
            organic_position = keyword_to_position[keyword]
            
            # Parse SERP features
            features_above = []
            visual_displacement = 0
            ctr_steal = 0
            
            # Featured snippet
            if serp_result.get('featured_snippet'):
                features_above.append('featured_snippet')
                visual_displacement += self.SERP_FEATURE_IMPACT['featured_snippet']['visual_displacement']
                ctr_steal += self.SERP_FEATURE_IMPACT['featured_snippet']['ctr_steal']
            
            # AI Overview / SGE
            if serp_result.get('ai_overview'):
                features_above.append('ai_overview')
                visual_displacement += self.SERP_FEATURE_IMPACT['ai_overview']['visual_displacement']
                ctr_steal += self.SERP_FEATURE_IMPACT['ai_overview']['ctr_steal']
            
            # People Also Ask
            paa_count = len(serp_result.get('people_also_ask', []))
            if paa_count > 0:
                features_above.append(f'paa_x{paa_count}')
                visual_displacement += paa_count * self.SERP_FEATURE_IMPACT['people_also_ask']['visual_displacement']
                ctr_steal += paa_count * self.SERP_FEATURE_IMPACT['people_also_ask']['ctr_steal']
            
            # Local pack
            if serp_result.get('local_pack'):
                features_above.append('local_pack')
                visual_displacement += self.SERP_FEATURE_IMPACT['local_pack']['visual_displacement']
                ctr_steal += self.SERP_FEATURE_IMPACT['local_pack']['ctr_steal']
            
            # Video carousel
            if serp_result.get('video_results'):
                features_above.append('video_carousel')
                visual_displacement += self.SERP_FEATURE_IMPACT['video_carousel']['visual_displacement']
                ctr_steal += self.SERP_FEATURE_IMPACT['video_carousel']['ctr_steal']
            
            # Image pack
            if serp_result.get('image_results'):
                features_above.append('image_pack')
                visual_displacement += self.SERP_FEATURE_IMPACT['image_pack']['visual_displacement']
                ctr_steal += self.SERP_FEATURE_IMPACT['image_pack']['ctr_steal']
            
            # Shopping results
            if serp_result.get('shopping_results'):
                features_above.append('shopping_results')
                visual_displacement += self.SERP_FEATURE_IMPACT['shopping_results']['visual_displacement']
                ctr_steal += self.SERP_FEATURE_IMPACT['shopping_results']['ctr_steal']
            
            # Top stories
            if serp_result.get('top_stories'):
                features_above.append('top_stories')
                visual_displacement += self.SERP_FEATURE_IMPACT['top_stories']['visual_displacement']
                ctr_steal += self.SERP_FEATURE_IMPACT['top_stories']['ctr_steal']
            
            # Knowledge panel
            if serp_result.get('knowledge_panel'):
                features_above.append('knowledge_panel')
                visual_displacement += self.SERP_FEATURE_IMPACT['knowledge_panel']['visual_displacement']
                ctr_steal += self.SERP_FEATURE_IMPACT['knowledge_panel']['ctr_steal']
            
            # Twitter/X results
            if serp_result.get('twitter_results'):
                features_above.append('twitter')
                visual_displacement += self.SERP_FEATURE_IMPACT['twitter']['visual_displacement']
                ctr_steal += self.SERP_FEATURE_IMPACT['twitter']['ctr_steal']
            
            # Reddit threads
            organic_results = serp_result.get('organic_results', [])
            reddit_count = sum(1 for r in organic_results[:10] if 'reddit.com' in r.get('url', ''))
            if reddit_count > 0:
                features_above.append(f'reddit_threads_x{reddit_count}')
                visual_displacement += reddit_count * self.SERP_FEATURE_IMPACT['reddit_threads']['visual_displacement']
                ctr_steal += reddit_count * self.SERP_FEATURE_IMPACT['reddit_threads']['ctr_steal']
            
            # Calculate visual position
            visual_position = organic_position + visual_displacement
            
            # Calculate CTR impact
            baseline_ctr = self.BASELINE_CTR.get(int(organic_position), 0.001)
            adjusted_ctr = baseline_ctr * (1 - min(ctr_steal, 0.8))  # Cap at 80% steal
            estimated_ctr_impact = adjusted_ctr - baseline_ctr
            
            # Only include if there's significant displacement (>2 positions)
            if visual_displacement > 2:
                displacement_results.append(SERPFeatureImpact(
                    keyword=keyword,
                    organic_position=round(organic_position, 1),
                    visual_position=round(visual_position, 1),
                    features_above=features_above,
                    estimated_ctr_impact=round(estimated_ctr_impact, 4),
                    current_ctr=keyword_to_ctr.get(keyword),
                    expected_ctr=round(baseline_ctr, 4)
                ))
        
        # Sort by impact magnitude
        displacement_results.sort(
            key=lambda x: abs(x.estimated_ctr_impact),
            reverse=True
        )
        
        logger.info(f"Analyzed SERP displacement for {len(displacement_results)} keywords")
        
        return displacement_results[:50]  # Return top 50 most impacted
    
    def _calculate_click_share(
        self,
        keywords_df: pd.DataFrame,
        serp_data: Optional[List[Dict[str, Any]]]
    ) -> Tuple[float, float]:
        """
        Calculate total click share and opportunity.
        
        Returns:
            Tuple of (total_click_share, click_share_opportunity)
        """
        total_available_clicks = keywords_df['impressions'].sum()
        total_captured_clicks = keywords_df['clicks'].sum()
        
        if total_available_clicks == 0:
            return 0.0, 0.0
        
        # Actual click share
        click_share = total_captured_clicks / total_available_clicks
        
        # Calculate potential click share if we ranked #1 for everything
        # (This is the theoretical maximum)
        potential_clicks = keywords_df['impressions'].sum() * self.BASELINE_CTR[1]
        
        # Opportunity is the gap between potential and actual
        opportunity_clicks = potential_clicks - total_captured_clicks
        click_share_opportunity = opportunity_clicks / total_available_clicks
        
        logger.info(
            f"Click share: {click_share:.2%} | "
            f"Opportunity: {click_share_opportunity:.2%}"
        )
        
        return round(click_share, 4), round(click_share_opportunity, 4)
    
    def _classify_intent(self, query: str) -> str:
        """Classify search intent based on query patterns."""
        query_lower = query.lower()
        
        # Check each intent pattern
        for intent, patterns in self.INTENT_PATTERNS.items():
            if any(pattern in query_lower for pattern in patterns):
                return intent
        
        # Check for navigational (brand terms)
        if any(brand in query_lower for brand in self.brand_terms):
            return 'navigational'
        
        # Default to informational
        return 'informational'
    
    def _generate_summary(
        self,
        competitors: List[CompetitorDomain],
        keyword_gaps: List[KeywordGap],
        serp_displacement: List[SERPFeatureImpact],
        click_share: float,
        opportunity: float
    ) -> Dict[str, Any]:
        """Generate summary statistics."""
        # Competitor summary
        threat_counts = defaultdict(int)
        for comp in competitors:
            threat_counts[comp.threat_level] += 1
        
        # Gap summary by intent
        gap_by_intent = defaultdict(int)
        for gap in keyword_gaps:
            gap_by_intent[gap.intent_type] += 1
        
        # Displacement summary
        total_displaced = len(serp_displacement)
        avg_displacement = (
            np.mean([s.visual_position - s.organic_position for s in serp_displacement])
            if serp_displacement else 0
        )
        
        # Most common SERP features
        all_features = []
        for s in serp_displacement:
            all_features.extend(s.features_above)
        
        feature_counts = defaultdict(int)
        for feature in all_features:
            # Normalize features with counts (e.g., "paa_x4" -> "paa")
            base_feature = feature.split('_x')[0]
            feature_counts[base_feature] += 1
        
        top_features = sorted(
            feature_counts.items(),
            key=lambda x: x[1],
            reverse=True
        )[:5]
        
        return {
            'total_competitors': len(competitors),
            'competitors_by_threat': dict(threat_counts),
            'primary_competitors': [
                {'domain': c.domain, 'overlap_score': c.overlap_score}
                for c in competitors[:5]
            ],
            'total_keyword_gaps': len(keyword_gaps),
            'keyword_gaps_by_intent': dict(gap_by_intent),
            'high_opportunity_gaps': [
                {'keyword': g.keyword, 'opportunity': g.estimated_opportunity}
                for g in keyword_gaps[:10]
            ],
            'keywords_with_serp_displacement': total_displaced,
            'avg_visual_displacement': round(avg_displacement, 1),
            'most_common_serp_features': [
                {'feature': f, 'count': c} for f, c in top_features
            ],
            'total_click_share': click_share,
            'click_share_opportunity': opportunity,
            'estimated_ctr_loss_from_features': round(
                sum(abs(s.estimated_ctr_impact) for s in serp_displacement), 4
            ) if serp_displacement else 0
        }
    
    def _empty_result(self) -> CompetitorAnalysisResult:
        """Return empty result structure."""
        return CompetitorAnalysisResult(
            competitors=[],
            keyword_gaps=[],
            serp_displacement=[],
            total_click_share=0.0,
            click_share_opportunity=0.0,
            keywords_analyzed=0,
            summary={},
            metadata={
                'analysis_date': datetime.now().isoformat(),
                'target_domain': self.target_domain,
                'error': 'No keywords found matching criteria'
            }
        )
    
    @staticmethod
    def _normalize_domain(domain: str) -> str:
        """Normalize domain to consistent format."""
        if not domain:
            return ''
        domain = domain.lower().strip()
        domain = domain.replace('http://', '').replace('https://', '')
        domain = domain.split('/')[0]
        domain = domain.replace('www.', '')
        return domain
    
    @staticmethod
    def _extract_domain(url: str) -> str:
        """Extract domain from URL."""
        if not url:
            return ''
        # Remove protocol
        url = url.replace('http://', '').replace('https://', '')
        # Get domain part
        domain = url.split('/')[0]
        return domain


def analyze_competitor_landscape(
    target_domain: str,
    gsc_keyword_data: pd.DataFrame,
    serp_data: Optional[List[Dict[str, Any]]] = None,
    brand_terms: Optional[List[str]] = None,
    min_impressions: int = 50,
    top_n_keywords: int = 100
) -> Dict[str, Any]:
    """
    Main entry point for Module 3: Competitor Landscape Analysis.
    
    Args:
        target_domain: The domain being analyzed
        gsc_keyword_data: GSC keyword performance data
        serp_data: Optional SERP results from DataForSEO
        brand_terms: Optional list of brand terms to filter
        min_impressions: Minimum impressions threshold
        top_n_keywords: Number of keywords to analyze
        
    Returns:
        Dictionary with complete competitor analysis results
    """
    analyzer = CompetitorLandscapeAnalyzer(target_domain, brand_terms)
    
    result = analyzer.analyze(
        gsc_keyword_data=gsc_keyword_data,
        serp_data=serp_data,
        min_impressions=min_impressions,
        top_n_keywords=top_n_keywords
    )
    
    # Convert to dictionary for JSON serialization
    return {
        'competitors': [asdict(c) for c in result.competitors],
        'keyword_gaps': [asdict(g) for g in result.keyword_gaps],
        'serp_displacement': [asdict(s) for s in result.serp_displacement],
        'total_click_share': result.total_click_share,
        'click_share_opportunity': result.click_share_opportunity,
        'keywords_analyzed': result.keywords_analyzed,
        'summary': result.summary,
        'metadata': result.metadata
    }
