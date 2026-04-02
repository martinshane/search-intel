"""
Module 3: Competitor Gap Analysis

Identifies competitors ranking in positions 1-10 where our site is not,
calculates keyword difficulty and opportunity scores, and returns prioritized gaps.
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
from collections import Counter, defaultdict
from datetime import datetime
import pandas as pd
import numpy as np
from dataclasses import dataclass, asdict
from urllib.parse import urlparse
import re

from api.services.dataforseo_service import DataForSEOService
from api.core.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


@dataclass
class CompetitorDomain:
    """Represents a competitor domain with metadata."""
    domain: str
    keywords_in_top10: int
    avg_position: float
    overlap_with_us: int
    keywords_we_miss: int
    threat_score: float


@dataclass
class KeywordGap:
    """Represents a keyword gap opportunity."""
    keyword: str
    our_position: Optional[int]
    competitor_positions: List[Dict[str, Any]]
    search_volume: int
    difficulty: int
    opportunity_score: float
    gap_type: str


class CompetitorGapAnalyzer:
    """
    Analyzes competitor keyword gaps using GSC data and DataForSEO SERP data.
    """

    def __init__(self, dataforseo_service: Optional[DataForSEOService] = None):
        """Initialize the analyzer with DataForSEO service."""
        self.dataforseo_service = dataforseo_service or DataForSEOService()

    def _extract_domain_from_url(self, url: str) -> str:
        """Extract clean domain from URL."""
        try:
            parsed = urlparse(url)
            domain = parsed.netloc or parsed.path
            # Remove www.
            domain = re.sub(r'^www\.', '', domain)
            return domain.lower()
        except Exception as e:
            logger.warning(f"Failed to parse URL {url}: {e}")
            return ""

    def _identify_user_domain(self, gsc_data: pd.DataFrame) -> str:
        """
        Identify user's domain from GSC data.
        GSC data should have a 'page' column with URLs.
        """
        if 'page' not in gsc_data.columns or gsc_data.empty:
            return ""
        
        # Get most common domain from page URLs
        domains = gsc_data['page'].apply(self._extract_domain_from_url)
        domain_counts = domains.value_counts()
        
        if domain_counts.empty:
            return ""
        
        return domain_counts.index[0]

    def _calculate_keyword_difficulty(self, serp_result: Dict[str, Any]) -> int:
        """
        Calculate keyword difficulty score (0-100) based on SERP competitiveness.
        
        Factors:
        - Domain authority of ranking pages (estimated from position stability)
        - Number of strong domains in top 10
        - SERP feature presence
        """
        try:
            organic_results = serp_result.get('items', [])
            
            # Count high-authority domains (simplified heuristic)
            # In production, would use actual DA scores if available
            strong_domains = 0
            for item in organic_results[:10]:
                domain = item.get('domain', '')
                # Heuristic: well-known domains or those with rich SERP features
                if any(tld in domain for tld in ['.gov', '.edu', '.org']):
                    strong_domains += 1
                if item.get('domain_rank'):  # If DataForSEO provides rank
                    if item['domain_rank'] > 70:
                        strong_domains += 1
            
            # SERP features increase difficulty
            serp_features_count = 0
            if serp_result.get('featured_snippet'):
                serp_features_count += 2
            if serp_result.get('knowledge_graph'):
                serp_features_count += 2
            if serp_result.get('local_pack'):
                serp_features_count += 1
            if serp_result.get('people_also_ask'):
                serp_features_count += 1
            
            # Calculate base difficulty
            base_difficulty = min(strong_domains * 10, 60)
            feature_penalty = min(serp_features_count * 8, 30)
            
            difficulty = base_difficulty + feature_penalty
            return min(int(difficulty), 100)
            
        except Exception as e:
            logger.warning(f"Error calculating difficulty: {e}")
            return 50  # Default moderate difficulty

    def _calculate_opportunity_score(
        self,
        search_volume: int,
        our_position: Optional[int],
        best_competitor_position: int,
        difficulty: int
    ) -> float:
        """
        Calculate opportunity score (0-100) for a keyword gap.
        
        Formula:
        - Higher search volume = higher opportunity
        - Lower difficulty = higher opportunity
        - Larger position gap or absence = higher opportunity (more to gain)
        - Weighted combination with volume as primary factor
        """
        try:
            # Normalize search volume (log scale)
            volume_score = min(np.log10(max(search_volume, 1)) * 15, 50)
            
            # Difficulty penalty (inverted, lower difficulty = higher score)
            difficulty_score = (100 - difficulty) * 0.25
            
            # Position gap score
            if our_position is None:
                # We're not ranking at all - high opportunity if competitor is ranking well
                position_gap_score = 30 if best_competitor_position <= 5 else 20
            else:
                # Calculate potential gain
                if our_position > 20:
                    position_gap_score = 25
                elif our_position > 10:
                    position_gap_score = 20
                else:
                    position_gap_score = max(15 - our_position, 5)
            
            opportunity = volume_score + difficulty_score + position_gap_score
            return min(round(opportunity, 2), 100.0)
            
        except Exception as e:
            logger.warning(f"Error calculating opportunity score: {e}")
            return 0.0

    def _determine_gap_type(
        self,
        our_position: Optional[int],
        competitor_positions: List[int]
    ) -> str:
        """Determine the type of keyword gap."""
        if our_position is None or our_position > 100:
            return "not_ranking"
        elif our_position > 20:
            return "ranking_low"
        elif our_position > 10:
            return "page_2"
        else:
            # We're in top 10 but competitors are ranking better
            best_comp_pos = min(competitor_positions) if competitor_positions else 100
            if best_comp_pos < our_position:
                return "position_gap"
        return "monitoring"

    async def analyze(
        self,
        gsc_data: pd.DataFrame,
        location_code: int = 2840,  # USA
        language_code: str = "en",
        max_keywords: int = 50
    ) -> Dict[str, Any]:
        """
        Main analysis function for competitor gap analysis.
        
        Process:
        1. Take top 50 GSC queries by impressions
        2. Call DataForSEO to get SERP rankings for each
        3. Identify competitors ranking in positions 1-10 where we're not
        4. Calculate difficulty and opportunity scores
        5. Return prioritized gaps
        
        Args:
            gsc_data: DataFrame with columns: query, position, clicks, impressions
            location_code: DataForSEO location code
            language_code: Language code
            max_keywords: Maximum keywords to analyze
            
        Returns:
            Dictionary with competitor_domains, keyword_gaps, and top_opportunities
        """
        logger.info("Starting competitor gap analysis")
        
        try:
            # Validate input data
            if gsc_data.empty:
                logger.warning("No GSC data provided")
                return self._empty_result()
            
            required_columns = ['query', 'position', 'impressions']
            if not all(col in gsc_data.columns for col in required_columns):
                logger.error(f"GSC data missing required columns. Has: {gsc_data.columns.tolist()}")
                return self._empty_result()
            
            # Identify user's domain
            user_domain = self._identify_user_domain(gsc_data)
            logger.info(f"Identified user domain: {user_domain}")
            
            # Select top keywords by impressions
            top_keywords_df = (
                gsc_data.nlargest(max_keywords, 'impressions')
                .copy()
            )
            
            logger.info(f"Analyzing top {len(top_keywords_df)} keywords")
            
            # Prepare keyword list with positions
            keywords_to_analyze = []
            user_positions = {}
            
            for _, row in top_keywords_df.iterrows():
                keyword = row['query']
                position = row.get('position')
                keywords_to_analyze.append(keyword)
                user_positions[keyword] = position if pd.notna(position) else None
            
            # Fetch SERP data from DataForSEO
            logger.info(f"Fetching SERP data for {len(keywords_to_analyze)} keywords")
            serp_results = await self.dataforseo_service.get_serp_results_batch(
                keywords=keywords_to_analyze,
                location_code=location_code,
                language_code=language_code
            )
            
            # Process SERP results
            competitor_data = defaultdict(lambda: {
                'keywords': set(),
                'positions': [],
                'total_appearances': 0
            })
            
            keyword_gaps = []
            
            for keyword, serp_result in zip(keywords_to_analyze, serp_results):
                if not serp_result or 'items' not in serp_result:
                    logger.warning(f"No SERP data for keyword: {keyword}")
                    continue
                
                our_position = user_positions.get(keyword)
                competitor_positions_for_keyword = []
                
                # Analyze organic results in positions 1-10
                organic_results = serp_result.get('items', [])[:10]
                
                for item in organic_results:
                    rank_group = item.get('rank_group', 0)
                    rank_absolute = item.get('rank_absolute', 0)
                    position = rank_absolute or rank_group
                    
                    if position > 10:
                        continue
                    
                    url = item.get('url', '')
                    domain = item.get('domain', '') or self._extract_domain_from_url(url)
                    
                    if not domain or domain == user_domain:
                        continue
                    
                    # This is a competitor
                    competitor_data[domain]['keywords'].add(keyword)
                    competitor_data[domain]['positions'].append(position)
                    competitor_data[domain]['total_appearances'] += 1
                    
                    competitor_positions_for_keyword.append({
                        'domain': domain,
                        'position': position,
                        'url': url
                    })
                
                # Determine if this is a gap opportunity
                # Gap exists if: we're not in top 10, or competitors rank better
                is_gap = False
                if our_position is None or our_position > 10:
                    is_gap = True
                elif competitor_positions_for_keyword:
                    best_competitor_pos = min(cp['position'] for cp in competitor_positions_for_keyword)
                    if best_competitor_pos < our_position:
                        is_gap = True
                
                if is_gap and competitor_positions_for_keyword:
                    # Calculate metrics
                    difficulty = self._calculate_keyword_difficulty(serp_result)
                    
                    # Get search volume from SERP data or GSC impressions as proxy
                    search_volume = serp_result.get('keyword_data', {}).get('keyword_info', {}).get('search_volume', 0)
                    if search_volume == 0:
                        # Use impressions as proxy
                        keyword_row = top_keywords_df[top_keywords_df['query'] == keyword]
                        if not keyword_row.empty:
                            search_volume = int(keyword_row.iloc[0]['impressions'])
                    
                    best_competitor_pos = min(cp['position'] for cp in competitor_positions_for_keyword)
                    
                    opportunity_score = self._calculate_opportunity_score(
                        search_volume=search_volume,
                        our_position=our_position,
                        best_competitor_position=best_competitor_pos,
                        difficulty=difficulty
                    )
                    
                    gap_type = self._determine_gap_type(
                        our_position=our_position,
                        competitor_positions=[cp['position'] for cp in competitor_positions_for_keyword]
                    )
                    
                    keyword_gaps.append(KeywordGap(
                        keyword=keyword,
                        our_position=our_position,
                        competitor_positions=competitor_positions_for_keyword,
                        search_volume=search_volume,
                        difficulty=difficulty,
                        opportunity_score=opportunity_score,
                        gap_type=gap_type
                    ))
            
            # Build competitor domain summaries
            competitor_domains = []
            
            for domain, data in competitor_data.items():
                keywords_in_top10 = len(data['keywords'])
                
                if keywords_in_top10 < 2:  # Filter out one-off competitors
                    continue
                
                avg_position = np.mean(data['positions']) if data['positions'] else 0
                
                # Calculate overlap with our keywords
                overlap = sum(1 for kw in data['keywords'] if user_positions.get(kw) is not None)
                
                # Calculate how many keywords they rank for that we don't (or rank poorly for)
                keywords_we_miss = sum(
                    1 for kw in data['keywords']
                    if user_positions.get(kw) is None or user_positions.get(kw, 100) > 20
                )
                
                # Threat score: combination of overlap and their ranking strength
                threat_score = (
                    (keywords_in_top10 * 2) +
                    (overlap * 3) +
                    (keywords_we_miss * 1.5) +
                    ((10 - avg_position) * 2)
                )
                
                competitor_domains.append(CompetitorDomain(
                    domain=domain,
                    keywords_in_top10=keywords_in_top10,
                    avg_position=round(avg_position, 1),
                    overlap_with_us=overlap,
                    keywords_we_miss=keywords_we_miss,
                    threat_score=round(threat_score, 2)
                ))
            
            # Sort by threat score
            competitor_domains.sort(key=lambda x: x.threat_score, reverse=True)
            
            # Sort keyword gaps by opportunity score
            keyword_gaps.sort(key=lambda x: x.opportunity_score, reverse=True)
            
            # Get top opportunities
            top_opportunities = keyword_gaps[:20]
            
            # Build result
            result = {
                'competitor_domains': [asdict(cd) for cd in competitor_domains[:15]],
                'keyword_gaps': [asdict(kg) for kg in keyword_gaps],
                'top_opportunities': [asdict(kg) for kg in top_opportunities],
                'summary': {
                    'total_keywords_analyzed': len(keywords_to_analyze),
                    'total_gaps_found': len(keyword_gaps),
                    'total_competitors_identified': len(competitor_domains),
                    'avg_opportunity_score': round(np.mean([kg.opportunity_score for kg in keyword_gaps]), 2) if keyword_gaps else 0,
                    'gaps_by_type': self._count_gaps_by_type(keyword_gaps),
                    'estimated_monthly_traffic_opportunity': sum(
                        self._estimate_traffic_gain(kg) for kg in top_opportunities
                    )
                },
                'analysis_date': datetime.utcnow().isoformat(),
                'parameters': {
                    'max_keywords': max_keywords,
                    'location_code': location_code,
                    'language_code': language_code
                }
            }
            
            logger.info(
                f"Competitor gap analysis complete. "
                f"Found {len(competitor_domains)} competitors and {len(keyword_gaps)} gaps"
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Error in competitor gap analysis: {e}", exc_info=True)
            return self._empty_result()

    def _count_gaps_by_type(self, keyword_gaps: List[KeywordGap]) -> Dict[str, int]:
        """Count keyword gaps by type."""
        counts = defaultdict(int)
        for gap in keyword_gaps:
            counts[gap.gap_type] += 1
        return dict(counts)

    def _estimate_traffic_gain(self, keyword_gap: KeywordGap) -> int:
        """
        Estimate monthly traffic gain if we captured this keyword.
        
        Uses simplified CTR curve:
        - Position 1: 30% CTR
        - Position 2: 15% CTR
        - Position 3: 10% CTR
        - Position 4-5: 7% CTR
        - Position 6-10: 3% CTR
        """
        ctr_curve = {
            1: 0.30,
            2: 0.15,
            3: 0.10,
            4: 0.07,
            5: 0.07,
            6: 0.03,
            7: 0.03,
            8: 0.03,
            9: 0.03,
            10: 0.03
        }
        
        # Estimate we could reach position 5 on average
        target_position = 5
        target_ctr = ctr_curve.get(target_position, 0.03)
        
        # Current CTR
        current_ctr = 0
        if keyword_gap.our_position and keyword_gap.our_position <= 10:
            current_ctr = ctr_curve.get(keyword_gap.our_position, 0.03)
        
        ctr_gain = target_ctr - current_ctr
        estimated_clicks = int(keyword_gap.search_volume * ctr_gain)
        
        return max(estimated_clicks, 0)

    def _empty_result(self) -> Dict[str, Any]:
        """Return empty result structure."""
        return {
            'competitor_domains': [],
            'keyword_gaps': [],
            'top_opportunities': [],
            'summary': {
                'total_keywords_analyzed': 0,
                'total_gaps_found': 0,
                'total_competitors_identified': 0,
                'avg_opportunity_score': 0,
                'gaps_by_type': {},
                'estimated_monthly_traffic_opportunity': 0
            },
            'analysis_date': datetime.utcnow().isoformat(),
            'parameters': {}
        }


# Synchronous wrapper for backwards compatibility
def analyze_competitor_gaps(
    gsc_data: pd.DataFrame,
    location_code: int = 2840,
    language_code: str = "en",
    max_keywords: int = 50
) -> Dict[str, Any]:
    """
    Synchronous wrapper for competitor gap analysis.
    
    Args:
        gsc_data: DataFrame with GSC query data
        location_code: DataForSEO location code (default: USA)
        language_code: Language code (default: en)
        max_keywords: Maximum keywords to analyze
        
    Returns:
        Analysis results dictionary
    """
    analyzer = CompetitorGapAnalyzer()
    
    # Run async function in event loop
    import asyncio
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    
    return loop.run_until_complete(
        analyzer.analyze(
            gsc_data=gsc_data,
            location_code=location_code,
            language_code=language_code,
            max_keywords=max_keywords
        )
    )
