"""
Module 3: Competitor Intelligence
Identifies top competitors, calculates visibility scores, finds keyword gaps,
and generates competitive positioning insights.
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
from collections import defaultdict, Counter
from dataclasses import dataclass
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class CompetitorProfile:
    """Profile for a single competitor domain."""
    domain: str
    keywords_shared: int
    avg_position: float
    visibility_score: float
    position_trend: Optional[str]
    keyword_list: List[str]
    avg_user_position_on_shared: float
    threat_level: str


@dataclass
class KeywordGap:
    """Keyword that competitor ranks for but user doesn't."""
    keyword: str
    competitor_domain: str
    competitor_position: int
    search_volume: int
    difficulty: float
    opportunity_score: float
    user_ranks: bool
    user_position: Optional[int]


def calculate_visibility_score(positions: List[int], impressions: List[int]) -> float:
    """
    Calculate visibility score for a competitor based on their positions and search volumes.
    
    Formula: sum(impression * position_weight) where position_weight = 1/position^1.5
    Normalized to 0-100 scale.
    """
    if not positions or not impressions:
        return 0.0
    
    visibility = 0.0
    for pos, imp in zip(positions, impressions):
        if pos > 0 and pos <= 100:  # Valid position range
            # Position weight: #1 = 1.0, #2 = 0.35, #3 = 0.19, #10 = 0.03
            position_weight = 1.0 / (pos ** 1.5)
            visibility += imp * position_weight
    
    # Normalize to 0-100 scale (arbitrary scaling factor based on typical values)
    normalized = min(100.0, (visibility / 1000.0))
    
    return round(normalized, 2)


def classify_threat_level(
    keywords_shared: int,
    avg_position: float,
    avg_user_position: float,
    visibility_score: float
) -> str:
    """
    Classify competitor threat level based on multiple factors.
    
    Returns: "critical", "high", "medium", or "low"
    """
    # Critical: competitor ranks better on many shared keywords
    if keywords_shared >= 15 and avg_position < avg_user_position - 2 and visibility_score > 60:
        return "critical"
    
    # High: strong presence across shared keywords
    if keywords_shared >= 10 and avg_position < avg_user_position and visibility_score > 40:
        return "high"
    
    # Medium: moderate overlap with comparable performance
    if keywords_shared >= 5 and abs(avg_position - avg_user_position) <= 3:
        return "medium"
    
    # Low: limited overlap or significantly worse performance
    return "low"


def extract_competitors_from_serp(
    serp_data: List[Dict[str, Any]],
    user_domain: str,
    min_appearances: int = 3
) -> Dict[str, List[Tuple[str, int, int]]]:
    """
    Extract competitor domains from SERP data.
    
    Args:
        serp_data: List of SERP results for multiple keywords
        user_domain: The user's domain to exclude
        min_appearances: Minimum number of keywords a domain must appear in
    
    Returns:
        Dict mapping competitor domain to list of (keyword, position, impressions)
    """
    competitor_data = defaultdict(list)
    
    for serp_result in serp_data:
        keyword = serp_result.get('keyword', '')
        impressions = serp_result.get('impressions', 0)
        organic_results = serp_result.get('organic_results', [])
        
        for result in organic_results:
            domain = result.get('domain', '')
            position = result.get('position', 0)
            
            # Skip user's domain and invalid data
            if not domain or domain == user_domain or position <= 0 or position > 20:
                continue
            
            competitor_data[domain].append((keyword, position, impressions))
    
    # Filter by minimum appearances
    filtered = {
        domain: appearances 
        for domain, appearances in competitor_data.items() 
        if len(appearances) >= min_appearances
    }
    
    return filtered


def analyze_keyword_overlap(
    user_keywords: Dict[str, Dict[str, Any]],
    competitor_data: Dict[str, List[Tuple[str, int, int]]],
    user_domain: str
) -> Dict[str, Dict[str, Any]]:
    """
    Analyze keyword overlap between user and competitors.
    
    Returns:
        Dict mapping competitor domain to overlap metrics
    """
    overlap_analysis = {}
    
    for competitor_domain, appearances in competitor_data.items():
        shared_keywords = []
        competitor_positions = []
        user_positions = []
        impressions_list = []
        
        for keyword, comp_pos, impressions in appearances:
            if keyword in user_keywords:
                shared_keywords.append(keyword)
                competitor_positions.append(comp_pos)
                user_positions.append(user_keywords[keyword].get('position', 999))
                impressions_list.append(impressions)
        
        if shared_keywords:
            overlap_analysis[competitor_domain] = {
                'shared_keywords': shared_keywords,
                'competitor_positions': competitor_positions,
                'user_positions': user_positions,
                'impressions': impressions_list,
                'overlap_count': len(shared_keywords),
                'avg_competitor_position': np.mean(competitor_positions),
                'avg_user_position': np.mean(user_positions)
            }
    
    return overlap_analysis


def find_keyword_gaps(
    user_keywords: Dict[str, Dict[str, Any]],
    competitor_data: Dict[str, List[Tuple[str, int, int]]],
    top_competitors: List[str],
    max_gaps: int = 50
) -> List[KeywordGap]:
    """
    Find keywords that competitors rank for but user doesn't (or ranks poorly).
    
    Args:
        user_keywords: User's keyword data with positions
        competitor_data: Competitor appearance data
        top_competitors: List of top competitor domains to analyze
        max_gaps: Maximum number of gaps to return
    
    Returns:
        List of KeywordGap objects, sorted by opportunity score
    """
    gaps = []
    keyword_competitor_map = defaultdict(list)
    
    # Build map of which competitors rank for which keywords
    for competitor_domain in top_competitors:
        if competitor_domain not in competitor_data:
            continue
            
        for keyword, position, impressions in competitor_data[competitor_domain]:
            keyword_competitor_map[keyword].append({
                'domain': competitor_domain,
                'position': position,
                'impressions': impressions
            })
    
    # Analyze gaps
    for keyword, competitors in keyword_competitor_map.items():
        user_data = user_keywords.get(keyword)
        user_ranks = user_data is not None
        user_position = user_data.get('position', None) if user_data else None
        
        # Consider it a gap if user doesn't rank, or ranks poorly (>20)
        is_gap = not user_ranks or (user_position and user_position > 20)
        
        if is_gap and competitors:
            # Find best competitor position for this keyword
            best_competitor = min(competitors, key=lambda x: x['position'])
            
            # Estimate difficulty based on top ranking positions
            top_positions = [c['position'] for c in competitors[:3]]
            difficulty = calculate_keyword_difficulty(top_positions)
            
            # Get search volume (impressions as proxy)
            search_volume = best_competitor['impressions']
            
            # Calculate opportunity score
            opportunity_score = calculate_opportunity_score(
                competitor_position=best_competitor['position'],
                search_volume=search_volume,
                difficulty=difficulty,
                num_competitors_ranking=len(competitors)
            )
            
            gap = KeywordGap(
                keyword=keyword,
                competitor_domain=best_competitor['domain'],
                competitor_position=best_competitor['position'],
                search_volume=search_volume,
                difficulty=difficulty,
                opportunity_score=opportunity_score,
                user_ranks=user_ranks,
                user_position=user_position
            )
            
            gaps.append(gap)
    
    # Sort by opportunity score and limit
    gaps.sort(key=lambda x: x.opportunity_score, reverse=True)
    return gaps[:max_gaps]


def calculate_keyword_difficulty(top_positions: List[int]) -> float:
    """
    Estimate keyword difficulty based on competitor positions.
    
    Returns value between 0-100, where 100 is most difficult.
    """
    if not top_positions:
        return 50.0
    
    avg_top_position = np.mean(top_positions)
    
    # If top competitors rank #1-3, it's harder to break in
    if avg_top_position <= 3:
        difficulty = 75 + (3 - avg_top_position) * 8
    # Positions 4-10 are moderately difficult
    elif avg_top_position <= 10:
        difficulty = 50 + (10 - avg_top_position) * 3
    # Beyond page 1 is easier
    else:
        difficulty = max(20, 50 - (avg_top_position - 10) * 2)
    
    return min(100.0, max(0.0, round(difficulty, 1)))


def calculate_opportunity_score(
    competitor_position: int,
    search_volume: int,
    difficulty: float,
    num_competitors_ranking: int
) -> float:
    """
    Calculate opportunity score for a keyword gap.
    
    Higher score = better opportunity.
    Factors: search volume (higher better), competitor position (mid-range better),
    difficulty (lower better), competition level (fewer better).
    """
    # Search volume component (0-50 points)
    volume_score = min(50, search_volume / 100)
    
    # Position component (0-25 points)
    # Sweet spot is positions 4-8 (easier to outrank than #1-3, more valuable than #10+)
    if 4 <= competitor_position <= 8:
        position_score = 25
    elif competitor_position <= 3:
        position_score = 15
    elif competitor_position <= 10:
        position_score = 20
    else:
        position_score = max(0, 20 - (competitor_position - 10))
    
    # Difficulty component (0-15 points, inverse)
    difficulty_score = (100 - difficulty) / 100 * 15
    
    # Competition component (0-10 points, inverse)
    competition_score = max(0, 10 - num_competitors_ranking)
    
    total_score = volume_score + position_score + difficulty_score + competition_score
    
    return round(total_score, 2)


def generate_competitive_insights(
    competitors: List[CompetitorProfile],
    gaps: List[KeywordGap],
    overlap_metrics: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Generate high-level competitive positioning insights.
    """
    insights = {
        'market_position': None,
        'primary_threats': [],
        'competitive_advantages': [],
        'strategic_recommendations': []
    }
    
    if not competitors:
        insights['market_position'] = 'insufficient_data'
        return insights
    
    # Analyze market position
    critical_threats = [c for c in competitors if c.threat_level == 'critical']
    high_threats = [c for c in competitors if c.threat_level == 'high']
    
    avg_user_position = overlap_metrics.get('user_avg_position', 0)
    avg_competitor_position = overlap_metrics.get('competitor_avg_position', 0)
    
    if len(critical_threats) >= 3:
        insights['market_position'] = 'under_pressure'
    elif len(critical_threats) >= 1 or len(high_threats) >= 3:
        insights['market_position'] = 'competitive'
    elif avg_user_position < avg_competitor_position:
        insights['market_position'] = 'strong'
    else:
        insights['market_position'] = 'emerging'
    
    # Identify primary threats (top 3 by threat level and visibility)
    sorted_competitors = sorted(
        competitors,
        key=lambda c: (
            {'critical': 4, 'high': 3, 'medium': 2, 'low': 1}[c.threat_level],
            c.visibility_score
        ),
        reverse=True
    )
    
    for comp in sorted_competitors[:3]:
        insights['primary_threats'].append({
            'domain': comp.domain,
            'threat_level': comp.threat_level,
            'visibility_score': comp.visibility_score,
            'keywords_shared': comp.keywords_shared,
            'reason': f"Ranks better on {comp.keywords_shared} shared keywords with avg position {comp.avg_position:.1f}"
        })
    
    # Identify competitive advantages
    if avg_user_position < avg_competitor_position:
        position_advantage = avg_competitor_position - avg_user_position
        insights['competitive_advantages'].append({
            'type': 'position_advantage',
            'description': f"Average position {position_advantage:.1f} spots better than competitors on shared keywords"
        })
    
    # Count keywords where user ranks #1-3
    top_positions = sum(1 for c in competitors for pos in c.keyword_list if avg_user_position <= 3)
    if top_positions > 0:
        insights['competitive_advantages'].append({
            'type': 'top_rankings',
            'description': f"Holds top 3 positions on multiple competitive keywords"
        })
    
    # Strategic recommendations
    if len(gaps) > 20:
        insights['strategic_recommendations'].append({
            'priority': 'high',
            'action': 'content_gap_filling',
            'description': f"Create content for {len(gaps)} keywords where competitors rank but you don't",
            'estimated_opportunity': sum(g.search_volume for g in gaps[:20])
        })
    
    if critical_threats:
        insights['strategic_recommendations'].append({
            'priority': 'high',
            'action': 'competitive_defense',
            'description': f"Strengthen content on keywords where {critical_threats[0].domain} is outranking you",
            'target_competitor': critical_threats[0].domain
        })
    
    if insights['market_position'] == 'emerging':
        insights['strategic_recommendations'].append({
            'priority': 'medium',
            'action': 'market_expansion',
            'description': "Focus on striking distance keywords (positions 8-15) to establish stronger presence"
        })
    
    return insights


def analyze_competitor_intelligence(
    gsc_data: pd.DataFrame,
    serp_data: List[Dict[str, Any]],
    user_domain: str,
    top_n_keywords: int = 50
) -> Dict[str, Any]:
    """
    Main analysis function for Module 3: Competitor Intelligence.
    
    Args:
        gsc_data: GSC data with query, page, position, clicks, impressions
        serp_data: DataForSEO SERP results for top keywords
        user_domain: User's primary domain
        top_n_keywords: Number of top keywords to analyze
    
    Returns:
        Structured competitor intelligence report
    """
    logger.info(f"Starting competitor intelligence analysis for {user_domain}")
    
    try:
        # Prepare user keyword data
        user_keywords = {}
        if not gsc_data.empty:
            # Group by query to get best position and total impressions
            query_groups = gsc_data.groupby('query').agg({
                'position': 'mean',
                'clicks': 'sum',
                'impressions': 'sum'
            }).reset_index()
            
            for _, row in query_groups.iterrows():
                user_keywords[row['query']] = {
                    'position': row['position'],
                    'clicks': row['clicks'],
                    'impressions': row['impressions']
                }
        
        logger.info(f"Loaded {len(user_keywords)} user keywords from GSC")
        
        # Extract competitors from SERP data
        competitor_raw_data = extract_competitors_from_serp(
            serp_data=serp_data,
            user_domain=user_domain,
            min_appearances=3
        )
        
        logger.info(f"Found {len(competitor_raw_data)} potential competitors in SERP data")
        
        if not competitor_raw_data:
            return {
                'competitors': [],
                'keyword_gaps': [],
                'overlap_metrics': {
                    'total_keywords_analyzed': len(user_keywords),
                    'total_competitors_found': 0,
                    'avg_competitors_per_keyword': 0
                },
                'competitive_insights': {
                    'market_position': 'insufficient_data',
                    'primary_threats': [],
                    'competitive_advantages': [],
                    'strategic_recommendations': []
                }
            }
        
        # Analyze keyword overlap
        overlap_analysis = analyze_keyword_overlap(
            user_keywords=user_keywords,
            competitor_data=competitor_raw_data,
            user_domain=user_domain
        )
        
        # Build competitor profiles
        competitor_profiles = []
        
        for domain, overlap_data in overlap_analysis.items():
            positions = overlap_data['competitor_positions']
            impressions_list = overlap_data['impressions']
            
            visibility = calculate_visibility_score(positions, impressions_list)
            
            threat = classify_threat_level(
                keywords_shared=overlap_data['overlap_count'],
                avg_position=overlap_data['avg_competitor_position'],
                avg_user_position=overlap_data['avg_user_position'],
                visibility_score=visibility
            )
            
            profile = CompetitorProfile(
                domain=domain,
                keywords_shared=overlap_data['overlap_count'],
                avg_position=round(overlap_data['avg_competitor_position'], 1),
                visibility_score=visibility,
                position_trend=None,  # Would need historical data
                keyword_list=overlap_data['shared_keywords'],
                avg_user_position_on_shared=round(overlap_data['avg_user_position'], 1),
                threat_level=threat
            )
            
            competitor_profiles.append(profile)
        
        # Sort by visibility score
        competitor_profiles.sort(key=lambda x: x.visibility_score, reverse=True)
        
        # Take top competitors for gap analysis
        top_competitors = [c.domain for c in competitor_profiles[:10]]
        
        # Find keyword gaps
        keyword_gaps = find_keyword_gaps(
            user_keywords=user_keywords,
            competitor_data=competitor_raw_data,
            top_competitors=top_competitors,
            max_gaps=50
        )
        
        logger.info(f"Identified {len(keyword_gaps)} keyword gap opportunities")
        
        # Calculate overlap metrics
        total_competitors_per_keyword = []
        for appearances in competitor_raw_data.values():
            keywords = set(kw for kw, _, _ in appearances)
            for kw in keywords:
                total_competitors_per_keyword.append(kw)
        
        keyword_competition_count = Counter(total_competitors_per_keyword)
        avg_competitors = (
            np.mean(list(keyword_competition_count.values())) 
            if keyword_competition_count else 0
        )
        
        overlap_metrics = {
            'total_keywords_analyzed': len(user_keywords),
            'total_competitors_found': len(competitor_profiles),
            'avg_competitors_per_keyword': round(avg_competitors, 1),
            'keywords_with_competition': len(keyword_competition_count),
            'user_avg_position': round(np.mean([d['position'] for d in user_keywords.values()]), 1) if user_keywords else 0,
            'competitor_avg_position': round(np.mean([c.avg_position for c in competitor_profiles]), 1) if competitor_profiles else 0
        }
        
        # Generate competitive insights
        competitive_insights = generate_competitive_insights(
            competitors=competitor_profiles,
            gaps=keyword_gaps,
            overlap_metrics=overlap_metrics
        )
        
        # Format output
        result = {
            'competitors': [
                {
                    'domain': c.domain,
                    'keywords_shared': c.keywords_shared,
                    'avg_position': c.avg_position,
                    'visibility_score': c.visibility_score,
                    'threat_level': c.threat_level,
                    'user_avg_position_on_shared': c.avg_user_position_on_shared,
                    'position_advantage': round(c.avg_user_position_on_shared - c.avg_position, 1)
                }
                for c in competitor_profiles
            ],
            'keyword_gaps': [
                {
                    'keyword': g.keyword,
                    'competitor_domain': g.competitor_domain,
                    'competitor_position': g.competitor_position,
                    'search_volume': g.search_volume,
                    'difficulty': g.difficulty,
                    'opportunity_score': g.opportunity_score,
                    'user_ranks': g.user_ranks,
                    'user_position': g.user_position
                }
                for g in keyword_gaps
            ],
            'overlap_metrics': overlap_metrics,
            'competitive_insights': competitive_insights
        }
        
        logger.info("Competitor intelligence analysis completed successfully")
        
        return result
        
    except Exception as e:
        logger.error(f"Error in competitor intelligence analysis: {str(e)}", exc_info=True)
        raise


def get_competitive_summary(analysis_result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generate a concise summary of competitive analysis for quick reference.
    """
    competitors = analysis_result.get('competitors', [])
    gaps = analysis_result.get('keyword_gaps', [])
    insights = analysis_result.get('competitive_insights', {})
    
    # Top threats
    critical_competitors = [c for c in competitors if c['threat_level'] in ['critical', 'high']]
    
    # High-value gaps
    top_gaps = sorted(gaps, key=lambda x: x['opportunity_score'], reverse=True)[:10]
    total_gap_volume = sum(g['search_volume'] for g in gaps)
    
    summary = {
        'competitive_landscape': insights.get('market_position', 'unknown'),
        'total_competitors_tracked': len(competitors),
        'critical_threats': len([c for c in competitors if c['threat_level'] == 'critical']),
        'top_competitor': competitors[0]['domain'] if competitors else None,
        'total_keyword_gaps': len(gaps),
        'total_gap_search_volume': total_gap_volume,
        'top_gap_opportunities': [
            {
                'keyword': g['keyword'],
                'opportunity_score': g['opportunity_score'],
                'search_volume': g['search_volume']
            }
            for g in top_gaps[:5]
        ],
        'immediate_actions': len([r for r in insights.get('strategic_recommendations', []) if r.get('priority') == 'high'])
    }
    
    return summary
