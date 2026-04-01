"""
Module 3: Competitor & Market Intelligence

Uses DataForSEO to map top competitors by search overlap, identify content gaps,
and analyze competitive positioning for target keywords.
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
from collections import defaultdict, Counter
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from dataclasses import dataclass, asdict

logger = logging.getLogger(__name__)


@dataclass
class CompetitorProfile:
    """Profile of a competitor in the search landscape."""
    domain: str
    keywords_shared: int
    avg_position: float
    visibility_score: float  # Weighted by keyword search volume
    position_trend: Optional[str]  # 'improving', 'declining', 'stable'
    threat_level: str  # 'high', 'medium', 'low'
    url_examples: List[str]
    shared_keyword_examples: List[Dict[str, Any]]


@dataclass
class ContentGap:
    """A content gap opportunity identified through competitor analysis."""
    keyword: str
    search_volume: int
    difficulty_score: float
    user_current_position: Optional[int]
    competitors_ranking: List[Dict[str, Any]]
    intent_type: str
    gap_type: str  # 'missing', 'underperforming', 'format_mismatch'
    opportunity_score: float
    recommended_action: str


@dataclass
class CompetitiveLandscape:
    """Overall competitive landscape analysis."""
    primary_competitors: List[CompetitorProfile]
    market_concentration: float  # 0-1, higher = more concentrated
    user_market_share: float  # 0-1, estimated click share
    content_gaps: List[ContentGap]
    competitive_advantages: List[str]
    competitive_weaknesses: List[str]
    total_addressable_opportunity: int  # Monthly clicks available


def analyze_competitor_intelligence(
    serp_data: List[Dict[str, Any]],
    gsc_keyword_data: pd.DataFrame,
    user_domain: str,
    top_n_competitors: int = 10
) -> Dict[str, Any]:
    """
    Main analysis function for Module 3: Competitor & Market Intelligence.
    
    Args:
        serp_data: List of SERP results from DataForSEO for top keywords
        gsc_keyword_data: GSC data with query, clicks, impressions, position
        user_domain: The user's domain being analyzed
        top_n_competitors: Number of top competitors to analyze in detail
        
    Returns:
        Dictionary with comprehensive competitor intelligence
    """
    logger.info(f"Starting competitor intelligence analysis for {user_domain}")
    
    # Step 1: Extract and normalize competitor domains from SERP data
    competitor_mapping = _build_competitor_mapping(serp_data, user_domain)
    
    # Step 2: Calculate visibility scores and identify top competitors
    top_competitors = _identify_top_competitors(
        competitor_mapping, 
        gsc_keyword_data,
        top_n_competitors
    )
    
    # Step 3: Analyze competitive positioning for each keyword
    positioning_analysis = _analyze_competitive_positioning(
        serp_data,
        gsc_keyword_data,
        user_domain,
        top_competitors
    )
    
    # Step 4: Identify content gaps
    content_gaps = _identify_content_gaps(
        serp_data,
        gsc_keyword_data,
        user_domain,
        competitor_mapping
    )
    
    # Step 5: Calculate market concentration and user's market share
    market_metrics = _calculate_market_metrics(
        competitor_mapping,
        gsc_keyword_data,
        user_domain
    )
    
    # Step 6: Identify competitive advantages and weaknesses
    competitive_profile = _analyze_competitive_profile(
        positioning_analysis,
        top_competitors,
        user_domain
    )
    
    # Step 7: Build detailed competitor profiles
    competitor_profiles = _build_competitor_profiles(
        top_competitors,
        competitor_mapping,
        gsc_keyword_data,
        serp_data
    )
    
    # Step 8: Synthesize into landscape
    landscape = CompetitiveLandscape(
        primary_competitors=competitor_profiles,
        market_concentration=market_metrics['concentration'],
        user_market_share=market_metrics['user_share'],
        content_gaps=content_gaps,
        competitive_advantages=competitive_profile['advantages'],
        competitive_weaknesses=competitive_profile['weaknesses'],
        total_addressable_opportunity=market_metrics['total_opportunity']
    )
    
    return _format_output(landscape, positioning_analysis)


def _build_competitor_mapping(
    serp_data: List[Dict[str, Any]], 
    user_domain: str
) -> Dict[str, Dict[str, Any]]:
    """
    Build a mapping of competitors from SERP data.
    
    Returns:
        Dict mapping domain to {keywords: [], positions: [], urls: []}
    """
    competitor_map = defaultdict(lambda: {
        'keywords': [],
        'positions': [],
        'urls': [],
        'serp_features': []
    })
    
    for serp_result in serp_data:
        keyword = serp_result.get('keyword', '')
        items = serp_result.get('items', [])
        
        for item in items:
            if item.get('type') != 'organic':
                continue
                
            url = item.get('url', '')
            domain = _extract_domain(url)
            
            # Skip the user's own domain
            if domain == user_domain or not domain:
                continue
            
            position = item.get('rank_absolute', 0)
            
            competitor_map[domain]['keywords'].append(keyword)
            competitor_map[domain]['positions'].append(position)
            competitor_map[domain]['urls'].append(url)
            
            # Track any special SERP features this competitor has
            features = []
            if item.get('is_featured_snippet'):
                features.append('featured_snippet')
            if item.get('faq'):
                features.append('faq')
            if item.get('rating'):
                features.append('rating')
            
            competitor_map[domain]['serp_features'].extend(features)
    
    return dict(competitor_map)


def _extract_domain(url: str) -> str:
    """Extract domain from URL."""
    if not url:
        return ''
    
    try:
        # Remove protocol
        url = url.replace('https://', '').replace('http://', '')
        # Remove www
        url = url.replace('www.', '')
        # Extract domain (before first /)
        domain = url.split('/')[0]
        # Remove port if present
        domain = domain.split(':')[0]
        return domain.lower()
    except Exception as e:
        logger.warning(f"Error extracting domain from {url}: {e}")
        return ''


def _identify_top_competitors(
    competitor_mapping: Dict[str, Dict[str, Any]],
    gsc_data: pd.DataFrame,
    top_n: int
) -> List[Dict[str, Any]]:
    """
    Identify top N competitors by visibility score.
    
    Visibility score = weighted sum of (impressions / (position^2)) for shared keywords
    """
    competitors = []
    
    for domain, data in competitor_mapping.items():
        keywords = data['keywords']
        positions = data['positions']
        
        # Calculate visibility score
        visibility_score = 0.0
        total_impressions = 0
        
        for keyword, position in zip(keywords, positions):
            # Find keyword in GSC data
            keyword_data = gsc_data[gsc_data['query'] == keyword]
            if keyword_data.empty:
                continue
            
            impressions = keyword_data['impressions'].iloc[0]
            total_impressions += impressions
            
            # Position-weighted visibility score
            # Higher positions (lower numbers) contribute more
            if position > 0:
                visibility_score += impressions / (position ** 1.5)
        
        avg_position = np.mean(positions) if positions else 0
        
        competitors.append({
            'domain': domain,
            'keywords_shared': len(set(keywords)),
            'avg_position': avg_position,
            'visibility_score': visibility_score,
            'total_impressions_overlap': total_impressions
        })
    
    # Sort by visibility score and return top N
    competitors.sort(key=lambda x: x['visibility_score'], reverse=True)
    return competitors[:top_n]


def _analyze_competitive_positioning(
    serp_data: List[Dict[str, Any]],
    gsc_data: pd.DataFrame,
    user_domain: str,
    top_competitors: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Analyze competitive positioning for each keyword.
    """
    positioning = []
    top_competitor_domains = {c['domain'] for c in top_competitors}
    
    for serp_result in serp_data:
        keyword = serp_result.get('keyword', '')
        items = serp_result.get('items', [])
        
        # Find user's position
        user_position = None
        user_url = None
        for item in items:
            if item.get('type') != 'organic':
                continue
            url = item.get('url', '')
            domain = _extract_domain(url)
            if domain == user_domain:
                user_position = item.get('rank_absolute')
                user_url = url
                break
        
        # Find competitor positions
        competitor_positions = {}
        for item in items:
            if item.get('type') != 'organic':
                continue
            url = item.get('url', '')
            domain = _extract_domain(url)
            if domain in top_competitor_domains:
                if domain not in competitor_positions:
                    competitor_positions[domain] = {
                        'position': item.get('rank_absolute'),
                        'url': url
                    }
        
        # Get GSC data for this keyword
        keyword_gsc = gsc_data[gsc_data['query'] == keyword]
        impressions = keyword_gsc['impressions'].iloc[0] if not keyword_gsc.empty else 0
        clicks = keyword_gsc['clicks'].iloc[0] if not keyword_gsc.empty else 0
        
        positioning.append({
            'keyword': keyword,
            'user_position': user_position,
            'user_url': user_url,
            'competitor_positions': competitor_positions,
            'impressions': impressions,
            'clicks': clicks,
            'user_outranked_by': len([c for c in competitor_positions.values() 
                                     if user_position and c['position'] < user_position])
        })
    
    return positioning


def _identify_content_gaps(
    serp_data: List[Dict[str, Any]],
    gsc_data: pd.DataFrame,
    user_domain: str,
    competitor_mapping: Dict[str, Dict[str, Any]]
) -> List[ContentGap]:
    """
    Identify content gaps where competitors are ranking but user is not,
    or where user is significantly underperforming.
    """
    gaps = []
    
    # Find keywords where competitors appear but user doesn't or ranks poorly
    competitor_keywords = set()
    for domain_data in competitor_mapping.values():
        competitor_keywords.update(domain_data['keywords'])
    
    # Get user's ranking keywords
    user_keywords = set(gsc_data['query'].tolist())
    
    for serp_result in serp_data:
        keyword = serp_result.get('keyword', '')
        items = serp_result.get('items', [])
        
        # Determine user's position
        user_position = None
        for item in items:
            if item.get('type') != 'organic':
                continue
            url = item.get('url', '')
            if _extract_domain(url) == user_domain:
                user_position = item.get('rank_absolute')
                break
        
        # Get competitor rankings
        competitors_ranking = []
        for item in items:
            if item.get('type') != 'organic':
                continue
            url = item.get('url', '')
            domain = _extract_domain(url)
            if domain != user_domain and domain:
                competitors_ranking.append({
                    'domain': domain,
                    'position': item.get('rank_absolute'),
                    'url': url,
                    'title': item.get('title', '')
                })
        
        # Get keyword metrics from GSC
        keyword_gsc = gsc_data[gsc_data['query'] == keyword]
        impressions = keyword_gsc['impressions'].iloc[0] if not keyword_gsc.empty else 0
        
        # Classify the gap
        gap_type = None
        opportunity_score = 0.0
        recommended_action = ''
        
        if user_position is None:
            # User not ranking at all
            if impressions > 100:  # But has search demand
                gap_type = 'missing'
                opportunity_score = impressions * 0.15  # Assume 15% CTR if ranking #5
                recommended_action = 'Create new content targeting this keyword'
        elif user_position > 10:
            # User ranking but on page 2+
            gap_type = 'underperforming'
            # Count how many competitors are in top 10
            top10_competitors = len([c for c in competitors_ranking if c['position'] <= 10])
            opportunity_score = impressions * (0.15 - (0.01 if user_position else 0))
            recommended_action = f'Improve existing content to break into top 10 (currently #{user_position})'
        elif user_position and user_position <= 10:
            # User in top 10 but could improve
            better_competitors = len([c for c in competitors_ranking if c['position'] < user_position])
            if better_competitors >= 3:
                gap_type = 'format_mismatch'
                current_ctr = _estimate_ctr_by_position(user_position)
                target_ctr = _estimate_ctr_by_position(max(3, user_position - 3))
                opportunity_score = impressions * (target_ctr - current_ctr)
                recommended_action = f'Optimize content format/structure (currently #{user_position})'
        
        if gap_type:
            # Classify intent
            intent_type = _classify_query_intent(keyword)
            
            # Calculate difficulty score based on competitor strength
            avg_competitor_pos = np.mean([c['position'] for c in competitors_ranking[:5]]) if competitors_ranking else 10
            difficulty_score = min(1.0, len(competitors_ranking) / 10 * (1 - (avg_competitor_pos / 10)))
            
            gaps.append(ContentGap(
                keyword=keyword,
                search_volume=int(impressions),  # Using impressions as proxy
                difficulty_score=difficulty_score,
                user_current_position=user_position,
                competitors_ranking=competitors_ranking[:5],  # Top 5 only
                intent_type=intent_type,
                gap_type=gap_type,
                opportunity_score=opportunity_score,
                recommended_action=recommended_action
            ))
    
    # Sort by opportunity score
    gaps.sort(key=lambda x: x.opportunity_score, reverse=True)
    
    return gaps[:50]  # Return top 50 gaps


def _estimate_ctr_by_position(position: int) -> float:
    """Estimate CTR based on organic position."""
    ctr_curve = {
        1: 0.28, 2: 0.15, 3: 0.11, 4: 0.08, 5: 0.06,
        6: 0.05, 7: 0.04, 8: 0.03, 9: 0.025, 10: 0.02
    }
    if position <= 10:
        return ctr_curve.get(position, 0.02)
    elif position <= 20:
        return 0.01
    else:
        return 0.005


def _classify_query_intent(query: str) -> str:
    """Classify query intent based on keywords and patterns."""
    query_lower = query.lower()
    
    # Transactional signals
    transactional_terms = ['buy', 'purchase', 'order', 'shop', 'price', 'cheap', 'discount', 'deal', 'coupon']
    if any(term in query_lower for term in transactional_terms):
        return 'transactional'
    
    # Commercial investigation signals
    commercial_terms = ['best', 'top', 'review', 'vs', 'versus', 'compare', 'alternative', 'recommend']
    if any(term in query_lower for term in commercial_terms):
        return 'commercial'
    
    # Informational signals
    informational_terms = ['how', 'what', 'why', 'when', 'where', 'who', 'guide', 'tutorial', 'learn', 'meaning']
    if any(term in query_lower for term in informational_terms):
        return 'informational'
    
    # Navigational (brand/domain names)
    if len(query_lower.split()) <= 2 and '.' not in query_lower:
        return 'navigational'
    
    return 'informational'  # Default


def _calculate_market_metrics(
    competitor_mapping: Dict[str, Dict[str, Any]],
    gsc_data: pd.DataFrame,
    user_domain: str
) -> Dict[str, Any]:
    """
    Calculate market concentration and user's market share.
    """
    # Calculate total impressions across all keywords
    total_market_impressions = gsc_data['impressions'].sum()
    user_clicks = gsc_data['clicks'].sum()
    
    # Calculate market concentration (HHI - Herfindahl Index)
    domain_impressions = {}
    domain_impressions[user_domain] = user_clicks  # Use clicks as proxy for user's share
    
    for domain, data in competitor_mapping.items():
        keywords = data['keywords']
        positions = data['positions']
        
        domain_estimated_clicks = 0
        for keyword, position in zip(keywords, positions):
            keyword_data = gsc_data[gsc_data['query'] == keyword]
            if keyword_data.empty:
                continue
            impressions = keyword_data['impressions'].iloc[0]
            estimated_ctr = _estimate_ctr_by_position(position)
            domain_estimated_clicks += impressions * estimated_ctr
        
        domain_impressions[domain] = domain_estimated_clicks
    
    total_estimated_clicks = sum(domain_impressions.values())
    
    if total_estimated_clicks > 0:
        market_shares = {d: clicks / total_estimated_clicks 
                        for d, clicks in domain_impressions.items()}
        hhi = sum(share ** 2 for share in market_shares.values())
        user_share = market_shares.get(user_domain, 0)
    else:
        hhi = 0
        user_share = 0
    
    # Calculate total addressable opportunity
    # Sum of estimated clicks for all competitors ranking above user
    total_opportunity = 0
    for keyword_row in gsc_data.itertuples():
        keyword = keyword_row.query
        user_position = keyword_row.position
        impressions = keyword_row.impressions
        
        # Find positions of top competitors
        for domain, data in competitor_mapping.items():
            if keyword in data['keywords']:
                idx = data['keywords'].index(keyword)
                comp_position = data['positions'][idx]
                if comp_position < user_position:
                    # This competitor is ranking better
                    comp_ctr = _estimate_ctr_by_position(comp_position)
                    total_opportunity += impressions * comp_ctr
    
    return {
        'concentration': hhi,
        'user_share': user_share,
        'total_opportunity': int(total_opportunity)
    }


def _analyze_competitive_profile(
    positioning_analysis: List[Dict[str, Any]],
    top_competitors: List[Dict[str, Any]],
    user_domain: str
) -> Dict[str, List[str]]:
    """
    Identify competitive advantages and weaknesses.
    """
    advantages = []
    weaknesses = []
    
    # Analyze keywords where user outranks major competitors
    outranking_count = 0
    outranked_count = 0
    
    for pos in positioning_analysis:
        if pos['user_position'] is None:
            continue
            
        for domain, comp_data in pos['competitor_positions'].items():
            if comp_data['position'] > pos['user_position']:
                outranking_count += 1
            else:
                outranked_count += 1
    
    total_comparisons = outranking_count + outranked_count
    if total_comparisons > 0:
        win_rate = outranking_count / total_comparisons
        if win_rate > 0.6:
            advantages.append(f"Strong competitive positioning: outranking competitors {win_rate:.1%} of the time")
        elif win_rate < 0.4:
            weaknesses.append(f"Weak competitive positioning: outranked by competitors {(1-win_rate):.1%} of the time")
    
    # Analyze position distribution
    user_positions = [p['user_position'] for p in positioning_analysis if p['user_position']]
    if user_positions:
        avg_position = np.mean(user_positions)
        if avg_position <= 5:
            advantages.append(f"Strong average ranking position: {avg_position:.1f}")
        elif avg_position > 10:
            weaknesses.append(f"Poor average ranking position: {avg_position:.1f}")
        
        # Position volatility
        position_std = np.std(user_positions)
        if position_std < 3:
            advantages.append("Consistent ranking positions across keywords")
        elif position_std > 5:
            weaknesses.append("High ranking volatility - inconsistent performance")
    
    # Analyze coverage
    total_keywords = len(positioning_analysis)
    keywords_ranking = len([p for p in positioning_analysis if p['user_position']])
    coverage_rate = keywords_ranking / total_keywords if total_keywords > 0 else 0
    
    if coverage_rate < 0.5:
        weaknesses.append(f"Low keyword coverage: ranking for only {coverage_rate:.1%} of target keywords")
    elif coverage_rate > 0.8:
        advantages.append(f"High keyword coverage: ranking for {coverage_rate:.1%} of target keywords")
    
    # Analyze top 3 performance
    top3_count = len([p for p in positioning_analysis if p['user_position'] and p['user_position'] <= 3])
    if keywords_ranking > 0:
        top3_rate = top3_count / keywords_ranking
        if top3_rate > 0.3:
            advantages.append(f"Strong top-3 presence: {top3_rate:.1%} of rankings in top 3")
        elif top3_rate < 0.1:
            weaknesses.append(f"Weak top-3 presence: only {top3_rate:.1%} of rankings in top 3")
    
    # Default messages if nothing found
    if not advantages:
        advantages.append("Opportunity to establish stronger market position")
    if not weaknesses:
        weaknesses.append("Maintain current competitive standing")
    
    return {
        'advantages': advantages,
        'weaknesses': weaknesses
    }


def _build_competitor_profiles(
    top_competitors: List[Dict[str, Any]],
    competitor_mapping: Dict[str, Dict[str, Any]],
    gsc_data: pd.DataFrame,
    serp_data: List[Dict[str, Any]]
) -> List[CompetitorProfile]:
    """
    Build detailed profiles for each top competitor.
    """
    profiles = []
    
    for comp in top_competitors:
        domain = comp['domain']
        domain_data = competitor_mapping[domain]
        
        # Determine threat level based on visibility score and position
        visibility_percentile = comp['visibility_score']
        max_visibility = max(c['visibility_score'] for c in top_competitors)
        normalized_visibility = visibility_percentile / max_visibility if max_visibility > 0 else 0
        
        if normalized_visibility > 0.7 and comp['avg_position'] < 5:
            threat_level = 'high'
        elif normalized_visibility > 0.4 or comp['avg_position'] < 8:
            threat_level = 'medium'
        else:
            threat_level = 'low'
        
        # Get example shared keywords
        shared_keywords = []
        for keyword in list(set(domain_data['keywords']))[:5]:
            keyword_gsc = gsc_data[gsc_data['query'] == keyword]
            if not keyword_gsc.empty:
                shared_keywords.append({
                    'keyword': keyword,
                    'competitor_position': domain_data['positions'][domain_data['keywords'].index(keyword)],
                    'user_position': keyword_gsc['position'].iloc[0],
                    'impressions': int(keyword_gsc['impressions'].iloc[0])
                })
        
        # Get example URLs
        url_examples = list(set(domain_data['urls']))[:3]
        
        profiles.append(CompetitorProfile(
            domain=domain,
            keywords_shared=comp['keywords_shared'],
            avg_position=round(comp['avg_position'], 1),
            visibility_score=round(comp['visibility_score'], 1),
            position_trend=None,  # Would need historical data
            threat_level=threat_level,
            url_examples=url_examples,
            shared_keyword_examples=shared_keywords
        ))
    
    return profiles


def _format_output(
    landscape: CompetitiveLandscape,
    positioning_analysis: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Format the analysis output into the final structure.
    """
    return {
        'competitors': [
            {
                'domain': comp.domain,
                'keywords_shared': comp.keywords_shared,
                'avg_position': comp.avg_position,
                'visibility_score': comp.visibility_score,
                'threat_level': comp.threat_level,
                'url_examples': comp.url_examples,
                'shared_keyword_examples': comp.shared_keyword_examples
            }
            for comp in landscape.primary_competitors
        ],
        'market_overview': {
            'market_concentration': round(landscape.market_concentration, 3),
            'user_market_share': round(landscape.user_market_share, 3),
            'total_addressable_opportunity_monthly_clicks': landscape.total_addressable_opportunity,
            'competitive_advantages': landscape.competitive_advantages,
            'competitive_weaknesses': landscape.competitive_weaknesses
        },
        'content_gaps': [
            {
                'keyword': gap.keyword,
                'search_volume': gap.search_volume,
                'difficulty_score': round(gap.difficulty_score, 2),
                'user_current_position': gap.user_current_position,
                'competitors_ranking': gap.competitors_ranking,
                'intent_type': gap.intent_type,
                'gap_type': gap.gap_type,
                'opportunity_score': round(gap.opportunity_score, 1),
                'recommended_action': gap.recommended_action
            }
            for gap in landscape.content_gaps
        ],
        'positioning_details': [
            {
                'keyword': pos['keyword'],
                'user_position': pos['user_position'],
                'user_url': pos['user_url'],
                'competitor_positions': pos['competitor_positions'],
                'impressions': int(pos['impressions']),
                'clicks': int(pos['clicks']),
                'user_outranked_by': pos['user_outranked_by']
            }
            for pos in positioning_analysis[:100]  # Limit to top 100 for output size
        ],
        'summary': {
            'total_competitors_analyzed': len(landscape.primary_competitors),
            'total_content_gaps_identified': len(landscape.content_gaps),
            'high_threat_competitors': len([c for c in landscape.primary_competitors if c.threat_level == 'high']),
            'total_opportunity_clicks': landscape.total_addressable_opportunity
        }
    }
