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
from urllib.parse import urlparse

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
    
    # Normalize user domain
    user_domain = _normalize_domain(user_domain)
    
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
        user_domain
    )
    
    # Step 4: Identify content gaps
    content_gaps = _identify_content_gaps(
        serp_data,
        gsc_keyword_data,
        competitor_mapping,
        user_domain
    )
    
    # Step 5: Calculate market metrics
    market_metrics = _calculate_market_metrics(
        serp_data,
        competitor_mapping,
        user_domain,
        gsc_keyword_data
    )
    
    # Step 6: Determine competitive advantages and weaknesses
    strengths_weaknesses = _analyze_strengths_weaknesses(
        positioning_analysis,
        competitor_mapping,
        user_domain
    )
    
    # Step 7: Build competitor profiles
    competitor_profiles = _build_competitor_profiles(
        top_competitors,
        serp_data,
        gsc_keyword_data,
        user_domain
    )
    
    logger.info(f"Identified {len(competitor_profiles)} primary competitors")
    logger.info(f"Found {len(content_gaps)} content gap opportunities")
    
    return {
        "competitors": [asdict(cp) for cp in competitor_profiles],
        "market_concentration": market_metrics['concentration'],
        "user_market_share": market_metrics['user_share'],
        "content_gaps": [asdict(cg) for cg in content_gaps],
        "competitive_advantages": strengths_weaknesses['advantages'],
        "competitive_weaknesses": strengths_weaknesses['weaknesses'],
        "total_addressable_opportunity": market_metrics['total_opportunity'],
        "positioning_summary": {
            "keywords_analyzed": len(serp_data),
            "avg_user_position": positioning_analysis['avg_user_position'],
            "keywords_dominated": positioning_analysis['dominated_count'],
            "keywords_competitive": positioning_analysis['competitive_count'],
            "keywords_losing": positioning_analysis['losing_count'],
            "keywords_absent": positioning_analysis['absent_count']
        },
        "metadata": {
            "user_domain": user_domain,
            "analysis_date": datetime.now().isoformat(),
            "total_competitors_identified": len(competitor_mapping),
            "keywords_with_serp_data": len(serp_data)
        }
    }


def _normalize_domain(domain: str) -> str:
    """Normalize domain to consistent format."""
    domain = domain.lower().strip()
    # Remove protocol if present
    domain = domain.replace('https://', '').replace('http://', '')
    # Remove www. if present
    domain = domain.replace('www.', '')
    # Remove trailing slash
    domain = domain.rstrip('/')
    # Extract just domain from full URL if needed
    if '/' in domain:
        domain = domain.split('/')[0]
    return domain


def _extract_domain_from_url(url: str) -> str:
    """Extract and normalize domain from URL."""
    try:
        parsed = urlparse(url)
        domain = parsed.netloc or parsed.path.split('/')[0]
        domain = domain.replace('www.', '').lower()
        return domain
    except:
        return url.lower()


def _build_competitor_mapping(
    serp_data: List[Dict[str, Any]], 
    user_domain: str
) -> Dict[str, Dict[str, Any]]:
    """
    Build mapping of competitors to keywords they rank for.
    
    Returns:
        Dict with structure:
        {
            'competitor.com': {
                'keywords': ['keyword1', 'keyword2'],
                'positions': [3, 5],
                'urls': ['url1', 'url2'],
                'avg_position': 4.0
            }
        }
    """
    competitor_data = defaultdict(lambda: {
        'keywords': [],
        'positions': [],
        'urls': [],
        'search_volumes': []
    })
    
    for serp_result in serp_data:
        keyword = serp_result.get('keyword', '')
        search_volume = serp_result.get('search_volume', 0)
        organic_results = serp_result.get('organic_results', [])
        
        for result in organic_results:
            url = result.get('url', '')
            position = result.get('position', 999)
            domain = _extract_domain_from_url(url)
            
            # Skip if it's the user's domain or invalid
            if not domain or domain == user_domain:
                continue
            
            competitor_data[domain]['keywords'].append(keyword)
            competitor_data[domain]['positions'].append(position)
            competitor_data[domain]['urls'].append(url)
            competitor_data[domain]['search_volumes'].append(search_volume)
    
    # Calculate aggregated metrics
    for domain, data in competitor_data.items():
        if data['positions']:
            data['avg_position'] = np.mean(data['positions'])
            data['keywords_count'] = len(set(data['keywords']))
            # Weighted visibility score
            data['visibility_score'] = sum(
                vol / (pos ** 2) for vol, pos in zip(data['search_volumes'], data['positions'])
            )
        else:
            data['avg_position'] = 999
            data['keywords_count'] = 0
            data['visibility_score'] = 0
    
    return dict(competitor_data)


def _identify_top_competitors(
    competitor_mapping: Dict[str, Dict[str, Any]],
    gsc_keyword_data: pd.DataFrame,
    top_n: int
) -> List[Tuple[str, Dict[str, Any]]]:
    """
    Identify top N competitors based on keyword overlap and visibility.
    """
    # Calculate overlap score for each competitor
    scored_competitors = []
    
    for domain, data in competitor_mapping.items():
        # Factors: keyword count, avg position, visibility score
        overlap_score = data['keywords_count']
        position_score = 1 / (data['avg_position'] + 1)  # Lower position = higher score
        visibility = data['visibility_score']
        
        # Combined score (weighted)
        total_score = (
            overlap_score * 0.4 +
            position_score * 100 * 0.3 +
            visibility * 0.3
        )
        
        scored_competitors.append((domain, data, total_score))
    
    # Sort by total score and take top N
    scored_competitors.sort(key=lambda x: x[2], reverse=True)
    return [(domain, data) for domain, data, score in scored_competitors[:top_n]]


def _analyze_competitive_positioning(
    serp_data: List[Dict[str, Any]],
    gsc_keyword_data: pd.DataFrame,
    user_domain: str
) -> Dict[str, Any]:
    """
    Analyze how user positions against competitors for each keyword.
    """
    user_positions = []
    dominated_count = 0  # User in top 3
    competitive_count = 0  # User in 4-7
    losing_count = 0  # User in 8-20
    absent_count = 0  # User not in top 20
    
    for serp_result in serp_data:
        keyword = serp_result.get('keyword', '')
        organic_results = serp_result.get('organic_results', [])
        
        user_position = None
        for result in organic_results:
            url = result.get('url', '')
            position = result.get('position', 999)
            domain = _extract_domain_from_url(url)
            
            if domain == user_domain:
                user_position = position
                break
        
        if user_position is not None:
            user_positions.append(user_position)
            if user_position <= 3:
                dominated_count += 1
            elif user_position <= 7:
                competitive_count += 1
            elif user_position <= 20:
                losing_count += 1
        else:
            absent_count += 1
    
    avg_position = np.mean(user_positions) if user_positions else None
    
    return {
        'avg_user_position': avg_position,
        'dominated_count': dominated_count,
        'competitive_count': competitive_count,
        'losing_count': losing_count,
        'absent_count': absent_count,
        'user_positions': user_positions
    }


def _identify_content_gaps(
    serp_data: List[Dict[str, Any]],
    gsc_keyword_data: pd.DataFrame,
    competitor_mapping: Dict[str, Dict[str, Any]],
    user_domain: str
) -> List[ContentGap]:
    """
    Identify content gaps where competitors are ranking but user is not,
    or where user is underperforming.
    """
    content_gaps = []
    
    # Create quick lookup for user's GSC data
    gsc_lookup = {}
    if not gsc_keyword_data.empty:
        for _, row in gsc_keyword_data.iterrows():
            gsc_lookup[row.get('query', '')] = {
                'position': row.get('position', None),
                'clicks': row.get('clicks', 0),
                'impressions': row.get('impressions', 0)
            }
    
    for serp_result in serp_data:
        keyword = serp_result.get('keyword', '')
        search_volume = serp_result.get('search_volume', 0)
        organic_results = serp_result.get('organic_results', [])
        
        # Find user's position
        user_position = None
        competitors_ranking = []
        
        for result in organic_results[:10]:  # Top 10 only
            url = result.get('url', '')
            position = result.get('position', 999)
            domain = _extract_domain_from_url(url)
            
            if domain == user_domain:
                user_position = position
            else:
                competitors_ranking.append({
                    'domain': domain,
                    'position': position,
                    'url': url,
                    'title': result.get('title', '')
                })
        
        # Determine gap type and opportunity
        gap_type = None
        opportunity_score = 0
        recommended_action = ""
        
        if user_position is None:
            # User is completely missing from top 20
            gap_type = 'missing'
            # Higher opportunity if search volume is good and top competitors aren't too strong
            avg_competitor_position = np.mean([c['position'] for c in competitors_ranking]) if competitors_ranking else 10
            opportunity_score = (search_volume / 100) * (1 / avg_competitor_position)
            recommended_action = "Create comprehensive content targeting this keyword"
        elif user_position > 10:
            # User is present but not on page 1
            gap_type = 'underperforming'
            # Calculate based on potential click gain
            gsc_data = gsc_lookup.get(keyword, {})
            current_clicks = gsc_data.get('clicks', 0)
            potential_clicks = _estimate_clicks_at_position(search_volume, 5)
            opportunity_score = potential_clicks - current_clicks
            recommended_action = "Optimize existing content to improve rankings"
        elif user_position > 3:
            # User is on page 1 but could be in top 3
            gap_type = 'underperforming'
            gsc_data = gsc_lookup.get(keyword, {})
            current_clicks = gsc_data.get('clicks', 0)
            potential_clicks = _estimate_clicks_at_position(search_volume, 2)
            opportunity_score = potential_clicks - current_clicks
            recommended_action = "Push into top 3 with targeted optimization"
        
        # Only add significant gaps
        if gap_type and opportunity_score > 5:  # Threshold: 5+ monthly clicks
            # Determine intent type
            intent_type = _classify_keyword_intent(keyword, organic_results)
            
            # Calculate difficulty
            difficulty_score = _calculate_keyword_difficulty(competitors_ranking)
            
            content_gaps.append(ContentGap(
                keyword=keyword,
                search_volume=search_volume,
                difficulty_score=difficulty_score,
                user_current_position=user_position,
                competitors_ranking=competitors_ranking[:5],  # Top 5 only
                intent_type=intent_type,
                gap_type=gap_type,
                opportunity_score=opportunity_score,
                recommended_action=recommended_action
            ))
    
    # Sort by opportunity score
    content_gaps.sort(key=lambda x: x.opportunity_score, reverse=True)
    
    return content_gaps[:50]  # Return top 50 opportunities


def _classify_keyword_intent(keyword: str, organic_results: List[Dict[str, Any]]) -> str:
    """
    Classify keyword intent based on keyword patterns and SERP composition.
    """
    keyword_lower = keyword.lower()
    
    # Transactional signals
    transactional_words = ['buy', 'price', 'cost', 'cheap', 'deal', 'discount', 'order', 'purchase', 'shop']
    if any(word in keyword_lower for word in transactional_words):
        return 'transactional'
    
    # Commercial investigation signals
    commercial_words = ['best', 'top', 'review', 'comparison', 'vs', 'alternative', 'versus']
    if any(word in keyword_lower for word in commercial_words):
        return 'commercial'
    
    # Navigational signals
    if len(keyword.split()) <= 2 and not any(word in keyword_lower for word in ['how', 'what', 'why', 'when']):
        return 'navigational'
    
    # Informational (default)
    return 'informational'


def _calculate_keyword_difficulty(competitors_ranking: List[Dict[str, Any]]) -> float:
    """
    Calculate keyword difficulty based on competitor strength in top 10.
    Returns score from 0-100.
    """
    if not competitors_ranking:
        return 0.0
    
    # Simple heuristic: more competitors in top positions = higher difficulty
    # Weight by position (higher positions = harder to beat)
    difficulty = 0
    for comp in competitors_ranking[:10]:
        position = comp['position']
        # Position 1 contributes most to difficulty
        position_weight = 1 / position
        difficulty += position_weight * 10
    
    # Normalize to 0-100 scale
    return min(difficulty, 100.0)


def _estimate_clicks_at_position(search_volume: int, position: int) -> float:
    """
    Estimate monthly clicks based on search volume and position.
    Uses standard CTR curve.
    """
    # Standard CTR curve (approximate)
    ctr_curve = {
        1: 0.28,
        2: 0.15,
        3: 0.11,
        4: 0.08,
        5: 0.06,
        6: 0.05,
        7: 0.04,
        8: 0.03,
        9: 0.025,
        10: 0.02
    }
    
    ctr = ctr_curve.get(position, 0.01)  # Default to 1% for positions > 10
    return search_volume * ctr


def _calculate_market_metrics(
    serp_data: List[Dict[str, Any]],
    competitor_mapping: Dict[str, Dict[str, Any]],
    user_domain: str,
    gsc_keyword_data: pd.DataFrame
) -> Dict[str, Any]:
    """
    Calculate market-level metrics like concentration and user share.
    """
    # Calculate Herfindahl-Hirschman Index for market concentration
    competitor_shares = []
    total_visibility = sum(data['visibility_score'] for data in competitor_mapping.values())
    
    if total_visibility > 0:
        for data in competitor_mapping.values():
            share = data['visibility_score'] / total_visibility
            competitor_shares.append(share ** 2)
        
        market_concentration = sum(competitor_shares)
    else:
        market_concentration = 0.0
    
    # Calculate user's market share
    user_clicks = 0
    total_available_clicks = 0
    
    for serp_result in serp_data:
        keyword = serp_result.get('keyword', '')
        search_volume = serp_result.get('search_volume', 0)
        organic_results = serp_result.get('organic_results', [])
        
        # Find user position
        user_position = None
        for result in organic_results:
            domain = _extract_domain_from_url(result.get('url', ''))
            if domain == user_domain:
                user_position = result.get('position', 999)
                break
        
        if user_position:
            user_clicks += _estimate_clicks_at_position(search_volume, user_position)
        
        # Total available (position 1-10)
        total_available_clicks += sum(
            _estimate_clicks_at_position(search_volume, i) for i in range(1, 11)
        )
    
    user_market_share = user_clicks / total_available_clicks if total_available_clicks > 0 else 0
    total_opportunity = int(total_available_clicks - user_clicks)
    
    return {
        'concentration': market_concentration,
        'user_share': user_market_share,
        'total_opportunity': total_opportunity,
        'user_estimated_clicks': int(user_clicks),
        'market_total_clicks': int(total_available_clicks)
    }


def _analyze_strengths_weaknesses(
    positioning_analysis: Dict[str, Any],
    competitor_mapping: Dict[str, Dict[str, Any]],
    user_domain: str
) -> Dict[str, List[str]]:
    """
    Determine competitive advantages and weaknesses.
    """
    advantages = []
    weaknesses = []
    
    # Analyze positioning
    dominated = positioning_analysis['dominated_count']
    competitive = positioning_analysis['competitive_count']
    losing = positioning_analysis['losing_count']
    absent = positioning_analysis['absent_count']
    total = dominated + competitive + losing + absent
    
    if total > 0:
        # Advantages
        if dominated / total > 0.3:
            advantages.append(f"Strong top-3 presence ({dominated} keywords dominated)")
        
        if positioning_analysis['avg_user_position'] and positioning_analysis['avg_user_position'] < 5:
            advantages.append(f"Above-average rankings (avg position {positioning_analysis['avg_user_position']:.1f})")
        
        # Weaknesses
        if absent / total > 0.3:
            weaknesses.append(f"Missing from SERPs for {absent} target keywords")
        
        if losing / total > 0.4:
            weaknesses.append(f"Page 2+ rankings for {losing} keywords indicate weak competitiveness")
        
        if positioning_analysis['avg_user_position'] and positioning_analysis['avg_user_position'] > 10:
            weaknesses.append(f"Poor average ranking ({positioning_analysis['avg_user_position']:.1f}) suggests content quality issues")
    
    # Analyze vs top competitor
    if competitor_mapping:
        top_competitor = max(competitor_mapping.items(), key=lambda x: x[1]['visibility_score'])
        top_domain, top_data = top_competitor
        
        if top_data['avg_position'] < (positioning_analysis.get('avg_user_position') or 999):
            weaknesses.append(f"{top_domain} outranks you on average ({top_data['avg_position']:.1f} vs {positioning_analysis.get('avg_user_position', 'N/A')})")
    
    # Default messages if nothing specific found
    if not advantages:
        advantages.append("Analysis requires more data to identify competitive advantages")
    
    if not weaknesses:
        weaknesses.append("No major competitive weaknesses identified")
    
    return {
        'advantages': advantages,
        'weaknesses': weaknesses
    }


def _build_competitor_profiles(
    top_competitors: List[Tuple[str, Dict[str, Any]]],
    serp_data: List[Dict[str, Any]],
    gsc_keyword_data: pd.DataFrame,
    user_domain: str
) -> List[CompetitorProfile]:
    """
    Build detailed profiles for top competitors.
    """
    profiles = []
    
    for domain, data in top_competitors:
        # Get example keywords and URLs
        keyword_examples = []
        seen_keywords = set()
        
        for keyword, position, url, search_vol in zip(
            data['keywords'][:10],
            data['positions'][:10],
            data['urls'][:10],
            data['search_volumes'][:10]
        ):
            if keyword not in seen_keywords:
                keyword_examples.append({
                    'keyword': keyword,
                    'position': position,
                    'search_volume': search_vol,
                    'url': url
                })
                seen_keywords.add(keyword)
        
        # Determine threat level
        visibility_score = data['visibility_score']
        avg_position = data['avg_position']
        keywords_count = data['keywords_count']
        
        if visibility_score > 1000 and avg_position < 4 and keywords_count > 20:
            threat_level = 'high'
        elif visibility_score > 500 and avg_position < 6 and keywords_count > 10:
            threat_level = 'medium'
        else:
            threat_level = 'low'
        
        # Position trend (would need historical data, set to None for now)
        position_trend = None
        
        # Unique URLs
        url_examples = list(set(data['urls'][:5]))
        
        profiles.append(CompetitorProfile(
            domain=domain,
            keywords_shared=keywords_count,
            avg_position=round(avg_position, 1),
            visibility_score=round(visibility_score, 2),
            position_trend=position_trend,
            threat_level=threat_level,
            url_examples=url_examples,
            shared_keyword_examples=keyword_examples
        ))
    
    return profiles


def format_competitor_report(analysis_result: Dict[str, Any]) -> str:
    """
    Format the competitor analysis into a readable report.
    """
    report_lines = []
    
    report_lines.append("=" * 80)
    report_lines.append("COMPETITOR & MARKET INTELLIGENCE REPORT")
    report_lines.append("=" * 80)
    report_lines.append("")
    
    # Market overview
    report_lines.append("MARKET OVERVIEW")
    report_lines.append("-" * 80)
    report_lines.append(f"Your Market Share: {analysis_result['user_market_share']:.1%}")
    report_lines.append(f"Market Concentration (HHI): {analysis_result['market_concentration']:.3f}")
    report_lines.append(f"Total Addressable Opportunity: {analysis_result['total_addressable_opportunity']:,} monthly clicks")
    report_lines.append("")
    
    # Positioning summary
    pos_summary = analysis_result['positioning_summary']
    report_lines.append("YOUR COMPETITIVE POSITIONING")
    report_lines.append("-" * 80)
    if pos_summary['avg_user_position']:
        report_lines.append(f"Average Position: {pos_summary['avg_user_position']:.1f}")
    report_lines.append(f"Keywords Dominated (Top 3): {pos_summary['keywords_dominated']}")
    report_lines.append(f"Keywords Competitive (4-7): {pos_summary['keywords_competitive']}")
    report_lines.append(f"Keywords Losing (8-20): {pos_summary['keywords_losing']}")
    report_lines.append(f"Keywords Absent: {pos_summary['keywords_absent']}")
    report_lines.append("")
    
    # Top competitors
    report_lines.append("PRIMARY COMPETITORS")
    report_lines.append("-" * 80)
    for comp in analysis_result['competitors'][:5]:
        report_lines.append(f"\n{comp['domain'].upper()} ({comp['threat_level'].upper()} THREAT)")
        report_lines.append(f"  Shared Keywords: {comp['keywords_shared']}")
        report_lines.append(f"  Average Position: {comp['avg_position']}")
        report_lines.append(f"  Visibility Score: {comp['visibility_score']:.1f}")
        if comp['shared_keyword_examples']:
            report_lines.append(f"  Example Rankings:")
            for ex in comp['shared_keyword_examples'][:3]:
                report_lines.append(f"    - '{ex['keyword']}' at position {ex['position']}")
    report_lines.append("")
    
    # Content gaps
    report_lines.append("TOP CONTENT GAP OPPORTUNITIES")
    report_lines.append("-" * 80)
    for gap in analysis_result['content_gaps'][:10]:
        report_lines.append(f"\n'{gap['keyword']}' ({gap['intent_type']})")
        report_lines.append(f"  Gap Type: {gap['gap_type']}")
        report_lines.append(f"  Search Volume: {gap['search_volume']:,}")
        report_lines.append(f"  Opportunity Score: {gap['opportunity_score']:.0f} monthly clicks")
        report_lines.append(f"  Difficulty: {gap['difficulty_score']:.0f}/100")
        report_lines.append(f"  Action: {gap['recommended_action']}")
    report_lines.append("")
    
    # Strengths and weaknesses
    report_lines.append("COMPETITIVE ANALYSIS")
    report_lines.append("-" * 80)
    report_lines.append("Your Advantages:")
    for adv in analysis_result['competitive_advantages']:
        report_lines.append(f"  + {adv}")
    report_lines.append("\nYour Weaknesses:")
    for weak in analysis_result['competitive_weaknesses']:
        report_lines.append(f"  - {weak}")
    report_lines.append("")
    
    report_lines.append("=" * 80)
    
    return "\n".join(report_lines)
