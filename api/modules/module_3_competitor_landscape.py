"""
Module 3: Competitor Landscape Analysis

Uses DataForSEO to identify top competing domains based on keyword overlap from GSC data.
Analyzes competitor visibility, shared keywords, and competitive gaps.
"""

import logging
from typing import Dict, Any, List, Optional, Tuple
from collections import defaultdict, Counter
from datetime import datetime
import statistics
import re
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


def analyze_competitor_landscape(
    gsc_query_data: List[Dict[str, Any]],
    serp_data: List[Dict[str, Any]],
    user_domain: str,
    min_impressions: int = 100
) -> Dict[str, Any]:
    """
    Main entry point for Module 3 - Competitor Landscape Analysis.
    
    Args:
        gsc_query_data: List of GSC query performance data (with keys: query, impressions, clicks, position)
        serp_data: List of DataForSEO SERP results for top queries
        user_domain: The user's domain (to identify their rankings)
        min_impressions: Minimum impressions to consider a query
        
    Returns:
        {
            "top_competitors": [
                {
                    "domain": "competitor.com",
                    "overlap_score": 0.78,
                    "shared_keywords_count": 45,
                    "avg_position": 3.2,
                    "visibility_share": 0.15,
                    "position_distribution": {"1-3": 12, "4-10": 25, "11-20": 8},
                    "better_than_you": 23,
                    "worse_than_you": 22
                }
            ],
            "competitive_positioning": {
                "your_visibility_share": 0.22,
                "market_leader": "competitor1.com",
                "market_leader_share": 0.28,
                "your_rank_among_competitors": 3,
                "total_competitors_analyzed": 47,
                "competitive_intensity": "high",
                "position_advantage_keywords": 12,
                "position_disadvantage_keywords": 33
            },
            "opportunity_keywords": [
                {
                    "query": "best crm software",
                    "impressions": 8900,
                    "your_position": null,
                    "competitor_positions": {"competitor.com": 2, "other.com": 5},
                    "estimated_monthly_clicks": 420,
                    "serp_features": ["featured_snippet", "paa"],
                    "difficulty_score": 0.72
                }
            ],
            "summary": {
                "total_queries_analyzed": 87,
                "total_competitors_found": 47,
                "avg_competitors_per_query": 8.3,
                "keywords_you_rank_for": 64,
                "keywords_only_competitors_rank": 23,
                "total_opportunity_clicks": 3400
            }
        }
    """
    
    try:
        logger.info(f"Starting competitor landscape analysis for {user_domain}")
        
        # Normalize user domain
        user_domain_normalized = normalize_domain(user_domain)
        
        # Filter queries by minimum impressions
        filtered_queries = [
            q for q in gsc_query_data 
            if q.get('impressions', 0) >= min_impressions
        ]
        
        logger.info(f"Analyzing {len(filtered_queries)} queries (min {min_impressions} impressions)")
        
        if not filtered_queries:
            return _empty_result("No queries meet minimum impression threshold")
        
        if not serp_data:
            return _empty_result("No SERP data available")
        
        # Build query lookup for impressions
        query_impressions = {
            q['query']: q.get('impressions', 0) 
            for q in gsc_query_data
        }
        
        # Extract competitors from SERP data
        competitors_data = extract_competitors_from_serp(
            serp_data, 
            user_domain_normalized,
            query_impressions
        )
        
        # Calculate overlap scores and metrics
        top_competitors = calculate_competitor_metrics(
            competitors_data,
            serp_data,
            user_domain_normalized,
            query_impressions
        )
        
        # Calculate competitive positioning
        competitive_positioning = calculate_competitive_positioning(
            top_competitors,
            serp_data,
            user_domain_normalized,
            len(filtered_queries)
        )
        
        # Identify opportunity keywords
        opportunity_keywords = identify_opportunity_keywords(
            serp_data,
            gsc_query_data,
            user_domain_normalized,
            top_competitors[:10]  # Top 10 competitors
        )
        
        # Generate summary
        summary = {
            "total_queries_analyzed": len(filtered_queries),
            "total_competitors_found": len(competitors_data),
            "avg_competitors_per_query": _safe_avg([
                len([r for r in serp.get('organic_results', []) if normalize_domain(r.get('domain', '')) != user_domain_normalized])
                for serp in serp_data
            ]),
            "keywords_you_rank_for": sum(1 for serp in serp_data if _user_ranks_in_serp(serp, user_domain_normalized)),
            "keywords_only_competitors_rank": len(opportunity_keywords),
            "total_opportunity_clicks": sum(kw.get('estimated_monthly_clicks', 0) for kw in opportunity_keywords)
        }
        
        result = {
            "top_competitors": top_competitors[:20],  # Top 20 competitors
            "competitive_positioning": competitive_positioning,
            "opportunity_keywords": opportunity_keywords[:50],  # Top 50 opportunities
            "summary": summary
        }
        
        logger.info(f"Competitor analysis complete: {len(top_competitors)} competitors, {len(opportunity_keywords)} opportunities")
        
        return result
        
    except Exception as e:
        logger.error(f"Error in competitor landscape analysis: {str(e)}", exc_info=True)
        return _empty_result(f"Analysis error: {str(e)}")


def normalize_domain(url_or_domain: str) -> str:
    """
    Normalize a URL or domain to just the domain name.
    
    Examples:
        "https://www.example.com/path" -> "example.com"
        "example.com" -> "example.com"
        "www.example.com" -> "example.com"
    """
    if not url_or_domain:
        return ""
    
    # If it looks like a URL, parse it
    if '://' in url_or_domain or url_or_domain.startswith('//'):
        parsed = urlparse(url_or_domain if '://' in url_or_domain else f"https:{url_or_domain}")
        domain = parsed.netloc or parsed.path
    else:
        domain = url_or_domain
    
    # Remove www prefix
    if domain.startswith('www.'):
        domain = domain[4:]
    
    # Remove trailing slash
    domain = domain.rstrip('/')
    
    # Extract base domain (remove subdomains except www was already removed)
    # Keep it simple - just use the domain as-is after www removal
    
    return domain.lower()


def extract_competitors_from_serp(
    serp_data: List[Dict[str, Any]],
    user_domain: str,
    query_impressions: Dict[str, int]
) -> Dict[str, Dict[str, Any]]:
    """
    Extract all competing domains from SERP data.
    
    Returns:
        Dict mapping domain -> {queries: set, positions: list, impression_weighted_position: float}
    """
    competitors = defaultdict(lambda: {
        'queries': set(),
        'positions': [],
        'impressions_list': [],
        'total_weighted_impressions': 0
    })
    
    for serp in serp_data:
        query = serp.get('keyword', '')
        impressions = query_impressions.get(query, 0)
        
        organic_results = serp.get('organic_results', [])
        
        for result in organic_results[:20]:  # Top 20 results
            domain = normalize_domain(result.get('domain', ''))
            position = result.get('rank_absolute', result.get('position', 999))
            
            if not domain or domain == user_domain:
                continue
            
            competitors[domain]['queries'].add(query)
            competitors[domain]['positions'].append(position)
            competitors[domain]['impressions_list'].append(impressions)
            competitors[domain]['total_weighted_impressions'] += impressions
    
    return dict(competitors)


def calculate_competitor_metrics(
    competitors_data: Dict[str, Dict[str, Any]],
    serp_data: List[Dict[str, Any]],
    user_domain: str,
    query_impressions: Dict[str, int]
) -> List[Dict[str, Any]]:
    """
    Calculate detailed metrics for each competitor.
    """
    total_queries = len(serp_data)
    total_impressions = sum(query_impressions.values())
    
    competitor_list = []
    
    for domain, data in competitors_data.items():
        shared_keywords_count = len(data['queries'])
        
        # Overlap score: what % of queries does this competitor appear in?
        overlap_score = shared_keywords_count / total_queries if total_queries > 0 else 0
        
        # Average position
        positions = data['positions']
        avg_position = statistics.mean(positions) if positions else 0
        
        # Position distribution
        position_distribution = {
            "1-3": sum(1 for p in positions if 1 <= p <= 3),
            "4-10": sum(1 for p in positions if 4 <= p <= 10),
            "11-20": sum(1 for p in positions if 11 <= p <= 20),
            "21+": sum(1 for p in positions if p > 20)
        }
        
        # Visibility share (impression-weighted)
        visibility_share = data['total_weighted_impressions'] / total_impressions if total_impressions > 0 else 0
        
        # Calculate head-to-head comparison with user
        better_count, worse_count = calculate_head_to_head(
            serp_data,
            domain,
            user_domain
        )
        
        competitor_list.append({
            "domain": domain,
            "overlap_score": round(overlap_score, 4),
            "shared_keywords_count": shared_keywords_count,
            "avg_position": round(avg_position, 2),
            "visibility_share": round(visibility_share, 4),
            "position_distribution": position_distribution,
            "better_than_you": better_count,
            "worse_than_you": worse_count,
            "threat_level": _calculate_threat_level(overlap_score, avg_position, visibility_share)
        })
    
    # Sort by overlap score (primary) and visibility share (secondary)
    competitor_list.sort(key=lambda x: (x['overlap_score'], x['visibility_share']), reverse=True)
    
    return competitor_list


def calculate_head_to_head(
    serp_data: List[Dict[str, Any]],
    competitor_domain: str,
    user_domain: str
) -> Tuple[int, int]:
    """
    Calculate how many times the competitor ranks better vs worse than the user
    in queries where both rank.
    
    Returns:
        (better_than_you_count, worse_than_you_count)
    """
    better_count = 0
    worse_count = 0
    
    for serp in serp_data:
        organic_results = serp.get('organic_results', [])
        
        user_position = None
        competitor_position = None
        
        for result in organic_results:
            domain = normalize_domain(result.get('domain', ''))
            position = result.get('rank_absolute', result.get('position', 999))
            
            if domain == user_domain:
                user_position = position
            elif domain == competitor_domain:
                competitor_position = position
        
        # Only count if both rank
        if user_position is not None and competitor_position is not None:
            if competitor_position < user_position:
                better_count += 1
            elif competitor_position > user_position:
                worse_count += 1
    
    return better_count, worse_count


def _calculate_threat_level(overlap_score: float, avg_position: float, visibility_share: float) -> str:
    """
    Calculate threat level based on competitor metrics.
    """
    # High threat: high overlap, good positions, good visibility
    if overlap_score > 0.5 and avg_position < 5 and visibility_share > 0.1:
        return "critical"
    elif overlap_score > 0.3 and avg_position < 7:
        return "high"
    elif overlap_score > 0.2 or avg_position < 10:
        return "medium"
    else:
        return "low"


def calculate_competitive_positioning(
    top_competitors: List[Dict[str, Any]],
    serp_data: List[Dict[str, Any]],
    user_domain: str,
    total_queries: int
) -> Dict[str, Any]:
    """
    Calculate the user's competitive positioning in the market.
    """
    # Calculate user's visibility share
    user_visibility = 0
    user_positions = []
    queries_user_ranks = 0
    
    for serp in serp_data:
        organic_results = serp.get('organic_results', [])
        
        for result in organic_results:
            domain = normalize_domain(result.get('domain', ''))
            if domain == user_domain:
                position = result.get('rank_absolute', result.get('position', 999))
                user_positions.append(position)
                queries_user_ranks += 1
                break
    
    # Calculate impression-weighted visibility (simplified - using position-based CTR)
    user_visibility = _calculate_visibility_from_positions(user_positions)
    
    # Market leader
    market_leader = top_competitors[0] if top_competitors else {"domain": "N/A", "visibility_share": 0}
    
    # User's rank among competitors
    user_rank = 1
    for comp in top_competitors:
        if comp['visibility_share'] > user_visibility:
            user_rank += 1
    
    # Calculate position advantages/disadvantages
    position_advantage = 0
    position_disadvantage = 0
    
    for serp in serp_data:
        user_pos, competitors_positions = _get_positions_in_serp(serp, user_domain, top_competitors[:10])
        
        if user_pos:
            better = sum(1 for cp in competitors_positions if cp > user_pos)
            worse = sum(1 for cp in competitors_positions if cp < user_pos)
            
            if better > worse:
                position_advantage += 1
            elif worse > better:
                position_disadvantage += 1
    
    # Competitive intensity
    avg_competitors_per_query = len(top_competitors) / total_queries if total_queries > 0 else 0
    if avg_competitors_per_query > 8:
        intensity = "very_high"
    elif avg_competitors_per_query > 6:
        intensity = "high"
    elif avg_competitors_per_query > 4:
        intensity = "medium"
    else:
        intensity = "low"
    
    return {
        "your_visibility_share": round(user_visibility, 4),
        "market_leader": market_leader['domain'],
        "market_leader_share": round(market_leader['visibility_share'], 4),
        "your_rank_among_competitors": user_rank,
        "total_competitors_analyzed": len(top_competitors),
        "competitive_intensity": intensity,
        "position_advantage_keywords": position_advantage,
        "position_disadvantage_keywords": position_disadvantage,
        "queries_you_rank_for": queries_user_ranks,
        "your_avg_position": round(statistics.mean(user_positions), 2) if user_positions else None
    }


def identify_opportunity_keywords(
    serp_data: List[Dict[str, Any]],
    gsc_query_data: List[Dict[str, Any]],
    user_domain: str,
    top_competitors: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Identify keywords where competitors rank but the user doesn't.
    """
    opportunities = []
    
    # Build set of queries user ranks for
    user_queries = set()
    for serp in serp_data:
        if _user_ranks_in_serp(serp, user_domain):
            user_queries.add(serp.get('keyword', ''))
    
    # Build GSC query lookup
    gsc_lookup = {q['query']: q for q in gsc_query_data}
    
    # Top competitor domains
    top_competitor_domains = {c['domain'] for c in top_competitors}
    
    for serp in serp_data:
        query = serp.get('keyword', '')
        
        # Skip if user already ranks
        if query in user_queries:
            continue
        
        # Get GSC data
        gsc_data = gsc_lookup.get(query, {})
        impressions = gsc_data.get('impressions', 0)
        
        # If no impressions, estimate based on search volume or use a default
        if impressions == 0:
            # Use a conservative estimate based on SERP data if available
            impressions = serp.get('search_volume', 1000)  # Default estimate
        
        # Find competitor positions
        competitor_positions = {}
        organic_results = serp.get('organic_results', [])
        
        for result in organic_results[:10]:  # Top 10 only
            domain = normalize_domain(result.get('domain', ''))
            if domain in top_competitor_domains:
                position = result.get('rank_absolute', result.get('position', 999))
                competitor_positions[domain] = position
        
        # Skip if no relevant competitors rank
        if not competitor_positions:
            continue
        
        # Get SERP features
        serp_features = _extract_serp_features(serp)
        
        # Estimate monthly clicks if user ranked #5
        estimated_clicks = _estimate_clicks_at_position(impressions, 5, serp_features)
        
        # Calculate difficulty score based on competitor positions and strength
        difficulty_score = _calculate_difficulty_score(
            competitor_positions,
            top_competitors,
            serp_features
        )
        
        opportunities.append({
            "query": query,
            "impressions": impressions,
            "your_position": None,
            "competitor_positions": competitor_positions,
            "best_competitor_position": min(competitor_positions.values()) if competitor_positions else None,
            "estimated_monthly_clicks": estimated_clicks,
            "serp_features": serp_features,
            "difficulty_score": round(difficulty_score, 2),
            "opportunity_score": round(estimated_clicks * (1 - difficulty_score), 1)
        })
    
    # Sort by opportunity score (clicks potential adjusted by difficulty)
    opportunities.sort(key=lambda x: x['opportunity_score'], reverse=True)
    
    return opportunities


def _user_ranks_in_serp(serp: Dict[str, Any], user_domain: str) -> bool:
    """Check if user ranks in this SERP."""
    organic_results = serp.get('organic_results', [])
    return any(
        normalize_domain(result.get('domain', '')) == user_domain
        for result in organic_results
    )


def _get_positions_in_serp(
    serp: Dict[str, Any],
    user_domain: str,
    top_competitors: List[Dict[str, Any]]
) -> Tuple[Optional[int], List[int]]:
    """
    Get user position and competitor positions in a SERP.
    
    Returns:
        (user_position, list_of_competitor_positions)
    """
    organic_results = serp.get('organic_results', [])
    competitor_domains = {c['domain'] for c in top_competitors}
    
    user_pos = None
    competitor_positions = []
    
    for result in organic_results:
        domain = normalize_domain(result.get('domain', ''))
        position = result.get('rank_absolute', result.get('position', 999))
        
        if domain == user_domain:
            user_pos = position
        elif domain in competitor_domains:
            competitor_positions.append(position)
    
    return user_pos, competitor_positions


def _extract_serp_features(serp: Dict[str, Any]) -> List[str]:
    """Extract present SERP features from DataForSEO result."""
    features = []
    
    # Check for various SERP features based on DataForSEO structure
    if serp.get('featured_snippet'):
        features.append('featured_snippet')
    
    if serp.get('knowledge_graph'):
        features.append('knowledge_graph')
    
    if serp.get('local_pack'):
        features.append('local_pack')
    
    # People also ask
    paa = serp.get('people_also_ask', [])
    if paa:
        features.append('paa')
    
    # Video results
    if serp.get('video', []) or serp.get('video_carousel'):
        features.append('video')
    
    # Image pack
    if serp.get('images', []):
        features.append('images')
    
    # Shopping results
    if serp.get('shopping', []):
        features.append('shopping')
    
    # News results
    if serp.get('top_stories', []):
        features.append('top_stories')
    
    # AI Overview / AI-generated
    if serp.get('ai_overview') or serp.get('ai_generated'):
        features.append('ai_overview')
    
    return features


def _estimate_clicks_at_position(
    impressions: int,
    position: int,
    serp_features: List[str]
) -> int:
    """
    Estimate monthly clicks if ranking at given position.
    Uses position-based CTR curves adjusted for SERP features.
    """
    # Base CTR by position (Advanced Web Ranking 2024 data)
    base_ctr = {
        1: 0.275, 2: 0.125, 3: 0.085, 4: 0.060, 5: 0.045,
        6: 0.035, 7: 0.028, 8: 0.023, 9: 0.019, 10: 0.016,
        11: 0.012, 12: 0.010, 13: 0.008, 14: 0.007, 15: 0.006
    }
    
    ctr = base_ctr.get(position, 0.005)
    
    # Adjust for SERP features (they steal clicks)
    if 'featured_snippet' in serp_features:
        ctr *= 0.7
    if 'ai_overview' in serp_features:
        ctr *= 0.6
    if 'paa' in serp_features:
        ctr *= 0.9
    if 'local_pack' in serp_features:
        ctr *= 0.8
    if 'shopping' in serp_features:
        ctr *= 0.85
    
    return int(impressions * ctr)


def _calculate_difficulty_score(
    competitor_positions: Dict[str, int],
    top_competitors: List[Dict[str, Any]],
    serp_features: List[str]
) -> float:
    """
    Calculate difficulty score (0-1) for ranking on this keyword.
    Higher = harder to rank.
    """
    if not competitor_positions:
        return 0.3  # Base difficulty if no competitors
    
    # Factor 1: Best competitor position
    best_position = min(competitor_positions.values())
    position_factor = min(best_position / 10.0, 1.0)  # Normalize to 0-1
    
    # Factor 2: Number of strong competitors (from top 10)
    competitor_domains = set(competitor_positions.keys())
    top_10_domains = {c['domain'] for c in top_competitors[:10]}
    strong_competitors_count = len(competitor_domains & top_10_domains)
    competitor_factor = min(strong_competitors_count / 5.0, 1.0)
    
    # Factor 3: SERP complexity (more features = harder)
    serp_complexity = len(serp_features) / 5.0  # Normalize
    serp_complexity = min(serp_complexity, 1.0)
    
    # Weighted combination
    difficulty = (
        position_factor * 0.4 +
        competitor_factor * 0.4 +
        serp_complexity * 0.2
    )
    
    return min(difficulty, 1.0)


def _calculate_visibility_from_positions(positions: List[int]) -> float:
    """
    Calculate visibility score from position list using CTR-based weighting.
    """
    if not positions:
        return 0.0
    
    # CTR weights by position
    ctr_weights = {
        1: 0.275, 2: 0.125, 3: 0.085, 4: 0.060, 5: 0.045,
        6: 0.035, 7: 0.028, 8: 0.023, 9: 0.019, 10: 0.016
    }
    
    total_visibility = sum(ctr_weights.get(min(p, 10), 0.01) for p in positions)
    max_possible = len(positions) * 0.275  # If all were #1
    
    return total_visibility / max_possible if max_possible > 0 else 0.0


def _safe_avg(values: List[float]) -> float:
    """Calculate average safely handling empty lists."""
    return statistics.mean(values) if values else 0.0


def _empty_result(reason: str = "") -> Dict[str, Any]:
    """Return empty result structure when analysis can't proceed."""
    return {
        "top_competitors": [],
        "competitive_positioning": {
            "your_visibility_share": 0,
            "market_leader": "N/A",
            "market_leader_share": 0,
            "your_rank_among_competitors": 0,
            "total_competitors_analyzed": 0,
            "competitive_intensity": "unknown",
            "position_advantage_keywords": 0,
            "position_disadvantage_keywords": 0
        },
        "opportunity_keywords": [],
        "summary": {
            "total_queries_analyzed": 0,
            "total_competitors_found": 0,
            "avg_competitors_per_query": 0,
            "keywords_you_rank_for": 0,
            "keywords_only_competitors_rank": 0,
            "total_opportunity_clicks": 0
        },
        "error": reason if reason else "No data available for analysis"
    }

