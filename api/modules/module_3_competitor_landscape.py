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


def normalize_domain(url_or_domain: str) -> str:
    """Extract and normalize domain from URL or domain string."""
    if not url_or_domain:
        return ""
    
    # Remove protocol if present
    url_or_domain = re.sub(r'^https?://', '', url_or_domain)
    
    # Parse to extract domain
    if '/' in url_or_domain:
        parsed = urlparse(f"http://{url_or_domain}")
        domain = parsed.netloc or parsed.path.split('/')[0]
    else:
        domain = url_or_domain
    
    # Remove www. prefix
    domain = re.sub(r'^www\.', '', domain)
    
    # Remove trailing slash and path
    domain = domain.split('/')[0].lower()
    
    return domain


def calculate_position_ctr(position: float, has_features: bool = False) -> float:
    """
    Estimate CTR based on position and SERP features.
    Uses industry-standard CTR curves.
    """
    if position <= 0:
        return 0.0
    
    # Base CTR curve (no features)
    ctr_curve = {
        1: 0.316, 2: 0.158, 3: 0.100, 4: 0.077, 5: 0.061,
        6: 0.048, 7: 0.039, 8: 0.032, 9: 0.027, 10: 0.023
    }
    
    # Interpolate for positions between integers
    pos_floor = int(position)
    if pos_floor in ctr_curve:
        if position == pos_floor:
            base_ctr = ctr_curve[pos_floor]
        else:
            # Linear interpolation
            if pos_floor + 1 in ctr_curve:
                ctr1 = ctr_curve[pos_floor]
                ctr2 = ctr_curve[pos_floor + 1]
                fraction = position - pos_floor
                base_ctr = ctr1 - (ctr1 - ctr2) * fraction
            else:
                base_ctr = ctr_curve[pos_floor]
    elif position > 10:
        # Exponential decay for positions beyond 10
        base_ctr = 0.023 * (0.85 ** (position - 10))
    else:
        base_ctr = 0.01
    
    # Adjust for SERP features (reduce CTR if features present)
    if has_features:
        base_ctr *= 0.7
    
    return base_ctr


def extract_serp_competitors(serp_results: List[Dict[str, Any]], user_domain: str) -> Dict[str, Any]:
    """
    Extract competitor information from SERP results.
    
    Returns:
        {
            "query": str,
            "user_position": float or None,
            "competitors": [
                {
                    "domain": str,
                    "position": int,
                    "url": str,
                    "title": str
                }
            ],
            "serp_features": list,
            "total_organic_results": int
        }
    """
    competitors = []
    user_position = None
    serp_features = []
    
    # Extract organic results
    organic_results = serp_results.get('organic', []) if isinstance(serp_results, dict) else []
    
    for result in organic_results:
        position = result.get('rank_absolute') or result.get('position', 0)
        url = result.get('url', '')
        domain = normalize_domain(url)
        
        if not domain:
            continue
        
        # Check if this is the user's domain
        if domain == user_domain:
            user_position = position
        else:
            competitors.append({
                'domain': domain,
                'position': position,
                'url': url,
                'title': result.get('title', '')
            })
    
    # Extract SERP features
    if isinstance(serp_results, dict):
        if serp_results.get('featured_snippet'):
            serp_features.append('featured_snippet')
        if serp_results.get('knowledge_graph'):
            serp_features.append('knowledge_panel')
        if serp_results.get('people_also_ask'):
            paa_count = len(serp_results['people_also_ask']) if isinstance(serp_results['people_also_ask'], list) else 0
            if paa_count > 0:
                serp_features.append(f'paa_x{paa_count}')
        if serp_results.get('video'):
            serp_features.append('video_carousel')
        if serp_results.get('local_pack'):
            serp_features.append('local_pack')
        if serp_results.get('top_stories'):
            serp_features.append('top_stories')
        if serp_results.get('images'):
            serp_features.append('image_pack')
        if serp_results.get('shopping'):
            serp_features.append('shopping_results')
    
    return {
        'query': serp_results.get('keyword', '') if isinstance(serp_results, dict) else '',
        'user_position': user_position,
        'competitors': competitors,
        'serp_features': serp_features,
        'total_organic_results': len(organic_results)
    }


def calculate_overlap_score(shared_keywords: int, total_competitor_keywords: int, total_user_keywords: int) -> float:
    """
    Calculate keyword overlap score using Jaccard similarity.
    
    Score = shared / (total_user + total_competitor - shared)
    """
    if total_user_keywords == 0 or total_competitor_keywords == 0:
        return 0.0
    
    union = total_user_keywords + total_competitor_keywords - shared_keywords
    if union == 0:
        return 0.0
    
    return shared_keywords / union


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
            "top_competitors": [...],
            "competitive_positioning": {...},
            "opportunity_keywords": [...],
            "summary": {...}
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
        
        logger.info(f"Analyzing {len(filtered_queries)} queries with >= {min_impressions} impressions")
        
        # Build competitor frequency map and detailed data
        competitor_data = defaultdict(lambda: {
            'appearances': 0,
            'positions': [],
            'shared_queries': [],
            'better_positions': 0,
            'worse_positions': 0,
            'urls': set()
        })
        
        user_keywords_set = set()
        all_queries_analyzed = []
        opportunity_keywords = []
        
        # Create query lookup for GSC data
        gsc_query_lookup = {q.get('query', '').lower(): q for q in filtered_queries}
        
        # Process SERP data
        for serp_result in serp_data:
            if not isinstance(serp_result, dict):
                continue
            
            extracted = extract_serp_competitors(serp_result, user_domain_normalized)
            query = extracted['query'].lower()
            
            if not query:
                continue
            
            all_queries_analyzed.append(query)
            
            # Get GSC data for this query
            gsc_data = gsc_query_lookup.get(query, {})
            impressions = gsc_data.get('impressions', 0)
            user_gsc_position = gsc_data.get('position')
            
            # Use SERP position if available, otherwise fall back to GSC
            user_position = extracted['user_position'] or user_gsc_position
            
            if user_position:
                user_keywords_set.add(query)
            
            # Track competitors for this query
            for comp in extracted['competitors']:
                domain = comp['domain']
                position = comp['position']
                
                competitor_data[domain]['appearances'] += 1
                competitor_data[domain]['positions'].append(position)
                competitor_data[domain]['shared_queries'].append(query)
                competitor_data[domain]['urls'].add(comp['url'])
                
                # Compare positions if user ranks for this query
                if user_position:
                    if position < user_position:
                        competitor_data[domain]['better_positions'] += 1
                    else:
                        competitor_data[domain]['worse_positions'] += 1
            
            # Identify opportunity keywords (user doesn't rank but competitors do)
            if not user_position and extracted['competitors']:
                competitor_positions = {
                    comp['domain']: comp['position'] 
                    for comp in extracted['competitors'][:5]  # Top 5 competitors
                }
                
                # Estimate potential clicks
                if competitor_positions:
                    best_comp_position = min(competitor_positions.values())
                    estimated_ctr = calculate_position_ctr(
                        best_comp_position, 
                        has_features=len(extracted['serp_features']) > 0
                    )
                    estimated_clicks = impressions * estimated_ctr if impressions > 0 else 0
                    
                    # Calculate difficulty score based on competition
                    avg_comp_position = statistics.mean(competitor_positions.values()) if competitor_positions else 10
                    difficulty_score = min(1.0, (len(competitor_positions) / 10) * (1 / max(1, avg_comp_position)))
                    
                    opportunity_keywords.append({
                        'query': query,
                        'impressions': impressions,
                        'your_position': None,
                        'competitor_positions': competitor_positions,
                        'estimated_monthly_clicks': int(estimated_clicks * 30),  # Convert daily to monthly
                        'serp_features': extracted['serp_features'],
                        'difficulty_score': round(difficulty_score, 2)
                    })
        
        # Calculate competitor metrics
        total_user_keywords = len(user_keywords_set)
        competitor_list = []
        
        for domain, data in competitor_data.items():
            if data['appearances'] < 3:  # Filter out one-off competitors
                continue
            
            shared_keywords_count = len(data['shared_queries'])
            avg_position = statistics.mean(data['positions']) if data['positions'] else 0
            
            # Position distribution
            position_distribution = {
                '1-3': len([p for p in data['positions'] if 1 <= p <= 3]),
                '4-10': len([p for p in data['positions'] if 4 <= p <= 10]),
                '11-20': len([p for p in data['positions'] if 11 <= p <= 20]),
                '21+': len([p for p in data['positions'] if p > 20])
            }
            
            # Calculate overlap score
            overlap_score = calculate_overlap_score(
                shared_keywords_count,
                data['appearances'],
                total_user_keywords
            )
            
            # Calculate visibility share (simplified)
            # Based on position-weighted appearances
            visibility_score = sum(1 / max(1, p) for p in data['positions'])
            
            competitor_list.append({
                'domain': domain,
                'overlap_score': round(overlap_score, 3),
                'shared_keywords_count': shared_keywords_count,
                'avg_position': round(avg_position, 1),
                'visibility_score': round(visibility_score, 2),
                'position_distribution': position_distribution,
                'better_than_you': data['better_positions'],
                'worse_than_you': data['worse_positions'],
                'total_appearances': data['appearances']
            })
        
        # Sort competitors by overlap score and visibility
        competitor_list.sort(
            key=lambda x: (x['overlap_score'], x['visibility_score']), 
            reverse=True
        )
        
        # Calculate visibility shares (normalize)
        total_visibility = sum(c['visibility_score'] for c in competitor_list)
        user_visibility_score = sum(
            1 / max(1, gsc_query_lookup.get(q, {}).get('position', 10))
            for q in user_keywords_set
            if q in gsc_query_lookup
        )
        
        total_visibility_with_user = total_visibility + user_visibility_score
        
        for comp in competitor_list:
            if total_visibility_with_user > 0:
                comp['visibility_share'] = round(
                    comp['visibility_score'] / total_visibility_with_user, 
                    3
                )
            else:
                comp['visibility_share'] = 0.0
        
        your_visibility_share = (
            round(user_visibility_score / total_visibility_with_user, 3) 
            if total_visibility_with_user > 0 
            else 0.0
        )
        
        # Take top 5-10 competitors
        top_competitors = competitor_list[:10]
        
        # Determine market leader
        market_leader = None
        market_leader_share = 0.0
        if top_competitors:
            market_leader = top_competitors[0]['domain']
            market_leader_share = top_competitors[0]['visibility_share']
        
        # Determine user's rank among competitors
        all_by_visibility = sorted(
            [{'domain': user_domain_normalized, 'visibility_share': your_visibility_share}] + top_competitors,
            key=lambda x: x['visibility_share'],
            reverse=True
        )
        your_rank = next(
            (i + 1 for i, c in enumerate(all_by_visibility) if c['domain'] == user_domain_normalized),
            len(all_by_visibility)
        )
        
        # Determine competitive intensity
        avg_competitors_per_query = len(all_queries_analyzed) / max(1, len(set(all_queries_analyzed)))
        if avg_competitors_per_query > 8:
            competitive_intensity = "high"
        elif avg_competitors_per_query > 5:
            competitive_intensity = "medium"
        else:
            competitive_intensity = "low"
        
        # Count position advantages/disadvantages
        position_advantage_keywords = sum(
            c['worse_than_you'] for c in top_competitors
        )
        position_disadvantage_keywords = sum(
            c['better_than_you'] for c in top_competitors
        )
        
        # Sort opportunity keywords by estimated clicks
        opportunity_keywords.sort(
            key=lambda x: x['estimated_monthly_clicks'], 
            reverse=True
        )
        
        # Take top 20 opportunities
        top_opportunities = opportunity_keywords[:20]
        
        # Calculate summary stats
        total_queries_analyzed = len(set(all_queries_analyzed))
        keywords_only_competitors_rank = len(opportunity_keywords)
        total_opportunity_clicks = sum(
            opp['estimated_monthly_clicks'] for opp in opportunity_keywords
        )
        
        result = {
            'top_competitors': top_competitors,
            'competitive_positioning': {
                'your_visibility_share': your_visibility_share,
                'market_leader': market_leader,
                'market_leader_share': market_leader_share,
                'your_rank_among_competitors': your_rank,
                'total_competitors_analyzed': len(competitor_list),
                'competitive_intensity': competitive_intensity,
                'position_advantage_keywords': position_advantage_keywords,
                'position_disadvantage_keywords': position_disadvantage_keywords
            },
            'opportunity_keywords': top_opportunities,
            'summary': {
                'total_queries_analyzed': total_queries_analyzed,
                'total_competitors_found': len(competitor_list),
                'avg_competitors_per_query': round(avg_competitors_per_query, 1),
                'keywords_you_rank_for': total_user_keywords,
                'keywords_only_competitors_rank': keywords_only_competitors_rank,
                'total_opportunity_clicks': total_opportunity_clicks
            }
        }
        
        logger.info(f"Competitor landscape analysis complete. Found {len(top_competitors)} top competitors")
        
        return result
        
    except Exception as e:
        logger.error(f"Error in competitor landscape analysis: {str(e)}", exc_info=True)
        return {
            'top_competitors': [],
            'competitive_positioning': {
                'your_visibility_share': 0.0,
                'market_leader': None,
                'market_leader_share': 0.0,
                'your_rank_among_competitors': 0,
                'total_competitors_analyzed': 0,
                'competitive_intensity': 'unknown',
                'position_advantage_keywords': 0,
                'position_disadvantage_keywords': 0
            },
            'opportunity_keywords': [],
            'summary': {
                'total_queries_analyzed': 0,
                'total_competitors_found': 0,
                'avg_competitors_per_query': 0.0,
                'keywords_you_rank_for': 0,
                'keywords_only_competitors_rank': 0,
                'total_opportunity_clicks': 0
            },
            'error': str(e)
        }


def get_competitor_keyword_overlap(
    competitor_domain: str,
    user_keywords: List[str],
    serp_data: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Get detailed keyword overlap analysis for a specific competitor.
    
    Returns:
        {
            "shared_keywords": [...],
            "competitor_only_keywords": [...],
            "overlap_percentage": float,
            "avg_position_gap": float
        }
    """
    competitor_domain_normalized = normalize_domain(competitor_domain)
    user_keywords_set = set(k.lower() for k in user_keywords)
    
    shared_keywords = []
    competitor_only_keywords = []
    position_gaps = []
    
    for serp_result in serp_data:
        if not isinstance(serp_result, dict):
            continue
        
        query = serp_result.get('keyword', '').lower()
        if not query:
            continue
        
        # Find competitor position
        comp_position = None
        user_position = None
        
        organic = serp_result.get('organic', [])
        for result in organic:
            domain = normalize_domain(result.get('url', ''))
            position = result.get('rank_absolute') or result.get('position', 0)
            
            if domain == competitor_domain_normalized:
                comp_position = position
            if query in user_keywords_set:
                user_position = position
        
        if comp_position:
            if query in user_keywords_set and user_position:
                shared_keywords.append({
                    'query': query,
                    'your_position': user_position,
                    'competitor_position': comp_position,
                    'gap': user_position - comp_position
                })
                position_gaps.append(user_position - comp_position)
            else:
                competitor_only_keywords.append({
                    'query': query,
                    'competitor_position': comp_position
                })
    
    overlap_percentage = (
        len(shared_keywords) / max(1, len(shared_keywords) + len(competitor_only_keywords))
    ) * 100
    
    avg_position_gap = (
        statistics.mean(position_gaps) if position_gaps else 0.0
    )
    
    return {
        'shared_keywords': shared_keywords,
        'competitor_only_keywords': competitor_only_keywords,
        'overlap_percentage': round(overlap_percentage, 1),
        'avg_position_gap': round(avg_position_gap, 1)
    }
