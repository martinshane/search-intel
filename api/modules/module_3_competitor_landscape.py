# api/modules/module_3_competitor_landscape.py

"""
Module 3: Competitor Landscape Analysis

Analyzes top competitors using GSC query data + DataForSEO SERP data.
Identifies competing domains, calculates visibility share, analyzes position
distribution, and generates competitor overlap insights with visual data for charts.
"""

import logging
from typing import Dict, Any, List, Optional, Tuple
from collections import defaultdict, Counter
from datetime import datetime, timedelta
import statistics

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
        gsc_query_data: List of GSC query performance data
        serp_data: List of DataForSEO SERP results for top queries
        user_domain: The user's domain (to identify their rankings)
        min_impressions: Minimum impressions to consider a query
        
    Returns:
        Structured analysis results with competitor insights and chart data
    """
    
    try:
        logger.info(f"Starting competitor landscape analysis for {user_domain}")
        
        # Filter queries by minimum impressions
        filtered_queries = [
            q for q in gsc_query_data 
            if q.get('impressions', 0) >= min_impressions
        ]
        
        logger.info(f"Analyzing {len(filtered_queries)} queries (min {min_impressions} impressions)")
        
        # Extract competitors from SERP data
        competitors = extract_competitors(serp_data, user_domain)
        
        # Calculate visibility metrics
        visibility_metrics = calculate_visibility_share(
            gsc_query_data, 
            serp_data, 
            user_domain, 
            competitors
        )
        
        # Analyze position distribution
        position_analysis = analyze_position_distribution(
            gsc_query_data,
            serp_data,
            user_domain,
            competitors
        )
        
        # Calculate competitor overlap
        overlap_analysis = calculate_competitor_overlap(
            serp_data,
            user_domain,
            competitors
        )
        
        # Identify head-to-head battles
        head_to_head = identify_head_to_head_keywords(
            serp_data,
            gsc_query_data,
            user_domain,
            competitors[:10]  # Top 10 competitors only
        )
        
        # Generate chart data
        chart_data = generate_chart_data(
            position_analysis,
            overlap_analysis,
            visibility_metrics,
            competitors
        )
        
        # Calculate summary metrics
        summary = generate_summary(
            competitors,
            visibility_metrics,
            position_analysis,
            overlap_analysis,
            len(filtered_queries)
        )
        
        return {
            "module": "competitor_landscape",
            "generated_at": datetime.utcnow().isoformat(),
            "domain": user_domain,
            "competitors": competitors[:20],  # Top 20 for detailed analysis
            "visibility_metrics": visibility_metrics,
            "position_distribution": position_analysis,
            "overlap_analysis": overlap_analysis,
            "head_to_head": head_to_head,
            "chart_data": chart_data,
            "summary": summary
        }
        
    except Exception as e:
        logger.error(f"Error in competitor landscape analysis: {str(e)}", exc_info=True)
        raise


def extract_competitors(
    serp_data: List[Dict[str, Any]],
    user_domain: str
) -> List[Dict[str, Any]]:
    """
    Extract and rank all competing domains from SERP data.
    
    Returns list of competitors sorted by frequency and average position.
    """
    
    competitor_stats = defaultdict(lambda: {
        'appearances': 0,
        'positions': [],
        'keywords': [],
        'urls': set()
    })
    
    for serp in serp_data:
        keyword = serp.get('keyword', '')
        results = serp.get('results', [])
        
        for result in results:
            domain = extract_domain(result.get('url', ''))
            if not domain or domain == user_domain:
                continue
                
            position = result.get('rank_absolute', 999)
            
            competitor_stats[domain]['appearances'] += 1
            competitor_stats[domain]['positions'].append(position)
            competitor_stats[domain]['keywords'].append(keyword)
            competitor_stats[domain]['urls'].add(result.get('url', ''))
    
    # Convert to list and calculate metrics
    competitors = []
    for domain, stats in competitor_stats.items():
        avg_position = statistics.mean(stats['positions']) if stats['positions'] else 999
        median_position = statistics.median(stats['positions']) if stats['positions'] else 999
        
        # Calculate visibility score (appearances weighted by position)
        visibility_score = sum(
            1 / pos if pos > 0 else 0 
            for pos in stats['positions']
        )
        
        competitors.append({
            'domain': domain,
            'appearances': stats['appearances'],
            'avg_position': round(avg_position, 2),
            'median_position': round(median_position, 1),
            'best_position': min(stats['positions']) if stats['positions'] else 999,
            'visibility_score': round(visibility_score, 2),
            'keywords_shared': list(set(stats['keywords'])),
            'keyword_count': len(set(stats['keywords'])),
            'unique_urls': len(stats['urls']),
            'threat_level': calculate_threat_level(
                stats['appearances'],
                avg_position,
                len(serp_data)
            )
        })
    
    # Sort by visibility score descending
    competitors.sort(key=lambda x: x['visibility_score'], reverse=True)
    
    return competitors


def extract_domain(url: str) -> Optional[str]:
    """Extract clean domain from URL."""
    if not url:
        return None
        
    # Remove protocol
    url = url.replace('https://', '').replace('http://', '')
    
    # Remove www
    url = url.replace('www.', '')
    
    # Get domain part (before first /)
    domain = url.split('/')[0]
    
    # Remove port if present
    domain = domain.split(':')[0]
    
    return domain.lower() if domain else None


def calculate_threat_level(
    appearances: int,
    avg_position: float,
    total_keywords: int
) -> str:
    """
    Calculate threat level based on frequency and position.
    
    Returns: 'critical', 'high', 'medium', or 'low'
    """
    
    appearance_rate = appearances / total_keywords if total_keywords > 0 else 0
    
    if appearance_rate > 0.4 and avg_position < 5:
        return 'critical'
    elif appearance_rate > 0.25 and avg_position < 7:
        return 'high'
    elif appearance_rate > 0.15 or avg_position < 10:
        return 'medium'
    else:
        return 'low'


def calculate_visibility_share(
    gsc_query_data: List[Dict[str, Any]],
    serp_data: List[Dict[str, Any]],
    user_domain: str,
    competitors: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Calculate visibility share metrics for user vs competitors.
    
    Uses position-based CTR curves to estimate click share.
    """
    
    # Standard CTR curve (position -> expected CTR)
    position_ctr = {
        1: 0.316, 2: 0.159, 3: 0.099, 4: 0.067, 5: 0.051,
        6: 0.041, 7: 0.035, 8: 0.030, 9: 0.026, 10: 0.023,
        11: 0.020, 12: 0.018, 13: 0.016, 14: 0.015, 15: 0.014,
        16: 0.013, 17: 0.012, 18: 0.011, 19: 0.010, 20: 0.010
    }
    
    # Build keyword -> position mapping from GSC
    user_positions = {}
    for query in gsc_query_data:
        keyword = query.get('keys', [''])[0] if query.get('keys') else query.get('query', '')
        position = query.get('position', 999)
        impressions = query.get('impressions', 0)
        user_positions[keyword] = {
            'position': position,
            'impressions': impressions
        }
    
    # Calculate estimated clicks per domain
    domain_clicks = defaultdict(float)
    keyword_coverage = defaultdict(set)
    
    for serp in serp_data:
        keyword = serp.get('keyword', '')
        results = serp.get('results', [])
        
        # Get total search volume (use impressions from GSC or estimate)
        search_volume = user_positions.get(keyword, {}).get('impressions', 1000)
        
        for result in results:
            domain = extract_domain(result.get('url', ''))
            if not domain:
                continue
                
            position = result.get('rank_absolute', 999)
            ctr = position_ctr.get(position, 0.005)  # Default 0.5% for positions > 20
            
            estimated_clicks = search_volume * ctr
            domain_clicks[domain] += estimated_clicks
            keyword_coverage[domain].add(keyword)
    
    # Calculate total available clicks
    total_clicks = sum(domain_clicks.values())
    
    # Get user's share
    user_clicks = domain_clicks.get(user_domain, 0)
    user_share = user_clicks / total_clicks if total_clicks > 0 else 0
    
    # Build competitor shares
    competitor_shares = []
    for comp in competitors[:10]:  # Top 10 only
        domain = comp['domain']
        clicks = domain_clicks.get(domain, 0)
        share = clicks / total_clicks if total_clicks > 0 else 0
        
        competitor_shares.append({
            'domain': domain,
            'estimated_clicks': round(clicks, 1),
            'share': round(share, 4),
            'share_pct': round(share * 100, 2),
            'keywords_ranking': len(keyword_coverage.get(domain, set()))
        })
    
    # Sort by share
    competitor_shares.sort(key=lambda x: x['share'], reverse=True)
    
    # Calculate opportunity (clicks going to competitors that user could capture)
    top_5_competitor_clicks = sum(
        c['estimated_clicks'] 
        for c in competitor_shares[:5]
    )
    
    return {
        'user_domain': user_domain,
        'user_estimated_clicks': round(user_clicks, 1),
        'user_share': round(user_share, 4),
        'user_share_pct': round(user_share * 100, 2),
        'total_market_clicks': round(total_clicks, 1),
        'competitor_shares': competitor_shares,
        'top_competitor': competitor_shares[0] if competitor_shares else None,
        'opportunity_clicks': round(top_5_competitor_clicks, 1),
        'opportunity_share_pct': round((top_5_competitor_clicks / total_clicks * 100) if total_clicks > 0 else 0, 2)
    }


def analyze_position_distribution(
    gsc_query_data: List[Dict[str, Any]],
    serp_data: List[Dict[str, Any]],
    user_domain: str,
    competitors: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Analyze position distribution for user vs top competitors.
    """
    
    # Position buckets
    buckets = {
        'top_3': (1, 3),
        'top_5': (4, 5),
        'top_10': (6, 10),
        'page_2': (11, 20),
        'page_3_plus': (21, 100)
    }
    
    # Initialize counters
    user_distribution = {bucket: 0 for bucket in buckets}
    competitor_distributions = defaultdict(lambda: {bucket: 0 for bucket in buckets})
    
    # Count user positions from GSC
    for query in gsc_query_data:
        position = query.get('position', 999)
        
        for bucket, (min_pos, max_pos) in buckets.items():
            if min_pos <= position <= max_pos:
                user_distribution[bucket] += 1
                break
    
    # Count competitor positions from SERP data
    for serp in serp_data:
        results = serp.get('results', [])
        
        for result in results:
            domain = extract_domain(result.get('url', ''))
            if not domain or domain == user_domain:
                continue
                
            position = result.get('rank_absolute', 999)
            
            for bucket, (min_pos, max_pos) in buckets.items():
                if min_pos <= position <= max_pos:
                    competitor_distributions[domain][bucket] += 1
                    break
    
    # Build competitor comparison (top 5 only)
    top_competitors = competitors[:5]
    competitor_comparison = []
    
    for comp in top_competitors:
        domain = comp['domain']
        dist = competitor_distributions.get(domain, {bucket: 0 for bucket in buckets})
        
        competitor_comparison.append({
            'domain': domain,
            'distribution': dist,
            'total_keywords': sum(dist.values())
        })
    
    return {
        'user_distribution': user_distribution,
        'user_total': sum(user_distribution.values()),
        'competitor_comparison': competitor_comparison,
        'buckets': buckets
    }


def calculate_competitor_overlap(
    serp_data: List[Dict[str, Any]],
    user_domain: str,
    competitors: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Calculate keyword overlap between user and each competitor.
    """
    
    # Get keywords where user appears
    user_keywords = set()
    keyword_user_position = {}
    
    for serp in serp_data:
        keyword = serp.get('keyword', '')
        results = serp.get('results', [])
        
        for result in results:
            domain = extract_domain(result.get('url', ''))
            if domain == user_domain:
                user_keywords.add(keyword)
                keyword_user_position[keyword] = result.get('rank_absolute', 999)
                break
    
    # Calculate overlap with each competitor
    overlap_data = []
    
    for comp in competitors[:10]:  # Top 10 only
        competitor_domain = comp['domain']
        shared_keywords = []
        competitor_only_keywords = []
        user_wins = 0
        competitor_wins = 0
        
        for serp in serp_data:
            keyword = serp.get('keyword', '')
            results = serp.get('results', [])
            
            user_in_serp = False
            comp_in_serp = False
            user_pos = 999
            comp_pos = 999
            
            for result in results:
                domain = extract_domain(result.get('url', ''))
                pos = result.get('rank_absolute', 999)
                
                if domain == user_domain:
                    user_in_serp = True
                    user_pos = pos
                elif domain == competitor_domain:
                    comp_in_serp = True
                    comp_pos = pos
            
            if user_in_serp and comp_in_serp:
                shared_keywords.append({
                    'keyword': keyword,
                    'user_position': user_pos,
                    'competitor_position': comp_pos,
                    'user_ahead': user_pos < comp_pos
                })
                
                if user_pos < comp_pos:
                    user_wins += 1
                else:
                    competitor_wins += 1
                    
            elif comp_in_serp and not user_in_serp:
                competitor_only_keywords.append({
                    'keyword': keyword,
                    'competitor_position': comp_pos
                })
        
        if shared_keywords:
            overlap_data.append({
                'domain': competitor_domain,
                'shared_keywords': len(shared_keywords),
                'user_wins': user_wins,
                'competitor_wins': competitor_wins,
                'win_rate': round(user_wins / len(shared_keywords), 3) if shared_keywords else 0,
                'competitor_only_keywords': len(competitor_only_keywords),
                'top_shared': sorted(
                    shared_keywords, 
                    key=lambda x: abs(x['user_position'] - x['competitor_position']),
                    reverse=True
                )[:5],  # Top 5 most contested
                'top_opportunities': sorted(
                    competitor_only_keywords,
                    key=lambda x: x['competitor_position']
                )[:5]  # Top 5 opportunities (where competitor ranks well but user doesn't)
            })
    
    return {
        'user_total_keywords': len(user_keywords),
        'overlap_by_competitor': overlap_data
    }


def identify_head_to_head_keywords(
    serp_data: List[Dict[str, Any]],
    gsc_query_data: List[Dict[str, Any]],
    user_domain: str,
    top_competitors: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Identify high-value keywords where user and competitors are closely matched.
    """
    
    # Build impressions map
    impressions_map = {}
    for query in gsc_query_data:
        keyword = query.get('keys', [''])[0] if query.get('keys') else query.get('query', '')
        impressions_map[keyword] = query.get('impressions', 0)
    
    head_to_head = []
    competitor_domains = {c['domain'] for c in top_competitors}
    
    for serp in serp_data:
        keyword = serp.get('keyword', '')
        results = serp.get('results', [])
        impressions = impressions_map.get(keyword, 0)
        
        # Find user position and nearby competitors
        user_pos = None
        nearby_competitors = []
        
        for result in results:
            domain = extract_domain(result.get('url', ''))
            pos = result.get('rank_absolute', 999)
            
            if domain == user_domain:
                user_pos = pos
            elif domain in competitor_domains:
                nearby_competitors.append({
                    'domain': domain,
                    'position': pos,
                    'url': result.get('url', '')
                })
        
        if user_pos is None:
            continue
        
        # Filter to competitors within 3 positions
        close_competitors = [
            c for c in nearby_competitors 
            if abs(c['position'] - user_pos) <= 3
        ]
        
        if close_competitors and impressions > 100:
            # Calculate potential impact
            for comp in close_competitors:
                position_gap = comp['position'] - user_pos
                
                head_to_head.append({
                    'keyword': keyword,
                    'impressions': impressions,
                    'user_position': user_pos,
                    'competitor_domain': comp['domain'],
                    'competitor_position': comp['position'],
                    'competitor_url': comp['url'],
                    'position_gap': position_gap,
                    'user_ahead': position_gap > 0,
                    'battle_intensity': 'high' if impressions > 1000 and abs(position_gap) <= 1 else 'medium'
                })
    
    # Sort by impressions (highest value battles first)
    head_to_head.sort(key=lambda x: x['impressions'], reverse=True)
    
    return head_to_head[:50]  # Top 50 head-to-head battles


def generate_chart_data(
    position_analysis: Dict[str, Any],
    overlap_analysis: Dict[str, Any],
    visibility_metrics: Dict[str, Any],
    competitors: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Generate structured data for frontend charts.
    """
    
    return {
        'position_distribution_chart': {
            'type': 'stacked_bar',
            'title': 'Position Distribution: You vs Competitors',
            'description': 'Keyword count by position bracket',
            'data': {
                'labels': ['Top 3', 'Top 5', 'Top 10', 'Page 2', 'Page 3+'],
                'datasets': [
                    {
                        'label': 'Your Site',
                        'data': [
                            position_analysis['user_distribution']['top_3'],
                            position_analysis['user_distribution']['top_5'],
                            position_analysis['user_distribution']['top_10'],
                            position_analysis['user_distribution']['page_2'],
                            position_analysis['user_distribution']['page_3_plus']
                        ],
                        'color': '#3b82f6'
                    }
                ] + [
                    {
                        'label': comp['domain'],
                        'data': [
                            comp['distribution']['top_3'],
                            comp['distribution']['top_5'],
                            comp['distribution']['top_10'],
                            comp['distribution']['page_2'],
                            comp['distribution']['page_3_plus']
                        ]
                    }
                    for comp in position_analysis['competitor_comparison'][:3]
                ]
            }
        },
        
        'visibility_share_chart': {
            'type': 'pie',
            'title': 'Estimated Click Share',
            'description': 'Market share based on position-weighted CTR',
            'data': {
                'labels': ['Your Site'] + [
                    c['domain'] for c in visibility_metrics['competitor_shares'][:5]
                ] + ['Others'],
                'values': [
                    visibility_metrics['user_share_pct']
                ] + [
                    c['share_pct'] for c in visibility_metrics['competitor_shares'][:5]
                ] + [
                    max(0, 100 - visibility_metrics['user_share_pct'] - sum(
                        c['share_pct'] for c in visibility_metrics['competitor_shares'][:5]
                    ))
                ],
                'colors': ['#3b82f6', '#8b5cf6', '#ec4899', '#f59e0b', '#10b981', '#6b7280', '#d1d5db']
            }
        },
        
        'competitor_overlap_chart': {
            'type': 'horizontal_bar',
            'title': 'Keyword Overlap with Top Competitors',
            'description': 'Shared keywords and win rate',
            'data': {
                'labels': [
                    c['domain'] 
                    for c in overlap_analysis['overlap_by_competitor'][:5]
                ],
                'datasets': [
                    {
                        'label': 'You Win',
                        'data': [
                            c['user_wins']
                            for c in overlap_analysis['overlap_by_competitor'][:5]
                        ],
                        'color': '#10b981'
                    },
                    {
                        'label': 'They Win',
                        'data': [
                            c['competitor_wins']
                            for c in overlap_analysis['overlap_by_competitor'][:5]
                        ],
                        'color': '#ef4444'
                    }
                ]
            }
        },
        
        'threat_matrix': {
            'type': 'scatter',
            'title': 'Competitor Threat Matrix',
            'description': 'Position vs frequency of appearance',
            'data': {
                'points': [
                    {
                        'domain': comp['domain'],
                        'x': comp['appearances'],
                        'y': comp['avg_position'],
                        'size': comp['visibility_score'],
                        'threat': comp['threat_level']
                    }
                    for comp in competitors[:20]
                ]
            }
        }
    }


def generate_summary(
    competitors: List[Dict[str, Any]],
    visibility_metrics: Dict[str, Any],
    position_analysis: Dict[str, Any],
    overlap_analysis: Dict[str, Any],
    total_keywords: int
) -> Dict[str, Any]:
    """
    Generate executive summary of competitor landscape.
    """
    
    top_competitor = competitors[0] if competitors else None
    
    # Calculate key metrics
    critical_threats = [c for c in competitors if c['threat_level'] == 'critical']
    high_threats = [c for c in competitors if c['threat_level'] == 'high']
    
    top_3_rate = (
        position_analysis['user_distribution']['top_3'] / 
        position_analysis['user_total']
    ) if position_analysis['user_total'] > 0 else 0
    
    avg_overlap = statistics.mean([
        c['shared_keywords'] 
        for c in overlap_analysis['overlap_by_competitor']
    ]) if overlap_analysis['overlap_by_competitor'] else 0
    
    return {
        'total_competitors_identified': len(competitors),
        'critical_threats': len(critical_threats),
        'high_threats': len(high_threats),
        'top_competitor': top_competitor['domain'] if top_competitor else 'None',
        'top_competitor_share_pct': (
            visibility_metrics['competitor_shares'][0]['share_pct']
            if visibility_metrics['competitor_shares'] else 0
        ),
        'your_market_share_pct': visibility_metrics['user_share_pct'],
        'market_share_rank': calculate_market_share_rank(
            visibility_metrics['user_share_pct'],
            visibility_metrics['competitor_shares']
        ),
        'top_3_position_rate': round(top_3_rate * 100, 1),
        'avg_keyword_overlap': round(avg_overlap, 1),
        'total_opportunity_clicks': visibility_metrics['opportunity_clicks'],
        'keywords_analyzed': total_keywords,
        'key_insight': generate_key_insight(
            visibility_metrics,
            competitors,
            position_analysis
        )
    }


def calculate_market_share_rank(
    user_share: float,
    competitor_shares: List[Dict[str, Any]]
) -> int:
    """Calculate where user ranks in market share."""
    
    all_shares = [user_share] + [c['share_pct'] for c in competitor_shares]
    all_shares.sort(reverse=True)
    
    return all_shares.index(user_share) + 1


def generate_key_insight(
    visibility_metrics: Dict[str, Any],
    competitors: List[Dict[str, Any]],
    position_analysis: Dict[str, Any]
) -> str:
    """Generate a key insight sentence."""
    
    user_share = visibility_metrics['user_share_pct']
    top_comp = competitors[0] if competitors else None
    
    if not top_comp:
        return "No significant competitors identified in current keyword set."
    
    top_comp_share = visibility_metrics['competitor_shares'][0]['share_pct']
    
    if user_share > top_comp_share:
        return f"You lead the market with {user_share:.1f}% visibility share, ahead of {top_comp['domain']} at {top_comp_share:.1f}%."
    else:
        gap = top_comp_share - user_share
        return f"{top_comp['domain']} leads with {top_comp_share:.1f}% visibility share, {gap:.1f}% ahead of your {user_share:.1f}%."