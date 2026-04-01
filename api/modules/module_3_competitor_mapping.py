import logging
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from collections import defaultdict, Counter
import math
from datetime import datetime, timedelta

import pandas as pd
import numpy as np

from api.clients.dataforseo_client import DataForSEOClient

logger = logging.getLogger(__name__)


@dataclass
class CompetitorMetrics:
    """Metrics for a single competitor."""
    domain: str
    keywords_shared: int
    overlap_percentage: float
    avg_position: float
    median_position: float
    avg_user_position_for_shared: float
    position_advantage: float  # negative if competitor ranks better
    estimated_visibility_share: float
    threat_level: str  # low, medium, high, critical
    shared_keywords: List[Dict[str, Any]]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "domain": self.domain,
            "keywords_shared": self.keywords_shared,
            "overlap_percentage": round(self.overlap_percentage, 2),
            "avg_position": round(self.avg_position, 2),
            "median_position": round(self.median_position, 2),
            "avg_user_position_for_shared": round(self.avg_user_position_for_shared, 2),
            "position_advantage": round(self.position_advantage, 2),
            "estimated_visibility_share": round(self.estimated_visibility_share, 4),
            "threat_level": self.threat_level,
            "top_shared_keywords": sorted(
                self.shared_keywords,
                key=lambda x: x.get("impressions", 0),
                reverse=True
            )[:10]  # Top 10 by impressions
        }


def extract_domain(url: str) -> str:
    """Extract clean domain from URL."""
    if not url:
        return ""
    
    # Remove protocol
    url = url.replace("https://", "").replace("http://", "")
    
    # Remove www
    if url.startswith("www."):
        url = url[4:]
    
    # Take domain part only (before first /)
    domain = url.split("/")[0]
    
    # Remove port if present
    domain = domain.split(":")[0]
    
    return domain.lower()


def is_branded_query(query: str, user_domain: str, brand_name: Optional[str] = None) -> bool:
    """
    Determine if a query is branded.
    
    Args:
        query: Search query to check
        user_domain: User's domain
        brand_name: Optional explicit brand name
    
    Returns:
        True if query appears to be branded
    """
    query_lower = query.lower()
    
    # Extract base domain (without TLD)
    domain_parts = user_domain.split(".")
    base_domain = domain_parts[0] if len(domain_parts) > 0 else user_domain
    
    # Check for domain presence
    if base_domain.lower() in query_lower:
        return True
    
    # Check for brand name if provided
    if brand_name and brand_name.lower() in query_lower:
        return True
    
    # Check for common branded patterns
    branded_patterns = [
        "login",
        "sign in",
        "account",
        "portal",
        "dashboard",
        "pricing",  # Only if combined with domain/brand
    ]
    
    for pattern in branded_patterns:
        if pattern in query_lower and base_domain.lower() in query_lower:
            return True
    
    return False


def select_top_keywords(
    gsc_keyword_data: pd.DataFrame,
    user_domain: str,
    max_keywords: int = 100,
    brand_name: Optional[str] = None
) -> List[str]:
    """
    Select top non-branded keywords from GSC data.
    
    Args:
        gsc_keyword_data: DataFrame with columns: query, impressions, clicks, position
        user_domain: User's domain for branded filtering
        max_keywords: Maximum number of keywords to return
        brand_name: Optional brand name for branded filtering
    
    Returns:
        List of selected keywords
    """
    if gsc_keyword_data.empty:
        logger.warning("No GSC keyword data available")
        return []
    
    # Filter out branded queries
    non_branded = gsc_keyword_data[
        ~gsc_keyword_data['query'].apply(
            lambda q: is_branded_query(q, user_domain, brand_name)
        )
    ].copy()
    
    logger.info(f"Filtered {len(gsc_keyword_data)} keywords to {len(non_branded)} non-branded")
    
    if non_branded.empty:
        logger.warning("No non-branded keywords found")
        return []
    
    # Create composite score: impressions (primary) + position factor
    # Boost keywords that have good impressions and rank in top 20
    non_branded['score'] = non_branded['impressions'] * (
        1 + (non_branded['position'] <= 20).astype(float) * 0.5
    )
    
    # Sort by score and take top N
    top_keywords = non_branded.nlargest(max_keywords, 'score')
    
    logger.info(f"Selected {len(top_keywords)} top keywords for SERP analysis")
    
    return top_keywords['query'].tolist()


def calculate_visibility_score(position: float) -> float:
    """
    Calculate visibility score based on position.
    Uses exponential decay curve approximating real CTR distribution.
    
    Position 1 = 1.0
    Position 10 = ~0.02
    Position >20 = ~0.0
    
    Args:
        position: Ranking position
    
    Returns:
        Visibility score between 0 and 1
    """
    if position <= 0:
        return 0.0
    
    # Exponential decay curve fitted to typical CTR data
    # CTR roughly halves every ~2.5 positions in top 10
    if position <= 10:
        score = math.exp(-0.3 * (position - 1))
    elif position <= 20:
        score = 0.02 * math.exp(-0.15 * (position - 10))
    else:
        score = 0.001 * math.exp(-0.1 * (position - 20))
    
    return max(0.0, min(1.0, score))


async def analyze_competitor_mapping(
    gsc_keyword_data: pd.DataFrame,
    user_domain: str,
    dataforseo_client: DataForSEOClient,
    location_code: int = 2840,  # USA
    language_code: str = "en",
    max_keywords: int = 100,
    brand_name: Optional[str] = None
) -> Dict[str, Any]:
    """
    Module 3: Competitor Mapping
    
    Takes top organic keywords from Module 1, queries DataForSEO for SERP data,
    identifies competitor domains ranking in top 10, calculates competitor visibility
    scores, maps keyword overlap, and returns comprehensive competitor analysis.
    
    Args:
        gsc_keyword_data: GSC keyword performance data
        user_domain: User's domain (clean, without www)
        dataforseo_client: Configured DataForSEO client
        location_code: Geographic location code
        language_code: Language code
        max_keywords: Maximum keywords to analyze
        brand_name: Optional brand name for filtering
    
    Returns:
        {
            "competitor_domains": [...],
            "overlap_matrix": {...},
            "visibility_metrics": {...},
            "keyword_competition": [...],
            "summary": {...}
        }
    """
    logger.info(f"Starting competitor mapping for {user_domain}")
    
    try:
        # Step 1: Select top non-branded keywords
        selected_keywords = select_top_keywords(
            gsc_keyword_data,
            user_domain,
            max_keywords,
            brand_name
        )
        
        if not selected_keywords:
            return {
                "error": "No suitable keywords found for analysis",
                "competitor_domains": [],
                "overlap_matrix": {},
                "visibility_metrics": {},
                "keyword_competition": [],
                "summary": {
                    "total_keywords_analyzed": 0,
                    "total_competitors_found": 0,
                    "user_domain": user_domain
                }
            }
        
        # Step 2: Fetch SERP data for selected keywords
        logger.info(f"Fetching SERP data for {len(selected_keywords)} keywords")
        serp_results = await fetch_serp_data_batch(
            dataforseo_client,
            selected_keywords,
            location_code,
            language_code
        )
        
        if not serp_results:
            return {
                "error": "Failed to fetch SERP data",
                "competitor_domains": [],
                "overlap_matrix": {},
                "visibility_metrics": {},
                "keyword_competition": [],
                "summary": {
                    "total_keywords_analyzed": 0,
                    "total_competitors_found": 0,
                    "user_domain": user_domain
                }
            }
        
        # Step 3: Extract competitors from SERP results
        logger.info("Extracting competitor domains from SERP results")
        competitor_data = extract_competitors_from_serps(
            serp_results,
            user_domain,
            gsc_keyword_data
        )
        
        # Step 4: Calculate competitor metrics
        logger.info("Calculating competitor metrics")
        competitor_metrics = calculate_competitor_metrics(
            competitor_data,
            len(selected_keywords)
        )
        
        # Step 5: Build overlap matrix
        logger.info("Building overlap matrix")
        overlap_matrix = build_overlap_matrix(competitor_data)
        
        # Step 6: Calculate visibility metrics
        logger.info("Calculating visibility metrics")
        visibility_metrics = calculate_visibility_metrics(
            serp_results,
            user_domain,
            competitor_metrics
        )
        
        # Step 7: Analyze keyword-level competition
        logger.info("Analyzing keyword-level competition")
        keyword_competition = analyze_keyword_competition(
            serp_results,
            user_domain,
            gsc_keyword_data
        )
        
        # Step 8: Build summary
        summary = {
            "total_keywords_analyzed": len(selected_keywords),
            "keywords_successfully_fetched": len(serp_results),
            "total_competitors_found": len(competitor_metrics),
            "primary_competitors": len([c for c in competitor_metrics if c['threat_level'] in ['high', 'critical']]),
            "user_domain": user_domain,
            "avg_competitors_per_keyword": round(
                np.mean([len(r.get('competitors', [])) for r in serp_results]),
                2
            ) if serp_results else 0,
            "user_avg_position_analyzed_keywords": round(
                visibility_metrics.get('user_avg_position', 0),
                2
            ),
            "user_total_visibility_score": round(
                visibility_metrics.get('user_total_visibility', 0),
                4
            ),
            "market_concentration": calculate_market_concentration(competitor_metrics),
            "analysis_timestamp": datetime.utcnow().isoformat()
        }
        
        logger.info(f"Competitor mapping complete. Found {len(competitor_metrics)} competitors")
        
        return {
            "competitor_domains": competitor_metrics,
            "overlap_matrix": overlap_matrix,
            "visibility_metrics": visibility_metrics,
            "keyword_competition": keyword_competition,
            "summary": summary
        }
        
    except Exception as e:
        logger.error(f"Error in competitor mapping analysis: {str(e)}", exc_info=True)
        return {
            "error": str(e),
            "competitor_domains": [],
            "overlap_matrix": {},
            "visibility_metrics": {},
            "keyword_competition": [],
            "summary": {
                "total_keywords_analyzed": 0,
                "total_competitors_found": 0,
                "user_domain": user_domain
            }
        }


async def fetch_serp_data_batch(
    client: DataForSEOClient,
    keywords: List[str],
    location_code: int,
    language_code: str,
    batch_size: int = 20
) -> List[Dict[str, Any]]:
    """
    Fetch SERP data for a batch of keywords.
    
    Args:
        client: DataForSEO client
        keywords: List of keywords to fetch
        location_code: Geographic location
        language_code: Language code
        batch_size: Number of keywords per batch
    
    Returns:
        List of SERP result dictionaries
    """
    results = []
    
    # Process in batches to avoid overwhelming the API
    for i in range(0, len(keywords), batch_size):
        batch = keywords[i:i + batch_size]
        logger.info(f"Fetching SERP data for batch {i//batch_size + 1} ({len(batch)} keywords)")
        
        try:
            batch_results = await client.get_serp_results(
                keywords=batch,
                location_code=location_code,
                language_code=language_code,
                device="desktop"
            )
            
            if batch_results:
                results.extend(batch_results)
                
        except Exception as e:
            logger.error(f"Error fetching batch {i//batch_size + 1}: {str(e)}")
            continue
    
    logger.info(f"Successfully fetched SERP data for {len(results)} keywords")
    return results


def extract_competitors_from_serps(
    serp_results: List[Dict[str, Any]],
    user_domain: str,
    gsc_keyword_data: pd.DataFrame
) -> Dict[str, Dict[str, Any]]:
    """
    Extract competitor domains and their metrics from SERP results.
    
    Args:
        serp_results: SERP data from DataForSEO
        user_domain: User's domain
        gsc_keyword_data: GSC data for user's keywords
    
    Returns:
        Dictionary mapping competitor domains to their data
    """
    competitor_data = defaultdict(lambda: {
        'keywords': [],
        'positions': [],
        'user_positions': [],
        'keyword_details': []
    })
    
    # Create lookup for user's GSC data
    gsc_lookup = {}
    if not gsc_keyword_data.empty:
        for _, row in gsc_keyword_data.iterrows():
            gsc_lookup[row['query'].lower()] = {
                'impressions': row.get('impressions', 0),
                'clicks': row.get('clicks', 0),
                'position': row.get('position', 100)
            }
    
    for serp_result in serp_results:
        keyword = serp_result.get('keyword', '')
        organic_results = serp_result.get('organic_results', [])
        
        if not organic_results:
            continue
        
        # Get user's GSC data for this keyword
        user_gsc_data = gsc_lookup.get(keyword.lower(), {})
        user_position = user_gsc_data.get('position', 100)
        
        # Track if user is ranking for this keyword in top 20
        user_in_top20 = user_position <= 20
        
        # Extract top 10 organic results
        for result in organic_results[:10]:
            url = result.get('url', '')
            position = result.get('rank_absolute', 100)
            
            if not url or position > 20:  # Only consider top 20
                continue
            
            domain = extract_domain(url)
            
            # Skip user's own domain
            if domain == user_domain or domain in user_domain or user_domain in domain:
                continue
            
            # Skip empty domains
            if not domain:
                continue
            
            # Add to competitor data
            competitor_data[domain]['keywords'].append(keyword)
            competitor_data[domain]['positions'].append(position)
            competitor_data[domain]['user_positions'].append(user_position)
            competitor_data[domain]['keyword_details'].append({
                'keyword': keyword,
                'competitor_position': position,
                'user_position': user_position,
                'impressions': user_gsc_data.get('impressions', 0),
                'clicks': user_gsc_data.get('clicks', 0),
                'url': url
            })
    
    return dict(competitor_data)


def calculate_competitor_metrics(
    competitor_data: Dict[str, Dict[str, Any]],
    total_keywords: int
) -> List[Dict[str, Any]]:
    """
    Calculate comprehensive metrics for each competitor.
    
    Args:
        competitor_data: Raw competitor data
        total_keywords: Total number of keywords analyzed
    
    Returns:
        List of competitor metric dictionaries
    """
    competitors = []
    
    for domain, data in competitor_data.items():
        if not data['keywords']:
            continue
        
        keywords_shared = len(data['keywords'])
        overlap_percentage = (keywords_shared / total_keywords) * 100
        
        # Position metrics
        positions = data['positions']
        avg_position = np.mean(positions)
        median_position = np.median(positions)
        
        # User position for shared keywords
        user_positions = data['user_positions']
        avg_user_position = np.mean(user_positions)
        
        # Position advantage (negative = competitor ranks better)
        position_advantage = avg_user_position - avg_position
        
        # Visibility score (sum of visibility for all shared keywords)
        visibility_scores = [calculate_visibility_score(pos) for pos in positions]
        estimated_visibility_share = sum(visibility_scores) / len(visibility_scores) if visibility_scores else 0
        
        # Threat level assessment
        threat_level = assess_threat_level(
            overlap_percentage,
            position_advantage,
            keywords_shared,
            estimated_visibility_share
        )
        
        competitor = {
            "domain": domain,
            "keywords_shared": keywords_shared,
            "overlap_percentage": round(overlap_percentage, 2),
            "avg_position": round(avg_position, 2),
            "median_position": round(median_position, 2),
            "avg_user_position_for_shared": round(avg_user_position, 2),
            "position_advantage": round(position_advantage, 2),
            "estimated_visibility_share": round(estimated_visibility_share, 4),
            "threat_level": threat_level,
            "shared_keywords": sorted(
                data['keyword_details'],
                key=lambda x: x.get('impressions', 0),
                reverse=True
            )[:20]  # Top 20 by impressions
        }
        
        competitors.append(competitor)
    
    # Sort by threat level and overlap
    threat_order = {'critical': 0, 'high': 1, 'medium': 2, 'low': 3}
    competitors.sort(
        key=lambda x: (threat_order.get(x['threat_level'], 4), -x['keywords_shared'])
    )
    
    return competitors


def assess_threat_level(
    overlap_percentage: float,
    position_advantage: float,
    keywords_shared: int,
    visibility_share: float
) -> str:
    """
    Assess competitor threat level.
    
    Args:
        overlap_percentage: Percentage of keywords shared
        position_advantage: User's position advantage (negative = competitor better)
        keywords_shared: Number of shared keywords
        visibility_share: Competitor's visibility score
    
    Returns:
        Threat level: critical, high, medium, low
    """
    # Critical: high overlap + competitor ranks better + significant keywords
    if overlap_percentage > 30 and position_advantage < -2 and keywords_shared > 20:
        return "critical"
    
    # High: significant overlap + ranks better
    if overlap_percentage > 20 and position_advantage < -1:
        return "high"
    
    # High: very high overlap regardless of position
    if overlap_percentage > 40:
        return "high"
    
    # Medium: moderate overlap + ranks better
    if overlap_percentage > 10 and position_advantage < 0:
        return "medium"
    
    # Medium: high visibility share
    if visibility_share > 0.3:
        return "medium"
    
    # Low: everything else
    return "low"


def build_overlap_matrix(
    competitor_data: Dict[str, Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Build keyword overlap matrix between competitors.
    
    Args:
        competitor_data: Raw competitor data
    
    Returns:
        Overlap matrix and related metrics
    """
    domains = list(competitor_data.keys())
    
    if not domains:
        return {
            "domains": [],
            "matrix": [],
            "top_overlaps": []
        }
    
    # Build matrix
    matrix = []
    for domain1 in domains:
        row = []
        keywords1 = set(competitor_data[domain1]['keywords'])
        
        for domain2 in domains:
            keywords2 = set(competitor_data[domain2]['keywords'])
            
            if domain1 == domain2:
                overlap = 100.0
            else:
                intersection = len(keywords1 & keywords2)
                union = len(keywords1 | keywords2)
                overlap = (intersection / union * 100) if union > 0 else 0
            
            row.append(round(overlap, 2))
        
        matrix.append(row)
    
    # Find top overlapping pairs
    top_overlaps = []
    for i, domain1 in enumerate(domains):
        for j, domain2 in enumerate(domains):
            if i < j:  # Only upper triangle
                overlap = matrix[i][j]
                if overlap > 10:  # Only significant overlaps
                    top_overlaps.append({
                        "domain1": domain1,
                        "domain2": domain2,
                        "overlap_percentage": overlap,
                        "shared_keywords": len(
                            set(competitor_data[domain1]['keywords']) & 
                            set(competitor_data[domain2]['keywords'])
                        )
                    })
    
    # Sort by overlap percentage
    top_overlaps.sort(key=lambda x: x['overlap_percentage'], reverse=True)
    
    return {
        "domains": domains[:50],  # Limit to top 50 for matrix display
        "matrix": [row[:50] for row in matrix[:50]],  # 50x50 matrix max
        "top_overlaps": top_overlaps[:20]  # Top 20 pairs
    }


def calculate_visibility_metrics(
    serp_results: List[Dict[str, Any]],
    user_domain: str,
    competitor_metrics: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Calculate overall visibility metrics.
    
    Args:
        serp_results: SERP data
        user_domain: User's domain
        competitor_metrics: Calculated competitor metrics
    
    Returns:
        Visibility metrics dictionary
    """
    user_visibility_scores = []
    user_positions = []
    total_market_visibility = []
    
    for serp_result in serp_results:
        organic_results = serp_result.get('organic_results', [])
        
        if not organic_results:
            continue
        
        # Calculate market visibility for this keyword
        keyword_visibility = 0
        user_visibility = 0
        user_position = None
        
        for result in organic_results[:20]:
            url = result.get('url', '')
            position = result.get('rank_absolute', 100)
            
            if not url:
                continue
            
            domain = extract_domain(url)
            visibility = calculate_visibility_score(position)
            keyword_visibility += visibility
            
            # Check if this is user's domain
            if domain == user_domain or domain in user_domain or user_domain in domain:
                user_visibility = visibility
                user_position = position
        
        if user_visibility > 0:
            user_visibility_scores.append(user_visibility)
            user_positions.append(user_position)
        
        total_market_visibility.append(keyword_visibility)
    
    # Calculate metrics
    user_total_visibility = sum(user_visibility_scores)
    market_total_visibility = sum(total_market_visibility)
    
    user_visibility_share = (
        (user_total_visibility / market_total_visibility * 100)
        if market_total_visibility > 0 else 0
    )
    
    # Competitor visibility distribution
    competitor_visibility_dist = []
    for comp in competitor_metrics[:10]:  # Top 10
        competitor_visibility_dist.append({
            "domain": comp['domain'],
            "visibility_share": comp['estimated_visibility_share'],
            "keywords_shared": comp['keywords_shared']
        })
    
    return {
        "user_total_visibility": user_total_visibility,
        "user_visibility_share": round(user_visibility_share, 2),
        "user_avg_position": round(np.mean(user_positions), 2) if user_positions else None,
        "user_median_position": round(np.median(user_positions), 2) if user_positions else None,
        "market_total_visibility": market_total_visibility,
        "avg_market_visibility_per_keyword": round(
            np.mean(total_market_visibility), 4
        ) if total_market_visibility else 0,
        "competitor_visibility_distribution": competitor_visibility_dist,
        "visibility_concentration": calculate_visibility_concentration(competitor_metrics)
    }


def calculate_visibility_concentration(
    competitor_metrics: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Calculate visibility concentration metrics (HHI-like).
    
    Args:
        competitor_metrics: List of competitor metrics
    
    Returns:
        Concentration metrics
    """
    if not competitor_metrics:
        return {
            "herfindahl_index": 0,
            "top3_share": 0,
            "top5_share": 0,
            "interpretation": "unknown"
        }
    
    # Get visibility shares
    visibility_shares = [c['estimated_visibility_share'] for c in competitor_metrics]
    total_visibility = sum(visibility_shares)
    
    if total_visibility == 0:
        return {
            "herfindahl_index": 0,
            "top3_share": 0,
            "top5_share": 0,
            "interpretation": "no_data"
        }
    
    # Normalize to percentages
    percentages = [(v / total_visibility * 100) for v in visibility_shares]
    
    # Calculate HHI
    hhi = sum([p ** 2 for p in percentages])
    
    # Top N shares
    top3_share = sum(percentages[:3])
    top5_share = sum(percentages[:5])
    
    # Interpretation
    if hhi > 2500:
        interpretation = "highly_concentrated"
    elif hhi > 1500:
        interpretation = "moderately_concentrated"
    else:
        interpretation = "competitive"
    
    return {
        "herfindahl_index": round(hhi, 2),
        "top3_share": round(top3_share, 2),
        "top5_share": round(top5_share, 2),
        "interpretation": interpretation
    }


def calculate_market_concentration(
    competitor_metrics: List[Dict[str, Any]]
) -> str:
    """
    Calculate overall market concentration level.
    
    Args:
        competitor_metrics: List of competitor metrics
    
    Returns:
        Concentration level description
    """
    if not competitor_metrics:
        return "unknown"
    
    # Count by threat level
    threat_counts = Counter([c['threat_level'] for c in competitor_metrics])
    
    critical_count = threat_counts.get('critical', 0)
    high_count = threat_counts.get('high', 0)
    
    total_competitors = len(competitor_metrics)
    
    # If 1-3 critical/high threats dominate
    if (critical_count + high_count) <= 3 and (critical_count + high_count) > 0:
        return "oligopoly"
    
    # If many high-threat competitors
    elif (critical_count + high_count) > total_competitors * 0.3:
        return "highly_competitive"
    
    # If threats are distributed
    elif total_competitors > 20:
        return "fragmented"
    
    else:
        return "moderately_competitive"


def analyze_keyword_competition(
    serp_results: List[Dict[str, Any]],
    user_domain: str,
    gsc_keyword_data: pd.DataFrame
) -> List[Dict[str, Any]]:
    """
    Analyze competition at keyword level.
    
    Args:
        serp_results: SERP results
        user_domain: User's domain
        gsc_keyword_data: GSC keyword data
    
    Returns:
        List of keyword competition analyses
    """
    keyword_analyses = []
    
    # Create GSC lookup
    gsc_lookup = {}
    if not gsc_keyword_data.empty:
        for _, row in gsc_keyword_data.iterrows():
            gsc_lookup[row['query'].lower()] = {
                'impressions': row.get('impressions', 0),
                'clicks': row.get('clicks', 0),
                'position': row.get('position', 100),
                'ctr': row.get('ctr', 0)
            }
    
    for serp_result in serp_results:
        keyword = serp_result.get('keyword', '')
        organic_results = serp_result.get('organic_results', [])
        
        if not organic_results:
            continue
        
        # Get user's GSC data
        user_gsc = gsc_lookup.get(keyword.lower(), {})
        user_position = user_gsc.get('position', 100)
        
        # Count competitors in top 10
        top10_competitors = []
        user_in_top10 = False
        
        for result in organic_results[:10]:
            url = result.get('url', '')
            position = result.get('rank_absolute', 100)
            domain = extract_domain(url)
            
            if domain == user_domain or domain in user_domain or user_domain in domain:
                user_in_top10 = True
                continue
            
            if domain:
                top10_competitors.append({
                    'domain': domain,
                    'position': position,
                    'url': url
                })
        
        # Competition intensity
        competition_score = len(top10_competitors) / 10  # 0-1 scale
        
        # Position opportunity
        if user_in_top10:
            opportunity = "optimize"  # Already ranking, optimize to move up
        elif user_position <= 20:
            opportunity = "push_to_top10"  # Close to top 10
        elif user_position <= 50:
            opportunity = "long_term_play"  # Ranking but far down
        else:
            opportunity = "new_entry"  # Not ranking
        
        keyword_analyses.append({
            "keyword": keyword,
            "user_position": user_position,
            "user_in_top10": user_in_top10,
            "impressions": user_gsc.get('impressions', 0),
            "clicks": user_gsc.get('clicks', 0),
            "competitors_in_top10": len(top10_competitors),
            "competition_score": round(competition_score, 2),
            "opportunity": opportunity,
            "top_competitors": top10_competitors[:5]  # Top 5
        })
    
    # Sort by impressions (prioritize high-traffic keywords)
    keyword_analyses.sort(key=lambda x: x.get('impressions', 0), reverse=True)
    
    return keyword_analyses[:100]  # Return top 100
