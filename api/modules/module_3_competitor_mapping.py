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
        user_domain.replace(".", " ")
    ]
    
    for pattern in branded_patterns:
        if pattern in query_lower:
            return True
    
    return False


def calculate_position_based_ctr(position: float) -> float:
    """
    Estimate CTR based on position using industry average CTR curve.
    
    Args:
        position: SERP position
    
    Returns:
        Estimated CTR (0-1)
    """
    # Industry average CTR by position (approximate)
    # Source: Various CTR studies
    if position <= 0:
        return 0.0
    elif position <= 1:
        return 0.316
    elif position <= 2:
        return 0.158
    elif position <= 3:
        return 0.107
    elif position <= 4:
        return 0.074
    elif position <= 5:
        return 0.059
    elif position <= 6:
        return 0.048
    elif position <= 7:
        return 0.040
    elif position <= 8:
        return 0.034
    elif position <= 9:
        return 0.029
    elif position <= 10:
        return 0.025
    elif position <= 20:
        # Exponential decay for positions 11-20
        return 0.025 * math.exp(-0.15 * (position - 10))
    else:
        # Very low CTR for positions beyond 20
        return 0.005 * math.exp(-0.1 * (position - 20))


def calculate_visibility_score(position: float, search_volume: float) -> float:
    """
    Calculate visibility score for a keyword ranking.
    
    Visibility = CTR × Search Volume
    
    Args:
        position: SERP position
        search_volume: Monthly search volume (or impressions as proxy)
    
    Returns:
        Visibility score
    """
    ctr = calculate_position_based_ctr(position)
    return ctr * search_volume


def determine_threat_level(
    overlap_pct: float,
    position_advantage: float,
    keywords_shared: int
) -> str:
    """
    Determine threat level of a competitor.
    
    Args:
        overlap_pct: Percentage of keywords shared
        position_advantage: Average position advantage (negative if competitor ranks better)
        keywords_shared: Number of keywords shared
    
    Returns:
        Threat level: "low", "medium", "high", or "critical"
    """
    # Normalize position advantage (-20 to +20 scale to 0-1)
    position_score = max(0, min(1, (-position_advantage + 10) / 20))
    
    # Normalize overlap (0-100 to 0-1)
    overlap_score = overlap_pct / 100
    
    # Normalize keyword count (logarithmic scale, 1-100+ keywords)
    keyword_score = min(1, math.log(keywords_shared + 1) / math.log(100))
    
    # Weighted threat score
    threat_score = (
        position_score * 0.4 +  # How much better they rank
        overlap_score * 0.35 +   # How much overlap
        keyword_score * 0.25     # How many keywords
    )
    
    if threat_score >= 0.7:
        return "critical"
    elif threat_score >= 0.5:
        return "high"
    elif threat_score >= 0.3:
        return "medium"
    else:
        return "low"


def select_top_keywords_for_analysis(
    gsc_data: pd.DataFrame,
    user_domain: str,
    max_keywords: int = 100,
    brand_name: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Select top non-branded keywords from GSC data for competitor analysis.
    
    Args:
        gsc_data: GSC query data DataFrame
        user_domain: User's domain
        max_keywords: Maximum number of keywords to analyze
        brand_name: Optional brand name for branded query filtering
    
    Returns:
        List of keyword dictionaries with query, impressions, position
    """
    if gsc_data.empty:
        logger.warning("Empty GSC data provided for keyword selection")
        return []
    
    # Ensure required columns exist
    required_cols = ['query', 'impressions', 'position']
    if not all(col in gsc_data.columns for col in required_cols):
        logger.error(f"GSC data missing required columns. Has: {gsc_data.columns.tolist()}")
        return []
    
    # Filter out branded queries
    non_branded = gsc_data[
        ~gsc_data['query'].apply(lambda q: is_branded_query(q, user_domain, brand_name))
    ].copy()
    
    logger.info(f"Filtered {len(gsc_data)} queries to {len(non_branded)} non-branded queries")
    
    if non_branded.empty:
        logger.warning("No non-branded queries found")
        return []
    
    # Sort by impressions descending
    non_branded = non_branded.sort_values('impressions', ascending=False)
    
    # Take top N
    top_keywords = non_branded.head(max_keywords)
    
    # Convert to list of dicts
    keywords = []
    for _, row in top_keywords.iterrows():
        keywords.append({
            'query': row['query'],
            'impressions': int(row['impressions']),
            'position': float(row['position']),
            'clicks': int(row.get('clicks', 0)),
            'ctr': float(row.get('ctr', 0))
        })
    
    logger.info(f"Selected {len(keywords)} keywords for competitor analysis")
    return keywords


async def fetch_serp_data_for_keywords(
    keywords: List[Dict[str, Any]],
    dataforseo_client: DataForSEOClient,
    location_code: int = 2840,  # United States
    language_code: str = "en"
) -> Dict[str, Any]:
    """
    Fetch SERP data for keywords via DataForSEO.
    
    Args:
        keywords: List of keyword dictionaries
        dataforseo_client: DataForSEO client instance
        location_code: Location code for SERP
        language_code: Language code for SERP
    
    Returns:
        Dictionary mapping query -> SERP data
    """
    if not keywords:
        logger.warning("No keywords provided for SERP fetching")
        return {}
    
    logger.info(f"Fetching SERP data for {len(keywords)} keywords")
    
    serp_results = {}
    batch_size = 10  # Process in batches to avoid rate limits
    
    for i in range(0, len(keywords), batch_size):
        batch = keywords[i:i + batch_size]
        
        for kw in batch:
            query = kw['query']
            
            try:
                # Fetch live SERP data
                serp_data = await dataforseo_client.get_serp_results(
                    keyword=query,
                    location_code=location_code,
                    language_code=language_code
                )
                
                if serp_data and 'items' in serp_data:
                    serp_results[query] = serp_data
                    logger.debug(f"Fetched SERP data for: {query}")
                else:
                    logger.warning(f"No SERP data returned for: {query}")
                    
            except Exception as e:
                logger.error(f"Error fetching SERP data for '{query}': {str(e)}")
                continue
    
    logger.info(f"Successfully fetched SERP data for {len(serp_results)} keywords")
    return serp_results


def extract_competitors_from_serp(
    serp_data: Dict[str, Any],
    user_domain: str,
    gsc_keywords: List[Dict[str, Any]]
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Extract competitor domains and their positions from SERP data.
    
    Args:
        serp_data: Dictionary mapping query -> SERP results
        user_domain: User's domain to exclude
        gsc_keywords: Original keyword list with GSC metrics
    
    Returns:
        Dictionary mapping competitor domain -> list of keyword ranking data
    """
    competitor_rankings = defaultdict(list)
    
    # Create lookup for GSC data
    gsc_lookup = {kw['query']: kw for kw in gsc_keywords}
    
    for query, serp in serp_data.items():
        if 'items' not in serp:
            continue
        
        gsc_data = gsc_lookup.get(query, {})
        user_position = gsc_data.get('position', 0)
        impressions = gsc_data.get('impressions', 0)
        
        # Extract organic results
        items = serp.get('items', [])
        if not items:
            continue
        
        # Get organic results (first item usually contains them)
        organic_results = []
        for item in items:
            if item.get('type') == 'organic':
                organic_results.append(item)
        
        # Parse rankings
        for result in organic_results[:20]:  # Top 20 positions
            url = result.get('url', '')
            domain = extract_domain(url)
            position = result.get('rank_absolute', 0)
            
            # Skip if it's the user's domain
            if domain == user_domain:
                continue
            
            # Skip invalid domains
            if not domain or domain == '':
                continue
            
            competitor_rankings[domain].append({
                'query': query,
                'position': position,
                'user_position': user_position,
                'impressions': impressions,
                'url': url,
                'title': result.get('title', ''),
                'description': result.get('description', '')
            })
    
    logger.info(f"Extracted {len(competitor_rankings)} competitors from SERP data")
    return dict(competitor_rankings)


def calculate_competitor_metrics(
    competitor_rankings: Dict[str, List[Dict[str, Any]]],
    total_keywords_analyzed: int
) -> List[CompetitorMetrics]:
    """
    Calculate detailed metrics for each competitor.
    
    Args:
        competitor_rankings: Dictionary mapping domain -> keyword rankings
        total_keywords_analyzed: Total number of keywords analyzed
    
    Returns:
        List of CompetitorMetrics objects
    """
    competitors = []
    
    for domain, rankings in competitor_rankings.items():
        if not rankings:
            continue
        
        # Basic counts
        keywords_shared = len(rankings)
        overlap_percentage = (keywords_shared / total_keywords_analyzed * 100) if total_keywords_analyzed > 0 else 0
        
        # Position metrics
        positions = [r['position'] for r in rankings if r['position'] > 0]
        user_positions = [r['user_position'] for r in rankings if r['user_position'] > 0]
        
        if not positions:
            continue
        
        avg_position = np.mean(positions)
        median_position = np.median(positions)
        avg_user_position = np.mean(user_positions) if user_positions else 0
        
        # Position advantage (negative if competitor ranks better)
        position_advantage = avg_user_position - avg_position
        
        # Calculate visibility share
        total_visibility = 0
        competitor_visibility = 0
        
        for ranking in rankings:
            impressions = ranking.get('impressions', 0)
            comp_pos = ranking.get('position', 0)
            user_pos = ranking.get('user_position', 0)
            
            if impressions > 0 and comp_pos > 0:
                # Total potential visibility for this keyword
                total_vis = impressions
                total_visibility += total_vis
                
                # Competitor's visibility
                comp_vis = calculate_visibility_score(comp_pos, impressions)
                competitor_visibility += comp_vis
        
        visibility_share = (competitor_visibility / total_visibility) if total_visibility > 0 else 0
        
        # Determine threat level
        threat_level = determine_threat_level(
            overlap_percentage,
            position_advantage,
            keywords_shared
        )
        
        # Create metrics object
        competitor = CompetitorMetrics(
            domain=domain,
            keywords_shared=keywords_shared,
            overlap_percentage=overlap_percentage,
            avg_position=avg_position,
            median_position=median_position,
            avg_user_position_for_shared=avg_user_position,
            position_advantage=position_advantage,
            estimated_visibility_share=visibility_share,
            threat_level=threat_level,
            shared_keywords=rankings
        )
        
        competitors.append(competitor)
    
    # Sort by overlap percentage descending
    competitors.sort(key=lambda c: c.overlap_percentage, reverse=True)
    
    logger.info(f"Calculated metrics for {len(competitors)} competitors")
    return competitors


def calculate_aggregate_metrics(
    competitors: List[CompetitorMetrics],
    user_domain: str,
    total_keywords: int
) -> Dict[str, Any]:
    """
    Calculate aggregate competitive metrics.
    
    Args:
        competitors: List of competitor metrics
        user_domain: User's domain
        total_keywords: Total keywords analyzed
    
    Returns:
        Dictionary of aggregate metrics
    """
    if not competitors:
        return {
            "total_competitors_found": 0,
            "avg_competitors_per_keyword": 0,
            "competitive_intensity": "low",
            "user_avg_advantage": 0,
            "market_concentration": 0
        }
    
    # Count competitors per threat level
    threat_counts = Counter(c.threat_level for c in competitors)
    
    # Calculate average number of competitors per keyword
    total_appearances = sum(c.keywords_shared for c in competitors)
    avg_competitors_per_kw = total_appearances / total_keywords if total_keywords > 0 else 0
    
    # Competitive intensity classification
    if avg_competitors_per_kw >= 8:
        competitive_intensity = "very_high"
    elif avg_competitors_per_kw >= 6:
        competitive_intensity = "high"
    elif avg_competitors_per_kw >= 4:
        competitive_intensity = "medium"
    else:
        competitive_intensity = "low"
    
    # Average position advantage across all competitors
    avg_advantage = np.mean([c.position_advantage for c in competitors])
    
    # Market concentration (top 3 competitors' share)
    top_3_share = sum(c.overlap_percentage for c in competitors[:3])
    
    return {
        "total_competitors_found": len(competitors),
        "competitors_by_threat": dict(threat_counts),
        "avg_competitors_per_keyword": round(avg_competitors_per_kw, 2),
        "competitive_intensity": competitive_intensity,
        "user_avg_position_advantage": round(avg_advantage, 2),
        "market_concentration_top3": round(top_3_share, 2),
        "primary_competitors": [c.domain for c in competitors[:5]]  # Top 5
    }


async def analyze_competitor_mapping(
    gsc_data: pd.DataFrame,
    user_domain: str,
    dataforseo_client: DataForSEOClient,
    max_keywords: int = 100,
    brand_name: Optional[str] = None,
    location_code: int = 2840,
    language_code: str = "en"
) -> Dict[str, Any]:
    """
    Main function for Module 3: Competitor Mapping Analysis.
    
    Identifies top competitors from GSC data by:
    1. Selecting top non-branded keywords
    2. Fetching live SERP data via DataForSEO
    3. Extracting competing domains and their positions
    4. Calculating competitive metrics (overlap, position delta, visibility share)
    5. Ranking competitors by threat level
    
    Args:
        gsc_data: DataFrame with GSC query data (query, impressions, position, clicks, ctr)
        user_domain: User's domain
        dataforseo_client: DataForSEO API client
        max_keywords: Maximum number of keywords to analyze
        brand_name: Optional brand name for branded query filtering
        location_code: DataForSEO location code
        language_code: DataForSEO language code
    
    Returns:
        Dictionary containing:
        - competitors: List of top competitors with detailed metrics
        - aggregate_metrics: Overall competitive landscape metrics
        - keywords_analyzed: Number of keywords analyzed
        - serp_data_coverage: Percentage of keywords with SERP data
    """
    try:
        logger.info(f"Starting competitor mapping analysis for {user_domain}")
        
        # Step 1: Select top keywords for analysis
        keywords = select_top_keywords_for_analysis(
            gsc_data=gsc_data,
            user_domain=user_domain,
            max_keywords=max_keywords,
            brand_name=brand_name
        )
        
        if not keywords:
            logger.warning("No keywords selected for analysis")
            return {
                "competitors": [],
                "aggregate_metrics": {
                    "total_competitors_found": 0,
                    "competitive_intensity": "unknown",
                    "error": "No non-branded keywords found"
                },
                "keywords_analyzed": 0,
                "serp_data_coverage": 0
            }
        
        # Step 2: Fetch SERP data
        serp_data = await fetch_serp_data_for_keywords(
            keywords=keywords,
            dataforseo_client=dataforseo_client,
            location_code=location_code,
            language_code=language_code
        )
        
        serp_coverage = (len(serp_data) / len(keywords) * 100) if keywords else 0
        logger.info(f"SERP data coverage: {serp_coverage:.1f}%")
        
        if not serp_data:
            logger.warning("No SERP data retrieved")
            return {
                "competitors": [],
                "aggregate_metrics": {
                    "total_competitors_found": 0,
                    "competitive_intensity": "unknown",
                    "error": "Failed to retrieve SERP data"
                },
                "keywords_analyzed": len(keywords),
                "serp_data_coverage": 0
            }
        
        # Step 3: Extract competitors from SERP data
        competitor_rankings = extract_competitors_from_serp(
            serp_data=serp_data,
            user_domain=user_domain,
            gsc_keywords=keywords
        )
        
        if not competitor_rankings:
            logger.warning("No competitors found in SERP data")
            return {
                "competitors": [],
                "aggregate_metrics": {
                    "total_competitors_found": 0,
                    "competitive_intensity": "low",
                    "message": "No competitors found in analyzed SERPs"
                },
                "keywords_analyzed": len(keywords),
                "serp_data_coverage": serp_coverage
            }
        
        # Step 4: Calculate competitor metrics
        competitors = calculate_competitor_metrics(
            competitor_rankings=competitor_rankings,
            total_keywords_analyzed=len(keywords)
        )
        
        # Step 5: Calculate aggregate metrics
        aggregate_metrics = calculate_aggregate_metrics(
            competitors=competitors,
            user_domain=user_domain,
            total_keywords=len(keywords)
        )
        
        # Prepare final result
        result = {
            "competitors": [c.to_dict() for c in competitors[:10]],  # Top 10 competitors
            "aggregate_metrics": aggregate_metrics,
            "keywords_analyzed": len(keywords),
            "serp_data_coverage": round(serp_coverage, 2),
            "analysis_timestamp": datetime.utcnow().isoformat(),
            "total_competitors_identified": len(competitors)
        }
        
        logger.info(f"Competitor mapping analysis complete. Found {len(competitors)} competitors")
        return result
        
    except Exception as e:
        logger.error(f"Error in competitor mapping analysis: {str(e)}", exc_info=True)
        return {
            "competitors": [],
            "aggregate_metrics": {
                "total_competitors_found": 0,
                "competitive_intensity": "unknown",
                "error": str(e)
            },
            "keywords_analyzed": 0,
            "serp_data_coverage": 0
        }