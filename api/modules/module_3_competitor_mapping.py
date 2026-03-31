"""
Module 3: Competitor Mapping

Identifies top 3-5 competitors based on keyword overlap from GSC data.
Queries DataForSEO for user's top keywords, analyzes ranking domains,
calculates overlap scores, and returns competitor list with shared keywords
and relative visibility metrics.
"""

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
    
    # Common branded patterns
    branded_patterns = [
        f"{base_domain} login",
        f"{base_domain} sign in",
        f"{base_domain} contact",
        f"{base_domain} support",
        f"{base_domain} pricing",
    ]
    
    for pattern in branded_patterns:
        if pattern in query_lower:
            return True
    
    return False


def select_top_keywords(
    gsc_keyword_data: pd.DataFrame,
    user_domain: str,
    brand_name: Optional[str] = None,
    max_keywords: int = 100,
    include_volatile: bool = True
) -> List[Dict[str, Any]]:
    """
    Select top keywords for SERP analysis.
    
    Strategy:
    1. Filter out branded queries
    2. Take top N by impressions
    3. Also include keywords with significant position changes (volatile)
    
    Args:
        gsc_keyword_data: DataFrame with columns: query, clicks, impressions, position
        user_domain: User's domain for branded filtering
        brand_name: Optional explicit brand name
        max_keywords: Maximum keywords to return
        include_volatile: Whether to include volatile keywords
    
    Returns:
        List of keyword dictionaries with query, impressions, position, clicks
    """
    if gsc_keyword_data.empty:
        logger.warning("No GSC keyword data provided")
        return []
    
    # Filter out branded queries
    non_branded = []
    for _, row in gsc_keyword_data.iterrows():
        query = row.get("query", "")
        if not is_branded_query(query, user_domain, brand_name):
            non_branded.append({
                "query": query,
                "clicks": row.get("clicks", 0),
                "impressions": row.get("impressions", 0),
                "position": row.get("position", 0),
                "ctr": row.get("ctr", 0)
            })
    
    if not non_branded:
        logger.warning("No non-branded keywords found")
        return []
    
    # Sort by impressions
    non_branded_sorted = sorted(
        non_branded,
        key=lambda x: x["impressions"],
        reverse=True
    )
    
    # Take top N by impressions
    top_by_impressions = non_branded_sorted[:max_keywords]
    
    # TODO: Add volatile keyword detection when historical data is available
    # For now, just return top by impressions
    
    logger.info(f"Selected {len(top_by_impressions)} keywords for competitor analysis")
    return top_by_impressions


def calculate_position_based_ctr(position: float) -> float:
    """
    Estimate CTR based on organic position using empirical curve.
    
    Based on industry averages:
    - Position 1: ~28%
    - Position 2: ~15%
    - Position 3: ~11%
    - Position 10: ~2.5%
    - Beyond page 1: <1%
    """
    if position <= 0:
        return 0.0
    
    if position <= 1:
        return 0.28
    elif position <= 2:
        return 0.15
    elif position <= 3:
        return 0.11
    elif position <= 4:
        return 0.08
    elif position <= 5:
        return 0.06
    elif position <= 10:
        # Exponential decay from position 5 to 10
        return 0.06 * math.exp(-0.3 * (position - 5))
    else:
        # Very low CTR beyond page 1
        return 0.01 * math.exp(-0.1 * (position - 10))


def calculate_visibility_score(position: float, impressions: float) -> float:
    """
    Calculate visibility score for a domain on a keyword.
    
    Visibility = impressions × position_weight
    Position weight favors higher rankings exponentially.
    """
    if position <= 0 or impressions <= 0:
        return 0.0
    
    # Position weight (exponential decay)
    position_weight = math.exp(-0.15 * (position - 1))
    
    # Normalize impressions (log scale to handle wide ranges)
    impression_weight = math.log1p(impressions)
    
    return position_weight * impression_weight


def determine_threat_level(
    overlap_percentage: float,
    position_advantage: float,
    keywords_shared: int,
    visibility_share: float
) -> str:
    """
    Determine competitor threat level based on multiple factors.
    
    Args:
        overlap_percentage: Percentage of user's keywords where competitor appears
        position_advantage: Average position difference (negative = competitor ranks better)
        keywords_shared: Absolute number of shared keywords
        visibility_share: Estimated visibility share of competitor
    
    Returns:
        Threat level: critical, high, medium, or low
    """
    score = 0
    
    # Overlap factor (0-40 points)
    if overlap_percentage >= 50:
        score += 40
    elif overlap_percentage >= 30:
        score += 30
    elif overlap_percentage >= 15:
        score += 20
    else:
        score += 10
    
    # Position advantage factor (0-30 points)
    if position_advantage <= -5:  # Competitor ranks 5+ positions better
        score += 30
    elif position_advantage <= -2:
        score += 20
    elif position_advantage <= 0:
        score += 10
    
    # Shared keywords volume factor (0-20 points)
    if keywords_shared >= 50:
        score += 20
    elif keywords_shared >= 25:
        score += 15
    elif keywords_shared >= 10:
        score += 10
    else:
        score += 5
    
    # Visibility share factor (0-10 points)
    if visibility_share >= 0.3:
        score += 10
    elif visibility_share >= 0.15:
        score += 7
    elif visibility_share >= 0.05:
        score += 5
    else:
        score += 2
    
    # Determine level based on total score
    if score >= 75:
        return "critical"
    elif score >= 55:
        return "high"
    elif score >= 35:
        return "medium"
    else:
        return "low"


async def analyze_competitor_mapping(
    gsc_keyword_data: pd.DataFrame,
    user_domain: str,
    dataforseo_client: DataForSEOClient,
    brand_name: Optional[str] = None,
    max_keywords: int = 100,
    min_competitor_overlap: int = 5,
    top_n_competitors: int = 5,
    location_code: int = 2840,  # USA
    language_code: str = "en"
) -> Dict[str, Any]:
    """
    Identify top competitors based on keyword overlap and visibility.
    
    Args:
        gsc_keyword_data: DataFrame with GSC keyword performance data
        user_domain: User's domain
        dataforseo_client: DataForSEO API client
        brand_name: Optional brand name for filtering
        max_keywords: Maximum keywords to analyze
        min_competitor_overlap: Minimum shared keywords to be considered a competitor
        top_n_competitors: Number of top competitors to return
        location_code: DataForSEO location code
        language_code: Language code
    
    Returns:
        Dictionary with competitor analysis results
    """
    logger.info("Starting competitor mapping analysis")
    
    try:
        # Select keywords for analysis
        keywords = select_top_keywords(
            gsc_keyword_data,
            user_domain,
            brand_name,
            max_keywords
        )
        
        if not keywords:
            return {
                "competitors": [],
                "total_keywords_analyzed": 0,
                "summary": {
                    "error": "No non-branded keywords found for analysis"
                }
            }
        
        # Fetch SERP data for selected keywords
        logger.info(f"Fetching SERP data for {len(keywords)} keywords")
        serp_results = await dataforseo_client.get_serp_results(
            keywords=[k["query"] for k in keywords],
            location_code=location_code,
            language_code=language_code
        )
        
        if not serp_results:
            logger.error("No SERP results returned from DataForSEO")
            return {
                "competitors": [],
                "total_keywords_analyzed": 0,
                "summary": {
                    "error": "Failed to fetch SERP data"
                }
            }
        
        # Build keyword lookup for impressions and user position
        keyword_lookup = {k["query"]: k for k in keywords}
        
        # Track competitor appearances
        competitor_data = defaultdict(lambda: {
            "positions": [],
            "keywords": [],
            "visibility_scores": []
        })
        
        user_positions_by_keyword = {}
        total_keywords_analyzed = 0
        keywords_with_serp_data = 0
        
        # Process SERP results
        for serp_result in serp_results:
            keyword = serp_result.get("keyword", "")
            if not keyword or keyword not in keyword_lookup:
                continue
            
            keyword_data = keyword_lookup[keyword]
            impressions = keyword_data.get("impressions", 0)
            user_position = keyword_data.get("position", 0)
            
            total_keywords_analyzed += 1
            
            organic_results = serp_result.get("organic_results", [])
            if not organic_results:
                continue
            
            keywords_with_serp_data += 1
            user_positions_by_keyword[keyword] = user_position
            
            # Extract domains and positions from top 20 results
            for result in organic_results[:20]:
                url = result.get("url", "")
                position = result.get("rank_absolute", 0)
                
                if not url or position <= 0:
                    continue
                
                domain = extract_domain(url)
                
                # Skip user's own domain
                if domain == user_domain:
                    continue
                
                # Skip empty domains
                if not domain:
                    continue
                
                # Calculate visibility for this keyword
                visibility = calculate_visibility_score(position, impressions)
                
                # Record competitor data
                competitor_data[domain]["positions"].append(position)
                competitor_data[domain]["keywords"].append({
                    "query": keyword,
                    "position": position,
                    "impressions": impressions,
                    "user_position": user_position,
                    "visibility": visibility
                })
                competitor_data[domain]["visibility_scores"].append(visibility)
        
        logger.info(f"Analyzed {total_keywords_analyzed} keywords, {keywords_with_serp_data} with SERP data")
        logger.info(f"Found {len(competitor_data)} potential competitors")
        
        # Filter and score competitors
        competitors = []
        total_visibility = sum(
            sum(data["visibility_scores"])
            for data in competitor_data.values()
        )
        
        for domain, data in competitor_data.items():
            keywords_shared = len(data["keywords"])
            
            # Filter by minimum overlap
            if keywords_shared < min_competitor_overlap:
                continue
            
            # Calculate metrics
            positions = data["positions"]
            avg_position = np.mean(positions)
            median_position = np.median(positions)
            
            # Calculate user's average position for shared keywords
            user_positions = [
                user_positions_by_keyword.get(kw["query"], 0)
                for kw in data["keywords"]
            ]
            avg_user_position = np.mean([p for p in user_positions if p > 0])
            
            # Position advantage (negative if competitor ranks better)
            position_advantage = avg_user_position - avg_position
            
            # Visibility metrics
            competitor_visibility = sum(data["visibility_scores"])
            visibility_share = competitor_visibility / total_visibility if total_visibility > 0 else 0
            
            # Overlap percentage
            overlap_percentage = (keywords_shared / total_keywords_analyzed) * 100
            
            # Determine threat level
            threat_level = determine_threat_level(
                overlap_percentage,
                position_advantage,
                keywords_shared,
                visibility_share
            )
            
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
                shared_keywords=data["keywords"]
            )
            
            competitors.append(competitor)
        
        # Sort by threat level priority, then visibility share
        threat_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
        competitors.sort(
            key=lambda c: (threat_order.get(c.threat_level, 4), -c.estimated_visibility_share)
        )
        
        # Take top N
        top_competitors = competitors[:top_n_competitors]
        
        # Calculate summary statistics
        if top_competitors:
            total_shared_keywords = sum(c.keywords_shared for c in top_competitors)
            avg_overlap = np.mean([c.overlap_percentage for c in top_competitors])
            
            threat_distribution = Counter(c.threat_level for c in competitors)
            
            summary = {
                "total_competitors_found": len(competitors),
                "top_competitors_analyzed": len(top_competitors),
                "total_shared_keyword_instances": total_shared_keywords,
                "avg_overlap_percentage": round(avg_overlap, 2),
                "threat_distribution": dict(threat_distribution),
                "keywords_analyzed": total_keywords_analyzed,
                "keywords_with_serp_data": keywords_with_serp_data
            }
        else:
            summary = {
                "total_competitors_found": 0,
                "message": "No significant competitors found based on current keyword set"
            }
        
        result = {
            "competitors": [c.to_dict() for c in top_competitors],
            "total_keywords_analyzed": total_keywords_analyzed,
            "summary": summary,
            "analysis_metadata": {
                "user_domain": user_domain,
                "max_keywords_requested": max_keywords,
                "min_overlap_threshold": min_competitor_overlap,
                "location_code": location_code,
                "language_code": language_code,
                "timestamp": datetime.utcnow().isoformat()
            }
        }
        
        logger.info(f"Competitor mapping complete: {len(top_competitors)} top competitors identified")
        return result
        
    except Exception as e:
        logger.error(f"Error in competitor mapping analysis: {str(e)}", exc_info=True)
        return {
            "competitors": [],
            "total_keywords_analyzed": 0,
            "summary": {
                "error": f"Analysis failed: {str(e)}"
            }
        }


def get_competitor_keyword_details(
    competitor_domain: str,
    competitor_analysis: Dict[str, Any],
    limit: int = 50
) -> List[Dict[str, Any]]:
    """
    Get detailed keyword overlap for a specific competitor.
    
    Args:
        competitor_domain: Domain of the competitor
        competitor_analysis: Full competitor analysis result
        limit: Maximum keywords to return
    
    Returns:
        List of shared keywords with details
    """
    for competitor in competitor_analysis.get("competitors", []):
        if competitor.get("domain") == competitor_domain:
            shared_keywords = competitor.get("top_shared_keywords", [])
            
            # Sort by impressions descending
            sorted_keywords = sorted(
                shared_keywords,
                key=lambda x: x.get("impressions", 0),
                reverse=True
            )
            
            return sorted_keywords[:limit]
    
    return []


def get_competitive_gap_analysis(
    competitor_analysis: Dict[str, Any],
    user_domain: str
) -> Dict[str, Any]:
    """
    Analyze competitive gaps and opportunities.
    
    Args:
        competitor_analysis: Full competitor analysis result
        user_domain: User's domain
    
    Returns:
        Gap analysis with opportunities and threats
    """
    competitors = competitor_analysis.get("competitors", [])
    
    if not competitors:
        return {"opportunities": [], "threats": []}
    
    opportunities = []
    threats = []
    
    for competitor in competitors:
        domain = competitor.get("domain", "")
        position_advantage = competitor.get("position_advantage", 0)
        keywords_shared = competitor.get("keywords_shared", 0)
        threat_level = competitor.get("threat_level", "low")
        
        # Opportunities: where user ranks better
        if position_advantage < -2:  # User ranks 2+ positions better
            opportunities.append({
                "type": "dominance",
                "competitor": domain,
                "keywords_count": keywords_shared,
                "avg_position_advantage": abs(position_advantage),
                "recommendation": f"Maintain and strengthen position against {domain}"
            })
        
        # Threats: where competitor ranks significantly better
        if position_advantage > 3:  # Competitor ranks 3+ positions better
            threats.append({
                "type": "underperformance",
                "competitor": domain,
                "keywords_count": keywords_shared,
                "avg_position_disadvantage": position_advantage,
                "threat_level": threat_level,
                "recommendation": f"Analyze and learn from {domain}'s content strategy"
            })
    
    return {
        "opportunities": opportunities,
        "threats": threats
    }
