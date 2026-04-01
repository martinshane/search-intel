import os
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import httpx
import json

# Environment variables
DATAFORSEO_LOGIN = os.getenv("DATAFORSEO_LOGIN")
DATAFORSEO_PASSWORD = os.getenv("DATAFORSEO_PASSWORD")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")


def _get_cache_key(domain: str, competitors: List[str]) -> str:
    """Generate cache key for competitor analysis data."""
    competitors_str = ",".join(sorted(competitors))
    return hashlib.md5(f"competitor_analysis:{domain}:{competitors_str}".encode()).hexdigest()


def _check_cache(supabase_client, cache_key: str) -> Optional[Dict]:
    """Check if cached data exists and is still valid (7 days)."""
    try:
        result = supabase_client.table("api_cache").select("*").eq("cache_key", cache_key).execute()
        
        if result.data and len(result.data) > 0:
            cache_entry = result.data[0]
            cached_at = datetime.fromisoformat(cache_entry["cached_at"].replace("Z", "+00:00"))
            
            if datetime.now().astimezone() - cached_at < timedelta(days=7):
                return json.loads(cache_entry["data"])
    except Exception as e:
        print(f"Cache check error: {e}")
    
    return None


def _set_cache(supabase_client, cache_key: str, data: Dict) -> None:
    """Store data in cache."""
    try:
        supabase_client.table("api_cache").upsert({
            "cache_key": cache_key,
            "data": json.dumps(data),
            "cached_at": datetime.now().isoformat()
        }).execute()
    except Exception as e:
        print(f"Cache set error: {e}")


async def _identify_competitors_from_serps(
    user_domain: str,
    top_keywords: List[Dict],
    location_code: int = 2840,
    language_code: str = "en"
) -> List[Dict]:
    """
    Identify competing domains by analyzing SERPs for user's top keywords.
    Returns list of competitor domains with overlap metrics.
    """
    url = "https://api.dataforseo.com/v3/serp/google/organic/live/advanced"
    
    auth = httpx.BasicAuth(DATAFORSEO_LOGIN, DATAFORSEO_PASSWORD)
    
    # Track domain appearances across keywords
    domain_appearances = {}
    domain_positions = {}
    domain_keywords = {}
    
    # Process top keywords (limit to 50 to manage API costs)
    keywords_to_check = top_keywords[:50]
    
    async with httpx.AsyncClient(timeout=60.0) as client:
        for keyword_data in keywords_to_check:
            keyword = keyword_data.get("query", "")
            if not keyword:
                continue
            
            payload = [{
                "keyword": keyword,
                "location_code": location_code,
                "language_code": language_code,
                "device": "desktop",
                "os": "windows",
                "depth": 100  # Check top 100 results
            }]
            
            try:
                response = await client.post(url, json=payload, auth=auth)
                response.raise_for_status()
                data = response.json()
                
                if data.get("status_code") == 20000 and data.get("tasks"):
                    task = data["tasks"][0]
                    if task.get("result") and len(task["result"]) > 0:
                        result = task["result"][0]
                        items = result.get("items", [])
                        
                        for item in items:
                            if item.get("type") == "organic":
                                domain = item.get("domain")
                                position = item.get("rank_absolute", 999)
                                
                                if domain and domain != user_domain:
                                    # Track appearances
                                    if domain not in domain_appearances:
                                        domain_appearances[domain] = 0
                                        domain_positions[domain] = []
                                        domain_keywords[domain] = []
                                    
                                    domain_appearances[domain] += 1
                                    domain_positions[domain].append(position)
                                    domain_keywords[domain].append(keyword)
                
                # Small delay to respect rate limits
                await asyncio.sleep(0.1)
                
            except Exception as e:
                print(f"Error fetching SERP for '{keyword}': {e}")
                continue
    
    # Calculate competitor metrics
    competitors = []
    total_keywords = len(keywords_to_check)
    
    for domain, appearances in domain_appearances.items():
        if appearances >= 3:  # Only include domains appearing in at least 3 keywords
            avg_position = sum(domain_positions[domain]) / len(domain_positions[domain])
            overlap_percentage = (appearances / total_keywords) * 100
            
            competitors.append({
                "domain": domain,
                "keyword_overlap_count": appearances,
                "overlap_percentage": round(overlap_percentage, 2),
                "avg_position": round(avg_position, 2),
                "shared_keywords": domain_keywords[domain][:10]  # Store up to 10 examples
            })
    
    # Sort by overlap percentage
    competitors.sort(key=lambda x: x["overlap_percentage"], reverse=True)
    
    return competitors[:10]  # Return top 10 competitors


async def _get_competitor_metrics(
    competitor_domain: str,
    user_keywords: List[str],
    location_code: int = 2840
) -> Dict:
    """
    Get detailed ranking metrics for a competitor across user's keywords.
    """
    url = "https://api.dataforseo.com/v3/serp/google/organic/live/advanced"
    auth = httpx.BasicAuth(DATAFORSEO_LOGIN, DATAFORSEO_PASSWORD)
    
    ranking_data = {
        "total_rankings": 0,
        "avg_position": 0,
        "position_1_3": 0,
        "position_4_10": 0,
        "position_11_20": 0,
        "position_21_plus": 0,
        "keyword_positions": {}
    }
    
    positions = []
    
    async with httpx.AsyncClient(timeout=60.0) as client:
        # Sample up to 20 keywords to analyze (cost management)
        sample_keywords = user_keywords[:20]
        
        for keyword in sample_keywords:
            payload = [{
                "keyword": keyword,
                "location_code": location_code,
                "language_code": "en",
                "device": "desktop",
                "os": "windows",
                "depth": 100
            }]
            
            try:
                response = await client.post(url, json=payload, auth=auth)
                response.raise_for_status()
                data = response.json()
                
                if data.get("status_code") == 20000 and data.get("tasks"):
                    task = data["tasks"][0]
                    if task.get("result") and len(task["result"]) > 0:
                        result = task["result"][0]
                        items = result.get("items", [])
                        
                        for item in items:
                            if item.get("type") == "organic" and item.get("domain") == competitor_domain:
                                position = item.get("rank_absolute", 999)
                                positions.append(position)
                                ranking_data["keyword_positions"][keyword] = position
                                ranking_data["total_rankings"] += 1
                                
                                if position <= 3:
                                    ranking_data["position_1_3"] += 1
                                elif position <= 10:
                                    ranking_data["position_4_10"] += 1
                                elif position <= 20:
                                    ranking_data["position_11_20"] += 1
                                else:
                                    ranking_data["position_21_plus"] += 1
                                break
                
                await asyncio.sleep(0.1)
                
            except Exception as e:
                print(f"Error fetching competitor data for '{keyword}': {e}")
                continue
    
    if positions:
        ranking_data["avg_position"] = round(sum(positions) / len(positions), 2)
    
    return ranking_data


def _calculate_competitive_strength(competitor_data: Dict, user_metrics: Dict) -> float:
    """
    Calculate competitive strength score (0-100) based on:
    - Keyword overlap percentage (40% weight)
    - Average position vs user (30% weight)
    - Top 3 ranking frequency (30% weight)
    """
    overlap_score = min(competitor_data.get("overlap_percentage", 0) * 2, 40)
    
    # Position comparison
    user_avg_position = user_metrics.get("avg_position", 20)
    comp_avg_position = competitor_data.get("avg_position", 50)
    
    if comp_avg_position < user_avg_position:
        position_score = 30
    else:
        # Closer positions = higher threat
        position_diff = abs(comp_avg_position - user_avg_position)
        position_score = max(0, 30 - (position_diff * 2))
    
    # Top 3 dominance
    total_rankings = competitor_data.get("total_rankings", 0)
    top3_count = competitor_data.get("position_1_3", 0)
    
    if total_rankings > 0:
        top3_rate = (top3_count / total_rankings) * 100
        top3_score = min(top3_rate * 0.3, 30)
    else:
        top3_score = 0
    
    total_score = overlap_score + position_score + top3_score
    return round(total_score, 2)


def _analyze_ranking_patterns(competitor_metrics: List[Dict]) -> Dict:
    """
    Analyze ranking patterns across competitors to identify:
    - Average competitive density per position tier
    - Difficulty scores for different keyword groups
    """
    patterns = {
        "avg_competitors_per_keyword": 0,
        "position_tier_competition": {
            "top_3": {"avg_competitors": 0, "difficulty": "high"},
            "4_10": {"avg_competitors": 0, "difficulty": "medium"},
            "11_20": {"avg_competitors": 0, "difficulty": "low"}
        },
        "competitive_density": 0  # Overall competitive landscape density
    }
    
    if not competitor_metrics:
        return patterns
    
    # Calculate average competitors
    total_competitors = len(competitor_metrics)
    patterns["avg_competitors_per_keyword"] = total_competitors
    
    # Analyze position tier competition
    top3_competitors = sum(1 for c in competitor_metrics if c.get("avg_position", 999) <= 3)
    mid_competitors = sum(1 for c in competitor_metrics if 4 <= c.get("avg_position", 999) <= 10)
    lower_competitors = sum(1 for c in competitor_metrics if 11 <= c.get("avg_position", 999) <= 20)
    
    patterns["position_tier_competition"]["top_3"]["avg_competitors"] = top3_competitors
    patterns["position_tier_competition"]["4_10"]["avg_competitors"] = mid_competitors
    patterns["position_tier_competition"]["11_20"]["avg_competitors"] = lower_competitors
    
    # Calculate competitive density (0-100 scale)
    # Based on number of strong competitors (overlap > 20%)
    strong_competitors = sum(1 for c in competitor_metrics if c.get("overlap_percentage", 0) > 20)
    patterns["competitive_density"] = min(strong_competitors * 10, 100)
    
    return patterns


def calculate(
    domain: str,
    gsc_data: Dict,
    supabase_client: Any,
    location_code: int = 2840,
    language_code: str = "en"
) -> Dict:
    """
    Module 3: Competitor Analysis
    
    Identifies top 5-10 competing domains based on keyword overlap,
    analyzes their ranking patterns, calculates competitive strength scores.
    
    Args:
        domain: User's domain
        gsc_data: GSC data including query performance
        supabase_client: Supabase client for caching
        location_code: DataForSEO location code (default: 2840 = US)
        language_code: Language code (default: "en")
    
    Returns:
        Dict containing competitor insights, metrics, and visualization data
    """
    import asyncio
    
    # Extract top keywords from GSC data
    queries = gsc_data.get("queries", [])
    
    # Sort by impressions and take top 100 for analysis
    top_queries = sorted(
        [q for q in queries if q.get("impressions", 0) > 100],
        key=lambda x: x.get("impressions", 0),
        reverse=True
    )[:100]
    
    # Check cache first
    competitor_domains = [c.get("domain") for c in gsc_data.get("competitors", [])][:10]
    cache_key = _get_cache_key(domain, competitor_domains)
    cached_data = _check_cache(supabase_client, cache_key)
    
    if cached_data:
        return cached_data
    
    # Run async competitor identification
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        competitors = loop.run_until_complete(
            _identify_competitors_from_serps(domain, top_queries, location_code, language_code)
        )
    finally:
        loop.close()
    
    if not competitors:
        return {
            "status": "insufficient_data",
            "insights": {
                "summary": "Unable to identify competitors. Ensure domain has sufficient keyword rankings.",
                "competitor_count": 0
            },
            "metrics": {},
            "visualization_data": {}
        }
    
    # Get detailed metrics for top competitors
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        detailed_competitors = []
        user_keywords = [q.get("query") for q in top_queries]
        user_avg_position = sum(q.get("position", 50) for q in top_queries) / len(top_queries)
        user_metrics = {"avg_position": user_avg_position}
        
        for competitor in competitors[:10]:
            comp_domain = competitor["domain"]
            comp_keywords = competitor.get("shared_keywords", user_keywords[:20])
            
            # Get detailed ranking metrics
            metrics = loop.run_until_complete(
                _get_competitor_metrics(comp_domain, comp_keywords, location_code)
            )
            
            # Merge metrics with basic competitor data
            detailed_competitor = {**competitor, **metrics}
            
            # Calculate competitive strength score
            strength_score = _calculate_competitive_strength(detailed_competitor, user_metrics)
            detailed_competitor["competitive_strength_score"] = strength_score
            
            # Categorize threat level
            if strength_score >= 70:
                detailed_competitor["threat_level"] = "high"
            elif strength_score >= 40:
                detailed_competitor["threat_level"] = "medium"
            else:
                detailed_competitor["threat_level"] = "low"
            
            detailed_competitors.append(detailed_competitor)
        
        # Sort by competitive strength
        detailed_competitors.sort(key=lambda x: x["competitive_strength_score"], reverse=True)
        
    finally:
        loop.close()
    
    # Analyze ranking patterns
    ranking_patterns = _analyze_ranking_patterns(detailed_competitors)
    
    # Generate insights
    top_competitor = detailed_competitors[0] if detailed_competitors else None
    high_threat_count = sum(1 for c in detailed_competitors if c.get("threat_level") == "high")
    
    insights = {
        "summary": f"Identified {len(detailed_competitors)} primary competitors. {high_threat_count} pose high competitive threat.",
        "top_competitor": {
            "domain": top_competitor["domain"] if top_competitor else "N/A",
            "overlap_percentage": top_competitor.get("overlap_percentage", 0) if top_competitor else 0,
            "avg_position": top_competitor.get("avg_position", 0) if top_competitor else 0,
            "strength_score": top_competitor.get("competitive_strength_score", 0) if top_competitor else 0
        },
        "competitive_landscape": {
            "density": ranking_patterns["competitive_density"],
            "description": f"Competitive density score of {ranking_patterns['competitive_density']}/100. " +
                         ("Highly competitive market." if ranking_patterns['competitive_density'] > 70 else
                          "Moderately competitive market." if ranking_patterns['competitive_density'] > 40 else
                          "Less competitive market with opportunities.")
        },
        "recommendations": []
    }
    
    # Generate recommendations
    if high_threat_count > 0:
        insights["recommendations"].append(
            f"Focus on differentiating from {high_threat_count} high-threat competitors through unique content angles."
        )
    
    if ranking_patterns["position_tier_competition"]["top_3"]["avg_competitors"] > 5:
        insights["recommendations"].append(
            "Top 3 positions are heavily contested. Consider targeting long-tail variations with less competition."
        )
    
    if user_avg_position > 10:
        insights["recommendations"].append(
            "Current average position suggests opportunity to improve through on-page optimization and content depth."
        )
    
    # Calculate aggregate metrics
    metrics = {
        "total_competitors_identified": len(detailed_competitors),
        "high_threat_competitors": high_threat_count,
        "medium_threat_competitors": sum(1 for c in detailed_competitors if c.get("threat_level") == "medium"),
        "low_threat_competitors": sum(1 for c in detailed_competitors if c.get("threat_level") == "low"),
        "avg_competitor_position": round(
            sum(c.get("avg_position", 0) for c in detailed_competitors) / len(detailed_competitors), 2
        ) if detailed_competitors else 0,
        "avg_keyword_overlap": round(
            sum(c.get("overlap_percentage", 0) for c in detailed_competitors) / len(detailed_competitors), 2
        ) if detailed_competitors else 0,
        "competitive_density_score": ranking_patterns["competitive_density"]
    }
    
    # Prepare visualization data
    visualization_data = {
        "competitor_overview": {
            "type": "bar_chart",
            "title": "Competitive Strength Scores",
            "data": [
                {
                    "domain": c["domain"],
                    "strength_score": c["competitive_strength_score"],
                    "threat_level": c["threat_level"]
                }
                for c in detailed_competitors[:10]
            ]
        },
        "keyword_overlap": {
            "type": "horizontal_bar",
            "title": "Keyword Overlap Percentage",
            "data": [
                {
                    "domain": c["domain"],
                    "overlap_percentage": c["overlap_percentage"]
                }
                for c in detailed_competitors[:10]
            ]
        },
        "position_comparison": {
            "type": "scatter_plot",
            "title": "Average Position vs Keyword Overlap",
            "data": [
                {
                    "domain": c["domain"],
                    "x": c.get("overlap_percentage", 0),
                    "y": c.get("avg_position", 0),
                    "size": c.get("competitive_strength_score", 0)
                }
                for c in detailed_competitors
            ],
            "user_position": user_avg_position
        },
        "ranking_distribution": {
            "type": "stacked_bar",
            "title": "Competitor Position Distribution",
            "data": [
                {
                    "domain": c["domain"],
                    "top_3": c.get("position_1_3", 0),
                    "4_10": c.get("position_4_10", 0),
                    "11_20": c.get("position_11_20", 0),
                    "21_plus": c.get("position_21_plus", 0)
                }
                for c in detailed_competitors[:10]
            ]
        },
        "competitive_density": {
            "type": "gauge",
            "title": "Market Competitive Density",
            "value": ranking_patterns["competitive_density"],
            "max": 100
        }
    }
    
    result = {
        "status": "success",
        "insights": insights,
        "metrics": metrics,
        "competitors": detailed_competitors,
        "ranking_patterns": ranking_patterns,
        "visualization_data": visualization_data,
        "generated_at": datetime.now().isoformat()
    }
    
    # Cache the results
    _set_cache(supabase_client, cache_key, result)
    
    return result