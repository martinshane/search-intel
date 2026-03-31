"""
Module 3: Keyword Expansion & Opportunity
Identifies top queries from GSC, expands with related keywords via DataForSEO,
scores opportunities, and returns top 50 actionable keyword targets.
"""

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


def _get_cache_key(domain: str, query: str) -> str:
    """Generate cache key for keyword expansion data."""
    return hashlib.md5(f"keyword_expansion:{domain}:{query}".encode()).hexdigest()


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


async def _get_related_keywords_dataforseo(
    query: str,
    location_code: int = 2840,  # US
    language_code: str = "en",
    min_volume: int = 500
) -> List[Dict]:
    """
    Get related keywords from DataForSEO Keywords Data API.
    Returns keywords with search volume >= min_volume.
    """
    url = "https://api.dataforseo.com/v3/keywords_data/google_ads/search_volume/live"
    
    auth = httpx.BasicAuth(DATAFORSEO_LOGIN, DATAFORSEO_PASSWORD)
    
    # First, get related keywords suggestions
    suggestions_url = "https://api.dataforseo.com/v3/keywords_data/google_ads/keywords_for_keywords/live"
    
    payload = [{
        "keywords": [query],
        "location_code": location_code,
        "language_code": language_code,
        "search_partners": False,
        "include_adult_keywords": False,
        "sort_by": "search_volume",
        "date_from": (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"),
        "date_to": datetime.now().strftime("%Y-%m-%d")
    }]
    
    related_keywords = []
    
    async with httpx.AsyncClient(timeout=60.0) as client:
        try:
            # Get keyword suggestions
            response = await client.post(
                suggestions_url,
                json=payload,
                auth=auth,
                headers={"Content-Type": "application/json"}
            )
            response.raise_for_status()
            data = response.json()
            
            if data.get("status_code") == 20000 and data.get("tasks"):
                task = data["tasks"][0]
                if task.get("result") and len(task["result"]) > 0:
                    items = task["result"][0].get("items", [])
                    
                    # Filter by minimum volume and collect keywords
                    for item in items:
                        keyword_data = item.get("keyword_data", {})
                        keyword_info = keyword_data.get("keyword_info", {})
                        search_volume = keyword_info.get("search_volume", 0)
                        
                        if search_volume >= min_volume:
                            keyword = item.get("keyword", "")
                            competition = keyword_info.get("competition", 0)
                            
                            # Map competition (0-1) to difficulty (0-100)
                            difficulty = int(competition * 100) if competition else 50
                            
                            related_keywords.append({
                                "keyword": keyword,
                                "search_volume": search_volume,
                                "difficulty": difficulty,
                                "competition": competition,
                                "cpc": keyword_info.get("cpc", 0)
                            })
            
            # Sort by search volume and limit to top results
            related_keywords.sort(key=lambda x: x["search_volume"], reverse=True)
            return related_keywords[:100]  # Top 100 related keywords per seed
            
        except httpx.HTTPError as e:
            print(f"DataForSEO API error for query '{query}': {e}")
            return []
        except Exception as e:
            print(f"Unexpected error fetching keywords for '{query}': {e}")
            return []


def _classify_intent(keyword: str, serp_features: Optional[List[str]] = None) -> str:
    """
    Classify search intent based on keyword patterns.
    Returns: informational, commercial, transactional, navigational
    """
    keyword_lower = keyword.lower()
    
    # Transactional signals
    transactional_terms = [
        "buy", "purchase", "order", "shop", "price", "cost", "cheap",
        "discount", "deal", "coupon", "sale", "affordable"
    ]
    
    # Commercial investigation signals
    commercial_terms = [
        "best", "top", "review", "comparison", "vs", "versus", "alternative",
        "compare", "recommendation", "rated", "ranking"
    ]
    
    # Informational signals
    informational_terms = [
        "how to", "what is", "why", "guide", "tutorial", "learn",
        "meaning", "definition", "explained", "tips", "ways to"
    ]
    
    # Navigational signals
    navigational_terms = [
        "login", "sign in", "app", "download", "official", "site"
    ]
    
    # Check patterns
    if any(term in keyword_lower for term in transactional_terms):
        return "transactional"
    elif any(term in keyword_lower for term in commercial_terms):
        return "commercial"
    elif any(term in keyword_lower for term in informational_terms):
        return "informational"
    elif any(term in keyword_lower for term in navigational_terms):
        return "navigational"
    
    # Default based on length and question words
    if keyword_lower.startswith(("how", "what", "why", "when", "where", "who")):
        return "informational"
    elif len(keyword.split()) <= 2:
        return "navigational"
    else:
        return "commercial"


def _calculate_opportunity_score(
    search_volume: int,
    difficulty: int,
    current_position: Optional[float] = None
) -> float:
    """
    Calculate opportunity score using the formula:
    volume * (100 - difficulty) * (1 - current_position/100)
    
    If current_position is None (not ranking), use position 100 for calculation.
    """
    if current_position is None:
        current_position = 100.0
    
    # Ensure current_position is at least 1
    current_position = max(1.0, current_position)
    
    # Calculate score
    volume_factor = search_volume
    difficulty_factor = (100 - difficulty)
    position_factor = (1 - (current_position / 100))
    
    score = volume_factor * difficulty_factor * position_factor
    
    return round(score, 2)


async def analyze_keyword_opportunities(
    supabase_client,
    gsc_data: Dict,
    domain: str,
    location_code: int = 2840
) -> Dict[str, Any]:
    """
    Main analysis function for keyword expansion and opportunity identification.
    
    Args:
        supabase_client: Supabase client instance
        gsc_data: GSC performance data with queries and their metrics
        domain: The domain being analyzed
        location_code: Geographic location code for DataForSEO (default: US)
    
    Returns:
        Dictionary with top 50 keyword opportunities and summary metrics
    """
    
    # Extract top 20 queries from GSC data
    queries = gsc_data.get("queries", [])
    
    # Filter out branded queries (fuzzy match on domain)
    domain_parts = domain.replace("www.", "").replace(".com", "").replace(".org", "").replace(".net", "").split(".")
    brand_terms = set(part.lower() for part in domain_parts if len(part) > 2)
    
    non_branded_queries = []
    for query in queries:
        query_text = query.get("query", "").lower()
        # Skip if query contains brand terms
        if not any(brand_term in query_text for brand_term in brand_terms):
            non_branded_queries.append(query)
    
    # Sort by impressions and take top 20
    non_branded_queries.sort(key=lambda x: x.get("impressions", 0), reverse=True)
    top_queries = non_branded_queries[:20]
    
    print(f"Analyzing top {len(top_queries)} non-branded queries for keyword expansion...")
    
    # Track all keyword opportunities
    all_opportunities = []
    processed_keywords = set()
    
    # Process each top query
    for query_data in top_queries:
        query_text = query_data.get("query", "")
        current_position = query_data.get("position", None)
        current_clicks = query_data.get("clicks", 0)
        
        # Check cache first
        cache_key = _get_cache_key(domain, query_text)
        cached_data = _check_cache(supabase_client, cache_key)
        
        if cached_data:
            print(f"Using cached data for query: {query_text}")
            related_keywords = cached_data
        else:
            print(f"Fetching related keywords for: {query_text}")
            related_keywords = await _get_related_keywords_dataforseo(
                query=query_text,
                location_code=location_code,
                min_volume=500
            )
            
            # Cache the results
            if related_keywords:
                _set_cache(supabase_client, cache_key, related_keywords)
        
        # Process related keywords
        for kw_data in related_keywords:
            keyword = kw_data["keyword"]
            
            # Skip if already processed
            if keyword.lower() in processed_keywords:
                continue
            
            processed_keywords.add(keyword.lower())
            
            search_volume = kw_data["search_volume"]
            difficulty = kw_data["difficulty"]
            
            # Check if we already rank for this keyword
            ranking_position = None
            if keyword.lower() == query_text.lower():
                ranking_position = current_position
            
            # Calculate opportunity score
            opportunity_score = _calculate_opportunity_score(
                search_volume=search_volume,
                difficulty=difficulty,
                current_position=ranking_position
            )
            
            # Classify intent
            intent = _classify_intent(keyword)
            
            all_opportunities.append({
                "query": keyword,
                "search_volume": search_volume,
                "difficulty": difficulty,
                "current_position": ranking_position,
                "opportunity_score": opportunity_score,
                "intent_category": intent,
                "cpc": kw_data.get("cpc", 0),
                "seed_query": query_text
            })
    
    # Sort by opportunity score and take top 50
    all_opportunities.sort(key=lambda x: x["opportunity_score"], reverse=True)
    top_opportunities = all_opportunities[:50]
    
    # Calculate summary metrics
    total_opportunity_volume = sum(kw["search_volume"] for kw in top_opportunities)
    avg_difficulty = sum(kw["difficulty"] for kw in top_opportunities) / len(top_opportunities) if top_opportunities else 0
    
    # Count by intent
    intent_breakdown = {}
    for kw in top_opportunities:
        intent = kw["intent_category"]
        intent_breakdown[intent] = intent_breakdown.get(intent, 0) + 1
    
    # Identify quick wins (high volume, low difficulty, not currently ranking)
    quick_wins = [
        kw for kw in top_opportunities
        if kw["difficulty"] < 40
        and kw["search_volume"] > 1000
        and kw["current_position"] is None
    ]
    
    # Identify optimization targets (already ranking but room to improve)
    optimization_targets = [
        kw for kw in top_opportunities
        if kw["current_position"] is not None
        and kw["current_position"] > 10
        and kw["search_volume"] > 500
    ]
    
    return {
        "top_opportunities": top_opportunities,
        "summary": {
            "total_keywords_analyzed": len(all_opportunities),
            "top_opportunities_count": len(top_opportunities),
            "total_search_volume": total_opportunity_volume,
            "average_difficulty": round(avg_difficulty, 1),
            "intent_breakdown": intent_breakdown,
            "quick_wins_count": len(quick_wins),
            "optimization_targets_count": len(optimization_targets)
        },
        "quick_wins": quick_wins[:10],
        "optimization_targets": optimization_targets[:10],
        "seed_queries_analyzed": len(top_queries),
        "cache_hit_rate": sum(
            1 for q in top_queries
            if _check_cache(supabase_client, _get_cache_key(domain, q.get("query", ""))) is not None
        ) / len(top_queries) if top_queries else 0
    }


# Synchronous wrapper for backwards compatibility
def analyze_keyword_opportunities_sync(
    supabase_client,
    gsc_data: Dict,
    domain: str,
    location_code: int = 2840
) -> Dict[str, Any]:
    """Synchronous wrapper for the async analysis function."""
    import asyncio
    
    # Create event loop if none exists
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    
    # Run the async function
    return loop.run_until_complete(
        analyze_keyword_opportunities(
            supabase_client=supabase_client,
            gsc_data=gsc_data,
            domain=domain,
            location_code=location_code
        )
    )
