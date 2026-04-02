import asyncio
import hashlib
import json
import math
import os
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse

import aiohttp
import pandas as pd
from dotenv import load_dotenv

load_dotenv()


class DataForSEOClient:
    """Async client for DataForSEO API with rate limiting and error handling."""

    BASE_URL = "https://api.dataforseo.com/v3"
    MAX_RETRIES = 3
    RETRY_DELAY = 2
    RATE_LIMIT_DELAY = 1

    def __init__(self, login: Optional[str] = None, password: Optional[str] = None):
        self.login = login or os.getenv("DATAFORSEO_LOGIN")
        self.password = password or os.getenv("DATAFORSEO_PASSWORD")
        
        if not self.login or not self.password:
            raise ValueError("DataForSEO credentials not provided")
        
        self.auth = aiohttp.BasicAuth(self.login, self.password)
        self.last_request_time = 0

    async def _rate_limit(self):
        """Implement rate limiting between requests."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.RATE_LIMIT_DELAY:
            await asyncio.sleep(self.RATE_LIMIT_DELAY - elapsed)
        self.last_request_time = time.time()

    async def _make_request(
        self,
        session: aiohttp.ClientSession,
        endpoint: str,
        payload: List[Dict[str, Any]],
        retry_count: int = 0
    ) -> Dict[str, Any]:
        """Make HTTP request with retry logic."""
        await self._rate_limit()
        
        url = f"{self.BASE_URL}{endpoint}"
        
        try:
            async with session.post(url, json=payload, auth=self.auth) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    if data.get("status_code") == 20000:
                        return data
                    elif data.get("status_code") == 40000:
                        if retry_count < self.MAX_RETRIES:
                            await asyncio.sleep(self.RETRY_DELAY * (retry_count + 1))
                            return await self._make_request(session, endpoint, payload, retry_count + 1)
                        else:
                            raise Exception(f"Rate limit exceeded: {data.get('status_message')}")
                    else:
                        raise Exception(f"API error: {data.get('status_message')}")
                else:
                    if retry_count < self.MAX_RETRIES:
                        await asyncio.sleep(self.RETRY_DELAY * (retry_count + 1))
                        return await self._make_request(session, endpoint, payload, retry_count + 1)
                    else:
                        raise Exception(f"HTTP error {response.status}")
        
        except asyncio.TimeoutError:
            if retry_count < self.MAX_RETRIES:
                await asyncio.sleep(self.RETRY_DELAY * (retry_count + 1))
                return await self._make_request(session, endpoint, payload, retry_count + 1)
            else:
                raise Exception("Request timeout after retries")

    async def get_serp_data(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
        device: str = "desktop"
    ) -> List[Dict[str, Any]]:
        """Fetch SERP data for multiple keywords."""
        
        tasks = []
        
        async with aiohttp.ClientSession() as session:
            for keyword in keywords:
                payload = [{
                    "keyword": keyword,
                    "location_code": location_code,
                    "language_code": language_code,
                    "device": device,
                    "os": "windows" if device == "desktop" else "ios",
                    "depth": 100
                }]
                
                task = self._make_request(
                    session,
                    "/serp/google/organic/live/advanced",
                    payload
                )
                tasks.append(task)
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
        
        serp_data = []
        for keyword, result in zip(keywords, results):
            if isinstance(result, Exception):
                print(f"Error fetching SERP for '{keyword}': {str(result)}")
                continue
            
            if result and "tasks" in result and len(result["tasks"]) > 0:
                task_result = result["tasks"][0]
                if task_result.get("status_code") == 20000 and "result" in task_result:
                    serp_data.append({
                        "keyword": keyword,
                        "data": task_result["result"][0] if task_result["result"] else None
                    })
        
        return serp_data


def extract_domain(url: str) -> str:
    """Extract root domain from URL."""
    try:
        parsed = urlparse(url)
        domain = parsed.netloc or parsed.path
        if domain.startswith("www."):
            domain = domain[4:]
        return domain
    except:
        return ""


def analyze_competitor_landscape(
    gsc_keyword_data: pd.DataFrame,
    dataforseo_client: DataForSEOClient,
    target_domain: str,
    top_n_keywords: int = 50,
    location_code: int = 2840,
    language_code: str = "en"
) -> Dict[str, Any]:
    """
    Analyze competitor landscape using top organic keywords.
    
    Args:
        gsc_keyword_data: DataFrame with GSC query data (columns: query, clicks, impressions, position, ctr)
        dataforseo_client: Initialized DataForSEO client
        target_domain: The user's domain (e.g., "example.com")
        top_n_keywords: Number of top keywords to analyze
        location_code: DataForSEO location code
        language_code: Language code for SERP results
    
    Returns:
        Dictionary with competitor analysis including:
        - Top competitors by keyword overlap
        - Shared keywords with each competitor
        - Ranking distributions
        - Domain authority proxies
        - Content gap analysis
        - Opportunity scores
    """
    
    # Clean target domain
    target_domain = target_domain.lower().replace("www.", "")
    
    # Select top keywords by impressions, filtering out likely branded terms
    brand_terms = target_domain.split(".")[0]
    non_branded = gsc_keyword_data[
        ~gsc_keyword_data["query"].str.lower().str.contains(brand_terms, na=False)
    ].copy()
    
    # Sort by impressions and take top N
    top_keywords = non_branded.nlargest(top_n_keywords, "impressions")["query"].tolist()
    
    if not top_keywords:
        return {
            "error": "No non-branded keywords found",
            "keywords_analyzed": 0,
            "competitors": [],
            "summary": {}
        }
    
    print(f"Fetching SERP data for {len(top_keywords)} keywords...")
    
    # Fetch SERP data asynchronously
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    serp_results = loop.run_until_complete(
        dataforseo_client.get_serp_data(
            top_keywords,
            location_code=location_code,
            language_code=language_code
        )
    )
    loop.close()
    
    print(f"Retrieved SERP data for {len(serp_results)} keywords")
    
    # Analyze competitors
    competitor_keywords = defaultdict(list)  # domain -> [keywords]
    competitor_positions = defaultdict(list)  # domain -> [positions]
    keyword_competitors = defaultdict(list)  # keyword -> [domains]
    target_keyword_positions = {}  # keyword -> position
    serp_features_by_keyword = {}  # keyword -> features list
    
    for serp in serp_results:
        keyword = serp["keyword"]
        data = serp.get("data")
        
        if not data or "items" not in data:
            continue
        
        # Extract SERP features
        features = []
        if data.get("featured_snippet"):
            features.append("featured_snippet")
        if data.get("knowledge_graph"):
            features.append("knowledge_graph")
        if data.get("local_pack"):
            features.append("local_pack")
        if data.get("people_also_ask"):
            features.append(f"people_also_ask_{len(data['people_also_ask'])}")
        if data.get("related_searches"):
            features.append("related_searches")
        if data.get("video"):
            features.append("video")
        if data.get("image"):
            features.append("image")
        if data.get("shopping"):
            features.append("shopping")
        if data.get("top_stories"):
            features.append("top_stories")
        
        serp_features_by_keyword[keyword] = features
        
        # Extract organic results
        organic_items = [
            item for item in data["items"]
            if item.get("type") == "organic"
        ]
        
        # Process top 10 organic results
        for item in organic_items[:10]:
            url = item.get("url", "")
            domain = extract_domain(url)
            
            if not domain:
                continue
            
            rank_group = item.get("rank_group", 0)
            
            # Track all competitors
            competitor_keywords[domain].append(keyword)
            competitor_positions[domain].append(rank_group)
            keyword_competitors[keyword].append(domain)
            
            # Track target domain position
            if domain == target_domain:
                target_keyword_positions[keyword] = rank_group
    
    # Remove target domain from competitors
    if target_domain in competitor_keywords:
        del competitor_keywords[target_domain]
        del competitor_positions[target_domain]
    
    # Calculate competitor metrics
    competitors_list = []
    
    for domain, keywords in competitor_keywords.items():
        keyword_count = len(keywords)
        positions = competitor_positions[domain]
        avg_position = sum(positions) / len(positions) if positions else 0
        
        # Calculate position distribution
        position_distribution = {
            "top_3": sum(1 for p in positions if p <= 3),
            "top_5": sum(1 for p in positions if p <= 5),
            "top_10": sum(1 for p in positions if p <= 10)
        }
        
        # Calculate overlap with target
        shared_keywords = []
        outranking_count = 0
        position_gaps = []
        
        for kw in keywords:
            if kw in target_keyword_positions:
                shared_keywords.append(kw)
                target_pos = target_keyword_positions[kw]
                competitor_pos = positions[keywords.index(kw)]
                
                if competitor_pos < target_pos:
                    outranking_count += 1
                    position_gaps.append(target_pos - competitor_pos)
        
        # Domain authority proxy: combination of keyword count, avg position, top 3 ratio
        da_proxy = (
            (keyword_count / len(top_keywords)) * 0.4 +
            (1 - (avg_position / 10)) * 0.3 +
            (position_distribution["top_3"] / max(keyword_count, 1)) * 0.3
        ) * 100
        
        # Threat level based on shared keywords and outranking
        if len(shared_keywords) > 0:
            threat_score = (
                (len(shared_keywords) / len(top_keywords)) * 0.5 +
                (outranking_count / len(shared_keywords)) * 0.5
            )
        else:
            threat_score = keyword_count / len(top_keywords)
        
        if threat_score > 0.3:
            threat_level = "high"
        elif threat_score > 0.15:
            threat_level = "medium"
        else:
            threat_level = "low"
        
        # Opportunity score: keywords where they rank well but we don't
        opportunity_keywords = [
            kw for kw in keywords
            if kw not in target_keyword_positions and positions[keywords.index(kw)] <= 5
        ]
        
        competitors_list.append({
            "domain": domain,
            "keywords_shared": len(shared_keywords),
            "keywords_total": keyword_count,
            "avg_position": round(avg_position, 2),
            "position_distribution": position_distribution,
            "outranking_count": outranking_count,
            "avg_position_gap": round(sum(position_gaps) / len(position_gaps), 2) if position_gaps else 0,
            "da_proxy": round(da_proxy, 1),
            "threat_level": threat_level,
            "threat_score": round(threat_score, 3),
            "opportunity_keywords": len(opportunity_keywords),
            "sample_shared_keywords": shared_keywords[:5],
            "sample_opportunity_keywords": opportunity_keywords[:5]
        })
    
    # Sort competitors by keyword count
    competitors_list.sort(key=lambda x: x["keywords_total"], reverse=True)
    
    # Identify primary competitors (appear in >20% of keywords)
    primary_competitors = [
        c for c in competitors_list
        if c["keywords_total"] / len(top_keywords) > 0.2
    ]
    
    # Content gap analysis: keywords where multiple competitors rank but target doesn't
    content_gaps = []
    
    for keyword, competitors in keyword_competitors.items():
        if keyword not in target_keyword_positions:
            # Count how many top competitors rank for this
            top_competitor_domains = {c["domain"] for c in competitors_list[:10]}
            ranking_top_competitors = [
                c for c in competitors if c in top_competitor_domains
            ]
            
            if len(ranking_top_competitors) >= 3:
                # Get GSC data for this keyword
                kw_data = gsc_keyword_data[gsc_keyword_data["query"] == keyword]
                
                if not kw_data.empty:
                    impressions = kw_data.iloc[0]["impressions"]
                    
                    content_gaps.append({
                        "keyword": keyword,
                        "competitor_count": len(ranking_top_competitors),
                        "competitors_ranking": ranking_top_competitors[:5],
                        "impressions": int(impressions),
                        "serp_features": serp_features_by_keyword.get(keyword, [])
                    })
    
    # Sort content gaps by impressions
    content_gaps.sort(key=lambda x: x["impressions"], reverse=True)
    
    # Ranking overlap analysis
    ranking_overlap = {}
    
    for competitor in competitors_list[:10]:
        domain = competitor["domain"]
        shared = competitor["keywords_shared"]
        
        if shared > 0:
            overlap_keywords = [
                kw for kw in competitor_keywords[domain]
                if kw in target_keyword_positions
            ]
            
            # Calculate position correlation
            position_pairs = []
            for kw in overlap_keywords:
                target_pos = target_keyword_positions[kw]
                comp_idx = competitor_keywords[domain].index(kw)
                comp_pos = competitor_positions[domain][comp_idx]
                position_pairs.append((target_pos, comp_pos))
            
            # Count position relationships
            target_wins = sum(1 for t, c in position_pairs if t < c)
            competitor_wins = sum(1 for t, c in position_pairs if c < t)
            ties = sum(1 for t, c in position_pairs if c == t)
            
            ranking_overlap[domain] = {
                "shared_keywords": shared,
                "target_wins": target_wins,
                "competitor_wins": competitor_wins,
                "ties": ties,
                "win_rate": round(target_wins / shared, 3) if shared > 0 else 0
            }
    
    # SERP position clustering: analyze position distribution patterns
    position_clusters = {
        "dominant_top_3": 0,  # Target in top 3
        "competitive_top_5": 0,  # Target in 4-5
        "page_1_lower": 0,  # Target in 6-10
        "page_2": 0,  # Target in 11-20
        "beyond": 0  # Target beyond 20
    }
    
    for kw, pos in target_keyword_positions.items():
        if pos <= 3:
            position_clusters["dominant_top_3"] += 1
        elif pos <= 5:
            position_clusters["competitive_top_5"] += 1
        elif pos <= 10:
            position_clusters["page_1_lower"] += 1
        elif pos <= 20:
            position_clusters["page_2"] += 1
        else:
            position_clusters["beyond"] += 1
    
    # Calculate overall opportunity score
    total_opportunity_keywords = sum(c["opportunity_keywords"] for c in competitors_list)
    avg_competitor_da = sum(c["da_proxy"] for c in competitors_list[:5]) / min(5, len(competitors_list)) if competitors_list else 0
    
    opportunity_score = (
        (total_opportunity_keywords / len(top_keywords)) * 40 +
        (len(content_gaps) / len(top_keywords)) * 30 +
        (position_clusters["page_2"] / max(len(target_keyword_positions), 1)) * 20 +
        (min(avg_competitor_da / 100, 1)) * 10
    )
    
    return {
        "keywords_analyzed": len(top_keywords),
        "keywords_with_serp_data": len(serp_results),
        "target_domain": target_domain,
        "target_ranking_keywords": len(target_keyword_positions),
        
        "competitors": competitors_list[:20],  # Top 20 competitors
        "primary_competitors": primary_competitors,
        "total_unique_competitors": len(competitors_list),
        
        "ranking_overlap": ranking_overlap,
        
        "position_clusters": position_clusters,
        
        "content_gaps": content_gaps[:30],  # Top 30 content gap opportunities
        "total_content_gaps": len(content_gaps),
        
        "opportunity_score": round(opportunity_score, 1),
        
        "summary": {
            "avg_competitors_per_keyword": round(
                sum(len(comps) for comps in keyword_competitors.values()) / max(len(keyword_competitors), 1),
                1
            ),
            "most_common_serp_features": dict(
                Counter([
                    f for features in serp_features_by_keyword.values()
                    for f in features
                ]).most_common(10)
            ),
            "competitive_intensity": (
                "high" if avg_competitor_da > 70 else
                "medium" if avg_competitor_da > 40 else
                "low"
            ),
            "primary_competitor_count": len(primary_competitors),
            "high_threat_competitors": len([c for c in competitors_list if c["threat_level"] == "high"]),
            "total_opportunity_keywords": total_opportunity_keywords
        },
        
        "recommendations": generate_competitor_recommendations(
            competitors_list,
            content_gaps,
            position_clusters,
            ranking_overlap
        )
    }


def generate_competitor_recommendations(
    competitors: List[Dict],
    content_gaps: List[Dict],
    position_clusters: Dict,
    ranking_overlap: Dict
) -> List[Dict[str, Any]]:
    """Generate actionable recommendations based on competitor analysis."""
    
    recommendations = []
    
    # High-threat competitor monitoring
    high_threat = [c for c in competitors if c["threat_level"] == "high"]
    if high_threat:
        recommendations.append({
            "category": "competitor_monitoring",
            "priority": "high",
            "action": f"Monitor {len(high_threat)} high-threat competitors closely",
            "details": f"Competitors {', '.join(c['domain'] for c in high_threat[:3])} are outranking you on {sum(c['outranking_count'] for c in high_threat)} shared keywords",
            "impact": "defensive"
        })
    
    # Content gap opportunities
    if content_gaps:
        high_volume_gaps = [g for g in content_gaps if g["impressions"] > 1000]
        if high_volume_gaps:
            recommendations.append({
                "category": "content_creation",
                "priority": "high",
                "action": f"Create content for {len(high_volume_gaps)} high-volume gap keywords",
                "details": f"Top opportunity: '{high_volume_gaps[0]['keyword']}' with {high_volume_gaps[0]['impressions']} monthly impressions, {high_volume_gaps[0]['competitor_count']} competitors ranking",
                "impact": "offensive",
                "estimated_monthly_impressions": sum(g["impressions"] for g in high_volume_gaps[:10])
            })
    
    # Position improvement opportunities
    if position_clusters.get("page_2", 0) > 5:
        recommendations.append({
            "category": "position_improvement",
            "priority": "medium",
            "action": f"Optimize {position_clusters['page_2']} page 2 rankings to reach page 1",
            "details": "These keywords already have search visibility; small improvements can yield significant traffic gains",
            "impact": "offensive"
        })
    
    # Competitive overlap strategy
    low_win_rate_competitors = [
        (domain, data) for domain, data in ranking_overlap.items()
        if data["win_rate"] < 0.4 and data["shared_keywords"] > 5
    ]
    
    if low_win_rate_competitors:
        domain, data = low_win_rate_competitors[0]
        recommendations.append({
            "category": "competitive_analysis",
            "priority": "medium",
            "action": f"Analyze content strategy of {domain}",
            "details": f"They outrank you on {data['competitor_wins']}/{data['shared_keywords']} shared keywords. Study their content approach for improvement opportunities",
            "impact": "strategic"
        })
    
    # Domain authority building
    avg_competitor_da = sum(c["da_proxy"] for c in competitors[:5]) / min(5, len(competitors)) if competitors else 0
    if avg_competitor_da > 60:
        recommendations.append({
            "category": "authority_building",
            "priority": "medium",
            "action": "Invest in domain authority building",
            "details": f"Top competitors have average DA proxy of {round(avg_competitor_da, 1)}. Focus on high-quality backlinks and brand mentions",
            "impact": "strategic"
        })
    
    return recommendations


# Example usage and testing
if __name__ == "__main__":
    # This is for testing only
    
    # Sample GSC data
    sample_gsc_data = pd.DataFrame({
        "query": [
            "best crm software",
            "crm for small business",
            "salesforce alternatives",
            "hubspot pricing",
            "zoho crm review"
        ],
        "clicks": [120, 85, 95, 110, 75],
        "impressions": [5000, 3200, 4100, 4800, 2900],
        "position": [8.5, 12.3, 9.1, 7.2, 15.6],
        "ctr": [0.024, 0.027, 0.023, 0.023, 0.026]
    })
    
    try:
        client = DataForSEOClient()
        
        result = analyze_competitor_landscape(
            gsc_keyword_data=sample_gsc_data,
            dataforseo_client=client,
            target_domain="example.com",
            top_n_keywords=5
        )
        
        print(json.dumps(result, indent=2))
        
    except Exception as e:
        print(f"Error: {str(e)}")