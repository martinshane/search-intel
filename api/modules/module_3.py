"""
Module 3: Competitor Landscape Analysis

Analyzes competitor domains using DataForSEO API to:
1. Identify top competing domains from GSC query data
2. Fetch competitor rankings for user's top keywords
3. Calculate competitor visibility scores and overlap metrics
4. Identify keyword gaps and opportunities
"""

import asyncio
import hashlib
import json
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
    RETRY_DELAY = 2  # seconds
    RATE_LIMIT_DELAY = 1  # seconds between requests

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
                        # Rate limit hit
                        if retry_count < self.MAX_RETRIES:
                            await asyncio.sleep(self.RETRY_DELAY * (retry_count + 1))
                            return await self._make_request(session, endpoint, payload, retry_count + 1)
                        else:
                            raise Exception(f"Rate limit exceeded: {data.get('status_message')}")
                    else:
                        raise Exception(f"API error: {data.get('status_message')}")
                
                elif response.status == 429:
                    # HTTP rate limit
                    if retry_count < self.MAX_RETRIES:
                        await asyncio.sleep(self.RETRY_DELAY * (retry_count + 1))
                        return await self._make_request(session, endpoint, payload, retry_count + 1)
                    else:
                        raise Exception("Rate limit exceeded")
                else:
                    raise Exception(f"HTTP error: {response.status}")
                    
        except aiohttp.ClientError as e:
            if retry_count < self.MAX_RETRIES:
                await asyncio.sleep(self.RETRY_DELAY * (retry_count + 1))
                return await self._make_request(session, endpoint, payload, retry_count + 1)
            else:
                raise Exception(f"Request failed: {str(e)}")

    async def get_serp_results(
        self,
        keywords: List[str],
        location_code: int = 2840,  # United States
        language_code: str = "en",
        device: str = "desktop",
        depth: int = 100
    ) -> Dict[str, Any]:
        """
        Fetch SERP results for multiple keywords.
        
        Args:
            keywords: List of keywords to analyze
            location_code: DataForSEO location code
            language_code: Language code
            device: Device type (desktop/mobile)
            depth: Number of results to fetch (max 100)
        
        Returns:
            Dictionary mapping keywords to SERP results
        """
        endpoint = "/serp/google/organic/live/advanced"
        
        # Build tasks for each keyword
        tasks = []
        for keyword in keywords:
            payload = [{
                "keyword": keyword,
                "location_code": location_code,
                "language_code": language_code,
                "device": device,
                "depth": depth,
                "calculate_rectangles": False
            }]
            tasks.append((keyword, payload))
        
        results = {}
        
        async with aiohttp.ClientSession() as session:
            for keyword, payload in tasks:
                try:
                    response = await self._make_request(session, endpoint, payload)
                    
                    if response.get("tasks") and len(response["tasks"]) > 0:
                        task = response["tasks"][0]
                        if task.get("result") and len(task["result"]) > 0:
                            results[keyword] = task["result"][0]
                        else:
                            results[keyword] = None
                    else:
                        results[keyword] = None
                        
                except Exception as e:
                    print(f"Error fetching SERP for '{keyword}': {str(e)}")
                    results[keyword] = None
        
        return results


def extract_domain(url: str) -> str:
    """Extract root domain from URL."""
    try:
        parsed = urlparse(url)
        domain = parsed.netloc or parsed.path
        # Remove www.
        if domain.startswith('www.'):
            domain = domain[4:]
        return domain.lower()
    except:
        return ""


def calculate_domain_authority_estimate(domain: str, serp_positions: List[int]) -> float:
    """
    Estimate domain authority based on ranking positions.
    
    Simple heuristic:
    - Average position in top 3: ~80-100
    - Average position 4-10: ~60-80
    - Average position 11-20: ~40-60
    - Average position 21-50: ~20-40
    - Average position 51+: ~0-20
    """
    if not serp_positions:
        return 0.0
    
    avg_position = sum(serp_positions) / len(serp_positions)
    
    if avg_position <= 3:
        return 90 + (3 - avg_position) * 3.33
    elif avg_position <= 10:
        return 60 + (10 - avg_position) * 2.86
    elif avg_position <= 20:
        return 40 + (20 - avg_position) * 2.0
    elif avg_position <= 50:
        return 20 + (50 - avg_position) * 0.67
    else:
        return max(0, 20 - (avg_position - 50) * 0.4)


def analyze_competitor_landscape(
    gsc_keyword_data: pd.DataFrame,
    serp_data: Dict[str, Any],
    user_domain: str,
    top_n_competitors: int = 10,
    min_keyword_overlap: int = 3
) -> Dict[str, Any]:
    """
    Analyze competitor landscape from SERP data.
    
    Args:
        gsc_keyword_data: DataFrame with columns: query, clicks, impressions, position
        serp_data: Dictionary mapping keywords to SERP results from DataForSEO
        user_domain: User's root domain
        top_n_competitors: Number of top competitors to analyze in detail
        min_keyword_overlap: Minimum keyword overlap to be considered a competitor
    
    Returns:
        Dictionary containing:
        - competitors: List of competitor domains with metrics
        - keyword_overlap: Overlap analysis per competitor
        - visibility_comparison: Visibility scores
        - keyword_gaps: Keywords where competitors rank but user doesn't
        - opportunity_keywords: Keywords with weak competition
    """
    
    # Normalize user domain
    user_domain = extract_domain(user_domain)
    
    # Track competitor appearances and positions
    competitor_keywords = defaultdict(list)  # domain -> list of (keyword, position, url)
    competitor_positions = defaultdict(list)  # domain -> list of positions
    keyword_competitors = defaultdict(set)  # keyword -> set of competitor domains
    
    # Track user's keywords and positions
    user_keywords = {}  # keyword -> position
    user_keyword_metrics = {}  # keyword -> {clicks, impressions, position}
    
    # Process GSC data
    for _, row in gsc_keyword_data.iterrows():
        keyword = row['query']
        user_keywords[keyword] = row.get('position', 0)
        user_keyword_metrics[keyword] = {
            'clicks': row.get('clicks', 0),
            'impressions': row.get('impressions', 0),
            'position': row.get('position', 0),
            'ctr': row.get('ctr', 0)
        }
    
    # Process SERP data
    for keyword, serp_result in serp_data.items():
        if not serp_result or 'items' not in serp_result:
            continue
        
        items = serp_result.get('items', [])
        
        for item in items:
            # Only process organic results
            if item.get('type') != 'organic':
                continue
            
            url = item.get('url', '')
            domain = extract_domain(url)
            position = item.get('rank_group', 0)
            
            if not domain or position == 0:
                continue
            
            # Skip user's own domain
            if domain == user_domain:
                continue
            
            # Track competitor
            competitor_keywords[domain].append({
                'keyword': keyword,
                'position': position,
                'url': url
            })
            competitor_positions[domain].append(position)
            keyword_competitors[keyword].add(domain)
    
    # Calculate competitor metrics
    competitor_metrics = []
    
    for domain, keywords_data in competitor_keywords.items():
        if len(keywords_data) < min_keyword_overlap:
            continue
        
        # Get all keywords this competitor ranks for
        competitor_keyword_set = {kd['keyword'] for kd in keywords_data}
        
        # Find overlap with user's keywords
        overlapping_keywords = competitor_keyword_set & set(user_keywords.keys())
        
        # Calculate average position
        positions = [kd['position'] for kd in keywords_data]
        avg_position = sum(positions) / len(positions) if positions else 0
        
        # Calculate visibility score (inverse of average position, weighted by keyword count)
        visibility_score = (len(keywords_data) * 100) / (avg_position if avg_position > 0 else 1)
        
        # Estimate domain authority
        domain_authority_estimate = calculate_domain_authority_estimate(domain, positions)
        
        # Calculate head-to-head win rate
        wins = 0
        losses = 0
        for keyword in overlapping_keywords:
            competitor_pos = next(
                (kd['position'] for kd in keywords_data if kd['keyword'] == keyword),
                None
            )
            user_pos = user_keywords.get(keyword)
            
            if competitor_pos and user_pos:
                if competitor_pos < user_pos:
                    wins += 1
                elif competitor_pos > user_pos:
                    losses += 1
        
        win_rate = wins / (wins + losses) if (wins + losses) > 0 else 0
        
        # Determine threat level
        if win_rate > 0.6 and len(overlapping_keywords) >= 10:
            threat_level = "high"
        elif win_rate > 0.4 and len(overlapping_keywords) >= 5:
            threat_level = "medium"
        else:
            threat_level = "low"
        
        # Top ranking keywords for this competitor
        top_keywords = sorted(keywords_data, key=lambda x: x['position'])[:5]
        
        competitor_metrics.append({
            'domain': domain,
            'keywords_total': len(keywords_data),
            'keywords_shared': len(overlapping_keywords),
            'avg_position': round(avg_position, 1),
            'visibility_score': round(visibility_score, 1),
            'domain_authority_estimate': round(domain_authority_estimate, 1),
            'win_rate': round(win_rate, 2),
            'threat_level': threat_level,
            'top_keywords': [
                {
                    'keyword': kd['keyword'],
                    'position': kd['position'],
                    'url': kd['url']
                }
                for kd in top_keywords
            ]
        })
    
    # Sort competitors by visibility score
    competitor_metrics.sort(key=lambda x: x['visibility_score'], reverse=True)
    
    # Limit to top N
    top_competitors = competitor_metrics[:top_n_competitors]
    
    # Analyze keyword gaps (keywords where competitors rank well but user doesn't)
    keyword_gaps = []
    
    for keyword, competitors in keyword_competitors.items():
        # Skip if user already ranks for this keyword
        if keyword in user_keywords:
            continue
        
        # Find how many top competitors rank for this keyword
        top_competitor_domains = {c['domain'] for c in top_competitors}
        competitors_ranking = competitors & top_competitor_domains
        
        if len(competitors_ranking) >= 2:  # At least 2 top competitors rank
            # Get positions of competitors for this keyword
            competitor_positions_for_kw = []
            for domain in competitors_ranking:
                for kd in competitor_keywords[domain]:
                    if kd['keyword'] == keyword:
                        competitor_positions_for_kw.append({
                            'domain': domain,
                            'position': kd['position'],
                            'url': kd['url']
                        })
            
            # Calculate average competitor position
            avg_comp_position = sum(c['position'] for c in competitor_positions_for_kw) / len(competitor_positions_for_kw)
            
            # Estimate opportunity (better if competitors rank lower on average)
            opportunity_score = max(0, 100 - avg_comp_position * 5)
            
            keyword_gaps.append({
                'keyword': keyword,
                'competitors_ranking': len(competitors_ranking),
                'avg_competitor_position': round(avg_comp_position, 1),
                'opportunity_score': round(opportunity_score, 1),
                'ranking_competitors': competitor_positions_for_kw[:3]  # Top 3
            })
    
    # Sort keyword gaps by opportunity score
    keyword_gaps.sort(key=lambda x: x['opportunity_score'], reverse=True)
    
    # Analyze opportunity keywords (keywords user ranks for with weak competition)
    opportunity_keywords = []
    
    for keyword in user_keywords:
        user_pos = user_keywords[keyword]
        
        # Skip if user already ranks in top 3
        if user_pos <= 3:
            continue
        
        competitors_for_kw = keyword_competitors.get(keyword, set())
        
        # Get top competitor domains
        top_competitor_domains = {c['domain'] for c in top_competitors}
        strong_competitors = competitors_for_kw & top_competitor_domains
        
        # Calculate average position of strong competitors
        strong_comp_positions = []
        for domain in strong_competitors:
            for kd in competitor_keywords[domain]:
                if kd['keyword'] == keyword:
                    strong_comp_positions.append(kd['position'])
        
        avg_strong_comp_pos = sum(strong_comp_positions) / len(strong_comp_positions) if strong_comp_positions else 100
        
        # Opportunity if user is close to or better than strong competitors
        if user_pos < avg_strong_comp_pos or (user_pos <= 10 and len(strong_competitors) < 2):
            metrics = user_keyword_metrics.get(keyword, {})
            
            # Estimate traffic gain from reaching top 3
            current_ctr = metrics.get('ctr', 0)
            impressions = metrics.get('impressions', 0)
            
            # Rough CTR estimates by position
            position_ctr = {
                1: 0.30, 2: 0.15, 3: 0.10, 4: 0.07, 5: 0.05,
                6: 0.04, 7: 0.03, 8: 0.025, 9: 0.02, 10: 0.015
            }
            
            estimated_top3_ctr = position_ctr.get(3, 0.10)
            estimated_click_gain = impressions * (estimated_top3_ctr - current_ctr) if impressions > 0 else 0
            
            opportunity_keywords.append({
                'keyword': keyword,
                'current_position': user_pos,
                'current_clicks': metrics.get('clicks', 0),
                'impressions': impressions,
                'strong_competitors': len(strong_competitors),
                'avg_competitor_position': round(avg_strong_comp_pos, 1),
                'estimated_click_gain': round(estimated_click_gain, 0),
                'difficulty': 'easy' if len(strong_competitors) < 2 else 'medium'
            })
    
    # Sort opportunity keywords by estimated click gain
    opportunity_keywords.sort(key=lambda x: x['estimated_click_gain'], reverse=True)
    
    # Calculate overall visibility comparison
    user_visibility = 0
    if user_keywords:
        # User visibility based on their rankings
        user_positions = [pos for pos in user_keywords.values() if pos > 0]
        if user_positions:
            avg_user_position = sum(user_positions) / len(user_positions)
            user_visibility = (len(user_keywords) * 100) / avg_user_position
    
    top_competitor_visibility = sum(c['visibility_score'] for c in top_competitors[:3]) / 3 if len(top_competitors) >= 3 else 0
    
    # Calculate market share estimate
    total_visibility = user_visibility + sum(c['visibility_score'] for c in top_competitors)
    user_market_share = (user_visibility / total_visibility * 100) if total_visibility > 0 else 0
    
    # Generate summary insights
    summary = {
        'total_competitors_identified': len(competitor_metrics),
        'top_competitors_analyzed': len(top_competitors),
        'total_keyword_gaps': len(keyword_gaps),
        'high_opportunity_gaps': len([k for k in keyword_gaps if k['opportunity_score'] > 70]),
        'total_opportunity_keywords': len(opportunity_keywords),
        'estimated_monthly_click_opportunity': sum(k['estimated_click_gain'] for k in opportunity_keywords),
        'user_visibility_score': round(user_visibility, 1),
        'market_leader_visibility': round(top_competitors[0]['visibility_score'], 1) if top_competitors else 0,
        'user_market_share_pct': round(user_market_share, 1),
        'primary_threat': top_competitors[0]['domain'] if top_competitors else None
    }
    
    return {
        'competitors': top_competitors,
        'all_competitors': competitor_metrics,  # Full list
        'keyword_gaps': keyword_gaps[:50],  # Top 50 gaps
        'opportunity_keywords': opportunity_keywords[:50],  # Top 50 opportunities
        'visibility_comparison': {
            'user_visibility': round(user_visibility, 1),
            'top_competitor_avg': round(top_competitor_visibility, 1),
            'market_share_pct': round(user_market_share, 1)
        },
        'summary': summary
    }


async def run_competitor_analysis(
    gsc_keyword_data: pd.DataFrame,
    user_domain: str,
    location_code: int = 2840,
    language_code: str = "en",
    max_keywords: int = 50
) -> Dict[str, Any]:
    """
    Main function to run complete competitor landscape analysis.
    
    Args:
        gsc_keyword_data: GSC keyword data with queries, clicks, impressions, positions
        user_domain: User's domain
        location_code: DataForSEO location code
        language_code: Language code
        max_keywords: Maximum number of keywords to analyze via SERP API
    
    Returns:
        Complete competitor analysis results
    """
    
    # Select top keywords to analyze
    # Prioritize by impressions and filter out branded terms
    gsc_data = gsc_keyword_data.copy()
    
    # Simple brand filtering (remove queries containing the domain name)
    domain_parts = extract_domain(user_domain).split('.')
    brand_terms = [part for part in domain_parts if len(part) > 3]
    
    def is_branded(query: str) -> bool:
        query_lower = query.lower()
        for term in brand_terms:
            if term.lower() in query_lower:
                return True
        return False
    
    gsc_data['is_branded'] = gsc_data['query'].apply(is_branded)
    non_branded = gsc_data[~gsc_data['is_branded']].copy()
    
    # Sort by impressions and take top N
    non_branded = non_branded.sort_values('impressions', ascending=False)
    top_keywords = non_branded.head(max_keywords)['query'].tolist()
    
    print(f"Analyzing {len(top_keywords)} non-branded keywords...")
    
    # Fetch SERP data
    client = DataForSEOClient()
    serp_data = await client.get_serp_results(
        keywords=top_keywords,
        location_code=location_code,
        language_code=language_code,
        depth=100
    )
    
    # Filter out failed requests
    successful_serps = {k: v for k, v in serp_data.items() if v is not None}
    print(f"Successfully fetched SERP data for {len(successful_serps)} keywords")
    
    # Run competitor analysis
    analysis = analyze_competitor_landscape(
        gsc_keyword_data=top_keywords,
        serp_data=successful_serps,
        user_domain=user_domain
    )
    
    # Add metadata
    analysis['metadata'] = {
        'analysis_date': datetime.now().isoformat(),
        'keywords_analyzed': len(successful_serps),
        'total_keywords_available': len(gsc_keyword_data),
        'location_code': location_code,
        'language_code': language_code,
        'user_domain': user_domain
    }
    
    return analysis


def analyze_competitor_landscape_sync(
    gsc_keyword_data: pd.DataFrame,
    user_domain: str,
    location_code: int = 2840,
    language_code: str = "en",
    max_keywords: int = 50
) -> Dict[str, Any]:
    """
    Synchronous wrapper for competitor analysis.
    
    Args:
        gsc_keyword_data: GSC keyword data
        user_domain: User's domain
        location_code: DataForSEO location code
        language_code: Language code
        max_keywords: Max keywords to analyze
    
    Returns:
        Competitor analysis results
    """
    return asyncio.run(
        run_competitor_analysis(
            gsc_keyword_data=gsc_keyword_data,
            user_domain=user_domain,
            location_code=location_code,
            language_code=language_code,
            max_keywords=max_keywords
        )
    )


if __name__ == "__main__":
    # Example usage
    import numpy as np
    
    # Create sample GSC data
    sample_data = pd.DataFrame({
        'query': [
            'best crm software',
            'crm for small business',
            'salesforce alternative',
            'affordable crm',
            'crm comparison'
        ],
        'clicks': [150, 89, 67, 45, 34],
        'impressions': [5000, 3200, 2100, 1800, 1200],
        'position': [8.5, 11.2, 6.3, 14.1, 9.8],
        'ctr': [0.03, 0.028, 0.032, 0.025, 0.028]
    })
    
    result = analyze_competitor_landscape_sync(
        gsc_keyword_data=sample_data,
        user_domain="example.com",
        max_keywords=5
    )
    
    print(json.dumps(result, indent=2))

