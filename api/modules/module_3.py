"""
Module 3: SERP Landscape Analysis

Analyzes search engine results page competition using DataForSEO API.
Fetches top 10 organic results for target keywords, analyzes competitor domains,
calculates keyword difficulty scores, identifies SERP features, and returns
structured data including competitor URLs, rankings, domain authority estimates,
and SERP feature presence.
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
                
                elif response.status == 401:
                    raise Exception("Authentication failed - check DataForSEO credentials")
                elif response.status == 429:
                    if retry_count < self.MAX_RETRIES:
                        await asyncio.sleep(self.RETRY_DELAY * (retry_count + 1))
                        return await self._make_request(session, endpoint, payload, retry_count + 1)
                    else:
                        raise Exception("Rate limit exceeded after retries")
                else:
                    response.raise_for_status()
                    
        except aiohttp.ClientError as e:
            if retry_count < self.MAX_RETRIES:
                await asyncio.sleep(self.RETRY_DELAY * (retry_count + 1))
                return await self._make_request(session, endpoint, payload, retry_count + 1)
            else:
                raise Exception(f"Request failed after {self.MAX_RETRIES} retries: {str(e)}")

    async def get_serp_results(
        self,
        keywords: List[str],
        location_code: int = 2840,  # USA
        language_code: str = "en",
        device: str = "desktop",
        depth: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Fetch SERP results for multiple keywords.
        
        Args:
            keywords: List of keywords to analyze
            location_code: DataForSEO location code (2840 = USA)
            language_code: Language code
            device: desktop, mobile, or tablet
            depth: Number of results to retrieve (max 100)
            
        Returns:
            List of SERP result dictionaries
        """
        if not keywords:
            return []
        
        # Build payload for batch request
        tasks = []
        for keyword in keywords:
            task_payload = {
                "keyword": keyword,
                "location_code": location_code,
                "language_code": language_code,
                "device": device,
                "depth": depth,
                "calculate_rectangles": True  # For SERP feature positioning
            }
            tasks.append(task_payload)
        
        results = []
        
        # Process in batches of 20 (API limit)
        batch_size = 20
        async with aiohttp.ClientSession() as session:
            for i in range(0, len(tasks), batch_size):
                batch = tasks[i:i + batch_size]
                
                try:
                    response = await self._make_request(
                        session,
                        "/serp/google/organic/live/advanced",
                        batch
                    )
                    
                    if response.get("tasks"):
                        for task in response["tasks"]:
                            if task.get("status_code") == 20000 and task.get("result"):
                                results.extend(task["result"])
                            else:
                                # Log failed task but continue
                                keyword = batch[task.get("id", 0) - 1].get("keyword", "unknown")
                                print(f"Warning: Failed to fetch SERP for keyword '{keyword}'")
                    
                    # Small delay between batches
                    if i + batch_size < len(tasks):
                        await asyncio.sleep(0.5)
                        
                except Exception as e:
                    print(f"Error fetching batch starting at index {i}: {str(e)}")
                    continue
        
        return results


def extract_domain(url: str) -> str:
    """Extract root domain from URL."""
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        # Remove www. prefix
        if domain.startswith("www."):
            domain = domain[4:]
        return domain
    except:
        return ""


def identify_serp_features(serp_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Identify all SERP features present in the result.
    
    Returns dict with feature presence and counts.
    """
    features = {
        "featured_snippet": False,
        "featured_snippet_type": None,
        "people_also_ask": False,
        "people_also_ask_count": 0,
        "local_pack": False,
        "local_pack_count": 0,
        "knowledge_panel": False,
        "image_pack": False,
        "image_pack_count": 0,
        "video_carousel": False,
        "video_carousel_count": 0,
        "shopping_results": False,
        "shopping_results_count": 0,
        "top_stories": False,
        "top_stories_count": 0,
        "twitter_results": False,
        "ai_overview": False,
        "related_searches": False,
        "site_links": False,
        "total_features": 0
    }
    
    items = serp_data.get("items", [])
    
    for item in items:
        item_type = item.get("type", "")
        
        if item_type == "featured_snippet":
            features["featured_snippet"] = True
            features["featured_snippet_type"] = item.get("featured_snippet", {}).get("type")
            features["total_features"] += 1
            
        elif item_type == "people_also_ask":
            features["people_also_ask"] = True
            items_list = item.get("items", [])
            features["people_also_ask_count"] = len(items_list)
            features["total_features"] += 1
            
        elif item_type == "local_pack":
            features["local_pack"] = True
            items_list = item.get("items", [])
            features["local_pack_count"] = len(items_list)
            features["total_features"] += 1
            
        elif item_type == "knowledge_graph":
            features["knowledge_panel"] = True
            features["total_features"] += 1
            
        elif item_type == "images":
            features["image_pack"] = True
            items_list = item.get("items", [])
            features["image_pack_count"] = len(items_list)
            features["total_features"] += 1
            
        elif item_type == "video":
            features["video_carousel"] = True
            items_list = item.get("items", [])
            features["video_carousel_count"] = len(items_list)
            features["total_features"] += 1
            
        elif item_type == "shopping":
            features["shopping_results"] = True
            items_list = item.get("items", [])
            features["shopping_results_count"] = len(items_list)
            features["total_features"] += 1
            
        elif item_type == "top_stories":
            features["top_stories"] = True
            items_list = item.get("items", [])
            features["top_stories_count"] = len(items_list)
            features["total_features"] += 1
            
        elif item_type == "twitter":
            features["twitter_results"] = True
            features["total_features"] += 1
            
        elif item_type == "ai_overview":
            features["ai_overview"] = True
            features["total_features"] += 1
            
        elif item_type == "related_searches":
            features["related_searches"] = True
            
        elif item_type == "organic" and item.get("links"):
            # Site links attached to organic result
            features["site_links"] = True
    
    return features


def calculate_visual_position(
    organic_rank: int,
    serp_features: Dict[str, Any],
    items: List[Dict[str, Any]]
) -> float:
    """
    Calculate visual position accounting for SERP features above the organic result.
    
    Each SERP feature adds "weight" to push organic results down visually.
    """
    visual_offset = 0.0
    
    # Parse items in order to count features before this organic position
    for item in items:
        item_type = item.get("type", "")
        rank_group = item.get("rank_group", 0)
        rank_absolute = item.get("rank_absolute", 0)
        
        # Only count features that appear before the organic result
        if rank_absolute >= organic_rank:
            break
        
        if item_type == "featured_snippet":
            visual_offset += 2.0  # Featured snippets are very prominent
            
        elif item_type == "people_also_ask":
            # Each PAA box
            count = len(item.get("items", []))
            visual_offset += count * 0.5
            
        elif item_type == "local_pack":
            visual_offset += 1.5  # Local pack with map
            
        elif item_type == "knowledge_graph":
            visual_offset += 1.0  # Usually on right side, but still draws attention
            
        elif item_type == "images":
            visual_offset += 1.0
            
        elif item_type == "video":
            count = len(item.get("items", []))
            visual_offset += min(count * 0.3, 1.5)  # Video carousel
            
        elif item_type == "shopping":
            visual_offset += 1.5
            
        elif item_type == "top_stories":
            visual_offset += 1.0
            
        elif item_type == "ai_overview":
            visual_offset += 2.5  # AI overviews are very prominent
    
    return organic_rank + visual_offset


def extract_organic_results(serp_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract organic results from SERP data."""
    organic_results = []
    
    items = serp_data.get("items", [])
    
    for item in items:
        if item.get("type") == "organic":
            result = {
                "position": item.get("rank_absolute", 0),
                "url": item.get("url", ""),
                "domain": extract_domain(item.get("url", "")),
                "title": item.get("title", ""),
                "description": item.get("description", ""),
                "breadcrumb": item.get("breadcrumb", ""),
                "has_site_links": bool(item.get("links")),
                "is_amp": item.get("amp_version", False),
                "rating": None,
                "reviews_count": None
            }
            
            # Extract rating if present
            rating_data = item.get("rating")
            if rating_data:
                result["rating"] = rating_data.get("rating_value")
                result["reviews_count"] = rating_data.get("votes_count") or rating_data.get("rating_votes")
            
            organic_results.append(result)
    
    return organic_results


def calculate_keyword_difficulty(
    organic_results: List[Dict[str, Any]],
    serp_features: Dict[str, Any]
) -> float:
    """
    Calculate keyword difficulty score (0-100).
    
    Based on:
    - Domain diversity in top 10
    - Presence of authoritative domains
    - SERP feature density
    - Result quality signals (ratings, reviews)
    """
    if not organic_results:
        return 50.0
    
    score = 0.0
    
    # 1. Domain diversity (fewer unique domains = harder)
    unique_domains = len(set(r["domain"] for r in organic_results if r["domain"]))
    domain_diversity_factor = (10 - unique_domains) / 10.0  # 0 to 1
    score += domain_diversity_factor * 30
    
    # 2. SERP feature density
    feature_count = serp_features.get("total_features", 0)
    feature_factor = min(feature_count / 5.0, 1.0)  # Cap at 5 features
    score += feature_factor * 25
    
    # 3. AI Overview presence (makes it harder)
    if serp_features.get("ai_overview"):
        score += 15
    
    # 4. Featured snippet presence
    if serp_features.get("featured_snippet"):
        score += 10
    
    # 5. Quality signals in results (ratings, site links)
    quality_count = sum(1 for r in organic_results if r.get("rating") or r.get("has_site_links"))
    quality_factor = quality_count / len(organic_results)
    score += quality_factor * 20
    
    return min(score, 100.0)


def classify_search_intent(
    keyword: str,
    serp_features: Dict[str, Any],
    organic_results: List[Dict[str, Any]]
) -> str:
    """
    Classify search intent based on keyword and SERP composition.
    
    Returns: informational, commercial, transactional, or navigational
    """
    keyword_lower = keyword.lower()
    
    # Navigational signals
    navigational_keywords = ["login", "sign in", "facebook", "youtube", "twitter", "linkedin"]
    if any(nk in keyword_lower for nk in navigational_keywords):
        return "navigational"
    
    # Transactional signals
    transactional_keywords = ["buy", "purchase", "order", "price", "cost", "cheap", "discount", "deal"]
    if any(tk in keyword_lower for tk in transactional_keywords):
        return "transactional"
    
    if serp_features.get("shopping_results"):
        return "transactional"
    
    # Commercial investigation signals
    commercial_keywords = ["best", "top", "review", "vs", "versus", "compare", "alternative"]
    if any(ck in keyword_lower for ck in commercial_keywords):
        return "commercial"
    
    # Informational signals
    informational_keywords = ["what", "how", "why", "when", "where", "guide", "tutorial", "learn"]
    if any(ik in keyword_lower for ik in informational_keywords):
        return "informational"
    
    if serp_features.get("people_also_ask") or serp_features.get("featured_snippet"):
        return "informational"
    
    if serp_features.get("knowledge_panel"):
        return "informational"
    
    # Default based on URL patterns in results
    if organic_results:
        blog_count = sum(1 for r in organic_results if any(
            b in r["url"].lower() for b in ["/blog/", "/article/", "/guide/", "/learn/"]
        ))
        product_count = sum(1 for r in organic_results if any(
            p in r["url"].lower() for p in ["/product/", "/buy/", "/shop/", "/cart/"]
        ))
        
        if blog_count > len(organic_results) / 2:
            return "informational"
        if product_count > len(organic_results) / 2:
            return "transactional"
    
    return "informational"  # Default


def estimate_ctr_impact(
    organic_rank: int,
    visual_position: float,
    serp_features: Dict[str, Any]
) -> float:
    """
    Estimate CTR impact due to SERP feature displacement.
    
    Returns negative value representing CTR loss.
    """
    # Base CTR by position (industry averages)
    base_ctr_curve = {
        1: 0.284, 2: 0.152, 3: 0.098, 4: 0.071, 5: 0.057,
        6: 0.045, 7: 0.036, 8: 0.030, 9: 0.025, 10: 0.021
    }
    
    base_ctr = base_ctr_curve.get(organic_rank, 0.01)
    
    # Calculate adjusted CTR based on visual position
    # Each position of displacement reduces CTR
    position_gap = visual_position - organic_rank
    
    if position_gap <= 0:
        return 0.0
    
    # Approximate: each visual position down reduces CTR by ~15%
    ctr_multiplier = 0.85 ** position_gap
    adjusted_ctr = base_ctr * ctr_multiplier
    
    ctr_loss = base_ctr - adjusted_ctr
    
    # Additional penalties for specific features
    if serp_features.get("ai_overview"):
        ctr_loss *= 1.3  # AI overviews reduce CTR more
    
    if serp_features.get("featured_snippet") and organic_rank > 1:
        ctr_loss *= 1.2  # Featured snippet steals clicks
    
    return -ctr_loss


async def analyze_serp_landscape(
    gsc_keyword_data: pd.DataFrame,
    user_domain: str,
    top_n_keywords: int = 50,
    client: Optional[DataForSEOClient] = None
) -> Dict[str, Any]:
    """
    Main analysis function for Module 3.
    
    Args:
        gsc_keyword_data: DataFrame with columns [query, clicks, impressions, ctr, position]
        user_domain: User's root domain (for filtering)
        top_n_keywords: Number of top keywords to analyze
        client: DataForSEO client instance (will create if not provided)
        
    Returns:
        Structured analysis results
    """
    if gsc_keyword_data.empty:
        return {
            "error": "No GSC keyword data provided",
            "keywords_analyzed": 0
        }
    
    # Initialize client if not provided
    if client is None:
        try:
            client = DataForSEOClient()
        except ValueError as e:
            return {
                "error": str(e),
                "keywords_analyzed": 0
            }
    
    # Filter and sort keywords
    # Remove branded queries (containing user's domain name)
    domain_parts = user_domain.replace("www.", "").split(".")
    brand_term = domain_parts[0] if domain_parts else ""
    
    non_branded = gsc_keyword_data[
        ~gsc_keyword_data["query"].str.contains(brand_term, case=False, na=False)
    ].copy()
    
    # Sort by impressions and take top N
    top_keywords = non_branded.nlargest(top_n_keywords, "impressions")
    
    if top_keywords.empty:
        return {
            "error": "No non-branded keywords found",
            "keywords_analyzed": 0
        }
    
    keywords_list = top_keywords["query"].tolist()
    
    # Fetch SERP data
    try:
        serp_results = await client.get_serp_results(keywords_list)
    except Exception as e:
        return {
            "error": f"Failed to fetch SERP data: {str(e)}",
            "keywords_analyzed": 0
        }
    
    if not serp_results:
        return {
            "error": "No SERP results returned",
            "keywords_analyzed": 0
        }
    
    # Analysis containers
    serp_feature_displacement = []
    competitor_domains = Counter()
    competitor_positions = defaultdict(list)
    intent_classification = {}
    keyword_difficulty_scores = {}
    user_positions = {}
    
    # Process each SERP result
    for serp_data in serp_results:
        keyword = serp_data.get("keyword", "")
        
        if not keyword:
            continue
        
        # Extract data
        features = identify_serp_features(serp_data)
        organic_results = extract_organic_results(serp_data)
        
        if not organic_results:
            continue
        
        # Find user's position
        user_rank = None
        user_result = None
        
        for result in organic_results:
            if user_domain in result["domain"]:
                user_rank = result["position"]
                user_result = result
                user_positions[keyword] = user_rank
                break
        
        # Calculate visual position if user ranks
        if user_rank:
            visual_pos = calculate_visual_position(
                user_rank,
                features,
                serp_data.get("items", [])
            )
            
            # Flag significant displacement
            if visual_pos > user_rank + 2:
                features_above = []
                
                if features.get("featured_snippet"):
                    features_above.append("featured_snippet")
                if features.get("people_also_ask"):
                    features_above.append(f"paa_x{features['people_also_ask_count']}")
                if features.get("ai_overview"):
                    features_above.append("ai_overview")
                if features.get("local_pack"):
                    features_above.append("local_pack")
                if features.get("shopping_results"):
                    features_above.append("shopping_results")
                if features.get("video_carousel"):
                    features_above.append("video_carousel")
                
                ctr_impact = estimate_ctr_impact(user_rank, visual_pos, features)
                
                serp_feature_displacement.append({
                    "keyword": keyword,
                    "organic_position": user_rank,
                    "visual_position": round(visual_pos, 1),
                    "features_above": features_above,
                    "estimated_ctr_impact": round(ctr_impact, 4)
                })
        
        # Track competitors
        for result in organic_results:
            domain = result["domain"]
            if domain and domain != user_domain:
                competitor_domains[domain] += 1
                competitor_positions[domain].append(result["position"])
        
        # Classify intent
        intent = classify_search_intent(keyword, features, organic_results)
        intent_classification[keyword] = intent
        
        # Calculate keyword difficulty
        difficulty = calculate_keyword_difficulty(organic_results, features)
        keyword_difficulty_scores[keyword] = difficulty
    
    # Build competitor analysis
    total_keywords = len(serp_results)
    competitors = []
    
    for domain, count in competitor_domains.most_common(20):
        positions = competitor_positions[domain]
        avg_position = sum(positions) / len(positions)
        
        # Determine threat level
        if count > total_keywords * 0.3:
            threat_level = "high"
        elif count > total_keywords * 0.15:
            threat_level = "medium"
        else:
            threat_level = "low"
        
        competitors.append({
            "domain": domain,
            "keywords_shared": count,
            "overlap_pct": round(count / total_keywords * 100, 1),
            "avg_position": round(avg_position, 1),
            "threat_level": threat_level
        })
    
    # Calculate click share
    # This is a simplified estimation
    total_potential_clicks = 0
    user_estimated_clicks = 0
    
    for keyword in user_positions:
        # Get impressions from GSC data
        keyword_data = top_keywords[top_keywords["query"] == keyword]
        if keyword_data.empty:
            continue
        
        impressions = keyword_data.iloc[0]["impressions"]
        position = user_positions[keyword]
        
        # Estimate total clicks available for this keyword (sum of all CTRs)
        total_potential_clicks += impressions * 1.0  # Assume 100% capture if rank #1
        
        # Estimate user's actual clicks based on position
        base_ctr_curve = {
            1: 0.284, 2: 0.152, 3: 0.098, 4: 0.071, 5: 0.057,
            6: 0.045, 7: 0.036, 8: 0.030, 9: 0.025, 10: 0.021
        }
        ctr = base_ctr_curve.get(position, 0.01)
        user_estimated_clicks += impressions * ctr
    
    click_share = user_estimated_clicks / total_potential_clicks if total_potential_clicks > 0 else 0
    click_share_opportunity = 1.0 - click_share
    
    # Intent distribution
    intent_counts = Counter(intent_classification.values())
    
    # Average keyword difficulty
    avg_difficulty = sum(keyword_difficulty_scores.values()) / len(keyword_difficulty_scores) if keyword_difficulty_scores else 0
    
    return {
        "keywords_analyzed": len(serp_results),
        "user_domain": user_domain,
        "avg_keyword_difficulty": round(avg_difficulty, 1),
        "serp_feature_displacement": sorted(
            serp_feature_displacement,
            key=lambda x: abs(x["estimated_ctr_impact"]),
            reverse=True
        )[:20],  # Top 20 most affected
        "competitors": competitors,
        "intent_distribution": dict(intent_counts),
        "total_click_share": round(click_share, 3),
        "click_share_opportunity": round(click_share_opportunity, 3),
        "keyword_details": [
            {
                "keyword": keyword,
                "user_position": user_positions.get(keyword),
                "intent": intent_classification.get(keyword),
                "difficulty": round(keyword_difficulty_scores.get(keyword, 0), 1)
            }
            for keyword in keywords_list[:100]  # Limit to first 100
        ]
    }


# Convenience function for sync contexts
def analyze_serp_landscape_sync(
    gsc_keyword_data: pd.DataFrame,
    user_domain: str,
    top_n_keywords: int = 50,
    client: Optional[DataForSEOClient] = None
) -> Dict[str, Any]:
    """Synchronous wrapper for analyze_serp_landscape."""
    return asyncio.run(
        analyze_serp_landscape(gsc_keyword_data, user_domain, top_n_keywords, client)
    )
