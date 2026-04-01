"""
Module 3: SERP Features & CTR Modeling

Analyzes SERP features and CTR performance using DataForSEO API to:
1. Fetch top keywords from GSC data
2. Get SERP features for each keyword (featured snippets, PAA, local packs, etc.)
3. Calculate expected vs actual CTR based on position and SERP features
4. Identify CTR opportunity gaps
5. Provide keyword-level SERP feature analysis and aggregate CTR impact summary
"""

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
                
                elif response.status == 401:
                    raise Exception("Authentication failed - check DataForSEO credentials")
                
                else:
                    raise Exception(f"HTTP error {response.status}")
        
        except aiohttp.ClientError as e:
            if retry_count < self.MAX_RETRIES:
                await asyncio.sleep(self.RETRY_DELAY * (retry_count + 1))
                return await self._make_request(session, endpoint, payload, retry_count + 1)
            else:
                raise Exception(f"Request failed after {self.MAX_RETRIES} retries: {str(e)}")

    async def get_serp_results(
        self,
        keywords: List[str],
        location_code: int = 2840,  # United States
        language_code: str = "en",
        device: str = "desktop"
    ) -> Dict[str, Any]:
        """
        Fetch SERP results for multiple keywords.
        
        Args:
            keywords: List of keywords to analyze
            location_code: DataForSEO location code
            language_code: Language code
            device: Device type (desktop, mobile)
        
        Returns:
            Dictionary mapping keywords to SERP results
        """
        results = {}
        
        async with aiohttp.ClientSession() as session:
            for keyword in keywords:
                payload = [{
                    "keyword": keyword,
                    "location_code": location_code,
                    "language_code": language_code,
                    "device": device,
                    "os": "windows" if device == "desktop" else "ios",
                    "depth": 100  # Get top 100 results
                }]
                
                try:
                    response = await self._make_request(
                        session,
                        "/serp/google/organic/live/advanced",
                        payload
                    )
                    
                    if response.get("tasks") and len(response["tasks"]) > 0:
                        task = response["tasks"][0]
                        if task.get("result") and len(task["result"]) > 0:
                            results[keyword] = task["result"][0]
                        else:
                            results[keyword] = {"error": "No results returned"}
                    else:
                        results[keyword] = {"error": "No tasks in response"}
                
                except Exception as e:
                    results[keyword] = {"error": str(e)}
                
                # Small delay between keywords
                await asyncio.sleep(0.5)
        
        return results


class CTRModelCalculator:
    """Calculate expected CTR based on position and SERP features."""
    
    # Base CTR curve by position (desktop, no SERP features)
    # Based on industry averages
    BASE_CTR = {
        1: 0.316,
        2: 0.158,
        3: 0.108,
        4: 0.079,
        5: 0.061,
        6: 0.049,
        7: 0.040,
        8: 0.034,
        9: 0.029,
        10: 0.025,
        11: 0.020,
        12: 0.017,
        13: 0.014,
        14: 0.012,
        15: 0.011,
        16: 0.009,
        17: 0.008,
        18: 0.007,
        19: 0.006,
        20: 0.006
    }
    
    # SERP feature impact multipliers (negative = reduces CTR)
    FEATURE_IMPACT = {
        "featured_snippet": -0.30,  # Reduces CTR by 30%
        "answer_box": -0.25,
        "knowledge_panel": -0.15,
        "local_pack": -0.20,
        "people_also_ask": -0.05,  # Per PAA box
        "video_carousel": -0.10,
        "image_pack": -0.08,
        "shopping_results": -0.12,
        "top_stories": -0.10,
        "twitter": -0.05,
        "ai_overview": -0.35,  # SGE/AI overview has major impact
        "site_links": 0.15,  # Positive if it's YOUR site links
        "reviews": -0.08,
        "map": -0.15
    }
    
    @classmethod
    def get_base_ctr(cls, position: int) -> float:
        """Get base CTR for a position."""
        if position <= 0:
            return 0.0
        if position in cls.BASE_CTR:
            return cls.BASE_CTR[position]
        
        # Exponential decay for positions > 20
        if position > 20:
            return cls.BASE_CTR[20] * (0.9 ** (position - 20))
        
        # Interpolate for missing positions
        lower = max([p for p in cls.BASE_CTR.keys() if p < position])
        upper = min([p for p in cls.BASE_CTR.keys() if p > position])
        ratio = (position - lower) / (upper - lower)
        return cls.BASE_CTR[lower] * (1 - ratio) + cls.BASE_CTR[upper] * ratio
    
    @classmethod
    def calculate_expected_ctr(
        cls,
        position: int,
        serp_features: List[str],
        owns_feature: Optional[str] = None
    ) -> Tuple[float, Dict[str, Any]]:
        """
        Calculate expected CTR considering SERP features.
        
        Args:
            position: Organic ranking position
            serp_features: List of SERP features present
            owns_feature: Feature owned by the site (e.g., "featured_snippet")
        
        Returns:
            Tuple of (expected_ctr, breakdown)
        """
        base_ctr = cls.get_base_ctr(position)
        adjusted_ctr = base_ctr
        
        feature_impacts = {}
        total_impact = 0.0
        
        for feature in serp_features:
            if feature in cls.FEATURE_IMPACT:
                impact = cls.FEATURE_IMPACT[feature]
                
                # If we own this feature, it's a positive impact
                if feature == owns_feature:
                    impact = abs(impact) * 1.5  # Owning a feature is better than base
                
                feature_impacts[feature] = impact
                total_impact += impact
        
        # Apply cumulative impact (multiplicative)
        adjusted_ctr = base_ctr * (1 + total_impact)
        
        # CTR can't be negative or > 1
        adjusted_ctr = max(0.0, min(1.0, adjusted_ctr))
        
        breakdown = {
            "base_ctr": round(base_ctr, 4),
            "adjusted_ctr": round(adjusted_ctr, 4),
            "total_impact": round(total_impact, 4),
            "feature_impacts": {k: round(v, 4) for k, v in feature_impacts.items()}
        }
        
        return adjusted_ctr, breakdown


class SERPFeatureAnalyzer:
    """Analyze SERP features from DataForSEO results."""
    
    @staticmethod
    def extract_serp_features(serp_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract SERP features from DataForSEO result.
        
        Returns:
            Dictionary with feature types and details
        """
        features = {
            "total_features": 0,
            "features_list": [],
            "feature_details": {},
            "organic_results_count": 0,
            "visual_position_offset": 0
        }
        
        if "error" in serp_result:
            features["error"] = serp_result["error"]
            return features
        
        items = serp_result.get("items", [])
        
        position_offset = 0
        
        for item in items:
            item_type = item.get("type", "")
            rank_group = item.get("rank_group")
            rank_absolute = item.get("rank_absolute", 0)
            
            # Count organic results
            if item_type == "organic":
                features["organic_results_count"] += 1
                continue
            
            # Skip ads for now
            if "paid" in item_type.lower() or "ad" in item_type.lower():
                continue
            
            # Map DataForSEO types to our feature names
            feature_map = {
                "featured_snippet": "featured_snippet",
                "answer_box": "answer_box",
                "knowledge_graph": "knowledge_panel",
                "local_pack": "local_pack",
                "people_also_ask": "people_also_ask",
                "video": "video_carousel",
                "images": "image_pack",
                "shopping": "shopping_results",
                "top_stories": "top_stories",
                "twitter": "twitter",
                "carousel": "carousel",
                "map": "map",
                "recipes": "recipes",
                "hotels_pack": "hotels",
                "flights": "flights",
                "jobs": "jobs"
            }
            
            feature_name = feature_map.get(item_type, item_type)
            
            if feature_name and feature_name not in features["features_list"]:
                features["features_list"].append(feature_name)
                features["total_features"] += 1
                
                # Calculate visual position offset
                # Each feature pushes organic results down
                if item_type == "featured_snippet":
                    position_offset += 2.0
                elif item_type == "people_also_ask":
                    # Count number of questions
                    paa_items = item.get("items", [])
                    position_offset += len(paa_items) * 0.5
                elif item_type in ["local_pack", "knowledge_graph"]:
                    position_offset += 1.5
                elif item_type in ["video", "images", "shopping", "top_stories"]:
                    position_offset += 1.0
                else:
                    position_offset += 0.5
                
                # Store feature details
                features["feature_details"][feature_name] = {
                    "type": item_type,
                    "rank_group": rank_group,
                    "rank_absolute": rank_absolute
                }
                
                # Special handling for PAA
                if item_type == "people_also_ask":
                    paa_items = item.get("items", [])
                    features["feature_details"][feature_name]["question_count"] = len(paa_items)
                    features["feature_details"][feature_name]["questions"] = [
                        q.get("title") for q in paa_items[:5]  # First 5 questions
                    ]
        
        features["visual_position_offset"] = round(position_offset, 1)
        
        return features
    
    @staticmethod
    def find_site_ranking(
        serp_result: Dict[str, Any],
        site_domain: str
    ) -> Optional[Dict[str, Any]]:
        """
        Find the site's ranking in SERP results.
        
        Returns:
            Dictionary with ranking info or None if not found
        """
        if "error" in serp_result:
            return None
        
        items = serp_result.get("items", [])
        
        # Normalize domain
        site_domain = site_domain.lower().replace("www.", "")
        
        for item in items:
            if item.get("type") != "organic":
                continue
            
            url = item.get("url", "")
            domain = item.get("domain", "")
            
            # Check if this is the user's site
            if site_domain in domain.lower() or site_domain in url.lower():
                return {
                    "position": item.get("rank_absolute"),
                    "url": url,
                    "title": item.get("title"),
                    "description": item.get("description"),
                    "domain": domain,
                    "has_sitelinks": bool(item.get("links")),
                    "has_rating": bool(item.get("rating"))
                }
        
        return None


def extract_domain_from_property(gsc_property: str) -> str:
    """Extract domain from GSC property URL."""
    if gsc_property.startswith("sc-domain:"):
        return gsc_property.replace("sc-domain:", "")
    
    parsed = urlparse(gsc_property)
    domain = parsed.netloc or parsed.path
    return domain.replace("www.", "")


def select_top_keywords(
    gsc_data: pd.DataFrame,
    site_domain: str,
    limit: int = 20,
    min_impressions: int = 100
) -> List[Dict[str, Any]]:
    """
    Select top keywords for SERP analysis.
    
    Filters out branded keywords and selects by impressions.
    
    Args:
        gsc_data: DataFrame with GSC query data (columns: query, clicks, impressions, ctr, position)
        site_domain: Site domain for filtering branded queries
        limit: Maximum number of keywords to return
        min_impressions: Minimum impressions threshold
    
    Returns:
        List of keyword dictionaries with GSC metrics
    """
    if gsc_data.empty:
        return []
    
    # Ensure required columns exist
    required_cols = ["query", "clicks", "impressions", "ctr", "position"]
    if not all(col in gsc_data.columns for col in required_cols):
        raise ValueError(f"GSC data must contain columns: {required_cols}")
    
    # Filter out branded queries
    brand_terms = site_domain.replace(".com", "").replace(".org", "").replace(".net", "").split(".")
    
    def is_branded(query: str) -> bool:
        query_lower = query.lower()
        return any(term.lower() in query_lower for term in brand_terms if len(term) > 3)
    
    # Filter and sort
    filtered_df = gsc_data[
        (gsc_data["impressions"] >= min_impressions) &
        (~gsc_data["query"].apply(is_branded))
    ].copy()
    
    # Sort by impressions
    filtered_df = filtered_df.sort_values("impressions", ascending=False)
    
    # Take top N
    top_keywords = filtered_df.head(limit)
    
    # Convert to list of dicts
    result = []
    for _, row in top_keywords.iterrows():
        result.append({
            "keyword": row["query"],
            "clicks": int(row["clicks"]),
            "impressions": int(row["impressions"]),
            "ctr": float(row["ctr"]),
            "position": float(row["position"])
        })
    
    return result


async def analyze_serp_features_and_ctr(
    gsc_data: pd.DataFrame,
    gsc_property: str,
    location_code: int = 2840,
    language_code: str = "en",
    device: str = "desktop",
    limit: int = 20
) -> Dict[str, Any]:
    """
    Main analysis function for Module 3: SERP Features & CTR Modeling.
    
    Args:
        gsc_data: DataFrame with GSC query data
        gsc_property: GSC property URL
        location_code: DataForSEO location code
        language_code: Language code
        device: Device type
        limit: Number of top keywords to analyze
    
    Returns:
        Complete SERP features and CTR analysis results
    """
    start_time = time.time()
    
    # Extract site domain
    site_domain = extract_domain_from_property(gsc_property)
    
    # Select top keywords
    try:
        top_keywords = select_top_keywords(gsc_data, site_domain, limit=limit)
    except Exception as e:
        return {
            "error": f"Failed to select keywords: {str(e)}",
            "keywords_analyzed": 0
        }
    
    if not top_keywords:
        return {
            "error": "No keywords found matching criteria",
            "keywords_analyzed": 0
        }
    
    # Initialize DataForSEO client
    try:
        client = DataForSEOClient()
    except Exception as e:
        return {
            "error": f"Failed to initialize DataForSEO client: {str(e)}",
            "keywords_analyzed": 0
        }
    
    # Fetch SERP data
    keywords_list = [kw["keyword"] for kw in top_keywords]
    
    try:
        serp_results = await client.get_serp_results(
            keywords_list,
            location_code=location_code,
            language_code=language_code,
            device=device
        )
    except Exception as e:
        return {
            "error": f"Failed to fetch SERP data: {str(e)}",
            "keywords_analyzed": 0
        }
    
    # Analyze each keyword
    analyzer = SERPFeatureAnalyzer()
    calculator = CTRModelCalculator()
    
    keyword_analyses = []
    total_ctr_gap = 0.0
    total_impressions = 0
    total_expected_clicks = 0.0
    total_actual_clicks = 0
    
    features_counter = Counter()
    displacement_cases = []
    
    for kw_data in top_keywords:
        keyword = kw_data["keyword"]
        gsc_position = kw_data["position"]
        gsc_ctr = kw_data["ctr"]
        impressions = kw_data["impressions"]
        actual_clicks = kw_data["clicks"]
        
        serp_result = serp_results.get(keyword, {})
        
        # Extract SERP features
        features = analyzer.extract_serp_features(serp_result)
        
        # Find site's ranking
        site_ranking = analyzer.find_site_ranking(serp_result, site_domain)
        
        # Determine if site owns any features
        owns_feature = None
        if site_ranking:
            serp_position = site_ranking["position"]
            # Check if site has featured snippet (position 0 or has_featured indicator)
            if serp_position == 0 or (serp_position == 1 and "featured_snippet" in features["features_list"]):
                owns_feature = "featured_snippet"
        else:
            serp_position = gsc_position  # Fallback to GSC position
        
        # Calculate expected CTR
        expected_ctr, ctr_breakdown = calculator.calculate_expected_ctr(
            int(round(gsc_position)),
            features["features_list"],
            owns_feature
        )
        
        # Calculate CTR gap
        ctr_gap = gsc_ctr - expected_ctr
        ctr_gap_pct = (ctr_gap / expected_ctr * 100) if expected_ctr > 0 else 0
        
        # Calculate visual position
        visual_position = gsc_position + features["visual_position_offset"]
        
        # Expected clicks
        expected_clicks = impressions * expected_ctr
        
        # Accumulate totals
        total_ctr_gap += (ctr_gap * impressions)
        total_impressions += impressions
        total_expected_clicks += expected_clicks
        total_actual_clicks += actual_clicks
        
        # Count features
        for feature in features["features_list"]:
            features_counter[feature] += 1
        
        # Check for significant displacement
        if visual_position - gsc_position > 2:
            displacement_cases.append({
                "keyword": keyword,
                "organic_position": gsc_position,
                "visual_position": round(visual_position, 1),
                "displacement": round(visual_position - gsc_position, 1),
                "features": features["features_list"],
                "impressions": impressions,
                "estimated_ctr_impact": round(ctr_gap, 4)
            })
        
        # Compile keyword analysis
        analysis = {
            "keyword": keyword,
            "gsc_metrics": {
                "position": round(gsc_position, 1),
                "clicks": actual_clicks,
                "impressions": impressions,
                "ctr": round(gsc_ctr, 4)
            },
            "serp_features": {
                "total_features": features["total_features"],
                "features_present": features["features_list"],
                "visual_position": round(visual_position, 1),
                "displacement": round(visual_position - gsc_position, 1)
            },
            "ctr_analysis": {
                "expected_ctr": round(expected_ctr, 4),
                "actual_ctr": round(gsc_ctr, 4),
                "ctr_gap": round(ctr_gap, 4),
                "ctr_gap_pct": round(ctr_gap_pct, 1),
                "expected_clicks": round(expected_clicks, 1),
                "actual_clicks": actual_clicks,
                "click_gap": round(actual_clicks - expected_clicks, 1),
                "breakdown": ctr_breakdown
            },
            "site_ranking": site_ranking,
            "owns_feature": owns_feature,
            "opportunity_type": None
        }
        
        # Classify opportunity
        if ctr_gap < -0.02:  # CTR more than 2% below expected
            if not site_ranking or visual_position > gsc_position + 2:
                analysis["opportunity_type"] = "feature_displacement"
            else:
                analysis["opportunity_type"] = "title_snippet_optimization"
        elif ctr_gap > 0.02 and gsc_position > 5:
            analysis["opportunity_type"] = "content_improvement"  # Doing well, could rank higher
        
        keyword_analyses.append(analysis)
    
    # Sort displacement cases by impact
    displacement_cases.sort(key=lambda x: abs(x["estimated_ctr_impact"]) * x["impressions"], reverse=True)
    
    # Calculate aggregate metrics
    avg_ctr_gap = (total_ctr_gap / total_impressions) if total_impressions > 0 else 0
    total_click_gap = total_actual_clicks - total_expected_clicks
    
    # Identify most common features
    top_features = [
        {"feature": feature, "count": count, "percentage": round(count / len(keyword_analyses) * 100, 1)}
        for feature, count in features_counter.most_common(10)
    ]
    
    # Classify opportunities
    opportunities = {
        "feature_displacement": [],
        "title_snippet_optimization": [],
        "content_improvement": [],
        "feature_capture": []
    }
    
    for analysis in keyword_analyses:
        if analysis["opportunity_type"]:
            opp = {
                "keyword": analysis["keyword"],
                "impressions": analysis["gsc_metrics"]["impressions"],
                "click_gap": analysis["ctr_analysis"]["click_gap"],
                "ctr_gap_pct": analysis["ctr_analysis"]["ctr_gap_pct"]
            }
            opportunities[analysis["opportunity_type"]].append(opp)
        
        # Check for feature capture opportunities
        if "featured_snippet" in analysis["serp_features"]["features_present"] and not analysis["owns_feature"]:
            if analysis["gsc_metrics"]["position"] <= 5:  # In striking distance
                opportunities["feature_capture"].append({
                    "keyword": analysis["keyword"],
                    "feature": "featured_snippet",
                    "current_position": analysis["gsc_metrics"]["position"],
                    "impressions": analysis["gsc_metrics"]["impressions"],
                    "potential_ctr_gain": 0.20  # Approximate gain from capturing snippet
                })
    
    # Sort opportunities by impact
    for opp_type in opportunities:
        opportunities[opp_type].sort(
            key=lambda x: abs(x.get("click_gap", 0)) if "click_gap" in x else x.get("impressions", 0) * x.get("potential_ctr_gain", 0),
            reverse=True
        )
    
    execution_time = time.time() - start_time
    
    return {
        "module": "serp_features_ctr_modeling",
        "generated_at": datetime.now().isoformat(),
        "execution_time_seconds": round(execution_time, 2),
        "keywords_analyzed": len(keyword_analyses),
        "keywords": keyword_analyses,
        "summary": {
            "total_impressions": total_impressions,
            "total_actual_clicks": total_actual_clicks,
            "total_expected_clicks": round(total_expected_clicks, 1),
            "total_click_gap": round(total_click_gap, 1),
            "avg_ctr_gap_pct": round(avg_ctr_gap * 100, 2),
            "click_recovery_potential": round(abs(min(0, total_click_gap)), 1),
            "top_serp_features": top_features,
            "displacement_cases_count": len(displacement_cases)
        },
        "serp_feature_displacement": displacement_cases[:10],  # Top 10
        "opportunities": {
            "feature_displacement": opportunities["feature_displacement"][:10],
            "title_snippet_optimization": opportunities["title_snippet_optimization"][:10],
            "content_improvement": opportunities["content_improvement"][:10],
            "feature_capture": opportunities["feature_capture"][:5]
        },
        "recommendations": generate_recommendations(
            keyword_analyses,
            opportunities,
            displacement_cases,
            top_features
        )
    }


def generate_recommendations(
    keyword_analyses: List[Dict[str, Any]],
    opportunities: Dict[str, List[Dict[str, Any]]],
    displacement_cases: List[Dict[str, Any]],
    top_features: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """Generate actionable recommendations based on analysis."""
    recommendations = []
    
    # Recommendation 1: Address severe displacement
    if displacement_cases:
        severe_displacement = [d for d in displacement_cases if d["displacement"] > 3]
        if severe_displacement:
            total_impressions = sum(d["impressions"] for d in severe_displacement)
            recommendations.append({
                "priority": "high",
                "category": "feature_displacement",
                "title": "Address SERP Feature Displacement",
                "description": f"{len(severe_displacement)} keywords are severely displaced by SERP features, affecting {total_impressions:,} monthly impressions",
                "action": "Focus on capturing featured snippets and optimizing for PAA boxes. Consider FAQ schema implementation.",
                "estimated_impact": f"{int(total_impressions * 0.15):,} potential monthly clicks",
                "keywords": [d["keyword"] for d in severe_displacement[:5]]
            })
    
    # Recommendation 2: Feature capture opportunities
    feature_capture = opportunities.get("feature_capture", [])
    if feature_capture:
        total_potential = sum(k["impressions"] * k["potential_ctr_gain"] for k in feature_capture[:10])
        recommendations.append({
            "priority": "high",
            "category": "feature_capture",
            "title": "Capture Featured Snippets",
            "description": f"{len(feature_capture)} keywords in positions 1-5 where competitors own featured snippets",
            "action": "Restructure content to target featured snippet formats: add concise definitions, use lists/tables, implement FAQ schema",
            "estimated_impact": f"{int(total_potential):,} potential monthly clicks",
            "keywords": [k["keyword"] for k in feature_capture[:5]]
        })
    
    # Recommendation 3: Title/snippet optimization
    title_opt = opportunities.get("title_snippet_optimization", [])
    if title_opt:
        total_gap = sum(abs(k["click_gap"]) for k in title_opt[:10])
        recommendations.append({
            "priority": "medium",
            "category": "title_optimization",
            "title": "Optimize Titles and Meta Descriptions",
            "description": f"{len(title_opt)} keywords with CTR significantly below expected for their position",
            "action": "A/B test more compelling titles and descriptions. Add power words, numbers, and clear value propositions.",
            "estimated_impact": f"{int(total_gap):,} recoverable monthly clicks",
            "keywords": [k["keyword"] for k in title_opt[:5]]
        })
    
    # Recommendation 4: Content improvement for high performers
    content_imp = opportunities.get("content_improvement", [])
    if content_imp:
        high_potential = [k for k in content_imp if k["impressions"] > 500]
        if high_potential:
            recommendations.append({
                "priority": "medium",
                "category": "content_improvement",
                "title": "Expand High-Performing Content",
                "description": f"{len(high_potential)} keywords performing above expected CTR but ranked outside top 5",
                "action": "These pages are resonating with users. Expand content depth, add internal links, and build backlinks to push into top 5.",
                "estimated_impact": "Potential to double click volume on these keywords",
                "keywords": [k["keyword"] for k in high_potential[:5]]
            })
    
    # Recommendation 5: PAA optimization if prevalent
    paa_feature = next((f for f in top_features if f["feature"] == "people_also_ask"), None)
    if paa_feature and paa_feature["percentage"] > 50:
        recommendations.append({
            "priority": "medium",
            "category": "paa_optimization",
            "title": "Optimize for People Also Ask",
            "description": f"PAA boxes appear in {paa_feature['percentage']}% of your top keywords",
            "action": "Add FAQ sections addressing common PAA questions. Use FAQ schema markup to increase chances of appearing in PAA.",
            "estimated_impact": "15-25% CTR boost on affected keywords",
            "keywords": []
        })
    
    return recommendations


# Synchronous wrapper for FastAPI
def run_serp_features_analysis(
    gsc_data: pd.DataFrame,
    gsc_property: str,
    location_code: int = 2840,
    language_code: str = "en",
    device: str = "desktop",
    limit: int = 20
) -> Dict[str, Any]:
    """Synchronous wrapper for the async analysis function."""
    return asyncio.run(
        analyze_serp_features_and_ctr(
            gsc_data,
            gsc_property,
            location_code,
            language_code,
            device,
            limit
        )
    )
