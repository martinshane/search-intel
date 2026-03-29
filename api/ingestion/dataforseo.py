"""
DataForSEO API integration for SERP data retrieval.

This module handles all interactions with the DataForSEO API for fetching
live SERP data, including organic rankings, SERP features, and competitor analysis.

Estimated cost per report: $0.10-0.20 (50-100 keywords × $0.002/query)
"""

import os
import httpx
import asyncio
from typing import List, Dict, Any, Optional
from datetime import datetime
import hashlib
import json


class DataForSEOClient:
    """
    Client for interacting with DataForSEO API.
    
    Primary endpoints used:
    - /v3/serp/google/organic/live/advanced (live SERP data)
    - /v3/serp/google/organic/task_post (async SERP tasks)
    """
    
    def __init__(
        self,
        login: Optional[str] = None,
        password: Optional[str] = None,
        base_url: str = "https://api.dataforseo.com"
    ):
        """
        Initialize DataForSEO client.
        
        Args:
            login: DataForSEO API login (defaults to env var DATAFORSEO_LOGIN)
            password: DataForSEO API password (defaults to env var DATAFORSEO_PASSWORD)
            base_url: API base URL
        """
        self.login = login or os.getenv("DATAFORSEO_LOGIN")
        self.password = password or os.getenv("DATAFORSEO_PASSWORD")
        self.base_url = base_url
        
        if not self.login or not self.password:
            raise ValueError(
                "DataForSEO credentials not provided. "
                "Set DATAFORSEO_LOGIN and DATAFORSEO_PASSWORD environment variables."
            )
    
    def _get_auth(self) -> tuple:
        """Get HTTP basic auth tuple."""
        return (self.login, self.password)
    
    async def get_live_serp(
        self,
        keyword: str,
        location_code: int = 2840,  # United States
        language_code: str = "en",
        device: str = "desktop",
        depth: int = 100
    ) -> Dict[str, Any]:
        """
        Fetch live SERP data for a single keyword.
        
        Args:
            keyword: Search query
            location_code: DataForSEO location code (2840 = United States)
            language_code: Language code (en, es, etc.)
            device: Device type (desktop, mobile)
            depth: Number of results to retrieve (max 100)
        
        Returns:
            Dict containing SERP data with structure:
            {
                "keyword": str,
                "organic_results": List[Dict],
                "serp_features": Dict,
                "competitors": List[str],
                "timestamp": str
            }
        """
        endpoint = f"{self.base_url}/v3/serp/google/organic/live/advanced"
        
        payload = [{
            "keyword": keyword,
            "location_code": location_code,
            "language_code": language_code,
            "device": device,
            "depth": depth,
            "calculate_rectangles": True  # Get visual position data
        }]
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                response = await client.post(
                    endpoint,
                    auth=self._get_auth(),
                    json=payload
                )
                response.raise_for_status()
                
                data = response.json()
                
                if data.get("status_code") != 20000:
                    error_message = data.get("status_message", "Unknown error")
                    raise Exception(f"DataForSEO API error: {error_message}")
                
                # Parse and structure the response
                tasks = data.get("tasks", [])
                if not tasks or not tasks[0].get("result"):
                    return self._empty_serp_result(keyword)
                
                result = tasks[0]["result"][0]
                return self._parse_serp_result(keyword, result)
                
            except httpx.HTTPError as e:
                raise Exception(f"HTTP error fetching SERP data: {str(e)}")
            except Exception as e:
                raise Exception(f"Error fetching SERP data: {str(e)}")
    
    async def get_bulk_serps(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
        device: str = "desktop",
        max_concurrent: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Fetch SERP data for multiple keywords with rate limiting.
        
        Args:
            keywords: List of search queries
            location_code: DataForSEO location code
            language_code: Language code
            device: Device type
            max_concurrent: Maximum concurrent API requests
        
        Returns:
            List of SERP data dictionaries
        """
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def fetch_with_semaphore(keyword: str):
            async with semaphore:
                try:
                    return await self.get_live_serp(
                        keyword=keyword,
                        location_code=location_code,
                        language_code=language_code,
                        device=device
                    )
                except Exception as e:
                    print(f"Error fetching SERP for '{keyword}': {str(e)}")
                    return self._empty_serp_result(keyword)
        
        tasks = [fetch_with_semaphore(kw) for kw in keywords]
        results = await asyncio.gather(*tasks)
        
        return results
    
    def _parse_serp_result(self, keyword: str, result: Dict) -> Dict[str, Any]:
        """
        Parse DataForSEO SERP result into structured format.
        
        Extracts:
        - Organic results with positions
        - SERP features (featured snippets, PAA, video carousels, etc.)
        - Competitor domains
        - Visual position adjustments
        """
        items = result.get("items", [])
        
        organic_results = []
        serp_features = {
            "featured_snippet": None,
            "people_also_ask": [],
            "video_carousel": None,
            "local_pack": None,
            "knowledge_panel": None,
            "ai_overview": None,
            "image_pack": None,
            "shopping_results": None,
            "top_stories": None,
            "site_links": []
        }
        
        competitors = set()
        
        for item in items:
            item_type = item.get("type")
            
            if item_type == "organic":
                organic_result = {
                    "position": item.get("rank_group", 0),
                    "url": item.get("url", ""),
                    "domain": item.get("domain", ""),
                    "title": item.get("title", ""),
                    "description": item.get("description", ""),
                    "breadcrumb": item.get("breadcrumb", ""),
                }
                organic_results.append(organic_result)
                
                if item.get("domain"):
                    competitors.add(item["domain"])
            
            elif item_type == "featured_snippet":
                serp_features["featured_snippet"] = {
                    "domain": item.get("domain", ""),
                    "url": item.get("url", ""),
                    "title": item.get("title", ""),
                    "description": item.get("description", "")
                }
            
            elif item_type == "people_also_ask":
                serp_features["people_also_ask"].append({
                    "question": item.get("title", ""),
                    "expanded": item.get("expanded", {})
                })
            
            elif item_type == "video":
                if not serp_features["video_carousel"]:
                    serp_features["video_carousel"] = []
                serp_features["video_carousel"].append({
                    "title": item.get("title", ""),
                    "url": item.get("url", ""),
                    "domain": item.get("domain", "")
                })
            
            elif item_type == "local_pack":
                serp_features["local_pack"] = {
                    "present": True,
                    "count": len(item.get("items", []))
                }
            
            elif item_type == "knowledge_graph":
                serp_features["knowledge_panel"] = {
                    "present": True,
                    "title": item.get("title", "")
                }
            
            elif item_type == "ai_overview":
                serp_features["ai_overview"] = {
                    "present": True,
                    "text": item.get("text", "")
                }
            
            elif item_type == "images":
                serp_features["image_pack"] = {
                    "present": True,
                    "count": len(item.get("items", []))
                }
            
            elif item_type == "shopping":
                serp_features["shopping_results"] = {
                    "present": True,
                    "count": len(item.get("items", []))
                }
            
            elif item_type == "top_stories":
                serp_features["top_stories"] = {
                    "present": True,
                    "count": len(item.get("items", []))
                }
        
        return {
            "keyword": keyword,
            "organic_results": organic_results,
            "serp_features": serp_features,
            "competitors": list(competitors),
            "timestamp": datetime.utcnow().isoformat(),
            "total_results": result.get("items_count", 0)
        }
    
    def _empty_serp_result(self, keyword: str) -> Dict[str, Any]:
        """Return empty SERP result structure on error."""
        return {
            "keyword": keyword,
            "organic_results": [],
            "serp_features": {},
            "competitors": [],
            "timestamp": datetime.utcnow().isoformat(),
            "total_results": 0,
            "error": True
        }
    
    def generate_cache_key(
        self,
        keyword: str,
        location_code: int,
        language_code: str,
        device: str
    ) -> str:
        """
        Generate cache key for SERP data.
        
        Used to avoid duplicate API calls within the same report generation.
        """
        cache_data = f"{keyword}:{location_code}:{language_code}:{device}"
        return hashlib.md5(cache_data.encode()).hexdigest()
    
    def calculate_visual_position(
        self,
        organic_position: int,
        serp_features: Dict[str, Any]
    ) -> float:
        """
        Calculate visual position accounting for SERP features above the result.
        
        SERP features displace organic results visually:
        - Featured snippet: +2 positions
        - AI Overview: +3 positions
        - People Also Ask (each): +0.5 positions
        - Video carousel: +1 position
        - Image pack: +0.5 positions
        - Shopping results: +1 position
        - Local pack: +1.5 positions
        - Knowledge panel: (side, no displacement)
        
        Args:
            organic_position: Actual organic ranking position
            serp_features: Dict of SERP features present
        
        Returns:
            Visual position as float (e.g., organic #3 might be visual #7.5)
        """
        displacement = 0.0
        
        if serp_features.get("featured_snippet"):
            displacement += 2.0
        
        if serp_features.get("ai_overview"):
            displacement += 3.0
        
        paa_count = len(serp_features.get("people_also_ask", []))
        displacement += paa_count * 0.5
        
        if serp_features.get("video_carousel"):
            displacement += 1.0
        
        if serp_features.get("image_pack"):
            displacement += 0.5
        
        if serp_features.get("shopping_results"):
            displacement += 1.0
        
        if serp_features.get("local_pack"):
            displacement += 1.5
        
        # Top stories typically appear above organic results
        if serp_features.get("top_stories"):
            displacement += 1.0
        
        return organic_position + displacement


# Utility functions for keyword selection

def select_keywords_for_serp_analysis(
    gsc_query_data: List[Dict[str, Any]],
    brand_terms: List[str],
    max_keywords: int = 100
) -> List[str]:
    """
    Select top keywords for SERP analysis.
    
    Prioritizes:
    1. Non-branded keywords (filter out brand name matches)
    2. High impression volume
    3. Keywords with recent position changes (>3 positions in last 30 days)
    
    Args:
        gsc_query_data: List of GSC query performance data
        brand_terms: List of brand names/terms to filter out
        max_keywords: Maximum number of keywords to analyze
    
    Returns:
        List of selected keywords
    """
    from rapidfuzz import fuzz
    
    # Filter out branded queries
    non_branded = []
    for query_data in gsc_query_data:
        query = query_data.get("query", "").lower()
        
        # Check if query contains brand terms
        is_branded = any(
            fuzz.partial_ratio(query, brand.lower()) > 85
            for brand in brand_terms
        )
        
        if not is_branded:
            non_branded.append(query_data)
    
    # Sort by impressions descending
    non_branded.sort(key=lambda x: x.get("impressions", 0), reverse=True)
    
    # Take top N
    selected = [q.get("query") for q in non_branded[:max_keywords]]
    
    return selected


def estimate_serp_api_cost(keyword_count: int, cost_per_query: float = 0.002) -> float:
    """
    Estimate DataForSEO API cost for SERP analysis.
    
    Args:
        keyword_count: Number of keywords to analyze
        cost_per_query: Cost per SERP query (default $0.002)
    
    Returns:
        Estimated cost in USD
    """
    return keyword_count * cost_per_query
