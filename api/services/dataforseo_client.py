import os
import asyncio
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
import httpx
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

logger = logging.getLogger(__name__)


class DataForSEOError(Exception):
    """Base exception for DataForSEO API errors"""
    pass


class DataForSEORateLimitError(DataForSEOError):
    """Raised when rate limit is exceeded"""
    pass


class DataForSEOClient:
    """
    Async client for DataForSEO API with rate limiting, retries, and error handling.
    
    Supports:
    - Live SERP results retrieval
    - Competitor analysis (top ranking domains)
    - SERP feature detection (featured snippets, PAA, knowledge panels, etc.)
    - Batch processing of multiple keywords
    """
    
    BASE_URL = "https://api.dataforseo.com/v3"
    
    def __init__(
        self,
        login: Optional[str] = None,
        password: Optional[str] = None,
        timeout: int = 60,
        max_retries: int = 3,
    ):
        """
        Initialize DataForSEO client.
        
        Args:
            login: DataForSEO login (defaults to DATAFORSEO_LOGIN env var)
            password: DataForSEO password (defaults to DATAFORSEO_PASSWORD env var)
            timeout: Request timeout in seconds
            max_retries: Maximum number of retry attempts
        """
        self.login = login or os.getenv("DATAFORSEO_LOGIN")
        self.password = password or os.getenv("DATAFORSEO_PASSWORD")
        
        if not self.login or not self.password:
            raise ValueError(
                "DataForSEO credentials not provided. "
                "Set DATAFORSEO_LOGIN and DATAFORSEO_PASSWORD environment variables."
            )
        
        self.timeout = timeout
        self.max_retries = max_retries
        self.auth = (self.login, self.password)
        
        # Rate limiting: DataForSEO allows ~2000 API units/minute
        # Conservative semaphore to prevent hammering
        self._semaphore = asyncio.Semaphore(50)
        self._last_request_time = 0
        self._min_request_interval = 0.03  # 30ms between requests
    
    async def authenticate(self) -> bool:
        """
        Test authentication by making a simple API call.
        
        Returns:
            True if authentication successful, False otherwise
        """
        try:
            async with httpx.AsyncClient(auth=self.auth, timeout=self.timeout) as client:
                response = await client.get(f"{self.BASE_URL}/user_data")
                
                if response.status_code == 200:
                    data = response.json()
                    if data.get("status_code") == 20000:
                        logger.info("DataForSEO authentication successful")
                        return True
                    else:
                        logger.error(f"DataForSEO auth failed: {data.get('status_message')}")
                        return False
                else:
                    logger.error(f"DataForSEO auth failed with status {response.status_code}")
                    return False
        except Exception as e:
            logger.error(f"DataForSEO authentication error: {e}")
            return False
    
    async def _rate_limit(self):
        """Enforce rate limiting between requests"""
        current_time = asyncio.get_event_loop().time()
        time_since_last = current_time - self._last_request_time
        if time_since_last < self._min_request_interval:
            await asyncio.sleep(self._min_request_interval - time_since_last)
        self._last_request_time = asyncio.get_event_loop().time()
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((httpx.HTTPError, DataForSEOError)),
    )
    async def _make_request(
        self,
        endpoint: str,
        method: str = "POST",
        data: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """
        Make an authenticated request to DataForSEO API with retry logic.
        
        Args:
            endpoint: API endpoint path
            method: HTTP method (GET or POST)
            data: Request payload for POST requests
            
        Returns:
            API response as dictionary
            
        Raises:
            DataForSEORateLimitError: If rate limit is exceeded
            DataForSEOError: For other API errors
        """
        async with self._semaphore:
            await self._rate_limit()
            
            url = f"{self.BASE_URL}/{endpoint}"
            
            async with httpx.AsyncClient(auth=self.auth, timeout=self.timeout) as client:
                try:
                    if method == "POST":
                        response = await client.post(url, json=data)
                    else:
                        response = await client.get(url)
                    
                    response.raise_for_status()
                    result = response.json()
                    
                    # Check DataForSEO-specific status codes
                    if result.get("status_code") == 40000:
                        # Rate limit error
                        raise DataForSEORateLimitError(result.get("status_message"))
                    elif result.get("status_code") != 20000:
                        # Other API errors
                        raise DataForSEOError(
                            f"API error: {result.get('status_message')} "
                            f"(code: {result.get('status_code')})"
                        )
                    
                    return result
                    
                except httpx.HTTPStatusError as e:
                    logger.error(f"HTTP error: {e.response.status_code} - {e.response.text}")
                    raise DataForSEOError(f"HTTP {e.response.status_code}: {e.response.text}")
                except httpx.HTTPError as e:
                    logger.error(f"HTTP transport error: {e}")
                    raise
    
    async def get_serp_results(
        self,
        keyword: str,
        location_code: int = 2840,  # United States
        language_code: str = "en",
        device: str = "desktop",
        depth: int = 100,
    ) -> Dict[str, Any]:
        """
        Get live SERP results for a single keyword.
        
        Args:
            keyword: Search query
            location_code: DataForSEO location code (2840 = US)
            language_code: Language code (ISO 639-1)
            device: Device type (desktop, mobile, tablet)
            depth: Number of results to retrieve (max 100)
            
        Returns:
            Parsed SERP data with organic results and features
        """
        payload = [
            {
                "keyword": keyword,
                "location_code": location_code,
                "language_code": language_code,
                "device": device,
                "depth": depth,
            }
        ]
        
        result = await self._make_request("serp/google/organic/live/advanced", data=payload)
        
        if result.get("tasks") and len(result["tasks"]) > 0:
            task = result["tasks"][0]
            if task.get("result") and len(task["result"]) > 0:
                return self._parse_serp_result(task["result"][0])
        
        return {}
    
    async def get_batch_serp_results(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
        device: str = "desktop",
        depth: int = 100,
        batch_size: int = 10,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Get SERP results for multiple keywords in batches.
        
        Args:
            keywords: List of search queries
            location_code: DataForSEO location code
            language_code: Language code
            device: Device type
            depth: Number of results per keyword
            batch_size: Number of keywords per API request
            
        Returns:
            Dictionary mapping keywords to parsed SERP data
        """
        results = {}
        
        # Process in batches to avoid overwhelming API
        for i in range(0, len(keywords), batch_size):
            batch = keywords[i:i + batch_size]
            
            payload = [
                {
                    "keyword": kw,
                    "location_code": location_code,
                    "language_code": language_code,
                    "device": device,
                    "depth": depth,
                }
                for kw in batch
            ]
            
            try:
                result = await self._make_request("serp/google/organic/live/advanced", data=payload)
                
                if result.get("tasks"):
                    for task in result["tasks"]:
                        if task.get("result") and len(task["result"]) > 0:
                            keyword_used = task["data"]["keyword"]
                            results[keyword_used] = self._parse_serp_result(task["result"][0])
            except Exception as e:
                logger.error(f"Error processing batch {i//batch_size + 1}: {e}")
                # Continue with next batch
        
        return results
    
    def _parse_serp_result(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse raw SERP result into structured format.
        
        Extracts:
        - Organic results (position, URL, title, description)
        - SERP features (featured snippet, PAA, knowledge panel, etc.)
        - Competitor domains
        - Click share estimation factors
        
        Args:
            result: Raw result from DataForSEO
            
        Returns:
            Structured SERP data
        """
        parsed = {
            "keyword": result.get("keyword"),
            "check_url": result.get("check_url"),
            "datetime": result.get("datetime"),
            "organic_results": [],
            "serp_features": {
                "featured_snippet": None,
                "knowledge_panel": None,
                "local_pack": None,
                "people_also_ask": [],
                "video_carousel": None,
                "image_pack": None,
                "shopping_results": None,
                "top_stories": None,
                "ai_overview": None,
                "reddit_threads": [],
            },
            "competitors": {},
        }
        
        items = result.get("items", [])
        organic_position = 1
        
        for item in items:
            item_type = item.get("type")
            
            # Organic results
            if item_type == "organic":
                organic_data = {
                    "position": organic_position,
                    "rank_absolute": item.get("rank_absolute"),
                    "domain": item.get("domain"),
                    "url": item.get("url"),
                    "title": item.get("title"),
                    "description": item.get("description"),
                    "breadcrumb": item.get("breadcrumb"),
                }
                parsed["organic_results"].append(organic_data)
                
                # Track competitor domains
                domain = item.get("domain")
                if domain:
                    if domain not in parsed["competitors"]:
                        parsed["competitors"][domain] = {
                            "domain": domain,
                            "appearances": 0,
                            "positions": [],
                        }
                    parsed["competitors"][domain]["appearances"] += 1
                    parsed["competitors"][domain]["positions"].append(organic_position)
                
                organic_position += 1
            
            # Featured Snippet
            elif item_type == "featured_snippet":
                parsed["serp_features"]["featured_snippet"] = {
                    "type": item.get("snippet_type"),
                    "url": item.get("url"),
                    "domain": item.get("domain"),
                    "title": item.get("title"),
                    "description": item.get("description"),
                }
            
            # Knowledge Panel
            elif item_type == "knowledge_panel":
                parsed["serp_features"]["knowledge_panel"] = {
                    "title": item.get("title"),
                    "description": item.get("description"),
                    "url": item.get("url"),
                }
            
            # Local Pack
            elif item_type == "local_pack":
                parsed["serp_features"]["local_pack"] = {
                    "title": item.get("title"),
                    "count": len(item.get("items", [])),
                }
            
            # People Also Ask
            elif item_type == "people_also_ask":
                for paa_item in item.get("items", []):
                    parsed["serp_features"]["people_also_ask"].append({
                        "question": paa_item.get("title"),
                        "url": paa_item.get("url"),
                        "domain": paa_item.get("domain"),
                    })
            
            # Video Carousel
            elif item_type == "video":
                parsed["serp_features"]["video_carousel"] = {
                    "title": item.get("title"),
                    "count": len(item.get("items", [])),
                }
            
            # Images
            elif item_type == "images":
                parsed["serp_features"]["image_pack"] = {
                    "title": item.get("title"),
                    "count": len(item.get("items", [])),
                }
            
            # Shopping Results
            elif item_type == "shopping":
                parsed["serp_features"]["shopping_results"] = {
                    "title": item.get("title"),
                    "count": len(item.get("items", [])),
                }
            
            # Top Stories
            elif item_type == "top_stories":
                parsed["serp_features"]["top_stories"] = {
                    "title": item.get("title"),
                    "count": len(item.get("items", [])),
                }
            
            # AI Overview (generative AI results)
            elif item_type == "ai_overview":
                parsed["serp_features"]["ai_overview"] = {
                    "text": item.get("text"),
                    "sources": item.get("sources", []),
                }
            
            # Reddit threads (increasingly common in SERPs)
            elif item_type == "discussions_and_forums":
                for forum_item in item.get("items", []):
                    if "reddit.com" in forum_item.get("url", ""):
                        parsed["serp_features"]["reddit_threads"].append({
                            "title": forum_item.get("title"),
                            "url": forum_item.get("url"),
                        })
        
        # Calculate visual position adjustment
        parsed["visual_position_adjustment"] = self._calculate_visual_position_adjustment(
            parsed["serp_features"]
        )
        
        return parsed
    
    def _calculate_visual_position_adjustment(self, features: Dict[str, Any]) -> float:
        """
        Calculate how many "positions" SERP features add above organic results.
        
        Used for visual position analysis (Module 3).
        
        Args:
            features: Parsed SERP features
            
        Returns:
            Number of visual positions added by SERP features
        """
        adjustment = 0.0
        
        if features["featured_snippet"]:
            adjustment += 2.0  # Featured snippet is very prominent
        
        if features["knowledge_panel"]:
            adjustment += 1.5
        
        if features["local_pack"]:
            adjustment += 2.0
        
        if features["ai_overview"]:
            adjustment += 3.0  # AI overviews take substantial space
        
        # Each PAA question adds ~0.5 positions
        adjustment += len(features["people_also_ask"]) * 0.5
        
        if features["video_carousel"]:
            adjustment += 1.5
        
        if features["image_pack"]:
            adjustment += 1.0
        
        if features["shopping_results"]:
            adjustment += 1.5
        
        if features["top_stories"]:
            adjustment += 1.5
        
        if features["reddit_threads"]:
            adjustment += len(features["reddit_threads"]) * 0.3
        
        return adjustment
    
    async def get_competitor_rankings(
        self,
        keywords: List[str],
        target_domain: str,
        location_code: int = 2840,
        language_code: str = "en",
    ) -> Dict[str, Any]:
        """
        Analyze competitor presence across a set of keywords.
        
        Args:
            keywords: List of keywords to analyze
            target_domain: User's domain for comparison
            location_code: DataForSEO location code
            language_code: Language code
            
        Returns:
            Competitor analysis with overlap scores and average positions
        """
        serp_results = await self.get_batch_serp_results(
            keywords=keywords,
            location_code=location_code,
            language_code=language_code,
        )
        
        # Aggregate competitor data across all keywords
        competitor_stats = {}
        total_keywords = len(keywords)
        
        for keyword, serp_data in serp_results.items():
            for domain, data in serp_data.get("competitors", {}).items():
                if domain == target_domain:
                    continue  # Skip user's own domain
                
                if domain not in competitor_stats:
                    competitor_stats[domain] = {
                        "domain": domain,
                        "keywords_shared": 0,
                        "positions": [],
                        "top_3_count": 0,
                        "top_10_count": 0,
                    }
                
                competitor_stats[domain]["keywords_shared"] += 1
                competitor_stats[domain]["positions"].extend(data["positions"])
                
                for pos in data["positions"]:
                    if pos <= 3:
                        competitor_stats[domain]["top_3_count"] += 1
                    if pos <= 10:
                        competitor_stats[domain]["top_10_count"] += 1
        
        # Calculate averages and threat levels
        competitors = []
        for domain, stats in competitor_stats.items():
            avg_position = sum(stats["positions"]) / len(stats["positions"])
            overlap_pct = (stats["keywords_shared"] / total_keywords) * 100
            
            # Threat level: high overlap + good positions = high threat
            if overlap_pct > 30 and avg_position < 5:
                threat_level = "high"
            elif overlap_pct > 20 or avg_position < 7:
                threat_level = "medium"
            else:
                threat_level = "low"
            
            competitors.append({
                "domain": domain,
                "keywords_shared": stats["keywords_shared"],
                "overlap_percentage": round(overlap_pct, 1),
                "avg_position": round(avg_position, 1),
                "top_3_appearances": stats["top_3_count"],
                "top_10_appearances": stats["top_10_count"],
                "threat_level": threat_level,
            })
        
        # Sort by overlap and position
        competitors.sort(
            key=lambda x: (x["overlap_percentage"], -x["avg_position"]),
            reverse=True,
        )
        
        return {
            "total_keywords_analyzed": total_keywords,
            "unique_competitors": len(competitors),
            "competitors": competitors[:20],  # Top 20 competitors
        }
    
    async def analyze_serp_features_impact(
        self,
        keyword: str,
        target_url: str,
        location_code: int = 2840,
        language_code: str = "en",
    ) -> Dict[str, Any]:
        """
        Analyze how SERP features impact a specific URL's visibility.
        
        Args:
            keyword: Search query
            target_url: URL to analyze
            location_code: DataForSEO location code
            language_code: Language code
            
        Returns:
            Feature impact analysis with CTR estimates
        """
        serp_data = await self.get_serp_results(
            keyword=keyword,
            location_code=location_code,
            language_code=language_code,
        )
        
        # Find target URL in organic results
        target_position = None
        for result in serp_data.get("organic_results", []):
            if result["url"] == target_url or target_url in result["url"]:
                target_position = result["position"]
                break
        
        if target_position is None:
            return {
                "keyword": keyword,
                "target_url": target_url,
                "found": False,
            }
        
        visual_adjustment = serp_data.get("visual_position_adjustment", 0)
        visual_position = target_position + visual_adjustment
        
        # Estimate CTR impact
        # Base CTR by position (simplified model)
        base_ctr_map = {
            1: 0.285, 2: 0.153, 3: 0.098, 4: 0.065, 5: 0.051,
            6: 0.042, 7: 0.036, 8: 0.031, 9: 0.027, 10: 0.024,
        }
        
        base_ctr = base_ctr_map.get(target_position, 0.01)
        # SERP features reduce CTR by roughly 15% per visual position lost
        ctr_impact = -0.15 * visual_adjustment
        estimated_ctr = max(0.001, base_ctr * (1 + ctr_impact))
        
        return {
            "keyword": keyword,
            "target_url": target_url,
            "found": True,
            "organic_position": target_position,
            "visual_position": round(visual_position, 1),
            "visual_adjustment": round(visual_adjustment, 1),
            "serp_features_present": [
                k for k, v in serp_data["serp_features"].items()
                if v and (isinstance(v, list) and len(v) > 0 or not isinstance(v, list))
            ],
            "base_ctr_estimate": round(base_ctr, 4),
            "adjusted_ctr_estimate": round(estimated_ctr, 4),
            "ctr_impact_percent": round(ctr_impact * 100, 1),
        }
    
    async def close(self):
        """Clean up resources"""
        # No persistent connections to close with httpx
        pass