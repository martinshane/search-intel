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
    
    async def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """
        Make HTTP request to DataForSEO API with rate limiting.
        
        Args:
            method: HTTP method (GET or POST)
            endpoint: API endpoint path
            data: Request payload for POST requests
            
        Returns:
            API response as dictionary
            
        Raises:
            DataForSEORateLimitError: If rate limit is exceeded
            DataForSEOError: For other API errors
        """
        async with self._semaphore:
            # Rate limiting
            current_time = asyncio.get_event_loop().time()
            time_since_last = current_time - self._last_request_time
            if time_since_last < self._min_request_interval:
                await asyncio.sleep(self._min_request_interval - time_since_last)
            
            url = f"{self.BASE_URL}/{endpoint}"
            
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                try:
                    if method.upper() == "POST":
                        response = await client.post(
                            url,
                            json=data,
                            auth=self.auth,
                        )
                    else:
                        response = await client.get(
                            url,
                            auth=self.auth,
                        )
                    
                    self._last_request_time = asyncio.get_event_loop().time()
                    
                    # Check for rate limiting
                    if response.status_code == 429:
                        raise DataForSEORateLimitError(
                            "Rate limit exceeded. Please wait and retry."
                        )
                    
                    response.raise_for_status()
                    result = response.json()
                    
                    # DataForSEO wraps responses in a tasks array
                    if "tasks" not in result:
                        raise DataForSEOError(f"Unexpected response format: {result}")
                    
                    # Check for errors in the response
                    for task in result.get("tasks", []):
                        if task.get("status_code") != 20000:
                            error_msg = task.get("status_message", "Unknown error")
                            raise DataForSEOError(
                                f"API returned error: {error_msg} (code: {task.get('status_code')})"
                            )
                    
                    return result
                
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 429:
                        raise DataForSEORateLimitError(str(e))
                    raise DataForSEOError(f"HTTP error: {e}")
                except httpx.RequestError as e:
                    raise DataForSEOError(f"Request error: {e}")
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((DataForSEORateLimitError, httpx.RequestError)),
        reraise=True,
    )
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
            language_code: Language code (en, es, etc.)
            device: Device type (desktop, mobile)
            depth: Number of results to retrieve (max 100)
            
        Returns:
            Parsed SERP data including:
            - Organic results with positions and URLs
            - SERP features (featured snippets, PAA, knowledge panels, etc.)
            - Ranking domains
        """
        endpoint = "serp/google/organic/live/advanced"
        
        payload = [{
            "keyword": keyword,
            "location_code": location_code,
            "language_code": language_code,
            "device": device,
            "depth": depth,
            "calculate_rectangles": True,
        }]
        
        response = await self._make_request("POST", endpoint, payload)
        
        # Parse and structure the response
        return self._parse_serp_response(response, keyword)
    
    async def get_serp_results_batch(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
        device: str = "desktop",
        depth: int = 100,
        batch_size: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Get SERP results for multiple keywords in batches.
        
        Args:
            keywords: List of search queries
            location_code: DataForSEO location code
            language_code: Language code
            device: Device type
            depth: Number of results per keyword
            batch_size: Max keywords per API call (DataForSEO max is 100)
            
        Returns:
            List of parsed SERP data dictionaries
        """
        results = []
        
        # Process in batches
        for i in range(0, len(keywords), batch_size):
            batch = keywords[i:i + batch_size]
            
            endpoint = "serp/google/organic/live/advanced"
            payload = [
                {
                    "keyword": kw,
                    "location_code": location_code,
                    "language_code": language_code,
                    "device": device,
                    "depth": depth,
                    "calculate_rectangles": True,
                }
                for kw in batch
            ]
            
            try:
                response = await self._make_request("POST", endpoint, payload)
                
                # Parse each task result
                for task in response.get("tasks", []):
                    if task.get("result"):
                        for result_item in task["result"]:
                            keyword_used = result_item.get("keyword", "")
                            parsed = self._parse_serp_result_item(result_item, keyword_used)
                            results.append(parsed)
                
            except Exception as e:
                logger.error(f"Error processing batch {i//batch_size + 1}: {e}")
                # Continue with other batches even if one fails
                continue
        
        return results
    
    def _parse_serp_response(self, response: Dict[str, Any], keyword: str) -> Dict[str, Any]:
        """Parse DataForSEO SERP response into structured format"""
        tasks = response.get("tasks", [])
        if not tasks or not tasks[0].get("result"):
            return {
                "keyword": keyword,
                "organic_results": [],
                "serp_features": {},
                "competitors": [],
                "error": "No results returned"
            }
        
        result_item = tasks[0]["result"][0]
        return self._parse_serp_result_item(result_item, keyword)
    
    def _parse_serp_result_item(self, result_item: Dict[str, Any], keyword: str) -> Dict[str, Any]:
        """Parse a single SERP result item"""
        items = result_item.get("items", [])
        
        # Extract organic results
        organic_results = []
        serp_features = {
            "featured_snippet": None,
            "people_also_ask": [],
            "knowledge_panel": None,
            "local_pack": None,
            "video_carousel": [],
            "image_pack": None,
            "shopping_results": [],
            "top_stories": [],
            "ai_overview": None,
            "reddit_threads": [],
        }
        
        # Track domains for competitor analysis
        domain_positions = {}
        
        for item in items:
            item_type = item.get("type", "")
            rank_group = item.get("rank_group", 0)
            rank_absolute = item.get("rank_absolute", 0)
            
            # Organic results
            if item_type == "organic":
                url = item.get("url", "")
                domain = item.get("domain", "")
                
                organic_results.append({
                    "position": rank_absolute,
                    "url": url,
                    "domain": domain,
                    "title": item.get("title", ""),
                    "description": item.get("description", ""),
                    "breadcrumb": item.get("breadcrumb", ""),
                })
                
                # Track domain positions
                if domain and rank_absolute <= 20:  # Top 20 only
                    if domain not in domain_positions:
                        domain_positions[domain] = []
                    domain_positions[domain].append(rank_absolute)
            
            # Featured snippet
            elif item_type == "featured_snippet":
                serp_features["featured_snippet"] = {
                    "type": item.get("feature_type", ""),
                    "url": item.get("url", ""),
                    "domain": item.get("domain", ""),
                    "title": item.get("title", ""),
                    "description": item.get("description", ""),
                }
            
            # People Also Ask
            elif item_type == "people_also_ask":
                for paa_item in item.get("items", []):
                    serp_features["people_also_ask"].append({
                        "question": paa_item.get("title", ""),
                        "answer_url": paa_item.get("url", ""),
                        "answer_domain": paa_item.get("domain", ""),
                    })
            
            # Knowledge panel
            elif item_type == "knowledge_panel":
                serp_features["knowledge_panel"] = {
                    "title": item.get("title", ""),
                    "description": item.get("description", ""),
                    "type": item.get("knowledge_panel_type", ""),
                }
            
            # Local pack
            elif item_type == "local_pack":
                serp_features["local_pack"] = {
                    "present": True,
                    "count": len(item.get("items", [])),
                }
            
            # Video carousel
            elif item_type == "video":
                serp_features["video_carousel"].append({
                    "url": item.get("url", ""),
                    "source": item.get("source", ""),
                })
            
            # Images
            elif item_type == "images":
                serp_features["image_pack"] = {
                    "present": True,
                    "count": len(item.get("items", [])),
                }
            
            # Shopping
            elif item_type == "shopping":
                serp_features["shopping_results"].append({
                    "title": item.get("title", ""),
                    "price": item.get("price", ""),
                })
            
            # Top stories
            elif item_type == "top_stories":
                for story in item.get("items", []):
                    serp_features["top_stories"].append({
                        "title": story.get("title", ""),
                        "url": story.get("url", ""),
                        "source": story.get("source", ""),
                    })
            
            # AI Overview (Google SGE)
            elif item_type == "ai_overview":
                serp_features["ai_overview"] = {
                    "present": True,
                    "text": item.get("text", ""),
                }
            
            # Reddit threads
            elif "reddit.com" in item.get("url", ""):
                serp_features["reddit_threads"].append({
                    "url": item.get("url", ""),
                    "title": item.get("title", ""),
                    "position": rank_absolute,
                })
        
        # Build competitor list
        competitors = [
            {
                "domain": domain,
                "positions": positions,
                "best_position": min(positions),
                "avg_position": sum(positions) / len(positions),
                "result_count": len(positions),
            }
            for domain, positions in domain_positions.items()
        ]
        competitors.sort(key=lambda x: x["best_position"])
        
        return {
            "keyword": keyword,
            "organic_results": organic_results,
            "serp_features": serp_features,
            "competitors": competitors,
            "total_results": len(organic_results),
            "serp_items_count": len(items),
        }
    
    async def get_competitors(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
        top_n: int = 10,
    ) -> Dict[str, Any]:
        """
        Analyze top ranking domains across multiple keywords.
        
        Args:
            keywords: List of keywords to analyze
            location_code: DataForSEO location code
            language_code: Language code
            top_n: Number of top competitors to return
            
        Returns:
            Dictionary containing:
            - competitors: List of domains with frequency and avg position
            - keyword_overlap: Matrix of which competitors rank for which keywords
        """
        serp_results = await self.get_serp_results_batch(
            keywords=keywords,
            location_code=location_code,
            language_code=language_code,
            depth=20,  # Only need top 20 for competitor analysis
        )
        
        # Aggregate competitor data across all keywords
        domain_data = {}
        keyword_domain_map = {}
        
        for result in serp_results:
            keyword = result["keyword"]
            keyword_domain_map[keyword] = []
            
            for competitor in result.get("competitors", []):
                domain = competitor["domain"]
                keyword_domain_map[keyword].append(domain)
                
                if domain not in domain_data:
                    domain_data[domain] = {
                        "domain": domain,
                        "keywords_ranking": [],
                        "positions": [],
                        "best_position": 100,
                        "avg_position": 0,
                    }
                
                domain_data[domain]["keywords_ranking"].append(keyword)
                domain_data[domain]["positions"].extend(competitor["positions"])
                domain_data[domain]["best_position"] = min(
                    domain_data[domain]["best_position"],
                    competitor["best_position"]
                )
        
        # Calculate averages and sort
        competitors = []
        for domain, data in domain_data.items():
            if data["positions"]:
                data["avg_position"] = sum(data["positions"]) / len(data["positions"])
                data["keyword_count"] = len(data["keywords_ranking"])
                data["frequency"] = len(data["keywords_ranking"]) / len(keywords)
                competitors.append(data)
        
        # Sort by frequency (how many keywords they rank for)
        competitors.sort(key=lambda x: (-x["keyword_count"], x["avg_position"]))
        
        return {
            "competitors": competitors[:top_n],
            "total_competitors": len(competitors),
            "keywords_analyzed": len(keywords),
            "keyword_domain_map": keyword_domain_map,
        }
    
    async def get_serp_features(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
    ) -> Dict[str, Any]:
        """
        Analyze SERP features across multiple keywords.
        
        Args:
            keywords: List of keywords to analyze
            location_code: DataForSEO location code
            language_code: Language code
            
        Returns:
            Dictionary containing:
            - feature_frequency: How often each feature appears
            - keywords_by_feature: Which keywords trigger which features
            - displacement_analysis: Keywords with heavy feature presence
        """
        serp_results = await self.get_serp_results_batch(
            keywords=keywords,
            location_code=location_code,
            language_code=language_code,
            depth=20,
        )
        
        # Aggregate feature data
        feature_frequency = {
            "featured_snippet": 0,
            "people_also_ask": 0,
            "knowledge_panel": 0,
            "local_pack": 0,
            "video_carousel": 0,
            "image_pack": 0,
            "shopping_results": 0,
            "top_stories": 0,
            "ai_overview": 0,
            "reddit_threads": 0,
        }
        
        keywords_by_feature = {feature: [] for feature in feature_frequency.keys()}
        displacement_data = []
        
        for result in serp_results:
            keyword = result["keyword"]
            features = result.get("serp_features", {})
            
            # Count features
            if features.get("featured_snippet"):
                feature_frequency["featured_snippet"] += 1
                keywords_by_feature["featured_snippet"].append(keyword)
            
            if features.get("people_also_ask"):
                feature_frequency["people_also_ask"] += 1
                keywords_by_feature["people_also_ask"].append(keyword)
                paa_count = len(features["people_also_ask"])
            else:
                paa_count = 0
            
            if features.get("knowledge_panel"):
                feature_frequency["knowledge_panel"] += 1
                keywords_by_feature["knowledge_panel"].append(keyword)
            
            if features.get("local_pack"):
                feature_frequency["local_pack"] += 1
                keywords_by_feature["local_pack"].append(keyword)
            
            if features.get("video_carousel"):
                feature_frequency["video_carousel"] += 1
                keywords_by_feature["video_carousel"].append(keyword)
            
            if features.get("image_pack"):
                feature_frequency["image_pack"] += 1
                keywords_by_feature["image_pack"].append(keyword)
            
            if features.get("shopping_results"):
                feature_frequency["shopping_results"] += 1
                keywords_by_feature["shopping_results"].append(keyword)
            
            if features.get("top_stories"):
                feature_frequency["top_stories"] += 1
                keywords_by_feature["top_stories"].append(keyword)
            
            if features.get("ai_overview"):
                feature_frequency["ai_overview"] += 1
                keywords_by_feature["ai_overview"].append(keyword)
            
            if features.get("reddit_threads"):
                feature_frequency["reddit_threads"] += 1
                keywords_by_feature["reddit_threads"].append(keyword)
            
            # Calculate displacement score (rough estimate)
            displacement_score = 0
            features_present = []
            
            if features.get("featured_snippet"):
                displacement_score += 2
                features_present.append("featured_snippet")
            
            if features.get("people_also_ask"):
                displacement_score += paa_count * 0.5
                features_present.append(f"paa_x{paa_count}")
            
            if features.get("ai_overview"):
                displacement_score += 3
                features_present.append("ai_overview")
            
            if features.get("local_pack"):
                displacement_score += 1.5
                features_present.append("local_pack")
            
            if features.get("video_carousel"):
                displacement_score += 1
                features_present.append("video_carousel")
            
            if features.get("shopping_results"):
                displacement_score += 1
                features_present.append("shopping_results")
            
            if displacement_score > 0:
                displacement_data.append({
                    "keyword": keyword,
                    "displacement_score": displacement_score,
                    "features_present": features_present,
                })
        
        # Sort displacement data by score
        displacement_data.sort(key=lambda x: -x["displacement_score"])
        
        # Calculate percentages
        total_keywords = len(keywords)
        feature_percentages = {
            feature: (count / total_keywords * 100) if total_keywords > 0 else 0
            for feature, count in feature_frequency.items()
        }
        
        return {
            "feature_frequency": feature_frequency,
            "feature_percentages": feature_percentages,
            "keywords_by_feature": keywords_by_feature,
            "displacement_analysis": displacement_data,
            "total_keywords_analyzed": total_keywords,
        }
    
    async def close(self):
        """Clean up resources"""
        # Nothing to clean up for httpx client (context managed)
        pass
