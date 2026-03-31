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
            logger.error(f"DataForSEO authentication error: {str(e)}")
            return False
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((httpx.HTTPError, DataForSEORateLimitError)),
    )
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
            
            try:
                async with httpx.AsyncClient(auth=self.auth, timeout=self.timeout) as client:
                    if method.upper() == "POST":
                        response = await client.post(url, json=data)
                    else:
                        response = await client.get(url)
                    
                    self._last_request_time = asyncio.get_event_loop().time()
                    
                    # Handle rate limiting
                    if response.status_code == 429:
                        logger.warning("DataForSEO rate limit hit, will retry")
                        raise DataForSEORateLimitError("Rate limit exceeded")
                    
                    response.raise_for_status()
                    result = response.json()
                    
                    # Check DataForSEO status code
                    status_code = result.get("status_code")
                    if status_code == 20000:
                        return result
                    elif status_code == 40100:
                        # Authentication error
                        raise DataForSEOError(f"Authentication failed: {result.get('status_message')}")
                    elif status_code == 50000:
                        # Rate limit in API response
                        raise DataForSEORateLimitError(result.get('status_message', 'Rate limit exceeded'))
                    else:
                        error_msg = result.get('status_message', 'Unknown error')
                        raise DataForSEOError(f"API error (code {status_code}): {error_msg}")
                        
            except httpx.HTTPError as e:
                logger.error(f"HTTP error in DataForSEO request: {str(e)}")
                raise
            except Exception as e:
                logger.error(f"Unexpected error in DataForSEO request: {str(e)}")
                raise DataForSEOError(f"Request failed: {str(e)}")
    
    def _parse_serp_features(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse SERP features from a result item.
        
        Args:
            item: SERP result item dictionary
            
        Returns:
            Dictionary with parsed SERP features
        """
        features = {
            "featured_snippet": False,
            "knowledge_panel": False,
            "local_pack": False,
            "people_also_ask": 0,
            "video_carousel": False,
            "image_pack": False,
            "shopping_results": False,
            "top_stories": False,
            "ai_overview": False,
            "reddit_threads": False,
            "twitter_results": False,
            "site_links": False,
        }
        
        # Parse items array for SERP features
        items = item.get("items", [])
        for result_item in items:
            item_type = result_item.get("type", "")
            
            if item_type == "featured_snippet":
                features["featured_snippet"] = True
            elif item_type == "people_also_ask":
                features["people_also_ask"] += 1
            elif item_type == "knowledge_graph":
                features["knowledge_panel"] = True
            elif item_type == "local_pack":
                features["local_pack"] = True
            elif item_type == "video":
                features["video_carousel"] = True
            elif item_type == "images":
                features["image_pack"] = True
            elif item_type == "shopping":
                features["shopping_results"] = True
            elif item_type == "top_stories":
                features["top_stories"] = True
            elif item_type == "ai_overview" or item_type == "answer_box":
                features["ai_overview"] = True
            elif item_type == "discussions_and_forums":
                # Check if it's Reddit specifically
                url = result_item.get("url", "")
                if "reddit.com" in url:
                    features["reddit_threads"] = True
            elif item_type == "twitter":
                features["twitter_results"] = True
        
        # Check for site links in organic results
        for result_item in items:
            if result_item.get("type") == "organic":
                if result_item.get("links"):
                    features["site_links"] = True
                    break
        
        return features
    
    def _calculate_visual_position(
        self,
        organic_position: int,
        serp_features: Dict[str, Any],
        organic_results: List[Dict[str, Any]],
    ) -> float:
        """
        Calculate visual position accounting for SERP features.
        
        Args:
            organic_position: Numeric organic ranking position
            serp_features: Dictionary of SERP features present
            organic_results: List of organic results to count elements above position
            
        Returns:
            Visual position (position adjusted for SERP features)
        """
        visual_offset = 0.0
        
        # Featured snippet pushes everything down ~2 positions
        if serp_features.get("featured_snippet"):
            visual_offset += 2.0
        
        # AI Overview pushes down ~3 positions
        if serp_features.get("ai_overview"):
            visual_offset += 3.0
        
        # Knowledge panel (right side, less impact)
        if serp_features.get("knowledge_panel"):
            visual_offset += 0.5
        
        # Local pack (3 results)
        if serp_features.get("local_pack"):
            visual_offset += 2.0
        
        # Each PAA question adds ~0.5 position
        paa_count = serp_features.get("people_also_ask", 0)
        visual_offset += paa_count * 0.5
        
        # Video carousel
        if serp_features.get("video_carousel"):
            visual_offset += 1.5
        
        # Image pack
        if serp_features.get("image_pack"):
            visual_offset += 1.0
        
        # Shopping results
        if serp_features.get("shopping_results"):
            visual_offset += 1.5
        
        # Top stories
        if serp_features.get("top_stories"):
            visual_offset += 1.5
        
        # Reddit threads
        if serp_features.get("reddit_threads"):
            visual_offset += 1.0
        
        return organic_position + visual_offset
    
    async def get_serp_results(
        self,
        keyword: str,
        location: Optional[str] = None,
        language_code: str = "en",
        device: str = "desktop",
        depth: int = 100,
    ) -> Dict[str, Any]:
        """
        Get live SERP results for a keyword.
        
        Args:
            keyword: Search keyword
            location: Location for search (e.g., "United States" or location code)
            language_code: Language code (default: "en")
            device: Device type ("desktop" or "mobile")
            depth: Number of results to retrieve (max 100)
            
        Returns:
            Dictionary with parsed SERP data including:
            - organic_results: List of organic results with positions
            - serp_features: Dictionary of detected SERP features
            - competitors: List of competing domains
            - total_results: Total number of results
        """
        # Build request payload
        payload = [{
            "keyword": keyword,
            "language_code": language_code,
            "device": device,
            "depth": min(depth, 100),
        }]
        
        # Add location if provided
        if location:
            if location.isdigit():
                payload[0]["location_code"] = int(location)
            else:
                payload[0]["location_name"] = location
        else:
            # Default to United States
            payload[0]["location_code"] = 2840
        
        try:
            response = await self._make_request(
                "POST",
                "serp/google/organic/live/advanced",
                data=payload
            )
            
            # Extract first task result
            tasks = response.get("tasks", [])
            if not tasks:
                raise DataForSEOError("No tasks in response")
            
            task = tasks[0]
            if task.get("status_code") != 20000:
                raise DataForSEOError(f"Task failed: {task.get('status_message')}")
            
            result = task.get("result", [{}])[0]
            items = result.get("items", [])
            
            # Parse SERP features
            serp_features = self._parse_serp_features(result)
            
            # Extract organic results
            organic_results = []
            competitors = {}
            
            for item in items:
                if item.get("type") == "organic":
                    rank_group = item.get("rank_group", 0)
                    rank_absolute = item.get("rank_absolute", 0)
                    url = item.get("url", "")
                    domain = item.get("domain", "")
                    title = item.get("title", "")
                    description = item.get("description", "")
                    
                    # Calculate visual position
                    visual_position = self._calculate_visual_position(
                        rank_absolute,
                        serp_features,
                        organic_results
                    )
                    
                    organic_result = {
                        "position": rank_absolute,
                        "visual_position": visual_position,
                        "url": url,
                        "domain": domain,
                        "title": title,
                        "description": description,
                        "rank_group": rank_group,
                    }
                    
                    # Add site links if present
                    if item.get("links"):
                        organic_result["site_links"] = [
                            {"title": link.get("title"), "url": link.get("url")}
                            for link in item.get("links", [])
                        ]
                    
                    organic_results.append(organic_result)
                    
                    # Track competitors
                    if domain and rank_absolute <= 20:  # Top 20 only
                        if domain not in competitors:
                            competitors[domain] = {
                                "domain": domain,
                                "positions": [],
                                "best_position": rank_absolute,
                                "urls": []
                            }
                        competitors[domain]["positions"].append(rank_absolute)
                        competitors[domain]["urls"].append(url)
                        if rank_absolute < competitors[domain]["best_position"]:
                            competitors[domain]["best_position"] = rank_absolute
            
            return {
                "keyword": keyword,
                "location": location or "United States",
                "language": language_code,
                "device": device,
                "timestamp": datetime.utcnow().isoformat(),
                "organic_results": organic_results,
                "serp_features": serp_features,
                "competitors": list(competitors.values()),
                "total_results": result.get("items_count", 0),
                "check_url": result.get("check_url"),
            }
            
        except Exception as e:
            logger.error(f"Error getting SERP results for '{keyword}': {str(e)}")
            raise
    
    async def get_competitor_metrics(self, domain: str) -> Dict[str, Any]:
        """
        Get competitor analysis metrics for a domain.
        
        Note: This uses the domain overview endpoint which provides
        competitive metrics like organic traffic, keywords ranking, etc.
        
        Args:
            domain: Domain to analyze (e.g., "example.com")
            
        Returns:
            Dictionary with competitor metrics
        """
        payload = [{
            "target": domain,
            "location_code": 2840,  # United States
            "language_code": "en",
        }]
        
        try:
            response = await self._make_request(
                "POST",
                "dataforseo_labs/google/domain_metrics/live",
                data=payload
            )
            
            tasks = response.get("tasks", [])
            if not tasks:
                raise DataForSEOError("No tasks in response")
            
            task = tasks[0]
            if task.get("status_code") != 20000:
                raise DataForSEOError(f"Task failed: {task.get('status_message')}")
            
            result = task.get("result", [{}])[0]
            metrics = result.get("metrics", {})
            
            return {
                "domain": domain,
                "organic_keywords": metrics.get("organic", {}).get("count", 0),
                "organic_traffic": metrics.get("organic", {}).get("etv", 0),
                "organic_cost": metrics.get("organic", {}).get("cost", 0),
                "paid_keywords": metrics.get("paid", {}).get("count", 0),
                "paid_traffic": metrics.get("paid", {}).get("etv", 0),
                "paid_cost": metrics.get("paid", {}).get("cost", 0),
                "timestamp": datetime.utcnow().isoformat(),
            }
            
        except Exception as e:
            logger.error(f"Error getting competitor metrics for '{domain}': {str(e)}")
            raise
    
    async def batch_keyword_analysis(
        self,
        keywords: List[str],
        location: Optional[str] = None,
        language_code: str = "en",
        device: str = "desktop",
        max_concurrent: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Analyze multiple keywords in batch with concurrency control.
        
        Args:
            keywords: List of keywords to analyze
            location: Location for searches
            language_code: Language code
            device: Device type
            max_concurrent: Maximum concurrent requests
            
        Returns:
            List of SERP analysis results for each keyword
        """
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def analyze_keyword(keyword: str) -> Optional[Dict[str, Any]]:
            async with semaphore:
                try:
                    return await self.get_serp_results(
                        keyword=keyword,
                        location=location,
                        language_code=language_code,
                        device=device
                    )
                except Exception as e:
                    logger.error(f"Error analyzing keyword '{keyword}': {str(e)}")
                    return {
                        "keyword": keyword,
                        "error": str(e),
                        "timestamp": datetime.utcnow().isoformat()
                    }
        
        logger.info(f"Starting batch analysis of {len(keywords)} keywords")
        
        tasks = [analyze_keyword(kw) for kw in keywords]
        results = await asyncio.gather(*tasks)
        
        # Filter out None results
        valid_results = [r for r in results if r is not None]
        
        logger.info(f"Completed batch analysis: {len(valid_results)} successful, {len(keywords) - len(valid_results)} failed")
        
        return valid_results
    
    async def get_keyword_difficulty(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
    ) -> List[Dict[str, Any]]:
        """
        Get keyword difficulty scores for a list of keywords.
        
        Args:
            keywords: List of keywords
            location_code: Location code (default: 2840 = United States)
            language_code: Language code
            
        Returns:
            List of keyword difficulty data
        """
        payload = [{
            "keywords": keywords,
            "location_code": location_code,
            "language_code": language_code,
        }]
        
        try:
            response = await self._make_request(
                "POST",
                "dataforseo_labs/google/keyword_ideas/live",
                data=payload
            )
            
            tasks = response.get("tasks", [])
            if not tasks:
                raise DataForSEOError("No tasks in response")
            
            task = tasks[0]
            if task.get("status_code") != 20000:
                raise DataForSEOError(f"Task failed: {task.get('status_message')}")
            
            result = task.get("result", [{}])[0]
            items = result.get("items", [])
            
            keyword_data = []
            for item in items:
                keyword_info = item.get("keyword_info", {})
                keyword_data.append({
                    "keyword": item.get("keyword", ""),
                    "search_volume": keyword_info.get("search_volume", 0),
                    "competition": keyword_info.get("competition", 0),
                    "cpc": keyword_info.get("cpc", 0),
                    "keyword_difficulty": item.get("keyword_properties", {}).get("keyword_difficulty", 0),
                })
            
            return keyword_data
            
        except Exception as e:
            logger.error(f"Error getting keyword difficulty: {str(e)}")
            raise
    
    async def close(self):
        """Clean up resources."""
        # Currently no persistent connections to close
        # This method exists for future extensibility
        pass
    
    async def __aenter__(self):
        """Async context manager entry."""
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
