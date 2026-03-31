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
    - Keyword search volume and competition data
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
                        logger.warning("DataForSEO rate limit exceeded")
                        raise DataForSEORateLimitError("Rate limit exceeded")
                    
                    response.raise_for_status()
                    result = response.json()
                    
                    # Check API-level status
                    if result.get("status_code") != 20000:
                        error_msg = result.get("status_message", "Unknown error")
                        logger.error(f"DataForSEO API error: {error_msg}")
                        raise DataForSEOError(f"API error: {error_msg}")
                    
                    return result
                    
                except httpx.HTTPStatusError as e:
                    logger.error(f"HTTP error calling DataForSEO: {e}")
                    raise DataForSEOError(f"HTTP error: {e}")
                except httpx.RequestError as e:
                    logger.error(f"Request error calling DataForSEO: {e}")
                    raise DataForSEOError(f"Request error: {e}")
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((DataForSEORateLimitError, httpx.RequestError)),
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
        Fetch live SERP results for a keyword.
        
        Args:
            keyword: Search query
            location_code: DataForSEO location code (2840 = US)
            language_code: Language code (en, es, etc.)
            device: Device type (desktop, mobile)
            depth: Number of results to fetch (max 100)
            
        Returns:
            Dictionary containing:
                - keyword: Original keyword
                - serp_data: List of organic results with positions, URLs, titles, etc.
                - serp_features: List of SERP features present
                - total_results: Total number of results Google reports
                - metadata: Location, language, device info
        """
        logger.info(f"Fetching SERP results for keyword: {keyword}")
        
        payload = [
            {
                "keyword": keyword,
                "location_code": location_code,
                "language_code": language_code,
                "device": device,
                "depth": depth,
                "calculate_rectangles": True,  # For SERP feature positioning
            }
        ]
        
        response = await self._make_request(
            "POST",
            "serp/google/organic/live/advanced",
            data=payload,
        )
        
        # Parse response
        if not response.get("tasks") or not response["tasks"][0].get("result"):
            logger.warning(f"No SERP results returned for keyword: {keyword}")
            return {
                "keyword": keyword,
                "serp_data": [],
                "serp_features": [],
                "total_results": 0,
                "metadata": {},
            }
        
        task_result = response["tasks"][0]["result"][0]
        
        # Extract organic results
        organic_results = []
        for item in task_result.get("items", []):
            if item.get("type") == "organic":
                organic_results.append({
                    "position": item.get("rank_group", item.get("rank_absolute", 0)),
                    "url": item.get("url"),
                    "domain": item.get("domain"),
                    "title": item.get("title"),
                    "description": item.get("description"),
                    "breadcrumb": item.get("breadcrumb"),
                    "is_https": item.get("url", "").startswith("https"),
                })
        
        # Extract SERP features
        serp_features = []
        feature_counts = {}
        
        for item in task_result.get("items", []):
            feature_type = item.get("type")
            if feature_type and feature_type != "organic":
                if feature_type == "people_also_ask":
                    feature_counts["paa"] = feature_counts.get("paa", 0) + 1
                elif feature_type not in ["related_searches"]:  # Exclude bottom-of-page features
                    if feature_type not in [f["type"] for f in serp_features]:
                        serp_features.append({
                            "type": feature_type,
                            "position": item.get("rank_absolute", 0),
                            "rectangle": item.get("rectangle"),
                        })
        
        # Add PAA count if present
        if "paa" in feature_counts:
            paa_feature = next((f for f in serp_features if f["type"] == "people_also_ask"), None)
            if paa_feature:
                paa_feature["count"] = feature_counts["paa"]
        
        return {
            "keyword": keyword,
            "serp_data": organic_results,
            "serp_features": serp_features,
            "total_results": task_result.get("se_results_count", 0),
            "metadata": {
                "location_code": location_code,
                "language_code": language_code,
                "device": device,
                "check_url": task_result.get("check_url"),
                "datetime": task_result.get("datetime"),
            },
        }
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((DataForSEORateLimitError, httpx.RequestError)),
    )
    async def get_keyword_data(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
    ) -> List[Dict[str, Any]]:
        """
        Fetch keyword metrics (search volume, CPC, competition).
        
        Args:
            keywords: List of keywords to analyze
            location_code: DataForSEO location code
            language_code: Language code
            
        Returns:
            List of dictionaries, each containing:
                - keyword: The keyword
                - search_volume: Monthly search volume
                - cpc: Cost per click in USD
                - competition: Competition level (0-1)
                - competition_level: Low/Medium/High
                - monthly_searches: Last 12 months of search volume data
        """
        logger.info(f"Fetching keyword data for {len(keywords)} keywords")
        
        # DataForSEO keyword data endpoint accepts up to 1000 keywords per request
        # but we'll batch at 100 for safety
        batch_size = 100
        all_results = []
        
        for i in range(0, len(keywords), batch_size):
            batch = keywords[i:i + batch_size]
            
            payload = [
                {
                    "keywords": batch,
                    "location_code": location_code,
                    "language_code": language_code,
                }
            ]
            
            response = await self._make_request(
                "POST",
                "keywords_data/google_ads/search_volume/live",
                data=payload,
            )
            
            if not response.get("tasks") or not response["tasks"][0].get("result"):
                logger.warning(f"No keyword data returned for batch {i // batch_size + 1}")
                continue
            
            for item in response["tasks"][0]["result"]:
                keyword_data = {
                    "keyword": item.get("keyword"),
                    "search_volume": item.get("search_volume"),
                    "cpc": item.get("cpc"),
                    "competition": item.get("competition"),
                    "competition_level": item.get("competition_level"),
                    "monthly_searches": item.get("monthly_searches", []),
                }
                all_results.append(keyword_data)
            
            # Small delay between batches
            if i + batch_size < len(keywords):
                await asyncio.sleep(0.5)
        
        return all_results
    
    async def batch_serp_analysis(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
        device: str = "desktop",
        max_concurrent: int = 10,
    ) -> Dict[str, Any]:
        """
        Process multiple keywords in parallel with concurrency control.
        
        Args:
            keywords: List of keywords to analyze
            location_code: DataForSEO location code
            language_code: Language code
            device: Device type
            max_concurrent: Maximum concurrent requests
            
        Returns:
            Dictionary containing:
                - results: List of SERP results for each keyword
                - summary: Aggregate statistics
                - errors: List of any errors encountered
        """
        logger.info(f"Starting batch SERP analysis for {len(keywords)} keywords")
        
        results = []
        errors = []
        
        # Create semaphore for concurrency control
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def fetch_with_semaphore(keyword: str):
            async with semaphore:
                try:
                    result = await self.get_serp_results(
                        keyword=keyword,
                        location_code=location_code,
                        language_code=language_code,
                        device=device,
                    )
                    return result
                except Exception as e:
                    logger.error(f"Error fetching SERP for '{keyword}': {e}")
                    errors.append({
                        "keyword": keyword,
                        "error": str(e),
                    })
                    return None
        
        # Fetch all keywords in parallel (with concurrency limit)
        tasks = [fetch_with_semaphore(kw) for kw in keywords]
        raw_results = await asyncio.gather(*tasks, return_exceptions=False)
        
        # Filter out None results
        results = [r for r in raw_results if r is not None]
        
        # Calculate summary statistics
        total_features = {}
        total_organic_results = 0
        
        for result in results:
            total_organic_results += len(result.get("serp_data", []))
            
            for feature in result.get("serp_features", []):
                feature_type = feature["type"]
                total_features[feature_type] = total_features.get(feature_type, 0) + 1
        
        summary = {
            "total_keywords": len(keywords),
            "successful_fetches": len(results),
            "failed_fetches": len(errors),
            "total_organic_results": total_organic_results,
            "avg_results_per_keyword": total_organic_results / len(results) if results else 0,
            "serp_features_frequency": total_features,
        }
        
        logger.info(
            f"Batch SERP analysis complete: {len(results)} successful, "
            f"{len(errors)} errors"
        )
        
        return {
            "results": results,
            "summary": summary,
            "errors": errors,
        }
    
    async def get_keywords_for_domain(
        self,
        domain: str,
        location_code: int = 2840,
        language_code: str = "en",
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        """
        Get organic keywords a domain ranks for (useful for competitor analysis).
        
        Args:
            domain: Domain to analyze (e.g., "example.com")
            location_code: DataForSEO location code
            language_code: Language code
            limit: Maximum number of keywords to return
            
        Returns:
            List of dictionaries with keyword, position, search_volume, etc.
        """
        logger.info(f"Fetching ranking keywords for domain: {domain}")
        
        payload = [
            {
                "target": domain,
                "location_code": location_code,
                "language_code": language_code,
                "limit": limit,
            }
        ]
        
        response = await self._make_request(
            "POST",
            "dataforseo_labs/google/ranked_keywords/live",
            data=payload,
        )
        
        if not response.get("tasks") or not response["tasks"][0].get("result"):
            logger.warning(f"No keyword data returned for domain: {domain}")
            return []
        
        items = response["tasks"][0]["result"][0].get("items", [])
        
        keywords = []
        for item in items:
            keyword_data = item.get("keyword_data", {})
            ranked_serp_element = item.get("ranked_serp_element", {})
            
            keywords.append({
                "keyword": keyword_data.get("keyword"),
                "search_volume": keyword_data.get("keyword_info", {}).get("search_volume"),
                "competition": keyword_data.get("keyword_info", {}).get("competition"),
                "cpc": keyword_data.get("keyword_info", {}).get("cpc"),
                "position": ranked_serp_element.get("serp_item", {}).get("rank_absolute"),
                "url": ranked_serp_element.get("serp_item", {}).get("url"),
                "etv": item.get("etv"),  # Estimated traffic value
            })
        
        return keywords
    
    async def close(self):
        """Cleanup method (placeholder for future connection pooling)"""
        pass
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


# Convenience function for simple use cases
async def fetch_serp_data(
    keywords: List[str],
    location_code: int = 2840,
    language_code: str = "en",
) -> Dict[str, Any]:
    """
    Convenience function to fetch SERP data for multiple keywords.
    
    Args:
        keywords: List of keywords to analyze
        location_code: DataForSEO location code
        language_code: Language code
        
    Returns:
        Batch analysis results
    """
    async with DataForSEOClient() as client:
        return await client.batch_serp_analysis(
            keywords=keywords,
            location_code=location_code,
            language_code=language_code,
        )
