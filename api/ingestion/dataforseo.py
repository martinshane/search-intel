"""
DataForSEO API integration for live SERP data retrieval.

Pulls SERP data for top non-branded keywords with rate limiting,
caching, and budget tracking.
"""

import os
import hashlib
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import asyncio
import logging

import httpx
from supabase import Client

logger = logging.getLogger(__name__)


class DataForSEOClient:
    """Client for DataForSEO SERP API with rate limiting and caching."""
    
    BASE_URL = "https://api.dataforseo.com/v3"
    COST_PER_QUERY = 0.002  # $0.002 per SERP request
    
    def __init__(
        self,
        supabase: Client,
        api_login: Optional[str] = None,
        api_password: Optional[str] = None,
        max_budget: float = 0.20
    ):
        """
        Initialize DataForSEO client.
        
        Args:
            supabase: Supabase client for caching
            api_login: DataForSEO API login (defaults to env var)
            api_password: DataForSEO API password (defaults to env var)
            max_budget: Maximum budget per report run in USD
        """
        self.supabase = supabase
        self.api_login = api_login or os.getenv("DATAFORSEO_LOGIN")
        self.api_password = api_password or os.getenv("DATAFORSEO_PASSWORD")
        self.max_budget = max_budget
        self.current_spend = 0.0
        
        if not self.api_login or not self.api_password:
            raise ValueError("DataForSEO credentials not provided")
    
    def _get_cache_key(self, keyword: str, location_code: int, language_code: str) -> str:
        """Generate cache key for a SERP request."""
        key_data = f"{keyword}:{location_code}:{language_code}"
        return hashlib.sha256(key_data.encode()).hexdigest()
    
    async def _get_cached_serp(
        self,
        keyword: str,
        location_code: int,
        language_code: str,
        max_age_days: int = 7
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieve cached SERP data if available and fresh.
        
        Args:
            keyword: Search keyword
            location_code: DataForSEO location code
            language_code: DataForSEO language code
            max_age_days: Maximum age of cache in days
            
        Returns:
            Cached SERP data or None
        """
        try:
            cutoff_date = (datetime.utcnow() - timedelta(days=max_age_days)).date()
            
            result = self.supabase.table("serp_snapshots").select("*").eq(
                "keyword", keyword
            ).gte(
                "snapshot_date", cutoff_date.isoformat()
            ).order(
                "snapshot_date", desc=True
            ).limit(1).execute()
            
            if result.data and len(result.data) > 0:
                logger.info(f"Cache hit for keyword: {keyword}")
                return result.data[0]["serp_data"]
            
            return None
            
        except Exception as e:
            logger.warning(f"Error retrieving cache for {keyword}: {e}")
            return None
    
    async def _save_serp_cache(
        self,
        keyword: str,
        serp_data: Dict[str, Any]
    ) -> None:
        """
        Save SERP data to cache.
        
        Args:
            keyword: Search keyword
            serp_data: SERP response data
        """
        try:
            snapshot_date = datetime.utcnow().date().isoformat()
            
            # Upsert to handle duplicates
            self.supabase.table("serp_snapshots").upsert({
                "keyword": keyword,
                "snapshot_date": snapshot_date,
                "serp_data": serp_data
            }, on_conflict="keyword,snapshot_date").execute()
            
            logger.info(f"Cached SERP data for: {keyword}")
            
        except Exception as e:
            logger.error(f"Error caching SERP for {keyword}: {e}")
    
    async def _fetch_live_serp(
        self,
        keyword: str,
        location_code: int = 2840,  # USA
        language_code: str = "en",
        device: str = "desktop"
    ) -> Dict[str, Any]:
        """
        Fetch live SERP data from DataForSEO API.
        
        Args:
            keyword: Search keyword
            location_code: DataForSEO location code (2840 = USA)
            language_code: Language code
            device: Device type (desktop, mobile)
            
        Returns:
            SERP data
            
        Raises:
            Exception: If API request fails or budget exceeded
        """
        if self.current_spend + self.COST_PER_QUERY > self.max_budget:
            raise Exception(
                f"Budget limit reached: ${self.current_spend:.3f} / ${self.max_budget:.3f}"
            )
        
        endpoint = f"{self.BASE_URL}/serp/google/organic/live/advanced"
        
        payload = [{
            "keyword": keyword,
            "location_code": location_code,
            "language_code": language_code,
            "device": device,
            "os": "windows" if device == "desktop" else "ios",
            "depth": 100  # Get top 100 results
        }]
        
        auth = httpx.BasicAuth(self.api_login, self.api_password)
        
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    endpoint,
                    json=payload,
                    auth=auth
                )
                response.raise_for_status()
                
                data = response.json()
                
                if data.get("status_code") != 20000:
                    error_msg = data.get("status_message", "Unknown error")
                    raise Exception(f"DataForSEO API error: {error_msg}")
                
                self.current_spend += self.COST_PER_QUERY
                logger.info(
                    f"Fetched SERP for '{keyword}' (spend: ${self.current_spend:.3f})"
                )
                
                return data
                
        except httpx.HTTPError as e:
            logger.error(f"HTTP error fetching SERP for '{keyword}': {e}")
            raise
        except Exception as e:
            logger.error(f"Error fetching SERP for '{keyword}': {e}")
            raise
    
    def _parse_serp_features(self, serp_item: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse SERP features from DataForSEO response.
        
        Args:
            serp_item: Single SERP item from DataForSEO
            
        Returns:
            Parsed SERP features
        """
        features = {
            "featured_snippet": False,
            "people_also_ask": 0,
            "video_carousel": False,
            "local_pack": False,
            "knowledge_panel": False,
            "ai_overview": False,
            "image_pack": False,
            "shopping_results": False,
            "top_stories": False,
            "ads_count": 0,
            "organic_count": 0
        }
        
        if not serp_item or "items" not in serp_item:
            return features
        
        for item in serp_item["items"]:
            item_type = item.get("type", "")
            
            if item_type == "featured_snippet":
                features["featured_snippet"] = True
            elif item_type == "people_also_ask":
                features["people_also_ask"] += 1
            elif item_type == "video":
                features["video_carousel"] = True
            elif item_type == "local_pack":
                features["local_pack"] = True
            elif item_type == "knowledge_panel":
                features["knowledge_panel"] = True
            elif item_type == "ai_overview":
                features["ai_overview"] = True
            elif item_type == "images":
                features["image_pack"] = True
            elif item_type == "shopping":
                features["shopping_results"] = True
            elif item_type == "top_stories":
                features["top_stories"] = True
            elif item_type == "paid":
                features["ads_count"] += 1
            elif item_type == "organic":
                features["organic_count"] += 1
        
        return features
    
    def _extract_organic_results(
        self,
        serp_item: Dict[str, Any],
        user_domain: str
    ) -> List[Dict[str, Any]]:
        """
        Extract organic search results from SERP.
        
        Args:
            serp_item: Single SERP item from DataForSEO
            user_domain: User's domain to identify their position
            
        Returns:
            List of organic results with metadata
        """
        results = []
        user_domain_normalized = user_domain.lower().replace("www.", "")
        
        if not serp_item or "items" not in serp_item:
            return results
        
        for item in serp_item["items"]:
            if item.get("type") != "organic":
                continue
            
            url = item.get("url", "")
            domain = item.get("domain", "")
            
            is_user_result = user_domain_normalized in domain.lower().replace("www.", "")
            
            result = {
                "position": item.get("rank_absolute", 0),
                "url": url,
                "domain": domain,
                "title": item.get("title", ""),
                "description": item.get("description", ""),
                "is_user_result": is_user_result
            }
            
            results.append(result)
        
        return results
    
    def _calculate_visual_position(
        self,
        organic_position: int,
        features: Dict[str, Any]
    ) -> float:
        """
        Calculate visual position accounting for SERP features.
        
        Args:
            organic_position: Organic ranking position
            features: Parsed SERP features
            
        Returns:
            Adjusted visual position
        """
        displacement = 0.0
        
        # Featured snippet pushes everything down ~2 positions
        if features["featured_snippet"]:
            displacement += 2.0
        
        # Each PAA pushes down by ~0.5 positions
        displacement += features["people_also_ask"] * 0.5
        
        # AI Overview can push down 1-3 positions depending on size
        if features["ai_overview"]:
            displacement += 2.0
        
        # Video carousel
        if features["video_carousel"]:
            displacement += 1.0
        
        # Local pack
        if features["local_pack"]:
            displacement += 1.5
        
        # Image pack
        if features["image_pack"]:
            displacement += 0.5
        
        # Shopping results
        if features["shopping_results"]:
            displacement += 1.0
        
        # Ads (each ad = 1 position)
        displacement += features["ads_count"]
        
        return organic_position + displacement
    
    async def get_serp_data(
        self,
        keyword: str,
        location_code: int = 2840,
        language_code: str = "en",
        device: str = "desktop",
        user_domain: str = "",
        use_cache: bool = True,
        cache_max_age_days: int = 7
    ) -> Dict[str, Any]:
        """
        Get SERP data for a keyword, using cache when available.
        
        Args:
            keyword: Search keyword
            location_code: DataForSEO location code
            language_code: Language code
            device: Device type
            user_domain: User's domain to identify their position
            use_cache: Whether to use cached data
            cache_max_age_days: Maximum cache age in days
            
        Returns:
            Processed SERP data
        """
        # Try cache first
        if use_cache:
            cached_data = await self._get_cached_serp(
                keyword,
                location_code,
                language_code,
                cache_max_age_days
            )
            if cached_data:
                return cached_data
        
        # Fetch live data
        raw_data = await self._fetch_live_serp(
            keyword,
            location_code,
            language_code,
            device
        )
        
        # Process the response
        if not raw_data.get("tasks") or len(raw_data["tasks"]) == 0:
            raise Exception(f"No SERP data returned for keyword: {keyword}")
        
        task = raw_data["tasks"][0]
        if task.get("status_code") != 20000:
            error_msg = task.get("status_message", "Unknown error")
            raise Exception(f"SERP task error for '{keyword}': {error_msg}")
        
        result = task.get("result", [{}])[0]
        
        # Parse features
        features = self._parse_serp_features(result)
        
        # Extract organic results
        organic_results = self._extract_organic_results(result, user_domain)
        
        # Find user's position
        user_position = None
        user_url = None
        visual_position = None
        
        for res in organic_results:
            if res["is_user_result"]:
                user_position = res["position"]
                user_url = res["url"]
                visual_position = self._calculate_visual_position(
                    user_position,
                    features
                )
                break
        
        # Extract competitor domains
        competitors = []
        for res in organic_results[:10]:  # Top 10 only
            if not res["is_user_result"]:
                competitors.append({
                    "domain": res["domain"],
                    "position": res["position"],
                    "url": res["url"],
                    "title": res["title"]
                })
        
        processed_data = {
            "keyword": keyword,
            "location_code": location_code,
            "language_code": language_code,
            "device": device,
            "fetched_at": datetime.utcnow().isoformat(),
            "serp_features": features,
            "organic_results": organic_results[:20],  # Store top 20
            "user_position": user_position,
            "user_url": user_url,
            "visual_position": visual_position,
            "competitors": competitors,
            "total_results": result.get("items_count", 0)
        }
        
        # Cache the processed data
        await self._save_serp_cache(keyword, processed_data)
        
        return processed_data
    
    async def get_batch_serp_data(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
        device: str = "desktop",
        user_domain: str = "",
        use_cache: bool = True,
        cache_max_age_days: int = 7,
        delay_between_requests: float = 0.5
    ) -> List[Dict[str, Any]]:
        """
        Fetch SERP data for multiple keywords with rate limiting.
        
        Args:
            keywords: List of keywords
            location_code: DataForSEO location code
            language_code: Language code
            device: Device type
            user_domain: User's domain
            use_cache: Whether to use cached data
            cache_max_age_days: Maximum cache age in days
            delay_between_requests: Delay between API calls in seconds
            
        Returns:
            List of SERP data dictionaries
        """
        results = []
        
        for i, keyword in enumerate(keywords):
            try:
                logger.info(
                    f"Fetching SERP {i+1}/{len(keywords)}: {keyword}"
                )
                
                serp_data = await self.get_serp_data(
                    keyword=keyword,
                    location_code=location_code,
                    language_code=language_code,
                    device=device,
                    user_domain=user_domain,
                    use_cache=use_cache,
                    cache_max_age_days=cache_max_age_days
                )
                
                results.append({
                    "keyword": keyword,
                    "success": True,
                    "data": serp_data
                })
                
                # Rate limiting - don't delay on cached results
                if not use_cache or serp_data.get("fetched_at"):
                    if i < len(keywords) - 1:  # Don't delay after last request
                        await asyncio.sleep(delay_between_requests)
                
            except Exception as e:
                logger.error(f"Error fetching SERP for '{keyword}': {e}")
                results.append({
                    "keyword": keyword,
                    "success": False,
                    "error": str(e)
                })
                
                # Check if we hit budget limit
                if "Budget limit reached" in str(e):
                    logger.warning("Budget limit reached, stopping batch fetch")
                    break
        
        logger.info(
            f"Batch fetch complete: {len([r for r in results if r['success']])} / "
            f"{len(results)} successful (spend: ${self.current_spend:.3f})"
        )
        
        return results
    
    def get_spending_report(self) -> Dict[str, Any]:
        """
        Get current spending report.
        
        Returns:
            Spending statistics
        """
        return {
            "current_spend": round(self.current_spend, 3),
            "max_budget": self.max_budget,
            "remaining_budget": round(self.max_budget - self.current_spend, 3),
            "queries_made": int(self.current_spend / self.COST_PER_QUERY),
            "queries_remaining": int(
                (self.max_budget - self.current_spend) / self.COST_PER_QUERY
            )
        }


async def fetch_serps_for_top_keywords(
    supabase: Client,
    keywords: List[str],
    user_domain: str,
    max_keywords: int = 50,
    budget: float = 0.20
) -> Dict[str, Any]:
    """
    Convenience function to fetch SERPs for top keywords.
    
    Args:
        supabase: Supabase client
        keywords: List of keywords to fetch
        user_domain: User's domain
        max_keywords: Maximum number of keywords to process
        budget: Maximum budget in USD
        
    Returns:
        Dictionary with results and spending report
    """
    # Limit keywords to budget
    keywords_to_fetch = keywords[:max_keywords]
    
    client = DataForSEOClient(supabase=supabase, max_budget=budget)
    
    results = await client.get_batch_serp_data(
        keywords=keywords_to_fetch,
        user_domain=user_domain,
        use_cache=True,
        cache_max_age_days=7,
        delay_between_requests=0.5
    )
    
    spending = client.get_spending_report()
    
    successful_results = [r for r in results if r["success"]]
    failed_results = [r for r in results if not r["success"]]
    
    return {
        "total_keywords_requested": len(keywords_to_fetch),
        "successful_fetches": len(successful_results),
        "failed_fetches": len(failed_results),
        "results": successful_results,
        "errors": failed_results,
        "spending": spending
    }
