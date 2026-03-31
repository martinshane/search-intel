import os
import asyncio
import hashlib
import json
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timedelta
from urllib.parse import urlencode
import aiohttp
from aiohttp import ClientSession, ClientTimeout
import backoff
from supabase import Client as SupabaseClient, create_client

from config import settings


class DataForSEOService:
    """
    DataForSEO API client service for:
    1. Keyword research (volume, difficulty, related keywords) - Module 3
    2. SERP features extraction - Module 8
    3. Competitor SERP position tracking - Module 11
    
    Features:
    - Rate limiting (configurable requests per second)
    - Exponential backoff retry logic
    - Caching layer via Supabase
    - Domain-specific search parameters (location, language)
    - Async/await for concurrent API calls
    """
    
    BASE_URL = "https://api.dataforseo.com"
    DEFAULT_CACHE_TTL_HOURS = 24
    DEFAULT_RATE_LIMIT_PER_SECOND = 2
    DEFAULT_TIMEOUT_SECONDS = 60
    
    def __init__(
        self,
        api_login: Optional[str] = None,
        api_password: Optional[str] = None,
        supabase_client: Optional[SupabaseClient] = None,
        cache_ttl_hours: int = DEFAULT_CACHE_TTL_HOURS,
        rate_limit_per_second: int = DEFAULT_RATE_LIMIT_PER_SECOND,
        timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS
    ):
        """
        Initialize DataForSEO service.
        
        Args:
            api_login: DataForSEO API login (defaults to env var DATAFORSEO_LOGIN)
            api_password: DataForSEO API password (defaults to env var DATAFORSEO_PASSWORD)
            supabase_client: Supabase client for caching (defaults to creating new client)
            cache_ttl_hours: Cache TTL in hours
            rate_limit_per_second: Maximum requests per second
            timeout_seconds: Request timeout in seconds
        """
        self.api_login = api_login or os.getenv("DATAFORSEO_LOGIN")
        self.api_password = api_password or os.getenv("DATAFORSEO_PASSWORD")
        
        if not self.api_login or not self.api_password:
            raise ValueError("DataForSEO credentials not provided")
        
        # Initialize Supabase client for caching
        if supabase_client:
            self.supabase = supabase_client
        else:
            supabase_url = os.getenv("SUPABASE_URL")
            supabase_key = os.getenv("SUPABASE_SERVICE_KEY")
            if supabase_url and supabase_key:
                self.supabase = create_client(supabase_url, supabase_key)
            else:
                self.supabase = None
        
        self.cache_ttl_hours = cache_ttl_hours
        self.rate_limit_per_second = rate_limit_per_second
        self.timeout_seconds = timeout_seconds
        
        # Rate limiting state
        self._rate_limit_semaphore = asyncio.Semaphore(rate_limit_per_second)
        self._last_request_times: List[float] = []
    
    async def _enforce_rate_limit(self):
        """Enforce rate limiting using sliding window."""
        async with self._rate_limit_semaphore:
            now = asyncio.get_event_loop().time()
            
            # Remove timestamps older than 1 second
            self._last_request_times = [
                t for t in self._last_request_times if now - t < 1.0
            ]
            
            # If at limit, wait until oldest request expires
            if len(self._last_request_times) >= self.rate_limit_per_second:
                sleep_time = 1.0 - (now - self._last_request_times[0])
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)
                # Clean up again after sleep
                now = asyncio.get_event_loop().time()
                self._last_request_times = [
                    t for t in self._last_request_times if now - t < 1.0
                ]
            
            self._last_request_times.append(now)
    
    def _generate_cache_key(self, endpoint: str, params: Dict[str, Any]) -> str:
        """Generate cache key from endpoint and parameters."""
        params_str = json.dumps(params, sort_keys=True)
        hash_str = hashlib.sha256(f"{endpoint}:{params_str}".encode()).hexdigest()
        return f"dataforseo:{endpoint}:{hash_str}"
    
    async def _get_cached(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """Retrieve cached response from Supabase."""
        if not self.supabase:
            return None
        
        try:
            result = self.supabase.table("dataforseo_cache").select("*").eq(
                "cache_key", cache_key
            ).execute()
            
            if result.data and len(result.data) > 0:
                cached = result.data[0]
                cached_at = datetime.fromisoformat(cached["cached_at"])
                expiry = cached_at + timedelta(hours=self.cache_ttl_hours)
                
                if datetime.utcnow() < expiry:
                    return cached["response_data"]
                else:
                    # Delete expired cache entry
                    self.supabase.table("dataforseo_cache").delete().eq(
                        "cache_key", cache_key
                    ).execute()
        except Exception as e:
            print(f"Cache retrieval error: {e}")
        
        return None
    
    async def _set_cached(self, cache_key: str, data: Dict[str, Any]):
        """Store response in Supabase cache."""
        if not self.supabase:
            return
        
        try:
            self.supabase.table("dataforseo_cache").upsert({
                "cache_key": cache_key,
                "response_data": data,
                "cached_at": datetime.utcnow().isoformat()
            }).execute()
        except Exception as e:
            print(f"Cache storage error: {e}")
    
    @backoff.on_exception(
        backoff.expo,
        (aiohttp.ClientError, asyncio.TimeoutError),
        max_tries=3,
        max_time=180
    )
    async def _make_request(
        self,
        session: ClientSession,
        endpoint: str,
        data: List[Dict[str, Any]],
        use_cache: bool = True
    ) -> Dict[str, Any]:
        """
        Make authenticated request to DataForSEO API with retry logic.
        
        Args:
            session: aiohttp ClientSession
            endpoint: API endpoint (e.g., "/v3/serp/google/organic/live/advanced")
            data: Request payload (list of task objects)
            use_cache: Whether to use caching
            
        Returns:
            API response as dictionary
        """
        # Check cache
        cache_key = self._generate_cache_key(endpoint, data[0] if data else {})
        if use_cache:
            cached = await self._get_cached(cache_key)
            if cached:
                return cached
        
        # Enforce rate limiting
        await self._enforce_rate_limit()
        
        url = f"{self.BASE_URL}{endpoint}"
        auth = aiohttp.BasicAuth(self.api_login, self.api_password)
        
        async with session.post(
            url,
            json=data,
            auth=auth,
            timeout=ClientTimeout(total=self.timeout_seconds)
        ) as response:
            response.raise_for_status()
            result = await response.json()
            
            # Cache successful response
            if use_cache and result.get("status_code") == 20000:
                await self._set_cached(cache_key, result)
            
            return result
    
    # ========================================================================
    # MODULE 3: Keyword Research
    # ========================================================================
    
    async def get_keyword_data(
        self,
        keywords: List[str],
        location_code: int = 2840,  # USA
        language_code: str = "en",
        use_cache: bool = True
    ) -> Dict[str, Any]:
        """
        Get keyword metrics (volume, difficulty, CPC, competition) for multiple keywords.
        
        Args:
            keywords: List of keywords to analyze
            location_code: DataForSEO location code (default: 2840 = USA)
            language_code: Language code (default: "en")
            use_cache: Whether to use caching
            
        Returns:
            {
                "keywords": [
                    {
                        "keyword": "best crm software",
                        "search_volume": 12000,
                        "competition": 0.67,
                        "cpc": 8.45,
                        "difficulty": 72,
                        "trend": [100, 95, 110, ...],  # 12-month trend
                        "related_keywords_count": 234
                    }
                ],
                "total_keywords": 50,
                "api_cost_usd": 0.05
            }
        """
        async with aiohttp.ClientSession() as session:
            # Split into batches of 100 (DataForSEO limit)
            batch_size = 100
            all_results = []
            
            for i in range(0, len(keywords), batch_size):
                batch = keywords[i:i + batch_size]
                
                data = [{
                    "location_code": location_code,
                    "language_code": language_code,
                    "keywords": batch
                }]
                
                try:
                    response = await self._make_request(
                        session,
                        "/v3/dataforseo_labs/google/keyword_data/live",
                        data,
                        use_cache
                    )
                    
                    if response.get("status_code") == 20000 and response.get("tasks"):
                        task = response["tasks"][0]
                        if task.get("result"):
                            all_results.extend(task["result"])
                except Exception as e:
                    print(f"Error fetching keyword data batch: {e}")
                    continue
            
            # Format results
            formatted_keywords = []
            for item in all_results:
                formatted_keywords.append({
                    "keyword": item.get("keyword"),
                    "search_volume": item.get("search_volume"),
                    "competition": item.get("competition"),
                    "cpc": item.get("cpc"),
                    "difficulty": item.get("keyword_difficulty"),
                    "trend": item.get("monthly_searches", []),
                    "related_keywords_count": item.get("related_keywords_count", 0)
                })
            
            return {
                "keywords": formatted_keywords,
                "total_keywords": len(formatted_keywords),
                "api_cost_usd": len(keywords) * 0.001  # Approximate cost
            }
    
    async def get_related_keywords(
        self,
        seed_keyword: str,
        location_code: int = 2840,
        language_code: str = "en",
        limit: int = 50,
        use_cache: bool = True
    ) -> Dict[str, Any]:
        """
        Get related keywords for a seed keyword.
        
        Args:
            seed_keyword: The seed keyword
            location_code: DataForSEO location code
            language_code: Language code
            limit: Maximum number of related keywords to return
            use_cache: Whether to use caching
            
        Returns:
            {
                "seed_keyword": "crm software",
                "related_keywords": [
                    {
                        "keyword": "best crm software",
                        "search_volume": 12000,
                        "relevance": 0.89,
                        "difficulty": 72
                    }
                ],
                "total_found": 234
            }
        """
        async with aiohttp.ClientSession() as session:
            data = [{
                "location_code": location_code,
                "language_code": language_code,
                "keyword": seed_keyword,
                "limit": limit,
                "include_serp_info": False,
                "include_seed_keyword": False
            }]
            
            try:
                response = await self._make_request(
                    session,
                    "/v3/dataforseo_labs/google/related_keywords/live",
                    data,
                    use_cache
                )
                
                if response.get("status_code") == 20000 and response.get("tasks"):
                    task = response["tasks"][0]
                    if task.get("result"):
                        items = task["result"][0].get("items", [])
                        
                        related = []
                        for item in items:
                            related.append({
                                "keyword": item.get("keyword"),
                                "search_volume": item.get("search_volume"),
                                "relevance": item.get("relevance"),
                                "difficulty": item.get("keyword_difficulty")
                            })
                        
                        return {
                            "seed_keyword": seed_keyword,
                            "related_keywords": related,
                            "total_found": task["result"][0].get("total_count", len(items))
                        }
            except Exception as e:
                print(f"Error fetching related keywords: {e}")
            
            return {
                "seed_keyword": seed_keyword,
                "related_keywords": [],
                "total_found": 0
            }
    
    # ========================================================================
    # MODULE 8: SERP Features Extraction
    # ========================================================================
    
    async def get_serp_features(
        self,
        keyword: str,
        location_code: int = 2840,
        language_code: str = "en",
        device: str = "desktop",
        use_cache: bool = True
    ) -> Dict[str, Any]:
        """
        Get live SERP data with full feature extraction for a single keyword.
        
        Args:
            keyword: Search query
            location_code: DataForSEO location code
            language_code: Language code
            device: "desktop" or "mobile"
            use_cache: Whether to use caching
            
        Returns:
            {
                "keyword": "best crm software",
                "se_results_count": 45000000,
                "organic_results": [
                    {
                        "position": 1,
                        "url": "https://example.com/page",
                        "domain": "example.com",
                        "title": "...",
                        "description": "...",
                        "breadcrumb": "example.com > blog > crm"
                    }
                ],
                "features": {
                    "featured_snippet": {
                        "present": true,
                        "type": "paragraph",
                        "url": "https://example.com/snippet-page"
                    },
                    "people_also_ask": {
                        "present": true,
                        "count": 4,
                        "questions": ["...", "..."]
                    },
                    "video_carousel": {
                        "present": false
                    },
                    "local_pack": {
                        "present": false
                    },
                    "knowledge_panel": {
                        "present": false
                    },
                    "ai_overview": {
                        "present": false
                    },
                    "image_pack": {
                        "present": true,
                        "position": 3
                    },
                    "shopping_results": {
                        "present": false
                    },
                    "top_stories": {
                        "present": false
                    },
                    "site_links": {
                        "present": true,
                        "for_domain": "example.com"
                    }
                },
                "visual_position_adjustments": {
                    "organic_1_visual_position": 5.0  # Actual visual position accounting for features
                }
            }
        """
        async with aiohttp.ClientSession() as session:
            data = [{
                "location_code": location_code,
                "language_code": language_code,
                "keyword": keyword,
                "device": device,
                "os": "windows" if device == "desktop" else "ios"
            }]
            
            try:
                response = await self._make_request(
                    session,
                    "/v3/serp/google/organic/live/advanced",
                    data,
                    use_cache
                )
                
                if response.get("status_code") == 20000 and response.get("tasks"):
                    task = response["tasks"][0]
                    if task.get("result"):
                        result = task["result"][0]
                        items = result.get("items", [])
                        
                        # Extract organic results
                        organic_results = []
                        for item in items:
                            if item.get("type") == "organic":
                                organic_results.append({
                                    "position": item.get("rank_group"),
                                    "url": item.get("url"),
                                    "domain": item.get("domain"),
                                    "title": item.get("title"),
                                    "description": item.get("description"),
                                    "breadcrumb": item.get("breadcrumb")
                                })
                        
                        # Extract SERP features
                        features = self._extract_serp_features(items)
                        
                        # Calculate visual positions
                        visual_adjustments = self._calculate_visual_positions(items, organic_results)
                        
                        return {
                            "keyword": keyword,
                            "se_results_count": result.get("se_results_count"),
                            "organic_results": organic_results,
                            "features": features,
                            "visual_position_adjustments": visual_adjustments
                        }
            except Exception as e:
                print(f"Error fetching SERP data for '{keyword}': {e}")
            
            return {
                "keyword": keyword,
                "se_results_count": 0,
                "organic_results": [],
                "features": {},
                "visual_position_adjustments": {}
            }
    
    async def get_bulk_serp_features(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
        device: str = "desktop",
        use_cache: bool = True,
        max_concurrent: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Get SERP features for multiple keywords concurrently.
        
        Args:
            keywords: List of keywords
            location_code: DataForSEO location code
            language_code: Language code
            device: "desktop" or "mobile"
            use_cache: Whether to use caching
            max_concurrent: Maximum concurrent requests
            
        Returns:
            List of SERP feature dictionaries (same format as get_serp_features)
        """
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def fetch_with_semaphore(keyword):
            async with semaphore:
                return await self.get_serp_features(
                    keyword, location_code, language_code, device, use_cache
                )
        
        tasks = [fetch_with_semaphore(kw) for kw in keywords]
        return await asyncio.gather(*tasks)
    
    def _extract_serp_features(self, items: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Extract SERP features from items list."""
        features = {
            "featured_snippet": {"present": False},
            "people_also_ask": {"present": False, "count": 0, "questions": []},
            "video_carousel": {"present": False},
            "local_pack": {"present": False},
            "knowledge_panel": {"present": False},
            "ai_overview": {"present": False},
            "image_pack": {"present": False, "position": None},
            "shopping_results": {"present": False},
            "top_stories": {"present": False},
            "site_links": {"present": False, "for_domain": None}
        }
        
        for item in items:
            item_type = item.get("type")
            
            if item_type == "featured_snippet":
                features["featured_snippet"] = {
                    "present": True,
                    "type": item.get("snippet_type"),
                    "url": item.get("url")
                }
            
            elif item_type == "people_also_ask":
                questions = []
                for q in item.get("items", []):
                    questions.append(q.get("title"))
                features["people_also_ask"] = {
                    "present": True,
                    "count": len(questions),
                    "questions": questions
                }
            
            elif item_type == "video":
                features["video_carousel"]["present"] = True
            
            elif item_type == "local_pack":
                features["local_pack"]["present"] = True
            
            elif item_type == "knowledge_panel":
                features["knowledge_panel"]["present"] = True
            
            elif item_type == "ai_overview":
                features["ai_overview"]["present"] = True
            
            elif item_type == "images":
                features["image_pack"] = {
                    "present": True,
                    "position": item.get("rank_group")
                }
            
            elif item_type == "shopping":
                features["shopping_results"]["present"] = True
            
            elif item_type == "top_stories":
                features["top_stories"]["present"] = True
            
            elif item_type == "organic" and item.get("links"):
                # Site links detected
                features["site_links"] = {
                    "present": True,
                    "for_domain": item.get("domain")
                }
        
        return features
    
    def _calculate_visual_positions(
        self,
        items: List[Dict[str, Any]],
        organic_results: List[Dict[str, Any]]
    ) -> Dict[str, float]:
        """
        Calculate visual position adjustments based on SERP features.
        
        Each SERP feature takes up visual space:
        - Featured snippet: 2.0 positions
        - Each PAA question: 0.5 positions
        - Knowledge panel: 0 (usually on side)
        - Video carousel: 1.5 positions
        - Local pack: 2.0 positions
        - Image pack: 1.0 position
        - Shopping results: 1.5 positions
        - Top stories: 1.0 position
        - AI Overview: 3.0 positions
        """
        adjustments = {}
        
        # Build position map of all items
        position_map = {}
        for item in items:
            rank = item.get("rank_group")
            item_type = item.get("type")
            if rank:
                if rank not in position_map:
                    position_map[rank] = []
                position_map[rank].append(item_type)
        
        # Calculate cumulative visual displacement for each organic result
        for organic in organic_results:
            position = organic["position"]
            visual_displacement = 0.0
            
            # Check all items appearing before this organic result
            for rank in sorted(position_map.keys()):
                if rank >= position:
                    break
                
                for item_type in position_map[rank]:
                    if item_type == "featured_snippet":
                        visual_displacement += 2.0
                    elif item_type == "people_also_ask":
                        visual_displacement += 0.5
                    elif item_type == "video":
                        visual_displacement += 1.5
                    elif item_type == "local_pack":
                        visual_displacement += 2.0
                    elif item_type == "images":
                        visual_displacement += 1.0
                    elif item_type == "shopping":
                        visual_displacement += 1.5
                    elif item_type == "top_stories":
                        visual_displacement += 1.0
                    elif item_type == "ai_overview":
                        visual_displacement += 3.0
            
            visual_position = position + visual_displacement
            adjustments[f"organic_{position}_visual_position"] = visual_position
        
        return adjustments
    
    # ========================================================================
    # MODULE 11: Competitor SERP Position Tracking
    # ========================================================================
    
    async def get_competitor_rankings(
        self,
        keywords: List[str],
        target_domains: List[str],
        location_code: int = 2840,
        language_code: str = "en",
        use_cache: bool = True
    ) -> Dict[str, Any]:
        """
        Track competitor positions for multiple keywords.
        
        Args:
            keywords: List of keywords to track
            target_domains: List of competitor domains to track
            location_code: DataForSEO location code
            language_code: Language code
            use_cache: Whether to use caching
            
        Returns:
            {
                "competitors": {
                    "competitor.com": {
                        "keywords_ranking": 34,
                        "avg_position": 4.2,
                        "keywords": [
                            {
                                "keyword": "best crm",
                                "position": 3,
                                "url": "https://competitor.com/page"
                            }
                        ]
                    }
                },
                "keyword_overlap": {
                    "best crm": {
                        "target_position": 5,
                        "competitors": {
                            "competitor.com": 3,
                            "another.com": 7
                        }
                    }
                },
                "total_keywords_analyzed": 87
            }
        """
        # Get SERP data for all keywords
        serp_results = await self.get_bulk_serp_features(
            keywords, location_code, language_code, "desktop", use_cache
        )
        
        # Initialize competitor tracking
        competitor_data = {domain: {
            "keywords_ranking": 0,
            "positions": [],
            "keywords": []
        } for domain in target_domains}
        
        keyword_overlap = {}
        
        # Process each SERP result
        for serp in serp_results:
            keyword = serp["keyword"]
            keyword_overlap[keyword] = {"competitors": {}}
            
            for result in serp["organic_results"]:
                domain = result["domain"]
                position = result["position"]
                
                if domain in competitor_data:
                    competitor_data[domain]["keywords_ranking"] += 1
                    competitor_data[domain]["positions"].append(position)
                    competitor_data[domain]["keywords"].append({
                        "keyword": keyword,
                        "position": position,
                        "url": result["url"]
                    })
                    
                    keyword_overlap[keyword]["competitors"][domain] = position
        
        # Calculate average positions
        competitors = {}
        for domain, data in competitor_data.items():
            if data["positions"]:
                avg_pos = sum(data["positions"]) / len(data["positions"])
            else:
                avg_pos = None
            
            competitors[domain] = {
                "keywords_ranking": data["keywords_ranking"],
                "avg_position": round(avg_pos, 1) if avg_pos else None,
                "keywords": data["keywords"]
            }
        
        return {
            "competitors": competitors,
            "keyword_overlap": keyword_overlap,
            "total_keywords_analyzed": len(keywords)
        }
    
    async def get_domain_keywords(
        self,
        domain: str,
        location_code: int = 2840,
        language_code: str = "en",
        limit: int = 100,
        use_cache: bool = True
    ) -> Dict[str, Any]:
        """
        Get all keywords a domain ranks for (DataForSEO Labs feature).
        
        Args:
            domain: Target domain
            location_code: DataForSEO location code
            language_code: Language code
            limit: Maximum keywords to return
            use_cache: Whether to use caching
            
        Returns:
            {
                "domain": "example.com",
                "keywords": [
                    {
                        "keyword": "best crm",
                        "position": 3,
                        "search_volume": 12000,
                        "estimated_traffic": 840
                    }
                ],
                "total_keywords": 2540,
                "estimated_total_traffic": 45600
            }
        """
        async with aiohttp.ClientSession() as session:
            data = [{
                "location_code": location_code,
                "language_code": language_code,
                "target": domain,
                "limit": limit,
                "order_by": ["ranked_serp_element.serp_item.rank_group,asc"]
            }]
            
            try:
                response = await self._make_request(
                    session,
                    "/v3/dataforseo_labs/google/ranked_keywords/live",
                    data,
                    use_cache
                )
                
                if response.get("status_code") == 20000 and response.get("tasks"):
                    task = response["tasks"][0]
                    if task.get("result"):
                        items = task["result"][0].get("items", [])
                        
                        keywords = []
                        total_traffic = 0
                        
                        for item in items:
                            kw_data = item.get("keyword_data", {})
                            serp = item.get("ranked_serp_element", {}).get("serp_item", {})
                            
                            search_volume = kw_data.get("search_volume", 0)
                            position = serp.get("rank_group")
                            
                            # Estimate traffic using position-based CTR curve
                            ctr = self._estimate_ctr(position) if position else 0
                            estimated_traffic = int(search_volume * ctr)
                            total_traffic += estimated_traffic
                            
                            keywords.append({
                                "keyword": kw_data.get("keyword"),
                                "position": position,
                                "search_volume": search_volume,
                                "estimated_traffic": estimated_traffic
                            })
                        
                        return {
                            "domain": domain,
                            "keywords": keywords,
                            "total_keywords": task["result"][0].get("total_count", len(items)),
                            "estimated_total_traffic": total_traffic
                        }
            except Exception as e:
                print(f"Error fetching domain keywords: {e}")
            
            return {
                "domain": domain,
                "keywords": [],
                "total_keywords": 0,
                "estimated_total_traffic": 0
            }
    
    def _estimate_ctr(self, position: int) -> float:
        """
        Estimate CTR based on position using advanced CTR curve.
        Based on industry research (Advanced Web Ranking, Sistrix).
        """
        if position is None or position < 1:
            return 0.0
        
        # CTR curve (approximate values)
        ctr_map = {
            1: 0.28,
            2: 0.15,
            3: 0.10,
            4: 0.07,
            5: 0.05,
            6: 0.04,
            7: 0.03,
            8: 0.025,
            9: 0.02,
            10: 0.015
        }
        
        if position in ctr_map:
            return ctr_map[position]
        elif position <= 20:
            return 0.01
        elif position <= 50:
            return 0.005
        else:
            return 0.001
    
    # ========================================================================
    # Historical Data & Tracking
    # ========================================================================
    
    async def store_serp_snapshot(
        self,
        keyword: str,
        serp_data: Dict[str, Any],
        domain: str
    ):
        """
        Store a SERP snapshot in Supabase for historical tracking.
        
        Args:
            keyword: The keyword
            serp_data: SERP data from get_serp_features
            domain: User's domain
        """
        if not self.supabase:
            return
        
        try:
            # Extract user's position if present
            user_position = None
            user_url = None
            for result in serp_data.get("organic_results", []):
                if domain in result.get("domain", ""):
                    user_position = result["position"]
                    user_url = result["url"]
                    break
            
            # Store snapshot
            self.supabase.table("serp_snapshots").insert({
                "keyword": keyword,
                "domain": domain,
                "snapshot_date": datetime.utcnow().isoformat(),
                "user_position": user_position,
                "user_url": user_url,
                "serp_features": serp_data.get("features"),
                "top_10_domains": [
                    r["domain"] for r in serp_data.get("organic_results", [])[:10]
                ],
                "se_results_count": serp_data.get("se_results_count")
            }).execute()
        except Exception as e:
            print(f"Error storing SERP snapshot: {e}")
    
    async def get_serp_history(
        self,
        keyword: str,
        domain: str,
        days: int = 90
    ) -> List[Dict[str, Any]]:
        """
        Retrieve historical SERP snapshots for a keyword.
        
        Args:
            keyword: The keyword
            domain: User's domain
            days: Number of days of history to retrieve
            
        Returns:
            List of historical snapshots
        """
        if not self.supabase:
            return []
        
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=days)
            
            result = self.supabase.table("serp_snapshots").select("*").eq(
                "keyword", keyword
            ).eq(
                "domain", domain
            ).gte(
                "snapshot_date", cutoff_date.isoformat()
            ).order(
                "snapshot_date", desc=False
            ).execute()
            
            return result.data if result.data else []
        except Exception as e:
            print(f"Error retrieving SERP history: {e}")
            return []


# Convenience function for creating service instance
def create_dataforseo_service(
    supabase_client: Optional[SupabaseClient] = None,
    **kwargs
) -> DataForSEOService:
    """
    Create DataForSEO service instance with default settings.
    
    Args:
        supabase_client: Optional Supabase client
        **kwargs: Additional arguments to pass to DataForSEOService
        
    Returns:
        Configured DataForSEOService instance
    """
    return DataForSEOService(supabase_client=supabase_client, **kwargs)
