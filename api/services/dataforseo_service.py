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

from api.config import settings


class DataForSEOService:
    """
    DataForSEO API client service for:
    1. Live SERP data (organic results, SERP features)
    2. Competitor domain discovery
    3. Keyword ranking data
    4. CTR modeling data
    
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
    
    # SERP feature types we care about
    SERP_FEATURES = [
        'featured_snippet',
        'people_also_ask',
        'video_carousel',
        'local_pack',
        'knowledge_panel',
        'ai_overview',
        'image_pack',
        'shopping_results',
        'top_stories',
        'twitter',
        'related_searches'
    ]
    
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
        self.api_login = api_login or settings.DATAFORSEO_LOGIN
        self.api_password = api_password or settings.DATAFORSEO_PASSWORD
        
        if not self.api_login or not self.api_password:
            raise ValueError("DataForSEO credentials not provided")
        
        # Initialize Supabase client for caching
        if supabase_client:
            self.supabase = supabase_client
        else:
            if settings.SUPABASE_URL and settings.SUPABASE_SERVICE_KEY:
                self.supabase = create_client(settings.SUPABASE_URL, settings.SUPABASE_SERVICE_KEY)
            else:
                self.supabase = None
        
        self.cache_ttl_hours = cache_ttl_hours
        self.rate_limit_per_second = rate_limit_per_second
        self.timeout_seconds = timeout_seconds
        
        # Rate limiting state
        self._rate_limit_lock = asyncio.Lock()
        self._last_request_time = None
        self._min_request_interval = 1.0 / rate_limit_per_second
    
    async def _rate_limit_wait(self):
        """Implement rate limiting by enforcing minimum time between requests."""
        async with self._rate_limit_lock:
            if self._last_request_time:
                elapsed = asyncio.get_event_loop().time() - self._last_request_time
                if elapsed < self._min_request_interval:
                    await asyncio.sleep(self._min_request_interval - elapsed)
            self._last_request_time = asyncio.get_event_loop().time()
    
    def _generate_cache_key(self, endpoint: str, params: Dict[str, Any]) -> str:
        """Generate a unique cache key from endpoint and parameters."""
        param_str = json.dumps(params, sort_keys=True)
        key_input = f"{endpoint}:{param_str}"
        return hashlib.sha256(key_input.encode()).hexdigest()
    
    async def _get_cached_response(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """Retrieve cached response from Supabase if available and not expired."""
        if not self.supabase:
            return None
        
        try:
            result = self.supabase.table("dataforseo_cache").select("*").eq("cache_key", cache_key).execute()
            
            if result.data and len(result.data) > 0:
                cached = result.data[0]
                cached_at = datetime.fromisoformat(cached["created_at"].replace("Z", "+00:00"))
                expiry = cached_at + timedelta(hours=self.cache_ttl_hours)
                
                if datetime.now(cached_at.tzinfo) < expiry:
                    return cached["response_data"]
        except Exception as e:
            # Log error but don't fail - just proceed without cache
            print(f"Cache retrieval error: {str(e)}")
        
        return None
    
    async def _set_cached_response(self, cache_key: str, response_data: Dict[str, Any]):
        """Store response in Supabase cache."""
        if not self.supabase:
            return
        
        try:
            self.supabase.table("dataforseo_cache").upsert({
                "cache_key": cache_key,
                "response_data": response_data,
                "created_at": datetime.utcnow().isoformat()
            }).execute()
        except Exception as e:
            # Log error but don't fail
            print(f"Cache storage error: {str(e)}")
    
    @backoff.on_exception(
        backoff.expo,
        (aiohttp.ClientError, asyncio.TimeoutError),
        max_tries=5,
        max_time=300
    )
    async def _make_request(
        self,
        session: ClientSession,
        endpoint: str,
        method: str = "POST",
        data: Optional[List[Dict[str, Any]]] = None,
        use_cache: bool = True
    ) -> Dict[str, Any]:
        """
        Make authenticated request to DataForSEO API with retry logic.
        
        Args:
            session: aiohttp ClientSession
            endpoint: API endpoint (e.g., "/v3/serp/google/organic/live/advanced")
            method: HTTP method (POST, GET)
            data: Request payload (for POST requests)
            use_cache: Whether to use caching
        
        Returns:
            API response as dict
        
        Raises:
            aiohttp.ClientError: On API request failure
            ValueError: On invalid API response
        """
        url = f"{self.BASE_URL}{endpoint}"
        
        # Check cache
        cache_key = None
        if use_cache and data:
            cache_key = self._generate_cache_key(endpoint, data[0] if data else {})
            cached_response = await self._get_cached_response(cache_key)
            if cached_response:
                return cached_response
        
        # Rate limiting
        await self._rate_limit_wait()
        
        # Prepare request
        auth = aiohttp.BasicAuth(self.api_login, self.api_password)
        headers = {"Content-Type": "application/json"}
        
        # Make request
        async with session.request(
            method,
            url,
            auth=auth,
            headers=headers,
            json=data if method == "POST" else None,
            timeout=ClientTimeout(total=self.timeout_seconds)
        ) as response:
            if response.status == 429:
                # Rate limit hit - exponential backoff will retry
                raise aiohttp.ClientError("Rate limit exceeded")
            
            response.raise_for_status()
            result = await response.json()
            
            # Validate response structure
            if "tasks" not in result:
                raise ValueError(f"Invalid DataForSEO response structure: {result}")
            
            # Cache successful response
            if use_cache and cache_key and result.get("status_code") == 20000:
                await self._set_cached_response(cache_key, result)
            
            return result
    
    async def get_serp_data(
        self,
        keywords: List[str],
        location_code: int = 2840,  # United States
        language_code: str = "en",
        device: str = "desktop",
        depth: int = 100,
        use_cache: bool = True
    ) -> Dict[str, Dict[str, Any]]:
        """
        Get live SERP data for multiple keywords.
        
        Args:
            keywords: List of keywords to fetch SERP data for
            location_code: DataForSEO location code (2840 = United States)
            language_code: Language code (en, es, etc.)
            device: Device type (desktop, mobile)
            depth: Number of results to return (max 100)
            use_cache: Whether to use cached results
        
        Returns:
            Dict mapping keyword to parsed SERP data:
            {
                "keyword": {
                    "organic_results": [...],
                    "serp_features": {...},
                    "competitors": [...],
                    "metadata": {...}
                }
            }
        """
        results = {}
        
        async with ClientSession() as session:
            # Process keywords in batches to respect API limits
            batch_size = 100  # DataForSEO allows up to 100 tasks per request
            
            for i in range(0, len(keywords), batch_size):
                batch = keywords[i:i + batch_size]
                
                # Prepare tasks for batch
                tasks_data = []
                for keyword in batch:
                    tasks_data.append({
                        "keyword": keyword,
                        "location_code": location_code,
                        "language_code": language_code,
                        "device": device,
                        "depth": depth,
                        "calculate_rectangles": True  # For SERP feature positioning
                    })
                
                # Make request
                try:
                    response = await self._make_request(
                        session,
                        "/v3/serp/google/organic/live/advanced",
                        method="POST",
                        data=tasks_data,
                        use_cache=use_cache
                    )
                    
                    # Parse results
                    if response.get("status_code") == 20000 and response.get("tasks"):
                        for task in response["tasks"]:
                            if task.get("status_code") == 20000 and task.get("result"):
                                for result_item in task["result"]:
                                    keyword = result_item.get("keyword")
                                    if keyword:
                                        results[keyword] = self._parse_serp_result(result_item)
                except Exception as e:
                    print(f"Error fetching SERP data for batch: {str(e)}")
                    # Continue with next batch
        
        return results
    
    def _parse_serp_result(self, result_item: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse DataForSEO SERP result into our internal format.
        
        Args:
            result_item: Single result item from DataForSEO response
        
        Returns:
            Parsed SERP data with:
            - organic_results: List of organic search results
            - serp_features: Dict of SERP features present
            - competitors: List of unique domains
            - metadata: Search metadata
        """
        parsed = {
            "organic_results": [],
            "serp_features": {},
            "competitors": [],
            "metadata": {
                "keyword": result_item.get("keyword"),
                "search_engine": result_item.get("se_domain"),
                "location_code": result_item.get("location_code"),
                "language_code": result_item.get("language_code"),
                "device": result_item.get("device"),
                "total_results": result_item.get("se_results_count", 0),
                "check_url": result_item.get("check_url")
            }
        }
        
        items = result_item.get("items", [])
        
        # Initialize SERP features
        for feature in self.SERP_FEATURES:
            parsed["serp_features"][feature] = {
                "present": False,
                "count": 0,
                "visual_position": None,
                "data": []
            }
        
        visual_position = 0
        competitor_domains = set()
        
        for item in items:
            item_type = item.get("type")
            
            # Organic results
            if item_type == "organic":
                organic_data = {
                    "position": item.get("rank_group"),
                    "url": item.get("url"),
                    "domain": item.get("domain"),
                    "title": item.get("title"),
                    "description": item.get("description"),
                    "breadcrumb": item.get("breadcrumb"),
                    "visual_position": visual_position,
                    "is_featured": item.get("is_featured", False),
                    "is_malicious": item.get("is_malicious", False),
                    "timestamp": item.get("timestamp")
                }
                
                # Extract links data if available
                if "links" in item:
                    organic_data["sitelinks"] = item["links"]
                
                # Extract rating if available
                if "rating" in item and item["rating"]:
                    organic_data["rating"] = {
                        "value": item["rating"].get("rating_value"),
                        "votes": item["rating"].get("votes_count"),
                        "type": item["rating"].get("rating_type")
                    }
                
                parsed["organic_results"].append(organic_data)
                
                # Track competitors
                if item.get("domain"):
                    competitor_domains.add(item["domain"])
                
                visual_position += 1
            
            # Featured Snippet
            elif item_type == "featured_snippet":
                parsed["serp_features"]["featured_snippet"]["present"] = True
                parsed["serp_features"]["featured_snippet"]["count"] = 1
                parsed["serp_features"]["featured_snippet"]["visual_position"] = visual_position
                parsed["serp_features"]["featured_snippet"]["data"].append({
                    "url": item.get("url"),
                    "domain": item.get("domain"),
                    "title": item.get("title"),
                    "description": item.get("description"),
                    "table": item.get("table"),
                    "list": item.get("list")
                })
                visual_position += 2  # Featured snippets take ~2 position equivalents
            
            # People Also Ask
            elif item_type == "people_also_ask":
                if not parsed["serp_features"]["people_also_ask"]["present"]:
                    parsed["serp_features"]["people_also_ask"]["present"] = True
                    parsed["serp_features"]["people_also_ask"]["visual_position"] = visual_position
                
                parsed["serp_features"]["people_also_ask"]["count"] += 1
                
                items_data = item.get("items", [])
                for paa_item in items_data:
                    parsed["serp_features"]["people_also_ask"]["data"].append({
                        "question": paa_item.get("title"),
                        "url": paa_item.get("url"),
                        "domain": paa_item.get("domain")
                    })
                
                visual_position += len(items_data) * 0.5  # Each PAA = 0.5 position
            
            # Video Carousel
            elif item_type in ("video", "video_carousel"):
                if not parsed["serp_features"]["video_carousel"]["present"]:
                    parsed["serp_features"]["video_carousel"]["present"] = True
                    parsed["serp_features"]["video_carousel"]["visual_position"] = visual_position
                
                parsed["serp_features"]["video_carousel"]["count"] += 1
                parsed["serp_features"]["video_carousel"]["data"].append({
                    "title": item.get("title"),
                    "url": item.get("url"),
                    "domain": item.get("domain"),
                    "source": item.get("source")
                })
                visual_position += 1
            
            # Local Pack
            elif item_type == "local_pack":
                parsed["serp_features"]["local_pack"]["present"] = True
                parsed["serp_features"]["local_pack"]["count"] = 1
                parsed["serp_features"]["local_pack"]["visual_position"] = visual_position
                
                items_data = item.get("items", [])
                for local_item in items_data:
                    parsed["serp_features"]["local_pack"]["data"].append({
                        "title": local_item.get("title"),
                        "url": local_item.get("url"),
                        "domain": local_item.get("domain"),
                        "rating": local_item.get("rating")
                    })
                
                visual_position += 3  # Local pack takes ~3 positions
            
            # Knowledge Panel
            elif item_type == "knowledge_graph":
                parsed["serp_features"]["knowledge_panel"]["present"] = True
                parsed["serp_features"]["knowledge_panel"]["count"] = 1
                parsed["serp_features"]["knowledge_panel"]["visual_position"] = visual_position
                parsed["serp_features"]["knowledge_panel"]["data"].append({
                    "title": item.get("title"),
                    "description": item.get("description"),
                    "url": item.get("url")
                })
                # Knowledge panels are on the side, don't increment visual position
            
            # AI Overview (if available in newer API versions)
            elif item_type in ("ai_overview", "generative_ai"):
                parsed["serp_features"]["ai_overview"]["present"] = True
                parsed["serp_features"]["ai_overview"]["count"] = 1
                parsed["serp_features"]["ai_overview"]["visual_position"] = visual_position
                parsed["serp_features"]["ai_overview"]["data"].append({
                    "text": item.get("description", ""),
                    "sources": item.get("items", [])
                })
                visual_position += 3  # AI overviews take significant space
            
            # Image Pack
            elif item_type == "images":
                parsed["serp_features"]["image_pack"]["present"] = True
                parsed["serp_features"]["image_pack"]["count"] = 1
                parsed["serp_features"]["image_pack"]["visual_position"] = visual_position
                visual_position += 1
            
            # Shopping Results
            elif item_type in ("shopping", "paid_shopping"):
                if not parsed["serp_features"]["shopping_results"]["present"]:
                    parsed["serp_features"]["shopping_results"]["present"] = True
                    parsed["serp_features"]["shopping_results"]["visual_position"] = visual_position
                
                parsed["serp_features"]["shopping_results"]["count"] += 1
                visual_position += 0.5
            
            # Top Stories
            elif item_type == "top_stories":
                parsed["serp_features"]["top_stories"]["present"] = True
                parsed["serp_features"]["top_stories"]["count"] = 1
                parsed["serp_features"]["top_stories"]["visual_position"] = visual_position
                visual_position += 2
            
            # Twitter
            elif item_type == "twitter":
                parsed["serp_features"]["twitter"]["present"] = True
                parsed["serp_features"]["twitter"]["count"] = 1
                parsed["serp_features"]["twitter"]["visual_position"] = visual_position
                visual_position += 1
            
            # Related Searches
            elif item_type == "related_searches":
                parsed["serp_features"]["related_searches"]["present"] = True
                parsed["serp_features"]["related_searches"]["count"] = 1
                items_data = item.get("items", [])
                for related_item in items_data:
                    parsed["serp_features"]["related_searches"]["data"].append({
                        "query": related_item.get("title")
                    })
        
        # Set competitors list
        parsed["competitors"] = sorted(list(competitor_domains))
        
        # Add SERP feature summary
        parsed["serp_features"]["summary"] = {
            "total_features": sum(1 for f in parsed["serp_features"].values() 
                                 if isinstance(f, dict) and f.get("present")),
            "feature_types": [name for name, data in parsed["serp_features"].items() 
                            if isinstance(data, dict) and data.get("present")]
        }
        
        return parsed
    
    async def get_serp_data_batch(
        self,
        keyword_configs: List[Dict[str, Any]],
        use_cache: bool = True
    ) -> Dict[str, Dict[str, Any]]:
        """
        Get SERP data for multiple keywords with different configurations.
        
        Args:
            keyword_configs: List of keyword configuration dicts:
                [
                    {
                        "keyword": "best crm",
                        "location_code": 2840,
                        "language_code": "en",
                        "device": "desktop"
                    },
                    ...
                ]
            use_cache: Whether to use cached results
        
        Returns:
            Dict mapping keyword to parsed SERP data
        """
        results = {}
        
        async with ClientSession() as session:
            batch_size = 100
            
            for i in range(0, len(keyword_configs), batch_size):
                batch = keyword_configs[i:i + batch_size]
                
                tasks_data = []
                for config in batch:
                    task = {
                        "keyword": config["keyword"],
                        "location_code": config.get("location_code", 2840),
                        "language_code": config.get("language_code", "en"),
                        "device": config.get("device", "desktop"),
                        "depth": config.get("depth", 100),
                        "calculate_rectangles": True
                    }
                    tasks_data.append(task)
                
                try:
                    response = await self._make_request(
                        session,
                        "/v3/serp/google/organic/live/advanced",
                        method="POST",
                        data=tasks_data,
                        use_cache=use_cache
                    )
                    
                    if response.get("status_code") == 20000 and response.get("tasks"):
                        for task in response["tasks"]:
                            if task.get("status_code") == 20000 and task.get("result"):
                                for result_item in task["result"]:
                                    keyword = result_item.get("keyword")
                                    if keyword:
                                        results[keyword] = self._parse_serp_result(result_item)
                except Exception as e:
                    print(f"Error fetching SERP data batch: {str(e)}")
        
        return results
    
    def calculate_visual_displacement(
        self,
        organic_position: int,
        serp_features: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Calculate how much SERP features push down an organic result.
        
        Args:
            organic_position: Organic ranking position (1-100)
            serp_features: Parsed SERP features dict
        
        Returns:
            {
                "visual_position": int,  # Estimated visual position
                "displacement": int,  # How many positions pushed down
                "features_above": List[str],  # Features appearing above this position
                "estimated_ctr_impact": float  # Estimated CTR reduction (0-1)
            }
        """
        features_above = []
        displacement = 0
        
        for feature_name, feature_data in serp_features.items():
            if feature_name == "summary":
                continue
            
            if (feature_data.get("present") and 
                feature_data.get("visual_position") is not None):
                
                # If feature appears before this organic position
                if feature_data["visual_position"] < organic_position:
                    features_above.append(feature_name)
                    
                    # Add displacement based on feature type
                    if feature_name == "featured_snippet":
                        displacement += 2
                    elif feature_name == "people_also_ask":
                        displacement += feature_data.get("count", 1) * 0.5
                    elif feature_name == "local_pack":
                        displacement += 3
                    elif feature_name == "ai_overview":
                        displacement += 3
                    elif feature_name == "video_carousel":
                        displacement += 1
                    elif feature_name == "top_stories":
                        displacement += 2
                    else:
                        displacement += 1
        
        visual_position = organic_position + int(displacement)
        
        # Estimate CTR impact using position-based CTR curve
        # Baseline CTR by position (approximate)
        position_ctr = {
            1: 0.316, 2: 0.158, 3: 0.100, 4: 0.077, 5: 0.061,
            6: 0.048, 7: 0.039, 8: 0.033, 9: 0.028, 10: 0.025
        }
        
        organic_ctr = position_ctr.get(organic_position, 0.01)
        visual_ctr = position_ctr.get(visual_position, 0.01)
        ctr_impact = visual_ctr - organic_ctr
        
        return {
            "visual_position": visual_position,
            "displacement": int(displacement),
            "features_above": features_above,
            "estimated_ctr_impact": round(ctr_impact, 4)
        }
    
    def extract_competitors(
        self,
        serp_results: Dict[str, Dict[str, Any]],
        min_appearances: int = 2
    ) -> List[Dict[str, Any]]:
        """
        Extract competitor domains across multiple SERP results.
        
        Args:
            serp_results: Dict of keyword -> parsed SERP data
            min_appearances: Minimum keyword appearances to be considered competitor
        
        Returns:
            List of competitor data:
            [
                {
                    "domain": "competitor.com",
                    "appearances": 15,
                    "keywords": ["keyword1", "keyword2", ...],
                    "avg_position": 4.2,
                    "position_range": [1, 10]
                },
                ...
            ]
        """
        domain_data = {}
        
        for keyword, serp_data in serp_results.items():
            for result in serp_data.get("organic_results", []):
                domain = result.get("domain")
                position = result.get("position")
                
                if not domain or not position:
                    continue
                
                if domain not in domain_data:
                    domain_data[domain] = {
                        "domain": domain,
                        "keywords": [],
                        "positions": []
                    }
                
                domain_data[domain]["keywords"].append(keyword)
                domain_data[domain]["positions"].append(position)
        
        # Filter and calculate stats
        competitors = []
        for domain, data in domain_data.items():
            if len(data["keywords"]) >= min_appearances:
                positions = data["positions"]
                competitors.append({
                    "domain": domain,
                    "appearances": len(data["keywords"]),
                    "keywords": data["keywords"],
                    "avg_position": round(sum(positions) / len(positions), 2),
                    "position_range": [min(positions), max(positions)],
                    "median_position": sorted(positions)[len(positions) // 2]
                })
        
        # Sort by appearances descending
        competitors.sort(key=lambda x: x["appearances"], reverse=True)
        
        return competitors
    
    def classify_serp_intent(self, serp_features: Dict[str, Any]) -> str:
        """
        Classify search intent based on SERP features present.
        
        Args:
            serp_features: Parsed SERP features dict
        
        Returns:
            Intent classification: "informational", "commercial", "transactional", "navigational"
        """
        # Navigational signals
        if serp_features.get("knowledge_panel", {}).get("present"):
            return "navigational"
        
        # Transactional signals
        if (serp_features.get("shopping_results", {}).get("present") and
            serp_features["shopping_results"].get("count", 0) > 3):
            return "transactional"
        
        # Commercial signals
        if serp_features.get("shopping_results", {}).get("present"):
            return "commercial"
        
        # Informational signals
        paa_count = serp_features.get("people_also_ask", {}).get("count", 0)
        if paa_count >= 4 or serp_features.get("featured_snippet", {}).get("present"):
            return "informational"
        
        # Default to informational
        return "informational"
    
    async def close(self):
        """Cleanup resources."""
        # No persistent connections to close in current implementation
        pass