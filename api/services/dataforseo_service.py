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
        self._rate_limit_semaphore = asyncio.Semaphore(rate_limit_per_second)
        self._last_request_times: List[float] = []
    
    async def _enforce_rate_limit(self):
        """Enforce rate limiting using sliding window algorithm."""
        now = asyncio.get_event_loop().time()
        
        # Remove timestamps older than 1 second
        self._last_request_times = [
            ts for ts in self._last_request_times 
            if now - ts < 1.0
        ]
        
        # If we've hit the limit, wait until the oldest request is 1 second old
        if len(self._last_request_times) >= self.rate_limit_per_second:
            oldest = self._last_request_times[0]
            wait_time = 1.0 - (now - oldest)
            if wait_time > 0:
                await asyncio.sleep(wait_time)
        
        self._last_request_times.append(now)
    
    def _generate_cache_key(self, endpoint: str, params: Dict[str, Any]) -> str:
        """Generate cache key from endpoint and parameters."""
        # Sort params for consistent hashing
        sorted_params = json.dumps(params, sort_keys=True)
        combined = f"{endpoint}:{sorted_params}"
        return hashlib.sha256(combined.encode()).hexdigest()
    
    async def _get_cached_response(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """Retrieve cached response from Supabase."""
        if not self.supabase:
            return None
        
        try:
            result = self.supabase.table('dataforseo_cache').select('*').eq('cache_key', cache_key).execute()
            
            if result.data and len(result.data) > 0:
                cached = result.data[0]
                
                # Check if cache is still valid
                cached_at = datetime.fromisoformat(cached['cached_at'].replace('Z', '+00:00'))
                age = datetime.now(cached_at.tzinfo) - cached_at
                
                if age < timedelta(hours=self.cache_ttl_hours):
                    return cached['response_data']
                else:
                    # Delete expired cache
                    self.supabase.table('dataforseo_cache').delete().eq('cache_key', cache_key).execute()
            
            return None
        except Exception as e:
            print(f"Cache retrieval error: {e}")
            return None
    
    async def _set_cached_response(self, cache_key: str, response_data: Dict[str, Any]):
        """Store response in Supabase cache."""
        if not self.supabase:
            return
        
        try:
            cache_entry = {
                'cache_key': cache_key,
                'response_data': response_data,
                'cached_at': datetime.utcnow().isoformat()
            }
            
            # Upsert (insert or update)
            self.supabase.table('dataforseo_cache').upsert(cache_entry).execute()
        except Exception as e:
            print(f"Cache storage error: {e}")
    
    @backoff.on_exception(
        backoff.expo,
        (aiohttp.ClientError, asyncio.TimeoutError),
        max_tries=3,
        max_time=300
    )
    async def _make_request(
        self,
        endpoint: str,
        method: str = "POST",
        data: Optional[List[Dict[str, Any]]] = None,
        use_cache: bool = True
    ) -> Dict[str, Any]:
        """
        Make authenticated request to DataForSEO API with retry logic.
        
        Args:
            endpoint: API endpoint path (e.g., "/v3/serp/google/organic/live/advanced")
            method: HTTP method (GET or POST)
            data: Request payload (for POST requests)
            use_cache: Whether to use caching
        
        Returns:
            API response data
        """
        url = f"{self.BASE_URL}{endpoint}"
        
        # Check cache
        if use_cache and method == "POST" and data:
            cache_key = self._generate_cache_key(endpoint, data[0] if data else {})
            cached_response = await self._get_cached_response(cache_key)
            if cached_response:
                return cached_response
        
        # Enforce rate limiting
        await self._enforce_rate_limit()
        
        # Create auth
        auth = aiohttp.BasicAuth(self.api_login, self.api_password)
        timeout = ClientTimeout(total=self.timeout_seconds)
        
        async with ClientSession(auth=auth, timeout=timeout) as session:
            try:
                if method == "POST":
                    async with session.post(url, json=data) as response:
                        response.raise_for_status()
                        result = await response.json()
                else:
                    async with session.get(url) as response:
                        response.raise_for_status()
                        result = await response.json()
                
                # Check for API errors
                if result.get('status_code') != 20000:
                    raise Exception(f"DataForSEO API error: {result.get('status_message', 'Unknown error')}")
                
                # Cache successful response
                if use_cache and method == "POST" and data:
                    await self._set_cached_response(cache_key, result)
                
                return result
            
            except aiohttp.ClientResponseError as e:
                if e.status == 401:
                    raise Exception("DataForSEO authentication failed. Check credentials.")
                elif e.status == 429:
                    raise Exception("DataForSEO rate limit exceeded.")
                else:
                    raise Exception(f"DataForSEO API error: {e.status} - {e.message}")
    
    async def get_live_serp(
        self,
        keyword: str,
        location_code: int = 2840,  # US
        language_code: str = "en",
        device: str = "desktop",
        depth: int = 100
    ) -> Dict[str, Any]:
        """
        Get live SERP data for a keyword.
        
        Args:
            keyword: Search query
            location_code: Location code (2840 = US, 2826 = UK, etc.)
            language_code: Language code
            device: Device type (desktop, mobile)
            depth: Number of results to fetch (max 700)
        
        Returns:
            {
                "keyword": str,
                "organic_results": [...],
                "serp_features": {...},
                "total_count": int
            }
        """
        endpoint = "/v3/serp/google/organic/live/advanced"
        
        payload = [{
            "keyword": keyword,
            "location_code": location_code,
            "language_code": language_code,
            "device": device,
            "os": "windows" if device == "desktop" else "ios",
            "depth": depth,
            "calculate_rectangles": True  # For SERP feature positioning
        }]
        
        response = await self._make_request(endpoint, method="POST", data=payload)
        
        # Parse response
        if not response.get('tasks') or not response['tasks'][0].get('result'):
            return {
                "keyword": keyword,
                "organic_results": [],
                "serp_features": {},
                "total_count": 0
            }
        
        task_result = response['tasks'][0]['result'][0]
        
        # Extract organic results
        organic_results = []
        for item in task_result.get('items', []):
            if item.get('type') == 'organic':
                organic_results.append({
                    "position": item.get('rank_absolute'),
                    "url": item.get('url'),
                    "domain": item.get('domain'),
                    "title": item.get('title'),
                    "description": item.get('description'),
                    "breadcrumb": item.get('breadcrumb'),
                    "is_featured": item.get('is_featured', False),
                    "rectangle": item.get('rectangle')  # Visual position data
                })
        
        # Extract SERP features
        serp_features = self._extract_serp_features(task_result.get('items', []))
        
        return {
            "keyword": keyword,
            "organic_results": organic_results,
            "serp_features": serp_features,
            "total_count": len(organic_results),
            "check_url": task_result.get('check_url')
        }
    
    def _extract_serp_features(self, items: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Extract SERP features from SERP items."""
        features = {
            "featured_snippet": None,
            "people_also_ask": [],
            "video_carousel": [],
            "local_pack": None,
            "knowledge_panel": None,
            "ai_overview": None,
            "image_pack": None,
            "shopping_results": [],
            "top_stories": [],
            "related_searches": [],
            "twitter": []
        }
        
        for item in items:
            item_type = item.get('type', '')
            
            if item_type == 'featured_snippet':
                features['featured_snippet'] = {
                    "position": item.get('rank_absolute'),
                    "title": item.get('title'),
                    "description": item.get('description'),
                    "url": item.get('url'),
                    "domain": item.get('domain'),
                    "rectangle": item.get('rectangle')
                }
            
            elif item_type == 'people_also_ask':
                features['people_also_ask'].append({
                    "question": item.get('title'),
                    "answer": item.get('expanded_element', [{}])[0].get('description') if item.get('expanded_element') else None,
                    "source_url": item.get('expanded_element', [{}])[0].get('url') if item.get('expanded_element') else None,
                    "rectangle": item.get('rectangle')
                })
            
            elif item_type == 'video':
                features['video_carousel'].append({
                    "title": item.get('title'),
                    "url": item.get('url'),
                    "source": item.get('source'),
                    "rectangle": item.get('rectangle')
                })
            
            elif item_type == 'local_pack':
                features['local_pack'] = {
                    "businesses": item.get('items', []),
                    "rectangle": item.get('rectangle')
                }
            
            elif item_type == 'knowledge_graph':
                features['knowledge_panel'] = {
                    "title": item.get('title'),
                    "description": item.get('description'),
                    "rectangle": item.get('rectangle')
                }
            
            elif item_type == 'ai_overview' or item_type == 'google_ai_overview':
                features['ai_overview'] = {
                    "text": item.get('text'),
                    "rectangle": item.get('rectangle')
                }
            
            elif item_type == 'images':
                features['image_pack'] = {
                    "count": len(item.get('items', [])),
                    "rectangle": item.get('rectangle')
                }
            
            elif item_type == 'shopping':
                features['shopping_results'].append({
                    "title": item.get('title'),
                    "price": item.get('price'),
                    "source": item.get('source'),
                    "rectangle": item.get('rectangle')
                })
            
            elif item_type == 'top_stories':
                features['top_stories'].append({
                    "title": item.get('title'),
                    "url": item.get('url'),
                    "source": item.get('source'),
                    "rectangle": item.get('rectangle')
                })
            
            elif item_type == 'related_searches':
                features['related_searches'] = [
                    item.get('title') for item in item.get('items', [])
                ]
            
            elif item_type == 'twitter':
                features['twitter'].append({
                    "title": item.get('title'),
                    "url": item.get('url'),
                    "rectangle": item.get('rectangle')
                })
        
        return features
    
    async def get_bulk_serp(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
        device: str = "desktop",
        max_concurrent: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Get SERP data for multiple keywords concurrently.
        
        Args:
            keywords: List of search queries
            location_code: Location code
            language_code: Language code
            device: Device type
            max_concurrent: Maximum concurrent requests
        
        Returns:
            List of SERP data dictionaries
        """
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def fetch_with_semaphore(keyword):
            async with semaphore:
                return await self.get_live_serp(
                    keyword=keyword,
                    location_code=location_code,
                    language_code=language_code,
                    device=device
                )
        
        tasks = [fetch_with_semaphore(kw) for kw in keywords]
        return await asyncio.gather(*tasks, return_exceptions=True)
    
    async def get_competitor_domains(
        self,
        keywords: List[str],
        location_code: int = 2840,
        min_keyword_overlap: int = 3
    ) -> List[Dict[str, Any]]:
        """
        Discover competitor domains across a set of keywords.
        
        Args:
            keywords: List of target keywords
            location_code: Location code
            min_keyword_overlap: Minimum keywords a domain must rank for to be considered
        
        Returns:
            List of competitor domains with metrics:
            [
                {
                    "domain": str,
                    "keywords_shared": int,
                    "avg_position": float,
                    "keyword_list": [str],
                    "threat_level": str  # high, medium, low
                }
            ]
        """
        # Get SERP data for all keywords
        serp_results = await self.get_bulk_serp(keywords, location_code=location_code)
        
        # Aggregate domain appearances
        domain_stats: Dict[str, Dict[str, Any]] = {}
        
        for serp in serp_results:
            if isinstance(serp, Exception):
                continue
            
            keyword = serp.get('keyword')
            
            for result in serp.get('organic_results', []):
                domain = result.get('domain')
                position = result.get('position')
                
                if not domain or not position:
                    continue
                
                if domain not in domain_stats:
                    domain_stats[domain] = {
                        "keywords": set(),
                        "positions": [],
                        "keyword_list": []
                    }
                
                domain_stats[domain]['keywords'].add(keyword)
                domain_stats[domain]['positions'].append(position)
                domain_stats[domain]['keyword_list'].append(keyword)
        
        # Filter and format results
        competitors = []
        
        for domain, stats in domain_stats.items():
            keywords_shared = len(stats['keywords'])
            
            if keywords_shared >= min_keyword_overlap:
                avg_position = sum(stats['positions']) / len(stats['positions'])
                
                # Determine threat level
                if keywords_shared >= len(keywords) * 0.5 and avg_position <= 5:
                    threat_level = "high"
                elif keywords_shared >= len(keywords) * 0.2 and avg_position <= 10:
                    threat_level = "medium"
                else:
                    threat_level = "low"
                
                competitors.append({
                    "domain": domain,
                    "keywords_shared": keywords_shared,
                    "avg_position": round(avg_position, 1),
                    "keyword_list": stats['keyword_list'],
                    "threat_level": threat_level
                })
        
        # Sort by keywords shared (descending)
        competitors.sort(key=lambda x: x['keywords_shared'], reverse=True)
        
        return competitors
    
    def calculate_visual_position(
        self,
        organic_position: int,
        serp_features: Dict[str, Any]
    ) -> float:
        """
        Calculate visual position based on SERP features above organic result.
        
        Position adjustments:
        - Featured snippet: +2 positions
        - AI overview: +3 positions
        - Each PAA question: +0.5 positions
        - Video carousel: +1.5 positions
        - Local pack: +3 positions
        - Knowledge panel: +2 positions
        - Image pack: +1 position
        - Shopping results: +0.5 per result (max 2)
        - Top stories: +1.5 positions
        
        Args:
            organic_position: Actual organic ranking position
            serp_features: Dictionary of SERP features
        
        Returns:
            Adjusted visual position (float)
        """
        adjustment = 0.0
        
        # Featured snippet
        if serp_features.get('featured_snippet'):
            fs_pos = serp_features['featured_snippet'].get('position', 0)
            if fs_pos < organic_position:
                adjustment += 2.0
        
        # AI overview
        if serp_features.get('ai_overview'):
            adjustment += 3.0
        
        # People Also Ask
        paa_count = len(serp_features.get('people_also_ask', []))
        if paa_count > 0:
            # Count only PAAs above the organic result
            paa_above = sum(1 for paa in serp_features.get('people_also_ask', [])
                          if paa.get('rectangle', {}).get('y', 999999) < 
                          (organic_position * 100))  # Rough estimate
            adjustment += paa_above * 0.5
        
        # Video carousel
        if serp_features.get('video_carousel') and len(serp_features['video_carousel']) > 0:
            adjustment += 1.5
        
        # Local pack
        if serp_features.get('local_pack'):
            adjustment += 3.0
        
        # Knowledge panel
        if serp_features.get('knowledge_panel'):
            adjustment += 2.0
        
        # Image pack
        if serp_features.get('image_pack'):
            adjustment += 1.0
        
        # Shopping results
        shopping_count = len(serp_features.get('shopping_results', []))
        if shopping_count > 0:
            adjustment += min(shopping_count * 0.5, 2.0)
        
        # Top stories
        if serp_features.get('top_stories') and len(serp_features['top_stories']) > 0:
            adjustment += 1.5
        
        return organic_position + adjustment
    
    def estimate_ctr_with_features(
        self,
        position: int,
        visual_position: float,
        serp_features: Dict[str, Any]
    ) -> float:
        """
        Estimate CTR based on position and SERP features.
        
        Uses advanced CTR modeling that accounts for:
        1. Base position CTR curve
        2. SERP feature displacement effects
        3. Visual position adjustments
        
        Args:
            position: Organic ranking position
            visual_position: Visual position after SERP features
            serp_features: Dictionary of SERP features
        
        Returns:
            Estimated CTR (0.0 to 1.0)
        """
        # Base CTR curve (Advanced Web Ranking 2023 data)
        base_ctr_desktop = {
            1: 0.394, 2: 0.184, 3: 0.107, 4: 0.074, 5: 0.057,
            6: 0.045, 7: 0.037, 8: 0.031, 9: 0.027, 10: 0.024,
            11: 0.017, 12: 0.014, 13: 0.012, 14: 0.011, 15: 0.010,
            16: 0.009, 17: 0.008, 18: 0.007, 19: 0.007, 20: 0.006
        }
        
        # Get base CTR
        base_ctr = base_ctr_desktop.get(position, 0.005)
        
        # Feature displacement penalties
        displacement_penalty = 1.0
        
        # Featured snippet above us (loses ~40% CTR)
        if serp_features.get('featured_snippet'):
            fs_pos = serp_features['featured_snippet'].get('position', 0)
            if fs_pos < position:
                displacement_penalty *= 0.6
        
        # AI overview (loses ~50% CTR)
        if serp_features.get('ai_overview'):
            displacement_penalty *= 0.5
        
        # PAA questions (loses ~5% per question, max 30%)
        paa_count = len(serp_features.get('people_also_ask', []))
        if paa_count > 0:
            paa_penalty = max(0.7, 1.0 - (paa_count * 0.05))
            displacement_penalty *= paa_penalty
        
        # Video carousel (loses ~20%)
        if serp_features.get('video_carousel') and len(serp_features['video_carousel']) > 0:
            displacement_penalty *= 0.8
        
        # Local pack (loses ~30%)
        if serp_features.get('local_pack'):
            displacement_penalty *= 0.7
        
        # Knowledge panel (loses ~25%)
        if serp_features.get('knowledge_panel'):
            displacement_penalty *= 0.75
        
        # Shopping results (loses ~15%)
        if serp_features.get('shopping_results') and len(serp_features['shopping_results']) > 0:
            displacement_penalty *= 0.85
        
        # Apply visual position penalty (exponential decay)
        visual_penalty = 1.0 / (1.0 + 0.15 * (visual_position - position))
        
        # Combine all factors
        estimated_ctr = base_ctr * displacement_penalty * visual_penalty
        
        return max(0.001, min(1.0, estimated_ctr))
    
    async def get_keyword_ranking_data(
        self,
        domain: str,
        keywords: List[str],
        location_code: int = 2840
    ) -> List[Dict[str, Any]]:
        """
        Get ranking positions for a domain across multiple keywords.
        
        Args:
            domain: Target domain
            keywords: List of keywords to check
            location_code: Location code
        
        Returns:
            List of keyword ranking data:
            [
                {
                    "keyword": str,
                    "position": int or None,
                    "url": str or None,
                    "visual_position": float,
                    "estimated_ctr": float,
                    "serp_features": {...}
                }
            ]
        """
        serp_results = await self.get_bulk_serp(keywords, location_code=location_code)
        
        ranking_data = []
        
        for serp in serp_results:
            if isinstance(serp, Exception):
                continue
            
            keyword = serp.get('keyword')
            position = None
            url = None
            
            # Find domain in organic results
            for result in serp.get('organic_results', []):
                if domain in result.get('domain', ''):
                    position = result.get('position')
                    url = result.get('url')
                    break
            
            serp_features = serp.get('serp_features', {})
            
            if position:
                visual_position = self.calculate_visual_position(position, serp_features)
                estimated_ctr = self.estimate_ctr_with_features(position, visual_position, serp_features)
            else:
                visual_position = None
                estimated_ctr = 0.0
            
            ranking_data.append({
                "keyword": keyword,
                "position": position,
                "url": url,
                "visual_position": visual_position,
                "estimated_ctr": estimated_ctr,
                "serp_features": serp_features
            })
        
        return ranking_data
    
    async def get_ctr_modeling_data(
        self,
        keywords_with_positions: List[Dict[str, Union[str, int]]],
        location_code: int = 2840
    ) -> Dict[str, Any]:
        """
        Get CTR modeling data for multiple keywords with known positions.
        
        Args:
            keywords_with_positions: List of {"keyword": str, "position": int}
            location_code: Location code
        
        Returns:
            {
                "keywords": [
                    {
                        "keyword": str,
                        "position": int,
                        "visual_position": float,
                        "base_ctr": float,
                        "adjusted_ctr": float,
                        "ctr_impact": float,
                        "serp_features": {...}
                    }
                ],
                "summary": {
                    "avg_ctr_loss_from_features": float,
                    "total_keywords": int,
                    "keywords_with_displacement": int
                }
            }
        """
        keywords = [kw['keyword'] for kw in keywords_with_positions]
        position_map = {kw['keyword']: kw['position'] for kw in keywords_with_positions}
        
        serp_results = await self.get_bulk_serp(keywords, location_code=location_code)
        
        ctr_data = []
        total_ctr_loss = 0.0
        keywords_with_displacement = 0
        
        for serp in serp_results:
            if isinstance(serp, Exception):
                continue
            
            keyword = serp.get('keyword')
            position = position_map.get(keyword)
            
            if not position:
                continue
            
            serp_features = serp.get('serp_features', {})
            visual_position = self.calculate_visual_position(position, serp_features)
            
            # Base CTR (no features)
            base_ctr_map = {
                1: 0.394, 2: 0.184, 3: 0.107, 4: 0.074, 5: 0.057,
                6: 0.045, 7: 0.037, 8: 0.031, 9: 0.027, 10: 0.024
            }
            base_ctr = base_ctr_map.get(position, 0.005)
            
            # Adjusted CTR (with features)
            adjusted_ctr = self.estimate_ctr_with_features(position, visual_position, serp_features)
            
            ctr_impact = base_ctr - adjusted_ctr
            total_ctr_loss += ctr_impact
            
            if ctr_impact > 0.01:  # More than 1% CTR loss
                keywords_with_displacement += 1
            
            ctr_data.append({
                "keyword": keyword,
                "position": position,
                "visual_position": visual_position,
                "base_ctr": round(base_ctr, 4),
                "adjusted_ctr": round(adjusted_ctr, 4),
                "ctr_impact": round(ctr_impact, 4),
                "serp_features": serp_features
            })
        
        return {
            "keywords": ctr_data,
            "summary": {
                "avg_ctr_loss_from_features": round(total_ctr_loss / len(ctr_data), 4) if ctr_data else 0.0,
                "total_keywords": len(ctr_data),
                "keywords_with_displacement": keywords_with_displacement
            }
        }
    
    async def close(self):
        """Clean up resources."""
        # Nothing to clean up for now, but good to have for future
        pass