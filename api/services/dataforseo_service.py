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
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from api.config import settings


class DataForSEOError(Exception):
    """Base exception for DataForSEO API errors"""
    pass


class RateLimitError(DataForSEOError):
    """Raised when rate limit is exceeded"""
    pass


class APIError(DataForSEOError):
    """Raised when API returns an error response"""
    pass


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
        supabase_client: Optional[Any] = None,
        cache_ttl_hours: int = DEFAULT_CACHE_TTL_HOURS,
        rate_limit_per_second: int = DEFAULT_RATE_LIMIT_PER_SECOND,
        timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS
    ):
        """
        Initialize DataForSEO service.
        
        Args:
            api_login: DataForSEO API login (defaults to env var DATAFORSEO_LOGIN)
            api_password: DataForSEO API password (defaults to env var DATAFORSEO_PASSWORD)
            supabase_client: Supabase client for caching
            cache_ttl_hours: Cache TTL in hours
            rate_limit_per_second: Maximum requests per second
            timeout_seconds: Request timeout in seconds
        """
        self.api_login = api_login or settings.DATAFORSEO_LOGIN
        self.api_password = api_password or settings.DATAFORSEO_PASSWORD
        
        if not self.api_login or not self.api_password:
            raise ValueError("DataForSEO credentials not provided. Set DATAFORSEO_LOGIN and DATAFORSEO_PASSWORD environment variables.")
        
        self.supabase = supabase_client
        self.cache_ttl_hours = cache_ttl_hours
        self.rate_limit_per_second = rate_limit_per_second
        self.timeout_seconds = timeout_seconds
        
        # Rate limiting state
        self._rate_limit_semaphore = asyncio.Semaphore(rate_limit_per_second)
        self._last_request_times: List[float] = []
        self._rate_limit_lock = asyncio.Lock()
    
    async def _wait_for_rate_limit(self):
        """Enforce rate limiting based on requests per second"""
        async with self._rate_limit_lock:
            current_time = asyncio.get_event_loop().time()
            
            # Remove timestamps older than 1 second
            self._last_request_times = [
                t for t in self._last_request_times 
                if current_time - t < 1.0
            ]
            
            # If we're at the limit, wait
            if len(self._last_request_times) >= self.rate_limit_per_second:
                sleep_time = 1.0 - (current_time - self._last_request_times[0])
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)
                    current_time = asyncio.get_event_loop().time()
                    self._last_request_times = [
                        t for t in self._last_request_times 
                        if current_time - t < 1.0
                    ]
            
            self._last_request_times.append(current_time)
    
    def _generate_cache_key(self, endpoint: str, params: Dict[str, Any]) -> str:
        """Generate a cache key from endpoint and parameters"""
        # Sort params for consistent hashing
        params_str = json.dumps(params, sort_keys=True)
        key_string = f"{endpoint}:{params_str}"
        return hashlib.sha256(key_string.encode()).hexdigest()
    
    async def _get_from_cache(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """Retrieve data from cache if available and not expired"""
        if not self.supabase:
            return None
        
        try:
            result = self.supabase.table('dataforseo_cache').select('*').eq('cache_key', cache_key).single().execute()
            
            if result.data:
                cached_at = datetime.fromisoformat(result.data['cached_at'])
                expiry = cached_at + timedelta(hours=self.cache_ttl_hours)
                
                if datetime.utcnow() < expiry:
                    return result.data['response_data']
        except Exception as e:
            # Cache miss or error - continue to API call
            pass
        
        return None
    
    async def _save_to_cache(self, cache_key: str, data: Dict[str, Any]):
        """Save API response to cache"""
        if not self.supabase:
            return
        
        try:
            cache_entry = {
                'cache_key': cache_key,
                'response_data': data,
                'cached_at': datetime.utcnow().isoformat()
            }
            
            self.supabase.table('dataforseo_cache').upsert(cache_entry).execute()
        except Exception as e:
            # Log but don't fail on cache errors
            print(f"Cache save error: {e}")
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
    )
    async def _make_request(
        self,
        endpoint: str,
        method: str = "POST",
        data: Optional[List[Dict[str, Any]]] = None,
        use_cache: bool = True
    ) -> Dict[str, Any]:
        """
        Make HTTP request to DataForSEO API with retry logic
        
        Args:
            endpoint: API endpoint path
            method: HTTP method
            data: Request payload (for POST requests)
            use_cache: Whether to use caching
            
        Returns:
            API response as dict
        """
        # Check cache first
        if use_cache and data:
            cache_key = self._generate_cache_key(endpoint, data[0] if data else {})
            cached_response = await self._get_from_cache(cache_key)
            if cached_response:
                return cached_response
        
        # Rate limiting
        await self._wait_for_rate_limit()
        
        # Make request
        url = f"{self.BASE_URL}{endpoint}"
        
        auth = aiohttp.BasicAuth(self.api_login, self.api_password)
        timeout = ClientTimeout(total=self.timeout_seconds)
        
        async with ClientSession(auth=auth, timeout=timeout) as session:
            if method == "POST":
                async with session.post(url, json=data) as response:
                    response_data = await response.json()
            else:
                async with session.get(url) as response:
                    response_data = await response.json()
            
            # Check for API errors
            if response.status != 200:
                raise APIError(f"API returned status {response.status}: {response_data}")
            
            # DataForSEO wraps responses in tasks array
            if 'tasks' in response_data and response_data['tasks']:
                task = response_data['tasks'][0]
                
                if task['status_code'] == 20000:
                    # Success - cache and return
                    if use_cache and data:
                        await self._save_to_cache(cache_key, response_data)
                    return response_data
                else:
                    error_msg = task.get('status_message', 'Unknown error')
                    raise APIError(f"DataForSEO API error: {error_msg}")
            
            return response_data
    
    async def get_live_serp(
        self,
        keyword: str,
        location_code: int = 2840,  # United States
        language_code: str = "en",
        device: str = "desktop",
        depth: int = 100,
        use_cache: bool = True
    ) -> Dict[str, Any]:
        """
        Get live SERP results for a keyword
        
        Args:
            keyword: Search query
            location_code: DataForSEO location code (2840 = United States)
            language_code: Language code (en, es, etc.)
            device: desktop or mobile
            depth: Number of results to return (max 100)
            use_cache: Whether to use cached results
            
        Returns:
            Standardized SERP data with organic results and features
        """
        endpoint = "/v3/serp/google/organic/live/advanced"
        
        payload = [{
            "keyword": keyword,
            "location_code": location_code,
            "language_code": language_code,
            "device": device,
            "os": "windows" if device == "desktop" else "ios",
            "depth": depth,
            "calculate_rectangles": True  # For visual position calculation
        }]
        
        response = await self._make_request(endpoint, data=payload, use_cache=use_cache)
        
        # Parse and standardize response
        return self._parse_serp_response(response, keyword)
    
    def _parse_serp_response(self, response: Dict[str, Any], keyword: str) -> Dict[str, Any]:
        """
        Parse DataForSEO SERP response into standardized format
        
        Returns structured data with:
        - organic_results: List of organic results with positions
        - serp_features: Dict of SERP features present
        - visual_positions: Adjusted positions accounting for SERP features
        - competitors: List of competing domains
        """
        if not response.get('tasks') or not response['tasks'][0].get('result'):
            return {
                'keyword': keyword,
                'organic_results': [],
                'serp_features': {},
                'competitors': [],
                'total_results': 0
            }
        
        result = response['tasks'][0]['result'][0]
        items = result.get('items', [])
        
        # Extract organic results
        organic_results = []
        for item in items:
            if item.get('type') == 'organic':
                organic_results.append({
                    'position': item.get('rank_group', 0),
                    'url': item.get('url', ''),
                    'domain': item.get('domain', ''),
                    'title': item.get('title', ''),
                    'description': item.get('description', ''),
                    'breadcrumb': item.get('breadcrumb', ''),
                    'rectangle': item.get('rectangle', {}),
                    'highlighted_text': item.get('highlighted', [])
                })
        
        # Extract SERP features
        serp_features = {}
        visual_offset = 0
        
        for item in items:
            item_type = item.get('type', '')
            
            # Featured snippet
            if item_type == 'featured_snippet':
                serp_features['featured_snippet'] = {
                    'present': True,
                    'domain': item.get('domain', ''),
                    'url': item.get('url', ''),
                    'visual_weight': 2.0
                }
                visual_offset += 2.0
            
            # People Also Ask
            elif item_type == 'people_also_ask':
                paa_items = item.get('items', [])
                serp_features['people_also_ask'] = {
                    'present': True,
                    'count': len(paa_items),
                    'questions': [q.get('title', '') for q in paa_items],
                    'visual_weight': len(paa_items) * 0.5
                }
                visual_offset += len(paa_items) * 0.5
            
            # Video carousel
            elif item_type == 'video':
                serp_features['video_carousel'] = {
                    'present': True,
                    'count': len(item.get('items', [])),
                    'visual_weight': 1.5
                }
                visual_offset += 1.5
            
            # Local pack
            elif item_type == 'local_pack':
                serp_features['local_pack'] = {
                    'present': True,
                    'count': len(item.get('items', [])),
                    'visual_weight': 3.0
                }
                visual_offset += 3.0
            
            # Knowledge panel
            elif item_type == 'knowledge_graph':
                serp_features['knowledge_panel'] = {
                    'present': True,
                    'title': item.get('title', ''),
                    'visual_weight': 0  # Sidebar, doesn't push down
                }
            
            # AI Overview (Google AI-generated answer)
            elif item_type == 'ai_overview':
                serp_features['ai_overview'] = {
                    'present': True,
                    'visual_weight': 2.5
                }
                visual_offset += 2.5
            
            # Image pack
            elif item_type == 'images':
                serp_features['image_pack'] = {
                    'present': True,
                    'visual_weight': 1.0
                }
                visual_offset += 1.0
            
            # Shopping results
            elif item_type == 'shopping':
                serp_features['shopping_results'] = {
                    'present': True,
                    'count': len(item.get('items', [])),
                    'visual_weight': 2.0
                }
                visual_offset += 2.0
            
            # Top stories
            elif item_type == 'top_stories':
                serp_features['top_stories'] = {
                    'present': True,
                    'count': len(item.get('items', [])),
                    'visual_weight': 1.5
                }
                visual_offset += 1.5
            
            # Twitter/X results
            elif item_type == 'twitter':
                serp_features['twitter'] = {
                    'present': True,
                    'count': len(item.get('items', [])),
                    'visual_weight': 1.0
                }
                visual_offset += 1.0
            
            # Related searches
            elif item_type == 'related_searches':
                serp_features['related_searches'] = {
                    'present': True,
                    'searches': [s.get('title', '') for s in item.get('items', [])],
                    'visual_weight': 0  # Bottom of page
                }
        
        # Calculate visual positions
        for result in organic_results:
            result['visual_position'] = result['position'] + visual_offset
        
        # Extract competitor domains
        competitors = list(set([r['domain'] for r in organic_results]))
        
        return {
            'keyword': keyword,
            'location_code': result.get('location_code'),
            'language_code': result.get('language_code'),
            'check_url': result.get('check_url', ''),
            'organic_results': organic_results,
            'serp_features': serp_features,
            'visual_offset': visual_offset,
            'competitors': competitors,
            'total_results': result.get('items_count', 0)
        }
    
    async def get_bulk_serp_data(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
        device: str = "desktop",
        max_concurrent: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Get SERP data for multiple keywords with concurrency control
        
        Args:
            keywords: List of search queries
            location_code: DataForSEO location code
            language_code: Language code
            device: desktop or mobile
            max_concurrent: Maximum concurrent API requests
            
        Returns:
            List of parsed SERP data for each keyword
        """
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def fetch_with_semaphore(keyword: str):
            async with semaphore:
                return await self.get_live_serp(
                    keyword=keyword,
                    location_code=location_code,
                    language_code=language_code,
                    device=device
                )
        
        tasks = [fetch_with_semaphore(kw) for kw in keywords]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out errors and return successful results
        return [r for r in results if not isinstance(r, Exception)]
    
    async def get_domain_overview(
        self,
        domain: str,
        location_code: int = 2840,
        language_code: str = "en",
        use_cache: bool = True
    ) -> Dict[str, Any]:
        """
        Get domain overview data including organic keywords count and visibility
        
        Args:
            domain: Target domain (e.g., "example.com")
            location_code: DataForSEO location code
            language_code: Language code
            use_cache: Whether to use cached results
            
        Returns:
            Domain metrics including keyword count, traffic estimates, visibility
        """
        endpoint = "/v3/dataforseo_labs/google/domain_overview/live"
        
        payload = [{
            "target": domain,
            "location_code": location_code,
            "language_code": language_code
        }]
        
        response = await self._make_request(endpoint, data=payload, use_cache=use_cache)
        
        if not response.get('tasks') or not response['tasks'][0].get('result'):
            return {
                'domain': domain,
                'metrics': {},
                'error': 'No data available'
            }
        
        result = response['tasks'][0]['result'][0]
        metrics = result.get('metrics', {})
        
        return {
            'domain': domain,
            'metrics': {
                'organic_keywords': metrics.get('organic', {}).get('count', 0),
                'organic_etv': metrics.get('organic', {}).get('etv', 0),  # Estimated traffic value
                'organic_count_top_3': metrics.get('organic', {}).get('count_top_3', 0),
                'organic_count_top_10': metrics.get('organic', {}).get('count_top_10', 0),
                'organic_count_top_100': metrics.get('organic', {}).get('count_top_100', 0),
                'paid_keywords': metrics.get('paid', {}).get('count', 0),
                'paid_etv': metrics.get('paid', {}).get('etv', 0)
            },
            'last_updated': result.get('datetime')
        }
    
    async def get_domain_keywords(
        self,
        domain: str,
        location_code: int = 2840,
        language_code: str = "en",
        limit: int = 1000,
        order_by: List[str] = None,
        filters: Optional[Dict[str, Any]] = None,
        use_cache: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Get organic keywords that a domain ranks for
        
        Args:
            domain: Target domain
            location_code: DataForSEO location code
            language_code: Language code
            limit: Maximum keywords to return (max 10000)
            order_by: Sort order, e.g., ["keyword_data.search_volume,desc"]
            filters: Optional filters, e.g., {"keyword_data.search_volume": ">100"}
            use_cache: Whether to use cached results
            
        Returns:
            List of keywords with ranking data
        """
        endpoint = "/v3/dataforseo_labs/google/ranked_keywords/live"
        
        if order_by is None:
            order_by = ["keyword_data.search_volume,desc"]
        
        payload = [{
            "target": domain,
            "location_code": location_code,
            "language_code": language_code,
            "limit": limit,
            "order_by": order_by
        }]
        
        if filters:
            payload[0]["filters"] = filters
        
        response = await self._make_request(endpoint, data=payload, use_cache=use_cache)
        
        if not response.get('tasks') or not response['tasks'][0].get('result'):
            return []
        
        items = response['tasks'][0]['result'][0].get('items', [])
        
        # Standardize keyword data
        keywords = []
        for item in items:
            keyword_data = item.get('keyword_data', {})
            ranked_serp_element = item.get('ranked_serp_element', {})
            
            keywords.append({
                'keyword': item.get('keyword', ''),
                'position': ranked_serp_element.get('serp_item', {}).get('rank_group', 0),
                'url': ranked_serp_element.get('serp_item', {}).get('url', ''),
                'search_volume': keyword_data.get('search_volume', 0),
                'cpc': keyword_data.get('cpc', 0),
                'competition': keyword_data.get('competition', 0),
                'monthly_searches': keyword_data.get('monthly_searches', [])
            })
        
        return keywords
    
    async def get_competitors(
        self,
        domain: str,
        location_code: int = 2840,
        language_code: str = "en",
        limit: int = 20,
        use_cache: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Get competitor domains based on keyword overlap
        
        Args:
            domain: Target domain
            location_code: DataForSEO location code
            language_code: Language code
            limit: Number of competitors to return
            use_cache: Whether to use cached results
            
        Returns:
            List of competitor domains with overlap metrics
        """
        endpoint = "/v3/dataforseo_labs/google/competitors_domain/live"
        
        payload = [{
            "target": domain,
            "location_code": location_code,
            "language_code": language_code,
            "limit": limit
        }]
        
        response = await self._make_request(endpoint, data=payload, use_cache=use_cache)
        
        if not response.get('tasks') or not response['tasks'][0].get('result'):
            return []
        
        items = response['tasks'][0]['result'][0].get('items', [])
        
        # Standardize competitor data
        competitors = []
        for item in items:
            metrics = item.get('avg_position', 0)
            
            competitors.append({
                'domain': item.get('domain', ''),
                'avg_position': item.get('avg_position', 0),
                'sum_position': item.get('sum_position', 0),
                'intersections': item.get('intersections', 0),  # Shared keywords
                'full_domain_metrics': item.get('full_domain_metrics', {}),
                'relevance_score': self._calculate_competitor_relevance(item)
            })
        
        return sorted(competitors, key=lambda x: x['relevance_score'], reverse=True)
    
    def _calculate_competitor_relevance(self, competitor_data: Dict[str, Any]) -> float:
        """
        Calculate relevance score for a competitor based on:
        - Number of shared keywords (intersections)
        - Average position quality
        - Domain authority proxy (organic keywords count)
        """
        intersections = competitor_data.get('intersections', 0)
        avg_position = competitor_data.get('avg_position', 100)
        
        # Higher intersections = more relevant
        # Lower avg position = more competitive (they rank better)
        # Score formula: intersections * (1 / (avg_position^0.5))
        if avg_position == 0:
            avg_position = 1
        
        score = intersections * (1 / (avg_position ** 0.5))
        return round(score, 2)
    
    async def get_serp_history(
        self,
        keyword: str,
        location_code: int = 2840,
        language_code: str = "en",
        use_cache: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Get historical SERP data for change-point detection
        Note: This endpoint may have limited historical data
        
        Args:
            keyword: Search query
            location_code: DataForSEO location code
            language_code: Language code
            use_cache: Whether to use cached results
            
        Returns:
            List of historical SERP snapshots
        """
        # DataForSEO doesn't have a direct history endpoint for all keywords
        # This would need to be built from stored past SERP pulls in Supabase
        # For now, return empty list and note in documentation
        
        # TODO: Implement by querying dataforseo_cache table for past SERP results
        # for the same keyword with different cached_at timestamps
        
        return []
    
    def estimate_ctr(
        self,
        position: int,
        serp_features: Dict[str, Any]
    ) -> float:
        """
        Estimate CTR for a given position accounting for SERP features
        
        Uses Advanced Web Ranking CTR curve as baseline, adjusted for SERP features
        
        Args:
            position: Organic ranking position (1-100)
            serp_features: Dict of SERP features present (from _parse_serp_response)
            
        Returns:
            Estimated CTR as decimal (e.g., 0.31 for 31%)
        """
        # Baseline CTR curve (desktop, 2024 AWR data approximation)
        baseline_ctrs = {
            1: 0.31, 2: 0.17, 3: 0.11, 4: 0.08, 5: 0.06,
            6: 0.05, 7: 0.04, 8: 0.03, 9: 0.03, 10: 0.02
        }
        
        # Positions 11-20
        for i in range(11, 21):
            baseline_ctrs[i] = 0.01
        
        # Positions 21-100
        for i in range(21, 101):
            baseline_ctrs[i] = 0.005
        
        base_ctr = baseline_ctrs.get(position, 0.001)
        
        # Adjustment factors based on SERP features
        adjustment = 1.0
        
        # Featured snippet steals ~30% of clicks from position 1
        if serp_features.get('featured_snippet', {}).get('present'):
            if position == 1:
                adjustment *= 0.7
            else:
                adjustment *= 0.95
        
        # AI Overview significantly impacts top results
        if serp_features.get('ai_overview', {}).get('present'):
            if position <= 3:
                adjustment *= 0.6
            elif position <= 5:
                adjustment *= 0.8
            else:
                adjustment *= 0.9
        
        # Local pack dominates local queries
        if serp_features.get('local_pack', {}).get('present'):
            if position <= 5:
                adjustment *= 0.5
            else:
                adjustment *= 0.85
        
        # PAA reduces clicks to top results
        paa_count = serp_features.get('people_also_ask', {}).get('count', 0)
        if paa_count > 0:
            adjustment *= (1 - (paa_count * 0.03))  # 3% reduction per PAA
        
        # Video carousel for video-intent queries
        if serp_features.get('video_carousel', {}).get('present'):
            adjustment *= 0.85
        
        # Shopping results for commercial queries
        if serp_features.get('shopping_results', {}).get('present'):
            if position <= 5:
                adjustment *= 0.7
            else:
                adjustment *= 0.9
        
        return round(base_ctr * adjustment, 4)
    
    def classify_serp_intent(self, serp_features: Dict[str, Any]) -> str:
        """
        Classify SERP intent based on features present
        
        Args:
            serp_features: Dict of SERP features from _parse_serp_response
            
        Returns:
            One of: "informational", "commercial", "transactional", "navigational"
        """
        # Transactional signals
        if serp_features.get('shopping_results', {}).get('present'):
            return "transactional"
        
        # Commercial investigation signals
        if serp_features.get('video_carousel', {}).get('present') and \
           serp_features.get('people_also_ask', {}).get('present'):
            return "commercial"
        
        # Navigational signals
        if serp_features.get('knowledge_panel', {}).get('present') and \
           not serp_features.get('people_also_ask', {}).get('present'):
            return "navigational"
        
        # Informational (default)
        return "informational"