import os
import asyncio
import hashlib
import json
from typing import Dict, List, Optional, Any, Union, Tuple
from datetime import datetime, timedelta
from urllib.parse import urlencode
import base64
import aiohttp
from aiohttp import ClientSession, ClientTimeout, BasicAuth
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log
)
import logging

logger = logging.getLogger(__name__)


class DataForSEOError(Exception):
    """Base exception for DataForSEO API errors"""
    pass


class RateLimitError(DataForSEOError):
    """Raised when rate limit is exceeded"""
    pass


class APIError(DataForSEOError):
    """Raised when API returns an error response"""
    pass


class AuthenticationError(DataForSEOError):
    """Raised when authentication fails"""
    pass


class DataForSEOService:
    """
    DataForSEO API client service for SERP data retrieval.
    
    Provides methods for:
    - Live SERP data (organic results, rankings, SERP features)
    - Competitor domain discovery from SERP results
    - CTR opportunity analysis
    
    Features:
    - Rate limiting (30 requests per minute)
    - Exponential backoff retry logic
    - Response caching via Redis/Supabase
    - Error handling and normalization
    - Async/await for concurrent requests
    """
    
    BASE_URL = "https://api.dataforseo.com"
    API_VERSION = "v3"
    
    # Rate limiting: 30 requests per minute = 1 request every 2 seconds
    RATE_LIMIT_REQUESTS = 30
    RATE_LIMIT_PERIOD = 60  # seconds
    MIN_REQUEST_INTERVAL = RATE_LIMIT_PERIOD / RATE_LIMIT_REQUESTS
    
    DEFAULT_CACHE_TTL_HOURS = 24
    DEFAULT_TIMEOUT_SECONDS = 60
    MAX_RETRIES = 3
    
    # SERP feature types we track
    SERP_FEATURES = [
        'featured_snippet',
        'people_also_ask',
        'video',
        'local_pack',
        'knowledge_panel',
        'ai_overview',
        'images',
        'shopping',
        'top_stories',
        'twitter',
        'related_searches',
        'recipes',
        'answer_box'
    ]
    
    def __init__(
        self,
        api_login: Optional[str] = None,
        api_password: Optional[str] = None,
        cache_client: Optional[Any] = None,
        cache_ttl_hours: int = DEFAULT_CACHE_TTL_HOURS,
        timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS
    ):
        """
        Initialize DataForSEO service.
        
        Args:
            api_login: DataForSEO API login (defaults to DATAFORSEO_LOGIN env var)
            api_password: DataForSEO API password (defaults to DATAFORSEO_PASSWORD env var)
            cache_client: Redis or Supabase client for caching
            cache_ttl_hours: Cache TTL in hours
            timeout_seconds: Request timeout in seconds
        """
        self.api_login = api_login or os.getenv('DATAFORSEO_LOGIN')
        self.api_password = api_password or os.getenv('DATAFORSEO_PASSWORD')
        
        if not self.api_login or not self.api_password:
            raise AuthenticationError(
                "DataForSEO credentials not provided. Set DATAFORSEO_LOGIN and "
                "DATAFORSEO_PASSWORD environment variables or pass to constructor."
            )
        
        self.cache_client = cache_client
        self.cache_ttl_hours = cache_ttl_hours
        self.timeout_seconds = timeout_seconds
        
        # Rate limiting state
        self._last_request_time = 0.0
        self._request_lock = asyncio.Lock()
        
        # Session will be created per request context
        self._session: Optional[ClientSession] = None
    
    def _get_auth(self) -> BasicAuth:
        """Get BasicAuth object for requests"""
        return BasicAuth(self.api_login, self.api_password)
    
    def _get_cache_key(self, endpoint: str, params: Dict[str, Any]) -> str:
        """
        Generate cache key from endpoint and parameters.
        
        Args:
            endpoint: API endpoint path
            params: Request parameters
            
        Returns:
            SHA256 hash as cache key
        """
        # Create deterministic string representation
        params_str = json.dumps(params, sort_keys=True)
        key_string = f"{endpoint}:{params_str}"
        
        # Hash to fixed length
        return f"dataforseo:{hashlib.sha256(key_string.encode()).hexdigest()}"
    
    async def _get_from_cache(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve data from cache.
        
        Args:
            cache_key: Cache key
            
        Returns:
            Cached data or None if not found/expired
        """
        if not self.cache_client:
            return None
        
        try:
            # Try Redis-style interface first
            if hasattr(self.cache_client, 'get'):
                cached = await self.cache_client.get(cache_key)
                if cached:
                    return json.loads(cached)
            
            # Try Supabase-style interface
            elif hasattr(self.cache_client, 'table'):
                result = await self.cache_client.table('dataforseo_cache') \
                    .select('data, created_at') \
                    .eq('cache_key', cache_key) \
                    .single() \
                    .execute()
                
                if result.data:
                    created_at = datetime.fromisoformat(result.data['created_at'])
                    expiry = created_at + timedelta(hours=self.cache_ttl_hours)
                    
                    if datetime.utcnow() < expiry:
                        return result.data['data']
                    else:
                        # Expired, delete it
                        await self.cache_client.table('dataforseo_cache') \
                            .delete() \
                            .eq('cache_key', cache_key) \
                            .execute()
            
            return None
            
        except Exception as e:
            logger.warning(f"Cache retrieval failed: {e}")
            return None
    
    async def _set_cache(self, cache_key: str, data: Dict[str, Any]) -> None:
        """
        Store data in cache.
        
        Args:
            cache_key: Cache key
            data: Data to cache
        """
        if not self.cache_client:
            return
        
        try:
            # Try Redis-style interface
            if hasattr(self.cache_client, 'setex'):
                ttl_seconds = self.cache_ttl_hours * 3600
                await self.cache_client.setex(
                    cache_key,
                    ttl_seconds,
                    json.dumps(data)
                )
            
            # Try Supabase-style interface
            elif hasattr(self.cache_client, 'table'):
                await self.cache_client.table('dataforseo_cache') \
                    .upsert({
                        'cache_key': cache_key,
                        'data': data,
                        'created_at': datetime.utcnow().isoformat()
                    }) \
                    .execute()
                    
        except Exception as e:
            logger.warning(f"Cache storage failed: {e}")
    
    async def _rate_limit(self) -> None:
        """
        Enforce rate limiting by ensuring minimum time between requests.
        """
        async with self._request_lock:
            current_time = asyncio.get_event_loop().time()
            time_since_last = current_time - self._last_request_time
            
            if time_since_last < self.MIN_REQUEST_INTERVAL:
                wait_time = self.MIN_REQUEST_INTERVAL - time_since_last
                logger.debug(f"Rate limiting: waiting {wait_time:.2f}s")
                await asyncio.sleep(wait_time)
            
            self._last_request_time = asyncio.get_event_loop().time()
    
    @retry(
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
        before_sleep=before_sleep_log(logger, logging.WARNING)
    )
    async def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[List[Dict[str, Any]]] = None,
        use_cache: bool = True
    ) -> Dict[str, Any]:
        """
        Make HTTP request to DataForSEO API with retry logic.
        
        Args:
            method: HTTP method (GET, POST)
            endpoint: API endpoint path
            data: Request payload for POST requests
            use_cache: Whether to use caching
            
        Returns:
            API response data
            
        Raises:
            RateLimitError: If rate limit exceeded
            APIError: If API returns error response
            AuthenticationError: If authentication fails
        """
        # Check cache for POST requests (GET requests typically don't need caching here)
        cache_key = None
        if use_cache and method == "POST" and data:
            cache_key = self._get_cache_key(endpoint, data[0] if data else {})
            cached = await self._get_from_cache(cache_key)
            if cached:
                logger.info(f"Cache hit for {endpoint}")
                return cached
        
        # Rate limiting
        await self._rate_limit()
        
        # Build URL
        url = f"{self.BASE_URL}/{endpoint}"
        
        # Create session if needed
        timeout = ClientTimeout(total=self.timeout_seconds)
        async with ClientSession(timeout=timeout, auth=self._get_auth()) as session:
            try:
                if method == "POST":
                    response = await session.post(url, json=data)
                else:
                    response = await session.get(url)
                
                # Handle response
                if response.status == 401:
                    raise AuthenticationError("Invalid DataForSEO credentials")
                elif response.status == 429:
                    raise RateLimitError("DataForSEO rate limit exceeded")
                elif response.status >= 400:
                    error_text = await response.text()
                    raise APIError(f"API error {response.status}: {error_text}")
                
                response_data = await response.json()
                
                # DataForSEO wraps responses in tasks array
                if 'tasks' in response_data and len(response_data['tasks']) > 0:
                    task = response_data['tasks'][0]
                    
                    # Check for task-level errors
                    if task.get('status_code') != 20000:
                        error_message = task.get('status_message', 'Unknown error')
                        raise APIError(f"Task failed: {error_message}")
                    
                    result = task.get('result', [])
                    
                    # Cache successful response
                    if use_cache and cache_key and result:
                        await self._set_cache(cache_key, {'result': result})
                    
                    return {'result': result}
                
                return response_data
                
            except aiohttp.ClientError as e:
                logger.error(f"Request failed: {e}")
                raise
    
    async def get_serp_live(
        self,
        keyword: str,
        location_code: int = 2840,  # United States
        language_code: str = "en",
        device: str = "desktop",
        depth: int = 100,
        use_cache: bool = True
    ) -> Dict[str, Any]:
        """
        Get live SERP data for a keyword.
        
        Args:
            keyword: Search keyword
            location_code: DataForSEO location code (2840 = US)
            language_code: Language code
            device: Device type (desktop, mobile)
            depth: Number of results to return (max 100)
            use_cache: Whether to use cached results
            
        Returns:
            Normalized SERP data with organic results and features
        """
        endpoint = f"{self.API_VERSION}/serp/google/organic/live/advanced"
        
        payload = [{
            "keyword": keyword,
            "location_code": location_code,
            "language_code": language_code,
            "device": device,
            "depth": depth,
            "calculate_rectangles": True  # For SERP feature positioning
        }]
        
        response = await self._make_request("POST", endpoint, payload, use_cache)
        
        # Normalize response
        return self._normalize_serp_response(keyword, response)
    
    async def get_serp_bulk(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
        device: str = "desktop",
        depth: int = 100,
        use_cache: bool = True,
        max_concurrent: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Get live SERP data for multiple keywords with concurrency control.
        
        Args:
            keywords: List of search keywords
            location_code: DataForSEO location code
            language_code: Language code
            device: Device type
            depth: Number of results per keyword
            use_cache: Whether to use cached results
            max_concurrent: Maximum concurrent requests
            
        Returns:
            List of normalized SERP data for each keyword
        """
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def fetch_with_semaphore(keyword: str) -> Dict[str, Any]:
            async with semaphore:
                return await self.get_serp_live(
                    keyword,
                    location_code,
                    language_code,
                    device,
                    depth,
                    use_cache
                )
        
        tasks = [fetch_with_semaphore(kw) for kw in keywords]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out exceptions and log errors
        valid_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Failed to fetch SERP for '{keywords[i]}': {result}")
            else:
                valid_results.append(result)
        
        return valid_results
    
    def _normalize_serp_response(
        self,
        keyword: str,
        response: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Normalize DataForSEO SERP response to standard format.
        
        Args:
            keyword: Search keyword
            response: Raw API response
            
        Returns:
            Normalized SERP data structure
        """
        if not response.get('result') or len(response['result']) == 0:
            return self._empty_serp_result(keyword)
        
        result = response['result'][0]
        items = result.get('items', [])
        
        # Extract organic results
        organic_results = []
        for item in items:
            if item.get('type') == 'organic':
                organic_results.append({
                    'position': item.get('rank_absolute', 0),
                    'url': item.get('url', ''),
                    'domain': item.get('domain', ''),
                    'title': item.get('title', ''),
                    'description': item.get('description', ''),
                    'breadcrumb': item.get('breadcrumb', ''),
                    'is_amp': item.get('amp_version', False),
                    'rating': item.get('rating', {}).get('rating_value') if item.get('rating') else None,
                    'timestamp': item.get('timestamp')
                })
        
        # Extract SERP features
        serp_features = self._extract_serp_features(items)
        
        # Calculate visual positions (accounting for SERP features)
        organic_with_visual = self._calculate_visual_positions(
            organic_results,
            serp_features
        )
        
        # Extract competitors
        competitors = self._extract_competitors(organic_results)
        
        return {
            'keyword': keyword,
            'timestamp': datetime.utcnow().isoformat(),
            'total_results': result.get('items_count', 0),
            'organic_results': organic_with_visual,
            'serp_features': serp_features,
            'competitors': competitors,
            'metadata': {
                'location_code': result.get('location_code'),
                'language_code': result.get('language_code'),
                'device': result.get('se_type'),
                'check_url': result.get('check_url')
            }
        }
    
    def _extract_serp_features(self, items: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Extract SERP features from result items.
        
        Args:
            items: SERP result items
            
        Returns:
            Dictionary of SERP features present
        """
        features = {
            'featured_snippet': None,
            'people_also_ask': [],
            'video': [],
            'local_pack': None,
            'knowledge_panel': None,
            'ai_overview': None,
            'images': None,
            'shopping': [],
            'top_stories': [],
            'twitter': [],
            'related_searches': [],
            'recipes': [],
            'answer_box': None
        }
        
        feature_count = 0
        
        for item in items:
            item_type = item.get('type', '')
            
            if item_type == 'featured_snippet':
                features['featured_snippet'] = {
                    'url': item.get('url'),
                    'domain': item.get('domain'),
                    'title': item.get('title'),
                    'description': item.get('description'),
                    'position': item.get('rank_absolute', 0)
                }
                feature_count += 1
                
            elif item_type == 'people_also_ask':
                features['people_also_ask'].append({
                    'question': item.get('title'),
                    'answer': item.get('expanded_element', [{}])[0].get('description') if item.get('expanded_element') else None,
                    'source_url': item.get('expanded_element', [{}])[0].get('url') if item.get('expanded_element') else None
                })
                feature_count += 1
                
            elif item_type == 'video':
                features['video'].append({
                    'title': item.get('title'),
                    'url': item.get('url'),
                    'domain': item.get('domain'),
                    'position': item.get('rank_absolute', 0)
                })
                feature_count += 1
                
            elif item_type == 'local_pack':
                features['local_pack'] = {
                    'title': item.get('title'),
                    'items': item.get('items', [])
                }
                feature_count += 1
                
            elif item_type == 'knowledge_graph':
                features['knowledge_panel'] = {
                    'title': item.get('title'),
                    'description': item.get('description'),
                    'url': item.get('url')
                }
                feature_count += 1
                
            elif item_type == 'ai_overview' or item_type == 'generative_ai':
                features['ai_overview'] = {
                    'text': item.get('text'),
                    'sources': item.get('sources', [])
                }
                feature_count += 1
                
            elif item_type == 'images':
                features['images'] = {
                    'title': item.get('title'),
                    'count': len(item.get('items', []))
                }
                feature_count += 1
                
            elif item_type == 'shopping':
                features['shopping'].append({
                    'title': item.get('title'),
                    'price': item.get('price'),
                    'url': item.get('url')
                })
                feature_count += 1
                
            elif item_type == 'top_stories':
                features['top_stories'].append({
                    'title': item.get('title'),
                    'url': item.get('url'),
                    'domain': item.get('domain')
                })
                feature_count += 1
                
            elif item_type == 'twitter':
                features['twitter'].append({
                    'title': item.get('title'),
                    'url': item.get('url')
                })
                feature_count += 1
                
            elif item_type == 'related_searches':
                features['related_searches'] = [
                    item.get('title') for item in item.get('items', [])
                ]
                
            elif item_type == 'recipes':
                features['recipes'].append({
                    'title': item.get('title'),
                    'url': item.get('url'),
                    'rating': item.get('rating')
                })
                feature_count += 1
                
            elif item_type == 'answer_box':
                features['answer_box'] = {
                    'text': item.get('text'),
                    'title': item.get('title'),
                    'url': item.get('url')
                }
                feature_count += 1
        
        # Add summary
        features['_summary'] = {
            'total_features': feature_count,
            'feature_types_present': [
                k for k, v in features.items()
                if not k.startswith('_') and (
                    (isinstance(v, list) and len(v) > 0) or
                    (isinstance(v, dict) and v is not None) or
                    v is not None
                )
            ]
        }
        
        return features
    
    def _calculate_visual_positions(
        self,
        organic_results: List[Dict[str, Any]],
        serp_features: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Calculate visual positions for organic results accounting for SERP features.
        
        Visual position = organic position + displacement from SERP features above.
        
        Args:
            organic_results: List of organic results
            serp_features: Extracted SERP features
            
        Returns:
            Organic results with visual_position added
        """
        # SERP feature displacement weights (how many "positions" each takes)
        feature_weights = {
            'featured_snippet': 2.0,
            'ai_overview': 3.0,
            'local_pack': 3.0,
            'knowledge_panel': 0.5,  # Usually on side
            'people_also_ask': 0.5,  # Per question
            'video': 1.0,  # Per video in carousel
            'images': 1.0,
            'shopping': 0.5,  # Per item
            'top_stories': 0.5,  # Per story
            'answer_box': 2.0
        }
        
        # Calculate total displacement
        displacement = 0.0
        
        if serp_features.get('featured_snippet'):
            displacement += feature_weights['featured_snippet']
        
        if serp_features.get('ai_overview'):
            displacement += feature_weights['ai_overview']
        
        if serp_features.get('local_pack'):
            displacement += feature_weights['local_pack']
        
        if serp_features.get('knowledge_panel'):
            displacement += feature_weights['knowledge_panel']
        
        if serp_features.get('answer_box'):
            displacement += feature_weights['answer_box']
        
        paa_count = len(serp_features.get('people_also_ask', []))
        displacement += paa_count * feature_weights['people_also_ask']
        
        video_count = len(serp_features.get('video', []))
        displacement += min(video_count, 3) * feature_weights['video']  # Usually show 3
        
        if serp_features.get('images'):
            displacement += feature_weights['images']
        
        shopping_count = len(serp_features.get('shopping', []))
        displacement += min(shopping_count, 4) * feature_weights['shopping']
        
        story_count = len(serp_features.get('top_stories', []))
        displacement += min(story_count, 3) * feature_weights['top_stories']
        
        # Add visual position to each organic result
        for result in organic_results:
            result['visual_position'] = result['position'] + displacement
            result['displacement'] = displacement
        
        return organic_results
    
    def _extract_competitors(
        self,
        organic_results: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Extract competitor domains from organic results.
        
        Args:
            organic_results: List of organic results
            
        Returns:
            List of competitor domains with positions
        """
        competitors = {}
        
        for result in organic_results:
            domain = result['domain']
            if domain and domain not in competitors:
                competitors[domain] = {
                    'domain': domain,
                    'position': result['position'],
                    'url': result['url'],
                    'title': result['title']
                }
        
        # Sort by position
        return sorted(
            competitors.values(),
            key=lambda x: x['position']
        )
    
    def _empty_serp_result(self, keyword: str) -> Dict[str, Any]:
        """
        Return empty SERP result structure.
        
        Args:
            keyword: Search keyword
            
        Returns:
            Empty result dictionary
        """
        return {
            'keyword': keyword,
            'timestamp': datetime.utcnow().isoformat(),
            'total_results': 0,
            'organic_results': [],
            'serp_features': {
                '_summary': {
                    'total_features': 0,
                    'feature_types_present': []
                }
            },
            'competitors': [],
            'metadata': {}
        }
    
    def calculate_ctr_opportunity(
        self,
        serp_data: Dict[str, Any],
        user_domain: str,
        impressions: int
    ) -> Dict[str, Any]:
        """
        Calculate CTR opportunity based on SERP features and position.
        
        Uses position-based CTR curves adjusted for SERP feature presence.
        
        Args:
            serp_data: Normalized SERP data from get_serp_live
            user_domain: User's domain to find in results
            impressions: Monthly impressions for this keyword
            
        Returns:
            CTR opportunity analysis
        """
        # Base CTR curve (desktop, no SERP features)
        # Source: Advanced Web Ranking CTR study
        base_ctr_curve = {
            1: 0.397, 2: 0.182, 3: 0.106, 4: 0.074, 5: 0.055,
            6: 0.044, 7: 0.036, 8: 0.030, 9: 0.026, 10: 0.022,
            11: 0.019, 12: 0.017, 13: 0.015, 14: 0.013, 15: 0.012,
            16: 0.011, 17: 0.010, 18: 0.009, 19: 0.008, 20: 0.008
        }
        
        # Find user's position
        user_result = None
        for result in serp_data['organic_results']:
            if user_domain in result['domain']:
                user_result = result
                break
        
        if not user_result:
            return {
                'user_present': False,
                'message': f"Domain {user_domain} not found in top results"
            }
        
        current_position = user_result['position']
        visual_position = user_result.get('visual_position', current_position)
        
        # Adjust CTR for SERP features
        feature_impact = self._calculate_feature_impact(serp_data['serp_features'])
        
        # Current CTR (adjusted)
        base_ctr = base_ctr_curve.get(current_position, 0.005)
        current_ctr = base_ctr * (1 - feature_impact)
        current_clicks = int(impressions * current_ctr)
        
        # Potential CTR if moved to position 1 (adjusted)
        potential_ctr_pos1 = base_ctr_curve[1] * (1 - feature_impact)
        potential_clicks_pos1 = int(impressions * potential_ctr_pos1)
        
        # Potential CTR if moved to top 5 (adjusted)
        potential_ctr_top5 = base_ctr_curve[5] * (1 - feature_impact)
        potential_clicks_top5 = int(impressions * potential_ctr_top5)
        
        return {
            'user_present': True,
            'current_position': current_position,
            'visual_position': visual_position,
            'displacement': user_result.get('displacement', 0),
            'current_ctr': round(current_ctr, 4),
            'current_estimated_clicks': current_clicks,
            'feature_impact_pct': round(feature_impact * 100, 1),
            'opportunity': {
                'to_position_1': {
                    'ctr': round(potential_ctr_pos1, 4),
                    'estimated_clicks': potential_clicks_pos1,
                    'click_gain': potential_clicks_pos1 - current_clicks
                },
                'to_top_5': {
                    'ctr': round(potential_ctr_top5, 4),
                    'estimated_clicks': potential_clicks_top5,
                    'click_gain': potential_clicks_top5 - current_clicks
                }
            },
            'recommendations': self._generate_ctr_recommendations(
                serp_data,
                user_result,
                feature_impact
            )
        }
    
    def _calculate_feature_impact(
        self,
        serp_features: Dict[str, Any]
    ) -> float:
        """
        Calculate overall CTR impact from SERP features.
        
        Args:
            serp_features: Extracted SERP features
            
        Returns:
            Impact factor (0.0 = no impact, 1.0 = 100% CTR reduction)
        """
        impact = 0.0
        
        # Major CTR reducers
        if serp_features.get('featured_snippet'):
            impact += 0.35  # Featured snippets capture ~35% of clicks
        
        if serp_features.get('ai_overview'):
            impact += 0.40  # AI overviews have major impact
        
        if serp_features.get('local_pack'):
            impact += 0.25
        
        # Moderate impact
        paa_count = len(serp_features.get('people_also_ask', []))
        impact += min(paa_count * 0.05, 0.20)  # Cap at 20%
        
        if serp_features.get('answer_box'):
            impact += 0.30
        
        # Minor impact
        if serp_features.get('images'):
            impact += 0.05
        
        if len(serp_features.get('video', [])) > 0:
            impact += 0.10
        
        if len(serp_features.get('shopping', [])) > 0:
            impact += 0.10
        
        # Cap total impact at 0.75 (always leave some organic CTR)
        return min(impact, 0.75)
    
    def _generate_ctr_recommendations(
        self,
        serp_data: Dict[str, Any],
        user_result: Dict[str, Any],
        feature_impact: float
    ) -> List[str]:
        """
        Generate recommendations for improving CTR.
        
        Args:
            serp_data: SERP data
            user_result: User's result
            feature_impact: Calculated feature impact
            
        Returns:
            List of recommendation strings
        """
        recommendations = []
        features = serp_data['serp_features']
        
        # Position-based
        if user_result['position'] > 10:
            recommendations.append(
                f"Priority: Improve ranking from position {user_result['position']} "
                f"to top 10 to increase visibility"
            )
        elif user_result['position'] > 5:
            recommendations.append(
                f"Target top 5 positions (currently #{user_result['position']}) "
                f"for significant CTR improvement"
            )
        
        # Feature-based opportunities
        if features.get('featured_snippet') and user_result['position'] <= 5:
            recommendations.append(
                "Optimize for featured snippet: Currently lost to competitor. "
                "Add FAQ schema, structured data, and direct question-answer format."
            )
        
        if features.get('people_also_ask') and len(features['people_also_ask']) > 2:
            recommendations.append(
                f"Add FAQ section answering {len(features['people_also_ask'])} "
                f"People Also Ask questions to capture more SERP real estate"
            )
        
        if features.get('video') and user_result['position'] <= 10:
            recommendations.append(
                "Create video content: SERP shows video carousel. "
                "Video can capture additional clicks above organic results."
            )
        
        if feature_impact > 0.4:
            recommendations.append(
                f"High SERP feature competition (CTR reduced by {feature_impact*100:.0f}%). "
                f"Focus on unique value proposition in title/description to stand out."
            )
        
        # Title/description optimization
        if user_result['position'] <= 10:
            recommendations.append(
                "Review title and meta description to maximize click appeal. "
                "Compare against top competitors for differentiation."
            )
        
        return recommendations
    
    async def analyze_competitor_overlap(
        self,
        serp_results: List[Dict[str, Any]],
        user_domain: str
    ) -> Dict[str, Any]:
        """
        Analyze competitor domain overlap across multiple SERP results.
        
        Args:
            serp_results: List of SERP data from get_serp_bulk
            user_domain: User's domain
            
        Returns:
            Competitor overlap analysis
        """
        competitor_appearances = {}
        total_keywords = len(serp_results)
        
        for serp in serp_results:
            keyword = serp['keyword']
            
            for competitor in serp['competitors']:
                domain = competitor['domain']
                
                # Skip user's own domain
                if user_domain in domain:
                    continue
                
                if domain not in competitor_appearances:
                    competitor_appearances[domain] = {
                        'domain': domain,
                        'keywords': [],
                        'positions': [],
                        'total_appearances': 0
                    }
                
                competitor_appearances[domain]['keywords'].append(keyword)
                competitor_appearances[domain]['positions'].append(competitor['position'])
                competitor_appearances[domain]['total_appearances'] += 1
        
        # Calculate metrics
        competitors_ranked = []
        for domain, data in competitor_appearances.items():
            overlap_pct = (data['total_appearances'] / total_keywords) * 100
            avg_position = sum(data['positions']) / len(data['positions'])
            
            # Classify threat level
            if overlap_pct > 50 and avg_position < 5:
                threat_level = 'critical'
            elif overlap_pct > 30 and avg_position < 8:
                threat_level = 'high'
            elif overlap_pct > 15:
                threat_level = 'medium'
            else:
                threat_level = 'low'
            
            competitors_ranked.append({
                'domain': domain,
                'overlap_pct': round(overlap_pct, 1),
                'keywords_shared': data['total_appearances'],
                'avg_position': round(avg_position, 1),
                'best_position': min(data['positions']),
                'worst_position': max(data['positions']),
                'threat_level': threat_level,
                'sample_keywords': data['keywords'][:5]  # Top 5 for reference
            })
        
        # Sort by overlap percentage
        competitors_ranked.sort(key=lambda x: x['overlap_pct'], reverse=True)
        
        return {
            'total_keywords_analyzed': total_keywords,
            'total_unique_competitors': len(competitors_ranked),
            'primary_competitors': competitors_ranked[:10],
            'all_competitors': competitors_ranked
        }
    
    async def close(self):
        """Close any open sessions"""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
