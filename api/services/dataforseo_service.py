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
            raise ValueError(
                "DataForSEO credentials not provided. "
                "Set DATAFORSEO_LOGIN and DATAFORSEO_PASSWORD environment variables."
            )
        
        self.cache_client = cache_client
        self.cache_ttl_hours = cache_ttl_hours
        self.timeout_seconds = timeout_seconds
        
        # Rate limiting state
        self._last_request_time = 0
        self._rate_limit_lock = asyncio.Lock()
        
        # Session will be created per request batch
        self._session: Optional[ClientSession] = None
    
    async def _get_session(self) -> ClientSession:
        """Get or create aiohttp session with auth."""
        if self._session is None or self._session.closed:
            auth = BasicAuth(self.api_login, self.api_password)
            timeout = ClientTimeout(total=self.timeout_seconds)
            self._session = ClientSession(
                auth=auth,
                timeout=timeout,
                headers={
                    'Content-Type': 'application/json',
                    'User-Agent': 'SearchIntelligenceReport/1.0'
                }
            )
        return self._session
    
    async def close(self):
        """Close the aiohttp session."""
        if self._session and not self._session.closed:
            await self._session.close()
    
    async def _enforce_rate_limit(self):
        """Enforce rate limiting between requests."""
        async with self._rate_limit_lock:
            current_time = asyncio.get_event_loop().time()
            time_since_last_request = current_time - self._last_request_time
            
            if time_since_last_request < self.MIN_REQUEST_INTERVAL:
                sleep_time = self.MIN_REQUEST_INTERVAL - time_since_last_request
                logger.debug(f"Rate limiting: sleeping {sleep_time:.2f}s")
                await asyncio.sleep(sleep_time)
            
            self._last_request_time = asyncio.get_event_loop().time()
    
    def _generate_cache_key(self, endpoint: str, params: Dict) -> str:
        """Generate cache key from endpoint and parameters."""
        # Sort params for consistent hashing
        sorted_params = json.dumps(params, sort_keys=True)
        key_string = f"dataforseo:{endpoint}:{sorted_params}"
        return hashlib.sha256(key_string.encode()).hexdigest()
    
    async def _get_cached_response(self, cache_key: str) -> Optional[Dict]:
        """Retrieve cached response if available and not expired."""
        if not self.cache_client:
            return None
        
        try:
            # Try Redis first
            if hasattr(self.cache_client, 'get'):
                cached = await self.cache_client.get(cache_key)
                if cached:
                    data = json.loads(cached)
                    # Check expiry
                    if 'cached_at' in data:
                        cached_at = datetime.fromisoformat(data['cached_at'])
                        if datetime.utcnow() - cached_at < timedelta(hours=self.cache_ttl_hours):
                            logger.info(f"Cache hit for key {cache_key[:16]}...")
                            return data.get('response')
            
            # Try Supabase
            elif hasattr(self.cache_client, 'table'):
                result = self.cache_client.table('dataforseo_cache').select('*').eq('cache_key', cache_key).execute()
                if result.data and len(result.data) > 0:
                    record = result.data[0]
                    cached_at = datetime.fromisoformat(record['cached_at'])
                    if datetime.utcnow() - cached_at < timedelta(hours=self.cache_ttl_hours):
                        logger.info(f"Cache hit for key {cache_key[:16]}...")
                        return json.loads(record['response_data'])
        
        except Exception as e:
            logger.warning(f"Cache retrieval error: {e}")
        
        return None
    
    async def _set_cached_response(self, cache_key: str, response: Dict):
        """Store response in cache."""
        if not self.cache_client:
            return
        
        try:
            cache_data = {
                'response': response,
                'cached_at': datetime.utcnow().isoformat()
            }
            
            # Try Redis
            if hasattr(self.cache_client, 'setex'):
                ttl_seconds = self.cache_ttl_hours * 3600
                await self.cache_client.setex(
                    cache_key,
                    ttl_seconds,
                    json.dumps(cache_data)
                )
                logger.info(f"Cached response with key {cache_key[:16]}...")
            
            # Try Supabase
            elif hasattr(self.cache_client, 'table'):
                self.cache_client.table('dataforseo_cache').upsert({
                    'cache_key': cache_key,
                    'response_data': json.dumps(response),
                    'cached_at': datetime.utcnow().isoformat()
                }).execute()
                logger.info(f"Cached response with key {cache_key[:16]}...")
        
        except Exception as e:
            logger.warning(f"Cache storage error: {e}")
    
    @retry(
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
        before_sleep=before_sleep_log(logger, logging.WARNING)
    )
    async def _make_request(
        self,
        endpoint: str,
        method: str = 'POST',
        data: Optional[Dict] = None,
        params: Optional[Dict] = None
    ) -> Dict:
        """
        Make HTTP request to DataForSEO API with retry logic.
        
        Args:
            endpoint: API endpoint (e.g., '/v3/serp/google/organic/live/advanced')
            method: HTTP method
            data: Request body (for POST)
            params: URL parameters (for GET)
        
        Returns:
            API response as dict
        
        Raises:
            AuthenticationError: If authentication fails
            RateLimitError: If rate limit exceeded
            APIError: If API returns error
        """
        await self._enforce_rate_limit()
        
        url = f"{self.BASE_URL}{endpoint}"
        session = await self._get_session()
        
        try:
            if method.upper() == 'POST':
                async with session.post(url, json=data) as response:
                    return await self._handle_response(response)
            else:
                async with session.get(url, params=params) as response:
                    return await self._handle_response(response)
        
        except aiohttp.ClientResponseError as e:
            if e.status == 401:
                raise AuthenticationError(f"Authentication failed: {e}")
            elif e.status == 429:
                raise RateLimitError(f"Rate limit exceeded: {e}")
            else:
                raise APIError(f"API error {e.status}: {e}")
    
    async def _handle_response(self, response: aiohttp.ClientResponse) -> Dict:
        """Handle API response and check for errors."""
        response.raise_for_status()
        
        data = await response.json()
        
        # DataForSEO wraps responses in tasks array
        if 'tasks' in data and len(data['tasks']) > 0:
            task = data['tasks'][0]
            
            if task.get('status_code') != 20000:
                error_message = task.get('status_message', 'Unknown error')
                raise APIError(f"API returned error: {error_message}")
            
            return task.get('result', [])
        
        return data
    
    async def authenticate(self) -> bool:
        """
        Test authentication with DataForSEO API.
        
        Returns:
            True if authentication successful
        
        Raises:
            AuthenticationError: If authentication fails
        """
        try:
            # Use the user endpoint to test auth
            endpoint = f"/{self.API_VERSION}/user/data"
            await self._make_request(endpoint, method='GET')
            logger.info("DataForSEO authentication successful")
            return True
        
        except Exception as e:
            raise AuthenticationError(f"Authentication failed: {e}")
    
    async def get_serp_data(
        self,
        keywords: List[str],
        location: str = "United States",
        language: str = "en",
        device: str = "desktop",
        use_cache: bool = True
    ) -> Dict[str, Dict]:
        """
        Get live SERP data for multiple keywords.
        
        Args:
            keywords: List of search queries
            location: Geographic location for search (country or city name)
            language: Language code (e.g., 'en')
            device: 'desktop' or 'mobile'
            use_cache: Whether to use cached results
        
        Returns:
            Dict mapping keyword to SERP data:
            {
                "keyword": {
                    "items": [  # Organic results
                        {
                            "type": "organic",
                            "rank_group": 1,
                            "rank_absolute": 1,
                            "position": "left",
                            "url": "https://example.com/page",
                            "domain": "example.com",
                            "title": "Page Title",
                            "description": "Meta description",
                            "breadcrumb": "example.com › category › page"
                        }
                    ],
                    "serp_features": {
                        "featured_snippet": {...},
                        "people_also_ask": [...],
                        "video": [...],
                        ...
                    },
                    "total_results": 1234567,
                    "serp_url": "https://google.com/search?q=...",
                    "metadata": {
                        "location": "United States",
                        "language": "en",
                        "device": "desktop",
                        "check_time": "2025-01-15T10:30:00Z"
                    }
                }
            }
        """
        endpoint = f"/{self.API_VERSION}/serp/google/organic/live/advanced"
        
        results = {}
        
        # Process keywords in batches (DataForSEO allows up to 100 per request)
        batch_size = 100
        for i in range(0, len(keywords), batch_size):
            batch = keywords[i:i + batch_size]
            
            # Build request payload
            tasks = []
            for keyword in batch:
                task = {
                    "keyword": keyword,
                    "location_name": location,
                    "language_code": language,
                    "device": device,
                    "os": "windows" if device == "desktop" else "ios",
                    "depth": 100,  # Get top 100 results
                    "calculate_rectangles": True  # For SERP feature positioning
                }
                tasks.append(task)
            
            # Check cache for each keyword
            cached_results = {}
            tasks_to_fetch = []
            
            if use_cache:
                for task in tasks:
                    cache_key = self._generate_cache_key(endpoint, task)
                    cached = await self._get_cached_response(cache_key)
                    if cached:
                        cached_results[task['keyword']] = cached
                    else:
                        tasks_to_fetch.append(task)
            else:
                tasks_to_fetch = tasks
            
            # Fetch non-cached keywords
            if tasks_to_fetch:
                try:
                    api_results = await self._make_request(endpoint, method='POST', data=tasks_to_fetch)
                    
                    # Process and cache results
                    for idx, task in enumerate(tasks_to_fetch):
                        if idx < len(api_results):
                            result_data = api_results[idx]
                            normalized = self._normalize_serp_data(result_data, task['keyword'])
                            results[task['keyword']] = normalized
                            
                            # Cache the result
                            cache_key = self._generate_cache_key(endpoint, task)
                            await self._set_cached_response(cache_key, normalized)
                
                except Exception as e:
                    logger.error(f"Error fetching SERP data for batch: {e}")
                    # Return empty results for failed keywords
                    for task in tasks_to_fetch:
                        results[task['keyword']] = self._empty_serp_result(task['keyword'])
            
            # Add cached results
            results.update(cached_results)
        
        return results
    
    def _normalize_serp_data(self, raw_data: Dict, keyword: str) -> Dict:
        """
        Normalize DataForSEO SERP response into standardized format.
        
        Args:
            raw_data: Raw API response for single keyword
            keyword: Search keyword
        
        Returns:
            Normalized SERP data
        """
        items = raw_data.get('items', [])
        
        # Separate organic results from SERP features
        organic_results = []
        serp_features = {}
        
        for item in items:
            item_type = item.get('type', '')
            
            if item_type == 'organic':
                organic_results.append({
                    'rank': item.get('rank_group', 0),
                    'rank_absolute': item.get('rank_absolute', 0),
                    'url': item.get('url', ''),
                    'domain': item.get('domain', ''),
                    'title': item.get('title', ''),
                    'description': item.get('description', ''),
                    'breadcrumb': item.get('breadcrumb', ''),
                    'is_amp': item.get('is_amp', False),
                    'rating': item.get('rating', {}),
                    'highlighted': item.get('highlighted', [])
                })
            
            elif item_type in ['featured_snippet', 'answer_box']:
                serp_features['featured_snippet'] = {
                    'type': item_type,
                    'url': item.get('url', ''),
                    'domain': item.get('domain', ''),
                    'title': item.get('title', ''),
                    'description': item.get('description', ''),
                    'rank_absolute': item.get('rank_absolute', 0)
                }
            
            elif item_type == 'people_also_ask':
                if 'people_also_ask' not in serp_features:
                    serp_features['people_also_ask'] = []
                serp_features['people_also_ask'].append({
                    'question': item.get('title', ''),
                    'url': item.get('url', ''),
                    'domain': item.get('domain', ''),
                    'rank_absolute': item.get('rank_absolute', 0)
                })
            
            elif item_type in ['video', 'video_carousel']:
                if 'video' not in serp_features:
                    serp_features['video'] = []
                serp_features['video'].append({
                    'title': item.get('title', ''),
                    'url': item.get('url', ''),
                    'domain': item.get('domain', ''),
                    'rank_absolute': item.get('rank_absolute', 0)
                })
            
            elif item_type == 'local_pack':
                serp_features['local_pack'] = {
                    'title': item.get('title', ''),
                    'rank_absolute': item.get('rank_absolute', 0),
                    'items': item.get('items', [])
                }
            
            elif item_type == 'knowledge_panel':
                serp_features['knowledge_panel'] = {
                    'title': item.get('title', ''),
                    'description': item.get('description', ''),
                    'rank_absolute': item.get('rank_absolute', 0)
                }
            
            elif item_type in ['images', 'carousel']:
                if 'images' not in serp_features:
                    serp_features['images'] = []
                serp_features['images'].append({
                    'title': item.get('title', ''),
                    'url': item.get('url', ''),
                    'rank_absolute': item.get('rank_absolute', 0)
                })
            
            elif item_type in ['shopping', 'shopping_carousel']:
                if 'shopping' not in serp_features:
                    serp_features['shopping'] = []
                serp_features['shopping'].append({
                    'title': item.get('title', ''),
                    'url': item.get('url', ''),
                    'price': item.get('price', {}),
                    'rank_absolute': item.get('rank_absolute', 0)
                })
            
            elif item_type == 'top_stories':
                if 'top_stories' not in serp_features:
                    serp_features['top_stories'] = []
                serp_features['top_stories'].append({
                    'title': item.get('title', ''),
                    'url': item.get('url', ''),
                    'domain': item.get('domain', ''),
                    'rank_absolute': item.get('rank_absolute', 0)
                })
            
            elif item_type == 'twitter':
                serp_features['twitter'] = {
                    'title': item.get('title', ''),
                    'url': item.get('url', ''),
                    'rank_absolute': item.get('rank_absolute', 0)
                }
            
            elif item_type == 'related_searches':
                if 'related_searches' not in serp_features:
                    serp_features['related_searches'] = []
                serp_features['related_searches'].append({
                    'query': item.get('title', ''),
                    'rank_absolute': item.get('rank_absolute', 0)
                })
        
        # Calculate visual positions accounting for SERP features
        visual_displacement = self._calculate_visual_displacement(serp_features)
        
        return {
            'keyword': keyword,
            'organic_results': organic_results,
            'serp_features': serp_features,
            'visual_displacement': visual_displacement,
            'total_results': raw_data.get('se_results_count', 0),
            'serp_url': raw_data.get('check_url', ''),
            'metadata': {
                'location': raw_data.get('location_name', ''),
                'language': raw_data.get('language_code', ''),
                'device': raw_data.get('device', ''),
                'check_time': raw_data.get('datetime', datetime.utcnow().isoformat())
            }
        }
    
    def _calculate_visual_displacement(self, serp_features: Dict) -> Dict[str, float]:
        """
        Calculate visual position displacement caused by SERP features.
        
        Returns dict mapping feature type to displacement value:
        - featured_snippet: 2.0 positions
        - people_also_ask: 0.5 per question
        - video: 1.0 per carousel
        - local_pack: 3.0
        - knowledge_panel: 1.0
        - etc.
        """
        displacement = {}
        total = 0.0
        
        if 'featured_snippet' in serp_features:
            displacement['featured_snippet'] = 2.0
            total += 2.0
        
        if 'people_also_ask' in serp_features:
            count = len(serp_features['people_also_ask'])
            displacement['people_also_ask'] = count * 0.5
            total += count * 0.5
        
        if 'video' in serp_features:
            displacement['video'] = 1.0
            total += 1.0
        
        if 'local_pack' in serp_features:
            displacement['local_pack'] = 3.0
            total += 3.0
        
        if 'knowledge_panel' in serp_features:
            displacement['knowledge_panel'] = 1.0
            total += 1.0
        
        if 'images' in serp_features:
            displacement['images'] = 0.5
            total += 0.5
        
        if 'shopping' in serp_features:
            displacement['shopping'] = 1.5
            total += 1.5
        
        if 'top_stories' in serp_features:
            displacement['top_stories'] = 1.0
            total += 1.0
        
        displacement['total'] = total
        return displacement
    
    def _empty_serp_result(self, keyword: str) -> Dict:
        """Return empty SERP result structure for failed requests."""
        return {
            'keyword': keyword,
            'organic_results': [],
            'serp_features': {},
            'visual_displacement': {'total': 0.0},
            'total_results': 0,
            'serp_url': '',
            'metadata': {
                'error': 'Failed to fetch SERP data',
                'check_time': datetime.utcnow().isoformat()
            }
        }
    
    async def get_competitor_domains(
        self,
        keyword: str,
        location: str = "United States",
        language: str = "en",
        top_n: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Get competitor domains ranking for a keyword.
        
        Args:
            keyword: Search query
            location: Geographic location
            language: Language code
            top_n: Number of top results to analyze
        
        Returns:
            List of competitor domains with ranking data:
            [
                {
                    "domain": "competitor.com",
                    "url": "https://competitor.com/page",
                    "rank": 1,
                    "title": "Page Title",
                    "description": "Meta description"
                }
            ]
        """
        serp_data = await self.get_serp_data(
            keywords=[keyword],
            location=location,
            language=language
        )
        
        result = serp_data.get(keyword, {})
        organic_results = result.get('organic_results', [])
        
        competitors = []
        for item in organic_results[:top_n]:
            competitors.append({
                'domain': item.get('domain', ''),
                'url': item.get('url', ''),
                'rank': item.get('rank', 0),
                'title': item.get('title', ''),
                'description': item.get('description', '')
            })
        
        return competitors
    
    async def get_serp_features(
        self,
        keyword: str,
        location: str = "United States",
        language: str = "en"
    ) -> Dict[str, Any]:
        """
        Get SERP features present for a keyword.
        
        Args:
            keyword: Search query
            location: Geographic location
            language: Language code
        
        Returns:
            Dict of SERP features and CTR impact analysis:
            {
                "features_present": ["featured_snippet", "people_also_ask", ...],
                "feature_details": {
                    "featured_snippet": {...},
                    "people_also_ask": [...]
                },
                "visual_displacement": {
                    "total": 3.5,
                    "by_feature": {...}
                },
                "ctr_impact": {
                    "baseline_ctr_position_1": 0.28,
                    "adjusted_ctr_position_1": 0.15,
                    "ctr_reduction_pct": 46.4
                }
            }
        """
        serp_data = await self.get_serp_data(
            keywords=[keyword],
            location=location,
            language=language
        )
        
        result = serp_data.get(keyword, {})
        serp_features = result.get('serp_features', {})
        visual_displacement = result.get('visual_displacement', {'total': 0.0})
        
        # CTR impact calculation
        # Baseline CTR curve for position 1-10 (desktop, no features)
        baseline_ctrs = {
            1: 0.28, 2: 0.15, 3: 0.11, 4: 0.08, 5: 0.06,
            6: 0.05, 7: 0.04, 8: 0.03, 9: 0.03, 10: 0.02
        }
        
        # Adjusted CTR accounting for SERP features
        # Rule of thumb: each position of visual displacement reduces CTR by ~15%
        ctr_impact = {}
        for position in range(1, 11):
            baseline_ctr = baseline_ctrs.get(position, 0.02)
            visual_position = position + visual_displacement.get('total', 0)
            
            # Approximate CTR reduction
            reduction_factor = 1.0 - (visual_displacement.get('total', 0) * 0.15)
            reduction_factor = max(0.1, reduction_factor)  # Floor at 10% of baseline
            
            adjusted_ctr = baseline_ctr * reduction_factor
            
            ctr_impact[f'position_{position}'] = {
                'baseline_ctr': baseline_ctr,
                'adjusted_ctr': adjusted_ctr,
                'ctr_reduction_pct': ((baseline_ctr - adjusted_ctr) / baseline_ctr * 100) if baseline_ctr > 0 else 0
            }
        
        return {
            'keyword': keyword,
            'features_present': list(serp_features.keys()),
            'feature_details': serp_features,
            'visual_displacement': visual_displacement,
            'ctr_impact': ctr_impact,
            'metadata': result.get('metadata', {})
        }
    
    async def batch_analyze_keywords(
        self,
        keywords: List[str],
        location: str = "United States",
        language: str = "en",
        device: str = "desktop"
    ) -> Dict[str, Any]:
        """
        Analyze multiple keywords in batch for comprehensive SERP landscape.
        
        Args:
            keywords: List of search queries
            location: Geographic location
            language: Language code
            device: 'desktop' or 'mobile'
        
        Returns:
            Aggregated analysis across all keywords:
            {
                "keywords_analyzed": 50,
                "total_serp_features": 127,
                "feature_frequency": {
                    "people_also_ask": 45,
                    "featured_snippet": 12,
                    ...
                },
                "avg_visual_displacement": 2.3,
                "competitor_frequency": {
                    "competitor1.com": 34,
                    "competitor2.com": 28,
                    ...
                },
                "keyword_details": {
                    "keyword1": {...},
                    "keyword2": {...}
                }
            }
        """
        # Fetch SERP data for all keywords
        serp_data = await self.get_serp_data(
            keywords=keywords,
            location=location,
            language=language,
            device=device
        )
        
        # Aggregate statistics
        feature_frequency = {}
        competitor_frequency = {}
        total_displacement = 0.0
        successful_keywords = 0
        
        for keyword, data in serp_data.items():
            if 'error' not in data.get('metadata', {}):
                successful_keywords += 1
                
                # Count SERP features
                for feature in data.get('serp_features', {}).keys():
                    feature_frequency[feature] = feature_frequency.get(feature, 0) + 1
                
                # Count competitor domains
                for result in data.get('organic_results', []):
                    domain = result.get('domain', '')
                    if domain:
                        competitor_frequency[domain] = competitor_frequency.get(domain, 0) + 1
                
                # Sum visual displacement
                total_displacement += data.get('visual_displacement', {}).get('total', 0.0)
        
        # Sort competitors by frequency
        top_competitors = sorted(
            competitor_frequency.items(),
            key=lambda x: x[1],
            reverse=True
        )[:20]
        
        return {
            'keywords_analyzed': successful_keywords,
            'total_keywords_requested': len(keywords),
            'total_serp_features': sum(feature_frequency.values()),
            'feature_frequency': feature_frequency,
            'avg_visual_displacement': total_displacement / successful_keywords if successful_keywords > 0 else 0.0,
            'competitor_frequency': dict(top_competitors),
            'keyword_details': serp_data,
            'metadata': {
                'location': location,
                'language': language,
                'device': device,
                'analysis_time': datetime.utcnow().isoformat()
            }
        }