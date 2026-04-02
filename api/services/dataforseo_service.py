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
    
    # CTR curves by position (average baseline CTR for organic results)
    # Based on industry data - will be adjusted for SERP features
    BASELINE_CTR_CURVE = {
        1: 0.316,
        2: 0.158,
        3: 0.100,
        4: 0.077,
        5: 0.059,
        6: 0.047,
        7: 0.039,
        8: 0.033,
        9: 0.028,
        10: 0.025,
        11: 0.020,
        12: 0.017,
        13: 0.015,
        14: 0.013,
        15: 0.011,
        16: 0.010,
        17: 0.009,
        18: 0.008,
        19: 0.007,
        20: 0.006
    }
    
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
                "DATAFORSEO_PASSWORD environment variables."
            )
        
        self.cache_client = cache_client
        self.cache_ttl_hours = cache_ttl_hours
        self.timeout = ClientTimeout(total=timeout_seconds)
        
        # Rate limiting state
        self._request_times: List[float] = []
        self._rate_limit_lock = asyncio.Lock()
        
        # Session will be created per request or can be managed externally
        self._session: Optional[ClientSession] = None
    
    def _get_auth(self) -> BasicAuth:
        """Get HTTP Basic Auth credentials"""
        return BasicAuth(self.api_login, self.api_password)
    
    def _generate_cache_key(self, endpoint: str, params: Dict[str, Any]) -> str:
        """Generate cache key from endpoint and parameters"""
        param_str = json.dumps(params, sort_keys=True)
        hash_input = f"{endpoint}:{param_str}"
        return f"dataforseo:{hashlib.md5(hash_input.encode()).hexdigest()}"
    
    async def _check_rate_limit(self):
        """Check and enforce rate limiting"""
        async with self._rate_limit_lock:
            now = asyncio.get_event_loop().time()
            
            # Remove requests older than rate limit period
            cutoff = now - self.RATE_LIMIT_PERIOD
            self._request_times = [t for t in self._request_times if t > cutoff]
            
            # If at limit, wait until we can make another request
            if len(self._request_times) >= self.RATE_LIMIT_REQUESTS:
                sleep_time = self._request_times[0] + self.RATE_LIMIT_PERIOD - now
                if sleep_time > 0:
                    logger.debug(f"Rate limit reached. Sleeping for {sleep_time:.2f}s")
                    await asyncio.sleep(sleep_time)
                    # Re-clean after sleep
                    now = asyncio.get_event_loop().time()
                    cutoff = now - self.RATE_LIMIT_PERIOD
                    self._request_times = [t for t in self._request_times if t > cutoff]
            
            # Add minimum interval between requests
            if self._request_times:
                time_since_last = now - self._request_times[-1]
                if time_since_last < self.MIN_REQUEST_INTERVAL:
                    sleep_time = self.MIN_REQUEST_INTERVAL - time_since_last
                    await asyncio.sleep(sleep_time)
                    now = asyncio.get_event_loop().time()
            
            self._request_times.append(now)
    
    async def _get_from_cache(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """Retrieve data from cache if available"""
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
                result = await self.cache_client.table('dataforseo_cache').select('*').eq('key', cache_key).execute()
                if result.data and len(result.data) > 0:
                    cache_entry = result.data[0]
                    expires_at = datetime.fromisoformat(cache_entry['expires_at'])
                    if expires_at > datetime.utcnow():
                        return cache_entry['data']
        except Exception as e:
            logger.warning(f"Cache retrieval error: {e}")
        
        return None
    
    async def _save_to_cache(self, cache_key: str, data: Dict[str, Any]):
        """Save data to cache"""
        if not self.cache_client:
            return
        
        try:
            expires_at = datetime.utcnow() + timedelta(hours=self.cache_ttl_hours)
            
            # Try Redis-style interface
            if hasattr(self.cache_client, 'setex'):
                ttl_seconds = int(self.cache_ttl_hours * 3600)
                await self.cache_client.setex(
                    cache_key,
                    ttl_seconds,
                    json.dumps(data)
                )
            
            # Try Supabase-style interface
            elif hasattr(self.cache_client, 'table'):
                await self.cache_client.table('dataforseo_cache').upsert({
                    'key': cache_key,
                    'data': data,
                    'expires_at': expires_at.isoformat(),
                    'created_at': datetime.utcnow().isoformat()
                }).execute()
        except Exception as e:
            logger.warning(f"Cache save error: {e}")
    
    @retry(
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError, RateLimitError)),
        before_sleep=before_sleep_log(logger, logging.WARNING)
    )
    async def _make_request(
        self,
        endpoint: str,
        method: str = 'POST',
        data: Optional[List[Dict[str, Any]]] = None,
        use_cache: bool = True
    ) -> Dict[str, Any]:
        """
        Make HTTP request to DataForSEO API.
        
        Args:
            endpoint: API endpoint path (e.g., '/v3/serp/google/organic/live/advanced')
            method: HTTP method
            data: Request payload (for POST requests)
            use_cache: Whether to use caching
        
        Returns:
            API response data
        """
        # Check cache first
        cache_key = None
        if use_cache and data:
            cache_key = self._generate_cache_key(endpoint, data[0] if data else {})
            cached = await self._get_from_cache(cache_key)
            if cached:
                logger.debug(f"Cache hit for {endpoint}")
                return cached
        
        # Rate limiting
        await self._check_rate_limit()
        
        # Make request
        url = f"{self.BASE_URL}{endpoint}"
        
        session = self._session
        close_session = False
        if not session:
            session = ClientSession(timeout=self.timeout)
            close_session = True
        
        try:
            logger.debug(f"Making {method} request to {url}")
            
            async with session.request(
                method,
                url,
                json=data,
                auth=self._get_auth()
            ) as response:
                
                # Handle rate limiting
                if response.status == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    logger.warning(f"Rate limit hit. Retry after {retry_after}s")
                    await asyncio.sleep(retry_after)
                    raise RateLimitError(f"Rate limit exceeded. Retry after {retry_after}s")
                
                # Handle authentication errors
                if response.status == 401:
                    raise AuthenticationError("Invalid DataForSEO credentials")
                
                # Parse response
                response_data = await response.json()
                
                # Check for API errors in response
                if response.status >= 400:
                    error_msg = response_data.get('status_message', f'HTTP {response.status}')
                    raise APIError(f"API error: {error_msg}")
                
                # DataForSEO returns status in the response body
                if 'tasks' in response_data:
                    for task in response_data.get('tasks', []):
                        if task.get('status_code') != 20000:
                            error_msg = task.get('status_message', 'Unknown error')
                            raise APIError(f"Task error: {error_msg}")
                
                # Cache successful response
                if use_cache and cache_key and response.status == 200:
                    await self._save_to_cache(cache_key, response_data)
                
                return response_data
        
        finally:
            if close_session:
                await session.close()
    
    async def get_serp_data(
        self,
        keywords: List[str],
        location_code: int = 2840,  # United States
        language_code: str = "en",
        device: str = "desktop",
        use_cache: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Get live SERP data for multiple keywords.
        
        Args:
            keywords: List of keywords to check
            location_code: DataForSEO location code (2840 = United States)
            language_code: Language code (en, es, etc.)
            device: Device type (desktop, mobile, tablet)
            use_cache: Whether to use caching
        
        Returns:
            List of standardized SERP data dictionaries
        """
        endpoint = f"/{self.API_VERSION}/serp/google/organic/live/advanced"
        
        # Build request payload
        tasks = []
        for keyword in keywords:
            tasks.append({
                "keyword": keyword,
                "location_code": location_code,
                "language_code": language_code,
                "device": device,
                "os": "windows" if device == "desktop" else "ios",
                "depth": 100,  # Get top 100 results
                "calculate_rectangles": False
            })
        
        # Make request
        response = await self._make_request(
            endpoint,
            method='POST',
            data=tasks,
            use_cache=use_cache
        )
        
        # Parse and normalize response
        results = []
        for task in response.get('tasks', []):
            if task.get('status_code') == 20000 and task.get('result'):
                for result in task['result']:
                    normalized = self._normalize_serp_result(result)
                    results.append(normalized)
        
        return results
    
    def _normalize_serp_result(self, raw_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize raw DataForSEO SERP result into standardized format.
        
        Returns:
            {
                'keyword': str,
                'location': str,
                'language': str,
                'device': str,
                'total_results': int,
                'serp_features': List[str],
                'serp_feature_details': Dict[str, Any],
                'organic_results': List[Dict],
                'competitors': List[str],
                'timestamp': str
            }
        """
        items = raw_result.get('items', [])
        
        # Extract SERP features
        serp_features = []
        serp_feature_details = {}
        
        for item in items:
            item_type = item.get('type', '')
            
            if item_type == 'featured_snippet':
                serp_features.append('featured_snippet')
                serp_feature_details['featured_snippet'] = {
                    'domain': self._extract_domain(item.get('url', '')),
                    'url': item.get('url'),
                    'title': item.get('title')
                }
            
            elif item_type == 'people_also_ask':
                if 'people_also_ask' not in serp_features:
                    serp_features.append('people_also_ask')
                    serp_feature_details['people_also_ask'] = {
                        'count': 0,
                        'questions': []
                    }
                serp_feature_details['people_also_ask']['count'] += 1
                serp_feature_details['people_also_ask']['questions'].append(
                    item.get('title', '')
                )
            
            elif item_type == 'video':
                if 'video' not in serp_features:
                    serp_features.append('video')
                    serp_feature_details['video'] = {'count': 0}
                serp_feature_details['video']['count'] += 1
            
            elif item_type == 'local_pack':
                serp_features.append('local_pack')
                serp_feature_details['local_pack'] = {
                    'count': len(item.get('items', []))
                }
            
            elif item_type == 'knowledge_graph':
                serp_features.append('knowledge_panel')
                serp_feature_details['knowledge_panel'] = {
                    'title': item.get('title')
                }
            
            elif item_type == 'ai_overview' or item_type == 'google_ai_overview':
                serp_features.append('ai_overview')
            
            elif item_type == 'images':
                serp_features.append('images')
                serp_feature_details['images'] = {
                    'count': len(item.get('items', []))
                }
            
            elif item_type == 'shopping':
                if 'shopping' not in serp_features:
                    serp_features.append('shopping')
                    serp_feature_details['shopping'] = {'count': 0}
                serp_feature_details['shopping']['count'] += 1
            
            elif item_type == 'top_stories':
                serp_features.append('top_stories')
                serp_feature_details['top_stories'] = {
                    'count': len(item.get('items', []))
                }
            
            elif item_type == 'twitter':
                serp_features.append('twitter')
            
            elif item_type == 'related_searches':
                serp_features.append('related_searches')
                serp_feature_details['related_searches'] = {
                    'terms': [i.get('title', '') for i in item.get('items', [])]
                }
            
            elif item_type == 'recipes':
                serp_features.append('recipes')
            
            elif item_type == 'answer_box':
                serp_features.append('answer_box')
        
        # Extract organic results
        organic_results = []
        competitors = set()
        
        for item in items:
            if item.get('type') == 'organic':
                domain = self._extract_domain(item.get('url', ''))
                competitors.add(domain)
                
                organic_results.append({
                    'position': item.get('rank_absolute', 0),
                    'url': item.get('url', ''),
                    'domain': domain,
                    'title': item.get('title', ''),
                    'description': item.get('description', ''),
                    'breadcrumb': item.get('breadcrumb', ''),
                    'is_amp': item.get('amp_version', False),
                    'rating': item.get('rating', {}).get('value') if item.get('rating') else None
                })
        
        return {
            'keyword': raw_result.get('keyword', ''),
            'location': raw_result.get('location_code', ''),
            'language': raw_result.get('language_code', ''),
            'device': raw_result.get('device', 'desktop'),
            'total_results': raw_result.get('se_results_count', 0),
            'serp_features': serp_features,
            'serp_feature_details': serp_feature_details,
            'organic_results': organic_results,
            'competitors': sorted(list(competitors)),
            'timestamp': datetime.utcnow().isoformat()
        }
    
    def _extract_domain(self, url: str) -> str:
        """Extract domain from URL"""
        if not url:
            return ''
        
        # Remove protocol
        domain = url.replace('https://', '').replace('http://', '')
        
        # Remove path
        domain = domain.split('/')[0]
        
        # Remove www
        if domain.startswith('www.'):
            domain = domain[4:]
        
        return domain
    
    async def get_competitor_domains(
        self,
        keywords: List[str],
        location_code: int = 2840,
        min_appearance_threshold: int = 3
    ) -> List[Dict[str, Any]]:
        """
        Identify competitor domains from SERP results.
        
        Args:
            keywords: List of keywords to analyze
            location_code: DataForSEO location code
            min_appearance_threshold: Minimum keyword appearances to be considered competitor
        
        Returns:
            List of competitor analysis dictionaries sorted by frequency
        """
        serp_results = await self.get_serp_data(
            keywords=keywords,
            location_code=location_code
        )
        
        # Count domain appearances
        domain_data = {}
        
        for serp in serp_results:
            keyword = serp['keyword']
            for result in serp['organic_results']:
                domain = result['domain']
                position = result['position']
                
                if domain not in domain_data:
                    domain_data[domain] = {
                        'domain': domain,
                        'appearances': 0,
                        'keywords': [],
                        'positions': [],
                        'avg_position': 0,
                        'top_3_count': 0,
                        'top_10_count': 0
                    }
                
                domain_data[domain]['appearances'] += 1
                domain_data[domain]['keywords'].append(keyword)
                domain_data[domain]['positions'].append(position)
                
                if position <= 3:
                    domain_data[domain]['top_3_count'] += 1
                if position <= 10:
                    domain_data[domain]['top_10_count'] += 1
        
        # Calculate averages and filter
        competitors = []
        for domain, data in domain_data.items():
            if data['appearances'] >= min_appearance_threshold:
                data['avg_position'] = sum(data['positions']) / len(data['positions'])
                data['keyword_share'] = data['appearances'] / len(keywords)
                
                # Calculate threat level
                threat_score = (
                    data['top_3_count'] * 3 +
                    data['top_10_count'] * 1 +
                    data['appearances'] * 0.5
                ) / len(keywords)
                
                if threat_score > 1.5:
                    data['threat_level'] = 'high'
                elif threat_score > 0.8:
                    data['threat_level'] = 'medium'
                else:
                    data['threat_level'] = 'low'
                
                competitors.append(data)
        
        # Sort by appearances descending
        competitors.sort(key=lambda x: x['appearances'], reverse=True)
        
        return competitors
    
    def calculate_ctr_opportunity(
        self,
        keyword: str,
        current_position: int,
        serp_features: List[str],
        monthly_impressions: int,
        current_ctr: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        Calculate CTR opportunity for a keyword considering SERP features.
        
        Args:
            keyword: The keyword
            current_position: Current organic position
            serp_features: List of SERP features present
            monthly_impressions: Monthly impressions from GSC
            current_ctr: Current CTR (optional, will use baseline if not provided)
        
        Returns:
            CTR opportunity analysis with visual position and estimated click impact
        """
        # Calculate visual position (accounting for SERP features above)
        visual_position = self._calculate_visual_position(current_position, serp_features)
        
        # Get baseline CTR for current position
        baseline_ctr = self.BASELINE_CTR_CURVE.get(
            min(current_position, 20),
            0.005
        )
        
        # Adjust for SERP features (reduce CTR based on features present)
        feature_impact = self._calculate_feature_impact(serp_features, current_position)
        adjusted_ctr = baseline_ctr * (1 - feature_impact)
        
        # Use actual CTR if provided, otherwise use adjusted baseline
        if current_ctr is None:
            current_ctr = adjusted_ctr
        
        # Calculate opportunity if moved to top 5
        opportunities = {}
        for target_position in [1, 2, 3, 4, 5]:
            if target_position < current_position:
                target_ctr = self.BASELINE_CTR_CURVE.get(target_position, 0.01)
                # Assume fewer SERP features impact at top positions
                target_feature_impact = feature_impact * 0.7
                target_ctr = target_ctr * (1 - target_feature_impact)
                
                click_gain = monthly_impressions * (target_ctr - current_ctr)
                
                opportunities[f'position_{target_position}'] = {
                    'position': target_position,
                    'estimated_ctr': round(target_ctr, 4),
                    'click_gain': round(click_gain, 1),
                    'ctr_increase_pct': round((target_ctr - current_ctr) / current_ctr * 100, 1) if current_ctr > 0 else 0
                }
        
        return {
            'keyword': keyword,
            'current_position': current_position,
            'visual_position': visual_position,
            'current_ctr': round(current_ctr, 4),
            'baseline_ctr': round(baseline_ctr, 4),
            'feature_impact_pct': round(feature_impact * 100, 1),
            'serp_features_above': serp_features,
            'monthly_impressions': monthly_impressions,
            'opportunities': opportunities,
            'max_opportunity': max(
                opportunities.values(),
                key=lambda x: x['click_gain']
            ) if opportunities else None
        }
    
    def _calculate_visual_position(
        self,
        organic_position: int,
        serp_features: List[str]
    ) -> int:
        """
        Calculate visual position accounting for SERP features above organic result.
        
        Each SERP feature adds to the visual distance from top of page.
        """
        visual_offset = 0
        
        # Feature weights (how many "positions" each feature adds)
        feature_weights = {
            'ai_overview': 3,
            'featured_snippet': 2,
            'knowledge_panel': 2,
            'local_pack': 3,
            'shopping': 2,
            'images': 1,
            'video': 1,
            'top_stories': 2,
            'people_also_ask': 0.5,  # Each PAA question is ~0.5 positions
            'related_searches': 0,  # Usually at bottom
            'answer_box': 2,
            'recipes': 1,
            'twitter': 1
        }
        
        for feature in serp_features:
            visual_offset += feature_weights.get(feature, 0)
        
        return organic_position + int(visual_offset)
    
    def _calculate_feature_impact(
        self,
        serp_features: List[str],
        position: int
    ) -> float:
        """
        Calculate the CTR impact of SERP features (as percentage reduction).
        
        Returns value between 0 and 1 (0 = no impact, 1 = complete CTR loss)
        """
        if not serp_features:
            return 0.0
        
        # Base impact by feature type
        feature_impacts = {
            'ai_overview': 0.35,
            'featured_snippet': 0.30,
            'knowledge_panel': 0.25,
            'local_pack': 0.40,
            'shopping': 0.20,
            'video': 0.15,
            'top_stories': 0.15,
            'images': 0.10,
            'people_also_ask': 0.05,
            'answer_box': 0.30,
            'recipes': 0.10,
            'twitter': 0.05
        }
        
        total_impact = 0.0
        for feature in serp_features:
            impact = feature_impacts.get(feature, 0)
            
            # Features impact lower positions more severely
            if position > 3:
                impact *= 1.2
            elif position <= 1:
                impact *= 0.7
            
            total_impact += impact
        
        # Cap total impact at 0.8 (features can't eliminate all clicks)
        return min(total_impact, 0.8)
    
    async def batch_serp_analysis(
        self,
        keywords: List[str],
        gsc_keyword_data: Dict[str, Dict[str, Any]],
        location_code: int = 2840,
        batch_size: int = 10
    ) -> Dict[str, Any]:
        """
        Perform batch SERP analysis for multiple keywords with rate limiting.
        
        Args:
            keywords: List of keywords to analyze
            gsc_keyword_data: Dict mapping keyword to GSC data (position, impressions, CTR)
            location_code: DataForSEO location code
            batch_size: Number of keywords per batch
        
        Returns:
            Comprehensive SERP analysis including opportunities and competitors
        """
        all_results = []
        
        # Process in batches
        for i in range(0, len(keywords), batch_size):
            batch = keywords[i:i + batch_size]
            logger.info(f"Processing batch {i // batch_size + 1}: {len(batch)} keywords")
            
            serp_results = await self.get_serp_data(
                keywords=batch,
                location_code=location_code
            )
            
            all_results.extend(serp_results)
            
            # Small delay between batches
            if i + batch_size < len(keywords):
                await asyncio.sleep(1)
        
        # Analyze opportunities
        opportunities = []
        for serp in all_results:
            keyword = serp['keyword']
            gsc_data = gsc_keyword_data.get(keyword, {})
            
            # Find user's position in organic results
            user_position = None
            for result in serp['organic_results']:
                # This would need user's domain passed in
                # For now, we'll use GSC position
                pass
            
            if gsc_data:
                opportunity = self.calculate_ctr_opportunity(
                    keyword=keyword,
                    current_position=int(gsc_data.get('position', 50)),
                    serp_features=serp['serp_features'],
                    monthly_impressions=int(gsc_data.get('impressions', 0)),
                    current_ctr=gsc_data.get('ctr')
                )
                opportunities.append(opportunity)
        
        # Get competitor analysis
        competitors = await self.get_competitor_domains(
            keywords=keywords,
            location_code=location_code,
            min_appearance_threshold=max(3, len(keywords) // 20)
        )
        
        # SERP feature frequency analysis
        feature_frequency = {}
        for serp in all_results:
            for feature in serp['serp_features']:
                feature_frequency[feature] = feature_frequency.get(feature, 0) + 1
        
        feature_frequency_pct = {
            feature: (count / len(all_results)) * 100
            for feature, count in feature_frequency.items()
        }
        
        return {
            'keywords_analyzed': len(keywords),
            'serp_data': all_results,
            'opportunities': sorted(
                opportunities,
                key=lambda x: x.get('max_opportunity', {}).get('click_gain', 0) if x.get('max_opportunity') else 0,
                reverse=True
            ),
            'competitors': competitors[:20],  # Top 20 competitors
            'serp_feature_frequency': feature_frequency_pct,
            'summary': {
                'avg_serp_features_per_keyword': sum(len(s['serp_features']) for s in all_results) / len(all_results) if all_results else 0,
                'most_common_features': sorted(
                    feature_frequency.items(),
                    key=lambda x: x[1],
                    reverse=True
                )[:5],
                'total_click_opportunity': sum(
                    opp.get('max_opportunity', {}).get('click_gain', 0)
                    for opp in opportunities
                    if opp.get('max_opportunity')
                )
            }
        }
    
    async def close(self):
        """Close the session if it was created internally"""
        if self._session:
            await self._session.close()
            self._session = None
    
    async def __aenter__(self):
        """Async context manager entry"""
        self._session = ClientSession(timeout=self.timeout)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close()


# Convenience function for quick SERP checks
async def quick_serp_check(
    keywords: Union[str, List[str]],
    location_code: int = 2840,
    api_login: Optional[str] = None,
    api_password: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Quick SERP check without managing service instance.
    
    Args:
        keywords: Single keyword or list of keywords
        location_code: DataForSEO location code
        api_login: DataForSEO API login
        api_password: DataForSEO API password
    
    Returns:
        List of SERP analysis results
    """
    if isinstance(keywords, str):
        keywords = [keywords]
    
    async with DataForSEOService(
        api_login=api_login,
        api_password=api_password
    ) as service:
        return await service.get_serp_data(
            keywords=keywords,
            location_code=location_code
        )