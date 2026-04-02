"""
DataForSEO API client for Search Intelligence Report.

Handles SERP requests, competitor analysis, and ranking data retrieval
with authentication, rate limiting, error handling, and response parsing.
"""

import os
import time
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import hashlib
import json

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import backoff

logger = logging.getLogger(__name__)


class DataForSEOError(Exception):
    """Base exception for DataForSEO API errors."""
    pass


class DataForSEORateLimitError(DataForSEOError):
    """Raised when API rate limit is exceeded."""
    pass


class DataForSEOAuthError(DataForSEOError):
    """Raised when authentication fails."""
    pass


class DataForSEOClient:
    """
    Client for DataForSEO API with support for SERP requests,
    competitor analysis, and ranking data retrieval.
    
    Features:
    - Automatic retry with exponential backoff
    - Rate limiting (respects DataForSEO API limits)
    - Request caching to avoid redundant API calls
    - Comprehensive error handling
    - Response parsing and validation
    """
    
    BASE_URL = "https://api.dataforseo.com/v3"
    
    # Rate limits (requests per second)
    DEFAULT_RATE_LIMIT = 2  # Conservative default
    
    def __init__(
        self,
        login: Optional[str] = None,
        password: Optional[str] = None,
        rate_limit: Optional[float] = None,
        cache_ttl: int = 3600,
    ):
        """
        Initialize DataForSEO client.
        
        Args:
            login: DataForSEO login (defaults to DATAFORSEO_LOGIN env var)
            password: DataForSEO password (defaults to DATAFORSEO_PASSWORD env var)
            rate_limit: Max requests per second (defaults to DEFAULT_RATE_LIMIT)
            cache_ttl: Cache time-to-live in seconds
        """
        self.login = login or os.getenv("DATAFORSEO_LOGIN")
        self.password = password or os.getenv("DATAFORSEO_PASSWORD")
        
        if not self.login or not self.password:
            raise DataForSEOAuthError(
                "DataForSEO credentials not provided. "
                "Set DATAFORSEO_LOGIN and DATAFORSEO_PASSWORD environment variables."
            )
        
        self.rate_limit = rate_limit or self.DEFAULT_RATE_LIMIT
        self.cache_ttl = cache_ttl
        
        # Request tracking for rate limiting
        self._request_times: List[float] = []
        self._last_request_time = 0.0
        
        # Setup session with retry logic
        self.session = self._create_session()
        
        # In-memory cache for responses
        self._cache: Dict[str, Dict[str, Any]] = {}
        
        logger.info(f"DataForSEO client initialized with rate limit: {self.rate_limit} req/s")
    
    def _create_session(self) -> requests.Session:
        """Create requests session with retry configuration."""
        session = requests.Session()
        
        # Configure retries
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Set auth
        session.auth = (self.login, self.password)
        
        # Set headers
        session.headers.update({
            "Content-Type": "application/json",
            "User-Agent": "SearchIntelligenceReport/1.0"
        })
        
        return session
    
    def _enforce_rate_limit(self):
        """Enforce rate limiting by sleeping if necessary."""
        now = time.time()
        
        # Clean up old request times (keep only last second)
        self._request_times = [
            t for t in self._request_times 
            if now - t < 1.0
        ]
        
        # If at rate limit, wait
        if len(self._request_times) >= self.rate_limit:
            sleep_time = 1.0 - (now - self._request_times[0])
            if sleep_time > 0:
                logger.debug(f"Rate limit reached, sleeping for {sleep_time:.2f}s")
                time.sleep(sleep_time)
                now = time.time()
        
        self._request_times.append(now)
        self._last_request_time = now
    
    def _get_cache_key(self, endpoint: str, payload: Dict) -> str:
        """Generate cache key from endpoint and payload."""
        payload_str = json.dumps(payload, sort_keys=True)
        content = f"{endpoint}:{payload_str}"
        return hashlib.md5(content.encode()).hexdigest()
    
    def _get_cached_response(self, cache_key: str) -> Optional[Dict]:
        """Get cached response if valid."""
        if cache_key not in self._cache:
            return None
        
        cached = self._cache[cache_key]
        if time.time() - cached["timestamp"] > self.cache_ttl:
            del self._cache[cache_key]
            return None
        
        logger.debug(f"Cache hit for key: {cache_key}")
        return cached["data"]
    
    def _cache_response(self, cache_key: str, data: Dict):
        """Cache API response."""
        self._cache[cache_key] = {
            "timestamp": time.time(),
            "data": data
        }
    
    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.RequestException, DataForSEORateLimitError),
        max_tries=5,
        max_time=300
    )
    def _make_request(
        self,
        endpoint: str,
        method: str = "POST",
        payload: Optional[Dict] = None,
        use_cache: bool = True
    ) -> Dict:
        """
        Make API request with rate limiting, caching, and error handling.
        
        Args:
            endpoint: API endpoint path
            method: HTTP method
            payload: Request payload
            use_cache: Whether to use caching
            
        Returns:
            API response data
            
        Raises:
            DataForSEOError: On API errors
            DataForSEORateLimitError: On rate limit errors
            DataForSEOAuthError: On authentication errors
        """
        url = f"{self.BASE_URL}{endpoint}"
        
        # Check cache
        if use_cache and payload:
            cache_key = self._get_cache_key(endpoint, payload)
            cached = self._get_cached_response(cache_key)
            if cached:
                return cached
        
        # Enforce rate limiting
        self._enforce_rate_limit()
        
        # Make request
        try:
            if method == "POST":
                response = self.session.post(url, json=payload)
            else:
                response = self.session.get(url, params=payload)
            
            # Handle response
            if response.status_code == 401:
                raise DataForSEOAuthError("Authentication failed")
            elif response.status_code == 429:
                raise DataForSEORateLimitError("Rate limit exceeded")
            elif response.status_code != 200:
                raise DataForSEOError(
                    f"API request failed with status {response.status_code}: {response.text}"
                )
            
            data = response.json()
            
            # Validate response structure
            if "status_code" not in data:
                raise DataForSEOError("Invalid API response format")
            
            if data["status_code"] != 20000:
                error_msg = data.get("status_message", "Unknown error")
                raise DataForSEOError(f"API error: {error_msg}")
            
            # Cache successful response
            if use_cache and payload:
                self._cache_response(cache_key, data)
            
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {str(e)}")
            raise
    
    def get_live_serp(
        self,
        keyword: str,
        location_code: int = 2840,  # United States
        language_code: str = "en",
        device: str = "desktop",
        depth: int = 100
    ) -> Dict:
        """
        Get live SERP data for a keyword.
        
        Args:
            keyword: Search keyword
            location_code: DataForSEO location code
            language_code: Language code
            device: Device type (desktop, mobile, tablet)
            depth: Number of results to retrieve
            
        Returns:
            Parsed SERP data with organic results and features
        """
        endpoint = "/serp/google/organic/live/advanced"
        
        payload = [{
            "keyword": keyword,
            "location_code": location_code,
            "language_code": language_code,
            "device": device,
            "depth": depth,
            "calculate_rectangles": True  # For SERP feature position analysis
        }]
        
        logger.info(f"Fetching live SERP for keyword: {keyword}")
        
        try:
            response = self._make_request(endpoint, payload=payload)
            return self._parse_serp_response(response)
        except Exception as e:
            logger.error(f"Failed to fetch SERP for '{keyword}': {str(e)}")
            raise
    
    def get_bulk_serp(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
        device: str = "desktop",
        depth: int = 100
    ) -> List[Dict]:
        """
        Get live SERP data for multiple keywords in bulk.
        
        Args:
            keywords: List of search keywords
            location_code: DataForSEO location code
            language_code: Language code
            device: Device type
            depth: Number of results per keyword
            
        Returns:
            List of parsed SERP data for each keyword
        """
        endpoint = "/serp/google/organic/live/advanced"
        
        # DataForSEO allows up to 100 tasks per request
        batch_size = 100
        all_results = []
        
        for i in range(0, len(keywords), batch_size):
            batch = keywords[i:i + batch_size]
            
            payload = [
                {
                    "keyword": kw,
                    "location_code": location_code,
                    "language_code": language_code,
                    "device": device,
                    "depth": depth,
                    "calculate_rectangles": True,
                    "tag": kw  # Use keyword as tag for identification
                }
                for kw in batch
            ]
            
            logger.info(f"Fetching SERP data for {len(batch)} keywords (batch {i//batch_size + 1})")
            
            try:
                response = self._make_request(endpoint, payload=payload)
                batch_results = self._parse_bulk_serp_response(response)
                all_results.extend(batch_results)
            except Exception as e:
                logger.error(f"Failed to fetch SERP batch: {str(e)}")
                # Continue with other batches
                continue
        
        return all_results
    
    def _parse_serp_response(self, response: Dict) -> Dict:
        """
        Parse single SERP API response.
        
        Returns:
            {
                "keyword": str,
                "organic_results": List[Dict],
                "serp_features": Dict,
                "total_results": int,
                "visual_positions": Dict
            }
        """
        if not response.get("tasks"):
            raise DataForSEOError("No tasks in response")
        
        task = response["tasks"][0]
        if task.get("status_code") != 20000:
            error = task.get("status_message", "Unknown error")
            raise DataForSEOError(f"Task failed: {error}")
        
        result = task.get("result", [{}])[0]
        items = result.get("items", [])
        
        # Extract organic results
        organic_results = []
        serp_features = {
            "featured_snippet": None,
            "people_also_ask": [],
            "local_pack": None,
            "knowledge_panel": None,
            "ai_overview": None,
            "video_carousel": None,
            "image_pack": None,
            "shopping_results": None,
            "top_stories": None,
            "twitter": None,
            "reddit_threads": []
        }
        
        visual_position_counter = 0
        
        for item in items:
            item_type = item.get("type")
            
            if item_type == "organic":
                # Calculate visual position
                visual_position = visual_position_counter
                visual_position_counter += 1
                
                organic_results.append({
                    "rank_absolute": item.get("rank_absolute"),
                    "rank_group": item.get("rank_group"),
                    "position": item.get("rank_absolute"),
                    "visual_position": visual_position,
                    "url": item.get("url"),
                    "domain": item.get("domain"),
                    "title": item.get("title"),
                    "description": item.get("description"),
                    "breadcrumb": item.get("breadcrumb"),
                    "website_name": item.get("website_name"),
                    "relative_url": item.get("relative_url")
                })
            
            elif item_type == "featured_snippet":
                serp_features["featured_snippet"] = {
                    "type": item.get("featured_snippet", {}).get("type"),
                    "url": item.get("url"),
                    "domain": item.get("domain"),
                    "title": item.get("title"),
                    "description": item.get("description")
                }
                visual_position_counter += 2  # Featured snippet takes ~2 position slots
            
            elif item_type == "people_also_ask":
                paa_items = item.get("items", [])
                serp_features["people_also_ask"] = [
                    {
                        "question": paa.get("title"),
                        "url": paa.get("url"),
                        "domain": paa.get("domain")
                    }
                    for paa in paa_items
                ]
                visual_position_counter += len(paa_items) * 0.5  # Each PAA ~0.5 positions
            
            elif item_type == "local_pack":
                serp_features["local_pack"] = {
                    "count": len(item.get("items", [])),
                    "items": item.get("items", [])
                }
                visual_position_counter += 3  # Local pack takes ~3 positions
            
            elif item_type == "knowledge_graph":
                serp_features["knowledge_panel"] = {
                    "title": item.get("title"),
                    "description": item.get("description"),
                    "url": item.get("url")
                }
                visual_position_counter += 4  # Knowledge panel takes significant space
            
            elif item_type == "ai_overview":
                serp_features["ai_overview"] = {
                    "text": item.get("text"),
                    "links": item.get("links", [])
                }
                visual_position_counter += 3  # AI overview takes ~3 positions
            
            elif item_type == "video":
                if not serp_features["video_carousel"]:
                    serp_features["video_carousel"] = []
                serp_features["video_carousel"].append({
                    "title": item.get("title"),
                    "url": item.get("url"),
                    "domain": item.get("domain")
                })
                visual_position_counter += 1.5  # Video carousel
            
            elif item_type == "images":
                serp_features["image_pack"] = {
                    "count": len(item.get("items", []))
                }
                visual_position_counter += 1  # Image pack
            
            elif item_type == "shopping":
                serp_features["shopping_results"] = {
                    "count": len(item.get("items", []))
                }
                visual_position_counter += 2  # Shopping results
            
            elif item_type == "top_stories":
                serp_features["top_stories"] = {
                    "count": len(item.get("items", []))
                }
                visual_position_counter += 2  # Top stories
            
            elif item_type == "twitter":
                serp_features["twitter"] = {
                    "count": len(item.get("items", []))
                }
                visual_position_counter += 1  # Twitter box
            
            elif item_type == "reddit":
                serp_features["reddit_threads"].append({
                    "title": item.get("title"),
                    "url": item.get("url")
                })
                visual_position_counter += 0.5  # Reddit threads
        
        # Update visual positions for organic results
        for result in organic_results:
            result["visual_displacement"] = result["visual_position"] - result["position"]
        
        return {
            "keyword": result.get("keyword"),
            "organic_results": organic_results,
            "serp_features": serp_features,
            "total_results": result.get("se_results_count", 0),
            "check_url": result.get("check_url")
        }
    
    def _parse_bulk_serp_response(self, response: Dict) -> List[Dict]:
        """Parse bulk SERP API response."""
        results = []
        
        for task in response.get("tasks", []):
            if task.get("status_code") != 20000:
                logger.warning(f"Task failed: {task.get('status_message')}")
                continue
            
            try:
                # Create single-task response format
                single_response = {
                    "tasks": [task]
                }
                parsed = self._parse_serp_response(single_response)
                results.append(parsed)
            except Exception as e:
                logger.error(f"Failed to parse SERP result: {str(e)}")
                continue
        
        return results
    
    def get_competitor_domains(
        self,
        serp_results: List[Dict],
        min_appearance_threshold: float = 0.2
    ) -> List[Dict]:
        """
        Analyze SERP results to identify competitor domains.
        
        Args:
            serp_results: List of parsed SERP results
            min_appearance_threshold: Minimum % of keywords domain must appear in
            
        Returns:
            List of competitor domains with metrics
        """
        domain_stats = {}
        total_keywords = len(serp_results)
        
        for serp in serp_results:
            keyword = serp["keyword"]
            
            for result in serp["organic_results"]:
                domain = result["domain"]
                position = result["position"]
                
                if domain not in domain_stats:
                    domain_stats[domain] = {
                        "domain": domain,
                        "keywords_count": 0,
                        "keywords": [],
                        "positions": [],
                        "avg_position": 0,
                        "top3_count": 0,
                        "top10_count": 0
                    }
                
                stats = domain_stats[domain]
                stats["keywords_count"] += 1
                stats["keywords"].append(keyword)
                stats["positions"].append(position)
                
                if position <= 3:
                    stats["top3_count"] += 1
                if position <= 10:
                    stats["top10_count"] += 1
        
        # Calculate averages and filter by threshold
        competitors = []
        for domain, stats in domain_stats.items():
            appearance_rate = stats["keywords_count"] / total_keywords
            
            if appearance_rate >= min_appearance_threshold:
                stats["appearance_rate"] = appearance_rate
                stats["avg_position"] = sum(stats["positions"]) / len(stats["positions"])
                
                # Calculate threat level
                threat_score = (
                    stats["appearance_rate"] * 0.4 +
                    (stats["top3_count"] / stats["keywords_count"]) * 0.3 +
                    (1 - stats["avg_position"] / 20) * 0.3
                )
                
                if threat_score > 0.7:
                    threat_level = "high"
                elif threat_score > 0.4:
                    threat_level = "medium"
                else:
                    threat_level = "low"
                
                stats["threat_level"] = threat_level
                stats["threat_score"] = threat_score
                
                # Remove detailed keyword list to save space (keep count)
                stats["sample_keywords"] = stats["keywords"][:5]
                del stats["keywords"]
                del stats["positions"]
                
                competitors.append(stats)
        
        # Sort by appearance rate
        competitors.sort(key=lambda x: x["appearance_rate"], reverse=True)
        
        return competitors
    
    def analyze_serp_features_impact(self, serp_results: List[Dict]) -> Dict:
        """
        Analyze SERP features impact across multiple keywords.
        
        Args:
            serp_results: List of parsed SERP results
            
        Returns:
            Analysis of SERP features and their impact
        """
        feature_stats = {
            "featured_snippet": {"count": 0, "avg_displacement": 0},
            "people_also_ask": {"count": 0, "avg_count": 0, "avg_displacement": 0},
            "local_pack": {"count": 0, "avg_displacement": 0},
            "knowledge_panel": {"count": 0, "avg_displacement": 0},
            "ai_overview": {"count": 0, "avg_displacement": 0},
            "video_carousel": {"count": 0, "avg_displacement": 0},
            "image_pack": {"count": 0, "avg_displacement": 0},
            "shopping_results": {"count": 0, "avg_displacement": 0},
            "top_stories": {"count": 0, "avg_displacement": 0}
        }
        
        keywords_with_displacement = []
        total_keywords = len(serp_results)
        
        for serp in serp_results:
            features = serp["serp_features"]
            
            # Count feature occurrences
            if features["featured_snippet"]:
                feature_stats["featured_snippet"]["count"] += 1
            
            if features["people_also_ask"]:
                feature_stats["people_also_ask"]["count"] += 1
                feature_stats["people_also_ask"]["avg_count"] += len(features["people_also_ask"])
            
            if features["local_pack"]:
                feature_stats["local_pack"]["count"] += 1
            
            if features["knowledge_panel"]:
                feature_stats["knowledge_panel"]["count"] += 1
            
            if features["ai_overview"]:
                feature_stats["ai_overview"]["count"] += 1
            
            if features["video_carousel"]:
                feature_stats["video_carousel"]["count"] += 1
            
            if features["image_pack"]:
                feature_stats["image_pack"]["count"] += 1
            
            if features["shopping_results"]:
                feature_stats["shopping_results"]["count"] += 1
            
            if features["top_stories"]:
                feature_stats["top_stories"]["count"] += 1
            
            # Find significant displacement
            for result in serp["organic_results"]:
                if result["visual_displacement"] > 3:
                    keywords_with_displacement.append({
                        "keyword": serp["keyword"],
                        "organic_position": result["position"],
                        "visual_position": result["visual_position"],
                        "displacement": result["visual_displacement"],
                        "url": result["url"],
                        "domain": result["domain"]
                    })
        
        # Calculate percentages
        for feature, stats in feature_stats.items():
            stats["percentage"] = (stats["count"] / total_keywords * 100) if total_keywords > 0 else 0
        
        # Calculate average PAA count
        if feature_stats["people_also_ask"]["count"] > 0:
            feature_stats["people_also_ask"]["avg_count"] /= feature_stats["people_also_ask"]["count"]
        
        # Sort displacement by impact
        keywords_with_displacement.sort(key=lambda x: x["displacement"], reverse=True)
        
        return {
            "feature_stats": feature_stats,
            "keywords_with_displacement": keywords_with_displacement[:50],  # Top 50
            "total_keywords_analyzed": total_keywords,
            "keywords_with_significant_displacement": len(keywords_with_displacement)
        }
    
    def classify_search_intent(self, serp_data: Dict) -> str:
        """
        Classify search intent based on SERP composition.
        
        Args:
            serp_data: Parsed SERP data
            
        Returns:
            Intent classification: informational, commercial, transactional, navigational
        """
        features = serp_data["serp_features"]
        keyword = serp_data["keyword"].lower()
        
        # Intent signals
        informational_signals = 0
        commercial_signals = 0
        transactional_signals = 0
        navigational_signals = 0
        
        # SERP feature signals
        if features["featured_snippet"]:
            informational_signals += 2
        
        if features["people_also_ask"]:
            informational_signals += len(features["people_also_ask"]) * 0.5
        
        if features["knowledge_panel"]:
            navigational_signals += 3
        
        if features["shopping_results"]:
            transactional_signals += 3
        
        if features["local_pack"]:
            transactional_signals += 2
        
        if features["video_carousel"]:
            informational_signals += 1
        
        if features["top_stories"]:
            informational_signals += 1
        
        # Keyword pattern signals
        informational_keywords = ["how", "what", "why", "when", "who", "guide", "tutorial", "learn"]
        commercial_keywords = ["best", "top", "review", "compare", "vs", "alternative"]
        transactional_keywords = ["buy", "price", "cheap", "deal", "discount", "order", "purchase"]
        navigational_keywords = ["login", "sign in", "official", "website"]
        
        for word in informational_keywords:
            if word in keyword:
                informational_signals += 1
        
        for word in commercial_keywords:
            if word in keyword:
                commercial_signals += 2
        
        for word in transactional_keywords:
            if word in keyword:
                transactional_signals += 2
        
        for word in navigational_keywords:
            if word in keyword:
                navigational_signals += 2
        
        # Determine dominant intent
        signals = {
            "informational": informational_signals,
            "commercial": commercial_signals,
            "transactional": transactional_signals,
            "navigational": navigational_signals
        }
        
        dominant_intent = max(signals, key=signals.get)
        
        # If commercial and transactional are close, classify as commercial
        if (signals["commercial"] > 0 and signals["transactional"] > 0 and
            abs(signals["commercial"] - signals["transactional"]) < 2):
            return "commercial"
        
        return dominant_intent
    
    def estimate_click_share(
        self,
        serp_data: Dict,
        user_domain: str,
        position: int
    ) -> Dict:
        """
        Estimate click share for a given position considering SERP features.
        
        Args:
            serp_data: Parsed SERP data
            user_domain: User's domain
            position: User's organic position
            
        Returns:
            Click share estimation
        """
        # Base CTR curves (desktop, no features)
        base_ctr_curve = {
            1: 0.2817, 2: 0.1488, 3: 0.1008, 4: 0.0731,
            5: 0.0597, 6: 0.0515, 7: 0.0460, 8: 0.0421,
            9: 0.0392, 10: 0.0370, 11: 0.0251, 12: 0.0229,
            13: 0.0212, 14: 0.0199, 15: 0.0188, 16: 0.0179,
            17: 0.0171, 18: 0.0165, 19: 0.0159, 20: 0.0154
        }
        
        # Get base CTR
        base_ctr = base_ctr_curve.get(position, 0.01)
        
        # Apply SERP feature modifiers
        features = serp_data["serp_features"]
        ctr_modifier = 1.0
        
        # Featured snippet above = -30% CTR
        if features["featured_snippet"] and position > 1:
            ctr_modifier *= 0.70
        
        # AI Overview = -20% CTR for top results
        if features["ai_overview"] and position <= 5:
            ctr_modifier *= 0.80
        
        # PAA = -5% per question above position
        if features["people_also_ask"]:
            paa_count = len(features["people_also_ask"])
            ctr_modifier *= (1 - (paa_count * 0.05))
        
        # Local pack above = -25% CTR
        if features["local_pack"] and position > 3:
            ctr_modifier *= 0.75
        
        # Shopping results = -15% CTR
        if features["shopping_results"]:
            ctr_modifier *= 0.85
        
        # Video carousel = -10% CTR
        if features["video_carousel"]:
            ctr_modifier *= 0.90
        
        adjusted_ctr = base_ctr * ctr_modifier
        
        # Calculate total available clicks (assuming 1000 searches/month as baseline)
        # In real implementation, this would use actual search volume data
        total_clicks_available = 1000
        
        # Calculate click share
        user_clicks = total_clicks_available * adjusted_ctr
        
        # Find user's result
        user_result = None
        for result in serp_data["organic_results"]:
            if user_domain in result["domain"]:
                user_result = result
                break
        
        return {
            "position": position,
            "base_ctr": base_ctr,
            "adjusted_ctr": adjusted_ctr,
            "ctr_modifier": ctr_modifier,
            "estimated_clicks": user_clicks,
            "visual_position": user_result["visual_position"] if user_result else position,
            "visual_displacement": user_result["visual_displacement"] if user_result else 0,
            "features_impacting": [
                feature for feature, data in features.items()
                if data is not None and data != [] and data != {}
            ]
        }
    
    def get_locations(self, country_code: str = "US") -> List[Dict]:
        """
        Get available location codes for a country.
        
        Args:
            country_code: ISO country code
            
        Returns:
            List of location data
        """
        endpoint = "/serp/google/locations"
        
        try:
            response = self._make_request(
                endpoint,
                method="GET",
                payload={"country_iso_code": country_code},
                use_cache=True
            )
            
            tasks = response.get("tasks", [])
            if not tasks:
                return []
            
            result = tasks[0].get("result", [])
            return result
            
        except Exception as e:
            logger.error(f"Failed to fetch locations: {str(e)}")
            return []
    
    def get_languages(self) -> List[Dict]:
        """
        Get available language codes.
        
        Returns:
            List of language data
        """
        endpoint = "/serp/google/languages"
        
        try:
            response = self._make_request(
                endpoint,
                method="GET",
                use_cache=True
            )
            
            tasks = response.get("tasks", [])
            if not tasks:
                return []
            
            result = tasks[0].get("result", [])
            return result
            
        except Exception as e:
            logger.error(f"Failed to fetch languages: {str(e)}")
            return []

