import os
import time
import logging
from typing import Dict, List, Optional, Any
from enum import Enum
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime, timedelta
import json

logger = logging.getLogger(__name__)


class DataForSEOEndpoint(Enum):
    """Available DataForSEO API endpoints"""
    SERP_LIVE = "/v3/serp/google/organic/live/advanced"
    SERP_TASK_POST = "/v3/serp/google/organic/task_post"
    SERP_TASK_GET = "/v3/serp/google/organic/task_get/advanced/{task_id}"
    KEYWORDS_FOR_SITE = "/v3/dataforseo_labs/google/keywords_for_site/live"
    RANKED_KEYWORDS = "/v3/dataforseo_labs/google/ranked_keywords/live"
    KEYWORD_DIFFICULTY = "/v3/dataforseo_labs/google/bulk_keyword_difficulty/live"


class RateLimitError(Exception):
    """Raised when rate limit is exceeded"""
    pass


class DataForSEOError(Exception):
    """Base exception for DataForSEO API errors"""
    pass


class DataForSEOClient:
    """
    Client for DataForSEO API with authentication, retry logic, and rate limiting.
    
    Handles:
    - Authentication via environment variables
    - Automatic retries with exponential backoff
    - Rate limit management
    - Response parsing and normalization
    - Error handling
    """
    
    BASE_URL = "https://api.dataforseo.com"
    
    def __init__(
        self,
        login: Optional[str] = None,
        password: Optional[str] = None,
        max_retries: int = 3,
        timeout: int = 60,
        rate_limit_delay: float = 0.5
    ):
        """
        Initialize DataForSEO client.
        
        Args:
            login: API login (defaults to DATAFORSEO_LOGIN env var)
            password: API password (defaults to DATAFORSEO_PASSWORD env var)
            max_retries: Maximum number of retry attempts
            timeout: Request timeout in seconds
            rate_limit_delay: Delay between requests in seconds
        """
        self.login = login or os.getenv("DATAFORSEO_LOGIN")
        self.password = password or os.getenv("DATAFORSEO_PASSWORD")
        
        if not self.login or not self.password:
            raise ValueError(
                "DataForSEO credentials not found. Set DATAFORSEO_LOGIN and "
                "DATAFORSEO_PASSWORD environment variables."
            )
        
        self.max_retries = max_retries
        self.timeout = timeout
        self.rate_limit_delay = rate_limit_delay
        self.last_request_time = 0
        
        # Configure session with retry strategy
        self.session = self._create_session()
        
        # Track usage for cost estimation
        self.request_count = 0
        self.estimated_cost = 0.0
    
    def _create_session(self) -> requests.Session:
        """Create requests session with retry configuration"""
        session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=self.max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Set authentication
        session.auth = (self.login, self.password)
        
        # Set headers
        session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json"
        })
        
        return session
    
    def _enforce_rate_limit(self):
        """Enforce rate limit between requests"""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        
        if time_since_last_request < self.rate_limit_delay:
            sleep_time = self.rate_limit_delay - time_since_last_request
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def _make_request(
        self,
        endpoint: str,
        method: str = "POST",
        data: Optional[List[Dict]] = None,
        params: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Make HTTP request to DataForSEO API.
        
        Args:
            endpoint: API endpoint path
            method: HTTP method (GET or POST)
            data: Request payload for POST requests
            params: Query parameters for GET requests
            
        Returns:
            Parsed JSON response
            
        Raises:
            DataForSEOError: On API errors
            RateLimitError: On rate limit exceeded
        """
        self._enforce_rate_limit()
        
        url = f"{self.BASE_URL}{endpoint}"
        
        try:
            if method == "POST":
                response = self.session.post(
                    url,
                    json=data,
                    timeout=self.timeout
                )
            else:
                response = self.session.get(
                    url,
                    params=params,
                    timeout=self.timeout
                )
            
            # Handle rate limiting
            if response.status_code == 429:
                raise RateLimitError("Rate limit exceeded")
            
            # Raise for other HTTP errors
            response.raise_for_status()
            
            # Parse response
            result = response.json()
            
            # Increment counters
            self.request_count += 1
            
            # Check for API-level errors
            if result.get("status_code") != 20000:
                error_message = result.get("status_message", "Unknown error")
                raise DataForSEOError(f"API error: {error_message}")
            
            return result
            
        except requests.exceptions.Timeout:
            logger.error(f"Request timeout for {endpoint}")
            raise DataForSEOError(f"Request timeout: {endpoint}")
        
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {endpoint}: {str(e)}")
            raise DataForSEOError(f"Request failed: {str(e)}")
    
    def get_live_serp(
        self,
        keyword: str,
        location_code: int = 2840,  # United States
        language_code: str = "en",
        device: str = "desktop",
        depth: int = 100
    ) -> Dict[str, Any]:
        """
        Get live SERP results for a keyword.
        
        Args:
            keyword: Search query
            location_code: Location code (2840 = United States)
            language_code: Language code
            device: Device type (desktop, mobile, tablet)
            depth: Number of results to return
            
        Returns:
            Normalized SERP data with organic results and features
        """
        endpoint = DataForSEOEndpoint.SERP_LIVE.value
        
        payload = [{
            "keyword": keyword,
            "location_code": location_code,
            "language_code": language_code,
            "device": device,
            "depth": depth,
            "calculate_rectangles": True
        }]
        
        response = self._make_request(endpoint, data=payload)
        
        # Estimate cost (approximately $0.002 per request)
        self.estimated_cost += 0.002
        
        return self._parse_serp_response(response, keyword)
    
    def get_bulk_serp(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
        device: str = "desktop",
        depth: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get live SERP results for multiple keywords.
        
        Args:
            keywords: List of search queries
            location_code: Location code
            language_code: Language code
            device: Device type
            depth: Number of results to return
            
        Returns:
            List of normalized SERP data for each keyword
        """
        results = []
        
        for keyword in keywords:
            try:
                result = self.get_live_serp(
                    keyword=keyword,
                    location_code=location_code,
                    language_code=language_code,
                    device=device,
                    depth=depth
                )
                results.append(result)
                
            except Exception as e:
                logger.error(f"Failed to fetch SERP for '{keyword}': {str(e)}")
                results.append({
                    "keyword": keyword,
                    "error": str(e),
                    "organic_results": [],
                    "serp_features": []
                })
        
        return results
    
    def get_keyword_difficulty(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en"
    ) -> List[Dict[str, Any]]:
        """
        Get keyword difficulty metrics for multiple keywords.
        
        Args:
            keywords: List of keywords to analyze
            location_code: Location code
            language_code: Language code
            
        Returns:
            List of keyword difficulty data
        """
        endpoint = DataForSEOEndpoint.KEYWORD_DIFFICULTY.value
        
        payload = [{
            "keywords": keywords,
            "location_code": location_code,
            "language_code": language_code
        }]
        
        response = self._make_request(endpoint, data=payload)
        
        # Estimate cost (approximately $0.01 per 100 keywords)
        self.estimated_cost += (len(keywords) / 100) * 0.01
        
        return self._parse_keyword_difficulty_response(response)
    
    def get_ranked_keywords(
        self,
        target_domain: str,
        location_code: int = 2840,
        language_code: str = "en",
        limit: int = 1000,
        offset: int = 0,
        filters: Optional[List[Any]] = None
    ) -> Dict[str, Any]:
        """
        Get keywords that a domain ranks for.
        
        Args:
            target_domain: Domain to analyze
            location_code: Location code
            language_code: Language code
            limit: Maximum number of keywords to return
            offset: Result offset for pagination
            filters: Optional filters array
            
        Returns:
            Ranked keywords data
        """
        endpoint = DataForSEOEndpoint.RANKED_KEYWORDS.value
        
        payload = [{
            "target": target_domain,
            "location_code": location_code,
            "language_code": language_code,
            "limit": limit,
            "offset": offset
        }]
        
        if filters:
            payload[0]["filters"] = filters
        
        response = self._make_request(endpoint, data=payload)
        
        # Estimate cost (approximately $0.05 per request)
        self.estimated_cost += 0.05
        
        return self._parse_ranked_keywords_response(response)
    
    def _parse_serp_response(self, response: Dict, keyword: str) -> Dict[str, Any]:
        """
        Parse and normalize SERP API response.
        
        Args:
            response: Raw API response
            keyword: Search keyword
            
        Returns:
            Normalized SERP data structure
        """
        if not response.get("tasks") or not response["tasks"][0].get("result"):
            return {
                "keyword": keyword,
                "organic_results": [],
                "serp_features": [],
                "total_results": 0
            }
        
        task_result = response["tasks"][0]["result"][0]
        items = task_result.get("items", [])
        
        # Extract organic results
        organic_results = []
        serp_features = []
        
        for item in items:
            item_type = item.get("type", "")
            
            if item_type == "organic":
                organic_results.append({
                    "position": item.get("rank_absolute", 0),
                    "url": item.get("url", ""),
                    "domain": item.get("domain", ""),
                    "title": item.get("title", ""),
                    "description": item.get("description", ""),
                    "breadcrumb": item.get("breadcrumb", ""),
                    "is_https": item.get("is_https", False),
                    "is_amp": item.get("is_amp", False),
                    "rating": self._extract_rating(item),
                    "highlighted": item.get("highlighted", [])
                })
            
            else:
                # Track SERP features
                serp_features.append({
                    "type": item_type,
                    "position": item.get("rank_absolute", 0),
                    "data": self._extract_feature_data(item, item_type)
                })
        
        return {
            "keyword": keyword,
            "organic_results": organic_results,
            "serp_features": serp_features,
            "total_results": task_result.get("se_results_count", 0),
            "timestamp": datetime.utcnow().isoformat()
        }
    
    def _extract_rating(self, item: Dict) -> Optional[Dict]:
        """Extract rating information from SERP item"""
        rating_data = item.get("rating")
        if not rating_data:
            return None
        
        return {
            "rating_type": rating_data.get("rating_type"),
            "value": rating_data.get("value"),
            "votes_count": rating_data.get("votes_count"),
            "rating_max": rating_data.get("rating_max")
        }
    
    def _extract_feature_data(self, item: Dict, feature_type: str) -> Dict:
        """Extract relevant data from SERP feature"""
        data = {
            "title": item.get("title", ""),
            "url": item.get("url", "")
        }
        
        # Feature-specific extraction
        if feature_type == "featured_snippet":
            data["description"] = item.get("description", "")
            data["table_data"] = item.get("table", {})
        
        elif feature_type == "people_also_ask":
            data["questions"] = [
                {
                    "question": q.get("title", ""),
                    "answer": q.get("expanded_element", [{}])[0].get("description", "")
                }
                for q in item.get("items", [])
            ]
        
        elif feature_type == "knowledge_graph":
            data["description"] = item.get("description", "")
            data["card_id"] = item.get("card_id", "")
        
        elif feature_type == "local_pack":
            data["locations"] = [
                {
                    "title": loc.get("title", ""),
                    "rating": loc.get("rating", {}).get("value"),
                    "address": loc.get("address", "")
                }
                for loc in item.get("items", [])
            ]
        
        elif feature_type == "video":
            data["videos"] = [
                {
                    "title": v.get("title", ""),
                    "source": v.get("source", ""),
                    "duration": v.get("duration", "")
                }
                for v in item.get("items", [])
            ]
        
        elif feature_type == "images":
            data["image_count"] = len(item.get("items", []))
        
        elif feature_type == "top_stories":
            data["stories"] = [
                {
                    "title": s.get("title", ""),
                    "source": s.get("source", ""),
                    "timestamp": s.get("timestamp", "")
                }
                for s in item.get("items", [])
            ]
        
        elif feature_type == "shopping":
            data["products"] = [
                {
                    "title": p.get("title", ""),
                    "price": p.get("price", {}),
                    "source": p.get("source", "")
                }
                for p in item.get("items", [])
            ]
        
        elif feature_type == "ai_overview":
            data["content"] = item.get("text", "")
            data["sources"] = [s.get("url", "") for s in item.get("links", [])]
        
        return data
    
    def _parse_keyword_difficulty_response(self, response: Dict) -> List[Dict[str, Any]]:
        """
        Parse keyword difficulty API response.
        
        Args:
            response: Raw API response
            
        Returns:
            List of keyword difficulty metrics
        """
        if not response.get("tasks") or not response["tasks"][0].get("result"):
            return []
        
        results = []
        items = response["tasks"][0]["result"][0].get("items", [])
        
        for item in items:
            results.append({
                "keyword": item.get("keyword", ""),
                "difficulty": item.get("keyword_difficulty", 0),
                "search_volume": item.get("search_volume", 0),
                "cpc": item.get("cpc", 0),
                "competition": item.get("competition", 0),
                "monthly_searches": item.get("monthly_searches", [])
            })
        
        return results
    
    def _parse_ranked_keywords_response(self, response: Dict) -> Dict[str, Any]:
        """
        Parse ranked keywords API response.
        
        Args:
            response: Raw API response
            
        Returns:
            Normalized ranked keywords data
        """
        if not response.get("tasks") or not response["tasks"][0].get("result"):
            return {
                "keywords": [],
                "total_count": 0,
                "metrics_summary": {}
            }
        
        task_result = response["tasks"][0]["result"][0]
        items = task_result.get("items", [])
        
        keywords = []
        for item in items:
            keyword_data = item.get("keyword_data", {})
            serp_info = item.get("serp_info", {})
            ranked_serp_element = item.get("ranked_serp_element", {})
            
            keywords.append({
                "keyword": keyword_data.get("keyword", ""),
                "search_volume": keyword_data.get("keyword_info", {}).get("search_volume", 0),
                "competition": keyword_data.get("keyword_info", {}).get("competition", 0),
                "cpc": keyword_data.get("keyword_info", {}).get("cpc", 0),
                "position": ranked_serp_element.get("serp_item", {}).get("rank_absolute", 0),
                "url": ranked_serp_element.get("serp_item", {}).get("url", ""),
                "etv": item.get("etv", 0),  # Estimated traffic value
                "impressions_etv": item.get("impressions_etv", 0),
                "estimated_paid_traffic_cost": item.get("estimated_paid_traffic_cost", 0)
            })
        
        return {
            "keywords": keywords,
            "total_count": task_result.get("total_count", 0),
            "metrics_summary": task_result.get("metrics", {}),
            "timestamp": datetime.utcnow().isoformat()
        }
    
    def calculate_visual_position(self, serp_data: Dict) -> Dict[str, Any]:
        """
        Calculate visual position considering SERP features.
        
        Args:
            serp_data: Normalized SERP data from get_live_serp()
            
        Returns:
            Enhanced organic results with visual position metrics
        """
        # SERP feature visual weight mapping
        feature_weights = {
            "featured_snippet": 2.0,
            "knowledge_graph": 1.5,
            "ai_overview": 2.5,
            "people_also_ask": 0.5,  # per question
            "local_pack": 1.5,
            "video": 1.0,
            "images": 0.5,
            "top_stories": 1.0,
            "shopping": 1.0,
            "twitter": 0.5,
            "related_searches": 0
        }
        
        # Sort features by position
        features = sorted(
            serp_data.get("serp_features", []),
            key=lambda x: x.get("position", 999)
        )
        
        # Calculate cumulative visual displacement
        enhanced_results = []
        
        for organic_result in serp_data.get("organic_results", []):
            organic_position = organic_result["position"]
            visual_displacement = 0
            
            # Count features above this organic result
            for feature in features:
                feature_position = feature.get("position", 999)
                
                if feature_position < organic_position:
                    feature_type = feature.get("type", "")
                    base_weight = feature_weights.get(feature_type, 0.5)
                    
                    # Adjust for multiple items (e.g., PAA questions)
                    if feature_type == "people_also_ask":
                        question_count = len(
                            feature.get("data", {}).get("questions", [])
                        )
                        visual_displacement += base_weight * question_count
                    else:
                        visual_displacement += base_weight
            
            visual_position = organic_position + visual_displacement
            
            enhanced_result = organic_result.copy()
            enhanced_result.update({
                "visual_position": round(visual_position, 1),
                "visual_displacement": round(visual_displacement, 1),
                "features_above": [
                    f["type"] for f in features
                    if f.get("position", 999) < organic_position
                ]
            })
            
            enhanced_results.append(enhanced_result)
        
        return {
            **serp_data,
            "organic_results": enhanced_results
        }
    
    def classify_serp_intent(self, serp_data: Dict) -> str:
        """
        Classify search intent based on SERP composition.
        
        Args:
            serp_data: Normalized SERP data
            
        Returns:
            Intent classification: informational, commercial, transactional, navigational
        """
        features = {f["type"] for f in serp_data.get("serp_features", [])}
        
        # Navigational signals
        if "knowledge_graph" in features and len(serp_data.get("organic_results", [])) > 0:
            first_result = serp_data["organic_results"][0]
            if first_result.get("domain", "") in serp_data["keyword"].lower():
                return "navigational"
        
        # Transactional signals
        transactional_features = {"shopping", "shopping_results"}
        if features & transactional_features:
            return "transactional"
        
        # Commercial signals
        commercial_features = {"local_pack"}
        if features & commercial_features:
            return "commercial"
        
        # Check for commercial intent in organic titles
        commercial_keywords = ["best", "top", "vs", "review", "compare", "price"]
        organic_titles = [
            r.get("title", "").lower()
            for r in serp_data.get("organic_results", [])[:5]
        ]
        commercial_count = sum(
            any(kw in title for kw in commercial_keywords)
            for title in organic_titles
        )
        if commercial_count >= 3:
            return "commercial"
        
        # Informational (default)
        informational_features = {
            "people_also_ask", "featured_snippet", "ai_overview"
        }
        if features & informational_features:
            return "informational"
        
        return "informational"
    
    def get_usage_stats(self) -> Dict[str, Any]:
        """
        Get current usage statistics.
        
        Returns:
            Dictionary with request count and estimated cost
        """
        return {
            "request_count": self.request_count,
            "estimated_cost_usd": round(self.estimated_cost, 4)
        }
    
    def reset_usage_stats(self):
        """Reset usage statistics counters"""
        self.request_count = 0
        self.estimated_cost = 0.0