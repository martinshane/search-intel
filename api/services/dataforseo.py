import os
import time
import logging
from typing import Dict, List, Optional, Any, Union
from enum import Enum
from dataclasses import dataclass
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
    SERP_COMPETITORS = "/v3/dataforseo_labs/google/serp_competitors/live"


class RateLimitError(Exception):
    """Raised when rate limit is exceeded"""
    pass


class DataForSEOError(Exception):
    """Base exception for DataForSEO API errors"""
    pass


@dataclass
class SERPFeature:
    """Represents a SERP feature on the page"""
    type: str
    position: int
    title: Optional[str] = None
    description: Optional[str] = None
    url: Optional[str] = None
    rank_absolute: Optional[int] = None


@dataclass
class OrganicResult:
    """Represents an organic search result"""
    url: str
    domain: str
    title: str
    description: Optional[str]
    position: int
    rank_absolute: int
    is_featured_snippet: bool = False
    breadcrumb: Optional[str] = None


@dataclass
class SERPAnalysis:
    """Complete SERP analysis for a keyword"""
    keyword: str
    location_code: int
    language_code: str
    se_results_count: int
    organic_results: List[OrganicResult]
    serp_features: List[SERPFeature]
    people_also_ask: List[Dict[str, Any]]
    related_searches: List[str]
    visual_position_map: Dict[int, int]  # organic_position -> visual_position
    timestamp: datetime


class DataForSEOClient:
    """
    Complete DataForSEO API client for Search Intelligence Report.
    
    Provides methods for:
    - SERP data retrieval with full feature extraction
    - Competitor analysis across keyword sets
    - SERP feature displacement calculation
    - CTR opportunity scoring based on SERP composition
    
    Handles authentication, rate limiting, retries, and response parsing.
    """
    
    BASE_URL = "https://api.dataforseo.com"
    
    # Cost per request (approximate, in USD)
    COSTS = {
        "serp_live": 0.002,
        "serp_task": 0.002,
        "keywords_for_site": 0.01,
        "ranked_keywords": 0.01,
        "keyword_difficulty": 0.0025,
        "serp_competitors": 0.01,
    }
    
    # Visual position weights for SERP features
    FEATURE_WEIGHTS = {
        "featured_snippet": 2.0,
        "knowledge_panel": 1.5,
        "local_pack": 1.5,
        "people_also_ask": 0.5,  # per question
        "ai_overview": 2.5,
        "video_carousel": 1.0,
        "image_pack": 0.5,
        "shopping_results": 1.0,
        "top_stories": 1.0,
        "twitter": 0.5,
        "recipes": 1.0,
        "answer_box": 2.0,
    }
    
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
        session.headers.update({
            "Content-Type": "application/json"
        })
        
        return session
    
    def _rate_limit(self):
        """Enforce rate limiting between requests"""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - elapsed)
        self.last_request_time = time.time()
    
    def _make_request(
        self,
        endpoint: str,
        method: str = "POST",
        data: Optional[List[Dict]] = None,
        params: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Make authenticated request to DataForSEO API.
        
        Args:
            endpoint: API endpoint path
            method: HTTP method (GET or POST)
            data: Request payload (for POST)
            params: Query parameters (for GET)
            
        Returns:
            Parsed JSON response
            
        Raises:
            DataForSEOError: On API errors
            RateLimitError: On rate limit exceeded
        """
        self._rate_limit()
        
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
                retry_after = int(response.headers.get("Retry-After", 60))
                raise RateLimitError(f"Rate limit exceeded. Retry after {retry_after}s")
            
            response.raise_for_status()
            
            result = response.json()
            
            # Check for API-level errors
            if result.get("status_code") != 20000:
                error_msg = result.get("status_message", "Unknown error")
                raise DataForSEOError(f"API error: {error_msg}")
            
            self.request_count += 1
            
            return result
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise DataForSEOError(f"Request failed: {e}")
    
    def get_serp_data(
        self,
        keyword: str,
        location_code: int = 2840,  # US
        language_code: str = "en",
        device: str = "desktop",
        depth: int = 100
    ) -> SERPAnalysis:
        """
        Get live SERP data for a keyword with full feature extraction.
        
        Args:
            keyword: Search query
            location_code: DataForSEO location code
            language_code: Language code
            device: Device type (desktop, mobile)
            depth: Number of results to retrieve
            
        Returns:
            SERPAnalysis object with complete SERP data
        """
        payload = [{
            "keyword": keyword,
            "location_code": location_code,
            "language_code": language_code,
            "device": device,
            "depth": depth,
            "calculate_rectangles": True  # For visual position calculation
        }]
        
        result = self._make_request(
            DataForSEOEndpoint.SERP_LIVE.value,
            data=payload
        )
        
        self.estimated_cost += self.COSTS["serp_live"]
        
        return self._parse_serp_response(result, keyword, location_code, language_code)
    
    def get_batch_serp_data(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
        device: str = "desktop",
        depth: int = 100,
        batch_size: int = 100
    ) -> List[SERPAnalysis]:
        """
        Get SERP data for multiple keywords in batches.
        
        Args:
            keywords: List of search queries
            location_code: DataForSEO location code
            language_code: Language code
            device: Device type
            depth: Number of results per query
            batch_size: Max keywords per request
            
        Returns:
            List of SERPAnalysis objects
        """
        results = []
        
        for i in range(0, len(keywords), batch_size):
            batch = keywords[i:i + batch_size]
            
            payload = [
                {
                    "keyword": kw,
                    "location_code": location_code,
                    "language_code": language_code,
                    "device": device,
                    "depth": depth,
                    "calculate_rectangles": True
                }
                for kw in batch
            ]
            
            response = self._make_request(
                DataForSEOEndpoint.SERP_LIVE.value,
                data=payload
            )
            
            self.estimated_cost += self.COSTS["serp_live"] * len(batch)
            
            # Parse each task result
            for task in response.get("tasks", []):
                if task.get("status_code") == 20000:
                    for item in task.get("result", []):
                        kw = item.get("keyword", "")
                        serp = self._parse_serp_item(item, kw, location_code, language_code)
                        results.append(serp)
            
            # Rate limiting between batches
            if i + batch_size < len(keywords):
                time.sleep(1)
        
        return results
    
    def _parse_serp_response(
        self,
        response: Dict,
        keyword: str,
        location_code: int,
        language_code: str
    ) -> SERPAnalysis:
        """Parse API response into SERPAnalysis object"""
        tasks = response.get("tasks", [])
        if not tasks or tasks[0].get("status_code") != 20000:
            raise DataForSEOError(f"Failed to get SERP data for '{keyword}'")
        
        result = tasks[0].get("result", [{}])[0]
        return self._parse_serp_item(result, keyword, location_code, language_code)
    
    def _parse_serp_item(
        self,
        item: Dict,
        keyword: str,
        location_code: int,
        language_code: str
    ) -> SERPAnalysis:
        """Parse individual SERP result item"""
        # Extract organic results
        organic_results = []
        items = item.get("items", [])
        
        for result in items:
            result_type = result.get("type", "")
            
            if result_type == "organic":
                organic_results.append(OrganicResult(
                    url=result.get("url", ""),
                    domain=result.get("domain", ""),
                    title=result.get("title", ""),
                    description=result.get("description"),
                    position=result.get("rank_group", 0),
                    rank_absolute=result.get("rank_absolute", 0),
                    breadcrumb=result.get("breadcrumb")
                ))
            elif result_type == "featured_snippet":
                # Also add to organic if present
                organic_results.append(OrganicResult(
                    url=result.get("url", ""),
                    domain=result.get("domain", ""),
                    title=result.get("title", ""),
                    description=result.get("description"),
                    position=0,
                    rank_absolute=0,
                    is_featured_snippet=True
                ))
        
        # Extract SERP features
        serp_features = self._extract_serp_features(items)
        
        # Extract People Also Ask
        paa = self._extract_people_also_ask(items)
        
        # Extract Related Searches
        related = self._extract_related_searches(items)
        
        # Calculate visual positions
        visual_map = self._calculate_visual_positions(items, organic_results)
        
        return SERPAnalysis(
            keyword=keyword,
            location_code=location_code,
            language_code=language_code,
            se_results_count=item.get("se_results_count", 0),
            organic_results=organic_results,
            serp_features=serp_features,
            people_also_ask=paa,
            related_searches=related,
            visual_position_map=visual_map,
            timestamp=datetime.utcnow()
        )
    
    def _extract_serp_features(self, items: List[Dict]) -> List[SERPFeature]:
        """Extract all SERP features from items"""
        features = []
        
        for item in items:
            item_type = item.get("type", "")
            rank_abs = item.get("rank_absolute", 0)
            
            if item_type in [
                "featured_snippet", "knowledge_panel", "local_pack",
                "people_also_ask", "video", "images", "shopping",
                "top_stories", "twitter", "recipes", "answer_box",
                "ai_overview", "knowledge_graph"
            ]:
                features.append(SERPFeature(
                    type=item_type,
                    position=rank_abs,
                    title=item.get("title"),
                    description=item.get("description"),
                    url=item.get("url"),
                    rank_absolute=rank_abs
                ))
        
        return features
    
    def _extract_people_also_ask(self, items: List[Dict]) -> List[Dict[str, Any]]:
        """Extract People Also Ask questions"""
        paa = []
        
        for item in items:
            if item.get("type") == "people_also_ask":
                questions = item.get("items", [])
                for q in questions:
                    paa.append({
                        "question": q.get("title", ""),
                        "answer": q.get("description", ""),
                        "url": q.get("url", "")
                    })
        
        return paa
    
    def _extract_related_searches(self, items: List[Dict]) -> List[str]:
        """Extract related searches"""
        related = []
        
        for item in items:
            if item.get("type") == "related_searches":
                searches = item.get("items", [])
                for s in searches:
                    if "title" in s:
                        related.append(s["title"])
        
        return related
    
    def _calculate_visual_positions(
        self,
        items: List[Dict],
        organic_results: List[OrganicResult]
    ) -> Dict[int, int]:
        """
        Calculate visual position for each organic result.
        
        Visual position = organic position + weighted SERP features above it
        """
        visual_map = {}
        
        for organic in organic_results:
            if organic.is_featured_snippet:
                visual_map[0] = 1  # Featured snippet is always position 1
                continue
            
            visual_offset = 0.0
            
            # Count features appearing above this organic result
            for item in items:
                item_type = item.get("type", "")
                rank_abs = item.get("rank_absolute", 0)
                
                if rank_abs < organic.rank_absolute:
                    if item_type in self.FEATURE_WEIGHTS:
                        weight = self.FEATURE_WEIGHTS[item_type]
                        
                        # Special handling for PAA (count questions)
                        if item_type == "people_also_ask":
                            num_questions = len(item.get("items", []))
                            visual_offset += weight * num_questions
                        else:
                            visual_offset += weight
            
            visual_position = organic.position + int(visual_offset)
            visual_map[organic.position] = visual_position
        
        return visual_map
    
    def get_competitor_analysis(
        self,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en"
    ) -> Dict[str, Any]:
        """
        Analyze competitors across a set of keywords.
        
        Args:
            keywords: List of keywords to analyze
            location_code: Location code
            language_code: Language code
            
        Returns:
            Dict with competitor frequency, average positions, and overlap metrics
        """
        serp_analyses = self.get_batch_serp_data(
            keywords,
            location_code=location_code,
            language_code=language_code,
            depth=10  # Top 10 only for competitor analysis
        )
        
        # Build competitor frequency map
        competitor_map = {}
        
        for serp in serp_analyses:
            for result in serp.organic_results:
                domain = result.domain
                
                if domain not in competitor_map:
                    competitor_map[domain] = {
                        "domain": domain,
                        "keywords_shared": 0,
                        "positions": [],
                        "urls": set()
                    }
                
                competitor_map[domain]["keywords_shared"] += 1
                competitor_map[domain]["positions"].append(result.position)
                competitor_map[domain]["urls"].add(result.url)
        
        # Calculate metrics
        competitors = []
        total_keywords = len(keywords)
        
        for domain, data in competitor_map.items():
            avg_position = sum(data["positions"]) / len(data["positions"])
            frequency = data["keywords_shared"] / total_keywords
            
            # Classify threat level
            if frequency > 0.3 and avg_position < 5:
                threat = "high"
            elif frequency > 0.2 and avg_position < 10:
                threat = "medium"
            else:
                threat = "low"
            
            competitors.append({
                "domain": domain,
                "keywords_shared": data["keywords_shared"],
                "keyword_frequency": round(frequency, 3),
                "avg_position": round(avg_position, 2),
                "threat_level": threat,
                "unique_urls": len(data["urls"])
            })
        
        # Sort by frequency desc
        competitors.sort(key=lambda x: x["keywords_shared"], reverse=True)
        
        return {
            "total_keywords_analyzed": total_keywords,
            "unique_competitors": len(competitors),
            "competitors": competitors[:50],  # Top 50
            "primary_competitors": [
                c for c in competitors if c["keyword_frequency"] > 0.2
            ]
        }
    
    def calculate_serp_displacement(
        self,
        serp_analysis: SERPAnalysis,
        user_domain: str
    ) -> Dict[str, Any]:
        """
        Calculate SERP feature displacement for a user's ranking.
        
        Args:
            serp_analysis: SERP analysis object
            user_domain: User's domain to find in results
            
        Returns:
            Dict with displacement metrics
        """
        user_result = None
        
        for result in serp_analysis.organic_results:
            if user_domain in result.domain:
                user_result = result
                break
        
        if not user_result:
            return {
                "ranking": False,
                "keyword": serp_analysis.keyword
            }
        
        organic_position = user_result.position
        visual_position = serp_analysis.visual_position_map.get(organic_position, organic_position)
        displacement = visual_position - organic_position
        
        # Find features above user's result
        features_above = []
        for feature in serp_analysis.serp_features:
            if feature.rank_absolute < user_result.rank_absolute:
                features_above.append(feature.type)
        
        # Estimate CTR impact
        base_ctr = self._get_base_ctr(organic_position)
        displaced_ctr = self._get_base_ctr(visual_position)
        ctr_impact = displaced_ctr - base_ctr
        
        return {
            "ranking": True,
            "keyword": serp_analysis.keyword,
            "organic_position": organic_position,
            "visual_position": visual_position,
            "displacement": displacement,
            "features_above": features_above,
            "num_features_above": len(features_above),
            "estimated_ctr_base": round(base_ctr, 4),
            "estimated_ctr_displaced": round(displaced_ctr, 4),
            "estimated_ctr_impact": round(ctr_impact, 4),
            "people_also_ask_count": len(serp_analysis.people_also_ask)
        }
    
    def _get_base_ctr(self, position: int) -> float:
        """
        Get base CTR for a position using industry average curve.
        
        Based on Advanced Web Ranking CTR study data.
        """
        ctr_curve = {
            1: 0.3945,
            2: 0.1517,
            3: 0.1008,
            4: 0.0731,
            5: 0.0577,
            6: 0.0444,
            7: 0.0375,
            8: 0.0324,
            9: 0.0290,
            10: 0.0251,
        }
        
        if position <= 10:
            return ctr_curve.get(position, 0.025)
        elif position <= 20:
            return 0.015
        else:
            return 0.005
    
    def calculate_ctr_opportunity(
        self,
        serp_analyses: List[SERPAnalysis],
        user_domain: str,
        gsc_impressions: Optional[Dict[str, int]] = None
    ) -> Dict[str, Any]:
        """
        Calculate CTR opportunity across all keywords.
        
        Args:
            serp_analyses: List of SERP analyses
            user_domain: User's domain
            gsc_impressions: Optional dict mapping keyword -> monthly impressions
            
        Returns:
            Opportunity analysis with prioritized keywords
        """
        opportunities = []
        
        for serp in serp_analyses:
            displacement = self.calculate_serp_displacement(serp, user_domain)
            
            if not displacement["ranking"]:
                continue
            
            keyword = serp.keyword
            impressions = gsc_impressions.get(keyword, 0) if gsc_impressions else 0
            
            # Calculate potential click gain
            ctr_gain = abs(displacement["estimated_ctr_impact"])
            potential_clicks = impressions * ctr_gain if impressions > 0 else 0
            
            # Determine if fixable (CTR issue vs position issue)
            fixable = displacement["displacement"] > 2
            
            opportunities.append({
                "keyword": keyword,
                "organic_position": displacement["organic_position"],
                "visual_position": displacement["visual_position"],
                "displacement": displacement["displacement"],
                "features_above": displacement["features_above"],
                "monthly_impressions": impressions,
                "current_ctr_estimate": displacement["estimated_ctr_displaced"],
                "potential_ctr": displacement["estimated_ctr_base"],
                "ctr_gain_potential": ctr_gain,
                "estimated_click_gain": int(potential_clicks),
                "fixable_via_optimization": fixable,
                "priority_score": potential_clicks * (2 if fixable else 1)
            })
        
        # Sort by priority score
        opportunities.sort(key=lambda x: x["priority_score"], reverse=True)
        
        # Calculate totals
        total_click_opportunity = sum(o["estimated_click_gain"] for o in opportunities)
        fixable_opportunity = sum(
            o["estimated_click_gain"] for o in opportunities 
            if o["fixable_via_optimization"]
        )
        
        return {
            "total_keywords_analyzed": len(serp_analyses),
            "keywords_ranking": len(opportunities),
            "total_monthly_click_opportunity": total_click_opportunity,
            "fixable_monthly_click_opportunity": fixable_opportunity,
            "opportunities": opportunities,
            "top_opportunities": opportunities[:20]
        }
    
    def get_ranked_keywords(
        self,
        target_domain: str,
        location_code: int = 2840,
        language_code: str = "en",
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """
        Get all ranked keywords for a domain via DataForSEO Labs.
        
        Args:
            target_domain: Domain to analyze
            location_code: Location code
            language_code: Language code
            limit: Max keywords to retrieve
            
        Returns:
            List of ranked keywords with metrics
        """
        payload = [{
            "target": target_domain,
            "location_code": location_code,
            "language_code": language_code,
            "limit": limit,
            "filters": [
                ["ranked_serp_element.serp_item.rank_group", "<=", 20]
            ]
        }]
        
        result = self._make_request(
            DataForSEOEndpoint.RANKED_KEYWORDS.value,
            data=payload
        )
        
        self.estimated_cost += self.COSTS["ranked_keywords"]
        
        keywords = []
        
        for task in result.get("tasks", []):
            if task.get("status_code") == 20000:
                for item in task.get("result", [{}])[0].get("items", []):
                    se_item = item.get("ranked_serp_element", {}).get("serp_item", {})
                    
                    keywords.append({
                        "keyword": item.get("keyword", ""),
                        "position": se_item.get("rank_group", 0),
                        "rank_absolute": se_item.get("rank_absolute", 0),
                        "search_volume": item.get("keyword_data", {}).get("keyword_info", {}).get("search_volume", 0),
                        "competition": item.get("keyword_data", {}).get("keyword_info", {}).get("competition", 0),
                        "cpc": item.get("keyword_data", {}).get("keyword_info", {}).get("cpc", 0),
                        "url": se_item.get("url", "")
                    })
        
        return keywords
    
    def get_usage_stats(self) -> Dict[str, Any]:
        """Get current session usage statistics"""
        return {
            "total_requests": self.request_count,
            "estimated_cost_usd": round(self.estimated_cost, 4)
        }
    
    def classify_serp_intent(self, serp_analysis: SERPAnalysis) -> str:
        """
        Classify SERP intent based on feature composition.
        
        Returns: informational, commercial, navigational, or transactional
        """
        features = [f.type for f in serp_analysis.serp_features]
        paa_count = len(serp_analysis.people_also_ask)
        
        # Scoring for each intent
        scores = {
            "informational": 0,
            "commercial": 0,
            "navigational": 0,
            "transactional": 0
        }
        
        # Informational signals
        if paa_count >= 3:
            scores["informational"] += 2
        if "knowledge_panel" in features or "knowledge_graph" in features:
            scores["informational"] += 2
        if "featured_snippet" in features:
            scores["informational"] += 1
        if "answer_box" in features:
            scores["informational"] += 2
        
        # Commercial signals
        if "shopping" in features or "shopping_results" in features:
            scores["commercial"] += 3
            scores["transactional"] += 1
        if "video" in features:
            scores["commercial"] += 1
        
        # Navigational signals
        if "knowledge_panel" in features:
            scores["navigational"] += 1
        
        # Transactional signals
        if "shopping" in features:
            scores["transactional"] += 2
        if "local_pack" in features:
            scores["transactional"] += 2
        
        # Check for recipe/how-to patterns
        if "recipes" in features:
            scores["informational"] += 2
        
        # Return highest scoring intent
        return max(scores, key=scores.get)
    
    def batch_classify_intents(
        self,
        serp_analyses: List[SERPAnalysis]
    ) -> Dict[str, List[str]]:
        """
        Classify intent for multiple SERPs.
        
        Returns dict mapping intent -> list of keywords
        """
        intent_map = {
            "informational": [],
            "commercial": [],
            "navigational": [],
            "transactional": []
        }
        
        for serp in serp_analyses:
            intent = self.classify_serp_intent(serp)
            intent_map[intent].append(serp.keyword)
        
        return intent_map
