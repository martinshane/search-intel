"""
TypedDict definitions and Pydantic models for DataForSEO API responses.

Provides type safety for dataforseo_service.py and consuming modules.
Covers SERP results, SERP features, competitor analysis, and full response structures.
"""

from typing import Dict, List, Optional, Any, Literal
from typing_extensions import TypedDict, NotRequired
from pydantic import BaseModel, Field, validator
from datetime import datetime


# ============================================================================
# TypedDict Definitions (for flexible dictionary structures)
# ============================================================================

class SERPFeatureDict(TypedDict):
    """SERP feature representation as TypedDict"""
    type: str
    count: NotRequired[int]
    position: NotRequired[float]
    rank_group: NotRequired[int]
    rank_absolute: NotRequired[int]


class SERPResultDict(TypedDict):
    """Individual SERP result as TypedDict"""
    position: int
    url: str
    domain: str
    title: NotRequired[str]
    description: NotRequired[str]
    breadcrumb: NotRequired[str]
    rank_group: NotRequired[int]
    rank_absolute: NotRequired[int]
    serp_features: NotRequired[List[str]]
    is_featured_snippet: NotRequired[bool]
    is_paid: NotRequired[bool]
    is_image: NotRequired[bool]
    is_video: NotRequired[bool]
    timestamp: NotRequired[str]


class CompetitorDomainDict(TypedDict):
    """Competitor domain analysis as TypedDict"""
    domain: str
    visibility_score: float
    common_keywords: int
    avg_position: NotRequired[float]
    total_appearances: NotRequired[int]
    threat_level: NotRequired[str]


class SERPResponseDict(TypedDict):
    """Complete SERP response as TypedDict"""
    keyword: str
    location_code: NotRequired[int]
    language_code: NotRequired[str]
    device: NotRequired[str]
    results: List[SERPResultDict]
    features: List[SERPFeatureDict]
    total_results: NotRequired[int]
    total_count: NotRequired[int]
    updated_at: str


# ============================================================================
# Pydantic Models (for validation and serialization)
# ============================================================================

class SERPFeature(BaseModel):
    """
    Represents a SERP feature (featured snippet, PAA, etc.)
    
    Attributes:
        type: Feature type (featured_snippet, people_also_ask, video_carousel, etc.)
        count: Number of items in the feature (e.g., number of PAA questions)
        position: Visual position in SERP (float to account for partial positions)
        rank_group: Position group (1-10, 11-20, etc.)
        rank_absolute: Absolute position in SERP
    """
    type: str = Field(..., description="SERP feature type")
    count: Optional[int] = Field(None, description="Number of items in feature")
    position: Optional[float] = Field(None, description="Visual position in SERP")
    rank_group: Optional[int] = Field(None, description="Position group")
    rank_absolute: Optional[int] = Field(None, description="Absolute SERP position")
    
    class Config:
        json_schema_extra = {
            "example": {
                "type": "people_also_ask",
                "count": 4,
                "position": 3.5,
                "rank_group": 1,
                "rank_absolute": 4
            }
        }


class SERPResult(BaseModel):
    """
    Individual organic search result from SERP
    
    Attributes:
        position: Organic ranking position (1-indexed)
        url: Full URL of the result
        domain: Domain name extracted from URL
        title: Page title as shown in SERP
        description: Meta description or snippet
        breadcrumb: Breadcrumb navigation shown in SERP
        rank_group: Position group (1-10, 11-20, etc.)
        rank_absolute: Absolute position including ads and features
        serp_features: List of SERP features this result participates in
        is_featured_snippet: Whether this result is in featured snippet
        is_paid: Whether this is a paid result
        is_image: Whether this is from image pack
        is_video: Whether this is from video results
        timestamp: When this result was captured
    """
    position: int = Field(..., ge=1, description="Organic ranking position")
    url: str = Field(..., description="Result URL")
    domain: str = Field(..., description="Domain name")
    title: Optional[str] = Field(None, description="Page title")
    description: Optional[str] = Field(None, description="Meta description/snippet")
    breadcrumb: Optional[str] = Field(None, description="Breadcrumb navigation")
    rank_group: Optional[int] = Field(None, description="Position group")
    rank_absolute: Optional[int] = Field(None, description="Absolute position")
    serp_features: Optional[List[str]] = Field(default_factory=list, description="SERP features")
    is_featured_snippet: bool = Field(default=False, description="Is featured snippet")
    is_paid: bool = Field(default=False, description="Is paid result")
    is_image: bool = Field(default=False, description="Is image result")
    is_video: bool = Field(default=False, description="Is video result")
    timestamp: Optional[str] = Field(None, description="Capture timestamp")
    
    @validator('domain', pre=True, always=True)
    def extract_domain(cls, v, values):
        """Extract domain from URL if not provided"""
        if v:
            return v
        if 'url' in values:
            from urllib.parse import urlparse
            parsed = urlparse(values['url'])
            return parsed.netloc.replace('www.', '')
        return ''
    
    class Config:
        json_schema_extra = {
            "example": {
                "position": 3,
                "url": "https://example.com/page",
                "domain": "example.com",
                "title": "Example Page Title",
                "description": "This is an example meta description",
                "rank_absolute": 7,
                "serp_features": ["sitelinks"],
                "is_featured_snippet": False
            }
        }


class CompetitorDomain(BaseModel):
    """
    Competitor domain analysis
    
    Attributes:
        domain: Competitor domain name
        visibility_score: Calculated visibility score (0-100)
        common_keywords: Number of keywords where this competitor appears
        avg_position: Average position across shared keywords
        total_appearances: Total number of SERP appearances
        threat_level: Assessed threat level (low/medium/high/critical)
    """
    domain: str = Field(..., description="Competitor domain")
    visibility_score: float = Field(..., ge=0, le=100, description="Visibility score 0-100")
    common_keywords: int = Field(..., ge=0, description="Number of shared keywords")
    avg_position: Optional[float] = Field(None, ge=1, description="Average position")
    total_appearances: Optional[int] = Field(None, ge=0, description="Total appearances")
    threat_level: Optional[Literal["low", "medium", "high", "critical"]] = Field(
        None, description="Threat assessment"
    )
    
    @validator('threat_level', pre=True, always=True)
    def calculate_threat_level(cls, v, values):
        """Auto-calculate threat level if not provided"""
        if v:
            return v
        
        if 'visibility_score' in values and 'common_keywords' in values:
            score = values['visibility_score']
            keywords = values['common_keywords']
            
            # Threat level based on visibility and keyword overlap
            if score > 70 and keywords > 50:
                return "critical"
            elif score > 50 and keywords > 30:
                return "high"
            elif score > 30 and keywords > 10:
                return "medium"
            else:
                return "low"
        
        return None
    
    class Config:
        json_schema_extra = {
            "example": {
                "domain": "competitor.com",
                "visibility_score": 67.5,
                "common_keywords": 34,
                "avg_position": 4.2,
                "total_appearances": 89,
                "threat_level": "high"
            }
        }


class SERPResponse(BaseModel):
    """
    Complete SERP response for a single keyword
    
    Attributes:
        keyword: Search query/keyword
        location_code: DataForSEO location code
        language_code: Language code (e.g., 'en')
        device: Device type (desktop/mobile)
        results: List of organic search results
        features: List of SERP features present
        total_results: Total number of results found
        total_count: Total count from search engine
        updated_at: Timestamp of when data was captured
    """
    keyword: str = Field(..., description="Search keyword")
    location_code: Optional[int] = Field(None, description="DataForSEO location code")
    language_code: Optional[str] = Field(None, description="Language code")
    device: Optional[Literal["desktop", "mobile"]] = Field(
        default="desktop", description="Device type"
    )
    results: List[SERPResult] = Field(default_factory=list, description="Organic results")
    features: List[SERPFeature] = Field(default_factory=list, description="SERP features")
    total_results: Optional[int] = Field(None, ge=0, description="Total results count")
    total_count: Optional[int] = Field(None, ge=0, description="Search engine total count")
    updated_at: str = Field(
        default_factory=lambda: datetime.utcnow().isoformat(),
        description="Capture timestamp"
    )
    
    @validator('updated_at', pre=True, always=True)
    def ensure_timestamp(cls, v):
        """Ensure updated_at is a valid ISO timestamp"""
        if isinstance(v, datetime):
            return v.isoformat()
        if isinstance(v, str):
            # Validate it's a valid ISO format
            try:
                datetime.fromisoformat(v.replace('Z', '+00:00'))
                return v
            except ValueError:
                return datetime.utcnow().isoformat()
        return datetime.utcnow().isoformat()
    
    def get_top_domains(self, limit: int = 10) -> List[str]:
        """Extract top N domains from results"""
        return [r.domain for r in self.results[:limit]]
    
    def get_user_position(self, user_domain: str) -> Optional[int]:
        """Find user's position for their domain"""
        for result in self.results:
            if user_domain.lower() in result.domain.lower():
                return result.position
        return None
    
    def get_visual_position(self, organic_position: int) -> float:
        """
        Calculate visual position accounting for SERP features
        
        Args:
            organic_position: Organic ranking position
            
        Returns:
            Adjusted visual position (float)
        """
        displacement = 0.0
        
        for feature in self.features:
            if feature.position and feature.position < organic_position:
                # Different features displace different amounts
                if feature.type == "featured_snippet":
                    displacement += 2.0
                elif feature.type == "people_also_ask":
                    # Each PAA question = 0.5 positions
                    displacement += (feature.count or 4) * 0.5
                elif feature.type in ["local_pack", "local_results"]:
                    displacement += 3.0
                elif feature.type in ["video_carousel", "video"]:
                    displacement += 1.5
                elif feature.type in ["image_pack", "images"]:
                    displacement += 1.0
                elif feature.type in ["shopping_results", "shopping"]:
                    displacement += 2.0
                elif feature.type == "knowledge_panel":
                    displacement += 3.0
                elif feature.type == "ai_overview":
                    displacement += 2.5
                elif feature.type in ["top_stories", "news"]:
                    displacement += 1.5
                else:
                    displacement += 0.5
        
        return organic_position + displacement
    
    def count_feature_type(self, feature_type: str) -> int:
        """Count occurrences of a specific feature type"""
        return sum(1 for f in self.features if f.type == feature_type)
    
    def has_feature(self, feature_type: str) -> bool:
        """Check if SERP has a specific feature type"""
        return any(f.type == feature_type for f in self.features)
    
    class Config:
        json_schema_extra = {
            "example": {
                "keyword": "best crm software",
                "location_code": 2840,
                "language_code": "en",
                "device": "desktop",
                "results": [
                    {
                        "position": 1,
                        "url": "https://example.com/crm",
                        "domain": "example.com",
                        "title": "Best CRM Software 2024",
                        "description": "Compare top CRM solutions"
                    }
                ],
                "features": [
                    {
                        "type": "people_also_ask",
                        "count": 4,
                        "position": 3.5
                    }
                ],
                "total_results": 147,
                "updated_at": "2024-01-15T10:30:00Z"
            }
        }


class BatchSERPResponse(BaseModel):
    """
    Batch response for multiple keywords
    
    Attributes:
        keywords: List of keywords requested
        responses: List of individual SERP responses
        success_count: Number of successful requests
        failed_keywords: List of keywords that failed
        batch_id: Unique identifier for this batch
        created_at: Batch creation timestamp
    """
    keywords: List[str] = Field(..., description="List of keywords")
    responses: List[SERPResponse] = Field(default_factory=list, description="SERP responses")
    success_count: int = Field(default=0, ge=0, description="Successful requests")
    failed_keywords: List[str] = Field(default_factory=list, description="Failed keywords")
    batch_id: Optional[str] = Field(None, description="Batch identifier")
    created_at: str = Field(
        default_factory=lambda: datetime.utcnow().isoformat(),
        description="Batch creation timestamp"
    )
    
    @validator('success_count', pre=True, always=True)
    def count_successes(cls, v, values):
        """Auto-count successful responses"""
        if 'responses' in values:
            return len(values['responses'])
        return v
    
    def get_response_for_keyword(self, keyword: str) -> Optional[SERPResponse]:
        """Get SERP response for a specific keyword"""
        for response in self.responses:
            if response.keyword.lower() == keyword.lower():
                return response
        return None
    
    def get_success_rate(self) -> float:
        """Calculate success rate as percentage"""
        total = len(self.keywords)
        if total == 0:
            return 0.0
        return (self.success_count / total) * 100
    
    class Config:
        json_schema_extra = {
            "example": {
                "keywords": ["best crm", "crm software"],
                "responses": [],
                "success_count": 2,
                "failed_keywords": [],
                "batch_id": "batch_20240115_001"
            }
        }


# ============================================================================
# DataForSEO API Response Models
# ============================================================================

class DataForSEOTaskInfo(BaseModel):
    """Task information from DataForSEO API"""
    id: str
    status_code: int
    status_message: str
    time: str
    cost: Optional[float] = None
    result_count: Optional[int] = None
    path: Optional[List[str]] = None
    data: Optional[Dict[str, Any]] = None
    result: Optional[List[Dict[str, Any]]] = None


class DataForSEOResponse(BaseModel):
    """Root response from DataForSEO API"""
    version: str
    status_code: int
    status_message: str
    time: str
    cost: float
    tasks_count: int
    tasks_error: int
    tasks: List[DataForSEOTaskInfo]
    
    def is_success(self) -> bool:
        """Check if API call was successful"""
        return self.status_code == 20000 and self.tasks_error == 0
    
    def get_first_result(self) -> Optional[Dict[str, Any]]:
        """Get first result from first task"""
        if self.tasks and self.tasks[0].result:
            return self.tasks[0].result[0] if self.tasks[0].result else None
        return None


# ============================================================================
# Analysis Results Models
# ============================================================================

class SERPIntentClassification(BaseModel):
    """
    SERP intent classification result
    
    Attributes:
        keyword: The keyword analyzed
        intent: Classified intent type
        confidence: Confidence score (0-1)
        signals: List of signals that influenced classification
    """
    keyword: str
    intent: Literal["informational", "commercial", "navigational", "transactional"]
    confidence: float = Field(..., ge=0, le=1)
    signals: List[str] = Field(default_factory=list)
    
    class Config:
        json_schema_extra = {
            "example": {
                "keyword": "best crm software",
                "intent": "commercial",
                "confidence": 0.85,
                "signals": ["shopping_results", "review_sites", "comparison_terms"]
            }
        }


class SERPDisplacement(BaseModel):
    """
    SERP feature displacement analysis
    
    Attributes:
        keyword: The keyword
        organic_position: Organic ranking position
        visual_position: Adjusted position accounting for features
        features_above: List of features appearing above organic result
        estimated_ctr_impact: Estimated CTR impact (negative value)
        displacement_score: Overall displacement severity (0-1)
    """
    keyword: str
    organic_position: int = Field(..., ge=1)
    visual_position: float = Field(..., ge=1)
    features_above: List[str] = Field(default_factory=list)
    estimated_ctr_impact: float = Field(..., le=0)
    displacement_score: float = Field(..., ge=0, le=1)
    
    @validator('displacement_score', pre=True, always=True)
    def calculate_displacement_score(cls, v, values):
        """Calculate displacement severity"""
        if v is not None:
            return v
        
        if 'organic_position' in values and 'visual_position' in values:
            organic = values['organic_position']
            visual = values['visual_position']
            
            # Displacement is more severe for higher organic positions
            displacement = visual - organic
            position_weight = max(0, 1 - (organic / 20))  # Higher weight for top positions
            
            score = min(1.0, (displacement * 0.1) * (1 + position_weight))
            return round(score, 3)
        
        return 0.0
    
    class Config:
        json_schema_extra = {
            "example": {
                "keyword": "best crm software",
                "organic_position": 3,
                "visual_position": 8.0,
                "features_above": ["featured_snippet", "paa_x4", "ai_overview"],
                "estimated_ctr_impact": -0.062,
                "displacement_score": 0.73
            }
        }


class CompetitorAnalysis(BaseModel):
    """
    Comprehensive competitor analysis
    
    Attributes:
        competitors: List of competitor domains
        primary_competitors: Top competitors (>20% keyword overlap)
        total_unique_domains: Total unique domains across all keywords
        avg_competitors_per_keyword: Average number of competitors per keyword
        market_concentration: Market concentration score (0-1, higher = more concentrated)
    """
    competitors: List[CompetitorDomain]
    primary_competitors: List[CompetitorDomain] = Field(default_factory=list)
    total_unique_domains: int = Field(..., ge=0)
    avg_competitors_per_keyword: float = Field(..., ge=0)
    market_concentration: float = Field(..., ge=0, le=1)
    
    @validator('primary_competitors', pre=True, always=True)
    def filter_primary_competitors(cls, v, values):
        """Extract primary competitors (>20% keyword overlap)"""
        if v:
            return v
        
        if 'competitors' in values:
            total_keywords = max(c.common_keywords for c in values['competitors']) if values['competitors'] else 1
            threshold = total_keywords * 0.2
            return [c for c in values['competitors'] if c.common_keywords >= threshold]
        
        return []
    
    def get_top_competitors(self, n: int = 5) -> List[CompetitorDomain]:
        """Get top N competitors by visibility score"""
        return sorted(self.competitors, key=lambda x: x.visibility_score, reverse=True)[:n]
    
    def get_high_threat_competitors(self) -> List[CompetitorDomain]:
        """Get all high or critical threat competitors"""
        return [c for c in self.competitors if c.threat_level in ["high", "critical"]]
    
    class Config:
        json_schema_extra = {
            "example": {
                "competitors": [],
                "primary_competitors": [],
                "total_unique_domains": 87,
                "avg_competitors_per_keyword": 9.3,
                "market_concentration": 0.42
            }
        }


# ============================================================================
# Utility Functions
# ============================================================================

def parse_dataforseo_serp_result(raw_result: Dict[str, Any]) -> SERPResult:
    """
    Parse raw DataForSEO SERP result into SERPResult model
    
    Args:
        raw_result: Raw result dictionary from DataForSEO API
        
    Returns:
        Parsed SERPResult instance
    """
    return SERPResult(
        position=raw_result.get('rank_group', raw_result.get('position', 0)),
        url=raw_result.get('url', ''),
        domain=raw_result.get('domain', ''),
        title=raw_result.get('title'),
        description=raw_result.get('description'),
        breadcrumb=raw_result.get('breadcrumb'),
        rank_group=raw_result.get('rank_group'),
        rank_absolute=raw_result.get('rank_absolute'),
        serp_features=raw_result.get('serp_features', []),
        is_featured_snippet=raw_result.get('is_featured_snippet', False),
        is_paid=raw_result.get('type') == 'paid',
        is_image=raw_result.get('type') == 'images',
        is_video=raw_result.get('type') == 'video'
    )


def parse_dataforseo_serp_features(raw_items: List[Dict[str, Any]]) -> List[SERPFeature]:
    """
    Parse raw DataForSEO SERP items into SERPFeature list
    
    Args:
        raw_items: List of raw SERP items from DataForSEO API
        
    Returns:
        List of SERPFeature instances
    """
    features = []
    
    for item in raw_items:
        item_type = item.get('type', '')
        
        # Map DataForSEO types to our feature types
        feature_type_map = {
            'featured_snippet': 'featured_snippet',
            'people_also_ask': 'people_also_ask',
            'local_pack': 'local_pack',
            'knowledge_graph': 'knowledge_panel',
            'video': 'video_carousel',
            'images': 'image_pack',
            'shopping': 'shopping_results',
            'top_stories': 'top_stories',
            'ai_overview': 'ai_overview',
            'recipes': 'recipes',
            'hotels': 'hotels_pack',
            'flights': 'flights',
            'events': 'events'
        }
        
        feature_type = feature_type_map.get(item_type, item_type)
        
        # Count items in the feature
        count = None
        if item_type == 'people_also_ask':
            count = len(item.get('items', []))
        elif 'items' in item:
            count = len(item['items'])
        
        features.append(SERPFeature(
            type=feature_type,
            count=count,
            position=item.get('rank_group'),
            rank_group=item.get('rank_group'),
            rank_absolute=item.get('rank_absolute')
        ))
    
    return features


def create_serp_response_from_dataforseo(
    keyword: str,
    raw_response: Dict[str, Any],
    device: str = "desktop"
) -> SERPResponse:
    """
    Create SERPResponse from raw DataForSEO API response
    
    Args:
        keyword: The search keyword
        raw_response: Raw response from DataForSEO API
        device: Device type (desktop/mobile)
        
    Returns:
        Complete SERPResponse instance
    """
    items = raw_response.get('items', [])
    
    # Separate organic results from features
    organic_results = []
    feature_items = []
    
    for item in items:
        item_type = item.get('type', '')
        if item_type == 'organic':
            organic_results.append(parse_dataforseo_serp_result(item))
        else:
            feature_items.append(item)
    
    features = parse_dataforseo_serp_features(feature_items)
    
    return SERPResponse(
        keyword=keyword,
        location_code=raw_response.get('location_code'),
        language_code=raw_response.get('language_code'),
        device=device,
        results=organic_results,
        features=features,
        total_results=raw_response.get('total_count'),
        total_count=raw_response.get('total_count')
    )
