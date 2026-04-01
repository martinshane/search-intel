"""
Pydantic models for SERP data structures.

This module defines the data models for SERP (Search Engine Results Page) data,
including individual results, SERP features, competitor metrics, and complete
SERP analysis results.
"""

from typing import List, Optional, Dict, Any, Literal
from datetime import datetime
from pydantic import BaseModel, Field, HttpUrl, field_validator, model_validator
from enum import Enum


class SERPFeatureType(str, Enum):
    """Types of SERP features that can appear in search results."""
    FEATURED_SNIPPET = "featured_snippet"
    PEOPLE_ALSO_ASK = "people_also_ask"
    VIDEO_CAROUSEL = "video_carousel"
    LOCAL_PACK = "local_pack"
    KNOWLEDGE_PANEL = "knowledge_panel"
    AI_OVERVIEW = "ai_overview"
    REDDIT_THREADS = "reddit_threads"
    IMAGE_PACK = "image_pack"
    SHOPPING_RESULTS = "shopping_results"
    TOP_STORIES = "top_stories"
    SITE_LINKS = "site_links"
    RELATED_SEARCHES = "related_searches"
    TWITTER_CAROUSEL = "twitter_carousel"
    REVIEWS = "reviews"
    JOBS = "jobs"
    RECIPES = "recipes"
    FAQ = "faq"
    OTHER = "other"


class SERPIntentType(str, Enum):
    """Classification of search intent based on SERP composition."""
    INFORMATIONAL = "informational"
    COMMERCIAL = "commercial"
    NAVIGATIONAL = "navigational"
    TRANSACTIONAL = "transactional"
    MIXED = "mixed"
    UNKNOWN = "unknown"


class SERPResult(BaseModel):
    """Individual organic search result from SERP."""
    
    position: int = Field(..., ge=1, le=100, description="Organic position in SERP")
    url: str = Field(..., description="Full URL of the result")
    title: str = Field(..., min_length=1, max_length=1000, description="Page title")
    snippet: Optional[str] = Field(None, max_length=5000, description="Meta description or snippet")
    domain: str = Field(..., description="Root domain of the URL")
    displayed_url: Optional[str] = Field(None, description="URL as displayed in SERP")
    cached_url: Optional[str] = Field(None, description="Cached version URL if available")
    
    # Rich result indicators
    has_site_links: bool = Field(default=False, description="Whether result has site links")
    has_rich_snippet: bool = Field(default=False, description="Whether result has rich snippet")
    schema_types: List[str] = Field(default_factory=list, description="Schema.org types detected")
    
    # Additional metadata
    breadcrumb: Optional[str] = Field(None, description="Breadcrumb path shown in SERP")
    date_published: Optional[datetime] = Field(None, description="Publication date if shown")
    author: Optional[str] = Field(None, description="Author if shown in SERP")
    rating: Optional[float] = Field(None, ge=0, le=5, description="Star rating if shown")
    review_count: Optional[int] = Field(None, ge=0, description="Number of reviews if shown")
    
    # Internal tracking
    is_user_site: bool = Field(default=False, description="Whether this result belongs to the analyzed site")
    visual_position: Optional[float] = Field(None, description="Adjusted position accounting for SERP features above")
    
    @field_validator('url', 'displayed_url', 'cached_url')
    @classmethod
    def validate_url(cls, v: Optional[str]) -> Optional[str]:
        """Ensure URLs are properly formatted."""
        if v is None:
            return v
        if not v.startswith(('http://', 'https://')):
            return f'https://{v}'
        return v
    
    @field_validator('domain')
    @classmethod
    def extract_domain(cls, v: str) -> str:
        """Ensure domain is clean root domain."""
        # Remove protocol if present
        domain = v.replace('http://', '').replace('https://', '')
        # Remove path if present
        domain = domain.split('/')[0]
        # Remove www if present
        domain = domain.replace('www.', '')
        return domain.lower()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary with non-null values."""
        return {k: v for k, v in self.model_dump().items() if v is not None}


class SERPFeature(BaseModel):
    """SERP feature (non-organic element) appearing in results."""
    
    type: SERPFeatureType = Field(..., description="Type of SERP feature")
    position: float = Field(..., ge=0, description="Vertical position in SERP (can be fractional)")
    title: Optional[str] = Field(None, description="Title/heading of the feature")
    
    # Feature-specific data stored as flexible dict
    data: Dict[str, Any] = Field(default_factory=dict, description="Feature-specific structured data")
    
    # Visual impact metrics
    estimated_height_px: Optional[int] = Field(None, ge=0, description="Estimated pixel height of feature")
    click_probability: Optional[float] = Field(None, ge=0, le=1, description="Estimated probability user clicks this feature")
    
    # Source/ownership
    source_domain: Optional[str] = Field(None, description="Domain that owns this feature (if applicable)")
    is_user_owned: bool = Field(default=False, description="Whether user's site owns this feature")
    
    @field_validator('position')
    @classmethod
    def validate_position(cls, v: float) -> float:
        """Ensure position is reasonable."""
        if v < 0:
            raise ValueError('Position must be non-negative')
        if v > 100:
            raise ValueError('Position seems unreasonably high')
        return v
    
    def get_visual_displacement(self) -> float:
        """
        Calculate how many organic positions this feature displaces.
        
        Returns:
            Number of position slots this feature occupies
        """
        displacement_map = {
            SERPFeatureType.FEATURED_SNIPPET: 2.0,
            SERPFeatureType.AI_OVERVIEW: 3.0,
            SERPFeatureType.LOCAL_PACK: 2.5,
            SERPFeatureType.KNOWLEDGE_PANEL: 0.0,  # Usually on side
            SERPFeatureType.PEOPLE_ALSO_ASK: 0.5 * self.data.get('question_count', 4),
            SERPFeatureType.VIDEO_CAROUSEL: 1.5,
            SERPFeatureType.IMAGE_PACK: 1.0,
            SERPFeatureType.SHOPPING_RESULTS: 2.0,
            SERPFeatureType.TOP_STORIES: 1.5,
            SERPFeatureType.REDDIT_THREADS: 1.0,
            SERPFeatureType.SITE_LINKS: 0.5,
            SERPFeatureType.RELATED_SEARCHES: 0.0,  # Bottom of page
        }
        return displacement_map.get(self.type, 0.5)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        result = self.model_dump()
        result['type'] = self.type.value
        return result


class CompetitorMetrics(BaseModel):
    """Metrics for a competitor domain across analyzed keywords."""
    
    domain: str = Field(..., description="Competitor domain")
    
    # Presence metrics
    keywords_appearing: int = Field(..., ge=0, description="Number of keywords where competitor appears in top 20")
    keywords_top10: int = Field(default=0, ge=0, description="Number of keywords where competitor is in top 10")
    keywords_top3: int = Field(default=0, ge=0, description="Number of keywords where competitor is in top 3")
    
    # Position metrics
    avg_position: float = Field(..., ge=1, le=100, description="Average position across all appearances")
    median_position: float = Field(..., ge=1, le=100, description="Median position across all appearances")
    best_position: int = Field(..., ge=1, le=100, description="Best position achieved")
    worst_position: int = Field(..., ge=1, le=100, description="Worst position achieved")
    
    # Advanced metrics
    visibility_score: float = Field(..., ge=0, le=1, description="Weighted visibility score (0-1)")
    serp_features_count: int = Field(default=0, ge=0, description="Number of SERP features owned")
    estimated_click_share: float = Field(default=0.0, ge=0, le=1, description="Estimated share of total clicks")
    
    # Overlap analysis
    overlap_percentage: float = Field(..., ge=0, le=1, description="% of user's keywords where this competitor appears")
    exclusive_keywords: int = Field(default=0, ge=0, description="Keywords where competitor appears but user doesn't")
    
    # Threat assessment
    threat_level: Literal["low", "medium", "high", "critical"] = Field(
        default="medium",
        description="Assessed threat level based on overlap and performance"
    )
    
    # Position trend (if historical data available)
    position_trend: Optional[Literal["improving", "stable", "declining"]] = Field(
        None,
        description="Position trend over time if available"
    )
    trend_slope: Optional[float] = Field(None, description="Position change per month (negative = improving)")
    
    @model_validator(mode='after')
    def validate_keyword_hierarchy(self) -> 'CompetitorMetrics':
        """Ensure keyword counts are hierarchical."""
        if self.keywords_top3 > self.keywords_top10:
            raise ValueError('keywords_top3 cannot exceed keywords_top10')
        if self.keywords_top10 > self.keywords_appearing:
            raise ValueError('keywords_top10 cannot exceed keywords_appearing')
        return self
    
    @field_validator('domain')
    @classmethod
    def clean_domain(cls, v: str) -> str:
        """Clean domain name."""
        domain = v.replace('http://', '').replace('https://', '')
        domain = domain.split('/')[0]
        domain = domain.replace('www.', '')
        return domain.lower()
    
    def calculate_threat_level(self) -> Literal["low", "medium", "high", "critical"]:
        """
        Calculate threat level based on metrics.
        
        Returns:
            Threat level classification
        """
        # High overlap + high visibility = critical threat
        if self.overlap_percentage > 0.5 and self.visibility_score > 0.6:
            return "critical"
        
        # High overlap or high visibility with good positions
        if self.overlap_percentage > 0.3 and self.avg_position < 5:
            return "high"
        
        if self.visibility_score > 0.4 or (self.overlap_percentage > 0.2 and self.keywords_top10 > 5):
            return "high"
        
        # Moderate presence
        if self.overlap_percentage > 0.15 or self.keywords_top10 > 3:
            return "medium"
        
        return "low"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return self.model_dump()


class SERPAnalysis(BaseModel):
    """Complete SERP analysis for a single keyword."""
    
    # Query metadata
    keyword: str = Field(..., min_length=1, description="Search keyword/query")
    location: str = Field(default="United States", description="Geographic location for SERP")
    language: str = Field(default="en", description="Language code")
    device: Literal["desktop", "mobile"] = Field(default="desktop", description="Device type")
    
    # Timestamp
    analyzed_at: datetime = Field(default_factory=datetime.utcnow, description="When this SERP was analyzed")
    
    # Core SERP data
    results: List[SERPResult] = Field(default_factory=list, description="Organic search results")
    features: List[SERPFeature] = Field(default_factory=list, description="SERP features present")
    
    # User's performance in this SERP
    user_domain: Optional[str] = Field(None, description="User's domain being analyzed")
    user_results: List[SERPResult] = Field(default_factory=list, description="User's results in this SERP")
    user_best_position: Optional[int] = Field(None, description="User's best organic position")
    user_visual_position: Optional[float] = Field(None, description="User's visual position after SERP features")
    user_owns_features: List[SERPFeature] = Field(default_factory=list, description="SERP features owned by user")
    
    # Competitor data
    competitors: List[CompetitorMetrics] = Field(default_factory=list, description="Competitor metrics in this SERP")
    total_unique_domains: int = Field(default=0, ge=0, description="Total unique domains in top 20")
    
    # SERP characteristics
    intent_classification: SERPIntentType = Field(default=SERPIntentType.UNKNOWN, description="Classified search intent")
    serp_volatility_score: Optional[float] = Field(None, ge=0, le=1, description="Estimated SERP volatility (0=stable, 1=very volatile)")
    
    # Click distribution estimates
    estimated_total_clicks: Optional[int] = Field(None, ge=0, description="Estimated total monthly clicks for this keyword")
    estimated_user_clicks: Optional[int] = Field(None, ge=0, description="Estimated monthly clicks user receives")
    estimated_user_click_share: Optional[float] = Field(None, ge=0, le=1, description="User's estimated click share")
    click_share_opportunity: Optional[float] = Field(None, ge=0, le=1, description="Potential additional click share available")
    
    # Displacement analysis
    total_visual_displacement: float = Field(default=0.0, ge=0, description="Total position displacement from SERP features")
    features_above_user: List[SERPFeature] = Field(default_factory=list, description="SERP features appearing above user's result")
    
    # Metadata
    data_source: str = Field(default="dataforseo", description="Source of SERP data")
    serp_url: Optional[str] = Field(None, description="URL to view actual SERP")
    
    @model_validator(mode='after')
    def extract_user_results(self) -> 'SERPAnalysis':
        """Extract user's results from all results."""
        if not self.user_domain:
            return self
        
        user_domain_clean = self.user_domain.replace('www.', '').lower()
        
        # Find user's organic results
        self.user_results = [
            r for r in self.results 
            if r.domain.replace('www.', '').lower() == user_domain_clean
        ]
        
        # Mark them as user's
        for result in self.user_results:
            result.is_user_site = True
        
        # Find best position
        if self.user_results:
            self.user_best_position = min(r.position for r in self.user_results)
            
            # Calculate visual position
            displacement_above = sum(
                f.get_visual_displacement() 
                for f in self.features 
                if f.position < self.user_best_position
            )
            self.user_visual_position = self.user_best_position + displacement_above
        
        # Find user's SERP features
        self.user_owns_features = [
            f for f in self.features
            if f.source_domain and f.source_domain.replace('www.', '').lower() == user_domain_clean
        ]
        
        # Mark as user-owned
        for feature in self.user_owns_features:
            feature.is_user_owned = True
        
        # Find features above user
        if self.user_best_position:
            self.features_above_user = [
                f for f in self.features
                if f.position < self.user_best_position and not f.is_user_owned
            ]
            self.total_visual_displacement = sum(
                f.get_visual_displacement() for f in self.features_above_user
            )
        
        return self
    
    @model_validator(mode='after')
    def count_unique_domains(self) -> 'SERPAnalysis':
        """Count unique domains in results."""
        unique_domains = set(r.domain for r in self.results)
        self.total_unique_domains = len(unique_domains)
        return self
    
    def classify_intent(self) -> SERPIntentType:
        """
        Classify search intent based on SERP composition.
        
        Returns:
            Classified intent type
        """
        feature_types = {f.type for f in self.features}
        
        # Navigational signals
        navigational_signals = {
            SERPFeatureType.KNOWLEDGE_PANEL,
            SERPFeatureType.SITE_LINKS
        }
        if feature_types & navigational_signals and self.total_unique_domains <= 3:
            return SERPIntentType.NAVIGATIONAL
        
        # Transactional signals
        transactional_signals = {
            SERPFeatureType.SHOPPING_RESULTS,
            SERPFeatureType.REVIEWS
        }
        if feature_types & transactional_signals:
            return SERPIntentType.TRANSACTIONAL
        
        # Commercial investigation signals
        commercial_signals = {
            SERPFeatureType.VIDEO_CAROUSEL,
            SERPFeatureType.REVIEWS
        }
        if feature_types & commercial_signals or "best" in self.keyword.lower() or "top" in self.keyword.lower():
            return SERPIntentType.COMMERCIAL
        
        # Informational signals
        informational_signals = {
            SERPFeatureType.FEATURED_SNIPPET,
            SERPFeatureType.PEOPLE_ALSO_ASK,
            SERPFeatureType.FAQ
        }
        if feature_types & informational_signals:
            return SERPIntentType.INFORMATIONAL
        
        # Mixed if multiple types
        if len(feature_types) >= 4:
            return SERPIntentType.MIXED
        
        return SERPIntentType.UNKNOWN
    
    def calculate_click_estimates(self, monthly_impressions: Optional[int] = None) -> None:
        """
        Calculate click distribution estimates.
        
        Args:
            monthly_impressions: Known monthly impressions from GSC (if available)
        """
        if not self.results:
            return
        
        # CTR curve adjusted for SERP features
        # Base CTR curve (desktop, no SERP features)
        base_ctr = {
            1: 0.28, 2: 0.15, 3: 0.10, 4: 0.07, 5: 0.05,
            6: 0.04, 7: 0.03, 8: 0.025, 9: 0.02, 10: 0.015
        }
        
        # Adjustment factor based on SERP features
        feature_impact = 1.0 - (len(self.features) * 0.08)  # Each feature reduces CTR by ~8%
        feature_impact = max(0.3, feature_impact)  # Floor at 30% of base
        
        # Calculate estimated clicks per position
        total_estimated_ctr = 0.0
        user_estimated_ctr = 0.0
        
        for result in self.results[:10]:  # Top 10 only
            position_ctr = base_ctr.get(result.position, 0.01) * feature_impact
            total_estimated_ctr += position_ctr
            
            if result.is_user_site:
                user_estimated_ctr += position_ctr
        
        # If we have impressions, estimate total clicks
        if monthly_impressions:
            self.estimated_total_clicks = int(monthly_impressions * 0.7)  # Assume 70% CTR overall
            self.estimated_user_clicks = int(self.estimated_total_clicks * user_estimated_ctr / total_estimated_ctr) if total_estimated_ctr > 0 else 0
        
        # Calculate share metrics
        if total_estimated_ctr > 0:
            self.estimated_user_click_share = user_estimated_ctr / total_estimated_ctr
            
            # Opportunity = what's capturable by improving to position 1-3
            top3_ctr = sum(base_ctr.get(i, 0) * feature_impact for i in [1, 2, 3])
            self.click_share_opportunity = max(0, (top3_ctr / 3 - user_estimated_ctr) / total_estimated_ctr)
    
    def get_displacement_impact(self) -> Dict[str, Any]:
        """
        Calculate the impact of SERP feature displacement on user's position.
        
        Returns:
            Dictionary with displacement analysis
        """
        if not self.user_best_position:
            return {
                "has_displacement": False,
                "organic_position": None,
                "visual_position": None,
                "positions_displaced": 0,
                "features_causing_displacement": []
            }
        
        return {
            "has_displacement": self.total_visual_displacement > 0,
            "organic_position": self.user_best_position,
            "visual_position": self.user_visual_position,
            "positions_displaced": self.total_visual_displacement,
            "features_causing_displacement": [
                {
                    "type": f.type.value,
                    "position": f.position,
                    "displacement": f.get_visual_displacement(),
                    "title": f.title
                }
                for f in self.features_above_user
            ],
            "estimated_ctr_impact": -0.01 * self.total_visual_displacement  # Rough estimate
        }
    
    def to_dict(self, include_full_results: bool = True) -> Dict[str, Any]:
        """
        Convert to dictionary.
        
        Args:
            include_full_results: Whether to include full results list (can be large)
            
        Returns:
            Dictionary representation
        """
        data = self.model_dump()
        
        # Convert enums to values
        data['intent_classification'] = self.intent_classification.value
        data['device'] = self.device
        
        if not include_full_results:
            # Summarize results instead of full list
            data['results_summary'] = {
                'total_results': len(self.results),
                'user_results_count': len(self.user_results),
                'top_3_domains': [r.domain for r in self.results[:3]]
            }
            del data['results']
        
        return data
    
    class Config:
        """Pydantic config."""
        json_encoders = {
            datetime: lambda v: v.isoformat(),
        }
        use_enum_values = True
