"""
Module 8: CTR Modeling by SERP Context

Builds a SERP-context-aware CTR model to predict expected CTR based on position
and SERP features present (featured snippets, PAA, AI Overviews, etc.).
Compares expected vs actual CTR to identify over/underperformers and
quantifies SERP feature opportunities.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score, mean_absolute_error
import logging

logger = logging.getLogger(__name__)


@dataclass
class SERPFeatures:
    """SERP features that affect CTR"""
    position: int
    has_featured_snippet_above: bool
    paa_count_above: int
    video_carousel_present: bool
    ai_overview_present: bool
    shopping_present: bool
    local_pack_present: bool
    ads_count_above: int
    organic_results_above_fold: int
    knowledge_panel_present: bool
    image_pack_present: bool
    top_stories_present: bool


@dataclass
class KeywordCTRAnalysis:
    """CTR analysis for a single keyword"""
    keyword: str
    position: float
    expected_ctr_generic: float
    expected_ctr_contextual: float
    actual_ctr: float
    performance: str  # "overperforming", "in_line", "underperforming"
    serp_features_present: List[str]
    performance_delta: float
    url: str


@dataclass
class FeatureOpportunity:
    """Opportunity to capture a SERP feature"""
    keyword: str
    feature: str
    current_holder: Optional[str]
    estimated_click_gain: int
    difficulty: str  # "easy", "medium", "hard"
    action_required: str
    priority_score: float


def _extract_serp_features(serp_item: Dict[str, Any], user_position: int) -> SERPFeatures:
    """
    Extract SERP features from DataForSEO SERP data.
    
    Args:
        serp_item: Raw SERP data for a keyword from DataForSEO
        user_position: The user's organic position for this keyword
        
    Returns:
        SERPFeatures object with all relevant features
    """
    items = serp_item.get('items', [])
    
    # Count features above user's position
    has_featured_snippet = False
    paa_count = 0
    ads_above = 0
    organic_above = 0
    
    video_carousel = False
    ai_overview = False
    shopping = False
    local_pack = False
    knowledge_panel = False
    image_pack = False
    top_stories = False
    
    for item in items:
        item_type = item.get('type', '')
        rank_group = item.get('rank_group', 0)
        rank_absolute = item.get('rank_absolute', 0)
        
        # Only count features above user's position
        if rank_absolute >= user_position:
            continue
            
        if item_type == 'featured_snippet':
            has_featured_snippet = True
        elif item_type == 'people_also_ask':
            paa_count += 1
        elif item_type == 'paid':
            ads_above += 1
        elif item_type == 'organic':
            organic_above += 1
        elif item_type == 'video':
            video_carousel = True
        elif item_type == 'ai_overview':
            ai_overview = True
        elif item_type == 'shopping':
            shopping = True
        elif item_type == 'local_pack':
            local_pack = True
        elif item_type == 'knowledge_graph':
            knowledge_panel = True
        elif item_type == 'images':
            image_pack = True
        elif item_type == 'top_stories':
            top_stories = True
    
    return SERPFeatures(
        position=int(user_position),
        has_featured_snippet_above=has_featured_snippet,
        paa_count_above=paa_count,
        video_carousel_present=video_carousel,
        ai_overview_present=ai_overview,
        shopping_present=shopping,
        local_pack_present=local_pack,
        ads_count_above=ads_above,
        organic_results_above_fold=organic_above,
        knowledge_panel_present=knowledge_panel,
        image_pack_present=image_pack,
        top_stories_present=top_stories
    )


def _features_to_vector(features: SERPFeatures) -> np.ndarray:
    """Convert SERPFeatures to numpy array for model input"""
    return np.array([
        features.position,
        float(features.has_featured_snippet_above),
        features.paa_count_above,
        float(features.video_carousel_present),
        float(features.ai_overview_present),
        float(features.shopping_present),
        float(features.local_pack_present),
        features.ads_count_above,
        features.organic_results_above_fold,
        float(features.knowledge_panel_present),
        float(features.image_pack_present),
        float(features.top_stories_present)
    ])


def _calculate_generic_ctr(position: int) -> float:
    """
    Calculate expected CTR based on position alone using industry benchmarks.
    
    Based on aggregated CTR curves from various studies.
    """
    # Industry benchmark CTR by position
    ctr_curve = {
        1: 0.316,
        2: 0.158,
        3: 0.105,
        4: 0.077,
        5: 0.060,
        6: 0.048,
        7: 0.039,
        8: 0.033,
        9: 0.028,
        10: 0.024
    }
    
    if position <= 10:
        return ctr_curve.get(position, 0.024)
    elif position <= 20:
        # Linear decay from position 10 to 20
        return max(0.005, 0.024 - (position - 10) * 0.0015)
    else:
        return 0.005


def _train_ctr_model(
    training_data: pd.DataFrame,
    serp_data: Dict[str, Any]
) -> GradientBoostingRegressor:
    """
    Train gradient boosting model to predict CTR based on SERP context.
    
    Args:
        training_data: DataFrame with columns [keyword, position, ctr, impressions]
        serp_data: Dictionary mapping keywords to SERP feature data
        
    Returns:
        Trained GradientBoostingRegressor model
    """
    features_list = []
    ctr_list = []
    
    for _, row in training_data.iterrows():
        keyword = row['keyword']
        position = row['position']
        ctr = row['ctr']
        
        # Need minimum impressions for reliable CTR
        if row.get('impressions', 0) < 100:
            continue
        
        # Get SERP features for this keyword
        serp_item = serp_data.get(keyword)
        if not serp_item:
            continue
        
        try:
            features = _extract_serp_features(serp_item, position)
            features_vector = _features_to_vector(features)
            
            features_list.append(features_vector)
            ctr_list.append(ctr)
        except Exception as e:
            logger.warning(f"Failed to extract features for {keyword}: {e}")
            continue
    
    if len(features_list) < 10:
        logger.warning("Insufficient training data for CTR model")
        return None
    
    X = np.array(features_list)
    y = np.array(ctr_list)
    
    # Train/test split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    
    # Train gradient boosting model
    model = GradientBoostingRegressor(
        n_estimators=100,
        learning_rate=0.1,
        max_depth=4,
        min_samples_split=5,
        min_samples_leaf=3,
        random_state=42
    )
    
    model.fit(X_train, y_train)
    
    # Evaluate
    y_pred = model.predict(X_test)
    r2 = r2_score(y_test, y_pred)
    mae = mean_absolute_error(y_test, y_pred)
    
    logger.info(f"CTR model trained: R² = {r2:.3f}, MAE = {mae:.4f}")
    
    return model


def _classify_performance(actual: float, expected: float) -> str:
    """Classify CTR performance vs expectation"""
    ratio = actual / expected if expected > 0 else 0
    
    if ratio > 1.2:
        return "overperforming"
    elif ratio < 0.8:
        return "underperforming"
    else:
        return "in_line"


def _get_serp_features_list(features: SERPFeatures) -> List[str]:
    """Convert SERPFeatures to list of human-readable feature names"""
    feature_list = []
    
    if features.has_featured_snippet_above:
        feature_list.append("featured_snippet")
    if features.paa_count_above > 0:
        feature_list.append(f"paa_x{features.paa_count_above}")
    if features.video_carousel_present:
        feature_list.append("video_carousel")
    if features.ai_overview_present:
        feature_list.append("ai_overview")
    if features.shopping_present:
        feature_list.append("shopping_results")
    if features.local_pack_present:
        feature_list.append("local_pack")
    if features.knowledge_panel_present:
        feature_list.append("knowledge_panel")
    if features.image_pack_present:
        feature_list.append("image_pack")
    if features.top_stories_present:
        feature_list.append("top_stories")
    if features.ads_count_above > 0:
        feature_list.append(f"ads_x{features.ads_count_above}")
    
    return feature_list


def _identify_feature_opportunities(
    gsc_data: pd.DataFrame,
    serp_data: Dict[str, Any]
) -> List[FeatureOpportunity]:
    """
    Identify opportunities to capture SERP features.
    
    Focus on:
    - Featured snippets that are available (none present or competitor holds it)
    - Video carousel opportunities where no video present
    - FAQ/PAA opportunities
    """
    opportunities = []
    
    for _, row in gsc_data.iterrows():
        keyword = row['keyword']
        position = row['position']
        impressions = row['impressions']
        
        # Only consider keywords with decent volume and position
        if impressions < 500 or position > 15:
            continue
        
        serp_item = serp_data.get(keyword)
        if not serp_item:
            continue
        
        items = serp_item.get('items', [])
        
        # Check for featured snippet opportunity
        has_featured_snippet = any(
            item.get('type') == 'featured_snippet' for item in items
        )
        
        if not has_featured_snippet and position <= 10:
            # No featured snippet present and we're on page 1
            estimated_gain = int(impressions * 0.08)  # ~8% CTR for position 0
            opportunities.append(FeatureOpportunity(
                keyword=keyword,
                feature="featured_snippet",
                current_holder=None,
                estimated_click_gain=estimated_gain,
                difficulty="medium",
                action_required="Add structured FAQ or list content",
                priority_score=estimated_gain * 1.5  # Higher priority
            ))
        elif has_featured_snippet and position <= 5:
            # Competitor holds it, we're close
            snippet_item = next(
                (item for item in items if item.get('type') == 'featured_snippet'),
                None
            )
            current_holder = snippet_item.get('domain', 'competitor') if snippet_item else 'competitor'
            estimated_gain = int(impressions * 0.05)
            
            opportunities.append(FeatureOpportunity(
                keyword=keyword,
                feature="featured_snippet",
                current_holder=current_holder,
                estimated_click_gain=estimated_gain,
                difficulty="hard",
                action_required=f"Outrank {current_holder} with better structured content",
                priority_score=estimated_gain * 0.8
            ))
        
        # Check for video carousel opportunity
        has_video = any(
            item.get('type') == 'video' for item in items
        )
        
        if not has_video and position <= 8:
            estimated_gain = int(impressions * 0.03)
            opportunities.append(FeatureOpportunity(
                keyword=keyword,
                feature="video_carousel",
                current_holder=None,
                estimated_click_gain=estimated_gain,
                difficulty="medium",
                action_required="Create video content and optimize for YouTube/video SERP",
                priority_score=estimated_gain * 1.0
            ))
        
        # Check for FAQ/PAA opportunity
        paa_count = sum(
            1 for item in items if item.get('type') == 'people_also_ask'
        )
        
        if paa_count > 2 and position <= 10:
            # Heavy PAA presence suggests FAQ schema opportunity
            estimated_gain = int(impressions * 0.02)
            opportunities.append(FeatureOpportunity(
                keyword=keyword,
                feature="faq_schema",
                current_holder=None,
                estimated_click_gain=estimated_gain,
                difficulty="easy",
                action_required="Add FAQ schema markup to page",
                priority_score=estimated_gain * 1.2
            ))
    
    # Sort by priority score
    opportunities.sort(key=lambda x: x.priority_score, reverse=True)
    
    return opportunities[:20]  # Top 20 opportunities


def model_contextual_ctr(
    serp_data: Dict[str, Any],
    gsc_data: pd.DataFrame
) -> Dict[str, Any]:
    """
    Build SERP-context-aware CTR model and analyze keyword performance.
    
    Args:
        serp_data: Dictionary mapping keywords to DataForSEO SERP data
        gsc_data: DataFrame with columns [keyword, position, ctr, clicks, impressions, url]
        
    Returns:
        Dictionary containing:
        - ctr_model_accuracy: R² score
        - keyword_ctr_analysis: List of KeywordCTRAnalysis objects
        - feature_opportunities: List of FeatureOpportunity objects
        - summary_stats: Aggregated statistics
    """
    try:
        logger.info("Starting CTR modeling analysis")
        
        # Train CTR model
        model = _train_ctr_model(gsc_data, serp_data)
        
        keyword_analyses = []
        
        for _, row in gsc_data.iterrows():
            keyword = row['keyword']
            position = row['position']
            actual_ctr = row['ctr']
            url = row.get('url', '')
            
            # Skip low-impression keywords
            if row.get('impressions', 0) < 100:
                continue
            
            # Calculate generic expected CTR
            expected_ctr_generic = _calculate_generic_ctr(int(position))
            
            # Get SERP features and calculate contextual CTR
            serp_item = serp_data.get(keyword)
            if serp_item and model is not None:
                try:
                    features = _extract_serp_features(serp_item, position)
                    features_vector = _features_to_vector(features).reshape(1, -1)
                    expected_ctr_contextual = model.predict(features_vector)[0]
                    expected_ctr_contextual = max(0.001, min(1.0, expected_ctr_contextual))
                    serp_features_list = _get_serp_features_list(features)
                except Exception as e:
                    logger.warning(f"Failed to get contextual CTR for {keyword}: {e}")
                    expected_ctr_contextual = expected_ctr_generic
                    serp_features_list = []
            else:
                expected_ctr_contextual = expected_ctr_generic
                serp_features_list = []
            
            performance = _classify_performance(actual_ctr, expected_ctr_contextual)
            performance_delta = actual_ctr - expected_ctr_contextual
            
            keyword_analyses.append(KeywordCTRAnalysis(
                keyword=keyword,
                position=position,
                expected_ctr_generic=expected_ctr_generic,
                expected_ctr_contextual=expected_ctr_contextual,
                actual_ctr=actual_ctr,
                performance=performance,
                serp_features_present=serp_features_list,
                performance_delta=performance_delta,
                url=url
            ))
        
        # Identify feature opportunities
        feature_opportunities = _identify_feature_opportunities(gsc_data, serp_data)
        
        # Calculate summary statistics
        overperformers = [k for k in keyword_analyses if k.performance == "overperforming"]
        underperformers = [k for k in keyword_analyses if k.performance == "underperforming"]
        
        total_opportunity_clicks = sum(
            opp.estimated_click_gain for opp in feature_opportunities
        )
        
        summary_stats = {
            "total_keywords_analyzed": len(keyword_analyses),
            "overperformers_count": len(overperformers),
            "underperformers_count": len(underperformers),
            "avg_generic_vs_contextual_gap": np.mean([
                k.expected_ctr_generic - k.expected_ctr_contextual
                for k in keyword_analyses
            ]),
            "total_feature_opportunity_clicks": total_opportunity_clicks,
            "feature_opportunities_count": len(feature_opportunities)
        }
        
        logger.info(f"CTR modeling complete: {len(keyword_analyses)} keywords analyzed")
        
        return {
            "ctr_model_accuracy": r2_score(
                [k.actual_ctr for k in keyword_analyses],
                [k.expected_ctr_contextual for k in keyword_analyses]
            ) if model else None,
            "keyword_ctr_analysis": [
                {
                    "keyword": k.keyword,
                    "position": k.position,
                    "expected_ctr_generic": round(k.expected_ctr_generic, 4),
                    "expected_ctr_contextual": round(k.expected_ctr_contextual, 4),
                    "actual_ctr": round(k.actual_ctr, 4),
                    "performance": k.performance,
                    "performance_delta": round(k.performance_delta, 4),
                    "serp_features_present": k.serp_features_present,
                    "url": k.url
                }
                for k in keyword_analyses
            ],
            "feature_opportunities": [
                {
                    "keyword": opp.keyword,
                    "feature": opp.feature,
                    "current_holder": opp.current_holder,
                    "estimated_click_gain": opp.estimated_click_gain,
                    "difficulty": opp.difficulty,
                    "action_required": opp.action_required,
                    "priority_score": round(opp.priority_score, 2)
                }
                for opp in feature_opportunities
            ],
            "summary_stats": summary_stats
        }
        
    except Exception as e:
        logger.error(f"Error in CTR modeling: {e}", exc_info=True)
        raise
