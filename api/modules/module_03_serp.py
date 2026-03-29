"""
Module 3: SERP Landscape Analysis

Analyzes the competitive SERP landscape for top keywords, including:
- SERP feature displacement analysis
- Competitor mapping and threat assessment
- Intent classification based on SERP composition
- Click share estimation with position-adjusted CTR
"""

from typing import Dict, List, Any, Optional
import pandas as pd
import logging
from collections import defaultdict

logger = logging.getLogger(__name__)


class SERPLandscapeAnalyzer:
    """Analyzes SERP features, competitors, and click share opportunities."""
    
    def __init__(self):
        # SERP feature visual position weights (approximate positions they consume)
        self.feature_weights = {
            'featured_snippet': 2.0,
            'knowledge_panel': 1.5,
            'ai_overview': 2.5,
            'local_pack': 3.0,
            'people_also_ask': 0.5,  # per question
            'video_carousel': 1.0,
            'image_pack': 0.5,
            'shopping_results': 1.0,
            'top_stories': 1.0,
            'site_links': 0.3,
            'ads': 1.0  # per ad
        }
        
        # Intent classification rules based on SERP composition
        self.intent_signals = {
            'informational': ['knowledge_panel', 'people_also_ask', 'featured_snippet'],
            'commercial': ['shopping_results', 'product_listings', 'reviews'],
            'navigational': ['site_links', 'knowledge_panel'],
            'transactional': ['shopping_results', 'local_pack', 'ads']
        }
    
    def analyze(
        self,
        serp_data: List[Dict[str, Any]],
        gsc_keyword_data: pd.DataFrame
    ) -> Dict[str, Any]:
        """
        Main analysis entry point.
        
        Args:
            serp_data: List of SERP results from DataForSEO with features
            gsc_keyword_data: DataFrame with columns: query, position, clicks, impressions, ctr
        
        Returns:
            Dictionary containing:
            - keywords_analyzed: int
            - serp_feature_displacement: list of displacement analysis per keyword
            - competitors: list of competitor analysis
            - intent_mismatches: list of intent mismatch flags
            - total_click_share: float (0-1)
            - click_share_opportunity: float (0-1)
        """
        try:
            logger.info(f"Analyzing SERP landscape for {len(serp_data)} keywords")
            
            # Merge SERP data with GSC data
            enriched_data = self._merge_serp_gsc_data(serp_data, gsc_keyword_data)
            
            # Run sub-analyses
            displacement_analysis = self._analyze_displacement(enriched_data)
            competitor_analysis = self._analyze_competitors(enriched_data)
            intent_analysis = self._analyze_intent(enriched_data)
            click_share_analysis = self._estimate_click_share(enriched_data)
            
            result = {
                'keywords_analyzed': len(enriched_data),
                'serp_feature_displacement': displacement_analysis,
                'competitors': competitor_analysis,
                'intent_mismatches': intent_analysis,
                'total_click_share': click_share_analysis['total_click_share'],
                'click_share_opportunity': click_share_analysis['opportunity']
            }
            
            logger.info(f"SERP landscape analysis complete. Click share: {result['total_click_share']:.2%}")
            return result
            
        except Exception as e:
            logger.error(f"Error in SERP landscape analysis: {str(e)}")
            raise
    
    def _merge_serp_gsc_data(
        self,
        serp_data: List[Dict[str, Any]],
        gsc_data: pd.DataFrame
    ) -> List[Dict[str, Any]]:
        """Merge SERP API data with GSC performance data."""
        enriched = []
        
        # Create lookup dict from GSC data
        gsc_lookup = {}
        if not gsc_data.empty:
            for _, row in gsc_data.iterrows():
                gsc_lookup[row['query'].lower()] = {
                    'position': row.get('position', 0),
                    'clicks': row.get('clicks', 0),
                    'impressions': row.get('impressions', 0),
                    'ctr': row.get('ctr', 0)
                }
        
        for serp in serp_data:
            keyword = serp.get('keyword', '').lower()
            gsc_metrics = gsc_lookup.get(keyword, {
                'position': 0,
                'clicks': 0,
                'impressions': 0,
                'ctr': 0
            })
            
            enriched.append({
                'keyword': keyword,
                'organic_position': gsc_metrics['position'],
                'clicks': gsc_metrics['clicks'],
                'impressions': gsc_metrics['impressions'],
                'actual_ctr': gsc_metrics['ctr'],
                'serp_features': serp.get('features', []),
                'organic_results': serp.get('organic_results', []),
                'serp_data': serp
            })
        
        return enriched
    
    def _analyze_displacement(
        self,
        enriched_data: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Analyze SERP feature displacement - how features push organic results down.
        """
        displacement_results = []
        
        for item in enriched_data:
            features = item.get('serp_features', [])
            organic_position = item.get('organic_position', 0)
            
            if organic_position == 0:
                continue
            
            # Calculate visual position based on features above the user's result
            visual_position = self._calculate_visual_position(
                organic_position,
                features,
                item.get('organic_results', [])
            )
            
            # Estimate CTR impact
            ctr_impact = self._estimate_ctr_impact(organic_position, visual_position)
            
            # Only flag significant displacement (> 3 position equivalent)
            if visual_position > organic_position + 3:
                displacement_results.append({
                    'keyword': item['keyword'],
                    'organic_position': organic_position,
                    'visual_position': round(visual_position, 1),
                    'features_above': self._extract_features_above(
                        features,
                        organic_position,
                        item.get('organic_results', [])
                    ),
                    'estimated_ctr_impact': round(ctr_impact, 4),
                    'impressions': item.get('impressions', 0)
                })
        
        # Sort by impact (impressions * ctr_impact)
        displacement_results.sort(
            key=lambda x: x['impressions'] * abs(x['estimated_ctr_impact']),
            reverse=True
        )
        
        return displacement_results
    
    def _calculate_visual_position(
        self,
        organic_position: float,
        features: List[Dict[str, Any]],
        organic_results: List[Dict[str, Any]]
    ) -> float:
        """
        Calculate the visual position accounting for SERP features.
        """
        visual_offset = 0.0
        
        # Parse features that appear above the organic result
        for feature in features:
            feature_type = feature.get('type', '').lower()
            position = feature.get('position', 999)
            
            # Only count features above the organic result
            if position < organic_position:
                if feature_type in self.feature_weights:
                    weight = self.feature_weights[feature_type]
                    
                    # Special handling for countable features
                    if feature_type == 'people_also_ask':
                        count = feature.get('items', 0)
                        visual_offset += weight * count
                    elif feature_type == 'ads':
                        count = feature.get('count', 0)
                        visual_offset += weight * count
                    else:
                        visual_offset += weight
        
        return organic_position + visual_offset
    
    def _extract_features_above(
        self,
        features: List[Dict[str, Any]],
        organic_position: float,
        organic_results: List[Dict[str, Any]]
    ) -> List[str]:
        """Extract names of features appearing above the user's organic result."""
        features_above = []
        
        for feature in features:
            feature_type = feature.get('type', '')
            position = feature.get('position', 999)
            
            if position < organic_position:
                # Format feature name with count for countable features
                if feature_type == 'people_also_ask':
                    count = feature.get('items', 0)
                    features_above.append(f"paa_x{count}")
                elif feature_type == 'ads':
                    count = feature.get('count', 0)
                    features_above.append(f"ads_x{count}")
                else:
                    features_above.append(feature_type)
        
        return features_above
    
    def _estimate_ctr_impact(
        self,
        organic_position: float,
        visual_position: float
    ) -> float:
        """
        Estimate CTR impact from position displacement.
        Uses simplified CTR curve estimation.
        """
        # Simplified CTR curve (position -> expected CTR)
        def position_to_ctr(pos: float) -> float:
            if pos <= 1:
                return 0.30
            elif pos <= 3:
                return 0.30 * (0.6 ** (pos - 1))
            elif pos <= 10:
                return 0.18 * (0.75 ** (pos - 3))
            else:
                return 0.02 * (0.8 ** (pos - 10))
        
        organic_ctr = position_to_ctr(organic_position)
        visual_ctr = position_to_ctr(visual_position)
        
        return visual_ctr - organic_ctr
    
    def _analyze_competitors(
        self,
        enriched_data: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Map competitor domains and their threat levels.
        """
        # Track competitor appearances
        competitor_stats = defaultdict(lambda: {
            'keywords': [],
            'positions': [],
            'url_count': 0
        })
        
        for item in enriched_data:
            organic_results = item.get('organic_results', [])
            keyword = item['keyword']
            
            for result in organic_results[:10]:  # Top 10 only
                domain = self._extract_domain(result.get('url', ''))
                position = result.get('position', 999)
                
                if domain:
                    competitor_stats[domain]['keywords'].append(keyword)
                    competitor_stats[domain]['positions'].append(position)
                    competitor_stats[domain]['url_count'] += 1
        
        # Convert to list and calculate metrics
        competitors = []
        for domain, stats in competitor_stats.items():
            keywords_shared = len(set(stats['keywords']))
            avg_position = sum(stats['positions']) / len(stats['positions']) if stats['positions'] else 0
            
            # Calculate threat level
            threat_level = self._calculate_threat_level(
                keywords_shared,
                avg_position,
                len(enriched_data)
            )
            
            competitors.append({
                'domain': domain,
                'keywords_shared': keywords_shared,
                'avg_position': round(avg_position, 1),
                'appearances': stats['url_count'],
                'threat_level': threat_level
            })
        
        # Sort by keywords shared (frequency)
        competitors.sort(key=lambda x: x['keywords_shared'], reverse=True)
        
        # Return top 20 competitors
        return competitors[:20]
    
    def _extract_domain(self, url: str) -> Optional[str]:
        """Extract domain from URL."""
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            domain = parsed.netloc or parsed.path
            # Remove www. prefix
            domain = domain.replace('www.', '')
            return domain if domain else None
        except Exception:
            return None
    
    def _calculate_threat_level(
        self,
        keywords_shared: int,
        avg_position: float,
        total_keywords: int
    ) -> str:
        """Calculate threat level based on competitor metrics."""
        keyword_overlap_ratio = keywords_shared / total_keywords if total_keywords > 0 else 0
        
        # High overlap + high positions = high threat
        if keyword_overlap_ratio > 0.3 and avg_position < 5:
            return 'high'
        elif keyword_overlap_ratio > 0.2 and avg_position < 7:
            return 'high'
        elif keyword_overlap_ratio > 0.15 or avg_position < 5:
            return 'medium'
        else:
            return 'low'
    
    def _analyze_intent(
        self,
        enriched_data: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Classify query intent based on SERP composition and flag mismatches.
        """
        intent_mismatches = []
        
        for item in enriched_data:
            features = item.get('serp_features', [])
            
            # Classify intent based on feature composition
            intent = self._classify_intent(features)
            
            # Check if user's content type matches intent
            # (This would require page type data - simplified for now)
            organic_results = item.get('organic_results', [])
            user_result = self._find_user_result(organic_results, item['organic_position'])
            
            if user_result:
                content_type = self._infer_content_type(user_result)
                
                # Flag mismatches
                if self._is_intent_mismatch(intent, content_type):
                    intent_mismatches.append({
                        'keyword': item['keyword'],
                        'detected_intent': intent,
                        'content_type': content_type,
                        'position': item['organic_position'],
                        'recommendation': self._get_intent_recommendation(intent, content_type)
                    })
        
        return intent_mismatches
    
    def _classify_intent(self, features: List[Dict[str, Any]]) -> str:
        """Classify search intent based on SERP features present."""
        feature_types = [f.get('type', '').lower() for f in features]
        
        # Count signals for each intent type
        intent_scores = defaultdict(int)
        for intent, signals in self.intent_signals.items():
            for signal in signals:
                if signal in feature_types:
                    intent_scores[intent] += 1
        
        # Return intent with highest score, default to informational
        if not intent_scores:
            return 'informational'
        
        return max(intent_scores.items(), key=lambda x: x[1])[0]
    
    def _find_user_result(
        self,
        organic_results: List[Dict[str, Any]],
        position: float
    ) -> Optional[Dict[str, Any]]:
        """Find the user's result in organic results."""
        for result in organic_results:
            if abs(result.get('position', 0) - position) < 0.5:
                return result
        return None
    
    def _infer_content_type(self, result: Dict[str, Any]) -> str:
        """Infer content type from URL and snippet."""
        url = result.get('url', '').lower()
        
        if '/blog/' in url or '/article/' in url:
            return 'blog_post'
        elif '/product/' in url or '/shop/' in url:
            return 'product_page'
        elif '/category/' in url or '/collection/' in url:
            return 'category_page'
        elif url.endswith('/'):
            return 'landing_page'
        else:
            return 'unknown'
    
    def _is_intent_mismatch(self, intent: str, content_type: str) -> bool:
        """Check if content type matches search intent."""
        mismatches = {
            'transactional': ['blog_post'],
            'informational': ['product_page'],
            'commercial': ['blog_post']
        }
        
        return content_type in mismatches.get(intent, [])
    
    def _get_intent_recommendation(self, intent: str, content_type: str) -> str:
        """Generate recommendation for intent mismatch."""
        recommendations = {
            ('transactional', 'blog_post'): 'Consider creating a product/pricing page for this transactional query',
            ('informational', 'product_page'): 'Consider creating an informational guide instead of pushing product',
            ('commercial', 'blog_post'): 'Add comparison tables and CTAs to convert commercial intent'
        }
        
        return recommendations.get((intent, content_type), 'Review content alignment with search intent')
    
    def _estimate_click_share(
        self,
        enriched_data: List[Dict[str, Any]]
    ) -> Dict[str, float]:
        """
        Estimate total click share and opportunity.
        """
        total_available_clicks = 0
        total_captured_clicks = 0
        
        for item in enriched_data:
            impressions = item.get('impressions', 0)
            actual_ctr = item.get('actual_ctr', 0)
            organic_position = item.get('organic_position', 0)
            
            if impressions == 0 or organic_position == 0:
                continue
            
            # Estimate total available clicks for this keyword
            # Assume average CTR across top 10 positions is ~15%
            available_clicks = impressions * 0.15
            captured_clicks = impressions * actual_ctr
            
            total_available_clicks += available_clicks
            total_captured_clicks += captured_clicks
        
        click_share = total_captured_clicks / total_available_clicks if total_available_clicks > 0 else 0
        opportunity = 1.0 - click_share
        
        return {
            'total_click_share': round(click_share, 4),
            'opportunity': round(opportunity, 4),
            'total_captured': int(total_captured_clicks),
            'total_available': int(total_available_clicks)
        }


def analyze_serp_landscape(
    serp_data: List[Dict[str, Any]],
    gsc_keyword_data: pd.DataFrame
) -> Dict[str, Any]:
    """
    Main entry point for SERP landscape analysis.
    
    Args:
        serp_data: List of SERP results from DataForSEO
        gsc_keyword_data: GSC performance data per keyword
    
    Returns:
        Dictionary containing complete SERP landscape analysis
    """
    analyzer = SERPLandscapeAnalyzer()
    return analyzer.analyze(serp_data, gsc_keyword_data)
