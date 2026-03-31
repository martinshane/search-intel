"""
Module 3: SERP Landscape Analysis

Analyzes SERP features, competitor presence, intent classification, and click share estimation.
"""

import logging
from typing import Dict, List, Any, Optional
from collections import defaultdict, Counter
import pandas as pd

logger = logging.getLogger(__name__)


class SERPLandscapeAnalyzer:
    """Analyzes SERP landscape including features, competitors, and intent."""
    
    # SERP feature weights for visual position calculation
    SERP_FEATURE_WEIGHTS = {
        'featured_snippet': 2.0,
        'knowledge_panel': 1.5,
        'ai_overview': 2.5,
        'local_pack': 3.0,
        'people_also_ask': 0.5,  # per question
        'video_carousel': 1.0,
        'image_pack': 0.5,
        'shopping_results': 1.0,
        'top_stories': 1.0,
        'reddit_threads': 0.3,  # per thread
    }
    
    # Position-based CTR curves (generic baseline)
    GENERIC_CTR_BY_POSITION = {
        1: 0.284, 2: 0.147, 3: 0.082, 4: 0.053, 5: 0.038,
        6: 0.030, 7: 0.024, 8: 0.020, 9: 0.017, 10: 0.015
    }
    
    def __init__(self):
        self.serp_data = []
        self.gsc_keyword_data = pd.DataFrame()
        
    def analyze(self, serp_data: List[Dict[str, Any]], gsc_keyword_data: pd.DataFrame) -> Dict[str, Any]:
        """
        Main analysis method.
        
        Args:
            serp_data: List of SERP results from DataForSEO
            gsc_keyword_data: GSC data with keyword performance
            
        Returns:
            Analysis results dictionary
        """
        try:
            self.serp_data = serp_data
            self.gsc_keyword_data = gsc_keyword_data
            
            if not serp_data:
                logger.warning("No SERP data provided, returning empty analysis")
                return self._empty_result()
            
            # 1. SERP feature displacement analysis
            feature_displacement = self._analyze_serp_feature_displacement()
            
            # 2. Competitor mapping
            competitors = self._analyze_competitors()
            
            # 3. Intent classification
            intent_analysis = self._classify_serp_intents()
            
            # 4. Click share estimation
            click_share = self._estimate_click_share()
            
            result = {
                'keywords_analyzed': len(serp_data),
                'serp_feature_displacement': feature_displacement,
                'competitors': competitors,
                'intent_analysis': intent_analysis,
                'click_share': click_share,
                'summary': self._generate_summary(feature_displacement, competitors, click_share)
            }
            
            logger.info(f"SERP landscape analysis complete: {len(serp_data)} keywords analyzed")
            return result
            
        except Exception as e:
            logger.error(f"Error in SERP landscape analysis: {str(e)}", exc_info=True)
            return self._empty_result()
    
    def _analyze_serp_feature_displacement(self) -> List[Dict[str, Any]]:
        """Analyze how SERP features displace organic results."""
        displacement_results = []
        
        for serp in self.serp_data:
            try:
                keyword = serp.get('keyword', '')
                user_position = self._find_user_position(serp)
                
                if user_position is None:
                    continue
                
                features_above = self._extract_features_above_position(serp, user_position)
                visual_position = self._calculate_visual_position(user_position, features_above)
                
                # Calculate CTR impact if visual position is significantly different
                if visual_position > user_position + 2:
                    generic_ctr = self.GENERIC_CTR_BY_POSITION.get(user_position, 0.01)
                    adjusted_ctr = self.GENERIC_CTR_BY_POSITION.get(
                        min(int(visual_position), 10), 0.01
                    )
                    ctr_impact = adjusted_ctr - generic_ctr
                    
                    displacement_results.append({
                        'keyword': keyword,
                        'organic_position': user_position,
                        'visual_position': round(visual_position, 1),
                        'displacement': round(visual_position - user_position, 1),
                        'features_above': features_above,
                        'estimated_ctr_impact': round(ctr_impact, 3)
                    })
                    
            except Exception as e:
                logger.warning(f"Error analyzing displacement for keyword: {str(e)}")
                continue
        
        # Sort by displacement magnitude
        displacement_results.sort(key=lambda x: x['displacement'], reverse=True)
        
        return displacement_results[:50]  # Return top 50 most displaced
    
    def _analyze_competitors(self) -> List[Dict[str, Any]]:
        """Map competitor presence across keywords."""
        competitor_frequency = Counter()
        competitor_positions = defaultdict(list)
        
        for serp in self.serp_data:
            try:
                organic_results = serp.get('organic_results', [])
                
                for result in organic_results[:10]:  # Top 10 only
                    domain = self._extract_domain(result.get('url', ''))
                    if domain and not self._is_user_domain(domain, serp):
                        competitor_frequency[domain] += 1
                        competitor_positions[domain].append(result.get('position', 100))
                        
            except Exception as e:
                logger.warning(f"Error analyzing competitors: {str(e)}")
                continue
        
        # Build competitor analysis
        competitors = []
        total_keywords = len(self.serp_data)
        
        for domain, count in competitor_frequency.most_common(20):
            avg_position = sum(competitor_positions[domain]) / len(competitor_positions[domain])
            overlap_pct = (count / total_keywords) * 100
            
            # Determine threat level
            threat_level = 'low'
            if overlap_pct > 40 and avg_position < 5:
                threat_level = 'critical'
            elif overlap_pct > 30 and avg_position < 7:
                threat_level = 'high'
            elif overlap_pct > 20 or avg_position < 5:
                threat_level = 'medium'
            
            competitors.append({
                'domain': domain,
                'keywords_shared': count,
                'overlap_percentage': round(overlap_pct, 1),
                'avg_position': round(avg_position, 1),
                'threat_level': threat_level
            })
        
        return competitors
    
    def _classify_serp_intents(self) -> Dict[str, Any]:
        """Classify intent based on SERP composition."""
        intent_distribution = {
            'informational': 0,
            'commercial': 0,
            'navigational': 0,
            'transactional': 0
        }
        
        intent_mismatches = []
        
        for serp in self.serp_data:
            try:
                keyword = serp.get('keyword', '').lower()
                
                # Classify based on SERP features and keyword patterns
                intent = self._classify_keyword_intent(keyword, serp)
                intent_distribution[intent] += 1
                
                # Check for potential mismatches
                user_result = self._find_user_result(serp)
                if user_result:
                    page_type = self._infer_page_type(user_result.get('url', ''))
                    if self._is_intent_mismatch(intent, page_type):
                        intent_mismatches.append({
                            'keyword': keyword,
                            'serp_intent': intent,
                            'page_type': page_type,
                            'user_position': user_result.get('position', 0),
                            'recommendation': self._get_mismatch_recommendation(intent, page_type)
                        })
                        
            except Exception as e:
                logger.warning(f"Error classifying intent: {str(e)}")
                continue
        
        total = sum(intent_distribution.values())
        if total > 0:
            for intent in intent_distribution:
                intent_distribution[intent] = round(intent_distribution[intent] / total, 3)
        
        return {
            'intent_distribution': intent_distribution,
            'intent_mismatches': intent_mismatches[:20]  # Top 20 mismatches
        }
    
    def _estimate_click_share(self) -> Dict[str, Any]:
        """Estimate click share across keyword portfolio."""
        total_estimated_clicks = 0
        total_potential_clicks = 0
        user_clicks = 0
        
        keyword_click_shares = []
        
        for serp in self.serp_data:
            try:
                keyword = serp.get('keyword', '')
                
                # Get GSC impressions for this keyword
                gsc_row = self.gsc_keyword_data[
                    self.gsc_keyword_data['query'] == keyword
                ] if not self.gsc_keyword_data.empty else pd.DataFrame()
                
                if gsc_row.empty:
                    continue
                
                impressions = gsc_row.iloc[0].get('impressions', 0)
                clicks = gsc_row.iloc[0].get('clicks', 0)
                position = gsc_row.iloc[0].get('position', 100)
                
                # Estimate total clicks available for this keyword
                # Use position 1 CTR as proxy for total available clicks
                potential_clicks = impressions * self.GENERIC_CTR_BY_POSITION.get(1, 0.28)
                
                # Adjust for SERP features
                features_above = self._extract_features_above_position(serp, position)
                if features_above:
                    # Reduce potential by 30% if heavy features present
                    feature_weight = sum([1 for f in features_above if f != 'people_also_ask'])
                    if feature_weight > 2:
                        potential_clicks *= 0.7
                
                click_share = (clicks / potential_clicks) if potential_clicks > 0 else 0
                
                total_estimated_clicks += clicks
                total_potential_clicks += potential_clicks
                user_clicks += clicks
                
                keyword_click_shares.append({
                    'keyword': keyword,
                    'clicks': clicks,
                    'potential_clicks': round(potential_clicks, 1),
                    'click_share': round(click_share, 3),
                    'position': round(position, 1)
                })
                
            except Exception as e:
                logger.warning(f"Error estimating click share: {str(e)}")
                continue
        
        overall_click_share = (total_estimated_clicks / total_potential_clicks) if total_potential_clicks > 0 else 0
        
        # Calculate opportunity
        click_opportunity = total_potential_clicks - total_estimated_clicks
        
        return {
            'total_click_share': round(overall_click_share, 3),
            'current_monthly_clicks': int(user_clicks),
            'potential_monthly_clicks': int(total_potential_clicks),
            'click_opportunity': int(click_opportunity),
            'keyword_breakdown': sorted(keyword_click_shares, key=lambda x: x['clicks'], reverse=True)[:30]
        }
    
    def _find_user_position(self, serp: Dict[str, Any]) -> Optional[int]:
        """Find user's organic position in SERP."""
        user_result = self._find_user_result(serp)
        return user_result.get('position') if user_result else None
    
    def _find_user_result(self, serp: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Find user's result in organic results."""
        organic_results = serp.get('organic_results', [])
        user_domain = serp.get('user_domain', '')
        
        for result in organic_results:
            domain = self._extract_domain(result.get('url', ''))
            if domain and user_domain and domain == user_domain:
                return result
        
        return None
    
    def _extract_features_above_position(self, serp: Dict[str, Any], position: int) -> List[str]:
        """Extract SERP features appearing above given position."""
        features = []
        
        # Check for various SERP features
        if serp.get('featured_snippet') and serp['featured_snippet'].get('position', 100) < position:
            features.append('featured_snippet')
        
        if serp.get('knowledge_panel'):
            features.append('knowledge_panel')
        
        if serp.get('ai_overview'):
            features.append('ai_overview')
        
        if serp.get('local_pack') and serp['local_pack'].get('position', 100) < position:
            features.append('local_pack')
        
        # People Also Ask - count how many above position
        paa = serp.get('people_also_ask', [])
        paa_above = sum(1 for q in paa if q.get('position', 100) < position)
        if paa_above > 0:
            features.extend(['people_also_ask'] * paa_above)
        
        if serp.get('video_results') and any(
            v.get('position', 100) < position for v in serp.get('video_results', [])
        ):
            features.append('video_carousel')
        
        if serp.get('images_pack') and serp['images_pack'].get('position', 100) < position:
            features.append('image_pack')
        
        if serp.get('shopping_results') and any(
            s.get('position', 100) < position for s in serp.get('shopping_results', [])
        ):
            features.append('shopping_results')
        
        return features
    
    def _calculate_visual_position(self, organic_position: int, features_above: List[str]) -> float:
        """Calculate visual position accounting for SERP features."""
        displacement = 0.0
        
        for feature in features_above:
            displacement += self.SERP_FEATURE_WEIGHTS.get(feature, 0.5)
        
        return organic_position + displacement
    
    def _extract_domain(self, url: str) -> str:
        """Extract domain from URL."""
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            # Remove www.
            if domain.startswith('www.'):
                domain = domain[4:]
            return domain
        except Exception:
            return ''
    
    def _is_user_domain(self, domain: str, serp: Dict[str, Any]) -> bool:
        """Check if domain belongs to user."""
        user_domain = serp.get('user_domain', '').lower()
        if user_domain.startswith('www.'):
            user_domain = user_domain[4:]
        return domain == user_domain
    
    def _classify_keyword_intent(self, keyword: str, serp: Dict[str, Any]) -> str:
        """Classify keyword intent based on patterns and SERP features."""
        keyword_lower = keyword.lower()
        
        # Navigational signals
        if any(word in keyword_lower for word in ['login', 'sign in', 'account', 'dashboard']):
            return 'navigational'
        
        # Transactional signals
        if any(word in keyword_lower for word in ['buy', 'purchase', 'price', 'deal', 'discount', 'coupon', 'order']):
            return 'transactional'
        
        # Commercial investigation signals
        if any(word in keyword_lower for word in ['best', 'top', 'review', 'compare', 'vs', 'alternative']):
            return 'commercial'
        
        # Check SERP features for additional signals
        if serp.get('shopping_results'):
            return 'transactional'
        
        if serp.get('knowledge_panel') or len(serp.get('people_also_ask', [])) > 3:
            return 'informational'
        
        # Default to informational for question words
        if any(keyword_lower.startswith(word) for word in ['how', 'what', 'why', 'when', 'who', 'where']):
            return 'informational'
        
        return 'informational'
    
    def _infer_page_type(self, url: str) -> str:
        """Infer page type from URL."""
        url_lower = url.lower()
        
        if any(word in url_lower for word in ['/blog/', '/article/', '/guide/', '/learn/']):
            return 'blog'
        
        if any(word in url_lower for word in ['/product/', '/pricing/', '/buy/', '/shop/']):
            return 'product'
        
        if any(word in url_lower for word in ['/category/', '/collection/']):
            return 'category'
        
        if url_lower.count('/') <= 3:
            return 'homepage'
        
        return 'other'
    
    def _is_intent_mismatch(self, serp_intent: str, page_type: str) -> bool:
        """Check if there's a mismatch between SERP intent and page type."""
        mismatches = {
            'transactional': ['blog'],
            'commercial': ['blog'],
            'informational': ['product']
        }
        
        return page_type in mismatches.get(serp_intent, [])
    
    def _get_mismatch_recommendation(self, serp_intent: str, page_type: str) -> str:
        """Get recommendation for intent mismatch."""
        if serp_intent in ['transactional', 'commercial'] and page_type == 'blog':
            return "Consider creating a dedicated product/comparison page for this keyword"
        
        if serp_intent == 'informational' and page_type == 'product':
            return "Consider adding educational content or creating a supporting blog post"
        
        return "Review content alignment with search intent"
    
    def _generate_summary(
        self,
        displacement: List[Dict[str, Any]],
        competitors: List[Dict[str, Any]],
        click_share: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate summary statistics."""
        return {
            'keywords_with_significant_displacement': len(displacement),
            'avg_visual_displacement': round(
                sum(d['displacement'] for d in displacement) / len(displacement), 1
            ) if displacement else 0,
            'primary_competitors_count': sum(1 for c in competitors if c['threat_level'] in ['high', 'critical']),
            'total_click_share': click_share.get('total_click_share', 0),
            'click_opportunity_size': click_share.get('click_opportunity', 0)
        }
    
    def _empty_result(self) -> Dict[str, Any]:
        """Return empty result structure."""
        return {
            'keywords_analyzed': 0,
            'serp_feature_displacement': [],
            'competitors': [],
            'intent_analysis': {
                'intent_distribution': {
                    'informational': 0,
                    'commercial': 0,
                    'navigational': 0,
                    'transactional': 0
                },
                'intent_mismatches': []
            },
            'click_share': {
                'total_click_share': 0,
                'current_monthly_clicks': 0,
                'potential_monthly_clicks': 0,
                'click_opportunity': 0,
                'keyword_breakdown': []
            },
            'summary': {
                'keywords_with_significant_displacement': 0,
                'avg_visual_displacement': 0,
                'primary_competitors_count': 0,
                'total_click_share': 0,
                'click_opportunity_size': 0
            }
        }


def analyze_serp_landscape(serp_data: List[Dict[str, Any]], gsc_keyword_data: pd.DataFrame) -> Dict[str, Any]:
    """
    Main entry point for SERP landscape analysis.
    
    Args:
        serp_data: List of SERP results from DataForSEO
        gsc_keyword_data: GSC keyword performance data
        
    Returns:
        Complete SERP landscape analysis
    """
    analyzer = SERPLandscapeAnalyzer()
    return analyzer.analyze(serp_data, gsc_keyword_data)
