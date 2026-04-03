import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
import pandas as pd
from collections import defaultdict, Counter
from dataclasses import dataclass, asdict
import re
from urllib.parse import urlparse

from api.services.dataforseo_service import DataForSEOService
from api.services.gsc_service import GSCService

logger = logging.getLogger(__name__)


@dataclass
class SERPFeature:
    """Represents a SERP feature with its characteristics"""
    type: str
    count: int
    visual_position_impact: float


@dataclass
class CompetitorPresence:
    """Represents a competitor's presence in SERPs"""
    domain: str
    appearances: int
    keywords: List[str]
    average_position: float
    positions: List[int]


@dataclass
class KeywordSERPAnalysis:
    """Complete SERP analysis for a single keyword"""
    keyword: str
    user_organic_position: Optional[int]
    user_url: Optional[str]
    visual_position: float
    serp_features: List[SERPFeature]
    competitors: List[Dict[str, Any]]
    intent_classification: str
    click_share_estimate: float
    total_organic_results: int
    displacement_severity: float


class Module3SERPIntelligence:
    """
    Module 3: SERP Landscape Analysis
    
    Integrates with DataForSEO API to analyze:
    - SERP features and their impact on visibility
    - Competitor rankings and overlap
    - Search intent classification
    - Click share estimation
    - Visual position vs organic position displacement
    """
    
    FEATURE_WEIGHTS = {
        'featured_snippet': 2.0,
        'answer_box': 2.0,
        'knowledge_panel': 1.5,
        'knowledge_graph': 1.5,
        'local_pack': 3.0,
        'local_services': 3.0,
        'maps': 2.5,
        'people_also_ask': 0.5,
        'paa': 0.5,
        'video_carousel': 2.0,
        'video': 1.5,
        'image_pack': 1.0,
        'images': 1.0,
        'shopping_results': 2.5,
        'shopping': 2.5,
        'top_stories': 2.0,
        'news': 2.0,
        'twitter': 1.0,
        'reddit': 1.0,
        'ai_overview': 3.0,
        'ai_answer': 3.0,
        'related_searches': 0.5,
        'site_links': 0.5
    }
    
    BASELINE_CTR = {
        1: 0.284, 2: 0.152, 3: 0.098, 4: 0.067, 5: 0.051,
        6: 0.041, 7: 0.034, 8: 0.029, 9: 0.025, 10: 0.022,
        11: 0.018, 12: 0.015, 13: 0.013, 14: 0.012, 15: 0.011,
        16: 0.010, 17: 0.009, 18: 0.009, 19: 0.008, 20: 0.008
    }
    
    def __init__(self, dataforseo_service: DataForSEOService, gsc_service: GSCService):
        self.dataforseo = dataforseo_service
        self.gsc = gsc_service
        
    async def analyze_serp_landscape(
        self,
        site_domain: str,
        top_keywords: List[Dict[str, Any]],
        gsc_keyword_data: pd.DataFrame,
        location_code: int = 2840,
        language_code: str = "en",
        max_keywords: int = 100
    ) -> Dict[str, Any]:
        """
        Main analysis function for SERP landscape.
        
        Args:
            site_domain: User's domain (e.g., "example.com")
            top_keywords: List of top keywords with GSC metrics
            gsc_keyword_data: DataFrame with GSC query performance data
            location_code: DataForSEO location code (default: US)
            language_code: Language code (default: en)
            max_keywords: Maximum keywords to analyze
            
        Returns:
            Complete SERP analysis results
        """
        logger.info(f"Starting SERP landscape analysis for {site_domain}")
        
        # Select keywords to analyze
        keywords_to_analyze = self._select_keywords_for_analysis(
            top_keywords, gsc_keyword_data, max_keywords
        )
        
        logger.info(f"Analyzing {len(keywords_to_analyze)} keywords")
        
        # Fetch SERP data from DataForSEO
        serp_results = await self._fetch_serp_data(
            keywords_to_analyze, location_code, language_code
        )
        
        # Analyze each keyword's SERP
        keyword_analyses = []
        for keyword, serp_data in serp_results.items():
            if serp_data:
                analysis = self._analyze_keyword_serp(
                    keyword, serp_data, site_domain, gsc_keyword_data
                )
                if analysis:
                    keyword_analyses.append(analysis)
        
        logger.info(f"Successfully analyzed {len(keyword_analyses)} keywords")
        
        # Aggregate competitor data
        competitor_analysis = self._aggregate_competitor_data(keyword_analyses)
        
        # Identify SERP feature displacement issues
        displacement_analysis = self._analyze_serp_displacement(keyword_analyses)
        
        # Classify search intents and identify mismatches
        intent_analysis = self._analyze_intent_classification(keyword_analyses, site_domain)
        
        # Calculate overall click share
        click_share_analysis = self._calculate_click_share(keyword_analyses, gsc_keyword_data)
        
        # Generate visibility trends
        visibility_trends = self._analyze_visibility_trends(keyword_analyses, gsc_keyword_data)
        
        # Compile final results
        results = {
            "keywords_analyzed": len(keyword_analyses),
            "analysis_date": datetime.utcnow().isoformat(),
            "site_domain": site_domain,
            "competitor_analysis": competitor_analysis,
            "serp_features": displacement_analysis,
            "intent_classification": intent_analysis,
            "click_share": click_share_analysis,
            "visibility_trends": visibility_trends,
            "detailed_keyword_data": [
                self._keyword_analysis_to_dict(ka) for ka in keyword_analyses[:20]
            ]
        }
        
        logger.info("SERP landscape analysis complete")
        return results
    
    def _select_keywords_for_analysis(
        self,
        top_keywords: List[Dict[str, Any]],
        gsc_data: pd.DataFrame,
        max_keywords: int
    ) -> List[str]:
        """
        Select the most important keywords to analyze.
        
        Prioritizes:
        1. High impression keywords
        2. Keywords with significant position changes
        3. Non-branded keywords
        """
        keywords = []
        
        # Get branded terms to filter out
        branded_patterns = self._get_branded_patterns(top_keywords)
        
        # Sort by impressions and filter
        for kw_data in top_keywords:
            keyword = kw_data.get('keys', [''])[0] if isinstance(kw_data.get('keys'), list) else kw_data.get('query', '')
            
            if not keyword:
                continue
                
            # Filter out branded
            if self._is_branded(keyword, branded_patterns):
                continue
            
            impressions = kw_data.get('impressions', 0)
            
            if impressions > 10:  # Minimum threshold
                keywords.append(keyword)
        
        # Also add keywords with significant position changes
        if not gsc_data.empty and 'query' in gsc_data.columns:
            recent_data = gsc_data[gsc_data['date'] >= (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')]
            older_data = gsc_data[
                (gsc_data['date'] >= (datetime.now() - timedelta(days=60)).strftime('%Y-%m-%d')) &
                (gsc_data['date'] < (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'))
            ]
            
            if not recent_data.empty and not older_data.empty:
                recent_avg = recent_data.groupby('query')['position'].mean()
                older_avg = older_data.groupby('query')['position'].mean()
                position_changes = (recent_avg - older_avg).abs()
                
                significant_changes = position_changes[position_changes > 3].index.tolist()
                for kw in significant_changes:
                    if kw not in keywords and not self._is_branded(kw, branded_patterns):
                        keywords.append(kw)
        
        # Return top N by impressions
        return keywords[:max_keywords]
    
    def _get_branded_patterns(self, top_keywords: List[Dict[str, Any]]) -> List[str]:
        """Extract branded terms from the data"""
        branded = set()
        
        for kw_data in top_keywords:
            keyword = kw_data.get('keys', [''])[0] if isinstance(kw_data.get('keys'), list) else kw_data.get('query', '')
            
            # Simple heuristic: very high CTR likely indicates branded
            ctr = kw_data.get('ctr', 0)
            if ctr > 0.3:  # 30%+ CTR is typically branded
                branded.add(keyword.lower())
        
        return list(branded)
    
    def _is_branded(self, keyword: str, branded_patterns: List[str]) -> bool:
        """Check if a keyword is branded"""
        keyword_lower = keyword.lower()
        
        # Check exact matches
        if keyword_lower in branded_patterns:
            return True
        
        # Check if any branded term is in the keyword
        for brand in branded_patterns:
            if brand in keyword_lower or keyword_lower in brand:
                return True
        
        return False
    
    async def _fetch_serp_data(
        self,
        keywords: List[str],
        location_code: int,
        language_code: str
    ) -> Dict[str, Any]:
        """
        Fetch SERP data from DataForSEO for all keywords.
        """
        results = {}
        
        for keyword in keywords:
            try:
                serp_data = await self.dataforseo.get_serp_results(
                    keyword=keyword,
                    location_code=location_code,
                    language_code=language_code
                )
                results[keyword] = serp_data
            except Exception as e:
                logger.error(f"Error fetching SERP data for '{keyword}': {str(e)}")
                results[keyword] = None
        
        return results
    
    def _analyze_keyword_serp(
        self,
        keyword: str,
        serp_data: Dict[str, Any],
        site_domain: str,
        gsc_data: pd.DataFrame
    ) -> Optional[KeywordSERPAnalysis]:
        """
        Analyze SERP data for a single keyword.
        """
        try:
            # Extract organic results
            items = serp_data.get('items', [])
            if not items:
                return None
            
            organic_results = [
                item for item in items[0].get('items', [])
                if item.get('type') == 'organic'
            ]
            
            # Find user's position
            user_position = None
            user_url = None
            
            for i, result in enumerate(organic_results, 1):
                result_url = result.get('url', '')
                result_domain = urlparse(result_url).netloc.replace('www.', '')
                
                if site_domain.replace('www.', '') in result_domain:
                    user_position = i
                    user_url = result_url
                    break
            
            # Extract SERP features
            serp_features = self._extract_serp_features(items[0].get('items', []))
            
            # Calculate visual position
            visual_position = self._calculate_visual_position(user_position, serp_features)
            
            # Extract competitors
            competitors = self._extract_competitors(organic_results, site_domain)
            
            # Classify intent
            intent = self._classify_intent(keyword, serp_features, organic_results)
            
            # Estimate click share
            click_share = self._estimate_click_share(user_position, serp_features, gsc_data, keyword)
            
            # Calculate displacement severity
            displacement = 0.0
            if user_position:
                displacement = visual_position - user_position
            
            return KeywordSERPAnalysis(
                keyword=keyword,
                user_organic_position=user_position,
                user_url=user_url,
                visual_position=visual_position,
                serp_features=serp_features,
                competitors=competitors,
                intent_classification=intent,
                click_share_estimate=click_share,
                total_organic_results=len(organic_results),
                displacement_severity=displacement
            )
            
        except Exception as e:
            logger.error(f"Error analyzing SERP for '{keyword}': {str(e)}")
            return None
    
    def _extract_serp_features(self, items: List[Dict[str, Any]]) -> List[SERPFeature]:
        """
        Extract and categorize SERP features from results.
        """
        features = []
        feature_counts = defaultdict(int)
        
        for item in items:
            item_type = item.get('type', '').lower()
            
            # Map DataForSEO types to our feature types
            feature_type = self._map_feature_type(item_type, item)
            
            if feature_type:
                feature_counts[feature_type] += 1
        
        # Convert to SERPFeature objects
        for feature_type, count in feature_counts.items():
            weight = self.FEATURE_WEIGHTS.get(feature_type, 1.0)
            
            # For PAA, multiply weight by count
            if feature_type in ['people_also_ask', 'paa']:
                impact = weight * count
            else:
                impact = weight
            
            features.append(SERPFeature(
                type=feature_type,
                count=count,
                visual_position_impact=impact
            ))
        
        return features
    
    def _map_feature_type(self, item_type: str, item: Dict[str, Any]) -> Optional[str]:
        """
        Map DataForSEO item types to our standardized feature types.
        """
        type_mapping = {
            'featured_snippet': 'featured_snippet',
            'answer_box': 'answer_box',
            'knowledge_graph': 'knowledge_graph',
            'local_pack': 'local_pack',
            'people_also_ask': 'people_also_ask',
            'video': 'video_carousel',
            'images': 'image_pack',
            'shopping': 'shopping_results',
            'top_stories': 'top_stories',
            'twitter': 'twitter',
            'map': 'maps',
            'related_searches': 'related_searches'
        }
        
        # Check for AI Overview (might be marked as different types)
        if 'ai' in item_type or 'gemini' in item_type.lower():
            return 'ai_overview'
        
        # Check for Reddit results
        if 'reddit' in item.get('url', '').lower():
            return 'reddit'
        
        return type_mapping.get(item_type)
    
    def _calculate_visual_position(
        self,
        organic_position: Optional[int],
        serp_features: List[SERPFeature]
    ) -> float:
        """
        Calculate effective visual position accounting for SERP features.
        """
        if not organic_position:
            return 0.0
        
        # Sum up the visual impact of all features
        total_displacement = sum(f.visual_position_impact for f in serp_features)
        
        return organic_position + total_displacement
    
    def _extract_competitors(
        self,
        organic_results: List[Dict[str, Any]],
        site_domain: str
    ) -> List[Dict[str, Any]]:
        """
        Extract competitor information from organic results.
        """
        competitors = []
        site_domain_clean = site_domain.replace('www.', '')
        
        for i, result in enumerate(organic_results[:10], 1):
            url = result.get('url', '')
            domain = urlparse(url).netloc.replace('www.', '')
            
            # Skip user's own domain
            if site_domain_clean in domain:
                continue
            
            competitors.append({
                'domain': domain,
                'position': i,
                'url': url,
                'title': result.get('title', ''),
                'description': result.get('description', '')
            })
        
        return competitors
    
    def _classify_intent(
        self,
        keyword: str,
        serp_features: List[SERPFeature],
        organic_results: List[Dict[str, Any]]
    ) -> str:
        """
        Classify search intent based on keyword and SERP composition.
        """
        keyword_lower = keyword.lower()
        
        # Intent signals from keyword patterns
        informational_patterns = [
            'how to', 'what is', 'why', 'when', 'where', 'who',
            'guide', 'tutorial', 'learn', 'meaning', 'definition'
        ]
        
        commercial_patterns = [
            'best', 'top', 'review', 'vs', 'compare', 'alternative',
            'recommendation', 'rating'
        ]
        
        transactional_patterns = [
            'buy', 'price', 'cheap', 'deal', 'discount', 'coupon',
            'order', 'purchase', 'shop', 'store'
        ]
        
        navigational_patterns = [
            'login', 'sign in', 'account', 'portal', 'official'
        ]
        
        # Check keyword patterns
        if any(pattern in keyword_lower for pattern in transactional_patterns):
            return 'transactional'
        
        if any(pattern in keyword_lower for pattern in navigational_patterns):
            return 'navigational'
        
        if any(pattern in keyword_lower for pattern in commercial_patterns):
            return 'commercial'
        
        if any(pattern in keyword_lower for pattern in informational_patterns):
            return 'informational'
        
        # Check SERP features for intent signals
        feature_types = [f.type for f in serp_features]
        
        if 'shopping_results' in feature_types or 'shopping' in feature_types:
            return 'transactional'
        
        if 'local_pack' in feature_types or 'maps' in feature_types:
            return 'local'
        
        if any(f in feature_types for f in ['featured_snippet', 'people_also_ask', 'knowledge_graph']):
            # Heavy informational features
            paa_count = sum(f.count for f in serp_features if f.type in ['people_also_ask', 'paa'])
            if paa_count >= 3:
                return 'informational'
        
        # Default to commercial if unclear
        return 'commercial'
    
    def _estimate_click_share(
        self,
        position: Optional[int],
        serp_features: List[SERPFeature],
        gsc_data: pd.DataFrame,
        keyword: str
    ) -> float:
        """
        Estimate the user's click share for this keyword.
        """
        if not position or position > 20:
            return 0.0
        
        # Start with baseline CTR for position
        base_ctr = self.BASELINE_CTR.get(position, 0.005)
        
        # Adjust for SERP features (they steal clicks)
        feature_impact = sum(f.visual_position_impact for f in serp_features)
        
        # Reduce CTR based on feature displacement
        # Each "position" of displacement reduces CTR by approximately 15%
        adjustment_factor = 0.85 ** feature_impact
        
        adjusted_ctr = base_ctr * adjustment_factor
        
        # Get actual CTR from GSC if available
        if not gsc_data.empty and 'query' in gsc_data.columns:
            keyword_data = gsc_data[gsc_data['query'] == keyword]
            if not keyword_data.empty:
                actual_ctr = keyword_data['ctr'].mean()
                # Blend estimated with actual (70% actual, 30% estimated)
                adjusted_ctr = (actual_ctr * 0.7) + (adjusted_ctr * 0.3)
        
        return round(adjusted_ctr, 4)
    
    def _aggregate_competitor_data(
        self,
        keyword_analyses: List[KeywordSERPAnalysis]
    ) -> Dict[str, Any]:
        """
        Aggregate competitor presence across all analyzed keywords.
        """
        competitor_data = defaultdict(lambda: {
            'appearances': 0,
            'keywords': [],
            'positions': []
        })
        
        for analysis in keyword_analyses:
            for comp in analysis.competitors:
                domain = comp['domain']
                competitor_data[domain]['appearances'] += 1
                competitor_data[domain]['keywords'].append(analysis.keyword)
                competitor_data[domain]['positions'].append(comp['position'])
        
        # Calculate statistics and filter significant competitors
        competitors = []
        total_keywords = len(keyword_analyses)
        
        for domain, data in competitor_data.items():
            if data['appearances'] < 2:  # Minimum threshold
                continue
            
            avg_position = sum(data['positions']) / len(data['positions'])
            overlap_pct = (data['appearances'] / total_keywords) * 100
            
            # Determine threat level
            threat_level = 'low'
            if overlap_pct > 40 and avg_position < 5:
                threat_level = 'high'
            elif overlap_pct > 20 or avg_position < 3:
                threat_level = 'medium'
            
            competitors.append({
                'domain': domain,
                'keywords_shared': data['appearances'],
                'overlap_percentage': round(overlap_pct, 1),
                'avg_position': round(avg_position, 1),
                'best_position': min(data['positions']),
                'threat_level': threat_level,
                'sample_keywords': data['keywords'][:5]
            })
        
        # Sort by overlap percentage
        competitors.sort(key=lambda x: x['overlap_percentage'], reverse=True)
        
        return {
            'total_competitors_found': len(competitors),
            'primary_competitors': competitors[:10],
            'competitor_concentration': self._calculate_competitor_concentration(competitors),
            'market_leader': competitors[0] if competitors else None
        }
    
    def _calculate_competitor_concentration(
        self,
        competitors: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Calculate market concentration metrics.
        """
        if not competitors:
            return {'herfindahl_index': 0, 'top3_share': 0, 'top5_share': 0}
        
        total_appearances = sum(c['keywords_shared'] for c in competitors)
        
        # Herfindahl index (market concentration)
        herfindahl = sum((c['keywords_shared'] / total_appearances) ** 2 for c in competitors)
        
        # Top N market shares
        top3_share = sum(c['keywords_shared'] for c in competitors[:3]) / total_appearances * 100
        top5_share = sum(c['keywords_shared'] for c in competitors[:5]) / total_appearances * 100
        
        return {
            'herfindahl_index': round(herfindahl, 3),
            'top3_share': round(top3_share, 1),
            'top5_share': round(top5_share, 1),
            'market_type': 'concentrated' if herfindahl > 0.25 else 'fragmented'
        }
    
    def _analyze_serp_displacement(
        self,
        keyword_analyses: List[KeywordSERPAnalysis]
    ) -> Dict[str, Any]:
        """
        Analyze SERP feature displacement impact.
        """
        displaced_keywords = []
        feature_frequency = defaultdict(int)
        total_displacement = 0
        
        for analysis in keyword_analyses:
            if analysis.user_organic_position and analysis.displacement_severity > 2:
                # Significant displacement
                features_above = [f.type for f in analysis.serp_features]
                
                # Estimate CTR impact
                original_ctr = self.BASELINE_CTR.get(analysis.user_organic_position, 0.01)
                actual_ctr = analysis.click_share_estimate
                ctr_impact = actual_ctr - original_ctr
                
                displaced_keywords.append({
                    'keyword': analysis.keyword,
                    'organic_position': analysis.user_organic_position,
                    'visual_position': round(analysis.visual_position, 1),
                    'displacement': round(analysis.displacement_severity, 1),
                    'features_above': features_above,
                    'estimated_ctr_impact': round(ctr_impact, 4)
                })
                
                total_displacement += analysis.displacement_severity
            
            # Count feature frequency
            for feature in analysis.serp_features:
                feature_frequency[feature.type] += 1
        
        # Sort by displacement severity
        displaced_keywords.sort(key=lambda x: x['displacement'], reverse=True)
        
        # Calculate feature statistics
        total_keywords = len(keyword_analyses)
        feature_stats = [
            {
                'feature_type': feature,
                'frequency': count,
                'percentage': round((count / total_keywords) * 100, 1),
                'avg_impact': self.FEATURE_WEIGHTS.get(feature, 1.0)
            }
            for feature, count in feature_frequency.items()
        ]
        feature_stats.sort(key=lambda x: x['frequency'], reverse=True)
        
        return {
            'total_displaced_keywords': len(displaced_keywords),
            'avg_displacement': round(total_displacement / max(len(displaced_keywords), 1), 2),
            'displaced_keywords': displaced_keywords[:20],
            'feature_frequency': feature_stats,
            'most_impactful_features': feature_stats[:5],
            'recommendations': self._generate_displacement_recommendations(feature_stats)
        }
    
    def _generate_displacement_recommendations(
        self,
        feature_stats: List[Dict[str, Any]]
    ) -> List[str]:
        """
        Generate recommendations based on SERP feature patterns.
        """
        recommendations = []
        
        for stat in feature_stats[:5]:
            feature = stat['feature_type']
            frequency = stat['percentage']
            
            if frequency > 30:  # Feature appears in >30% of keywords
                if feature in ['people_also_ask', 'paa']:
                    recommendations.append(
                        f"Add FAQ schema to target pages - PAA boxes appear in {frequency}% of your keywords"
                    )
                elif feature == 'featured_snippet':
                    recommendations.append(
                        f"Optimize content for featured snippets - they appear in {frequency}% of searches"
                    )
                elif feature in ['video_carousel', 'video']:
                    recommendations.append(
                        f"Consider video content - video features appear in {frequency}% of your keywords"
                    )
                elif feature == 'local_pack':
                    recommendations.append(
                        f"Optimize Google Business Profile - local pack appears in {frequency}% of keywords"
                    )
                elif feature == 'ai_overview':
                    recommendations.append(
                        f"Monitor AI Overview impact - appearing in {frequency}% of searches and reducing organic CTR"
                    )
        
        return recommendations
    
    def _analyze_intent_classification(
        self,
        keyword_analyses: List[KeywordSERPAnalysis],
        site_domain: str
    ) -> Dict[str, Any]:
        """
        Analyze search intent distribution and identify mismatches.
        """
        intent_distribution = Counter(a.intent_classification for a in keyword_analyses)
        
        # Analyze intent vs performance
        intent_performance = defaultdict(lambda: {
            'keywords': [],
            'avg_position': [],
            'avg_click_share': []
        })
        
        for analysis in keyword_analyses:
            intent = analysis.intent_classification
            intent_performance[intent]['keywords'].append(analysis.keyword)
            
            if analysis.user_organic_position:
                intent_performance[intent]['avg_position'].append(analysis.user_organic_position)
            
            intent_performance[intent]['avg_click_share'].append(analysis.click_share_estimate)
        
        # Calculate averages
        intent_summary = []
        for intent, data in intent_performance.items():
            avg_pos = sum(data['avg_position']) / len(data['avg_position']) if data['avg_position'] else None
            avg_clicks = sum(data['avg_click_share']) / len(data['avg_click_share'])
            
            intent_summary.append({
                'intent': intent,
                'keyword_count': len(data['keywords']),
                'percentage': round((len(data['keywords']) / len(keyword_analyses)) * 100, 1),
                'avg_position': round(avg_pos, 1) if avg_pos else None,
                'avg_click_share': round(avg_clicks, 4),
                'sample_keywords': data['keywords'][:3]
            })
        
        intent_summary.sort(key=lambda x: x['keyword_count'], reverse=True)
        
        return {
            'intent_distribution': dict(intent_distribution),
            'intent_breakdown': intent_summary,
            'dominant_intent': intent_summary[0]['intent'] if intent_summary else None,
            'intent_diversity_score': len(intent_distribution) / 5.0  # Max 5 intent types
        }
    
    def _calculate_click_share(
        self,
        keyword_analyses: List[KeywordSERPAnalysis],
        gsc_data: pd.DataFrame
    ) -> Dict[str, Any]:
        """
        Calculate overall click share and opportunity.
        """
        total_click_share = 0
        potential_click_share = 0
        keywords_with_position = 0
        
        for analysis in keyword_analyses:
            if analysis.user_organic_position:
                keywords_with_position += 1
                total_click_share += analysis.click_share_estimate
                
                # Calculate potential if at position 1
                potential = self.BASELINE_CTR[1]
                potential_click_share += potential
        
        avg_click_share = total_click_share / max(keywords_with_position, 1)
        avg_potential = potential_click_share / max(keywords_with_position, 1)
        opportunity = avg_potential - avg_click_share
        
        # Calculate weighted click share based on impressions
        if not gsc_data.empty and 'query' in gsc_data.columns:
            weighted_share = 0
            weighted_potential = 0
            total_impressions = 0
            
            for analysis in keyword_analyses:
                if analysis.user_organic_position:
                    keyword_data = gsc_data[gsc_data['query'] == analysis.keyword]
                    if not keyword_data.empty:
                        impressions = keyword_data['impressions'].sum()
                        total_impressions += impressions
                        weighted_share += analysis.click_share_estimate * impressions
                        weighted_potential += self.BASELINE_CTR[1] * impressions
            
            if total_impressions > 0:
                avg_click_share = weighted_share / total_impressions
                avg_potential = weighted_potential / total_impressions
                opportunity = avg_potential - avg_click_share
        
        return {
            'total_click_share': round(avg_click_share, 4),
            'potential_click_share': round(avg_potential, 4),
            'click_share_opportunity': round(opportunity, 4),
            'opportunity_percentage': round((opportunity / avg_potential) * 100, 1) if avg_potential > 0 else 0,
            'keywords_ranking': keywords_with_position,
            'keywords_not_ranking': len(keyword_analyses) - keywords_with_position
        }
    
    def _analyze_visibility_trends(
        self,
        keyword_analyses: List[KeywordSERPAnalysis],
        gsc_data: pd.DataFrame
    ) -> Dict[str, Any]:
        """
        Analyze visibility trends across different dimensions.
        """
        # Position distribution
        position_buckets = {
            '1-3': 0, '4-10': 0, '11-20': 0, '21+': 0, 'not_ranking': 0
        }
        
        for analysis in keyword_analyses:
            pos = analysis.user_organic_position
            if not pos:
                position_buckets['not_ranking'] += 1
            elif pos <= 3:
                position_buckets['1-3'] += 1
            elif pos <= 10:
                position_buckets['4-10'] += 1
            elif pos <= 20:
                position_buckets['11-20'] += 1
            else:
                position_buckets['21+'] += 1
        
        # Calculate visibility score (weighted by position quality)
        visibility_score = (
            position_buckets['1-3'] * 1.0 +
            position_buckets['4-10'] * 0.5 +
            position_buckets['11-20'] * 0.1
        ) / len(keyword_analyses)
        
        # Analyze by intent
        intent_visibility = defaultdict(lambda: {'ranking': 0, 'not_ranking': 0})
        for analysis in keyword_analyses:
            intent = analysis.intent_classification
            if analysis.user_organic_position:
                intent_visibility[intent]['ranking'] += 1
            else:
                intent_visibility[intent]['not_ranking'] += 1
        
        return {
            'visibility_score': round(visibility_score, 3),
            'position_distribution': position_buckets,
            'page_one_keywords': position_buckets['1-3'] + position_buckets['4-10'],
            'page_one_percentage': round(
                ((position_buckets['1-3'] + position_buckets['4-10']) / len(keyword_analyses)) * 100, 1
            ),
            'intent_visibility': dict(intent_visibility),
            'opportunities': {
                'striking_distance': position_buckets['11-20'],
                'quick_wins': position_buckets['4-10'],
                'top_positions': position_buckets['1-3']
            }
        }
    
    def _keyword_analysis_to_dict(self, analysis: KeywordSERPAnalysis) -> Dict[str, Any]:
        """
        Convert KeywordSERPAnalysis to dictionary for JSON serialization.
        """
        return {
            'keyword': analysis.keyword,
            'user_organic_position': analysis.user_organic_position,
            'user_url': analysis.user_url,
            'visual_position': round(analysis.visual_position, 1),
            'displacement_severity': round(analysis.displacement_severity, 1),
            'serp_features': [
                {
                    'type': f.type,
                    'count': f.count,
                    'impact': round(f.visual_position_impact, 1)
                }
                for f in analysis.serp_features
            ],
            'top_competitors': analysis.competitors[:5],
            'intent': analysis.intent_classification,
            'click_share_estimate': analysis.click_share_estimate,
            'total_organic_results': analysis.total_organic_results
        }


async def run_module_3(
    dataforseo_service: DataForSEOService,
    gsc_service: GSCService,
    site_domain: str,
    top_keywords: List[Dict[str, Any]],
    gsc_keyword_data: pd.DataFrame,
    location_code: int = 2840,
    language_code: str = "en",
    max_keywords: int = 100
) -> Dict[str, Any]:
    """
    Main entry point for Module 3 analysis.
    
    Args:
        dataforseo_service: Initialized DataForSEO service
        gsc_service: Initialized GSC service
        site_domain: User's domain
        top_keywords: List of top keywords from GSC
        gsc_keyword_data: DataFrame with GSC query data
        location_code: DataForSEO location code
        language_code: Language code
        max_keywords: Maximum keywords to analyze
        
    Returns:
        Complete SERP intelligence analysis
    """
    module = Module3SERPIntelligence(dataforseo_service, gsc_service)
    
    results = await module.analyze_serp_landscape(
        site_domain=site_domain,
        top_keywords=top_keywords,
        gsc_keyword_data=gsc_keyword_data,
        location_code=location_code,
        language_code=language_code,
        max_keywords=max_keywords
    )
    
    return results