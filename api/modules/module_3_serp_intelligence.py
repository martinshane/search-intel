import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
import pandas as pd
from collections import defaultdict, Counter
from dataclasses import dataclass
import re

from api.services.dataforseo_service import DataForSEOService
from api.services.gsc_service import GSCService

logger = logging.getLogger(__name__)


@dataclass
class SERPFeature:
    """Represents a SERP feature with its characteristics"""
    type: str
    count: int  # For features like PAA that can have multiple items
    visual_position_impact: float  # How many "positions" this pushes organic results down


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
    
    # SERP feature visual impact weights (how many "positions" each feature consumes)
    FEATURE_WEIGHTS = {
        'featured_snippet': 2.0,
        'answer_box': 2.0,
        'knowledge_panel': 1.5,
        'knowledge_graph': 1.5,
        'local_pack': 3.0,
        'local_services': 3.0,
        'maps': 2.5,
        'people_also_ask': 0.5,  # Per question
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
    
    # Position-based CTR curves (baseline, adjusted for features)
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
        max_keywords: int = 100
    ) -> Dict[str, Any]:
        """
        Main analysis function for SERP landscape.
        
        Args:
            site_domain: User's domain
            top_keywords: List of top keywords from GSC with impressions/clicks
            gsc_keyword_data: Full GSC keyword performance data
            max_keywords: Maximum keywords to analyze
            
        Returns:
            Complete SERP landscape analysis
        """
        logger.info(f"Starting SERP landscape analysis for {site_domain}")
        
        # Filter and prepare keywords for analysis
        keywords_to_analyze = self._select_keywords_for_analysis(
            top_keywords, 
            gsc_keyword_data, 
            max_keywords
        )
        
        logger.info(f"Analyzing {len(keywords_to_analyze)} keywords")
        
        # Fetch SERP data for all keywords
        serp_results = await self._fetch_serp_data(keywords_to_analyze)
        
        # Analyze each keyword's SERP
        keyword_analyses = []
        for keyword_data in keywords_to_analyze:
            keyword = keyword_data['query']
            serp_data = serp_results.get(keyword)
            
            if not serp_data:
                logger.warning(f"No SERP data for keyword: {keyword}")
                continue
                
            analysis = self._analyze_keyword_serp(
                keyword=keyword,
                serp_data=serp_data,
                site_domain=site_domain,
                gsc_data=keyword_data
            )
            
            if analysis:
                keyword_analyses.append(analysis)
        
        # Aggregate competitor intelligence
        competitor_map = self._build_competitor_map(keyword_analyses, site_domain)
        
        # Identify SERP feature displacement issues
        displacement_analysis = self._analyze_displacement(keyword_analyses)
        
        # Classify search intents and find mismatches
        intent_analysis = self._analyze_intent_landscape(keyword_analyses, gsc_keyword_data)
        
        # Calculate overall click share
        click_share_metrics = self._calculate_click_share_metrics(keyword_analyses, gsc_keyword_data)
        
        # Generate insights and recommendations
        insights = self._generate_insights(
            keyword_analyses,
            competitor_map,
            displacement_analysis,
            intent_analysis,
            click_share_metrics
        )
        
        return {
            'keywords_analyzed': len(keyword_analyses),
            'serp_feature_displacement': displacement_analysis,
            'competitors': competitor_map,
            'intent_analysis': intent_analysis,
            'click_share_metrics': click_share_metrics,
            'keyword_details': [self._serialize_keyword_analysis(ka) for ka in keyword_analyses],
            'insights': insights,
            'recommendations': self._generate_recommendations(
                competitor_map,
                displacement_analysis,
                intent_analysis
            )
        }
    
    def _select_keywords_for_analysis(
        self,
        top_keywords: List[Dict[str, Any]],
        gsc_data: pd.DataFrame,
        max_keywords: int
    ) -> List[Dict[str, Any]]:
        """
        Select the most important keywords for SERP analysis.
        
        Priority:
        1. High impression keywords (top traffic drivers)
        2. Keywords with significant position changes
        3. Keywords in striking distance (position 8-20)
        4. Branded keywords (to understand brand SERP control)
        """
        selected = []
        
        # Sort by impressions
        sorted_keywords = sorted(
            top_keywords,
            key=lambda x: x.get('impressions', 0),
            reverse=True
        )
        
        # Take top impression keywords
        high_volume = sorted_keywords[:max_keywords // 2]
        selected.extend(high_volume)
        
        # Add keywords with position changes if available
        if 'position_change' in gsc_data.columns:
            position_movers = gsc_data[
                gsc_data['position_change'].abs() > 3
            ].nlargest(max_keywords // 4, 'impressions')
            
            for _, row in position_movers.iterrows():
                if len(selected) >= max_keywords:
                    break
                if row['query'] not in [k['query'] for k in selected]:
                    selected.append(row.to_dict())
        
        # Add striking distance keywords
        striking_distance = [
            k for k in sorted_keywords
            if 8 <= k.get('position', 999) <= 20
            and k['query'] not in [s['query'] for s in selected]
        ]
        
        remaining_slots = max_keywords - len(selected)
        selected.extend(striking_distance[:remaining_slots])
        
        return selected[:max_keywords]
    
    async def _fetch_serp_data(
        self,
        keywords: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Fetch SERP data for all keywords via DataForSEO.
        """
        serp_results = {}
        
        # Batch keywords for efficient API usage
        batch_size = 20
        for i in range(0, len(keywords), batch_size):
            batch = keywords[i:i + batch_size]
            keyword_list = [k['query'] for k in batch]
            
            try:
                results = await self.dataforseo.get_serp_results(keyword_list)
                serp_results.update(results)
            except Exception as e:
                logger.error(f"Error fetching SERP data for batch: {e}")
                continue
        
        return serp_results
    
    def _analyze_keyword_serp(
        self,
        keyword: str,
        serp_data: Dict[str, Any],
        site_domain: str,
        gsc_data: Dict[str, Any]
    ) -> Optional[KeywordSERPAnalysis]:
        """
        Analyze a single keyword's SERP composition.
        """
        try:
            # Extract SERP features
            features = self._extract_serp_features(serp_data)
            
            # Find user's position
            user_position, user_url = self._find_user_position(serp_data, site_domain)
            
            # Calculate visual position
            visual_position = self._calculate_visual_position(user_position, features)
            
            # Extract competitors
            competitors = self._extract_competitors(serp_data, site_domain)
            
            # Classify intent
            intent = self._classify_serp_intent(serp_data, features)
            
            # Estimate click share
            click_share = self._estimate_click_share(
                user_position,
                visual_position,
                features,
                len(competitors)
            )
            
            # Calculate displacement severity
            displacement = visual_position - user_position if user_position else 0
            
            return KeywordSERPAnalysis(
                keyword=keyword,
                user_organic_position=user_position,
                user_url=user_url,
                visual_position=visual_position,
                serp_features=features,
                competitors=competitors,
                intent_classification=intent,
                click_share_estimate=click_share,
                total_organic_results=len(competitors),
                displacement_severity=displacement
            )
            
        except Exception as e:
            logger.error(f"Error analyzing SERP for keyword '{keyword}': {e}")
            return None
    
    def _extract_serp_features(self, serp_data: Dict[str, Any]) -> List[SERPFeature]:
        """
        Extract and categorize SERP features from DataForSEO response.
        """
        features = []
        items = serp_data.get('items', [])
        
        if not items:
            return features
        
        result = items[0]
        serp_items = result.get('items', [])
        
        feature_counts = defaultdict(int)
        
        for item in serp_items:
            item_type = item.get('type', '').lower()
            rank_group = item.get('rank_group', 0)
            
            # Map DataForSEO types to our feature categories
            if item_type == 'featured_snippet':
                feature_counts['featured_snippet'] += 1
            elif item_type == 'answer_box':
                feature_counts['answer_box'] += 1
            elif item_type == 'knowledge_graph' or item_type == 'knowledge_panel':
                feature_counts['knowledge_panel'] += 1
            elif item_type == 'local_pack':
                feature_counts['local_pack'] += 1
            elif item_type == 'people_also_ask':
                feature_counts['people_also_ask'] += 1
            elif 'video' in item_type:
                feature_counts['video_carousel'] += 1
            elif 'image' in item_type:
                feature_counts['image_pack'] += 1
            elif 'shopping' in item_type or item_type == 'google_shopping':
                feature_counts['shopping_results'] += 1
            elif item_type == 'top_stories' or 'news' in item_type:
                feature_counts['top_stories'] += 1
            elif 'twitter' in item_type or 'tweets' in item_type:
                feature_counts['twitter'] += 1
            elif 'reddit' in item_type:
                feature_counts['reddit'] += 1
            elif 'ai' in item_type or 'generative' in item_type:
                feature_counts['ai_overview'] += 1
            elif item_type == 'related_searches':
                feature_counts['related_searches'] += 1
        
        # Convert to SERPFeature objects
        for feature_type, count in feature_counts.items():
            weight = self.FEATURE_WEIGHTS.get(feature_type, 1.0)
            features.append(SERPFeature(
                type=feature_type,
                count=count,
                visual_position_impact=weight * count
            ))
        
        return features
    
    def _find_user_position(
        self,
        serp_data: Dict[str, Any],
        site_domain: str
    ) -> Tuple[Optional[int], Optional[str]]:
        """
        Find the user's organic ranking position.
        """
        items = serp_data.get('items', [])
        if not items:
            return None, None
        
        result = items[0]
        serp_items = result.get('items', [])
        
        # Normalize domain for comparison
        normalized_domain = site_domain.lower().replace('www.', '')
        
        for item in serp_items:
            # Only consider organic results
            if item.get('type') != 'organic':
                continue
            
            url = item.get('url', '')
            domain = item.get('domain', '')
            
            # Check if this is the user's domain
            if normalized_domain in domain.lower() or normalized_domain in url.lower():
                position = item.get('rank_absolute')
                return position, url
        
        return None, None
    
    def _calculate_visual_position(
        self,
        organic_position: Optional[int],
        features: List[SERPFeature]
    ) -> float:
        """
        Calculate effective visual position considering SERP features.
        """
        if not organic_position:
            return 999.0
        
        # Sum up the visual impact of features above the organic result
        # Assume features appear before organic results typically
        total_impact = sum(f.visual_position_impact for f in features)
        
        # Visual position = organic position + feature displacement
        return organic_position + total_impact
    
    def _extract_competitors(
        self,
        serp_data: Dict[str, Any],
        site_domain: str
    ) -> List[Dict[str, Any]]:
        """
        Extract competitor domains and their positions from SERP.
        """
        competitors = []
        items = serp_data.get('items', [])
        
        if not items:
            return competitors
        
        result = items[0]
        serp_items = result.get('items', [])
        
        normalized_domain = site_domain.lower().replace('www.', '')
        
        for item in serp_items:
            if item.get('type') != 'organic':
                continue
            
            domain = item.get('domain', '')
            url = item.get('url', '')
            position = item.get('rank_absolute')
            title = item.get('title', '')
            
            # Skip user's own domain
            if normalized_domain in domain.lower():
                continue
            
            competitors.append({
                'domain': domain,
                'url': url,
                'position': position,
                'title': title
            })
        
        return competitors
    
    def _classify_serp_intent(
        self,
        serp_data: Dict[str, Any],
        features: List[SERPFeature]
    ) -> str:
        """
        Classify the search intent based on SERP composition.
        
        Intent types:
        - informational: Heavy on knowledge panels, PAA, featured snippets
        - commercial: Reviews, comparisons, shopping results
        - transactional: Shopping ads/results, local packs
        - navigational: Site links, brand knowledge panel
        """
        feature_types = [f.type for f in features]
        
        # Scoring system
        scores = {
            'informational': 0,
            'commercial': 0,
            'transactional': 0,
            'navigational': 0
        }
        
        # Informational signals
        if 'people_also_ask' in feature_types:
            scores['informational'] += 3
        if 'featured_snippet' in feature_types or 'answer_box' in feature_types:
            scores['informational'] += 2
        if 'knowledge_panel' in feature_types:
            scores['informational'] += 2
        
        # Commercial signals
        if 'video_carousel' in feature_types:
            scores['commercial'] += 2
        if 'top_stories' in feature_types:
            scores['informational'] += 1
        
        # Transactional signals
        if 'shopping_results' in feature_types:
            scores['transactional'] += 3
        if 'local_pack' in feature_types:
            scores['transactional'] += 2
        
        # Check keyword patterns from SERP titles
        items = serp_data.get('items', [])
        if items:
            result = items[0]
            serp_items = result.get('items', [])
            titles = [item.get('title', '').lower() for item in serp_items if item.get('type') == 'organic']
            
            all_titles = ' '.join(titles)
            
            # Commercial keywords
            if any(word in all_titles for word in ['best', 'top', 'review', 'comparison', 'vs', 'alternative']):
                scores['commercial'] += 2
            
            # Transactional keywords
            if any(word in all_titles for word in ['buy', 'price', 'cost', 'cheap', 'deal', 'discount']):
                scores['transactional'] += 2
            
            # Informational keywords
            if any(word in all_titles for word in ['what', 'how', 'why', 'guide', 'tutorial', 'learn']):
                scores['informational'] += 2
            
            # Navigational keywords
            if any(word in all_titles for word in ['login', 'sign in', 'account', 'official']):
                scores['navigational'] += 2
        
        # Return intent with highest score
        if max(scores.values()) == 0:
            return 'informational'  # Default
        
        return max(scores.items(), key=lambda x: x[1])[0]
    
    def _estimate_click_share(
        self,
        organic_position: Optional[int],
        visual_position: float,
        features: List[SERPFeature],
        total_competitors: int
    ) -> float:
        """
        Estimate the user's click share for this keyword.
        
        Based on:
        - Position-based CTR curves
        - SERP feature impact on CTR
        - Total competition
        """
        if not organic_position or organic_position > 20:
            return 0.0
        
        # Get baseline CTR for position
        baseline_ctr = self.BASELINE_CTR.get(organic_position, 0.005)
        
        # Adjust for SERP features (features steal clicks)
        feature_penalty = 0.0
        for feature in features:
            # High-impact features steal more clicks
            if feature.type in ['featured_snippet', 'ai_overview', 'local_pack']:
                feature_penalty += 0.3
            elif feature.type in ['people_also_ask', 'video_carousel', 'shopping_results']:
                feature_penalty += 0.15 * feature.count
            else:
                feature_penalty += 0.05
        
        # Cap penalty at 80%
        feature_penalty = min(feature_penalty, 0.8)
        
        # Calculate effective CTR
        effective_ctr = baseline_ctr * (1 - feature_penalty)
        
        # Click share is the user's CTR divided by total available clicks
        # Assume 100 searches for simplicity
        user_clicks = effective_ctr * 100
        
        # Estimate total clicks going to organic results
        total_organic_ctr = sum(
            self.BASELINE_CTR.get(i, 0.005) * (1 - feature_penalty * 0.5)
            for i in range(1, min(total_competitors + 1, 11))
        )
        total_organic_clicks = total_organic_ctr * 100
        
        if total_organic_clicks == 0:
            return 0.0
        
        click_share = user_clicks / total_organic_clicks
        return round(click_share, 4)
    
    def _build_competitor_map(
        self,
        keyword_analyses: List[KeywordSERPAnalysis],
        site_domain: str
    ) -> List[Dict[str, Any]]:
        """
        Build aggregated competitor intelligence across all keywords.
        """
        competitor_data = defaultdict(lambda: {
            'domain': '',
            'appearances': 0,
            'keywords': [],
            'positions': [],
            'urls': []
        })
        
        for analysis in keyword_analyses:
            for competitor in analysis.competitors:
                domain = competitor['domain']
                competitor_data[domain]['domain'] = domain
                competitor_data[domain]['appearances'] += 1
                competitor_data[domain]['keywords'].append(analysis.keyword)
                competitor_data[domain]['positions'].append(competitor['position'])
                competitor_data[domain]['urls'].append(competitor['url'])
        
        # Convert to list and calculate metrics
        competitors = []
        for domain, data in competitor_data.items():
            avg_position = sum(data['positions']) / len(data['positions']) if data['positions'] else 999
            
            # Calculate threat level
            appearance_rate = data['appearances'] / len(keyword_analyses)
            threat_level = 'high' if appearance_rate > 0.3 else 'medium' if appearance_rate > 0.15 else 'low'
            
            competitors.append({
                'domain': domain,
                'appearances': data['appearances'],
                'keywords_shared': data['keywords'][:10],  # Limit for response size
                'keyword_count': len(data['keywords']),
                'avg_position': round(avg_position, 1),
                'positions': data['positions'][:20],  # Sample
                'threat_level': threat_level,
                'overlap_rate': round(appearance_rate, 3)
            })
        
        # Sort by appearances
        competitors.sort(key=lambda x: x['appearances'], reverse=True)
        
        return competitors[:50]  # Top 50 competitors
    
    def _analyze_displacement(
        self,
        keyword_analyses: List[KeywordSERPAnalysis]
    ) -> List[Dict[str, Any]]:
        """
        Identify keywords with significant SERP feature displacement.
        """
        displacement_issues = []
        
        for analysis in keyword_analyses:
            if not analysis.user_organic_position:
                continue
            
            # Significant displacement = visual position > organic + 3
            if analysis.displacement_severity > 3:
                feature_list = [
                    f"{f.type}" + (f"_x{f.count}" if f.count > 1 else "")
                    for f in analysis.serp_features
                ]
                
                # Estimate CTR impact
                baseline_ctr = self.BASELINE_CTR.get(analysis.user_organic_position, 0.01)
                visual_ctr = self.BASELINE_CTR.get(int(analysis.visual_position), 0.005)
                ctr_impact = baseline_ctr - visual_ctr
                
                displacement_issues.append({
                    'keyword': analysis.keyword,
                    'organic_position': analysis.user_organic_position,
                    'visual_position': round(analysis.visual_position, 1),
                    'displacement': round(analysis.displacement_severity, 1),
                    'features_above': feature_list,
                    'estimated_ctr_impact': round(ctr_impact, 3),
                    'severity': 'critical' if analysis.displacement_severity > 5 else 'high'
                })
        
        # Sort by displacement severity
        displacement_issues.sort(key=lambda x: x['displacement'], reverse=True)
        
        return displacement_issues
    
    def _analyze_intent_landscape(
        self,
        keyword_analyses: List[KeywordSERPAnalysis],
        gsc_data: pd.DataFrame
    ) -> Dict[str, Any]:
        """
        Analyze search intent distribution and identify mismatches.
        """
        intent_distribution = Counter()
        intent_mismatches = []
        
        for analysis in keyword_analyses:
            intent_distribution[analysis.intent_classification] += 1
            
            # Try to infer page type from URL
            if analysis.user_url:
                page_type = self._infer_page_type(analysis.user_url)
                
                # Check for mismatches
                is_mismatch = False
                mismatch_reason = ""
                
                if analysis.intent_classification == 'transactional' and page_type in ['blog', 'article']:
                    is_mismatch = True
                    mismatch_reason = "Blog post ranking for transactional query"
                elif analysis.intent_classification == 'informational' and page_type == 'product':
                    is_mismatch = True
                    mismatch_reason = "Product page ranking for informational query"
                elif analysis.intent_classification == 'commercial' and page_type == 'homepage':
                    is_mismatch = True
                    mismatch_reason = "Homepage ranking for commercial comparison query"
                
                if is_mismatch:
                    intent_mismatches.append({
                        'keyword': analysis.keyword,
                        'serp_intent': analysis.intent_classification,
                        'page_type': page_type,
                        'page_url': analysis.user_url,
                        'reason': mismatch_reason,
                        'position': analysis.user_organic_position
                    })
        
        return {
            'intent_distribution': dict(intent_distribution),
            'intent_mismatches': intent_mismatches[:20],  # Top 20 mismatches
            'total_mismatches': len(intent_mismatches)
        }
    
    def _infer_page_type(self, url: str) -> str:
        """
        Infer page type from URL patterns.
        """
        url_lower = url.lower()
        
        if '/blog/' in url_lower or '/article/' in url_lower or '/post/' in url_lower:
            return 'blog'
        elif '/product/' in url_lower or '/item/' in url_lower or '/shop/' in url_lower:
            return 'product'
        elif '/category/' in url_lower or '/collection/' in url_lower:
            return 'category'
        elif url_lower.endswith('/') and url_lower.count('/') <= 3:
            return 'homepage'
        else:
            return 'other'
    
    def _calculate_click_share_metrics(
        self,
        keyword_analyses: List[KeywordSERPAnalysis],
        gsc_data: pd.DataFrame
    ) -> Dict[str, Any]:
        """
        Calculate overall click share metrics.
        """
        total_click_share = 0.0
        weighted_click_share = 0.0
        total_impressions = 0
        
        for analysis in keyword_analyses:
            # Get impressions from GSC data
            keyword_row = gsc_data[gsc_data['query'] == analysis.keyword]
            impressions = keyword_row['impressions'].values[0] if len(keyword_row) > 0 else 0
            
            total_click_share += analysis.click_share_estimate
            weighted_click_share += analysis.click_share_estimate * impressions
            total_impressions += impressions
        
        avg_click_share = total_click_share / len(keyword_analyses) if keyword_analyses else 0
        weighted_avg_click_share = weighted_click_share / total_impressions if total_impressions > 0 else 0
        
        # Calculate opportunity (assume potential to reach 30% average click share)
        target_click_share = 0.30
        opportunity = max(0, target_click_share - weighted_avg_click_share)
        
        return {
            'total_click_share': round(avg_click_share, 4),
            'weighted_click_share': round(weighted_avg_click_share, 4),
            'click_share_opportunity': round(opportunity, 4),
            'total_impressions_analyzed': total_impressions,
            'potential_click_gain': round(opportunity * total_impressions, 0)
        }
    
    def _generate_insights(
        self,
        keyword_analyses: List[KeywordSERPAnalysis],
        competitor_map: List[Dict[str, Any]],
        displacement_analysis: List[Dict[str, Any]],
        intent_analysis: Dict[str, Any],
        click_share_metrics: Dict[str, Any]
    ) -> List[str]:
        """
        Generate high-level insights from the analysis.
        """
        insights = []
        
        # Displacement insight
        if displacement_analysis:
            severe_count = len([d for d in displacement_analysis if d['severity'] == 'critical'])
            if severe_count > 0:
                insights.append(
                    f"{severe_count} keywords have critical SERP feature displacement "
                    f"(organic position vs visual position gap > 5 spots)"
                )
        
        # Competitor insight
        if competitor_map:
            top_competitor = competitor_map[0]
            insights.append(
                f"{top_competitor['domain']} is your primary competitor, "
                f"appearing in {top_competitor['overlap_rate']*100:.0f}% of your keyword SERPs "
                f"at an average position of {top_competitor['avg_position']}"
            )
        
        # Intent mismatch insight
        mismatches = intent_analysis.get('total_mismatches', 0)
        if mismatches > 0:
            insights.append(
                f"{mismatches} keywords show intent mismatches between SERP features "
                f"and your ranking page type"
            )
        
        # Click share insight
        opportunity = click_share_metrics['click_share_opportunity']
        if opportunity > 0.1:
            potential_clicks = click_share_metrics.get('potential_click_gain', 0)
            insights.append(
                f"You're capturing {click_share_metrics['weighted_click_share']*100:.1f}% "
                f"of available clicks. Opportunity to gain ~{potential_clicks:.0f} clicks/month "
                f"by improving click share to 30%"
            )
        
        # Feature opportunity insight
        feature_counts = Counter()
        for analysis in keyword_analyses:
            for feature in analysis.serp_features:
                feature_counts[feature.type] += 1
        
        if feature_counts:
            top_feature = feature_counts.most_common(1)[0]
            insights.append(
                f"Most common SERP feature: {top_feature[0]} "
                f"(present in {top_feature[1]} of {len(keyword_analyses)} keywords)"
            )
        
        return insights
    
    def _generate_recommendations(
        self,
        competitor_map: List[Dict[str, Any]],
        displacement_analysis: List[Dict[str, Any]],
        intent_analysis: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Generate actionable recommendations.
        """
        recommendations = []
        
        # SERP feature optimization
        if displacement_analysis:
            critical_displacements = [d for d in displacement_analysis if d['severity'] == 'critical']
            if critical_displacements:
                feature_types = set()
                for d in critical_displacements:
                    feature_types.update(d['features_above'])
                
                rec = {
                    'category': 'SERP Feature Optimization',
                    'priority': 'high',
                    'action': 'Optimize content to capture SERP features',
                    'details': f"Focus on: {', '.join(list(feature_types)[:5])}",
                    'keywords_affected': len(critical_displacements),
                    'estimated_impact': 'Could reduce visual displacement and increase CTR by 20-40%'
                }
                recommendations.append(rec)
        
        # Competitor monitoring
        if competitor_map and competitor_map[0]['threat_level'] == 'high':
            top_competitor = competitor_map[0]
            rec = {
                'category': 'Competitive Intelligence',
                'priority': 'medium',
                'action': f"Monitor and analyze {top_competitor['domain']}",
                'details': f"They rank for {top_competitor['keyword_count']} of your keywords at avg position {top_competitor['avg_position']}",
                'keywords_affected': top_competitor['keyword_count'],
                'estimated_impact': 'Identify content gaps and optimization opportunities'
            }
            recommendations.append(rec)
        
        # Intent alignment
        mismatches = intent_analysis.get('intent_mismatches', [])
        if len(mismatches) >= 5:
            rec = {
                'category': 'Intent Alignment',
                'priority': 'high',
                'action': 'Fix content-intent mismatches',
                'details': f"{len(mismatches)} pages ranking for wrong intent type",
                'keywords_affected': len(mismatches),
                'estimated_impact': 'Better intent alignment typically improves engagement metrics by 30-50%'
            }
            recommendations.append(rec)
        
        # Featured snippet opportunities
        paa_heavy = [
            d for d in displacement_analysis
            if any('people_also_ask' in f for f in d.get('features_above', []))
        ]
        if len(paa_heavy) >= 10:
            rec = {
                'category': 'Quick Win',
                'priority': 'medium',
                'action': 'Target People Also Ask boxes',
                'details': f"{len(paa_heavy)} keywords show PAA boxes - add FAQ schema",
                'keywords_affected': len(paa_heavy),
                'estimated_impact': 'FAQ schema can help you appear in PAA, gaining visibility'
            }
            recommendations.append(rec)
        
        return recommendations
    
    def _serialize_keyword_analysis(self, analysis: KeywordSERPAnalysis) -> Dict[str, Any]:
        """
        Convert KeywordSERPAnalysis to serializable dict.
        """
        return {
            'keyword': analysis.keyword,
            'user_organic_position': analysis.user_organic_position,
            'user_url': analysis.user_url,
            'visual_position': round(analysis.visual_position, 1),
            'serp_features': [
                {
                    'type': f.type,
                    'count': f.count,
                    'impact': round(f.visual_position_impact, 1)
                }
                for f in analysis.serp_features
            ],
            'top_competitors': analysis.competitors[:5],  # Top 5 only
            'intent': analysis.intent_classification,
            'click_share': round(analysis.click_share_estimate, 4),
            'displacement_severity': round(analysis.displacement_severity, 1)
        }


async def run_module_3(
    site_domain: str,
    top_keywords: List[Dict[str, Any]],
    gsc_keyword_data: pd.DataFrame,
    dataforseo_service: DataForSEOService,
    gsc_service: GSCService,
    max_keywords: int = 100
) -> Dict[str, Any]:
    """
    Convenience function to run Module 3 analysis.
    
    Args:
        site_domain: User's domain
        top_keywords: Top keywords from GSC
        gsc_keyword_data: Full keyword performance data
        dataforseo_service: DataForSEO service instance
        gsc_service: GSC service instance
        max_keywords: Max keywords to analyze
        
    Returns:
        Complete SERP intelligence analysis
    """
    module = Module3SERPIntelligence(dataforseo_service, gsc_service)
    
    return await module.analyze_serp_landscape(
        site_domain=site_domain,
        top_keywords=top_keywords,
        gsc_keyword_data=gsc_keyword_data,
        max_keywords=max_keywords
    )