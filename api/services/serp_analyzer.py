"""
SERP Analysis Service

Integrates with DataForSEO to provide comprehensive SERP analysis:
1. Fetch SERP data for top keywords from GSC
2. Identify competing domains and their visibility
3. Extract SERP features for each query
4. Calculate expected vs actual CTR based on position and SERP features
5. Store results in Supabase serp_data table

This service is used by the report generation pipeline (Module 3: SERP Landscape Analysis)
"""

import os
import json
import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from collections import defaultdict, Counter
import pandas as pd
import numpy as np
from dataclasses import dataclass, asdict

from .dataforseo_client import DataForSEOClient
from .supabase_client import get_supabase_client


@dataclass
class SERPFeature:
    """Represents a SERP feature detected in search results"""
    feature_type: str
    position: int  # Position in SERP where this feature appears
    visual_weight: float  # How much vertical space it takes (in "position units")
    
    
@dataclass
class CompetitorPresence:
    """Represents a competitor's presence in SERP"""
    domain: str
    position: int
    url: str
    title: str
    snippet: str
    

@dataclass
class SERPAnalysis:
    """Complete SERP analysis for a single keyword"""
    keyword: str
    search_volume: Optional[int]
    user_position: Optional[int]
    user_url: Optional[str]
    organic_position: Optional[int]  # Actual organic position
    visual_position: float  # Position adjusted for SERP features above
    serp_features: List[SERPFeature]
    competitors: List[CompetitorPresence]
    total_organic_results: int
    intent_classification: str
    expected_ctr: float
    actual_ctr: Optional[float]
    ctr_impact: float
    estimated_clicks_available: float
    user_click_share: float
    analyzed_at: datetime


class SERPAnalyzer:
    """
    Main SERP analysis service
    """
    
    def __init__(self):
        self.dataforseo_client = DataForSEOClient()
        self.supabase = get_supabase_client()
        
        # CTR curves based on position (baseline, before SERP feature adjustments)
        # Source: Advanced Web Ranking CTR study averages
        self.baseline_ctr_curve = {
            1: 0.284,
            2: 0.147,
            3: 0.096,
            4: 0.067,
            5: 0.051,
            6: 0.041,
            7: 0.034,
            8: 0.029,
            9: 0.025,
            10: 0.022,
            11: 0.018,
            12: 0.016,
            13: 0.014,
            14: 0.013,
            15: 0.012,
            16: 0.011,
            17: 0.010,
            18: 0.009,
            19: 0.008,
            20: 0.008
        }
        
        # SERP feature impact modifiers
        self.feature_visual_weights = {
            'featured_snippet': 2.5,  # Takes up ~2.5 organic positions worth of space
            'knowledge_panel': 0,  # Side panel, doesn't push down
            'knowledge_card': 2.0,
            'ai_overview': 3.0,  # New AI overviews take significant space
            'people_also_ask': 0.5,  # Per question
            'local_pack': 2.0,
            'video_carousel': 1.5,
            'image_pack': 1.0,
            'shopping_results': 1.5,
            'top_stories': 1.5,
            'twitter': 1.0,
            'reddit_threads': 0.5,  # Per thread
            'site_links': 0,  # Attached to organic result, doesn't add space
        }
        
        # CTR impact multipliers when SERP features are present
        self.feature_ctr_multipliers = {
            'featured_snippet': 0.65,  # Reduces CTR of position 1 by 35%
            'ai_overview': 0.55,  # AI overviews have larger impact
            'people_also_ask': 0.90,  # Modest impact per PAA
            'local_pack': 0.70,
            'video_carousel': 0.85,
            'shopping_results': 0.75,
        }

    async def analyze_keywords_for_report(
        self,
        report_id: str,
        domain: str,
        gsc_keyword_data: pd.DataFrame,
        top_n: int = 100,
        branded_terms: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Main entry point for SERP analysis in report generation pipeline.
        
        Args:
            report_id: Report ID for storing results
            domain: User's domain
            gsc_keyword_data: DataFrame with GSC keyword performance data
                             Expected columns: query, clicks, impressions, ctr, position
            top_n: Number of top keywords to analyze (default 100)
            branded_terms: List of branded terms to filter out (e.g., company name)
        
        Returns:
            Dictionary with complete SERP analysis results
        """
        
        # Step 1: Select keywords to analyze
        selected_keywords = self._select_keywords_for_analysis(
            gsc_keyword_data,
            top_n,
            branded_terms
        )
        
        print(f"Selected {len(selected_keywords)} keywords for SERP analysis")
        
        # Step 2: Check cache for recent SERP data
        cached_results, keywords_to_fetch = await self._check_serp_cache(
            domain,
            selected_keywords
        )
        
        print(f"Found {len(cached_results)} cached results, need to fetch {len(keywords_to_fetch)}")
        
        # Step 3: Fetch fresh SERP data for non-cached keywords
        fresh_results = []
        if keywords_to_fetch:
            fresh_results = await self._fetch_serp_data(
                keywords_to_fetch,
                domain
            )
        
        # Step 4: Combine cached and fresh results
        all_serp_data = cached_results + fresh_results
        
        # Step 5: Analyze each SERP
        analyses = []
        for serp_data in all_serp_data:
            keyword = serp_data['keyword']
            
            # Get GSC performance data for this keyword
            gsc_row = gsc_keyword_data[gsc_keyword_data['query'] == keyword]
            actual_ctr = gsc_row['ctr'].values[0] if not gsc_row.empty else None
            clicks = gsc_row['clicks'].values[0] if not gsc_row.empty else 0
            impressions = gsc_row['impressions'].values[0] if not gsc_row.empty else 0
            
            analysis = self._analyze_single_serp(
                serp_data,
                domain,
                actual_ctr,
                clicks,
                impressions
            )
            analyses.append(analysis)
        
        # Step 6: Store results in Supabase
        await self._store_serp_analyses(report_id, analyses)
        
        # Step 7: Generate aggregate insights
        aggregated = self._aggregate_serp_insights(analyses, domain)
        
        return aggregated

    def _select_keywords_for_analysis(
        self,
        gsc_data: pd.DataFrame,
        top_n: int,
        branded_terms: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Select top keywords to analyze, filtering out branded terms.
        
        Returns list of dicts with: query, impressions, position, clicks
        """
        df = gsc_data.copy()
        
        # Filter out branded terms
        if branded_terms:
            branded_pattern = '|'.join([term.lower() for term in branded_terms])
            df = df[~df['query'].str.lower().str.contains(branded_pattern, na=False)]
        
        # Also include keywords with significant position changes (even if not in top N by impressions)
        # This requires position_change column if available
        high_change_keywords = pd.DataFrame()
        if 'position_change_30d' in df.columns:
            high_change_keywords = df[abs(df['position_change_30d']) > 3].copy()
        
        # Sort by impressions and take top N
        top_by_impressions = df.nlargest(top_n, 'impressions')
        
        # Combine and deduplicate
        combined = pd.concat([top_by_impressions, high_change_keywords]).drop_duplicates(subset=['query'])
        
        # Limit to top_n total
        combined = combined.nlargest(top_n, 'impressions')
        
        return combined[['query', 'impressions', 'position', 'clicks', 'ctr']].to_dict('records')

    async def _check_serp_cache(
        self,
        domain: str,
        keywords: List[Dict[str, Any]],
        cache_hours: int = 24
    ) -> Tuple[List[Dict], List[Dict]]:
        """
        Check Supabase for cached SERP data.
        
        Returns:
            (cached_results, keywords_to_fetch)
        """
        cached_results = []
        keywords_to_fetch = []
        
        cutoff_time = datetime.utcnow() - timedelta(hours=cache_hours)
        
        for kw in keywords:
            query = kw['query']
            
            # Query Supabase for cached SERP data
            response = self.supabase.table('serp_cache').select('*').eq(
                'domain', domain
            ).eq(
                'keyword', query
            ).gte(
                'fetched_at', cutoff_time.isoformat()
            ).order(
                'fetched_at', desc=True
            ).limit(1).execute()
            
            if response.data and len(response.data) > 0:
                cached_results.append({
                    'keyword': query,
                    'serp_data': response.data[0]['serp_data'],
                    'fetched_at': response.data[0]['fetched_at']
                })
            else:
                keywords_to_fetch.append(kw)
        
        return cached_results, keywords_to_fetch

    async def _fetch_serp_data(
        self,
        keywords: List[Dict[str, Any]],
        domain: str,
        location_code: int = 2840,  # United States
        language_code: str = 'en'
    ) -> List[Dict]:
        """
        Fetch live SERP data from DataForSEO for given keywords.
        
        Uses batching to optimize API calls.
        """
        results = []
        
        # Batch keywords (DataForSEO allows up to 100 tasks per request)
        batch_size = 100
        
        for i in range(0, len(keywords), batch_size):
            batch = keywords[i:i + batch_size]
            
            # Prepare tasks for DataForSEO
            tasks = []
            for kw in batch:
                tasks.append({
                    'keyword': kw['query'],
                    'location_code': location_code,
                    'language_code': language_code,
                    'device': 'desktop',
                    'os': 'windows',
                    'depth': 100,  # Get up to 100 results
                })
            
            # Make API call
            try:
                response = await self.dataforseo_client.post_serp_tasks(tasks)
                
                # Process response
                if response and 'tasks' in response:
                    for task in response['tasks']:
                        if task['status_code'] == 20000 and task['result']:
                            keyword = task['result'][0]['keyword']
                            serp_data = task['result'][0]
                            
                            results.append({
                                'keyword': keyword,
                                'serp_data': serp_data,
                                'fetched_at': datetime.utcnow().isoformat()
                            })
                            
                            # Cache in Supabase
                            await self._cache_serp_data(
                                domain,
                                keyword,
                                serp_data
                            )
                
                # Rate limiting
                await asyncio.sleep(1)
                
            except Exception as e:
                print(f"Error fetching SERP data for batch: {e}")
                continue
        
        return results

    async def _cache_serp_data(
        self,
        domain: str,
        keyword: str,
        serp_data: Dict
    ):
        """Store SERP data in cache table"""
        try:
            self.supabase.table('serp_cache').upsert({
                'domain': domain,
                'keyword': keyword,
                'serp_data': serp_data,
                'fetched_at': datetime.utcnow().isoformat()
            }, on_conflict='domain,keyword').execute()
        except Exception as e:
            print(f"Error caching SERP data for {keyword}: {e}")

    def _analyze_single_serp(
        self,
        serp_result: Dict,
        user_domain: str,
        actual_ctr: Optional[float],
        clicks: float,
        impressions: float
    ) -> SERPAnalysis:
        """
        Analyze a single SERP result.
        
        Extracts:
        - SERP features present
        - User's position (organic and visual)
        - Competitors
        - Expected CTR vs actual
        - Intent classification
        """
        keyword = serp_result['keyword']
        serp_data = serp_result['serp_data']
        
        # Extract SERP features
        serp_features = self._extract_serp_features(serp_data)
        
        # Find user's position
        user_position, user_url, organic_results = self._find_user_position(
            serp_data,
            user_domain
        )
        
        # Calculate visual position (accounting for SERP features above)
        visual_position = self._calculate_visual_position(
            user_position,
            serp_features
        ) if user_position else None
        
        # Extract competitors
        competitors = self._extract_competitors(
            serp_data,
            user_domain
        )
        
        # Classify intent
        intent = self._classify_intent(serp_data, serp_features)
        
        # Calculate expected CTR (based on visual position and SERP features)
        expected_ctr = self._calculate_expected_ctr(
            visual_position if visual_position else user_position,
            serp_features
        ) if user_position else 0.0
        
        # Calculate CTR impact
        ctr_impact = (actual_ctr - expected_ctr) if actual_ctr else 0.0
        
        # Estimate total available clicks for this keyword
        search_volume = serp_data.get('keyword_data', {}).get('keyword_info', {}).get('search_volume')
        
        if search_volume:
            # Sum up CTRs of all organic positions to estimate total organic clicks available
            total_organic_ctr = sum([
                self.baseline_ctr_curve.get(i, 0.002) 
                for i in range(1, min(21, len(organic_results) + 1))
            ])
            estimated_clicks_available = search_volume * total_organic_ctr
            
            # User's click share
            user_click_share = (clicks / estimated_clicks_available) if estimated_clicks_available > 0 else 0.0
        else:
            # Fall back to impressions if no search volume
            estimated_clicks_available = impressions * 0.15  # Rough estimate
            user_click_share = (clicks / estimated_clicks_available) if estimated_clicks_available > 0 else 0.0
        
        return SERPAnalysis(
            keyword=keyword,
            search_volume=search_volume,
            user_position=user_position,
            user_url=user_url,
            organic_position=user_position,
            visual_position=visual_position if visual_position else (user_position if user_position else 0),
            serp_features=serp_features,
            competitors=competitors,
            total_organic_results=len(organic_results),
            intent_classification=intent,
            expected_ctr=expected_ctr,
            actual_ctr=actual_ctr,
            ctr_impact=ctr_impact,
            estimated_clicks_available=estimated_clicks_available,
            user_click_share=user_click_share,
            analyzed_at=datetime.utcnow()
        )

    def _extract_serp_features(self, serp_data: Dict) -> List[SERPFeature]:
        """
        Parse SERP data and extract all present features with their positions.
        """
        features = []
        items = serp_data.get('items', [])
        
        position_counter = 0
        
        for item in items:
            item_type = item.get('type')
            rank_absolute = item.get('rank_absolute', 0)
            
            if item_type == 'featured_snippet':
                features.append(SERPFeature(
                    feature_type='featured_snippet',
                    position=rank_absolute,
                    visual_weight=self.feature_visual_weights['featured_snippet']
                ))
            
            elif item_type == 'knowledge_panel':
                features.append(SERPFeature(
                    feature_type='knowledge_panel',
                    position=rank_absolute,
                    visual_weight=self.feature_visual_weights['knowledge_panel']
                ))
            
            elif item_type == 'knowledge_card':
                features.append(SERPFeature(
                    feature_type='knowledge_card',
                    position=rank_absolute,
                    visual_weight=self.feature_visual_weights['knowledge_card']
                ))
            
            elif item_type == 'ai_overview' or item_type == 'generative_ai':
                features.append(SERPFeature(
                    feature_type='ai_overview',
                    position=rank_absolute,
                    visual_weight=self.feature_visual_weights['ai_overview']
                ))
            
            elif item_type == 'people_also_ask':
                # Count individual questions
                questions = item.get('items', [])
                num_questions = len(questions) if questions else 1
                features.append(SERPFeature(
                    feature_type='people_also_ask',
                    position=rank_absolute,
                    visual_weight=self.feature_visual_weights['people_also_ask'] * num_questions
                ))
            
            elif item_type == 'local_pack':
                features.append(SERPFeature(
                    feature_type='local_pack',
                    position=rank_absolute,
                    visual_weight=self.feature_visual_weights['local_pack']
                ))
            
            elif item_type == 'video':
                features.append(SERPFeature(
                    feature_type='video_carousel',
                    position=rank_absolute,
                    visual_weight=self.feature_visual_weights['video_carousel']
                ))
            
            elif item_type == 'images':
                features.append(SERPFeature(
                    feature_type='image_pack',
                    position=rank_absolute,
                    visual_weight=self.feature_visual_weights['image_pack']
                ))
            
            elif item_type == 'shopping':
                features.append(SERPFeature(
                    feature_type='shopping_results',
                    position=rank_absolute,
                    visual_weight=self.feature_visual_weights['shopping_results']
                ))
            
            elif item_type == 'top_stories':
                features.append(SERPFeature(
                    feature_type='top_stories',
                    position=rank_absolute,
                    visual_weight=self.feature_visual_weights['top_stories']
                ))
            
            elif item_type == 'twitter':
                features.append(SERPFeature(
                    feature_type='twitter',
                    position=rank_absolute,
                    visual_weight=self.feature_visual_weights['twitter']
                ))
            
            elif item_type == 'reddit':
                # Count individual threads
                threads = item.get('items', [])
                num_threads = len(threads) if threads else 1
                features.append(SERPFeature(
                    feature_type='reddit_threads',
                    position=rank_absolute,
                    visual_weight=self.feature_visual_weights['reddit_threads'] * num_threads
                ))
        
        return features

    def _find_user_position(
        self,
        serp_data: Dict,
        user_domain: str
    ) -> Tuple[Optional[int], Optional[str], List[Dict]]:
        """
        Find user's organic position in SERP.
        
        Returns: (position, url, all_organic_results)
        """
        organic_results = []
        user_position = None
        user_url = None
        
        items = serp_data.get('items', [])
        
        for item in items:
            if item.get('type') == 'organic':
                organic_results.append(item)
                
                url = item.get('url', '')
                domain = item.get('domain', '')
                
                if user_domain.lower() in domain.lower() or user_domain.lower() in url.lower():
                    if user_position is None:  # Take first match
                        user_position = item.get('rank_absolute')
                        user_url = url
        
        return user_position, user_url, organic_results

    def _calculate_visual_position(
        self,
        organic_position: int,
        serp_features: List[SERPFeature]
    ) -> float:
        """
        Calculate visual position accounting for SERP features above the organic result.
        
        Visual position = organic position + sum of visual weights of features appearing before it
        """
        visual_displacement = 0.0
        
        for feature in serp_features:
            # If feature appears before the organic result, add its weight
            if feature.position < organic_position:
                visual_displacement += feature.visual_weight
        
        return organic_position + visual_displacement

    def _extract_competitors(
        self,
        serp_data: Dict,
        user_domain: str
    ) -> List[CompetitorPresence]:
        """
        Extract all competing domains from organic results.
        """
        competitors = []
        items = serp_data.get('items', [])
        
        for item in items:
            if item.get('type') == 'organic':
                domain = item.get('domain', '')
                
                # Skip user's own domain
                if user_domain.lower() in domain.lower():
                    continue
                
                competitors.append(CompetitorPresence(
                    domain=domain,
                    position=item.get('rank_absolute'),
                    url=item.get('url', ''),
                    title=item.get('title', ''),
                    snippet=item.get('description', '')
                ))
        
        return competitors

    def _classify_intent(
        self,
        serp_data: Dict,
        serp_features: List[SERPFeature]
    ) -> str:
        """
        Classify search intent based on SERP composition.
        
        Returns: informational, commercial, transactional, or navigational
        """
        feature_types = [f.feature_type for f in serp_features]
        
        # Count indicators
        has_shopping = 'shopping_results' in feature_types
        has_local = 'local_pack' in feature_types
        has_paa = 'people_also_ask' in feature_types
        has_knowledge = 'knowledge_panel' in feature_types or 'knowledge_card' in feature_types
        has_video = 'video_carousel' in feature_types
        has_news = 'top_stories' in feature_types
        
        # Check for brand indicators in organic results
        items = serp_data.get('items', [])
        organic_results = [item for item in items if item.get('type') == 'organic']
        
        if organic_results:
            first_result = organic_results[0]
            has_sitelinks = bool(first_result.get('links'))
        else:
            has_sitelinks = False
        
        # Classification logic
        if has_shopping or (has_local and not has_paa):
            return 'transactional'
        
        if has_sitelinks and has_knowledge:
            return 'navigational'
        
        if has_paa or has_knowledge or has_video or has_news:
            return 'informational'
        
        # Check keyword patterns in title/snippet
        keyword = serp_data.get('keyword', '').lower()
        if any(word in keyword for word in ['buy', 'price', 'cost', 'cheap', 'deal', 'discount']):
            return 'transactional'
        
        if any(word in keyword for word in ['best', 'top', 'review', 'vs', 'compare']):
            return 'commercial'
        
        if any(word in keyword for word in ['how', 'what', 'why', 'when', 'where', 'guide', 'tutorial']):
            return 'informational'
        
        # Default
        return 'informational'

    def _calculate_expected_ctr(
        self,
        position: Optional[float],
        serp_features: List[SERPFeature]
    ) -> float:
        """
        Calculate expected CTR based on position and SERP features present.
        
        Start with baseline CTR curve, then apply multipliers based on features.
        """
        if position is None or position < 1:
            return 0.0
        
        # Get baseline CTR for the position (interpolate if needed)
        if position <= 20:
            if position == int(position):
                base_ctr = self.baseline_ctr_curve.get(int(position), 0.008)
            else:
                # Linear interpolation between positions
                lower_pos = int(position)
                upper_pos = lower_pos + 1
                lower_ctr = self.baseline_ctr_curve.get(lower_pos, 0.008)
                upper_ctr = self.baseline_ctr_curve.get(upper_pos, 0.008)
                weight = position - lower_pos
                base_ctr = lower_ctr * (1 - weight) + upper_ctr * weight
        else:
            base_ctr = 0.005  # Very low CTR for positions beyond 20
        
        # Apply SERP feature multipliers
        adjusted_ctr = base_ctr
        feature_types = [f.feature_type for f in serp_features]
        
        for feature_type, multiplier in self.feature_ctr_multipliers.items():
            if feature_type in feature_types:
                adjusted_ctr *= multiplier
        
        return adjusted_ctr

    async def _store_serp_analyses(
        self,
        report_id: str,
        analyses: List[SERPAnalysis]
    ):
        """
        Store SERP analysis results in Supabase serp_data table.
        """
        records = []
        
        for analysis in analyses:
            records.append({
                'report_id': report_id,
                'keyword': analysis.keyword,
                'search_volume': analysis.search_volume,
                'user_position': analysis.user_position,
                'user_url': analysis.user_url,
                'organic_position': analysis.organic_position,
                'visual_position': analysis.visual_position,
                'serp_features': [asdict(f) for f in analysis.serp_features],
                'competitors': [asdict(c) for c in analysis.competitors],
                'total_organic_results': analysis.total_organic_results,
                'intent_classification': analysis.intent_classification,
                'expected_ctr': analysis.expected_ctr,
                'actual_ctr': analysis.actual_ctr,
                'ctr_impact': analysis.ctr_impact,
                'estimated_clicks_available': analysis.estimated_clicks_available,
                'user_click_share': analysis.user_click_share,
                'analyzed_at': analysis.analyzed_at.isoformat()
            })
        
        # Batch insert
        if records:
            try:
                self.supabase.table('serp_data').insert(records).execute()
                print(f"Stored {len(records)} SERP analyses for report {report_id}")
            except Exception as e:
                print(f"Error storing SERP analyses: {e}")

    def _aggregate_serp_insights(
        self,
        analyses: List[SERPAnalysis],
        user_domain: str
    ) -> Dict[str, Any]:
        """
        Generate aggregate insights from all SERP analyses.
        
        This is the main output consumed by the report generation pipeline.
        """
        # Keywords with significant SERP feature displacement
        displacement_threshold = 3.0
        displaced_keywords = [
            a for a in analyses 
            if a.visual_position and a.organic_position and 
            (a.visual_position - a.organic_position) > displacement_threshold
        ]
        
        # Competitor frequency analysis
        competitor_counter = Counter()
        competitor_positions = defaultdict(list)
        
        for analysis in analyses:
            for competitor in analysis.competitors:
                competitor_counter[competitor.domain] += 1
                competitor_positions[competitor.domain].append(competitor.position)
        
        # Identify primary competitors (appear in >20% of keywords)
        total_keywords = len(analyses)
        primary_competitors = []
        
        for domain, count in competitor_counter.most_common():
            if count / total_keywords >= 0.20:
                positions = competitor_positions[domain]
                primary_competitors.append({
                    'domain': domain,
                    'keywords_shared': count,
                    'share_pct': (count / total_keywords) * 100,
                    'avg_position': np.mean(positions),
                    'median_position': np.median(positions),
                    'threat_level': 'high' if np.mean(positions) < 5 else 'medium'
                })
        
        # Intent classification distribution
        intent_distribution = Counter([a.intent_classification for a in analyses])
        
        # Intent mismatches (would require page type data - placeholder for now)
        intent_mismatches = []
        
        # Total click share calculation
        total_clicks_available = sum(a.estimated_clicks_available for a in analyses)
        total_user_clicks = sum(a.actual_ctr * a.search_volume if a.search_volume else 0 for a in analyses)
        total_click_share = total_user_clicks / total_clicks_available if total_clicks_available > 0 else 0
        
        # Calculate opportunity (clicks we could get if we reached top 3 for all)
        opportunity_clicks = 0
        for analysis in analyses:
            if analysis.search_volume and analysis.user_position and analysis.user_position > 3:
                # CTR difference between current position and position 3
                current_ctr = self.baseline_ctr_curve.get(int(analysis.user_position), 0.005)
                top3_ctr = self.baseline_ctr_curve[3]
                opportunity_clicks += analysis.search_volume * (top3_ctr - current_ctr)
        
        click_share_opportunity = opportunity_clicks / total_clicks_available if total_clicks_available > 0 else 0
        
        # CTR underperformers (actual CTR significantly below expected)
        ctr_underperformers = sorted(
            [a for a in analyses if a.actual_ctr and a.expected_ctr and a.ctr_impact < -0.01],
            key=lambda x: x.ctr_impact
        )[:20]  # Top 20 worst
        
        # SERP feature opportunities
        feature_opportunities = self._identify_feature_opportunities(analyses)
        
        return {
            'keywords_analyzed': total_keywords,
            'serp_feature_displacement': [
                {
                    'keyword': a.keyword,
                    'organic_position': a.organic_position,
                    'visual_position': a.visual_position,
                    'features_above': [f.feature_type for f in a.serp_features if f.position < a.organic_position],
                    'estimated_ctr_impact': a.expected_ctr - self.baseline_ctr_curve.get(a.organic_position, 0.005)
                }
                for a in displaced_keywords
            ],
            'competitors': primary_competitors,
            'intent_distribution': dict(intent_distribution),
            'intent_mismatches': intent_mismatches,  # Placeholder
            'total_click_share': total_click_share,
            'click_share_opportunity': click_share_opportunity,
            'opportunity_clicks_monthly': opportunity_clicks,
            'ctr_underperformers': [
                {
                    'keyword': a.keyword,
                    'position': a.user_position,
                    'expected_ctr': a.expected_ctr,
                    'actual_ctr': a.actual_ctr,
                    'ctr_impact': a.ctr_impact,
                    'url': a.user_url,
                    'features_present': [f.feature_type for f in a.serp_features]
                }
                for a in ctr_underperformers
            ],
            'feature_opportunities': feature_opportunities,
            'summary': {
                'avg_visual_displacement': np.mean([
                    a.visual_position - a.organic_position 
                    for a in analyses 
                    if a.visual_position and a.organic_position
                ]),
                'keywords_with_ai_overview': len([a for a in analyses if any(f.feature_type == 'ai_overview' for f in a.serp_features)]),
                'keywords_with_featured_snippet': len([a for a in analyses if any(f.feature_type == 'featured_snippet' for f in a.serp_features)]),
                'keywords_with_paa': len([a for a in analyses if any(f.feature_type == 'people_also_ask' for f in a.serp_features)]),
                'primary_competitors_count': len(primary_competitors),
                'avg_competitor_overlap': np.mean([c['keywords_shared'] for c in primary_competitors]) if primary_competitors else 0
            }
        }

    def _identify_feature_opportunities(
        self,
        analyses: List[SERPAnalysis]
    ) -> List[Dict[str, Any]]:
        """
        Identify SERP feature optimization opportunities.
        
        For example:
        - Keywords with PAA where we rank well but don't have FAQ schema
        - Keywords with video carousels where we don't have video
        - Keywords with featured snippets we could target
        """
        opportunities = []
        
        # Group by feature type
        for analysis in analyses:
            feature_types = [f.feature_type for f in analysis.serp_features]
            
            # PAA opportunity
            if 'people_also_ask' in feature_types and analysis.user_position and analysis.user_position <= 10:
                opportunities.append({
                    'keyword': analysis.keyword,
                    'opportunity_type': 'faq_schema',
                    'reason': f'PAA present, you rank #{analysis.user_position}',
                    'estimated_ctr_gain': 0.02,  # Rough estimate
                    'priority': 'medium' if analysis.user_position <= 5 else 'low'
                })
            
            # Video carousel opportunity
            if 'video_carousel' in feature_types and analysis.user_position and analysis.user_position <= 10:
                opportunities.append({
                    'keyword': analysis.keyword,
                    'opportunity_type': 'video_content',
                    'reason': f'Video carousel present, you rank #{analysis.user_position}',
                    'estimated_ctr_gain': 0.03,
                    'priority': 'medium'
                })
            
            # Featured snippet opportunity (you rank 2-5, snippet present)
            if 'featured_snippet' in feature_types and analysis.user_position and 2 <= analysis.user_position <= 5:
                opportunities.append({
                    'keyword': analysis.keyword,
                    'opportunity_type': 'featured_snippet_targeting',
                    'reason': f'Featured snippet present, you rank #{analysis.user_position}',
                    'estimated_ctr_gain': 0.15,  # Featured snippets can have high impact
                    'priority': 'high'
                })
        
        # Sort by estimated CTR gain
        opportunities.sort(key=lambda x: x['estimated_ctr_gain'], reverse=True)
        
        return opportunities[:30]  # Top 30 opportunities


async def main():
    """
    Test function for development
    """
    analyzer = SERPAnalyzer()
    
    # Sample GSC data for testing
    sample_gsc_data = pd.DataFrame([
        {'query': 'best project management software', 'clicks': 120, 'impressions': 5000, 'ctr': 0.024, 'position': 4.2},
        {'query': 'project management tools', 'clicks': 95, 'impressions': 4200, 'ctr': 0.023, 'position': 5.1},
        {'query': 'agile project management', 'clicks': 78, 'impressions': 3100, 'ctr': 0.025, 'position': 3.8},
    ])
    
    results = await analyzer.analyze_keywords_for_report(
        report_id='test-report-123',
        domain='example.com',
        gsc_keyword_data=sample_gsc_data,
        top_n=3,
        branded_terms=['example', 'examplesoft']
    )
    
    print(json.dumps(results, indent=2, default=str))


if __name__ == '__main__':
    asyncio.run(main())
