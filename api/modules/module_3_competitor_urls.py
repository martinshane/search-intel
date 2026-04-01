"""
Module 3: Competitor & SERP Landscape Analysis

Discovers competing URLs and domains for top-performing keywords from Module 1.
Uses DataForSEO to analyze SERP composition, competitor overlap, and ranking patterns.
"""

import os
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
from collections import defaultdict, Counter
import logging

import pandas as pd
import numpy as np
from dataclasses import dataclass, asdict

from api.utils.dataforseo_client import DataForSEOClient
from api.utils.database import Database

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class CompetitorDomain:
    """Represents a competing domain with metrics."""
    domain: str
    keywords_shared: int
    overlap_score: float
    avg_position: float
    avg_user_position: float
    position_advantage: float  # negative means competitor ranks better
    total_impressions: int
    estimated_traffic_share: float
    threat_level: str  # high, medium, low
    common_positions: List[int]  # positions where they typically rank
    strongest_keywords: List[Dict[str, Any]]  # keywords where they dominate


@dataclass
class SERPFeatureImpact:
    """Represents SERP feature displacement impact."""
    keyword: str
    organic_position: int
    visual_position: float
    features_above: List[str]
    estimated_ctr_loss: float
    monthly_impressions: int
    estimated_click_loss: int


@dataclass
class CompetitorAnalysisResult:
    """Complete result from Module 3."""
    competitors: List[CompetitorDomain]
    serp_feature_impacts: List[SERPFeatureImpact]
    primary_competitors: List[str]  # domains appearing in >20% of keywords
    total_keywords_analyzed: int
    avg_competitors_per_keyword: float
    total_click_share: float  # user's estimated share of all clicks
    potential_click_share: float  # what they could capture
    serp_feature_summary: Dict[str, int]
    competitive_pressure_score: float  # 0-1, higher = more competitive
    generated_at: str


class Module3CompetitorAnalysis:
    """Module 3: Competitor & SERP landscape analysis."""
    
    def __init__(self, db: Database, dataforseo_client: Optional[DataForSEOClient] = None):
        self.db = db
        self.dataforseo = dataforseo_client or DataForSEOClient(
            login=os.getenv('DATAFORSEO_LOGIN'),
            password=os.getenv('DATAFORSEO_PASSWORD')
        )
        
        # CTR curves by position (baseline, no features)
        self.baseline_ctr = {
            1: 0.316, 2: 0.158, 3: 0.105, 4: 0.078, 5: 0.062,
            6: 0.051, 7: 0.043, 8: 0.037, 9: 0.032, 10: 0.028,
            11: 0.020, 12: 0.016, 13: 0.013, 14: 0.011, 15: 0.009,
            16: 0.008, 17: 0.007, 18: 0.006, 19: 0.005, 20: 0.005
        }
        
        # SERP feature impact multipliers
        self.feature_impacts = {
            'featured_snippet': {'visual_offset': 2.0, 'ctr_multiplier': 0.60},
            'ai_overview': {'visual_offset': 2.5, 'ctr_multiplier': 0.55},
            'knowledge_panel': {'visual_offset': 1.5, 'ctr_multiplier': 0.80},
            'local_pack': {'visual_offset': 3.0, 'ctr_multiplier': 0.50},
            'people_also_ask': {'visual_offset': 0.5, 'ctr_multiplier': 0.85},  # per question
            'video_carousel': {'visual_offset': 2.0, 'ctr_multiplier': 0.65},
            'image_pack': {'visual_offset': 1.0, 'ctr_multiplier': 0.85},
            'shopping_results': {'visual_offset': 2.0, 'ctr_multiplier': 0.60},
            'top_stories': {'visual_offset': 2.0, 'ctr_multiplier': 0.70},
            'twitter': {'visual_offset': 1.5, 'ctr_multiplier': 0.75},
            'reddit_threads': {'visual_offset': 1.0, 'ctr_multiplier': 0.80}
        }
    
    async def analyze(
        self,
        report_id: str,
        site_domain: str,
        module_1_results: Dict[str, Any],
        gsc_keyword_data: pd.DataFrame,
        max_keywords: int = 50
    ) -> CompetitorAnalysisResult:
        """
        Main analysis method for Module 3.
        
        Args:
            report_id: Unique identifier for this report
            site_domain: The user's domain (to filter out from competitors)
            module_1_results: Results from Module 1 (health & trajectory)
            gsc_keyword_data: GSC keyword performance data
            max_keywords: Maximum number of keywords to analyze via SERP API
            
        Returns:
            CompetitorAnalysisResult with all competitor and SERP data
        """
        logger.info(f"Starting Module 3 analysis for report {report_id}")
        
        # Step 1: Select top keywords to analyze
        top_keywords = self._select_keywords_for_analysis(
            gsc_keyword_data,
            site_domain,
            max_keywords
        )
        logger.info(f"Selected {len(top_keywords)} keywords for SERP analysis")
        
        # Step 2: Fetch SERP data from DataForSEO (or cache)
        serp_data = await self._fetch_serp_data(report_id, top_keywords)
        logger.info(f"Retrieved SERP data for {len(serp_data)} keywords")
        
        # Step 3: Parse SERP features and calculate visual positions
        serp_features = self._analyze_serp_features(serp_data, gsc_keyword_data, site_domain)
        
        # Step 4: Extract and analyze competitors
        competitors = self._analyze_competitors(serp_data, gsc_keyword_data, site_domain)
        
        # Step 5: Calculate competitive metrics
        competitive_metrics = self._calculate_competitive_metrics(
            competitors,
            serp_features,
            gsc_keyword_data,
            len(top_keywords)
        )
        
        # Step 6: Assemble final result
        result = CompetitorAnalysisResult(
            competitors=competitors,
            serp_feature_impacts=serp_features,
            primary_competitors=competitive_metrics['primary_competitors'],
            total_keywords_analyzed=len(top_keywords),
            avg_competitors_per_keyword=competitive_metrics['avg_competitors_per_keyword'],
            total_click_share=competitive_metrics['total_click_share'],
            potential_click_share=competitive_metrics['potential_click_share'],
            serp_feature_summary=competitive_metrics['serp_feature_summary'],
            competitive_pressure_score=competitive_metrics['competitive_pressure_score'],
            generated_at=datetime.utcnow().isoformat()
        )
        
        # Step 7: Store results in database
        await self._store_results(report_id, result)
        
        logger.info(f"Module 3 analysis complete. Found {len(competitors)} competitors")
        return result
    
    def _select_keywords_for_analysis(
        self,
        gsc_data: pd.DataFrame,
        site_domain: str,
        max_keywords: int
    ) -> List[Dict[str, Any]]:
        """
        Select top keywords for SERP analysis.
        
        Prioritizes:
        1. High impression volume
        2. Non-branded queries
        3. Keywords with recent position changes (from Module 1 if available)
        4. Keywords currently ranking in positions 1-20
        """
        # Extract brand name from domain
        brand_terms = self._extract_brand_terms(site_domain)
        
        # Filter out branded queries
        def is_branded(query: str) -> bool:
            query_lower = query.lower()
            return any(brand.lower() in query_lower for brand in brand_terms)
        
        gsc_data = gsc_data.copy()
        gsc_data['is_branded'] = gsc_data['query'].apply(is_branded)
        non_branded = gsc_data[~gsc_data['is_branded']].copy()
        
        # Filter to reasonable positions (top 20)
        non_branded = non_branded[non_branded['position'] <= 20].copy()
        
        # Calculate priority score: impressions * (1 / position)
        non_branded['priority_score'] = (
            non_branded['impressions'] * (1 / non_branded['position'])
        )
        
        # Sort by priority and take top N
        top_keywords = non_branded.nlargest(max_keywords, 'priority_score')
        
        return [
            {
                'query': row['query'],
                'position': row['position'],
                'impressions': row['impressions'],
                'clicks': row.get('clicks', 0),
                'ctr': row.get('ctr', 0)
            }
            for _, row in top_keywords.iterrows()
        ]
    
    def _extract_brand_terms(self, domain: str) -> List[str]:
        """Extract potential brand terms from domain."""
        # Remove TLD and www
        domain = domain.replace('www.', '').split('.')[0]
        
        # Split on hyphens and common separators
        parts = domain.replace('-', ' ').replace('_', ' ').split()
        
        # Return domain and its parts
        return [domain] + parts
    
    async def _fetch_serp_data(
        self,
        report_id: str,
        keywords: List[Dict[str, Any]]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetch SERP data from DataForSEO or cache.
        
        Returns dict keyed by query with SERP results.
        """
        serp_data = {}
        
        # Check cache first
        cached_data = await self._get_cached_serp_data(report_id, keywords)
        serp_data.update(cached_data)
        
        # Identify keywords needing fresh data
        keywords_to_fetch = [
            kw for kw in keywords
            if kw['query'] not in serp_data
        ]
        
        if not keywords_to_fetch:
            logger.info("All SERP data retrieved from cache")
            return serp_data
        
        logger.info(f"Fetching SERP data for {len(keywords_to_fetch)} keywords")
        
        # Batch fetch from DataForSEO
        for keyword in keywords_to_fetch:
            try:
                result = await self.dataforseo.get_serp_results(
                    keyword=keyword['query'],
                    location_code=2840,  # United States - can be parameterized
                    language_code='en'
                )
                
                if result and 'items' in result and result['items']:
                    serp_data[keyword['query']] = result['items'][0]
                    
                    # Cache the result
                    await self._cache_serp_data(report_id, keyword['query'], result['items'][0])
                    
            except Exception as e:
                logger.error(f"Error fetching SERP for '{keyword['query']}': {str(e)}")
                continue
        
        return serp_data
    
    async def _get_cached_serp_data(
        self,
        report_id: str,
        keywords: List[Dict[str, Any]]
    ) -> Dict[str, Dict[str, Any]]:
        """Retrieve cached SERP data (within 7 days)."""
        cached = {}
        
        # Query cache table
        cache_cutoff = datetime.utcnow() - timedelta(days=7)
        
        for keyword in keywords:
            result = await self.db.fetch_one(
                """
                SELECT serp_data, cached_at
                FROM serp_cache
                WHERE keyword = $1 AND cached_at > $2
                ORDER BY cached_at DESC
                LIMIT 1
                """,
                keyword['query'],
                cache_cutoff
            )
            
            if result:
                cached[keyword['query']] = result['serp_data']
        
        return cached
    
    async def _cache_serp_data(
        self,
        report_id: str,
        keyword: str,
        serp_data: Dict[str, Any]
    ):
        """Store SERP data in cache."""
        await self.db.execute(
            """
            INSERT INTO serp_cache (keyword, serp_data, report_id, cached_at)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (keyword, report_id) 
            DO UPDATE SET serp_data = $2, cached_at = $4
            """,
            keyword,
            serp_data,
            report_id,
            datetime.utcnow()
        )
    
    def _analyze_serp_features(
        self,
        serp_data: Dict[str, Dict[str, Any]],
        gsc_data: pd.DataFrame,
        site_domain: str
    ) -> List[SERPFeatureImpact]:
        """
        Analyze SERP features and calculate visual position displacement.
        """
        impacts = []
        
        for query, serp in serp_data.items():
            # Find user's organic position
            user_position = self._find_user_position(serp, site_domain)
            if not user_position:
                continue
            
            # Identify SERP features above user's result
            features_above = self._identify_features_above(serp, user_position)
            
            # Calculate visual position
            visual_position = self._calculate_visual_position(
                user_position,
                features_above
            )
            
            # Calculate CTR impact
            baseline_ctr = self.baseline_ctr.get(user_position, 0.005)
            adjusted_ctr = self._calculate_adjusted_ctr(
                user_position,
                features_above
            )
            ctr_loss = baseline_ctr - adjusted_ctr
            
            # Get impressions from GSC data
            gsc_row = gsc_data[gsc_data['query'] == query]
            impressions = int(gsc_row['impressions'].iloc[0]) if len(gsc_row) > 0 else 0
            
            # Calculate estimated click loss
            estimated_click_loss = int(ctr_loss * impressions)
            
            # Only report significant impacts
            if visual_position > user_position + 2 or estimated_click_loss > 10:
                impacts.append(SERPFeatureImpact(
                    keyword=query,
                    organic_position=user_position,
                    visual_position=visual_position,
                    features_above=[f['type'] for f in features_above],
                    estimated_ctr_loss=ctr_loss,
                    monthly_impressions=impressions,
                    estimated_click_loss=estimated_click_loss
                ))
        
        # Sort by estimated click loss
        impacts.sort(key=lambda x: x.estimated_click_loss, reverse=True)
        
        return impacts
    
    def _find_user_position(
        self,
        serp: Dict[str, Any],
        site_domain: str
    ) -> Optional[int]:
        """Find user's organic position in SERP results."""
        items = serp.get('items', [])
        
        for item in items:
            if item.get('type') != 'organic':
                continue
            
            url = item.get('url', '')
            domain = item.get('domain', '')
            
            # Check if this is the user's domain
            if site_domain in domain or site_domain in url:
                return item.get('rank_absolute', item.get('rank_group', None))
        
        return None
    
    def _identify_features_above(
        self,
        serp: Dict[str, Any],
        user_position: int
    ) -> List[Dict[str, Any]]:
        """Identify all SERP features appearing above user's result."""
        features = []
        items = serp.get('items', [])
        
        for item in items:
            item_type = item.get('type', '')
            rank = item.get('rank_absolute', item.get('rank_group', 999))
            
            # Skip if below user's position
            if rank >= user_position:
                continue
            
            # Map DataForSEO types to our feature names
            if item_type == 'featured_snippet':
                features.append({'type': 'featured_snippet', 'rank': rank})
            elif item_type == 'answer_box':
                features.append({'type': 'ai_overview', 'rank': rank})
            elif item_type == 'knowledge_graph':
                features.append({'type': 'knowledge_panel', 'rank': rank})
            elif item_type == 'local_pack':
                features.append({'type': 'local_pack', 'rank': rank})
            elif item_type == 'people_also_ask':
                # Count number of questions
                questions = item.get('items', [])
                features.append({
                    'type': 'people_also_ask',
                    'rank': rank,
                    'count': len(questions)
                })
            elif item_type == 'video':
                features.append({'type': 'video_carousel', 'rank': rank})
            elif item_type == 'images':
                features.append({'type': 'image_pack', 'rank': rank})
            elif item_type == 'shopping':
                features.append({'type': 'shopping_results', 'rank': rank})
            elif item_type == 'top_stories':
                features.append({'type': 'top_stories', 'rank': rank})
            elif item_type == 'twitter':
                features.append({'type': 'twitter', 'rank': rank})
            elif 'reddit' in item.get('url', '').lower():
                features.append({'type': 'reddit_threads', 'rank': rank})
        
        return features
    
    def _calculate_visual_position(
        self,
        organic_position: int,
        features_above: List[Dict[str, Any]]
    ) -> float:
        """Calculate visual position accounting for SERP features."""
        visual_position = float(organic_position)
        
        for feature in features_above:
            feature_type = feature['type']
            impact = self.feature_impacts.get(feature_type, {})
            offset = impact.get('visual_offset', 0)
            
            # For PAA, multiply by number of questions
            if feature_type == 'people_also_ask':
                count = feature.get('count', 4)
                offset = offset * count
            
            visual_position += offset
        
        return visual_position
    
    def _calculate_adjusted_ctr(
        self,
        organic_position: int,
        features_above: List[Dict[str, Any]]
    ) -> float:
        """Calculate CTR adjusted for SERP features."""
        baseline_ctr = self.baseline_ctr.get(organic_position, 0.005)
        
        # Apply cumulative multiplier for each feature
        multiplier = 1.0
        for feature in features_above:
            feature_type = feature['type']
            impact = self.feature_impacts.get(feature_type, {})
            feature_multiplier = impact.get('ctr_multiplier', 0.9)
            multiplier *= feature_multiplier
        
        return baseline_ctr * multiplier
    
    def _analyze_competitors(
        self,
        serp_data: Dict[str, Dict[str, Any]],
        gsc_data: pd.DataFrame,
        site_domain: str
    ) -> List[CompetitorDomain]:
        """
        Analyze competing domains across all SERPs.
        """
        # Track competitor appearances
        competitor_data = defaultdict(lambda: {
            'keywords': set(),
            'positions': [],
            'user_positions': [],
            'impressions': [],
            'keyword_details': []
        })
        
        for query, serp in serp_data.items():
            # Get user's position for this keyword
            user_position = self._find_user_position(serp, site_domain)
            if not user_position:
                continue
            
            # Get impressions from GSC
            gsc_row = gsc_data[gsc_data['query'] == query]
            impressions = int(gsc_row['impressions'].iloc[0]) if len(gsc_row) > 0 else 0
            
            # Extract competitor domains from top 20 organic results
            items = serp.get('items', [])
            for item in items:
                if item.get('type') != 'organic':
                    continue
                
                rank = item.get('rank_absolute', item.get('rank_group', 999))
                if rank > 20:
                    continue
                
                domain = item.get('domain', '')
                if not domain or site_domain in domain:
                    continue
                
                # Record this competitor appearance
                competitor_data[domain]['keywords'].add(query)
                competitor_data[domain]['positions'].append(rank)
                competitor_data[domain]['user_positions'].append(user_position)
                competitor_data[domain]['impressions'].append(impressions)
                competitor_data[domain]['keyword_details'].append({
                    'query': query,
                    'competitor_position': rank,
                    'user_position': user_position,
                    'impressions': impressions
                })
        
        # Convert to CompetitorDomain objects
        competitors = []
        total_keywords = len(serp_data)
        
        for domain, data in competitor_data.items():
            keywords_shared = len(data['keywords'])
            
            # Calculate metrics
            avg_position = np.mean(data['positions'])
            avg_user_position = np.mean(data['user_positions'])
            position_advantage = avg_user_position - avg_position  # negative = they rank better
            
            # Overlap score: % of user's keywords where this competitor appears
            overlap_score = keywords_shared / total_keywords
            
            # Estimated traffic share (simplified)
            total_impressions = sum(data['impressions'])
            competitor_traffic = sum(
                imp * self.baseline_ctr.get(int(pos), 0.005)
                for imp, pos in zip(data['impressions'], data['positions'])
            )
            user_traffic = sum(
                imp * self.baseline_ctr.get(int(pos), 0.005)
                for imp, pos in zip(data['impressions'], data['user_positions'])
            )
            traffic_share = competitor_traffic / (competitor_traffic + user_traffic) if (competitor_traffic + user_traffic) > 0 else 0
            
            # Threat level
            if overlap_score > 0.3 and position_advantage < -2:
                threat_level = 'high'
            elif overlap_score > 0.15 and position_advantage < 0:
                threat_level = 'medium'
            else:
                threat_level = 'low'
            
            # Strongest keywords (where competitor has biggest advantage)
            keyword_details = sorted(
                data['keyword_details'],
                key=lambda x: (x['user_position'] - x['competitor_position']) * x['impressions'],
                reverse=True
            )[:5]  # Top 5
            
            competitors.append(CompetitorDomain(
                domain=domain,
                keywords_shared=keywords_shared,
                overlap_score=overlap_score,
                avg_position=avg_position,
                avg_user_position=avg_user_position,
                position_advantage=position_advantage,
                total_impressions=total_impressions,
                estimated_traffic_share=traffic_share,
                threat_level=threat_level,
                common_positions=[int(p) for p in data['positions']],
                strongest_keywords=keyword_details
            ))
        
        # Sort by overlap score
        competitors.sort(key=lambda x: x.overlap_score, reverse=True)
        
        return competitors
    
    def _calculate_competitive_metrics(
        self,
        competitors: List[CompetitorDomain],
        serp_features: List[SERPFeatureImpact],
        gsc_data: pd.DataFrame,
        total_keywords: int
    ) -> Dict[str, Any]:
        """Calculate aggregate competitive metrics."""
        # Primary competitors (appear in >20% of keywords)
        primary_competitors = [
            c.domain for c in competitors
            if c.overlap_score > 0.20
        ]
        
        # Average competitors per keyword
        total_competitor_appearances = sum(c.keywords_shared for c in competitors)
        avg_competitors_per_keyword = (
            total_competitor_appearances / total_keywords
            if total_keywords > 0 else 0
        )
        
        # Click share calculation
        # User's estimated clicks
        user_clicks = 0
        total_available_clicks = 0
        
        for _, row in gsc_data.iterrows():
            impressions = row.get('impressions', 0)
            position = row.get('position', 20)
            ctr = self.baseline_ctr.get(int(position), 0.005)
            
            user_clicks += impressions * ctr
            
            # Total available: assume all top 10 positions get clicked
            for pos in range(1, 11):
                total_available_clicks += impressions * self.baseline_ctr.get(pos, 0.005)
        
        total_click_share = user_clicks / total_available_clicks if total_available_clicks > 0 else 0
        
        # Potential click share (if moved to position 1 for all keywords)
        potential_clicks = sum(
            row.get('impressions', 0) * self.baseline_ctr[1]
            for _, row in gsc_data.iterrows()
        )
        potential_click_share = potential_clicks / total_available_clicks if total_available_clicks > 0 else 0
        
        # SERP feature summary
        serp_feature_summary = Counter()
        for impact in serp_features:
            for feature in impact.features_above:
                serp_feature_summary[feature] += 1
        
        # Competitive pressure score
        # Factors: number of strong competitors, average overlap, average position disadvantage
        if competitors:
            high_threat_count = sum(1 for c in competitors if c.threat_level == 'high')
            avg_overlap = np.mean([c.overlap_score for c in competitors[:10]])  # Top 10
            avg_position_gap = np.mean([abs(c.position_advantage) for c in competitors[:10]])
            
            pressure_score = (
                (high_threat_count / 10) * 0.4 +  # 0-1 normalized
                avg_overlap * 0.3 +
                min(avg_position_gap / 5, 1.0) * 0.3  # 0-1 normalized
            )
        else:
            pressure_score = 0.0
        
        return {
            'primary_competitors': primary_competitors,
            'avg_competitors_per_keyword': avg_competitors_per_keyword,
            'total_click_share': total_click_share,
            'potential_click_share': potential_click_share,
            'serp_feature_summary': dict(serp_feature_summary),
            'competitive_pressure_score': min(pressure_score, 1.0)
        }
    
    async def _store_results(
        self,
        report_id: str,
        result: CompetitorAnalysisResult
    ):
        """Store Module 3 results in database."""
        await self.db.execute(
            """
            INSERT INTO module_results (report_id, module_number, module_name, results, generated_at)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (report_id, module_number)
            DO UPDATE SET results = $4, generated_at = $5
            """,
            report_id,
            3,
            'competitor_analysis',
            asdict(result),
            datetime.utcnow()
        )


async def run_module_3(
    db: Database,
    report_id: str,
    site_domain: str,
    module_1_results: Dict[str, Any],
    gsc_keyword_data: pd.DataFrame,
    max_keywords: int = 50
) -> CompetitorAnalysisResult:
    """
    Convenience function to run Module 3 analysis.
    
    Args:
        db: Database instance
        report_id: Unique report identifier
        site_domain: User's domain
        module_1_results: Results from Module 1
        gsc_keyword_data: GSC keyword performance data
        max_keywords: Maximum keywords to analyze
        
    Returns:
        CompetitorAnalysisResult
    """
    module = Module3CompetitorAnalysis(db)
    return await module.analyze(
        report_id=report_id,
        site_domain=site_domain,
        module_1_results=module_1_results,
        gsc_keyword_data=gsc_keyword_data,
        max_keywords=max_keywords
    )
