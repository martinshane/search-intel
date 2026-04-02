"""
SERP Intelligence Module

This module handles DataForSEO SERP data fetching and analysis for:
- Module 3: SERP Landscape Analysis
- Module 8: Query Intent Migration Tracking
- Module 11: Competitive Intelligence

Provides competitor data, SERP feature analysis, and ranking positions
for target keywords.
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from collections import Counter, defaultdict
import re

import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from .dataforseo_client import DataForSEOClient
from ..core.database import Database
from ..core.config import settings

logger = logging.getLogger(__name__)


class SERPIntelligence:
    """SERP Intelligence analysis using DataForSEO."""
    
    # SERP feature visual position weights
    FEATURE_WEIGHTS = {
        'featured_snippet': 2.0,
        'knowledge_panel': 1.5,
        'ai_overview': 2.5,
        'local_pack': 2.0,
        'video_carousel': 1.0,
        'image_pack': 0.5,
        'shopping_results': 1.0,
        'top_stories': 1.0,
        'people_also_ask': 0.5,  # per question
        'twitter': 0.5,
        'reddit_threads': 0.5,
    }
    
    # Intent classification patterns
    INTENT_PATTERNS = {
        'informational': [
            r'\bhow\s+to\b',
            r'\bwhat\s+is\b',
            r'\bwhy\s+',
            r'\bguide\b',
            r'\btutorial\b',
            r'\blearn\b',
            r'\bexplain\b',
        ],
        'commercial': [
            r'\bbest\b',
            r'\btop\s+\d+',
            r'\breview',
            r'\bcompare',
            r'\bvs\b',
            r'\balternative',
            r'\bcheap',
            r'\baffordable',
        ],
        'transactional': [
            r'\bbuy\b',
            r'\bprice',
            r'\bcost',
            r'\bdeal',
            r'\bdiscount',
            r'\bcoupon',
            r'\bfor\s+sale\b',
            r'\border\b',
        ],
        'navigational': [
            r'\blogin\b',
            r'\bsign\s+in\b',
            r'\bcontact\b',
            r'\baddress\b',
            r'\blocation\b',
        ],
    }
    
    def __init__(self, db: Database):
        """Initialize SERP Intelligence module."""
        self.db = db
        self.client = DataForSEOClient()
    
    async def analyze_serp_landscape(
        self,
        site_id: str,
        keywords: List[Dict[str, Any]],
        gsc_keyword_data: pd.DataFrame,
        location_code: int = 2840,  # United States
        language_code: str = "en",
    ) -> Dict[str, Any]:
        """
        Analyze SERP landscape for Module 3.
        
        Args:
            site_id: Site identifier
            keywords: List of keyword dicts with 'query', 'impressions', 'position'
            gsc_keyword_data: GSC keyword performance data
            location_code: DataForSEO location code
            language_code: Language code
            
        Returns:
            SERP landscape analysis results
        """
        logger.info(f"Analyzing SERP landscape for {len(keywords)} keywords")
        
        # Check cache first
        cached_data = await self._get_cached_serp_data(site_id, keywords)
        keywords_to_fetch = [
            k for k in keywords 
            if k['query'] not in cached_data
        ]
        
        # Fetch live SERP data for uncached keywords
        if keywords_to_fetch:
            logger.info(f"Fetching live SERP data for {len(keywords_to_fetch)} keywords")
            serp_results = await self._fetch_serp_data_batch(
                keywords_to_fetch,
                location_code,
                language_code
            )
            
            # Cache results
            await self._cache_serp_data(site_id, serp_results)
            cached_data.update(serp_results)
        
        serp_data = cached_data
        
        # Analyze SERP features
        feature_analysis = self._analyze_serp_features(
            serp_data,
            gsc_keyword_data
        )
        
        # Map competitors
        competitor_analysis = self._analyze_competitors(
            serp_data,
            gsc_keyword_data
        )
        
        # Classify intents
        intent_analysis = self._classify_serp_intents(serp_data)
        
        # Estimate click share
        click_share = self._estimate_click_share(
            serp_data,
            gsc_keyword_data
        )
        
        return {
            'keywords_analyzed': len(serp_data),
            'serp_feature_displacement': feature_analysis['displacement'],
            'feature_summary': feature_analysis['summary'],
            'competitors': competitor_analysis['competitors'],
            'competitor_overlap_matrix': competitor_analysis['overlap_matrix'],
            'intent_distribution': intent_analysis['distribution'],
            'intent_mismatches': intent_analysis['mismatches'],
            'total_click_share': click_share['total_share'],
            'click_share_opportunity': click_share['opportunity'],
            'click_share_by_keyword': click_share['by_keyword'],
            'timestamp': datetime.utcnow().isoformat(),
        }
    
    async def _fetch_serp_data_batch(
        self,
        keywords: List[Dict[str, Any]],
        location_code: int,
        language_code: str,
        batch_size: int = 20,
    ) -> Dict[str, Dict[str, Any]]:
        """Fetch SERP data in batches."""
        results = {}
        
        for i in range(0, len(keywords), batch_size):
            batch = keywords[i:i + batch_size]
            
            # Prepare tasks for DataForSEO
            tasks = []
            for kw in batch:
                task = {
                    'keyword': kw['query'],
                    'location_code': location_code,
                    'language_code': language_code,
                    'device': 'desktop',
                    'depth': 100,  # Get top 100 results
                }
                tasks.append(task)
            
            # Fetch SERP data
            try:
                batch_results = await self.client.serp_google_organic_live(tasks)
                
                for task, result in zip(batch, batch_results):
                    if result and 'items' in result:
                        results[task['keyword']] = self._parse_serp_result(result)
                    else:
                        logger.warning(f"No SERP data for keyword: {task['keyword']}")
                        results[task['keyword']] = self._empty_serp_result()
                
                # Rate limiting
                if i + batch_size < len(keywords):
                    await asyncio.sleep(1)
                    
            except Exception as e:
                logger.error(f"Error fetching SERP data batch: {str(e)}")
                # Fill with empty results
                for kw in batch:
                    results[kw['query']] = self._empty_serp_result()
        
        return results
    
    def _parse_serp_result(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """Parse DataForSEO SERP result into structured format."""
        items = result.get('items', [{}])[0]
        
        # Extract organic results
        organic_results = []
        for item in items.get('items', []):
            if item.get('type') == 'organic':
                organic_results.append({
                    'position': item.get('rank_absolute', 0),
                    'url': item.get('url', ''),
                    'domain': item.get('domain', ''),
                    'title': item.get('title', ''),
                    'description': item.get('description', ''),
                })
        
        # Extract SERP features
        features = []
        feature_items = items.get('items', [])
        
        for item in feature_items:
            item_type = item.get('type', '')
            
            if item_type == 'featured_snippet':
                features.append({
                    'type': 'featured_snippet',
                    'position': item.get('rank_absolute', 0),
                    'domain': item.get('domain', ''),
                })
            elif item_type == 'knowledge_panel':
                features.append({
                    'type': 'knowledge_panel',
                    'position': item.get('rank_absolute', 0),
                })
            elif item_type == 'local_pack':
                features.append({
                    'type': 'local_pack',
                    'position': item.get('rank_absolute', 0),
                })
            elif item_type == 'people_also_ask':
                features.append({
                    'type': 'people_also_ask',
                    'position': item.get('rank_absolute', 0),
                    'count': len(item.get('items', [])),
                })
            elif item_type == 'video':
                features.append({
                    'type': 'video_carousel',
                    'position': item.get('rank_absolute', 0),
                })
            elif item_type == 'images':
                features.append({
                    'type': 'image_pack',
                    'position': item.get('rank_absolute', 0),
                })
            elif item_type == 'shopping':
                features.append({
                    'type': 'shopping_results',
                    'position': item.get('rank_absolute', 0),
                })
            elif item_type == 'top_stories':
                features.append({
                    'type': 'top_stories',
                    'position': item.get('rank_absolute', 0),
                })
            elif item_type == 'twitter':
                features.append({
                    'type': 'twitter',
                    'position': item.get('rank_absolute', 0),
                })
            elif 'reddit' in item.get('url', '').lower():
                features.append({
                    'type': 'reddit_threads',
                    'position': item.get('rank_absolute', 0),
                })
        
        # Check for AI Overview (may be in different format)
        if items.get('ai_overview'):
            features.append({
                'type': 'ai_overview',
                'position': 0,  # Usually at top
            })
        
        return {
            'organic_results': organic_results,
            'features': features,
            'total_results': items.get('se_results_count', 0),
            'fetched_at': datetime.utcnow().isoformat(),
        }
    
    def _empty_serp_result(self) -> Dict[str, Any]:
        """Return empty SERP result structure."""
        return {
            'organic_results': [],
            'features': [],
            'total_results': 0,
            'fetched_at': datetime.utcnow().isoformat(),
        }
    
    def _analyze_serp_features(
        self,
        serp_data: Dict[str, Dict[str, Any]],
        gsc_data: pd.DataFrame,
    ) -> Dict[str, Any]:
        """Analyze SERP feature displacement."""
        displacement_issues = []
        feature_counts = Counter()
        
        # Get user's domain from GSC data
        user_domain = self._extract_domain_from_urls(gsc_data)
        
        for keyword, data in serp_data.items():
            # Find user's position in organic results
            user_position = None
            user_url = None
            
            for result in data['organic_results']:
                if user_domain in result['domain']:
                    user_position = result['position']
                    user_url = result['url']
                    break
            
            if user_position is None:
                continue
            
            # Calculate visual position based on features above
            visual_position = user_position
            features_above = []
            
            for feature in data['features']:
                if feature['position'] < user_position:
                    feature_type = feature['type']
                    feature_counts[feature_type] += 1
                    
                    # Add weighted displacement
                    weight = self.FEATURE_WEIGHTS.get(feature_type, 0.5)
                    if feature_type == 'people_also_ask':
                        # Count individual PAA questions
                        count = feature.get('count', 1)
                        weight *= count
                    
                    visual_position += weight
                    features_above.append({
                        'type': feature_type,
                        'count': feature.get('count', 1),
                    })
            
            # Calculate CTR impact
            organic_ctr = self._estimate_position_ctr(user_position, False)
            visual_ctr = self._estimate_position_ctr(visual_position, True)
            ctr_impact = visual_ctr - organic_ctr
            
            # Flag significant displacement
            if visual_position > user_position + 3 and abs(ctr_impact) > 0.02:
                displacement_issues.append({
                    'keyword': keyword,
                    'organic_position': user_position,
                    'visual_position': round(visual_position, 1),
                    'displacement': round(visual_position - user_position, 1),
                    'features_above': features_above,
                    'estimated_ctr_impact': round(ctr_impact, 4),
                    'url': user_url,
                })
        
        # Sort by impact
        displacement_issues.sort(
            key=lambda x: abs(x['estimated_ctr_impact']),
            reverse=True
        )
        
        return {
            'displacement': displacement_issues[:50],  # Top 50
            'summary': {
                'total_keywords_displaced': len(displacement_issues),
                'avg_displacement': round(
                    np.mean([d['displacement'] for d in displacement_issues]) 
                    if displacement_issues else 0, 
                    2
                ),
                'total_ctr_impact': round(
                    sum(d['estimated_ctr_impact'] for d in displacement_issues),
                    4
                ),
                'feature_frequency': dict(feature_counts.most_common()),
            }
        }
    
    def _analyze_competitors(
        self,
        serp_data: Dict[str, Dict[str, Any]],
        gsc_data: pd.DataFrame,
    ) -> Dict[str, Any]:
        """Analyze competitor presence and overlap."""
        user_domain = self._extract_domain_from_urls(gsc_data)
        
        # Count competitor appearances
        competitor_appearances = defaultdict(list)
        
        for keyword, data in serp_data.items():
            for result in data['organic_results']:
                domain = result['domain']
                if domain and domain != user_domain:
                    competitor_appearances[domain].append({
                        'keyword': keyword,
                        'position': result['position'],
                    })
        
        # Calculate competitor metrics
        competitors = []
        for domain, appearances in competitor_appearances.items():
            if len(appearances) < 3:  # Filter out one-off competitors
                continue
            
            positions = [a['position'] for a in appearances]
            keywords_shared = len(appearances)
            
            # Threat level calculation
            avg_position = np.mean(positions)
            keyword_overlap = keywords_shared / len(serp_data)
            
            # Simple threat scoring
            if avg_position <= 3 and keyword_overlap > 0.3:
                threat_level = 'critical'
            elif avg_position <= 5 and keyword_overlap > 0.2:
                threat_level = 'high'
            elif avg_position <= 10 and keyword_overlap > 0.1:
                threat_level = 'medium'
            else:
                threat_level = 'low'
            
            competitors.append({
                'domain': domain,
                'keywords_shared': keywords_shared,
                'keyword_overlap_pct': round(keyword_overlap * 100, 1),
                'avg_position': round(avg_position, 1),
                'best_position': min(positions),
                'worst_position': max(positions),
                'threat_level': threat_level,
                'sample_keywords': [a['keyword'] for a in appearances[:5]],
            })
        
        # Sort by threat
        threat_order = {'critical': 0, 'high': 1, 'medium': 2, 'low': 3}
        competitors.sort(
            key=lambda x: (threat_order[x['threat_level']], -x['keywords_shared'])
        )
        
        # Build overlap matrix for top competitors
        top_competitors = [c['domain'] for c in competitors[:10]]
        overlap_matrix = self._build_overlap_matrix(
            serp_data,
            top_competitors,
            user_domain
        )
        
        return {
            'competitors': competitors,
            'overlap_matrix': overlap_matrix,
        }
    
    def _build_overlap_matrix(
        self,
        serp_data: Dict[str, Dict[str, Any]],
        domains: List[str],
        user_domain: str,
    ) -> List[Dict[str, Any]]:
        """Build keyword overlap matrix between competitors."""
        matrix = []
        
        # Build domain -> keyword sets
        domain_keywords = defaultdict(set)
        for keyword, data in serp_data.items():
            for result in data['organic_results'][:10]:  # Top 10 only
                domain = result['domain']
                if domain in domains or domain == user_domain:
                    domain_keywords[domain].add(keyword)
        
        # Calculate pairwise overlaps
        all_domains = [user_domain] + domains
        for i, domain1 in enumerate(all_domains):
            for domain2 in all_domains[i+1:]:
                kw1 = domain_keywords[domain1]
                kw2 = domain_keywords[domain2]
                
                if not kw1 or not kw2:
                    continue
                
                overlap = len(kw1 & kw2)
                jaccard = overlap / len(kw1 | kw2)
                
                matrix.append({
                    'domain1': domain1,
                    'domain2': domain2,
                    'shared_keywords': overlap,
                    'jaccard_similarity': round(jaccard, 3),
                })
        
        return matrix
    
    def _classify_serp_intents(
        self,
        serp_data: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Classify SERP intents based on features and content."""
        intent_distribution = Counter()
        intent_by_keyword = {}
        
        for keyword, data in serp_data.items():
            # Score each intent type
            intent_scores = {
                'informational': 0,
                'commercial': 0,
                'transactional': 0,
                'navigational': 0,
            }
            
            # Pattern matching on keyword
            query_lower = keyword.lower()
            for intent, patterns in self.INTENT_PATTERNS.items():
                for pattern in patterns:
                    if re.search(pattern, query_lower):
                        intent_scores[intent] += 2
            
            # SERP feature signals
            features = {f['type'] for f in data['features']}
            
            if 'knowledge_panel' in features or 'people_also_ask' in features:
                intent_scores['informational'] += 1
            
            if 'shopping_results' in features:
                intent_scores['transactional'] += 2
            
            if 'local_pack' in features:
                intent_scores['transactional'] += 1
            
            if 'video_carousel' in features:
                intent_scores['informational'] += 1
            
            # Organic result title analysis
            for result in data['organic_results'][:5]:
                title_lower = result.get('title', '').lower()
                
                if any(word in title_lower for word in ['buy', 'price', 'shop', 'deal']):
                    intent_scores['transactional'] += 0.5
                
                if any(word in title_lower for word in ['best', 'review', 'top', 'vs']):
                    intent_scores['commercial'] += 0.5
                
                if any(word in title_lower for word in ['how', 'what', 'guide', 'tutorial']):
                    intent_scores['informational'] += 0.5
            
            # Determine primary intent
            primary_intent = max(intent_scores.items(), key=lambda x: x[1])[0]
            
            # Require minimum score
            if intent_scores[primary_intent] < 1:
                primary_intent = 'informational'  # Default
            
            intent_distribution[primary_intent] += 1
            intent_by_keyword[keyword] = {
                'primary_intent': primary_intent,
                'scores': intent_scores,
            }
        
        # Calculate intent mismatches (would need page type data)
        # For now, return structure without mismatches
        mismatches = []
        
        return {
            'distribution': dict(intent_distribution),
            'by_keyword': intent_by_keyword,
            'mismatches': mismatches,
        }
    
    def _estimate_click_share(
        self,
        serp_data: Dict[str, Dict[str, Any]],
        gsc_data: pd.DataFrame,
    ) -> Dict[str, Any]:
        """Estimate click share and opportunity."""
        user_domain = self._extract_domain_from_urls(gsc_data)
        
        total_estimated_clicks = 0
        total_available_clicks = 0
        by_keyword = []
        
        # Merge GSC impressions data
        gsc_dict = {}
        if not gsc_data.empty:
            for _, row in gsc_data.iterrows():
                gsc_dict[row['query']] = {
                    'impressions': row.get('impressions', 0),
                    'clicks': row.get('clicks', 0),
                    'position': row.get('position', 100),
                }
        
        for keyword, data in serp_data.items():
            # Find user's position
            user_position = None
            for result in data['organic_results']:
                if user_domain in result['domain']:
                    user_position = result['position']
                    break
            
            if user_position is None:
                continue
            
            # Get impressions from GSC
            impressions = gsc_dict.get(keyword, {}).get('impressions', 0)
            if impressions == 0:
                continue
            
            # Calculate visual position
            visual_position = user_position
            has_features = False
            for feature in data['features']:
                if feature['position'] < user_position:
                    has_features = True
                    weight = self.FEATURE_WEIGHTS.get(feature['type'], 0.5)
                    if feature['type'] == 'people_also_ask':
                        weight *= feature.get('count', 1)
                    visual_position += weight
            
            # Estimate CTR
            estimated_ctr = self._estimate_position_ctr(visual_position, has_features)
            estimated_clicks = impressions * estimated_ctr
            
            # Estimate potential (position 1 CTR)
            potential_ctr = self._estimate_position_ctr(1, False)
            potential_clicks = impressions * potential_ctr
            
            total_estimated_clicks += estimated_clicks
            total_available_clicks += potential_clicks
            
            by_keyword.append({
                'keyword': keyword,
                'impressions': impressions,
                'position': user_position,
                'visual_position': round(visual_position, 1),
                'estimated_ctr': round(estimated_ctr, 4),
                'estimated_clicks': round(estimated_clicks, 1),
                'potential_clicks': round(potential_clicks, 1),
                'click_gap': round(potential_clicks - estimated_clicks, 1),
            })
        
        # Sort by opportunity
        by_keyword.sort(key=lambda x: x['click_gap'], reverse=True)
        
        # Calculate overall metrics
        total_share = (
            total_estimated_clicks / total_available_clicks 
            if total_available_clicks > 0 
            else 0
        )
        opportunity = (
            (total_available_clicks - total_estimated_clicks) / total_available_clicks
            if total_available_clicks > 0
            else 0
        )
        
        return {
            'total_share': round(total_share, 4),
            'opportunity': round(opportunity, 4),
            'estimated_monthly_clicks': round(total_estimated_clicks, 0),
            'potential_monthly_clicks': round(total_available_clicks, 0),
            'by_keyword': by_keyword[:100],  # Top 100
        }
    
    def _estimate_position_ctr(
        self,
        position: float,
        has_features: bool,
    ) -> float:
        """
        Estimate CTR for a given position.
        
        Uses advanced CTR curve model from industry data.
        """
        if position < 1:
            position = 1
        
        # Base CTR curve (without features)
        # Based on aggregate CTR studies
        if position <= 1:
            base_ctr = 0.28
        elif position <= 2:
            base_ctr = 0.15
        elif position <= 3:
            base_ctr = 0.11
        elif position <= 4:
            base_ctr = 0.08
        elif position <= 5:
            base_ctr = 0.06
        elif position <= 10:
            base_ctr = 0.05 * (11 - position) / 5
        else:
            base_ctr = max(0.01, 0.02 * (20 - position) / 10)
        
        # Adjust for SERP features
        if has_features:
            # Features reduce CTR by 20-40% depending on position
            feature_penalty = 0.3 if position <= 3 else 0.2
            base_ctr *= (1 - feature_penalty)
        
        return max(0.001, base_ctr)
    
    def _extract_domain_from_urls(self, gsc_data: pd.DataFrame) -> str:
        """Extract primary domain from GSC URL data."""
        if gsc_data.empty or 'page' not in gsc_data.columns:
            return ''
        
        # Get most common domain from pages
        domains = []
        for url in gsc_data['page'].head(100):
            try:
                if '://' in url:
                    domain = url.split('://')[1].split('/')[0]
                    domains.append(domain)
            except:
                continue
        
        if not domains:
            return ''
        
        return Counter(domains).most_common(1)[0][0]
    
    async def _get_cached_serp_data(
        self,
        site_id: str,
        keywords: List[Dict[str, Any]],
    ) -> Dict[str, Dict[str, Any]]:
        """Get cached SERP data."""
        cached = {}
        
        # Check cache for each keyword
        for kw in keywords:
            query = kw['query']
            cache_key = f"serp:{site_id}:{query}"
            
            result = await self.db.get_cache(cache_key)
            if result:
                cached[query] = result
        
        return cached
    
    async def _cache_serp_data(
        self,
        site_id: str,
        serp_data: Dict[str, Dict[str, Any]],
    ):
        """Cache SERP data with 24 hour TTL."""
        for query, data in serp_data.items():
            cache_key = f"serp:{site_id}:{query}"
            await self.db.set_cache(cache_key, data, ttl=86400)  # 24 hours
    
    async def track_intent_migration(
        self,
        site_id: str,
        keywords: List[str],
        current_serp_data: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Track query intent migration for Module 8.
        
        Compares current SERP features to historical data to identify
        intent shifts over time.
        """
        migrations = []
        
        for keyword in keywords:
            # Get historical SERP data
            historical = await self._get_historical_serp_features(site_id, keyword)
            
            if not historical:
                continue
            
            current = current_serp_data.get(keyword, {})
            
            # Compare feature presence over time
            migration = self._detect_intent_migration(
                keyword,
                historical,
                current
            )
            
            if migration:
                migrations.append(migration)
        
        return {
            'migrations_detected': len(migrations),
            'migrations': migrations,
            'timestamp': datetime.utcnow().isoformat(),
        }
    
    async def _get_historical_serp_features(
        self,
        site_id: str,
        keyword: str,
        lookback_days: int = 90,
    ) -> List[Dict[str, Any]]:
        """Get historical SERP feature data for a keyword."""
        # Query database for historical snapshots
        query = """
            SELECT features, fetched_at
            FROM serp_snapshots
            WHERE site_id = $1
            AND keyword = $2
            AND fetched_at >= $3
            ORDER BY fetched_at DESC
        """
        
        cutoff = datetime.utcnow() - timedelta(days=lookback_days)
        
        try:
            rows = await self.db.fetch(query, site_id, keyword, cutoff)
            return [
                {
                    'features': row['features'],
                    'fetched_at': row['fetched_at'],
                }
                for row in rows
            ]
        except:
            return []
    
    def _detect_intent_migration(
        self,
        keyword: str,
        historical: List[Dict[str, Any]],
        current: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """Detect significant intent migration."""
        if not historical or not current:
            return None
        
        current_features = {f['type'] for f in current.get('features', [])}
        
        # Compare to oldest historical snapshot
        old_features = {f['type'] for f in historical[-1].get('features', [])}
        
        # Calculate feature change
        added_features = current_features - old_features
        removed_features = old_features - current_features
        
        # Determine if this represents intent change
        significant_adds = added_features & {
            'shopping_results', 'ai_overview', 'local_pack'
        }
        significant_removes = removed_features & {
            'shopping_results', 'ai_overview', 'local_pack'
        }
        
        if not (significant_adds or significant_removes):
            return None
        
        # Classify old and new intent
        old_intent = self._infer_intent_from_features(old_features)
        new_intent = self._infer_intent_from_features(current_features)
        
        if old_intent == new_intent:
            return None
        
        return {
            'keyword': keyword,
            'old_intent': old_intent,
            'new_intent': new_intent,
            'added_features': list(added_features),
            'removed_features': list(removed_features),
            'first_detected': historical[-1].get('fetched_at'),
            'last_checked': current.get('fetched_at'),
        }
    
    def _infer_intent_from_features(self, features: set) -> str:
        """Infer dominant intent from SERP features."""
        if 'shopping_results' in features or 'local_pack' in features:
            return 'transactional'
        
        if 'knowledge_panel' in features or 'people_also_ask' in features:
            return 'informational'
        
        if 'video_carousel' in features:
            return 'informational'
        
        # Default
        return 'informational'
    
    async def get_competitor_intelligence(
        self,
        site_id: str,
        competitor_domains: List[str],
        keywords: List[str],
    ) -> Dict[str, Any]:
        """
        Get competitive intelligence for Module 11.
        
        Analyzes competitor positions, content patterns, and strategies.
        """
        # Get SERP data for keywords
        keyword_objs = [{'query': k, 'impressions': 1000} for k in keywords]
        serp_data = await self._get_cached_serp_data(site_id, keyword_objs)
        
        # If not cached, fetch
        if len(serp_data) < len(keywords) * 0.5:  # Less than 50% cached
            new_serp = await self._fetch_serp_data_batch(keyword_objs, 2840, 'en')
            serp_data.update(new_serp)
            await self._cache_serp_data(site_id, new_serp)
        
        # Analyze each competitor
        competitor_profiles = []
        
        for domain in competitor_domains:
            profile = self._analyze_competitor_profile(
                domain,
                serp_data,
                keywords
            )
            competitor_profiles.append(profile)
        
        # Identify content gaps
        content_gaps = self._identify_content_gaps(
            competitor_profiles,
            serp_data
        )
        
        return {
            'competitors_analyzed': len(competitor_profiles),
            'competitor_profiles': competitor_profiles,
            'content_gaps': content_gaps,
            'timestamp': datetime.utcnow().isoformat(),
        }
    
    def _analyze_competitor_profile(
        self,
        domain: str,
        serp_data: Dict[str, Dict[str, Any]],
        keywords: List[str],
    ) -> Dict[str, Any]:
        """Analyze a single competitor's profile."""
        appearances = []
        
        for keyword, data in serp_data.items():
            for result in data.get('organic_results', []):
                if domain in result.get('domain', ''):
                    appearances.append({
                        'keyword': keyword,
                        'position': result['position'],
                        'url': result['url'],
                        'title': result.get('title', ''),
                    })
        
        if not appearances:
            return {
                'domain': domain,
                'visibility': 0,
                'avg_position': 0,
                'keywords_ranking': 0,
            }
        
        # Calculate metrics
        positions = [a['position'] for a in appearances]
        
        # Extract URL patterns
        urls = [a['url'] for a in appearances]
        url_patterns = self._extract_url_patterns(urls)
        
        # Analyze title patterns
        titles = [a['title'] for a in appearances]
        title_patterns = self._extract_title_patterns(titles)
        
        return {
            'domain': domain,
            'visibility': round(len(appearances) / len(keywords), 3),
            'keywords_ranking': len(appearances),
            'avg_position': round(np.mean(positions), 1),
            'median_position': round(np.median(positions), 1),
            'top_3_count': sum(1 for p in positions if p <= 3),
            'top_10_count': sum(1 for p in positions if p <= 10),
            'url_patterns': url_patterns,
            'title_patterns': title_patterns,
            'sample_rankings': appearances[:10],
        }
    
    def _extract_url_patterns(self, urls: List[str]) -> Dict[str, Any]:
        """Extract common URL structure patterns."""
        if not urls:
            return {}
        
        # Extract path segments
        path_segments = []
        for url in urls:
            try:
                path = url.split('://')[1].split('?')[0]
                segments = [s for s in path.split('/') if s]
                path_segments.extend(segments[1:])  # Skip domain
            except:
                continue
        
        # Find common segments
        segment_counts = Counter(path_segments)
        
        return {
            'common_segments': dict(segment_counts.most_common(10)),
            'avg_path_depth': round(
                np.mean([url.count('/') - 2 for url in urls]),
                1
            ),
        }
    
    def _extract_title_patterns(self, titles: List[str]) -> Dict[str, Any]:
        """Extract common title patterns."""
        if not titles:
            return {}
        
        # Vectorize titles
        try:
            vectorizer = TfidfVectorizer(
                max_features=50,
                stop_words='english',
                ngram_range=(1, 2)
            )
            tfidf_matrix = vectorizer.fit_transform(titles)
            
            # Get top terms
            feature_names = vectorizer.get_feature_names_out()
            avg_tfidf = np.mean(tfidf_matrix.toarray(), axis=0)
            top_indices = avg_tfidf.argsort()[-10:][::-1]
            
            top_terms = [feature_names[i] for i in top_indices]
        except:
            top_terms = []
        
        # Calculate avg length
        avg_length = round(np.mean([len(t) for t in titles]), 1)
        
        return {
            'common_terms': top_terms,
            'avg_title_length': avg_length,
        }
    
    def _identify_content_gaps(
        self,
        competitor_profiles: List[Dict[str, Any]],
        serp_data: Dict[str, Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Identify content gap opportunities."""
        gaps = []
        
        # Find keywords where competitors rank but user doesn't
        for keyword, data in serp_data.items():
            competitor_count = sum(
                1 for profile in competitor_profiles
                if any(
                    keyword == a['keyword']
                    for a in profile.get('sample_rankings', [])
                )
            )
            
            if competitor_count >= 2:  # Multiple competitors ranking
                # Check if there's a pattern in competitor content
                competitor_urls = [
                    result['url']
                    for result in data.get('organic_results', [])
                    if any(
                        profile['domain'] in result.get('domain', '')
                        for profile in competitor_profiles
                    )
                ]
                
                if len(competitor_urls) >= 2:
                    gaps.append({
                        'keyword': keyword,
                        'competitors_ranking': competitor_count,
                        'avg_competitor_position': round(
                            np.mean([
                                r['position']
                                for r in data.get('organic_results', [])
                                if r['url'] in competitor_urls
                            ]),
                            1
                        ),
                        'opportunity_score': competitor_count * 10,
                    })
        
        # Sort by opportunity
        gaps.sort(key=lambda x: x['opportunity_score'], reverse=True)
        
        return gaps[:50]  # Top 50 gaps
