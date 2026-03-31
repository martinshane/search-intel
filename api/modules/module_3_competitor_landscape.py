"""
Module 3: Competitor Landscape Analysis

Analyzes SERP competition by:
1. Fetching top keywords from GSC data
2. Getting live SERP data from DataForSEO
3. Identifying competitor domains
4. Calculating visibility scores
5. Mapping keyword overlap
6. Identifying keyword gaps
"""

import logging
from typing import Dict, List, Any, Optional
import pandas as pd
from datetime import datetime, timedelta
from collections import defaultdict
import os

logger = logging.getLogger(__name__)


class CompetitorLandscapeAnalyzer:
    """Analyzes competitor landscape using SERP data"""
    
    def __init__(self, dataforseo_client=None):
        """
        Initialize analyzer
        
        Args:
            dataforseo_client: DataForSEO API client instance
        """
        self.dataforseo_client = dataforseo_client
        self.analyzed_domain = None
        
    def analyze(
        self,
        gsc_data: pd.DataFrame,
        site_domain: str,
        top_n_keywords: int = 20,
        min_impressions: int = 100
    ) -> Dict[str, Any]:
        """
        Perform complete competitor landscape analysis
        
        Args:
            gsc_data: GSC query performance data
            site_domain: The domain being analyzed
            top_n_keywords: Number of top keywords to analyze
            min_impressions: Minimum impressions threshold
            
        Returns:
            Dictionary with competitor analysis results
        """
        try:
            logger.info(f"Starting competitor landscape analysis for {site_domain}")
            self.analyzed_domain = self._normalize_domain(site_domain)
            
            # Step 1: Get top keywords from GSC
            top_keywords = self._get_top_keywords(gsc_data, top_n_keywords, min_impressions)
            
            if not top_keywords:
                logger.warning("No keywords found meeting criteria")
                return self._empty_result()
            
            logger.info(f"Selected {len(top_keywords)} top keywords for SERP analysis")
            
            # Step 2: Fetch SERP data for each keyword
            serp_results = self._fetch_serp_data(top_keywords)
            
            if not serp_results:
                logger.warning("No SERP data retrieved")
                return self._empty_result()
            
            # Step 3: Extract competitors and calculate metrics
            competitors_data = self._analyze_competitors(serp_results, top_keywords, gsc_data)
            
            # Step 4: Identify keyword gaps
            keyword_gaps = self._identify_keyword_gaps(serp_results, gsc_data)
            
            # Step 5: Find competitive keywords
            competitive_keywords = self._identify_competitive_keywords(serp_results, gsc_data)
            
            return {
                "success": True,
                "analyzed_domain": self.analyzed_domain,
                "keywords_analyzed": len(top_keywords),
                "serp_data_retrieved": len(serp_results),
                "top_competitors": competitors_data,
                "keyword_gaps": keyword_gaps,
                "competitive_keywords": competitive_keywords,
                "summary": self._generate_summary(competitors_data, keyword_gaps, competitive_keywords)
            }
            
        except Exception as e:
            logger.error(f"Error in competitor landscape analysis: {str(e)}", exc_info=True)
            return {
                "success": False,
                "error": str(e),
                "analyzed_domain": site_domain,
                **self._empty_result()
            }
    
    def _get_top_keywords(
        self,
        gsc_data: pd.DataFrame,
        top_n: int,
        min_impressions: int
    ) -> List[Dict[str, Any]]:
        """
        Extract top keywords from GSC data
        
        Args:
            gsc_data: GSC performance data
            top_n: Number of keywords to return
            min_impressions: Minimum impression threshold
            
        Returns:
            List of keyword dictionaries with metrics
        """
        try:
            # Ensure required columns exist
            required_cols = ['query', 'impressions', 'clicks', 'position']
            if not all(col in gsc_data.columns for col in required_cols):
                logger.error(f"Missing required columns. Have: {gsc_data.columns.tolist()}")
                return []
            
            # Filter by minimum impressions
            filtered = gsc_data[gsc_data['impressions'] >= min_impressions].copy()
            
            if filtered.empty:
                logger.warning(f"No keywords with >= {min_impressions} impressions")
                return []
            
            # Remove branded keywords (containing domain name)
            if self.analyzed_domain:
                domain_parts = self.analyzed_domain.replace('www.', '').split('.')[0]
                filtered = filtered[~filtered['query'].str.contains(domain_parts, case=False, na=False)]
            
            # Sort by impressions (search volume proxy)
            filtered = filtered.sort_values('impressions', ascending=False)
            
            # Take top N
            top_keywords_df = filtered.head(top_n)
            
            # Convert to list of dicts
            keywords = []
            for _, row in top_keywords_df.iterrows():
                keywords.append({
                    'keyword': row['query'],
                    'impressions': int(row['impressions']),
                    'clicks': int(row['clicks']),
                    'position': float(row['position']),
                    'ctr': float(row.get('ctr', 0))
                })
            
            return keywords
            
        except Exception as e:
            logger.error(f"Error extracting top keywords: {str(e)}", exc_info=True)
            return []
    
    def _fetch_serp_data(self, keywords: List[Dict[str, Any]]) -> Dict[str, List[Dict]]:
        """
        Fetch SERP data from DataForSEO for each keyword
        
        Args:
            keywords: List of keyword dictionaries
            
        Returns:
            Dictionary mapping keywords to SERP results
        """
        serp_results = {}
        
        if not self.dataforseo_client:
            logger.warning("No DataForSEO client available, using mock data")
            return self._generate_mock_serp_data(keywords)
        
        try:
            for kw_data in keywords:
                keyword = kw_data['keyword']
                
                try:
                    # Call DataForSEO API
                    results = self.dataforseo_client.get_serp_results(
                        keyword=keyword,
                        location_code=2840,  # USA
                        language_code="en"
                    )
                    
                    if results and 'items' in results:
                        serp_results[keyword] = results['items']
                        logger.info(f"Retrieved SERP data for '{keyword}': {len(results['items'])} results")
                    else:
                        logger.warning(f"No SERP results for '{keyword}'")
                        
                except Exception as e:
                    logger.error(f"Error fetching SERP for '{keyword}': {str(e)}")
                    continue
            
            return serp_results
            
        except Exception as e:
            logger.error(f"Error in SERP data fetch: {str(e)}", exc_info=True)
            return {}
    
    def _generate_mock_serp_data(self, keywords: List[Dict[str, Any]]) -> Dict[str, List[Dict]]:
        """Generate mock SERP data for testing"""
        mock_competitors = [
            'hubspot.com', 'salesforce.com', 'zoho.com', 
            'pipedrive.com', 'monday.com', 'freshworks.com',
            'zendesk.com', 'intercom.com', 'drift.com'
        ]
        
        serp_results = {}
        
        for kw_data in keywords:
            keyword = kw_data['keyword']
            results = []
            
            # Generate 10 organic results
            for i in range(10):
                position = i + 1
                # Randomly assign competitors, ensuring analyzed domain appears sometimes
                if i < 8:  # First 8 are competitors
                    domain = mock_competitors[i % len(mock_competitors)]
                else:  # Last 2 might be the analyzed site
                    domain = self.analyzed_domain if i == 7 else mock_competitors[i % len(mock_competitors)]
                
                results.append({
                    'type': 'organic',
                    'rank_group': position,
                    'rank_absolute': position,
                    'position': 'left',
                    'domain': domain,
                    'url': f'https://{domain}/page-{i+1}',
                    'title': f'Title for {keyword} - {domain}',
                    'description': f'Description about {keyword}'
                })
            
            serp_results[keyword] = results
        
        return serp_results
    
    def _analyze_competitors(
        self,
        serp_results: Dict[str, List[Dict]],
        keywords: List[Dict[str, Any]],
        gsc_data: pd.DataFrame
    ) -> List[Dict[str, Any]]:
        """
        Analyze competitors from SERP data
        
        Args:
            serp_results: SERP data by keyword
            keywords: Original keyword list with metrics
            gsc_data: GSC data for the analyzed site
            
        Returns:
            List of competitor dictionaries with metrics
        """
        # Build keyword metrics lookup
        keyword_metrics = {kw['keyword']: kw for kw in keywords}
        
        # Track competitor data
        competitor_data = defaultdict(lambda: {
            'domain': '',
            'appearances': 0,
            'total_weighted_position': 0,
            'positions': [],
            'keywords': [],
            'keyword_positions': {}
        })
        
        # Get site's keyword positions
        site_positions = self._get_site_keyword_positions(gsc_data)
        
        # Process each SERP result
        for keyword, results in serp_results.items():
            search_volume = keyword_metrics.get(keyword, {}).get('impressions', 0)
            
            for result in results:
                if result.get('type') != 'organic':
                    continue
                
                domain = self._normalize_domain(result.get('domain', ''))
                
                # Skip the analyzed domain
                if domain == self.analyzed_domain:
                    continue
                
                # Skip empty domains
                if not domain:
                    continue
                
                position = result.get('rank_absolute', result.get('rank_group', 999))
                
                # Only consider top 20 positions
                if position > 20:
                    continue
                
                # Record competitor data
                competitor_data[domain]['domain'] = domain
                competitor_data[domain]['appearances'] += 1
                competitor_data[domain]['positions'].append(position)
                competitor_data[domain]['keywords'].append(keyword)
                competitor_data[domain]['keyword_positions'][keyword] = position
                
                # Calculate weighted position (position weighted by search volume)
                # Lower positions are better, so we use position directly
                # Weight by log of search volume to avoid dominance by few high-volume keywords
                import math
                weight = math.log1p(search_volume)
                competitor_data[domain]['total_weighted_position'] += position * weight
        
        # Calculate final metrics for each competitor
        competitors = []
        for domain, data in competitor_data.items():
            if data['appearances'] == 0:
                continue
            
            # Calculate visibility score (inverse of weighted average position)
            # Higher score = better visibility
            avg_weighted_position = data['total_weighted_position'] / data['appearances']
            visibility_score = 100 / avg_weighted_position if avg_weighted_position > 0 else 0
            
            # Count shared keywords (keywords where both site and competitor rank)
            shared_keywords = []
            for kw in data['keywords']:
                if kw in site_positions:
                    shared_keywords.append({
                        'keyword': kw,
                        'site_position': site_positions[kw],
                        'competitor_position': data['keyword_positions'][kw],
                        'position_diff': site_positions[kw] - data['keyword_positions'][kw]
                    })
            
            competitors.append({
                'domain': domain,
                'visibility_score': round(visibility_score, 2),
                'appearances': data['appearances'],
                'avg_position': round(sum(data['positions']) / len(data['positions']), 1),
                'shared_keywords_count': len(shared_keywords),
                'shared_keywords': shared_keywords,
                'all_keywords': data['keywords']
            })
        
        # Sort by visibility score
        competitors.sort(key=lambda x: x['visibility_score'], reverse=True)
        
        return competitors
    
    def _identify_keyword_gaps(
        self,
        serp_results: Dict[str, List[Dict]],
        gsc_data: pd.DataFrame
    ) -> List[Dict[str, Any]]:
        """
        Identify keywords where competitors rank but the site doesn't
        
        Args:
            serp_results: SERP data by keyword
            gsc_data: GSC data for the site
            
        Returns:
            List of keyword gap opportunities
        """
        keyword_gaps = []
        site_positions = self._get_site_keyword_positions(gsc_data)
        
        for keyword, results in serp_results.items():
            # Check if site ranks for this keyword
            site_ranks = site_positions.get(keyword, None)
            
            # If site doesn't rank or ranks very low (>20)
            if site_ranks is None or site_ranks > 20:
                # Find which competitors rank well
                ranking_competitors = []
                for result in results:
                    if result.get('type') != 'organic':
                        continue
                    
                    domain = self._normalize_domain(result.get('domain', ''))
                    if domain == self.analyzed_domain or not domain:
                        continue
                    
                    position = result.get('rank_absolute', result.get('rank_group', 999))
                    if position <= 10:  # Top 10 only
                        ranking_competitors.append({
                            'domain': domain,
                            'position': position,
                            'url': result.get('url', '')
                        })
                
                if ranking_competitors:
                    # Get search volume from original keyword data
                    search_volume = 0
                    for row_idx, row in gsc_data.iterrows():
                        if row.get('query') == keyword:
                            search_volume = int(row.get('impressions', 0))
                            break
                    
                    keyword_gaps.append({
                        'keyword': keyword,
                        'search_volume': search_volume,
                        'site_position': site_ranks if site_ranks else None,
                        'top_competitors': ranking_competitors[:5],
                        'opportunity_score': self._calculate_opportunity_score(
                            search_volume,
                            len(ranking_competitors),
                            site_ranks
                        )
                    })
        
        # Sort by opportunity score
        keyword_gaps.sort(key=lambda x: x['opportunity_score'], reverse=True)
        
        return keyword_gaps
    
    def _identify_competitive_keywords(
        self,
        serp_results: Dict[str, List[Dict]],
        gsc_data: pd.DataFrame
    ) -> List[Dict[str, Any]]:
        """
        Identify keywords where both site and competitors rank
        
        Args:
            serp_results: SERP data by keyword
            gsc_data: GSC data for the site
            
        Returns:
            List of competitive keyword battles
        """
        competitive_keywords = []
        site_positions = self._get_site_keyword_positions(gsc_data)
        
        for keyword, results in serp_results.items():
            site_position = site_positions.get(keyword)
            
            # Only include if site ranks in top 20
            if site_position is None or site_position > 20:
                continue
            
            # Find competitors ranking nearby
            nearby_competitors = []
            for result in results:
                if result.get('type') != 'organic':
                    continue
                
                domain = self._normalize_domain(result.get('domain', ''))
                if domain == self.analyzed_domain or not domain:
                    continue
                
                position = result.get('rank_absolute', result.get('rank_group', 999))
                
                # Consider competitors within 5 positions
                if abs(position - site_position) <= 5 and position <= 20:
                    nearby_competitors.append({
                        'domain': domain,
                        'position': position,
                        'position_diff': position - site_position,
                        'url': result.get('url', '')
                    })
            
            if nearby_competitors:
                # Get keyword metrics
                kw_metrics = gsc_data[gsc_data['query'] == keyword]
                clicks = 0
                impressions = 0
                
                if not kw_metrics.empty:
                    clicks = int(kw_metrics.iloc[0].get('clicks', 0))
                    impressions = int(kw_metrics.iloc[0].get('impressions', 0))
                
                competitive_keywords.append({
                    'keyword': keyword,
                    'site_position': site_position,
                    'clicks': clicks,
                    'impressions': impressions,
                    'nearby_competitors': nearby_competitors,
                    'competition_intensity': len(nearby_competitors),
                    'defense_priority': self._calculate_defense_priority(
                        site_position,
                        clicks,
                        len(nearby_competitors)
                    )
                })
        
        # Sort by defense priority
        competitive_keywords.sort(key=lambda x: x['defense_priority'], reverse=True)
        
        return competitive_keywords
    
    def _get_site_keyword_positions(self, gsc_data: pd.DataFrame) -> Dict[str, float]:
        """Extract site's positions for each keyword"""
        positions = {}
        
        if 'query' not in gsc_data.columns or 'position' not in gsc_data.columns:
            return positions
        
        for _, row in gsc_data.iterrows():
            keyword = row['query']
            position = float(row['position'])
            positions[keyword] = position
        
        return positions
    
    def _calculate_opportunity_score(
        self,
        search_volume: int,
        num_competitors: int,
        current_position: Optional[float]
    ) -> float:
        """
        Calculate opportunity score for a keyword gap
        
        Higher score = better opportunity
        """
        import math
        
        # Base score from search volume (log scale)
        volume_score = math.log1p(search_volume) * 10
        
        # Competition factor (more competitors = validated opportunity)
        competition_score = min(num_competitors * 2, 20)
        
        # Penalty if already ranking but poorly
        position_penalty = 0
        if current_position and current_position > 20:
            position_penalty = (current_position - 20) * 0.5
        
        score = volume_score + competition_score - position_penalty
        
        return round(max(score, 0), 2)
    
    def _calculate_defense_priority(
        self,
        site_position: float,
        clicks: int,
        num_competitors: int
    ) -> float:
        """
        Calculate priority score for defending a competitive keyword
        
        Higher score = more important to defend
        """
        import math
        
        # Value of current traffic (log scale)
        traffic_value = math.log1p(clicks) * 15
        
        # Position vulnerability (lower positions more vulnerable)
        position_factor = 10 / site_position if site_position > 0 else 0
        
        # Competition pressure
        competition_pressure = num_competitors * 3
        
        score = traffic_value + position_factor + competition_pressure
        
        return round(score, 2)
    
    def _normalize_domain(self, domain: str) -> str:
        """Normalize domain to consistent format"""
        if not domain:
            return ''
        
        # Remove protocol
        domain = domain.replace('https://', '').replace('http://', '')
        
        # Remove www.
        domain = domain.replace('www.', '')
        
        # Remove path
        domain = domain.split('/')[0]
        
        # Remove trailing dots
        domain = domain.rstrip('.')
        
        return domain.lower()
    
    def _generate_summary(
        self,
        competitors: List[Dict],
        keyword_gaps: List[Dict],
        competitive_keywords: List[Dict]
    ) -> Dict[str, Any]:
        """Generate summary statistics"""
        
        total_gap_volume = sum(kw.get('search_volume', 0) for kw in keyword_gaps)
        total_competitive_clicks = sum(kw.get('clicks', 0) for kw in competitive_keywords)
        
        # Find primary competitor (highest visibility)
        primary_competitor = competitors[0] if competitors else None
        
        # Count high-priority gaps
        high_priority_gaps = len([kw for kw in keyword_gaps if kw.get('opportunity_score', 0) > 50])
        
        # Count at-risk keywords (competitive keywords where site is losing)
        at_risk_keywords = len([
            kw for kw in competitive_keywords 
            if any(c['position_diff'] < 0 for c in kw.get('nearby_competitors', []))
        ])
        
        return {
            'total_competitors_identified': len(competitors),
            'primary_competitor': primary_competitor['domain'] if primary_competitor else None,
            'primary_competitor_visibility_score': primary_competitor['visibility_score'] if primary_competitor else 0,
            'total_keyword_gaps': len(keyword_gaps),
            'high_priority_gaps': high_priority_gaps,
            'total_gap_search_volume': total_gap_volume,
            'total_competitive_keywords': len(competitive_keywords),
            'at_risk_keywords': at_risk_keywords,
            'total_competitive_clicks': total_competitive_clicks
        }
    
    def _empty_result(self) -> Dict[str, Any]:
        """Return empty result structure"""
        return {
            "keywords_analyzed": 0,
            "serp_data_retrieved": 0,
            "top_competitors": [],
            "keyword_gaps": [],
            "competitive_keywords": [],
            "summary": {
                "total_competitors_identified": 0,
                "primary_competitor": None,
                "primary_competitor_visibility_score": 0,
                "total_keyword_gaps": 0,
                "high_priority_gaps": 0,
                "total_gap_search_volume": 0,
                "total_competitive_keywords": 0,
                "at_risk_keywords": 0,
                "total_competitive_clicks": 0
            }
        }


def analyze_competitor_landscape(
    gsc_data: pd.DataFrame,
    site_domain: str,
    dataforseo_client=None,
    top_n_keywords: int = 20,
    min_impressions: int = 100
) -> Dict[str, Any]:
    """
    Main entry point for competitor landscape analysis
    
    Args:
        gsc_data: GSC query performance data
        site_domain: The domain being analyzed
        dataforseo_client: DataForSEO API client
        top_n_keywords: Number of top keywords to analyze
        min_impressions: Minimum impressions threshold
        
    Returns:
        Analysis results dictionary
    """
    analyzer = CompetitorLandscapeAnalyzer(dataforseo_client)
    return analyzer.analyze(gsc_data, site_domain, top_n_keywords, min_impressions)
