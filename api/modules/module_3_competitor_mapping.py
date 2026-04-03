import logging
from typing import Dict, Any, List, Optional, Set, Tuple
from datetime import datetime
import pandas as pd
import numpy as np
from collections import defaultdict, Counter
import re

from .module_base import ModuleBase

logger = logging.getLogger(__name__)


class CompetitorMappingModule(ModuleBase):
    """
    Module 3: Competitor Mapping
    
    Identifies and scores competing domains based on:
    - Keyword overlap with site's ranking queries
    - Competitor positions and visibility in SERPs
    - Estimated traffic share and competitive threat
    
    Takes top 20 keywords from Module 1, fetches SERP data via DataForSEO,
    identifies domains ranking in top 10, calculates overlap and metrics,
    returns top 5-10 competitors with detailed scoring.
    """

    def __init__(self, site_id: str):
        super().__init__(site_id, "competitor_mapping")
        self.min_keywords_for_competitor = 2  # Min shared keywords to be considered
        self.top_n_competitors = 10  # Max competitors to return
        self.min_impressions_threshold = 50  # Min monthly impressions to consider query
        self.top_position = 10  # Only consider top 10 organic results

    async def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Main execution method for competitor mapping.

        Args:
            context: Shared context containing:
                - top_keywords: List of top keywords from Module 1
                - gsc_query_data: GSC query performance data
                - serp_results: DataForSEO SERP results
                - site_domain: User's domain

        Returns:
            Dictionary with competitor analysis results matching spec
        """
        try:
            logger.info(f"Starting competitor mapping for site {self.site_id}")

            # Extract required data from context
            top_keywords = context.get("top_keywords", [])
            gsc_query_data = context.get("gsc_query_data", [])
            serp_results = context.get("serp_results", {})
            site_domain = context.get("site_domain", "")

            if not site_domain:
                raise ValueError("site_domain is required in context")

            # Normalize domain for comparison
            site_domain_normalized = self._normalize_domain(site_domain)

            # If no top_keywords provided, extract from GSC data
            if not top_keywords and gsc_query_data:
                top_keywords = self._extract_top_keywords_from_gsc(gsc_query_data, limit=20)

            if not top_keywords:
                logger.warning("No keywords available for competitor analysis")
                return self._empty_result()

            logger.info(f"Analyzing {len(top_keywords)} keywords for competitors")

            # Build keyword metadata from GSC data
            keyword_metadata = self._build_keyword_metadata(gsc_query_data, top_keywords)

            # Extract competitor domains from SERP results
            competitor_data = self._extract_competitors_from_serps(
                serp_results, 
                top_keywords, 
                site_domain_normalized
            )

            if not competitor_data:
                logger.warning("No competitor domains found in SERP data")
                return self._empty_result()

            # Score and rank competitors
            scored_competitors = self._score_competitors(
                competitor_data,
                keyword_metadata,
                site_domain_normalized,
                len(top_keywords)
            )

            # Generate final result structure
            result = self._build_result(
                scored_competitors,
                len(top_keywords),
                site_domain_normalized
            )

            logger.info(f"Competitor mapping complete. Found {len(result['competitors'])} competitors")
            return result

        except Exception as e:
            logger.error(f"Error in competitor mapping execution: {str(e)}", exc_info=True)
            raise

    def _normalize_domain(self, domain: str) -> str:
        """Normalize domain for consistent comparison."""
        domain = domain.lower().strip()
        # Remove protocol
        domain = re.sub(r'^https?://', '', domain)
        # Remove www
        domain = re.sub(r'^www\.', '', domain)
        # Remove trailing slash and path
        domain = domain.split('/')[0]
        return domain

    def _extract_top_keywords_from_gsc(self, gsc_data: List[Dict], limit: int = 20) -> List[str]:
        """
        Extract top keywords from GSC data if not provided.
        Sort by impressions descending.
        """
        if not gsc_data:
            return []

        # Create DataFrame for easier manipulation
        df = pd.DataFrame(gsc_data)
        
        if 'query' not in df.columns or 'impressions' not in df.columns:
            return []

        # Sort by impressions and take top N
        df = df.sort_values('impressions', ascending=False)
        return df.head(limit)['query'].tolist()

    def _build_keyword_metadata(self, gsc_data: List[Dict], keywords: List[str]) -> Dict[str, Dict]:
        """
        Build metadata dict for each keyword from GSC data.
        Returns: {keyword: {impressions, clicks, position, ctr}}
        """
        metadata = {}
        
        if not gsc_data:
            # Return empty metadata if no GSC data
            for kw in keywords:
                metadata[kw] = {
                    'impressions': 0,
                    'clicks': 0,
                    'position': 0,
                    'ctr': 0
                }
            return metadata

        # Create DataFrame for easier lookup
        df = pd.DataFrame(gsc_data)
        
        for keyword in keywords:
            # Find matching row
            row = df[df['query'] == keyword]
            
            if not row.empty:
                row = row.iloc[0]
                metadata[keyword] = {
                    'impressions': float(row.get('impressions', 0)),
                    'clicks': float(row.get('clicks', 0)),
                    'position': float(row.get('position', 0)),
                    'ctr': float(row.get('ctr', 0))
                }
            else:
                metadata[keyword] = {
                    'impressions': 0,
                    'clicks': 0,
                    'position': 0,
                    'ctr': 0
                }

        return metadata

    def _extract_competitors_from_serps(
        self, 
        serp_results: Dict[str, Any], 
        keywords: List[str],
        site_domain: str
    ) -> Dict[str, Dict]:
        """
        Extract competitor domains from SERP results.
        
        Returns: {
            domain: {
                'keywords': [list of shared keywords],
                'positions': {keyword: position},
                'urls': {keyword: url}
            }
        }
        """
        competitor_data = defaultdict(lambda: {
            'keywords': [],
            'positions': {},
            'urls': {}
        })

        for keyword in keywords:
            # SERP results might be keyed by keyword
            serp_for_keyword = serp_results.get(keyword, {})
            
            # Handle different possible SERP data structures
            organic_results = []
            
            if isinstance(serp_for_keyword, dict):
                # Could be nested under 'items' or 'organic_results'
                organic_results = serp_for_keyword.get('organic_results', [])
                if not organic_results:
                    organic_results = serp_for_keyword.get('items', [])
                if not organic_results and 'tasks' in serp_for_keyword:
                    # DataForSEO task structure
                    tasks = serp_for_keyword.get('tasks', [])
                    if tasks and len(tasks) > 0:
                        result = tasks[0].get('result', [])
                        if result and len(result) > 0:
                            organic_results = result[0].get('items', [])
            elif isinstance(serp_for_keyword, list):
                organic_results = serp_for_keyword

            # Process organic results
            for item in organic_results[:self.top_position]:
                try:
                    url = item.get('url', '')
                    if not url:
                        continue

                    # Extract domain from URL
                    domain = self._extract_domain_from_url(url)
                    if not domain or domain == site_domain:
                        continue

                    # Get position (rank_group or rank_absolute)
                    position = item.get('rank_group', item.get('rank_absolute', 0))
                    if position > self.top_position:
                        continue

                    # Add to competitor data
                    if keyword not in competitor_data[domain]['keywords']:
                        competitor_data[domain]['keywords'].append(keyword)
                    competitor_data[domain]['positions'][keyword] = position
                    competitor_data[domain]['urls'][keyword] = url

                except Exception as e:
                    logger.warning(f"Error processing SERP item for keyword '{keyword}': {str(e)}")
                    continue

        # Filter out competitors with too few shared keywords
        filtered_competitors = {
            domain: data 
            for domain, data in competitor_data.items()
            if len(data['keywords']) >= self.min_keywords_for_competitor
        }

        return filtered_competitors

    def _extract_domain_from_url(self, url: str) -> str:
        """Extract and normalize domain from URL."""
        try:
            # Remove protocol
            url = re.sub(r'^https?://', '', url)
            # Extract domain (before first /)
            domain = url.split('/')[0]
            # Remove www
            domain = re.sub(r'^www\.', '', domain)
            # Remove port if present
            domain = domain.split(':')[0]
            return domain.lower()
        except:
            return ""

    def _score_competitors(
        self,
        competitor_data: Dict[str, Dict],
        keyword_metadata: Dict[str, Dict],
        site_domain: str,
        total_keywords: int
    ) -> List[Dict[str, Any]]:
        """
        Score and rank competitors based on multiple factors.
        
        Scoring considers:
        - Keyword overlap percentage
        - Average position
        - Position advantage (ranking above user)
        - Weighted by keyword importance (impressions)
        """
        scored_competitors = []

        for domain, data in competitor_data.items():
            shared_keywords = data['keywords']
            positions = data['positions']
            urls = data['urls']

            # Calculate keyword overlap percentage
            overlap_pct = (len(shared_keywords) / total_keywords) * 100

            # Calculate average position
            avg_position = np.mean(list(positions.values())) if positions else 0

            # Calculate weighted metrics
            total_impressions = 0
            total_weighted_position = 0
            positions_above_user = 0
            positions_below_user = 0

            keyword_details = []

            for keyword in shared_keywords:
                kw_meta = keyword_metadata.get(keyword, {})
                impressions = kw_meta.get('impressions', 0)
                user_position = kw_meta.get('position', 0)
                comp_position = positions.get(keyword, 0)

                total_impressions += impressions
                total_weighted_position += comp_position * impressions

                # Track competitive positioning
                if comp_position > 0 and user_position > 0:
                    if comp_position < user_position:
                        positions_above_user += 1
                    else:
                        positions_below_user += 1

                keyword_details.append({
                    'keyword': keyword,
                    'competitor_position': int(comp_position),
                    'user_position': int(user_position),
                    'impressions': int(impressions),
                    'url': urls.get(keyword, '')
                })

            # Calculate weighted average position
            weighted_avg_position = (
                total_weighted_position / total_impressions 
                if total_impressions > 0 
                else avg_position
            )

            # Calculate threat score (0-100)
            # Higher overlap + better positions + more high-impression keywords = higher threat
            threat_score = self._calculate_threat_score(
                overlap_pct,
                weighted_avg_position,
                positions_above_user,
                len(shared_keywords),
                total_impressions
            )

            # Determine threat level
            threat_level = self._categorize_threat(threat_score)

            scored_competitors.append({
                'domain': domain,
                'shared_keywords_count': len(shared_keywords),
                'shared_keywords': sorted(shared_keywords),
                'overlap_percentage': round(overlap_pct, 2),
                'avg_position': round(avg_position, 2),
                'weighted_avg_position': round(weighted_avg_position, 2),
                'positions_above_user': positions_above_user,
                'positions_below_user': positions_below_user,
                'total_impressions_overlap': int(total_impressions),
                'threat_score': round(threat_score, 2),
                'threat_level': threat_level,
                'keyword_details': keyword_details
            })

        # Sort by threat score descending
        scored_competitors.sort(key=lambda x: x['threat_score'], reverse=True)

        # Return top N
        return scored_competitors[:self.top_n_competitors]

    def _calculate_threat_score(
        self,
        overlap_pct: float,
        avg_position: float,
        positions_above: int,
        shared_keywords: int,
        total_impressions: float
    ) -> float:
        """
        Calculate competitive threat score (0-100).
        
        Factors:
        - Overlap percentage (40% weight)
        - Position quality (30% weight) - lower is better
        - Positions above user (20% weight)
        - Total impression volume (10% weight)
        """
        # Overlap score (0-40)
        overlap_score = (overlap_pct / 100) * 40

        # Position score (0-30) - inverse, so position 1 = 30 points, position 10 = 3 points
        position_score = max(0, (11 - avg_position) / 10 * 30) if avg_position > 0 else 0

        # Dominance score (0-20) - based on how often they rank above user
        dominance_ratio = positions_above / shared_keywords if shared_keywords > 0 else 0
        dominance_score = dominance_ratio * 20

        # Volume score (0-10) - logarithmic scale for impressions
        volume_score = min(10, np.log10(total_impressions + 1)) if total_impressions > 0 else 0

        threat_score = overlap_score + position_score + dominance_score + volume_score

        return min(100, max(0, threat_score))

    def _categorize_threat(self, threat_score: float) -> str:
        """Categorize threat level based on score."""
        if threat_score >= 70:
            return "critical"
        elif threat_score >= 50:
            return "high"
        elif threat_score >= 30:
            return "medium"
        else:
            return "low"

    def _build_result(
        self,
        scored_competitors: List[Dict],
        total_keywords_analyzed: int,
        site_domain: str
    ) -> Dict[str, Any]:
        """
        Build final result structure matching spec.
        
        Returns top 5-10 competitors with:
        - overlap %
        - shared keywords list
        - avg position
        - threat metrics
        """
        result = {
            'module': 'competitor_mapping',
            'generated_at': datetime.utcnow().isoformat(),
            'site_domain': site_domain,
            'total_keywords_analyzed': total_keywords_analyzed,
            'total_competitors_found': len(scored_competitors),
            'competitors': [],
            'summary': {
                'critical_threats': 0,
                'high_threats': 0,
                'medium_threats': 0,
                'low_threats': 0,
                'avg_overlap_percentage': 0,
                'avg_competitor_position': 0,
                'most_contested_keywords': []
            }
        }

        if not scored_competitors:
            return result

        # Process competitors
        threat_counts = {'critical': 0, 'high': 0, 'medium': 0, 'low': 0}
        total_overlap = 0
        total_position = 0
        keyword_contest_count = Counter()

        for comp in scored_competitors:
            # Add to result
            result['competitors'].append({
                'domain': comp['domain'],
                'overlap_percentage': comp['overlap_percentage'],
                'shared_keywords_count': comp['shared_keywords_count'],
                'shared_keywords': comp['shared_keywords'],
                'avg_position': comp['avg_position'],
                'weighted_avg_position': comp['weighted_avg_position'],
                'positions_above_user': comp['positions_above_user'],
                'positions_below_user': comp['positions_below_user'],
                'threat_score': comp['threat_score'],
                'threat_level': comp['threat_level'],
                'total_impressions_overlap': comp['total_impressions_overlap'],
                'top_competing_keywords': sorted(
                    comp['keyword_details'],
                    key=lambda x: x['impressions'],
                    reverse=True
                )[:10]  # Top 10 most valuable competing keywords
            })

            # Update summary stats
            threat_counts[comp['threat_level']] += 1
            total_overlap += comp['overlap_percentage']
            total_position += comp['avg_position']

            # Count keyword contests
            for keyword in comp['shared_keywords']:
                keyword_contest_count[keyword] += 1

        # Calculate summary metrics
        result['summary']['critical_threats'] = threat_counts['critical']
        result['summary']['high_threats'] = threat_counts['high']
        result['summary']['medium_threats'] = threat_counts['medium']
        result['summary']['low_threats'] = threat_counts['low']
        result['summary']['avg_overlap_percentage'] = round(
            total_overlap / len(scored_competitors), 2
        )
        result['summary']['avg_competitor_position'] = round(
            total_position / len(scored_competitors), 2
        )
        result['summary']['most_contested_keywords'] = [
            {'keyword': kw, 'competitor_count': count}
            for kw, count in keyword_contest_count.most_common(10)
        ]

        return result

    def _empty_result(self) -> Dict[str, Any]:
        """Return empty result structure when no data available."""
        return {
            'module': 'competitor_mapping',
            'generated_at': datetime.utcnow().isoformat(),
            'site_domain': '',
            'total_keywords_analyzed': 0,
            'total_competitors_found': 0,
            'competitors': [],
            'summary': {
                'critical_threats': 0,
                'high_threats': 0,
                'medium_threats': 0,
                'low_threats': 0,
                'avg_overlap_percentage': 0,
                'avg_competitor_position': 0,
                'most_contested_keywords': []
            }
        }