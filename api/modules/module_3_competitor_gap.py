"""
Module 3: Competitor Gap Analysis

Identifies competitor domains from GSC data, fetches their ranking keywords via DataForSEO,
and calculates keyword gap opportunities based on search volume and ranking position differences.
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
from collections import Counter, defaultdict
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from dataclasses import dataclass
import asyncio
import aiohttp
from urllib.parse import urlparse

from api.services.dataforseo_service import DataForSEOService
from api.core.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


@dataclass
class CompetitorDomain:
    """Represents a competitor domain with metadata."""
    domain: str
    overlap_count: int
    overlap_percentage: float
    avg_position: float
    keywords_ranking: int
    threat_score: float


@dataclass
class KeywordGap:
    """Represents a keyword gap opportunity."""
    keyword: str
    search_volume: int
    competitor_domain: str
    competitor_position: int
    user_position: Optional[int]
    difficulty: Optional[int]
    opportunity_score: float
    gap_type: str  # 'unique_competitor', 'position_gap', 'missing'


@dataclass
class CompetitorKeyword:
    """Represents a keyword that a competitor ranks for."""
    keyword: str
    position: int
    search_volume: int
    url: str
    difficulty: Optional[int]


class CompetitorGapAnalyzer:
    """
    Analyzes competitor keyword gaps using GSC data and DataForSEO API.
    """

    def __init__(self, dataforseo_service: Optional[DataForSEOService] = None):
        """Initialize the analyzer with optional DataForSEO service."""
        self.dataforseo_service = dataforseo_service or DataForSEOService()
        self.settings = get_settings()

    def identify_competitors_from_gsc(
        self,
        gsc_keyword_data: pd.DataFrame,
        serp_data: List[Dict[str, Any]],
        min_overlap: int = 5,
        top_n: int = 10
    ) -> List[CompetitorDomain]:
        """
        Identify top competitor domains based on SERP overlap with user's keywords.

        Args:
            gsc_keyword_data: DataFrame with columns: query, position, clicks, impressions
            serp_data: List of SERP results from DataForSEO for user's keywords
            min_overlap: Minimum number of shared keywords to consider a competitor
            top_n: Number of top competitors to return

        Returns:
            List of CompetitorDomain objects sorted by threat score
        """
        logger.info("Identifying competitors from GSC and SERP data")

        user_domain = self._extract_user_domain(gsc_keyword_data)
        competitor_appearances = defaultdict(lambda: {
            'keywords': set(),
            'positions': [],
            'keywords_ranking': 0
        })

        # Parse SERP data to find competing domains
        for serp_result in serp_data:
            keyword = serp_result.get('keyword', '')
            organic_results = serp_result.get('organic_results', [])

            for result in organic_results:
                domain = self._extract_domain(result.get('url', ''))
                position = result.get('rank_absolute', 999)

                # Skip user's own domain and invalid domains
                if not domain or domain == user_domain:
                    continue

                competitor_appearances[domain]['keywords'].add(keyword)
                competitor_appearances[domain]['positions'].append(position)
                competitor_appearances[domain]['keywords_ranking'] += 1

        # Calculate metrics for each competitor
        total_keywords = len(gsc_keyword_data) if not gsc_keyword_data.empty else len(set(
            r.get('keyword', '') for r in serp_data
        ))

        competitors = []
        for domain, data in competitor_appearances.items():
            overlap_count = len(data['keywords'])

            if overlap_count < min_overlap:
                continue

            overlap_percentage = (overlap_count / total_keywords * 100) if total_keywords > 0 else 0
            avg_position = np.mean(data['positions']) if data['positions'] else 999
            keywords_ranking = data['keywords_ranking']

            # Calculate threat score
            # Higher overlap + better positions = higher threat
            threat_score = (
                (overlap_percentage / 100) * 0.4 +
                (1 - (avg_position / 20)) * 0.4 +  # Normalize positions (assume top 20)
                (min(keywords_ranking / 100, 1.0)) * 0.2
            ) * 100

            competitors.append(CompetitorDomain(
                domain=domain,
                overlap_count=overlap_count,
                overlap_percentage=round(overlap_percentage, 2),
                avg_position=round(avg_position, 2),
                keywords_ranking=keywords_ranking,
                threat_score=round(threat_score, 2)
            ))

        # Sort by threat score descending
        competitors.sort(key=lambda x: x.threat_score, reverse=True)

        logger.info(f"Identified {len(competitors)} competitors, returning top {top_n}")
        return competitors[:top_n]

    async def fetch_competitor_keywords(
        self,
        competitor_domain: str,
        limit: int = 500,
        location_code: int = 2840,  # USA
        language_code: str = "en"
    ) -> List[CompetitorKeyword]:
        """
        Fetch ranking keywords for a competitor domain using DataForSEO.

        Args:
            competitor_domain: The competitor's domain
            limit: Maximum number of keywords to fetch
            location_code: DataForSEO location code
            language_code: Language code

        Returns:
            List of CompetitorKeyword objects
        """
        logger.info(f"Fetching keywords for competitor: {competitor_domain}")

        try:
            # Use DataForSEO's domain analytics endpoint
            result = await self.dataforseo_service.get_domain_keywords(
                domain=competitor_domain,
                location_code=location_code,
                language_code=language_code,
                limit=limit
            )

            keywords = []
            items = result.get('items', [])

            for item in items:
                keyword_data = item.get('keyword_data', {})
                serp_info = item.get('serp_info', {})

                keyword = keyword_data.get('keyword', '')
                if not keyword:
                    continue

                keywords.append(CompetitorKeyword(
                    keyword=keyword,
                    position=serp_info.get('rank_absolute', 999),
                    search_volume=keyword_data.get('search_volume', 0),
                    url=serp_info.get('url', ''),
                    difficulty=keyword_data.get('keyword_difficulty', None)
                ))

            logger.info(f"Fetched {len(keywords)} keywords for {competitor_domain}")
            return keywords

        except Exception as e:
            logger.error(f"Error fetching competitor keywords: {str(e)}")
            return []

    async def fetch_multiple_competitors_keywords(
        self,
        competitor_domains: List[str],
        limit_per_domain: int = 300
    ) -> Dict[str, List[CompetitorKeyword]]:
        """
        Fetch keywords for multiple competitors concurrently.

        Args:
            competitor_domains: List of competitor domains
            limit_per_domain: Max keywords per domain

        Returns:
            Dictionary mapping domain to list of CompetitorKeyword objects
        """
        logger.info(f"Fetching keywords for {len(competitor_domains)} competitors")

        tasks = [
            self.fetch_competitor_keywords(domain, limit=limit_per_domain)
            for domain in competitor_domains
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        competitor_keywords = {}
        for domain, result in zip(competitor_domains, results):
            if isinstance(result, Exception):
                logger.error(f"Error fetching keywords for {domain}: {str(result)}")
                competitor_keywords[domain] = []
            else:
                competitor_keywords[domain] = result

        return competitor_keywords

    def calculate_keyword_gaps(
        self,
        user_keywords: pd.DataFrame,
        competitor_keywords: Dict[str, List[CompetitorKeyword]],
        search_volume_threshold: int = 100,
        max_gaps_per_competitor: int = 50
    ) -> List[KeywordGap]:
        """
        Calculate keyword gap opportunities between user and competitors.

        Args:
            user_keywords: DataFrame with user's ranking keywords (query, position, clicks, impressions)
            competitor_keywords: Dict mapping competitor domain to their keywords
            search_volume_threshold: Minimum search volume to consider
            max_gaps_per_competitor: Maximum gaps to return per competitor

        Returns:
            List of KeywordGap objects sorted by opportunity score
        """
        logger.info("Calculating keyword gaps")

        # Create lookup for user's keywords
        user_keyword_positions = {}
        if not user_keywords.empty:
            for _, row in user_keywords.iterrows():
                user_keyword_positions[row['query'].lower()] = row['position']

        all_gaps = []

        for competitor_domain, keywords in competitor_keywords.items():
            logger.info(f"Analyzing gaps for competitor: {competitor_domain}")

            for comp_keyword in keywords:
                keyword_lower = comp_keyword.keyword.lower()
                
                # Skip low-volume keywords
                if comp_keyword.search_volume < search_volume_threshold:
                    continue

                user_position = user_keyword_positions.get(keyword_lower)

                # Determine gap type and calculate opportunity score
                if user_position is None:
                    # User doesn't rank for this keyword at all
                    gap_type = 'missing'
                    position_gap = 100 - comp_keyword.position
                    opportunity_score = self._calculate_opportunity_score(
                        search_volume=comp_keyword.search_volume,
                        competitor_position=comp_keyword.position,
                        user_position=None,
                        difficulty=comp_keyword.difficulty
                    )
                elif comp_keyword.position < user_position - 3:
                    # Competitor ranks significantly better
                    gap_type = 'position_gap'
                    position_gap = user_position - comp_keyword.position
                    opportunity_score = self._calculate_opportunity_score(
                        search_volume=comp_keyword.search_volume,
                        competitor_position=comp_keyword.position,
                        user_position=user_position,
                        difficulty=comp_keyword.difficulty
                    )
                else:
                    # User ranks similarly or better - skip
                    continue

                all_gaps.append(KeywordGap(
                    keyword=comp_keyword.keyword,
                    search_volume=comp_keyword.search_volume,
                    competitor_domain=competitor_domain,
                    competitor_position=comp_keyword.position,
                    user_position=user_position,
                    difficulty=comp_keyword.difficulty,
                    opportunity_score=round(opportunity_score, 2),
                    gap_type=gap_type
                ))

        # Sort by opportunity score descending
        all_gaps.sort(key=lambda x: x.opportunity_score, reverse=True)

        # Limit per competitor to avoid dominance by single competitor
        if max_gaps_per_competitor:
            filtered_gaps = []
            competitor_counts = defaultdict(int)

            for gap in all_gaps:
                if competitor_counts[gap.competitor_domain] < max_gaps_per_competitor:
                    filtered_gaps.append(gap)
                    competitor_counts[gap.competitor_domain] += 1

            logger.info(f"Filtered to {len(filtered_gaps)} gaps from {len(all_gaps)} total")
            return filtered_gaps

        logger.info(f"Identified {len(all_gaps)} keyword gaps")
        return all_gaps

    def _calculate_opportunity_score(
        self,
        search_volume: int,
        competitor_position: int,
        user_position: Optional[int],
        difficulty: Optional[int]
    ) -> float:
        """
        Calculate opportunity score for a keyword gap.

        Score considers:
        - Search volume (higher = better)
        - Competitor position (better position = validated opportunity)
        - Position gap (larger gap = more opportunity)
        - Keyword difficulty (lower = easier to capture)

        Returns:
            Opportunity score between 0-100
        """
        # Normalize search volume (log scale, cap at 10k)
        volume_score = min(np.log10(search_volume + 1) / np.log10(10000), 1.0) * 30

        # Competitor position score (top 3 = best validation)
        if competitor_position <= 3:
            position_score = 30
        elif competitor_position <= 10:
            position_score = 20
        elif competitor_position <= 20:
            position_score = 10
        else:
            position_score = 5

        # Position gap score (how much better competitor is)
        if user_position is None:
            gap_score = 25  # Missing entirely = high opportunity
        else:
            gap = user_position - competitor_position
            gap_score = min(gap / 50 * 25, 25)  # Cap at 25 points

        # Difficulty score (lower difficulty = higher score)
        if difficulty is not None:
            difficulty_score = (100 - difficulty) / 100 * 15
        else:
            difficulty_score = 7.5  # Neutral score if unknown

        total_score = volume_score + position_score + gap_score + difficulty_score
        return min(total_score, 100)

    def generate_gap_summary(
        self,
        gaps: List[KeywordGap],
        competitors: List[CompetitorDomain]
    ) -> Dict[str, Any]:
        """
        Generate summary statistics for keyword gaps.

        Args:
            gaps: List of KeywordGap objects
            competitors: List of CompetitorDomain objects

        Returns:
            Dictionary with gap analysis summary
        """
        if not gaps:
            return {
                'total_gaps': 0,
                'by_type': {},
                'by_competitor': {},
                'top_opportunities': [],
                'estimated_traffic_opportunity': 0
            }

        # Count by gap type
        by_type = defaultdict(int)
        for gap in gaps:
            by_type[gap.gap_type] += 1

        # Aggregate by competitor
        by_competitor = defaultdict(lambda: {
            'gaps': 0,
            'total_search_volume': 0,
            'avg_opportunity_score': 0,
            'threat_score': 0
        })

        for gap in gaps:
            by_competitor[gap.competitor_domain]['gaps'] += 1
            by_competitor[gap.competitor_domain]['total_search_volume'] += gap.search_volume

        # Calculate averages and add threat scores
        for comp in competitors:
            if comp.domain in by_competitor:
                stats = by_competitor[comp.domain]
                comp_gaps = [g for g in gaps if g.competitor_domain == comp.domain]
                stats['avg_opportunity_score'] = round(
                    np.mean([g.opportunity_score for g in comp_gaps]), 2
                ) if comp_gaps else 0
                stats['threat_score'] = comp.threat_score

        # Estimate traffic opportunity (rough CTR-based calculation)
        estimated_traffic = 0
        for gap in gaps[:50]:  # Top 50 opportunities
            # Rough CTR estimates for top 5 positions
            if gap.competitor_position <= 5:
                estimated_ctr = [0.30, 0.15, 0.10, 0.07, 0.05][gap.competitor_position - 1]
                estimated_traffic += gap.search_volume * estimated_ctr * 0.5  # 50% capture rate

        return {
            'total_gaps': len(gaps),
            'by_type': dict(by_type),
            'by_competitor': dict(by_competitor),
            'top_opportunities': [
                {
                    'keyword': gap.keyword,
                    'search_volume': gap.search_volume,
                    'competitor_domain': gap.competitor_domain,
                    'competitor_position': gap.competitor_position,
                    'user_position': gap.user_position,
                    'opportunity_score': gap.opportunity_score,
                    'gap_type': gap.gap_type
                }
                for gap in gaps[:20]
            ],
            'estimated_monthly_traffic_opportunity': round(estimated_traffic)
        }

    def _extract_domain(self, url: str) -> str:
        """Extract domain from URL."""
        try:
            parsed = urlparse(url)
            domain = parsed.netloc or parsed.path
            # Remove www. prefix
            domain = domain.replace('www.', '')
            return domain.lower()
        except Exception:
            return ''

    def _extract_user_domain(self, gsc_data: pd.DataFrame) -> str:
        """Extract user's domain from GSC data (if page column exists)."""
        if 'page' in gsc_data.columns and not gsc_data.empty:
            # Get most common domain from pages
            domains = gsc_data['page'].apply(self._extract_domain)
            domain_counts = Counter(domains)
            if domain_counts:
                return domain_counts.most_common(1)[0][0]
        return ''


async def analyze_competitor_gap(
    gsc_keyword_data: pd.DataFrame,
    serp_data: List[Dict[str, Any]],
    dataforseo_service: Optional[DataForSEOService] = None,
    min_competitor_overlap: int = 5,
    top_n_competitors: int = 5,
    keywords_per_competitor: int = 300,
    search_volume_threshold: int = 100
) -> Dict[str, Any]:
    """
    Main function to analyze competitor keyword gaps.

    Args:
        gsc_keyword_data: DataFrame with GSC keyword data (query, position, clicks, impressions)
        serp_data: List of SERP results from DataForSEO
        dataforseo_service: Optional DataForSEO service instance
        min_competitor_overlap: Minimum shared keywords to consider a competitor
        top_n_competitors: Number of top competitors to analyze
        keywords_per_competitor: Max keywords to fetch per competitor
        search_volume_threshold: Min search volume for gap analysis

    Returns:
        Dictionary with complete competitor gap analysis
    """
    logger.info("Starting competitor gap analysis")

    analyzer = CompetitorGapAnalyzer(dataforseo_service)

    # Step 1: Identify competitors from SERP overlap
    competitors = analyzer.identify_competitors_from_gsc(
        gsc_keyword_data=gsc_keyword_data,
        serp_data=serp_data,
        min_overlap=min_competitor_overlap,
        top_n=top_n_competitors
    )

    if not competitors:
        logger.warning("No competitors identified")
        return {
            'competitors': [],
            'keyword_gaps': [],
            'summary': {
                'total_competitors': 0,
                'total_gaps': 0,
                'estimated_monthly_traffic_opportunity': 0
            }
        }

    # Step 2: Fetch keywords for each competitor
    competitor_domains = [comp.domain for comp in competitors]
    competitor_keywords = await analyzer.fetch_multiple_competitors_keywords(
        competitor_domains=competitor_domains,
        limit_per_domain=keywords_per_competitor
    )

    # Step 3: Calculate keyword gaps
    gaps = analyzer.calculate_keyword_gaps(
        user_keywords=gsc_keyword_data,
        competitor_keywords=competitor_keywords,
        search_volume_threshold=search_volume_threshold
    )

    # Step 4: Generate summary
    summary = analyzer.generate_gap_summary(gaps, competitors)

    logger.info(f"Competitor gap analysis complete: {len(competitors)} competitors, {len(gaps)} gaps")

    return {
        'competitors': [
            {
                'domain': comp.domain,
                'overlap_count': comp.overlap_count,
                'overlap_percentage': comp.overlap_percentage,
                'avg_position': comp.avg_position,
                'keywords_ranking': comp.keywords_ranking,
                'threat_score': comp.threat_score
            }
            for comp in competitors
        ],
        'keyword_gaps': [
            {
                'keyword': gap.keyword,
                'search_volume': gap.search_volume,
                'competitor_domain': gap.competitor_domain,
                'competitor_position': gap.competitor_position,
                'user_position': gap.user_position,
                'difficulty': gap.difficulty,
                'opportunity_score': gap.opportunity_score,
                'gap_type': gap.gap_type
            }
            for gap in gaps
        ],
        'summary': summary,
        'metadata': {
            'analysis_date': datetime.utcnow().isoformat(),
            'total_competitors_analyzed': len(competitors),
            'total_competitor_keywords_fetched': sum(
                len(kws) for kws in competitor_keywords.values()
            ),
            'parameters': {
                'min_competitor_overlap': min_competitor_overlap,
                'top_n_competitors': top_n_competitors,
                'keywords_per_competitor': keywords_per_competitor,
                'search_volume_threshold': search_volume_threshold
            }
        }
    }
