"""
Module 3: Competitor Mapping

Identifies top 5-10 competing domains by analyzing GSC queries and SERP data.
Scores competitors by keyword overlap, position, and estimated visibility.
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
import pandas as pd
import numpy as np
from collections import defaultdict, Counter
import re

from .module_base import ModuleBase

logger = logging.getLogger(__name__)


class CompetitorMappingModule(ModuleBase):
    """
    Identifies and scores competing domains based on:
    - Keyword overlap with site's ranking queries
    - Competitor positions and visibility
    - Estimated traffic share
    """

    def __init__(self, site_id: str):
        super().__init__(site_id, "competitor_mapping")
        self.min_keywords_for_competitor = 3  # Min shared keywords to be considered
        self.top_n_competitors = 10  # Max competitors to return
        self.min_impressions_threshold = 50  # Min monthly impressions to consider query

    async def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Main execution method for competitor mapping.

        Args:
            context: Shared context containing:
                - gsc_data: GSC query performance data
                - serp_data: DataForSEO SERP results
                - site_domain: User's domain

        Returns:
            Dictionary with competitor analysis results
        """
        try:
            logger.info(f"Starting competitor mapping for site {self.site_id}")

            # Extract required data from context
            gsc_data = context.get("gsc_data", {})
            serp_data = context.get("serp_data", {})
            site_domain = context.get("site_domain", "")

            if not gsc_data or not serp_data:
                raise ValueError("Missing required GSC or SERP data in context")

            # Step 1: Extract and classify queries from GSC
            queries_df = self._prepare_query_data(gsc_data, site_domain)
            if queries_df.empty:
                logger.warning("No valid queries found in GSC data")
                return self._empty_result()

            # Step 2: Extract competitor domains from SERP data
            competitor_domains = self._extract_competitor_domains(serp_data, site_domain)
            if not competitor_domains:
                logger.warning("No competitor domains found in SERP data")
                return self._empty_result()

            # Step 3: Build keyword-domain matrix
            keyword_domain_matrix = self._build_keyword_domain_matrix(
                serp_data, queries_df, site_domain
            )

            # Step 4: Score competitors
            competitors = self._score_competitors(
                competitor_domains,
                keyword_domain_matrix,
                queries_df,
                site_domain
            )

            # Step 5: Calculate aggregate metrics
            summary = self._calculate_summary(competitors, queries_df)

            # Step 6: Generate insights
            insights = self._generate_insights(competitors, queries_df)

            result = {
                "competitors": competitors[:self.top_n_competitors],
                "total_competitors_found": len(competitors),
                "total_keywords_analyzed": len(queries_df),
                "summary": summary,
                "insights": insights,
                "metadata": {
                    "site_domain": site_domain,
                    "analysis_date": datetime.utcnow().isoformat(),
                    "min_keywords_threshold": self.min_keywords_for_competitor
                }
            }

            logger.info(f"Competitor mapping completed: {len(competitors)} competitors found")
            return result

        except Exception as e:
            logger.error(f"Error in competitor mapping execution: {str(e)}", exc_info=True)
            raise

    def _prepare_query_data(
        self, gsc_data: Dict[str, Any], site_domain: str
    ) -> pd.DataFrame:
        """
        Prepare and filter query data from GSC.

        Args:
            gsc_data: Raw GSC data
            site_domain: User's domain for brand detection

        Returns:
            DataFrame with query, clicks, impressions, position, is_branded
        """
        # Extract query performance data
        queries = gsc_data.get("queries", [])
        if not queries:
            return pd.DataFrame()

        df = pd.DataFrame(queries)

        # Ensure required columns exist
        required_cols = ["query", "clicks", "impressions", "position"]
        for col in required_cols:
            if col not in df.columns:
                df[col] = 0 if col != "position" else 100

        # Filter by impressions threshold
        df = df[df["impressions"] >= self.min_impressions_threshold].copy()

        # Classify queries as branded or non-branded
        df["is_branded"] = df["query"].apply(
            lambda q: self._is_branded_query(q, site_domain)
        )

        # Calculate estimated monthly values (GSC usually returns last 16 months)
        # For simplicity, use the values as-is (assuming they're recent monthly aggregates)
        df = df.sort_values("impressions", ascending=False).reset_index(drop=True)

        logger.info(f"Prepared {len(df)} queries ({df['is_branded'].sum()} branded)")
        return df

    def _is_branded_query(self, query: str, site_domain: str) -> bool:
        """
        Determine if a query contains brand/domain terms.

        Args:
            query: Search query
            site_domain: User's domain

        Returns:
            True if query is branded
        """
        query_lower = query.lower()

        # Extract brand name from domain
        # Remove common TLDs and www
        domain_clean = re.sub(r"^www\.", "", site_domain)
        brand_parts = domain_clean.split(".")[0]

        # Split brand name by common separators
        brand_terms = re.split(r"[-_]", brand_parts)

        # Check if any brand term appears in query (min 3 chars to avoid false positives)
        for term in brand_terms:
            if len(term) >= 3 and term.lower() in query_lower:
                return True

        # Also check for full domain
        if domain_clean.lower() in query_lower:
            return True

        return False

    def _extract_competitor_domains(
        self, serp_data: Dict[str, Any], site_domain: str
    ) -> List[str]:
        """
        Extract all unique competitor domains from SERP results.

        Args:
            serp_data: DataForSEO SERP results
            site_domain: User's domain to exclude

        Returns:
            List of competitor domains
        """
        domains = set()
        site_domain_clean = self._normalize_domain(site_domain)

        for keyword, serp_result in serp_data.items():
            organic_results = serp_result.get("organic_results", [])

            for result in organic_results:
                url = result.get("url", "")
                domain = self._extract_domain_from_url(url)

                if domain and self._normalize_domain(domain) != site_domain_clean:
                    domains.add(domain)

        return list(domains)

    def _build_keyword_domain_matrix(
        self,
        serp_data: Dict[str, Any],
        queries_df: pd.DataFrame,
        site_domain: str
    ) -> Dict[str, Dict[str, Dict[str, Any]]]:
        """
        Build a matrix mapping keywords to domains with position and metrics.

        Args:
            serp_data: DataForSEO SERP results
            queries_df: Prepared query data
            site_domain: User's domain

        Returns:
            Nested dict: {keyword: {domain: {position, url, title, ...}}}
        """
        matrix = {}
        site_domain_clean = self._normalize_domain(site_domain)

        for keyword, serp_result in serp_data.items():
            # Only process keywords that are in our filtered query list
            if keyword not in queries_df["query"].values:
                continue

            matrix[keyword] = {}
            organic_results = serp_result.get("organic_results", [])

            for idx, result in enumerate(organic_results, start=1):
                url = result.get("url", "")
                domain = self._extract_domain_from_url(url)

                if not domain:
                    continue

                domain_clean = self._normalize_domain(domain)

                # Skip user's own domain in competitor matrix
                if domain_clean == site_domain_clean:
                    continue

                matrix[keyword][domain] = {
                    "position": idx,
                    "url": url,
                    "title": result.get("title", ""),
                    "description": result.get("description", "")
                }

        return matrix

    def _score_competitors(
        self,
        competitor_domains: List[str],
        keyword_domain_matrix: Dict[str, Dict[str, Dict[str, Any]]],
        queries_df: pd.DataFrame,
        site_domain: str
    ) -> List[Dict[str, Any]]:
        """
        Score each competitor based on overlap and visibility.

        Args:
            competitor_domains: List of competitor domains
            keyword_domain_matrix: Keyword-domain matrix
            queries_df: Query data with impressions
            site_domain: User's domain

        Returns:
            List of competitor dicts with scores, sorted by importance
        """
        competitors = []

        # Get user's keywords for overlap calculation
        user_keywords = set(queries_df["query"].values)
        total_user_impressions = queries_df["impressions"].sum()

        for domain in competitor_domains:
            # Find all keywords where this competitor appears
            competitor_keywords = set()
            positions = []
            shared_impressions = 0
            keyword_details = []

            for keyword, domains_dict in keyword_domain_matrix.items():
                if domain in domains_dict:
                    competitor_keywords.add(keyword)
                    position = domains_dict[domain]["position"]
                    positions.append(position)

                    # Get impressions for this keyword
                    keyword_impressions = queries_df[
                        queries_df["query"] == keyword
                    ]["impressions"].iloc[0] if keyword in queries_df["query"].values else 0

                    shared_impressions += keyword_impressions

                    keyword_details.append({
                        "keyword": keyword,
                        "position": position,
                        "impressions": int(keyword_impressions),
                        "url": domains_dict[domain].get("url", "")
                    })

            # Only include competitors that meet minimum threshold
            if len(competitor_keywords) < self.min_keywords_for_competitor:
                continue

            # Calculate metrics
            overlap_count = len(competitor_keywords)
            overlap_percentage = (overlap_count / len(user_keywords)) * 100 if user_keywords else 0
            avg_position = np.mean(positions) if positions else 100
            median_position = np.median(positions) if positions else 100

            # Estimate traffic share (simplified CTR model)
            estimated_traffic = sum(
                self._estimate_clicks(kw["position"], kw["impressions"])
                for kw in keyword_details
            )

            # Calculate threat score (higher = more competitive)
            # Factors: overlap %, average position, estimated traffic
            threat_score = (
                (overlap_percentage / 100) * 0.4 +
                (max(0, 20 - avg_position) / 20) * 0.4 +
                (min(estimated_traffic / total_user_impressions if total_user_impressions else 0, 1)) * 0.2
            ) * 100

            competitors.append({
                "domain": domain,
                "keywords_shared": overlap_count,
                "overlap_percentage": round(overlap_percentage, 1),
                "avg_position": round(avg_position, 1),
                "median_position": round(median_position, 1),
                "best_position": min(positions) if positions else 100,
                "worst_position": max(positions) if positions else 100,
                "total_shared_impressions": int(shared_impressions),
                "estimated_monthly_traffic": int(estimated_traffic),
                "threat_score": round(threat_score, 1),
                "keyword_details": sorted(
                    keyword_details,
                    key=lambda x: x["impressions"],
                    reverse=True
                )[:20]  # Top 20 keywords only
            })

        # Sort by threat score
        competitors.sort(key=lambda x: x["threat_score"], reverse=True)

        return competitors

    def _estimate_clicks(self, position: int, impressions: int) -> float:
        """
        Estimate clicks based on position and impressions using CTR curve.

        Args:
            position: Organic position
            impressions: Monthly impressions

        Returns:
            Estimated monthly clicks
        """
        # Standard organic CTR curve (simplified)
        ctr_curve = {
            1: 0.28, 2: 0.15, 3: 0.11, 4: 0.08, 5: 0.06,
            6: 0.05, 7: 0.04, 8: 0.03, 9: 0.03, 10: 0.02
        }

        # Beyond position 10, use decay formula
        if position <= 10:
            ctr = ctr_curve.get(position, 0.02)
        else:
            ctr = 0.02 * (0.8 ** (position - 10))

        return impressions * ctr

    def _calculate_summary(
        self, competitors: List[Dict[str, Any]], queries_df: pd.DataFrame
    ) -> Dict[str, Any]:
        """
        Calculate aggregate summary metrics.

        Args:
            competitors: Scored competitor list
            queries_df: Query data

        Returns:
            Summary statistics
        """
        if not competitors:
            return {
                "total_competitors": 0,
                "avg_overlap_percentage": 0,
                "highest_overlap_percentage": 0,
                "most_threatened_keywords": 0,
                "primary_competitors": []
            }

        overlaps = [c["overlap_percentage"] for c in competitors]
        threat_scores = [c["threat_score"] for c in competitors]

        # Primary competitors = top 20% by threat score or top 3, whichever is more
        threshold_count = max(3, len(competitors) // 5)
        primary_competitors = [
            {
                "domain": c["domain"],
                "threat_score": c["threat_score"],
                "keywords_shared": c["keywords_shared"]
            }
            for c in competitors[:threshold_count]
        ]

        # Keywords where multiple competitors rank highly (competitive keywords)
        keyword_competition = defaultdict(int)
        for comp in competitors:
            for kw_detail in comp["keyword_details"]:
                if kw_detail["position"] <= 10:
                    keyword_competition[kw_detail["keyword"]] += 1

        most_competitive_keywords = sorted(
            keyword_competition.items(),
            key=lambda x: x[1],
            reverse=True
        )[:10]

        return {
            "total_competitors": len(competitors),
            "avg_overlap_percentage": round(np.mean(overlaps), 1),
            "median_overlap_percentage": round(np.median(overlaps), 1),
            "highest_overlap_percentage": round(max(overlaps), 1),
            "avg_threat_score": round(np.mean(threat_scores), 1),
            "primary_competitors": primary_competitors,
            "most_competitive_keywords": [
                {"keyword": kw, "competitor_count": count}
                for kw, count in most_competitive_keywords
            ]
        }

    def _generate_insights(
        self, competitors: List[Dict[str, Any]], queries_df: pd.DataFrame
    ) -> List[str]:
        """
        Generate human-readable insights about the competitive landscape.

        Args:
            competitors: Scored competitor list
            queries_df: Query data

        Returns:
            List of insight strings
        """
        insights = []

        if not competitors:
            insights.append("No significant competitors detected in the analyzed keyword set.")
            return insights

        # Insight 1: Competitive intensity
        avg_overlap = np.mean([c["overlap_percentage"] for c in competitors])
        if avg_overlap > 30:
            insights.append(
                f"High competitive overlap detected: competitors share {avg_overlap:.0f}% "
                f"of your keywords on average, indicating a crowded market."
            )
        elif avg_overlap > 15:
            insights.append(
                f"Moderate competition: {avg_overlap:.0f}% average keyword overlap "
                f"with {len(competitors)} competing domains."
            )
        else:
            insights.append(
                f"Low direct competition: only {avg_overlap:.0f}% average keyword overlap, "
                f"suggesting opportunity to capture more market share."
            )

        # Insight 2: Primary threat
        if competitors:
            top_competitor = competitors[0]
            insights.append(
                f"Primary competitor '{top_competitor['domain']}' ranks for "
                f"{top_competitor['keywords_shared']} of your keywords "
                f"(avg position {top_competitor['avg_position']:.1f})."
            )

        # Insight 3: Position comparison
        top_3_competitors = competitors[:3]
        user_avg_position = queries_df["position"].mean()
        competitor_positions = [c["avg_position"] for c in top_3_competitors]

        if competitor_positions and user_avg_position < np.mean(competitor_positions):
            insights.append(
                f"You currently outrank your top competitors on average "
                f"(your avg: {user_avg_position:.1f} vs their avg: {np.mean(competitor_positions):.1f})."
            )
        elif competitor_positions:
            position_gap = np.mean(competitor_positions) - user_avg_position
            insights.append(
                f"Your top competitors rank {abs(position_gap):.1f} positions higher on average. "
                f"This represents an opportunity to study their content strategies."
            )

        # Insight 4: Traffic opportunity
        total_competitor_traffic = sum(c["estimated_monthly_traffic"] for c in competitors[:5])
        if total_competitor_traffic > 1000:
            insights.append(
                f"Your top 5 competitors capture an estimated {total_competitor_traffic:,} "
                f"monthly clicks from shared keywords, representing significant traffic opportunity."
            )

        return insights

    def _extract_domain_from_url(self, url: str) -> Optional[str]:
        """Extract clean domain from URL."""
        if not url:
            return None

        try:
            # Remove protocol
            url = re.sub(r"^https?://", "", url)
            # Get domain part (before first /)
            domain = url.split("/")[0]
            # Remove port if present
            domain = domain.split(":")[0]
            return domain.lower()
        except Exception:
            return None

    def _normalize_domain(self, domain: str) -> str:
        """Normalize domain for comparison (remove www, lowercase)."""
        if not domain:
            return ""
        domain = domain.lower()
        domain = re.sub(r"^www\.", "", domain)
        return domain

    def _empty_result(self) -> Dict[str, Any]:
        """Return empty result structure."""
        return {
            "competitors": [],
            "total_competitors_found": 0,
            "total_keywords_analyzed": 0,
            "summary": {
                "total_competitors": 0,
                "avg_overlap_percentage": 0,
                "highest_overlap_percentage": 0,
                "most_threatened_keywords": 0,
                "primary_competitors": []
            },
            "insights": ["Insufficient data to identify competitors."],
            "metadata": {
                "site_domain": "",
                "analysis_date": datetime.utcnow().isoformat(),
                "min_keywords_threshold": self.min_keywords_for_competitor
            }
        }
