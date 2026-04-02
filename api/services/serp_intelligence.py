import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import pandas as pd
import numpy as np
from collections import defaultdict

from api.services.dataforseo_client import DataForSEOClient
from api.services.gsc_client import GSCClient
from api.models.serp_intelligence import (
    SERPIntelligenceResult,
    CompetitorData,
    SERPFeatureDisplacement,
    CTROpportunity,
    KeywordSERPData
)

logger = logging.getLogger(__name__)


class SERPIntelligenceService:
    """
    Service layer for SERP intelligence analysis.
    
    Responsibilities:
    1. Identify top competitors from GSC query data
    2. Fetch SERP positions and features for key queries
    3. Calculate CTR opportunity gaps
    4. Detect SERP feature presence and displacement
    5. Prepare data for modules 3, 8, 11
    """

    def __init__(
        self,
        dataforseo_client: DataForSEOClient,
        gsc_client: GSCClient
    ):
        self.dataforseo_client = dataforseo_client
        self.gsc_client = gsc_client
        
        # SERP feature to visual position weight mapping
        self.feature_weights = {
            "featured_snippet": 2.0,
            "knowledge_panel": 2.5,
            "ai_overview": 3.0,
            "local_pack": 2.0,
            "people_also_ask": 0.5,  # per question
            "video_carousel": 1.5,
            "image_pack": 1.0,
            "shopping_results": 1.5,
            "top_stories": 1.0,
            "twitter_results": 0.5,
            "reddit_threads": 0.5,
        }
        
        # Position-based CTR curves (baseline, no SERP features)
        self.baseline_ctr_curve = {
            1: 0.285, 2: 0.153, 3: 0.098, 4: 0.070, 5: 0.054,
            6: 0.043, 7: 0.036, 8: 0.030, 9: 0.026, 10: 0.023,
            11: 0.018, 12: 0.015, 13: 0.013, 14: 0.011, 15: 0.010,
            16: 0.009, 17: 0.008, 18: 0.007, 19: 0.007, 20: 0.006
        }

    async def analyze_serp_intelligence(
        self,
        property_url: str,
        start_date: datetime,
        end_date: datetime,
        top_n_keywords: int = 100,
        brand_terms: Optional[List[str]] = None
    ) -> SERPIntelligenceResult:
        """
        Main entry point for SERP intelligence analysis.
        
        Args:
            property_url: GSC property URL
            start_date: Start date for analysis
            end_date: End date for analysis
            top_n_keywords: Number of top keywords to analyze
            brand_terms: List of brand terms to exclude from analysis
            
        Returns:
            SERPIntelligenceResult with comprehensive SERP intelligence
        """
        logger.info(f"Starting SERP intelligence analysis for {property_url}")
        
        try:
            # Step 1: Get GSC query data to identify top keywords
            gsc_queries = await self._fetch_gsc_query_data(
                property_url, start_date, end_date
            )
            
            # Step 2: Filter and rank queries
            target_queries = self._select_target_queries(
                gsc_queries, top_n_keywords, brand_terms
            )
            
            logger.info(f"Selected {len(target_queries)} queries for SERP analysis")
            
            # Step 3: Fetch live SERP data for target queries
            serp_data = await self._fetch_serp_data_batch(target_queries)
            
            # Step 4: Analyze competitors
            competitor_analysis = self._analyze_competitors(
                serp_data, property_url, gsc_queries
            )
            
            # Step 5: Detect SERP feature displacement
            displacement_analysis = self._analyze_serp_displacement(
                serp_data, gsc_queries, property_url
            )
            
            # Step 6: Calculate CTR opportunities
            ctr_opportunities = self._calculate_ctr_opportunities(
                serp_data, gsc_queries, property_url
            )
            
            # Step 7: Classify query intents based on SERP composition
            intent_classification = self._classify_serp_intents(serp_data)
            
            # Step 8: Calculate click share metrics
            click_share_analysis = self._calculate_click_share(
                serp_data, gsc_queries, property_url
            )
            
            # Step 9: Generate summary statistics
            summary = self._generate_summary_stats(
                serp_data,
                competitor_analysis,
                displacement_analysis,
                ctr_opportunities,
                click_share_analysis
            )
            
            return SERPIntelligenceResult(
                property_url=property_url,
                analysis_date=datetime.now(),
                date_range_start=start_date,
                date_range_end=end_date,
                keywords_analyzed=len(target_queries),
                serp_data=serp_data,
                competitors=competitor_analysis,
                displacement_analysis=displacement_analysis,
                ctr_opportunities=ctr_opportunities,
                intent_classification=intent_classification,
                click_share=click_share_analysis,
                summary=summary
            )
            
        except Exception as e:
            logger.error(f"Error in SERP intelligence analysis: {str(e)}")
            raise

    async def _fetch_gsc_query_data(
        self,
        property_url: str,
        start_date: datetime,
        end_date: datetime
    ) -> pd.DataFrame:
        """Fetch query-level data from GSC."""
        logger.info("Fetching GSC query data")
        
        query_data = await self.gsc_client.fetch_query_performance(
            property_url=property_url,
            start_date=start_date,
            end_date=end_date,
            dimensions=["query"],
            row_limit=25000
        )
        
        df = pd.DataFrame(query_data)
        
        # Also fetch query+page data for mapping
        query_page_data = await self.gsc_client.fetch_query_performance(
            property_url=property_url,
            start_date=start_date,
            end_date=end_date,
            dimensions=["query", "page"],
            row_limit=25000
        )
        
        query_page_df = pd.DataFrame(query_page_data)
        
        # Merge to add primary landing page for each query
        primary_pages = query_page_df.loc[
            query_page_df.groupby("query")["clicks"].idxmax()
        ][["query", "page"]]
        
        df = df.merge(
            primary_pages,
            on="query",
            how="left"
        )
        
        return df

    def _select_target_queries(
        self,
        gsc_queries: pd.DataFrame,
        top_n: int,
        brand_terms: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Select target queries for SERP analysis.
        
        Criteria:
        - Exclude branded queries
        - Focus on high-impression queries
        - Include queries with significant position changes
        - Cap at top_n
        """
        df = gsc_queries.copy()
        
        # Filter out branded queries
        if brand_terms:
            brand_pattern = "|".join(
                [term.lower() for term in brand_terms]
            )
            df = df[~df["query"].str.lower().str.contains(brand_pattern, na=False)]
        
        # Filter minimum impressions threshold
        df = df[df["impressions"] >= 10]
        
        # Sort by impressions (primary) and position (secondary for ties)
        df = df.sort_values(
            by=["impressions", "position"],
            ascending=[False, True]
        )
        
        # Take top N
        top_queries = df.head(top_n)
        
        # Convert to list of dicts with metadata
        target_queries = []
        for _, row in top_queries.iterrows():
            target_queries.append({
                "query": row["query"],
                "impressions": int(row["impressions"]),
                "clicks": int(row["clicks"]),
                "ctr": float(row["ctr"]),
                "position": float(row["position"]),
                "landing_page": row.get("page", "")
            })
        
        return target_queries

    async def _fetch_serp_data_batch(
        self,
        queries: List[Dict[str, Any]]
    ) -> List[KeywordSERPData]:
        """
        Fetch SERP data for a batch of queries using DataForSEO.
        
        Uses batch API to optimize cost and performance.
        """
        logger.info(f"Fetching SERP data for {len(queries)} queries")
        
        serp_results = []
        
        # Process in batches of 20 to avoid API limits
        batch_size = 20
        for i in range(0, len(queries), batch_size):
            batch = queries[i:i + batch_size]
            
            try:
                batch_results = await self.dataforseo_client.fetch_serp_batch(
                    keywords=[q["query"] for q in batch],
                    location_code=2840,  # United States
                    language_code="en"
                )
                
                # Process each result
                for j, result in enumerate(batch_results):
                    query_data = batch[j]
                    
                    if result and "items" in result and len(result["items"]) > 0:
                        serp_item = result["items"][0]
                        
                        parsed_serp = self._parse_serp_response(
                            query_data["query"],
                            serp_item,
                            query_data
                        )
                        
                        serp_results.append(parsed_serp)
                    else:
                        logger.warning(
                            f"No SERP data returned for query: {query_data['query']}"
                        )
                        
            except Exception as e:
                logger.error(f"Error fetching SERP batch: {str(e)}")
                continue
        
        logger.info(f"Successfully fetched SERP data for {len(serp_results)} queries")
        return serp_results

    def _parse_serp_response(
        self,
        query: str,
        serp_data: Dict[str, Any],
        gsc_data: Dict[str, Any]
    ) -> KeywordSERPData:
        """
        Parse DataForSEO SERP response into structured format.
        """
        items = serp_data.get("items", [])
        
        # Extract organic results
        organic_results = []
        user_position = None
        user_url = gsc_data.get("landing_page", "")
        
        for item in items:
            item_type = item.get("type", "")
            
            if item_type == "organic":
                rank = item.get("rank_group", 0)
                url = item.get("url", "")
                domain = item.get("domain", "")
                
                organic_results.append({
                    "position": rank,
                    "url": url,
                    "domain": domain,
                    "title": item.get("title", ""),
                    "description": item.get("description", "")
                })
                
                # Check if this is the user's URL
                if user_url and user_url in url:
                    user_position = rank
        
        # Extract SERP features
        serp_features = self._extract_serp_features(items)
        
        # Calculate visual position (accounting for SERP features)
        visual_position = self._calculate_visual_position(
            user_position or gsc_data.get("position", 0),
            serp_features,
            user_position
        )
        
        return KeywordSERPData(
            keyword=query,
            gsc_position=gsc_data.get("position", 0),
            gsc_impressions=gsc_data.get("impressions", 0),
            gsc_clicks=gsc_data.get("clicks", 0),
            gsc_ctr=gsc_data.get("ctr", 0),
            serp_position=user_position or 0,
            visual_position=visual_position,
            organic_results=organic_results,
            serp_features=serp_features,
            landing_page=user_url
        )

    def _extract_serp_features(self, serp_items: List[Dict]) -> Dict[str, Any]:
        """
        Extract and count SERP features from raw SERP data.
        """
        features = {
            "featured_snippet": False,
            "knowledge_panel": False,
            "ai_overview": False,
            "local_pack": False,
            "people_also_ask": 0,
            "video_carousel": False,
            "image_pack": False,
            "shopping_results": False,
            "top_stories": False,
            "twitter_results": 0,
            "reddit_threads": 0,
        }
        
        for item in serp_items:
            item_type = item.get("type", "")
            
            if item_type == "featured_snippet":
                features["featured_snippet"] = True
            elif item_type == "knowledge_graph":
                features["knowledge_panel"] = True
            elif item_type == "ai_overview":
                features["ai_overview"] = True
            elif item_type == "local_pack":
                features["local_pack"] = True
            elif item_type == "people_also_ask":
                features["people_also_ask"] += 1
            elif item_type == "video":
                features["video_carousel"] = True
            elif item_type == "images":
                features["image_pack"] = True
            elif item_type == "shopping":
                features["shopping_results"] = True
            elif item_type == "top_stories":
                features["top_stories"] = True
            elif item_type == "twitter":
                features["twitter_results"] += 1
            elif "reddit" in item.get("url", "").lower():
                features["reddit_threads"] += 1
        
        return features

    def _calculate_visual_position(
        self,
        organic_position: float,
        serp_features: Dict[str, Any],
        actual_position: Optional[int] = None
    ) -> float:
        """
        Calculate the visual position accounting for SERP features.
        
        Visual position = organic position + displacement from features above
        """
        if not actual_position:
            return organic_position
        
        displacement = 0.0
        
        for feature, present in serp_features.items():
            if feature in self.feature_weights:
                if isinstance(present, bool) and present:
                    displacement += self.feature_weights[feature]
                elif isinstance(present, int) and present > 0:
                    # For countable features like PAA
                    displacement += self.feature_weights[feature] * present
        
        return organic_position + displacement

    def _analyze_competitors(
        self,
        serp_data: List[KeywordSERPData],
        user_domain: str,
        gsc_data: pd.DataFrame
    ) -> List[CompetitorData]:
        """
        Identify and analyze top competitors across the keyword set.
        """
        # Extract domain from user URL
        from urllib.parse import urlparse
        user_domain_clean = urlparse(user_domain).netloc.replace("www.", "")
        
        # Count domain appearances across keywords
        domain_counter = defaultdict(lambda: {
            "keyword_count": 0,
            "keywords": [],
            "positions": [],
            "avg_position": 0
        })
        
        total_keywords = len(serp_data)
        
        for serp in serp_data:
            keyword = serp.keyword
            
            # Look at top 10 organic results
            for result in serp.organic_results[:10]:
                domain = result["domain"].replace("www.", "")
                
                # Skip user's own domain
                if domain == user_domain_clean:
                    continue
                
                domain_counter[domain]["keyword_count"] += 1
                domain_counter[domain]["keywords"].append(keyword)
                domain_counter[domain]["positions"].append(result["position"])
        
        # Calculate averages and create competitor objects
        competitors = []
        
        for domain, data in domain_counter.items():
            if data["keyword_count"] < 3:  # Minimum threshold
                continue
            
            keyword_overlap_pct = (data["keyword_count"] / total_keywords) * 100
            avg_position = np.mean(data["positions"])
            
            # Calculate threat level
            threat_level = self._calculate_threat_level(
                keyword_overlap_pct,
                avg_position
            )
            
            competitors.append(CompetitorData(
                domain=domain,
                keywords_shared=data["keyword_count"],
                keyword_overlap_pct=keyword_overlap_pct,
                keywords=data["keywords"][:20],  # Sample of keywords
                avg_position=avg_position,
                position_distribution={
                    "top_3": sum(1 for p in data["positions"] if p <= 3),
                    "top_5": sum(1 for p in data["positions"] if p <= 5),
                    "top_10": sum(1 for p in data["positions"] if p <= 10)
                },
                threat_level=threat_level
            ))
        
        # Sort by keyword overlap (primary indicator of competition)
        competitors.sort(key=lambda x: x.keywords_shared, reverse=True)
        
        return competitors[:20]  # Top 20 competitors

    def _calculate_threat_level(
        self,
        keyword_overlap_pct: float,
        avg_position: float
    ) -> str:
        """
        Calculate threat level based on overlap and position.
        """
        if keyword_overlap_pct > 30 and avg_position < 5:
            return "critical"
        elif keyword_overlap_pct > 20 and avg_position < 7:
            return "high"
        elif keyword_overlap_pct > 10 or avg_position < 5:
            return "medium"
        else:
            return "low"

    def _analyze_serp_displacement(
        self,
        serp_data: List[KeywordSERPData],
        gsc_data: pd.DataFrame,
        user_domain: str
    ) -> List[SERPFeatureDisplacement]:
        """
        Analyze SERP feature displacement impact.
        
        Identifies queries where the user's visual position is significantly
        worse than their organic position due to SERP features.
        """
        displacements = []
        
        for serp in serp_data:
            if not serp.serp_position or serp.serp_position == 0:
                continue
            
            displacement_amount = serp.visual_position - serp.serp_position
            
            # Only flag significant displacement (>2 positions)
            if displacement_amount > 2:
                # Identify which features are causing displacement
                features_above = []
                for feature, present in serp.serp_features.items():
                    if isinstance(present, bool) and present:
                        features_above.append(feature)
                    elif isinstance(present, int) and present > 0:
                        features_above.append(f"{feature}_x{present}")
                
                # Estimate CTR impact
                baseline_ctr = self._get_ctr_for_position(serp.serp_position)
                actual_ctr = self._get_ctr_for_position(
                    serp.visual_position,
                    has_features=True
                )
                ctr_impact = actual_ctr - baseline_ctr
                
                # Calculate monthly click impact
                estimated_monthly_impressions = serp.gsc_impressions
                click_impact = estimated_monthly_impressions * ctr_impact
                
                displacements.append(SERPFeatureDisplacement(
                    keyword=serp.keyword,
                    organic_position=serp.serp_position,
                    visual_position=serp.visual_position,
                    displacement_amount=displacement_amount,
                    features_above=features_above,
                    estimated_ctr_impact=ctr_impact,
                    estimated_click_impact=int(click_impact),
                    monthly_impressions=estimated_monthly_impressions
                ))
        
        # Sort by click impact
        displacements.sort(key=lambda x: abs(x.estimated_click_impact), reverse=True)
        
        return displacements

    def _calculate_ctr_opportunities(
        self,
        serp_data: List[KeywordSERPData],
        gsc_data: pd.DataFrame,
        user_domain: str
    ) -> List[CTROpportunity]:
        """
        Calculate CTR optimization opportunities.
        
        Identifies queries where actual CTR is below expected CTR for the
        position, indicating title/snippet optimization potential.
        """
        opportunities = []
        
        for serp in serp_data:
            if serp.gsc_ctr == 0 or serp.gsc_impressions < 50:
                continue
            
            # Get expected CTR for this position
            expected_ctr = self._get_ctr_for_position(
                serp.gsc_position,
                has_features=any(serp.serp_features.values())
            )
            
            actual_ctr = serp.gsc_ctr
            ctr_gap = actual_ctr - expected_ctr
            
            # Only flag significant underperformance (> 20% below expected)
            if ctr_gap < -0.01 and (ctr_gap / expected_ctr) < -0.2:
                # Calculate potential click gain
                potential_clicks = serp.gsc_impressions * expected_ctr
                current_clicks = serp.gsc_clicks
                click_opportunity = potential_clicks - current_clicks
                
                # Determine likely cause
                cause = self._diagnose_ctr_issue(serp, ctr_gap)
                
                opportunities.append(CTROpportunity(
                    keyword=serp.keyword,
                    current_position=serp.gsc_position,
                    current_ctr=actual_ctr,
                    expected_ctr=expected_ctr,
                    ctr_gap=ctr_gap,
                    ctr_gap_pct=(ctr_gap / expected_ctr) * 100,
                    monthly_impressions=serp.gsc_impressions,
                    current_monthly_clicks=current_clicks,
                    potential_monthly_clicks=int(potential_clicks),
                    click_opportunity=int(click_opportunity),
                    landing_page=serp.landing_page,
                    likely_cause=cause,
                    serp_features_present=list(
                        k for k, v in serp.serp_features.items()
                        if (isinstance(v, bool) and v) or (isinstance(v, int) and v > 0)
                    )
                ))
        
        # Sort by click opportunity
        opportunities.sort(key=lambda x: x.click_opportunity, reverse=True)
        
        return opportunities[:50]  # Top 50 opportunities

    def _get_ctr_for_position(
        self,
        position: float,
        has_features: bool = False
    ) -> float:
        """
        Get expected CTR for a given position.
        
        Applies adjustments for SERP features if present.
        """
        # Round to nearest integer position
        pos_int = max(1, min(20, round(position)))
        
        base_ctr = self.baseline_ctr_curve.get(pos_int, 0.005)
        
        # Apply feature discount if applicable
        if has_features:
            # Features typically reduce CTR by 15-30%
            base_ctr *= 0.75
        
        return base_ctr

    def _diagnose_ctr_issue(
        self,
        serp: KeywordSERPData,
        ctr_gap: float
    ) -> str:
        """
        Diagnose the likely cause of CTR underperformance.
        """
        # Check for SERP feature displacement
        if serp.visual_position - serp.serp_position > 2:
            return "serp_feature_displacement"
        
        # Check for strong competitors above
        if serp.serp_position > 3:
            return "position_too_low"
        
        # Check for query-page mismatch indicators
        # (would need content analysis, placeholder for now)
        if serp.gsc_position < 5:
            return "title_snippet_optimization"
        
        return "title_snippet_optimization"

    def _classify_serp_intents(
        self,
        serp_data: List[KeywordSERPData]
    ) -> Dict[str, Any]:
        """
        Classify SERP intent based on SERP feature composition.
        """
        intent_classification = {
            "informational": [],
            "commercial": [],
            "transactional": [],
            "navigational": []
        }
        
        for serp in serp_data:
            intent = self._classify_single_serp_intent(serp)
            intent_classification[intent].append({
                "keyword": serp.keyword,
                "confidence": 0.8,  # Placeholder confidence score
                "features": list(
                    k for k, v in serp.serp_features.items()
                    if (isinstance(v, bool) and v) or (isinstance(v, int) and v > 0)
                )
            })
        
        # Calculate distribution
        total = len(serp_data)
        distribution = {
            intent: {
                "count": len(keywords),
                "percentage": (len(keywords) / total * 100) if total > 0 else 0
            }
            for intent, keywords in intent_classification.items()
        }
        
        return {
            "classification": intent_classification,
            "distribution": distribution
        }

    def _classify_single_serp_intent(self, serp: KeywordSERPData) -> str:
        """
        Classify a single SERP's intent based on features.
        """
        features = serp.serp_features
        
        # Transactional signals
        if features.get("shopping_results"):
            return "transactional"
        
        # Navigational signals
        if features.get("knowledge_panel"):
            return "navigational"
        
        # Commercial signals
        if features.get("video_carousel") or features.get("image_pack"):
            return "commercial"
        
        # Informational signals (default)
        if features.get("people_also_ask", 0) > 2 or features.get("featured_snippet"):
            return "informational"
        
        # Default to commercial (most common for ambiguous cases)
        return "commercial"

    def _calculate_click_share(
        self,
        serp_data: List[KeywordSERPData],
        gsc_data: pd.DataFrame,
        user_domain: str
    ) -> Dict[str, Any]:
        """
        Calculate the user's click share across their keyword portfolio.
        """
        total_clicks_captured = 0
        total_clicks_available = 0
        
        keyword_click_shares = []
        
        for serp in serp_data:
            # User's actual clicks
            user_clicks = serp.gsc_clicks
            
            # Estimate total available clicks for this keyword
            # Based on search volume proxy (impressions) and position-based CTR
            total_available = 0
            for i in range(1, 11):  # Top 10 positions
                position_ctr = self._get_ctr_for_position(
                    i,
                    has_features=any(serp.serp_features.values())
                )
                total_available += serp.gsc_impressions * position_ctr
            
            # User's click share for this keyword
            click_share = user_clicks / total_available if total_available > 0 else 0
            
            total_clicks_captured += user_clicks
            total_clicks_available += total_available
            
            keyword_click_shares.append({
                "keyword": serp.keyword,
                "user_clicks": user_clicks,
                "available_clicks": int(total_available),
                "click_share": click_share,
                "position": serp.gsc_position
            })
        
        # Overall click share
        overall_click_share = (
            total_clicks_captured / total_clicks_available
            if total_clicks_available > 0 else 0
        )
        
        # Click share opportunity (headroom)
        click_share_opportunity = 1.0 - overall_click_share
        
        # Potential monthly clicks if click share was optimized
        potential_monthly_clicks = total_clicks_available * 0.5  # Assume 50% is achievable
        click_opportunity = potential_monthly_clicks - total_clicks_captured
        
        return {
            "total_clicks_captured": int(total_clicks_captured),
            "total_clicks_available": int(total_clicks_available),
            "overall_click_share": overall_click_share,
            "click_share_opportunity": click_share_opportunity,
            "potential_monthly_clicks": int(potential_monthly_clicks),
            "monthly_click_opportunity": int(click_opportunity),
            "keyword_breakdown": sorted(
                keyword_click_shares,
                key=lambda x: x["available_clicks"],
                reverse=True
            )[:30]  # Top 30
        }

    def _generate_summary_stats(
        self,
        serp_data: List[KeywordSERPData],
        competitors: List[CompetitorData],
        displacements: List[SERPFeatureDisplacement],
        ctr_opportunities: List[CTROpportunity],
        click_share: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Generate summary statistics for the SERP intelligence analysis.
        """
        # Feature presence summary
        feature_counts = defaultdict(int)
        for serp in serp_data:
            for feature, present in serp.serp_features.items():
                if isinstance(present, bool) and present:
                    feature_counts[feature] += 1
                elif isinstance(present, int) and present > 0:
                    feature_counts[feature] += 1
        
        total_keywords = len(serp_data)
        feature_prevalence = {
            feature: {
                "count": count,
                "percentage": (count / total_keywords * 100) if total_keywords > 0 else 0
            }
            for feature, count in feature_counts.items()
        }
        
        # Position distribution
        position_bins = {
            "top_3": 0,
            "positions_4_10": 0,
            "positions_11_20": 0,
            "below_20": 0
        }
        
        for serp in serp_data:
            pos = serp.gsc_position
            if pos <= 3:
                position_bins["top_3"] += 1
            elif pos <= 10:
                position_bins["positions_4_10"] += 1
            elif pos <= 20:
                position_bins["positions_11_20"] += 1
            else:
                position_bins["below_20"] += 1
        
        # Opportunity summary
        total_displacement_impact = sum(
            abs(d.estimated_click_impact) for d in displacements
        )
        
        total_ctr_opportunity = sum(
            o.click_opportunity for o in ctr_opportunities
        )
        
        return {
            "keywords_analyzed": total_keywords,
            "competitors_identified": len(competitors),
            "top_competitor": competitors[0].domain if competitors else None,
            "feature_prevalence": feature_prevalence,
            "position_distribution": position_bins,
            "avg_position": np.mean([s.gsc_position for s in serp_data]),
            "displacement_issues": len(displacements),
            "total_displacement_click_impact": int(total_displacement_impact),
            "ctr_optimization_opportunities": len(ctr_opportunities),
            "total_ctr_click_opportunity": int(total_ctr_opportunity),
            "current_click_share": click_share["overall_click_share"],
            "click_share_opportunity": click_share["click_share_opportunity"],
            "total_monthly_click_opportunity": (
                int(total_displacement_impact) + int(total_ctr_opportunity)
            )
        }
