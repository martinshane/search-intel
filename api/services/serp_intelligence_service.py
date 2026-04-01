"""
SERP Intelligence Service

Service layer that uses dataforseo_client to aggregate SERP intelligence:
1) Fetch top 10 competitors for key queries from GSC
2) Analyze SERP features present
3) Prepare data structures for Module 3 (competitor mapping), Module 8 (CTR opportunity modeling), and Module 11 (SERP feature recommendations)
4) Includes caching logic to avoid redundant API calls

Integrates with:
- dataforseo_client for live SERP data
- gsc_service for keyword selection
- Supabase for caching
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Set
from collections import defaultdict, Counter
import json
import hashlib

from api.clients.dataforseo_client import DataForSEOClient
from api.database import db

logger = logging.getLogger(__name__)


class SerpIntelligenceService:
    """
    Aggregates SERP intelligence data for competitor analysis, CTR modeling, and feature recommendations.
    """

    def __init__(self, dataforseo_client: DataForSEOClient):
        self.dataforseo_client = dataforseo_client
        self.cache_ttl_hours = 24  # Cache SERP data for 24 hours

    # ============================================================================
    # PUBLIC API
    # ============================================================================

    async def analyze_serp_landscape(
        self,
        report_id: str,
        keywords: List[Dict[str, Any]],
        gsc_keyword_data: List[Dict[str, Any]],
        location_code: int = 2840,  # USA
        language_code: str = "en",
    ) -> Dict[str, Any]:
        """
        Main entry point for SERP landscape analysis.

        Args:
            report_id: Report identifier for caching
            keywords: List of keyword dicts with 'query', 'impressions', 'position', etc.
            gsc_keyword_data: Raw GSC data for position tracking
            location_code: DataForSEO location code
            language_code: Language code

        Returns:
            Dictionary containing:
            - keywords_analyzed: int
            - serp_feature_displacement: List of keywords with visual position impact
            - competitors: List of competitor domains with metrics
            - intent_classifications: Intent analysis per keyword
            - total_click_share: Estimated click share across portfolio
            - click_share_opportunity: Potential click share
            - serp_features_summary: Aggregate feature presence data
        """
        logger.info(f"Starting SERP landscape analysis for report {report_id} with {len(keywords)} keywords")

        # Fetch SERP data for all keywords
        serp_results = await self._fetch_serp_data_batch(
            report_id=report_id,
            keywords=keywords,
            location_code=location_code,
            language_code=language_code,
        )

        # Analyze SERP feature displacement
        displacement_analysis = self._analyze_serp_feature_displacement(
            serp_results=serp_results,
            gsc_keyword_data=gsc_keyword_data,
        )

        # Build competitor mapping
        competitor_mapping = self._build_competitor_mapping(
            serp_results=serp_results,
            keywords=keywords,
        )

        # Classify intent based on SERP composition
        intent_classifications = self._classify_serp_intent(serp_results)

        # Estimate click share
        click_share_data = self._estimate_click_share(
            serp_results=serp_results,
            keywords=keywords,
        )

        # Aggregate SERP features summary
        features_summary = self._aggregate_serp_features(serp_results)

        return {
            "keywords_analyzed": len([r for r in serp_results if r.get("success")]),
            "serp_feature_displacement": displacement_analysis,
            "competitors": competitor_mapping,
            "intent_classifications": intent_classifications,
            "total_click_share": click_share_data["total_click_share"],
            "click_share_opportunity": click_share_data["click_share_opportunity"],
            "serp_features_summary": features_summary,
            "raw_serp_data": serp_results,  # For downstream modules
        }

    async def get_competitor_details(
        self,
        report_id: str,
        competitor_domain: str,
        keywords: List[str],
        location_code: int = 2840,
        language_code: str = "en",
    ) -> Dict[str, Any]:
        """
        Get detailed competitor analysis for a specific domain.

        Args:
            report_id: Report identifier
            competitor_domain: Competitor domain to analyze
            keywords: List of keyword queries to check
            location_code: DataForSEO location code
            language_code: Language code

        Returns:
            Dictionary with competitor performance metrics
        """
        logger.info(f"Fetching competitor details for {competitor_domain}")

        keyword_dicts = [{"query": kw, "impressions": 0, "position": 0} for kw in keywords]
        serp_results = await self._fetch_serp_data_batch(
            report_id=report_id,
            keywords=keyword_dicts,
            location_code=location_code,
            language_code=language_code,
        )

        competitor_data = {
            "domain": competitor_domain,
            "keywords_ranking": [],
            "avg_position": 0,
            "position_distribution": defaultdict(int),
            "serp_features_captured": defaultdict(int),
        }

        positions = []
        for result in serp_results:
            if not result.get("success"):
                continue

            keyword = result["keyword"]
            items = result.get("items", [])

            for item in items:
                if not isinstance(item, dict):
                    continue

                # Check organic results
                organic_results = item.get("items", [])
                for org_result in organic_results:
                    if org_result.get("type") != "organic":
                        continue

                    result_domain = self._extract_domain(org_result.get("url", ""))
                    if result_domain == competitor_domain:
                        rank_absolute = org_result.get("rank_absolute", 0)
                        positions.append(rank_absolute)
                        competitor_data["keywords_ranking"].append({
                            "keyword": keyword,
                            "position": rank_absolute,
                            "url": org_result.get("url"),
                            "title": org_result.get("title"),
                        })
                        competitor_data["position_distribution"][rank_absolute] += 1

                # Check SERP features
                for feature_item in organic_results:
                    if feature_item.get("type") == "organic":
                        continue
                    result_domain = self._extract_domain(feature_item.get("url", ""))
                    if result_domain == competitor_domain:
                        feature_type = feature_item.get("type", "unknown")
                        competitor_data["serp_features_captured"][feature_type] += 1

        if positions:
            competitor_data["avg_position"] = sum(positions) / len(positions)

        competitor_data["position_distribution"] = dict(competitor_data["position_distribution"])
        competitor_data["serp_features_captured"] = dict(competitor_data["serp_features_captured"])

        return competitor_data

    async def get_serp_features_for_keyword(
        self,
        report_id: str,
        keyword: str,
        location_code: int = 2840,
        language_code: str = "en",
    ) -> Dict[str, Any]:
        """
        Get detailed SERP feature breakdown for a single keyword.

        Args:
            report_id: Report identifier
            keyword: Keyword query
            location_code: DataForSEO location code
            language_code: Language code

        Returns:
            Dictionary with SERP features present and their details
        """
        serp_results = await self._fetch_serp_data_batch(
            report_id=report_id,
            keywords=[{"query": keyword, "impressions": 0, "position": 0}],
            location_code=location_code,
            language_code=language_code,
        )

        if not serp_results or not serp_results[0].get("success"):
            return {"keyword": keyword, "features": {}, "error": "Failed to fetch SERP data"}

        result = serp_results[0]
        items = result.get("items", [])

        features_data = {
            "keyword": keyword,
            "features": {},
            "organic_results": [],
            "total_serp_elements": 0,
        }

        if not items:
            return features_data

        item = items[0]
        organic_results = item.get("items", [])

        element_count = 0
        for serp_element in organic_results:
            element_type = serp_element.get("type", "unknown")

            if element_type == "organic":
                features_data["organic_results"].append({
                    "position": serp_element.get("rank_absolute"),
                    "url": serp_element.get("url"),
                    "domain": self._extract_domain(serp_element.get("url", "")),
                    "title": serp_element.get("title"),
                })
            else:
                if element_type not in features_data["features"]:
                    features_data["features"][element_type] = []

                features_data["features"][element_type].append({
                    "position": serp_element.get("rank_absolute"),
                    "title": serp_element.get("title"),
                    "url": serp_element.get("url"),
                })

            element_count += 1

        features_data["total_serp_elements"] = element_count

        return features_data

    # ============================================================================
    # INTERNAL METHODS - SERP DATA FETCHING
    # ============================================================================

    async def _fetch_serp_data_batch(
        self,
        report_id: str,
        keywords: List[Dict[str, Any]],
        location_code: int,
        language_code: str,
    ) -> List[Dict[str, Any]]:
        """
        Fetch SERP data for a batch of keywords with caching.

        Args:
            report_id: Report identifier for cache namespacing
            keywords: List of keyword dicts
            location_code: DataForSEO location code
            language_code: Language code

        Returns:
            List of SERP results (one per keyword)
        """
        results = []

        for keyword_dict in keywords:
            keyword = keyword_dict["query"]

            # Check cache first
            cached_result = await self._get_cached_serp_data(
                report_id=report_id,
                keyword=keyword,
                location_code=location_code,
                language_code=language_code,
            )

            if cached_result:
                logger.debug(f"Using cached SERP data for keyword: {keyword}")
                results.append(cached_result)
                continue

            # Fetch from DataForSEO
            try:
                serp_data = await self.dataforseo_client.get_serp_results(
                    keyword=keyword,
                    location_code=location_code,
                    language_code=language_code,
                    depth=100,  # Get up to 100 results
                )

                result = {
                    "keyword": keyword,
                    "success": True,
                    "items": serp_data.get("items", []),
                    "fetched_at": datetime.utcnow().isoformat(),
                }

                # Cache the result
                await self._cache_serp_data(
                    report_id=report_id,
                    keyword=keyword,
                    location_code=location_code,
                    language_code=language_code,
                    data=result,
                )

                results.append(result)

            except Exception as e:
                logger.error(f"Error fetching SERP data for keyword '{keyword}': {str(e)}")
                results.append({
                    "keyword": keyword,
                    "success": False,
                    "error": str(e),
                })

        return results

    async def _get_cached_serp_data(
        self,
        report_id: str,
        keyword: str,
        location_code: int,
        language_code: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieve cached SERP data if available and fresh.

        Args:
            report_id: Report identifier
            keyword: Keyword query
            location_code: Location code
            language_code: Language code

        Returns:
            Cached SERP data or None
        """
        cache_key = self._generate_cache_key(keyword, location_code, language_code)

        try:
            result = db.table("serp_cache").select("*").eq("report_id", report_id).eq("cache_key", cache_key).execute()

            if result.data and len(result.data) > 0:
                cache_entry = result.data[0]
                cached_at = datetime.fromisoformat(cache_entry["cached_at"])

                # Check if cache is still fresh
                if datetime.utcnow() - cached_at < timedelta(hours=self.cache_ttl_hours):
                    return cache_entry["serp_data"]

            return None

        except Exception as e:
            logger.error(f"Error retrieving cached SERP data: {str(e)}")
            return None

    async def _cache_serp_data(
        self,
        report_id: str,
        keyword: str,
        location_code: int,
        language_code: str,
        data: Dict[str, Any],
    ) -> None:
        """
        Cache SERP data in Supabase.

        Args:
            report_id: Report identifier
            keyword: Keyword query
            location_code: Location code
            language_code: Language code
            data: SERP data to cache
        """
        cache_key = self._generate_cache_key(keyword, location_code, language_code)

        try:
            # Upsert cache entry
            db.table("serp_cache").upsert({
                "report_id": report_id,
                "cache_key": cache_key,
                "keyword": keyword,
                "location_code": location_code,
                "language_code": language_code,
                "serp_data": data,
                "cached_at": datetime.utcnow().isoformat(),
            }).execute()

        except Exception as e:
            logger.error(f"Error caching SERP data: {str(e)}")

    def _generate_cache_key(self, keyword: str, location_code: int, language_code: str) -> str:
        """
        Generate a unique cache key for a SERP query.

        Args:
            keyword: Keyword query
            location_code: Location code
            language_code: Language code

        Returns:
            Cache key string
        """
        key_string = f"{keyword}:{location_code}:{language_code}"
        return hashlib.md5(key_string.encode()).hexdigest()

    # ============================================================================
    # INTERNAL METHODS - SERP FEATURE DISPLACEMENT ANALYSIS
    # ============================================================================

    def _analyze_serp_feature_displacement(
        self,
        serp_results: List[Dict[str, Any]],
        gsc_keyword_data: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Analyze how SERP features displace organic results.

        Calculates "visual position" (number of SERP elements above the user's listing)
        and compares it to organic rank.

        Args:
            serp_results: SERP data from DataForSEO
            gsc_keyword_data: GSC keyword performance data

        Returns:
            List of displacement analysis dicts
        """
        displacement_data = []

        # Build GSC lookup
        gsc_lookup = {item["query"]: item for item in gsc_keyword_data}

        for result in serp_results:
            if not result.get("success"):
                continue

            keyword = result["keyword"]
            items = result.get("items", [])

            if not items:
                continue

            # Get user's organic position from GSC
            gsc_data = gsc_lookup.get(keyword)
            if not gsc_data:
                continue

            organic_position = gsc_data.get("position", 0)
            if organic_position == 0:
                continue

            # Parse SERP to find user's listing and features above it
            item = items[0]
            serp_items = item.get("items", [])

            user_listing_found = False
            features_above = []
            visual_position = 0

            for serp_element in serp_items:
                element_type = serp_element.get("type", "unknown")
                rank_absolute = serp_element.get("rank_absolute", 0)

                if element_type == "organic":
                    # Check if this is the user's listing
                    # We don't have the user's domain here, so we use position matching
                    if abs(rank_absolute - organic_position) <= 1:
                        user_listing_found = True
                        break
                else:
                    # SERP feature
                    features_above.append(element_type)
                    visual_position += self._calculate_feature_visual_weight(element_type)

            if user_listing_found and visual_position > 0:
                visual_position_final = organic_position + visual_position
                displacement = visual_position_final - organic_position

                if displacement >= 2:  # Only report significant displacement
                    # Estimate CTR impact
                    ctr_impact = self._estimate_ctr_impact(
                        organic_position=organic_position,
                        visual_position=visual_position_final,
                        features_above=features_above,
                    )

                    displacement_data.append({
                        "keyword": keyword,
                        "organic_position": organic_position,
                        "visual_position": visual_position_final,
                        "features_above": features_above,
                        "displacement_magnitude": displacement,
                        "estimated_ctr_impact": ctr_impact,
                        "impressions": gsc_data.get("impressions", 0),
                        "clicks": gsc_data.get("clicks", 0),
                    })

        # Sort by estimated impact
        displacement_data.sort(key=lambda x: abs(x["estimated_ctr_impact"]) * x["impressions"], reverse=True)

        return displacement_data

    def _calculate_feature_visual_weight(self, feature_type: str) -> float:
        """
        Calculate how many "positions" a SERP feature displaces.

        Args:
            feature_type: Type of SERP feature

        Returns:
            Visual displacement weight
        """
        weights = {
            "featured_snippet": 2.0,
            "knowledge_panel": 1.5,
            "local_pack": 2.0,
            "people_also_ask": 0.5,  # per item
            "video": 1.0,
            "video_carousel": 1.5,
            "image_pack": 1.0,
            "shopping_results": 1.5,
            "top_stories": 1.0,
            "ai_overview": 2.5,
            "reddit_results": 0.5,
            "twitter": 0.5,
        }

        return weights.get(feature_type, 0.5)

    def _estimate_ctr_impact(
        self,
        organic_position: float,
        visual_position: float,
        features_above: List[str],
    ) -> float:
        """
        Estimate CTR impact of SERP feature displacement.

        Args:
            organic_position: Organic rank
            visual_position: Visual position after displacement
            features_above: List of SERP features above the listing

        Returns:
            Estimated CTR impact (negative value = CTR loss)
        """
        # Base CTR curve (simplified)
        base_ctrs = {
            1: 0.315, 2: 0.156, 3: 0.101, 4: 0.075, 5: 0.061,
            6: 0.049, 7: 0.042, 8: 0.037, 9: 0.033, 10: 0.030,
        }

        organic_ctr = base_ctrs.get(int(organic_position), 0.02)
        visual_ctr = base_ctrs.get(int(visual_position), 0.01)

        # Apply feature-specific modifiers
        feature_modifier = 1.0
        if "ai_overview" in features_above:
            feature_modifier *= 0.6  # AI overviews significantly reduce CTR
        if "featured_snippet" in features_above:
            feature_modifier *= 0.7
        if "local_pack" in features_above:
            feature_modifier *= 0.75

        adjusted_ctr = visual_ctr * feature_modifier
        impact = adjusted_ctr - organic_ctr

        return round(impact, 4)

    # ============================================================================
    # INTERNAL METHODS - COMPETITOR MAPPING
    # ============================================================================

    def _build_competitor_mapping(
        self,
        serp_results: List[Dict[str, Any]],
        keywords: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Build competitor frequency matrix from SERP results.

        Identifies domains that appear most frequently across the user's keyword set.

        Args:
            serp_results: SERP data from DataForSEO
            keywords: Keyword list with impressions

        Returns:
            List of competitor dicts with metrics
        """
        # Track competitor appearances
        competitor_data = defaultdict(lambda: {
            "appearances": 0,
            "positions": [],
            "keywords": [],
            "serp_features": defaultdict(int),
        })

        keyword_lookup = {kw["query"]: kw for kw in keywords}

        for result in serp_results:
            if not result.get("success"):
                continue

            keyword = result["keyword"]
            keyword_data = keyword_lookup.get(keyword, {})
            items = result.get("items", [])

            if not items:
                continue

            item = items[0]
            serp_items = item.get("items", [])

            # Track domains in top 10 organic results
            for serp_element in serp_items[:20]:  # Look at top 20 to catch all top 10 organic
                element_type = serp_element.get("type", "unknown")
                url = serp_element.get("url", "")
                domain = self._extract_domain(url)

                if not domain:
                    continue

                if element_type == "organic":
                    rank = serp_element.get("rank_absolute", 0)
                    if rank <= 10:
                        competitor_data[domain]["appearances"] += 1
                        competitor_data[domain]["positions"].append(rank)
                        competitor_data[domain]["keywords"].append({
                            "keyword": keyword,
                            "position": rank,
                            "impressions": keyword_data.get("impressions", 0),
                        })
                else:
                    # Track SERP feature captures
                    competitor_data[domain]["serp_features"][element_type] += 1

        # Convert to list and calculate metrics
        competitors = []
        total_keywords = len([r for r in serp_results if r.get("success")])

        for domain, data in competitor_data.items():
            if data["appearances"] == 0:
                continue

            avg_position = sum(data["positions"]) / len(data["positions"]) if data["positions"] else 0
            overlap_pct = (data["appearances"] / total_keywords) * 100 if total_keywords > 0 else 0

            # Calculate threat level
            threat_level = "low"
            if overlap_pct > 40 or avg_position < 4:
                threat_level = "high"
            elif overlap_pct > 20 or avg_position < 7:
                threat_level = "medium"

            # Calculate total impressions shared
            total_shared_impressions = sum(kw["impressions"] for kw in data["keywords"])

            competitors.append({
                "domain": domain,
                "keywords_shared": data["appearances"],
                "overlap_percentage": round(overlap_pct, 1),
                "avg_position": round(avg_position, 1),
                "position_distribution": dict(Counter([int(p) for p in data["positions"]])),
                "threat_level": threat_level,
                "serp_features_captured": dict(data["serp_features"]),
                "total_shared_impressions": total_shared_impressions,
                "top_competing_keywords": sorted(
                    data["keywords"],
                    key=lambda x: x["impressions"],
                    reverse=True
                )[:10],
            })

        # Sort by threat (combination of overlap and position)
        competitors.sort(
            key=lambda x: (x["overlap_percentage"] * (11 - x["avg_position"])),
            reverse=True
        )

        return competitors

    # ============================================================================
    # INTERNAL METHODS - INTENT CLASSIFICATION
    # ============================================================================

    def _classify_serp_intent(self, serp_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Classify search intent based on SERP composition.

        Args:
            serp_results: SERP data from DataForSEO

        Returns:
            List of intent classification dicts
        """
        intent_classifications = []

        for result in serp_results:
            if not result.get("success"):
                continue

            keyword = result["keyword"]
            items = result.get("items", [])

            if not items:
                continue

            item = items[0]
            serp_items = item.get("items", [])

            # Count feature types
            feature_counts = defaultdict(int)
            organic_count = 0

            for serp_element in serp_items:
                element_type = serp_element.get("type", "unknown")
                feature_counts[element_type] += 1
                if element_type == "organic":
                    organic_count += 1

            # Intent classification logic
            intent = self._determine_intent_from_features(feature_counts)

            # Extract query patterns for additional signals
            query_intent = self._classify_query_pattern(keyword)

            # Combine SERP-based and query-based intent
            final_intent = self._merge_intent_signals(intent, query_intent)

            intent_classifications.append({
                "keyword": keyword,
                "intent": final_intent,
                "confidence": self._calculate_intent_confidence(feature_counts, keyword),
                "serp_features_present": dict(feature_counts),
                "organic_results_count": organic_count,
                "intent_signals": {
                    "serp_based": intent,
                    "query_based": query_intent,
                },
            })

        return intent_classifications

    def _determine_intent_from_features(self, feature_counts: Dict[str, int]) -> str:
        """
        Determine intent from SERP feature composition.

        Args:
            feature_counts: Count of each SERP feature type

        Returns:
            Intent classification string
        """
        # Transactional signals
        if feature_counts.get("shopping_results", 0) > 0:
            return "transactional"

        # Local intent
        if feature_counts.get("local_pack", 0) > 0:
            return "local"

        # Navigational signals
        if feature_counts.get("knowledge_panel", 0) > 0 and feature_counts.get("people_also_ask", 0) == 0:
            return "navigational"

        # Informational signals
        if feature_counts.get("people_also_ask", 0) >= 3 or feature_counts.get("featured_snippet", 0) > 0:
            return "informational"

        # Commercial investigation
        if feature_counts.get("video_carousel", 0) > 0 or feature_counts.get("image_pack", 0) > 0:
            return "commercial"

        # Default to informational
        return "informational"

    def _classify_query_pattern(self, keyword: str) -> str:
        """
        Classify intent based on keyword pattern.

        Args:
            keyword: Search query

        Returns:
            Intent classification
        """
        keyword_lower = keyword.lower()

        # Transactional patterns
        transactional_terms = ["buy", "purchase", "order", "shop", "price", "cheap", "deal", "discount"]
        if any(term in keyword_lower for term in transactional_terms):
            return "transactional"

        # Commercial investigation patterns
        commercial_terms = ["best", "top", "review", "comparison", "vs", "alternative"]
        if any(term in keyword_lower for term in commercial_terms):
            return "commercial"

        # Informational patterns
        informational_terms = ["how", "what", "why", "when", "where", "guide", "tutorial", "tips"]
        if any(term in keyword_lower for term in informational_terms):
            return "informational"

        # Navigational patterns (brand/company names)
        # This is simplified - would need brand list in production
        if len(keyword_lower.split()) <= 2:
            return "navigational"

        return "informational"

    def _merge_intent_signals(self, serp_intent: str, query_intent: str) -> str:
        """
        Merge SERP-based and query-based intent signals.

        Args:
            serp_intent: Intent from SERP features
            query_intent: Intent from query pattern

        Returns:
            Final intent classification
        """
        # If both agree, high confidence
        if serp_intent == query_intent:
            return serp_intent

        # Priority order: transactional > commercial > navigational > informational
        priority = ["transactional", "local", "commercial", "navigational", "informational"]

        for intent_type in priority:
            if serp_intent == intent_type or query_intent == intent_type:
                return intent_type

        return "informational"

    def _calculate_intent_confidence(self, feature_counts: Dict[str, int], keyword: str) -> float:
        """
        Calculate confidence score for intent classification.

        Args:
            feature_counts: SERP feature counts
            keyword: Search query

        Returns:
            Confidence score (0-1)
        """
        # Base confidence
        confidence = 0.5

        # Strong signals boost confidence
        strong_signals = {
            "shopping_results": 0.3,
            "local_pack": 0.3,
            "knowledge_panel": 0.2,
            "featured_snippet": 0.2,
            "people_also_ask": 0.1,
        }

        for feature, boost in strong_signals.items():
            if feature_counts.get(feature, 0) > 0:
                confidence += boost

        # Multiple weak signals also boost confidence
        if len(feature_counts) > 3:
            confidence += 0.1

        return min(confidence, 1.0)

    # ============================================================================
    # INTERNAL METHODS - CLICK SHARE ESTIMATION
    # ============================================================================

    def _estimate_click_share(
        self,
        serp_results: List[Dict[str, Any]],
        keywords: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Estimate click share across keyword portfolio.

        Args:
            serp_results: SERP data
            keywords: Keyword list with GSC data

        Returns:
            Click share analysis dict
        """
        keyword_lookup = {kw["query"]: kw for kw in keywords}

        total_estimated_clicks = 0
        total_potential_clicks = 0
        keyword_details = []

        for result in serp_results:
            if not result.get("success"):
                continue

            keyword = result["keyword"]
            keyword_data = keyword_lookup.get(keyword)
            if not keyword_data:
                continue

            impressions = keyword_data.get("impressions", 0)
            position = keyword_data.get("position", 0)

            if impressions == 0 or position == 0:
                continue

            # Calculate position-adjusted CTR
            actual_ctr = keyword_data.get("ctr", 0)
            expected_ctr = self._get_position_ctr(position, result)

            # Estimate clicks
            estimated_clicks = impressions * expected_ctr
            total_estimated_clicks += estimated_clicks

            # Potential clicks (if in position 1 with no features)
            potential_ctr = 0.315  # Position 1 baseline
            potential_clicks = impressions * potential_ctr
            total_potential_clicks += potential_clicks

            keyword_details.append({
                "keyword": keyword,
                "position": position,
                "impressions": impressions,
                "actual_ctr": actual_ctr,
                "expected_ctr": expected_ctr,
                "estimated_clicks": round(estimated_clicks, 1),
                "potential_clicks": round(potential_clicks, 1),
                "click_share": round(estimated_clicks / potential_clicks, 3) if potential_clicks > 0 else 0,
            })

        total_click_share = total_estimated_clicks / total_potential_clicks if total_potential_clicks > 0 else 0
        click_share_opportunity = 1.0 - total_click_share

        return {
            "total_click_share": round(total_click_share, 3),
            "click_share_opportunity": round(click_share_opportunity, 3),
            "total_estimated_clicks": round(total_estimated_clicks, 1),
            "total_potential_clicks": round(total_potential_clicks, 1),
            "keyword_details": keyword_details,
        }

    def _get_position_ctr(self, position: float, serp_result: Dict[str, Any]) -> float:
        """
        Get position-adjusted CTR based on SERP features.

        Args:
            position: Organic position
            serp_result: SERP data for context

        Returns:
            Expected CTR
        """
        # Base CTR curve
        base_ctrs = {
            1: 0.315, 2: 0.156, 3: 0.101, 4: 0.075, 5: 0.061,
            6: 0.049, 7: 0.042, 8: 0.037, 9: 0.033, 10: 0.030,
        }

        base_ctr = base_ctrs.get(int(position), 0.02)

        # Adjust for SERP features
        items = serp_result.get("items", [])
        if items:
            item = items[0]
            serp_items = item.get("items", [])

            feature_modifier = 1.0
            for serp_element in serp_items:
                element_type = serp_element.get("type", "unknown")
                if element_type != "organic":
                    if element_type == "ai_overview":
                        feature_modifier *= 0.6
                    elif element_type == "featured_snippet":
                        feature_modifier *= 0.7
                    elif element_type in ["local_pack", "shopping_results"]:
                        feature_modifier *= 0.75

            base_ctr *= feature_modifier

        return base_ctr

    # ============================================================================
    # INTERNAL METHODS - SERP FEATURES AGGREGATION
    # ============================================================================

    def _aggregate_serp_features(self, serp_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Aggregate SERP feature presence across all keywords.

        Args:
            serp_results: SERP data

        Returns:
            Aggregated feature statistics
        """
        feature_counts = defaultdict(int)
        total_keywords = 0

        for result in serp_results:
            if not result.get("success"):
                continue

            total_keywords += 1
            items = result.get("items", [])

            if not items:
                continue

            item = items[0]
            serp_items = item.get("items", [])

            features_in_serp = set()
            for serp_element in serp_items:
                element_type = serp_element.get("type", "unknown")
                if element_type != "organic":
                    features_in_serp.add(element_type)

            for feature in features_in_serp:
                feature_counts[feature] += 1

        # Calculate percentages
        feature_stats = {}
        for feature, count in feature_counts.items():
            feature_stats[feature] = {
                "count": count,
                "percentage": round((count / total_keywords) * 100, 1) if total_keywords > 0 else 0,
            }

        # Sort by frequency
        sorted_features = sorted(
            feature_stats.items(),
            key=lambda x: x[1]["count"],
            reverse=True
        )

        return {
            "total_keywords_analyzed": total_keywords,
            "features": dict(sorted_features),
            "most_common_feature": sorted_features[0][0] if sorted_features else None,
            "avg_features_per_serp": round(sum(feature_counts.values()) / total_keywords, 1) if total_keywords > 0 else 0,
        }

    # ============================================================================
    # UTILITY METHODS
    # ============================================================================

    def _extract_domain(self, url: str) -> str:
        """
        Extract domain from URL.

        Args:
            url: Full URL

        Returns:
            Domain string
        """
        if not url:
            return ""

        # Remove protocol
        if "://" in url:
            url = url.split("://", 1)[1]

        # Get domain (before first /)
        domain = url.split("/")[0]

        # Remove www.
        if domain.startswith("www."):
            domain = domain[4:]

        return domain.lower()
