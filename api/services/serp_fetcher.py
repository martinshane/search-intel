"""
SERP Fetcher Service

Service layer that uses dataforseo_client to fetch SERP data for a domain's top queries.
Handles batch SERP requests, extracts competitor domains, SERP features, and position data.
Integrates with Supabase to cache SERP results in serp_data table.
"""

import asyncio
import hashlib
import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse

from api.clients.dataforseo_client import DataForSEOClient
from api.config import get_settings
from api.db.supabase_client import get_supabase_client

settings = get_settings()


class SERPFetcher:
    """Service for fetching and processing SERP data"""

    def __init__(self):
        self.dataforseo_client = DataForSEOClient()
        self.supabase = get_supabase_client()

    def _generate_serp_cache_key(
        self, keyword: str, location_code: int, language_code: str
    ) -> str:
        """Generate cache key for SERP data"""
        cache_str = f"{keyword}_{location_code}_{language_code}"
        return hashlib.md5(cache_str.encode()).hexdigest()

    def _is_cache_valid(self, cached_at: str, ttl_hours: int = 24) -> bool:
        """Check if cached data is still valid"""
        cached_time = datetime.fromisoformat(cached_at.replace("Z", "+00:00"))
        return datetime.utcnow() - cached_time < timedelta(hours=ttl_hours)

    async def _get_cached_serp(
        self, keyword: str, location_code: int, language_code: str
    ) -> Optional[Dict[str, Any]]:
        """Retrieve cached SERP data if available and valid"""
        cache_key = self._generate_serp_cache_key(keyword, location_code, language_code)

        response = (
            self.supabase.table("serp_data")
            .select("*")
            .eq("cache_key", cache_key)
            .order("created_at", desc=True)
            .limit(1)
            .execute()
        )

        if response.data and len(response.data) > 0:
            cached = response.data[0]
            if self._is_cache_valid(cached["created_at"], ttl_hours=24):
                return {
                    "keyword": cached["keyword"],
                    "serp_data": cached["serp_data"],
                    "from_cache": True,
                    "cached_at": cached["created_at"],
                }

        return None

    async def _cache_serp_data(
        self,
        keyword: str,
        location_code: int,
        language_code: str,
        serp_data: Dict[str, Any],
        report_id: Optional[str] = None,
    ) -> None:
        """Store SERP data in cache"""
        cache_key = self._generate_serp_cache_key(keyword, location_code, language_code)

        data = {
            "cache_key": cache_key,
            "keyword": keyword,
            "location_code": location_code,
            "language_code": language_code,
            "serp_data": serp_data,
            "report_id": report_id,
            "created_at": datetime.utcnow().isoformat(),
        }

        self.supabase.table("serp_data").upsert(data, on_conflict="cache_key").execute()

    async def fetch_serp_for_queries(
        self,
        queries: List[str],
        location_code: int = 2840,  # USA
        language_code: str = "en",
        use_cache: bool = True,
        batch_size: int = 10,
        report_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch SERP data for a batch of queries

        Args:
            queries: List of search queries
            location_code: DataForSEO location code
            language_code: Language code
            use_cache: Whether to use cached results
            batch_size: Number of queries to fetch concurrently
            report_id: Optional report ID for associating cached data

        Returns:
            List of SERP results with extracted data
        """
        results = []

        # Process queries in batches to avoid overwhelming the API
        for i in range(0, len(queries), batch_size):
            batch = queries[i : i + batch_size]
            batch_tasks = []

            for keyword in batch:
                batch_tasks.append(
                    self._fetch_single_serp(
                        keyword, location_code, language_code, use_cache, report_id
                    )
                )

            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)

            # Filter out exceptions and add successful results
            for result in batch_results:
                if isinstance(result, Exception):
                    print(f"Error fetching SERP: {result}")
                    continue
                if result:
                    results.append(result)

            # Small delay between batches
            if i + batch_size < len(queries):
                await asyncio.sleep(1)

        return results

    async def _fetch_single_serp(
        self,
        keyword: str,
        location_code: int,
        language_code: str,
        use_cache: bool,
        report_id: Optional[str],
    ) -> Optional[Dict[str, Any]]:
        """Fetch SERP data for a single query"""
        # Check cache first
        if use_cache:
            cached = await self._get_cached_serp(keyword, location_code, language_code)
            if cached:
                return cached

        # Fetch fresh data
        try:
            serp_data = await self.dataforseo_client.get_serp_results(
                keyword=keyword,
                location_code=location_code,
                language_code=language_code,
            )

            if not serp_data:
                return None

            # Cache the results
            await self._cache_serp_data(
                keyword, location_code, language_code, serp_data, report_id
            )

            return {
                "keyword": keyword,
                "serp_data": serp_data,
                "from_cache": False,
                "cached_at": datetime.utcnow().isoformat(),
            }

        except Exception as e:
            print(f"Error fetching SERP for '{keyword}': {e}")
            return None

    def extract_competitor_domains(
        self, serp_results: List[Dict[str, Any]], user_domain: str, top_n: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Extract competitor domains from SERP results

        Args:
            serp_results: List of SERP results from fetch_serp_for_queries
            user_domain: The user's domain to exclude
            top_n: Number of top competitors to return

        Returns:
            List of competitor domains with frequency and metrics
        """
        competitor_freq: Dict[str, Dict[str, Any]] = {}
        user_domain_clean = urlparse(f"https://{user_domain}").netloc.lower()

        for result in serp_results:
            if not result or "serp_data" not in result:
                continue

            keyword = result["keyword"]
            serp_data = result["serp_data"]

            # Extract organic results
            items = serp_data.get("items", [])
            if not items or len(items) == 0:
                continue

            organic_results = items[0].get("items", [])

            for idx, item in enumerate(organic_results, 1):
                if item.get("type") != "organic":
                    continue

                url = item.get("url", "")
                if not url:
                    continue

                domain = urlparse(url).netloc.lower()

                # Skip user's own domain
                if domain == user_domain_clean or user_domain_clean in domain:
                    continue

                # Initialize or update competitor data
                if domain not in competitor_freq:
                    competitor_freq[domain] = {
                        "domain": domain,
                        "keywords_count": 0,
                        "total_position": 0,
                        "keywords": [],
                        "positions": [],
                    }

                competitor_freq[domain]["keywords_count"] += 1
                competitor_freq[domain]["total_position"] += idx
                competitor_freq[domain]["keywords"].append(keyword)
                competitor_freq[domain]["positions"].append(idx)

        # Calculate average positions and sort by frequency
        competitors = []
        for domain, data in competitor_freq.items():
            avg_position = (
                data["total_position"] / data["keywords_count"]
                if data["keywords_count"] > 0
                else 0
            )

            competitors.append(
                {
                    "domain": domain,
                    "keywords_shared": data["keywords_count"],
                    "avg_position": round(avg_position, 1),
                    "keywords": data["keywords"][:10],  # Store sample keywords
                    "position_distribution": {
                        "top_3": sum(1 for p in data["positions"] if p <= 3),
                        "top_5": sum(1 for p in data["positions"] if p <= 5),
                        "top_10": sum(1 for p in data["positions"] if p <= 10),
                    },
                }
            )

        # Sort by keywords_shared descending
        competitors.sort(key=lambda x: x["keywords_shared"], reverse=True)

        return competitors[:top_n]

    def extract_serp_features(
        self, serp_results: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Extract SERP features from results (featured snippets, PAA, local pack, etc.)

        Args:
            serp_results: List of SERP results from fetch_serp_for_queries

        Returns:
            List of keywords with their SERP features
        """
        features_data = []

        for result in serp_results:
            if not result or "serp_data" not in result:
                continue

            keyword = result["keyword"]
            serp_data = result["serp_data"]

            items = serp_data.get("items", [])
            if not items or len(items) == 0:
                continue

            serp_items = items[0].get("items", [])

            # Track all features found
            features = {
                "keyword": keyword,
                "featured_snippet": False,
                "people_also_ask": False,
                "paa_count": 0,
                "video_carousel": False,
                "video_count": 0,
                "local_pack": False,
                "knowledge_panel": False,
                "knowledge_graph": False,
                "ai_overview": False,
                "shopping_results": False,
                "image_pack": False,
                "top_stories": False,
                "twitter": False,
                "reddit_threads": False,
                "site_links": False,
                "reviews": False,
                "total_features": 0,
                "features_above_organic": [],
            }

            organic_position = None

            # Analyze each SERP item
            for idx, item in enumerate(serp_items):
                item_type = item.get("type", "")
                rank_group = item.get("rank_group")
                rank_absolute = item.get("rank_absolute", idx + 1)

                # Track first organic result position
                if item_type == "organic" and organic_position is None:
                    organic_position = rank_absolute

                # Featured snippet
                if item_type == "featured_snippet":
                    features["featured_snippet"] = True
                    features["total_features"] += 1
                    if organic_position is None or rank_absolute < organic_position:
                        features["features_above_organic"].append("featured_snippet")

                # People Also Ask
                elif item_type == "people_also_ask":
                    features["people_also_ask"] = True
                    paa_items = item.get("items", [])
                    features["paa_count"] = len(paa_items)
                    features["total_features"] += 1
                    if organic_position is None or rank_absolute < organic_position:
                        features["features_above_organic"].append(
                            f"paa_x{features['paa_count']}"
                        )

                # Video results
                elif item_type == "video":
                    features["video_carousel"] = True
                    video_items = item.get("items", [])
                    features["video_count"] = len(video_items)
                    features["total_features"] += 1
                    if organic_position is None or rank_absolute < organic_position:
                        features["features_above_organic"].append("video_carousel")

                # Local pack
                elif item_type == "local_pack" or item_type == "map":
                    features["local_pack"] = True
                    features["total_features"] += 1
                    if organic_position is None or rank_absolute < organic_position:
                        features["features_above_organic"].append("local_pack")

                # Knowledge panel/graph
                elif item_type == "knowledge_graph":
                    features["knowledge_panel"] = True
                    features["knowledge_graph"] = True
                    features["total_features"] += 1
                    if organic_position is None or rank_absolute < organic_position:
                        features["features_above_organic"].append("knowledge_panel")

                # AI Overview / AI-generated content
                elif item_type == "ai_overview" or "ai" in item_type.lower():
                    features["ai_overview"] = True
                    features["total_features"] += 1
                    if organic_position is None or rank_absolute < organic_position:
                        features["features_above_organic"].append("ai_overview")

                # Shopping results
                elif item_type == "shopping" or item_type == "google_shopping":
                    features["shopping_results"] = True
                    features["total_features"] += 1
                    if organic_position is None or rank_absolute < organic_position:
                        features["features_above_organic"].append("shopping_results")

                # Image pack
                elif item_type == "images":
                    features["image_pack"] = True
                    features["total_features"] += 1
                    if organic_position is None or rank_absolute < organic_position:
                        features["features_above_organic"].append("image_pack")

                # Top stories / News
                elif item_type == "top_stories" or item_type == "news":
                    features["top_stories"] = True
                    features["total_features"] += 1
                    if organic_position is None or rank_absolute < organic_position:
                        features["features_above_organic"].append("top_stories")

                # Twitter/X results
                elif item_type == "twitter" or "twitter" in item.get("url", "").lower():
                    features["twitter"] = True
                    features["total_features"] += 1
                    if organic_position is None or rank_absolute < organic_position:
                        features["features_above_organic"].append("twitter")

                # Reddit threads
                elif "reddit.com" in item.get("url", "").lower():
                    features["reddit_threads"] = True
                    features["total_features"] += 1

                # Site links (enhanced organic result)
                if item.get("links") and len(item.get("links", [])) > 0:
                    features["site_links"] = True

                # Reviews/ratings
                if item.get("rating"):
                    features["reviews"] = True

            features_data.append(features)

        return features_data

    def extract_position_data(
        self, serp_results: List[Dict[str, Any]], target_domain: str
    ) -> List[Dict[str, Any]]:
        """
        Extract ranking positions and URLs for the target domain

        Args:
            serp_results: List of SERP results from fetch_serp_for_queries
            target_domain: Domain to extract positions for

        Returns:
            List of position data for each keyword
        """
        position_data = []
        target_domain_clean = urlparse(f"https://{target_domain}").netloc.lower()

        for result in serp_results:
            if not result or "serp_data" not in result:
                continue

            keyword = result["keyword"]
            serp_data = result["serp_data"]

            items = serp_data.get("items", [])
            if not items or len(items) == 0:
                continue

            serp_items = items[0].get("items", [])

            # Find target domain in results
            found = False
            keyword_data = {
                "keyword": keyword,
                "ranking": False,
                "position": None,
                "url": None,
                "title": None,
                "description": None,
                "visual_position": None,
                "features_above": 0,
            }

            features_above = 0

            for idx, item in enumerate(serp_items):
                item_type = item.get("type", "")
                rank_absolute = item.get("rank_absolute", idx + 1)

                # Count non-organic features
                if item_type != "organic":
                    if not found:  # Only count features above our result
                        features_above += 1
                    continue

                url = item.get("url", "")
                if not url:
                    continue

                domain = urlparse(url).netloc.lower()

                # Check if this is the target domain
                if domain == target_domain_clean or target_domain_clean in domain:
                    keyword_data["ranking"] = True
                    keyword_data["position"] = rank_absolute
                    keyword_data["url"] = url
                    keyword_data["title"] = item.get("title", "")
                    keyword_data["description"] = item.get("description", "")
                    keyword_data["features_above"] = features_above
                    keyword_data["visual_position"] = rank_absolute + features_above
                    found = True
                    break

            position_data.append(keyword_data)

        return position_data

    def calculate_visual_displacement(
        self, position_data: List[Dict[str, Any]], serp_features: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Calculate visual position displacement due to SERP features

        Args:
            position_data: Position data from extract_position_data
            serp_features: SERP features from extract_serp_features

        Returns:
            List of keywords with displacement analysis
        """
        # Create feature lookup by keyword
        features_map = {f["keyword"]: f for f in serp_features}

        displacement_data = []

        for pos in position_data:
            keyword = pos["keyword"]
            organic_position = pos.get("position")

            if not organic_position or not pos["ranking"]:
                continue

            features = features_map.get(keyword, {})

            # Calculate visual position based on SERP features
            visual_displacement = 0

            # Weight different features by their visual impact
            if features.get("featured_snippet"):
                visual_displacement += 2.0  # Featured snippets are prominent

            if features.get("ai_overview"):
                visual_displacement += 2.5  # AI overviews are very prominent

            if features.get("paa_count"):
                visual_displacement += features["paa_count"] * 0.5

            if features.get("local_pack"):
                visual_displacement += 1.5

            if features.get("video_carousel"):
                visual_displacement += 1.0

            if features.get("shopping_results"):
                visual_displacement += 1.0

            if features.get("image_pack"):
                visual_displacement += 0.5

            if features.get("top_stories"):
                visual_displacement += 1.0

            visual_position = organic_position + visual_displacement

            displacement_data.append(
                {
                    "keyword": keyword,
                    "organic_position": organic_position,
                    "visual_position": round(visual_position, 1),
                    "displacement": round(visual_displacement, 1),
                    "features_causing_displacement": features.get(
                        "features_above_organic", []
                    ),
                    "url": pos["url"],
                    "is_displaced": visual_displacement >= 3.0,  # Significant displacement
                }
            )

        # Sort by displacement descending
        displacement_data.sort(key=lambda x: x["displacement"], reverse=True)

        return displacement_data

    def classify_serp_intent(
        self, serp_features: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Classify search intent based on SERP composition

        Args:
            serp_features: SERP features from extract_serp_features

        Returns:
            List of keywords with intent classification
        """
        intent_data = []

        for features in serp_features:
            keyword = features["keyword"]

            # Initialize intent scores
            informational_score = 0
            commercial_score = 0
            transactional_score = 0
            navigational_score = 0

            # Informational signals
            if features.get("people_also_ask"):
                informational_score += 2
            if features.get("featured_snippet"):
                informational_score += 1.5
            if features.get("knowledge_panel"):
                informational_score += 2
            if features.get("ai_overview"):
                informational_score += 1

            # Commercial signals
            if features.get("shopping_results"):
                commercial_score += 3
            if features.get("reviews"):
                commercial_score += 1.5

            # Transactional signals
            if features.get("shopping_results"):
                transactional_score += 2
            if features.get("local_pack"):
                transactional_score += 1.5

            # Navigational signals
            if features.get("knowledge_panel"):
                navigational_score += 2
            if features.get("site_links"):
                navigational_score += 2

            # Keyword pattern analysis
            keyword_lower = keyword.lower()

            # Informational patterns
            if any(
                word in keyword_lower
                for word in ["how", "what", "why", "when", "guide", "tutorial"]
            ):
                informational_score += 2

            # Commercial patterns
            if any(
                word in keyword_lower
                for word in ["best", "top", "review", "vs", "versus", "compare"]
            ):
                commercial_score += 2

            # Transactional patterns
            if any(
                word in keyword_lower
                for word in [
                    "buy",
                    "price",
                    "cheap",
                    "discount",
                    "deal",
                    "order",
                    "purchase",
                ]
            ):
                transactional_score += 3

            if any(word in keyword_lower for word in ["near me", "near", "in"]):
                transactional_score += 1.5

            # Navigational patterns
            # (Usually brand + product name, harder to detect generically)

            # Determine primary intent
            scores = {
                "informational": informational_score,
                "commercial": commercial_score,
                "transactional": transactional_score,
                "navigational": navigational_score,
            }

            primary_intent = max(scores, key=scores.get)
            confidence = scores[primary_intent] / (sum(scores.values()) or 1)

            intent_data.append(
                {
                    "keyword": keyword,
                    "primary_intent": primary_intent,
                    "confidence": round(confidence, 2),
                    "intent_scores": {k: round(v, 1) for k, v in scores.items()},
                    "serp_signals": {
                        "paa": features.get("people_also_ask", False),
                        "featured_snippet": features.get("featured_snippet", False),
                        "shopping": features.get("shopping_results", False),
                        "local_pack": features.get("local_pack", False),
                        "knowledge_panel": features.get("knowledge_panel", False),
                    },
                }
            )

        return intent_data

    async def fetch_and_analyze_serps(
        self,
        queries: List[str],
        user_domain: str,
        location_code: int = 2840,
        language_code: str = "en",
        use_cache: bool = True,
        report_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Comprehensive SERP analysis pipeline

        Args:
            queries: List of keywords to analyze
            user_domain: The user's domain
            location_code: DataForSEO location code
            language_code: Language code
            use_cache: Whether to use cached results
            report_id: Optional report ID

        Returns:
            Complete SERP analysis including competitors, features, positions, etc.
        """
        # Fetch SERP data
        serp_results = await self.fetch_serp_for_queries(
            queries=queries,
            location_code=location_code,
            language_code=language_code,
            use_cache=use_cache,
            report_id=report_id,
        )

        if not serp_results:
            return {
                "error": "No SERP data retrieved",
                "keywords_attempted": len(queries),
            }

        # Extract all analysis components
        competitors = self.extract_competitor_domains(serp_results, user_domain)
        features = self.extract_serp_features(serp_results)
        positions = self.extract_position_data(serp_results, user_domain)
        displacement = self.calculate_visual_displacement(positions, features)
        intent = self.classify_serp_intent(features)

        # Calculate summary statistics
        total_keywords = len(queries)
        keywords_analyzed = len(serp_results)
        keywords_ranking = sum(1 for p in positions if p["ranking"])
        avg_position = (
            sum(p["position"] for p in positions if p["position"]) / keywords_ranking
            if keywords_ranking > 0
            else None
        )
        avg_visual_position = (
            sum(p["visual_position"] for p in positions if p["visual_position"])
            / keywords_ranking
            if keywords_ranking > 0
            else None
        )

        # Feature prevalence
        feature_counts = {
            "featured_snippet": sum(1 for f in features if f.get("featured_snippet")),
            "people_also_ask": sum(1 for f in features if f.get("people_also_ask")),
            "video_carousel": sum(1 for f in features if f.get("video_carousel")),
            "local_pack": sum(1 for f in features if f.get("local_pack")),
            "knowledge_panel": sum(1 for f in features if f.get("knowledge_panel")),
            "ai_overview": sum(1 for f in features if f.get("ai_overview")),
            "shopping_results": sum(1 for f in features if f.get("shopping_results")),
        }

        return {
            "summary": {
                "total_keywords": total_keywords,
                "keywords_analyzed": keywords_analyzed,
                "keywords_ranking": keywords_ranking,
                "avg_organic_position": round(avg_position, 1) if avg_position else None,
                "avg_visual_position": (
                    round(avg_visual_position, 1) if avg_visual_position else None
                ),
                "feature_counts": feature_counts,
            },
            "competitors": competitors,
            "serp_features": features,
            "position_data": positions,
            "displacement_analysis": displacement,
            "intent_classification": intent,
            "keywords_analyzed": [r["keyword"] for r in serp_results],
        }
