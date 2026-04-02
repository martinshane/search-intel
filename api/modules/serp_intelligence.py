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
            r'\bmeaning\b',
            r'\bdefinition\b',
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
            r'\boptions\b',
            r'\bchoice\b',
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
            r'\bpurchase\b',
            r'\bshop\b',
        ],
        'navigational': [
            r'\blogin\b',
            r'\bsign\s+in\b',
            r'\bcontact\b',
            r'\baddress\b',
            r'\blocation\b',
            r'\bwebsite\b',
            r'\bofficial\b',
        ],
    }
    
    # Position-based CTR curves (baseline, no SERP features)
    BASELINE_CTR = {
        1: 0.28,
        2: 0.15,
        3: 0.11,
        4: 0.08,
        5: 0.06,
        6: 0.05,
        7: 0.04,
        8: 0.03,
        9: 0.025,
        10: 0.02,
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
        user_domain: str,
        location_code: int = 2840,  # United States
        language_code: str = "en",
    ) -> Dict[str, Any]:
        """
        Analyze SERP landscape for Module 3.
        
        Args:
            site_id: Site identifier
            keywords: List of keyword dicts with 'query', 'impressions', 'position'
            gsc_keyword_data: GSC keyword performance data
            user_domain: User's domain for identifying their listings
            location_code: DataForSEO location code
            language_code: Language code
        
        Returns:
            Dict containing SERP landscape analysis
        """
        logger.info(f"Analyzing SERP landscape for {len(keywords)} keywords")
        
        # Fetch SERP data for all keywords
        serp_results = await self._fetch_serp_data(
            keywords,
            site_id,
            location_code,
            language_code
        )
        
        # Analyze SERP features and displacement
        displacement_analysis = self._analyze_serp_displacement(
            serp_results,
            gsc_keyword_data,
            user_domain
        )
        
        # Build competitor map
        competitors = self._analyze_competitors(serp_results, user_domain)
        
        # Classify intent and find mismatches
        intent_analysis = self._analyze_intent_mismatches(
            serp_results,
            gsc_keyword_data
        )
        
        # Estimate click share
        click_share = self._estimate_click_share(
            serp_results,
            gsc_keyword_data,
            user_domain
        )
        
        return {
            "keywords_analyzed": len([r for r in serp_results if r is not None]),
            "serp_feature_displacement": displacement_analysis,
            "competitors": competitors,
            "intent_mismatches": intent_analysis,
            "total_click_share": click_share["current_share"],
            "click_share_opportunity": click_share["opportunity_share"],
            "serp_feature_summary": self._summarize_serp_features(serp_results),
        }
    
    async def _fetch_serp_data(
        self,
        keywords: List[Dict[str, Any]],
        site_id: str,
        location_code: int,
        language_code: str,
    ) -> List[Optional[Dict[str, Any]]]:
        """Fetch SERP data for keywords with caching."""
        results = []
        
        for keyword_data in keywords:
            keyword = keyword_data["query"]
            
            # Check cache first
            cached = await self._get_cached_serp(site_id, keyword)
            if cached:
                results.append(cached)
                continue
            
            # Fetch live data
            try:
                serp_data = await self.client.get_serp_results(
                    keyword=keyword,
                    location_code=location_code,
                    language_code=language_code,
                )
                
                if serp_data:
                    # Enrich with keyword metadata
                    serp_data["keyword_data"] = keyword_data
                    results.append(serp_data)
                    
                    # Cache result
                    await self._cache_serp(site_id, keyword, serp_data)
                else:
                    results.append(None)
                
                # Rate limiting
                await asyncio.sleep(0.1)
                
            except Exception as e:
                logger.error(f"Error fetching SERP for '{keyword}': {e}")
                results.append(None)
        
        return results
    
    async def _get_cached_serp(
        self,
        site_id: str,
        keyword: str
    ) -> Optional[Dict[str, Any]]:
        """Get cached SERP data if recent."""
        query = """
        SELECT serp_data
        FROM serp_cache
        WHERE site_id = $1 AND keyword = $2
        AND cached_at > NOW() - INTERVAL '24 hours'
        """
        
        row = await self.db.fetchrow(query, site_id, keyword)
        return row["serp_data"] if row else None
    
    async def _cache_serp(
        self,
        site_id: str,
        keyword: str,
        serp_data: Dict[str, Any]
    ):
        """Cache SERP data."""
        query = """
        INSERT INTO serp_cache (site_id, keyword, serp_data, cached_at)
        VALUES ($1, $2, $3, NOW())
        ON CONFLICT (site_id, keyword)
        DO UPDATE SET serp_data = $3, cached_at = NOW()
        """
        
        await self.db.execute(query, site_id, keyword, serp_data)
    
    def _analyze_serp_displacement(
        self,
        serp_results: List[Optional[Dict[str, Any]]],
        gsc_data: pd.DataFrame,
        user_domain: str
    ) -> List[Dict[str, Any]]:
        """Analyze SERP feature displacement."""
        displacement = []
        
        for result in serp_results:
            if not result:
                continue
            
            keyword = result.get("keyword_data", {}).get("query")
            if not keyword:
                continue
            
            # Get organic position from GSC
            gsc_position = result["keyword_data"].get("position", 0)
            
            # Calculate visual position
            visual_position = self._calculate_visual_position(result, user_domain)
            
            # Check for significant displacement
            displacement_gap = visual_position - gsc_position
            if displacement_gap > 3:
                features_above = self._identify_features_above(result, user_domain)
                
                # Estimate CTR impact
                baseline_ctr = self._get_baseline_ctr(gsc_position)
                actual_ctr = self._get_baseline_ctr(visual_position)
                ctr_impact = actual_ctr - baseline_ctr
                
                displacement.append({
                    "keyword": keyword,
                    "organic_position": round(gsc_position, 1),
                    "visual_position": round(visual_position, 1),
                    "displacement_gap": round(displacement_gap, 1),
                    "features_above": features_above,
                    "estimated_ctr_impact": round(ctr_impact, 4),
                    "monthly_impressions": result["keyword_data"].get("impressions", 0),
                })
        
        # Sort by impact (impressions * CTR impact)
        displacement.sort(
            key=lambda x: abs(x["estimated_ctr_impact"] * x["monthly_impressions"]),
            reverse=True
        )
        
        return displacement[:50]  # Top 50 most impacted
    
    def _calculate_visual_position(
        self,
        serp_result: Dict[str, Any],
        user_domain: str
    ) -> float:
        """Calculate visual position accounting for SERP features."""
        items = serp_result.get("items", [])
        
        visual_pos = 0
        user_found = False
        
        for item in items:
            item_type = item.get("type", "")
            
            # Add visual weight for this item
            if item_type == "organic":
                visual_pos += 1
                
                # Check if this is the user's listing
                domain = self._extract_domain(item.get("url", ""))
                if domain == user_domain and not user_found:
                    user_found = True
                    return visual_pos
            
            elif item_type == "featured_snippet":
                visual_pos += self.FEATURE_WEIGHTS["featured_snippet"]
            
            elif item_type == "knowledge_panel":
                visual_pos += self.FEATURE_WEIGHTS["knowledge_panel"]
            
            elif item_type == "ai_overview":
                visual_pos += self.FEATURE_WEIGHTS["ai_overview"]
            
            elif item_type == "local_pack":
                visual_pos += self.FEATURE_WEIGHTS["local_pack"]
            
            elif item_type == "video":
                visual_pos += self.FEATURE_WEIGHTS["video_carousel"]
            
            elif item_type == "images":
                visual_pos += self.FEATURE_WEIGHTS["image_pack"]
            
            elif item_type == "shopping":
                visual_pos += self.FEATURE_WEIGHTS["shopping_results"]
            
            elif item_type == "top_stories":
                visual_pos += self.FEATURE_WEIGHTS["top_stories"]
            
            elif item_type == "people_also_ask":
                # Count PAA questions
                questions = item.get("items", [])
                visual_pos += len(questions) * self.FEATURE_WEIGHTS["people_also_ask"]
            
            elif item_type == "twitter":
                visual_pos += self.FEATURE_WEIGHTS["twitter"]
            
            elif item_type == "reddit":
                visual_pos += self.FEATURE_WEIGHTS["reddit_threads"]
        
        return visual_pos if user_found else 0
    
    def _identify_features_above(
        self,
        serp_result: Dict[str, Any],
        user_domain: str
    ) -> List[str]:
        """Identify SERP features appearing above user's listing."""
        items = serp_result.get("items", [])
        features = []
        user_found = False
        
        for item in items:
            if user_found:
                break
            
            item_type = item.get("type", "")
            
            if item_type == "organic":
                domain = self._extract_domain(item.get("url", ""))
                if domain == user_domain:
                    user_found = True
            
            elif item_type == "featured_snippet":
                features.append("featured_snippet")
            
            elif item_type == "knowledge_panel":
                features.append("knowledge_panel")
            
            elif item_type == "ai_overview":
                features.append("ai_overview")
            
            elif item_type == "local_pack":
                features.append("local_pack")
            
            elif item_type == "video":
                features.append("video_carousel")
            
            elif item_type == "people_also_ask":
                questions = item.get("items", [])
                features.append(f"paa_x{len(questions)}")
            
            elif item_type == "shopping":
                features.append("shopping_results")
            
            elif item_type == "top_stories":
                features.append("top_stories")
            
            elif item_type == "images":
                features.append("image_pack")
            
            elif item_type == "twitter":
                features.append("twitter")
            
            elif item_type == "reddit":
                features.append("reddit_threads")
        
        return features
    
    def _analyze_competitors(
        self,
        serp_results: List[Optional[Dict[str, Any]]],
        user_domain: str
    ) -> List[Dict[str, Any]]:
        """Build competitor frequency map."""
        competitor_data = defaultdict(lambda: {
            "keywords_shared": 0,
            "positions": [],
            "domains": set(),
        })
        
        for result in serp_results:
            if not result:
                continue
            
            items = result.get("items", [])
            organic_items = [
                item for item in items
                if item.get("type") == "organic"
            ][:10]  # Top 10 only
            
            for item in organic_items:
                domain = self._extract_domain(item.get("url", ""))
                if domain and domain != user_domain:
                    competitor_data[domain]["keywords_shared"] += 1
                    competitor_data[domain]["positions"].append(
                        item.get("rank_absolute", 100)
                    )
        
        # Build competitor list
        competitors = []
        total_keywords = len([r for r in serp_results if r])
        
        for domain, data in competitor_data.items():
            if data["keywords_shared"] < 3:  # Min threshold
                continue
            
            avg_position = np.mean(data["positions"]) if data["positions"] else 100
            share_pct = (data["keywords_shared"] / total_keywords) * 100
            
            # Determine threat level
            if share_pct > 30 and avg_position < 5:
                threat_level = "high"
            elif share_pct > 15 and avg_position < 8:
                threat_level = "medium"
            else:
                threat_level = "low"
            
            competitors.append({
                "domain": domain,
                "keywords_shared": data["keywords_shared"],
                "share_percentage": round(share_pct, 1),
                "avg_position": round(avg_position, 1),
                "threat_level": threat_level,
            })
        
        # Sort by keywords shared
        competitors.sort(key=lambda x: x["keywords_shared"], reverse=True)
        
        return competitors[:20]  # Top 20 competitors
    
    def _analyze_intent_mismatches(
        self,
        serp_results: List[Optional[Dict[str, Any]]],
        gsc_data: pd.DataFrame
    ) -> List[Dict[str, Any]]:
        """Identify intent classification mismatches."""
        mismatches = []
        
        for result in serp_results:
            if not result:
                continue
            
            keyword = result.get("keyword_data", {}).get("query")
            if not keyword:
                continue
            
            # Classify query intent
            query_intent = self._classify_query_intent(keyword)
            
            # Classify SERP intent based on features
            serp_intent = self._classify_serp_intent(result)
            
            # Check for mismatch
            if query_intent != serp_intent and serp_intent != "mixed":
                mismatches.append({
                    "keyword": keyword,
                    "query_intent": query_intent,
                    "serp_intent": serp_intent,
                    "monthly_impressions": result["keyword_data"].get("impressions", 0),
                    "current_position": result["keyword_data"].get("position", 0),
                })
        
        # Sort by impressions
        mismatches.sort(key=lambda x: x["monthly_impressions"], reverse=True)
        
        return mismatches[:30]
    
    def _classify_query_intent(self, query: str) -> str:
        """Classify query intent based on patterns."""
        query_lower = query.lower()
        
        scores = {intent: 0 for intent in self.INTENT_PATTERNS}
        
        for intent, patterns in self.INTENT_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, query_lower):
                    scores[intent] += 1
        
        if max(scores.values()) == 0:
            return "informational"  # Default
        
        return max(scores, key=scores.get)
    
    def _classify_serp_intent(self, serp_result: Dict[str, Any]) -> str:
        """Classify SERP intent based on features present."""
        items = serp_result.get("items", [])
        
        feature_counts = {
            "informational": 0,
            "commercial": 0,
            "transactional": 0,
            "navigational": 0,
        }
        
        for item in items:
            item_type = item.get("type", "")
            
            if item_type in ["knowledge_panel", "people_also_ask", "ai_overview"]:
                feature_counts["informational"] += 1
            
            elif item_type in ["shopping", "shopping_carousel"]:
                feature_counts["transactional"] += 2
            
            elif item_type in ["video", "top_stories"]:
                feature_counts["informational"] += 0.5
            
            elif item_type == "local_pack":
                feature_counts["transactional"] += 1
        
        # Check organic results titles/descriptions for commercial terms
        organic_items = [i for i in items if i.get("type") == "organic"][:5]
        commercial_terms = ["review", "best", "top", "vs", "comparison", "alternative"]
        
        for item in organic_items:
            title = (item.get("title") or "").lower()
            description = (item.get("description") or "").lower()
            text = f"{title} {description}"
            
            if any(term in text for term in commercial_terms):
                feature_counts["commercial"] += 1
        
        # Determine dominant intent
        if max(feature_counts.values()) == 0:
            return "informational"
        
        max_count = max(feature_counts.values())
        dominant_intents = [k for k, v in feature_counts.items() if v == max_count]
        
        if len(dominant_intents) > 1:
            return "mixed"
        
        return dominant_intents[0]
    
    def _estimate_click_share(
        self,
        serp_results: List[Optional[Dict[str, Any]]],
        gsc_data: pd.DataFrame,
        user_domain: str
    ) -> Dict[str, float]:
        """Estimate click share for user vs. total available."""
        total_available_clicks = 0
        user_clicks = 0
        
        for result in serp_results:
            if not result:
                continue
            
            keyword_data = result.get("keyword_data", {})
            monthly_impressions = keyword_data.get("impressions", 0)
            position = keyword_data.get("position", 100)
            
            # Estimate total clicks available for this keyword
            # Sum CTRs for all positions (approximate)
            total_ctr = sum(self.BASELINE_CTR.get(i, 0.01) for i in range(1, 11))
            keyword_available_clicks = monthly_impressions * total_ctr
            total_available_clicks += keyword_available_clicks
            
            # User's actual clicks (CTR * impressions)
            user_ctr = keyword_data.get("ctr", 0) or self._get_baseline_ctr(position)
            keyword_user_clicks = monthly_impressions * user_ctr
            user_clicks += keyword_user_clicks
        
        current_share = user_clicks / total_available_clicks if total_available_clicks > 0 else 0
        opportunity_share = 1.0 - current_share  # Theoretical maximum
        
        # More realistic opportunity (assuming can reach top 3 positions)
        realistic_opportunity = 0
        for result in serp_results:
            if not result:
                continue
            
            keyword_data = result.get("keyword_data", {})
            monthly_impressions = keyword_data.get("impressions", 0)
            position = keyword_data.get("position", 100)
            
            current_ctr = keyword_data.get("ctr", 0) or self._get_baseline_ctr(position)
            target_ctr = self.BASELINE_CTR.get(3, 0.11)  # Target position 3
            
            if target_ctr > current_ctr:
                gain = (target_ctr - current_ctr) * monthly_impressions
                realistic_opportunity += gain
        
        realistic_share = realistic_opportunity / total_available_clicks if total_available_clicks > 0 else 0
        
        return {
            "current_share": round(current_share, 3),
            "opportunity_share": round(realistic_share, 3),
            "total_available_clicks": round(total_available_clicks, 0),
            "current_clicks": round(user_clicks, 0),
        }
    
    def _summarize_serp_features(
        self,
        serp_results: List[Optional[Dict[str, Any]]]
    ) -> Dict[str, Any]:
        """Summarize SERP feature prevalence."""
        feature_counts = Counter()
        total_serps = len([r for r in serp_results if r])
        
        for result in serp_results:
            if not result:
                continue
            
            items = result.get("items", [])
            seen_features = set()
            
            for item in items:
                item_type = item.get("type", "")
                if item_type and item_type not in seen_features:
                    feature_counts[item_type] += 1
                    seen_features.add(item_type)
        
        summary = {
            feature: {
                "count": count,
                "percentage": round((count / total_serps) * 100, 1) if total_serps > 0 else 0,
            }
            for feature, count in feature_counts.most_common()
        }
        
        return summary
    
    def _get_baseline_ctr(self, position: float) -> float:
        """Get baseline CTR for a position."""
        if position < 1:
            return 0
        
        pos_int = int(round(position))
        if pos_int in self.BASELINE_CTR:
            return self.BASELINE_CTR[pos_int]
        
        # Exponential decay for positions > 10
        if pos_int > 10:
            return 0.02 * (0.8 ** (pos_int - 10))
        
        return 0.02
    
    def _extract_domain(self, url: str) -> str:
        """Extract domain from URL."""
        if not url:
            return ""
        
        # Remove protocol
        url = re.sub(r'^https?://', '', url)
        
        # Remove www
        url = re.sub(r'^www\.', '', url)
        
        # Extract domain (before first /)
        domain = url.split('/')[0]
        
        return domain.lower()
    
    async def track_query_intent_migration(
        self,
        site_id: str,
        keywords: List[Dict[str, Any]],
        historical_serp_data: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """
        Track query intent migration over time for Module 8.
        
        Args:
            site_id: Site identifier
            keywords: Current keyword performance data
            historical_serp_data: Historical SERP data if available
        
        Returns:
            Dict containing intent migration analysis
        """
        logger.info(f"Analyzing intent migration for {len(keywords)} keywords")
        
        migrations = []
        
        if not historical_serp_data:
            # Try to fetch from cache/database
            historical_serp_data = await self._get_historical_serp_data(site_id, keywords)
        
        if not historical_serp_data:
            logger.warning("No historical SERP data available for migration analysis")
            return {
                "migrations_detected": 0,
                "migrations": [],
                "recommendation": "Insufficient historical data. Run analysis again in 30 days.",
            }
        
        # Compare historical vs current SERP intent
        for keyword_data in keywords:
            keyword = keyword_data["query"]
            
            # Find historical data for this keyword
            historical = next(
                (h for h in historical_serp_data if h.get("keyword") == keyword),
                None
            )
            
            if not historical:
                continue
            
            # Get current SERP features
            current_serp = await self._get_cached_serp(site_id, keyword)
            if not current_serp:
                continue
            
            # Classify intents
            historical_intent = historical.get("intent") or self._classify_serp_intent(historical)
            current_intent = self._classify_serp_intent(current_serp)
            
            # Detect migration
            if historical_intent != current_intent and current_intent != "mixed":
                # Calculate impact
                position_change = keyword_data.get("position", 100) - historical.get("position", 100)
                click_change = keyword_data.get("clicks", 0) - historical.get("clicks", 0)
                
                migrations.append({
                    "keyword": keyword,
                    "previous_intent": historical_intent,
                    "current_intent": current_intent,
                    "migration_date": historical.get("date", "unknown"),
                    "position_change": round(position_change, 1),
                    "click_change": click_change,
                    "current_position": keyword_data.get("position", 0),
                    "monthly_impressions": keyword_data.get("impressions", 0),
                    "recommendation": self._get_migration_recommendation(
                        historical_intent,
                        current_intent,
                        position_change
                    ),
                })
        
        # Sort by impact (impressions)
        migrations.sort(key=lambda x: x["monthly_impressions"], reverse=True)
        
        return {
            "migrations_detected": len(migrations),
            "migrations": migrations[:30],  # Top 30
            "summary": self._summarize_migrations(migrations),
        }
    
    async def _get_historical_serp_data(
        self,
        site_id: str,
        keywords: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Retrieve historical SERP data from database."""
        query = """
        SELECT keyword, serp_data, cached_at
        FROM serp_cache
        WHERE site_id = $1
        AND keyword = ANY($2)
        AND cached_at < NOW() - INTERVAL '30 days'
        ORDER BY cached_at DESC
        """
        
        keyword_list = [k["query"] for k in keywords]
        rows = await self.db.fetch(query, site_id, keyword_list)
        
        historical_data = []
        for row in rows:
            data = row["serp_data"]
            data["date"] = row["cached_at"]
            historical_data.append(data)
        
        return historical_data
    
    def _get_migration_recommendation(
        self,
        previous_intent: str,
        current_intent: str,
        position_change: float
    ) -> str:
        """Generate recommendation for intent migration."""
        if previous_intent == "informational" and current_intent == "commercial":
            return "Add comparison tables, product reviews, and CTAs"
        
        elif previous_intent == "commercial" and current_intent == "transactional":
            return "Add pricing, buy buttons, and conversion-focused content"
        
        elif previous_intent == "transactional" and current_intent == "informational":
            return "Add educational content, guides, and remove hard sells"
        
        elif previous_intent == "informational" and current_intent == "transactional":
            return "Restructure as product/service page with clear conversion path"
        
        elif position_change > 5:
            return f"Intent shift caused ranking drop. Realign content with {current_intent} intent."
        
        else:
            return f"Adapt content to match new {current_intent} intent signals"
    
    def _summarize_migrations(self, migrations: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Summarize migration patterns."""
        if not migrations:
            return {}
        
        # Count migration types
        migration_types = Counter([
            f"{m['previous_intent']} → {m['current_intent']}"
            for m in migrations
        ])
        
        # Calculate impact
        total_click_loss = sum(m["click_change"] for m in migrations if m["click_change"] < 0)
        total_click_gain = sum(m["click_change"] for m in migrations if m["click_change"] > 0)
        
        return {
            "total_migrations": len(migrations),
            "migration_patterns": dict(migration_types.most_common(5)),
            "net_click_impact": total_click_gain + total_click_loss,
            "keywords_needing_realignment": len([
                m for m in migrations if m["position_change"] > 3
            ]),
        }
    
    async def analyze_competitive_intelligence(
        self,
        site_id: str,
        competitors: List[str],
        keywords: List[Dict[str, Any]],
        user_domain: str,
    ) -> Dict[str, Any]:
        """
        Competitive intelligence analysis for Module 11.
        
        Args:
            site_id: Site identifier
            competitors: List of competitor domains to analyze
            keywords: User's keyword portfolio
            user_domain: User's domain
        
        Returns:
            Dict containing competitive intelligence
        """
        logger.info(f"Analyzing {len(competitors)} competitors")
        
        # Fetch SERP data (should already be cached from Module 3)
        serp_results = []
        for keyword_data in keywords:
            cached = await self._get_cached_serp(site_id, keyword_data["query"])
            if cached:
                cached["keyword_data"] = keyword_data
                serp_results.append(cached)
        
        # Analyze each competitor
        competitor_profiles = []
        for competitor_domain in competitors[:10]:  # Top 10 competitors
            profile = self._build_competitor_profile(
                competitor_domain,
                serp_results,
                user_domain
            )
            competitor_profiles.append(profile)
        
        # Find content gaps
        content_gaps = self._identify_content_gaps(
            serp_results,
            user_domain,
            competitors
        )
        
        # Opportunity analysis
        opportunities = self._find_competitive_opportunities(
            serp_results,
            user_domain,
            competitor_profiles
        )
        
        return {
            "competitors_analyzed": len(competitor_profiles),
            "competitor_profiles": competitor_profiles,
            "content_gaps": content_gaps,
            "opportunities": opportunities,
            "market_share": self._calculate_market_share(
                serp_results,
                user_domain,
                competitors
            ),
        }
    
    def _build_competitor_profile(
        self,
        competitor_domain: str,
        serp_results: List[Dict[str, Any]],
        user_domain: str
    ) -> Dict[str, Any]:
        """Build detailed profile for a competitor."""
        profile = {
            "domain": competitor_domain,
            "keywords_ranking": 0,
            "avg_position": 0,
            "positions": [],
            "content_types": Counter(),
            "serp_features_owned": Counter(),
            "head_to_head_wins": 0,
            "head_to_head_losses": 0,
        }
        
        positions = []
        user_positions = []
        
        for result in serp_results:
            items = result.get("items", [])
            
            competitor_position = None
            user_position = None
            
            for item in items:
                if item.get("type") != "organic":
                    continue
                
                domain = self._extract_domain(item.get("url", ""))
                position = item.get("rank_absolute", 100)
                
                if domain == competitor_domain:
                    competitor_position = position
                    profile["keywords_ranking"] += 1
                    positions.append(position)
                    
                    # Classify content type from URL
                    url = item.get("url", "")
                    content_type = self._classify_content_type(url)
                    profile["content_types"][content_type] += 1
                
                elif domain == user_domain:
                    user_position = position
            
            # Check for SERP feature ownership
            for item in items:
                if item.get("type") == "featured_snippet":
                    domain = self._extract_domain(item.get("url", ""))
                    if domain == competitor_domain:
                        profile["serp_features_owned"]["featured_snippet"] += 1
            
            # Head-to-head comparison
            if competitor_position and user_position:
                if competitor_position < user_position:
                    profile["head_to_head_wins"] += 1
                else:
                    profile["head_to_head_losses"] += 1
                
                user_positions.append(user_position)
        
        if positions:
            profile["avg_position"] = round(np.mean(positions), 1)
            profile["median_position"] = round(np.median(positions), 1)
        
        # Calculate threat score
        threat_score = 0
        if profile["keywords_ranking"] > 0:
            keyword_overlap = profile["keywords_ranking"] / len(serp_results)
            position_advantage = (
                (np.mean(user_positions) - profile["avg_position"])
                if user_positions and positions else 0
            )
            
            threat_score = (keyword_overlap * 50) + (position_advantage * 10)
            threat_score = max(0, min(100, threat_score))
        
        profile["threat_score"] = round(threat_score, 1)
        profile["content_types"] = dict(profile["content_types"].most_common(3))
        profile["serp_features_owned"] = dict(profile["serp_features_owned"])
        
        return profile
    
    def _classify_content_type(self, url: str) -> str:
        """Classify content type from URL patterns."""
        url_lower = url.lower()
        
        if "/blog/" in url_lower or "/article/" in url_lower:
            return "blog"
        elif "/product/" in url_lower or "/item/" in url_lower:
            return "product"
        elif "/category/" in url_lower or "/collection/" in url_lower:
            return "category"
        elif "/guide/" in url_lower or "/tutorial/" in url_lower:
            return "guide"
        elif "/review/" in url_lower:
            return "review"
        else:
            return "other"
    
    def _identify_content_gaps(
        self,
        serp_results: List[Dict[str, Any]],
        user_domain: str,
        competitors: List[str]
    ) -> List[Dict[str, Any]]:
        """Identify keywords where user is not ranking but competitors are."""
        gaps = []
        
        for result in serp_results:
            items = result.get("items", [])
            keyword = result.get("keyword_data", {}).get("query")
            
            user_ranking = False
            competitor_ranking = []
            
            for item in items:
                if item.get("type") != "organic":
                    continue
                
                domain = self._extract_domain(item.get("url", ""))
                
                if domain == user_domain:
                    user_ranking = True
                
                elif domain in competitors:
                    competitor_ranking.append({
                        "domain": domain,
                        "position": item.get("rank_absolute", 100),
                        "url": item.get("url"),
                    })
            
            # Gap if competitors rank but user doesn't (in top 20)
            if competitor_ranking and not user_ranking:
                gaps.append({
                    "keyword": keyword,
                    "monthly_impressions": result.get("keyword_data", {}).get("impressions", 0),
                    "competitors_ranking": len(competitor_ranking),
                    "best_competitor_position": min(
                        c["position"] for c in competitor_ranking
                    ),
                    "competitor_urls": competitor_ranking[:3],
                })
        
        # Sort by opportunity (impressions)
        gaps.sort(key=lambda x: x["monthly_impressions"], reverse=True)
        
        return gaps[:30]
    
    def _find_competitive_opportunities(
        self,
        serp_results: List[Dict[str, Any]],
        user_domain: str,
        competitor_profiles: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Find opportunities to outrank competitors."""
        opportunities = []
        
        for result in serp_results:
            items = result.get("items", [])
            keyword = result.get("keyword_data", {}).get("query")
            user_position = result.get("keyword_data", {}).get("position", 100)
            
            if user_position > 20:  # Only consider if user is ranking
                continue
            
            # Find competitors ranking above user
            competitors_above = []
            for item in items:
                if item.get("type") != "organic":
                    continue
                
                domain = self._extract_domain(item.get("url", ""))
                position = item.get("rank_absolute", 100)
                
                if position < user_position and domain != user_domain:
                    # Check if this is a tracked competitor
                    comp_profile = next(
                        (c for c in competitor_profiles if c["domain"] == domain),
                        None
                    )
                    
                    if comp_profile:
                        competitors_above.append({
                            "domain": domain,
                            "position": position,
                            "threat_score": comp_profile.get("threat_score", 0),
                        })
            
            if competitors_above:
                # Calculate opportunity score
                position_gap = min(c["position"] for c in competitors_above) - user_position
                impressions = result.get("keyword_data", {}).get("impressions", 0)
                
                # Estimate click gain
                current_ctr = self._get_baseline_ctr(user_position)
                target_ctr = self._get_baseline_ctr(
                    min(c["position"] for c in competitors_above)
                )
                estimated_click_gain = impressions * (target_ctr - current_ctr)
                
                opportunities.append({
                    "keyword": keyword,
                    "current_position": round(user_position, 1),
                    "target_position": min(c["position"] for c in competitors_above),
                    "competitors_to_outrank": len(competitors_above),
                    "monthly_impressions": impressions,
                    "estimated_monthly_click_gain": round(estimated_click_gain, 0),
                    "difficulty": "low" if position_gap < 3 else "medium" if position_gap < 7 else "high",
                })
        
        # Sort by estimated click gain
        opportunities.sort(key=lambda x: x["estimated_monthly_click_gain"], reverse=True)
        
        return opportunities[:30]
    
    def _calculate_market_share(
        self,
        serp_results: List[Dict[str, Any]],
        user_domain: str,
        competitors: List[str]
    ) -> Dict[str, float]:
        """Calculate visibility market share."""
        user_visibility = 0
        competitor_visibility = 0
        total_visibility = 0
        
        for result in serp_results:
            items = result.get("items", [])
            
            for item in items:
                if item.get("type") != "organic":
                    continue
                
                domain = self._extract_domain(item.get("url", ""))
                position = item.get("rank_absolute", 100)
                
                # Visibility = 1/position
                visibility = 1.0 / position if position > 0 else 0
                
                total_visibility += visibility
                
                if domain == user_domain:
                    user_visibility += visibility
                elif domain in competitors:
                    competitor_visibility += visibility
        
        return {
            "user_share": round(user_visibility / total_visibility, 3) if total_visibility > 0 else 0,
            "competitor_share": round(competitor_visibility / total_visibility, 3) if total_visibility > 0 else 0,
            "other_share": round(1 - (user_visibility + competitor_visibility) / total_visibility, 3) if total_visibility > 0 else 0,
        }

