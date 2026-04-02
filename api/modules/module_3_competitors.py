"""
Module 3: Competitor Analysis
Identifies competing domains for the site's top keywords, calculates overlap scores,
visibility metrics, and generates strategic insights about competitive landscape.
"""

import asyncio
from typing import Dict, List, Any, Optional, Tuple
import pandas as pd
import numpy as np
from collections import defaultdict, Counter
from datetime import datetime, timedelta
import json
from dataclasses import dataclass, asdict
from enum import Enum

from ..utils.logger import get_logger
from ..services.dataforseo_service import DataForSEOService
from ..services.gsc_service import GSCService
from ..services.supabase_service import SupabaseService

logger = get_logger(__name__)


class ThreatLevel(Enum):
    """Competitor threat level classification"""
    CRITICAL = "critical"  # >40% keyword overlap, better avg position
    HIGH = "high"  # 20-40% overlap or similar position with growth trend
    MEDIUM = "medium"  # 10-20% overlap
    LOW = "low"  # <10% overlap
    EMERGING = "emerging"  # New competitor, rapidly gaining visibility


@dataclass
class CompetitorProfile:
    """Individual competitor profile"""
    domain: str
    keywords_shared: int
    keyword_overlap_pct: float
    avg_position: float
    median_position: float
    position_volatility: float
    keywords_they_win: int  # They rank better
    keywords_we_win: int  # We rank better
    keywords_contested: int  # Within 3 positions
    threat_level: str
    visibility_score: float  # 0-100 score based on weighted position
    estimated_traffic_overlap: int
    top_competing_keywords: List[Dict[str, Any]]
    serp_features_they_own: Dict[str, int]
    strategic_gap: str  # What they do better
    
    
@dataclass
class CompetitiveGap:
    """Opportunity where competitor ranks but we don't"""
    keyword: str
    competitor_domain: str
    competitor_position: int
    our_position: Optional[int]
    search_volume: int
    intent: str
    difficulty: str
    opportunity_score: float
    recommended_action: str


@dataclass
class CompetitiveLandscape:
    """Overall competitive landscape analysis"""
    total_keywords_analyzed: int
    unique_competitors: int
    avg_competitors_per_keyword: float
    market_concentration: float  # 0-1, higher = few players dominate
    our_market_position: int  # Rank among competitors by visibility
    our_visibility_share: float  # % of total visibility we capture
    primary_competitors: List[CompetitorProfile]
    emerging_threats: List[CompetitorProfile]
    competitive_gaps: List[CompetitiveGap]
    head_to_head_matrix: Dict[str, Dict[str, int]]  # domain -> wins/losses
    strategic_insights: List[str]


class CompetitorAnalyzer:
    """Handles all competitor analysis logic"""
    
    def __init__(
        self,
        dataforseo_service: DataForSEOService,
        gsc_service: GSCService,
        supabase_service: SupabaseService
    ):
        self.dataforseo = dataforseo_service
        self.gsc = gsc_service
        self.supabase = supabase_service
        
    async def analyze(
        self,
        report_id: str,
        gsc_data: Dict[str, Any],
        serp_data: List[Dict[str, Any]],
        site_domain: str
    ) -> Dict[str, Any]:
        """
        Main entry point for competitor analysis.
        
        Args:
            report_id: Unique report identifier
            gsc_data: GSC performance data with keywords
            serp_data: Live SERP data from Module 2 or fresh pull
            site_domain: User's domain (normalized)
            
        Returns:
            Structured competitor analysis results
        """
        logger.info(f"Starting competitor analysis for report {report_id}")
        
        try:
            # Step 1: Extract competitors from SERP data
            logger.info("Extracting competitors from SERP data")
            competitors_raw = self._extract_competitors_from_serps(
                serp_data, site_domain
            )
            
            # Step 2: Calculate competitor metrics
            logger.info(f"Analyzing {len(competitors_raw)} unique competitors")
            competitor_profiles = self._build_competitor_profiles(
                competitors_raw, gsc_data, serp_data, site_domain
            )
            
            # Step 3: Identify competitive gaps
            logger.info("Identifying competitive gaps")
            competitive_gaps = self._identify_competitive_gaps(
                competitors_raw, gsc_data, serp_data, site_domain
            )
            
            # Step 4: Calculate market metrics
            logger.info("Calculating market position metrics")
            market_metrics = self._calculate_market_metrics(
                competitor_profiles, site_domain, gsc_data
            )
            
            # Step 5: Build head-to-head comparison matrix
            logger.info("Building head-to-head comparison matrix")
            h2h_matrix = self._build_head_to_head_matrix(
                competitors_raw, site_domain
            )
            
            # Step 6: Generate strategic insights
            logger.info("Generating strategic insights")
            strategic_insights = self._generate_strategic_insights(
                competitor_profiles, competitive_gaps, market_metrics
            )
            
            # Step 7: Classify emerging threats
            emerging_threats = [
                cp for cp in competitor_profiles 
                if cp.threat_level == ThreatLevel.EMERGING.value
            ]
            
            # Build final landscape object
            landscape = CompetitiveLandscape(
                total_keywords_analyzed=len(serp_data),
                unique_competitors=len(competitor_profiles),
                avg_competitors_per_keyword=market_metrics["avg_competitors_per_keyword"],
                market_concentration=market_metrics["market_concentration"],
                our_market_position=market_metrics["our_market_position"],
                our_visibility_share=market_metrics["our_visibility_share"],
                primary_competitors=competitor_profiles[:10],  # Top 10
                emerging_threats=emerging_threats,
                competitive_gaps=competitive_gaps[:50],  # Top 50 opportunities
                head_to_head_matrix=h2h_matrix,
                strategic_insights=strategic_insights
            )
            
            result = asdict(landscape)
            
            # Cache results
            await self.supabase.cache_module_result(
                report_id, "module_3_competitors", result
            )
            
            logger.info(f"Competitor analysis complete. Found {len(competitor_profiles)} competitors")
            return result
            
        except Exception as e:
            logger.error(f"Error in competitor analysis: {str(e)}", exc_info=True)
            raise
            
    def _extract_competitors_from_serps(
        self,
        serp_data: List[Dict[str, Any]],
        site_domain: str
    ) -> Dict[str, Dict[str, Any]]:
        """
        Extract all competing domains from SERP data.
        
        Returns:
            Dict mapping competitor domain to list of keyword appearances
        """
        competitors = defaultdict(list)
        
        for serp in serp_data:
            keyword = serp.get("keyword", "")
            search_volume = serp.get("search_volume", 0)
            organic_results = serp.get("organic_results", [])
            serp_features = serp.get("serp_features", {})
            
            for result in organic_results:
                domain = self._extract_domain(result.get("url", ""))
                if not domain or domain == site_domain:
                    continue
                    
                competitors[domain].append({
                    "keyword": keyword,
                    "position": result.get("position", 100),
                    "url": result.get("url", ""),
                    "title": result.get("title", ""),
                    "search_volume": search_volume,
                    "serp_features": serp_features
                })
                
        return dict(competitors)
        
    def _build_competitor_profiles(
        self,
        competitors_raw: Dict[str, List[Dict[str, Any]]],
        gsc_data: Dict[str, Any],
        serp_data: List[Dict[str, Any]],
        site_domain: str
    ) -> List[CompetitorProfile]:
        """Build detailed profiles for each competitor"""
        profiles = []
        total_keywords = len(serp_data)
        
        # Get our positions for comparison
        our_positions = self._get_our_positions(gsc_data, serp_data)
        
        for domain, appearances in competitors_raw.items():
            if len(appearances) < 2:  # Skip one-off competitors
                continue
                
            keywords_shared = len(appearances)
            overlap_pct = (keywords_shared / total_keywords) * 100
            
            positions = [a["position"] for a in appearances]
            avg_position = np.mean(positions)
            median_position = np.median(positions)
            position_volatility = np.std(positions)
            
            # Calculate win/loss record
            wins, losses, contested = self._calculate_win_loss_record(
                appearances, our_positions
            )
            
            # Calculate visibility score (weighted by position and volume)
            visibility_score = self._calculate_visibility_score(appearances)
            
            # Estimate traffic overlap
            traffic_overlap = self._estimate_traffic_overlap(
                appearances, our_positions
            )
            
            # Get top competing keywords
            top_keywords = self._get_top_competing_keywords(
                appearances, our_positions
            )
            
            # Analyze SERP features they own
            serp_features_owned = self._analyze_serp_features(appearances)
            
            # Determine threat level
            threat_level = self._determine_threat_level(
                overlap_pct, avg_position, wins, losses, visibility_score
            )
            
            # Identify strategic gap
            strategic_gap = self._identify_strategic_gap(
                appearances, serp_features_owned, avg_position
            )
            
            profile = CompetitorProfile(
                domain=domain,
                keywords_shared=keywords_shared,
                keyword_overlap_pct=round(overlap_pct, 2),
                avg_position=round(avg_position, 2),
                median_position=round(median_position, 2),
                position_volatility=round(position_volatility, 2),
                keywords_they_win=wins,
                keywords_we_win=losses,
                keywords_contested=contested,
                threat_level=threat_level.value,
                visibility_score=round(visibility_score, 2),
                estimated_traffic_overlap=traffic_overlap,
                top_competing_keywords=top_keywords,
                serp_features_they_own=serp_features_owned,
                strategic_gap=strategic_gap
            )
            
            profiles.append(profile)
            
        # Sort by threat level and visibility
        threat_order = {
            ThreatLevel.CRITICAL.value: 0,
            ThreatLevel.HIGH.value: 1,
            ThreatLevel.EMERGING.value: 2,
            ThreatLevel.MEDIUM.value: 3,
            ThreatLevel.LOW.value: 4
        }
        
        profiles.sort(
            key=lambda p: (threat_order.get(p.threat_level, 99), -p.visibility_score)
        )
        
        return profiles
        
    def _get_our_positions(
        self,
        gsc_data: Dict[str, Any],
        serp_data: List[Dict[str, Any]]
    ) -> Dict[str, float]:
        """Extract our positions for keywords from GSC data"""
        our_positions = {}
        
        # Get from GSC query data
        if "queries" in gsc_data:
            for query_data in gsc_data["queries"]:
                query = query_data.get("query", "").lower()
                position = query_data.get("position", 100)
                our_positions[query] = position
                
        return our_positions
        
    def _calculate_win_loss_record(
        self,
        appearances: List[Dict[str, Any]],
        our_positions: Dict[str, float]
    ) -> Tuple[int, int, int]:
        """
        Calculate win/loss record against this competitor.
        
        Returns:
            (they_win, we_win, contested)
        """
        they_win = 0
        we_win = 0
        contested = 0
        
        for appearance in appearances:
            keyword = appearance["keyword"].lower()
            their_position = appearance["position"]
            our_position = our_positions.get(keyword, 100)
            
            diff = our_position - their_position
            
            if abs(diff) <= 3:
                contested += 1
            elif diff > 3:
                they_win += 1
            else:
                we_win += 1
                
        return they_win, we_win, contested
        
    def _calculate_visibility_score(
        self,
        appearances: List[Dict[str, Any]]
    ) -> float:
        """
        Calculate visibility score based on positions and search volume.
        Uses exponential decay: position 1 = 100%, position 10 = ~20%, position 20 = ~5%
        """
        total_weighted_visibility = 0
        total_volume = 0
        
        for appearance in appearances:
            position = appearance["position"]
            volume = appearance.get("search_volume", 100)  # Default to 100 if missing
            
            # Exponential decay visibility curve
            visibility_pct = 100 * np.exp(-0.15 * (position - 1))
            weighted_visibility = visibility_pct * volume
            
            total_weighted_visibility += weighted_visibility
            total_volume += volume
            
        if total_volume == 0:
            return 0
            
        return (total_weighted_visibility / total_volume)
        
    def _estimate_traffic_overlap(
        self,
        appearances: List[Dict[str, Any]],
        our_positions: Dict[str, float]
    ) -> int:
        """Estimate monthly traffic overlap with this competitor"""
        estimated_overlap = 0
        
        for appearance in appearances:
            keyword = appearance["keyword"].lower()
            their_position = appearance["position"]
            our_position = our_positions.get(keyword, 100)
            volume = appearance.get("search_volume", 0)
            
            # Only count if we both rank on page 1-2
            if their_position <= 20 and our_position <= 20:
                # Use CTR curve to estimate clicks at each position
                their_ctr = self._position_to_ctr(their_position)
                our_ctr = self._position_to_ctr(our_position)
                
                their_clicks = volume * their_ctr
                our_clicks = volume * our_ctr
                
                # Overlap is the minimum of the two
                estimated_overlap += min(their_clicks, our_clicks)
                
        return int(estimated_overlap)
        
    def _position_to_ctr(self, position: int) -> float:
        """Convert position to estimated CTR"""
        # Advanced CTR curve based on industry data
        ctr_map = {
            1: 0.31, 2: 0.15, 3: 0.10, 4: 0.07, 5: 0.05,
            6: 0.04, 7: 0.03, 8: 0.025, 9: 0.02, 10: 0.015
        }
        
        if position in ctr_map:
            return ctr_map[position]
        elif position <= 20:
            return 0.01 * np.exp(-0.1 * (position - 10))
        else:
            return 0.001
            
    def _get_top_competing_keywords(
        self,
        appearances: List[Dict[str, Any]],
        our_positions: Dict[str, float],
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Get top keywords where we compete with this competitor"""
        competing_keywords = []
        
        for appearance in appearances:
            keyword = appearance["keyword"].lower()
            their_position = appearance["position"]
            our_position = our_positions.get(keyword, 100)
            volume = appearance.get("search_volume", 0)
            
            # Calculate competition intensity
            if our_position <= 20 and their_position <= 20:
                position_gap = abs(our_position - their_position)
                intensity = volume * (21 - min(their_position, our_position)) / (position_gap + 1)
                
                competing_keywords.append({
                    "keyword": appearance["keyword"],
                    "our_position": round(our_position, 1),
                    "their_position": their_position,
                    "search_volume": volume,
                    "gap": round(position_gap, 1),
                    "intensity": round(intensity, 2)
                })
                
        # Sort by intensity
        competing_keywords.sort(key=lambda x: x["intensity"], reverse=True)
        return competing_keywords[:limit]
        
    def _analyze_serp_features(
        self,
        appearances: List[Dict[str, Any]]
    ) -> Dict[str, int]:
        """Count SERP features owned by this competitor"""
        feature_counts = Counter()
        
        for appearance in appearances:
            serp_features = appearance.get("serp_features", {})
            
            # Check if this competitor owns any featured positions
            if isinstance(serp_features, dict):
                if serp_features.get("featured_snippet_domain") == self._extract_domain(appearance["url"]):
                    feature_counts["featured_snippet"] += 1
                    
                # Could add more feature tracking here
                # (people_also_ask, video_carousel, etc.)
                
        return dict(feature_counts)
        
    def _determine_threat_level(
        self,
        overlap_pct: float,
        avg_position: float,
        wins: int,
        losses: int,
        visibility_score: float
    ) -> ThreatLevel:
        """Determine threat level based on multiple factors"""
        
        # Critical: High overlap + they're beating us
        if overlap_pct >= 40 and wins > losses and avg_position < 8:
            return ThreatLevel.CRITICAL
            
        # High: Significant overlap + competitive positioning
        if overlap_pct >= 20 and (wins >= losses or avg_position < 10):
            return ThreatLevel.HIGH
            
        # Emerging: Lower overlap but very high visibility (growing fast)
        if overlap_pct >= 10 and visibility_score > 70 and avg_position < 6:
            return ThreatLevel.EMERGING
            
        # Medium: Moderate overlap
        if overlap_pct >= 10:
            return ThreatLevel.MEDIUM
            
        # Low: Minimal overlap
        return ThreatLevel.LOW
        
    def _identify_strategic_gap(
        self,
        appearances: List[Dict[str, Any]],
        serp_features: Dict[str, int],
        avg_position: float
    ) -> str:
        """Identify what this competitor does better than us"""
        
        if serp_features.get("featured_snippet", 0) > 0:
            return "Owns featured snippets - better structured content"
            
        if avg_position < 5:
            return "Consistently ranks in top 5 - stronger authority"
            
        if len(appearances) > 50:
            return "Broader keyword coverage - more comprehensive content"
            
        return "Competitive positioning in shared keyword space"
        
    def _identify_competitive_gaps(
        self,
        competitors_raw: Dict[str, List[Dict[str, Any]]],
        gsc_data: Dict[str, Any],
        serp_data: List[Dict[str, Any]],
        site_domain: str
    ) -> List[CompetitiveGap]:
        """
        Identify keywords where competitors rank but we don't (or rank poorly).
        These are opportunities to target.
        """
        gaps = []
        our_positions = self._get_our_positions(gsc_data, serp_data)
        
        # Track which keywords have been processed
        keyword_tracker = defaultdict(list)
        
        for domain, appearances in competitors_raw.items():
            for appearance in appearances:
                keyword = appearance["keyword"].lower()
                their_position = appearance["position"]
                volume = appearance.get("search_volume", 0)
                
                # Only consider if they rank well and it's valuable
                if their_position > 10 or volume < 50:
                    continue
                    
                our_position = our_positions.get(keyword)
                
                # Gap exists if we don't rank or rank poorly
                if our_position is None or our_position > their_position + 10:
                    keyword_tracker[keyword].append({
                        "domain": domain,
                        "position": their_position,
                        "volume": volume
                    })
                    
        # Build gap objects for keywords where multiple competitors rank
        for keyword, competitor_data in keyword_tracker.items():
            if len(competitor_data) < 2:  # Only care if 2+ competitors rank
                continue
                
            # Find best ranking competitor
            best_competitor = min(competitor_data, key=lambda x: x["position"])
            
            our_position = our_positions.get(keyword)
            volume = best_competitor["volume"]
            
            # Classify intent (simple heuristic)
            intent = self._classify_keyword_intent(keyword)
            
            # Estimate difficulty (more competitors = harder)
            difficulty = self._estimate_difficulty(len(competitor_data), best_competitor["position"])
            
            # Calculate opportunity score
            opportunity_score = self._calculate_opportunity_score(
                volume, best_competitor["position"], len(competitor_data), our_position
            )
            
            # Recommend action
            action = self._recommend_gap_action(
                our_position, best_competitor["position"], intent
            )
            
            gap = CompetitiveGap(
                keyword=keyword,
                competitor_domain=best_competitor["domain"],
                competitor_position=best_competitor["position"],
                our_position=our_position,
                search_volume=volume,
                intent=intent,
                difficulty=difficulty,
                opportunity_score=round(opportunity_score, 2),
                recommended_action=action
            )
            
            gaps.append(gap)
            
        # Sort by opportunity score
        gaps.sort(key=lambda g: g.opportunity_score, reverse=True)
        return gaps
        
    def _classify_keyword_intent(self, keyword: str) -> str:
        """Simple intent classification based on keyword patterns"""
        keyword_lower = keyword.lower()
        
        if any(word in keyword_lower for word in ["how to", "what is", "why", "guide", "tutorial"]):
            return "informational"
        elif any(word in keyword_lower for word in ["best", "top", "review", "vs", "comparison"]):
            return "commercial"
        elif any(word in keyword_lower for word in ["buy", "price", "pricing", "cost", "cheap", "discount"]):
            return "transactional"
        elif any(word in keyword_lower for word in ["near me", "in", "location"]):
            return "local"
        else:
            return "informational"
            
    def _estimate_difficulty(self, num_competitors: int, best_position: int) -> str:
        """Estimate keyword difficulty"""
        if num_competitors >= 5 and best_position <= 3:
            return "high"
        elif num_competitors >= 3 or best_position <= 5:
            return "medium"
        else:
            return "low"
            
    def _calculate_opportunity_score(
        self,
        volume: int,
        competitor_position: int,
        num_competitors: int,
        our_position: Optional[float]
    ) -> float:
        """
        Calculate opportunity score for a competitive gap.
        Higher score = better opportunity.
        """
        # Base score from volume and competitor position
        base_score = volume * (11 - competitor_position)
        
        # Penalty for high competition
        competition_factor = 1 / (1 + 0.1 * num_competitors)
        
        # Bonus if we already rank (easier to improve)
        ranking_bonus = 1.5 if our_position and our_position <= 50 else 1.0
        
        return base_score * competition_factor * ranking_bonus
        
    def _recommend_gap_action(
        self,
        our_position: Optional[float],
        competitor_position: int,
        intent: str
    ) -> str:
        """Recommend action to close a competitive gap"""
        
        if our_position is None:
            if intent == "informational":
                return "Create comprehensive guide/tutorial content"
            elif intent == "commercial":
                return "Create comparison/review content"
            elif intent == "transactional":
                return "Create product/service landing page"
            else:
                return "Create targeted content page"
        elif our_position > 20:
            return "Expand existing content + build internal links"
        else:
            return "Optimize existing page - improve depth and structure"
            
    def _calculate_market_metrics(
        self,
        competitor_profiles: List[CompetitorProfile],
        site_domain: str,
        gsc_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Calculate overall market position metrics"""
        
        if not competitor_profiles:
            return {
                "avg_competitors_per_keyword": 0,
                "market_concentration": 0,
                "our_market_position": 1,
                "our_visibility_share": 100
            }
            
        # Calculate average competitors per keyword
        total_keywords = competitor_profiles[0].keywords_shared if competitor_profiles else 0
        avg_competitors = np.mean([cp.keywords_shared for cp in competitor_profiles]) if competitor_profiles else 0
        
        # Calculate market concentration (HHI-style)
        total_visibility = sum(cp.visibility_score for cp in competitor_profiles)
        if total_visibility > 0:
            concentration_scores = [(cp.visibility_score / total_visibility) ** 2 for cp in competitor_profiles]
            market_concentration = sum(concentration_scores)
        else:
            market_concentration = 0
            
        # Calculate our visibility
        our_visibility = self._calculate_our_visibility(gsc_data)
        
        # Determine our market position (rank among all players)
        all_visibility_scores = [cp.visibility_score for cp in competitor_profiles] + [our_visibility]
        all_visibility_scores.sort(reverse=True)
        our_position = all_visibility_scores.index(our_visibility) + 1
        
        # Calculate our visibility share
        total_visibility_with_us = total_visibility + our_visibility
        our_share = (our_visibility / total_visibility_with_us * 100) if total_visibility_with_us > 0 else 0
        
        return {
            "avg_competitors_per_keyword": round(avg_competitors, 2),
            "market_concentration": round(market_concentration, 3),
            "our_market_position": our_position,
            "our_visibility_share": round(our_share, 2)
        }
        
    def _calculate_our_visibility(self, gsc_data: Dict[str, Any]) -> float:
        """Calculate our own visibility score for comparison"""
        if "queries" not in gsc_data:
            return 0
            
        total_weighted_visibility = 0
        total_impressions = 0
        
        for query_data in gsc_data["queries"]:
            position = query_data.get("position", 100)
            impressions = query_data.get("impressions", 0)
            
            # Use same exponential decay formula
            visibility_pct = 100 * np.exp(-0.15 * (position - 1))
            weighted_visibility = visibility_pct * impressions
            
            total_weighted_visibility += weighted_visibility
            total_impressions += impressions
            
        if total_impressions == 0:
            return 0
            
        return total_weighted_visibility / total_impressions
        
    def _build_head_to_head_matrix(
        self,
        competitors_raw: Dict[str, List[Dict[str, Any]]],
        site_domain: str
    ) -> Dict[str, Dict[str, int]]:
        """
        Build head-to-head comparison matrix showing direct competition.
        
        Returns:
            Dict of domain -> {wins, losses, contested}
        """
        h2h_matrix = {}
        
        # Get our positions
        our_keywords = {}
        for domain, appearances in competitors_raw.items():
            for appearance in appearances:
                keyword = appearance["keyword"].lower()
                if keyword not in our_keywords:
                    # We need to infer our position from context
                    # In practice, this comes from GSC data
                    our_keywords[keyword] = 50  # Default assumption
                    
        # Build matrix for each competitor
        for domain, appearances in competitors_raw.items():
            wins = 0
            losses = 0
            contested = 0
            
            for appearance in appearances:
                keyword = appearance["keyword"].lower()
                their_position = appearance["position"]
                our_position = our_keywords.get(keyword, 100)
                
                diff = our_position - their_position
                
                if abs(diff) <= 3:
                    contested += 1
                elif diff > 3:
                    wins += 1  # They win
                else:
                    losses += 1  # We win
                    
            h2h_matrix[domain] = {
                "they_win": wins,
                "we_win": losses,
                "contested": contested
            }
            
        return h2h_matrix
        
    def _generate_strategic_insights(
        self,
        competitor_profiles: List[CompetitorProfile],
        competitive_gaps: List[CompetitiveGap],
        market_metrics: Dict[str, Any]
    ) -> List[str]:
        """Generate human-readable strategic insights"""
        insights = []
        
        if not competitor_profiles:
            insights.append("Low competitive pressure detected. Opportunity to establish market leadership.")
            return insights
            
        # Market position insight
        position = market_metrics["our_market_position"]
        if position == 1:
            insights.append(f"You're the visibility leader. Focus on defending against {len(competitor_profiles)} competitors.")
        elif position <= 3:
            insights.append(f"You're #{position} in market visibility. Top competitor: {competitor_profiles[0].domain}")
        else:
            insights.append(f"You rank #{position} among competitors. Significant catch-up opportunity exists.")
            
        # Concentration insight
        concentration = market_metrics["market_concentration"]
        if concentration > 0.3:
            insights.append("Market is highly concentrated. A few players dominate most keywords.")
        else:
            insights.append("Market is fragmented. Multiple players competing for visibility.")
            
        # Critical threats
        critical_threats = [cp for cp in competitor_profiles if cp.threat_level == ThreatLevel.CRITICAL.value]
        if critical_threats:
            threat_domains = [ct.domain for ct in critical_threats[:3]]
            insights.append(f"CRITICAL THREATS: {', '.join(threat_domains)} outrank you on key terms.")
            
        # Emerging threats
        emerging = [cp for cp in competitor_profiles if cp.threat_level == ThreatLevel.EMERGING.value]
        if emerging:
            insights.append(f"{len(emerging)} emerging competitors gaining rapid visibility. Monitor closely.")
            
        # Gap opportunities
        if competitive_gaps:
            high_value_gaps = [g for g in competitive_gaps if g.opportunity_score > 1000]
            if high_value_gaps:
                insights.append(f"{len(high_value_gaps)} high-value keyword gaps identified where competitors rank but you don't.")
                
        # Strategic gap insight
        if competitor_profiles:
            top_competitor = competitor_profiles[0]
            insights.append(f"Top competitor's advantage: {top_competitor.strategic_gap}")
            
        # SERP features
        feature_leaders = [
            cp for cp in competitor_profiles 
            if cp.serp_features_they_own.get("featured_snippet", 0) > 0
        ]
        if feature_leaders:
            insights.append(f"{len(feature_leaders)} competitors own featured snippets. Opportunity to compete for position zero.")
            
        return insights
        
    def _extract_domain(self, url: str) -> str:
        """Extract domain from URL"""
        if not url:
            return ""
            
        # Remove protocol
        url = url.replace("https://", "").replace("http://", "")
        
        # Remove path
        domain = url.split("/")[0]
        
        # Remove www
        domain = domain.replace("www.", "")
        
        return domain


async def run_module_3(
    report_id: str,
    gsc_data: Dict[str, Any],
    serp_data: List[Dict[str, Any]],
    site_domain: str,
    dataforseo_service: DataForSEOService,
    gsc_service: GSCService,
    supabase_service: SupabaseService
) -> Dict[str, Any]:
    """
    Main entry point for Module 3: Competitor Analysis
    
    Args:
        report_id: Unique report identifier
        gsc_data: GSC performance data
        serp_data: SERP data from DataForSEO
        site_domain: User's domain (normalized)
        dataforseo_service: DataForSEO API service
        gsc_service: Google Search Console service
        supabase_service: Database service
        
    Returns:
        Structured competitor analysis results
    """
    analyzer = CompetitorAnalyzer(
        dataforseo_service=dataforseo_service,
        gsc_service=gsc_service,
        supabase_service=supabase_service
    )
    
    return await analyzer.analyze(
        report_id=report_id,
        gsc_data=gsc_data,
        serp_data=serp_data,
        site_domain=site_domain
    )
