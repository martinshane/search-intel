"""
Module 11: Competitive Threat Radar

Analyzes competitor presence across user's keyword set to identify:
- Primary competitors (highest keyword overlap)
- Emerging threats (new domains rapidly climbing)
- Keyword vulnerability (competitors closing in on user's positions)
- Competitive content velocity estimates

Input: DataForSEO SERP data, GSC query data
Output: Structured competitive intelligence with threat scoring
"""

import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from collections import defaultdict
import pandas as pd
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class CompetitorProfile:
    """Profile of a competitor domain."""
    domain: str
    keyword_overlap: int
    avg_position: float
    positions: List[float]
    keywords: List[str]
    first_seen: Optional[datetime] = None
    trajectory: Optional[str] = None


@dataclass
class EmergingThreat:
    """Represents a new or rapidly improving competitor."""
    domain: str
    first_seen: datetime
    keywords_entered: int
    avg_entry_position: float
    current_avg_position: float
    trajectory: str  # "rapidly_improving", "steady_climb", "volatile"
    threat_level: str  # "critical", "high", "medium", "low"
    keywords: List[str]


@dataclass
class KeywordVulnerability:
    """Vulnerability assessment for a specific keyword."""
    keyword: str
    user_position: float
    competitors_within_3: int
    nearest_competitor_domain: str
    nearest_competitor_position: float
    position_gap: float
    gap_trend: str  # "narrowing", "widening", "stable"


class CompetitiveAnalyzer:
    """Analyzes competitive landscape from SERP and GSC data."""
    
    def __init__(self, user_domain: str):
        """
        Initialize analyzer.
        
        Args:
            user_domain: The user's domain (to exclude from competitor analysis)
        """
        self.user_domain = user_domain.lower().replace('www.', '')
    
    def analyze(
        self,
        serp_data: List[Dict[str, Any]],
        gsc_data: pd.DataFrame,
        historical_serp_data: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Run complete competitive threat analysis.
        
        Args:
            serp_data: List of SERP results from DataForSEO
                Each dict should have: keyword, organic_results (list of dicts with domain, position)
            gsc_data: DataFrame with columns: query, position, clicks, impressions
            historical_serp_data: Optional historical SERP data for trend detection
        
        Returns:
            Dict with competitive analysis results
        """
        try:
            logger.info("Starting competitive threat analysis")
            
            # Extract competitor positions from SERP data
            competitor_positions = self._extract_competitor_positions(serp_data)
            
            # Identify primary competitors
            primary_competitors = self._identify_primary_competitors(
                competitor_positions, gsc_data
            )
            
            # Detect emerging threats
            emerging_threats = self._detect_emerging_threats(
                serp_data, historical_serp_data
            )
            
            # Analyze keyword vulnerability
            keyword_vulnerability = self._analyze_keyword_vulnerability(
                serp_data, gsc_data
            )
            
            # Estimate competitor content velocity
            content_velocity = self._estimate_content_velocity(
                serp_data, historical_serp_data
            )
            
            # Calculate summary metrics
            summary = self._generate_summary(
                primary_competitors,
                emerging_threats,
                keyword_vulnerability,
                content_velocity
            )
            
            logger.info("Competitive analysis complete")
            
            return {
                "primary_competitors": [
                    {
                        "domain": comp.domain,
                        "keyword_overlap": comp.keyword_overlap,
                        "avg_position": round(comp.avg_position, 1),
                        "keywords": comp.keywords[:10],  # Top 10 for brevity
                        "position_distribution": self._calculate_position_distribution(comp.positions)
                    }
                    for comp in primary_competitors
                ],
                "emerging_threats": [
                    {
                        "domain": threat.domain,
                        "first_seen": threat.first_seen.isoformat() if threat.first_seen else None,
                        "keywords_entered": threat.keywords_entered,
                        "avg_entry_position": round(threat.avg_entry_position, 1),
                        "current_avg_position": round(threat.current_avg_position, 1),
                        "position_improvement": round(
                            threat.avg_entry_position - threat.current_avg_position, 1
                        ),
                        "trajectory": threat.trajectory,
                        "threat_level": threat.threat_level,
                        "keywords": threat.keywords[:5]
                    }
                    for threat in emerging_threats
                ],
                "keyword_vulnerability": [
                    {
                        "keyword": vuln.keyword,
                        "your_position": round(vuln.user_position, 1),
                        "competitors_within_3": vuln.competitors_within_3,
                        "nearest_competitor": vuln.nearest_competitor_domain,
                        "nearest_competitor_position": round(vuln.nearest_competitor_position, 1),
                        "position_gap": round(vuln.position_gap, 1),
                        "gap_trend": vuln.gap_trend,
                        "risk_level": self._calculate_risk_level(vuln)
                    }
                    for vuln in keyword_vulnerability
                ],
                "content_velocity": content_velocity,
                "summary": summary
            }
            
        except Exception as e:
            logger.error(f"Error in competitive analysis: {str(e)}", exc_info=True)
            raise
    
    def _extract_competitor_positions(
        self,
        serp_data: List[Dict[str, Any]]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Extract competitor positions from SERP data.
        
        Returns:
            Dict mapping domain -> list of {keyword, position}
        """
        competitor_data = defaultdict(list)
        
        for serp_result in serp_data:
            keyword = serp_result.get('keyword', '')
            organic_results = serp_result.get('organic_results', [])
            
            for result in organic_results:
                domain = self._normalize_domain(result.get('domain', ''))
                position = result.get('position', 0)
                
                # Skip user's own domain
                if domain == self.user_domain:
                    continue
                
                competitor_data[domain].append({
                    'keyword': keyword,
                    'position': position
                })
        
        return competitor_data
    
    def _identify_primary_competitors(
        self,
        competitor_positions: Dict[str, List[Dict[str, Any]]],
        gsc_data: pd.DataFrame
    ) -> List[CompetitorProfile]:
        """
        Identify primary competitors based on keyword overlap frequency.
        
        Returns:
            List of CompetitorProfile objects, sorted by keyword overlap DESC
        """
        competitors = []
        
        for domain, positions in competitor_positions.items():
            if len(positions) < 3:  # Require at least 3 shared keywords
                continue
            
            keywords = [p['keyword'] for p in positions]
            position_values = [p['position'] for p in positions]
            avg_position = sum(position_values) / len(position_values)
            
            competitors.append(CompetitorProfile(
                domain=domain,
                keyword_overlap=len(positions),
                avg_position=avg_position,
                positions=position_values,
                keywords=keywords
            ))
        
        # Sort by keyword overlap (most threatening competitors first)
        competitors.sort(key=lambda x: x.keyword_overlap, reverse=True)
        
        return competitors[:10]  # Top 10 competitors
    
    def _detect_emerging_threats(
        self,
        serp_data: List[Dict[str, Any]],
        historical_serp_data: Optional[List[Dict[str, Any]]]
    ) -> List[EmergingThreat]:
        """
        Detect domains that are new or rapidly improving in rankings.
        
        Returns:
            List of EmergingThreat objects
        """
        if not historical_serp_data:
            logger.info("No historical SERP data available for threat detection")
            return []
        
        # Build current domain presence
        current_domains = defaultdict(list)
        for serp in serp_data:
            keyword = serp.get('keyword', '')
            for result in serp.get('organic_results', []):
                domain = self._normalize_domain(result.get('domain', ''))
                if domain != self.user_domain:
                    current_domains[domain].append({
                        'keyword': keyword,
                        'position': result.get('position', 0)
                    })
        
        # Build historical domain presence
        historical_domains = defaultdict(list)
        for serp in historical_serp_data:
            keyword = serp.get('keyword', '')
            for result in serp.get('organic_results', []):
                domain = self._normalize_domain(result.get('domain', ''))
                if domain != self.user_domain:
                    historical_domains[domain].append({
                        'keyword': keyword,
                        'position': result.get('position', 0)
                    })
        
        threats = []
        
        for domain, current_positions in current_domains.items():
            historical_positions = historical_domains.get(domain, [])
            
            # New entrant check
            is_new = len(historical_positions) == 0 and len(current_positions) >= 3
            
            # Rapid improvement check
            is_rapidly_improving = False
            if historical_positions:
                hist_avg = sum(p['position'] for p in historical_positions) / len(historical_positions)
                curr_avg = sum(p['position'] for p in current_positions) / len(current_positions)
                improvement = hist_avg - curr_avg
                is_rapidly_improving = improvement >= 5  # Improved by 5+ positions on average
            
            if is_new or is_rapidly_improving:
                keywords = [p['keyword'] for p in current_positions]
                current_avg_pos = sum(p['position'] for p in current_positions) / len(current_positions)
                entry_avg_pos = current_avg_pos
                
                if historical_positions:
                    entry_avg_pos = sum(p['position'] for p in historical_positions) / len(historical_positions)
                
                # Determine trajectory
                if is_new:
                    trajectory = "new_entrant"
                elif is_rapidly_improving:
                    trajectory = "rapidly_improving"
                else:
                    trajectory = "steady_climb"
                
                # Calculate threat level
                threat_level = self._calculate_threat_level(
                    len(current_positions),
                    current_avg_pos,
                    entry_avg_pos
                )
                
                threats.append(EmergingThreat(
                    domain=domain,
                    first_seen=datetime.now() - timedelta(days=60) if is_new else None,
                    keywords_entered=len(current_positions),
                    avg_entry_position=entry_avg_pos,
                    current_avg_position=current_avg_pos,
                    trajectory=trajectory,
                    threat_level=threat_level,
                    keywords=keywords
                ))
        
        # Sort by threat level and position improvement
        threats.sort(key=lambda x: (
            {"critical": 0, "high": 1, "medium": 2, "low": 3}[x.threat_level],
            x.current_avg_position
        ))
        
        return threats
    
    def _analyze_keyword_vulnerability(
        self,
        serp_data: List[Dict[str, Any]],
        gsc_data: pd.DataFrame
    ) -> List[KeywordVulnerability]:
        """
        Analyze how vulnerable each keyword is to competitor threats.
        
        Returns:
            List of KeywordVulnerability objects
        """
        vulnerabilities = []
        
        # Create lookup for user's positions
        user_positions = {}
        if not gsc_data.empty:
            for _, row in gsc_data.iterrows():
                user_positions[row['query']] = row['position']
        
        for serp_result in serp_data:
            keyword = serp_result.get('keyword', '')
            user_position = user_positions.get(keyword, 0)
            
            if user_position == 0 or user_position > 20:
                continue  # Skip if user doesn't rank or ranks too low
            
            # Find competitors within 3 positions
            organic_results = serp_result.get('organic_results', [])
            competitors_nearby = []
            
            for result in organic_results:
                domain = self._normalize_domain(result.get('domain', ''))
                position = result.get('position', 0)
                
                if domain != self.user_domain:
                    position_diff = abs(position - user_position)
                    if position_diff <= 3:
                        competitors_nearby.append({
                            'domain': domain,
                            'position': position,
                            'gap': position - user_position
                        })
            
            if competitors_nearby:
                # Find nearest competitor
                nearest = min(
                    competitors_nearby,
                    key=lambda x: abs(x['gap'])
                )
                
                vulnerabilities.append(KeywordVulnerability(
                    keyword=keyword,
                    user_position=user_position,
                    competitors_within_3=len(competitors_nearby),
                    nearest_competitor_domain=nearest['domain'],
                    nearest_competitor_position=nearest['position'],
                    position_gap=abs(nearest['gap']),
                    gap_trend="stable"  # Would need historical data for actual trend
                ))
        
        # Sort by number of nearby competitors and user position
        vulnerabilities.sort(
            key=lambda x: (x.competitors_within_3, x.user_position),
            reverse=True
        )
        
        return vulnerabilities[:20]  # Top 20 most vulnerable
    
    def _estimate_content_velocity(
        self,
        serp_data: List[Dict[str, Any]],
        historical_serp_data: Optional[List[Dict[str, Any]]]
    ) -> Dict[str, Any]:
        """
        Estimate competitor publishing frequency based on new URL appearances.
        
        Returns:
            Dict with content velocity estimates per competitor
        """
        if not historical_serp_data:
            return {"note": "Historical data required for velocity estimation"}
        
        # Collect URLs per domain
        current_urls = defaultdict(set)
        historical_urls = defaultdict(set)
        
        for serp in serp_data:
            for result in serp.get('organic_results', []):
                domain = self._normalize_domain(result.get('domain', ''))
                url = result.get('url', '')
                if domain != self.user_domain and url:
                    current_urls[domain].add(url)
        
        for serp in historical_serp_data:
            for result in serp.get('organic_results', []):
                domain = self._normalize_domain(result.get('domain', ''))
                url = result.get('url', '')
                if domain != self.user_domain and url:
                    historical_urls[domain].add(url)
        
        velocity_estimates = []
        
        for domain in current_urls:
            new_urls = current_urls[domain] - historical_urls.get(domain, set())
            total_current = len(current_urls[domain])
            
            if total_current >= 5:  # Only include domains with significant presence
                # Estimate weekly publishing rate (assuming 8-week gap between snapshots)
                estimated_weekly = len(new_urls) / 8 if new_urls else 0
                
                velocity_estimates.append({
                    "domain": domain,
                    "new_urls_detected": len(new_urls),
                    "total_urls_ranking": total_current,
                    "estimated_weekly_publish_rate": round(estimated_weekly, 1),
                    "content_velocity": self._classify_velocity(estimated_weekly)
                })
        
        # Sort by velocity
        velocity_estimates.sort(
            key=lambda x: x['estimated_weekly_publish_rate'],
            reverse=True
        )
        
        return {
            "competitors": velocity_estimates[:10],
            "analysis_note": "Based on new URL appearances in SERP results"
        }
    
    def _generate_summary(
        self,
        primary_competitors: List[CompetitorProfile],
        emerging_threats: List[EmergingThreat],
        vulnerabilities: List[KeywordVulnerability],
        content_velocity: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate executive summary of competitive landscape."""
        
        high_threats = [t for t in emerging_threats if t.threat_level in ["critical", "high"]]
        high_vulnerability_keywords = [v for v in vulnerabilities if v.competitors_within_3 >= 3]
        
        return {
            "total_competitors_tracked": len(primary_competitors),
            "emerging_threats_count": len(emerging_threats),
            "high_priority_threats": len(high_threats),
            "vulnerable_keywords": len(high_vulnerability_keywords),
            "competitive_pressure": self._calculate_competitive_pressure(
                primary_competitors, vulnerabilities
            ),
            "top_threat": emerging_threats[0].domain if emerging_threats else None,
            "most_vulnerable_keyword": vulnerabilities[0].keyword if vulnerabilities else None
        }
    
    def _normalize_domain(self, domain: str) -> str:
        """Normalize domain for consistent comparison."""
        if not domain:
            return ""
        return domain.lower().replace('www.', '').replace('https://', '').replace('http://', '').split('/')[0]
    
    def _calculate_position_distribution(self, positions: List[float]) -> Dict[str, int]:
        """Calculate distribution of positions."""
        distribution = {
            "top_3": 0,
            "positions_4_10": 0,
            "positions_11_20": 0,
            "beyond_20": 0
        }
        
        for pos in positions:
            if pos <= 3:
                distribution["top_3"] += 1
            elif pos <= 10:
                distribution["positions_4_10"] += 1
            elif pos <= 20:
                distribution["positions_11_20"] += 1
            else:
                distribution["beyond_20"] += 1
        
        return distribution
    
    def _calculate_threat_level(
        self,
        keyword_count: int,
        current_avg_position: float,
        entry_avg_position: float
    ) -> str:
        """Calculate threat level for emerging competitor."""
        improvement = entry_avg_position - current_avg_position
        
        if keyword_count >= 10 and current_avg_position <= 5 and improvement >= 8:
            return "critical"
        elif keyword_count >= 5 and current_avg_position <= 8 and improvement >= 5:
            return "high"
        elif keyword_count >= 3 and improvement >= 3:
            return "medium"
        else:
            return "low"
    
    def _calculate_risk_level(self, vulnerability: KeywordVulnerability) -> str:
        """Calculate risk level for keyword vulnerability."""
        if vulnerability.competitors_within_3 >= 4 and vulnerability.user_position >= 5:
            return "high"
        elif vulnerability.competitors_within_3 >= 3 or vulnerability.position_gap <= 1:
            return "medium"
        else:
            return "low"
    
    def _classify_velocity(self, weekly_rate: float) -> str:
        """Classify content velocity."""
        if weekly_rate >= 4:
            return "very_high"
        elif weekly_rate >= 2:
            return "high"
        elif weekly_rate >= 1:
            return "medium"
        else:
            return "low"
    
    def _calculate_competitive_pressure(
        self,
        competitors: List[CompetitorProfile],
        vulnerabilities: List[KeywordVulnerability]
    ) -> str:
        """Calculate overall competitive pressure level."""
        if not competitors:
            return "low"
        
        avg_overlap = sum(c.keyword_overlap for c in competitors) / len(competitors)
        high_vuln_count = len([v for v in vulnerabilities if v.competitors_within_3 >= 3])
        
        if avg_overlap >= 20 and high_vuln_count >= 10:
            return "very_high"
        elif avg_overlap >= 10 or high_vuln_count >= 5:
            return "high"
        elif avg_overlap >= 5:
            return "medium"
        else:
            return "low"


def analyze_competitive_threats(
    serp_data: List[Dict[str, Any]],
    gsc_data: pd.DataFrame,
    user_domain: str,
    historical_serp_data: Optional[List[Dict[str, Any]]] = None
) -> Dict[str, Any]:
    """
    Main entry point for Module 11: Competitive Threat Radar analysis.
    
    Args:
        serp_data: List of SERP results from DataForSEO
        gsc_data: DataFrame with GSC query performance data
        user_domain: The user's domain
        historical_serp_data: Optional historical SERP data for trend analysis
    
    Returns:
        Dict containing competitive threat analysis results
    """
    analyzer = CompetitiveAnalyzer(user_domain)
    return analyzer.analyze(serp_data, gsc_data, historical_serp_data)
