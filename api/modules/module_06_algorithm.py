"""
Module 6: Algorithm Update Impact Analysis

Detects algorithm update impacts by correlating traffic change points
with known Google algorithm updates. Identifies which pages and content
types were most affected, and assesses site vulnerability to future updates.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import pandas as pd
import numpy as np
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class AlgorithmUpdate:
    """Represents a known Google algorithm update."""
    date: datetime
    name: str
    type: str  # core, spam, helpful_content, link, product_reviews, etc.
    source: str
    description: Optional[str] = None


@dataclass
class ImpactAssessment:
    """Assessment of an algorithm update's impact on the site."""
    update_name: str
    update_date: datetime
    update_type: str
    site_impact: str  # positive, negative, neutral
    click_change_pct: float
    impression_change_pct: float
    position_change_avg: float
    pages_most_affected: List[Dict[str, Any]]
    common_characteristics: List[str]
    recovery_status: str  # recovered, partial_recovery, not_recovered, ongoing
    days_since_update: int


class AlgorithmImpactAnalyzer:
    """Analyzes the impact of algorithm updates on site performance."""
    
    def __init__(self, algorithm_updates: List[AlgorithmUpdate]):
        """
        Initialize with known algorithm updates.
        
        Args:
            algorithm_updates: List of known Google algorithm updates
        """
        self.algorithm_updates = sorted(
            algorithm_updates,
            key=lambda x: x.date,
            reverse=True
        )
    
    def analyze(
        self,
        daily_data: pd.DataFrame,
        page_daily_data: pd.DataFrame,
        change_points: List[Dict[str, Any]],
        page_metadata: Optional[pd.DataFrame] = None
    ) -> Dict[str, Any]:
        """
        Analyze algorithm update impacts on site performance.
        
        Args:
            daily_data: Daily aggregate metrics (date, clicks, impressions, position)
            page_daily_data: Per-page daily metrics (date, page, clicks, impressions, position)
            change_points: Change points detected in Module 1
            page_metadata: Optional metadata about pages (word_count, has_schema, content_type, etc.)
        
        Returns:
            Dictionary containing:
                - updates_impacting_site: List of ImpactAssessment objects
                - vulnerability_score: Float 0-1 indicating algorithmic vulnerability
                - vulnerability_factors: List of factors contributing to vulnerability
                - recommendation: Strategic recommendation based on update history
                - unexplained_changes: Change points not attributable to updates
        """
        logger.info("Starting algorithm update impact analysis")
        
        try:
            # Ensure datetime types
            daily_data = daily_data.copy()
            daily_data['date'] = pd.to_datetime(daily_data['date'])
            page_daily_data = page_daily_data.copy()
            page_daily_data['date'] = pd.to_datetime(page_daily_data['date'])
            
            # Match change points to algorithm updates
            matched_impacts = []
            unexplained_changes = []
            
            for cp in change_points:
                cp_date = pd.to_datetime(cp['date'])
                matched_update = self._find_matching_update(cp_date)
                
                if matched_update:
                    impact = self._assess_update_impact(
                        matched_update,
                        cp,
                        daily_data,
                        page_daily_data,
                        page_metadata
                    )
                    matched_impacts.append(impact)
                else:
                    unexplained_changes.append(cp)
            
            # Calculate vulnerability score
            vulnerability_score, vulnerability_factors = self._calculate_vulnerability(
                matched_impacts,
                daily_data
            )
            
            # Generate recommendation
            recommendation = self._generate_recommendation(
                matched_impacts,
                vulnerability_score,
                vulnerability_factors
            )
            
            return {
                'updates_impacting_site': [self._impact_to_dict(imp) for imp in matched_impacts],
                'vulnerability_score': round(vulnerability_score, 2),
                'vulnerability_factors': vulnerability_factors,
                'recommendation': recommendation,
                'unexplained_changes': unexplained_changes,
                'total_updates_in_period': len(self.algorithm_updates),
                'updates_with_site_impact': len(matched_impacts)
            }
            
        except Exception as e:
            logger.error(f"Error in algorithm impact analysis: {str(e)}")
            raise
    
    def _find_matching_update(
        self,
        change_point_date: datetime,
        window_days: int = 7
    ) -> Optional[AlgorithmUpdate]:
        """
        Find algorithm update within ±window_days of change point.
        
        Args:
            change_point_date: Date of detected change point
            window_days: Days before/after to search for update
        
        Returns:
            Matching AlgorithmUpdate or None
        """
        for update in self.algorithm_updates:
            days_diff = abs((update.date - change_point_date).days)
            if days_diff <= window_days:
                return update
        return None
    
    def _assess_update_impact(
        self,
        update: AlgorithmUpdate,
        change_point: Dict[str, Any],
        daily_data: pd.DataFrame,
        page_daily_data: pd.DataFrame,
        page_metadata: Optional[pd.DataFrame]
    ) -> ImpactAssessment:
        """
        Assess the impact of an algorithm update on the site.
        
        Args:
            update: The algorithm update
            change_point: Change point data from Module 1
            daily_data: Daily aggregate metrics
            page_daily_data: Per-page daily metrics
            page_metadata: Optional page metadata
        
        Returns:
            ImpactAssessment object
        """
        update_date = update.date
        
        # Define pre/post windows (14 days before, 14 days after)
        pre_start = update_date - timedelta(days=14)
        pre_end = update_date - timedelta(days=1)
        post_start = update_date
        post_end = update_date + timedelta(days=14)
        
        # Calculate aggregate impact
        pre_data = daily_data[
            (daily_data['date'] >= pre_start) &
            (daily_data['date'] <= pre_end)
        ]
        post_data = daily_data[
            (daily_data['date'] >= post_start) &
            (daily_data['date'] <= post_end)
        ]
        
        if len(pre_data) == 0 or len(post_data) == 0:
            # Not enough data around this update
            click_change_pct = 0
            impression_change_pct = 0
            position_change_avg = 0
        else:
            pre_clicks = pre_data['clicks'].mean()
            post_clicks = post_data['clicks'].mean()
            click_change_pct = ((post_clicks - pre_clicks) / pre_clicks * 100) if pre_clicks > 0 else 0
            
            pre_impressions = pre_data['impressions'].mean()
            post_impressions = post_data['impressions'].mean()
            impression_change_pct = ((post_impressions - pre_impressions) / pre_impressions * 100) if pre_impressions > 0 else 0
            
            pre_position = pre_data['position'].mean()
            post_position = post_data['position'].mean()
            position_change_avg = post_position - pre_position
        
        # Determine impact direction
        if click_change_pct > 5:
            site_impact = "positive"
        elif click_change_pct < -5:
            site_impact = "negative"
        else:
            site_impact = "neutral"
        
        # Find most affected pages
        pages_most_affected = self._find_affected_pages(
            page_daily_data,
            update_date,
            top_n=10
        )
        
        # Identify common characteristics
        common_characteristics = self._identify_common_characteristics(
            pages_most_affected,
            page_metadata
        )
        
        # Assess recovery status
        recovery_status = self._assess_recovery_status(
            daily_data,
            update_date,
            click_change_pct
        )
        
        days_since_update = (datetime.now() - update_date).days
        
        return ImpactAssessment(
            update_name=update.name,
            update_date=update_date,
            update_type=update.type,
            site_impact=site_impact,
            click_change_pct=round(click_change_pct, 1),
            impression_change_pct=round(impression_change_pct, 1),
            position_change_avg=round(position_change_avg, 2),
            pages_most_affected=pages_most_affected,
            common_characteristics=common_characteristics,
            recovery_status=recovery_status,
            days_since_update=days_since_update
        )
    
    def _find_affected_pages(
        self,
        page_daily_data: pd.DataFrame,
        update_date: datetime,
        top_n: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Find pages most affected by an algorithm update.
        
        Args:
            page_daily_data: Per-page daily metrics
            update_date: Date of the algorithm update
            top_n: Number of top affected pages to return
        
        Returns:
            List of page impact dictionaries
        """
        pre_start = update_date - timedelta(days=14)
        pre_end = update_date - timedelta(days=1)
        post_start = update_date
        post_end = update_date + timedelta(days=14)
        
        # Calculate per-page changes
        page_impacts = []
        
        for page in page_daily_data['page'].unique():
            page_data = page_daily_data[page_daily_data['page'] == page]
            
            pre_page = page_data[
                (page_data['date'] >= pre_start) &
                (page_data['date'] <= pre_end)
            ]
            post_page = page_data[
                (page_data['date'] >= post_start) &
                (page_data['date'] <= post_end)
            ]
            
            if len(pre_page) == 0 or len(post_page) == 0:
                continue
            
            pre_clicks = pre_page['clicks'].sum()
            post_clicks = post_page['clicks'].sum()
            click_change = post_clicks - pre_clicks
            click_change_pct = (click_change / pre_clicks * 100) if pre_clicks > 0 else 0
            
            pre_position = pre_page['position'].mean()
            post_position = post_page['position'].mean()
            position_change = post_position - pre_position
            
            page_impacts.append({
                'page': page,
                'click_change': int(click_change),
                'click_change_pct': round(click_change_pct, 1),
                'position_change': round(position_change, 2),
                'pre_clicks': int(pre_clicks),
                'post_clicks': int(post_clicks)
            })
        
        # Sort by absolute click change
        page_impacts.sort(key=lambda x: abs(x['click_change']), reverse=True)
        
        return page_impacts[:top_n]
    
    def _identify_common_characteristics(
        self,
        affected_pages: List[Dict[str, Any]],
        page_metadata: Optional[pd.DataFrame]
    ) -> List[str]:
        """
        Identify common characteristics among affected pages.
        
        Args:
            affected_pages: List of affected page dictionaries
            page_metadata: Optional metadata about pages
        
        Returns:
            List of common characteristic strings
        """
        if not page_metadata or len(affected_pages) == 0:
            return []
        
        characteristics = []
        
        # Get pages with negative impact
        negative_pages = [p['page'] for p in affected_pages if p['click_change'] < 0]
        
        if len(negative_pages) == 0:
            return characteristics
        
        # Filter metadata to affected pages
        affected_metadata = page_metadata[page_metadata['page'].isin(negative_pages)]
        
        if len(affected_metadata) == 0:
            return characteristics
        
        # Check word count
        if 'word_count' in affected_metadata.columns:
            avg_word_count = affected_metadata['word_count'].mean()
            if avg_word_count < 500:
                characteristics.append("thin_content")
            elif avg_word_count < 1000:
                characteristics.append("short_content")
        
        # Check schema presence
        if 'has_schema' in affected_metadata.columns:
            schema_pct = affected_metadata['has_schema'].mean()
            if schema_pct < 0.3:
                characteristics.append("no_schema")
        
        # Check content type
        if 'content_type' in affected_metadata.columns:
            content_types = affected_metadata['content_type'].value_counts()
            if len(content_types) > 0:
                dominant_type = content_types.index[0]
                if content_types[dominant_type] / len(affected_metadata) > 0.6:
                    characteristics.append(f"content_type_{dominant_type}")
        
        # Check backlinks
        if 'backlink_count' in affected_metadata.columns:
            avg_backlinks = affected_metadata['backlink_count'].mean()
            if avg_backlinks < 5:
                characteristics.append("low_backlinks")
        
        # Check freshness
        if 'last_modified' in affected_metadata.columns:
            affected_metadata['days_since_update'] = (
                datetime.now() - pd.to_datetime(affected_metadata['last_modified'])
            ).dt.days
            avg_age = affected_metadata['days_since_update'].mean()
            if avg_age > 365:
                characteristics.append("outdated_content")
        
        return characteristics
    
    def _assess_recovery_status(
        self,
        daily_data: pd.DataFrame,
        update_date: datetime,
        initial_impact_pct: float
    ) -> str:
        """
        Assess whether the site has recovered from an update impact.
        
        Args:
            daily_data: Daily aggregate metrics
            update_date: Date of the update
            initial_impact_pct: Initial percentage impact
        
        Returns:
            Recovery status string
        """
        # If update was recent (< 30 days), status is ongoing
        days_since = (datetime.now() - update_date).days
        if days_since < 30:
            return "ongoing"
        
        # Compare recent performance to pre-update baseline
        pre_start = update_date - timedelta(days=14)
        pre_end = update_date - timedelta(days=1)
        recent_start = datetime.now() - timedelta(days=14)
        recent_end = datetime.now()
        
        pre_data = daily_data[
            (daily_data['date'] >= pre_start) &
            (daily_data['date'] <= pre_end)
        ]
        recent_data = daily_data[
            (daily_data['date'] >= recent_start) &
            (daily_data['date'] <= recent_end)
        ]
        
        if len(pre_data) == 0 or len(recent_data) == 0:
            return "unknown"
        
        pre_clicks = pre_data['clicks'].mean()
        recent_clicks = recent_data['clicks'].mean()
        
        recovery_pct = ((recent_clicks - pre_clicks) / pre_clicks * 100) if pre_clicks > 0 else 0
        
        # If negative impact
        if initial_impact_pct < 0:
            if recovery_pct >= -5:  # Back to within 5% of baseline
                return "recovered"
            elif recovery_pct >= initial_impact_pct / 2:  # Recovered more than half
                return "partial_recovery"
            else:
                return "not_recovered"
        else:
            # Positive impact - check if gains sustained
            if recovery_pct >= initial_impact_pct * 0.8:
                return "recovered"
            else:
                return "partial_recovery"
    
    def _calculate_vulnerability(
        self,
        matched_impacts: List[ImpactAssessment],
        daily_data: pd.DataFrame
    ) -> tuple[float, List[str]]:
        """
        Calculate overall algorithmic vulnerability score.
        
        Args:
            matched_impacts: List of impact assessments
            daily_data: Daily aggregate metrics
        
        Returns:
            Tuple of (vulnerability_score, vulnerability_factors)
        """
        factors = []
        score_components = []
        
        # Factor 1: Frequency of negative impacts
        negative_impacts = [imp for imp in matched_impacts if imp.site_impact == "negative"]
        if len(matched_impacts) > 0:
            negative_rate = len(negative_impacts) / len(matched_impacts)
            score_components.append(negative_rate)
            if negative_rate > 0.5:
                factors.append("frequent_negative_impacts")
        
        # Factor 2: Severity of impacts
        if len(negative_impacts) > 0:
            avg_negative_impact = np.mean([imp.click_change_pct for imp in negative_impacts])
            severity_score = min(abs(avg_negative_impact) / 50, 1.0)  # Normalize to 0-1
            score_components.append(severity_score)
            if avg_negative_impact < -20:
                factors.append("severe_impact_history")
        
        # Factor 3: Recovery capability
        if len(negative_impacts) > 0:
            not_recovered = len([imp for imp in negative_impacts if imp.recovery_status == "not_recovered"])
            recovery_rate = 1 - (not_recovered / len(negative_impacts))
            score_components.append(1 - recovery_rate)
            if recovery_rate < 0.5:
                factors.append("poor_recovery_rate")
        
        # Factor 4: Common vulnerability patterns
        all_characteristics = []
        for imp in negative_impacts:
            all_characteristics.extend(imp.common_characteristics)
        
        if all_characteristics:
            char_counts = pd.Series(all_characteristics).value_counts()
            recurring_issues = char_counts[char_counts >= 2].index.tolist()
            
            if recurring_issues:
                factors.append(f"recurring_issues: {', '.join(recurring_issues[:3])}")
                score_components.append(0.3 * len(recurring_issues))
        
        # Factor 5: Traffic volatility
        if len(daily_data) > 30:
            clicks_std = daily_data['clicks'].std()
            clicks_mean = daily_data['clicks'].mean()
            cv = clicks_std / clicks_mean if clicks_mean > 0 else 0
            if cv > 0.3:
                factors.append("high_traffic_volatility")
                score_components.append(min(cv, 1.0))
        
        # Calculate final score (average of components, capped at 1.0)
        if score_components:
            vulnerability_score = min(np.mean(score_components), 1.0)
        else:
            vulnerability_score = 0.0
        
        return vulnerability_score, factors
    
    def _generate_recommendation(
        self,
        matched_impacts: List[ImpactAssessment],
        vulnerability_score: float,
        vulnerability_factors: List[str]
    ) -> str:
        """
        Generate strategic recommendation based on update history.
        
        Args:
            matched_impacts: List of impact assessments
            vulnerability_score: Calculated vulnerability score
            vulnerability_factors: List of vulnerability factors
        
        Returns:
            Recommendation string
        """
        if len(matched_impacts) == 0:
            return "No significant algorithm update impacts detected in the analysis period."
        
        # Count characteristics across all negative impacts
        negative_impacts = [imp for imp in matched_impacts if imp.site_impact == "negative"]
        
        if len(negative_impacts) == 0:
            return "Your site has shown resilience to recent algorithm updates. Continue current content strategy."
        
        # Aggregate characteristics
        char_counter = {}
        for imp in negative_impacts:
            for char in imp.common_characteristics:
                char_counter[char] = char_counter.get(char, 0) + 1
        
        # Sort by frequency
        sorted_chars = sorted(char_counter.items(), key=lambda x: x[1], reverse=True)
        
        # Build recommendation
        rec_parts = []
        
        if vulnerability_score > 0.7:
            rec_parts.append("HIGH VULNERABILITY: Your site is highly susceptible to algorithm updates.")
        elif vulnerability_score > 0.4:
            rec_parts.append("MODERATE VULNERABILITY: Your site shows some algorithmic weakness.")
        
        # Address most common issues
        if sorted_chars:
            most_common = sorted_chars[0][0]
            
            if "thin_content" in most_common or "short_content" in most_common:
                rec_parts.append("Focus on content depth: expand thin pages to 1000+ words with comprehensive coverage.")
            elif "no_schema" in most_common:
                rec_parts.append("Implement structured data: add relevant schema markup to improve SERP presentation.")
            elif "low_backlinks" in most_common:
                rec_parts.append("Build authority: focus link building efforts on algorithmically vulnerable pages.")
            elif "outdated_content" in most_common:
                rec_parts.append("Content freshness critical: prioritize updating old pages with current information.")
            elif "content_type" in most_common:
                content_type = most_common.split("_")[-1]
                rec_parts.append(f"Your {content_type} pages are most vulnerable. Review and strengthen this content type.")
        
        # Recovery advice
        not_recovered = [imp for imp in negative_impacts if imp.recovery_status == "not_recovered"]
        if len(not_recovered) > 0:
            rec_parts.append(f"{len(not_recovered)} update impact(s) have not recovered. Immediate remediation needed for affected pages.")
        
        return " ".join(rec_parts) if rec_parts else "Continue monitoring algorithm updates and maintain content quality standards."
    
    def _impact_to_dict(self, impact: ImpactAssessment) -> Dict[str, Any]:
        """Convert ImpactAssessment to dictionary."""
        return {
            'update_name': impact.update_name,
            'date': impact.update_date.isoformat(),
            'update_type': impact.update_type,
            'site_impact': impact.site_impact,
            'click_change_pct': impact.click_change_pct,
            'impression_change_pct': impact.impression_change_pct,
            'position_change_avg': impact.position_change_avg,
            'pages_most_affected': impact.pages_most_affected,
            'common_characteristics': impact.common_characteristics,
            'recovery_status': impact.recovery_status,
            'days_since_update': impact.days_since_update
        }