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
            impacting_updates = self._match_changes_to_updates(
                change_points,
                daily_data,
                page_daily_data,
                page_metadata
            )
            
            # Identify unexplained changes
            unexplained = self._identify_unexplained_changes(
                change_points,
                impacting_updates
            )
            
            # Calculate vulnerability score
            vulnerability_score, vulnerability_factors = self._calculate_vulnerability(
                impacting_updates,
                page_metadata
            )
            
            # Generate strategic recommendation
            recommendation = self._generate_recommendation(
                impacting_updates,
                vulnerability_score,
                vulnerability_factors
            )
            
            return {
                "updates_impacting_site": [
                    self._impact_to_dict(impact) for impact in impacting_updates
                ],
                "vulnerability_score": round(vulnerability_score, 2),
                "vulnerability_factors": vulnerability_factors,
                "recommendation": recommendation,
                "unexplained_changes": unexplained,
                "total_updates_tracked": len(self.algorithm_updates),
                "analysis_period_days": (
                    daily_data['date'].max() - daily_data['date'].min()
                ).days
            }
            
        except Exception as e:
            logger.error(f"Error in algorithm impact analysis: {str(e)}", exc_info=True)
            raise
    
    def _match_changes_to_updates(
        self,
        change_points: List[Dict[str, Any]],
        daily_data: pd.DataFrame,
        page_daily_data: pd.DataFrame,
        page_metadata: Optional[pd.DataFrame]
    ) -> List[ImpactAssessment]:
        """Match detected change points to known algorithm updates."""
        impacting_updates = []
        
        for change_point in change_points:
            change_date = pd.to_datetime(change_point['date'])
            
            # Find algorithm updates within ±7 days
            matching_update = self._find_nearest_update(change_date, window_days=7)
            
            if matching_update:
                # Assess the impact of this update
                impact = self._assess_update_impact(
                    matching_update,
                    change_date,
                    change_point,
                    daily_data,
                    page_daily_data,
                    page_metadata
                )
                impacting_updates.append(impact)
        
        # Also check for any major updates that might have been missed
        for update in self.algorithm_updates:
            if not any(imp.update_name == update.name for imp in impacting_updates):
                # Check if this update had a subtle impact
                impact = self._check_subtle_impact(
                    update,
                    daily_data,
                    page_daily_data,
                    page_metadata
                )
                if impact and abs(impact.click_change_pct) > 3.0:  # >3% change threshold
                    impacting_updates.append(impact)
        
        return sorted(impacting_updates, key=lambda x: x.update_date, reverse=True)
    
    def _find_nearest_update(
        self,
        date: datetime,
        window_days: int = 7
    ) -> Optional[AlgorithmUpdate]:
        """Find the nearest algorithm update within the time window."""
        date = pd.to_datetime(date)
        
        for update in self.algorithm_updates:
            update_date = pd.to_datetime(update.date)
            days_diff = abs((date - update_date).days)
            
            if days_diff <= window_days:
                return update
        
        return None
    
    def _assess_update_impact(
        self,
        update: AlgorithmUpdate,
        change_date: datetime,
        change_point: Dict[str, Any],
        daily_data: pd.DataFrame,
        page_daily_data: pd.DataFrame,
        page_metadata: Optional[pd.DataFrame]
    ) -> ImpactAssessment:
        """Assess the detailed impact of an algorithm update."""
        update_date = pd.to_datetime(update.date)
        
        # Define pre/post windows (14 days each)
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
            # Not enough data for comparison
            click_change_pct = 0.0
            impression_change_pct = 0.0
            position_change_avg = 0.0
        else:
            pre_clicks_avg = pre_data['clicks'].mean()
            post_clicks_avg = post_data['clicks'].mean()
            click_change_pct = (
                ((post_clicks_avg - pre_clicks_avg) / pre_clicks_avg * 100)
                if pre_clicks_avg > 0 else 0.0
            )
            
            pre_impressions_avg = pre_data['impressions'].mean()
            post_impressions_avg = post_data['impressions'].mean()
            impression_change_pct = (
                ((post_impressions_avg - pre_impressions_avg) / pre_impressions_avg * 100)
                if pre_impressions_avg > 0 else 0.0
            )
            
            pre_position_avg = pre_data['position'].mean()
            post_position_avg = post_data['position'].mean()
            position_change_avg = post_position_avg - pre_position_avg
        
        # Identify most affected pages
        pages_most_affected = self._identify_affected_pages(
            page_daily_data,
            pre_start,
            pre_end,
            post_start,
            post_end,
            limit=10
        )
        
        # Find common characteristics among affected pages
        common_characteristics = self._find_common_characteristics(
            pages_most_affected,
            page_metadata
        )
        
        # Determine impact direction
        if click_change_pct > 5.0:
            site_impact = "positive"
        elif click_change_pct < -5.0:
            site_impact = "negative"
        else:
            site_impact = "neutral"
        
        # Assess recovery status
        recovery_status = self._assess_recovery_status(
            update_date,
            daily_data,
            pre_clicks_avg if len(pre_data) > 0 else 0
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
    
    def _check_subtle_impact(
        self,
        update: AlgorithmUpdate,
        daily_data: pd.DataFrame,
        page_daily_data: pd.DataFrame,
        page_metadata: Optional[pd.DataFrame]
    ) -> Optional[ImpactAssessment]:
        """Check if an update had a subtle impact not caught by change point detection."""
        update_date = pd.to_datetime(update.date)
        
        # Only check updates within our data range
        if update_date < daily_data['date'].min() or update_date > daily_data['date'].max():
            return None
        
        # Create a dummy change point
        change_point = {
            'date': update_date,
            'magnitude': 0.0,
            'direction': 'unknown'
        }
        
        return self._assess_update_impact(
            update,
            update_date,
            change_point,
            daily_data,
            page_daily_data,
            page_metadata
        )
    
    def _identify_affected_pages(
        self,
        page_daily_data: pd.DataFrame,
        pre_start: datetime,
        pre_end: datetime,
        post_start: datetime,
        post_end: datetime,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Identify pages most affected by the update."""
        affected_pages = []
        
        # Group by page
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
            
            pre_clicks = pre_page['clicks'].mean()
            post_clicks = post_page['clicks'].mean()
            
            if pre_clicks == 0:
                continue
            
            click_change_pct = (post_clicks - pre_clicks) / pre_clicks * 100
            
            pre_position = pre_page['position'].mean()
            post_position = post_page['position'].mean()
            position_change = post_position - pre_position
            
            affected_pages.append({
                'page': page,
                'pre_clicks_avg': round(pre_clicks, 1),
                'post_clicks_avg': round(post_clicks, 1),
                'click_change_pct': round(click_change_pct, 1),
                'pre_position': round(pre_position, 2),
                'post_position': round(post_position, 2),
                'position_change': round(position_change, 2),
                'impact_magnitude': abs(click_change_pct)
            })
        
        # Sort by impact magnitude and return top N
        affected_pages.sort(key=lambda x: x['impact_magnitude'], reverse=True)
        return affected_pages[:limit]
    
    def _find_common_characteristics(
        self,
        affected_pages: List[Dict[str, Any]],
        page_metadata: Optional[pd.DataFrame]
    ) -> List[str]:
        """Find common characteristics among affected pages."""
        if not affected_pages or page_metadata is None or len(page_metadata) == 0:
            return []
        
        characteristics = []
        
        # Extract page URLs
        affected_urls = [p['page'] for p in affected_pages]
        
        # Merge with metadata
        affected_meta = page_metadata[page_metadata['page'].isin(affected_urls)]
        
        if len(affected_meta) == 0:
            return []
        
        # Check for common patterns
        # Content type
        if 'content_type' in affected_meta.columns:
            content_types = affected_meta['content_type'].value_counts()
            dominant_type = content_types.idxmax() if len(content_types) > 0 else None
            if dominant_type and content_types[dominant_type] / len(affected_meta) > 0.6:
                characteristics.append(f"content_type_{dominant_type}")
        
        # Thin content
        if 'word_count' in affected_meta.columns:
            avg_words = affected_meta['word_count'].mean()
            if avg_words < 500:
                characteristics.append("thin_content")
            elif avg_words < 1000:
                characteristics.append("medium_content")
        
        # Schema presence
        if 'has_schema' in affected_meta.columns:
            schema_pct = affected_meta['has_schema'].sum() / len(affected_meta)
            if schema_pct < 0.3:
                characteristics.append("no_schema")
        
        # URL pattern (blog vs product vs other)
        blog_count = sum(1 for url in affected_urls if '/blog/' in url.lower())
        if blog_count / len(affected_urls) > 0.6:
            characteristics.append("blog_content")
        
        product_count = sum(1 for url in affected_urls if any(
            keyword in url.lower() 
            for keyword in ['/product/', '/shop/', '/store/']
        ))
        if product_count / len(affected_urls) > 0.6:
            characteristics.append("product_pages")
        
        return characteristics if characteristics else ["no_clear_pattern"]
    
    def _assess_recovery_status(
        self,
        update_date: datetime,
        daily_data: pd.DataFrame,
        pre_update_baseline: float
    ) -> str:
        """Assess whether the site has recovered from the update."""
        update_date = pd.to_datetime(update_date)
        days_since = (datetime.now() - update_date).days
        
        if days_since < 30:
            return "ongoing"
        
        # Look at most recent 14 days
        recent_data = daily_data[
            daily_data['date'] >= daily_data['date'].max() - timedelta(days=14)
        ]
        
        if len(recent_data) == 0:
            return "unknown"
        
        recent_avg = recent_data['clicks'].mean()
        
        if pre_update_baseline == 0:
            return "unknown"
        
        recovery_pct = (recent_avg / pre_update_baseline) * 100
        
        if recovery_pct >= 95:
            return "recovered"
        elif recovery_pct >= 80:
            return "partial_recovery"
        else:
            return "not_recovered"
    
    def _identify_unexplained_changes(
        self,
        change_points: List[Dict[str, Any]],
        impacting_updates: List[ImpactAssessment]
    ) -> List[Dict[str, Any]]:
        """Identify change points not attributable to algorithm updates."""
        explained_dates = {
            pd.to_datetime(impact.update_date).date()
            for impact in impacting_updates
        }
        
        unexplained = []
        for cp in change_points:
            cp_date = pd.to_datetime(cp['date']).date()
            
            # Check if any explained update is within 7 days
            is_explained = any(
                abs((cp_date - exp_date).days) <= 7
                for exp_date in explained_dates
            )
            
            if not is_explained:
                unexplained.append({
                    'date': cp['date'],
                    'magnitude': cp.get('magnitude', 0.0),
                    'direction': cp.get('direction', 'unknown'),
                    'possible_causes': [
                        "manual_action",
                        "technical_issue",
                        "competitor_movement",
                        "seasonal_effect",
                        "external_link_change"
                    ]
                })
        
        return unexplained
    
    def _calculate_vulnerability(
        self,
        impacting_updates: List[ImpactAssessment],
        page_metadata: Optional[pd.DataFrame]
    ) -> tuple[float, List[str]]:
        """Calculate algorithmic vulnerability score (0-1) and contributing factors."""
        factors = []
        score_components = []
        
        # Factor 1: Frequency of negative impacts (weight: 0.3)
        negative_impacts = [
            imp for imp in impacting_updates
            if imp.site_impact == "negative"
        ]
        
        if len(impacting_updates) > 0:
            negative_frequency = len(negative_impacts) / len(impacting_updates)
            score_components.append(negative_frequency * 0.3)
            
            if negative_frequency > 0.5:
                factors.append("high_negative_update_frequency")
        else:
            score_components.append(0.0)
        
        # Factor 2: Severity of impacts (weight: 0.25)
        if negative_impacts:
            avg_negative_impact = np.mean([
                abs(imp.click_change_pct) for imp in negative_impacts
            ])
            severity_score = min(avg_negative_impact / 30.0, 1.0)  # 30% = max
            score_components.append(severity_score * 0.25)
            
            if avg_negative_impact > 15:
                factors.append("severe_impact_history")
        else:
            score_components.append(0.0)
        
        # Factor 3: Recovery rate (weight: 0.25)
        not_recovered = [
            imp for imp in negative_impacts
            if imp.recovery_status == "not_recovered" and imp.days_since_update > 60
        ]
        
        if negative_impacts:
            non_recovery_rate = len(not_recovered) / len(negative_impacts)
            score_components.append(non_recovery_rate * 0.25)
            
            if non_recovery_rate > 0.5:
                factors.append("poor_recovery_rate")
        else:
            score_components.append(0.0)
        
        # Factor 4: Common vulnerability characteristics (weight: 0.2)
        if page_metadata is not None and len(page_metadata) > 0:
            vulnerability_chars = 0
            
            if 'word_count' in page_metadata.columns:
                thin_content_pct = (
                    (page_metadata['word_count'] < 500).sum() / len(page_metadata)
                )
                if thin_content_pct > 0.3:
                    factors.append("high_thin_content_percentage")
                    vulnerability_chars += 1
            
            if 'has_schema' in page_metadata.columns:
                no_schema_pct = (
                    (~page_metadata['has_schema']).sum() / len(page_metadata)
                )
                if no_schema_pct > 0.7:
                    factors.append("low_schema_adoption")
                    vulnerability_chars += 1
            
            char_score = min(vulnerability_chars / 2.0, 1.0)
            score_components.append(char_score * 0.2)
        else:
            score_components.append(0.0)
        
        vulnerability_score = sum(score_components)
        
        # Add overall assessment
        if vulnerability_score > 0.7:
            factors.insert(0, "critically_vulnerable")
        elif vulnerability_score > 0.5:
            factors.insert(0, "highly_vulnerable")
        elif vulnerability_score > 0.3:
            factors.insert(0, "moderately_vulnerable")
        else:
            factors.insert(0, "low_vulnerability")
        
        return vulnerability_score, factors
    
    def _generate_recommendation(
        self,
        impacting_updates: List[ImpactAssessment],
        vulnerability_score: float,
        vulnerability_factors: List[str]
    ) -> str:
        """Generate strategic recommendation based on update history."""
        if not impacting_updates:
            return (
                "Insufficient algorithm update history to generate specific "
                "recommendations. Continue monitoring for future updates."
            )
        
        recommendations = []
        
        # Analyze recent impacts
        recent_negative = [
            imp for imp in impacting_updates[:3]  # Last 3 updates
            if imp.site_impact == "negative"
        ]
        
        if recent_negative:
            # Find most common characteristics across recent negative impacts
            all_chars = []
            for imp in recent_negative:
                all_chars.extend(imp.common_characteristics)
            
            if all_chars:
                from collections import Counter
                char_counts = Counter(all_chars)
                most_common = char_counts.most_common(3)
                
                for char, count in most