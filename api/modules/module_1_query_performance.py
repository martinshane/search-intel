"""
Module 1: Query Performance Analysis
Clusters queries by performance tier, calculates traffic concentration, 
identifies seasonal patterns, and generates actionable insights.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from collections import defaultdict
import logging
from dataclasses import dataclass, asdict

logger = logging.getLogger(__name__)


@dataclass
class QueryMetrics:
    """Metrics for a single query"""
    query: str
    clicks: int
    impressions: int
    ctr: float
    position: float
    monthly_clicks: int
    click_share: float
    impression_share: float


@dataclass
class PerformanceTier:
    """Performance tier with queries and metrics"""
    tier_name: str
    queries: List[QueryMetrics]
    total_clicks: int
    total_impressions: int
    avg_ctr: float
    avg_position: float
    click_share: float
    query_count: int


@dataclass
class SeasonalPattern:
    """Seasonal pattern detected in query data"""
    query: str
    pattern_type: str  # weekly, monthly, yearly
    peak_period: str
    trough_period: str
    variation_pct: float
    confidence: float


@dataclass
class TrafficConcentration:
    """Traffic concentration metrics"""
    top_10_click_share: float
    top_20_click_share: float
    top_50_click_share: float
    gini_coefficient: float
    herfindahl_index: float
    concentration_risk: str  # low, medium, high


class QueryPerformanceAnalyzer:
    """Main analyzer for query performance metrics"""
    
    # Performance tier thresholds
    HERO_CTR_THRESHOLD = 0.15  # Top 15% CTR for position range
    HERO_MIN_CLICKS = 100  # Minimum monthly clicks
    STRONG_CTR_THRESHOLD = 0.08
    STRONG_MIN_CLICKS = 20
    OPPORTUNITY_POSITION_MAX = 20  # Striking distance
    OPPORTUNITY_MIN_IMPRESSIONS = 100
    
    def __init__(self, gsc_data: pd.DataFrame, date_range_days: int = 90):
        """
        Initialize analyzer with GSC data
        
        Args:
            gsc_data: DataFrame with columns [query, date, clicks, impressions, ctr, position]
            date_range_days: Number of days to analyze (default 90)
        """
        self.gsc_data = gsc_data.copy()
        self.date_range_days = date_range_days
        self._prepare_data()
        
    def _prepare_data(self):
        """Prepare and validate data"""
        required_cols = ['query', 'date', 'clicks', 'impressions', 'ctr', 'position']
        
        if not all(col in self.gsc_data.columns for col in required_cols):
            raise ValueError(f"GSC data must contain columns: {required_cols}")
        
        # Convert date to datetime
        self.gsc_data['date'] = pd.to_datetime(self.gsc_data['date'])
        
        # Filter to date range
        end_date = self.gsc_data['date'].max()
        start_date = end_date - timedelta(days=self.date_range_days)
        self.gsc_data = self.gsc_data[self.gsc_data['date'] >= start_date].copy()
        
        # Calculate daily averages per query
        self.query_summary = self._calculate_query_summary()
        
        logger.info(f"Prepared data: {len(self.query_summary)} queries, "
                   f"{len(self.gsc_data)} daily records")
    
    def _calculate_query_summary(self) -> pd.DataFrame:
        """Calculate summary metrics per query"""
        summary = self.gsc_data.groupby('query').agg({
            'clicks': 'sum',
            'impressions': 'sum',
            'position': 'mean'
        }).reset_index()
        
        # Calculate CTR from totals
        summary['ctr'] = summary['clicks'] / summary['impressions'].replace(0, 1)
        
        # Monthly projection (from daily average)
        days_in_data = (self.gsc_data['date'].max() - self.gsc_data['date'].min()).days + 1
        summary['monthly_clicks'] = (summary['clicks'] / days_in_data) * 30
        
        return summary
    
    def analyze(self) -> Dict[str, Any]:
        """
        Run complete query performance analysis
        
        Returns:
            Dictionary with performance tiers, concentration metrics, 
            seasonal patterns, and recommendations
        """
        logger.info("Starting query performance analysis...")
        
        # Calculate traffic shares
        total_clicks = self.query_summary['clicks'].sum()
        total_impressions = self.query_summary['impressions'].sum()
        
        self.query_summary['click_share'] = (
            self.query_summary['clicks'] / total_clicks
        )
        self.query_summary['impression_share'] = (
            self.query_summary['impressions'] / total_impressions
        )
        
        # Sort by clicks
        self.query_summary = self.query_summary.sort_values(
            'clicks', ascending=False
        ).reset_index(drop=True)
        
        # Classify queries into tiers
        tiers = self._classify_performance_tiers()
        
        # Calculate traffic concentration
        concentration = self._calculate_traffic_concentration()
        
        # Identify seasonal patterns
        seasonal_patterns = self._identify_seasonal_patterns()
        
        # Generate insights and recommendations
        insights = self._generate_insights(tiers, concentration, seasonal_patterns)
        
        # Compile results
        results = {
            'summary': {
                'total_queries': len(self.query_summary),
                'total_clicks': int(total_clicks),
                'total_impressions': int(total_impressions),
                'avg_ctr': float(total_clicks / total_impressions) if total_impressions > 0 else 0.0,
                'avg_position': float(self.query_summary['position'].mean()),
                'date_range_days': self.date_range_days,
                'analysis_date': datetime.now().isoformat()
            },
            'performance_tiers': {
                tier.tier_name: self._tier_to_dict(tier) for tier in tiers
            },
            'traffic_concentration': asdict(concentration),
            'seasonal_patterns': [asdict(p) for p in seasonal_patterns],
            'insights': insights
        }
        
        logger.info("Query performance analysis complete")
        return results
    
    def _classify_performance_tiers(self) -> List[PerformanceTier]:
        """Classify queries into performance tiers"""
        logger.info("Classifying queries into performance tiers...")
        
        hero_queries = []
        strong_queries = []
        opportunity_queries = []
        underperforming_queries = []
        
        for _, row in self.query_summary.iterrows():
            query_metrics = QueryMetrics(
                query=row['query'],
                clicks=int(row['clicks']),
                impressions=int(row['impressions']),
                ctr=float(row['ctr']),
                position=float(row['position']),
                monthly_clicks=int(row['monthly_clicks']),
                click_share=float(row['click_share']),
                impression_share=float(row['impression_share'])
            )
            
            # Get expected CTR for position
            expected_ctr = self._get_expected_ctr(row['position'])
            ctr_performance = row['ctr'] / expected_ctr if expected_ctr > 0 else 0
            
            # Classify
            if (row['monthly_clicks'] >= self.HERO_MIN_CLICKS and 
                ctr_performance >= 1.5):  # 50% better than expected
                hero_queries.append(query_metrics)
            
            elif (row['monthly_clicks'] >= self.STRONG_MIN_CLICKS and 
                  ctr_performance >= 1.0):  # Meeting or exceeding expected
                strong_queries.append(query_metrics)
            
            elif (row['position'] >= 8 and 
                  row['position'] <= self.OPPORTUNITY_POSITION_MAX and
                  row['impressions'] >= self.OPPORTUNITY_MIN_IMPRESSIONS):
                opportunity_queries.append(query_metrics)
            
            else:
                # Underperforming: either low CTR or poor position with decent volume
                if (row['impressions'] >= self.OPPORTUNITY_MIN_IMPRESSIONS and
                    (ctr_performance < 0.7 or row['position'] > self.OPPORTUNITY_POSITION_MAX)):
                    underperforming_queries.append(query_metrics)
        
        # Create tier objects
        tiers = []
        for tier_name, queries in [
            ('hero', hero_queries),
            ('strong', strong_queries),
            ('opportunity', opportunity_queries),
            ('underperforming', underperforming_queries)
        ]:
            if queries:
                tier = self._create_tier_object(tier_name, queries)
                tiers.append(tier)
        
        logger.info(f"Classified queries - Hero: {len(hero_queries)}, "
                   f"Strong: {len(strong_queries)}, "
                   f"Opportunity: {len(opportunity_queries)}, "
                   f"Underperforming: {len(underperforming_queries)}")
        
        return tiers
    
    def _create_tier_object(self, tier_name: str, 
                           queries: List[QueryMetrics]) -> PerformanceTier:
        """Create a PerformanceTier object from query list"""
        total_clicks = sum(q.clicks for q in queries)
        total_impressions = sum(q.impressions for q in queries)
        
        return PerformanceTier(
            tier_name=tier_name,
            queries=queries,
            total_clicks=total_clicks,
            total_impressions=total_impressions,
            avg_ctr=total_clicks / total_impressions if total_impressions > 0 else 0.0,
            avg_position=sum(q.position for q in queries) / len(queries),
            click_share=sum(q.click_share for q in queries),
            query_count=len(queries)
        )
    
    def _get_expected_ctr(self, position: float) -> float:
        """
        Get expected CTR for a given position based on industry benchmarks
        Uses Advanced Web Ranking CTR curve data
        """
        # Position-based CTR curve (approximate)
        ctr_curve = {
            1: 0.28, 2: 0.15, 3: 0.11, 4: 0.08, 5: 0.07,
            6: 0.05, 7: 0.04, 8: 0.03, 9: 0.03, 10: 0.025
        }
        
        # Round position to nearest integer
        pos_int = int(round(position))
        
        if pos_int <= 10:
            return ctr_curve.get(pos_int, 0.025)
        elif pos_int <= 20:
            return 0.01  # Page 2 average
        else:
            return 0.005  # Page 3+ average
    
    def _calculate_traffic_concentration(self) -> TrafficConcentration:
        """Calculate traffic concentration metrics"""
        logger.info("Calculating traffic concentration metrics...")
        
        # Sort by clicks descending (already done, but ensure)
        sorted_queries = self.query_summary.sort_values('clicks', ascending=False)
        
        # Calculate cumulative click shares
        total_clicks = sorted_queries['clicks'].sum()
        cumulative_clicks = sorted_queries['clicks'].cumsum()
        
        # Top N shares
        top_10_share = 0.0
        top_20_share = 0.0
        top_50_share = 0.0
        
        if len(sorted_queries) >= 10:
            top_10_share = cumulative_clicks.iloc[9] / total_clicks
        if len(sorted_queries) >= 20:
            top_20_share = cumulative_clicks.iloc[19] / total_clicks
        if len(sorted_queries) >= 50:
            top_50_share = cumulative_clicks.iloc[49] / total_clicks
        
        # Gini coefficient
        gini = self._calculate_gini_coefficient(sorted_queries['clicks'].values)
        
        # Herfindahl-Hirschman Index (HHI)
        click_shares = sorted_queries['clicks'] / total_clicks
        hhi = (click_shares ** 2).sum()
        
        # Determine concentration risk
        if top_10_share > 0.7 or hhi > 0.15:
            risk = "high"
        elif top_10_share > 0.5 or hhi > 0.10:
            risk = "medium"
        else:
            risk = "low"
        
        return TrafficConcentration(
            top_10_click_share=float(top_10_share),
            top_20_click_share=float(top_20_share),
            top_50_click_share=float(top_50_share),
            gini_coefficient=float(gini),
            herfindahl_index=float(hhi),
            concentration_risk=risk
        )
    
    def _calculate_gini_coefficient(self, values: np.ndarray) -> float:
        """Calculate Gini coefficient for inequality measurement"""
        if len(values) == 0:
            return 0.0
        
        # Sort values
        sorted_values = np.sort(values)
        n = len(sorted_values)
        
        # Calculate Gini
        cumsum = np.cumsum(sorted_values)
        gini = (2 * np.sum((np.arange(1, n + 1)) * sorted_values)) / (n * cumsum[-1]) - (n + 1) / n
        
        return float(gini)
    
    def _identify_seasonal_patterns(self) -> List[SeasonalPattern]:
        """Identify seasonal patterns in query performance"""
        logger.info("Identifying seasonal patterns...")
        
        patterns = []
        
        # Only analyze queries with sufficient data
        top_queries = self.query_summary.nlargest(50, 'clicks')['query'].tolist()
        
        for query in top_queries:
            query_data = self.gsc_data[self.gsc_data['query'] == query].copy()
            
            if len(query_data) < 28:  # Need at least 4 weeks
                continue
            
            # Weekly pattern detection
            weekly_pattern = self._detect_weekly_pattern(query_data)
            if weekly_pattern:
                patterns.append(weekly_pattern)
            
            # Monthly pattern detection (if enough data)
            if len(query_data) >= 60:
                monthly_pattern = self._detect_monthly_pattern(query_data)
                if monthly_pattern:
                    patterns.append(monthly_pattern)
        
        logger.info(f"Identified {len(patterns)} seasonal patterns")
        return patterns
    
    def _detect_weekly_pattern(self, query_data: pd.DataFrame) -> Optional[SeasonalPattern]:
        """Detect day-of-week patterns"""
        query_data['day_of_week'] = query_data['date'].dt.day_name()
        
        # Average clicks by day of week
        daily_avg = query_data.groupby('day_of_week')['clicks'].mean()
        
        if len(daily_avg) < 7:
            return None
        
        # Calculate variation
        max_day = daily_avg.idxmax()
        min_day = daily_avg.idxmin()
        variation = ((daily_avg.max() - daily_avg.min()) / daily_avg.mean()) * 100
        
        # Only report if variation is significant
        if variation < 20:  # Less than 20% variation
            return None
        
        # Calculate confidence based on consistency
        query_data['week'] = query_data['date'].dt.isocalendar().week
        weekly_patterns = query_data.groupby(['week', 'day_of_week'])['clicks'].mean().unstack(fill_value=0)
        
        if len(weekly_patterns) < 2:
            confidence = 0.5
        else:
            # Consistency across weeks
            correlations = []
            for i in range(len(weekly_patterns) - 1):
                corr = np.corrcoef(weekly_patterns.iloc[i], weekly_patterns.iloc[i + 1])[0, 1]
                if not np.isnan(corr):
                    correlations.append(corr)
            confidence = np.mean(correlations) if correlations else 0.5
        
        return SeasonalPattern(
            query=query_data['query'].iloc[0],
            pattern_type='weekly',
            peak_period=max_day,
            trough_period=min_day,
            variation_pct=float(variation),
            confidence=float(confidence)
        )
    
    def _detect_monthly_pattern(self, query_data: pd.DataFrame) -> Optional[SeasonalPattern]:
        """Detect day-of-month patterns"""
        query_data['day_of_month'] = query_data['date'].dt.day
        
        # Group into periods (beginning, middle, end of month)
        def month_period(day):
            if day <= 10:
                return 'beginning'
            elif day <= 20:
                return 'middle'
            else:
                return 'end'
        
        query_data['month_period'] = query_data['day_of_month'].apply(month_period)
        
        # Average clicks by period
        period_avg = query_data.groupby('month_period')['clicks'].mean()
        
        if len(period_avg) < 3:
            return None
        
        max_period = period_avg.idxmax()
        min_period = period_avg.idxmin()
        variation = ((period_avg.max() - period_avg.min()) / period_avg.mean()) * 100
        
        # Only report if variation is significant
        if variation < 25:
            return None
        
        # Simple confidence based on data volume
        confidence = min(len(query_data) / 90, 1.0)  # Max confidence at 90 days
        
        return SeasonalPattern(
            query=query_data['query'].iloc[0],
            pattern_type='monthly',
            peak_period=f'{max_period}_of_month',
            trough_period=f'{min_period}_of_month',
            variation_pct=float(variation),
            confidence=float(confidence)
        )
    
    def _generate_insights(self, tiers: List[PerformanceTier],
                          concentration: TrafficConcentration,
                          patterns: List[SeasonalPattern]) -> Dict[str, Any]:
        """Generate actionable insights and recommendations"""
        logger.info("Generating insights and recommendations...")
        
        insights = {
            'key_findings': [],
            'recommendations': [],
            'priorities': []
        }
        
        # Traffic concentration insights
        if concentration.concentration_risk == 'high':
            insights['key_findings'].append({
                'type': 'concentration_risk',
                'severity': 'high',
                'message': f"High traffic concentration: Top 10 queries drive {concentration.top_10_click_share*100:.1f}% of clicks",
                'detail': "Site is vulnerable to algorithm updates or SERP changes affecting top queries"
            })
            insights['recommendations'].append({
                'action': 'diversify_traffic',
                'priority': 'high',
                'description': "Expand content to rank for more queries and reduce dependency on top performers",
                'estimated_impact': 'Reduce risk of 50%+ traffic loss from single update'
            })
        
        # Hero query insights
        hero_tier = next((t for t in tiers if t.tier_name == 'hero'), None)
        if hero_tier and hero_tier.query_count > 0:
            insights['key_findings'].append({
                'type': 'hero_queries',
                'severity': 'success',
                'message': f"{hero_tier.query_count} hero queries driving {hero_tier.click_share*100:.1f}% of traffic",
                'detail': f"These queries have exceptional CTR ({hero_tier.avg_ctr*100:.1f}%) and strong positions"
            })
            insights['recommendations'].append({
                'action': 'protect_hero_queries',
                'priority': 'high',
                'description': "Monitor hero queries daily and maintain content freshness",
                'estimated_impact': f"Preserve {hero_tier.total_clicks} monthly clicks"
            })
        
        # Opportunity query insights
        opportunity_tier = next((t for t in tiers if t.tier_name == 'opportunity'), None)
        if opportunity_tier and opportunity_tier.query_count > 0:
            # Estimate potential gain (moving from avg position to position 5)
            potential_clicks = 0
            for query in opportunity_tier.queries[:10]:  # Top 10 opportunities
                current_ctr = query.ctr
                target_ctr = self._get_expected_ctr(5.0)
                potential_clicks += query.impressions * (target_ctr - current_ctr)
            
            insights['key_findings'].append({
                'type': 'opportunity_queries',
                'severity': 'opportunity',
                'message': f"{opportunity_tier.query_count} queries in striking distance (positions 8-20)",
                'detail': f"Average position: {opportunity_tier.avg_position:.1f}, driving {opportunity_tier.total_clicks} clicks/month"
            })
            insights['recommendations'].append({
                'action': 'optimize_striking_distance',
                'priority': 'high',
                'description': "Focus on improving content and links for top opportunity queries",
                'estimated_impact': f"Potential to gain {int(potential_clicks)} monthly clicks"
            })
        
        # Underperforming query insights
        underperforming_tier = next((t for t in tiers if t.tier_name == 'underperforming'), None)
        if underperforming_tier and underperforming_tier.query_count > 0:
            # Calculate CTR gap
            expected_ctr = sum(
                self._get_expected_ctr(q.position) * q.impressions 
                for q in underperforming_tier.queries
            ) / sum(q.impressions for q in underperforming_tier.queries)
            
            ctr_gap = expected_ctr - underperforming_tier.avg_ctr
            
            if ctr_gap > 0.01:  # Significant CTR underperformance
                potential_clicks = sum(q.impressions for q in underperforming_tier.queries) * ctr_gap
                
                insights['key_findings'].append({
                    'type': 'ctr_underperformance',
                    'severity': 'warning',
                    'message': f"{underperforming_tier.query_count} queries underperforming on CTR",
                    'detail': f"Average CTR {underperforming_tier.avg_ctr*100:.2f}% vs expected {expected_ctr*100:.2f}%"
                })
                insights['recommendations'].append({
                    'action': 'improve_titles_descriptions',
                    'priority': 'medium',
                    'description': "Rewrite titles and meta descriptions for underperforming queries",
                    'estimated_impact': f"Potential to gain {int(potential_clicks)} monthly clicks through CTR optimization"
                })
        
        # Seasonal pattern insights
        strong_patterns = [p for p in patterns if p.confidence > 0.6 and p.variation_pct > 30]
        if strong_patterns:
            weekly_patterns = [p for p in strong_patterns if p.pattern_type == 'weekly']
            monthly_patterns = [p for p in strong_patterns if p.pattern_type == 'monthly']
            
            if weekly_patterns:
                insights['key_findings'].append({
                    'type': 'weekly_seasonality',
                    'severity': 'info',
                    'message': f"{len(weekly_patterns)} queries show strong day-of-week patterns",
                    'detail': "Traffic varies by 30%+ across different days"
                })
                insights['recommendations'].append({
                    'action': 'schedule_content_updates',
                    'priority': 'low',
                    'description': "Schedule important content updates and promotions for peak traffic days",
                    'estimated_impact': "Maximize visibility of time-sensitive content"
                })
            
            if monthly_patterns:
                insights['key_findings'].append({
                    'type': 'monthly_seasonality',
                    'severity': 'info',
                    'message': f"{len(monthly_patterns)} queries show monthly cycle patterns",
                    'detail': "Consider budget and inventory planning around these cycles"
                })
        
        # Prioritize actions
        high_priority = [r for r in insights['recommendations'] if r['priority'] == 'high']
        medium_priority = [r for r in insights['recommendations'] if r['priority'] == 'medium']
        low_priority = [r for r in insights['recommendations'] if r['priority'] == 'low']
        
        insights['priorities'] = {
            'immediate': high_priority,
            'this_month': medium_priority,
            'ongoing': low_priority
        }
        
        return insights
    
    def _tier_to_dict(self, tier: PerformanceTier) -> Dict[str, Any]:
        """Convert PerformanceTier to dictionary with top queries"""
        return {
            'total_clicks': tier.total_clicks,
            'total_impressions': tier.total_impressions,
            'avg_ctr': tier.avg_ctr,
            'avg_position': tier.avg_position,
            'click_share': tier.click_share,
            'query_count': tier.query_count,
            'top_queries': [
                {
                    'query': q.query,
                    'clicks': q.clicks,
                    'impressions': q.impressions,
                    'ctr': q.ctr,
                    'position': q.position,
                    'monthly_clicks': q.monthly_clicks
                }
                for q in sorted(tier.queries, key=lambda x: x.clicks, reverse=True)[:20]
            ]
        }


def analyze_query_performance(gsc_data: pd.DataFrame, 
                              date_range_days: int = 90) -> Dict[str, Any]:
    """
    Main entry point for query performance analysis
    
    Args:
        gsc_data: DataFrame with GSC query data
        date_range_days: Number of days to analyze
    
    Returns:
        Dictionary with complete analysis results
    """
    try:
        analyzer = QueryPerformanceAnalyzer(gsc_data, date_range_days)
        results = analyzer.analyze()
        return results
    except Exception as e:
        logger.error(f"Query performance analysis failed: {str(e)}", exc_info=True)
        raise

