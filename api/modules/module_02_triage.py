"""
Module 2: Page-Level Triage
Complete implementation with per-page trend fitting, CTR anomaly detection using PyOD Isolation Forest,
GA4 engagement cross-reference, and priority scoring algorithm.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from scipy import stats
from pyod.models.iforest import IForest
import logging

logger = logging.getLogger(__name__)


class PageTriageAnalyzer:
    """Analyzes page-level performance trends, CTR anomalies, and engagement patterns."""
    
    def __init__(self):
        self.decay_thresholds = {
            'growing': 0.1,
            'stable_upper': 0.1,
            'stable_lower': -0.1,
            'decaying_lower': -0.5,
        }
        self.engagement_thresholds = {
            'bounce_rate': 0.80,
            'avg_session_duration': 30,  # seconds
        }
        
    def analyze(
        self,
        page_daily_data: pd.DataFrame,
        ga4_landing_data: Optional[pd.DataFrame],
        gsc_page_summary: pd.DataFrame
    ) -> Dict[str, Any]:
        """
        Main analysis pipeline for page-level triage.
        
        Args:
            page_daily_data: Daily time series per page (columns: date, page, clicks, impressions, ctr, position)
            ga4_landing_data: GA4 landing page metrics (columns: page, sessions, bounce_rate, avg_session_duration, conversions)
            gsc_page_summary: GSC page summary stats (columns: page, total_clicks, total_impressions, avg_ctr, avg_position)
            
        Returns:
            Dictionary containing page analysis results and summary stats
        """
        logger.info("Starting page-level triage analysis")
        
        try:
            # 1. Per-page trend fitting
            logger.info("Fitting per-page trends")
            page_trends = self._fit_page_trends(page_daily_data)
            
            # 2. CTR anomaly detection
            logger.info("Detecting CTR anomalies")
            ctr_anomalies = self._detect_ctr_anomalies(gsc_page_summary)
            
            # 3. Engagement cross-reference
            logger.info("Cross-referencing engagement data")
            engagement_flags = self._analyze_engagement(gsc_page_summary, ga4_landing_data)
            
            # 4. Priority scoring
            logger.info("Calculating priority scores")
            pages_analyzed = self._calculate_priority_scores(
                page_trends,
                ctr_anomalies,
                engagement_flags,
                gsc_page_summary
            )
            
            # 5. Generate summary statistics
            summary = self._generate_summary(pages_analyzed)
            
            logger.info(f"Analysis complete. Analyzed {len(pages_analyzed)} pages")
            
            return {
                "pages": pages_analyzed,
                "summary": summary,
                "metadata": {
                    "analyzed_at": datetime.utcnow().isoformat(),
                    "total_pages": len(pages_analyzed),
                    "data_date_range": self._get_date_range(page_daily_data)
                }
            }
            
        except Exception as e:
            logger.error(f"Error in page triage analysis: {str(e)}", exc_info=True)
            raise
    
    def _fit_page_trends(self, page_daily_data: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
        """
        Fit linear regression trend for each page with sufficient data.
        
        Returns:
            Dictionary mapping page URL to trend statistics
        """
        trends = {}
        
        # Ensure date column is datetime
        if 'date' in page_daily_data.columns:
            page_daily_data['date'] = pd.to_datetime(page_daily_data['date'])
        
        # Group by page
        for page, group in page_daily_data.groupby('page'):
            # Require at least 30 days of data
            if len(group) < 30:
                continue
            
            # Sort by date
            group = group.sort_values('date')
            
            # Convert dates to numeric (days since first observation)
            group = group.copy()
            first_date = group['date'].min()
            group['days'] = (group['date'] - first_date).dt.days
            
            # Fit linear regression on clicks
            if 'clicks' in group.columns and group['clicks'].sum() > 0:
                X = group['days'].values
                y = group['clicks'].values
                
                # Handle missing or zero values
                valid_mask = ~np.isnan(y) & (y >= 0)
                if valid_mask.sum() < 10:  # Need at least 10 valid points
                    continue
                
                X_valid = X[valid_mask]
                y_valid = y[valid_mask]
                
                try:
                    slope, intercept, r_value, p_value, std_err = stats.linregress(X_valid, y_valid)
                    
                    # Calculate daily change rate
                    avg_clicks = y_valid.mean()
                    daily_change_rate = slope
                    
                    # Calculate current monthly clicks (last 30 days)
                    last_30_days = group[group['days'] >= (group['days'].max() - 30)]
                    current_monthly_clicks = last_30_days['clicks'].sum()
                    
                    # Project when page might fall below page 1 (position 10)
                    current_position = group['position'].tail(30).mean() if 'position' in group.columns else None
                    projected_loss_date = None
                    
                    if current_position and current_position < 10 and slope < 0:
                        # Estimate position decay (simplified)
                        # Assume position worsens proportionally to click decay
                        if avg_clicks > 0:
                            position_slope = slope * (current_position / avg_clicks) * 0.1
                            if position_slope < 0:
                                days_to_position_10 = (10 - current_position) / abs(position_slope)
                                if days_to_position_10 > 0 and days_to_position_10 < 365:
                                    projected_loss_date = (datetime.now() + timedelta(days=days_to_position_10)).date().isoformat()
                    
                    # Classify bucket
                    bucket = self._classify_trend_bucket(daily_change_rate)
                    
                    trends[page] = {
                        'slope': float(daily_change_rate),
                        'intercept': float(intercept),
                        'r_squared': float(r_value ** 2),
                        'p_value': float(p_value),
                        'bucket': bucket,
                        'current_monthly_clicks': int(current_monthly_clicks),
                        'projected_page1_loss_date': projected_loss_date,
                        'avg_position': float(current_position) if current_position else None,
                        'data_points': int(valid_mask.sum())
                    }
                    
                except Exception as e:
                    logger.warning(f"Failed to fit trend for page {page}: {str(e)}")
                    continue
        
        return trends
    
    def _classify_trend_bucket(self, slope: float) -> str:
        """Classify page into trend bucket based on slope."""
        if slope > self.decay_thresholds['growing']:
            return 'growing'
        elif slope >= self.decay_thresholds['stable_lower']:
            return 'stable'
        elif slope >= self.decay_thresholds['decaying_lower']:
            return 'decaying'
        else:
            return 'critical'
    
    def _detect_ctr_anomalies(self, gsc_page_summary: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
        """
        Detect CTR anomalies using Isolation Forest, grouped by position.
        
        Returns:
            Dictionary mapping page URL to CTR anomaly information
        """
        anomalies = {}
        
        if gsc_page_summary.empty:
            return anomalies
        
        # Ensure required columns exist
        required_cols = ['page', 'avg_position', 'avg_ctr', 'total_impressions']
        if not all(col in gsc_page_summary.columns for col in required_cols):
            logger.warning("Missing required columns for CTR anomaly detection")
            return anomalies
        
        # Filter to pages with sufficient impressions
        df = gsc_page_summary[gsc_page_summary['total_impressions'] >= 100].copy()
        
        if len(df) < 10:
            logger.warning("Not enough pages with sufficient impressions for CTR anomaly detection")
            return anomalies
        
        # Round position to group similar positions
        df['position_group'] = df['avg_position'].round().astype(int)
        
        # Detect anomalies within each position group
        for position_group, group in df.groupby('position_group'):
            if len(group) < 5:  # Need at least 5 pages in group
                continue
            
            try:
                # Prepare data for Isolation Forest
                X = group[['avg_ctr']].values
                
                # Configure and fit Isolation Forest
                clf = IForest(contamination=0.1, random_state=42, n_estimators=100)
                clf.fit(X)
                
                # Get anomaly predictions (-1 for anomalies, 1 for normal)
                predictions = clf.predict(X)
                anomaly_scores = clf.decision_function(X)
                
                # Calculate expected CTR as median of normal points
                normal_mask = predictions == 1
                if normal_mask.sum() > 0:
                    expected_ctr = group.loc[normal_mask, 'avg_ctr'].median()
                else:
                    expected_ctr = group['avg_ctr'].median()
                
                # Store anomalies
                for idx, (_, row) in enumerate(group.iterrows()):
                    if predictions[idx] == -1:  # Anomaly detected
                        anomalies[row['page']] = {
                            'ctr_anomaly': True,
                            'ctr_expected': float(expected_ctr),
                            'ctr_actual': float(row['avg_ctr']),
                            'ctr_deviation': float(row['avg_ctr'] - expected_ctr),
                            'anomaly_score': float(anomaly_scores[idx]),
                            'position_group': int(position_group)
                        }
                
            except Exception as e:
                logger.warning(f"Failed to detect anomalies for position group {position_group}: {str(e)}")
                continue
        
        return anomalies
    
    def _analyze_engagement(
        self,
        gsc_page_summary: pd.DataFrame,
        ga4_landing_data: Optional[pd.DataFrame]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Cross-reference GSC pages with GA4 engagement metrics.
        
        Returns:
            Dictionary mapping page URL to engagement flags
        """
        engagement_flags = {}
        
        if ga4_landing_data is None or ga4_landing_data.empty:
            logger.warning("No GA4 data available for engagement analysis")
            return engagement_flags
        
        # Normalize page URLs for matching
        gsc_pages = gsc_page_summary.copy()
        ga4_pages = ga4_landing_data.copy()
        
        # Ensure 'page' column exists and normalize
        if 'page' not in gsc_pages.columns or 'page' not in ga4_pages.columns:
            logger.warning("Missing 'page' column in GSC or GA4 data")
            return engagement_flags
        
        gsc_pages['page_normalized'] = gsc_pages['page'].str.lower().str.strip()
        ga4_pages['page_normalized'] = ga4_pages['page'].str.lower().str.strip()
        
        # Merge datasets
        merged = gsc_pages.merge(
            ga4_pages,
            on='page_normalized',
            how='left',
            suffixes=('_gsc', '_ga4')
        )
        
        # Analyze engagement for each page
        for _, row in merged.iterrows():
            page = row['page_gsc']
            
            # Check if GA4 data exists
            if pd.isna(row.get('sessions', np.nan)):
                continue
            
            flags = {
                'has_ga4_data': True,
                'sessions': int(row.get('sessions', 0)),
                'bounce_rate': float(row.get('bounce_rate', 0)) if not pd.isna(row.get('bounce_rate')) else None,
                'avg_session_duration': float(row.get('avg_session_duration', 0)) if not pd.isna(row.get('avg_session_duration')) else None,
                'engagement_issues': []
            }
            
            # Flag high bounce rate
            if flags['bounce_rate'] and flags['bounce_rate'] > self.engagement_thresholds['bounce_rate']:
                flags['engagement_issues'].append('high_bounce_rate')
            
            # Flag low session duration
            if flags['avg_session_duration'] and flags['avg_session_duration'] < self.engagement_thresholds['avg_session_duration']:
                flags['engagement_issues'].append('low_session_duration')
            
            # Determine overall engagement flag
            if len(flags['engagement_issues']) >= 2:
                flags['engagement_flag'] = 'low_engagement'
            elif len(flags['engagement_issues']) == 1:
                flags['engagement_flag'] = 'moderate_engagement'
            else:
                flags['engagement_flag'] = 'good_engagement'
            
            engagement_flags[page] = flags
        
        return engagement_flags
    
    def _calculate_priority_scores(
        self,
        page_trends: Dict[str, Dict[str, Any]],
        ctr_anomalies: Dict[str, Dict[str, Any]],
        engagement_flags: Dict[str, Dict[str, Any]],
        gsc_page_summary: pd.DataFrame
    ) -> List[Dict[str, Any]]:
        """
        Calculate priority scores and recommended actions for each page.
        
        Priority scoring algorithm:
        score = (current_monthly_clicks × abs(decay_rate)) × recoverability_factor
        
        Returns:
            List of page analysis dictionaries
        """
        pages_analyzed = []
        
        # Create lookup dictionary from GSC summary
        gsc_lookup = {}
        for _, row in gsc_page_summary.iterrows():
            gsc_lookup[row['page']] = row.to_dict()
        
        # Combine all page data
        all_pages = set(page_trends.keys()) | set(ctr_anomalies.keys()) | set(engagement_flags.keys())
        
        for page in all_pages:
            trend = page_trends.get(page, {})
            anomaly = ctr_anomalies.get(page, {})
            engagement = engagement_flags.get(page, {})
            gsc_data = gsc_lookup.get(page, {})
            
            # Skip if no meaningful data
            if not trend and not anomaly and not engagement:
                continue
            
            # Build page analysis object
            page_data = {
                'url': page,
                'bucket': trend.get('bucket', 'unknown'),
                'current_monthly_clicks': trend.get('current_monthly_clicks', 0),
                'trend_slope': trend.get('slope'),
                'projected_page1_loss_date': trend.get('projected_page1_loss_date'),
                'ctr_anomaly': anomaly.get('ctr_anomaly', False),
                'ctr_expected': anomaly.get('ctr_expected'),
                'ctr_actual': anomaly.get('ctr_actual'),
                'engagement_flag': engagement.get('engagement_flag'),
                'avg_position': trend.get('avg_position') or gsc_data.get('avg_position')
            }
            
            # Calculate recoverability factor
            recoverability = self._calculate_recoverability(page_data, trend, anomaly, engagement)
            
            # Calculate priority score
            monthly_clicks = page_data['current_monthly_clicks']
            decay_rate = abs(trend.get('slope', 0))
            
            priority_score = monthly_clicks * decay_rate * recoverability
            page_data['priority_score'] = round(priority_score, 1)
            page_data['recoverability_factor'] = round(recoverability, 2)
            
            # Determine recommended action
            page_data['recommended_action'] = self._determine_recommended_action(page_data, anomaly, engagement)
            
            pages_analyzed.append(page_data)
        
        # Sort by priority score descending
        pages_analyzed.sort(key=lambda x: x['priority_score'], reverse=True)
        
        return pages_analyzed
    
    def _calculate_recoverability(
        self,
        page_data: Dict[str, Any],
        trend: Dict[str, Any],
        anomaly: Dict[str, Any],
        engagement: Dict[str, Any]
    ) -> float:
        """
        Calculate recoverability factor based on multiple signals.
        
        Higher score = easier to recover
        """
        factor = 1.0
        
        # CTR anomaly = easy fix (title/snippet rewrite)
        if anomaly.get('ctr_anomaly'):
            factor *= 1.5
        
        # Recent decay = easier to recover
        if trend.get('data_points', 0) < 60:  # Decay started recently
            factor *= 1.3
        
        # Position-based recoverability
        avg_position = page_data.get('avg_position')
        if avg_position:
            if avg_position <= 10:  # Already on page 1
                factor *= 1.4
            elif avg_position <= 20:  # Page 2
                factor *= 1.2
            elif avg_position > 30:  # Deep positions
                factor *= 0.7
        
        # Engagement issues = harder fix (content problem)
        if engagement.get('engagement_flag') == 'low_engagement':
            factor *= 0.8
        
        # Bucket-based adjustment
        bucket = page_data.get('bucket')
        if bucket == 'critical':
            factor *= 0.9  # Severe decay may indicate structural issues
        elif bucket == 'growing':
            factor *= 1.1  # Already has momentum
        
        return max(factor, 0.1)  # Floor at 0.1
    
    def _determine_recommended_action(
        self,
        page_data: Dict[str, Any],
        anomaly: Dict[str, Any],
        engagement: Dict[str, Any]
    ) -> str:
        """Determine the primary recommended action for a page."""
        
        # Priority: CTR anomaly > engagement issues > content update > monitoring
        
        if anomaly.get('ctr_anomaly'):
            return 'title_rewrite'
        
        engagement_flag = engagement.get('engagement_flag')
        if engagement_flag == 'low_engagement':
            issues = engagement.get('engagement_issues', [])
            if 'high_bounce_rate' in issues and 'low_session_duration' in issues:
                return 'content_overhaul'
            else:
                return 'content_enhancement'
        
        bucket = page_data.get('bucket')
        if bucket == 'critical':
            return 'urgent_content_update'
        elif bucket == 'decaying':
            return 'content_refresh'
        elif bucket == 'growing':
            return 'double_down'
        else:
            return 'monitor'
    
    def _generate_summary(self, pages_analyzed: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate summary statistics from analyzed pages."""
        
        total_pages = len(pages_analyzed)
        
        # Count by bucket
        bucket_counts = {
            'growing': 0,
            'stable': 0,
            'decaying': 0,
            'critical': 0,
            'unknown': 0
        }
        
        for page in pages_analyzed:
            bucket = page.get('bucket', 'unknown')
            if bucket in bucket_counts:
                bucket_counts[bucket] += 1
        
        # Calculate total recoverable clicks
        recoverable_clicks = sum(
            page['current_monthly_clicks']
            for page in pages_analyzed
            if page.get('bucket') in ['decaying', 'critical']
            and page.get('recoverability_factor', 0) > 0.5
        )
        
        # Count pages with various issues
        ctr_anomalies = sum(1 for page in pages_analyzed if page.get('ctr_anomaly'))
        engagement_issues = sum(
            1 for page in pages_analyzed
            if page.get('engagement_flag') in ['low_engagement', 'moderate_engagement']
        )
        
        return {
            'total_pages_analyzed': total_pages,
            'growing': bucket_counts['growing'],
            'stable': bucket_counts['stable'],
            'decaying': bucket_counts['decaying'],
            'critical': bucket_counts['critical'],
            'total_recoverable_clicks_monthly': int(recoverable_clicks),
            'pages_with_ctr_anomalies': ctr_anomalies,
            'pages_with_engagement_issues': engagement_issues,
            'avg_priority_score': round(np.mean([p['priority_score'] for p in pages_analyzed]), 1) if pages_analyzed else 0
        }
    
    def _get_date_range(self, page_daily_data: pd.DataFrame) -> Dict[str, str]:
        """Extract date range from time series data."""
        if page_daily_data.empty or 'date' not in page_daily_data.columns:
            return {'start': None, 'end': None}
        
        dates = pd.to_datetime(page_daily_data['date'])
        return {
            'start': dates.min().date().isoformat(),
            'end': dates.max().date().isoformat()
        }


def analyze_page_triage(
    page_daily_data: pd.DataFrame,
    ga4_landing_data: Optional[pd.DataFrame] = None,
    gsc_page_summary: Optional[pd.DataFrame] = None
) -> Dict[str, Any]:
    """
    Main entry point for Module 2: Page-Level Triage analysis.
    
    Args:
        page_daily_data: DataFrame with columns [date, page, clicks, impressions, ctr, position]
        ga4_landing_data: Optional DataFrame with columns [page, sessions, bounce_rate, avg_session_duration, conversions]
        gsc_page_summary: Optional DataFrame with columns [page, total_clicks, total_impressions, avg_ctr, avg_position]
    
    Returns:
        Dictionary containing:
        - pages: List of analyzed pages with metrics and recommendations
        - summary: Overall statistics and counts
        - metadata: Analysis metadata
    """
    # If gsc_page_summary not provided, generate from daily data
    if gsc_page_summary is None and not page_daily_data.empty:
        gsc_page_summary = page_daily_data.groupby('page').agg({
            'clicks': 'sum',
            'impressions': 