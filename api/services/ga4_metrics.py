"""
GA4 Metrics Service

Extracts Core Web Vitals and performance metrics from GA4 API responses.
Handles metric aggregation, percentile calculations, and mobile vs desktop breakdowns.
"""

from typing import Dict, List, Optional, Any
import numpy as np
import pandas as pd
from datetime import datetime, timedelta


class GA4MetricsService:
    """Service for extracting and processing GA4 performance metrics."""
    
    # Core Web Vitals thresholds (Google-defined)
    CWV_THRESHOLDS = {
        'lcp': {'good': 2500, 'needs_improvement': 4000},  # milliseconds
        'fid': {'good': 100, 'needs_improvement': 300},     # milliseconds
        'cls': {'good': 0.1, 'needs_improvement': 0.25},    # score
        'inp': {'good': 200, 'needs_improvement': 500},     # milliseconds
        'fcp': {'good': 1800, 'needs_improvement': 3000},   # milliseconds
        'ttfb': {'good': 800, 'needs_improvement': 1800}    # milliseconds
    }
    
    def __init__(self):
        """Initialize GA4 metrics service."""
        pass
    
    def extract_core_web_vitals(self, ga4_response: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract Core Web Vitals from GA4 response.
        
        Args:
            ga4_response: Raw GA4 API response with web vitals data
            
        Returns:
            Dictionary with aggregated CWV metrics by device category
        """
        if not ga4_response or 'rows' not in ga4_response:
            return self._empty_cwv_response()
        
        # Parse rows into DataFrame
        df = self._parse_ga4_rows(ga4_response)
        
        if df.empty:
            return self._empty_cwv_response()
        
        # Calculate metrics by device
        metrics = {
            'overall': self._calculate_cwv_metrics(df),
            'mobile': self._calculate_cwv_metrics(
                df[df.get('deviceCategory', '') == 'mobile']
            ),
            'desktop': self._calculate_cwv_metrics(
                df[df.get('deviceCategory', '') == 'desktop']
            ),
            'tablet': self._calculate_cwv_metrics(
                df[df.get('deviceCategory', '') == 'tablet']
            ),
            'by_page': self._calculate_cwv_by_page(df),
            'timestamp': datetime.utcnow().isoformat()
        }
        
        # Add assessments
        metrics['overall']['assessment'] = self._assess_cwv(metrics['overall'])
        metrics['mobile']['assessment'] = self._assess_cwv(metrics['mobile'])
        metrics['desktop']['assessment'] = self._assess_cwv(metrics['desktop'])
        
        return metrics
    
    def extract_engagement_metrics(self, ga4_response: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract engagement metrics from GA4 response.
        
        Args:
            ga4_response: Raw GA4 API response with engagement data
            
        Returns:
            Dictionary with engagement metrics by device and traffic source
        """
        if not ga4_response or 'rows' not in ga4_response:
            return self._empty_engagement_response()
        
        df = self._parse_ga4_rows(ga4_response)
        
        if df.empty:
            return self._empty_engagement_response()
        
        return {
            'overall': self._calculate_engagement_metrics(df),
            'by_device': self._calculate_engagement_by_dimension(df, 'deviceCategory'),
            'by_source': self._calculate_engagement_by_dimension(df, 'sessionSource'),
            'by_medium': self._calculate_engagement_by_dimension(df, 'sessionMedium'),
            'by_page': self._calculate_engagement_by_page(df),
            'timestamp': datetime.utcnow().isoformat()
        }
    
    def extract_landing_page_metrics(self, ga4_response: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract landing page performance metrics.
        
        Args:
            ga4_response: Raw GA4 API response with landing page data
            
        Returns:
            List of landing page metrics dictionaries
        """
        if not ga4_response or 'rows' not in ga4_response:
            return []
        
        df = self._parse_ga4_rows(ga4_response)
        
        if df.empty:
            return []
        
        # Group by landing page
        pages = []
        for page_path, group in df.groupby('landingPage'):
            page_metrics = {
                'page': page_path,
                'sessions': int(group.get('sessions', 0).sum()),
                'users': int(group.get('totalUsers', 0).sum()),
                'new_users': int(group.get('newUsers', 0).sum()),
                'engaged_sessions': int(group.get('engagementRate', 0).sum()),
                'engagement_rate': float(group.get('engagementRate', 0).mean()),
                'avg_session_duration': float(group.get('averageSessionDuration', 0).mean()),
                'bounce_rate': float(group.get('bounceRate', 0).mean()),
                'conversions': int(group.get('conversions', 0).sum()),
                'conversion_rate': float(group.get('conversions', 0).sum() / max(group.get('sessions', 1).sum(), 1)),
                'pageviews': int(group.get('screenPageViews', 0).sum()),
                'by_device': {}
            }
            
            # Device breakdown
            if 'deviceCategory' in group.columns:
                for device, device_group in group.groupby('deviceCategory'):
                    page_metrics['by_device'][device] = {
                        'sessions': int(device_group.get('sessions', 0).sum()),
                        'engagement_rate': float(device_group.get('engagementRate', 0).mean()),
                        'avg_session_duration': float(device_group.get('averageSessionDuration', 0).mean()),
                        'bounce_rate': float(device_group.get('bounceRate', 0).mean())
                    }
            
            pages.append(page_metrics)
        
        # Sort by sessions descending
        pages.sort(key=lambda x: x['sessions'], reverse=True)
        
        return pages
    
    def extract_traffic_sources(self, ga4_response: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract traffic source breakdown.
        
        Args:
            ga4_response: Raw GA4 API response with traffic source data
            
        Returns:
            Dictionary with traffic source metrics
        """
        if not ga4_response or 'rows' not in ga4_response:
            return self._empty_traffic_response()
        
        df = self._parse_ga4_rows(ga4_response)
        
        if df.empty:
            return self._empty_traffic_response()
        
        total_sessions = df.get('sessions', 0).sum()
        
        sources = {
            'total_sessions': int(total_sessions),
            'by_channel': [],
            'by_source': [],
            'by_medium': [],
            'organic_search_breakdown': {},
            'timestamp': datetime.utcnow().isoformat()
        }
        
        # Channel grouping
        if 'sessionDefaultChannelGroup' in df.columns:
            for channel, group in df.groupby('sessionDefaultChannelGroup'):
                channel_sessions = int(group.get('sessions', 0).sum())
                sources['by_channel'].append({
                    'channel': channel,
                    'sessions': channel_sessions,
                    'percentage': float(channel_sessions / total_sessions * 100) if total_sessions > 0 else 0,
                    'users': int(group.get('totalUsers', 0).sum()),
                    'new_users': int(group.get('newUsers', 0).sum()),
                    'engagement_rate': float(group.get('engagementRate', 0).mean()),
                    'avg_session_duration': float(group.get('averageSessionDuration', 0).mean()),
                    'conversions': int(group.get('conversions', 0).sum())
                })
            sources['by_channel'].sort(key=lambda x: x['sessions'], reverse=True)
        
        # Source breakdown
        if 'sessionSource' in df.columns:
            for source, group in df.groupby('sessionSource'):
                source_sessions = int(group.get('sessions', 0).sum())
                sources['by_source'].append({
                    'source': source,
                    'sessions': source_sessions,
                    'percentage': float(source_sessions / total_sessions * 100) if total_sessions > 0 else 0,
                    'engagement_rate': float(group.get('engagementRate', 0).mean()),
                    'conversions': int(group.get('conversions', 0).sum())
                })
            sources['by_source'].sort(key=lambda x: x['sessions'], reverse=True)
            sources['by_source'] = sources['by_source'][:20]  # Top 20
        
        # Medium breakdown
        if 'sessionMedium' in df.columns:
            for medium, group in df.groupby('sessionMedium'):
                medium_sessions = int(group.get('sessions', 0).sum())
                sources['by_medium'].append({
                    'medium': medium,
                    'sessions': medium_sessions,
                    'percentage': float(medium_sessions / total_sessions * 100) if total_sessions > 0 else 0,
                    'engagement_rate': float(group.get('engagementRate', 0).mean())
                })
            sources['by_medium'].sort(key=lambda x: x['sessions'], reverse=True)
        
        # Organic search details
        organic_df = df[
            (df.get('sessionMedium', '') == 'organic') |
            (df.get('sessionDefaultChannelGroup', '') == 'Organic Search')
        ]
        if not organic_df.empty:
            sources['organic_search_breakdown'] = {
                'total_sessions': int(organic_df.get('sessions', 0).sum()),
                'percentage_of_total': float(
                    organic_df.get('sessions', 0).sum() / total_sessions * 100
                ) if total_sessions > 0 else 0,
                'engagement_rate': float(organic_df.get('engagementRate', 0).mean()),
                'avg_session_duration': float(organic_df.get('averageSessionDuration', 0).mean()),
                'conversions': int(organic_df.get('conversions', 0).sum()),
                'by_search_engine': []
            }
            
            # Search engine breakdown
            if 'sessionSource' in organic_df.columns:
                for source, group in organic_df.groupby('sessionSource'):
                    sources['organic_search_breakdown']['by_search_engine'].append({
                        'search_engine': source,
                        'sessions': int(group.get('sessions', 0).sum()),
                        'engagement_rate': float(group.get('engagementRate', 0).mean())
                    })
                sources['organic_search_breakdown']['by_search_engine'].sort(
                    key=lambda x: x['sessions'], reverse=True
                )
        
        return sources
    
    def calculate_time_series(self, ga4_response: Dict[str, Any], 
                             metric: str = 'sessions',
                             dimension: str = 'date') -> List[Dict[str, Any]]:
        """
        Calculate time series data for a given metric.
        
        Args:
            ga4_response: Raw GA4 API response
            metric: Metric name to track
            dimension: Time dimension (date, week, month)
            
        Returns:
            List of time series data points
        """
        if not ga4_response or 'rows' not in ga4_response:
            return []
        
        df = self._parse_ga4_rows(ga4_response)
        
        if df.empty or dimension not in df.columns:
            return []
        
        # Group by time dimension
        time_series = []
        for date_val, group in df.groupby(dimension):
            data_point = {
                'date': str(date_val),
                metric: float(group.get(metric, 0).sum())
            }
            
            # Add additional common metrics
            if 'sessions' in group.columns and metric != 'sessions':
                data_point['sessions'] = int(group.get('sessions', 0).sum())
            if 'totalUsers' in group.columns:
                data_point['users'] = int(group.get('totalUsers', 0).sum())
            if 'engagementRate' in group.columns:
                data_point['engagement_rate'] = float(group.get('engagementRate', 0).mean())
            
            time_series.append(data_point)
        
        # Sort by date
        time_series.sort(key=lambda x: x['date'])
        
        return time_series
    
    def _parse_ga4_rows(self, ga4_response: Dict[str, Any]) -> pd.DataFrame:
        """
        Parse GA4 API response rows into DataFrame.
        
        Args:
            ga4_response: Raw GA4 API response
            
        Returns:
            DataFrame with parsed data
        """
        if 'rows' not in ga4_response:
            return pd.DataFrame()
        
        # Extract dimension and metric headers
        dimension_headers = []
        metric_headers = []
        
        if 'dimensionHeaders' in ga4_response:
            dimension_headers = [h['name'] for h in ga4_response['dimensionHeaders']]
        
        if 'metricHeaders' in ga4_response:
            metric_headers = [h['name'] for h in ga4_response['metricHeaders']]
        
        # Parse rows
        data = []
        for row in ga4_response['rows']:
            row_data = {}
            
            # Add dimensions
            if 'dimensionValues' in row:
                for i, dim_value in enumerate(row['dimensionValues']):
                    if i < len(dimension_headers):
                        row_data[dimension_headers[i]] = dim_value.get('value', '')
            
            # Add metrics
            if 'metricValues' in row:
                for i, metric_value in enumerate(row['metricValues']):
                    if i < len(metric_headers):
                        value = metric_value.get('value', '0')
                        # Try to convert to numeric
                        try:
                            row_data[metric_headers[i]] = float(value)
                        except (ValueError, TypeError):
                            row_data[metric_headers[i]] = value
            
            data.append(row_data)
        
        return pd.DataFrame(data)
    
    def _calculate_cwv_metrics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate Core Web Vitals metrics from DataFrame."""
        if df.empty:
            return self._empty_cwv_metrics()
        
        metrics = {}
        
        # LCP (Largest Contentful Paint)
        if 'largestContentfulPaint' in df.columns:
            lcp_values = df['largestContentfulPaint'].dropna()
            if len(lcp_values) > 0:
                metrics['lcp'] = self._calculate_percentile_metrics(
                    lcp_values, 'lcp'
                )
        
        # FID (First Input Delay) - being deprecated but still reported
        if 'firstInputDelay' in df.columns:
            fid_values = df['firstInputDelay'].dropna()
            if len(fid_values) > 0:
                metrics['fid'] = self._calculate_percentile_metrics(
                    fid_values, 'fid'
                )
        
        # CLS (Cumulative Layout Shift)
        if 'cumulativeLayoutShift' in df.columns:
            cls_values = df['cumulativeLayoutShift'].dropna()
            if len(cls_values) > 0:
                metrics['cls'] = self._calculate_percentile_metrics(
                    cls_values, 'cls'
                )
        
        # INP (Interaction to Next Paint) - replaces FID
        if 'interactionToNextPaint' in df.columns:
            inp_values = df['interactionToNextPaint'].dropna()
            if len(inp_values) > 0:
                metrics['inp'] = self._calculate_percentile_metrics(
                    inp_values, 'inp'
                )
        
        # FCP (First Contentful Paint)
        if 'firstContentfulPaint' in df.columns:
            fcp_values = df['firstContentfulPaint'].dropna()
            if len(fcp_values) > 0:
                metrics['fcp'] = self._calculate_percentile_metrics(
                    fcp_values, 'fcp'
                )
        
        # TTFB (Time to First Byte)
        if 'timeToFirstByte' in df.columns:
            ttfb_values = df['timeToFirstByte'].dropna()
            if len(ttfb_values) > 0:
                metrics['ttfb'] = self._calculate_percentile_metrics(
                    ttfb_values, 'ttfb'
                )
        
        # Sample size
        metrics['sample_size'] = len(df)
        
        return metrics
    
    def _calculate_percentile_metrics(self, values: pd.Series, 
                                     metric_name: str) -> Dict[str, Any]:
        """Calculate percentile-based metrics for a given metric."""
        if len(values) == 0:
            return {}
        
        values_array = np.array(values)
        
        # Calculate percentiles
        p75 = float(np.percentile(values_array, 75))
        p50 = float(np.percentile(values_array, 50))
        p25 = float(np.percentile(values_array, 25))
        p90 = float(np.percentile(values_array, 90))
        p95 = float(np.percentile(values_array, 95))
        
        # Get thresholds
        thresholds = self.CWV_THRESHOLDS.get(metric_name, {})
        good_threshold = thresholds.get('good', 0)
        needs_improvement_threshold = thresholds.get('needs_improvement', 0)
        
        # Calculate distribution
        if good_threshold > 0:
            good_pct = float(np.sum(values_array <= good_threshold) / len(values_array) * 100)
            needs_improvement_pct = float(
                np.sum((values_array > good_threshold) & 
                      (values_array <= needs_improvement_threshold)) / len(values_array) * 100
            )
            poor_pct = float(np.sum(values_array > needs_improvement_threshold) / len(values_array) * 100)
        else:
            good_pct = needs_improvement_pct = poor_pct = 0
        
        # Determine status based on p75 (Google's standard)
        if good_threshold > 0:
            if p75 <= good_threshold:
                status = 'good'
            elif p75 <= needs_improvement_threshold:
                status = 'needs_improvement'
            else:
                status = 'poor'
        else:
            status = 'unknown'
        
        return {
            'p75': p75,
            'p50': p50,
            'p25': p25,
            'p90': p90,
            'p95': p95,
            'mean': float(np.mean(values_array)),
            'status': status,
            'distribution': {
                'good_pct': good_pct,
                'needs_improvement_pct': needs_improvement_pct,
                'poor_pct': poor_pct
            },
            'thresholds': {
                'good': good_threshold,
                'needs_improvement': needs_improvement_threshold
            }
        }
    
    def _calculate_cwv_by_page(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Calculate CWV metrics grouped by page."""
        if df.empty or 'pagePath' not in df.columns:
            return []
        
        pages = []
        for page_path, group in df.groupby('pagePath'):
            page_metrics = {
                'page': page_path,
                'sample_size': len(group),
                'metrics': self._calculate_cwv_metrics(group)
            }
            pages.append(page_metrics)
        
        # Sort by sample size
        pages.sort(key=lambda x: x['sample_size'], reverse=True)
        
        # Limit to top 100 pages
        return pages[:100]
    
    def _assess_cwv(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Generate overall CWV assessment."""
        if not metrics or metrics.get('sample_size', 0) == 0:
            return {
                'overall_status': 'unknown',
                'passing_metrics': 0,
                'total_metrics': 0,
                'critical_issues': []
            }
        
        statuses = []
        critical_issues = []
        
        # Check each core metric
        for metric_name in ['lcp', 'cls', 'inp']:  # Core Web Vitals
            if metric_name in metrics:
                metric_data = metrics[metric_name]
                status = metric_data.get('status', 'unknown')
                statuses.append(status)
                
                if status == 'poor':
                    critical_issues.append({
                        'metric': metric_name.upper(),
                        'p75_value': metric_data.get('p75'),
                        'threshold': metric_data.get('thresholds', {}).get('needs_improvement'),
                        'poor_percentage': metric_data.get('distribution', {}).get('poor_pct', 0)
                    })
        
        # Overall assessment
        if not statuses:
            overall_status = 'unknown'
        elif all(s == 'good' for s in statuses):
            overall_status = 'good'
        elif any(s == 'poor' for s in statuses):
            overall_status = 'poor'
        else:
            overall_status = 'needs_improvement'
        
        passing_metrics = sum(1 for s in statuses if s == 'good')
        
        return {
            'overall_status': overall_status,
            'passing_metrics': passing_metrics,
            'total_metrics': len(statuses),
            'critical_issues': critical_issues
        }
    
    def _calculate_engagement_metrics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate engagement metrics from DataFrame."""
        if df.empty:
            return self._empty_engagement_metrics()
        
        return {
            'sessions': int(df.get('sessions', 0).sum()),
            'users': int(df.get('totalUsers', 0).sum()),
            'new_users': int(df.get('newUsers', 0).sum()),
            'engaged_sessions': int(df.get('engagedSessions', 0).sum()),
            'engagement_rate': float(df.get('engagementRate', 0).mean()),
            'engaged_sessions_per_user': float(
                df.get('engagedSessions', 0).sum() / max(df.get('totalUsers', 1).sum(), 1)
            ),
            'avg_session_duration': float(df.get('averageSessionDuration', 0).mean()),
            'bounce_rate': float(df.get('bounceRate', 0).mean()),
            'events_per_session': float(df.get('eventCount', 0).sum() / max(df.get('sessions', 1).sum(), 1)),
            'conversions': int(df.get('conversions', 0).sum()),
            'conversion_rate': float(
                df.get('conversions', 0).sum() / max(df.get('sessions', 1).sum(), 1) * 100
            ),
            'pageviews': int(df.get('screenPageViews', 0).sum()),
            'pageviews_per_session': float(
                df.get('screenPageViews', 0).sum() / max(df.get('sessions', 1).sum(), 1)
            )
        }
    
    def _calculate_engagement_by_dimension(self, df: pd.DataFrame, 
                                          dimension: str) -> List[Dict[str, Any]]:
        """Calculate engagement metrics grouped by a dimension."""
        if df.empty or dimension not in df.columns:
            return []
        
        results = []
        for dim_value, group in df.groupby(dimension):
            metrics = self._calculate_engagement_metrics(group)
            metrics['dimension_value'] = dim_value
            results.append(metrics)
        
        # Sort by sessions
        results.sort(key=lambda x: x['sessions'], reverse=True)
        
        return results
    
    def _calculate_engagement_by_page(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Calculate engagement metrics grouped by page."""
        if df.empty or 'pagePath' not in df.columns:
            return []
        
        pages = []
        for page_path, group in df.groupby('pagePath'):
            metrics = self._calculate_engagement_metrics(group)
            metrics['page'] = page_path
            pages.append(metrics)
        
        # Sort by pageviews
        pages.sort(key=lambda x: x['pageviews'], reverse=True)
        
        # Limit to top 100 pages
        return pages[:100]
    
    def _empty_cwv_response(self) -> Dict[str, Any]:
        """Return empty CWV response structure."""
        return {
            'overall': self._empty_cwv_metrics(),
            'mobile': self._empty_cwv_metrics(),
            'desktop': self._empty_cwv_metrics(),
            'tablet': self._empty_cwv_metrics(),
            'by_page': [],
            'timestamp': datetime.utcnow().isoformat()
        }
    
    def _empty_cwv_metrics(self) -> Dict[str, Any]:
        """Return empty CWV metrics structure."""
        return {
            'sample_size': 0,
            'assessment': {
                'overall_status': 'unknown',
                'passing_metrics': 0,
                'total_metrics': 0,
                'critical_issues': []
            }
        }
    
    def _empty_engagement_response(self) -> Dict[str, Any]:
        """Return empty engagement response structure."""
        return {
            'overall': self._empty_engagement_metrics(),
            'by_device': [],
            'by_source': [],
            'by_medium': [],
            'by_page': [],
            'timestamp': datetime.utcnow().isoformat()
        }
    
    def _empty_engagement_metrics(self) -> Dict[str, Any]:
        """Return empty engagement metrics structure."""
        return {
            'sessions': 0,
            'users': 0,
            'new_users': 0,
            'engaged_sessions': 0,
            'engagement_rate': 0.0,
            'engaged_sessions_per_user': 0.0,
            'avg_session_duration': 0.0,
            'bounce_rate': 0.0,
            'events_per_session': 0.0,
            'conversions': 0,
            'conversion_rate': 0.0,
            'pageviews': 0,
            'pageviews_per_session': 0.0
        }
    
    def _empty_traffic_response(self) -> Dict[str, Any]:
        """Return empty traffic response structure."""
        return {
            'total_sessions': 0,
            'by_channel': [],
            'by_source': [],
            'by_medium': [],
            'organic_search_breakdown': {},
            'timestamp': datetime.utcnow().isoformat()
        }

