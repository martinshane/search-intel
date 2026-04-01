# api/modules/module_1_technical.py
"""
Module 1: Technical Performance & Core Web Vitals Analysis

Analyzes GA4 data for:
- Core Web Vitals (LCP, FID, CLS)
- Page load times
- Mobile vs desktop performance
- Technical health metrics
- Performance trends and issue identification

Returns structured data matching the module spec.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from scipy import stats
from dataclasses import dataclass, asdict


@dataclass
class PerformanceMetric:
    """Container for a performance metric with thresholds"""
    name: str
    value: float
    unit: str
    threshold_good: float
    threshold_needs_improvement: float
    status: str  # 'good', 'needs_improvement', 'poor'
    percentile_75: Optional[float] = None
    percentile_95: Optional[float] = None


@dataclass
class DevicePerformance:
    """Performance breakdown by device"""
    device: str
    lcp: float
    fid: float
    cls: float
    page_load_time: float
    sample_size: int
    performance_score: float


@dataclass
class PagePerformance:
    """Per-page performance metrics"""
    page_path: str
    lcp: float
    fid: float
    cls: float
    page_views: int
    issues: List[str]
    priority_score: float


class TechnicalPerformanceAnalyzer:
    """Analyzes technical performance metrics from GA4 data"""
    
    # Core Web Vitals thresholds (Google's official values)
    LCP_GOOD = 2500  # milliseconds
    LCP_NEEDS_IMPROVEMENT = 4000
    
    FID_GOOD = 100  # milliseconds
    FID_NEEDS_IMPROVEMENT = 300
    
    CLS_GOOD = 0.1  # score
    CLS_NEEDS_IMPROVEMENT = 0.25
    
    PAGE_LOAD_GOOD = 3000  # milliseconds
    PAGE_LOAD_NEEDS_IMPROVEMENT = 5000
    
    def __init__(self, ga4_data: Dict[str, pd.DataFrame]):
        """
        Initialize analyzer with GA4 data
        
        Args:
            ga4_data: Dictionary containing GA4 dataframes:
                - 'web_vitals': LCP, FID, CLS metrics
                - 'page_timings': Page load time data
                - 'device_breakdown': Performance by device
                - 'daily_vitals': Daily time series of metrics
        """
        self.ga4_data = ga4_data
        self.web_vitals_df = ga4_data.get('web_vitals', pd.DataFrame())
        self.page_timings_df = ga4_data.get('page_timings', pd.DataFrame())
        self.device_df = ga4_data.get('device_breakdown', pd.DataFrame())
        self.daily_vitals_df = ga4_data.get('daily_vitals', pd.DataFrame())
    
    def analyze(self) -> Dict[str, Any]:
        """
        Run complete technical performance analysis
        
        Returns:
            Structured dict matching module spec
        """
        return {
            'overall_performance': self._calculate_overall_performance(),
            'core_web_vitals': self._analyze_core_web_vitals(),
            'device_breakdown': self._analyze_device_performance(),
            'page_analysis': self._analyze_page_level_performance(),
            'trends': self._analyze_performance_trends(),
            'issues': self._identify_technical_issues(),
            'recommendations': self._generate_recommendations(),
            'metadata': {
                'analysis_date': datetime.now().isoformat(),
                'data_range_days': self._get_data_range_days(),
                'pages_analyzed': len(self._get_unique_pages()),
                'total_page_views': self._get_total_pageviews()
            }
        }
    
    def _calculate_overall_performance(self) -> Dict[str, Any]:
        """Calculate overall performance score (0-100)"""
        scores = []
        
        # LCP score (40% weight)
        lcp_score = self._metric_to_score(
            self._get_median_metric('lcp'),
            self.LCP_GOOD,
            self.LCP_NEEDS_IMPROVEMENT,
            inverse=True
        )
        scores.append(('lcp', lcp_score, 0.4))
        
        # FID score (25% weight)
        fid_score = self._metric_to_score(
            self._get_median_metric('fid'),
            self.FID_GOOD,
            self.FID_NEEDS_IMPROVEMENT,
            inverse=True
        )
        scores.append(('fid', fid_score, 0.25))
        
        # CLS score (25% weight)
        cls_score = self._metric_to_score(
            self._get_median_metric('cls'),
            self.CLS_GOOD,
            self.CLS_NEEDS_IMPROVEMENT,
            inverse=True
        )
        scores.append(('cls', cls_score, 0.25))
        
        # Page load time (10% weight)
        load_score = self._metric_to_score(
            self._get_median_metric('page_load_time'),
            self.PAGE_LOAD_GOOD,
            self.PAGE_LOAD_NEEDS_IMPROVEMENT,
            inverse=True
        )
        scores.append(('page_load', load_score, 0.1))
        
        # Calculate weighted average
        overall_score = sum(score * weight for _, score, weight in scores)
        
        # Determine grade
        if overall_score >= 90:
            grade = 'A'
            status = 'excellent'
        elif overall_score >= 75:
            grade = 'B'
            status = 'good'
        elif overall_score >= 60:
            grade = 'C'
            status = 'needs_improvement'
        elif overall_score >= 40:
            grade = 'D'
            status = 'poor'
        else:
            grade = 'F'
            status = 'critical'
        
        return {
            'score': round(overall_score, 1),
            'grade': grade,
            'status': status,
            'component_scores': {
                name: round(score, 1) for name, score, _ in scores
            },
            'passing_core_web_vitals': self._check_passing_cwv()
        }
    
    def _analyze_core_web_vitals(self) -> Dict[str, Any]:
        """Detailed Core Web Vitals analysis"""
        metrics = []
        
        # LCP Analysis
        lcp_value = self._get_median_metric('lcp')
        lcp_p75 = self._get_percentile_metric('lcp', 75)
        lcp_p95 = self._get_percentile_metric('lcp', 95)
        
        metrics.append(PerformanceMetric(
            name='Largest Contentful Paint (LCP)',
            value=lcp_value,
            unit='ms',
            threshold_good=self.LCP_GOOD,
            threshold_needs_improvement=self.LCP_NEEDS_IMPROVEMENT,
            status=self._get_metric_status(lcp_value, self.LCP_GOOD, self.LCP_NEEDS_IMPROVEMENT),
            percentile_75=lcp_p75,
            percentile_95=lcp_p95
        ))
        
        # FID Analysis
        fid_value = self._get_median_metric('fid')
        fid_p75 = self._get_percentile_metric('fid', 75)
        fid_p95 = self._get_percentile_metric('fid', 95)
        
        metrics.append(PerformanceMetric(
            name='First Input Delay (FID)',
            value=fid_value,
            unit='ms',
            threshold_good=self.FID_GOOD,
            threshold_needs_improvement=self.FID_NEEDS_IMPROVEMENT,
            status=self._get_metric_status(fid_value, self.FID_GOOD, self.FID_NEEDS_IMPROVEMENT),
            percentile_75=fid_p75,
            percentile_95=fid_p95
        ))
        
        # CLS Analysis
        cls_value = self._get_median_metric('cls')
        cls_p75 = self._get_percentile_metric('cls', 75)
        cls_p95 = self._get_percentile_metric('cls', 95)
        
        metrics.append(PerformanceMetric(
            name='Cumulative Layout Shift (CLS)',
            value=cls_value,
            unit='score',
            threshold_good=self.CLS_GOOD,
            threshold_needs_improvement=self.CLS_NEEDS_IMPROVEMENT,
            status=self._get_metric_status(cls_value, self.CLS_GOOD, self.CLS_NEEDS_IMPROVEMENT),
            percentile_75=cls_p75,
            percentile_95=cls_p95
        ))
        
        return {
            'metrics': [asdict(m) for m in metrics],
            'summary': {
                'all_passing': all(m.status == 'good' for m in metrics),
                'metrics_passing': sum(1 for m in metrics if m.status == 'good'),
                'metrics_needs_improvement': sum(1 for m in metrics if m.status == 'needs_improvement'),
                'metrics_poor': sum(1 for m in metrics if m.status == 'poor')
            }
        }
    
    def _analyze_device_performance(self) -> Dict[str, Any]:
        """Break down performance by device type"""
        if self.device_df.empty:
            return {'devices': [], 'mobile_performance_gap': None}
        
        devices = []
        
        for device_type in ['mobile', 'desktop', 'tablet']:
            device_data = self.device_df[self.device_df['device_category'] == device_type]
            
            if device_data.empty:
                continue
            
            lcp = device_data['lcp'].median() if 'lcp' in device_data else 0
            fid = device_data['fid'].median() if 'fid' in device_data else 0
            cls = device_data['cls'].median() if 'cls' in device_data else 0
            page_load = device_data['page_load_time'].median() if 'page_load_time' in device_data else 0
            sample_size = len(device_data)
            
            # Calculate device-specific performance score
            device_score = self._calculate_device_score(lcp, fid, cls, page_load)
            
            devices.append(DevicePerformance(
                device=device_type,
                lcp=round(lcp, 1),
                fid=round(fid, 1),
                cls=round(cls, 3),
                page_load_time=round(page_load, 1),
                sample_size=sample_size,
                performance_score=round(device_score, 1)
            ))
        
        # Calculate mobile vs desktop gap
        mobile_gap = None
        mobile_perf = next((d for d in devices if d.device == 'mobile'), None)
        desktop_perf = next((d for d in devices if d.device == 'desktop'), None)
        
        if mobile_perf and desktop_perf:
            mobile_gap = {
                'score_difference': round(desktop_perf.performance_score - mobile_perf.performance_score, 1),
                'lcp_difference_ms': round(mobile_perf.lcp - desktop_perf.lcp, 1),
                'fid_difference_ms': round(mobile_perf.fid - desktop_perf.fid, 1),
                'cls_difference': round(mobile_perf.cls - desktop_perf.cls, 3),
                'status': self._assess_mobile_gap(desktop_perf.performance_score - mobile_perf.performance_score)
            }
        
        return {
            'devices': [asdict(d) for d in devices],
            'mobile_performance_gap': mobile_gap,
            'worst_performing_device': min(devices, key=lambda d: d.performance_score).device if devices else None
        }
    
    def _analyze_page_level_performance(self) -> Dict[str, Any]:
        """Identify worst performing pages"""
        if self.page_timings_df.empty:
            return {'pages': [], 'summary': {}}
        
        pages = []
        
        # Group by page path
        for page_path, page_data in self.page_timings_df.groupby('page_path'):
            lcp = page_data['lcp'].median() if 'lcp' in page_data else 0
            fid = page_data['fid'].median() if 'fid' in page_data else 0
            cls = page_data['cls'].median() if 'cls' in page_data else 0
            page_views = len(page_data)
            
            issues = []
            
            # Identify issues
            if lcp > self.LCP_NEEDS_IMPROVEMENT:
                issues.append(f'Poor LCP: {int(lcp)}ms (target: <{self.LCP_GOOD}ms)')
            elif lcp > self.LCP_GOOD:
                issues.append(f'Slow LCP: {int(lcp)}ms (target: <{self.LCP_GOOD}ms)')
            
            if fid > self.FID_NEEDS_IMPROVEMENT:
                issues.append(f'Poor FID: {int(fid)}ms (target: <{self.FID_GOOD}ms)')
            elif fid > self.FID_GOOD:
                issues.append(f'Slow FID: {int(fid)}ms (target: <{self.FID_GOOD}ms)')
            
            if cls > self.CLS_NEEDS_IMPROVEMENT:
                issues.append(f'Poor CLS: {cls:.3f} (target: <{self.CLS_GOOD})')
            elif cls > self.CLS_GOOD:
                issues.append(f'High CLS: {cls:.3f} (target: <{self.CLS_GOOD})')
            
            # Calculate priority score (worse metrics + higher traffic = higher priority)
            priority_score = self._calculate_page_priority(lcp, fid, cls, page_views)
            
            if issues:  # Only include pages with issues
                pages.append(PagePerformance(
                    page_path=page_path,
                    lcp=round(lcp, 1),
                    fid=round(fid, 1),
                    cls=round(cls, 3),
                    page_views=page_views,
                    issues=issues,
                    priority_score=round(priority_score, 1)
                ))
        
        # Sort by priority score
        pages.sort(key=lambda p: p.priority_score, reverse=True)
        
        return {
            'pages': [asdict(p) for p in pages[:20]],  # Top 20 worst pages
            'summary': {
                'total_pages_with_issues': len(pages),
                'pages_with_lcp_issues': sum(1 for p in pages if any('LCP' in i for i in p.issues)),
                'pages_with_fid_issues': sum(1 for p in pages if any('FID' in i for i in p.issues)),
                'pages_with_cls_issues': sum(1 for p in pages if any('CLS' in i for i in p.issues)),
                'total_affected_pageviews': sum(p.page_views for p in pages)
            }
        }
    
    def _analyze_performance_trends(self) -> Dict[str, Any]:
        """Analyze performance trends over time"""
        if self.daily_vitals_df.empty or 'date' not in self.daily_vitals_df:
            return {'trend_direction': 'unknown', 'metrics': {}}
        
        # Ensure date is datetime
        df = self.daily_vitals_df.copy()
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date')
        
        trends = {}
        
        for metric in ['lcp', 'fid', 'cls']:
            if metric not in df:
                continue
            
            # Calculate trend using linear regression
            days = (df['date'] - df['date'].min()).dt.days.values
            values = df[metric].values
            
            # Remove NaN values
            mask = ~np.isnan(values)
            if mask.sum() < 2:
                continue
            
            days_clean = days[mask]
            values_clean = values[mask]
            
            slope, intercept, r_value, p_value, std_err = stats.linregress(days_clean, values_clean)
            
            # Calculate percent change per 30 days
            mean_value = values_clean.mean()
            pct_change_per_30d = (slope * 30 / mean_value * 100) if mean_value > 0 else 0
            
            # Determine direction
            if abs(pct_change_per_30d) < 2:
                direction = 'stable'
            elif pct_change_per_30d > 0:
                direction = 'worsening'  # For performance metrics, increase is bad
            else:
                direction = 'improving'
            
            trends[metric] = {
                'direction': direction,
                'percent_change_per_30d': round(pct_change_per_30d, 2),
                'r_squared': round(r_value ** 2, 3),
                'is_significant': p_value < 0.05,
                'recent_value': round(values_clean[-1], 1 if metric in ['lcp', 'fid'] else 3),
                'baseline_value': round(values_clean[0], 1 if metric in ['lcp', 'fid'] else 3)
            }
        
        # Overall trend assessment
        worsening_count = sum(1 for t in trends.values() if t['direction'] == 'worsening')
        improving_count = sum(1 for t in trends.values() if t['direction'] == 'improving')
        
        if worsening_count > improving_count:
            overall = 'worsening'
        elif improving_count > worsening_count:
            overall = 'improving'
        else:
            overall = 'stable'
        
        return {
            'trend_direction': overall,
            'metrics': trends,
            'analysis_period_days': int(days_clean[-1] - days_clean[0]) if len(days_clean) > 0 else 0
        }
    
    def _identify_technical_issues(self) -> List[Dict[str, Any]]:
        """Identify specific technical issues requiring attention"""
        issues = []
        
        # Check overall Core Web Vitals
        lcp = self._get_median_metric('lcp')
        fid = self._get_median_metric('fid')
        cls = self._get_median_metric('cls')
        
        if lcp > self.LCP_NEEDS_IMPROVEMENT:
            issues.append({
                'type': 'core_web_vital',
                'severity': 'critical',
                'metric': 'LCP',
                'value': lcp,
                'threshold': self.LCP_GOOD,
                'description': f'LCP is {int(lcp)}ms (poor). Target: <{self.LCP_GOOD}ms',
                'impact': 'Failing Core Web Vitals assessment, negative ranking impact'
            })
        elif lcp > self.LCP_GOOD:
            issues.append({
                'type': 'core_web_vital',
                'severity': 'high',
                'metric': 'LCP',
                'value': lcp,
                'threshold': self.LCP_GOOD,
                'description': f'LCP is {int(lcp)}ms (needs improvement). Target: <{self.LCP_GOOD}ms',
                'impact': 'Borderline Core Web Vitals, may affect rankings'
            })
        
        if fid > self.FID_NEEDS_IMPROVEMENT:
            issues.append({
                'type': 'core_web_vital',
                'severity': 'critical',
                'metric': 'FID',
                'value': fid,
                'threshold': self.FID_GOOD,
                'description': f'FID is {int(fid)}ms (poor). Target: <{self.FID_GOOD}ms',
                'impact': 'Failing Core Web Vitals assessment, negative ranking impact'
            })
        elif fid > self.FID_GOOD:
            issues.append({
                'type': 'core_web_vital',
                'severity': 'high',
                'metric': 'FID',
                'value': fid,
                'threshold': self.FID_GOOD,
                'description': f'FID is {int(fid)}ms (needs improvement). Target: <{self.FID_GOOD}ms',
                'impact': 'Borderline Core Web Vitals, may affect rankings'
            })
        
        if cls > self.CLS_NEEDS_IMPROVEMENT:
            issues.append({
                'type': 'core_web_vital',
                'severity': 'critical',
                'metric': 'CLS',
                'value': cls,
                'threshold': self.CLS_GOOD,
                'description': f'CLS is {cls:.3f} (poor). Target: <{self.CLS_GOOD}',
                'impact': 'Failing Core Web Vitals assessment, negative ranking impact'
            })
        elif cls > self.CLS_GOOD:
            issues.append({
                'type': 'core_web_vital',
                'severity': 'high',
                'metric': 'CLS',
                'value': cls,
                'threshold': self.CLS_GOOD,
                'description': f'CLS is {cls:.3f} (needs improvement). Target: <{self.CLS_GOOD}',
                'impact': 'Borderline Core Web Vitals, may affect rankings'
            })
        
        # Check mobile vs desktop gap
        if not self.device_df.empty:
            mobile_data = self.device_df[self.device_df['device_category'] == 'mobile']
            desktop_data = self.device_df[self.device_df['device_category'] == 'desktop']
            
            if not mobile_data.empty and not desktop_data.empty:
                mobile_lcp = mobile_data['lcp'].median() if 'lcp' in mobile_data else 0
                desktop_lcp = desktop_data['lcp'].median() if 'lcp' in desktop_data else 0
                
                if mobile_lcp > desktop_lcp * 1.5:  # Mobile 50% slower
                    issues.append({
                        'type': 'mobile_performance',
                        'severity': 'high',
                        'metric': 'LCP',
                        'value': mobile_lcp - desktop_lcp,
                        'description': f'Mobile LCP is {int(mobile_lcp - desktop_lcp)}ms slower than desktop',
                        'impact': 'Poor mobile experience affects mobile-first indexing'
                    })
        
        # Check for worsening trends
        trends = self._analyze_performance_trends()
        for metric, trend_data in trends.get('metrics', {}).items():
            if trend_data['direction'] == 'worsening' and trend_data['is_significant']:
                issues.append({
                    'type': 'performance_regression',
                    'severity': 'medium',
                    'metric': metric.upper(),
                    'value': trend_data['percent_change_per_30d'],
                    'description': f'{metric.upper()} worsening by {abs(trend_data["percent_change_per_30d"]):.1f}% per month',
                    'impact': 'Performance degradation over time, investigate recent changes'
                })
        
        return issues
    
    def _generate_recommendations(self) -> List[Dict[str, Any]]:
        """Generate actionable recommendations based on analysis"""
        recommendations = []
        
        lcp = self._get_median_metric('lcp')
        fid = self._get_median_metric('fid')
        cls = self._get_median_metric('cls')
        
        # LCP recommendations
        if lcp > self.LCP_GOOD:
            priority = 'critical' if lcp > self.LCP_NEEDS_IMPROVEMENT else 'high'
            recommendations.append({
                'priority': priority,
                'category': 'LCP Optimization',
                'action': 'Optimize Largest Contentful Paint',
                'specific_steps': [
                    'Optimize and compress hero images',
                    'Implement lazy loading for below-fold images',
                    'Use a CDN for faster asset delivery',
                    'Preload critical resources (fonts, hero images)',
                    'Reduce server response time (TTFB)',
                    'Eliminate render-blocking resources'
                ],
                'estimated_impact': 'Could improve LCP by 30-50%',
                'effort': 'medium'
            })
        
        # FID recommendations
        if fid > self.FID_GOOD:
            priority = 'critical' if fid > self.FID_NEEDS_IMPROVEMENT else 'high'
            recommendations.append({
                'priority': priority,
                'category': 'FID Optimization',
                'action': 'Reduce JavaScript execution time',
                'specific_steps': [
                    'Break up long JavaScript tasks',
                    'Implement code splitting',
                    'Remove unused JavaScript',
                    'Use a web worker for heavy computations',
                    'Defer non-critical JavaScript',
                    'Optimize third-party scripts'
                ],
                'estimated_impact': 'Could improve FID by 40-60%',
                'effort': 'medium'
            })
        
        # CLS recommendations
        if cls > self.CLS_GOOD:
            priority = 'critical' if cls > self.CLS_NEEDS_IMPROVEMENT else 'high'
            recommendations.append({
                'priority': priority,
                'category': 'CLS Optimization',
                'action': 'Eliminate layout shifts',
                'specific_steps': [
                    'Add width and height attributes to all images and video',
                    'Reserve space for ad slots and embeds',
                    'Avoid inserting content above existing content',
                    'Use CSS aspect-ratio for responsive images',
                    'Preload fonts to prevent FOIT/FOUT',
                    'Avoid animations that trigger layout shifts'
                ],
                'estimated_impact': 'Could reduce CLS by 50-70%',
                'effort': 'low'
            })
        
        # Mobile-specific recommendations
        if not self.device_df.empty:
            mobile_data = self.device_df[self.device_df['device_category'] == 'mobile']
            desktop_data = self.device_df[self.device_df['device_category'] == 'desktop']
            
            if not mobile_data.empty and not desktop_data.empty:
                mobile_lcp = mobile_data['lcp'].median() if 'lcp' in mobile_data else 0
                desktop_lcp = desktop_data['lcp'].median() if 'lcp' in desktop_data else 0
                
                if mobile_lcp > desktop_lcp * 1.3:
                    recommendations.append({
                        'priority': 'high',
                        'category': 'Mobile Performance',
                        'action': 'Optimize mobile experience',
                        'specific_steps': [
                            'Implement responsive images with srcset',
                            'Reduce mobile-specific JavaScript payload',
                            'Optimize for slower network conditions',
                            'Test on real mobile devices with throttling',
                            'Consider AMP or similar mobile framework'
                        ],
                        'estimated_impact': 'Improve mobile Core Web Vitals scores',
                        'effort': 'high'
                    })
        
        # Page-specific recommendations
        page_analysis = self._analyze_page_level_performance()
        if page_analysis.get('summary', {}).get('total_pages_with_issues', 0) > 0:
            top_pages = page_analysis.get('pages', [])[:5]
            if top_pages:
                recommendations.append({
                    'priority': 'high',
                    'category': 'Page-Level Fixes',
                    'action': f'Fix performance issues on {len(top_pages)} high-traffic pages',
                    'specific_steps': [
                        f'Optimize {page["page_path"]}: {", ".join(page["issues"][:2])}'
                        for page in top_pages
                    ],
                    'estimated_impact': f'Affects {sum(p["page_views"] for p in top_pages)} pageviews',
                    'effort': 'medium'
                })
        
        # Sort by priority
        priority_order = {'critical': 0, 'high': 1, 'medium': 2, 'low': 3}
        recommendations.sort(key=lambda r: priority_order.get(r['priority'], 4))
        
        return recommendations
    
    # Helper methods
    
    def _get_median_metric(self, metric_name: str) -> float:
        """Get median value for a metric across all data"""
        if self.web_vitals_df.empty or metric_name not in self.web_vitals_df:
            return 0.0
        return float(self.web_vitals_df[metric_name].median())
    
    def _get_percentile_metric(self, metric_name: str, percentile: int) -> float:
        """Get percentile value for a metric"""
        if self.web_vitals_df.empty or metric_name not in self.web_vitals_df:
            return 0.0
        return float(self.web_vitals_df[metric_name].quantile(percentile / 100))
    
    def _get_metric_status(self, value: float, good_threshold: float, needs_improvement_threshold: float) -> str:
        """Determine status of a metric value"""
        if value <= good_threshold:
            return 'good'
        elif value <= needs_improvement_threshold:
            return 'needs_improvement'
        else:
            return 'poor'
    
    def _metric_to_score(self, value: float, good: float, needs_improvement: float, inverse: bool = True) -> float:
        """
        Convert a metric value to a 0-100 score
        
        Args:
            value: Metric value
            good: Good threshold
            needs_improvement: Needs improvement threshold
            inverse: If True, lower values are better (like for LCP, FID)
        """
        if inverse:
            if value <= good:
                return 100
            elif value >= needs_improvement:
                return 0
            else:
                # Linear interpolation between good and needs_improvement
                return 100 * (1 - (value - good) / (needs_improvement - good))
        else:
            if value >= good:
                return 100
            elif value <= needs_improvement:
                return 0
            else:
                return 100 * ((value - needs_improvement) / (good - needs_improvement))
    
    def _calculate_device_score(self, lcp: float, fid: float, cls: float, page_load: float) -> float:
        """Calculate overall performance score for a device"""
        lcp_score = self._metric_to_score(lcp, self.LCP_GOOD, self.LCP_NEEDS_IMPROVEMENT, inverse=True)
        fid_score = self._metric_to_score(fid, self.FID_GOOD, self.FID_NEEDS_IMPROVEMENT, inverse=True)
        cls_score = self._metric_to_score(cls, self.CLS_GOOD, self.CLS_NEEDS_IMPROVEMENT, inverse=True)
        load_score = self._metric_to_score(page_load, self.PAGE_LOAD_GOOD, self.PAGE_LOAD_NEEDS_IMPROVEMENT, inverse=True)
        
        return (lcp_score * 0.4 + fid_score * 0.25 + cls_score * 0.25 + load_score * 0.1)
    
    def _calculate_page_priority(self, lcp: float, fid: float, cls: float, page_views: int) -> float:
        """Calculate priority score for a page (higher = more urgent)"""
        # How bad is each metric (0-1 scale)
        lcp_badness = min(1.0, max(0, (lcp - self.LCP_GOOD) / self.LCP_GOOD))
        fid_badness = min(1.0, max(0, (fid - self.FID_GOOD) / self.FID_GOOD))
        cls_badness = min(1.0, max(0, (cls - self.CLS_GOOD) / self.CLS_GOOD))
        
        avg_badness = (lcp_badness + fid_badness + cls_badness) / 3
        
        # Normalize page views (log scale)
        traffic_factor = np.log10(max(1, page_views))
        
        return avg_badness * traffic_factor * 100
    
    def _check_passing_cwv(self) -> bool:
        """Check if site passes Core Web Vitals assessment"""
        lcp = self._get_median_metric('lcp')
        fid = self._get_median_metric('fid')
        cls = self._get_median_metric('cls')
        
        return (
            lcp <= self.LCP_GOOD and
            fid <= self.FID_GOOD and
            cls <= self.CLS_GOOD
        )
    
    def _assess_mobile_gap(self, score_diff: float) -> str:
        """Assess severity of mobile performance gap"""
        if score_diff <= 5:
            return 'acceptable'
        elif score_diff <= 15:
            return 'needs_attention'
        else:
            return 'critical'
    
    def _get_data_range_days(self) -> int:
        """Get number of days in data range"""
        if self.daily_vitals_df.empty or 'date' not in self.daily_vitals_df:
            return 0
        
        df = self.daily_vitals_df.copy()
        df['date'] = pd.to_datetime(df['date'])
        return int((df['date'].max() - df['date'].min()).days)
    
    def _get_unique_pages(self) -> List[str]:
        """Get list of unique pages analyzed"""
        if self.page_timings_df.empty or 'page_path' not in self.page_timings_df:
            return []
        return self.page_timings_df['page_path'].unique().tolist()
    
    def _get_total_pageviews(self) -> int:
        """Get total pageviews in analysis"""
        if self.page_timings_df.empty:
            return 0
        return len(self.page_timings_df)


def analyze_technical_performance(ga4_data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
    """
    Main entry point for technical performance analysis
    
    Args:
        ga4_data: Dictionary containing GA4 dataframes
        
    Returns:
        Complete technical performance analysis
    """
    analyzer = TechnicalPerformanceAnalyzer(ga4_data)
    return analyzer.analyze()