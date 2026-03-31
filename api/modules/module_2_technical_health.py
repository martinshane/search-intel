"""
Module 2: Technical Health Analysis
Analyzes GA4 data for technical SEO health metrics including page load times,
mobile vs desktop traffic split, bounce rates by device, Core Web Vitals signals,
and generates actionable technical SEO recommendations.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


def analyze_technical_health(
    ga4_data: Dict[str, pd.DataFrame],
    date_ranges: Dict[str, tuple]
) -> Dict[str, Any]:
    """
    Main entry point for Module 2: Technical Health Analysis
    
    Args:
        ga4_data: Dictionary containing GA4 dataframes:
            - 'traffic_overview': sessions, users, pageviews, bounce, engagement
            - 'landing_pages': landing page performance with engagement metrics
            - 'device_breakdown': traffic split by device category
            - 'page_timings': page load speed metrics if available
            - 'web_vitals': Core Web Vitals data if available
        date_ranges: Dictionary with 'current' and 'previous' period tuples
    
    Returns:
        Dictionary containing technical health analysis results
    """
    
    logger.info("Starting technical health analysis")
    
    try:
        results = {
            "device_analysis": analyze_device_split(ga4_data.get('device_breakdown')),
            "page_speed_analysis": analyze_page_speed(
                ga4_data.get('page_timings'),
                ga4_data.get('landing_pages')
            ),
            "core_web_vitals": analyze_web_vitals(ga4_data.get('web_vitals')),
            "bounce_rate_analysis": analyze_bounce_rates(
                ga4_data.get('device_breakdown'),
                ga4_data.get('landing_pages')
            ),
            "mobile_usability": analyze_mobile_usability(
                ga4_data.get('device_breakdown'),
                ga4_data.get('landing_pages')
            ),
            "technical_issues": identify_technical_issues(ga4_data),
            "recommendations": [],
            "priority_score": 0,
            "summary": {}
        }
        
        # Generate recommendations based on all findings
        results["recommendations"] = generate_recommendations(results)
        
        # Calculate overall priority score
        results["priority_score"] = calculate_priority_score(results)
        
        # Generate executive summary
        results["summary"] = generate_summary(results)
        
        logger.info("Technical health analysis completed successfully")
        return results
        
    except Exception as e:
        logger.error(f"Error in technical health analysis: {str(e)}")
        raise


def analyze_device_split(device_data: Optional[pd.DataFrame]) -> Dict[str, Any]:
    """
    Analyze traffic distribution across device types
    
    Args:
        device_data: DataFrame with columns: device_category, sessions, users, 
                     pageviews, bounce_rate, avg_session_duration
    
    Returns:
        Device split analysis with insights
    """
    
    if device_data is None or device_data.empty:
        return {
            "status": "no_data",
            "message": "No device breakdown data available"
        }
    
    try:
        # Calculate device percentages
        total_sessions = device_data['sessions'].sum()
        
        device_breakdown = []
        for _, row in device_data.iterrows():
            device_breakdown.append({
                "device": row['device_category'],
                "sessions": int(row['sessions']),
                "percentage": round((row['sessions'] / total_sessions) * 100, 2),
                "bounce_rate": round(row.get('bounce_rate', 0) * 100, 2),
                "avg_session_duration": round(row.get('avg_session_duration', 0), 2)
            })
        
        # Sort by sessions
        device_breakdown = sorted(device_breakdown, key=lambda x: x['sessions'], reverse=True)
        
        # Find mobile percentage
        mobile_pct = next((d['percentage'] for d in device_breakdown if d['device'] == 'mobile'), 0)
        desktop_pct = next((d['percentage'] for d in device_breakdown if d['device'] == 'desktop'), 0)
        tablet_pct = next((d['percentage'] for d in device_breakdown if d['device'] == 'tablet'), 0)
        
        # Determine if mobile-first
        is_mobile_first = mobile_pct > desktop_pct
        
        # Flag issues
        issues = []
        if mobile_pct < 30 and is_mobile_first == False:
            issues.append({
                "type": "low_mobile_traffic",
                "severity": "medium",
                "message": f"Mobile traffic is only {mobile_pct}% - may indicate mobile usability issues"
            })
        
        if mobile_pct > 60 and desktop_pct > 30:
            issues.append({
                "type": "mobile_dominant",
                "severity": "info",
                "message": "Site is mobile-dominant - ensure mobile experience is optimized"
            })
        
        return {
            "status": "success",
            "breakdown": device_breakdown,
            "mobile_percentage": mobile_pct,
            "desktop_percentage": desktop_pct,
            "tablet_percentage": tablet_pct,
            "is_mobile_first": is_mobile_first,
            "total_sessions": int(total_sessions),
            "issues": issues
        }
        
    except Exception as e:
        logger.error(f"Error analyzing device split: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }


def analyze_page_speed(
    page_timings: Optional[pd.DataFrame],
    landing_pages: Optional[pd.DataFrame]
) -> Dict[str, Any]:
    """
    Analyze page load speed metrics
    
    Args:
        page_timings: DataFrame with page load time data
        landing_pages: Landing page performance data
    
    Returns:
        Page speed analysis with problem pages identified
    """
    
    if page_timings is None or page_timings.empty:
        return {
            "status": "no_data",
            "message": "No page timing data available from GA4",
            "recommendation": "Enable enhanced measurement in GA4 to track page load times"
        }
    
    try:
        # Calculate overall metrics
        avg_page_load_time = page_timings['avg_page_load_time'].mean()
        median_page_load_time = page_timings['avg_page_load_time'].median()
        p90_page_load_time = page_timings['avg_page_load_time'].quantile(0.90)
        
        # Industry benchmarks (in seconds)
        GOOD_THRESHOLD = 2.5
        NEEDS_IMPROVEMENT_THRESHOLD = 4.0
        
        # Classify overall performance
        if avg_page_load_time < GOOD_THRESHOLD:
            performance_rating = "good"
        elif avg_page_load_time < NEEDS_IMPROVEMENT_THRESHOLD:
            performance_rating = "needs_improvement"
        else:
            performance_rating = "poor"
        
        # Identify slow pages
        slow_pages = page_timings[
            page_timings['avg_page_load_time'] > NEEDS_IMPROVEMENT_THRESHOLD
        ].copy()
        
        # Add traffic impact if landing page data available
        if landing_pages is not None and not landing_pages.empty:
            slow_pages = slow_pages.merge(
                landing_pages[['page_path', 'sessions', 'bounce_rate']],
                left_on='page_path',
                right_on='page_path',
                how='left'
            )
            slow_pages['impact_score'] = (
                slow_pages['sessions'] * slow_pages['avg_page_load_time']
            )
            slow_pages = slow_pages.sort_values('impact_score', ascending=False)
        else:
            slow_pages = slow_pages.sort_values('avg_page_load_time', ascending=False)
        
        # Format slow pages for output
        slow_pages_list = []
        for _, row in slow_pages.head(20).iterrows():
            page_info = {
                "url": row['page_path'],
                "avg_load_time": round(row['avg_page_load_time'], 2),
                "sessions": int(row.get('sessions', 0)),
                "bounce_rate": round(row.get('bounce_rate', 0) * 100, 2) if 'bounce_rate' in row else None
            }
            slow_pages_list.append(page_info)
        
        # Calculate percentage of pages that are slow
        total_pages = len(page_timings)
        slow_page_count = len(slow_pages)
        slow_page_percentage = (slow_page_count / total_pages) * 100 if total_pages > 0 else 0
        
        return {
            "status": "success",
            "overall_rating": performance_rating,
            "metrics": {
                "avg_page_load_time": round(avg_page_load_time, 2),
                "median_page_load_time": round(median_page_load_time, 2),
                "p90_page_load_time": round(p90_page_load_time, 2)
            },
            "benchmarks": {
                "good": GOOD_THRESHOLD,
                "needs_improvement": NEEDS_IMPROVEMENT_THRESHOLD
            },
            "slow_pages": {
                "count": int(slow_page_count),
                "percentage": round(slow_page_percentage, 2),
                "pages": slow_pages_list
            },
            "total_pages_analyzed": int(total_pages)
        }
        
    except Exception as e:
        logger.error(f"Error analyzing page speed: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }


def analyze_web_vitals(web_vitals_data: Optional[pd.DataFrame]) -> Dict[str, Any]:
    """
    Analyze Core Web Vitals (LCP, FID, CLS) if available
    
    Args:
        web_vitals_data: DataFrame with web vitals metrics
    
    Returns:
        Core Web Vitals analysis
    """
    
    if web_vitals_data is None or web_vitals_data.empty:
        return {
            "status": "no_data",
            "message": "Core Web Vitals data not available in GA4",
            "recommendation": "Use Google Search Console or PageSpeed Insights API for Web Vitals data"
        }
    
    try:
        # Google's Core Web Vitals thresholds
        thresholds = {
            "lcp": {"good": 2.5, "needs_improvement": 4.0},  # seconds
            "fid": {"good": 100, "needs_improvement": 300},  # milliseconds
            "cls": {"good": 0.1, "needs_improvement": 0.25}  # score
        }
        
        results = {
            "status": "success",
            "metrics": {}
        }
        
        # Analyze each metric
        for metric in ['lcp', 'fid', 'cls']:
            metric_col = f'avg_{metric}'
            if metric_col in web_vitals_data.columns:
                avg_value = web_vitals_data[metric_col].mean()
                p75_value = web_vitals_data[metric_col].quantile(0.75)
                
                # Classify performance
                if p75_value <= thresholds[metric]["good"]:
                    rating = "good"
                elif p75_value <= thresholds[metric]["needs_improvement"]:
                    rating = "needs_improvement"
                else:
                    rating = "poor"
                
                results["metrics"][metric.upper()] = {
                    "avg_value": round(avg_value, 3),
                    "p75_value": round(p75_value, 3),
                    "rating": rating,
                    "threshold_good": thresholds[metric]["good"],
                    "threshold_poor": thresholds[metric]["needs_improvement"]
                }
        
        # Calculate overall pass rate
        passed_metrics = sum(
            1 for m in results["metrics"].values() if m["rating"] == "good"
        )
        total_metrics = len(results["metrics"])
        
        results["overall_pass_rate"] = (
            (passed_metrics / total_metrics) * 100 if total_metrics > 0 else 0
        )
        results["passed_metrics"] = passed_metrics
        results["total_metrics"] = total_metrics
        
        # Identify problematic pages if available
        if 'page_path' in web_vitals_data.columns:
            problem_pages = []
            for metric in ['lcp', 'fid', 'cls']:
                metric_col = f'avg_{metric}'
                if metric_col in web_vitals_data.columns:
                    poor_pages = web_vitals_data[
                        web_vitals_data[metric_col] > thresholds[metric]["needs_improvement"]
                    ].copy()
                    
                    for _, row in poor_pages.head(10).iterrows():
                        problem_pages.append({
                            "url": row['page_path'],
                            "metric": metric.upper(),
                            "value": round(row[metric_col], 3),
                            "sessions": int(row.get('sessions', 0))
                        })
            
            results["problem_pages"] = sorted(
                problem_pages,
                key=lambda x: x.get('sessions', 0),
                reverse=True
            )[:20]
        
        return results
        
    except Exception as e:
        logger.error(f"Error analyzing web vitals: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }


def analyze_bounce_rates(
    device_data: Optional[pd.DataFrame],
    landing_pages: Optional[pd.DataFrame]
) -> Dict[str, Any]:
    """
    Analyze bounce rates by device and identify problem pages
    
    Args:
        device_data: Device breakdown with bounce rates
        landing_pages: Landing page performance data
    
    Returns:
        Bounce rate analysis with problem areas identified
    """
    
    if device_data is None or device_data.empty:
        return {
            "status": "no_data",
            "message": "No device data available for bounce rate analysis"
        }
    
    try:
        # Bounce rate thresholds (as percentages)
        GOOD_BOUNCE_RATE = 40
        POOR_BOUNCE_RATE = 70
        
        # Analyze by device
        device_bounce_rates = []
        for _, row in device_data.iterrows():
            bounce_rate_pct = row.get('bounce_rate', 0) * 100
            
            if bounce_rate_pct < GOOD_BOUNCE_RATE:
                rating = "good"
            elif bounce_rate_pct < POOR_BOUNCE_RATE:
                rating = "acceptable"
            else:
                rating = "poor"
            
            device_bounce_rates.append({
                "device": row['device_category'],
                "bounce_rate": round(bounce_rate_pct, 2),
                "rating": rating,
                "sessions": int(row['sessions'])
            })
        
        # Calculate device-specific issues
        mobile_bounce = next(
            (d for d in device_bounce_rates if d['device'] == 'mobile'),
            None
        )
        desktop_bounce = next(
            (d for d in device_bounce_rates if d['device'] == 'desktop'),
            None
        )
        
        issues = []
        if mobile_bounce and desktop_bounce:
            bounce_diff = mobile_bounce['bounce_rate'] - desktop_bounce['bounce_rate']
            if bounce_diff > 15:
                issues.append({
                    "type": "mobile_bounce_high",
                    "severity": "high",
                    "message": f"Mobile bounce rate ({mobile_bounce['bounce_rate']}%) is {round(bounce_diff, 1)}% higher than desktop - indicates mobile UX issues",
                    "impact": "high"
                })
            elif bounce_diff < -15:
                issues.append({
                    "type": "desktop_bounce_high",
                    "severity": "medium",
                    "message": f"Desktop bounce rate ({desktop_bounce['bounce_rate']}%) is {round(abs(bounce_diff), 1)}% higher than mobile",
                    "impact": "medium"
                })
        
        results = {
            "status": "success",
            "by_device": device_bounce_rates,
            "issues": issues
        }
        
        # Analyze landing pages if available
        if landing_pages is not None and not landing_pages.empty:
            high_bounce_pages = landing_pages[
                landing_pages.get('bounce_rate', 0) > (POOR_BOUNCE_RATE / 100)
            ].copy()
            
            # Sort by traffic impact
            high_bounce_pages['impact'] = (
                high_bounce_pages['sessions'] * high_bounce_pages['bounce_rate']
            )
            high_bounce_pages = high_bounce_pages.sort_values('impact', ascending=False)
            
            problem_pages = []
            for _, row in high_bounce_pages.head(20).iterrows():
                problem_pages.append({
                    "url": row['page_path'],
                    "bounce_rate": round(row['bounce_rate'] * 100, 2),
                    "sessions": int(row['sessions']),
                    "avg_session_duration": round(row.get('avg_session_duration', 0), 2)
                })
            
            results["high_bounce_pages"] = {
                "count": len(high_bounce_pages),
                "pages": problem_pages
            }
        
        return results
        
    except Exception as e:
        logger.error(f"Error analyzing bounce rates: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }


def analyze_mobile_usability(
    device_data: Optional[pd.DataFrame],
    landing_pages: Optional[pd.DataFrame]
) -> Dict[str, Any]:
    """
    Comprehensive mobile usability analysis
    
    Args:
        device_data: Device breakdown data
        landing_pages: Landing page performance by device
    
    Returns:
        Mobile usability analysis with problem areas
    """
    
    if device_data is None or device_data.empty:
        return {
            "status": "no_data",
            "message": "No device data available for mobile analysis"
        }
    
    try:
        # Get mobile metrics
        mobile_data = device_data[device_data['device_category'] == 'mobile']
        desktop_data = device_data[device_data['device_category'] == 'desktop']
        
        if mobile_data.empty:
            return {
                "status": "no_mobile_data",
                "message": "No mobile traffic data available"
            }
        
        mobile_row = mobile_data.iloc[0]
        mobile_sessions = int(mobile_row['sessions'])
        mobile_bounce = mobile_row.get('bounce_rate', 0) * 100
        mobile_session_duration = mobile_row.get('avg_session_duration', 0)
        mobile_pages_per_session = mobile_row.get('pages_per_session', 0)
        
        # Compare to desktop if available
        desktop_comparison = {}
        if not desktop_data.empty:
            desktop_row = desktop_data.iloc[0]
            desktop_bounce = desktop_row.get('bounce_rate', 0) * 100
            desktop_session_duration = desktop_row.get('avg_session_duration', 0)
            desktop_pages_per_session = desktop_row.get('pages_per_session', 0)
            
            desktop_comparison = {
                "bounce_rate_diff": round(mobile_bounce - desktop_bounce, 2),
                "session_duration_diff": round(mobile_session_duration - desktop_session_duration, 2),
                "pages_per_session_diff": round(mobile_pages_per_session - desktop_pages_per_session, 2)
            }
        
        # Identify issues
        issues = []
        
        if mobile_bounce > 70:
            issues.append({
                "type": "high_mobile_bounce",
                "severity": "high",
                "message": f"Mobile bounce rate of {round(mobile_bounce, 1)}% is critically high",
                "recommendation": "Audit mobile page speed, CTAs, and content formatting"
            })
        
        if desktop_comparison.get('bounce_rate_diff', 0) > 15:
            issues.append({
                "type": "mobile_desktop_gap",
                "severity": "high",
                "message": f"Mobile bounce rate is {round(desktop_comparison['bounce_rate_diff'], 1)}% higher than desktop",
                "recommendation": "Focus on mobile-specific UX improvements"
            })
        
        if mobile_session_duration < 30:
            issues.append({
                "type": "low_mobile_engagement",
                "severity": "medium",
                "message": f"Average mobile session duration is only {round(mobile_session_duration, 1)} seconds",
                "recommendation": "Improve mobile content readability and navigation"
            })
        
        if desktop_comparison.get('pages_per_session_diff', 0) < -1:
            issues.append({
                "type": "poor_mobile_navigation",
                "severity": "medium",
                "message": "Mobile users view significantly fewer pages than desktop users",
                "recommendation": "Simplify mobile navigation and improve internal linking"
            })
        
        # Calculate mobile health score (0-100)
        health_score = 100
        health_score -= min(30, (mobile_bounce - 40) * 0.5) if mobile_bounce > 40 else 0
        health_score -= min(20, (30 - mobile_session_duration) * 0.3) if mobile_session_duration < 30 else 0
        health_score -= min(20, abs(desktop_comparison.get('bounce_rate_diff', 0)) * 0.5) if abs(desktop_comparison.get('bounce_rate_diff', 0)) > 10 else 0
        health_score = max(0, health_score)
        
        return {
            "status": "success",
            "mobile_health_score": round(health_score, 1),
            "metrics": {
                "sessions": mobile_sessions,
                "bounce_rate": round(mobile_bounce, 2),
                "avg_session_duration": round(mobile_session_duration, 2),
                "pages_per_session": round(mobile_pages_per_session, 2)
            },
            "desktop_comparison": desktop_comparison if desktop_comparison else None,
            "issues": issues,
            "overall_assessment": (
                "good" if health_score >= 80 else
                "needs_improvement" if health_score >= 60 else
                "poor"
            )
        }
        
    except Exception as e:
        logger.error(f"Error analyzing mobile usability: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }


def identify_technical_issues(ga4_data: Dict[str, pd.DataFrame]) -> List[Dict[str, Any]]:
    """
    Identify specific technical issues from all available data
    
    Args:
        ga4_data: All GA4 dataframes
    
    Returns:
        List of identified technical issues
    """
    
    issues = []
    
    try:
        # Check for JavaScript errors (if available in GA4 events)
        if 'events' in ga4_data and not ga4_data['events'].empty:
            js_errors = ga4_data['events'][
                ga4_data['events']['event_name'].str.contains('error|exception', case=False, na=False)
            ]
            if not js_errors.empty:
                error_count = len(js_errors)
                issues.append({
                    "type": "javascript_errors",
                    "severity": "high",
                    "count": int(error_count),
                    "message": f"{error_count} JavaScript errors detected in tracking",
                    "recommendation": "Review console errors and fix broken scripts"
                })
        
        # Check for 404 errors in page paths
        if 'landing_pages' in ga4_data and not ga4_data['landing_pages'].empty:
            landing_pages = ga4_data['landing_pages']
            potential_404s = landing_pages[
                landing_pages['bounce_rate'] > 0.95
            ]
            if len(potential_404s) > 0:
                issues.append({
                    "type": "potential_404_pages",
                    "severity": "medium",
                    "count": len(potential_404s),
                    "pages": potential_404s['page_path'].head(10).tolist(),
                    "message": f"{len(potential_404s)} pages with 95%+ bounce rate (possible 404s)",
                    "recommendation": "Check these pages for errors or remove from sitemap"
                })
        
        # Check for extremely slow pages
        if 'page_timings' in ga4_data and not ga4_data['page_timings'].empty:
            critical_slow = ga4_data['page_timings'][
                ga4_data['page_timings']['avg_page_load_time'] > 10
            ]
            if not critical_slow.empty:
                issues.append({
                    "type": "critical_slow_pages",
                    "severity": "critical",
                    "count": len(critical_slow),
                    "pages": critical_slow['page_path'].head(5).tolist(),
                    "message": f"{len(critical_slow)} pages taking over 10 seconds to load",
                    "recommendation": "Immediate optimization required for these pages"
                })
        
        # Check for device-specific rendering issues
        if 'device_breakdown' in ga4_data and not ga4_data['device_breakdown'].empty:
            device_data = ga4_data['device_breakdown']
            mobile_data = device_data[device_data['device_category'] == 'mobile']
            desktop_data = device_data[device_data['device_category'] == 'desktop']
            
            if not mobile_data.empty and not desktop_data.empty:
                mobile_bounce = mobile_data.iloc[0].get('bounce_rate', 0)
                desktop_bounce = desktop_data.iloc[0].get('bounce_rate', 0)
                
                if mobile_bounce > desktop_bounce * 1.5:
                    issues.append({
                        "type": "mobile_rendering_issue",
                        "severity": "high",
                        "message": "Mobile bounce rate 50%+ higher than desktop",
                        "mobile_bounce": round(mobile_bounce * 100, 2),
                        "desktop_bounce": round(desktop_bounce * 100, 2),
                        "recommendation": "Test mobile viewport, touch targets, and responsive design"
                    })
        
    except Exception as e:
        logger.error(f"Error identifying technical issues: {str(e)}")
    
    return issues


def generate_recommendations(analysis_results: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Generate prioritized technical recommendations based on all analysis
    
    Args:
        analysis_results: All analysis results from this module
    
    Returns:
        List of actionable recommendations
    """
    
    recommendations = []
    
    try:
        # Page speed recommendations
        page_speed = analysis_results.get('page_speed_analysis', {})
        if page_speed.get('status') == 'success':
            if page_speed.get('overall_rating') == 'poor':
                recommendations.append({
                    "category": "page_speed",
                    "priority": "critical",
                    "title": "Critical Page Speed Issues",
                    "issue": f"Average page load time of {page_speed['metrics']['avg_page_load_time']}s is well above acceptable threshold",
                    "action": "Implement immediate optimizations: enable compression, minify CSS/JS, optimize images, leverage browser caching",
                    "estimated_impact": "high",
                    "effort": "medium",
                    "affected_pages": page_speed.get('slow_pages', {}).get('count', 0)
                })
            elif page_speed.get('overall_rating') == 'needs_improvement':
                recommendations.append({
                    "category": "page_speed",
                    "priority": "high",
                    "title": "Page Speed Optimization Needed",
                    "issue": f"{page_speed.get('slow_pages', {}).get('count', 0)} pages loading slower than 4 seconds",
                    "action": "Focus on optimizing the slowest high-traffic pages first. Implement lazy loading for images below the fold",
                    "estimated_impact": "medium",
                    "effort": "low",
                    "affected_pages": page_speed.get('slow_pages', {}).get('count', 0)
                })
        
        # Core Web Vitals recommendations
        web_vitals = analysis_results.get('core_web_vitals', {})
        if web_vitals.get('status') == 'success':
            for metric, data in web_vitals.get('metrics', {}).items():
                if data['rating'] == 'poor':
                    metric_recommendations = {
                        'LCP': {
                            "action": "Optimize LCP: reduce server response time, eliminate render-blocking resources, optimize images",
                            "technical_detail": "Largest Contentful Paint measures loading performance"
                        },
                        'FID': {
                            "action": "Improve FID: minimize JavaScript execution time, break up long tasks, use web workers",
                            "technical_detail": "First Input Delay measures interactivity"
                        },
                        'CLS': {
                            "action": "Fix CLS: set size attributes on images/video, avoid inserting content above existing content, use transform animations",
                            "technical_detail": "Cumulative Layout Shift measures visual stability"
                        }
                    }
                    
                    if metric in metric_recommendations:
                        recommendations.append({
                            "category": "core_web_vitals",
                            "priority": "high",
                            "title": f"Fix {metric} Issues",
                            "issue": f"{metric} score of {data['p75_value']} is in poor range (threshold: {data['threshold_poor']})",
                            "action": metric_recommendations[metric]['action'],
                            "technical_detail": metric_recommendations[metric]['technical_detail'],
                            "estimated_impact": "high",
                            "effort": "medium"
                        })
        
        # Mobile usability recommendations
        mobile_analysis = analysis_results.get('mobile_usability', {})
        if mobile_analysis.get('status') == 'success':
            if mobile_analysis.get('overall_assessment') in ['poor', 'needs_improvement']:
                for issue in mobile_analysis.get('issues', []):
                    recommendations.append({
                        "category": "mobile_usability",
                        "priority": issue.get('severity', 'medium'),
                        "title": issue['message'],
                        "issue": issue['message'],
                        "action": issue.get('recommendation', ''),
                        "estimated_impact": "high" if issue.get('severity') == 'high' else "medium",
                        "effort": "medium"
                    })
        
        # Bounce rate recommendations
        bounce_analysis = analysis_results.get('bounce_rate_analysis', {})
        if bounce_analysis.get('status') == 'success':
            high_bounce_pages = bounce_analysis.get('high_bounce_pages', {})
            if high_bounce_pages.get('count', 0) > 0:
                recommendations.append({
                    "category": "bounce_rate",
                    "priority": "high",
                    "title": "High Bounce Rate Pages",
                    "issue": f"{high_bounce_pages['count']} pages with bounce rate over 70%",
                    "action": "Review content quality, improve internal linking, ensure mobile-friendliness, add clear CTAs",
                    "estimated_impact": "medium",
                    "effort": "medium",
                    "affected_pages": high_bounce_pages['count'],
                    "top_pages": [p['url'] for p in high_bounce_pages.get('pages', [])[:5]]
                })
        
        # Device-specific recommendations
        device_analysis = analysis_results.get('device_analysis', {})
        if device_analysis.get('status') == 'success':
            for issue in device_analysis.get('issues', []):
                if issue['type'] == 'low_mobile_traffic':
                    recommendations.append({
                        "category": "mobile_traffic",
                        "priority": "medium",
                        "title": "Low Mobile Traffic Share",
                        "issue": issue['message'],
                        "action": "Audit mobile usability, ensure mobile-friendly design, check for mobile crawl/indexing issues in GSC",
                        "estimated_impact": "medium",
                        "effort": "high"
                    })
        
        # Technical issues recommendations
        for issue in analysis_results.get('technical_issues', []):
            recommendations.append({
                "category": "technical_issue",
                "priority": issue.get('severity', 'medium'),
                "title": issue.get('type', 'Technical Issue'),
                "issue": issue['message'],
                "action": issue.get('recommendation', 'Review and fix identified issue'),
                "estimated_impact": "high" if issue.get('severity') == 'critical' else "medium",
                "effort": "medium",
                "technical_detail": issue
            })
        
        # Sort by priority
        priority_order = {'critical': 0, 'high': 1, 'medium': 2, 'low': 3}
        recommendations.sort(key=lambda x: priority_order.get(x['priority'], 4))
        
    except Exception as e:
        logger.error(f"Error generating recommendations: {str(e)}")
    
    return recommendations


def calculate_priority_score(analysis_results: Dict[str, Any]) -> float:
    """
    Calculate overall priority score for technical health (0-100)
    
    Args:
        analysis_results: All analysis results
    
    Returns:
        Priority score from 0 (perfect) to 100 (critical issues)
    """
    
    score = 0
    
    try:
        # Page speed impact (0-30 points)
        page_speed = analysis_results.get('page_speed_analysis', {})
        if page_speed.get('status') == 'success':
            rating = page_speed.get('overall_rating')
            if rating == 'poor':
                score += 30
            elif rating == 'needs_improvement':
                score += 15
        
        # Core Web Vitals impact (0-25 points)
        web_vitals = analysis_results.get('core_web_vitals', {})
        if web_vitals.get('status') == 'success':
            pass_rate = web_vitals.get('overall_pass_rate', 100)
            score += (100 - pass_rate) * 0.25
        
        # Mobile usability impact (0-25 points)
        mobile = analysis_results.get('mobile_usability', {})
        if mobile.get('status') == 'success':
            health_score = mobile.get('mobile_health_score', 100)
            score += (100 - health_score) * 0.25
        
        # Bounce rate impact (0-15 points)
        bounce = analysis_results.get('bounce_rate_analysis', {})
        if bounce.get('status') == 'success':
            issue_count = len(bounce.get('issues', []))
            score += min(15, issue_count * 5)
        
        # Technical issues impact (0-5 points)
        issues = analysis_results.get('technical_issues', [])
        critical_issues = sum(1 for i in issues if i.get('severity') == 'critical')
        score += min(5, critical_issues * 5)
        
        score = min(100, max(0, score))
        
    except Exception as e:
        logger.error(f"Error calculating priority score: {str(e)}")
        score = 50  # Default to medium priority if error
    
    return round(score, 1)


def generate_summary(analysis_results: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generate executive summary of technical health analysis
    
    Args:
        analysis_results: All analysis results
    
    Returns:
        Summary dictionary
    """
    
    try:
        priority_score = analysis_results.get('priority_score', 0)
        
        # Determine overall health
        if priority_score < 20:
            overall_health = "excellent"
            health_description = "Technical health is excellent with no critical issues"
        elif priority_score < 40:
            overall_health = "good"
            health_description = "Technical health is good with minor optimization opportunities"
        elif priority_score < 60:
            overall_health = "needs_improvement"
            health_description = "Technical health needs improvement with several issues to address"
        elif priority_score < 80:
            overall_health = "poor"
            health_description = "Technical health is poor with multiple critical issues"
        else:
            overall_health = "critical"
            health_description = "Technical health is critical and requires immediate attention"
        
        # Count issues by severity
        recommendations = analysis_results.get('recommendations', [])
        critical_count = sum(1 for r in recommendations if r.get('priority') == 'critical')
        high_count = sum(1 for r in recommendations if r.get('priority') == 'high')
        medium_count = sum(1 for r in recommendations if r.get('priority') == 'medium')
        
        # Key metrics
        key_metrics = {}
        
        page_speed = analysis_results.get('page_speed_analysis', {})
        if page_speed.get('status') == 'success':
            key_metrics['avg_page_load_time'] = page_speed.get('metrics', {}).get('avg_page_load_time')
        
        mobile = analysis_results.get('mobile_usability', {})
        if mobile.get('status') == 'success':
            key_metrics['mobile_health_score'] = mobile.get('mobile_health_score')
        
        device = analysis_results.get('device_analysis', {})
        if device.get('status') == 'success':
            key_metrics['mobile_traffic_pct'] = device.get('mobile_percentage')
        
        return {
            "overall_health": overall_health,
            "priority_score": priority_score,
            "description": health_description,
            "issue_counts": {
                "critical": critical_count,
                "high": high_count,
                "medium": medium_count,
                "total": len(recommendations)
            },
            "key_metrics": key_metrics,
            "top_priority": recommendations[0] if recommendations else None
        }
        
    except Exception as e:
        logger.error(f"Error generating summary: {str(e)}")
        return {
            "overall_health": "unknown",
            "priority_score": 0,
            "description": "Unable to generate summary",
            "issue_counts": {"critical": 0, "high": 0, "medium": 0, "total": 0}
        }
