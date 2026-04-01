"""
Module 5: Technical SEO Analysis

Analyzes technical SEO issues including mobile usability, page speed,
HTTPS coverage, and crawl errors/indexing issues from GSC and GA4 data.
"""

import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from collections import defaultdict, Counter

logger = logging.getLogger(__name__)


class TechnicalSEOAnalyzer:
    """Analyzes technical SEO issues from GSC and GA4 data."""
    
    def __init__(self):
        self.severity_weights = {
            'critical': 1.0,
            'high': 0.7,
            'medium': 0.4,
            'low': 0.2
        }
    
    def analyze(
        self,
        gsc_data: Dict[str, Any],
        ga4_data: Dict[str, Any],
        crawl_data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Main analysis function for technical SEO issues.
        
        Args:
            gsc_data: GSC data including coverage, URL inspection, sitemaps
            ga4_data: GA4 data including device breakdown and page speed metrics
            crawl_data: Optional crawl data for additional technical checks
            
        Returns:
            Structured results with issues, severity, and recommendations
        """
        try:
            results = {
                'mobile_usability': self._analyze_mobile_usability(ga4_data, gsc_data),
                'page_speed': self._analyze_page_speed(ga4_data, gsc_data),
                'https_coverage': self._analyze_https_coverage(gsc_data, crawl_data),
                'crawl_errors': self._analyze_crawl_errors(gsc_data),
                'indexing_issues': self._analyze_indexing_issues(gsc_data),
                'technical_health_score': 0.0,
                'priority_fixes': [],
                'summary': {}
            }
            
            # Calculate overall technical health score
            results['technical_health_score'] = self._calculate_health_score(results)
            
            # Generate prioritized fix list
            results['priority_fixes'] = self._generate_priority_fixes(results)
            
            # Generate summary statistics
            results['summary'] = self._generate_summary(results)
            
            return results
            
        except Exception as e:
            logger.error(f"Error in technical SEO analysis: {str(e)}")
            raise
    
    def _analyze_mobile_usability(
        self,
        ga4_data: Dict[str, Any],
        gsc_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Analyze mobile usability issues from GA4 device data and GSC mobile usability reports.
        """
        issues = []
        
        # Analyze GA4 device breakdown
        device_data = ga4_data.get('device_breakdown', {})
        
        if device_data:
            mobile_sessions = device_data.get('mobile', {}).get('sessions', 0)
            desktop_sessions = device_data.get('desktop', {}).get('sessions', 0)
            tablet_sessions = device_data.get('tablet', {}).get('sessions', 0)
            
            total_sessions = mobile_sessions + desktop_sessions + tablet_sessions
            
            if total_sessions > 0:
                mobile_pct = (mobile_sessions / total_sessions) * 100
                
                # Mobile engagement metrics
                mobile_bounce = device_data.get('mobile', {}).get('bounce_rate', 0)
                desktop_bounce = device_data.get('desktop', {}).get('bounce_rate', 0)
                
                mobile_engagement = device_data.get('mobile', {}).get('avg_engagement_time', 0)
                desktop_engagement = device_data.get('desktop', {}).get('avg_engagement_time', 0)
                
                # Check for mobile usability issues based on engagement disparity
                if mobile_bounce > desktop_bounce * 1.3 and mobile_pct > 30:
                    issues.append({
                        'issue': 'high_mobile_bounce_rate',
                        'severity': 'high',
                        'description': f'Mobile bounce rate ({mobile_bounce:.1f}%) is {mobile_bounce/desktop_bounce:.1f}x higher than desktop ({desktop_bounce:.1f}%)',
                        'affected_traffic_pct': mobile_pct,
                        'metrics': {
                            'mobile_bounce_rate': mobile_bounce,
                            'desktop_bounce_rate': desktop_bounce,
                            'mobile_traffic_pct': mobile_pct
                        },
                        'recommendation': 'Improve mobile page layout, increase touch target sizes, optimize for viewport, reduce interstitials'
                    })
                
                if desktop_engagement > 0 and mobile_engagement < desktop_engagement * 0.6 and mobile_pct > 30:
                    issues.append({
                        'issue': 'low_mobile_engagement',
                        'severity': 'medium',
                        'description': f'Mobile engagement time ({mobile_engagement:.0f}s) is significantly lower than desktop ({desktop_engagement:.0f}s)',
                        'affected_traffic_pct': mobile_pct,
                        'metrics': {
                            'mobile_engagement_time': mobile_engagement,
                            'desktop_engagement_time': desktop_engagement,
                            'engagement_ratio': mobile_engagement / desktop_engagement if desktop_engagement > 0 else 0
                        },
                        'recommendation': 'Optimize mobile content readability, reduce above-the-fold clutter, improve mobile navigation'
                    })
                
                # Check if mobile traffic is low despite mobile-first indexing
                if mobile_pct < 40 and total_sessions > 1000:
                    issues.append({
                        'issue': 'low_mobile_traffic_share',
                        'severity': 'low',
                        'description': f'Mobile traffic ({mobile_pct:.1f}%) is below expected levels for most industries',
                        'affected_traffic_pct': mobile_pct,
                        'metrics': {
                            'mobile_traffic_pct': mobile_pct,
                            'expected_range': '50-70%'
                        },
                        'recommendation': 'Verify mobile-friendliness in GSC, check for mobile-specific crawl errors, ensure responsive design'
                    })
        
        # Analyze GSC mobile usability issues if available
        gsc_mobile_issues = gsc_data.get('mobile_usability_issues', [])
        
        for gsc_issue in gsc_mobile_issues:
            issue_type = gsc_issue.get('issue_type', '')
            affected_pages = gsc_issue.get('affected_pages', [])
            
            severity = 'high' if len(affected_pages) > 10 else 'medium'
            
            issue_recommendations = {
                'MOBILE_USABILITY_ERROR': 'Fix mobile usability errors reported in GSC',
                'VIEWPORT_NOT_CONFIGURED': 'Add viewport meta tag: <meta name="viewport" content="width=device-width, initial-scale=1">',
                'TEXT_TOO_SMALL': 'Increase base font size to at least 16px for mobile devices',
                'CLICKABLE_ELEMENTS_TOO_CLOSE': 'Increase spacing between clickable elements to at least 48x48 CSS pixels',
                'CONTENT_WIDER_THAN_SCREEN': 'Ensure all content fits within viewport width, avoid fixed-width elements',
                'USES_INCOMPATIBLE_PLUGINS': 'Remove Flash or other incompatible plugins, replace with HTML5 alternatives'
            }
            
            if issue_type and affected_pages:
                issues.append({
                    'issue': issue_type.lower(),
                    'severity': severity,
                    'description': f'GSC reports {issue_type.replace("_", " ").lower()} on {len(affected_pages)} pages',
                    'affected_pages': affected_pages[:10],  # Limit to first 10
                    'total_affected_pages': len(affected_pages),
                    'recommendation': issue_recommendations.get(issue_type, 'Review and fix mobile usability issue in GSC')
                })
        
        return {
            'issues': issues,
            'total_issues': len(issues),
            'critical_issues': len([i for i in issues if i['severity'] == 'critical']),
            'high_issues': len([i for i in issues if i['severity'] == 'high']),
            'medium_issues': len([i for i in issues if i['severity'] == 'medium']),
            'low_issues': len([i for i in issues if i['severity'] == 'low']),
            'mobile_traffic_share': mobile_pct if device_data else 0
        }
    
    def _analyze_page_speed(
        self,
        ga4_data: Dict[str, Any],
        gsc_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Analyze page speed issues from GA4 and GSC Core Web Vitals data.
        """
        issues = []
        
        # Analyze Core Web Vitals from GSC if available
        cwv_data = gsc_data.get('core_web_vitals', {})
        
        if cwv_data:
            # LCP (Largest Contentful Paint) analysis
            lcp_data = cwv_data.get('lcp', {})
            poor_lcp_urls = lcp_data.get('poor_urls', [])
            needs_improvement_lcp_urls = lcp_data.get('needs_improvement_urls', [])
            
            if poor_lcp_urls:
                issues.append({
                    'issue': 'poor_lcp',
                    'severity': 'high',
                    'description': f'{len(poor_lcp_urls)} pages have poor LCP (>4.0s)',
                    'affected_pages': poor_lcp_urls[:10],
                    'total_affected_pages': len(poor_lcp_urls),
                    'metric': 'LCP',
                    'threshold': '>4.0s',
                    'recommendation': 'Optimize LCP by: reducing server response time (TTFB), optimizing images, preloading critical resources, removing render-blocking JavaScript/CSS'
                })
            
            if needs_improvement_lcp_urls and len(needs_improvement_lcp_urls) > 20:
                issues.append({
                    'issue': 'needs_improvement_lcp',
                    'severity': 'medium',
                    'description': f'{len(needs_improvement_lcp_urls)} pages need LCP improvement (2.5s-4.0s)',
                    'affected_pages': needs_improvement_lcp_urls[:10],
                    'total_affected_pages': len(needs_improvement_lcp_urls),
                    'metric': 'LCP',
                    'threshold': '2.5s-4.0s',
                    'recommendation': 'Improve LCP to <2.5s by optimizing largest element loading (usually hero image or banner)'
                })
            
            # FID/INP (First Input Delay / Interaction to Next Paint) analysis
            fid_data = cwv_data.get('fid', {})
            inp_data = cwv_data.get('inp', {})
            
            interaction_data = inp_data if inp_data else fid_data
            poor_interaction_urls = interaction_data.get('poor_urls', [])
            
            if poor_interaction_urls:
                metric_name = 'INP' if inp_data else 'FID'
                threshold = '>500ms' if inp_data else '>300ms'
                
                issues.append({
                    'issue': 'poor_interaction_responsiveness',
                    'severity': 'high',
                    'description': f'{len(poor_interaction_urls)} pages have poor {metric_name} ({threshold})',
                    'affected_pages': poor_interaction_urls[:10],
                    'total_affected_pages': len(poor_interaction_urls),
                    'metric': metric_name,
                    'threshold': threshold,
                    'recommendation': 'Improve responsiveness by: breaking up long JavaScript tasks, optimizing third-party scripts, using web workers, debouncing event handlers'
                })
            
            # CLS (Cumulative Layout Shift) analysis
            cls_data = cwv_data.get('cls', {})
            poor_cls_urls = cls_data.get('poor_urls', [])
            
            if poor_cls_urls:
                issues.append({
                    'issue': 'poor_cls',
                    'severity': 'high',
                    'description': f'{len(poor_cls_urls)} pages have poor CLS (>0.25)',
                    'affected_pages': poor_cls_urls[:10],
                    'total_affected_pages': len(poor_cls_urls),
                    'metric': 'CLS',
                    'threshold': '>0.25',
                    'recommendation': 'Fix layout shifts by: specifying image/video dimensions, reserving space for ads/embeds, avoiding inserting content above existing content, using transform animations instead of layout-triggering properties'
                })
        
        # Analyze page load times from GA4 if available
        page_speed_data = ga4_data.get('page_speed', {})
        
        if page_speed_data:
            slow_pages = page_speed_data.get('slow_pages', [])
            
            for page in slow_pages:
                avg_load_time = page.get('avg_load_time', 0)
                page_views = page.get('page_views', 0)
                
                if avg_load_time > 5 and page_views > 100:
                    severity = 'high' if avg_load_time > 8 else 'medium'
                    
                    issues.append({
                        'issue': 'slow_page_load',
                        'severity': severity,
                        'description': f'Page has slow average load time: {avg_load_time:.1f}s',
                        'affected_pages': [page.get('page_path', '')],
                        'page_views': page_views,
                        'avg_load_time': avg_load_time,
                        'recommendation': 'Conduct detailed page speed audit using Lighthouse or PageSpeed Insights'
                    })
        
        # Check for mobile vs desktop speed disparity
        mobile_speed = ga4_data.get('device_breakdown', {}).get('mobile', {}).get('avg_page_load_time', 0)
        desktop_speed = ga4_data.get('device_breakdown', {}).get('desktop', {}).get('avg_page_load_time', 0)
        
        if mobile_speed > 0 and desktop_speed > 0 and mobile_speed > desktop_speed * 1.5:
            issues.append({
                'issue': 'mobile_speed_disparity',
                'severity': 'medium',
                'description': f'Mobile load time ({mobile_speed:.1f}s) is significantly slower than desktop ({desktop_speed:.1f}s)',
                'metrics': {
                    'mobile_load_time': mobile_speed,
                    'desktop_load_time': desktop_speed,
                    'disparity_ratio': mobile_speed / desktop_speed
                },
                'recommendation': 'Optimize for mobile specifically: reduce image sizes, minimize JavaScript, implement lazy loading, consider AMP or similar'
            })
        
        return {
            'issues': issues,
            'total_issues': len(issues),
            'critical_issues': len([i for i in issues if i['severity'] == 'critical']),
            'high_issues': len([i for i in issues if i['severity'] == 'high']),
            'medium_issues': len([i for i in issues if i['severity'] == 'medium']),
            'low_issues': len([i for i in issues if i['severity'] == 'low']),
            'cwv_summary': self._summarize_cwv(cwv_data) if cwv_data else {}
        }
    
    def _summarize_cwv(self, cwv_data: Dict[str, Any]) -> Dict[str, Any]:
        """Summarize Core Web Vitals status."""
        summary = {
            'lcp_status': 'unknown',
            'fid_inp_status': 'unknown',
            'cls_status': 'unknown',
            'overall_status': 'unknown'
        }
        
        # LCP status
        lcp_data = cwv_data.get('lcp', {})
        lcp_good = len(lcp_data.get('good_urls', []))
        lcp_poor = len(lcp_data.get('poor_urls', []))
        lcp_needs_improvement = len(lcp_data.get('needs_improvement_urls', []))
        lcp_total = lcp_good + lcp_poor + lcp_needs_improvement
        
        if lcp_total > 0:
            lcp_good_pct = (lcp_good / lcp_total) * 100
            if lcp_good_pct >= 75:
                summary['lcp_status'] = 'good'
            elif lcp_good_pct >= 50:
                summary['lcp_status'] = 'needs_improvement'
            else:
                summary['lcp_status'] = 'poor'
        
        # FID/INP status
        fid_data = cwv_data.get('fid', {})
        inp_data = cwv_data.get('inp', {})
        interaction_data = inp_data if inp_data else fid_data
        
        interaction_good = len(interaction_data.get('good_urls', []))
        interaction_poor = len(interaction_data.get('poor_urls', []))
        interaction_needs_improvement = len(interaction_data.get('needs_improvement_urls', []))
        interaction_total = interaction_good + interaction_poor + interaction_needs_improvement
        
        if interaction_total > 0:
            interaction_good_pct = (interaction_good / interaction_total) * 100
            if interaction_good_pct >= 75:
                summary['fid_inp_status'] = 'good'
            elif interaction_good_pct >= 50:
                summary['fid_inp_status'] = 'needs_improvement'
            else:
                summary['fid_inp_status'] = 'poor'
        
        # CLS status
        cls_data = cwv_data.get('cls', {})
        cls_good = len(cls_data.get('good_urls', []))
        cls_poor = len(cls_data.get('poor_urls', []))
        cls_needs_improvement = len(cls_data.get('needs_improvement_urls', []))
        cls_total = cls_good + cls_poor + cls_needs_improvement
        
        if cls_total > 0:
            cls_good_pct = (cls_good / cls_total) * 100
            if cls_good_pct >= 75:
                summary['cls_status'] = 'good'
            elif cls_good_pct >= 50:
                summary['cls_status'] = 'needs_improvement'
            else:
                summary['cls_status'] = 'poor'
        
        # Overall status (needs all three to be good)
        statuses = [summary['lcp_status'], summary['fid_inp_status'], summary['cls_status']]
        if all(s == 'good' for s in statuses):
            summary['overall_status'] = 'good'
        elif any(s == 'poor' for s in statuses):
            summary['overall_status'] = 'poor'
        else:
            summary['overall_status'] = 'needs_improvement'
        
        return summary
    
    def _analyze_https_coverage(
        self,
        gsc_data: Dict[str, Any],
        crawl_data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Analyze HTTPS coverage and mixed content issues.
        """
        issues = []
        
        # Check URL inspection data for HTTPS issues
        url_inspection_data = gsc_data.get('url_inspection_results', [])
        
        http_pages = []
        mixed_content_pages = []
        certificate_issues = []
        
        for inspection in url_inspection_data:
            url = inspection.get('url', '')
            verdict = inspection.get('indexing_state', '')
            
            # Check for HTTP pages
            if url.startswith('http://'):
                http_pages.append(url)
            
            # Check for security issues
            security_issues = inspection.get('security_issues', [])
            for issue in security_issues:
                if 'mixed' in issue.lower():
                    mixed_content_pages.append(url)
                if 'certificate' in issue.lower() or 'ssl' in issue.lower():
                    certificate_issues.append(url)
        
        if http_pages:
            issues.append({
                'issue': 'http_pages',
                'severity': 'critical',
                'description': f'{len(http_pages)} pages still using HTTP instead of HTTPS',
                'affected_pages': http_pages[:10],
                'total_affected_pages': len(http_pages),
                'recommendation': 'Migrate all pages to HTTPS: obtain SSL certificate, update internal links, implement 301 redirects from HTTP to HTTPS, update canonical tags'
            })
        
        if mixed_content_pages:
            issues.append({
                'issue': 'mixed_content',
                'severity': 'high',
                'description': f'{len(mixed_content_pages)} HTTPS pages loading insecure (HTTP) resources',
                'affected_pages': mixed_content_pages[:10],
                'total_affected_pages': len(mixed_content_pages),
                'recommendation': 'Fix mixed content warnings: update all resource URLs (images, scripts, stylesheets) to use HTTPS or protocol-relative URLs'
            })
        
        if certificate_issues:
            issues.append({
                'issue': 'ssl_certificate_issues',
                'severity': 'critical',
                'description': f'{len(certificate_issues)} pages have SSL certificate issues',
                'affected_pages': certificate_issues[:10],
                'total_affected_pages': len(certificate_issues),
                'recommendation': 'Fix SSL certificate issues: ensure certificate is valid, not expired, and covers all subdomains. Check certificate chain completeness.'
            })
        
        # Analyze from crawl data if available
        if crawl_data:
            crawled_urls = crawl_data.get('urls', [])
            
            http_urls = [url for url in crawled_urls if url.get('url', '').startswith('http://')]
            
            if http_urls and not http_pages:  # If we found HTTP in crawl but not in GSC
                issues.append({
                    'issue': 'http_pages_in_crawl',
                    'severity': 'high',
                    'description': f'{len(http_urls)} HTTP pages found in site crawl',
                    'affected_pages': [u.get('url') for u in http_urls[:10]],
                    'total_affected_pages': len(http_urls),
                    'recommendation': 'Internal links pointing to HTTP pages found. Update internal link structure to use HTTPS URLs.'
                })
        
        # Check GSC settings for preferred HTTPS version
        site_url = gsc_data.get('site_url', '')
        if site_url.startswith('http://'):
            issues.append({
                'issue': 'gsc_http_property',
                'severity': 'medium',
                'description': 'GSC property is set to HTTP version',
                'recommendation': 'Add HTTPS version as a separate property in GSC and set it as preferred. Move all tracking to HTTPS property.'
            })
        
        return {
            'issues': issues,
            'total_issues': len(issues),
            'critical_issues': len([i for i in issues if i['severity'] == 'critical']),
            'high_issues': len([i for i in issues if i['severity'] == 'high']),
            'medium_issues': len([i for i in issues if i['severity'] == 'medium']),
            'low_issues': len([i for i in issues if i['severity'] == 'low']),
            'https_percentage': self._calculate_https_percentage(url_inspection_data)
        }
    
    def _calculate_https_percentage(self, url_inspection_data: List[Dict]) -> float:
        """Calculate percentage of pages using HTTPS."""
        if not url_inspection_data:
            return 0.0
        
        https_count = sum(1 for u in url_inspection_data if u.get('url', '').startswith('https://'))
        total = len(url_inspection_data)
        
        return (https_count / total * 100) if total > 0 else 0.0
    
    def _analyze_crawl_errors(self, gsc_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze crawl errors from GSC coverage report.
        """
        issues = []
        
        # Get crawl errors from coverage data
        coverage_data = gsc_data.get('coverage', {})
        
        error_types = {
            'server_error': {
                'patterns': ['server_error', '5xx', '500', '502', '503'],
                'severity': 'critical',
                'description': 'Server errors (5xx) preventing crawling'
            },
            'not_found': {
                'patterns': ['not_found', '404'],
                'severity': 'high',
                'description': '404 errors for pages that should exist'
            },
            'redirect_error': {
                'patterns': ['redirect_error', 'redirect_loop', 'redirect_chain'],
                'severity': 'medium',
                'description': 'Redirect issues (loops, long chains)'
            },
            'robots_blocked': {
                'patterns': ['blocked_by_robots', 'robots.txt'],
                'severity': 'high',
                'description': 'Pages blocked by robots.txt that should be crawlable'
            },
            'soft_404': {
                'patterns': ['soft_404', 'soft 404'],
                'severity': 'medium',
                'description': 'Soft 404 errors (page returns 200 but appears to be 404)'
            }
        }
        
        errors = coverage_data.get('errors', [])
        
        for error in errors:
            error_type = error.get('type', '').lower()
            affected_urls = error.get('urls', [])
            
            for error_category, config in error_types.items():
                if any(pattern in error_type for pattern in config['patterns']):
                    if affected_urls:
                        # Get click data for affected URLs if available
                        urls_with_clicks = self._get_urls_with_traffic(
                            affected_urls,
                            gsc_data.get('performance', {})
                        )
                        
                        # Higher severity if URLs have search traffic
                        adjusted_severity = config['severity']
                        if urls_with_clicks and config['severity'] != 'critical':
                            adjusted_severity = 'critical' if config['severity'] == 'high' else 'high'
                        
                        recommendations = {
                            'server_error': 'Investigate server errors immediately: check server logs, increase server resources, fix application errors',
                            'not_found': 'Fix or redirect 404 pages: restore content if deleted accidentally, implement 301 redirects to relevant pages, or return proper 410 Gone if intentional',
                            'redirect_error': 'Fix redirect issues: remove redirect loops, consolidate redirect chains to single-hop redirects, use 301 for permanent redirects',
                            'robots_blocked': 'Update robots.txt to allow crawling of important pages. Review disallow rules and remove unnecessary blocks.',
                            'soft_404': 'Fix soft 404s: ensure error pages return proper 404 status code, add substantial content to thin pages, or implement proper redirects'
                        }
                        
                        issues.append({
                            'issue': error_category,
                            'severity': adjusted_severity,
                            'description': f'{config["description"]}: {len(affected_urls)} URLs affected',
                            'affected_pages': affected_urls[:10],
                            'total_affected_pages': len(affected_urls),
                            'urls_with_traffic': len(urls_with_clicks),
                            'error_type': error_type,
                            'recommendation': recommendations.get(error_category, 'Review and fix crawl errors in GSC')
                        })
                    break
        
        # Check crawl rate issues
        crawl_stats = gsc_data.get('crawl_stats', {})
        if crawl_stats:
            avg_crawl_rate = crawl_stats.get('avg_requests_per_day', 0)
            total_pages = crawl_stats.get('total_pages_crawled', 0)
            
            if total_pages > 1000 and avg_crawl_rate < total_pages * 0.1:
                issues.append({
                    'issue': 'low_crawl_rate',
                    'severity': 'medium',
                    'description': f'Low crawl rate: only {avg_crawl_rate:.0f} pages/day crawled out of {total_pages} total pages',
                    'metrics': {
                        'avg_crawl_rate': avg_crawl_rate,
                        'total_pages': total_pages,
                        'crawl_percentage': (avg_crawl_rate / total_pages * 100) if total_pages > 0 else 0
                    },
                    'recommendation': 'Improve crawl efficiency: fix server response times, reduce duplicate content, improve internal linking, submit XML sitemap'
                })
        
        return {
            'issues': issues,
            'total_issues': len(issues),
            'critical_issues': len([i for i in issues if i['severity'] == 'critical']),
            'high_issues': len([i for i in issues if i['severity'] == 'high']),
            'medium_issues': len([i for i in issues if i['severity'] == 'medium']),
            'low_issues': len([i for i in issues if i['severity'] == 'low']),
            'total_error_urls': sum(i.get('total_affected_pages', 0) for i in issues)
        }
    
    def _analyze_indexing_issues(self, gsc_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze indexing issues from GSC coverage report.
        """
        issues = []
        
        coverage_data = gsc_data.get('coverage', {})
        
        # Analyze excluded pages
        excluded = coverage_data.get('excluded', [])
        
        exclusion_categories = {
            'excluded_by_noindex': {
                'severity': 'medium',
                'description': 'Pages excluded due to noindex tag',
                'recommendation': 'Review noindex tags: ensure they are intentional. Remove noindex from pages that should be indexed.'
            },
            'crawled_not_indexed': {
                'severity': 'high',
                'description': 'Pages crawled but not indexed (quality issue)',
                'recommendation': 'Improve page quality: add substantial unique content, improve relevance, build internal links, ensure proper schema markup'
            },
            'discovered_not_crawled': {
                'severity': 'medium',
                'description': 'Pages discovered but not yet crawled',
                'recommendation': 'Improve crawlability: add to XML sitemap, add internal links, increase site crawl budget'
            },
            'alternate_page_with_canonical': {
                'severity': 'low',
                'description': 'Alternate page with proper canonical tag',
                'recommendation': 'No action needed if canonical is correct. Verify canonical tags point to preferred versions.'
            },
            'duplicate_content': {
                'severity': 'high',
                'description': 'Pages marked as duplicate without user-selected canonical',
                'recommendation': 'Fix duplicate content: consolidate similar pages, implement canonical tags, use 301 redirects, add substantial unique content'
            },
            'page_with_redirect': {
                'severity': 'medium',
                'description': 'URLs that redirect',
                'recommendation': 'Update internal links to point directly to final destination. Check for redirect chains.'
            }
        }
        
        for exclusion in excluded:
            exclusion_type = exclusion.get('type', '').lower()
            affected_urls = exclusion.get('urls', [])
            
            for category, config in exclusion_categories.items():
                category_lower = category.lower().replace('_', ' ')
                
                if category_lower in exclusion_type or any(
                    word in exclusion_type for word in category.split('_')
                ):
                    if affected_urls:
                        # Check if excluded pages have historical traffic
                        urls_with_history = self._get_urls_with_historical_traffic(
                            affected_urls,
                            gsc_data.get('performance', {})
                        )
                        
                        # Increase severity if pages had traffic before
                        adjusted_severity = config['severity']
                        if urls_with_history and len(urls_with_history) > len(affected_urls) * 0.3:
                            if adjusted_severity == 'medium':
                                adjusted_severity = 'high'
                            elif adjusted_severity == 'low':
                                adjusted_severity = 'medium'
                        
                        issues.append({
                            'issue': category,
                            'severity': adjusted_severity,
                            'description': f'{config["description"]}: {len(affected_urls)} pages',
                            'affected_pages': affected_urls[:10],
                            'total_affected_pages': len(affected_urls),
                            'pages_with_historical_traffic': len(urls_with_history),
                            'recommendation': config['recommendation']
                        })
                    break
        
        # Analyze valid pages with issues
        valid_with_warnings = coverage_data.get('valid_with_warnings', [])
        
        for warning in valid_with_warnings:
            warning_type = warning.get('type', '').lower()
            affected_urls = warning.get('urls', [])
            
            if 'indexed_not_submitted' in warning_type and len(affected_urls) > 100:
                issues.append({
                    'issue': 'indexed_not_in_sitemap',
                    'severity': 'low',
                    'description': f'{len(affected_urls)} indexed pages not in sitemap',
                    'affected_pages': affected_urls[:10],
                    'total_affected_pages': len(affected_urls),
                    'recommendation': 'Add important pages to XML sitemap. Regenerate sitemap to include all indexable pages.'
                })
        
        # Check indexing coverage ratio
        total_valid = len(coverage_data.get('valid', []))
        total_excluded = len(excluded)
        total_errors = len(coverage_data.get('errors', []))
        
        total_discovered = total_valid + total_excluded + total_errors
        
        if total_discovered > 0:
            index_coverage_ratio = (total_valid / total_discovered) * 100
            
            if index_coverage_ratio < 60 and total_discovered > 100:
                issues.append({
                    'issue': 'low_index_coverage',
                    'severity': 'high',
                    'description': f'Only {index_coverage_ratio:.1f}% of discovered pages are indexed',
                    'metrics': {
                        'total_valid': total_valid,
                        'total_excluded': total_excluded,
                        'total_errors': total_errors,
                        'index_coverage_ratio': index_coverage_ratio
                    },
                    'recommendation': 'Investigate why so many pages are excluded or have errors. Focus on fixing high-severity issues first.'
                })
        
        return {
            'issues': issues,
            'total_issues': len(issues),
            'critical_issues': len([i for i in issues if i['severity'] == 'critical']),
            'high_issues': len([i for i in issues if i['severity'] == 'high']),
            'medium_issues': len([i for i in issues if i['severity'] == 'medium']),
            'low_issues': len([i for i in issues if i['severity'] == 'low']),
            'index_coverage_ratio': (total_valid / total_discovered * 100) if total_discovered > 0 else 0,
            'total_valid_pages': total_valid,
            'total_excluded_pages': total_excluded
        }
    
    def _get_urls_with_traffic(
        self,
        urls: List[str],
        performance_data: Dict[str, Any]
    ) -> List[str]:
        """Identify which URLs have current search traffic."""
        urls_with_traffic = []
        
        page_data = performance_data.get('by_page', [])
        
        for page in page_data:
            page_url = page.get('page', '')
            clicks = page.get('clicks', 0)
            
            if page_url in urls and clicks > 0:
                urls_with_traffic.append(page_url)
        
        return urls_with_traffic
    
    def _get_urls_with_historical_traffic(
        self,
        urls: List[str],
        performance_data: Dict[str, Any]
    ) -> List[str]:
        """Identify which URLs had historical search traffic."""
        urls_with_history = []
        
        # Check time series data for any historical clicks
        time_series = performance_data.get('page_time_series', [])
        
        pages_with_history = set()
        for entry in time_series:
            page_url = entry.get('page', '')
            clicks = entry.get('clicks', 0)
            
            if clicks > 0:
                pages_with_history.add(page_url)
        
        for url in urls:
            if url in pages_with_history:
                urls_with_history.append(url)
        
        return urls_with_history
    
    def _calculate_health_score(self, results: Dict[str, Any]) -> float:
        """
        Calculate overall technical health score (0-100).
        """
        category_scores = {}
        
        # Score each category
        for category in ['mobile_usability', 'page_speed', 'https_coverage', 
                        'crawl_errors', 'indexing_issues']:
            category_data = results.get(category, {})
            issues = category_data.get('issues', [])
            
            if not issues:
                category_scores[category] = 100.0
            else:
                # Calculate deductions based on severity
                total_deduction = 0
                for issue in issues:
                    severity = issue.get('severity', 'low')
                    weight = self.severity_weights.get(severity, 0.2)
                    
                    # Base deduction
                    deduction = weight * 20  # Max 20 points per critical issue
                    
                    # Scale by number of affected pages (capped)
                    affected = min(issue.get('total_affected_pages', 1), 100)
                    page_multiplier = 1 + (affected / 100)  # Max 2x multiplier
                    
                    total_deduction += deduction * page_multiplier
                
                # Cap total deduction at 100
                total_deduction = min(total_deduction, 100)
                category_scores[category] = max(0, 100 - total_deduction)
        
        # Weighted average (some categories more important than others)
        weights = {
            'mobile_usability': 0.25,
            'page_speed': 0.30,
            'https_coverage': 0.15,
            'crawl_errors': 0.20,
            'indexing_issues': 0.10
        }
        
        weighted_score = sum(
            category_scores.get(cat, 0) * weight
            for cat, weight in weights.items()
        )
        
        return round(weighted_score, 1)
    
    def _generate_priority_fixes(self, results: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Generate prioritized list of fixes across all categories.
        """
        all_issues = []
        
        # Collect all issues with their category
        for category in ['mobile_usability', 'page_speed', 'https_coverage',
                        'crawl_errors', 'indexing_issues']:
            category_data = results.get(category, {})
            issues = category_data.get('issues', [])
            
            for issue in issues:
                issue_copy = issue.copy()
                issue_copy['category'] = category
                all_issues.append(issue_copy)
        
        # Calculate priority score for each issue
        for issue in all_issues:
            severity = issue.get('severity', 'low')
            affected_pages = issue.get('total_affected_pages', 1)
            has_traffic = issue.get('urls_with_traffic', 0) or issue.get('pages_with_historical_traffic', 0)
            
            # Base score from severity
            base_score = {
                'critical': 100,
                'high': 70,
                'medium': 40,
                'low': 20
            }.get(severity, 20)
            
            # Impact multiplier from affected pages (logarithmic scale)
            impact_multiplier = 1 + np.log10(max(affected_pages, 1)) / 2
            
            # Traffic multiplier
            traffic_multiplier = 1.5 if has_traffic else 1.0
            
            # Effort estimate
            effort_map = {
                'http_pages': 'high',
                'ssl_certificate_issues': 'high',
                'poor_lcp': 'medium',
                'poor_cls': 'medium',
                'poor_interaction_responsiveness': 'high',
                'server_error': 'high',
                'not_found': 'low',
                'redirect_error': 'low',
                'crawled_not_indexed': 'medium',
                'duplicate_content': 'medium',
                'high_mobile_bounce_rate': 'medium',
                'mixed_content': 'low'
            }
            
            effort = effort_map.get(issue.get('issue', ''), 'medium')
            
            # Effort multiplier (inverse - lower effort = higher priority)
            effort_multiplier = {
                'low': 1.3,
                'medium': 1.0,
                'high': 0.8
            }.get(effort, 1.0)
            
            priority_score = base_score * impact_multiplier * traffic_multiplier * effort_multiplier
            
            issue['priority_score'] = round(priority_score, 1)
            issue['effort'] = effort
            
            # Estimate traffic impact if possible
            if has_traffic:
                # Rough estimation based on affected pages and severity
                estimated_impact = affected_pages * {
                    'critical': 50,
                    'high': 20,
                    'medium': 5,
                    'low': 1
                }.get(severity, 1)
                
                issue['estimated_monthly_traffic_impact'] = min(estimated_impact, 10000)
        
        # Sort by priority score
        all_issues.sort(key=lambda x: x['priority_score'], reverse=True)
        
        # Add rank
        for i, issue in enumerate(all_issues, 1):
            issue['priority_rank'] = i
        
        # Return top 20 issues
        return all_issues[:20]
    
    def _generate_summary(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate summary statistics across all categories.
        """
        total_issues = 0
        critical_count = 0
        high_count = 0
        medium_count = 0
        low_count = 0
        
        for category in ['mobile_usability', 'page_speed', 'https_coverage',
                        'crawl_errors', 'indexing_issues']:
            category_data = results.get(category, {})
            total_issues += category_data.get('total_issues', 0)
            critical_count += category_data.get('critical_issues', 0)
            high_count += category_data.get('high_issues', 0)
            medium_count += category_data.get('medium_issues', 0)
            low_count += category_data.get('low_issues', 0)
        
        # Calculate category breakdown
        category_breakdown = {}
        for category in ['mobile_usability', 'page_speed', 'https_coverage',
                        'crawl_errors', 'indexing_issues']:
            category_data = results.get(category, {})
            category_breakdown[category] = {
                'total_issues': category_data.get('total_issues', 0),
                'critical': category_data.get('critical_issues', 0),
                'high': category_data.get('high_issues', 0),
                'medium': category_data.get('medium_issues', 0),
                'low': category_data.get('low_issues', 0)
            }
        
        health_score = results.get('technical_health_score', 0)
        
        # Determine health status
        if health_score >= 90:
            health_status = 'excellent'
            health_message = 'Your technical SEO is in excellent shape'
        elif health_score >= 75:
            health_status = 'good'
            health_message = 'Good technical SEO with minor issues to address'
        elif health_score >= 60:
            health_status = 'needs_improvement'
            health_message = 'Several technical issues need attention'
        elif health_score >= 40:
            health_status = 'poor'
            health_message = 'Significant technical issues impacting performance'
        else:
            health_status = 'critical'
            health_message = 'Critical technical issues require immediate attention'
        
        return {
            'total_issues': total_issues,
            'by_severity': {
                'critical': critical_count,
                'high': high_count,
                'medium': medium_count,
                'low': low_count
            },
            'by_category': category_breakdown,
            'health_score': health_score,
            'health_status': health_status,
            'health_message': health_message,
            'top_priority_count': min(len(results.get('priority_fixes', [])), 5)
        }


def analyze_technical_seo(
    gsc_data: Dict[str, Any],
    ga4_data: Dict[str, Any],
    crawl_data: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Main entry point for technical SEO analysis.
    
    Args:
        gsc_data: Google Search Console data
        ga4_data: Google Analytics 4 data
        crawl_data: Optional site crawl data
        
    Returns:
        Complete technical SEO analysis results
    """
    analyzer = TechnicalSEOAnalyzer()
    return analyzer.analyze(gsc_data, ga4_data, crawl_data)
