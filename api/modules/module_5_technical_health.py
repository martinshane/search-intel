"""
Module 5: Technical Health Analysis

Calculates technical health score based on:
- Core Web Vitals from GA4
- Mobile usability from Google Search Console
- Indexing status
- Crawl errors and site structure

Returns structured data with overall score (0-100), component scores,
issues list, and actionable recommendations.
"""

from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import logging
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class SeverityLevel(Enum):
    """Severity levels for technical issues"""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"


class IssueCategory(Enum):
    """Categories of technical issues"""
    CORE_WEB_VITALS = "core_web_vitals"
    MOBILE_USABILITY = "mobile_usability"
    INDEXING = "indexing"
    CRAWL_ERRORS = "crawl_errors"
    SITE_STRUCTURE = "site_structure"
    HTTPS = "https"
    SCHEMA = "schema"


@dataclass
class TechnicalIssue:
    """Represents a single technical issue"""
    category: IssueCategory
    severity: SeverityLevel
    title: str
    description: str
    affected_count: int
    affected_urls: List[str]
    impact_score: float  # 0-100
    recommendation: str
    documentation_url: Optional[str] = None


@dataclass
class CoreWebVitalsMetric:
    """Core Web Vitals metric data"""
    metric_name: str  # LCP, FID, CLS
    good_percentage: float
    needs_improvement_percentage: float
    poor_percentage: float
    p75_value: float
    threshold_good: float
    threshold_poor: float
    status: str  # "passing", "needs_improvement", "failing"


@dataclass
class ComponentScore:
    """Score for a technical health component"""
    component: str
    score: float  # 0-100
    weight: float  # contribution to overall score
    status: str  # "excellent", "good", "fair", "poor", "critical"
    issues_count: int
    passing: bool


def analyze_technical_health(
    ga4_data: Dict[str, Any],
    gsc_data: Dict[str, Any],
    crawl_data: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Main function to analyze technical health.
    
    Args:
        ga4_data: GA4 data including Core Web Vitals
        gsc_data: Google Search Console data including mobile usability
        crawl_data: Optional crawl data for additional insights
        
    Returns:
        Dictionary with overall score, component scores, issues, and recommendations
    """
    logger.info("Starting technical health analysis")
    
    # Initialize components
    components: List[ComponentScore] = []
    all_issues: List[TechnicalIssue] = []
    
    # Analyze Core Web Vitals
    cwv_score, cwv_issues = analyze_core_web_vitals(ga4_data)
    components.append(cwv_score)
    all_issues.extend(cwv_issues)
    
    # Analyze Mobile Usability
    mobile_score, mobile_issues = analyze_mobile_usability(gsc_data)
    components.append(mobile_score)
    all_issues.extend(mobile_issues)
    
    # Analyze Indexing Status
    indexing_score, indexing_issues = analyze_indexing_status(gsc_data)
    components.append(indexing_score)
    all_issues.extend(indexing_issues)
    
    # Analyze Crawl Health
    crawl_score, crawl_issues = analyze_crawl_health(gsc_data, crawl_data)
    components.append(crawl_score)
    all_issues.extend(crawl_issues)
    
    # Analyze HTTPS and Security
    https_score, https_issues = analyze_https_security(gsc_data, crawl_data)
    components.append(https_score)
    all_issues.extend(https_issues)
    
    # Analyze Structured Data
    schema_score, schema_issues = analyze_structured_data(gsc_data, crawl_data)
    components.append(schema_score)
    all_issues.extend(schema_issues)
    
    # Calculate overall score (weighted average)
    overall_score = calculate_overall_score(components)
    
    # Generate priority recommendations
    recommendations = generate_recommendations(all_issues, components)
    
    # Calculate health status
    health_status = determine_health_status(overall_score)
    
    # Sort issues by impact
    all_issues.sort(key=lambda x: x.impact_score, reverse=True)
    
    logger.info(f"Technical health analysis complete. Overall score: {overall_score:.1f}")
    
    return {
        "overall_score": round(overall_score, 1),
        "health_status": health_status,
        "components": [
            {
                "name": comp.component,
                "score": round(comp.score, 1),
                "weight": comp.weight,
                "status": comp.status,
                "issues_count": comp.issues_count,
                "passing": comp.passing
            }
            for comp in components
        ],
        "issues": [
            {
                "category": issue.category.value,
                "severity": issue.severity.value,
                "title": issue.title,
                "description": issue.description,
                "affected_count": issue.affected_count,
                "affected_urls": issue.affected_urls[:10],  # Limit to 10 examples
                "impact_score": round(issue.impact_score, 1),
                "recommendation": issue.recommendation,
                "documentation_url": issue.documentation_url
            }
            for issue in all_issues
        ],
        "recommendations": recommendations,
        "summary": generate_summary(overall_score, components, all_issues),
        "metrics": {
            "total_issues": len(all_issues),
            "critical_issues": sum(1 for i in all_issues if i.severity == SeverityLevel.CRITICAL),
            "high_issues": sum(1 for i in all_issues if i.severity == SeverityLevel.HIGH),
            "medium_issues": sum(1 for i in all_issues if i.severity == SeverityLevel.MEDIUM),
            "low_issues": sum(1 for i in all_issues if i.severity == SeverityLevel.LOW),
            "components_passing": sum(1 for c in components if c.passing),
            "components_failing": sum(1 for c in components if not c.passing)
        },
        "analyzed_at": datetime.utcnow().isoformat()
    }


def analyze_core_web_vitals(ga4_data: Dict[str, Any]) -> tuple[ComponentScore, List[TechnicalIssue]]:
    """
    Analyze Core Web Vitals from GA4 data.
    
    Core Web Vitals thresholds:
    - LCP (Largest Contentful Paint): Good < 2.5s, Poor > 4.0s
    - FID (First Input Delay): Good < 100ms, Poor > 300ms
    - CLS (Cumulative Layout Shift): Good < 0.1, Poor > 0.25
    
    Score is based on % of page views with "good" ratings.
    """
    issues: List[TechnicalIssue] = []
    
    # Extract CWV data from GA4
    cwv_data = ga4_data.get("core_web_vitals", {})
    
    if not cwv_data:
        # No CWV data available
        return ComponentScore(
            component="Core Web Vitals",
            score=0,
            weight=0.30,
            status="unknown",
            issues_count=0,
            passing=False
        ), [TechnicalIssue(
            category=IssueCategory.CORE_WEB_VITALS,
            severity=SeverityLevel.HIGH,
            title="Core Web Vitals Data Not Available",
            description="Core Web Vitals data is not being collected in GA4. Enable web vitals reporting to monitor page experience.",
            affected_count=0,
            affected_urls=[],
            impact_score=80,
            recommendation="Enable Core Web Vitals reporting in GA4 by ensuring you're using the latest gtag.js and have enabled the web vitals events.",
            documentation_url="https://support.google.com/analytics/answer/9964640"
        )]
    
    # Parse individual metrics
    lcp = parse_cwv_metric(cwv_data.get("lcp", {}), "LCP", 2.5, 4.0)
    fid = parse_cwv_metric(cwv_data.get("fid", {}), "FID", 0.1, 0.3)
    cls = parse_cwv_metric(cwv_data.get("cls", {}), "CLS", 0.1, 0.25)
    
    metrics = [lcp, fid, cls]
    
    # Calculate score: average of % good across all three metrics
    # Goal is >75% good for each metric to pass Core Web Vitals assessment
    avg_good_percentage = sum(m.good_percentage for m in metrics) / len(metrics)
    score = avg_good_percentage  # Already 0-100
    
    # Check each metric for issues
    for metric in metrics:
        if metric.status == "failing":
            issues.append(TechnicalIssue(
                category=IssueCategory.CORE_WEB_VITALS,
                severity=SeverityLevel.CRITICAL,
                title=f"{metric.metric_name} Failing",
                description=f"{metric.metric_name} is failing with only {metric.good_percentage:.1f}% of page views in the 'good' range (p75: {metric.p75_value}). This directly impacts search rankings as Core Web Vitals are a ranking factor.",
                affected_count=int(metric.poor_percentage),
                affected_urls=cwv_data.get("poor_urls", {}).get(metric.metric_name.lower(), [])[:10],
                impact_score=90,
                recommendation=get_cwv_recommendation(metric.metric_name),
                documentation_url=get_cwv_documentation(metric.metric_name)
            ))
        elif metric.status == "needs_improvement":
            issues.append(TechnicalIssue(
                category=IssueCategory.CORE_WEB_VITALS,
                severity=SeverityLevel.HIGH,
                title=f"{metric.metric_name} Needs Improvement",
                description=f"{metric.metric_name} has {metric.good_percentage:.1f}% of page views in the 'good' range (p75: {metric.p75_value}). Aim for at least 75% to pass Core Web Vitals assessment.",
                affected_count=int(metric.needs_improvement_percentage + metric.poor_percentage),
                affected_urls=cwv_data.get("needs_improvement_urls", {}).get(metric.metric_name.lower(), [])[:10],
                impact_score=70,
                recommendation=get_cwv_recommendation(metric.metric_name),
                documentation_url=get_cwv_documentation(metric.metric_name)
            ))
    
    # Determine status
    passing = avg_good_percentage >= 75
    if score >= 90:
        status = "excellent"
    elif score >= 75:
        status = "good"
    elif score >= 50:
        status = "fair"
    elif score >= 25:
        status = "poor"
    else:
        status = "critical"
    
    component = ComponentScore(
        component="Core Web Vitals",
        score=score,
        weight=0.30,
        status=status,
        issues_count=len(issues),
        passing=passing
    )
    
    return component, issues


def parse_cwv_metric(
    metric_data: Dict[str, Any],
    metric_name: str,
    threshold_good: float,
    threshold_poor: float
) -> CoreWebVitalsMetric:
    """Parse Core Web Vitals metric data"""
    if not metric_data:
        return CoreWebVitalsMetric(
            metric_name=metric_name,
            good_percentage=0,
            needs_improvement_percentage=0,
            poor_percentage=0,
            p75_value=0,
            threshold_good=threshold_good,
            threshold_poor=threshold_poor,
            status="unknown"
        )
    
    good_pct = metric_data.get("good_percentage", 0)
    needs_improvement_pct = metric_data.get("needs_improvement_percentage", 0)
    poor_pct = metric_data.get("poor_percentage", 0)
    p75 = metric_data.get("p75_value", 0)
    
    # Determine status based on % good
    if good_pct >= 75:
        status = "passing"
    elif good_pct >= 50:
        status = "needs_improvement"
    else:
        status = "failing"
    
    return CoreWebVitalsMetric(
        metric_name=metric_name,
        good_percentage=good_pct,
        needs_improvement_percentage=needs_improvement_pct,
        poor_percentage=poor_pct,
        p75_value=p75,
        threshold_good=threshold_good,
        threshold_poor=threshold_poor,
        status=status
    )


def get_cwv_recommendation(metric_name: str) -> str:
    """Get specific recommendation for CWV metric"""
    recommendations = {
        "LCP": "Optimize Largest Contentful Paint by: reducing server response times, optimizing and compressing images, preloading key resources, eliminating render-blocking resources, and using a CDN. Focus on the largest image or text block above the fold.",
        "FID": "Improve First Input Delay by: breaking up long JavaScript tasks, optimizing page for interaction readiness, using a web worker for heavy computations, reducing JavaScript execution time, and minimizing third-party script impact.",
        "CLS": "Fix Cumulative Layout Shift by: always including size attributes on images and videos, never inserting content above existing content except in response to user interaction, using transform animations instead of properties that trigger layout, and reserving space for ad slots."
    }
    return recommendations.get(metric_name, "Review Google's Core Web Vitals documentation for optimization guidance.")


def get_cwv_documentation(metric_name: str) -> str:
    """Get documentation URL for CWV metric"""
    docs = {
        "LCP": "https://web.dev/lcp/",
        "FID": "https://web.dev/fid/",
        "CLS": "https://web.dev/cls/"
    }
    return docs.get(metric_name, "https://web.dev/vitals/")


def analyze_mobile_usability(gsc_data: Dict[str, Any]) -> tuple[ComponentScore, List[TechnicalIssue]]:
    """
    Analyze mobile usability from Google Search Console data.
    
    Checks for:
    - Mobile-friendly pages
    - Viewport configuration
    - Text sizing issues
    - Clickable elements spacing
    - Content width issues
    """
    issues: List[TechnicalIssue] = []
    
    mobile_data = gsc_data.get("mobile_usability", {})
    
    if not mobile_data:
        return ComponentScore(
            component="Mobile Usability",
            score=50,
            weight=0.25,
            status="unknown",
            issues_count=0,
            passing=False
        ), []
    
    total_pages = mobile_data.get("total_pages", 0)
    mobile_friendly_pages = mobile_data.get("mobile_friendly_pages", 0)
    
    if total_pages == 0:
        mobile_friendly_rate = 100
    else:
        mobile_friendly_rate = (mobile_friendly_pages / total_pages) * 100
    
    score = mobile_friendly_rate
    
    # Check for specific mobile usability issues
    usability_issues = mobile_data.get("issues", [])
    
    for issue_data in usability_issues:
        issue_type = issue_data.get("issue_type", "")
        affected_pages = issue_data.get("affected_pages", [])
        affected_count = len(affected_pages)
        
        if affected_count == 0:
            continue
        
        severity = SeverityLevel.HIGH if affected_count > total_pages * 0.1 else SeverityLevel.MEDIUM
        
        issue_titles = {
            "MOBILE_FRIENDLY_RULE_VIOLATED": "Mobile-Friendly Test Failed",
            "TEXT_TOO_SMALL": "Text Too Small to Read",
            "CLICKABLE_ELEMENTS_TOO_CLOSE": "Clickable Elements Too Close Together",
            "CONTENT_WIDER_THAN_SCREEN": "Content Wider Than Screen",
            "VIEWPORT_NOT_CONFIGURED": "Viewport Not Configured",
            "USES_INCOMPATIBLE_PLUGINS": "Uses Incompatible Plugins"
        }
        
        issue_descriptions = {
            "MOBILE_FRIENDLY_RULE_VIOLATED": "Pages are not mobile-friendly according to Google's mobile-friendly test. This impacts mobile search rankings.",
            "TEXT_TOO_SMALL": "Text on these pages is too small to read comfortably on mobile devices without zooming. Use at least 16px font size for body text.",
            "CLICKABLE_ELEMENTS_TOO_CLOSE": "Links, buttons, and other clickable elements are too close together, making it difficult for users to tap the intended target on mobile.",
            "CONTENT_WIDER_THAN_SCREEN": "Content is wider than the screen, requiring horizontal scrolling. Use responsive design with width: 100% and max-width.",
            "VIEWPORT_NOT_CONFIGURED": "Pages are missing viewport meta tag configuration. Add: <meta name='viewport' content='width=device-width, initial-scale=1'>",
            "USES_INCOMPATIBLE_PLUGINS": "Pages use plugins like Flash that are not supported on mobile devices. Remove or replace with modern web technologies."
        }
        
        issue_recommendations = {
            "MOBILE_FRIENDLY_RULE_VIOLATED": "Test pages with Google's Mobile-Friendly Test tool and address all identified issues. Implement responsive design.",
            "TEXT_TOO_SMALL": "Increase base font size to at least 16px. Use relative units (rem/em) and ensure adequate line height (1.5+).",
            "CLICKABLE_ELEMENTS_TOO_CLOSE": "Add spacing between interactive elements. Use padding and margins to ensure at least 48x48px touch targets with adequate spacing.",
            "CONTENT_WIDER_THAN_SCREEN": "Use responsive CSS (width: 100%, max-width: 100vw) and avoid fixed-width elements. Test on various screen sizes.",
            "VIEWPORT_NOT_CONFIGURED": "Add viewport meta tag to all pages: <meta name='viewport' content='width=device-width, initial-scale=1'>",
            "USES_INCOMPATIBLE_PLUGINS": "Replace Flash and other plugins with HTML5, CSS3, and JavaScript alternatives."
        }
        
        issues.append(TechnicalIssue(
            category=IssueCategory.MOBILE_USABILITY,
            severity=severity,
            title=issue_titles.get(issue_type, f"Mobile Usability Issue: {issue_type}"),
            description=issue_descriptions.get(issue_type, f"Mobile usability issue detected: {issue_type}"),
            affected_count=affected_count,
            affected_urls=affected_pages[:10],
            impact_score=min(90, 50 + (affected_count / total_pages) * 50) if total_pages > 0 else 50,
            recommendation=issue_recommendations.get(issue_type, "Review Google Search Console for detailed information."),
            documentation_url="https://developers.google.com/search/mobile-sites/"
        ))
    
    # Determine status
    passing = score >= 95
    if score >= 98:
        status = "excellent"
    elif score >= 95:
        status = "good"
    elif score >= 85:
        status = "fair"
    elif score >= 70:
        status = "poor"
    else:
        status = "critical"
    
    component = ComponentScore(
        component="Mobile Usability",
        score=score,
        weight=0.25,
        status=status,
        issues_count=len(issues),
        passing=passing
    )
    
    return component, issues


def analyze_indexing_status(gsc_data: Dict[str, Any]) -> tuple[ComponentScore, List[TechnicalIssue]]:
    """
    Analyze indexing status from Google Search Console.
    
    Checks:
    - Index coverage issues
    - Pages indexed vs submitted
    - Excluded pages and reasons
    - Validation status
    """
    issues: List[TechnicalIssue] = []
    
    index_data = gsc_data.get("index_coverage", {})
    
    if not index_data:
        return ComponentScore(
            component="Indexing Status",
            score=50,
            weight=0.20,
            status="unknown",
            issues_count=0,
            passing=False
        ), []
    
    total_pages = index_data.get("total_pages", 0)
    indexed_pages = index_data.get("valid_pages", 0)
    excluded_pages = index_data.get("excluded_pages", 0)
    error_pages = index_data.get("error_pages", 0)
    
    if total_pages == 0:
        indexing_rate = 100
    else:
        indexing_rate = (indexed_pages / total_pages) * 100
    
    # Score based on indexing rate, but penalize errors heavily
    error_penalty = (error_pages / max(total_pages, 1)) * 50
    score = max(0, indexing_rate - error_penalty)
    
    # Check for specific indexing issues
    indexing_issues = index_data.get("issues", [])
    
    issue_severity_map = {
        "SERVER_ERROR": SeverityLevel.CRITICAL,
        "REDIRECT_ERROR": SeverityLevel.CRITICAL,
        "BLOCKED_BY_ROBOTS": SeverityLevel.CRITICAL,
        "NOT_FOUND": SeverityLevel.HIGH,
        "SOFT_404": SeverityLevel.HIGH,
        "DUPLICATE_CONTENT": SeverityLevel.HIGH,
        "CRAWLED_NOT_INDEXED": SeverityLevel.MEDIUM,
        "DISCOVERED_NOT_CRAWLED": SeverityLevel.MEDIUM,
        "ALTERNATE_PAGE_WITH_PROPER_CANONICAL": SeverityLevel.LOW,
        "EXCLUDED_BY_NOINDEX": SeverityLevel.INFO
    }
    
    for issue_data in indexing_issues:
        issue_type = issue_data.get("issue_type", "")
        affected_pages = issue_data.get("affected_pages", [])
        affected_count = len(affected_pages)
        
        if affected_count == 0:
            continue
        
        severity = issue_severity_map.get(issue_type, SeverityLevel.MEDIUM)
        
        issues.append(TechnicalIssue(
            category=IssueCategory.INDEXING,
            severity=severity,
            title=format_issue_title(issue_type),
            description=get_indexing_issue_description(issue_type, affected_count),
            affected_count=affected_count,
            affected_urls=affected_pages[:10],
            impact_score=calculate_indexing_impact(issue_type, affected_count, total_pages),
            recommendation=get_indexing_recommendation(issue_type),
            documentation_url="https://support.google.com/webmasters/answer/7440203"
        ))
    
    # Check for declining index coverage
    historical_data = index_data.get("historical", [])
    if len(historical_data) >= 30:
        recent_avg = sum(d.get("valid_pages", 0) for d in historical_data[-7:]) / 7
        older_avg = sum(d.get("valid_pages", 0) for d in historical_data[-30:-7]) / 23
        
        if older_avg > 0:
            change_pct = ((recent_avg - older_avg) / older_avg) * 100
            
            if change_pct < -10:
                issues.append(TechnicalIssue(
                    category=IssueCategory.INDEXING,
                    severity=SeverityLevel.HIGH,
                    title="Declining Index Coverage",
                    description=f"The number of indexed pages has declined by {abs(change_pct):.1f}% in the last 7 days compared to the prior 23 days. This suggests an emerging indexing problem.",
                    affected_count=int(older_avg - recent_avg),
                    affected_urls=[],
                    impact_score=75,
                    recommendation="Review Google Search Console for recent indexing errors, check server logs for crawl errors, verify robots.txt and sitemap are functioning correctly.",
                    documentation_url="https://support.google.com/webmasters/answer/9012289"
                ))
    
    # Determine status
    passing = score >= 90 and error_pages < total_pages * 0.01
    if score >= 95:
        status = "excellent"
    elif score >= 90:
        status = "good"
    elif score >= 75:
        status = "fair"
    elif score >= 50:
        status = "poor"
    else:
        status = "critical"
    
    component = ComponentScore(
        component="Indexing Status",
        score=score,
        weight=0.20,
        status=status,
        issues_count=len(issues),
        passing=passing
    )
    
    return component, issues


def analyze_crawl_health(
    gsc_data: Dict[str, Any],
    crawl_data: Optional[Dict[str, Any]]
) -> tuple[ComponentScore, List[TechnicalIssue]]:
    """
    Analyze crawl health from GSC and optional crawl data.
    
    Checks:
    - Crawl errors (4xx, 5xx)
    - Redirect chains
    - Broken internal links
    - Orphan pages
    - Crawl budget efficiency
    """
    issues: List[TechnicalIssue] = []
    
    crawl_stats = gsc_data.get("crawl_stats", {})
    
    # Initialize score
    score = 100
    
    # Check crawl errors from GSC
    total_requests = crawl_stats.get("total_requests", 0)
    error_4xx = crawl_stats.get("response_code_4xx", 0)
    error_5xx = crawl_stats.get("response_code_5xx", 0)
    
    if total_requests > 0:
        error_rate = ((error_4xx + error_5xx) / total_requests) * 100
        score -= min(50, error_rate * 2)
        
        if error_5xx > 0:
            issues.append(TechnicalIssue(
                category=IssueCategory.CRAWL_ERRORS,
                severity=SeverityLevel.CRITICAL,
                title="Server Errors (5xx) Detected",
                description=f"Googlebot encountered {error_5xx} server errors out of {total_requests} crawl requests ({(error_5xx/total_requests)*100:.1f}%). These prevent indexing and negatively impact rankings.",
                affected_count=error_5xx,
                affected_urls=crawl_stats.get("5xx_examples", [])[:10],
                impact_score=95,
                recommendation="Investigate server logs to identify failing URLs. Fix server configuration, increase resources, optimize database queries, or implement better error handling.",
                documentation_url="https://support.google.com/webmasters/answer/9008080"
            ))
        
        if error_4xx > total_requests * 0.05:  # More than 5% 404s
            issues.append(TechnicalIssue(
                category=IssueCategory.CRAWL_ERRORS,
                severity=SeverityLevel.HIGH,
                title="High Rate of 404 Errors",
                description=f"Googlebot encountered {error_4xx} 404 errors out of {total_requests} crawl requests ({(error_4xx/total_requests)*100:.1f}%). This wastes crawl budget and may indicate broken internal links.",
                affected_count=error_4xx,
                affected_urls=crawl_stats.get("4xx_examples", [])[:10],
                impact_score=70,
                recommendation="Audit internal links and fix broken references. Implement 301 redirects for moved content. Update sitemaps to remove deleted pages. Monitor external links pointing to your site.",
                documentation_url="https://support.google.com/webmasters/answer/9008080"
            ))
    
    # Check redirect chains if crawl data available
    if crawl_data:
        redirect_chains = crawl_data.get("redirect_chains", [])
        if redirect_chains:
            issues.append(TechnicalIssue(
                category=IssueCategory.CRAWL_ERRORS,
                severity=SeverityLevel.MEDIUM,
                title="Redirect Chains Detected",
                description=f"Found {len(redirect_chains)} redirect chains (A→B→C). Each redirect adds latency and wastes crawl budget. Maximum efficient path is a single redirect.",
                affected_count=len(redirect_chains),
                affected_urls=[chain[0] for chain in redirect_chains[:10]],
                impact_score=50,
                recommendation="Update redirects to point directly to final destination. Example: If A→B→C, change A→C and remove B redirect.",
                documentation_url="https://developers.google.com/search/docs/crawling-indexing/301-redirects"
            ))
            score -= min(20, len(redirect_chains) / 10)
        
        # Check for orphan pages
        orphan_pages = crawl_data.get("orphan_pages", [])
        if orphan_pages:
            issues.append(TechnicalIssue(
                category=IssueCategory.SITE_STRUCTURE,
                severity=SeverityLevel.MEDIUM,
                title="Orphan Pages Found",
                description=f"Found {len(orphan_pages)} pages with no internal links pointing to them. These pages are hard for users and search engines to discover.",
                affected_count=len(orphan_pages),
                affected_urls=orphan_pages[:10],
                impact_score=60,
                recommendation="Add internal links to orphan pages from relevant existing content. If pages are intentionally isolated, use noindex or remove from sitemap.",
                documentation_url="https://developers.google.com/search/docs/crawling-indexing/links-crawlable"
            ))
            score -= min(15, len(orphan_pages) / 20)
        
        # Check broken internal links
        broken_links = crawl_data.get("broken_internal_links", [])
        if broken_links:
            issues.append(TechnicalIssue(
                category=IssueCategory.CRAWL_ERRORS,
                severity=SeverityLevel.HIGH,
                title="Broken Internal Links",
                description=f"Found {len(broken_links)} broken internal links on your site. These create poor user experience and waste crawl budget.",
                affected_count=len(broken_links),
                affected_urls=[link.get("source_url") for link in broken_links[:10]],
                impact_score=65,
                recommendation="Fix or remove broken internal links. For moved pages, update links to new URLs. For deleted pages, remove links or implement 301 redirects.",
                documentation_url="https://developers.google.com/search/docs/crawling-indexing/links-crawlable"
            ))
            score -= min(20, len(broken_links) / 10)
    
    # Check crawl rate and efficiency
    daily_crawl_requests = crawl_stats.get("daily_crawl_requests", [])
    if daily_crawl_requests:
        avg_daily_crawls = sum(daily_crawl_requests) / len(daily_crawl_requests)
        total_pages = crawl_data.get("total_pages", 0) if crawl_data else 0
        
        if total_pages > 0 and avg_daily_crawls > 0:
            days_to_full_crawl = total_pages / avg_daily_crawls
            
            if days_to_full_crawl > 30:
                issues.append(TechnicalIssue(
                    category=IssueCategory.CRAWL_ERRORS,
                    severity=SeverityLevel.MEDIUM,
                    title="Low Crawl Frequency",
                    description=f"At current crawl rate ({avg_daily_crawls:.0f} pages/day), it would take {days_to_full_crawl:.0f} days to crawl your entire site. This may delay indexing of new content.",
                    affected_count=0,
                    affected_urls=[],
                    impact_score=55,
                    recommendation="Improve crawl efficiency: ensure fast server response times, fix crawl errors, optimize robots.txt, submit XML sitemaps, improve internal linking structure, and build site authority through quality backlinks.",
                    documentation_url="https://developers.google.com/search/docs/crawling-indexing/large-site-managing-crawl-budget"
                ))
    
    score = max(0, score)
    
    # Determine status
    passing = score >= 85 and len([i for i in issues if i.severity in [SeverityLevel.CRITICAL, SeverityLevel.HIGH]]) == 0
    if score >= 95:
        status = "excellent"
    elif score >= 85:
        status = "good"
    elif score >= 70:
        status = "fair"
    elif score >= 50:
        status = "poor"
    else:
        status = "critical"
    
    component = ComponentScore(
        component="Crawl Health",
        score=score,
        weight=0.15,
        status=status,
        issues_count=len(issues),
        passing=passing
    )
    
    return component, issues


def analyze_https_security(
    gsc_data: Dict[str, Any],
    crawl_data: Optional[Dict[str, Any]]
) -> tuple[ComponentScore, List[TechnicalIssue]]:
    """
    Analyze HTTPS and security configuration.
    
    Checks:
    - HTTPS usage
    - Mixed content issues
    - Certificate validity
    - Security headers
    """
    issues: List[TechnicalIssue] = []
    
    # Start with perfect score
    score = 100
    
    # Check HTTPS coverage from crawl data
    if crawl_data:
        total_pages = crawl_data.get("total_pages", 0)
        https_pages = crawl_data.get("https_pages", 0)
        http_pages = crawl_data.get("http_pages", 0)
        
        if total_pages > 0:
            https_rate = (https_pages / total_pages) * 100
            
            if https_rate < 100:
                severity = SeverityLevel.CRITICAL if https_rate < 95 else SeverityLevel.HIGH
                issues.append(TechnicalIssue(
                    category=IssueCategory.HTTPS,
                    severity=severity,
                    title="Incomplete HTTPS Migration",
                    description=f"Only {https_rate:.1f}% of your pages use HTTPS. HTTPS is a ranking factor and builds user trust. All pages should use HTTPS.",
                    affected_count=http_pages,
                    affected_urls=crawl_data.get("http_urls", [])[:10],
                    impact_score=85,
                    recommendation="Migrate all pages to HTTPS. Install SSL certificate, update internal links, implement 301 redirects from HTTP to HTTPS, update canonical tags, and submit HTTPS sitemap to GSC.",
                    documentation_url="https://developers.google.com/search/docs/crawling-indexing/https"
                ))
                score -= (100 - https_rate) / 2
        
        # Check for mixed content
        mixed_content_pages = crawl_data.get("mixed_content_pages", [])
        if mixed_content_pages:
            issues.append(TechnicalIssue(
                category=IssueCategory.HTTPS,
                severity=SeverityLevel.HIGH,
                title="Mixed Content Issues",
                description=f"Found {len(mixed_content_pages)} HTTPS pages loading insecure HTTP resources. This triggers browser warnings and security risks.",
                affected_count=len(mixed_content_pages),
                affected_urls=mixed_content_pages[:10],
                impact_score=75,
                recommendation="Update all resource URLs (images, scripts, stylesheets) to use HTTPS or protocol-relative URLs (//). Use browser console to identify mixed content warnings.",
                documentation_url="https://developers.google.com/web/fundamentals/security/prevent-mixed-content/fixing-mixed-content"
            ))
            score -= min(25, len(mixed_content_pages) / 10)
        
        # Check security headers
        missing_security_headers = crawl_data.get("missing_security_headers", {})
        if missing_security_headers:
            critical_headers = ["Strict-Transport-Security", "X-Content-Type-Options", "X-Frame-Options"]
            missing_critical = [h for h in critical_headers if missing_security_headers.get(h, 0) > 0]
            
            if missing_critical:
                issues.append(TechnicalIssue(
                    category=IssueCategory.HTTPS,
                    severity=SeverityLevel.MEDIUM,
                    title="Missing Security Headers",
                    description=f"Critical security headers are missing: {', '.join(missing_critical)}. These headers protect against common web vulnerabilities.",
                    affected_count=sum(missing_security_headers.get(h, 0) for h in missing_critical),
                    affected_urls=[],
                    impact_score=50,
                    recommendation="Configure web server to send security headers: Strict-Transport-Security (HSTS), X-Content-Type-Options: nosniff, X-Frame-Options: DENY or SAMEORIGIN, Content-Security-Policy.",
                    documentation_url="https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers#security"
                ))
                score -= len(missing_critical) * 5
    
    score = max(0, score)
    
    # Determine status
    passing = score >= 90
    if score >= 98:
        status = "excellent"
    elif score >= 90:
        status = "good"
    elif score >= 75:
        status = "fair"
    elif score >= 50:
        status = "poor"
    else:
        status = "critical"
    
    component = ComponentScore(
        component="HTTPS & Security",
        score=score,
        weight=0.05,
        status=status,
        issues_count=len(issues),
        passing=passing
    )
    
    return component, issues


def analyze_structured_data(
    gsc_data: Dict[str, Any],
    crawl_data: Optional[Dict[str, Any]]
) -> tuple[ComponentScore, List[TechnicalIssue]]:
    """
    Analyze structured data (Schema.org) implementation.
    
    Checks:
    - Schema markup coverage
    - Schema validation errors
    - Rich results eligibility
    """
    issues: List[TechnicalIssue] = []
    
    schema_data = gsc_data.get("structured_data", {})
    
    if not schema_data and not crawl_data:
        # No schema data available
        return ComponentScore(
            component="Structured Data",
            score=50,
            weight=0.05,
            status="unknown",
            issues_count=0,
            passing=False
        ), []
    
    # Start with base score
    score = 70  # Default for some schema present
    
    # Check schema coverage
    if crawl_data:
        total_pages = crawl_data.get("total_pages", 0)
        pages_with_schema = crawl_data.get("pages_with_schema", 0)
        
        if total_pages > 0:
            schema_coverage = (pages_with_schema / total_pages) * 100
            score = 50 + (schema_coverage / 100) * 30  # 50-80 based on coverage
            
            if schema_coverage < 50:
                issues.append(TechnicalIssue(
                    category=IssueCategory.SCHEMA,
                    severity=SeverityLevel.MEDIUM,
                    title="Low Structured Data Coverage",
                    description=f"Only {schema_coverage:.1f}% of pages have structured data markup. Schema helps search engines understand your content and can enable rich results.",
                    affected_count=total_pages - pages_with_schema,
                    affected_urls=[],
                    impact_score=55,
                    recommendation="Implement relevant Schema.org markup for your content types: Article, Product, Organization, LocalBusiness, FAQ, HowTo, Recipe, etc. Use JSON-LD format for easiest implementation.",
                    documentation_url="https://developers.google.com/search/docs/appearance/structured-data/intro-structured-data"
                ))
    
    # Check for schema errors
    schema_errors = schema_data.get("errors", [])
    if schema_errors:
        total_errors = sum(e.get("count", 0) for e in schema_errors)
        issues.append(TechnicalIssue(
            category=IssueCategory.SCHEMA,
            severity=SeverityLevel.MEDIUM,
            title="Structured Data Errors",
            description=f"Found {total_errors} structured data errors across {len(schema_errors)} error types. Errors prevent rich results and may indicate implementation issues.",
            affected_count=total_errors,
            affected_urls=[e.get("example_url") for e in schema_errors[:10] if e.get("example_url")],
            impact_score=60,
            recommendation="Use Google's Rich Results Test to identify and fix schema errors. Common issues: missing required properties, incorrect data types, invalid values.",
            documentation_url="https://search.google.com/test/rich-results"
        ))
        score -= min(20, len(schema_errors) * 3)
    
    # Check for schema warnings
    schema_warnings = schema_data.get("warnings", [])
    if schema_warnings:
        total_warnings = sum(w.get("count", 0) for w in schema_warnings)
        if total_warnings > 100:
            issues.append(TechnicalIssue(
                category=IssueCategory.SCHEMA,
                severity=SeverityLevel.LOW,
                title="Structured Data Warnings",
                description=f"Found {total_warnings} structured data warnings. While not critical, addressing these improves schema quality and rich results eligibility.",
                affected_count=total_warnings,
                affected_urls=[w.get("example_url") for w in schema_warnings[:10] if w.get("example_url")],
                impact_score=30,
                recommendation="Review warnings in Google Search Console. Add recommended properties to improve rich results eligibility.",
                documentation_url="https://search.google.com/search-console"
            ))
            score -= min(10, len(schema_warnings))
    
    # Check rich results eligibility
    rich_results = schema_data.get("rich_results", {})
    eligible_types = rich_results.get("eligible_types", [])
    
    if eligible_types:
        score += 20  # Bonus for rich results eligibility
    
    score = max(0, min(100, score))
    
    # Determine status
    passing = score >= 70
    if score >= 90:
        status = "excellent"
    elif score >= 70:
        status = "good"
    elif score >= 50:
        status = "fair"
    elif score >= 30:
        status = "poor"
    else:
        status = "critical"
    
    component = ComponentScore(
        component="Structured Data",
        score=score,
        weight=0.05,
        status=status,
        issues_count=len(issues),
        passing=passing
    )
    
    return component, issues


def calculate_overall_score(components: List[ComponentScore]) -> float:
    """Calculate weighted overall score from components"""
    total_weight = sum(c.weight for c in components)
    if total_weight == 0:
        return 0
    
    weighted_sum = sum(c.score * c.weight for c in components)
    return weighted_sum / total_weight


def determine_health_status(score: float) -> str:
    """Determine overall health status from score"""
    if score >= 90:
        return "excellent"
    elif score >= 75:
        return "good"
    elif score >= 60:
        return "fair"
    elif score >= 40:
        return "poor"
    else:
        return "critical"


def format_issue_title(issue_type: str) -> str:
    """Format issue type into readable title"""
    return issue_type.replace("_", " ").title()


def get_indexing_issue_description(issue_type: str, affected_count: int) -> str:
    """Get description for indexing issue"""
    descriptions = {
        "SERVER_ERROR": f"Google encountered server errors (5xx) when trying to crawl {affected_count} URLs. These pages cannot be indexed.",
        "REDIRECT_ERROR": f"Google encountered redirect errors on {affected_count} URLs. Redirect chains or loops prevent indexing.",
        "BLOCKED_BY_ROBOTS": f"{affected_count} URLs are blocked by robots.txt. If these should be indexed, update robots.txt.",
        "NOT_FOUND": f"{affected_count} URLs returned 404 errors. These pages don't exist but are being discovered by Google.",
        "SOFT_404": f"{affected_count} URLs appear to be 404 pages but return 200 status code. This confuses search engines.",
        "DUPLICATE_CONTENT": f"{affected_count} URLs are excluded as duplicates. Implement canonical tags or consolidate content.",
        "CRAWLED_NOT_INDEXED": f"{affected_count} URLs were crawled but not indexed. Often indicates low-quality or thin content.",
        "DISCOVERED_NOT_CRAWLED": f"Google discovered {affected_count} URLs but hasn't crawled them yet. May indicate crawl budget issues.",
        "ALTERNATE_PAGE_WITH_PROPER_CANONICAL": f"{affected_count} URLs are properly canonicalized to other pages. This is usually correct.",
        "EXCLUDED_BY_NOINDEX": f"{affected_count} URLs have noindex directive. If intentional, this is fine."
    }
    return descriptions.get(issue_type, f"Indexing issue: {issue_type} affecting {affected_count} URLs")


def calculate_indexing_impact(issue_type: str, affected_count: int, total_pages: int) -> float:
    """Calculate impact score for indexing issue"""
    severity_multiplier = {
        "SERVER_ERROR": 1.0,
        "REDIRECT_ERROR": 0.9,
        "BLOCKED_BY_ROBOTS": 0.85,
        "NOT_FOUND": 0.7,
        "SOFT_404": 0.75,
        "DUPLICATE_CONTENT": 0.6,
        "CRAWLED_NOT_INDEXED": 0.5,
        "DISCOVERED_NOT_CRAWLED": 0.4,
        "ALTERNATE_PAGE_WITH_PROPER_CANONICAL": 0.1,
        "EXCLUDED_BY_NOINDEX": 0.05
    }.get(issue_type, 0.5)
    
    if total_pages == 0:
        percentage_affected = 0
    else:
        percentage_affected = (affected_count / total_pages) * 100
    
    return min(100, percentage_affected * severity_multiplier)


def get_indexing_recommendation(issue_type: str) -> str:
    """Get recommendation for indexing issue"""
    recommendations = {
        "SERVER_ERROR": "Fix server errors by reviewing server logs, increasing server resources, optimizing code, or contacting hosting provider.",
        "REDIRECT_ERROR": "Fix redirect chains and loops. Ensure redirects point directly to final destination. Remove broken redirects.",
        "BLOCKED_BY_ROBOTS": "Review robots.txt and remove blocks for pages that should be indexed. Use 'Allow' directive if needed.",
        "NOT_FOUND": "Fix or remove broken links pointing to these URLs. Implement 301 redirects if pages moved. Remove from sitemap.",
        "SOFT_404": "Return proper 404 status code for non-existent pages. Update server configuration to send correct status codes.",
        "DUPLICATE_CONTENT": "Implement canonical tags pointing to preferred version. Consider consolidating similar pages or using parameter handling in GSC.",
        "CRAWLED_NOT_INDEXED": "Improve content quality and uniqueness. Add internal links. Ensure page provides value. Check for thin content.",
        "DISCOVERED_NOT_CRAWLED": "Add pages to sitemap. Improve internal linking. Increase site authority. Be patient as Google prioritizes crawling.",
        "ALTERNATE_PAGE_WITH_PROPER_CANONICAL": "No action needed. These pages are correctly canonicalized.",
        "EXCLUDED_BY_NOINDEX": "If pages should be indexed, remove noindex directive. Otherwise, this is working as intended."
    }
    return recommendations.get(issue_type, "Review Google Search Console for specific guidance.")


def generate_recommendations(issues: List[TechnicalIssue], components: List[ComponentScore]) -> List[Dict[str, Any]]:
    """Generate prioritized recommendations"""
    recommendations = []
    
    # Group issues by category and severity
    critical_issues = [i for i in issues if i.severity == SeverityLevel.CRITICAL]
    high_issues = [i for i in issues if i.severity == SeverityLevel.HIGH]
    
    # Critical issues first
    if critical_issues:
        rec = {
            "priority": "critical",
            "title": "Address Critical Technical Issues Immediately",
            "description": f"Found {len(critical_issues)} critical issues that are actively harming your search performance.",
            "actions": [
                {
                    "issue": issue.title,
                    "action": issue.recommendation,
                    "impact": "high",
                    "effort": estimate_effort(issue),
                    "affected_count": issue.affected_count
                }
                for issue in critical_issues[:5]  # Top 5 critical
            ],
            "estimated_impact": "Resolving these issues could recover significant search visibility within 2-4 weeks."
        }
        recommendations.append(rec)
    
    # High priority issues
    if high_issues:
        rec = {
            "priority": "high",
            "title": "Fix High-Priority Technical Issues",
            "description": f"Found {len(high_issues)} high-priority issues that should be addressed soon.",
            "actions": [
                {
                    "issue": issue.title,
                    "action": issue.recommendation,
                    "impact": "medium-high",
                    "effort": estimate_effort(issue),
                    "affected_count": issue.affected_count
                }
                for issue in high_issues[:5]  # Top 5 high
            ],
            "estimated_impact": "Addressing these issues will improve crawlability and user experience."
        }
        recommendations.append(rec)
    
    # Component-specific recommendations
    failing_components = [c for c in components if not c.passing]
    if failing_components:
        for comp in failing_components[:3]:  # Top 3 failing components
            rec = {
                "priority": "medium",
                "title": f"Improve {comp.component}",
                "description": f"Your {comp.component} score is {comp.score:.1f}/100, below the passing threshold.",
                "actions": get_component_actions(comp, issues),
                "estimated_impact": f"Improving this could add {comp.weight * 100:.0f} points to your overall technical health score."
            }
            recommendations.append(rec)
    
    return recommendations


def estimate_effort(issue: TechnicalIssue) -> str:
    """Estimate effort required to fix issue"""
    if issue.affected_count > 100:
        return "high"
    elif issue.affected_count > 20:
        return "medium"
    else:
        return "low"


def get_component_actions(component: ComponentScore, issues: List[TechnicalIssue]) -> List[Dict[str, Any]]:
    """Get actionable items for a component"""
    component_map = {
        "Core Web Vitals": IssueCategory.CORE_WEB_VITALS,
        "Mobile Usability": IssueCategory.MOBILE_USABILITY,
        "Indexing Status": IssueCategory.INDEXING,
        "Crawl Health": IssueCategory.CRAWL_ERRORS,
        "HTTPS & Security": IssueCategory.HTTPS,
        "Structured Data": IssueCategory.SCHEMA
    }
    
    category = component_map.get(component.component)
    if not category:
        return []
    
    component_issues = [i for i in issues if i.category == category][:3]
    
    return [
        {
            "issue": issue.title,
            "action": issue.recommendation,
            "impact": "high" if issue.severity in [SeverityLevel.CRITICAL, SeverityLevel.HIGH] else "medium",
            "effort": estimate_effort(issue)
        }
        for issue in component_issues
    ]


def generate_summary(score: float, components: List[ComponentScore], issues: List[TechnicalIssue]) -> Dict[str, Any]:
    """Generate executive summary"""
    critical_count = sum(1 for i in issues if i.severity == SeverityLevel.CRITICAL)
    high_count = sum(1 for i in issues if i.severity == SeverityLevel.HIGH)
    
    if score >= 90:
        overall_assessment = "Your site has excellent technical health with minimal issues. Focus on maintaining current standards and monitoring for any new issues."
    elif score >= 75:
        overall_assessment = "Your site has good technical health overall, but there are some areas for improvement that could enhance performance and user experience."
    elif score >= 60:
        overall_assessment = "Your site has fair technical health with several issues that should be addressed to improve search performance and user experience."
    elif score >= 40:
        overall_assessment = "Your site has poor technical health with significant issues that are likely impacting search visibility and user experience. Priority fixes are needed."
    else:
        overall_assessment = "Your site has critical technical health issues that are severely impacting search performance. Immediate action is required to prevent further visibility loss."
    
    # Identify weakest component
    weakest = min(components, key=lambda c: c.score)
    
    # Identify strongest component
    strongest = max(components, key=lambda c: c.score)
    
    return {
        "overall_assessment": overall_assessment,
        "score": round(score, 1),
        "grade": get_letter_grade(score),
        "critical_issues": critical_count,
        "high_priority_issues": high_count,
        "weakest_area": {
            "component": weakest.component,
            "score": round(weakest.score, 1),
            "status": weakest.status
        },
        "strongest_area": {
            "component": strongest.component,
            "score": round(strongest.score, 1),
            "status": strongest.status
        },
        "next_steps": generate_next_steps(score, critical_count, high_count)
    }


def get_letter_grade(score: float) -> str:
    """Convert numeric score to letter grade"""
    if score >= 95:
        return "A+"
    elif score >= 90:
        return "A"
    elif score >= 85:
        return "A-"
    elif score >= 80:
        return "B+"
    elif score >= 75:
        return "B"
    elif score >= 70:
        return "B-"
    elif score >= 65:
        return "C+"
    elif score >= 60:
        return "C"
    elif score >= 55:
        return "C-"
    elif score >= 50:
        return "D+"
    elif score >= 40:
        return "D"
    else:
        return "F"


def generate_next_steps(score: float, critical_count: int, high_count: int) -> List[str]:
    """Generate next steps based on score and issues"""
    steps = []
    
    if critical_count > 0:
        steps.append(f"Immediately address {critical_count} critical issue{'s' if critical_count != 1 else ''} that are blocking search performance")
    
    if high_count > 0:
        steps.append(f"Fix {high_count} high-priority issue{'s' if high_count != 1 else ''} within the next 2 weeks")
    
    if score < 75:
        steps.append("Schedule a comprehensive technical audit to identify root causes")
    
    if score < 60:
        steps.append("Consider hiring a technical SEO specialist to help resolve complex issues")
    
    steps.extend([
        "Monitor Google Search Console weekly for new issues",
        "Set up automated monitoring for Core Web Vitals and page speed",
        "Re-run this analysis monthly to track improvement"
    ])
    
    return steps[:5]  # Return top 5 steps
