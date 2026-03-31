"""
Module 1: Technical Health Analysis

Analyzes website technical SEO health including Core Web Vitals,
mobile usability, HTTPS/security, and structured data implementation.
"""

import asyncio
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import statistics
from collections import defaultdict

import httpx
from bs4 import BeautifulSoup
import ssl
import socket
from urllib.parse import urlparse
import json


async def fetch_core_web_vitals(
    ga4_client: Any,
    property_id: str,
    start_date: str,
    end_date: str
) -> Dict[str, Any]:
    """
    Fetch and analyze Core Web Vitals from GA4.
    
    Metrics analyzed:
    - LCP (Largest Contentful Paint): < 2.5s = good, 2.5-4s = needs improvement, > 4s = poor
    - FID (First Input Delay): < 100ms = good, 100-300ms = needs improvement, > 300ms = poor
    - CLS (Cumulative Layout Shift): < 0.1 = good, 0.1-0.25 = needs improvement, > 0.25 = poor
    - INP (Interaction to Next Paint): < 200ms = good, 200-500ms = needs improvement, > 500ms = poor
    
    Args:
        ga4_client: Authenticated GA4 client
        property_id: GA4 property ID
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
    
    Returns:
        Dict with CWV metrics, scores, trends, and page-level breakdown
    """
    try:
        # Request CWV metrics from GA4
        # Note: CWV metrics may not be available in all GA4 properties
        # They require enhanced measurement and Chrome User Experience Report integration
        
        request_body = {
            "dateRanges": [{"startDate": start_date, "endDate": end_date}],
            "dimensions": [
                {"name": "date"},
                {"name": "pagePath"}
            ],
            "metrics": [
                {"name": "sessions"},
                # CWV metrics (if available)
                {"name": "userEngagementDuration"},
                {"name": "eventCount"}
            ]
        }
        
        # For CWV, we need to check web vitals events
        cwv_request = {
            "dateRanges": [{"startDate": start_date, "endDate": end_date}],
            "dimensions": [
                {"name": "eventName"},
                {"name": "pagePath"}
            ],
            "metrics": [
                {"name": "eventCount"},
                {"name": "eventValue"}  # This contains the CWV timing values
            ],
            "dimensionFilter": {
                "filter": {
                    "fieldName": "eventName",
                    "inListFilter": {
                        "values": ["LCP", "FID", "CLS", "INP", "TTFB"]
                    }
                }
            }
        }
        
        response = await ga4_client.run_report(property_id, cwv_request)
        
        # Parse CWV data
        cwv_data = defaultdict(lambda: {"values": [], "pages": defaultdict(list)})
        
        for row in response.get("rows", []):
            event_name = row["dimensionValues"][0]["value"]
            page_path = row["dimensionValues"][1]["value"]
            event_count = int(row["metricValues"][0]["value"])
            event_value = float(row["metricValues"][1]["value"])
            
            if event_count > 0:
                # Calculate average value for this metric
                avg_value = event_value / event_count
                cwv_data[event_name]["values"].append(avg_value)
                cwv_data[event_name]["pages"][page_path].append(avg_value)
        
        # Calculate aggregate metrics
        metrics = {}
        
        # LCP (Largest Contentful Paint) - in seconds
        if cwv_data["LCP"]["values"]:
            lcp_values = cwv_data["LCP"]["values"]
            lcp_avg = statistics.mean(lcp_values)
            lcp_p75 = statistics.quantiles(lcp_values, n=4)[2]  # 75th percentile
            
            if lcp_p75 <= 2500:
                lcp_score = "good"
            elif lcp_p75 <= 4000:
                lcp_score = "needs_improvement"
            else:
                lcp_score = "poor"
            
            metrics["lcp"] = {
                "value_ms": lcp_p75,
                "value_seconds": round(lcp_p75 / 1000, 2),
                "average_ms": round(lcp_avg, 0),
                "score": lcp_score,
                "threshold_good": 2500,
                "threshold_poor": 4000,
                "samples": len(lcp_values)
            }
        else:
            metrics["lcp"] = {
                "value_ms": None,
                "score": "no_data",
                "message": "LCP data not available in GA4"
            }
        
        # FID (First Input Delay) - in milliseconds
        if cwv_data["FID"]["values"]:
            fid_values = cwv_data["FID"]["values"]
            fid_avg = statistics.mean(fid_values)
            fid_p75 = statistics.quantiles(fid_values, n=4)[2]
            
            if fid_p75 <= 100:
                fid_score = "good"
            elif fid_p75 <= 300:
                fid_score = "needs_improvement"
            else:
                fid_score = "poor"
            
            metrics["fid"] = {
                "value_ms": round(fid_p75, 1),
                "average_ms": round(fid_avg, 1),
                "score": fid_score,
                "threshold_good": 100,
                "threshold_poor": 300,
                "samples": len(fid_values)
            }
        else:
            metrics["fid"] = {
                "value_ms": None,
                "score": "no_data",
                "message": "FID data not available (or INP is the new metric)"
            }
        
        # INP (Interaction to Next Paint) - newer metric replacing FID
        if cwv_data["INP"]["values"]:
            inp_values = cwv_data["INP"]["values"]
            inp_avg = statistics.mean(inp_values)
            inp_p75 = statistics.quantiles(inp_values, n=4)[2]
            
            if inp_p75 <= 200:
                inp_score = "good"
            elif inp_p75 <= 500:
                inp_score = "needs_improvement"
            else:
                inp_score = "poor"
            
            metrics["inp"] = {
                "value_ms": round(inp_p75, 1),
                "average_ms": round(inp_avg, 1),
                "score": inp_score,
                "threshold_good": 200,
                "threshold_poor": 500,
                "samples": len(inp_values)
            }
        else:
            metrics["inp"] = {
                "value_ms": None,
                "score": "no_data"
            }
        
        # CLS (Cumulative Layout Shift) - unitless
        if cwv_data["CLS"]["values"]:
            cls_values = cwv_data["CLS"]["values"]
            cls_avg = statistics.mean(cls_values)
            cls_p75 = statistics.quantiles(cls_values, n=4)[2]
            
            if cls_p75 <= 0.1:
                cls_score = "good"
            elif cls_p75 <= 0.25:
                cls_score = "needs_improvement"
            else:
                cls_score = "poor"
            
            metrics["cls"] = {
                "value": round(cls_p75, 3),
                "average": round(cls_avg, 3),
                "score": cls_score,
                "threshold_good": 0.1,
                "threshold_poor": 0.25,
                "samples": len(cls_values)
            }
        else:
            metrics["cls"] = {
                "value": None,
                "score": "no_data",
                "message": "CLS data not available in GA4"
            }
        
        # Identify worst performing pages for each metric
        worst_pages = {}
        
        for metric_name, metric_data in cwv_data.items():
            if metric_data["pages"]:
                page_scores = {}
                for page, values in metric_data["pages"].items():
                    if values:
                        page_scores[page] = statistics.mean(values)
                
                # Get top 10 worst pages
                sorted_pages = sorted(page_scores.items(), key=lambda x: x[1], reverse=True)
                worst_pages[metric_name.lower()] = [
                    {"page": page, "value": round(value, 2)}
                    for page, value in sorted_pages[:10]
                ]
        
        # Calculate overall CWV pass rate
        good_count = sum(1 for m in metrics.values() if m.get("score") == "good")
        total_count = sum(1 for m in metrics.values() if m.get("score") != "no_data")
        
        if total_count > 0:
            pass_rate = good_count / total_count
        else:
            pass_rate = 0
        
        # Overall health score (0-100)
        score_map = {"good": 100, "needs_improvement": 50, "poor": 0, "no_data": None}
        scores = [score_map[m.get("score")] for m in metrics.values() if score_map[m.get("score")] is not None]
        overall_score = statistics.mean(scores) if scores else 0
        
        return {
            "metrics": metrics,
            "worst_pages": worst_pages,
            "overall_score": round(overall_score, 1),
            "pass_rate": round(pass_rate * 100, 1),
            "summary": {
                "good_metrics": good_count,
                "needs_improvement": sum(1 for m in metrics.values() if m.get("score") == "needs_improvement"),
                "poor_metrics": sum(1 for m in metrics.values() if m.get("score") == "poor"),
                "no_data_metrics": sum(1 for m in metrics.values() if m.get("score") == "no_data")
            },
            "recommendations": _generate_cwv_recommendations(metrics, worst_pages)
        }
    
    except Exception as e:
        return {
            "error": str(e),
            "metrics": {},
            "overall_score": 0,
            "message": "Failed to fetch Core Web Vitals data from GA4"
        }


def _generate_cwv_recommendations(metrics: Dict, worst_pages: Dict) -> List[Dict[str, Any]]:
    """Generate actionable recommendations based on CWV performance."""
    recommendations = []
    
    # LCP recommendations
    lcp = metrics.get("lcp", {})
    if lcp.get("score") == "poor":
        recommendations.append({
            "metric": "LCP",
            "severity": "high",
            "issue": f"LCP is {lcp.get('value_seconds')}s (target: <2.5s)",
            "actions": [
                "Optimize and compress images (use WebP format, lazy loading)",
                "Minimize render-blocking resources (defer non-critical CSS/JS)",
                "Implement a CDN for faster asset delivery",
                "Use resource hints (preconnect, preload) for critical assets",
                "Consider server-side rendering or static site generation"
            ]
        })
    elif lcp.get("score") == "needs_improvement":
        recommendations.append({
            "metric": "LCP",
            "severity": "medium",
            "issue": f"LCP is {lcp.get('value_seconds')}s (target: <2.5s)",
            "actions": [
                "Optimize hero images and above-the-fold content",
                "Review and optimize web fonts loading",
                "Consider implementing a service worker for caching"
            ]
        })
    
    # FID/INP recommendations
    inp = metrics.get("inp", {})
    fid = metrics.get("fid", {})
    
    interaction_metric = inp if inp.get("score") != "no_data" else fid
    
    if interaction_metric.get("score") == "poor":
        recommendations.append({
            "metric": "INP" if inp.get("score") != "no_data" else "FID",
            "severity": "high",
            "issue": f"Interaction delay is {interaction_metric.get('value_ms')}ms",
            "actions": [
                "Break up long JavaScript tasks (use code splitting)",
                "Minimize main thread work",
                "Reduce JavaScript execution time",
                "Implement web worker for heavy computations",
                "Optimize event handlers and remove unused listeners"
            ]
        })
    elif interaction_metric.get("score") == "needs_improvement":
        recommendations.append({
            "metric": "INP" if inp.get("score") != "no_data" else "FID",
            "severity": "medium",
            "issue": f"Interaction delay is {interaction_metric.get('value_ms')}ms",
            "actions": [
                "Audit and optimize third-party scripts",
                "Defer non-critical JavaScript",
                "Review event listener efficiency"
            ]
        })
    
    # CLS recommendations
    cls = metrics.get("cls", {})
    if cls.get("score") == "poor":
        recommendations.append({
            "metric": "CLS",
            "severity": "high",
            "issue": f"CLS is {cls.get('value')} (target: <0.1)",
            "actions": [
                "Set explicit width and height on images and video elements",
                "Reserve space for ad slots and embeds",
                "Avoid inserting content above existing content",
                "Use CSS aspect-ratio for responsive images",
                "Preload fonts and use font-display: swap carefully"
            ]
        })
    elif cls.get("score") == "needs_improvement":
        recommendations.append({
            "metric": "CLS",
            "severity": "medium",
            "issue": f"CLS is {cls.get('value')} (target: <0.1)",
            "actions": [
                "Review dynamic content insertion patterns",
                "Audit animations and transitions",
                "Ensure web fonts load without layout shift"
            ]
        })
    
    # Page-specific recommendations
    if worst_pages:
        for metric_name, pages in worst_pages.items():
            if pages and len(pages) > 0:
                recommendations.append({
                    "metric": metric_name.upper(),
                    "severity": "medium",
                    "issue": f"Specific pages performing poorly for {metric_name.upper()}",
                    "actions": [
                        f"Prioritize optimization for: {pages[0]['page']}",
                        "Run PageSpeed Insights on these specific URLs",
                        "Compare with better-performing pages to identify issues"
                    ],
                    "affected_pages": [p["page"] for p in pages[:5]]
                })
    
    return recommendations


async def check_mobile_usability(
    domain: str,
    sample_urls: List[str]
) -> Dict[str, Any]:
    """
    Check mobile usability using multiple methods:
    1. Viewport meta tag presence
    2. Text size and readability
    3. Tap target sizing
    4. Mobile-friendly content width
    5. Responsive design detection
    
    Args:
        domain: Domain to check
        sample_urls: List of URLs to test (5-10 representative pages)
    
    Returns:
        Dict with mobile usability scores and issues
    """
    results = {
        "tested_urls": len(sample_urls),
        "pages": [],
        "overall_score": 0,
        "issues_summary": defaultdict(int),
        "recommendations": []
    }
    
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        for url in sample_urls[:10]:  # Limit to 10 URLs
            try:
                response = await client.get(
                    url,
                    headers={"User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 14_7_1 like Mac OS X) AppleWebKit/605.1.15"}
                )
                
                if response.status_code != 200:
                    continue
                
                soup = BeautifulSoup(response.text, 'html.parser')
                page_issues = []
                page_score = 100
                
                # Check 1: Viewport meta tag
                viewport = soup.find('meta', attrs={'name': 'viewport'})
                if not viewport:
                    page_issues.append("missing_viewport")
                    page_score -= 30
                    results["issues_summary"]["missing_viewport"] += 1
                else:
                    content = viewport.get('content', '')
                    if 'width=device-width' not in content:
                        page_issues.append("incorrect_viewport")
                        page_score -= 15
                        results["issues_summary"]["incorrect_viewport"] += 1
                
                # Check 2: Text sizing
                # Look for font-size in inline styles or style tags
                small_text_count = 0
                for element in soup.find_all(style=True):
                    style = element.get('style', '')
                    if 'font-size' in style:
                        # Simple check for very small fonts
                        if 'font-size:10px' in style or 'font-size:11px' in style:
                            small_text_count += 1
                
                if small_text_count > 5:
                    page_issues.append("small_text")
                    page_score -= 10
                    results["issues_summary"]["small_text"] += 1
                
                # Check 3: Tap targets (buttons and links)
                clickable_elements = soup.find_all(['a', 'button'])
                small_tap_targets = 0
                
                for element in clickable_elements[:50]:  # Sample first 50
                    style = element.get('style', '')
                    # Very basic check - in production, would need to render
                    if 'padding' not in style and 'height' not in style:
                        # Check if text is very short (likely small target)
                        text = element.get_text(strip=True)
                        if len(text) <= 2:
                            small_tap_targets += 1
                
                if small_tap_targets > 10:
                    page_issues.append("small_tap_targets")
                    page_score -= 15
                    results["issues_summary"]["small_tap_targets"] += 1
                
                # Check 4: Fixed width content
                # Look for fixed widths in style attributes or style tags
                fixed_width_elements = soup.find_all(style=lambda value: value and 'width:' in value and 'px' in value)
                if len(fixed_width_elements) > 20:
                    page_issues.append("fixed_width_content")
                    page_score -= 10
                    results["issues_summary"]["fixed_width_content"] += 1
                
                # Check 5: Responsive images
                images = soup.find_all('img')
                non_responsive_images = 0
                for img in images[:30]:  # Sample first 30
                    if not img.get('srcset') and not img.get('style'):
                        non_responsive_images += 1
                
                if non_responsive_images > len(images) * 0.5 and len(images) > 5:
                    page_issues.append("non_responsive_images")
                    page_score -= 10
                    results["issues_summary"]["non_responsive_images"] += 1
                
                # Check 6: Horizontal scrolling indicators
                # Look for elements with very wide fixed widths
                wide_elements = 0
                for element in soup.find_all(style=True):
                    style = element.get('style', '')
                    if 'width:' in style:
                        # Look for widths > 1000px which would cause scrolling on mobile
                        import re
                        width_match = re.search(r'width:\s*(\d+)px', style)
                        if width_match and int(width_match.group(1)) > 1000:
                            wide_elements += 1
                
                if wide_elements > 3:
                    page_issues.append("horizontal_scroll_risk")
                    page_score -= 15
                    results["issues_summary"]["horizontal_scroll_risk"] += 1
                
                results["pages"].append({
                    "url": url,
                    "score": max(0, page_score),
                    "issues": page_issues,
                    "has_viewport": bool(viewport),
                    "clickable_elements_count": len(clickable_elements)
                })
            
            except Exception as e:
                results["pages"].append({
                    "url": url,
                    "error": str(e),
                    "score": 0
                })
    
    # Calculate overall score
    if results["pages"]:
        valid_scores = [p["score"] for p in results["pages"] if "error" not in p]
        if valid_scores:
            results["overall_score"] = round(statistics.mean(valid_scores), 1)
    
    # Generate recommendations
    if results["issues_summary"]["missing_viewport"] > 0:
        results["recommendations"].append({
            "severity": "critical",
            "issue": f"{results['issues_summary']['missing_viewport']} pages missing viewport meta tag",
            "action": "Add <meta name='viewport' content='width=device-width, initial-scale=1'> to all pages"
        })
    
    if results["issues_summary"]["small_tap_targets"] > 0:
        results["recommendations"].append({
            "severity": "high",
            "issue": f"{results['issues_summary']['small_tap_targets']} pages have small tap targets",
            "action": "Ensure interactive elements are at least 48x48px with adequate spacing"
        })
    
    if results["issues_summary"]["fixed_width_content"] > 0:
        results["recommendations"].append({
            "severity": "medium",
            "issue": f"{results['issues_summary']['fixed_width_content']} pages use fixed-width layouts",
            "action": "Use responsive CSS (flexbox, grid, percentages) instead of fixed pixel widths"
        })
    
    if results["issues_summary"]["horizontal_scroll_risk"] > 0:
        results["recommendations"].append({
            "severity": "high",
            "issue": f"{results['issues_summary']['horizontal_scroll_risk']} pages may cause horizontal scrolling",
            "action": "Audit wide elements and ensure max-width: 100% on content containers"
        })
    
    if results["issues_summary"]["non_responsive_images"] > 0:
        results["recommendations"].append({
            "severity": "medium",
            "issue": "Many images lack responsive attributes",
            "action": "Implement srcset and sizes attributes for responsive images"
        })
    
    return results


async def analyze_https_security(
    domain: str,
    sample_urls: List[str]
) -> Dict[str, Any]:
    """
    Analyze HTTPS implementation and security headers.
    
    Checks:
    1. SSL certificate validity and strength
    2. HTTPS redirect (HTTP -> HTTPS)
    3. Mixed content issues
    4. Security headers (HSTS, CSP, X-Frame-Options, etc.)
    5. Certificate expiration
    
    Args:
        domain: Domain to analyze
        sample_urls: Sample URLs to test
    
    Returns:
        Dict with security analysis and recommendations
    """
    results = {
        "domain": domain,
        "ssl_certificate": {},
        "https_redirect": {},
        "security_headers": {},
        "mixed_content": {},
        "overall_score": 0,
        "issues": [],
        "recommendations": []
    }
    
    parsed_domain = urlparse(f"https://{domain}" if not domain.startswith("http") else domain)
    hostname = parsed_domain.netloc or parsed_domain.path
    
    # Check 1: SSL Certificate
    try:
        context = ssl.create_default_context()
        with socket.create_connection((hostname, 443), timeout=10) as sock:
            with context.wrap_socket(sock, server_hostname=hostname) as secure_sock:
                cert = secure_sock.getpeercert()
                
                # Parse certificate info
                not_after = datetime.strptime(cert['notAfter'], '%b %d %H:%M:%S %Y %Z')
                days_until_expiry = (not_after - datetime.now()).days
                
                results["ssl_certificate"] = {
                    "valid": True,
                    "issuer": dict(x[0] for x in cert['issuer']),
                    "subject": dict(x[0] for x in cert['subject']),
                    "expires": not_after.isoformat(),
                    "days_until_expiry": days_until_expiry,
                    "version": cert['version']
                }
                
                if days_until_expiry < 30:
                    results["issues"].append("ssl_expiring_soon")
                    results["recommendations"].append({
                        "severity": "high",
                        "issue": f"SSL certificate expires in {days_until_expiry} days",
                        "action": "Renew SSL certificate immediately"
                    })
    
    except Exception as e:
        results["ssl_certificate"] = {
            "valid": False,
            "error": str(e)
        }
        results["issues"].append("ssl_error")
        results["recommendations"].append({
            "severity": "critical",
            "issue": "SSL certificate issue detected",
            "action": f"Fix SSL configuration: {str(e)}"
        })
    
    # Check 2: HTTPS Redirect
    async with httpx.AsyncClient(timeout=10.0, follow_redirects=False) as client:
        try:
            http_url = f"http://{hostname}"
            response = await client.get(http_url)
            
            if response.status_code in [301, 302, 307, 308]:
                location = response.headers.get('location', '')
                if location.startswith('https://'):
                    results["https_redirect"] = {
                        "enabled": True,
                        "status_code": response.status_code,
                        "permanent": response.status_code in [301, 308]
                    }
                    if response.status_code in [302, 307]:
                        results["recommendations"].append({
                            "severity": "low",
                            "issue": "HTTP to HTTPS redirect uses temporary redirect",
                            "action": "Use 301 (permanent) redirect instead of 302 for SEO benefits"
                        })
                else:
                    results["https_redirect"] = {"enabled": False}
                    results["issues"].append("no_https_redirect")
                    results["recommendations"].append({
                        "severity": "high",
                        "issue": "HTTP requests don't redirect to HTTPS",
                        "action": "Implement 301 redirect from HTTP to HTTPS"
                    })
            else:
                results["https_redirect"] = {"enabled": False}
                results["issues"].append("no_https_redirect")
        
        except Exception as e:
            results["https_redirect"] = {"error": str(e)}
    
    # Check 3: Security Headers
    if sample_urls:
        test_url = sample_urls[0]
    else:
        test_url = f"https://{hostname}"
    
    async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
        try:
            response = await client.get(test_url)
            headers = response.headers
            
            security_headers = {}
            
            # HSTS (HTTP Strict Transport Security)
            hsts = headers.get('strict-transport-security')
            security_headers['hsts'] = {
                "present": bool(hsts),
                "value": hsts,
                "score": 100 if hsts else 0
            }
            if not hsts:
                results["issues"].append("missing_hsts")
                results["recommendations"].append({
                    "severity": "high",
                    "issue": "Missing HSTS header",
                    "action": "Add Strict-Transport-Security: max-age=31536000; includeSubDomains"
                })
            
            # Content Security Policy
            csp = headers.get('content-security-policy')
            security_headers['csp'] = {
                "present": bool(csp),
                "value": csp[:100] if csp else None,
                "score": 100 if csp else 0
            }
            if not csp:
                results["issues"].append("missing_csp")
                results["recommendations"].append({
                    "severity": "medium",
                    "issue": "Missing Content-Security-Policy header",
                    "action": "Implement CSP to prevent XSS attacks"
                })
            
            # X-Frame-Options
            xfo = headers.get('x-frame-options')
            security_headers['x_frame_options'] = {
                "present": bool(xfo),
                "value": xfo,
                "score": 100 if xfo else 0
            }
            if not xfo:
                results["issues"].append("missing_x_frame_options")
                results["recommendations"].append({
                    "severity": "medium",
                    "issue": "Missing X-Frame-Options header",
                    "action": "Add X-Frame-Options: SAMEORIGIN to prevent clickjacking"
                })
            
            # X-Content-Type-Options
            xcto = headers.get('x-content-type-options')
            security_headers['x_content_type_options'] = {
                "present": bool(xcto),
                "value": xcto,
                "score": 100 if xcto else 0
            }
            if not xcto:
                results["issues"].append("missing_x_content_type_options")
            
            # Referrer-Policy
            rp = headers.get('referrer-policy')
            security_headers['referrer_policy'] = {
                "present": bool(rp),
                "value": rp,
                "score": 100 if rp else 0
            }
            
            # Permissions-Policy (formerly Feature-Policy)
            pp = headers.get('permissions-policy')
            security_headers['permissions_policy'] = {
                "present": bool(pp),
                "value": pp[:100] if pp else None,
                "score": 100 if pp else 0
            }
            
            results["security_headers"] = security_headers
            
            # Calculate header score
            header_scores = [h["score"] for h in security_headers.values()]
            results["security_headers"]["overall_score"] = round(statistics.mean(header_scores), 1)
            
        except Exception as e:
            results["security_headers"] = {"error": str(e)}
    
    # Check 4: Mixed Content
    mixed_content_issues = []
    
    for url in sample_urls[:5]:  # Check first 5 URLs
        if not url.startswith('https://'):
            continue
        
        try:
            async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
                response = await client.get(url)
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Check for HTTP resources
                http_resources = []
                
                # Images
                for img in soup.find_all('img', src=True):
                    if img['src'].startswith('http://'):
                        http_resources.append(('image', img['src']))
                
                # Scripts
                for script in soup.find_all('script', src=True):
                    if script['src'].startswith('http://'):
                        http_resources.append(('script', script['src']))
                
                # Stylesheets
                for link in soup.find_all('link', href=True):
                    if link['href'].startswith('http://'):
                        http_resources.append(('stylesheet', link['href']))
                
                if http_resources:
                    mixed_content_issues.append({
                        "url": url,
                        "count": len(http_resources),
                        "types": list(set(r[0] for r in http_resources)),
                        "examples": http_resources[:3]
                    })
        
        except Exception:
            pass
    
    results["mixed_content"] = {
        "pages_checked": len(sample_urls[:5]),
        "pages_with_issues": len(mixed_content_issues),
        "issues": mixed_content_issues
    }
    
    if mixed_content_issues:
        results["issues"].append("mixed_content")
        results["recommendations"].append({
            "severity": "high",
            "issue": f"{len(mixed_content_issues)} pages have mixed content (HTTP resources on HTTPS pages)",
            "action": "Update all resource URLs to use HTTPS or protocol-relative URLs"
        })
    
    # Calculate overall score
    ssl_score = 100 if results["ssl_certificate"].get("valid") else 0
    redirect_score = 100 if results["https_redirect"].get("enabled") else 0
    headers_score = results["security_headers"].get("overall_score", 0)
    mixed_content_score = 100 if len(mixed_content_issues) == 0 else max(0, 100 - (len(mixed_content_issues) * 20))
    
    results["overall_score"] = round(statistics.mean([ssl_score, redirect_score, headers_score, mixed_content_score]), 1)
    
    return results


async def validate_structured_data(
    sample_urls: List[str]
) -> Dict[str, Any]:
    """
    Validate structured data (schema.org) implementation.
    
    Checks:
    1. Presence of JSON-LD, Microdata, or RDFa
    2. Schema types implemented
    3. Validation errors
    4. Coverage across pages
    5. Recommended schemas for page type
    
    Args:
        sample_urls: URLs to test for structured data
    
    Returns:
        Dict with structured data analysis and recommendations
    """
    results = {
        "pages_analyzed": len(sample_urls),
        "pages_with_schema": 0,
        "schema_types_found": defaultdict(int),
        "pages": [],
        "recommendations": [],
        "overall_coverage": 0
    }
    
    async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
        for url in sample_urls:
            try:
                response = await client.get(url)
                if response.status_code != 200:
                    continue
                
                soup = BeautifulSoup(response.text, 'html.parser')
                page_result = {
                    "url": url,
                    "schemas": [],
                    "format": [],
                    "errors": [],
                    "warnings": []
                }
                
                # Check for JSON-LD
                json_ld_scripts = soup.find_all('script', type='application/ld+json')
                for script in json_ld_scripts:
                    try:
                        data = json.loads(script.string)
                        
                        # Handle @graph array
                        if isinstance(data, dict) and '@graph' in data:
                            schemas = data['@graph']
                        elif isinstance(data, list):
                            schemas = data
                        else:
                            schemas = [data]
                        
                        for schema in schemas:
                            if isinstance(schema, dict) and '@type' in schema:
                                schema_type = schema['@type']
                                if isinstance(schema_type, list):
                                    for st in schema_type:
                                        page_result["schemas"].append(st)
                                        results["schema_types_found"][st] += 1
                                else:
                                    page_result["schemas"].append(schema_type)
                                    results["schema_types_found"][schema_type] += 1
                        
                        page_result["format"].append("JSON-LD")
                    
                    except json.JSONDecodeError as e:
                        page_result["errors"].append(f"Invalid JSON-LD: {str(e)}")
                
                # Check for Microdata
                microdata_items = soup.find_all(attrs={"itemscope": True})
                if microdata_items:
                    page_result["format"].append("Microdata")
                    for item in microdata_items:
                        itemtype = item.get('itemtype', '')
                        if itemtype:
                            schema_type = itemtype.split('/')[-1]
                            page_result["schemas"].append(schema_type)
                            results["schema_types_found"][schema_type] += 1
                
                # Check for RDFa
                rdfa_items = soup.find_all(attrs={"vocab": True})
                if rdfa_items:
                    page_result["format"].append("RDFa")
                
                # Validation: Check for common required properties
                if "Article" in page_result["schemas"] or "BlogPosting" in page_result["schemas"]:
                    # Check for required Article properties
                    if json_ld_scripts:
                        for script in json_ld_scripts:
                            try:
                                data = json.loads(script.string)
                                schemas = data.get('@graph', [data]) if isinstance(data, dict) else [data]
                                
                                for schema in schemas:
                                    if isinstance(schema, dict) and schema.get('@type') in ['Article', 'BlogPosting']:
                                        if not schema.get('headline'):
                                            page_result["warnings"].append("Article missing 'headline' property")
                                        if not schema.get('datePublished'):
                                            page_result["warnings"].append("Article missing 'datePublished' property")
                                        if not schema.get('author'):
                                            page_result["warnings"].append("Article missing 'author' property")
                            except:
                                pass
                
                if "Product" in page_result["schemas"]:
                    if json_ld_scripts:
                        for script in json_ld_scripts:
                            try:
                                data = json.loads(script.string)
                                schemas = data.get('@graph', [data]) if isinstance(data, dict) else [data]
                                
                                for schema in schemas:
                                    if isinstance(schema, dict) and schema.get('@type') == 'Product':
                                        if not schema.get('name'):
                                            page_result["warnings"].append("Product missing 'name' property")
                                        if not schema.get('offers'):
                                            page_result["warnings"].append("Product missing 'offers' property")
                            except:
                                pass
                
                if page_result["schemas"]:
                    results["pages_with_schema"] += 1
                
                results["pages"].append(page_result)
            
            except Exception as e:
                results["pages"].append({
                    "url": url,
                    "error": str(e)
                })
    
    # Calculate coverage
    if results["pages_analyzed"] > 0:
        results["overall_coverage"] = round((results["pages_with_schema"] / results["pages_analyzed"]) * 100, 1)
    
    # Generate recommendations
    if results["overall_coverage"] < 50:
        results["recommendations"].append({
            "severity": "high",
            "issue": f"Only {results['overall_coverage']}% of pages have structured data",
            "action": "Implement schema markup across all pages. Start with Organization, WebSite, and BreadcrumbList."
        })
    
    # Recommend missing common schemas
    common_schemas = {
        "Organization": "Add Organization schema to homepage for brand identity",
        "WebSite": "Add WebSite schema with siteNavigationElement for site-wide search",
        "BreadcrumbList": "Add BreadcrumbList to improve breadcrumb display in search",
        "Article": "Add Article schema to blog posts and content pages",
        "FAQPage": "Add FAQPage schema to FAQ pages for rich results",
        "Product": "Add Product schema with reviews and pricing for e-commerce",
        "LocalBusiness": "Add LocalBusiness schema if you have physical locations"
    }
    
    for schema_type, recommendation in common_schemas.items():
        if schema_type not in results["schema_types_found"]:
            results["recommendations"].append({
                "severity": "medium",
                "issue": f"Missing {schema_type} schema",
                "action": recommendation
            })
        elif results["schema_types_found"][schema_type] < results["pages_analyzed"] * 0.3:
            results["recommendations"].append({
                "severity": "low",
                "issue": f"{schema_type} schema only on {results['schema_types_found'][schema_type]} pages",
                "action": f"Expand {schema_type} implementation to more relevant pages"
            })
    
    # Check for validation errors
    pages_with_errors = sum(1 for p in results["pages"] if p.get("errors"))
    if pages_with_errors > 0:
        results["recommendations"].append({
            "severity": "medium",
            "issue": f"{pages_with_errors} pages have structured data errors",
            "action": "Fix JSON-LD syntax errors and validate with Google's Rich Results Test"
        })
    
    # Convert defaultdict to regular dict for JSON serialization
    results["schema_types_found"] = dict(results["schema_types_found"])
    
    return results


async def generate_technical_health(
    domain: str,
    ga4_client: Any,
    ga4_property_id: str,
    start_date: str,
    end_date: str,
    sample_urls: List[str]
) -> Dict[str, Any]:
    """
    Orchestrator function that runs all technical health checks and combines results.
    
    Args:
        domain: Domain to analyze
        ga4_client: Authenticated GA4 client
        ga4_property_id: GA4 property ID
        start_date: Analysis start date
        end_date: Analysis end date
        sample_urls: Sample URLs for testing (from sitemap or GSC)
    
    Returns:
        Complete technical health report with scores, issues, and recommendations
    """
    
    # Run all checks concurrently
    cwv_task = fetch_core_web_vitals(ga4_client, ga4_property_id, start_date, end_date)
    mobile_task = check_mobile_usability(domain, sample_urls)
    security_task = analyze_https_security(domain, sample_urls)
    schema_task = validate_structured_data(sample_urls)
    
    cwv_results, mobile_results, security_results, schema_results = await asyncio.gather(
        cwv_task, mobile_task, security_task, schema_task, return_exceptions=True
    )
    
    # Handle any exceptions
    if isinstance(cwv_results, Exception):
        cwv_results = {"error": str(cwv_results), "overall_score": 0}
    if isinstance(mobile_results, Exception):
        mobile_results = {"error": str(mobile_results), "overall_score": 0}
    if isinstance(security_results, Exception):
        security_results = {"error": str(security_results), "overall_score": 0}
    if isinstance(schema_results, Exception):
        schema_results = {"error": str(schema_results), "overall_coverage": 0}
    
    # Calculate overall technical health score (weighted average)
    weights = {
        "core_web_vitals": 0.35,  # Most important for rankings
        "mobile_usability": 0.25,
        "https_security": 0.20,
        "structured_data": 0.20
    }
    
    overall_score = (
        cwv_results.get("overall_score", 0) * weights["core_web_vitals"] +
        mobile_results.get("overall_score", 0) * weights["mobile_usability"] +
        security_results.get("overall_score", 0) * weights["https_security"] +
        schema_results.get("overall_coverage", 0) * weights["structured_data"]
    )
    
    # Combine all recommendations
    all_recommendations = []
    
    for rec in cwv_results.get("recommendations", []):
        rec["category"] = "Core Web Vitals"
        all_recommendations.append(rec)
    
    for rec in mobile_results.get("recommendations", []):
        rec["category"] = "Mobile Usability"
        all_recommendations.append(rec)
    
    for rec in security_results.get("recommendations", []):
        rec["category"] = "HTTPS & Security"
        all_recommendations.append(rec)
    
    for rec in schema_results.get("recommendations", []):
        rec["category"] = "Structured Data"
        all_recommendations.append(rec)
    
    # Sort recommendations by severity
    severity_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    all_recommendations.sort(key=lambda x: severity_order.get(x.get("severity", "low"), 3))
    
    # Compile all issues
    all_issues = []
    all_issues.extend(cwv_results.get("summary", {}).get("poor_metrics", 0) * ["poor_cwv"])
    all_issues.extend(mobile_results.get("issues_summary", {}).items())
    all_issues.extend(security_results.get("issues", []))
    if schema_results.get("overall_coverage", 0) < 50:
        all_issues.append("low_schema_coverage")
    
    return {
        "domain": domain,
        "analysis_date": datetime.now().isoformat(),
        "date_range": {
            "start": start_date,
            "end": end_date
        },
        "overall_score": round(overall_score, 1),
        "scores": {
            "core_web_vitals": cwv_results.get("overall_score", 0),
            "mobile_usability": mobile_results.get("overall_score", 0),
            "https_security": security_results.get("overall_score", 0),
            "structured_data": schema_results.get("overall_coverage", 0)
        },
        "core_web_vitals": cwv_results,
        "mobile_usability": mobile_results,
        "https_security": security_results,
        "structured_data": schema_results,
        "total_issues": len(all_issues),
        "critical_issues": sum(1 for r in all_recommendations if r.get("severity") == "critical"),
        "high_priority_issues": sum(1 for r in all_recommendations if r.get("severity") == "high"),
        "recommendations": all_recommendations[:20],  # Top 20 recommendations
        "summary": {
            "health_status": _calculate_health_status(overall_score),
            "top_priority": all_recommendations[0] if all_recommendations else None,
            "pages_analyzed": len(sample_urls),
            "key_findings": _generate_key_findings(cwv_results, mobile_results, security_results, schema_results)
        }
    }


def _calculate_health_status(score: float) -> str:
    """Convert numerical score to health status label."""
    if score >= 80:
        return "excellent"
    elif score >= 60:
        return "good"
    elif score >= 40:
        return "needs_improvement"
    else:
        return "critical"


def _generate_key_findings(cwv, mobile, security, schema) -> List[str]:
    """Generate human-readable key findings."""
    findings = []
    
    # CWV findings
    if cwv.get("overall_score", 0) < 50:
        poor_metrics = []
        for metric, data in cwv.get("metrics", {}).items():
            if data.get("score") == "poor":
                poor_metrics.append(metric.upper())
        if poor_metrics:
            findings.append(f"Core Web Vitals failing: {', '.join(poor_metrics)}")
    
    # Mobile findings
    if mobile.get("overall_score", 0) < 60:
        findings.append(f"Mobile usability issues on {mobile.get('tested_urls', 0)} pages")
    
    # Security findings
    if not security.get("ssl_certificate", {}).get("valid"):
        findings.append("SSL certificate issues detected")
    if not security.get("https_redirect", {}).get("enabled"):
        findings.append("Missing HTTPS redirect")
    
    # Schema findings
    coverage = schema.get("overall_coverage", 0)
    if coverage < 30:
        findings.append(f"Low structured data coverage ({coverage}%)")
    elif coverage < 70:
        findings.append(f"Moderate structured data coverage ({coverage}%)")
    
    if not findings:
        findings.append("Technical health is good overall")
    
    return findings
