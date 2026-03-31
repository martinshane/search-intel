"""
Module 8: Technical Health — Core Web Vitals analysis, indexing coverage,
mobile usability, crawl error classification, and technical debt scoring.

Phase 2 full implementation.  Consumes GA4 Core Web Vitals data, GSC index
coverage, GSC mobile usability reports, and crawl data to produce:
  1. Core Web Vitals assessment (LCP, INP, CLS) with pass/fail and trends
  2. Indexing coverage analysis — valid, excluded, errors by reason
  3. Mobile usability issue detection and severity scoring
  4. Crawl error classification and priority ranking
  5. Technical debt score (0–100) with actionable fix list
"""

import logging
from typing import Any, Dict, List, Optional
from collections import Counter, defaultdict

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Core Web Vitals thresholds (Google's "good" / "needs improvement" / "poor")
CWV_THRESHOLDS = {
    "lcp": {"good": 2500, "poor": 4000},       # milliseconds
    "inp": {"good": 200, "poor": 500},          # milliseconds
    "cls": {"good": 0.1, "poor": 0.25},         # unitless
    "fid": {"good": 100, "poor": 300},          # milliseconds (legacy)
    "fcp": {"good": 1800, "poor": 3000},        # milliseconds
    "ttfb": {"good": 800, "poor": 1800},        # milliseconds
}

# Weight each CWV metric contributes to overall CWV score
CWV_SCORE_WEIGHTS = {
    "lcp": 0.35,
    "inp": 0.35,
    "cls": 0.30,
}

# Indexing issue severity map
INDEX_SEVERITY = {
    "noindex": "high",
    "blocked_by_robots_txt": "high",
    "redirect": "medium",
    "not_found": "high",
    "soft_404": "medium",
    "server_error": "critical",
    "crawl_anomaly": "medium",
    "duplicate_without_canonical": "medium",
    "duplicate_submitted_not_selected": "low",
    "crawled_not_indexed": "high",
    "discovered_not_indexed": "medium",
    "alternate_page": "low",
    "excluded_by_tag": "low",
}

# Mobile usability issue severity
MOBILE_SEVERITY = {
    "text_too_small": "medium",
    "clickable_elements_too_close": "medium",
    "content_wider_than_screen": "high",
    "viewport_not_set": "critical",
    "incompatible_plugins": "high",
}


# ---------------------------------------------------------------------------
# Core Web Vitals Analysis
# ---------------------------------------------------------------------------

def _classify_cwv_value(metric: str, value: float) -> str:
    """Classify a CWV value as good / needs_improvement / poor."""
    thresholds = CWV_THRESHOLDS.get(metric)
    if not thresholds:
        return "unknown"
    if value <= thresholds["good"]:
        return "good"
    elif value <= thresholds["poor"]:
        return "needs_improvement"
    else:
        return "poor"


def _score_cwv_metric(metric: str, value: float) -> float:
    """
    Score a single CWV metric 0-100.
    100 = at or below the 'good' threshold.
    0   = at or above 2x the 'poor' threshold.
    Linear interpolation in between.
    """
    thresholds = CWV_THRESHOLDS.get(metric)
    if not thresholds:
        return 50.0  # unknown metric, neutral score
    good = thresholds["good"]
    poor = thresholds["poor"]
    if value <= good:
        return 100.0
    elif value >= poor * 2:
        return 0.0
    elif value <= poor:
        # Linear 100->40 between good and poor
        ratio = (value - good) / (poor - good)
        return 100.0 - (ratio * 60.0)
    else:
        # Linear 40->0 between poor and 2xpoor
        ratio = (value - poor) / poor
        return max(0.0, 40.0 - (ratio * 40.0))


def _analyze_core_web_vitals(ga4_cwv_data: Optional[Dict]) -> Dict[str, Any]:
    """
    Analyse Core Web Vitals from GA4 CrUX-style data.

    Expected input shape (flexible -- handles multiple formats):
    {
        "metrics": {
            "lcp": {"p75": 2400, "values": [...], "good_pct": 0.72, ...},
            "inp": {"p75": 180, ...},
            "cls": {"p75": 0.08, ...}
        },
        "pages": [
            {"url": "/page", "lcp": 2600, "inp": 210, "cls": 0.12, ...}
        ]
    }
    """
    result = {
        "overall_score": 0.0,
        "pass": False,
        "metrics": {},
        "page_level": [],
        "recommendations": [],
    }

    if not ga4_cwv_data or not isinstance(ga4_cwv_data, dict):
        result["recommendations"].append(
            "No Core Web Vitals data available. Ensure GA4 is collecting CrUX data "
            "or implement the web-vitals JS library for field measurements."
        )
        return result

    metrics_data = ga4_cwv_data.get("metrics", {})
    if not metrics_data and isinstance(ga4_cwv_data, dict):
        # Fallback: maybe the data is flat {lcp: ..., inp: ..., cls: ...}
        for key in ("lcp", "inp", "cls", "fid", "fcp", "ttfb"):
            if key in ga4_cwv_data:
                metrics_data[key] = ga4_cwv_data[key]

    # --- Per-metric analysis ---
    weighted_score = 0.0
    weight_sum = 0.0

    for metric_name in ("lcp", "inp", "cls"):
        metric_info = metrics_data.get(metric_name, {})
        if isinstance(metric_info, dict):
            p75 = metric_info.get("p75", metric_info.get("value"))
        elif isinstance(metric_info, (int, float)):
            p75 = metric_info
        else:
            p75 = None

        if p75 is None:
            result["metrics"][metric_name] = {"status": "no_data"}
            continue

        p75 = float(p75)
        classification = _classify_cwv_value(metric_name, p75)
        score = _score_cwv_metric(metric_name, p75)
        weight = CWV_SCORE_WEIGHTS.get(metric_name, 0.33)

        result["metrics"][metric_name] = {
            "p75": p75,
            "classification": classification,
            "score": round(score, 1),
            "threshold_good": CWV_THRESHOLDS[metric_name]["good"],
            "threshold_poor": CWV_THRESHOLDS[metric_name]["poor"],
            "good_pct": metric_info.get("good_pct") if isinstance(metric_info, dict) else None,
        }

        weighted_score += score * weight
        weight_sum += weight

        # Recommendations
        if classification == "poor":
            result["recommendations"].append(
                _cwv_recommendation(metric_name, p75, "poor")
            )
        elif classification == "needs_improvement":
            result["recommendations"].append(
                _cwv_recommendation(metric_name, p75, "needs_improvement")
            )

    if weight_sum > 0:
        result["overall_score"] = round(weighted_score / weight_sum, 1)
        result["pass"] = all(
            result["metrics"].get(m, {}).get("classification") == "good"
            for m in ("lcp", "inp", "cls")
            if result["metrics"].get(m, {}).get("classification") != "no_data"
        )

    # --- Page-level analysis (top slowest pages) ---
    pages = ga4_cwv_data.get("pages", [])
    if pages and isinstance(pages, list):
        scored_pages = []
        for page in pages[:200]:  # cap to avoid huge payloads
            url = page.get("url", page.get("page_path", "unknown"))
            page_scores = {}
            worst_metric = None
            worst_class = "good"
            for m in ("lcp", "inp", "cls"):
                val = page.get(m)
                if val is not None:
                    val = float(val)
                    cls_ = _classify_cwv_value(m, val)
                    page_scores[m] = {"value": val, "classification": cls_}
                    if cls_ == "poor" or (cls_ == "needs_improvement" and worst_class != "poor"):
                        worst_class = cls_
                        worst_metric = m
            scored_pages.append({
                "url": url,
                "metrics": page_scores,
                "worst_metric": worst_metric,
                "worst_classification": worst_class,
            })
        # Sort: poor first, then needs_improvement, then good
        priority_order = {"poor": 0, "needs_improvement": 1, "good": 2, None: 3}
        scored_pages.sort(key=lambda p: priority_order.get(p["worst_classification"], 3))
        result["page_level"] = scored_pages[:30]  # Top 30 worst pages

    return result


def _cwv_recommendation(metric: str, value: float, classification: str) -> str:
    """Generate a specific recommendation for a failing CWV metric."""
    severity = "Critical" if classification == "poor" else "Moderate"
    recs = {
        "lcp": (
            f"{severity}: Largest Contentful Paint is {value:.0f}ms (p75). "
            f"Optimize by: (1) preloading hero images/fonts, (2) using next-gen image "
            f"formats (WebP/AVIF), (3) reducing server response time (TTFB), "
            f"(4) eliminating render-blocking resources, (5) implementing CDN caching."
        ),
        "inp": (
            f"{severity}: Interaction to Next Paint is {value:.0f}ms (p75). "
            f"Optimize by: (1) breaking long tasks into smaller chunks, (2) yielding "
            f"to the main thread with scheduler.yield(), (3) reducing JavaScript bundle "
            f"size, (4) debouncing input handlers, (5) using web workers for heavy "
            f"computation."
        ),
        "cls": (
            f"{severity}: Cumulative Layout Shift is {value:.3f} (p75). "
            f"Optimize by: (1) setting explicit width/height on images and embeds, "
            f"(2) reserving space for dynamic content (ads, lazy-loaded elements), "
            f"(3) avoiding inserting content above existing content, (4) using "
            f"CSS contain and content-visibility, (5) loading web fonts with "
            f"font-display: optional."
        ),
    }
    return recs.get(metric, f"{severity}: {metric} is {value} -- investigate further.")


# ---------------------------------------------------------------------------
# Indexing Coverage Analysis
# ---------------------------------------------------------------------------

def _analyze_indexing_coverage(gsc_coverage: Optional[Dict]) -> Dict[str, Any]:
    """
    Analyse GSC index coverage data.

    Expected input shape:
    {
        "summary": {"valid": 1200, "warning": 30, "excluded": 450, "error": 12},
        "issues": [
            {"reason": "crawled_not_indexed", "count": 150, "urls": [...]},
            {"reason": "noindex", "count": 80, "urls": [...]},
            ...
        ]
    }
    """
    result = {
        "summary": {"valid": 0, "warning": 0, "excluded": 0, "error": 0, "total": 0},
        "index_ratio": 0.0,
        "issues_by_severity": {"critical": [], "high": [], "medium": [], "low": []},
        "top_issues": [],
        "recommendations": [],
    }

    if not gsc_coverage or not isinstance(gsc_coverage, dict):
        result["recommendations"].append(
            "No indexing coverage data available. Connect Google Search Console "
            "to access URL inspection and coverage reports."
        )
        return result

    # Summary stats
    summary = gsc_coverage.get("summary", {})
    valid = int(summary.get("valid", 0))
    warning = int(summary.get("warning", 0))
    excluded = int(summary.get("excluded", 0))
    error = int(summary.get("error", 0))
    total = valid + warning + excluded + error

    result["summary"] = {
        "valid": valid,
        "warning": warning,
        "excluded": excluded,
        "error": error,
        "total": total,
    }
    result["index_ratio"] = round(valid / total, 3) if total > 0 else 0.0

    # Classify issues by severity
    issues = gsc_coverage.get("issues", [])
    for issue in issues:
        reason = issue.get("reason", "unknown")
        count = int(issue.get("count", 0))
        severity = INDEX_SEVERITY.get(reason, "low")
        sample_urls = issue.get("urls", [])[:5]

        issue_entry = {
            "reason": reason,
            "count": count,
            "severity": severity,
            "sample_urls": sample_urls,
        }
        result["issues_by_severity"][severity].append(issue_entry)
        result["top_issues"].append(issue_entry)

    # Sort top issues by count desc
    result["top_issues"].sort(key=lambda x: x["count"], reverse=True)
    result["top_issues"] = result["top_issues"][:15]

    # Recommendations
    if error > 0:
        result["recommendations"].append(
            f"{error} pages have indexing errors. Prioritize server errors and "
            f"'crawled but not indexed' pages -- these represent lost organic potential."
        )
    if result["index_ratio"] < 0.5 and total > 50:
        result["recommendations"].append(
            f"Only {result['index_ratio']*100:.0f}% of discovered URLs are indexed. "
            f"Review excluded URLs to identify accidental noindex/robots.txt blocks."
        )
    crawled_not_indexed = sum(
        i["count"] for i in issues if i.get("reason") == "crawled_not_indexed"
    )
    if crawled_not_indexed > 20:
        result["recommendations"].append(
            f"{crawled_not_indexed} pages are crawled but not indexed -- Google saw "
            f"them but chose not to include them. Improve content quality, add "
            f"internal links, and consolidate thin/duplicate pages."
        )

    return result


# ---------------------------------------------------------------------------
# Mobile Usability Analysis
# ---------------------------------------------------------------------------

def _analyze_mobile_usability(gsc_mobile: Optional[Dict]) -> Dict[str, Any]:
    """
    Analyse GSC mobile usability data.

    Expected input shape:
    {
        "summary": {"pages_with_issues": 45, "total_pages": 1200},
        "issues": [
            {"type": "text_too_small", "count": 30, "urls": [...]},
            ...
        ]
    }
    """
    result = {
        "mobile_friendly_pct": 100.0,
        "pages_with_issues": 0,
        "total_pages": 0,
        "issues": [],
        "recommendations": [],
    }

    if not gsc_mobile or not isinstance(gsc_mobile, dict):
        result["recommendations"].append(
            "No mobile usability data available. Ensure the site is verified "
            "in Google Search Console for mobile usability reports."
        )
        return result

    summary = gsc_mobile.get("summary", {})
    pages_with_issues = int(summary.get("pages_with_issues", 0))
    total_pages = int(summary.get("total_pages", 0))

    result["pages_with_issues"] = pages_with_issues
    result["total_pages"] = total_pages
    if total_pages > 0:
        result["mobile_friendly_pct"] = round(
            (1 - pages_with_issues / total_pages) * 100, 1
        )

    issues = gsc_mobile.get("issues", [])
    for issue in issues:
        issue_type = issue.get("type", "unknown")
        count = int(issue.get("count", 0))
        severity = MOBILE_SEVERITY.get(issue_type, "low")
        sample_urls = issue.get("urls", [])[:5]

        result["issues"].append({
            "type": issue_type,
            "count": count,
            "severity": severity,
            "sample_urls": sample_urls,
        })

    result["issues"].sort(
        key=lambda x: {"critical": 0, "high": 1, "medium": 2, "low": 3}.get(x["severity"], 4)
    )

    if pages_with_issues > 0:
        critical_issues = [i for i in result["issues"] if i["severity"] == "critical"]
        if critical_issues:
            result["recommendations"].append(
                f"Critical mobile issues found: {', '.join(i['type'] for i in critical_issues)}. "
                f"These may prevent Google from mobile-indexing affected pages."
            )
        if result["mobile_friendly_pct"] < 90:
            result["recommendations"].append(
                f"Only {result['mobile_friendly_pct']}% of pages are mobile-friendly. "
                f"With mobile-first indexing, this directly impacts rankings."
            )

    return result


# ---------------------------------------------------------------------------
# Crawl Error Analysis
# ---------------------------------------------------------------------------

def _analyze_crawl_errors(crawl_technical: Optional[Dict]) -> Dict[str, Any]:
    """
    Analyse crawl data for technical SEO issues.

    Expected input shape:
    {
        "pages": [
            {
                "url": "https://example.com/page",
                "status_code": 200,
                "redirect_chain": [],
                "canonical": "https://example.com/page",
                "meta_robots": "index,follow",
                "h1": ["Page Title"],
                "title": "Page Title | Site",
                "meta_description": "...",
                "internal_links_in": 5,
                "internal_links_out": 12,
                "external_links_out": 3,
                "load_time_ms": 1200,
                "content_length": 45000,
                "word_count": 800,
                "has_schema": true,
                "schema_types": ["Article", "BreadcrumbList"],
                "images_without_alt": 2,
                "broken_links": [],
                "mixed_content": false
            }
        ],
        "summary": {
            "total_pages": 150,
            "total_errors": 12
        }
    }
    """
    result = {
        "total_pages_crawled": 0,
        "status_code_distribution": {},
        "redirect_issues": [],
        "canonical_issues": [],
        "missing_meta": {"no_title": [], "no_description": [], "no_h1": []},
        "broken_links": [],
        "performance_issues": [],
        "schema_coverage": {"with_schema": 0, "without_schema": 0, "types": {}},
        "accessibility_issues": [],
        "recommendations": [],
    }

    if not crawl_technical or not isinstance(crawl_technical, dict):
        result["recommendations"].append(
            "No crawl data available. A technical site crawl is recommended "
            "to identify broken links, redirect chains, and meta tag issues."
        )
        return result

    pages = crawl_technical.get("pages", [])
    if not pages:
        result["recommendations"].append("Crawl returned zero pages.")
        return result

    result["total_pages_crawled"] = len(pages)
    status_counter: Counter = Counter()
    schema_type_counter: Counter = Counter()
    slow_pages = []
    redirect_chains = []
    canonical_mismatches = []
    broken = []
    no_title = []
    no_desc = []
    no_h1 = []
    images_no_alt = []

    for page in pages:
        url = page.get("url", "unknown")
        status = page.get("status_code", 0)
        status_counter[status] += 1

        # Redirect chains (>1 hop)
        chain = page.get("redirect_chain", [])
        if len(chain) > 1:
            redirect_chains.append({
                "url": url,
                "hops": len(chain),
                "chain": chain[:5],  # cap displayed chain
            })

        # Canonical mismatch
        canonical = page.get("canonical", "")
        if canonical and canonical != url and status == 200:
            canonical_mismatches.append({
                "url": url,
                "canonical": canonical,
            })

        # Missing meta
        title = page.get("title", "")
        if not title or len(title.strip()) == 0:
            no_title.append(url)
        desc = page.get("meta_description", "")
        if not desc or len(desc.strip()) == 0:
            no_desc.append(url)
        h1_list = page.get("h1", [])
        if not h1_list:
            no_h1.append(url)

        # Broken links
        page_broken = page.get("broken_links", [])
        for bl in page_broken:
            broken.append({"source": url, "target": bl})

        # Performance
        load_time = page.get("load_time_ms", 0)
        if load_time and load_time > 3000:
            slow_pages.append({"url": url, "load_time_ms": load_time})

        # Schema
        has_schema = page.get("has_schema", False)
        if has_schema:
            result["schema_coverage"]["with_schema"] += 1
            for st in page.get("schema_types", []):
                schema_type_counter[st] += 1
        else:
            result["schema_coverage"]["without_schema"] += 1

        # Images without alt
        img_no_alt = page.get("images_without_alt", 0)
        if img_no_alt and img_no_alt > 0:
            images_no_alt.append({"url": url, "count": img_no_alt})

    result["status_code_distribution"] = dict(status_counter.most_common())

    # Redirect issues -- sorted by hops desc
    redirect_chains.sort(key=lambda x: x["hops"], reverse=True)
    result["redirect_issues"] = redirect_chains[:20]

    # Canonical issues
    result["canonical_issues"] = canonical_mismatches[:20]

    # Missing meta
    result["missing_meta"]["no_title"] = no_title[:20]
    result["missing_meta"]["no_description"] = no_desc[:20]
    result["missing_meta"]["no_h1"] = no_h1[:20]

    # Broken links -- deduplicated
    seen_broken = set()
    unique_broken = []
    for bl in broken:
        key = (bl["source"], bl["target"])
        if key not in seen_broken:
            seen_broken.add(key)
            unique_broken.append(bl)
    result["broken_links"] = unique_broken[:30]

    # Performance issues
    slow_pages.sort(key=lambda x: x["load_time_ms"], reverse=True)
    result["performance_issues"] = slow_pages[:20]

    # Schema coverage
    result["schema_coverage"]["types"] = dict(schema_type_counter.most_common(10))

    # Accessibility issues (images without alt)
    images_no_alt.sort(key=lambda x: x["count"], reverse=True)
    result["accessibility_issues"] = images_no_alt[:20]

    # --- Recommendations ---
    error_pages = sum(v for k, v in status_counter.items() if k >= 400)
    if error_pages > 0:
        result["recommendations"].append(
            f"{error_pages} pages returned 4xx/5xx status codes. Fix or redirect "
            f"broken URLs to preserve link equity and user experience."
        )

    if redirect_chains:
        long_chains = [r for r in redirect_chains if r["hops"] > 2]
        if long_chains:
            result["recommendations"].append(
                f"{len(long_chains)} URLs have redirect chains of 3+ hops. "
                f"Shorten these to single redirects to reduce crawl budget waste."
            )

    if len(no_title) > 5:
        result["recommendations"].append(
            f"{len(no_title)} pages are missing title tags. Add unique, keyword-rich "
            f"titles to improve rankings and click-through rates."
        )

    if len(no_desc) > 10:
        result["recommendations"].append(
            f"{len(no_desc)} pages are missing meta descriptions. While not a direct "
            f"ranking factor, descriptions improve CTR from search results."
        )

    if len(unique_broken) > 5:
        result["recommendations"].append(
            f"{len(unique_broken)} broken internal links found. Fix these to improve "
            f"crawlability and prevent users from hitting dead ends."
        )

    schema_pct = (
        result["schema_coverage"]["with_schema"] / len(pages) * 100 if pages else 0
    )
    if schema_pct < 30:
        result["recommendations"].append(
            f"Only {schema_pct:.0f}% of pages have structured data. Add Schema.org "
            f"markup (Article, FAQ, Product, etc.) to improve rich snippet eligibility."
        )

    if slow_pages:
        result["recommendations"].append(
            f"{len(slow_pages)} pages load in over 3 seconds. Optimize server "
            f"response, reduce JS/CSS payload, and enable compression."
        )

    return result


# ---------------------------------------------------------------------------
# Technical Debt Score
# ---------------------------------------------------------------------------

def _compute_technical_debt_score(
    cwv_result: Dict,
    indexing_result: Dict,
    mobile_result: Dict,
    crawl_result: Dict,
) -> Dict[str, Any]:
    """
    Compute a composite Technical Health Score (0-100) across four dimensions.

    Weights:
    - Core Web Vitals: 30 points
    - Indexing Coverage:  25 points
    - Mobile Usability:  20 points
    - Crawl Health:      25 points
    """
    scores = {}

    # --- CWV (30 pts) ---
    cwv_raw = cwv_result.get("overall_score", 50.0)
    scores["core_web_vitals"] = {
        "score": round(cwv_raw * 0.30, 1),
        "max": 30,
        "raw": round(cwv_raw, 1),
        "pass": cwv_result.get("pass", False),
    }

    # --- Indexing (25 pts) ---
    index_ratio = indexing_result.get("index_ratio", 0.5)
    critical_issues = len(indexing_result.get("issues_by_severity", {}).get("critical", []))
    high_issues = len(indexing_result.get("issues_by_severity", {}).get("high", []))
    # Deduct for issues
    index_raw = index_ratio * 100
    index_raw -= critical_issues * 15
    index_raw -= high_issues * 5
    index_raw = max(0, min(100, index_raw))
    scores["indexing_coverage"] = {
        "score": round(index_raw * 0.25, 1),
        "max": 25,
        "raw": round(index_raw, 1),
        "index_ratio": index_ratio,
    }

    # --- Mobile (20 pts) ---
    mobile_pct = mobile_result.get("mobile_friendly_pct", 100.0)
    mobile_raw = mobile_pct  # Already 0-100
    critical_mobile = sum(
        1 for i in mobile_result.get("issues", []) if i.get("severity") == "critical"
    )
    mobile_raw -= critical_mobile * 20
    mobile_raw = max(0, min(100, mobile_raw))
    scores["mobile_usability"] = {
        "score": round(mobile_raw * 0.20, 1),
        "max": 20,
        "raw": round(mobile_raw, 1),
        "mobile_friendly_pct": mobile_pct,
    }

    # --- Crawl Health (25 pts) ---
    total_crawled = crawl_result.get("total_pages_crawled", 0)
    if total_crawled > 0:
        status_dist = crawl_result.get("status_code_distribution", {})
        error_pages = sum(v for k, v in status_dist.items() if int(k) >= 400)
        error_rate = error_pages / total_crawled
        broken_count = len(crawl_result.get("broken_links", []))
        redirect_count = len(crawl_result.get("redirect_issues", []))
        missing_title = len(crawl_result.get("missing_meta", {}).get("no_title", []))

        crawl_raw = 100.0
        crawl_raw -= error_rate * 200  # Heavy penalty for errors
        crawl_raw -= min(30, broken_count * 2)
        crawl_raw -= min(15, redirect_count * 3)
        crawl_raw -= min(15, missing_title * 1.5)
        crawl_raw = max(0, min(100, crawl_raw))
    else:
        crawl_raw = 50.0  # No crawl data -- neutral

    scores["crawl_health"] = {
        "score": round(crawl_raw * 0.25, 1),
        "max": 25,
        "raw": round(crawl_raw, 1),
    }

    # --- Composite ---
    total_score = sum(s["score"] for s in scores.values())
    grade = _score_to_grade(total_score)

    return {
        "total_score": round(total_score, 1),
        "max_score": 100,
        "grade": grade,
        "dimensions": scores,
    }


def _score_to_grade(score: float) -> str:
    """Convert numeric score to letter grade."""
    if score >= 90:
        return "A"
    elif score >= 80:
        return "B"
    elif score >= 65:
        return "C"
    elif score >= 50:
        return "D"
    else:
        return "F"


# ---------------------------------------------------------------------------
# Main Entry Point
# ---------------------------------------------------------------------------

def analyze_technical_health(
    ga4_cwv_data=None,
    gsc_coverage=None,
    gsc_mobile=None,
    crawl_technical=None,
) -> Dict[str, Any]:
    """
    Module 8: Technical Health -- full analysis.

    Parameters
    ----------
    ga4_cwv_data : dict or None
        Core Web Vitals data from GA4 / CrUX.
    gsc_coverage : dict or None
        Index coverage data from Google Search Console.
    gsc_mobile : dict or None
        Mobile usability data from Google Search Console.
    crawl_technical : dict or None
        Crawl results from site crawler.

    Returns
    -------
    dict
        Comprehensive technical health report with scores and recommendations.
    """
    logger.info("Running Module 8: Technical Health (full implementation)")

    # 1. Core Web Vitals
    cwv_result = _analyze_core_web_vitals(ga4_cwv_data)

    # 2. Indexing coverage
    indexing_result = _analyze_indexing_coverage(gsc_coverage)

    # 3. Mobile usability
    mobile_result = _analyze_mobile_usability(gsc_mobile)

    # 4. Crawl errors
    crawl_result = _analyze_crawl_errors(crawl_technical)

    # 5. Technical debt score
    debt_score = _compute_technical_debt_score(
        cwv_result, indexing_result, mobile_result, crawl_result
    )

    # 6. Aggregate all recommendations
    all_recommendations = []
    all_recommendations.extend(cwv_result.get("recommendations", []))
    all_recommendations.extend(indexing_result.get("recommendations", []))
    all_recommendations.extend(mobile_result.get("recommendations", []))
    all_recommendations.extend(crawl_result.get("recommendations", []))

    summary_parts = []
    summary_parts.append(
        f"Technical Health Score: {debt_score['total_score']}/100 (Grade: {debt_score['grade']})"
    )
    if cwv_result.get("pass"):
        summary_parts.append("Core Web Vitals: PASSING")
    elif cwv_result.get("overall_score", 0) > 0:
        summary_parts.append(
            f"Core Web Vitals: FAILING (score {cwv_result['overall_score']}/100)"
        )
    summary_parts.append(
        f"Indexing: {indexing_result['summary']['valid']} of "
        f"{indexing_result['summary']['total']} URLs indexed "
        f"({indexing_result['index_ratio']*100:.0f}%)"
    )
    if mobile_result.get("pages_with_issues", 0) > 0:
        summary_parts.append(
            f"Mobile: {mobile_result['pages_with_issues']} pages with issues"
        )
    else:
        summary_parts.append("Mobile: No issues detected")
    summary_parts.append(
        f"Crawl: {crawl_result['total_pages_crawled']} pages analysed, "
        f"{len(crawl_result.get('broken_links', []))} broken links"
    )

    return {
        "summary": " | ".join(summary_parts),
        "technical_score": debt_score,
        "core_web_vitals": cwv_result,
        "indexing_coverage": indexing_result,
        "mobile_usability": mobile_result,
        "crawl_health": crawl_result,
        "all_recommendations": all_recommendations,
        "priority_fixes": all_recommendations[:5],
    }
