"""
Consulting CTA (Call-to-Action) service for Search Intelligence Reports.

Generates contextual, data-driven consulting CTAs that appear throughout
reports — in the executive summary, per-module sections, PDF exports,
and email deliveries. Each CTA is personalised based on the actual
findings so prospects see immediate relevance.

Target: clankermarketing.com/contact
"""

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CONTACT_URL = "https://clankermarketing.com/contact"
BOOKING_URL = "https://clankermarketing.com/book"
AUDIT_URL = "https://clankermarketing.com/free-audit"

CTA_STYLES = {
    "banner": "full-width banner with accent background",
    "inline": "subtle in-line text link",
    "card": "highlighted card with icon and button",
    "sidebar": "persistent sidebar widget",
    "modal": "exit-intent or scroll-triggered overlay",
}

# Module-to-service mapping: which consulting service each module upsells
MODULE_SERVICE_MAP = {
    1: "search_health_audit",
    2: "page_triage_consultation",
    3: "competitive_strategy_session",
    4: "content_strategy_workshop",
    5: "strategic_gameplan_session",
    6: "algorithm_recovery_plan",
    7: "intent_migration_strategy",
    8: "technical_seo_audit",
    9: "site_architecture_review",
    10: "brand_strategy_session",
    11: "competitive_intelligence_retainer",
    12: "revenue_attribution_setup",
}

SERVICE_DETAILS = {
    "search_health_audit": {
        "name": "Search Health Audit",
        "description": "Deep-dive analysis of your search visibility trajectory with actionable recovery or growth plan.",
        "duration": "60-minute session",
        "price_hint": "from $497",
    },
    "page_triage_consultation": {
        "name": "Page Triage Consultation",
        "description": "Expert prioritisation of which pages to fix, consolidate, or remove for maximum impact.",
        "duration": "90-minute workshop",
        "price_hint": "from $697",
    },
    "competitive_strategy_session": {
        "name": "Competitive Strategy Session",
        "description": "Detailed competitor gap analysis with a custom playbook to capture market share.",
        "duration": "90-minute session",
        "price_hint": "from $797",
    },
    "content_strategy_workshop": {
        "name": "Content Strategy Workshop",
        "description": "Data-driven content roadmap aligned with search intent and conversion potential.",
        "duration": "Half-day workshop",
        "price_hint": "from $1,497",
    },
    "strategic_gameplan_session": {
        "name": "Strategic Gameplan Session",
        "description": "Comprehensive 90-day action plan combining quick wins and long-term growth initiatives.",
        "duration": "2-hour session",
        "price_hint": "from $997",
    },
    "algorithm_recovery_plan": {
        "name": "Algorithm Recovery Plan",
        "description": "Expert diagnosis of algorithm impact with step-by-step recovery roadmap.",
        "duration": "60-minute session",
        "price_hint": "from $597",
    },
    "intent_migration_strategy": {
        "name": "Intent Migration Strategy",
        "description": "Plan to realign content with evolving search intent patterns and protect rankings.",
        "duration": "90-minute session",
        "price_hint": "from $697",
    },
    "technical_seo_audit": {
        "name": "Technical SEO Audit",
        "description": "Comprehensive crawl analysis with prioritised fix list for indexing and performance issues.",
        "duration": "Full audit + report",
        "price_hint": "from $1,997",
    },
    "site_architecture_review": {
        "name": "Site Architecture Review",
        "description": "Internal linking and information architecture overhaul for optimal crawl efficiency.",
        "duration": "Half-day workshop",
        "price_hint": "from $1,497",
    },
    "brand_strategy_session": {
        "name": "Brand vs Non-Brand Strategy",
        "description": "Reduce brand dependency and unlock non-branded organic growth opportunities.",
        "duration": "90-minute session",
        "price_hint": "from $697",
    },
    "competitive_intelligence_retainer": {
        "name": "Competitive Intelligence Retainer",
        "description": "Ongoing monitoring of competitor SERP movements with monthly strategy adjustments.",
        "duration": "Monthly retainer",
        "price_hint": "from $1,497/mo",
    },
    "revenue_attribution_setup": {
        "name": "Revenue Attribution Setup",
        "description": "Connect search performance to revenue with custom attribution modelling and ROI tracking.",
        "duration": "Implementation + training",
        "price_hint": "from $2,497",
    },
}


# ---------------------------------------------------------------------------
# CTA generation helpers
# ---------------------------------------------------------------------------


def _severity_label(score: float, thresholds: tuple = (40, 70)) -> str:
    """Classify a 0-100 score into severity."""
    low, high = thresholds
    if score < low:
        return "critical"
    if score < high:
        return "needs_attention"
    return "healthy"


def _extract_metric(results: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    """Safely traverse nested dicts."""
    current = results
    for k in keys:
        if isinstance(current, dict):
            current = current.get(k, default)
        else:
            return default
    return current


def _format_number(n: Any) -> str:
    """Format a number for display."""
    if n is None:
        return "N/A"
    try:
        n = float(n)
        if n >= 1_000_000:
            return f"{n / 1_000_000:.1f}M"
        if n >= 1_000:
            return f"{n / 1_000:.1f}K"
        if n == int(n):
            return str(int(n))
        return f"{n:.1f}"
    except (TypeError, ValueError):
        return str(n)


# ---------------------------------------------------------------------------
# Per-module CTA generators
# ---------------------------------------------------------------------------


def _cta_module_1(results: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Health Trajectory CTA — triggered by declining trends."""
    summary = _extract_metric(results, "summary", default={})
    if isinstance(summary, str):
        # summary is a text narrative
        declining = "declin" in summary.lower() or "drop" in summary.lower()
    else:
        trend = _extract_metric(summary, "trend_direction", default="stable")
        declining = trend in ("declining", "decreasing", "negative")

    if not declining:
        return None

    return {
        "module": 1,
        "trigger": "declining_health_trajectory",
        "urgency": "high",
        "headline": "Your search visibility is declining — let's reverse it.",
        "body": (
            "Our analysis detected a downward trend in your search health metrics. "
            "Left unchecked, this compounds quickly. A focused Search Health Audit "
            "identifies the root causes and builds a prioritised recovery plan."
        ),
        "cta_text": "Book a Search Health Audit",
        "cta_url": BOOKING_URL + "?service=search_health_audit&ref=module_1",
        "service": SERVICE_DETAILS["search_health_audit"],
    }


def _cta_module_2(results: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Page Triage CTA — triggered by many underperforming pages."""
    pages = _extract_metric(results, "underperforming_pages", default=[])
    if not isinstance(pages, list):
        pages = _extract_metric(results, "pages_to_fix", default=[])
    count = len(pages) if isinstance(pages, list) else 0

    if count < 10:
        return None

    return {
        "module": 2,
        "trigger": f"{count}_underperforming_pages",
        "urgency": "high" if count > 50 else "medium",
        "headline": f"{_format_number(count)} pages need attention — we can help prioritise.",
        "body": (
            f"We found {_format_number(count)} pages that are underperforming relative "
            "to their potential. A Page Triage Consultation helps you decide what to "
            "fix, consolidate, or prune — saving months of trial and error."
        ),
        "cta_text": "Get Expert Page Triage",
        "cta_url": BOOKING_URL + "?service=page_triage&ref=module_2",
        "service": SERVICE_DETAILS["page_triage_consultation"],
    }


def _cta_module_3(results: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """SERP Landscape / Competitive CTA — triggered by strong competitors."""
    competitors = _extract_metric(results, "competitors", default=[])
    if not isinstance(competitors, list):
        competitors = _extract_metric(results, "competitor_domains", default=[])

    count = len(competitors) if isinstance(competitors, list) else 0
    if count < 3:
        return None

    return {
        "module": 3,
        "trigger": f"{count}_competitors_detected",
        "urgency": "medium",
        "headline": f"You're competing against {count}+ domains — do you have a plan?",
        "body": (
            "The SERP landscape analysis shows significant competition for your target "
            "keywords. A Competitive Strategy Session maps exactly where you can win "
            "and where to avoid wasting resources."
        ),
        "cta_text": "Book a Competitive Strategy Session",
        "cta_url": BOOKING_URL + "?service=competitive_strategy&ref=module_3",
        "service": SERVICE_DETAILS["competitive_strategy_session"],
    }


def _cta_module_4(results: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Content Intelligence CTA — triggered by content gaps."""
    gaps = _extract_metric(results, "content_gaps", default=[])
    if not isinstance(gaps, list):
        gaps = _extract_metric(results, "opportunities", default=[])

    count = len(gaps) if isinstance(gaps, list) else 0
    if count < 5:
        return None

    return {
        "module": 4,
        "trigger": f"{count}_content_gaps",
        "urgency": "medium",
        "headline": f"{_format_number(count)} content gaps found — turn them into traffic.",
        "body": (
            "There are clear content opportunities your competitors are capitalising on. "
            "Our Content Strategy Workshop builds a prioritised editorial calendar "
            "backed by real search data."
        ),
        "cta_text": "Plan Your Content Strategy",
        "cta_url": BOOKING_URL + "?service=content_strategy&ref=module_4",
        "service": SERVICE_DETAILS["content_strategy_workshop"],
    }


def _cta_module_5(results: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Gameplan CTA — always relevant for completed reports."""
    recommendations = _extract_metric(results, "recommendations", default=[])
    count = len(recommendations) if isinstance(recommendations, list) else 0

    if count < 3:
        return None

    return {
        "module": 5,
        "trigger": f"{count}_recommendations_generated",
        "urgency": "medium",
        "headline": "Ready to execute? Let us build your 90-day gameplan.",
        "body": (
            f"This report generated {count} recommendations. A Strategic Gameplan Session "
            "turns those into a sequenced, resourced action plan with clear milestones "
            "and expected outcomes."
        ),
        "cta_text": "Get Your Custom Gameplan",
        "cta_url": BOOKING_URL + "?service=strategic_gameplan&ref=module_5",
        "service": SERVICE_DETAILS["strategic_gameplan_session"],
    }


def _cta_module_8(results: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Technical Health / CTR CTA — triggered by technical issues."""
    issues = _extract_metric(results, "issues", default=[])
    if not isinstance(issues, list):
        issues = _extract_metric(results, "technical_issues", default=[])

    count = len(issues) if isinstance(issues, list) else 0
    if count < 5:
        return None

    return {
        "module": 8,
        "trigger": f"{count}_technical_issues",
        "urgency": "high" if count > 20 else "medium",
        "headline": f"{_format_number(count)} technical issues are holding you back.",
        "body": (
            "Technical SEO issues silently erode your rankings. Our Technical SEO Audit "
            "crawls your entire site, prioritises every issue by impact, and delivers "
            "a developer-ready fix list."
        ),
        "cta_text": "Request a Technical Audit",
        "cta_url": BOOKING_URL + "?service=technical_seo&ref=module_8",
        "service": SERVICE_DETAILS["technical_seo_audit"],
    }


def _cta_module_9(results: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Site Architecture CTA — triggered by orphan pages or deep pages."""
    orphans = _extract_metric(results, "orphan_pages", default=[])
    orphan_count = len(orphans) if isinstance(orphans, list) else 0

    bottlenecks = _extract_metric(results, "equity_bottlenecks", default=[])
    bottleneck_count = len(bottlenecks) if isinstance(bottlenecks, list) else 0

    if orphan_count < 5 and bottleneck_count < 3:
        return None

    return {
        "module": 9,
        "trigger": f"{orphan_count}_orphans_{bottleneck_count}_bottlenecks",
        "urgency": "high" if orphan_count > 20 else "medium",
        "headline": "Your site architecture has hidden problems.",
        "body": (
            f"We found {_format_number(orphan_count)} orphan pages and "
            f"{_format_number(bottleneck_count)} link equity bottlenecks. "
            "A Site Architecture Review redesigns your internal linking for "
            "maximum crawl efficiency and ranking power."
        ),
        "cta_text": "Fix Your Site Architecture",
        "cta_url": BOOKING_URL + "?service=architecture_review&ref=module_9",
        "service": SERVICE_DETAILS["site_architecture_review"],
    }


def _cta_module_10(results: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Branded Split CTA — triggered by high brand dependency."""
    dep_score = _extract_metric(results, "brand_dependency", "dependency_score", default=0)
    if not isinstance(dep_score, (int, float)):
        try:
            dep_score = float(dep_score)
        except (TypeError, ValueError):
            dep_score = 0

    if dep_score < 60:
        return None

    return {
        "module": 10,
        "trigger": f"brand_dependency_{int(dep_score)}",
        "urgency": "high" if dep_score >= 80 else "medium",
        "headline": f"Brand dependency score: {int(dep_score)}/100 — you're exposed.",
        "body": (
            "Heavy reliance on branded searches means you're missing non-branded "
            "growth. Our Brand Strategy Session builds a diversification plan to "
            "reduce risk and unlock new traffic sources."
        ),
        "cta_text": "Diversify Your Traffic",
        "cta_url": BOOKING_URL + "?service=brand_strategy&ref=module_10",
        "service": SERVICE_DETAILS["brand_strategy_session"],
    }


def _cta_module_12(results: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Revenue Attribution CTA — triggered by revenue at risk."""
    at_risk = _extract_metric(results, "revenue_at_risk", default=[])
    if isinstance(at_risk, list):
        total_risk = sum(
            r.get("estimated_revenue", 0) for r in at_risk if isinstance(r, dict)
        )
    else:
        total_risk = 0

    if total_risk < 1000:
        return None

    return {
        "module": 12,
        "trigger": f"revenue_at_risk_{_format_number(total_risk)}",
        "urgency": "critical",
        "headline": f"${_format_number(total_risk)} in revenue is at risk.",
        "body": (
            "Our analysis identified significant revenue tied to at-risk pages. "
            "A Revenue Attribution Setup connects your search data to actual dollars "
            "so you can prioritise what protects the bottom line."
        ),
        "cta_text": "Protect Your Revenue",
        "cta_url": BOOKING_URL + "?service=revenue_attribution&ref=module_12",
        "service": SERVICE_DETAILS["revenue_attribution_setup"],
    }


# Map module numbers to their CTA generators
_MODULE_CTA_GENERATORS = {
    1: _cta_module_1,
    2: _cta_module_2,
    3: _cta_module_3,
    4: _cta_module_4,
    5: _cta_module_5,
    8: _cta_module_8,
    9: _cta_module_9,
    10: _cta_module_10,
    12: _cta_module_12,
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def generate_module_cta(
    module_number: int,
    module_results: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """
    Generate a consulting CTA for a single module based on its results.

    Returns None if the module's findings don't warrant a CTA.
    """
    generator = _MODULE_CTA_GENERATORS.get(module_number)
    if generator is None:
        return None

    try:
        return generator(module_results)
    except Exception as e:
        logger.warning(f"CTA generation failed for module {module_number}: {e}")
        return None


def generate_report_ctas(
    module_results: Dict[int, Dict[str, Any]],
    max_ctas: int = 5,
) -> Dict[str, Any]:
    """
    Generate all consulting CTAs for a completed report.

    Scans every module's results, generates contextual CTAs, ranks them
    by urgency, and returns the top ``max_ctas``.

    Returns a dict with:
      - ctas: list of CTA dicts (sorted by urgency)
      - executive_cta: the single most important CTA for the exec summary
      - total_generated: how many CTAs were triggered before capping
      - contact_url: primary contact URL
    """
    urgency_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    all_ctas: List[Dict[str, Any]] = []

    for mod_num, results in sorted(module_results.items()):
        cta = generate_module_cta(mod_num, results)
        if cta is not None:
            all_ctas.append(cta)

    # Sort by urgency
    all_ctas.sort(key=lambda c: urgency_order.get(c.get("urgency", "low"), 99))

    total_generated = len(all_ctas)
    top_ctas = all_ctas[:max_ctas]

    # Executive CTA: highest urgency, or fallback generic
    executive_cta = top_ctas[0] if top_ctas else {
        "module": None,
        "trigger": "report_completed",
        "urgency": "low",
        "headline": "Want expert help implementing these insights?",
        "body": (
            "This report is packed with opportunities. Our search consultants "
            "can help you prioritise and execute — turning data into results."
        ),
        "cta_text": "Talk to a Search Consultant",
        "cta_url": CONTACT_URL + "?ref=executive_summary",
        "service": None,
    }

    return {
        "ctas": top_ctas,
        "executive_cta": executive_cta,
        "total_generated": total_generated,
        "contact_url": CONTACT_URL,
        "booking_url": BOOKING_URL,
        "audit_url": AUDIT_URL,
    }


def generate_pdf_ctas(
    module_results: Dict[int, Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Generate CTAs specifically formatted for PDF report insertion.

    Returns structured data that the PDF export service can render
    as branded CTA blocks within the document.
    """
    cta_data = generate_report_ctas(module_results, max_ctas=3)

    # Add PDF-specific formatting hints
    pdf_ctas = []
    for cta in cta_data["ctas"]:
        pdf_ctas.append({
            **cta,
            "placement": "after_module",
            "style": "card",
            "background_color": "#1a1a2e",
            "accent_color": "#e94560",
            "text_color": "#ffffff",
        })

    # Closing page CTA
    closing_cta = {
        "headline": "Ready to Turn Insights Into Growth?",
        "body": (
            "This Search Intelligence Report revealed real opportunities "
            "for your business. Our team of search consultants has helped "
            "companies like yours achieve 3-5x organic traffic growth. "
            "Let's discuss your specific situation."
        ),
        "cta_text": "Schedule a Free Strategy Call",
        "cta_url": AUDIT_URL + "?ref=pdf_closing",
        "secondary_cta_text": "Email Us",
        "secondary_cta_url": "mailto:hello@clankermarketing.com?subject=Search%20Intelligence%20Report",
        "style": "full_page",
        "background_color": "#1a1a2e",
        "accent_color": "#e94560",
        "text_color": "#ffffff",
    }

    return {
        "module_ctas": pdf_ctas,
        "closing_cta": closing_cta,
        "contact_url": CONTACT_URL,
        "booking_url": BOOKING_URL,
    }


def generate_email_ctas(
    module_results: Dict[int, Dict[str, Any]],
    domain: str = "",
) -> Dict[str, Any]:
    """
    Generate CTAs specifically formatted for email delivery.

    Returns structured data with HTML-safe content for email templates.
    """
    cta_data = generate_report_ctas(module_results, max_ctas=2)

    # Email header CTA
    header_cta = {
        "text": "View your full report and book a free strategy call",
        "url": AUDIT_URL + f"?ref=email_header&domain={domain}",
    }

    # Email footer CTA
    footer_cta = {
        "headline": "Want help implementing these recommendations?",
        "body": (
            "Our search consultants specialise in turning data into growth. "
            "Book a free 30-minute strategy call to discuss your report findings."
        ),
        "cta_text": "Book Your Free Call",
        "cta_url": BOOKING_URL + f"?ref=email_footer&domain={domain}",
        "secondary_text": "Or reply to this email to start a conversation.",
    }

    # Inline CTAs (placed between module summaries in email body)
    inline_ctas = []
    for cta in cta_data["ctas"]:
        inline_ctas.append({
            "module": cta.get("module"),
            "text": cta.get("headline", ""),
            "link_text": cta.get("cta_text", "Learn more"),
            "url": cta.get("cta_url", CONTACT_URL),
        })

    return {
        "header_cta": header_cta,
        "footer_cta": footer_cta,
        "inline_ctas": inline_ctas,
        "contact_url": CONTACT_URL,
    }


def get_available_services() -> List[Dict[str, Any]]:
    """
    Return the full catalogue of consulting services for display.

    Used by the frontend to render a services page or consulting menu.
    """
    services = []
    for key, details in SERVICE_DETAILS.items():
        services.append({
            "id": key,
            "name": details["name"],
            "description": details["description"],
            "duration": details["duration"],
            "price_hint": details["price_hint"],
            "booking_url": BOOKING_URL + f"?service={key}",
            "modules": [
                mod for mod, svc in MODULE_SERVICE_MAP.items() if svc == key
            ],
        })
    return services
