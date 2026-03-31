"""
PDF Report Export Service for Search Intelligence Reports.

Generates a professional, multi-page PDF summarising all 12 module results
for a given report.  Uses reportlab for PDF generation.

Usage:
    from api.services.pdf_export import generate_pdf_report
    pdf_bytes = generate_pdf_report(report_data, module_results)
"""

from __future__ import annotations

import io
import logging
import textwrap
from datetime import datetime
from typing import Any, Dict, List, Optional

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import LETTER
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (
    BaseDocTemplate,
    Frame,
    Image,
    NextPageTemplate,
    PageBreak,
    PageTemplate,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Colour palette
# ---------------------------------------------------------------------------
BRAND_PRIMARY = colors.HexColor("#1a56db")
BRAND_DARK = colors.HexColor("#1e293b")
BRAND_LIGHT = colors.HexColor("#f1f5f9")
BRAND_ACCENT = colors.HexColor("#059669")
BRAND_WARNING = colors.HexColor("#d97706")
BRAND_DANGER = colors.HexColor("#dc2626")
BRAND_GRAY = colors.HexColor("#64748b")

# ---------------------------------------------------------------------------
# Module metadata
# ---------------------------------------------------------------------------
MODULE_TITLES: Dict[int, str] = {
    1: "Health & Trajectory",
    2: "Page Triage",
    3: "SERP Landscape",
    4: "Content Intelligence",
    5: "Game Plan",
    6: "Algorithm Updates",
    7: "Intent Migration",
    8: "Technical Health",
    9: "Site Architecture",
    10: "Branded vs Non-Branded",
    11: "Competitive Threats",
    12: "Revenue Attribution",
}

MODULE_DESCRIPTIONS: Dict[int, str] = {
    1: "Trend decomposition, change-point detection, anomaly identification, and forward projections.",
    2: "Page-level performance triage — winners, decliners, and opportunities requiring action.",
    3: "SERP feature landscape and competitive positioning analysis.",
    4: "Content gap analysis, thin-content detection, and topical authority mapping.",
    5: "Prioritised action plan synthesising insights from all other modules.",
    6: "Correlation of performance shifts with known Google algorithm updates.",
    7: "Search-intent classification changes and content-alignment audit.",
    8: "Technical SEO health including CTR benchmarking and indexing issues.",
    9: "Internal link graph analysis, orphan pages, crawl depth, and PageRank distribution.",
    10: "Brand dependency analysis — branded vs non-branded traffic split and risk.",
    11: "Competitive threat identification and market positioning.",
    12: "Revenue attribution to pages and queries, ROI modelling, and at-risk revenue.",
}


# ---------------------------------------------------------------------------
# Style helpers
# ---------------------------------------------------------------------------

def _build_styles() -> Dict[str, ParagraphStyle]:
    """Return a dictionary of custom ParagraphStyles."""
    base = getSampleStyleSheet()
    styles: Dict[str, ParagraphStyle] = {}

    styles["cover_title"] = ParagraphStyle(
        "cover_title",
        parent=base["Title"],
        fontSize=28,
        leading=34,
        textColor=colors.white,
        alignment=TA_CENTER,
        spaceAfter=12,
    )
    styles["cover_subtitle"] = ParagraphStyle(
        "cover_subtitle",
        parent=base["Normal"],
        fontSize=14,
        leading=18,
        textColor=colors.HexColor("#cbd5e1"),
        alignment=TA_CENTER,
        spaceAfter=6,
    )
    styles["section_heading"] = ParagraphStyle(
        "section_heading",
        parent=base["Heading1"],
        fontSize=18,
        leading=22,
        textColor=BRAND_PRIMARY,
        spaceBefore=18,
        spaceAfter=8,
        borderWidth=0,
        borderPadding=0,
    )
    styles["module_heading"] = ParagraphStyle(
        "module_heading",
        parent=base["Heading2"],
        fontSize=14,
        leading=18,
        textColor=BRAND_DARK,
        spaceBefore=14,
        spaceAfter=6,
    )
    styles["body"] = ParagraphStyle(
        "body",
        parent=base["Normal"],
        fontSize=10,
        leading=14,
        textColor=BRAND_DARK,
        spaceAfter=6,
    )
    styles["body_small"] = ParagraphStyle(
        "body_small",
        parent=base["Normal"],
        fontSize=8,
        leading=11,
        textColor=BRAND_GRAY,
        spaceAfter=4,
    )
    styles["metric_label"] = ParagraphStyle(
        "metric_label",
        parent=base["Normal"],
        fontSize=8,
        leading=10,
        textColor=BRAND_GRAY,
        alignment=TA_CENTER,
    )
    styles["metric_value"] = ParagraphStyle(
        "metric_value",
        parent=base["Normal"],
        fontSize=16,
        leading=20,
        textColor=BRAND_DARK,
        alignment=TA_CENTER,
        fontName="Helvetica-Bold",
    )
    styles["recommendation"] = ParagraphStyle(
        "recommendation",
        parent=base["Normal"],
        fontSize=9,
        leading=13,
        textColor=BRAND_DARK,
        leftIndent=12,
        spaceAfter=4,
    )
    styles["footer"] = ParagraphStyle(
        "footer",
        parent=base["Normal"],
        fontSize=7,
        leading=9,
        textColor=BRAND_GRAY,
        alignment=TA_CENTER,
    )
    return styles


# ---------------------------------------------------------------------------
# Page decorators
# ---------------------------------------------------------------------------

def _cover_page(canvas, doc):
    """Draw cover page background."""
    canvas.saveState()
    w, h = LETTER
    # Dark gradient background
    canvas.setFillColor(BRAND_DARK)
    canvas.rect(0, 0, w, h, fill=1, stroke=0)
    # Accent stripe
    canvas.setFillColor(BRAND_PRIMARY)
    canvas.rect(0, h * 0.35, w, 4, fill=1, stroke=0)
    canvas.restoreState()


def _normal_page(canvas, doc):
    """Draw header/footer on normal pages."""
    canvas.saveState()
    w, h = LETTER
    # Top line
    canvas.setStrokeColor(BRAND_PRIMARY)
    canvas.setLineWidth(2)
    canvas.line(0.75 * inch, h - 0.5 * inch, w - 0.75 * inch, h - 0.5 * inch)
    # Footer
    canvas.setFont("Helvetica", 7)
    canvas.setFillColor(BRAND_GRAY)
    canvas.drawCentredString(w / 2, 0.4 * inch, f"Search Intelligence Report  |  Page {doc.page}")
    canvas.drawRightString(
        w - 0.75 * inch,
        0.4 * inch,
        f"Generated {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}",
    )
    canvas.restoreState()


# ---------------------------------------------------------------------------
# Content builders
# ---------------------------------------------------------------------------

def _safe_str(value: Any) -> str:
    """Safely convert any value to a display string."""
    if value is None:
        return "N/A"
    if isinstance(value, float):
        if abs(value) >= 1_000_000:
            return f"{value:,.0f}"
        if abs(value) >= 100:
            return f"{value:,.1f}"
        return f"{value:.2f}"
    return str(value)


def _build_metric_card(label: str, value: str, styles: Dict) -> Table:
    """Create a small metric card as a Table."""
    data = [
        [Paragraph(value, styles["metric_value"])],
        [Paragraph(label, styles["metric_label"])],
    ]
    t = Table(data, colWidths=[1.8 * inch])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), BRAND_LIGHT),
        ("BOX", (0, 0), (-1, -1), 0.5, BRAND_PRIMARY),
        ("TOPPADDING", (0, 0), (-1, 0), 8),
        ("BOTTOMPADDING", (0, -1), (-1, -1), 8),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]))
    return t


def _build_summary_section(
    module_num: int,
    result: Dict[str, Any],
    styles: Dict[str, ParagraphStyle],
) -> List:
    """Build flowable elements for one module's summary."""
    elements: List = []
    title = MODULE_TITLES.get(module_num, f"Module {module_num}")
    desc = MODULE_DESCRIPTIONS.get(module_num, "")

    elements.append(Paragraph(f"Module {module_num}: {title}", styles["module_heading"]))
    if desc:
        elements.append(Paragraph(desc, styles["body_small"]))

    # Extract summary text
    summary_text = None
    if isinstance(result, dict):
        summary_text = result.get("summary") or result.get("executive_summary") or result.get("narrative")
        if isinstance(summary_text, dict):
            summary_text = summary_text.get("narrative") or summary_text.get("text") or str(summary_text)

    if summary_text and isinstance(summary_text, str):
        # Truncate very long summaries for the PDF
        if len(summary_text) > 1500:
            summary_text = summary_text[:1497] + "..."
        # Escape XML-sensitive chars for reportlab
        safe_text = (
            summary_text
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )
        elements.append(Paragraph(safe_text, styles["body"]))

    # Extract key metrics if present
    metrics = result.get("metrics") or result.get("key_metrics") if isinstance(result, dict) else None
    if isinstance(metrics, dict) and metrics:
        cards = []
        for k, v in list(metrics.items())[:4]:
            label = k.replace("_", " ").title()
            cards.append(_build_metric_card(label, _safe_str(v), styles))
        if cards:
            row = Table([cards], colWidths=[2.0 * inch] * len(cards))
            row.setStyle(TableStyle([
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 2),
                ("RIGHTPADDING", (0, 0), (-1, -1), 2),
            ]))
            elements.append(Spacer(1, 6))
            elements.append(row)

    # Extract recommendations
    recs = result.get("recommendations") if isinstance(result, dict) else None
    if isinstance(recs, list) and recs:
        elements.append(Spacer(1, 4))
        elements.append(Paragraph("Key Recommendations:", styles["body_small"]))
        for i, rec in enumerate(recs[:5]):
            if isinstance(rec, dict):
                rec_text = rec.get("recommendation") or rec.get("text") or rec.get("title") or str(rec)
            else:
                rec_text = str(rec)
            safe_rec = (
                rec_text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            )
            bullet = f"{i + 1}. {safe_rec}"
            if len(bullet) > 300:
                bullet = bullet[:297] + "..."
            elements.append(Paragraph(bullet, styles["recommendation"]))

    elements.append(Spacer(1, 12))
    return elements


def _build_cover_elements(
    report_data: Dict[str, Any],
    styles: Dict[str, ParagraphStyle],
) -> List:
    """Build cover page flowable elements."""
    elements: List = []
    elements.append(Spacer(1, 2.5 * inch))

    domain = report_data.get("domain", "Unknown Domain")
    elements.append(Paragraph("Search Intelligence Report", styles["cover_title"]))
    elements.append(Spacer(1, 8))
    elements.append(Paragraph(domain, styles["cover_subtitle"]))
    elements.append(Spacer(1, 4))
    generated_at = datetime.utcnow().strftime("%B %d, %Y")
    elements.append(Paragraph(f"Generated {generated_at}", styles["cover_subtitle"]))
    elements.append(Spacer(1, 0.5 * inch))
    elements.append(
        Paragraph(
            "Powered by Search Intelligence  |  clankermarketing.com",
            styles["cover_subtitle"],
        )
    )
    elements.append(NextPageTemplate("normal"))
    elements.append(PageBreak())
    return elements


def _build_toc(available_modules: List[int], styles: Dict[str, ParagraphStyle]) -> List:
    """Build a simple table of contents page."""
    elements: List = []
    elements.append(Paragraph("Table of Contents", styles["section_heading"]))
    elements.append(Spacer(1, 12))
    for num in sorted(available_modules):
        title = MODULE_TITLES.get(num, f"Module {num}")
        elements.append(
            Paragraph(f"Module {num}: {title}", styles["body"])
        )
    elements.append(PageBreak())
    return elements


def _build_executive_summary(
    module_results: Dict[int, Dict[str, Any]],
    styles: Dict[str, ParagraphStyle],
) -> List:
    """Build executive summary page pulling key data from modules."""
    elements: List = []
    elements.append(Paragraph("Executive Summary", styles["section_heading"]))
    elements.append(Spacer(1, 8))

    # Pull headline metrics from key modules
    highlights = []

    # Module 1 — health summary
    m1 = module_results.get(1, {})
    if isinstance(m1, dict):
        summary = m1.get("summary") or m1.get("executive_summary")
        if isinstance(summary, dict):
            summary = summary.get("narrative") or summary.get("text")
        if isinstance(summary, str) and summary:
            safe = summary[:500].replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            highlights.append(f"<b>Health &amp; Trajectory:</b> {safe}")

    # Module 5 — gameplan headline
    m5 = module_results.get(5, {})
    if isinstance(m5, dict):
        summary = m5.get("summary") or m5.get("executive_summary")
        if isinstance(summary, dict):
            summary = summary.get("narrative") or summary.get("text")
        if isinstance(summary, str) and summary:
            safe = summary[:500].replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            highlights.append(f"<b>Game Plan:</b> {safe}")

    # Module 12 — revenue headline
    m12 = module_results.get(12, {})
    if isinstance(m12, dict):
        summary = m12.get("summary") or m12.get("executive_summary")
        if isinstance(summary, dict):
            summary = summary.get("narrative") or summary.get("text")
        if isinstance(summary, str) and summary:
            safe = summary[:500].replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            highlights.append(f"<b>Revenue Attribution:</b> {safe}")

    if highlights:
        for h in highlights:
            elements.append(Paragraph(h, styles["body"]))
            elements.append(Spacer(1, 6))
    else:
        elements.append(
            Paragraph(
                "Module results are summarised in the following pages.",
                styles["body"],
            )
        )

    elements.append(PageBreak())
    return elements


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_pdf_report(
    report_data: Dict[str, Any],
    module_results: Dict[int, Dict[str, Any]],
) -> bytes:
    """
    Generate a complete PDF report.

    Parameters
    ----------
    report_data : dict
        Report metadata — must include at minimum ``domain``.
        Optional keys: ``gsc_property``, ``ga4_property``, ``created_at``.
    module_results : dict
        Mapping of module number (int) to that module's result dict.

    Returns
    -------
    bytes
        The raw PDF bytes, ready to be returned as an HTTP response body.
    """
    buf = io.BytesIO()
    styles = _build_styles()

    doc = BaseDocTemplate(
        buf,
        pagesize=LETTER,
        leftMargin=0.75 * inch,
        rightMargin=0.75 * inch,
        topMargin=0.75 * inch,
        bottomMargin=0.75 * inch,
        title=f"Search Intelligence Report — {report_data.get('domain', '')}",
        author="Search Intelligence / Clanker Marketing",
    )

    # Page templates
    w, h = LETTER
    frame_cover = Frame(
        0.75 * inch, 0.75 * inch,
        w - 1.5 * inch, h - 1.5 * inch,
        id="cover",
    )
    frame_normal = Frame(
        0.75 * inch, 0.75 * inch,
        w - 1.5 * inch, h - 1.5 * inch,
        id="normal",
    )

    doc.addPageTemplates([
        PageTemplate(id="cover", frames=[frame_cover], onPage=_cover_page),
        PageTemplate(id="normal", frames=[frame_normal], onPage=_normal_page),
    ])

    # Build story
    story: List = []

    # Cover
    story.extend(_build_cover_elements(report_data, styles))

    # TOC
    available = sorted(k for k in module_results if module_results[k])
    if available:
        story.extend(_build_toc(available, styles))

    # Executive summary
    story.extend(_build_executive_summary(module_results, styles))

    # Module detail pages
    for num in sorted(MODULE_TITLES.keys()):
        result = module_results.get(num)
        if not result:
            continue
        story.extend(_build_summary_section(num, result, styles))

    # Closing page
    story.append(PageBreak())
    story.append(Spacer(1, 2 * inch))
    story.append(
        Paragraph(
            "Next Steps",
            styles["section_heading"],
        )
    )
    story.append(Spacer(1, 12))
    story.append(
        Paragraph(
            "This report was generated by the Search Intelligence platform. "
            "For a detailed walkthrough of findings and a customised action plan, "
            "schedule a free consultation at <b>clankermarketing.com</b>.",
            styles["body"],
        )
    )
    story.append(Spacer(1, 24))
    story.append(
        Paragraph(
            "© Search Intelligence / Clanker Marketing. All rights reserved.",
            styles["footer"],
        )
    )

    doc.build(story)
    pdf_bytes = buf.getvalue()
    buf.close()
    logger.info(f"Generated PDF report: {len(pdf_bytes)} bytes, {doc.page} pages")
    return pdf_bytes
