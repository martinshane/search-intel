"""
PDF Report Generator for Search Intelligence Reports.

Generates a professional, multi-page PDF summarising all 12 module results
with module-specific data tables, metric grids, and action items.

Usage::

    pdf_bytes = generate_pdf_report(report_data, module_results)

Requires ``reportlab`` (included in requirements.txt).
"""

import io
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import LETTER
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (
    BaseDocTemplate,
    Frame,
    NextPageTemplate,
    PageBreak,
    PageTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Colour palette
# ---------------------------------------------------------------------------
BRAND_DARK = colors.HexColor("#1a1a2e")
BRAND_ACCENT = colors.HexColor("#4361ee")
BRAND_GREEN = colors.HexColor("#2ecc71")
BRAND_RED = colors.HexColor("#e74c3c")
BRAND_AMBER = colors.HexColor("#f39c12")
BRAND_GRAY = colors.HexColor("#6c757d")
BRAND_LIGHT_BG = colors.HexColor("#f8f9fa")
TABLE_HEADER_BG = colors.HexColor("#343a40")
TABLE_ALT_ROW = colors.HexColor("#f2f2f2")

# ---------------------------------------------------------------------------
# Module metadata
# ---------------------------------------------------------------------------
MODULE_TITLES: Dict[int, str] = {
    1: "Health & Trajectory",
    2: "Page Triage",
    3: "SERP Landscape",
    4: "Content Intelligence",
    5: "Prioritised Game Plan",
    6: "Algorithm Updates",
    7: "Intent Migration",
    8: "CTR Modelling",
    9: "Site Architecture",
    10: "Branded vs Non-Branded",
    11: "Competitive Threats",
    12: "Revenue Attribution",
}

MODULE_DESCRIPTIONS: Dict[int, str] = {
    1: "Overall site health trend analysis with change-point detection and forecasting.",
    2: "Page-level performance triage — winners, decliners, and opportunities requiring action.",
    3: "SERP feature landscape and competitive positioning analysis.",
    4: "Content quality audit — cannibalization, thin content, and striking-distance opportunities.",
    5: "Prioritised action plan synthesising insights from all other modules.",
    6: "Correlation of performance shifts with known Google algorithm updates.",
    7: "Search-intent classification changes and content-alignment audit.",
    8: "Expected vs actual CTR benchmarking and SERP feature opportunity scoring.",
    9: "Internal link graph analysis — PageRank flow, orphan pages, content silos.",
    10: "Brand dependency analysis — branded vs non-branded traffic split and risk.",
    11: "Competitive threat identification and market positioning.",
    12: "Revenue attribution to pages and queries, ROI modelling, and at-risk revenue.",
}


# ---------------------------------------------------------------------------
# Styles
# ---------------------------------------------------------------------------
def _build_styles() -> Dict[str, ParagraphStyle]:
    """Build the paragraph style dict for the entire report."""
    base = getSampleStyleSheet()
    styles: Dict[str, ParagraphStyle] = {}

    styles["cover_title"] = ParagraphStyle(
        "cover_title",
        parent=base["Title"],
        fontSize=28,
        leading=34,
        textColor=BRAND_DARK,
        alignment=TA_CENTER,
        spaceAfter=8,
    )
    styles["cover_subtitle"] = ParagraphStyle(
        "cover_subtitle",
        parent=base["Normal"],
        fontSize=14,
        leading=18,
        textColor=BRAND_GRAY,
        alignment=TA_CENTER,
    )
    styles["section_heading"] = ParagraphStyle(
        "section_heading",
        parent=base["Heading1"],
        fontSize=18,
        leading=22,
        textColor=BRAND_DARK,
        spaceAfter=8,
        spaceBefore=16,
    )
    styles["module_heading"] = ParagraphStyle(
        "module_heading",
        parent=base["Heading2"],
        fontSize=14,
        leading=18,
        textColor=BRAND_ACCENT,
        spaceAfter=6,
        spaceBefore=12,
        keepWithNext=True,
    )
    styles["sub_heading"] = ParagraphStyle(
        "sub_heading",
        parent=base["Heading3"],
        fontSize=11,
        leading=14,
        textColor=BRAND_DARK,
        spaceAfter=4,
        spaceBefore=8,
        keepWithNext=True,
    )
    styles["body"] = ParagraphStyle(
        "body",
        parent=base["Normal"],
        fontSize=10,
        leading=14,
        textColor=colors.black,
        spaceAfter=4,
    )
    styles["body_small"] = ParagraphStyle(
        "body_small",
        parent=base["Normal"],
        fontSize=9,
        leading=12,
        textColor=BRAND_GRAY,
        spaceAfter=4,
    )
    styles["recommendation"] = ParagraphStyle(
        "recommendation",
        parent=base["Normal"],
        fontSize=9,
        leading=12,
        leftIndent=12,
        textColor=colors.black,
        spaceAfter=2,
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
        fontSize=14,
        leading=18,
        textColor=BRAND_DARK,
        alignment=TA_CENTER,
    )
    styles["footer"] = ParagraphStyle(
        "footer",
        parent=base["Normal"],
        fontSize=8,
        leading=10,
        textColor=BRAND_GRAY,
        alignment=TA_CENTER,
    )
    styles["table_header"] = ParagraphStyle(
        "table_header",
        parent=base["Normal"],
        fontSize=8,
        leading=10,
        textColor=colors.white,
    )
    styles["table_cell"] = ParagraphStyle(
        "table_cell",
        parent=base["Normal"],
        fontSize=8,
        leading=10,
        textColor=colors.black,
    )
    return styles


# ---------------------------------------------------------------------------
# Page templates
# ---------------------------------------------------------------------------
def _cover_page(canvas, doc):
    """Draw cover page decorations."""
    canvas.saveState()
    w, h = LETTER
    canvas.setFillColor(BRAND_ACCENT)
    canvas.rect(0, h - 4, w, 4, fill=True, stroke=False)
    canvas.rect(0, 0, w, 4, fill=True, stroke=False)
    canvas.restoreState()


def _normal_page(canvas, doc):
    """Draw header/footer on normal pages."""
    canvas.saveState()
    w, h = LETTER
    # Header line
    canvas.setStrokeColor(BRAND_ACCENT)
    canvas.setLineWidth(0.5)
    canvas.line(0.75 * inch, h - 0.55 * inch, w - 0.75 * inch, h - 0.55 * inch)
    # Footer
    canvas.setFont("Helvetica", 7)
    canvas.setFillColor(BRAND_GRAY)
    canvas.drawString(0.75 * inch, 0.45 * inch, "Search Intelligence Report — clankermarketing.com")
    canvas.drawRightString(w - 0.75 * inch, 0.45 * inch, "Page %d" % doc.page)
    canvas.restoreState()


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------
def _safe_str(value: Any) -> str:
    """Safely convert a value to a display string."""
    if value is None:
        return "N/A"
    if isinstance(value, float):
        if abs(value) >= 1000:
            return "{:,.0f}".format(value)
        return "{:.2f}".format(value)
    if isinstance(value, int):
        return "{:,}".format(value)
    return str(value)[:200]


def _escape_xml(text: str) -> str:
    """Escape XML chars for reportlab Paragraph."""
    return (
        text
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def _truncate(text: str, max_len: int = 300) -> str:
    """Truncate text to max_len chars."""
    if len(text) > max_len:
        return text[: max_len - 3] + "..."
    return text


def _safe_get(d: Any, *keys: str, default: Any = None) -> Any:
    """Safely traverse nested dicts."""
    current = d
    for k in keys:
        if isinstance(current, dict):
            current = current.get(k, default)
        else:
            return default
    return current


# ---------------------------------------------------------------------------
# Metric card builder
# ---------------------------------------------------------------------------
def _build_metric_card(label: str, value: str, styles: Dict) -> Table:
    """Build a small metric card table."""
    t = Table(
        [
            [Paragraph(value, styles["metric_value"])],
            [Paragraph(label, styles["metric_label"])],
        ],
        colWidths=[1.8 * inch],
        rowHeights=[22, 14],
    )
    t.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), BRAND_LIGHT_BG),
                ("BOX", (0, 0), (-1, -1), 0.5, BRAND_GRAY),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
            ]
        )
    )
    return t


def _build_metric_row(metrics: List[Tuple[str, str]], styles: Dict) -> Table:
    """Build a row of metric cards (up to 4)."""
    cards = [_build_metric_card(label, value, styles) for label, value in metrics[:4]]
    if not cards:
        return Spacer(1, 1)
    t = Table([cards], colWidths=[2.0 * inch] * len(cards))
    t.setStyle(
        TableStyle(
            [
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 2),
                ("RIGHTPADDING", (0, 0), (-1, -1), 2),
            ]
        )
    )
    return t


# ---------------------------------------------------------------------------
# Data table builder
# ---------------------------------------------------------------------------
def _build_data_table(
    headers: List[str],
    rows: List[List[str]],
    styles: Dict,
    col_widths: Optional[List[float]] = None,
    max_rows: int = 15,
) -> List:
    """Build a formatted data table with headers and alternating row colours."""
    elements: List = []
    if not rows:
        return elements

    display_rows = rows[:max_rows]
    header_paras = [Paragraph(_escape_xml(h), styles["table_header"]) for h in headers]
    data = [header_paras]
    for row in display_rows:
        data.append([Paragraph(_escape_xml(_truncate(str(c), 80)), styles["table_cell"]) for c in row])

    if col_widths is None:
        available = 7.0 * inch
        col_widths = [available / len(headers)] * len(headers)

    t = Table(data, colWidths=col_widths, repeatRows=1)
    style_commands = [
        ("BACKGROUND", (0, 0), (-1, 0), TABLE_HEADER_BG),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.lightgrey),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ]
    # Alternating row colours
    for i in range(1, len(data)):
        if i % 2 == 0:
            style_commands.append(("BACKGROUND", (0, i), (-1, i), TABLE_ALT_ROW))

    t.setStyle(TableStyle(style_commands))
    elements.append(t)

    if len(rows) > max_rows:
        elements.append(
            Spacer(1, 2)
        )
        elements.append(
            Paragraph(
                "... and %d more rows (see full report online)" % (len(rows) - max_rows),
                styles["body_small"],
            )
        )
    return elements


# ---------------------------------------------------------------------------
# Recommendations builder
# ---------------------------------------------------------------------------
def _build_recommendations(recs: Any, styles: Dict, max_items: int = 5) -> List:
    """Build a numbered list of recommendations."""
    elements: List = []
    if not isinstance(recs, list) or not recs:
        return elements
    elements.append(Paragraph("Key Recommendations:", styles["sub_heading"]))
    for i, rec in enumerate(recs[:max_items]):
        if isinstance(rec, dict):
            text = rec.get("recommendation") or rec.get("text") or rec.get("title") or str(rec)
        else:
            text = str(rec)
        safe = _escape_xml(_truncate(text))
        elements.append(Paragraph("%d. %s" % (i + 1, safe), styles["recommendation"]))
    return elements


# ---------------------------------------------------------------------------
# Module-specific section builders
# ---------------------------------------------------------------------------

def _build_module_1(result: Dict, styles: Dict) -> List:
    """Health & Trajectory — trend metrics, change points, forecast."""
    elements: List = []

    # Key metrics
    metrics = []
    summary = result.get("summary") or result.get("executive_summary") or {}
    if isinstance(summary, dict):
        direction = summary.get("direction") or summary.get("trend_direction", "")
        if direction:
            metrics.append(("Trend Direction", str(direction).title()))
    total_clicks = _safe_get(result, "metrics", "total_clicks") or _safe_get(result, "total_clicks")
    if total_clicks is not None:
        metrics.append(("Total Clicks", _safe_str(total_clicks)))
    total_impressions = _safe_get(result, "metrics", "total_impressions") or _safe_get(result, "total_impressions")
    if total_impressions is not None:
        metrics.append(("Total Impressions", _safe_str(total_impressions)))
    slope = _safe_get(result, "trend", "slope") or _safe_get(result, "metrics", "trend_slope")
    if slope is not None:
        metrics.append(("Trend Slope", _safe_str(slope)))
    if metrics:
        elements.append(_build_metric_row(metrics, styles))
        elements.append(Spacer(1, 6))

    # Change points
    change_points = result.get("change_points") or []
    if isinstance(change_points, list) and change_points:
        elements.append(Paragraph("Change Points Detected:", styles["sub_heading"]))
        headers = ["Date", "Direction", "Magnitude", "Confidence"]
        rows = []
        for cp in change_points[:10]:
            if isinstance(cp, dict):
                rows.append([
                    str(cp.get("date", "")),
                    str(cp.get("direction", "")),
                    _safe_str(cp.get("magnitude", "")),
                    _safe_str(cp.get("confidence", "")),
                ])
        elements.extend(_build_data_table(headers, rows, styles))
        elements.append(Spacer(1, 6))

    # Forecast
    forecast = result.get("forecast") or {}
    if isinstance(forecast, dict) and forecast:
        f30 = forecast.get("30_day") or forecast.get("next_30_days")
        f90 = forecast.get("90_day") or forecast.get("next_90_days")
        if f30 or f90:
            fm = []
            if f30 is not None:
                fm.append(("30-Day Forecast", _safe_str(f30)))
            if f90 is not None:
                fm.append(("90-Day Forecast", _safe_str(f90)))
            elements.append(Paragraph("Forecast:", styles["sub_heading"]))
            elements.append(_build_metric_row(fm, styles))
            elements.append(Spacer(1, 6))

    return elements


def _build_module_2(result: Dict, styles: Dict) -> List:
    """Page Triage — bucket counts, top declining/opportunity pages."""
    elements: List = []

    # Bucket summary
    buckets = result.get("buckets") or result.get("triage_buckets") or {}
    if isinstance(buckets, dict):
        metrics = []
        for bname in ["growing", "stable", "decaying", "critical"]:
            pages = buckets.get(bname) or []
            count = len(pages) if isinstance(pages, list) else pages
            metrics.append((bname.title(), _safe_str(count)))
        if any(m[1] != "0" and m[1] != "N/A" for m in metrics):
            elements.append(_build_metric_row(metrics, styles))
            elements.append(Spacer(1, 6))

    # Top declining pages
    declining = buckets.get("decaying") or buckets.get("declining") or result.get("declining_pages") or []
    if isinstance(declining, list) and declining:
        elements.append(Paragraph("Top Declining Pages:", styles["sub_heading"]))
        headers = ["URL", "Clicks (current)", "Decay Rate", "Priority"]
        rows = []
        for page in declining[:10]:
            if isinstance(page, dict):
                url = str(page.get("url") or page.get("page", ""))
                if len(url) > 50:
                    url = url[:47] + "..."
                rows.append([
                    url,
                    _safe_str(page.get("clicks") or page.get("current_clicks", "")),
                    _safe_str(page.get("decay_rate") or page.get("trend_slope", "")),
                    str(page.get("priority") or page.get("score", "")),
                ])
        elements.extend(_build_data_table(headers, rows, styles,
                                          col_widths=[3.0 * inch, 1.2 * inch, 1.2 * inch, 1.2 * inch]))
        elements.append(Spacer(1, 6))

    # Quick win opportunities
    opportunities = result.get("opportunities") or result.get("striking_distance") or []
    if isinstance(opportunities, list) and opportunities:
        elements.append(Paragraph("Quick-Win Opportunities:", styles["sub_heading"]))
        headers = ["URL", "Potential Clicks", "Current Position"]
        rows = []
        for opp in opportunities[:10]:
            if isinstance(opp, dict):
                url = str(opp.get("url") or opp.get("page", ""))
                if len(url) > 50:
                    url = url[:47] + "..."
                rows.append([
                    url,
                    _safe_str(opp.get("potential_clicks") or opp.get("recoverable_clicks", "")),
                    _safe_str(opp.get("position") or opp.get("avg_position", "")),
                ])
        elements.extend(_build_data_table(headers, rows, styles,
                                          col_widths=[3.5 * inch, 1.75 * inch, 1.75 * inch]))

    return elements


def _build_module_3(result: Dict, styles: Dict) -> List:
    """SERP Landscape — competitor map, SERP features, click share."""
    elements: List = []

    # Click share estimate
    click_share = result.get("click_share") or result.get("estimated_click_share")
    if click_share is not None:
        metrics = [("Your Click Share", _safe_str(click_share))]
        total_kw = result.get("total_keywords_analyzed") or result.get("keywords_analyzed")
        if total_kw is not None:
            metrics.append(("Keywords Analysed", _safe_str(total_kw)))
        elements.append(_build_metric_row(metrics, styles))
        elements.append(Spacer(1, 6))

    # Top competitors
    competitors = result.get("competitors") or result.get("competitor_map") or []
    if isinstance(competitors, list) and competitors:
        elements.append(Paragraph("Top Competitors:", styles["sub_heading"]))
        headers = ["Domain", "Overlap Keywords", "Avg Position", "Threat Level"]
        rows = []
        for comp in competitors[:10]:
            if isinstance(comp, dict):
                rows.append([
                    str(comp.get("domain") or comp.get("competitor", "")),
                    _safe_str(comp.get("overlap_count") or comp.get("keyword_overlap", "")),
                    _safe_str(comp.get("avg_position", "")),
                    str(comp.get("threat_level") or comp.get("threat", "")),
                ])
        elements.extend(_build_data_table(headers, rows, styles))
        elements.append(Spacer(1, 6))

    # SERP features
    features = result.get("serp_features") or result.get("feature_distribution") or {}
    if isinstance(features, dict) and features:
        elements.append(Paragraph("SERP Feature Distribution:", styles["sub_heading"]))
        headers = ["Feature Type", "Frequency", "Your Presence"]
        rows = []
        for feat, data in list(features.items())[:10]:
            if isinstance(data, dict):
                rows.append([
                    str(feat),
                    _safe_str(data.get("count") or data.get("frequency", "")),
                    str(data.get("your_presence") or data.get("present", "")),
                ])
            else:
                rows.append([str(feat), _safe_str(data), ""])
        elements.extend(_build_data_table(headers, rows, styles))

    return elements


def _build_module_4(result: Dict, styles: Dict) -> List:
    """Content Intelligence — cannibalization, thin content, striking distance."""
    elements: List = []

    # Summary metrics
    metrics = []
    cannibal = result.get("cannibalization_clusters") or result.get("cannibalization") or []
    if isinstance(cannibal, list):
        metrics.append(("Cannibalization Clusters", _safe_str(len(cannibal))))
    thin = result.get("thin_content_pages") or result.get("thin_content") or []
    if isinstance(thin, list):
        metrics.append(("Thin Content Pages", _safe_str(len(thin))))
    striking = result.get("striking_distance") or result.get("striking_distance_opportunities") or []
    if isinstance(striking, list):
        metrics.append(("Striking Distance", _safe_str(len(striking))))
    if metrics:
        elements.append(_build_metric_row(metrics, styles))
        elements.append(Spacer(1, 6))

    # Cannibalization clusters
    if isinstance(cannibal, list) and cannibal:
        elements.append(Paragraph("Top Cannibalization Clusters:", styles["sub_heading"]))
        headers = ["Target Keyword", "Competing Pages", "Impressions Lost"]
        rows = []
        for cluster in cannibal[:8]:
            if isinstance(cluster, dict):
                pages = cluster.get("pages") or cluster.get("urls") or []
                rows.append([
                    str(cluster.get("keyword") or cluster.get("query", "")),
                    _safe_str(len(pages) if isinstance(pages, list) else pages),
                    _safe_str(cluster.get("impressions_lost") or cluster.get("wasted_impressions", "")),
                ])
        elements.extend(_build_data_table(headers, rows, styles))
        elements.append(Spacer(1, 6))

    # Striking distance
    if isinstance(striking, list) and striking:
        elements.append(Paragraph("Striking-Distance Opportunities:", styles["sub_heading"]))
        headers = ["Query", "Page", "Position", "Potential Gain"]
        rows = []
        for opp in striking[:8]:
            if isinstance(opp, dict):
                url = str(opp.get("page") or opp.get("url", ""))
                if len(url) > 40:
                    url = url[:37] + "..."
                rows.append([
                    str(opp.get("query") or opp.get("keyword", "")),
                    url,
                    _safe_str(opp.get("position") or opp.get("avg_position", "")),
                    _safe_str(opp.get("potential_clicks") or opp.get("estimated_gain", "")),
                ])
        elements.extend(_build_data_table(headers, rows, styles,
                                          col_widths=[2.0 * inch, 2.5 * inch, 1.0 * inch, 1.5 * inch]))

    return elements


def _build_module_5(result: Dict, styles: Dict) -> List:
    """Prioritised Game Plan — action items by category."""
    elements: List = []

    categories = ["critical", "quick_wins", "strategic", "structural"]
    cat_labels = {
        "critical": "Critical Fixes",
        "quick_wins": "Quick Wins",
        "strategic": "Strategic Investments",
        "structural": "Structural Improvements",
    }

    # Count per category
    metrics = []
    actions = result.get("actions") or result.get("action_plan") or result.get("priorities") or {}
    if isinstance(actions, dict):
        for cat in categories:
            items = actions.get(cat) or []
            if isinstance(items, list):
                metrics.append((cat_labels.get(cat, cat), _safe_str(len(items))))
    elif isinstance(actions, list):
        metrics.append(("Total Actions", _safe_str(len(actions))))
    if metrics:
        elements.append(_build_metric_row(metrics, styles))
        elements.append(Spacer(1, 6))

    # Action tables per category
    if isinstance(actions, dict):
        for cat in categories:
            items = actions.get(cat) or []
            if not isinstance(items, list) or not items:
                continue
            elements.append(Paragraph(cat_labels.get(cat, cat) + ":", styles["sub_heading"]))
            headers = ["Action", "Impact", "Effort", "Module Source"]
            rows = []
            for item in items[:8]:
                if isinstance(item, dict):
                    action_text = str(item.get("action") or item.get("recommendation") or item.get("title", ""))
                    if len(action_text) > 60:
                        action_text = action_text[:57] + "..."
                    rows.append([
                        action_text,
                        str(item.get("impact") or item.get("estimated_impact", "")),
                        str(item.get("effort") or ""),
                        str(item.get("source_module") or item.get("module", "")),
                    ])
            elements.extend(_build_data_table(headers, rows, styles,
                                              col_widths=[3.0 * inch, 1.3 * inch, 1.0 * inch, 1.7 * inch]))
            elements.append(Spacer(1, 4))
    elif isinstance(actions, list):
        elements.append(Paragraph("Action Items:", styles["sub_heading"]))
        headers = ["Action", "Impact", "Priority"]
        rows = []
        for item in actions[:12]:
            if isinstance(item, dict):
                action_text = str(item.get("action") or item.get("recommendation") or item.get("title", ""))
                if len(action_text) > 70:
                    action_text = action_text[:67] + "..."
                rows.append([
                    action_text,
                    str(item.get("impact") or item.get("estimated_impact", "")),
                    str(item.get("priority") or ""),
                ])
            else:
                text = str(item)
                if len(text) > 70:
                    text = text[:67] + "..."
                rows.append([text, "", ""])
        elements.extend(_build_data_table(headers, rows, styles))

    # Estimated recovery/growth
    recovery = result.get("estimated_recovery") or result.get("total_recovery_clicks")
    growth = result.get("estimated_growth") or result.get("total_growth_clicks")
    if recovery is not None or growth is not None:
        fm = []
        if recovery is not None:
            fm.append(("Recoverable Clicks/mo", _safe_str(recovery)))
        if growth is not None:
            fm.append(("Growth Opportunity/mo", _safe_str(growth)))
        elements.append(Spacer(1, 4))
        elements.append(_build_metric_row(fm, styles))

    return elements


def _build_module_6(result: Dict, styles: Dict) -> List:
    """Algorithm Updates — vulnerability score, update impacts."""
    elements: List = []

    vuln = result.get("vulnerability_score") or _safe_get(result, "metrics", "vulnerability_score")
    if vuln is not None:
        metrics = [("Vulnerability Score", _safe_str(vuln))]
        total_updates = result.get("updates_analyzed") or result.get("total_updates")
        if total_updates is not None:
            metrics.append(("Updates Analysed", _safe_str(total_updates)))
        impacted = result.get("impacted_updates") or result.get("updates_with_impact")
        if impacted is not None:
            metrics.append(("Updates With Impact", _safe_str(impacted)))
        elements.append(_build_metric_row(metrics, styles))
        elements.append(Spacer(1, 6))

    # Update timeline
    updates = result.get("update_impacts") or result.get("algorithm_impacts") or result.get("updates") or []
    if isinstance(updates, list) and updates:
        elements.append(Paragraph("Algorithm Update Impact Timeline:", styles["sub_heading"]))
        headers = ["Update Name", "Date", "Click Change %", "Recovery Status"]
        rows = []
        for upd in updates[:10]:
            if isinstance(upd, dict):
                rows.append([
                    str(upd.get("name") or upd.get("update_name", "")),
                    str(upd.get("date") or upd.get("rollout_date", "")),
                    _safe_str(upd.get("click_change_pct") or upd.get("impact_pct", "")),
                    str(upd.get("recovery_status") or upd.get("status", "")),
                ])
        elements.extend(_build_data_table(headers, rows, styles))

    return elements


def _build_module_7(result: Dict, styles: Dict) -> List:
    """Intent Migration — intent distribution shifts, AI Overview impact."""
    elements: List = []

    # Intent distribution
    distribution = result.get("intent_distribution") or result.get("current_distribution") or {}
    if isinstance(distribution, dict) and distribution:
        metrics = []
        for intent_type in ["informational", "commercial", "transactional", "navigational"]:
            val = distribution.get(intent_type)
            if val is not None:
                label = intent_type.title()
                metrics.append((label, _safe_str(val)))
        if metrics:
            elements.append(Paragraph("Current Intent Distribution:", styles["sub_heading"]))
            elements.append(_build_metric_row(metrics, styles))
            elements.append(Spacer(1, 6))

    # AI Overview impact
    ai_overview = result.get("ai_overview_impact") or result.get("ai_overview") or {}
    if isinstance(ai_overview, dict) and ai_overview:
        queries_affected = ai_overview.get("queries_affected") or ai_overview.get("affected_queries")
        clicks_lost = ai_overview.get("estimated_clicks_lost") or ai_overview.get("clicks_displaced")
        if queries_affected is not None or clicks_lost is not None:
            elements.append(Paragraph("AI Overview Displacement:", styles["sub_heading"]))
            fm = []
            if queries_affected is not None:
                fm.append(("Queries Affected", _safe_str(queries_affected)))
            if clicks_lost is not None:
                fm.append(("Est. Clicks Lost", _safe_str(clicks_lost)))
            elements.append(_build_metric_row(fm, styles))
            elements.append(Spacer(1, 6))

    return elements


def _build_module_8(result: Dict, styles: Dict) -> List:
    """CTR Modelling — model accuracy, top under/over-performers, SERP opportunities."""
    elements: List = []

    # Model accuracy
    accuracy = result.get("model_accuracy") or result.get("model_r2") or _safe_get(result, "metrics", "model_r2")
    if accuracy is not None:
        metrics = [("Model R-squared", _safe_str(accuracy))]
        total = result.get("keywords_modeled") or result.get("total_keywords")
        if total is not None:
            metrics.append(("Keywords Modelled", _safe_str(total)))
        elements.append(_build_metric_row(metrics, styles))
        elements.append(Spacer(1, 6))

    # SERP feature opportunities
    opportunities = result.get("serp_opportunities") or result.get("feature_opportunities") or []
    if isinstance(opportunities, list) and opportunities:
        elements.append(Paragraph("SERP Feature Opportunities:", styles["sub_heading"]))
        headers = ["Keyword", "Feature", "Current Holder", "Est. Click Gain", "Difficulty"]
        rows = []
        for opp in opportunities[:10]:
            if isinstance(opp, dict):
                rows.append([
                    str(opp.get("keyword") or opp.get("query", "")),
                    str(opp.get("feature_type") or opp.get("feature", "")),
                    str(opp.get("current_holder", "")),
                    _safe_str(opp.get("estimated_click_gain") or opp.get("click_gain", "")),
                    str(opp.get("difficulty", "")),
                ])
        elements.extend(_build_data_table(headers, rows, styles,
                                          col_widths=[1.6 * inch, 1.2 * inch, 1.4 * inch, 1.2 * inch, 1.0 * inch]))

    # Underperforming keywords
    underperformers = result.get("underperforming_keywords") or result.get("ctr_underperformers") or []
    if isinstance(underperformers, list) and underperformers:
        elements.append(Spacer(1, 6))
        elements.append(Paragraph("CTR Underperformers (biggest gaps):", styles["sub_heading"]))
        headers = ["Keyword", "Position", "Actual CTR", "Expected CTR", "Gap"]
        rows = []
        for kw in underperformers[:10]:
            if isinstance(kw, dict):
                rows.append([
                    str(kw.get("keyword") or kw.get("query", "")),
                    _safe_str(kw.get("position") or kw.get("avg_position", "")),
                    _safe_str(kw.get("actual_ctr", "")),
                    _safe_str(kw.get("expected_ctr", "")),
                    _safe_str(kw.get("gap") or kw.get("ctr_gap", "")),
                ])
        elements.extend(_build_data_table(headers, rows, styles,
                                          col_widths=[2.0 * inch, 1.0 * inch, 1.2 * inch, 1.2 * inch, 1.0 * inch]))

    return elements


def _build_module_9(result: Dict, styles: Dict) -> List:
    """Site Architecture — PageRank stats, orphans, silos, link recommendations."""
    elements: List = []

    # Key metrics
    metrics = []
    orphan_count = result.get("orphan_page_count") or len(result.get("orphan_pages") or [])
    if orphan_count:
        metrics.append(("Orphan Pages", _safe_str(orphan_count)))
    silos = result.get("content_silos") or result.get("silos") or []
    if isinstance(silos, list):
        metrics.append(("Content Silos", _safe_str(len(silos))))
    authority_flow = result.get("authority_flow_to_conversion") or _safe_get(result, "metrics", "authority_flow")
    if authority_flow is not None:
        metrics.append(("Authority Flow", _safe_str(authority_flow)))
    if metrics:
        elements.append(_build_metric_row(metrics, styles))
        elements.append(Spacer(1, 6))

    # Content silos table
    if isinstance(silos, list) and silos:
        elements.append(Paragraph("Content Silos:", styles["sub_heading"]))
        headers = ["Silo", "Pages", "Authority %", "Avg PageRank"]
        rows = []
        for silo in silos[:10]:
            if isinstance(silo, dict):
                rows.append([
                    str(silo.get("name") or silo.get("silo", "")),
                    _safe_str(silo.get("page_count") or silo.get("pages", "")),
                    _safe_str(silo.get("authority_pct") or silo.get("authority_share", "")),
                    _safe_str(silo.get("avg_pagerank", "")),
                ])
        elements.extend(_build_data_table(headers, rows, styles))
        elements.append(Spacer(1, 6))

    # Internal link recommendations
    link_recs = result.get("link_recommendations") or result.get("internal_link_suggestions") or []
    if isinstance(link_recs, list) and link_recs:
        elements.append(Paragraph("Internal Link Recommendations:", styles["sub_heading"]))
        headers = ["From Page", "Target Page", "Anchor Text", "PR Boost"]
        rows = []
        for rec in link_recs[:8]:
            if isinstance(rec, dict):
                from_url = str(rec.get("from_url") or rec.get("source", ""))
                to_url = str(rec.get("to_url") or rec.get("target", ""))
                if len(from_url) > 35:
                    from_url = from_url[:32] + "..."
                if len(to_url) > 35:
                    to_url = to_url[:32] + "..."
                rows.append([
                    from_url,
                    to_url,
                    str(rec.get("anchor_text", "")),
                    _safe_str(rec.get("pr_boost") or rec.get("pagerank_boost", "")),
                ])
        elements.extend(_build_data_table(headers, rows, styles,
                                          col_widths=[2.2 * inch, 2.2 * inch, 1.5 * inch, 1.1 * inch]))

    return elements


def _build_module_10(result: Dict, styles: Dict) -> List:
    """Branded vs Non-Branded split — dependency, growth rates."""
    elements: List = []

    metrics = []
    dependency = result.get("dependency_level") or result.get("brand_dependency")
    if dependency is not None:
        metrics.append(("Dependency Level", str(dependency).title()))
    branded_ratio = result.get("branded_ratio") or result.get("branded_share")
    if branded_ratio is not None:
        metrics.append(("Branded Share", _safe_str(branded_ratio)))
    non_branded_gap = result.get("non_branded_opportunity_gap") or result.get("opportunity_gap")
    if non_branded_gap is not None:
        metrics.append(("Non-Branded Gap", _safe_str(non_branded_gap)))
    months_to_meaningful = result.get("months_to_meaningful") or _safe_get(result, "non_branded", "months_to_meaningful")
    if months_to_meaningful is not None:
        metrics.append(("Months to Growth", _safe_str(months_to_meaningful)))
    if metrics:
        elements.append(_build_metric_row(metrics, styles))
        elements.append(Spacer(1, 6))

    # Growth rates
    branded_growth = result.get("branded_growth_rate") or _safe_get(result, "branded", "growth_rate")
    non_branded_growth = result.get("non_branded_growth_rate") or _safe_get(result, "non_branded", "growth_rate")
    if branded_growth is not None or non_branded_growth is not None:
        elements.append(Paragraph("Growth Rate Comparison:", styles["sub_heading"]))
        headers = ["Segment", "Growth Rate", "Current Clicks", "Trend"]
        rows = []
        if branded_growth is not None:
            branded_clicks = _safe_get(result, "branded", "current_clicks") or result.get("branded_clicks")
            branded_trend = _safe_get(result, "branded", "trend") or ""
            rows.append(["Branded", _safe_str(branded_growth), _safe_str(branded_clicks), str(branded_trend)])
        if non_branded_growth is not None:
            nb_clicks = _safe_get(result, "non_branded", "current_clicks") or result.get("non_branded_clicks")
            nb_trend = _safe_get(result, "non_branded", "trend") or ""
            rows.append(["Non-Branded", _safe_str(non_branded_growth), _safe_str(nb_clicks), str(nb_trend)])
        elements.extend(_build_data_table(headers, rows, styles))

    return elements


def _build_module_11(result: Dict, styles: Dict) -> List:
    """Competitive Threats — competitor overlap, emerging threats, keyword vulnerability."""
    elements: List = []

    # Top competitors by overlap
    competitors = result.get("competitors") or result.get("competitor_matrix") or []
    if isinstance(competitors, list) and competitors:
        elements.append(Paragraph("Competitor Overlap:", styles["sub_heading"]))
        headers = ["Competitor", "Keyword Overlap", "Threat Level", "Trajectory"]
        rows = []
        for comp in competitors[:10]:
            if isinstance(comp, dict):
                rows.append([
                    str(comp.get("domain") or comp.get("competitor", "")),
                    _safe_str(comp.get("keyword_overlap") or comp.get("overlap_count", "")),
                    str(comp.get("threat_level") or comp.get("threat", "")),
                    str(comp.get("trajectory") or comp.get("trend", "")),
                ])
        elements.extend(_build_data_table(headers, rows, styles))
        elements.append(Spacer(1, 6))

    # Emerging threats
    threats = result.get("emerging_threats") or result.get("new_threats") or []
    if isinstance(threats, list) and threats:
        elements.append(Paragraph("Emerging Threats:", styles["sub_heading"]))
        headers = ["Domain", "First Seen", "Keywords Gained", "Avg Position"]
        rows = []
        for threat in threats[:8]:
            if isinstance(threat, dict):
                rows.append([
                    str(threat.get("domain") or threat.get("competitor", "")),
                    str(threat.get("first_seen") or threat.get("date", "")),
                    _safe_str(threat.get("keywords_gained") or threat.get("keyword_count", "")),
                    _safe_str(threat.get("avg_position", "")),
                ])
        elements.extend(_build_data_table(headers, rows, styles))
        elements.append(Spacer(1, 6))

    # Vulnerable keywords
    vulnerable = result.get("vulnerable_keywords") or result.get("keyword_vulnerabilities") or []
    if isinstance(vulnerable, list) and vulnerable:
        elements.append(Paragraph("Keyword Vulnerabilities:", styles["sub_heading"]))
        headers = ["Keyword", "Your Position", "Competitors Within 3", "Gap Trend"]
        rows = []
        for kw in vulnerable[:10]:
            if isinstance(kw, dict):
                rows.append([
                    str(kw.get("keyword") or kw.get("query", "")),
                    _safe_str(kw.get("your_position") or kw.get("position", "")),
                    _safe_str(kw.get("competitors_within_3") or kw.get("close_competitors", "")),
                    str(kw.get("gap_trend") or kw.get("trend", "")),
                ])
        elements.extend(_build_data_table(headers, rows, styles))

    return elements


def _build_module_12(result: Dict, styles: Dict) -> List:
    """Revenue Attribution — monthly revenue, at-risk, ROI opportunities."""
    elements: List = []

    # Key revenue metrics
    metrics = []
    monthly_rev = result.get("monthly_revenue") or result.get("total_monthly_revenue") or _safe_get(result, "metrics", "monthly_revenue")
    if monthly_rev is not None:
        metrics.append(("Monthly Revenue", "$" + _safe_str(monthly_rev)))
    at_risk = result.get("revenue_at_risk") or result.get("at_risk_90d") or _safe_get(result, "metrics", "at_risk")
    if at_risk is not None:
        metrics.append(("At Risk (90d)", "$" + _safe_str(at_risk)))
    total_opp = result.get("total_opportunity") or result.get("roi_opportunity") or _safe_get(result, "metrics", "total_opportunity")
    if total_opp is not None:
        metrics.append(("Total Opportunity", "$" + _safe_str(total_opp)))
    if metrics:
        elements.append(_build_metric_row(metrics, styles))
        elements.append(Spacer(1, 6))

    # ROI breakdown
    roi = result.get("roi_breakdown") or result.get("roi_waterfall") or {}
    if isinstance(roi, dict) and roi:
        elements.append(Paragraph("ROI Breakdown:", styles["sub_heading"]))
        headers = ["Category", "Estimated Monthly Value"]
        rows = []
        for cat in ["critical_fixes", "quick_wins", "strategic", "total"]:
            val = roi.get(cat)
            if val is not None:
                label = cat.replace("_", " ").title()
                rows.append([label, "$" + _safe_str(val)])
        elements.extend(_build_data_table(headers, rows, styles,
                                          col_widths=[3.5 * inch, 3.5 * inch]))
        elements.append(Spacer(1, 6))

    # Top revenue keywords
    top_keywords = result.get("top_revenue_keywords") or result.get("revenue_keywords") or []
    if isinstance(top_keywords, list) and top_keywords:
        elements.append(Paragraph("Top Revenue Keywords:", styles["sub_heading"]))
        headers = ["Keyword", "Current Revenue", "Potential (Top 3)", "Gap"]
        rows = []
        for kw in top_keywords[:10]:
            if isinstance(kw, dict):
                rows.append([
                    str(kw.get("keyword") or kw.get("query", "")),
                    "$" + _safe_str(kw.get("current_revenue") or kw.get("monthly_revenue", "")),
                    "$" + _safe_str(kw.get("potential_revenue") or kw.get("potential_if_top3", "")),
                    "$" + _safe_str(kw.get("gap") or kw.get("revenue_gap", "")),
                ])
        elements.extend(_build_data_table(headers, rows, styles))

    return elements


# ---------------------------------------------------------------------------
# Module builder dispatch
# ---------------------------------------------------------------------------
MODULE_BUILDERS: Dict[int, Any] = {
    1: _build_module_1,
    2: _build_module_2,
    3: _build_module_3,
    4: _build_module_4,
    5: _build_module_5,
    6: _build_module_6,
    7: _build_module_7,
    8: _build_module_8,
    9: _build_module_9,
    10: _build_module_10,
    11: _build_module_11,
    12: _build_module_12,
}


def _build_module_section(
    module_num: int,
    result: Dict[str, Any],
    styles: Dict[str, ParagraphStyle],
) -> List:
    """Build flowable elements for one module, using the dedicated builder if available."""
    elements: List = []
    title = MODULE_TITLES.get(module_num, "Module %d" % module_num)
    desc = MODULE_DESCRIPTIONS.get(module_num, "")

    elements.append(Paragraph("Module %d: %s" % (module_num, title), styles["module_heading"]))
    if desc:
        elements.append(Paragraph(desc, styles["body_small"]))
    elements.append(Spacer(1, 4))

    # Narrative summary
    summary_text = None
    if isinstance(result, dict):
        summary_text = (
            result.get("summary")
            or result.get("executive_summary")
            or result.get("narrative")
        )
        if isinstance(summary_text, dict):
            summary_text = summary_text.get("narrative") or summary_text.get("text") or None
    if summary_text and isinstance(summary_text, str):
        safe = _escape_xml(_truncate(summary_text, 1200))
        elements.append(Paragraph(safe, styles["body"]))
        elements.append(Spacer(1, 4))

    # Module-specific content
    builder = MODULE_BUILDERS.get(module_num)
    if builder and isinstance(result, dict):
        try:
            module_elements = builder(result, styles)
            elements.extend(module_elements)
        except Exception as exc:
            logger.warning("Module %d PDF builder error: %s", module_num, exc)
            # Fall back to generic rendering
            elements.extend(_build_generic_section(result, styles))
    else:
        elements.extend(_build_generic_section(result, styles))

    # Recommendations (common across all modules)
    recs = result.get("recommendations") if isinstance(result, dict) else None
    elements.extend(_build_recommendations(recs, styles))

    elements.append(Spacer(1, 12))
    return elements


def _build_generic_section(result: Dict, styles: Dict) -> List:
    """Fallback generic renderer for any module without a dedicated builder."""
    elements: List = []
    metrics = result.get("metrics") or result.get("key_metrics")
    if isinstance(metrics, dict) and metrics:
        cards = []
        for k, v in list(metrics.items())[:4]:
            label = k.replace("_", " ").title()
            cards.append((label, _safe_str(v)))
        if cards:
            elements.append(_build_metric_row(cards, styles))
            elements.append(Spacer(1, 6))
    return elements


# ---------------------------------------------------------------------------
# Cover, TOC, Executive Summary
# ---------------------------------------------------------------------------

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
    elements.append(Paragraph("Generated %s" % generated_at, styles["cover_subtitle"]))
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
        title = MODULE_TITLES.get(num, "Module %d" % num)
        elements.append(
            Paragraph("Module %d: %s" % (num, title), styles["body"])
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

    highlights = []

    # Module 1 — health summary
    m1 = module_results.get(1, {})
    if isinstance(m1, dict):
        summary = m1.get("summary") or m1.get("executive_summary")
        if isinstance(summary, dict):
            summary = summary.get("narrative") or summary.get("text")
        if isinstance(summary, str) and summary:
            safe = _escape_xml(_truncate(summary, 500))
            highlights.append("<b>Health &amp; Trajectory:</b> %s" % safe)

    # Module 2 — triage headline
    m2 = module_results.get(2, {})
    if isinstance(m2, dict):
        buckets = m2.get("buckets") or m2.get("triage_buckets") or {}
        if isinstance(buckets, dict):
            critical = buckets.get("critical") or []
            decaying = buckets.get("decaying") or []
            c_count = len(critical) if isinstance(critical, list) else 0
            d_count = len(decaying) if isinstance(decaying, list) else 0
            if c_count or d_count:
                highlights.append(
                    "<b>Page Triage:</b> %d critical pages and %d decaying pages identified." % (c_count, d_count)
                )

    # Module 5 — gameplan headline
    m5 = module_results.get(5, {})
    if isinstance(m5, dict):
        summary = m5.get("summary") or m5.get("executive_summary")
        if isinstance(summary, dict):
            summary = summary.get("narrative") or summary.get("text")
        if isinstance(summary, str) and summary:
            safe = _escape_xml(_truncate(summary, 500))
            highlights.append("<b>Game Plan:</b> %s" % safe)

    # Module 12 — revenue headline
    m12 = module_results.get(12, {})
    if isinstance(m12, dict):
        monthly_rev = m12.get("monthly_revenue") or m12.get("total_monthly_revenue")
        at_risk = m12.get("revenue_at_risk") or m12.get("at_risk_90d")
        if monthly_rev is not None:
            text = "Estimated monthly search revenue: $%s." % _safe_str(monthly_rev)
            if at_risk is not None:
                text += " Revenue at risk (90d): $%s." % _safe_str(at_risk)
            highlights.append("<b>Revenue Attribution:</b> %s" % text)
        else:
            summary = m12.get("summary") or m12.get("executive_summary")
            if isinstance(summary, dict):
                summary = summary.get("narrative") or summary.get("text")
            if isinstance(summary, str) and summary:
                safe = _escape_xml(_truncate(summary, 500))
                highlights.append("<b>Revenue Attribution:</b> %s" % safe)

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
    Generate a complete PDF report with module-specific detail sections.

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
        title="Search Intelligence Report — %s" % report_data.get("domain", ""),
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
        story.extend(_build_module_section(num, result, styles))

    # Closing page — consulting CTA
    story.append(PageBreak())
    story.append(Spacer(1, 2 * inch))
    story.append(
        Paragraph("Next Steps", styles["section_heading"])
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
    story.append(Spacer(1, 12))
    story.append(
        Paragraph(
            "Our team will help you prioritise the actions identified in this report, "
            "build an execution roadmap, and track progress month over month.",
            styles["body"],
        )
    )
    story.append(Spacer(1, 24))
    story.append(
        Paragraph(
            "clankermarketing.com  |  Search Intelligence Report",
            styles["footer"],
        )
    )

    doc.build(story)
    pdf_bytes = buf.getvalue()
    buf.close()
    logger.info("Generated PDF report: %d bytes, %d pages", len(pdf_bytes), doc.page)
    return pdf_bytes
