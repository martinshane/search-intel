"""
Report Comparison Service for Search Intelligence Reports.

Compares two completed reports (current vs baseline) and produces a
structured delta for every module. This powers:
  - Historical comparison UI (this month vs last month)
  - Weekly re-run emails showing what changed
  - Trend-over-time tracking for consulting clients

Usage:
    from api.services.report_comparison import compare_reports
    delta = compare_reports(current_modules, baseline_modules, current_meta, baseline_meta)
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_get(d: Optional[Dict], *keys: str, default: Any = None) -> Any:
    """Nested dict access that never raises."""
    val = d
    for k in keys:
        if not isinstance(val, dict):
            return default
        val = val.get(k, default)
    return val


def _pct_change(current: Optional[float], baseline: Optional[float]) -> Optional[float]:
    """Percentage change from baseline to current. Returns None if either is missing."""
    if current is None or baseline is None:
        return None
    if baseline == 0:
        return None if current == 0 else 100.0
    return round(((current - baseline) / abs(baseline)) * 100, 2)


def _delta(current: Optional[float], baseline: Optional[float]) -> Optional[float]:
    """Absolute delta. Returns None if either is missing."""
    if current is None or baseline is None:
        return None
    return round(current - baseline, 4)


def _direction_label(change: Optional[float]) -> str:
    """Human-readable direction from a numeric change."""
    if change is None:
        return "unknown"
    if change > 0:
        return "improved"
    elif change < 0:
        return "declined"
    return "unchanged"


def _compare_lists_by_key(
    current_list: List[Dict],
    baseline_list: List[Dict],
    key: str,
    metric: str,
) -> Dict[str, Any]:
    """
    Compare two lists of dicts on a shared key, producing added/removed/changed items.
    """
    curr_map = {item.get(key, ""): item for item in (current_list or [])}
    base_map = {item.get(key, ""): item for item in (baseline_list or [])}

    added = [k for k in curr_map if k not in base_map]
    removed = [k for k in base_map if k not in curr_map]

    changed = []
    for k in curr_map:
        if k in base_map:
            cv = curr_map[k].get(metric)
            bv = base_map[k].get(metric)
            if cv is not None and bv is not None and cv != bv:
                changed.append({
                    key: k,
                    "current": cv,
                    "baseline": bv,
                    "delta": _delta(cv, bv),
                    "pct_change": _pct_change(cv, bv),
                })

    return {
        "added_count": len(added),
        "removed_count": len(removed),
        "changed_count": len(changed),
        "added": added[:20],
        "removed": removed[:20],
        "changed": sorted(changed, key=lambda x: abs(x.get("delta") or 0), reverse=True)[:20],
    }


# ---------------------------------------------------------------------------
# Per-module comparison functions
# ---------------------------------------------------------------------------

def _compare_module_1(current: Dict, baseline: Dict) -> Dict[str, Any]:
    """Health & Trajectory delta."""
    return {
        "module": 1,
        "name": "Health & Trajectory",
        "metrics": {
            "overall_direction": {
                "current": _safe_get(current, "overall_direction"),
                "baseline": _safe_get(baseline, "overall_direction"),
                "changed": _safe_get(current, "overall_direction") != _safe_get(baseline, "overall_direction"),
            },
            "trend_slope_pct_per_month": {
                "current": _safe_get(current, "trend_slope_pct_per_month"),
                "baseline": _safe_get(baseline, "trend_slope_pct_per_month"),
                "delta": _delta(
                    _safe_get(current, "trend_slope_pct_per_month"),
                    _safe_get(baseline, "trend_slope_pct_per_month"),
                ),
                "direction": _direction_label(
                    _delta(
                        _safe_get(current, "trend_slope_pct_per_month"),
                        _safe_get(baseline, "trend_slope_pct_per_month"),
                    )
                ),
            },
            "forecast_30d_clicks": {
                "current": _safe_get(current, "forecast", "30d", "clicks"),
                "baseline": _safe_get(baseline, "forecast", "30d", "clicks"),
                "delta": _delta(
                    _safe_get(current, "forecast", "30d", "clicks"),
                    _safe_get(baseline, "forecast", "30d", "clicks"),
                ),
                "pct_change": _pct_change(
                    _safe_get(current, "forecast", "30d", "clicks"),
                    _safe_get(baseline, "forecast", "30d", "clicks"),
                ),
            },
        },
        "change_points": {
            "current_count": len(_safe_get(current, "change_points", default=[]) or []),
            "baseline_count": len(_safe_get(baseline, "change_points", default=[]) or []),
            "new_change_points": [
                cp for cp in (_safe_get(current, "change_points", default=[]) or [])
                if cp.get("date") not in [
                    bcp.get("date") for bcp in (_safe_get(baseline, "change_points", default=[]) or [])
                ]
            ],
        },
    }


def _compare_module_2(current: Dict, baseline: Dict) -> Dict[str, Any]:
    """Page Triage delta."""
    curr_summary = _safe_get(current, "summary", default={}) or {}
    base_summary = _safe_get(baseline, "summary", default={}) or {}

    buckets = ["growing", "stable", "decaying", "critical"]
    bucket_deltas = {}
    for b in buckets:
        cv = curr_summary.get(b, 0)
        bv = base_summary.get(b, 0)
        bucket_deltas[b] = {
            "current": cv,
            "baseline": bv,
            "delta": cv - bv,
        }

    # Pages that moved to critical
    curr_pages = {p.get("url"): p for p in (_safe_get(current, "pages", default=[]) or [])}
    base_pages = {p.get("url"): p for p in (_safe_get(baseline, "pages", default=[]) or [])}

    new_critical = []
    recovered = []
    for url, page in curr_pages.items():
        if page.get("bucket") == "critical" and base_pages.get(url, {}).get("bucket") != "critical":
            new_critical.append(url)
        if page.get("bucket") in ("growing", "stable") and base_pages.get(url, {}).get("bucket") in ("decaying", "critical"):
            recovered.append(url)

    return {
        "module": 2,
        "name": "Page Triage",
        "bucket_changes": bucket_deltas,
        "total_pages_analyzed": {
            "current": curr_summary.get("total_pages_analyzed", 0),
            "baseline": base_summary.get("total_pages_analyzed", 0),
        },
        "recoverable_clicks": {
            "current": curr_summary.get("total_recoverable_clicks_monthly", 0),
            "baseline": base_summary.get("total_recoverable_clicks_monthly", 0),
            "delta": _delta(
                curr_summary.get("total_recoverable_clicks_monthly"),
                base_summary.get("total_recoverable_clicks_monthly"),
            ),
        },
        "new_critical_pages": new_critical[:20],
        "recovered_pages": recovered[:20],
    }


def _compare_module_3(current: Dict, baseline: Dict) -> Dict[str, Any]:
    """SERP Landscape delta."""
    return {
        "module": 3,
        "name": "SERP Landscape",
        "metrics": {
            "keywords_analyzed": {
                "current": _safe_get(current, "keywords_analyzed"),
                "baseline": _safe_get(baseline, "keywords_analyzed"),
            },
            "total_click_share": {
                "current": _safe_get(current, "total_click_share"),
                "baseline": _safe_get(baseline, "total_click_share"),
                "delta": _delta(
                    _safe_get(current, "total_click_share"),
                    _safe_get(baseline, "total_click_share"),
                ),
                "direction": _direction_label(
                    _delta(
                        _safe_get(current, "total_click_share"),
                        _safe_get(baseline, "total_click_share"),
                    )
                ),
            },
            "click_share_opportunity": {
                "current": _safe_get(current, "click_share_opportunity"),
                "baseline": _safe_get(baseline, "click_share_opportunity"),
            },
        },
        "competitors": _compare_lists_by_key(
            _safe_get(current, "competitors", default=[]) or [],
            _safe_get(baseline, "competitors", default=[]) or [],
            "domain",
            "keywords_shared",
        ),
    }


def _compare_module_4(current: Dict, baseline: Dict) -> Dict[str, Any]:
    """Content Intelligence delta."""
    curr_cannibal = _safe_get(current, "cannibalization_clusters", default=[]) or []
    base_cannibal = _safe_get(baseline, "cannibalization_clusters", default=[]) or []
    curr_striking = _safe_get(current, "striking_distance", default=[]) or []
    base_striking = _safe_get(baseline, "striking_distance", default=[]) or []
    curr_thin = _safe_get(current, "thin_content", default=[]) or []
    base_thin = _safe_get(baseline, "thin_content", default=[]) or []

    return {
        "module": 4,
        "name": "Content Intelligence",
        "cannibalization": {
            "current_clusters": len(curr_cannibal),
            "baseline_clusters": len(base_cannibal),
            "delta": len(curr_cannibal) - len(base_cannibal),
        },
        "striking_distance": {
            "current_count": len(curr_striking),
            "baseline_count": len(base_striking),
            "delta": len(curr_striking) - len(base_striking),
            "top_new": [
                s for s in curr_striking
                if s.get("query") not in [b.get("query") for b in base_striking]
            ][:10],
        },
        "thin_content": {
            "current_count": len(curr_thin),
            "baseline_count": len(base_thin),
            "delta": len(curr_thin) - len(base_thin),
        },
    }


def _compare_module_5(current: Dict, baseline: Dict) -> Dict[str, Any]:
    """Gameplan delta."""
    categories = ["critical", "quick_wins", "strategic", "structural"]
    action_counts = {}
    for cat in categories:
        cv = len(_safe_get(current, cat, default=[]) or [])
        bv = len(_safe_get(baseline, cat, default=[]) or [])
        action_counts[cat] = {"current": cv, "baseline": bv, "delta": cv - bv}

    return {
        "module": 5,
        "name": "Game Plan",
        "action_counts": action_counts,
        "estimated_recovery": {
            "current": _safe_get(current, "total_estimated_monthly_click_recovery"),
            "baseline": _safe_get(baseline, "total_estimated_monthly_click_recovery"),
            "delta": _delta(
                _safe_get(current, "total_estimated_monthly_click_recovery"),
                _safe_get(baseline, "total_estimated_monthly_click_recovery"),
            ),
        },
        "estimated_growth": {
            "current": _safe_get(current, "total_estimated_monthly_click_growth"),
            "baseline": _safe_get(baseline, "total_estimated_monthly_click_growth"),
            "delta": _delta(
                _safe_get(current, "total_estimated_monthly_click_growth"),
                _safe_get(baseline, "total_estimated_monthly_click_growth"),
            ),
        },
    }


def _compare_module_6(current: Dict, baseline: Dict) -> Dict[str, Any]:
    """Algorithm Impact delta."""
    curr_updates = _safe_get(current, "updates_impacting_site", default=[]) or []
    base_updates = _safe_get(baseline, "updates_impacting_site", default=[]) or []

    return {
        "module": 6,
        "name": "Algorithm Impact",
        "vulnerability_score": {
            "current": _safe_get(current, "vulnerability_score"),
            "baseline": _safe_get(baseline, "vulnerability_score"),
            "delta": _delta(
                _safe_get(current, "vulnerability_score"),
                _safe_get(baseline, "vulnerability_score"),
            ),
            "direction": _direction_label(
                # Lower vulnerability is better, so invert
                _delta(
                    _safe_get(baseline, "vulnerability_score"),
                    _safe_get(current, "vulnerability_score"),
                )
            ),
        },
        "impacting_updates": {
            "current_count": len(curr_updates),
            "baseline_count": len(base_updates),
            "new_impacts": [
                u for u in curr_updates
                if u.get("update_name") not in [b.get("update_name") for b in base_updates]
            ],
        },
    }


def _compare_module_7(current: Dict, baseline: Dict) -> Dict[str, Any]:
    """Intent Migration delta."""
    curr_dist = _safe_get(current, "intent_distribution_current", default={}) or {}
    base_dist = _safe_get(baseline, "intent_distribution_current", default={}) or {}

    intent_shifts = {}
    for intent in ["informational", "commercial", "navigational", "transactional"]:
        cv = curr_dist.get(intent)
        bv = base_dist.get(intent)
        intent_shifts[intent] = {
            "current": cv,
            "baseline": bv,
            "delta": _delta(cv, bv),
        }

    return {
        "module": 7,
        "name": "Intent Migration",
        "intent_shifts": intent_shifts,
        "ai_overview_impact": {
            "queries_affected": {
                "current": _safe_get(current, "ai_overview_impact", "queries_affected"),
                "baseline": _safe_get(baseline, "ai_overview_impact", "queries_affected"),
                "delta": _delta(
                    _safe_get(current, "ai_overview_impact", "queries_affected"),
                    _safe_get(baseline, "ai_overview_impact", "queries_affected"),
                ),
            },
            "estimated_clicks_lost": {
                "current": _safe_get(current, "ai_overview_impact", "estimated_monthly_clicks_lost"),
                "baseline": _safe_get(baseline, "ai_overview_impact", "estimated_monthly_clicks_lost"),
                "delta": _delta(
                    _safe_get(current, "ai_overview_impact", "estimated_monthly_clicks_lost"),
                    _safe_get(baseline, "ai_overview_impact", "estimated_monthly_clicks_lost"),
                ),
            },
        },
    }


def _compare_module_8(current: Dict, baseline: Dict) -> Dict[str, Any]:
    """CTR Modeling delta."""
    return {
        "module": 8,
        "name": "CTR Modeling",
        "model_accuracy": {
            "current": _safe_get(current, "ctr_model_accuracy"),
            "baseline": _safe_get(baseline, "ctr_model_accuracy"),
        },
        "keyword_performance": _compare_lists_by_key(
            _safe_get(current, "keyword_ctr_analysis", default=[]) or [],
            _safe_get(baseline, "keyword_ctr_analysis", default=[]) or [],
            "keyword",
            "actual_ctr",
        ),
        "feature_opportunities": {
            "current_count": len(_safe_get(current, "feature_opportunities", default=[]) or []),
            "baseline_count": len(_safe_get(baseline, "feature_opportunities", default=[]) or []),
        },
    }


def _compare_module_9(current: Dict, baseline: Dict) -> Dict[str, Any]:
    """Site Architecture delta."""
    return {
        "module": 9,
        "name": "Site Architecture",
        "authority_flow_to_conversion": {
            "current": _safe_get(current, "authority_flow_to_conversion"),
            "baseline": _safe_get(baseline, "authority_flow_to_conversion"),
            "delta": _delta(
                _safe_get(current, "authority_flow_to_conversion"),
                _safe_get(baseline, "authority_flow_to_conversion"),
            ),
        },
        "orphan_pages": {
            "current_count": len(_safe_get(current, "orphan_pages", default=[]) or []),
            "baseline_count": len(_safe_get(baseline, "orphan_pages", default=[]) or []),
        },
        "content_silos": {
            "current_count": len(_safe_get(current, "content_silos", default=[]) or []),
            "baseline_count": len(_safe_get(baseline, "content_silos", default=[]) or []),
        },
        "link_recommendations": {
            "current_count": len(_safe_get(current, "link_recommendations", default=[]) or []),
            "baseline_count": len(_safe_get(baseline, "link_recommendations", default=[]) or []),
        },
    }


def _compare_module_10(current: Dict, baseline: Dict) -> Dict[str, Any]:
    """Branded vs Non-Branded delta."""
    return {
        "module": 10,
        "name": "Branded vs Non-Branded",
        "branded_ratio": {
            "current": _safe_get(current, "branded_ratio"),
            "baseline": _safe_get(baseline, "branded_ratio"),
            "delta": _delta(
                _safe_get(current, "branded_ratio"),
                _safe_get(baseline, "branded_ratio"),
            ),
        },
        "dependency_level": {
            "current": _safe_get(current, "dependency_level"),
            "baseline": _safe_get(baseline, "dependency_level"),
            "changed": _safe_get(current, "dependency_level") != _safe_get(baseline, "dependency_level"),
        },
        "non_branded_opportunity": {
            "current_gap": _safe_get(current, "non_branded_opportunity", "gap"),
            "baseline_gap": _safe_get(baseline, "non_branded_opportunity", "gap"),
            "delta": _delta(
                _safe_get(current, "non_branded_opportunity", "gap"),
                _safe_get(baseline, "non_branded_opportunity", "gap"),
            ),
        },
        "non_branded_trend": {
            "current_slope": _safe_get(current, "non_branded_trend", "slope"),
            "baseline_slope": _safe_get(baseline, "non_branded_trend", "slope"),
            "delta": _delta(
                _safe_get(current, "non_branded_trend", "slope"),
                _safe_get(baseline, "non_branded_trend", "slope"),
            ),
        },
    }


def _compare_module_11(current: Dict, baseline: Dict) -> Dict[str, Any]:
    """Competitive Threats delta."""
    curr_threats = _safe_get(current, "emerging_threats", default=[]) or []
    base_threats = _safe_get(baseline, "emerging_threats", default=[]) or []

    return {
        "module": 11,
        "name": "Competitive Threats",
        "primary_competitors": _compare_lists_by_key(
            _safe_get(current, "primary_competitors", default=[]) or [],
            _safe_get(baseline, "primary_competitors", default=[]) or [],
            "domain",
            "keyword_overlap",
        ),
        "emerging_threats": {
            "current_count": len(curr_threats),
            "baseline_count": len(base_threats),
            "new_threats": [
                t for t in curr_threats
                if t.get("domain") not in [b.get("domain") for b in base_threats]
            ][:10],
        },
        "keyword_vulnerability": _compare_lists_by_key(
            _safe_get(current, "keyword_vulnerability", default=[]) or [],
            _safe_get(baseline, "keyword_vulnerability", default=[]) or [],
            "keyword",
            "competitors_within_3",
        ),
    }


def _compare_module_12(current: Dict, baseline: Dict) -> Dict[str, Any]:
    """Revenue Attribution delta."""
    return {
        "module": 12,
        "name": "Revenue Attribution",
        "total_revenue": {
            "current": _safe_get(current, "total_search_attributed_revenue_monthly"),
            "baseline": _safe_get(baseline, "total_search_attributed_revenue_monthly"),
            "delta": _delta(
                _safe_get(current, "total_search_attributed_revenue_monthly"),
                _safe_get(baseline, "total_search_attributed_revenue_monthly"),
            ),
            "pct_change": _pct_change(
                _safe_get(current, "total_search_attributed_revenue_monthly"),
                _safe_get(baseline, "total_search_attributed_revenue_monthly"),
            ),
        },
        "revenue_at_risk": {
            "current": _safe_get(current, "revenue_at_risk_90d"),
            "baseline": _safe_get(baseline, "revenue_at_risk_90d"),
            "delta": _delta(
                _safe_get(current, "revenue_at_risk_90d"),
                _safe_get(baseline, "revenue_at_risk_90d"),
            ),
        },
        "action_roi": {
            "critical_fixes": {
                "current": _safe_get(current, "action_roi", "critical_fixes_monthly_value"),
                "baseline": _safe_get(baseline, "action_roi", "critical_fixes_monthly_value"),
                "delta": _delta(
                    _safe_get(current, "action_roi", "critical_fixes_monthly_value"),
                    _safe_get(baseline, "action_roi", "critical_fixes_monthly_value"),
                ),
            },
            "total_opportunity": {
                "current": _safe_get(current, "action_roi", "total_opportunity"),
                "baseline": _safe_get(baseline, "action_roi", "total_opportunity"),
                "delta": _delta(
                    _safe_get(current, "action_roi", "total_opportunity"),
                    _safe_get(baseline, "action_roi", "total_opportunity"),
                ),
                "pct_change": _pct_change(
                    _safe_get(current, "action_roi", "total_opportunity"),
                    _safe_get(baseline, "action_roi", "total_opportunity"),
                ),
            },
        },
        "top_revenue_keywords": _compare_lists_by_key(
            _safe_get(current, "top_revenue_keywords", default=[]) or [],
            _safe_get(baseline, "top_revenue_keywords", default=[]) or [],
            "keyword",
            "current_revenue_monthly",
        ),
    }


# ---------------------------------------------------------------------------
# Module comparison dispatch
# ---------------------------------------------------------------------------

_MODULE_COMPARATORS = {
    1: _compare_module_1,
    2: _compare_module_2,
    3: _compare_module_3,
    4: _compare_module_4,
    5: _compare_module_5,
    6: _compare_module_6,
    7: _compare_module_7,
    8: _compare_module_8,
    9: _compare_module_9,
    10: _compare_module_10,
    11: _compare_module_11,
    12: _compare_module_12,
}


# ---------------------------------------------------------------------------
# Executive summary generation
# ---------------------------------------------------------------------------

def _generate_executive_summary(module_deltas: List[Dict]) -> Dict[str, Any]:
    """
    Produce a high-level executive summary of what changed between reports.

    Extracts the most important signals across all modules and presents
    them as a concise narrative-ready structure.
    """
    highlights: List[Dict[str, Any]] = []
    warnings: List[Dict[str, Any]] = []

    for md in module_deltas:
        mod_num = md.get("module", 0)

        # Module 1: Trend direction changed
        if mod_num == 1:
            direction_data = _safe_get(md, "metrics", "overall_direction", default={})
            if direction_data and direction_data.get("changed"):
                item = {
                    "module": 1,
                    "signal": "trend_direction_changed",
                    "from": direction_data.get("baseline"),
                    "to": direction_data.get("current"),
                }
                if direction_data.get("current") in ("declining", "strong_decline"):
                    warnings.append(item)
                else:
                    highlights.append(item)

            slope_data = _safe_get(md, "metrics", "trend_slope_pct_per_month", default={})
            if slope_data and slope_data.get("delta") is not None:
                item = {
                    "module": 1,
                    "signal": "trend_slope_shift",
                    "delta": slope_data["delta"],
                    "direction": slope_data.get("direction"),
                }
                if slope_data["delta"] < -1:
                    warnings.append(item)
                elif slope_data["delta"] > 1:
                    highlights.append(item)

        # Module 2: New critical pages
        if mod_num == 2:
            new_critical = md.get("new_critical_pages", [])
            if new_critical:
                warnings.append({
                    "module": 2,
                    "signal": "new_critical_pages",
                    "count": len(new_critical),
                    "pages": new_critical[:5],
                })
            recovered = md.get("recovered_pages", [])
            if recovered:
                highlights.append({
                    "module": 2,
                    "signal": "pages_recovered",
                    "count": len(recovered),
                    "pages": recovered[:5],
                })

        # Module 6: Vulnerability score change
        if mod_num == 6:
            vuln_data = _safe_get(md, "vulnerability_score", default={})
            if vuln_data and vuln_data.get("delta") is not None:
                if vuln_data["delta"] > 0.1:
                    warnings.append({
                        "module": 6,
                        "signal": "vulnerability_increased",
                        "delta": vuln_data["delta"],
                    })
                elif vuln_data["delta"] < -0.1:
                    highlights.append({
                        "module": 6,
                        "signal": "vulnerability_decreased",
                        "delta": vuln_data["delta"],
                    })

        # Module 10: Branded dependency change
        if mod_num == 10:
            dep_data = _safe_get(md, "dependency_level", default={})
            if dep_data and dep_data.get("changed"):
                item = {
                    "module": 10,
                    "signal": "dependency_level_changed",
                    "from": dep_data.get("baseline"),
                    "to": dep_data.get("current"),
                }
                if dep_data.get("current") in ("critical", "high_dependency"):
                    warnings.append(item)
                else:
                    highlights.append(item)

        # Module 12: Revenue change
        if mod_num == 12:
            rev_data = _safe_get(md, "total_revenue", default={})
            if rev_data and rev_data.get("delta") is not None:
                item = {
                    "module": 12,
                    "signal": "revenue_changed",
                    "delta": rev_data["delta"],
                    "pct_change": rev_data.get("pct_change"),
                }
                if rev_data["delta"] < 0:
                    warnings.append(item)
                else:
                    highlights.append(item)

            risk_data = _safe_get(md, "revenue_at_risk", default={})
            if risk_data and risk_data.get("delta") is not None and risk_data["delta"] > 500:
                warnings.append({
                    "module": 12,
                    "signal": "revenue_risk_increased",
                    "delta": risk_data["delta"],
                })

    return {
        "highlights": highlights,
        "warnings": warnings,
        "total_highlights": len(highlights),
        "total_warnings": len(warnings),
        "overall_sentiment": (
            "declining" if len(warnings) > len(highlights) + 2
            else "improving" if len(highlights) > len(warnings) + 2
            else "mixed"
        ),
    }


# ---------------------------------------------------------------------------
# Main comparison function
# ---------------------------------------------------------------------------

def compare_reports(
    current_modules: Dict[int, Dict[str, Any]],
    baseline_modules: Dict[int, Dict[str, Any]],
    current_meta: Optional[Dict[str, Any]] = None,
    baseline_meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Compare two completed reports and return a structured delta.

    Args:
        current_modules:  Module results keyed by module number (1-12)
                          for the current (newer) report.
        baseline_modules: Module results keyed by module number (1-12)
                          for the baseline (older) report.
        current_meta:     Optional report metadata (domain, created_at, etc.)
        baseline_meta:    Optional baseline report metadata.

    Returns:
        A dict containing:
          - metadata: report IDs, domains, dates
          - executive_summary: highlights and warnings
          - module_deltas: per-module structured comparison (list of 12 items)
          - modules_compared: count of modules with data in both reports
          - modules_missing: list of module numbers with insufficient data
    """
    current_meta = current_meta or {}
    baseline_meta = baseline_meta or {}

    module_deltas: List[Dict[str, Any]] = []
    modules_missing: List[int] = []

    for mod_num in range(1, 13):
        comparator = _MODULE_COMPARATORS.get(mod_num)
        if not comparator:
            modules_missing.append(mod_num)
            continue

        curr_data = current_modules.get(mod_num) or {}
        base_data = baseline_modules.get(mod_num) or {}

        if not curr_data and not base_data:
            modules_missing.append(mod_num)
            module_deltas.append({
                "module": mod_num,
                "name": _MODULE_COMPARATORS.get(mod_num, lambda c, b: {}).__doc__ or f"Module {mod_num}",
                "status": "no_data",
            })
            continue

        try:
            delta = comparator(curr_data, base_data)
            delta["status"] = "compared"
            module_deltas.append(delta)
        except Exception as exc:
            logger.warning("Failed to compare module %d: %s", mod_num, exc)
            module_deltas.append({
                "module": mod_num,
                "status": "error",
                "error": str(exc),
            })
            modules_missing.append(mod_num)

    executive_summary = _generate_executive_summary(module_deltas)

    return {
        "metadata": {
            "current_report_id": current_meta.get("id"),
            "baseline_report_id": baseline_meta.get("id"),
            "current_domain": current_meta.get("domain"),
            "baseline_domain": baseline_meta.get("domain"),
            "current_created_at": current_meta.get("created_at"),
            "baseline_created_at": baseline_meta.get("created_at"),
            "compared_at": datetime.utcnow().isoformat(),
        },
        "executive_summary": executive_summary,
        "module_deltas": module_deltas,
        "modules_compared": 12 - len(modules_missing),
        "modules_missing": modules_missing,
    }
