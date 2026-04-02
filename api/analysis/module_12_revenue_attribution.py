"""
Module 12: Revenue Attribution & ROI Modeling

Maps search queries and landing pages to conversions and revenue, quantifies
revenue at risk from declining pages, estimates ROI of position improvements,
and generates prioritised action recommendations with projected returns.

Data inputs (from routes/modules.py):
  - gsc_data: GSC query-level data with clicks, impressions, position, CTR, page
  - ga4_conversions: GA4 conversion/goal data per page (conversions, conversion_rate)
  - ga4_engagement: GA4 engagement data per page (sessions, bounce_rate, avg_session_duration, pages_per_session)
  - ga4_ecommerce: GA4 ecommerce data per page (transactions, revenue, avg_order_value) — optional
"""

import logging
import math
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CTR benchmark curve (position -> expected organic CTR)
# ---------------------------------------------------------------------------
_CTR_BENCHMARKS = {
    1: 0.316, 2: 0.241, 3: 0.187, 4: 0.133, 5: 0.095,
    6: 0.063, 7: 0.046, 8: 0.031, 9: 0.022, 10: 0.017,
    11: 0.012, 12: 0.010, 13: 0.008, 14: 0.007, 15: 0.006,
    16: 0.005, 17: 0.004, 18: 0.004, 19: 0.003, 20: 0.003,
}


def _expected_ctr(position: float) -> float:
    """Interpolated CTR for a fractional position."""
    if position <= 1:
        return _CTR_BENCHMARKS[1]
    if position >= 20:
        return _CTR_BENCHMARKS[20]
    low = int(position)
    high = low + 1
    frac = position - low
    return _CTR_BENCHMARKS.get(low, 0.003) * (1 - frac) + _CTR_BENCHMARKS.get(high, 0.003) * frac


def _page_key(url: str) -> str:
    """Normalise a URL to a path key for matching GA4 data."""
    try:
        parsed = urlparse(url)
        return parsed.path.rstrip("/").lower() or "/"
    except Exception:
        return url.rstrip("/").lower()


# ---------------------------------------------------------------------------
# Revenue Attribution Analyzer
# ---------------------------------------------------------------------------

class RevenueAttributionAnalyzer:
    """
    Comprehensive revenue attribution engine.

    Combines GSC search performance with GA4 conversion / ecommerce data to
    attribute revenue to individual queries and pages, model position-
    improvement ROI, and flag revenue at risk.
    """

    def __init__(
        self,
        gsc_data: Any,
        ga4_conversions: Any = None,
        ga4_engagement: Any = None,
        ga4_ecommerce: Any = None,
    ):
        self.gsc_rows = self._normalise_gsc(gsc_data)
        self.conversion_map = self._build_page_map(ga4_conversions)
        self.engagement_map = self._build_page_map(ga4_engagement)
        self.ecommerce_map = self._build_page_map(ga4_ecommerce)
        self.has_ecommerce = bool(self.ecommerce_map)
        self.has_conversions = bool(self.conversion_map)

    # ------------------------------------------------------------------
    # Normalisation helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _normalise_gsc(data: Any) -> List[Dict]:
        """Convert GSC data (DataFrame or list of dicts) to list of dicts."""
        if data is None:
            return []
        try:
            if hasattr(data, "to_dict"):
                return data.to_dict("records")
        except Exception:
            pass
        if isinstance(data, list):
            return data
        return []

    @staticmethod
    def _build_page_map(data: Any) -> Dict[str, Dict]:
        """Convert GA4 data to {page_path: metrics} dict."""
        if data is None:
            return {}
        rows = data
        try:
            if hasattr(data, "to_dict"):
                rows = data.to_dict("records")
        except Exception:
            pass
        if not isinstance(rows, list):
            return {}
        result: Dict[str, Dict] = {}
        for row in rows:
            page = (
                row.get("page")
                or row.get("page_path")
                or row.get("landing_page")
                or row.get("url")
                or ""
            )
            if page:
                result[_page_key(page)] = row
        return result

    # ------------------------------------------------------------------
    # Core analysis methods
    # ------------------------------------------------------------------

    def _aggregate_by_page(self) -> Dict[str, Dict]:
        """Aggregate GSC metrics per landing page."""
        pages: Dict[str, Dict] = defaultdict(lambda: {
            "clicks": 0, "impressions": 0, "position_sum": 0.0,
            "queries": [], "query_count": 0,
        })
        for row in self.gsc_rows:
            page = row.get("page") or row.get("url") or ""
            if not page:
                continue
            key = _page_key(page)
            p = pages[key]
            clicks = float(row.get("clicks", 0))
            impressions = float(row.get("impressions", 0))
            position = float(row.get("position", 0))
            p["clicks"] += clicks
            p["impressions"] += impressions
            p["position_sum"] += position * impressions
            p["query_count"] += 1
            p["queries"].append({
                "query": row.get("query", ""),
                "clicks": clicks,
                "impressions": impressions,
                "position": position,
                "ctr": float(row.get("ctr", 0)),
            })
        for key, p in pages.items():
            p["avg_position"] = round(p["position_sum"] / max(p["impressions"], 1), 2)
            p["ctr"] = round(p["clicks"] / max(p["impressions"], 1), 4)
            p["page"] = key
            p["queries"].sort(key=lambda q: q["clicks"], reverse=True)
        return dict(pages)

    def _aggregate_by_query(self) -> Dict[str, Dict]:
        """Aggregate GSC metrics per query across all pages."""
        queries: Dict[str, Dict] = defaultdict(lambda: {
            "clicks": 0, "impressions": 0, "position_sum": 0.0,
            "pages": set(),
        })
        for row in self.gsc_rows:
            query = row.get("query", "")
            if not query:
                continue
            q = queries[query]
            clicks = float(row.get("clicks", 0))
            impressions = float(row.get("impressions", 0))
            position = float(row.get("position", 0))
            q["clicks"] += clicks
            q["impressions"] += impressions
            q["position_sum"] += position * impressions
            page = row.get("page") or row.get("url") or ""
            if page:
                q["pages"].add(_page_key(page))
        for qname, q in queries.items():
            q["avg_position"] = round(q["position_sum"] / max(q["impressions"], 1), 2)
            q["ctr"] = round(q["clicks"] / max(q["impressions"], 1), 4)
            q["page_count"] = len(q["pages"])
            q["pages"] = list(q["pages"])[:5]
            q["query"] = qname
        return dict(queries)

    def _compute_page_revenue(self, page_agg: Dict[str, Dict]) -> List[Dict]:
        """
        Attribute revenue to each page using GA4 conversion/ecommerce data.

        Attribution model:
        - If ecommerce revenue data exists -> use actual revenue
        - If conversion data exists -> estimate value per conversion x conversions
        - Fallback -> no revenue attributed
        """
        results = []

        for page_path, gsc in page_agg.items():
            entry: Dict[str, Any] = {
                "page": page_path,
                "clicks": gsc["clicks"],
                "impressions": gsc["impressions"],
                "avg_position": gsc["avg_position"],
                "ctr": gsc["ctr"],
                "query_count": gsc["query_count"],
                "top_queries": [q["query"] for q in gsc["queries"][:5]],
            }

            ecom = self.ecommerce_map.get(page_path, {})
            conv = self.conversion_map.get(page_path, {})
            eng = self.engagement_map.get(page_path, {})

            actual_revenue = float(ecom.get("revenue", 0))
            transactions = float(ecom.get("transactions", 0))
            aov = float(ecom.get("avg_order_value", 0))
            conversions = float(conv.get("conversions", 0))
            conversion_rate = float(conv.get("conversion_rate", 0))
            sessions = float(eng.get("sessions", 0))
            bounce_rate = float(eng.get("bounce_rate", 0))
            avg_duration = float(eng.get("avg_session_duration", 0))

            if actual_revenue > 0:
                entry["revenue"] = round(actual_revenue, 2)
                entry["revenue_source"] = "ecommerce_actual"
                entry["transactions"] = transactions
                entry["aov"] = round(aov, 2)
            elif conversions > 0 and aov > 0:
                entry["revenue"] = round(conversions * aov, 2)
                entry["revenue_source"] = "conversion_estimated"
            elif conversions > 0:
                entry["revenue"] = round(conversions * 50, 2)
                entry["revenue_source"] = "conversion_default_value"
            else:
                entry["revenue"] = 0
                entry["revenue_source"] = "no_conversion_data"

            entry["conversions"] = conversions
            entry["conversion_rate"] = round(conversion_rate, 4)
            entry["sessions"] = sessions
            entry["bounce_rate"] = round(bounce_rate, 4)
            entry["avg_session_duration"] = round(avg_duration, 1)
            entry["revenue_per_click"] = round(
                entry["revenue"] / max(gsc["clicks"], 1), 2
            )

            results.append(entry)

        results.sort(key=lambda x: x.get("revenue", 0), reverse=True)
        return results

    def _compute_query_revenue(
        self, query_agg: Dict[str, Dict], page_revenue: List[Dict]
    ) -> List[Dict]:
        """
        Attribute revenue to queries by distributing page revenue
        proportionally based on click share per page.
        """
        page_rev_map = {p["page"]: p.get("revenue", 0) for p in page_revenue}
        page_click_map = {p["page"]: p["clicks"] for p in page_revenue}

        # Pre-build query-page click index for O(1) lookup
        qp_clicks: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
        for row in self.gsc_rows:
            qname = row.get("query", "")
            page = row.get("page") or row.get("url") or ""
            if qname and page:
                qp_clicks[qname][_page_key(page)] += float(row.get("clicks", 0))

        results = []
        for qname, q in query_agg.items():
            attributed_revenue = 0.0
            for page in q["pages"]:
                page_total_clicks = page_click_map.get(page, 0)
                page_rev = page_rev_map.get(page, 0)
                if page_total_clicks > 0 and page_rev > 0:
                    query_clicks_to_page = qp_clicks.get(qname, {}).get(page, 0)
                    share = query_clicks_to_page / page_total_clicks
                    attributed_revenue += page_rev * share

            results.append({
                "query": qname,
                "clicks": q["clicks"],
                "impressions": q["impressions"],
                "avg_position": q["avg_position"],
                "ctr": q["ctr"],
                "page_count": q["page_count"],
                "attributed_revenue": round(attributed_revenue, 2),
                "revenue_per_click": round(
                    attributed_revenue / max(q["clicks"], 1), 2
                ),
            })

        results.sort(key=lambda x: x["attributed_revenue"], reverse=True)
        return results[:50]

    def _revenue_at_risk(self, page_revenue: List[Dict]) -> List[Dict]:
        """
        Identify pages with revenue at risk from poor or declining performance.

        Risk factors:
        - High bounce rate (>80%)
        - Low avg session duration (<15s)
        - Position slipping (avg > 10)
        - Low CTR relative to position benchmark
        - High revenue concentration on few queries
        """
        at_risk = []
        for page in page_revenue:
            if page.get("revenue", 0) <= 0:
                continue

            risk_factors = []
            risk_score = 0.0

            br = page.get("bounce_rate", 0)
            if br > 0.80:
                risk_factors.append("high_bounce_rate")
                risk_score += 20
            elif br > 0.65:
                risk_factors.append("elevated_bounce_rate")
                risk_score += 10

            dur = page.get("avg_session_duration", 0)
            if 0 < dur < 15:
                risk_factors.append("very_low_engagement")
                risk_score += 20
            elif 0 < dur < 30:
                risk_factors.append("low_engagement")
                risk_score += 10

            pos = page.get("avg_position", 0)
            if pos > 15:
                risk_factors.append("deep_position")
                risk_score += 25
            elif pos > 10:
                risk_factors.append("declining_position")
                risk_score += 15

            expected = _expected_ctr(pos)
            actual_ctr = page.get("ctr", 0)
            if expected > 0 and actual_ctr < expected * 0.5:
                risk_factors.append("ctr_significantly_below_benchmark")
                risk_score += 20
            elif expected > 0 and actual_ctr < expected * 0.75:
                risk_factors.append("ctr_below_benchmark")
                risk_score += 10

            qcount = page.get("query_count", 0)
            if qcount <= 2 and page["clicks"] > 50:
                risk_factors.append("query_concentration")
                risk_score += 15

            if risk_factors:
                severity = (
                    "critical" if risk_score >= 50
                    else "high" if risk_score >= 30
                    else "moderate"
                )
                at_risk.append({
                    "page": page["page"],
                    "revenue": page["revenue"],
                    "revenue_per_click": page.get("revenue_per_click", 0),
                    "risk_score": round(risk_score, 1),
                    "severity": severity,
                    "risk_factors": risk_factors,
                    "clicks": page["clicks"],
                    "avg_position": page["avg_position"],
                    "bounce_rate": page.get("bounce_rate", 0),
                    "top_queries": page.get("top_queries", [])[:3],
                })

        at_risk.sort(key=lambda x: x["revenue"] * x["risk_score"], reverse=True)
        return at_risk[:30]

    def _position_improvement_roi(self, query_revenue: List[Dict]) -> List[Dict]:
        """
        Estimate ROI of moving each query up in position.

        For queries in positions 4-30, estimate additional clicks and revenue
        if improved to top 3 / top 5 / position 1.
        """
        opportunities = []
        for q in query_revenue:
            pos = q["avg_position"]
            if pos < 4 or pos > 30:
                continue
            if q["impressions"] < 50:
                continue

            current_ctr = _expected_ctr(pos)
            rpc = q["revenue_per_click"]

            scenarios = []
            for target_pos, label in [(1, "position_1"), (3, "top_3"), (5, "top_5")]:
                if target_pos >= pos:
                    continue
                target_ctr = _expected_ctr(target_pos)
                additional_clicks = q["impressions"] * (target_ctr - current_ctr)
                if additional_clicks <= 0:
                    continue
                additional_revenue = additional_clicks * rpc
                scenarios.append({
                    "target": label,
                    "target_position": target_pos,
                    "estimated_additional_clicks": round(additional_clicks, 0),
                    "estimated_additional_revenue": round(additional_revenue, 2),
                    "ctr_improvement": round(target_ctr - current_ctr, 4),
                })

            if scenarios:
                best = max(scenarios, key=lambda s: s["estimated_additional_revenue"])
                opportunities.append({
                    "query": q["query"],
                    "current_position": pos,
                    "clicks": q["clicks"],
                    "impressions": q["impressions"],
                    "current_revenue": q["attributed_revenue"],
                    "revenue_per_click": rpc,
                    "scenarios": scenarios,
                    "best_scenario_revenue": best["estimated_additional_revenue"],
                    "best_scenario_target": best["target"],
                    "priority_score": round(
                        best["estimated_additional_revenue"]
                        * math.log1p(q["impressions"])
                        / max(pos, 1),
                        2,
                    ),
                })

        opportunities.sort(key=lambda x: x["priority_score"], reverse=True)
        return opportunities[:40]

    def _conversion_funnel_analysis(self, page_revenue: List[Dict]) -> Dict[str, Any]:
        """
        Analyse the search-to-conversion funnel across all pages.

        Segments pages into performance tiers and identifies funnel leaks.
        """
        total_clicks = sum(p["clicks"] for p in page_revenue) or 1
        total_revenue = sum(p.get("revenue", 0) for p in page_revenue)
        total_conversions = sum(p.get("conversions", 0) for p in page_revenue)
        total_sessions = sum(p.get("sessions", 0) for p in page_revenue) or 1

        high_value = [
            p for p in page_revenue
            if p.get("revenue", 0) > 0 and p.get("conversion_rate", 0) > 0.03
        ]
        mid_value = [
            p for p in page_revenue
            if p.get("revenue", 0) > 0 and p.get("conversion_rate", 0) <= 0.03
        ]
        traffic_only = [
            p for p in page_revenue
            if p.get("revenue", 0) == 0 and p["clicks"] > 20
        ]
        low_traffic = [
            p for p in page_revenue if p["clicks"] <= 20
        ]

        leaks = []
        for p in page_revenue:
            if p["clicks"] > 50 and p.get("conversions", 0) == 0 and p.get("bounce_rate", 0) > 0.7:
                leaks.append({
                    "page": p["page"],
                    "clicks": p["clicks"],
                    "bounce_rate": p.get("bounce_rate", 0),
                    "issue": "high_traffic_no_conversions",
                    "potential_revenue": round(p["clicks"] * 0.02 * 50, 2),
                })
            elif p["clicks"] > 30 and p.get("conversion_rate", 0) > 0 and p.get("bounce_rate", 0) > 0.8:
                leaks.append({
                    "page": p["page"],
                    "clicks": p["clicks"],
                    "bounce_rate": p.get("bounce_rate", 0),
                    "conversion_rate": p.get("conversion_rate", 0),
                    "issue": "high_bounce_despite_conversions",
                    "potential_uplift": round(
                        p.get("revenue", 0) * (p["bounce_rate"] - 0.5) / max(1 - p["bounce_rate"], 0.1), 2
                    ),
                })

        leaks.sort(
            key=lambda x: x.get("potential_revenue", 0) + x.get("potential_uplift", 0),
            reverse=True,
        )

        return {
            "total_clicks": total_clicks,
            "total_revenue": round(total_revenue, 2),
            "total_conversions": total_conversions,
            "overall_conversion_rate": round(total_conversions / total_sessions, 4),
            "avg_revenue_per_click": round(total_revenue / total_clicks, 2),
            "tiers": {
                "high_value": {
                    "count": len(high_value),
                    "clicks": sum(p["clicks"] for p in high_value),
                    "revenue": round(sum(p.get("revenue", 0) for p in high_value), 2),
                },
                "mid_value": {
                    "count": len(mid_value),
                    "clicks": sum(p["clicks"] for p in mid_value),
                    "revenue": round(sum(p.get("revenue", 0) for p in mid_value), 2),
                },
                "traffic_only": {
                    "count": len(traffic_only),
                    "clicks": sum(p["clicks"] for p in traffic_only),
                },
                "low_traffic": {
                    "count": len(low_traffic),
                    "clicks": sum(p["clicks"] for p in low_traffic),
                },
            },
            "funnel_leaks": leaks[:20],
        }

    def _revenue_concentration(
        self, page_revenue: List[Dict], query_revenue: List[Dict]
    ) -> Dict[str, Any]:
        """Measure revenue concentration risk (Pareto analysis)."""
        total_page_rev = sum(p.get("revenue", 0) for p in page_revenue) or 1
        total_query_rev = sum(q.get("attributed_revenue", 0) for q in query_revenue) or 1

        sorted_pages = sorted(
            page_revenue, key=lambda p: p.get("revenue", 0), reverse=True
        )
        cumulative = 0.0
        pages_for_80 = 0
        for p in sorted_pages:
            cumulative += p.get("revenue", 0)
            pages_for_80 += 1
            if cumulative >= total_page_rev * 0.80:
                break

        sorted_queries = sorted(
            query_revenue, key=lambda q: q.get("attributed_revenue", 0), reverse=True
        )
        q_cumulative = 0.0
        queries_for_80 = 0
        for q in sorted_queries:
            q_cumulative += q.get("attributed_revenue", 0)
            queries_for_80 += 1
            if q_cumulative >= total_query_rev * 0.80:
                break

        total_pages = len([p for p in page_revenue if p.get("revenue", 0) > 0])
        total_queries = len([q for q in query_revenue if q.get("attributed_revenue", 0) > 0])

        page_concentration = round(pages_for_80 / max(total_pages, 1), 4)

        if page_concentration < 0.10:
            page_risk = "critical"
        elif page_concentration < 0.20:
            page_risk = "high"
        elif page_concentration < 0.35:
            page_risk = "moderate"
        else:
            page_risk = "low"

        return {
            "pages_for_80_pct_revenue": pages_for_80,
            "total_revenue_pages": total_pages,
            "page_concentration_ratio": page_concentration,
            "page_concentration_risk": page_risk,
            "queries_for_80_pct_revenue": queries_for_80,
            "total_revenue_queries": total_queries,
            "query_concentration_ratio": round(queries_for_80 / max(total_queries, 1), 4),
            "top_5_pages_revenue_share": round(
                sum(p.get("revenue", 0) for p in sorted_pages[:5]) / total_page_rev, 4
            ),
            "top_10_queries_revenue_share": round(
                sum(q.get("attributed_revenue", 0) for q in sorted_queries[:10]) / total_query_rev, 4
            ),
        }

    def _generate_recommendations(
        self,
        page_revenue: List[Dict],
        query_revenue: List[Dict],
        at_risk: List[Dict],
        roi_opportunities: List[Dict],
        funnel: Dict,
        concentration: Dict,
    ) -> List[Dict]:
        """Generate prioritised, actionable recommendations with projected ROI."""
        recs: List[Dict] = []

        # 1. Revenue at risk
        critical_risk = [r for r in at_risk if r["severity"] == "critical"]
        if critical_risk:
            total_at_risk = sum(r["revenue"] for r in critical_risk)
            recs.append({
                "priority": 1,
                "category": "protect_revenue",
                "title": "Address critical revenue-at-risk pages",
                "description": (
                    f"{len(critical_risk)} pages with ${total_at_risk:,.0f} in attributed revenue "
                    f"show critical risk factors. Focus on improving engagement and CTR "
                    f"to protect this revenue."
                ),
                "impact": "high",
                "estimated_value": round(total_at_risk * 0.3, 2),
                "affected_pages": [r["page"] for r in critical_risk[:5]],
            })

        # 2. Position improvement ROI
        if roi_opportunities:
            top_opp = roi_opportunities[:10]
            total_potential = sum(o["best_scenario_revenue"] for o in top_opp)
            recs.append({
                "priority": 2,
                "category": "position_improvement",
                "title": "Improve rankings for high-revenue queries",
                "description": (
                    f"Top 10 position improvement opportunities could generate "
                    f"${total_potential:,.0f} in additional revenue. Focus on queries "
                    f"currently in positions 4-15 with strong revenue per click."
                ),
                "impact": "high",
                "estimated_value": round(total_potential, 2),
                "top_queries": [o["query"] for o in top_opp[:5]],
            })

        # 3. Funnel leak repair
        leaks = funnel.get("funnel_leaks", [])
        if leaks:
            leak_value = sum(
                l.get("potential_revenue", 0) + l.get("potential_uplift", 0)
                for l in leaks[:10]
            )
            recs.append({
                "priority": 3,
                "category": "funnel_optimization",
                "title": "Fix conversion funnel leaks",
                "description": (
                    f"{len(leaks)} pages identified with funnel leaks (high traffic, "
                    f"low or zero conversions). Improving CTAs, page speed, and content "
                    f"alignment could recover an estimated ${leak_value:,.0f}."
                ),
                "impact": "high" if leak_value > 1000 else "medium",
                "estimated_value": round(leak_value, 2),
                "affected_pages": [l["page"] for l in leaks[:5]],
            })

        # 4. Revenue concentration diversification
        if concentration.get("page_concentration_risk") in ("critical", "high"):
            recs.append({
                "priority": 4,
                "category": "diversification",
                "title": "Reduce revenue concentration risk",
                "description": (
                    f"80% of revenue comes from just {concentration['pages_for_80_pct_revenue']} "
                    f"pages out of {concentration['total_revenue_pages']}. Diversify by "
                    f"improving conversion paths on traffic-only pages and expanding "
                    f"content for mid-value queries."
                ),
                "impact": "medium",
                "estimated_value": 0,
            })

        # 5. Traffic-only pages
        traffic_only = funnel.get("tiers", {}).get("traffic_only", {})
        if traffic_only.get("count", 0) > 5:
            potential = traffic_only.get("clicks", 0) * 0.02 * 50
            recs.append({
                "priority": 5,
                "category": "conversion_expansion",
                "title": "Add conversion paths to traffic-only pages",
                "description": (
                    f"{traffic_only['count']} pages drive {traffic_only['clicks']:,} clicks "
                    f"but generate zero revenue. Adding relevant CTAs, forms, or product "
                    f"links could capture an estimated ${potential:,.0f}."
                ),
                "impact": "medium",
                "estimated_value": round(potential, 2),
            })

        # 6. High-RPC query expansion
        high_rpc = [
            q for q in query_revenue
            if q["revenue_per_click"] > 2 and q["clicks"] < 100
        ]
        if high_rpc:
            recs.append({
                "priority": 6,
                "category": "high_value_expansion",
                "title": "Expand content for high-value, low-volume queries",
                "description": (
                    f"{len(high_rpc)} queries have revenue per click above $2.00 but fewer "
                    f"than 100 clicks. Creating supporting content, improving internal "
                    f"linking, and building topical authority could significantly increase "
                    f"traffic to these high-value terms."
                ),
                "impact": "medium",
                "top_queries": [q["query"] for q in high_rpc[:5]],
            })

        recs.sort(key=lambda r: r["priority"])
        return recs

    def _build_summary(
        self,
        page_revenue: List[Dict],
        query_revenue: List[Dict],
        at_risk: List[Dict],
        roi_opportunities: List[Dict],
        funnel: Dict,
        concentration: Dict,
        recommendations: List[Dict],
    ) -> str:
        """Generate a narrative executive summary."""
        total_rev = funnel.get("total_revenue", 0)
        total_clicks = funnel.get("total_clicks", 0)
        total_conv = funnel.get("total_conversions", 0)
        rpc = funnel.get("avg_revenue_per_click", 0)
        rev_pages = len([p for p in page_revenue if p.get("revenue", 0) > 0])
        risk_total = sum(r["revenue"] for r in at_risk)
        roi_total = sum(o["best_scenario_revenue"] for o in roi_opportunities[:10])

        parts = [
            f"Revenue Attribution Analysis: ${total_rev:,.0f} total attributed revenue "
            f"across {rev_pages} pages from {total_clicks:,} organic clicks "
            f"(${rpc:.2f} average revenue per click).",
        ]

        if total_conv > 0:
            parts.append(
                f"{total_conv:,.0f} conversions tracked at "
                f"{funnel.get('overall_conversion_rate', 0):.2%} overall conversion rate."
            )

        if at_risk:
            parts.append(
                f"${risk_total:,.0f} in revenue is at risk across {len(at_risk)} pages "
                f"with performance issues."
            )

        if roi_opportunities:
            parts.append(
                f"Top 10 position improvement opportunities could generate "
                f"an estimated ${roi_total:,.0f} in additional revenue."
            )

        conc_risk = concentration.get("page_concentration_risk", "low")
        if conc_risk in ("critical", "high"):
            parts.append(
                f"Revenue concentration risk is {conc_risk} -- "
                f"{concentration.get('pages_for_80_pct_revenue', 0)} pages account for "
                f"80% of all revenue."
            )

        parts.append(f"{len(recommendations)} strategic recommendations generated.")

        return " ".join(parts)

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def analyze(self) -> Dict[str, Any]:
        """Run the full revenue attribution analysis."""
        if not self.gsc_rows:
            return {
                "summary": "Insufficient GSC data for revenue attribution analysis.",
                "revenue_by_page": [],
                "top_converting_queries": [],
                "revenue_at_risk": [],
                "position_improvement_roi": [],
                "conversion_funnel": {},
                "revenue_concentration": {},
                "recommendations": [],
            }

        page_agg = self._aggregate_by_page()
        query_agg = self._aggregate_by_query()

        page_revenue = self._compute_page_revenue(page_agg)
        query_revenue = self._compute_query_revenue(query_agg, page_revenue)

        at_risk = self._revenue_at_risk(page_revenue)
        roi_opportunities = self._position_improvement_roi(query_revenue)
        funnel = self._conversion_funnel_analysis(page_revenue)
        concentration = self._revenue_concentration(page_revenue, query_revenue)

        recommendations = self._generate_recommendations(
            page_revenue, query_revenue, at_risk,
            roi_opportunities, funnel, concentration,
        )

        summary = self._build_summary(
            page_revenue, query_revenue, at_risk,
            roi_opportunities, funnel, concentration, recommendations,
        )

        # Compute total potential value for the consulting CTA.
        # Current attributed revenue + best-case additional revenue from
        # ALL position improvement opportunities (not just top 10).
        current_revenue = funnel.get("total_revenue", 0)
        potential_additional = sum(
            o.get("best_scenario_revenue", 0) for o in roi_opportunities
        )
        total_potential_value = round(current_revenue + potential_additional, 2)

        return {
            "summary": summary,
            "total_potential_value": total_potential_value,
            "revenue_by_page": page_revenue[:50],
            "top_converting_queries": query_revenue,
            "revenue_at_risk": at_risk,
            "position_improvement_roi": roi_opportunities,
            "conversion_funnel": funnel,
            "revenue_concentration": concentration,
            "recommendations": recommendations,
            "data_quality": {
                "has_ecommerce_data": self.has_ecommerce,
                "has_conversion_data": self.has_conversions,
                "gsc_rows_analyzed": len(self.gsc_rows),
                "pages_analyzed": len(page_agg),
                "queries_analyzed": len(query_agg),
                "total_potential_value": total_potential_value,
            },
        }


# ---------------------------------------------------------------------------
# Public function (called by routes/modules.py)
# ---------------------------------------------------------------------------

def estimate_revenue_attribution(
    gsc_data,
    ga4_conversions=None,
    ga4_engagement=None,
    ga4_ecommerce=None,
) -> Dict[str, Any]:
    """
    Module 12: Revenue Attribution & ROI Modeling.

    Maps search queries and landing pages to conversions and revenue,
    quantifies revenue at risk, estimates position improvement ROI,
    and generates prioritised action recommendations.

    Args:
        gsc_data: GSC query-level data (DataFrame or list of dicts)
        ga4_conversions: GA4 conversion data per page (optional)
        ga4_engagement: GA4 engagement metrics per page (optional)
        ga4_ecommerce: GA4 ecommerce revenue data per page (optional)

    Returns:
        Dict with: summary, revenue_by_page, top_converting_queries,
        revenue_at_risk, position_improvement_roi, conversion_funnel,
        revenue_concentration, recommendations, data_quality
    """
    logger.info("Running Module 12: Revenue Attribution & ROI Modeling")
    analyzer = RevenueAttributionAnalyzer(
        gsc_data=gsc_data,
        ga4_conversions=ga4_conversions,
        ga4_engagement=ga4_engagement,
        ga4_ecommerce=ga4_ecommerce,
    )
    return analyzer.analyze()
