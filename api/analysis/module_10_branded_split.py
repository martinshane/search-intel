"""
Module 10: Branded vs Non-Branded Split — brand query dependency analysis.

Segments GSC query data into branded and non-branded buckets, measures brand
dependency risk, identifies non-branded growth opportunities, analyses segment
trends over time, and generates strategic recommendations.
"""

import logging
import math
import re
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normalise_brand_terms(brand_terms: Optional[List[str]]) -> List[str]:
    """Return lower-cased, deduplicated brand terms; add common variations."""
    if not brand_terms:
        return []
    seen = set()
    result = []
    for term in brand_terms:
        t = term.strip().lower()
        if t and t not in seen:
            seen.add(t)
            result.append(t)
            # Add no-space and hyphenated variants for multi-word brands
            if " " in t:
                no_space = t.replace(" ", "")
                hyphenated = t.replace(" ", "-")
                for variant in (no_space, hyphenated):
                    if variant not in seen:
                        seen.add(variant)
                        result.append(variant)
    return result


def _is_branded(query: str, brand_patterns: List[re.Pattern]) -> bool:
    """Check if a query matches any brand pattern."""
    q = query.lower().strip()
    return any(p.search(q) for p in brand_patterns)


def _compile_brand_patterns(brand_terms: List[str]) -> List[re.Pattern]:
    """Compile brand terms into regex patterns with word-boundary matching."""
    patterns = []
    for term in brand_terms:
        try:
            patterns.append(re.compile(r"(?:^|\s|[-/])" + re.escape(term) + r"(?:\s|[-/]|$)", re.IGNORECASE))
        except re.error:
            logger.warning(f"Could not compile brand pattern for term: {term}")
    return patterns


def _safe_div(a: float, b: float, default: float = 0.0) -> float:
    return a / b if b else default


def _pct_change(old: float, new: float) -> Optional[float]:
    if old == 0:
        return None
    return round(((new - old) / old) * 100, 2)


# ---------------------------------------------------------------------------
# Main analyser class
# ---------------------------------------------------------------------------

class BrandedSplitAnalyzer:
    """Full Branded vs Non-Branded Split analysis engine."""

    def __init__(self, gsc_query_data: Any, brand_terms: Optional[List[str]] = None):
        self.raw_data = gsc_query_data if isinstance(gsc_query_data, list) else []
        self.brand_terms = _normalise_brand_terms(brand_terms)
        self.brand_patterns = _compile_brand_patterns(self.brand_terms)

        # Classified rows
        self.branded_rows: List[Dict] = []
        self.non_branded_rows: List[Dict] = []

    # ------------------------------------------------------------------
    # 1. Classify every query row
    # ------------------------------------------------------------------
    def _classify_queries(self) -> None:
        """Split raw rows into branded / non-branded buckets."""
        for row in self.raw_data:
            query = row.get("query", row.get("keys", [""])[0] if isinstance(row.get("keys"), list) else "")
            if not query:
                continue
            if self.brand_patterns and _is_branded(query, self.brand_patterns):
                self.branded_rows.append({**row, "_query": query})
            else:
                self.non_branded_rows.append({**row, "_query": query})

    # ------------------------------------------------------------------
    # 2. Aggregate segment metrics
    # ------------------------------------------------------------------
    def _aggregate_segment(self, rows: List[Dict]) -> Dict[str, Any]:
        """Compute aggregate metrics for a segment."""
        total_clicks = sum(r.get("clicks", 0) for r in rows)
        total_impressions = sum(r.get("impressions", 0) for r in rows)
        unique_queries = len(set(r["_query"] for r in rows))
        unique_pages = len(set(r.get("page", r.get("url", "")) for r in rows))
        positions = [r.get("position", 0) for r in rows if r.get("position")]
        avg_position = round(sum(positions) / len(positions), 2) if positions else None
        avg_ctr = round(_safe_div(total_clicks, total_impressions) * 100, 2)

        return {
            "total_clicks": total_clicks,
            "total_impressions": total_impressions,
            "unique_queries": unique_queries,
            "unique_pages": unique_pages,
            "avg_position": avg_position,
            "avg_ctr_pct": avg_ctr,
        }

    # ------------------------------------------------------------------
    # 3. Brand dependency risk
    # ------------------------------------------------------------------
    def _assess_brand_dependency(self, branded_agg: Dict, non_branded_agg: Dict) -> Dict[str, Any]:
        """Compute brand dependency score and risk classification."""
        total_clicks = branded_agg["total_clicks"] + non_branded_agg["total_clicks"]
        branded_click_share = round(_safe_div(branded_agg["total_clicks"], total_clicks) * 100, 2)

        total_impressions = branded_agg["total_impressions"] + non_branded_agg["total_impressions"]
        branded_impression_share = round(_safe_div(branded_agg["total_impressions"], total_impressions) * 100, 2)

        # Dependency score 0-100 (higher = more dependent on brand)
        # Weighted: 60% click share + 30% impression share + 10% query diversity penalty
        query_diversity_ratio = _safe_div(
            branded_agg["unique_queries"],
            branded_agg["unique_queries"] + non_branded_agg["unique_queries"],
        )
        dependency_score = round(
            branded_click_share * 0.60
            + branded_impression_share * 0.30
            + query_diversity_ratio * 100 * 0.10,
            1,
        )

        if dependency_score >= 70:
            risk_level = "critical"
            risk_label = "Heavily brand-dependent — organic discovery is weak"
        elif dependency_score >= 50:
            risk_level = "high"
            risk_label = "Significant brand reliance — diversification needed"
        elif dependency_score >= 30:
            risk_level = "moderate"
            risk_label = "Healthy brand presence with room for non-branded growth"
        else:
            risk_level = "low"
            risk_label = "Strong non-branded organic footprint"

        return {
            "dependency_score": dependency_score,
            "risk_level": risk_level,
            "risk_label": risk_label,
            "branded_click_share_pct": branded_click_share,
            "branded_impression_share_pct": branded_impression_share,
            "non_branded_click_share_pct": round(100 - branded_click_share, 2),
            "non_branded_impression_share_pct": round(100 - branded_impression_share, 2),
        }

    # ------------------------------------------------------------------
    # 4. Top queries per segment
    # ------------------------------------------------------------------
    def _top_queries(self, rows: List[Dict], limit: int = 25) -> List[Dict[str, Any]]:
        """Return top queries by clicks within a segment."""
        agg: Dict[str, Dict] = {}
        for r in rows:
            q = r["_query"]
            if q not in agg:
                agg[q] = {"query": q, "clicks": 0, "impressions": 0, "positions": [], "pages": set()}
            agg[q]["clicks"] += r.get("clicks", 0)
            agg[q]["impressions"] += r.get("impressions", 0)
            if r.get("position"):
                agg[q]["positions"].append(r["position"])
            page = r.get("page", r.get("url", ""))
            if page:
                agg[q]["pages"].add(page)

        result = []
        for q_data in sorted(agg.values(), key=lambda x: x["clicks"], reverse=True)[:limit]:
            avg_pos = round(sum(q_data["positions"]) / len(q_data["positions"]), 1) if q_data["positions"] else None
            ctr = round(_safe_div(q_data["clicks"], q_data["impressions"]) * 100, 2)
            result.append({
                "query": q_data["query"],
                "clicks": q_data["clicks"],
                "impressions": q_data["impressions"],
                "avg_position": avg_pos,
                "ctr_pct": ctr,
                "page_count": len(q_data["pages"]),
            })
        return result

    # ------------------------------------------------------------------
    # 5. Non-branded opportunity analysis
    # ------------------------------------------------------------------
    def _find_non_branded_opportunities(self) -> List[Dict[str, Any]]:
        """Identify non-branded queries with high impressions but low CTR/position
        — these are the biggest growth levers."""
        agg: Dict[str, Dict] = {}
        for r in self.non_branded_rows:
            q = r["_query"]
            if q not in agg:
                agg[q] = {"query": q, "clicks": 0, "impressions": 0, "positions": []}
            agg[q]["clicks"] += r.get("clicks", 0)
            agg[q]["impressions"] += r.get("impressions", 0)
            if r.get("position"):
                agg[q]["positions"].append(r["position"])

        opportunities = []
        for q_data in agg.values():
            if q_data["impressions"] < 50:
                continue
            avg_pos = sum(q_data["positions"]) / len(q_data["positions"]) if q_data["positions"] else 50
            ctr = _safe_div(q_data["clicks"], q_data["impressions"]) * 100

            # Score: high impression, poor position/CTR = big opportunity
            # Normalise impressions (log scale), penalise good positions
            imp_score = min(math.log10(max(q_data["impressions"], 1)) / 5, 1.0)  # 0-1
            pos_score = min(max(avg_pos - 3, 0) / 17, 1.0)  # positions 3-20 -> 0-1
            ctr_gap = max(0, 5 - ctr) / 5  # CTR < 5% -> opportunity
            priority = round((imp_score * 0.4 + pos_score * 0.35 + ctr_gap * 0.25) * 100, 1)

            if avg_pos > 3:  # already top-3 -> less opportunity
                opportunities.append({
                    "query": q_data["query"],
                    "clicks": q_data["clicks"],
                    "impressions": q_data["impressions"],
                    "avg_position": round(avg_pos, 1),
                    "ctr_pct": round(ctr, 2),
                    "priority_score": priority,
                    "opportunity_type": "striking_distance" if avg_pos <= 20 else "long_tail",
                })

        opportunities.sort(key=lambda x: x["priority_score"], reverse=True)
        return opportunities[:40]

    # ------------------------------------------------------------------
    # 6. Time-series trend analysis (if date dimension present)
    # ------------------------------------------------------------------
    def _analyze_trends(self) -> Dict[str, Any]:
        """If rows contain a date field, compute weekly branded/non-branded trends."""
        def _extract_date(row: Dict) -> Optional[str]:
            for key in ("date", "keys"):
                val = row.get(key)
                if isinstance(val, str) and len(val) == 10:
                    return val
                if isinstance(val, list):
                    for item in val:
                        if isinstance(item, str) and len(item) == 10 and "-" in item:
                            return item
            return None

        branded_by_week: Dict[str, Dict] = defaultdict(lambda: {"clicks": 0, "impressions": 0})
        non_branded_by_week: Dict[str, Dict] = defaultdict(lambda: {"clicks": 0, "impressions": 0})

        has_dates = False
        for r in self.branded_rows:
            d = _extract_date(r)
            if d:
                has_dates = True
                try:
                    dt = datetime.strptime(d, "%Y-%m-%d")
                    week_start = (dt - timedelta(days=dt.weekday())).strftime("%Y-%m-%d")
                    branded_by_week[week_start]["clicks"] += r.get("clicks", 0)
                    branded_by_week[week_start]["impressions"] += r.get("impressions", 0)
                except ValueError:
                    pass

        for r in self.non_branded_rows:
            d = _extract_date(r)
            if d:
                has_dates = True
                try:
                    dt = datetime.strptime(d, "%Y-%m-%d")
                    week_start = (dt - timedelta(days=dt.weekday())).strftime("%Y-%m-%d")
                    non_branded_by_week[week_start]["clicks"] += r.get("clicks", 0)
                    non_branded_by_week[week_start]["impressions"] += r.get("impressions", 0)
                except ValueError:
                    pass

        if not has_dates or (not branded_by_week and not non_branded_by_week):
            return {"available": False, "reason": "No date dimension in query data"}

        all_weeks = sorted(set(list(branded_by_week.keys()) + list(non_branded_by_week.keys())))

        timeline = []
        for week in all_weeks:
            b = branded_by_week.get(week, {"clicks": 0, "impressions": 0})
            nb = non_branded_by_week.get(week, {"clicks": 0, "impressions": 0})
            total_clicks = b["clicks"] + nb["clicks"]
            timeline.append({
                "week": week,
                "branded_clicks": b["clicks"],
                "non_branded_clicks": nb["clicks"],
                "branded_impressions": b["impressions"],
                "non_branded_impressions": nb["impressions"],
                "branded_click_share_pct": round(_safe_div(b["clicks"], total_clicks) * 100, 1),
            })

        # Trend direction: compare first third vs last third
        trend_info = {}
        if len(timeline) >= 6:
            third = max(len(timeline) // 3, 1)
            early = timeline[:third]
            late = timeline[-third:]
            early_branded_share = sum(t["branded_click_share_pct"] for t in early) / len(early)
            late_branded_share = sum(t["branded_click_share_pct"] for t in late) / len(late)
            share_delta = round(late_branded_share - early_branded_share, 2)

            early_nb_clicks = sum(t["non_branded_clicks"] for t in early)
            late_nb_clicks = sum(t["non_branded_clicks"] for t in late)
            nb_growth = _pct_change(early_nb_clicks, late_nb_clicks)

            early_b_clicks = sum(t["branded_clicks"] for t in early)
            late_b_clicks = sum(t["branded_clicks"] for t in late)
            b_growth = _pct_change(early_b_clicks, late_b_clicks)

            if share_delta > 3:
                trend_direction = "increasing_brand_dependency"
            elif share_delta < -3:
                trend_direction = "decreasing_brand_dependency"
            else:
                trend_direction = "stable"

            trend_info = {
                "branded_share_change_pp": share_delta,
                "non_branded_click_growth_pct": nb_growth,
                "branded_click_growth_pct": b_growth,
                "trend_direction": trend_direction,
            }

        return {
            "available": True,
            "weeks_analyzed": len(timeline),
            "timeline": timeline,
            "trend": trend_info,
        }

    # ------------------------------------------------------------------
    # 7. Page-level brand dependency
    # ------------------------------------------------------------------
    def _page_brand_dependency(self) -> List[Dict[str, Any]]:
        """Identify pages that are overly reliant on branded queries."""
        page_data: Dict[str, Dict] = defaultdict(lambda: {
            "branded_clicks": 0, "non_branded_clicks": 0,
            "branded_impressions": 0, "non_branded_impressions": 0,
        })

        for r in self.branded_rows:
            page = r.get("page", r.get("url", ""))
            if page:
                page_data[page]["branded_clicks"] += r.get("clicks", 0)
                page_data[page]["branded_impressions"] += r.get("impressions", 0)

        for r in self.non_branded_rows:
            page = r.get("page", r.get("url", ""))
            if page:
                page_data[page]["non_branded_clicks"] += r.get("clicks", 0)
                page_data[page]["non_branded_impressions"] += r.get("impressions", 0)

        results = []
        for page, data in page_data.items():
            total = data["branded_clicks"] + data["non_branded_clicks"]
            if total < 10:
                continue
            branded_share = round(_safe_div(data["branded_clicks"], total) * 100, 1)
            results.append({
                "page": page,
                "total_clicks": total,
                "branded_clicks": data["branded_clicks"],
                "non_branded_clicks": data["non_branded_clicks"],
                "branded_share_pct": branded_share,
                "dependency_level": (
                    "high" if branded_share >= 80
                    else "moderate" if branded_share >= 50
                    else "low"
                ),
            })

        results.sort(key=lambda x: x["branded_share_pct"], reverse=True)
        return results[:30]

    # ------------------------------------------------------------------
    # 8. Brand cannibalisation detection
    # ------------------------------------------------------------------
    def _detect_brand_cannibalization(self) -> List[Dict[str, Any]]:
        """Find branded queries ranking for multiple pages (potential cannibalisation)."""
        query_pages: Dict[str, List[Dict]] = defaultdict(list)
        for r in self.branded_rows:
            page = r.get("page", r.get("url", ""))
            if page:
                query_pages[r["_query"]].append({
                    "page": page,
                    "clicks": r.get("clicks", 0),
                    "impressions": r.get("impressions", 0),
                    "position": r.get("position"),
                })

        cannibalised = []
        for query, pages in query_pages.items():
            unique_pages: Dict[str, Dict] = {}
            for p in pages:
                url = p["page"]
                if url not in unique_pages:
                    unique_pages[url] = {"page": url, "clicks": 0, "impressions": 0, "positions": []}
                unique_pages[url]["clicks"] += p["clicks"]
                unique_pages[url]["impressions"] += p["impressions"]
                if p["position"]:
                    unique_pages[url]["positions"].append(p["position"])

            if len(unique_pages) >= 2:
                page_list = []
                for pdata in unique_pages.values():
                    avg_pos = (
                        round(sum(pdata["positions"]) / len(pdata["positions"]), 1)
                        if pdata["positions"] else None
                    )
                    page_list.append({
                        "page": pdata["page"],
                        "clicks": pdata["clicks"],
                        "impressions": pdata["impressions"],
                        "avg_position": avg_pos,
                    })
                page_list.sort(key=lambda x: x["clicks"], reverse=True)
                total_imp = sum(p["impressions"] for p in page_list)
                cannibalised.append({
                    "query": query,
                    "page_count": len(page_list),
                    "total_impressions": total_imp,
                    "pages": page_list[:5],
                })

        cannibalised.sort(key=lambda x: x["total_impressions"], reverse=True)
        return cannibalised[:20]

    # ------------------------------------------------------------------
    # 9. Recommendations
    # ------------------------------------------------------------------
    def _generate_recommendations(
        self,
        dependency: Dict,
        opportunities: List[Dict],
        trends: Dict,
        page_dep: List[Dict],
        cannibalization: List[Dict],
    ) -> List[Dict[str, Any]]:
        """Generate prioritised, actionable recommendations."""
        recs: List[Dict[str, Any]] = []

        # 1. Brand dependency
        if dependency["risk_level"] in ("critical", "high"):
            recs.append({
                "category": "brand_dependency",
                "priority": "critical" if dependency["risk_level"] == "critical" else "high",
                "title": "Reduce brand dependency risk",
                "detail": (
                    f"Brand queries account for {dependency['branded_click_share_pct']}% of clicks. "
                    f"A brand-perception event (bad PR, algorithm shift) could devastate traffic. "
                    f"Invest in non-branded content targeting informational and commercial queries."
                ),
                "impact": "high",
            })

        # 2. Non-branded opportunities
        if opportunities:
            top5 = [o["query"] for o in opportunities[:5]]
            total_potential_imp = sum(o["impressions"] for o in opportunities[:10])
            recs.append({
                "category": "non_branded_growth",
                "priority": "high",
                "title": "Capitalise on non-branded keyword opportunities",
                "detail": (
                    f"Found {len(opportunities)} non-branded queries with high impressions but low rankings. "
                    f"Top targets: {', '.join(top5)}. Combined impression pool of top 10: {total_potential_imp:,}."
                ),
                "impact": "high",
                "affected_queries": len(opportunities),
            })

        # 3. Trend warnings
        trend = trends.get("trend", {})
        if trend.get("trend_direction") == "increasing_brand_dependency":
            recs.append({
                "category": "trend_warning",
                "priority": "high",
                "title": "Brand dependency is increasing over time",
                "detail": (
                    f"Branded click share grew by {trend['branded_share_change_pp']} percentage points. "
                    f"Non-branded click growth: {trend.get('non_branded_click_growth_pct', 'N/A')}%. "
                    f"Prioritise non-branded content pipeline."
                ),
                "impact": "medium",
            })
        elif (
            trend.get("non_branded_click_growth_pct") is not None
            and trend["non_branded_click_growth_pct"] > 20
        ):
            recs.append({
                "category": "positive_trend",
                "priority": "medium",
                "title": "Non-branded traffic growing — maintain momentum",
                "detail": (
                    f"Non-branded clicks grew {trend['non_branded_click_growth_pct']}% over the analysis period. "
                    f"Continue investing in the content strategies driving this growth."
                ),
                "impact": "medium",
            })

        # 4. Page-level dependency
        high_dep_pages = [p for p in page_dep if p["dependency_level"] == "high"]
        if high_dep_pages:
            recs.append({
                "category": "page_dependency",
                "priority": "medium",
                "title": "Diversify traffic sources for brand-dependent pages",
                "detail": (
                    f"{len(high_dep_pages)} pages receive 80%+ of clicks from branded queries. "
                    f"Add non-branded keyword targeting and internal links to these pages."
                ),
                "impact": "medium",
                "affected_pages": len(high_dep_pages),
            })

        # 5. Brand cannibalisation
        if cannibalization:
            recs.append({
                "category": "brand_cannibalization",
                "priority": "medium",
                "title": "Resolve branded query cannibalisation",
                "detail": (
                    f"{len(cannibalization)} branded queries rank for multiple pages, diluting CTR. "
                    f"Consolidate or canonicalise to a single best page per brand query."
                ),
                "impact": "medium",
                "affected_queries": len(cannibalization),
            })

        # 6. Low brand coverage
        if dependency["branded_click_share_pct"] < 10:
            recs.append({
                "category": "brand_awareness",
                "priority": "low",
                "title": "Brand awareness may be low",
                "detail": (
                    f"Only {dependency['branded_click_share_pct']}% of clicks come from branded queries. "
                    f"Consider brand-building activities (PR, social, partnerships) to grow branded search demand."
                ),
                "impact": "low",
            })

        recs.sort(key=lambda r: {"critical": 0, "high": 1, "medium": 2, "low": 3}.get(r["priority"], 4))
        return recs

    # ------------------------------------------------------------------
    # 10. Summary
    # ------------------------------------------------------------------
    def _build_summary(
        self,
        branded_agg: Dict,
        non_branded_agg: Dict,
        dependency: Dict,
        opportunities: List,
        trends: Dict,
        recommendations: List,
    ) -> str:
        """Human-readable executive summary."""
        parts = []
        total_clicks = branded_agg["total_clicks"] + non_branded_agg["total_clicks"]
        parts.append(
            f"Across {total_clicks:,} total organic clicks, "
            f"{dependency['branded_click_share_pct']}% came from branded queries "
            f"and {dependency['non_branded_click_share_pct']}% from non-branded queries."
        )
        parts.append(
            f"Brand dependency score: {dependency['dependency_score']}/100 "
            f"({dependency['risk_level']} risk — {dependency['risk_label']})."
        )
        parts.append(
            f"Branded segment: {branded_agg['unique_queries']:,} unique queries, "
            f"avg position {branded_agg['avg_position'] or 'N/A'}, CTR {branded_agg['avg_ctr_pct']}%."
        )
        parts.append(
            f"Non-branded segment: {non_branded_agg['unique_queries']:,} unique queries, "
            f"avg position {non_branded_agg['avg_position'] or 'N/A'}, CTR {non_branded_agg['avg_ctr_pct']}%."
        )
        if opportunities:
            parts.append(
                f"Identified {len(opportunities)} non-branded growth opportunities "
                f"with high impression volume."
            )
        trend = trends.get("trend", {})
        if trend.get("trend_direction"):
            direction_labels = {
                "increasing_brand_dependency": (
                    "Brand dependency is increasing over time — non-branded growth is lagging."
                ),
                "decreasing_brand_dependency": (
                    "Brand dependency is decreasing — non-branded organic is gaining share."
                ),
                "stable": "Brand/non-brand split is stable over the analysis period.",
            }
            parts.append(direction_labels.get(trend["trend_direction"], ""))
        parts.append(f"Generated {len(recommendations)} prioritised recommendations.")
        return " ".join(parts)

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------
    def analyze(self) -> Dict[str, Any]:
        """Run the full branded split analysis and return results dict."""
        if not self.raw_data:
            return {
                "summary": "No GSC query data available for branded split analysis.",
                "branded_pct": None,
                "non_branded_growth": None,
                "error": "no_data",
            }

        if not self.brand_terms:
            return {
                "summary": (
                    "No brand terms provided — cannot segment branded vs non-branded queries. "
                    "Please configure brand terms in the report settings or ensure a domain is set."
                ),
                "branded_pct": None,
                "non_branded_growth": None,
                "error": "no_brand_terms",
            }

        # Step 1: classify
        self._classify_queries()

        # Step 2: aggregate
        branded_agg = self._aggregate_segment(self.branded_rows)
        non_branded_agg = self._aggregate_segment(self.non_branded_rows)

        # Step 3: dependency
        dependency = self._assess_brand_dependency(branded_agg, non_branded_agg)

        # Step 4: top queries
        top_branded = self._top_queries(self.branded_rows, limit=25)
        top_non_branded = self._top_queries(self.non_branded_rows, limit=25)

        # Step 5: opportunities
        opportunities = self._find_non_branded_opportunities()

        # Step 6: trends
        trends = self._analyze_trends()

        # Step 7: page dependency
        page_dep = self._page_brand_dependency()

        # Step 8: cannibalisation
        cannibalization = self._detect_brand_cannibalization()

        # Step 9: recommendations
        recommendations = self._generate_recommendations(
            dependency, opportunities, trends, page_dep, cannibalization
        )

        # Step 10: summary
        summary = self._build_summary(
            branded_agg, non_branded_agg, dependency, opportunities, trends, recommendations
        )

        return {
            "summary": summary,
            "brand_terms_used": self.brand_terms,
            "branded_pct": dependency["branded_click_share_pct"],
            "non_branded_growth": trends.get("trend", {}).get("non_branded_click_growth_pct"),
            "segments": {
                "branded": branded_agg,
                "non_branded": non_branded_agg,
            },
            "brand_dependency": dependency,
            "top_branded_queries": top_branded,
            "top_non_branded_queries": top_non_branded,
            "non_branded_opportunities": opportunities,
            "trends": trends,
            "page_brand_dependency": page_dep,
            "brand_cannibalization": cannibalization,
            "recommendations": recommendations,
        }


# ---------------------------------------------------------------------------
# Public API  (matches routes/modules.py call signature)
# ---------------------------------------------------------------------------

def analyze_branded_split(gsc_query_data, brand_terms=None) -> Dict[str, Any]:
    """
    Module 10: Branded vs Non-Branded Split — brand query dependency analysis.

    Args:
        gsc_query_data: list of GSC query-level rows, each with keys like
            query, clicks, impressions, position, ctr, page, date.
        brand_terms: list of brand name strings to match against queries.

    Returns:
        Dict with full branded/non-branded analysis results.
    """
    logger.info(
        f"Running analyze_branded_split with "
        f"{len(gsc_query_data) if isinstance(gsc_query_data, list) else 0} rows, "
        f"{len(brand_terms) if brand_terms else 0} brand terms"
    )
    analyzer = BrandedSplitAnalyzer(gsc_query_data, brand_terms)
    return analyzer.analyze()
