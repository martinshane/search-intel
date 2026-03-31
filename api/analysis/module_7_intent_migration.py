"""
Module 7: Intent Migration — tracks query intent shifts over time.

Analyzes how search intent evolves across a site's keyword portfolio:
  - Classifies queries by intent (informational / commercial / transactional / navigational)
  - Detects intent shifts by comparing SERP composition across time windows
  - Identifies emerging intents and declining patterns
  - Maps content alignment (does page type match dominant intent?)
  - Recommends content strategy adjustments

Phase 3 — full implementation replacing the stub.
"""

import logging
import re
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Intent classification
# ---------------------------------------------------------------------------

# Keyword pattern signals for intent classification
_TRANSACTIONAL_PATTERNS = [
    r"\bbuy\b", r"\bpurchase\b", r"\border\b", r"\bprice\b", r"\bcheap\b",
    r"\bdiscount\b", r"\bcoupon\b", r"\bdeal\b", r"\bsale\b", r"\bsubscribe\b",
    r"\bsign\s?up\b", r"\bdownload\b", r"\bget\b(?=.*(?:free|now|started))",
    r"\bbook\b(?=.*(?:now|online|appointment))", r"\bhire\b", r"\bfree\s?trial\b",
]

_COMMERCIAL_PATTERNS = [
    r"\bbest\b", r"\btop\s?\d+\b", r"\breview[s]?\b", r"\bcompare\b",
    r"\bcomparison\b", r"\bvs\.?\b", r"\balternative[s]?\b", r"\brating[s]?\b",
    r"\bworth\b", r"\bpros?\s?(and|&)\s?cons?\b", r"\brecommend\b",
    r"\baffordable\b", r"\bpremium\b",
]

_NAVIGATIONAL_PATTERNS = [
    r"\blogin\b", r"\blog\s?in\b", r"\bsign\s?in\b", r"\baccount\b",
    r"\bdashboard\b", r"\bofficial\b", r"\bwebsite\b", r"\bcontact\b",
    r"\bsupport\b", r"\bapp\b", r"\bportal\b",
]

# SERP feature signals that indicate intent
_SERP_INTENT_SIGNALS = {
    "shopping_results": "transactional",
    "local_pack": "transactional",
    "product_carousel": "transactional",
    "ads_top": "transactional",
    "featured_snippet": "informational",
    "people_also_ask": "informational",
    "knowledge_panel": "informational",
    "ai_overview": "informational",
    "video_carousel": "informational",
    "image_pack": "informational",
    "related_searches": "informational",
    "site_links": "navigational",
    "top_stories": "informational",
}


def classify_query_intent(query: str, serp_features: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Classify a single query's intent using keyword patterns and SERP features.

    Returns dict with 'primary_intent', 'confidence', and 'signals'.
    """
    query_lower = query.lower().strip()
    scores = {"informational": 0.0, "commercial": 0.0, "transactional": 0.0, "navigational": 0.0}
    signals: List[str] = []

    # Keyword pattern scoring
    for pattern in _TRANSACTIONAL_PATTERNS:
        if re.search(pattern, query_lower):
            scores["transactional"] += 1.5
            signals.append(f"keyword_transactional")
            break
    for pattern in _COMMERCIAL_PATTERNS:
        if re.search(pattern, query_lower):
            scores["commercial"] += 1.5
            signals.append(f"keyword_commercial")
            break
    for pattern in _NAVIGATIONAL_PATTERNS:
        if re.search(pattern, query_lower):
            scores["navigational"] += 1.5
            signals.append(f"keyword_navigational")
            break

    # SERP feature scoring (stronger signal)
    if serp_features:
        for feat in serp_features:
            feat_key = feat.lower().replace(" ", "_")
            if feat_key in _SERP_INTENT_SIGNALS:
                intent = _SERP_INTENT_SIGNALS[feat_key]
                scores[intent] += 2.0
                signals.append(f"serp:{feat_key}")

    # Default: if no strong signals, informational is the baseline
    if sum(scores.values()) == 0:
        scores["informational"] = 1.0
        signals.append("default:no_strong_signals")

    total = sum(scores.values())
    probs = {k: v / total for k, v in scores.items()}
    primary = max(probs, key=probs.get)
    confidence = probs[primary]

    return {
        "primary_intent": primary,
        "confidence": round(confidence, 3),
        "intent_distribution": {k: round(v, 3) for k, v in probs.items()},
        "signals": signals,
    }


# ---------------------------------------------------------------------------
# Content type inference from page data
# ---------------------------------------------------------------------------

_PAGE_TYPE_PATTERNS = {
    "blog": [r"/blog/", r"/article", r"/post/", r"/news/"],
    "product": [r"/product", r"/shop/", r"/store/", r"/buy/", r"/item/"],
    "category": [r"/category/", r"/collection", r"/browse/", r"/catalog/"],
    "landing_page": [r"/lp/", r"/landing", r"/offer/", r"/promo/"],
    "documentation": [r"/docs/", r"/guide/", r"/help/", r"/faq/", r"/how-to/", r"/tutorial/"],
    "tool": [r"/tool", r"/calculator", r"/generator", r"/checker/"],
    "homepage": [r"^https?://[^/]+/?$"],
}


def infer_page_type(url: str, page_meta: Optional[Dict[str, Any]] = None) -> str:
    """Infer content type from URL patterns and optional page metadata."""
    url_lower = url.lower()
    for ptype, patterns in _PAGE_TYPE_PATTERNS.items():
        for pat in patterns:
            if re.search(pat, url_lower):
                return ptype
    # Check page_meta title/description hints
    if page_meta:
        title = str(page_meta.get("title") or "").lower()
        if any(w in title for w in ["buy", "shop", "price"]):
            return "product"
        if any(w in title for w in ["guide", "how to", "what is", "tutorial"]):
            return "documentation"
    return "other"


# ---------------------------------------------------------------------------
# Core analysis class
# ---------------------------------------------------------------------------

class IntentMigrationAnalyzer:
    """Tracks query intent shifts over time and identifies migration patterns."""

    # Time window configuration
    RECENT_WINDOW_DAYS = 30
    COMPARISON_WINDOW_DAYS = 60
    MIN_IMPRESSIONS_THRESHOLD = 50
    MIN_SHIFT_CONFIDENCE = 0.15  # minimum change in intent probability to flag

    def __init__(
        self,
        query_timeseries: pd.DataFrame,
        serp_data: Optional[Dict[str, Any]] = None,
        page_data: Optional[pd.DataFrame] = None,
    ):
        self.query_ts = query_timeseries
        self.serp_data = serp_data or {}
        self.page_data = page_data
        self._serp_features_map = self._build_serp_features_map()
        self._page_meta_map = self._build_page_meta_map()

    # ------------------------------------------------------------------
    # Helpers: build lookup maps
    # ------------------------------------------------------------------

    def _build_serp_features_map(self) -> Dict[str, List[str]]:
        """Build query -> list of SERP features from serp_data."""
        fmap: Dict[str, List[str]] = {}
        results = self.serp_data.get("results", [])
        if isinstance(results, list):
            for entry in results:
                kw = entry.get("keyword") or entry.get("query", "")
                items = entry.get("items") or entry.get("result", [])
                features = set()
                if isinstance(items, list):
                    for item in items:
                        item_type = item.get("type", "")
                        if item_type and item_type != "organic":
                            features.add(item_type)
                        # Also check feature flags
                        for feat_key in ["featured_snippet", "people_also_ask", "local_pack",
                                         "shopping", "knowledge_graph", "ai_overview",
                                         "video", "image_pack", "top_stories", "ads"]:
                            if item.get(feat_key) or item.get(f"has_{feat_key}"):
                                features.add(feat_key)
                if kw and features:
                    fmap[kw.lower()] = list(features)
        return fmap

    def _build_page_meta_map(self) -> Dict[str, Dict[str, Any]]:
        """Build URL -> page metadata from page_data."""
        if self.page_data is None:
            return {}
        pmap: Dict[str, Dict[str, Any]] = {}
        if isinstance(self.page_data, pd.DataFrame):
            if self.page_data.empty:
                return {}
            for _, row in self.page_data.iterrows():
                url = row.get("url") or row.get("page", "")
                if url:
                    pmap[url] = row.to_dict()
        elif isinstance(self.page_data, dict):
            for url, meta in self.page_data.items():
                pmap[url] = meta if isinstance(meta, dict) else {"data": meta}
        return pmap

    # ------------------------------------------------------------------
    # 1. Classify intent across time windows
    # ------------------------------------------------------------------

    def _split_time_windows(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Split timeseries into recent and comparison windows."""
        df = self.query_ts.copy()

        # Ensure date column is datetime
        date_col = "date"
        if date_col not in df.columns:
            for candidate in ["day", "Date", "timestamp"]:
                if candidate in df.columns:
                    date_col = candidate
                    break

        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        max_date = df[date_col].max()

        recent_start = max_date - timedelta(days=self.RECENT_WINDOW_DAYS)
        comparison_end = recent_start - timedelta(days=1)
        comparison_start = comparison_end - timedelta(days=self.COMPARISON_WINDOW_DAYS)

        recent = df[df[date_col] >= recent_start]
        comparison = df[(df[date_col] >= comparison_start) & (df[date_col] <= comparison_end)]

        return recent, comparison

    def _aggregate_query_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aggregate daily data to query-level metrics."""
        query_col = "query"
        if query_col not in df.columns:
            for candidate in ["keyword", "search_query"]:
                if candidate in df.columns:
                    query_col = candidate
                    break
        if query_col not in df.columns:
            return pd.DataFrame()

        agg_dict = {"clicks": ("clicks", "sum"), "impressions": ("impressions", "sum")}
        if "position" in df.columns:
            agg_dict["avg_position"] = ("position", "mean")

        agg = df.groupby(query_col).agg(**agg_dict).reset_index()

        if "avg_position" not in agg.columns:
            agg["avg_position"] = 0.0

        agg["ctr"] = np.where(agg["impressions"] > 0, agg["clicks"] / agg["impressions"], 0)
        agg = agg.rename(columns={query_col: "query"})

        return agg

    def _classify_window(self, agg: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
        """Classify intent for each query in an aggregated window."""
        results: Dict[str, Dict[str, Any]] = {}
        for _, row in agg.iterrows():
            query = row["query"]
            serp_feats = self._serp_features_map.get(query.lower())
            classification = classify_query_intent(query, serp_feats)
            classification["impressions"] = float(row.get("impressions", 0))
            classification["clicks"] = float(row.get("clicks", 0))
            classification["avg_position"] = float(row.get("avg_position", 0))
            results[query] = classification
        return results

    # ------------------------------------------------------------------
    # 2. Detect intent shifts
    # ------------------------------------------------------------------

    def _detect_shifts(
        self,
        recent_intents: Dict[str, Dict[str, Any]],
        comparison_intents: Dict[str, Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Compare intent distributions across windows to detect shifts.

        A shift is flagged when a query's primary intent changes OR the
        probability distribution shifts significantly (>= MIN_SHIFT_CONFIDENCE).
        """
        shifts: List[Dict[str, Any]] = []
        common_queries = set(recent_intents.keys()) & set(comparison_intents.keys())

        for query in common_queries:
            recent = recent_intents[query]
            prev = comparison_intents[query]

            # Skip low-volume queries
            if recent.get("impressions", 0) < self.MIN_IMPRESSIONS_THRESHOLD:
                continue

            r_dist = recent.get("intent_distribution", {})
            p_dist = prev.get("intent_distribution", {})

            # Calculate distribution shift magnitude
            all_intents = set(list(r_dist.keys()) + list(p_dist.keys()))
            shift_magnitude = sum(
                abs(r_dist.get(i, 0) - p_dist.get(i, 0)) for i in all_intents
            ) / 2  # Normalize to 0-1 range

            primary_changed = recent.get("primary_intent") != prev.get("primary_intent")

            if primary_changed or shift_magnitude >= self.MIN_SHIFT_CONFIDENCE:
                # Determine direction: which intent grew most?
                deltas = {
                    i: r_dist.get(i, 0) - p_dist.get(i, 0)
                    for i in all_intents
                }
                growing_intent = max(deltas, key=deltas.get)
                declining_intent = min(deltas, key=deltas.get)

                shifts.append({
                    "query": query,
                    "previous_intent": prev.get("primary_intent"),
                    "current_intent": recent.get("primary_intent"),
                    "primary_changed": primary_changed,
                    "shift_magnitude": round(shift_magnitude, 3),
                    "growing_intent": growing_intent,
                    "growing_delta": round(deltas[growing_intent], 3),
                    "declining_intent": declining_intent,
                    "declining_delta": round(deltas[declining_intent], 3),
                    "impressions": recent.get("impressions", 0),
                    "clicks": recent.get("clicks", 0),
                    "avg_position": round(recent.get("avg_position", 0), 1),
                    "recent_distribution": r_dist,
                    "previous_distribution": p_dist,
                })

        # Sort by impact: primary changes first, then by impressions
        shifts.sort(key=lambda s: (not s["primary_changed"], -s["impressions"]))
        return shifts[:50]

    # ------------------------------------------------------------------
    # 3. Identify emerging intents
    # ------------------------------------------------------------------

    def _identify_emerging_intents(
        self,
        recent_intents: Dict[str, Dict[str, Any]],
        comparison_intents: Dict[str, Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Find queries that are new or rapidly growing — indicative of
        emerging search intent the site should address.
        """
        emerging: List[Dict[str, Any]] = []

        # Queries present in recent but not in comparison
        new_queries = set(recent_intents.keys()) - set(comparison_intents.keys())
        for query in new_queries:
            info = recent_intents[query]
            if info.get("impressions", 0) >= self.MIN_IMPRESSIONS_THRESHOLD:
                emerging.append({
                    "query": query,
                    "type": "new_query",
                    "intent": info.get("primary_intent"),
                    "confidence": info.get("confidence", 0),
                    "impressions": info.get("impressions", 0),
                    "clicks": info.get("clicks", 0),
                    "avg_position": round(info.get("avg_position", 0), 1),
                    "growth_signal": "appeared_recently",
                })

        # Queries in both windows but with large impression growth
        common = set(recent_intents.keys()) & set(comparison_intents.keys())
        for query in common:
            r = recent_intents[query]
            p = comparison_intents[query]
            r_imp = r.get("impressions", 0)
            p_imp = p.get("impressions", 0)

            if r_imp < self.MIN_IMPRESSIONS_THRESHOLD:
                continue

            # Normalize: recent window is shorter, so scale comparison proportionally
            scale = self.RECENT_WINDOW_DAYS / max(self.COMPARISON_WINDOW_DAYS, 1)
            p_imp_scaled = p_imp * scale

            if p_imp_scaled > 0:
                growth_rate = (r_imp - p_imp_scaled) / p_imp_scaled
            else:
                growth_rate = float("inf") if r_imp > 0 else 0

            if growth_rate >= 0.5:  # 50%+ growth
                emerging.append({
                    "query": query,
                    "type": "rapid_growth",
                    "intent": r.get("primary_intent"),
                    "confidence": r.get("confidence", 0),
                    "impressions": r_imp,
                    "clicks": r.get("clicks", 0),
                    "avg_position": round(r.get("avg_position", 0), 1),
                    "growth_rate": round(growth_rate, 2),
                    "growth_signal": "impression_surge",
                })

        emerging.sort(key=lambda e: -e.get("impressions", 0))
        return emerging[:40]

    # ------------------------------------------------------------------
    # 4. Content alignment analysis
    # ------------------------------------------------------------------

    def _analyze_content_alignment(
        self,
        recent_intents: Dict[str, Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Check whether ranking pages match the dominant intent of their queries.

        Uses page_data URL patterns and the query-page mapping from timeseries.
        """
        misalignments: List[Dict[str, Any]] = []

        # Build query -> pages map from timeseries
        df = self.query_ts.copy()
        query_col = "query"
        page_col = "page"
        for col in ["keyword", "search_query"]:
            if col in df.columns and query_col not in df.columns:
                query_col = col
        for col in ["url", "landing_page"]:
            if col in df.columns and page_col not in df.columns:
                page_col = col

        if query_col not in df.columns or page_col not in df.columns:
            return misalignments

        query_pages = df.groupby(query_col)[page_col].apply(
            lambda x: x.value_counts().index[0] if len(x) > 0 else None
        ).to_dict()

        # Intent -> ideal page type mapping
        intent_page_fit = {
            "transactional": {"product", "landing_page", "category", "tool"},
            "commercial": {"blog", "category", "product", "documentation"},
            "informational": {"blog", "documentation", "tool", "other"},
            "navigational": {"homepage", "landing_page", "other"},
        }

        for query, intent_info in recent_intents.items():
            if intent_info.get("impressions", 0) < self.MIN_IMPRESSIONS_THRESHOLD:
                continue

            primary_intent = intent_info.get("primary_intent", "informational")
            page_url = query_pages.get(query)
            if not page_url:
                continue

            page_meta = self._page_meta_map.get(page_url, {})
            page_type = infer_page_type(page_url, page_meta)
            ideal_types = intent_page_fit.get(primary_intent, set())

            if page_type not in ideal_types:
                severity = "high" if intent_info.get("confidence", 0) >= 0.7 else "medium"
                # Stronger severity if transactional intent served by blog
                if primary_intent == "transactional" and page_type in ("blog", "documentation"):
                    severity = "critical"

                misalignments.append({
                    "query": query,
                    "query_intent": primary_intent,
                    "intent_confidence": intent_info.get("confidence", 0),
                    "ranking_page": page_url,
                    "page_type": page_type,
                    "ideal_page_types": sorted(ideal_types),
                    "severity": severity,
                    "impressions": intent_info.get("impressions", 0),
                    "clicks": intent_info.get("clicks", 0),
                    "avg_position": round(intent_info.get("avg_position", 0), 1),
                    "recommendation": self._alignment_recommendation(
                        primary_intent, page_type, query
                    ),
                })

        misalignments.sort(
            key=lambda m: (
                {"critical": 0, "high": 1, "medium": 2}.get(m["severity"], 3),
                -m["impressions"],
            )
        )
        return misalignments[:40]

    @staticmethod
    def _alignment_recommendation(intent: str, page_type: str, query: str) -> str:
        """Generate a specific recommendation for a misaligned page."""
        if intent == "transactional" and page_type in ("blog", "documentation"):
            return (
                f"Create a dedicated product/landing page targeting '{query}' — "
                f"searchers want to take action but are landing on a {page_type} page. "
                f"Add CTAs and conversion elements to the existing page as a short-term fix."
            )
        if intent == "commercial" and page_type == "product":
            return (
                f"'{query}' shows comparison/research intent. Consider adding a comparison "
                f"or review section to the product page, or create a supporting blog post "
                f"that links to this product."
            )
        if intent == "informational" and page_type in ("product", "landing_page"):
            return (
                f"'{query}' is informational but ranks a {page_type}. Create an educational "
                f"blog post or guide targeting this query, and link it to the {page_type} "
                f"for conversion."
            )
        if intent == "navigational":
            return (
                f"'{query}' appears navigational — ensure the target page has clear branding "
                f"and direct navigation. Consider adding structured data (Organization/Website schema)."
            )
        return (
            f"Review the content on the ranking page for '{query}' and ensure it matches "
            f"the dominant {intent} search intent."
        )

    # ------------------------------------------------------------------
    # 5. Portfolio-level intent distribution
    # ------------------------------------------------------------------

    def _intent_portfolio_summary(
        self,
        recent_intents: Dict[str, Dict[str, Any]],
        comparison_intents: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Summarise the overall intent distribution of the query portfolio
        and how it has shifted between windows.
        """
        def _weighted_distribution(intents: Dict[str, Dict[str, Any]]) -> Dict[str, float]:
            totals: Dict[str, float] = defaultdict(float)
            total_imp = 0.0
            for info in intents.values():
                imp = info.get("impressions", 0)
                total_imp += imp
                dist = info.get("intent_distribution", {})
                for intent_type, prob in dist.items():
                    totals[intent_type] += prob * imp
            if total_imp > 0:
                return {k: round(v / total_imp, 4) for k, v in totals.items()}
            return dict(totals)

        recent_dist = _weighted_distribution(recent_intents)
        prev_dist = _weighted_distribution(comparison_intents)

        changes = {}
        all_types = set(list(recent_dist.keys()) + list(prev_dist.keys()))
        for it in all_types:
            r = recent_dist.get(it, 0)
            p = prev_dist.get(it, 0)
            changes[it] = {
                "recent": round(r, 4),
                "previous": round(p, 4),
                "change": round(r - p, 4),
                "direction": "growing" if r > p else ("declining" if r < p else "stable"),
            }

        # Dominant intent
        dominant = max(recent_dist, key=recent_dist.get) if recent_dist else "informational"

        return {
            "recent_distribution": recent_dist,
            "previous_distribution": prev_dist,
            "changes_by_intent": changes,
            "dominant_intent": dominant,
            "total_queries_recent": len(recent_intents),
            "total_queries_comparison": len(comparison_intents),
        }

    # ------------------------------------------------------------------
    # 6. Generate recommendations
    # ------------------------------------------------------------------

    def _generate_recommendations(
        self,
        shifts: List[Dict[str, Any]],
        emerging: List[Dict[str, Any]],
        misalignments: List[Dict[str, Any]],
        portfolio: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """Produce prioritised, actionable recommendations."""
        recs: List[Dict[str, Any]] = []

        # Critical misalignments
        critical_misalign = [m for m in misalignments if m["severity"] == "critical"]
        if critical_misalign:
            total_imp = sum(m["impressions"] for m in critical_misalign)
            recs.append({
                "priority": 1,
                "category": "content_misalignment",
                "title": "Fix critical intent-content mismatches",
                "description": (
                    f"{len(critical_misalign)} queries with transactional intent are served by "
                    f"blog/doc pages ({int(total_imp):,} total impressions). Create dedicated "
                    f"landing or product pages to capture conversion-ready traffic."
                ),
                "affected_queries": [m["query"] for m in critical_misalign[:5]],
                "estimated_impact": "high",
            })

        # Major intent shifts
        primary_shifts = [s for s in shifts if s["primary_changed"]]
        if primary_shifts:
            to_transactional = [
                s for s in primary_shifts if s["current_intent"] == "transactional"
            ]
            if to_transactional:
                recs.append({
                    "priority": 2,
                    "category": "intent_shift",
                    "title": "Queries shifting to transactional intent",
                    "description": (
                        f"{len(to_transactional)} queries have shifted toward transactional intent. "
                        f"Users searching these terms now want to buy/act — update content with "
                        f"CTAs, pricing, and conversion elements."
                    ),
                    "affected_queries": [s["query"] for s in to_transactional[:5]],
                    "estimated_impact": "high",
                })

            from_informational = [
                s for s in primary_shifts
                if s["previous_intent"] == "informational" and s["current_intent"] != "informational"
            ]
            if from_informational and len(from_informational) != len(to_transactional):
                recs.append({
                    "priority": 3,
                    "category": "intent_shift",
                    "title": "Queries migrating away from informational intent",
                    "description": (
                        f"{len(from_informational)} queries are shifting from informational to "
                        f"commercial/transactional. These represent funnel progression — ensure "
                        f"content evolves to match the new dominant intent."
                    ),
                    "affected_queries": [s["query"] for s in from_informational[:5]],
                    "estimated_impact": "medium",
                })

        # Emerging queries
        new_queries = [e for e in emerging if e["type"] == "new_query"]
        if new_queries:
            recs.append({
                "priority": 4,
                "category": "emerging_intent",
                "title": "New queries appearing in your portfolio",
                "description": (
                    f"{len(new_queries)} new queries have appeared with significant impressions. "
                    f"Create or optimise content to capture these emerging opportunities."
                ),
                "affected_queries": [e["query"] for e in new_queries[:5]],
                "estimated_impact": "medium",
            })

        surging = [e for e in emerging if e["type"] == "rapid_growth"]
        if surging:
            recs.append({
                "priority": 5,
                "category": "emerging_intent",
                "title": "Rapidly growing search demand",
                "description": (
                    f"{len(surging)} existing queries show 50%+ impression growth. "
                    f"Prioritise content updates and expansion for these high-momentum terms."
                ),
                "affected_queries": [e["query"] for e in surging[:5]],
                "estimated_impact": "medium",
            })

        # Portfolio-level shift
        changes = portfolio.get("changes_by_intent", {})
        commercial_change = changes.get("commercial", {}).get("change", 0)
        transactional_change = changes.get("transactional", {}).get("change", 0)
        if commercial_change + transactional_change > 0.05:
            recs.append({
                "priority": 6,
                "category": "portfolio_trend",
                "title": "Portfolio shifting toward commercial/transactional",
                "description": (
                    f"Your overall query portfolio is trending toward purchase-related intent "
                    f"(commercial +{commercial_change:.1%}, transactional +{transactional_change:.1%}). "
                    f"Consider adding more bottom-of-funnel content, product comparisons, and "
                    f"conversion-optimised pages."
                ),
                "affected_queries": [],
                "estimated_impact": "medium",
            })

        # High-volume misalignments
        high_misalign = [m for m in misalignments if m["severity"] == "high"]
        if high_misalign:
            recs.append({
                "priority": 7,
                "category": "content_misalignment",
                "title": "High-volume intent-content mismatches",
                "description": (
                    f"{len(high_misalign)} additional queries have significant intent-content "
                    f"mismatches. Review and realign page types to match user intent."
                ),
                "affected_queries": [m["query"] for m in high_misalign[:5]],
                "estimated_impact": "medium",
            })

        recs.sort(key=lambda r: r["priority"])
        return recs

    # ------------------------------------------------------------------
    # 7. Build summary
    # ------------------------------------------------------------------

    def _build_summary(
        self,
        shifts: List[Dict[str, Any]],
        emerging: List[Dict[str, Any]],
        misalignments: List[Dict[str, Any]],
        portfolio: Dict[str, Any],
    ) -> str:
        """Generate a human-readable narrative summary."""
        parts: List[str] = []

        dominant = portfolio.get("dominant_intent", "informational")
        n_recent = portfolio.get("total_queries_recent", 0)
        parts.append(
            f"Analyzed intent migration across {n_recent:,} queries. "
            f"The portfolio's dominant intent is '{dominant}'."
        )

        primary_shifts = [s for s in shifts if s["primary_changed"]]
        if primary_shifts:
            parts.append(
                f"{len(primary_shifts)} queries have undergone a primary intent shift."
            )
            # Most common shift direction
            shift_dirs: Dict[str, int] = defaultdict(int)
            for s in primary_shifts:
                key = f"{s['previous_intent']} -> {s['current_intent']}"
                shift_dirs[key] += 1
            top_dir = max(shift_dirs, key=shift_dirs.get)
            parts.append(f"The most common shift direction is {top_dir} ({shift_dirs[top_dir]} queries).")
        else:
            parts.append("No primary intent shifts detected — the query portfolio is stable.")

        new_q = [e for e in emerging if e["type"] == "new_query"]
        surge_q = [e for e in emerging if e["type"] == "rapid_growth"]
        if new_q or surge_q:
            parts.append(
                f"Found {len(new_q)} new queries and {len(surge_q)} rapidly growing queries."
            )

        critical = [m for m in misalignments if m["severity"] == "critical"]
        high = [m for m in misalignments if m["severity"] == "high"]
        if critical or high:
            parts.append(
                f"Identified {len(critical)} critical and {len(high)} high-severity "
                f"content-intent misalignments requiring attention."
            )

        return " ".join(parts)

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def analyze(self) -> Dict[str, Any]:
        """Run the full intent migration analysis."""
        logger.info("Starting intent migration analysis")

        # Step 1: Split time windows
        recent_df, comparison_df = self._split_time_windows()

        # Step 2: Aggregate to query level
        recent_agg = self._aggregate_query_metrics(recent_df)
        comparison_agg = self._aggregate_query_metrics(comparison_df)

        if recent_agg.empty:
            logger.warning("No recent query data — returning minimal results")
            return {
                "summary": "Insufficient recent query data for intent migration analysis.",
                "intent_shifts": [],
                "emerging_intents": [],
                "content_alignment": [],
                "portfolio_distribution": {},
                "recommendations": [],
            }

        # Step 3: Classify intent per window
        recent_intents = self._classify_window(recent_agg)
        comparison_intents = self._classify_window(comparison_agg) if not comparison_agg.empty else {}

        # Step 4: Detect shifts
        shifts = self._detect_shifts(recent_intents, comparison_intents)

        # Step 5: Emerging intents
        emerging = self._identify_emerging_intents(recent_intents, comparison_intents)

        # Step 6: Content alignment
        misalignments = self._analyze_content_alignment(recent_intents)

        # Step 7: Portfolio summary
        portfolio = self._intent_portfolio_summary(recent_intents, comparison_intents)

        # Step 8: Recommendations
        recommendations = self._generate_recommendations(shifts, emerging, misalignments, portfolio)

        # Step 9: Narrative summary
        summary = self._build_summary(shifts, emerging, misalignments, portfolio)

        logger.info(
            "Intent migration analysis complete: %d shifts, %d emerging, %d misalignments",
            len(shifts), len(emerging), len(misalignments),
        )

        return {
            "summary": summary,
            "intent_shifts": shifts,
            "emerging_intents": emerging,
            "content_alignment": misalignments,
            "portfolio_distribution": portfolio,
            "recommendations": recommendations,
        }


# ---------------------------------------------------------------------------
# Public function — matches the signature expected by routes/modules.py
# ---------------------------------------------------------------------------

def analyze_intent_migration(
    query_timeseries,
    serp_data=None,
    page_data=None,
) -> Dict[str, Any]:
    """
    Module 7: Intent Migration — tracks query intent shifts over time.

    Args:
        query_timeseries: pandas DataFrame with columns
            [query, page, date, clicks, impressions, ctr, position].
            Daily granularity, from GSC query_daily_timeseries.
        serp_data: Optional dict with DataForSEO SERP results (features, rankings).
        page_data: Optional DataFrame or dict with crawl metadata per URL
            (title, word_count, content_type, etc.).

    Returns:
        Dict with keys: summary, intent_shifts, emerging_intents,
        content_alignment, portfolio_distribution, recommendations.
    """
    logger.info("Running analyze_intent_migration (full implementation)")

    analyzer = IntentMigrationAnalyzer(
        query_timeseries=query_timeseries,
        serp_data=serp_data,
        page_data=page_data,
    )
    return analyzer.analyze()
