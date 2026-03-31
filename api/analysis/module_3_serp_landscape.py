"""
Module 3: SERP Landscape — SERP feature detection, competitor overlap,
keyword opportunity mapping, intent classification, and click share estimation.

Phase 2 full implementation.  Consumes DataForSEO SERP results together with
GSC keyword performance data and produces:
  1. SERP feature displacement analysis (visual-position shift)
  2. Competitor mapping with threat scoring
  3. Intent classification & mismatch detection
  4. Click-share estimation per keyword
"""

import logging
from typing import Any, Dict, List, Optional
from collections import Counter, defaultdict
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SERP_FEATURE_WEIGHTS: Dict[str, float] = {
    "featured_snippet": 2.0,
    "knowledge_panel": 1.5,
    "ai_overview": 2.5,
    "local_pack": 3.0,
    "people_also_ask": 0.5,
    "video_carousel": 1.0,
    "image_pack": 0.5,
    "shopping_results": 1.0,
    "top_stories": 1.0,
    "reddit_threads": 0.3,
}

GENERIC_CTR_BY_POSITION: Dict[int, float] = {
    1: 0.284, 2: 0.147, 3: 0.082, 4: 0.053, 5: 0.038,
    6: 0.030, 7: 0.024, 8: 0.020, 9: 0.017, 10: 0.015,
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_domain(url: str) -> str:
    """Return the bare domain (no www.) from *url*."""
    try:
        domain = urlparse(url).netloc.lower()
        return domain[4:] if domain.startswith("www.") else domain
    except Exception:
        return ""


def _is_user_domain(domain: str, serp: Dict[str, Any]) -> bool:
    user_domain = serp.get("user_domain", "").lower()
    if user_domain.startswith("www."):
        user_domain = user_domain[4:]
    return domain == user_domain


def _find_user_result(serp: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    user_domain = serp.get("user_domain", "").lower()
    if user_domain.startswith("www."):
        user_domain = user_domain[4:]
    for result in serp.get("organic_results", []):
        if _extract_domain(result.get("url", "")) == user_domain:
            return result
    return None


def _features_above_position(serp: Dict[str, Any], position: float) -> List[str]:
    """Return list of SERP feature names appearing above *position*."""
    features: List[str] = []

    if serp.get("featured_snippet") and serp["featured_snippet"].get("position", 100) < position:
        features.append("featured_snippet")

    if serp.get("knowledge_panel"):
        features.append("knowledge_panel")

    if serp.get("ai_overview"):
        features.append("ai_overview")

    if serp.get("local_pack") and serp["local_pack"].get("position", 100) < position:
        features.append("local_pack")

    paa = serp.get("people_also_ask", [])
    paa_above = sum(1 for q in paa if q.get("position", 100) < position)
    features.extend(["people_also_ask"] * paa_above)

    if serp.get("video_results") and any(
        v.get("position", 100) < position for v in serp.get("video_results", [])
    ):
        features.append("video_carousel")

    if serp.get("images_pack") and serp["images_pack"].get("position", 100) < position:
        features.append("image_pack")

    if serp.get("shopping_results") and any(
        s.get("position", 100) < position for s in serp.get("shopping_results", [])
    ):
        features.append("shopping_results")

    return features


def _visual_position(organic_position: int, features_above: List[str]) -> float:
    displacement = sum(SERP_FEATURE_WEIGHTS.get(f, 0.5) for f in features_above)
    return organic_position + displacement


def _classify_keyword_intent(keyword: str, serp: Dict[str, Any]) -> str:
    kw = keyword.lower()
    if any(w in kw for w in ("login", "sign in", "account", "dashboard")):
        return "navigational"
    if any(w in kw for w in ("buy", "purchase", "price", "deal", "discount", "coupon", "order")):
        return "transactional"
    if any(w in kw for w in ("best", "top", "review", "compare", "vs", "alternative")):
        return "commercial"
    if serp.get("shopping_results"):
        return "transactional"
    if serp.get("knowledge_panel") or len(serp.get("people_also_ask", [])) > 3:
        return "informational"
    if any(kw.startswith(w) for w in ("how", "what", "why", "when", "who", "where")):
        return "informational"
    return "informational"


def _infer_page_type(url: str) -> str:
    u = url.lower()
    if any(p in u for p in ("/blog/", "/article/", "/guide/", "/learn/")):
        return "blog"
    if any(p in u for p in ("/product/", "/pricing/", "/buy/", "/shop/")):
        return "product"
    if any(p in u for p in ("/category/", "/collection/")):
        return "category"
    if u.count("/") <= 3:
        return "homepage"
    return "other"


def _is_intent_mismatch(serp_intent: str, page_type: str) -> bool:
    mismatches = {
        "transactional": ["blog"],
        "commercial": ["blog"],
        "informational": ["product"],
    }
    return page_type in mismatches.get(serp_intent, [])


def _mismatch_recommendation(serp_intent: str, page_type: str) -> str:
    if serp_intent in ("transactional", "commercial") and page_type == "blog":
        return "Consider creating a dedicated product/comparison page for this keyword"
    if serp_intent == "informational" and page_type == "product":
        return "Consider adding educational content or creating a supporting blog post"
    return "Review content alignment with search intent"


# ---------------------------------------------------------------------------
# SERP feature summary  (new — not in modules/ version)
# ---------------------------------------------------------------------------

def _serp_feature_summary(serp_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Aggregate which SERP features appear across all analysed keywords."""
    feature_counts: Dict[str, int] = Counter()
    feature_keywords: Dict[str, List[str]] = defaultdict(list)

    for serp in serp_data:
        kw = serp.get("keyword", "")
        if serp.get("featured_snippet"):
            feature_counts["featured_snippet"] += 1
            feature_keywords["featured_snippet"].append(kw)
        if serp.get("knowledge_panel"):
            feature_counts["knowledge_panel"] += 1
            feature_keywords["knowledge_panel"].append(kw)
        if serp.get("ai_overview"):
            feature_counts["ai_overview"] += 1
            feature_keywords["ai_overview"].append(kw)
        if serp.get("local_pack"):
            feature_counts["local_pack"] += 1
            feature_keywords["local_pack"].append(kw)
        if serp.get("people_also_ask"):
            feature_counts["people_also_ask"] += 1
            feature_keywords["people_also_ask"].append(kw)
        if serp.get("video_results"):
            feature_counts["video_carousel"] += 1
            feature_keywords["video_carousel"].append(kw)
        if serp.get("images_pack"):
            feature_counts["image_pack"] += 1
            feature_keywords["image_pack"].append(kw)
        if serp.get("shopping_results"):
            feature_counts["shopping_results"] += 1
            feature_keywords["shopping_results"].append(kw)

    total = len(serp_data) or 1
    return {
        "feature_prevalence": {
            f: {"count": c, "pct": round(c / total * 100, 1)}
            for f, c in feature_counts.most_common()
        },
        "feature_sample_keywords": {
            f: kws[:5] for f, kws in feature_keywords.items()
        },
    }


# ---------------------------------------------------------------------------
# Core analysis sections
# ---------------------------------------------------------------------------

def _analyze_displacement(serp_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """SERP feature displacement — how far visual position shifts from organic."""
    results: List[Dict[str, Any]] = []

    for serp in serp_data:
        try:
            user_result = _find_user_result(serp)
            if user_result is None:
                continue
            user_pos = user_result.get("position")
            if user_pos is None:
                continue

            features = _features_above_position(serp, user_pos)
            vis_pos = _visual_position(user_pos, features)

            if vis_pos > user_pos + 2:
                generic_ctr = GENERIC_CTR_BY_POSITION.get(user_pos, 0.01)
                adjusted_ctr = GENERIC_CTR_BY_POSITION.get(min(int(vis_pos), 10), 0.01)

                results.append({
                    "keyword": serp.get("keyword", ""),
                    "organic_position": user_pos,
                    "visual_position": round(vis_pos, 1),
                    "displacement": round(vis_pos - user_pos, 1),
                    "features_above": features,
                    "estimated_ctr_impact": round(adjusted_ctr - generic_ctr, 3),
                })
        except Exception as exc:
            logger.warning("Displacement error for keyword: %s", exc)

    results.sort(key=lambda x: x["displacement"], reverse=True)
    return results[:50]


def _analyze_competitors(serp_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Map competitor presence across the analysed keyword set."""
    freq: Counter = Counter()
    positions: Dict[str, List[int]] = defaultdict(list)

    for serp in serp_data:
        try:
            for result in serp.get("organic_results", [])[:10]:
                domain = _extract_domain(result.get("url", ""))
                if domain and not _is_user_domain(domain, serp):
                    freq[domain] += 1
                    positions[domain].append(result.get("position", 100))
        except Exception as exc:
            logger.warning("Competitor mapping error: %s", exc)

    total_kw = len(serp_data) or 1
    competitors: List[Dict[str, Any]] = []

    for domain, count in freq.most_common(20):
        avg_pos = sum(positions[domain]) / len(positions[domain])
        overlap_pct = count / total_kw * 100

        if overlap_pct > 40 and avg_pos < 5:
            threat = "critical"
        elif overlap_pct > 30 and avg_pos < 7:
            threat = "high"
        elif overlap_pct > 20 or avg_pos < 5:
            threat = "medium"
        else:
            threat = "low"

        competitors.append({
            "domain": domain,
            "keywords_shared": count,
            "overlap_percentage": round(overlap_pct, 1),
            "avg_position": round(avg_pos, 1),
            "threat_level": threat,
        })

    return competitors


def _analyze_intents(serp_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Classify search intent and detect user-page mismatches."""
    dist: Dict[str, int] = {"informational": 0, "commercial": 0, "navigational": 0, "transactional": 0}
    mismatches: List[Dict[str, Any]] = []

    for serp in serp_data:
        try:
            kw = serp.get("keyword", "")
            intent = _classify_keyword_intent(kw, serp)
            dist[intent] += 1

            user_result = _find_user_result(serp)
            if user_result:
                pt = _infer_page_type(user_result.get("url", ""))
                if _is_intent_mismatch(intent, pt):
                    mismatches.append({
                        "keyword": kw,
                        "serp_intent": intent,
                        "page_type": pt,
                        "user_position": user_result.get("position", 0),
                        "recommendation": _mismatch_recommendation(intent, pt),
                    })
        except Exception as exc:
            logger.warning("Intent classification error: %s", exc)

    total = sum(dist.values()) or 1
    return {
        "intent_distribution": {k: round(v / total, 3) for k, v in dist.items()},
        "intent_mismatches": mismatches[:20],
    }


def _estimate_click_share(serp_data: List[Dict[str, Any]], gsc_keyword_data) -> Dict[str, Any]:
    """Estimate organic click-share across the keyword portfolio."""
    try:
        import pandas as pd
        gsc_df = gsc_keyword_data if isinstance(gsc_keyword_data, pd.DataFrame) else pd.DataFrame()
    except ImportError:
        gsc_df = None

    total_clicks = 0
    total_potential = 0
    breakdown: List[Dict[str, Any]] = []

    for serp in serp_data:
        try:
            kw = serp.get("keyword", "")

            # Look up GSC data for this keyword
            if gsc_df is not None and not gsc_df.empty:
                row = gsc_df[gsc_df["query"] == kw]
                if row.empty:
                    continue
                row = row.iloc[0]
                impressions = row.get("impressions", 0)
                clicks = row.get("clicks", 0)
                position = row.get("position", 100)
            else:
                continue

            potential = impressions * GENERIC_CTR_BY_POSITION.get(1, 0.28)

            # Reduce potential when heavy SERP features are present
            features = _features_above_position(serp, position)
            heavy = sum(1 for f in features if f != "people_also_ask")
            if heavy > 2:
                potential *= 0.7

            share = clicks / potential if potential > 0 else 0.0
            total_clicks += clicks
            total_potential += potential

            breakdown.append({
                "keyword": kw,
                "clicks": int(clicks),
                "potential_clicks": round(potential, 1),
                "click_share": round(share, 3),
                "position": round(position, 1),
            })
        except Exception as exc:
            logger.warning("Click-share error: %s", exc)

    overall = total_clicks / total_potential if total_potential > 0 else 0.0
    breakdown.sort(key=lambda x: x["clicks"], reverse=True)

    return {
        "total_click_share": round(overall, 3),
        "current_monthly_clicks": int(total_clicks),
        "potential_monthly_clicks": int(total_potential),
        "click_opportunity": int(total_potential - total_clicks),
        "keyword_breakdown": breakdown[:30],
    }


# ---------------------------------------------------------------------------
# Public entry point  (imported by api.routes.modules)
# ---------------------------------------------------------------------------

def analyze_serp_landscape(serp_data, gsc_keyword_data=None) -> Dict[str, Any]:
    """
    Module 3: SERP Landscape Analysis.

    Args:
        serp_data: list of SERP result dicts from DataForSEO / serp_helper.
        gsc_keyword_data: optional pandas DataFrame of GSC keyword metrics.

    Returns:
        Dict with displacement, competitors, intent, click_share, feature_summary, and summary.
    """
    logger.info("Running analyze_serp_landscape — Phase 2 full implementation")

    if not serp_data:
        logger.warning("No SERP data provided; returning empty analysis")
        return _empty_result()

    try:
        displacement = _analyze_displacement(serp_data)
        competitors = _analyze_competitors(serp_data)
        intent_analysis = _analyze_intents(serp_data)
        click_share = _estimate_click_share(serp_data, gsc_keyword_data)
        feature_summary = _serp_feature_summary(serp_data)

        summary = {
            "keywords_analyzed": len(serp_data),
            "keywords_with_significant_displacement": len(displacement),
            "avg_visual_displacement": (
                round(sum(d["displacement"] for d in displacement) / len(displacement), 1)
                if displacement else 0
            ),
            "primary_competitors_count": sum(
                1 for c in competitors if c["threat_level"] in ("high", "critical")
            ),
            "total_click_share": click_share.get("total_click_share", 0),
            "click_opportunity_size": click_share.get("click_opportunity", 0),
            "dominant_intent": max(
                intent_analysis["intent_distribution"],
                key=intent_analysis["intent_distribution"].get,
            ) if intent_analysis["intent_distribution"] else "unknown",
            "intent_mismatches_found": len(intent_analysis.get("intent_mismatches", [])),
        }

        result = {
            "keywords_analyzed": len(serp_data),
            "serp_feature_displacement": displacement,
            "serp_feature_summary": feature_summary,
            "competitors": competitors,
            "intent_analysis": intent_analysis,
            "click_share": click_share,
            "summary": summary,
        }

        logger.info(
            "SERP landscape analysis complete: %d keywords, %d displaced, %d competitors",
            len(serp_data), len(displacement), len(competitors),
        )
        return result

    except Exception as exc:
        logger.error("Error in SERP landscape analysis: %s", exc, exc_info=True)
        return _empty_result()


def _empty_result() -> Dict[str, Any]:
    return {
        "keywords_analyzed": 0,
        "serp_feature_displacement": [],
        "serp_feature_summary": {"feature_prevalence": {}, "feature_sample_keywords": {}},
        "competitors": [],
        "intent_analysis": {
            "intent_distribution": {"informational": 0, "commercial": 0, "navigational": 0, "transactional": 0},
            "intent_mismatches": [],
        },
        "click_share": {
            "total_click_share": 0,
            "current_monthly_clicks": 0,
            "potential_monthly_clicks": 0,
            "click_opportunity": 0,
            "keyword_breakdown": [],
        },
        "summary": {
            "keywords_analyzed": 0,
            "keywords_with_significant_displacement": 0,
            "avg_visual_displacement": 0,
            "primary_competitors_count": 0,
            "total_click_share": 0,
            "click_opportunity_size": 0,
            "dominant_intent": "unknown",
            "intent_mismatches_found": 0,
        },
    }
