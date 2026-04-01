"""
Module 8: CTR Modeling by SERP Context — build a gradient-boosting CTR
prediction model on the user's own GSC + SERP data, identify over/under-
performers, calculate SERP-context-adjusted click values, and score
SERP-feature acquisition opportunities.

Spec reference (supabase/spec.md — Module 8):
  Input:  DataForSEO SERP data, GSC position + CTR data
  Output: ctr_model_accuracy, keyword_ctr_analysis, feature_opportunities,
          contextual_ctr_benchmarks

This replaces the prior "Technical Health" implementation which did not
match the spec.  Technical SEO auditing (CWV, indexing, mobile) is not
part of the module numbering — it could be added as a supplementary
check inside Module 2 (Page Triage) or as a future Module 13.
"""

import logging
import math
from collections import Counter, defaultdict
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Generic CTR benchmarks by organic position (no SERP-feature context).
# Derived from aggregated industry data — used as a fallback when the
# gradient-boosting model cannot be trained (insufficient data).
GENERIC_CTR_BY_POSITION = {
    1: 0.280, 2: 0.155, 3: 0.110, 4: 0.080, 5: 0.067,
    6: 0.047, 7: 0.038, 8: 0.031, 9: 0.026, 10: 0.022,
    11: 0.012, 12: 0.010, 13: 0.009, 14: 0.008, 15: 0.007,
    16: 0.006, 17: 0.005, 18: 0.005, 19: 0.004, 20: 0.004,
}

# SERP feature weights — approximate CTR displacement each feature
# causes for organic results positioned below it.
FEATURE_CTR_DISPLACEMENT = {
    "featured_snippet": -0.08,
    "ai_overview": -0.10,
    "people_also_ask": -0.015,  # per PAA block (typically 4 items)
    "video_carousel": -0.03,
    "local_pack": -0.05,
    "shopping_results": -0.06,
    "knowledge_panel": -0.04,
    "top_stories": -0.03,
    "image_pack": -0.02,
    "sitelinks": -0.02,
    "ads_top": -0.035,  # per ad
    "reddit_threads": -0.02,
}

# Minimum rows required to train the gradient-boosting model.
MIN_TRAINING_ROWS = 30

# SERP features that can be targeted (opportunity scoring)
TARGETABLE_FEATURES = {
    "featured_snippet": {
        "difficulty_base": "medium",
        "content_types": ["how-to", "definition", "list", "comparison"],
        "effort": "Add structured content targeting the snippet format",
    },
    "people_also_ask": {
        "difficulty_base": "low",
        "content_types": ["faq"],
        "effort": "Add FAQ schema + answer-formatted sections",
    },
    "video_carousel": {
        "difficulty_base": "high",
        "content_types": ["video"],
        "effort": "Create and embed a YouTube video targeting this query",
    },
}


# ---------------------------------------------------------------------------
# Feature extraction
# ---------------------------------------------------------------------------

def _extract_serp_features(serp_entry: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse a DataForSEO SERP result dict and extract boolean/count features
    for the CTR model.

    DataForSEO organic/live/advanced returns items with ``type`` fields like
    ``"featured_snippet"``, ``"people_also_ask"``, ``"local_pack"`` etc.
    We also look for ``"paid"`` items (ads) and count them.
    """
    features: Dict[str, Any] = {
        "has_featured_snippet": False,
        "has_ai_overview": False,
        "paa_count": 0,
        "has_video_carousel": False,
        "has_local_pack": False,
        "has_shopping": False,
        "has_knowledge_panel": False,
        "has_top_stories": False,
        "has_image_pack": False,
        "has_reddit_threads": False,
        "ads_above_count": 0,
        "organic_results_above_fold": 0,
    }

    if not serp_entry or not isinstance(serp_entry, dict):
        return features

    items = serp_entry.get("items") or serp_entry.get("result", [])
    if isinstance(items, dict):
        items = items.get("items", [])
    if not isinstance(items, list):
        return features

    for item in items:
        if not isinstance(item, dict):
            continue
        item_type = str(item.get("type", "")).lower()
        position = item.get("position", item.get("rank_absolute", 99))

        if "featured_snippet" in item_type:
            features["has_featured_snippet"] = True
        elif "ai_overview" in item_type or "ai_result" in item_type:
            features["has_ai_overview"] = True
        elif "people_also_ask" in item_type:
            # Count individual PAA items
            sub_items = item.get("items", [])
            features["paa_count"] = len(sub_items) if isinstance(sub_items, list) else 4
        elif "video" in item_type:
            features["has_video_carousel"] = True
        elif "local_pack" in item_type or "maps" in item_type:
            features["has_local_pack"] = True
        elif "shopping" in item_type:
            features["has_shopping"] = True
        elif "knowledge" in item_type:
            features["has_knowledge_panel"] = True
        elif "top_stories" in item_type or "news" in item_type:
            features["has_top_stories"] = True
        elif "images" in item_type or "image_pack" in item_type:
            features["has_image_pack"] = True
        elif "reddit" in item_type or "discussions" in item_type:
            features["has_reddit_threads"] = True
        elif "paid" in item_type or "ad" in item_type:
            features["ads_above_count"] += 1
        elif "organic" in item_type:
            if isinstance(position, (int, float)) and position <= 5:
                features["organic_results_above_fold"] += 1

    return features


def _calculate_visual_position(
    organic_rank: int,
    serp_features: Dict[str, Any],
) -> float:
    """
    Estimate how far down the user's listing appears visually, accounting
    for SERP features that push organic results down.

    Each SERP feature occupies visual "slots":
      featured_snippet  = 2 slots
      ai_overview       = 3 slots
      PAA               = 0.5 slots each (typically 4 = 2 slots)
      ads               = 1 slot each
      local_pack        = 3 slots
      shopping          = 2 slots
      everything else   = 1 slot
    """
    visual_offset = 0.0

    if serp_features.get("has_featured_snippet"):
        visual_offset += 2.0
    if serp_features.get("has_ai_overview"):
        visual_offset += 3.0
    visual_offset += serp_features.get("paa_count", 0) * 0.5
    visual_offset += serp_features.get("ads_above_count", 0) * 1.0
    if serp_features.get("has_local_pack"):
        visual_offset += 3.0
    if serp_features.get("has_shopping"):
        visual_offset += 2.0
    if serp_features.get("has_video_carousel"):
        visual_offset += 1.5
    if serp_features.get("has_knowledge_panel"):
        visual_offset += 1.0
    if serp_features.get("has_top_stories"):
        visual_offset += 1.5
    if serp_features.get("has_image_pack"):
        visual_offset += 1.0
    if serp_features.get("has_reddit_threads"):
        visual_offset += 1.0

    return organic_rank + visual_offset


def _build_feature_vector(
    position: float,
    serp_features: Dict[str, Any],
) -> List[float]:
    """
    Build a numeric feature vector for the gradient-boosting model.

    Features (12 total):
      0: position (float)
      1: has_featured_snippet (0/1)
      2: has_ai_overview (0/1)
      3: paa_count (int)
      4: has_video_carousel (0/1)
      5: has_local_pack (0/1)
      6: has_shopping (0/1)
      7: has_knowledge_panel (0/1)
      8: ads_above_count (int)
      9: has_top_stories (0/1)
     10: has_image_pack (0/1)
     11: has_reddit_threads (0/1)
    """
    return [
        float(position),
        float(serp_features.get("has_featured_snippet", False)),
        float(serp_features.get("has_ai_overview", False)),
        float(serp_features.get("paa_count", 0)),
        float(serp_features.get("has_video_carousel", False)),
        float(serp_features.get("has_local_pack", False)),
        float(serp_features.get("has_shopping", False)),
        float(serp_features.get("has_knowledge_panel", False)),
        float(serp_features.get("ads_above_count", 0)),
        float(serp_features.get("has_top_stories", False)),
        float(serp_features.get("has_image_pack", False)),
        float(serp_features.get("has_reddit_threads", False)),
    ]


# ---------------------------------------------------------------------------
# CTR estimation (rule-based fallback)
# ---------------------------------------------------------------------------

def _estimate_contextual_ctr(
    position: int,
    serp_features: Dict[str, Any],
) -> float:
    """
    Rule-based contextual CTR estimate — used when the gradient-boosting
    model cannot be trained (insufficient data).

    Starts with the generic position-based CTR and adjusts downward for
    each SERP feature present.
    """
    base_ctr = GENERIC_CTR_BY_POSITION.get(
        min(max(round(position), 1), 20),
        0.003,
    )

    adjustment = 0.0
    if serp_features.get("has_featured_snippet"):
        adjustment += FEATURE_CTR_DISPLACEMENT["featured_snippet"]
    if serp_features.get("has_ai_overview"):
        adjustment += FEATURE_CTR_DISPLACEMENT["ai_overview"]
    adjustment += serp_features.get("paa_count", 0) * FEATURE_CTR_DISPLACEMENT["people_also_ask"]
    if serp_features.get("has_video_carousel"):
        adjustment += FEATURE_CTR_DISPLACEMENT["video_carousel"]
    if serp_features.get("has_local_pack"):
        adjustment += FEATURE_CTR_DISPLACEMENT["local_pack"]
    if serp_features.get("has_shopping"):
        adjustment += FEATURE_CTR_DISPLACEMENT["shopping_results"]
    if serp_features.get("has_knowledge_panel"):
        adjustment += FEATURE_CTR_DISPLACEMENT["knowledge_panel"]
    if serp_features.get("has_top_stories"):
        adjustment += FEATURE_CTR_DISPLACEMENT["top_stories"]
    if serp_features.get("has_image_pack"):
        adjustment += FEATURE_CTR_DISPLACEMENT["image_pack"]
    adjustment += serp_features.get("ads_above_count", 0) * FEATURE_CTR_DISPLACEMENT["ads_top"]

    return max(base_ctr + adjustment, 0.001)


# ---------------------------------------------------------------------------
# Gradient-boosting CTR model
# ---------------------------------------------------------------------------

def _train_ctr_model(
    X: np.ndarray,
    y: np.ndarray,
) -> Tuple[Any, float]:
    """
    Train a gradient-boosting regressor on the user's own CTR data.

    Returns (model, r2_score).  Uses a 75/25 train/test split.
    Falls back to (None, 0.0) if scikit-learn is unavailable or data is
    insufficient.
    """
    try:
        from sklearn.ensemble import GradientBoostingRegressor
        from sklearn.model_selection import train_test_split
        from sklearn.metrics import r2_score

        if len(X) < MIN_TRAINING_ROWS:
            logger.info(
                "Insufficient data for CTR model (%d rows, need %d) — using rule-based fallback",
                len(X), MIN_TRAINING_ROWS,
            )
            return None, 0.0

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.25, random_state=42,
        )

        model = GradientBoostingRegressor(
            n_estimators=100,
            max_depth=4,
            learning_rate=0.1,
            min_samples_leaf=3,
            subsample=0.8,
            random_state=42,
        )
        model.fit(X_train, y_train)

        y_pred = model.predict(X_test)
        r2 = r2_score(y_test, y_pred)

        logger.info(
            "CTR model trained: R²=%.3f, %d train rows, %d test rows",
            r2, len(X_train), len(X_test),
        )
        return model, float(r2)

    except ImportError:
        logger.warning("scikit-learn not available — CTR model disabled")
        return None, 0.0
    except Exception as exc:
        logger.error("CTR model training failed: %s", exc)
        return None, 0.0


# ---------------------------------------------------------------------------
# Opportunity scoring
# ---------------------------------------------------------------------------

def _score_feature_opportunities(
    keyword_analyses: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    For keywords where acquirable SERP features are NOT currently held by
    the user, estimate the click gain from capturing them.

    Focuses on featured_snippet, people_also_ask, and video_carousel
    because those are the three features site owners can actively target.
    """
    opportunities: List[Dict[str, Any]] = []

    for kw in keyword_analyses:
        features = kw.get("serp_features_present", [])
        impressions = kw.get("impressions", 0)
        position = kw.get("position", 99)

        if position > 20 or impressions < 100:
            continue  # Not worth targeting low-visibility keywords

        for feature_name, meta in TARGETABLE_FEATURES.items():
            # Skip if the feature isn't present in this SERP at all
            # (can't capture what doesn't exist)
            feature_key = f"has_{feature_name}"
            serp_has_feature = feature_key in features or feature_name in features

            # For featured_snippet — opportunity if someone else holds it
            # For PAA — opportunity if PAA exists but user has no FAQ schema
            # For video — opportunity if no video carousel yet
            if feature_name == "featured_snippet":
                # Opportunity: feature exists and user is in top 5 (can compete)
                if not serp_has_feature and position > 5:
                    continue
                if serp_has_feature:
                    # Someone else has it — user could take it
                    ctr_gain_estimate = abs(FEATURE_CTR_DISPLACEMENT.get(feature_name, 0.05))
                    estimated_clicks = int(impressions * ctr_gain_estimate)
                else:
                    continue
            elif feature_name == "people_also_ask":
                # Opportunity: PAA exists, user can add FAQ schema
                paa_present = any("paa" in str(f).lower() or "people_also_ask" in str(f).lower() for f in features) if features else False
                if not paa_present:
                    continue
                ctr_gain_estimate = 0.02  # FAQ schema visibility boost
                estimated_clicks = int(impressions * ctr_gain_estimate)
            elif feature_name == "video_carousel":
                video_present = any("video" in str(f).lower() for f in features) if features else False
                if video_present:
                    # Carousel exists — user can create video to appear
                    ctr_gain_estimate = abs(FEATURE_CTR_DISPLACEMENT.get(feature_name, 0.03))
                    estimated_clicks = int(impressions * ctr_gain_estimate)
                else:
                    continue
            else:
                continue

            if estimated_clicks < 10:
                continue

            # Difficulty scales with position (easier if already ranking well)
            if position <= 3:
                difficulty = "low"
            elif position <= 7:
                difficulty = meta["difficulty_base"]
            else:
                difficulty = "high"

            opportunities.append({
                "keyword": kw.get("keyword", ""),
                "feature": feature_name,
                "current_position": round(position, 1),
                "impressions": impressions,
                "estimated_click_gain": estimated_clicks,
                "difficulty": difficulty,
                "effort": meta["effort"],
            })

    # Sort by estimated click gain descending
    opportunities.sort(key=lambda x: x["estimated_click_gain"], reverse=True)
    return opportunities[:30]  # Top 30 opportunities


# ---------------------------------------------------------------------------
# Contextual CTR benchmarks
# ---------------------------------------------------------------------------

def _compute_contextual_benchmarks(
    keyword_analyses: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Aggregate CTR analysis into contextual benchmarks.

    Groups keywords by SERP complexity (simple = few features, complex =
    many features) and position band, then computes average expected vs
    actual CTR for each group.
    """
    by_complexity: Dict[str, List[Dict]] = {"simple": [], "moderate": [], "complex": []}

    for kw in keyword_analyses:
        feature_count = kw.get("serp_feature_count", 0)
        if feature_count <= 1:
            by_complexity["simple"].append(kw)
        elif feature_count <= 3:
            by_complexity["moderate"].append(kw)
        else:
            by_complexity["complex"].append(kw)

    benchmarks: Dict[str, Any] = {}
    for complexity, kws in by_complexity.items():
        if not kws:
            benchmarks[complexity] = {"avg_expected_ctr": 0, "avg_actual_ctr": 0, "count": 0}
            continue
        avg_expected = sum(k.get("expected_ctr_contextual", 0) for k in kws) / len(kws)
        avg_actual = sum(k.get("actual_ctr", 0) for k in kws) / len(kws)
        benchmarks[complexity] = {
            "avg_expected_ctr": round(avg_expected, 4),
            "avg_actual_ctr": round(avg_actual, 4),
            "count": len(kws),
        }

    return benchmarks


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def analyze_technical_health(
    gsc_coverage: Any = None,
    crawl_technical: Any = None,
) -> Dict[str, Any]:
    """
    Module 8: CTR Modeling by SERP Context.

    Accepts the LEGACY parameter names (gsc_coverage, crawl_technical) for
    pipeline backward-compatibility, but also accepts the spec-correct
    names (serp_data, gsc_data) via kwargs.

    The pipeline's _prepare_module_inputs maps:
        gsc_coverage  → gsc_keyword_data  (contains query, position, ctr, impressions, clicks)
        crawl_technical → serp_data        (DataForSEO SERP results)

    If SERP data is unavailable (DataForSEO not configured), falls back to
    rule-based CTR estimation using only GSC position data.

    Returns the spec-defined output:
        {
            "ctr_model_accuracy": float,       # R² or 0.0 if rule-based
            "model_type": str,                 # "gradient_boosting" | "rule_based"
            "keyword_ctr_analysis": [...],     # Per-keyword CTR breakdown
            "feature_opportunities": [...],    # Targetable SERP features
            "contextual_ctr_benchmarks": {...}, # Grouped benchmarks
            "summary": {...},                  # High-level stats
        }
    """
    # --- Normalise inputs ---
    serp_data = crawl_technical  # pipeline passes serp_data as crawl_technical
    gsc_data = gsc_coverage      # pipeline passes gsc_keyword_data as gsc_coverage

    # --- Parse GSC keyword data into a DataFrame ---
    gsc_df: Optional[pd.DataFrame] = None
    if gsc_data is not None:
        if isinstance(gsc_data, pd.DataFrame):
            gsc_df = gsc_data
        elif isinstance(gsc_data, list) and len(gsc_data) > 0:
            gsc_df = pd.DataFrame(gsc_data)
        elif isinstance(gsc_data, dict):
            for key in ("rows", "data", "results"):
                if key in gsc_data and isinstance(gsc_data[key], list):
                    gsc_df = pd.DataFrame(gsc_data[key])
                    break

    if gsc_df is None or gsc_df.empty:
        logger.warning("Module 8: No GSC keyword data — returning empty result")
        return {
            "ctr_model_accuracy": 0.0,
            "model_type": "none",
            "keyword_ctr_analysis": [],
            "feature_opportunities": [],
            "contextual_ctr_benchmarks": {},
            "summary": {
                "keywords_analyzed": 0,
                "overperformers": 0,
                "underperformers": 0,
                "in_line": 0,
                "total_click_share": 0.0,
                "total_click_share_opportunity": 0.0,
            },
        }

    # Ensure required columns
    for col in ["query", "position", "ctr", "impressions", "clicks"]:
        if col not in gsc_df.columns:
            # Try GSC-style "keys" column
            if col == "query" and "keys" in gsc_df.columns:
                gsc_df["query"] = gsc_df["keys"].apply(
                    lambda k: k[0] if isinstance(k, list) and len(k) > 0 else str(k)
                )
            else:
                gsc_df[col] = 0

    gsc_df["position"] = pd.to_numeric(gsc_df["position"], errors="coerce").fillna(50)
    gsc_df["ctr"] = pd.to_numeric(gsc_df["ctr"], errors="coerce").fillna(0)
    gsc_df["impressions"] = pd.to_numeric(gsc_df["impressions"], errors="coerce").fillna(0)
    gsc_df["clicks"] = pd.to_numeric(gsc_df["clicks"], errors="coerce").fillna(0)

    # Filter to keywords with meaningful data
    gsc_df = gsc_df[gsc_df["impressions"] >= 10].copy()
    if gsc_df.empty:
        logger.warning("Module 8: No keywords with >= 10 impressions")
        return {
            "ctr_model_accuracy": 0.0,
            "model_type": "none",
            "keyword_ctr_analysis": [],
            "feature_opportunities": [],
            "contextual_ctr_benchmarks": {},
            "summary": {
                "keywords_analyzed": 0,
                "overperformers": 0,
                "underperformers": 0,
                "in_line": 0,
                "total_click_share": 0.0,
                "total_click_share_opportunity": 0.0,
            },
        }

    # --- Parse SERP data ---
    serp_by_keyword: Dict[str, Dict[str, Any]] = {}
    has_serp_data = False

    if serp_data and isinstance(serp_data, dict):
        # DataForSEO result from report_runner: may have "results" key
        serp_results = serp_data.get("results", serp_data.get("serp_results", {}))
        if isinstance(serp_results, dict):
            for kw, result in serp_results.items():
                serp_by_keyword[kw.lower()] = _extract_serp_features(result)
            has_serp_data = len(serp_by_keyword) > 0
        elif isinstance(serp_results, list):
            for entry in serp_results:
                if isinstance(entry, dict) and "keyword" in entry:
                    kw = entry["keyword"].lower()
                    serp_by_keyword[kw] = _extract_serp_features(entry)
            has_serp_data = len(serp_by_keyword) > 0

    logger.info(
        "Module 8: %d GSC keywords, %d with SERP data",
        len(gsc_df), len(serp_by_keyword),
    )

    # --- Build per-keyword analysis ---
    keyword_analyses: List[Dict[str, Any]] = []
    X_rows: List[List[float]] = []
    y_rows: List[float] = []

    for _, row in gsc_df.iterrows():
        query = str(row.get("query", "")).lower()
        position = float(row["position"])
        actual_ctr = float(row["ctr"])
        impressions = int(row["impressions"])
        clicks = int(row["clicks"])

        # Get SERP features if available
        serp_features = serp_by_keyword.get(query, {})
        has_kw_serp = bool(serp_features)

        # Generic CTR (position-only)
        pos_rounded = min(max(round(position), 1), 20)
        generic_ctr = GENERIC_CTR_BY_POSITION.get(pos_rounded, 0.003)

        # Contextual CTR (rule-based)
        if has_kw_serp:
            contextual_ctr = _estimate_contextual_ctr(position, serp_features)
        else:
            contextual_ctr = generic_ctr  # No SERP data → fall back to generic

        # Visual position
        visual_pos = _calculate_visual_position(pos_rounded, serp_features) if has_kw_serp else float(pos_rounded)

        # Classify performance
        if actual_ctr > 0:
            ratio = actual_ctr / max(contextual_ctr, 0.001)
            if ratio > 1.3:
                performance = "overperforming"
            elif ratio < 0.7:
                performance = "underperforming"
            else:
                performance = "in_line"
        else:
            performance = "underperforming"

        # Count SERP features
        feature_list = []
        for feat_key in ["has_featured_snippet", "has_ai_overview", "has_video_carousel",
                         "has_local_pack", "has_shopping", "has_knowledge_panel",
                         "has_top_stories", "has_image_pack", "has_reddit_threads"]:
            if serp_features.get(feat_key):
                feature_list.append(feat_key.replace("has_", ""))
        if serp_features.get("paa_count", 0) > 0:
            feature_list.append(f"paa_x{serp_features['paa_count']}")
        if serp_features.get("ads_above_count", 0) > 0:
            feature_list.append(f"ads_x{serp_features['ads_above_count']}")

        analysis = {
            "keyword": query,
            "position": round(position, 1),
            "visual_position": round(visual_pos, 1),
            "expected_ctr_generic": round(generic_ctr, 4),
            "expected_ctr_contextual": round(contextual_ctr, 4),
            "actual_ctr": round(actual_ctr, 4),
            "impressions": impressions,
            "clicks": clicks,
            "performance": performance,
            "serp_features_present": feature_list,
            "serp_feature_count": len(feature_list),
            "has_serp_data": has_kw_serp,
        }
        keyword_analyses.append(analysis)

        # Collect training data for gradient-boosting model
        if has_kw_serp and actual_ctr > 0 and position <= 20:
            X_rows.append(_build_feature_vector(position, serp_features))
            y_rows.append(actual_ctr)

    # --- Train gradient-boosting CTR model (if enough SERP data) ---
    model = None
    model_r2 = 0.0
    model_type = "rule_based"

    if len(X_rows) >= MIN_TRAINING_ROWS:
        X_arr = np.array(X_rows)
        y_arr = np.array(y_rows)
        model, model_r2 = _train_ctr_model(X_arr, y_arr)

        if model is not None and model_r2 > 0.05:
            model_type = "gradient_boosting"

            # Re-predict expected CTR using the trained model for keywords
            # that have SERP data
            for kw_analysis in keyword_analyses:
                if not kw_analysis["has_serp_data"]:
                    continue
                query = kw_analysis["keyword"]
                serp_features = serp_by_keyword.get(query, {})
                if not serp_features:
                    continue
                vec = np.array([_build_feature_vector(
                    kw_analysis["position"], serp_features,
                )])
                predicted = float(model.predict(vec)[0])
                predicted = max(predicted, 0.001)
                kw_analysis["expected_ctr_contextual"] = round(predicted, 4)

                # Reclassify performance with model predictions
                actual = kw_analysis["actual_ctr"]
                if actual > 0:
                    ratio = actual / predicted
                    if ratio > 1.3:
                        kw_analysis["performance"] = "overperforming"
                    elif ratio < 0.7:
                        kw_analysis["performance"] = "underperforming"
                    else:
                        kw_analysis["performance"] = "in_line"

            logger.info("Module 8: Model predictions applied to %d keywords", len(X_rows))
        else:
            logger.info("Module 8: Model R² too low (%.3f) — keeping rule-based estimates", model_r2)
            model_type = "rule_based"
            model_r2 = 0.0
    else:
        logger.info(
            "Module 8: Only %d keywords with SERP data (need %d) — using rule-based CTR estimation",
            len(X_rows), MIN_TRAINING_ROWS,
        )

    # --- Sort keyword analyses by impressions descending ---
    keyword_analyses.sort(key=lambda x: x["impressions"], reverse=True)

    # --- Score SERP feature opportunities ---
    feature_opps = _score_feature_opportunities(keyword_analyses)

    # --- Contextual CTR benchmarks ---
    benchmarks = _compute_contextual_benchmarks(keyword_analyses)

    # --- Summary stats ---
    overperformers = sum(1 for k in keyword_analyses if k["performance"] == "overperforming")
    underperformers = sum(1 for k in keyword_analyses if k["performance"] == "underperforming")
    in_line = sum(1 for k in keyword_analyses if k["performance"] == "in_line")

    total_impressions = sum(k["impressions"] for k in keyword_analyses)
    total_clicks = sum(k["clicks"] for k in keyword_analyses)
    total_click_share = total_clicks / max(total_impressions, 1)

    # Estimate potential click share if all underperformers reached expected CTR
    potential_additional_clicks = 0
    for kw in keyword_analyses:
        if kw["performance"] == "underperforming":
            expected_clicks = kw["impressions"] * kw["expected_ctr_contextual"]
            actual_clicks = kw["clicks"]
            if expected_clicks > actual_clicks:
                potential_additional_clicks += int(expected_clicks - actual_clicks)

    total_click_share_opportunity = (total_clicks + potential_additional_clicks) / max(total_impressions, 1)

    result = {
        "ctr_model_accuracy": round(model_r2, 3),
        "model_type": model_type,
        "keyword_ctr_analysis": keyword_analyses[:200],  # Top 200 by impressions
        "feature_opportunities": feature_opps,
        "contextual_ctr_benchmarks": benchmarks,
        "summary": {
            "keywords_analyzed": len(keyword_analyses),
            "overperformers": overperformers,
            "underperformers": underperformers,
            "in_line": in_line,
            "total_click_share": round(total_click_share, 4),
            "total_click_share_opportunity": round(total_click_share_opportunity, 4),
            "potential_additional_monthly_clicks": potential_additional_clicks,
            "serp_data_coverage": f"{len(serp_by_keyword)}/{len(keyword_analyses)} keywords",
        },
    }

    logger.info(
        "Module 8 complete: %d keywords, model=%s (R²=%.3f), %d opps, %d overperformers, %d underperformers",
        len(keyword_analyses), model_type, model_r2,
        len(feature_opps), overperformers, underperformers,
    )

    return result
