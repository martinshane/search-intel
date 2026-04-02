"""
Module 11: Competitive Threats — competitor identification, keyword vulnerability,
emerging threat detection, competitive pressure scoring, and content velocity estimation.

Phase 2 full implementation.  Consumes DataForSEO SERP results together with
GSC keyword performance data and produces:
  1. Primary competitor profiling with overlap & position analysis
  2. Keyword vulnerability assessment (where competitors outrank user)
  3. Emerging threat detection (rising competitors)
  4. Content velocity estimation (competitor freshness signals)
  5. Competitive pressure scoring per keyword cluster
"""

import logging
import math
from typing import Any, Dict, List, Optional, Tuple
from collections import Counter, defaultdict
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

GENERIC_CTR_BY_POSITION: Dict[int, float] = {
    1: 0.284, 2: 0.147, 3: 0.082, 4: 0.053, 5: 0.038,
    6: 0.030, 7: 0.024, 8: 0.020, 9: 0.017, 10: 0.015,
}

# Threat-level thresholds
THREAT_CRITICAL_OVERLAP_PCT = 40
THREAT_CRITICAL_AVG_POS = 5
THREAT_HIGH_OVERLAP_PCT = 30
THREAT_HIGH_AVG_POS = 7
THREAT_MEDIUM_OVERLAP_PCT = 20

# Vulnerability thresholds
VULNERABILITY_POSITION_DIFF = 3  # competitor beats user by 3+ positions
VULNERABILITY_HIGH_IMPRESSIONS = 500

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


def _normalize_domain(domain: str) -> str:
    """Strip www. and trailing slashes for consistent matching."""
    d = domain.lower().strip().rstrip("/")
    return d[4:] if d.startswith("www.") else d


def _is_user_url(url: str, user_domain: str) -> bool:
    """Check whether *url* belongs to the user's domain."""
    return _extract_domain(url) == _normalize_domain(user_domain)


def _find_user_result(serp: Dict[str, Any], user_domain: str) -> Optional[Dict[str, Any]]:
    """Find the user's organic result in a SERP, if present."""
    norm = _normalize_domain(user_domain)
    for result in serp.get("organic_results", []):
        if _extract_domain(result.get("url", "")) == norm:
            return result
    return None


def _find_competitor_result(
    serp: Dict[str, Any], comp_domain: str
) -> Optional[Dict[str, Any]]:
    """Find a specific competitor's organic result in a SERP."""
    norm = _normalize_domain(comp_domain)
    for result in serp.get("organic_results", []):
        if _extract_domain(result.get("url", "")) == norm:
            return result
    return None


def _keyword_cluster(keyword: str) -> str:
    """Return a simple cluster label based on head term.

    Extracts the longest meaningful word (>3 chars) as a proxy cluster.
    Production systems should use embeddings, but this is sufficient
    for grouping competitive pressure.
    """
    words = keyword.lower().split()
    long_words = [w for w in words if len(w) > 3]
    return long_words[0] if long_words else (words[0] if words else "other")


# ---------------------------------------------------------------------------
# 1. Primary Competitor Profiling
# ---------------------------------------------------------------------------

def _profile_competitors(
    serp_data: List[Dict[str, Any]], user_domain: str
) -> List[Dict[str, Any]]:
    """
    Build a ranked profile of every competitor seen in the SERP dataset.

    For each competitor we track:
      - Total keyword overlap count & percentage
      - Average organic position
      - Position distribution (top-3, 4-10, 11+)
      - Keywords where they rank #1
      - Threat level classification
      - Head-to-head win rate vs the user
    """
    norm_user = _normalize_domain(user_domain)

    freq: Counter = Counter()
    positions: Dict[str, List[int]] = defaultdict(list)
    rank1_keywords: Dict[str, List[str]] = defaultdict(list)
    head_to_head_wins: Dict[str, int] = Counter()
    head_to_head_total: Dict[str, int] = Counter()
    comp_urls: Dict[str, set] = defaultdict(set)

    for serp in serp_data:
        kw = serp.get("keyword", "")
        user_result = _find_user_result(serp, user_domain)
        user_pos = user_result.get("position", 999) if user_result else 999

        for result in serp.get("organic_results", [])[:20]:
            domain = _extract_domain(result.get("url", ""))
            if not domain or domain == norm_user:
                continue

            pos = result.get("position", 100)
            freq[domain] += 1
            positions[domain].append(pos)
            comp_urls[domain].add(result.get("url", ""))

            if pos == 1:
                rank1_keywords[domain].append(kw)

            if user_pos < 999:
                head_to_head_total[domain] += 1
                if pos < user_pos:
                    head_to_head_wins[domain] += 1

    total_kw = len(serp_data) or 1
    competitors: List[Dict[str, Any]] = []

    for domain, count in freq.most_common(30):
        avg_pos = sum(positions[domain]) / len(positions[domain])
        overlap_pct = count / total_kw * 100
        pos_list = positions[domain]

        top3 = sum(1 for p in pos_list if p <= 3)
        pos4_10 = sum(1 for p in pos_list if 4 <= p <= 10)
        pos11plus = sum(1 for p in pos_list if p > 10)

        h2h_total = head_to_head_total.get(domain, 0)
        h2h_wins = head_to_head_wins.get(domain, 0)
        win_rate = h2h_wins / h2h_total if h2h_total > 0 else 0.0

        # Threat classification
        if overlap_pct > THREAT_CRITICAL_OVERLAP_PCT and avg_pos < THREAT_CRITICAL_AVG_POS:
            threat = "critical"
        elif overlap_pct > THREAT_HIGH_OVERLAP_PCT and avg_pos < THREAT_HIGH_AVG_POS:
            threat = "high"
        elif overlap_pct > THREAT_MEDIUM_OVERLAP_PCT or avg_pos < THREAT_CRITICAL_AVG_POS:
            threat = "medium"
        else:
            threat = "low"

        competitors.append({
            "domain": domain,
            "keywords_shared": count,
            "overlap_percentage": round(overlap_pct, 1),
            "avg_position": round(avg_pos, 1),
            "position_distribution": {
                "top_3": top3,
                "pos_4_10": pos4_10,
                "pos_11_plus": pos11plus,
            },
            "rank_1_keywords": rank1_keywords.get(domain, [])[:10],
            "unique_urls_seen": len(comp_urls.get(domain, set())),
            "head_to_head_win_rate": round(win_rate, 3),
            "head_to_head_contests": h2h_total,
            "threat_level": threat,
        })

    competitors.sort(key=lambda c: (-c["overlap_percentage"], c["avg_position"]))
    return competitors[:25]


# ---------------------------------------------------------------------------
# 2. Keyword Vulnerability Assessment
# ---------------------------------------------------------------------------

def _assess_keyword_vulnerability(
    serp_data: List[Dict[str, Any]],
    gsc_data,
    user_domain: str,
) -> Dict[str, Any]:
    """
    Identify keywords where the user is vulnerable to competitor displacement.

    Vulnerability signals:
      - Competitor ranks significantly higher
      - User position is borderline (page 1/2 boundary)
      - High-impression keywords with weak positions
      - Multiple strong competitors ahead
    """
    try:
        import pandas as pd
        gsc_df = gsc_data if isinstance(gsc_data, pd.DataFrame) else pd.DataFrame()
    except ImportError:
        gsc_df = None

    vulnerable: List[Dict[str, Any]] = []
    defended: List[Dict[str, Any]] = []

    for serp in serp_data:
        kw = serp.get("keyword", "")
        user_result = _find_user_result(serp, user_domain)

        if not user_result:
            # User not ranking at all — high vulnerability
            top_comp = None
            for r in serp.get("organic_results", [])[:3]:
                d = _extract_domain(r.get("url", ""))
                if d and d != _normalize_domain(user_domain):
                    top_comp = {"domain": d, "position": r.get("position", 0)}
                    break

            impressions = 0
            if gsc_df is not None and not gsc_df.empty:
                row = gsc_df[gsc_df["query"] == kw]
                if not row.empty:
                    impressions = int(row.iloc[0].get("impressions", 0))

            if top_comp and impressions > 50:
                vulnerable.append({
                    "keyword": kw,
                    "user_position": None,
                    "top_competitor": top_comp["domain"],
                    "competitor_position": top_comp["position"],
                    "position_gap": None,
                    "impressions": impressions,
                    "vulnerability": "not_ranking",
                    "risk_level": "critical" if impressions > VULNERABILITY_HIGH_IMPRESSIONS else "high",
                    "recommendation": "Create targeted content for this keyword — competitors dominate while you have no ranking page.",
                })
            continue

        user_pos = user_result.get("position", 100)

        # Count competitors ahead
        competitors_ahead: List[Dict[str, str]] = []
        for r in serp.get("organic_results", []):
            d = _extract_domain(r.get("url", ""))
            r_pos = r.get("position", 100)
            if d and d != _normalize_domain(user_domain) and r_pos < user_pos:
                competitors_ahead.append({"domain": d, "position": r_pos})

        # Get impressions from GSC
        impressions = 0
        clicks = 0
        if gsc_df is not None and not gsc_df.empty:
            row = gsc_df[gsc_df["query"] == kw]
            if not row.empty:
                impressions = int(row.iloc[0].get("impressions", 0))
                clicks = int(row.iloc[0].get("clicks", 0))

        if not competitors_ahead:
            if user_pos <= 3:
                defended.append({
                    "keyword": kw,
                    "user_position": user_pos,
                    "impressions": impressions,
                })
            continue

        best_comp = min(competitors_ahead, key=lambda c: c["position"])
        gap = user_pos - best_comp["position"]

        if gap >= VULNERABILITY_POSITION_DIFF or (user_pos > 10 and impressions > 100):
            risk = "critical" if (gap >= 5 and impressions > VULNERABILITY_HIGH_IMPRESSIONS) else \
                   "high" if (gap >= 3 and impressions > 200) else \
                   "medium"

            rec = _vulnerability_recommendation(user_pos, gap, len(competitors_ahead))

            vulnerable.append({
                "keyword": kw,
                "user_position": user_pos,
                "top_competitor": best_comp["domain"],
                "competitor_position": best_comp["position"],
                "position_gap": round(gap, 1),
                "competitors_ahead": len(competitors_ahead),
                "impressions": impressions,
                "clicks": clicks,
                "vulnerability": "outranked",
                "risk_level": risk,
                "recommendation": rec,
            })

    vulnerable.sort(key=lambda v: (
        {"critical": 0, "high": 1, "medium": 2}.get(v["risk_level"], 3),
        -v.get("impressions", 0),
    ))

    return {
        "vulnerable_keywords": vulnerable[:40],
        "defended_keywords_count": len(defended),
        "total_vulnerable": len(vulnerable),
        "critical_count": sum(1 for v in vulnerable if v["risk_level"] == "critical"),
        "high_count": sum(1 for v in vulnerable if v["risk_level"] == "high"),
        "medium_count": sum(1 for v in vulnerable if v["risk_level"] == "medium"),
    }


def _vulnerability_recommendation(user_pos: float, gap: float, comps_ahead: int) -> str:
    """Generate a specific recommendation based on vulnerability signals."""
    if user_pos > 20:
        return "This keyword needs a dedicated content strategy — current ranking is too deep to recover with minor edits."
    if gap >= 7:
        return f"Major position gap ({gap:.0f} positions behind). Audit competitor content depth, backlink profiles, and consider a full page rewrite."
    if gap >= 4:
        return f"Significant gap ({gap:.0f} positions). Focus on content freshness, internal linking, and matching competitor content comprehensiveness."
    if comps_ahead >= 5:
        return f"{comps_ahead} competitors rank above you. Differentiate with unique data, better UX, or a more targeted content angle."
    return "Incremental optimisation — improve title tags, add structured data, enhance internal linking to this page."


# ---------------------------------------------------------------------------
# 3. Emerging Threat Detection
# ---------------------------------------------------------------------------

def _detect_emerging_threats(
    serp_data: List[Dict[str, Any]], user_domain: str
) -> List[Dict[str, Any]]:
    """
    Identify domains showing signals of being emerging competitive threats.

    Signals (from SERP snapshot analysis):
      - Appearing across many keywords but with lower avg positions (new entrants)
      - Ranking with fresh / recently published content
      - Multiple unique URLs (high content velocity)
      - High page diversity relative to keyword overlap
    """
    norm_user = _normalize_domain(user_domain)

    domain_data: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
        "keywords": 0,
        "positions": [],
        "urls": set(),
        "fresh_signals": 0,
    })

    for serp in serp_data:
        for result in serp.get("organic_results", [])[:20]:
            domain = _extract_domain(result.get("url", ""))
            if not domain or domain == norm_user:
                continue

            pos = result.get("position", 100)
            url = result.get("url", "")
            d = domain_data[domain]
            d["keywords"] += 1
            d["positions"].append(pos)
            d["urls"].add(url)

            # Fresh content signals
            snippet = (result.get("snippet", "") or "").lower()
            title = (result.get("title", "") or "").lower()
            if any(sig in snippet or sig in title for sig in (
                "2026", "2025", "updated", "new", "latest", "just published"
            )):
                d["fresh_signals"] += 1

    total_kw = len(serp_data) or 1
    emerging: List[Dict[str, Any]] = []

    for domain, data in domain_data.items():
        kw_count = data["keywords"]
        overlap_pct = kw_count / total_kw * 100

        if kw_count < 3:
            continue

        avg_pos = sum(data["positions"]) / len(data["positions"])
        unique_urls = len(data["urls"])
        url_diversity = unique_urls / kw_count if kw_count > 0 else 0
        fresh_pct = data["fresh_signals"] / kw_count * 100 if kw_count > 0 else 0

        # Emerging threat score: weighted composite
        # Higher = more threatening emerging competitor
        score = 0.0
        score += min(overlap_pct / 50, 1.0) * 25          # Breadth (max 25)
        score += min(url_diversity, 1.0) * 20              # Content diversity (max 20)
        score += min(fresh_pct / 50, 1.0) * 25             # Freshness (max 25)
        score += max(0, (20 - avg_pos)) / 20 * 15          # Position strength (max 15)
        score += min(unique_urls / 10, 1.0) * 15           # Content volume (max 15)

        # Only flag if they show meaningful emerging signals
        if score >= 25 and (url_diversity > 0.3 or fresh_pct > 15 or (avg_pos > 10 and overlap_pct > 15)):
            emerging.append({
                "domain": domain,
                "keywords_present": kw_count,
                "overlap_percentage": round(overlap_pct, 1),
                "avg_position": round(avg_pos, 1),
                "unique_urls": unique_urls,
                "url_diversity_ratio": round(url_diversity, 2),
                "fresh_content_pct": round(fresh_pct, 1),
                "emerging_threat_score": round(score, 1),
                "signal": _emerging_signal_label(avg_pos, fresh_pct, url_diversity),
            })

    emerging.sort(key=lambda e: -e["emerging_threat_score"])
    return emerging[:15]


def _emerging_signal_label(avg_pos: float, fresh_pct: float, url_diversity: float) -> str:
    """Classify the primary emerging threat signal."""
    if fresh_pct > 30:
        return "high_content_velocity"
    if url_diversity > 0.7:
        return "broad_content_expansion"
    if avg_pos > 10:
        return "new_market_entrant"
    return "growing_presence"


# ---------------------------------------------------------------------------
# 4. Content Velocity Estimation
# ---------------------------------------------------------------------------

def _estimate_content_velocity(
    serp_data: List[Dict[str, Any]], user_domain: str
) -> Dict[str, Any]:
    """
    Estimate how many unique pages each top competitor deploys across the
    keyword set, as a proxy for content production velocity.

    Also compares the user's content footprint to competitors.
    """
    norm_user = _normalize_domain(user_domain)

    user_urls: set = set()
    comp_urls: Dict[str, set] = defaultdict(set)

    for serp in serp_data:
        for result in serp.get("organic_results", [])[:10]:
            url = result.get("url", "")
            domain = _extract_domain(url)
            if not domain:
                continue
            if domain == norm_user:
                user_urls.add(url)
            else:
                comp_urls[domain].add(url)

    user_count = len(user_urls)
    velocity_comparison: List[Dict[str, Any]] = []

    for domain, urls in sorted(comp_urls.items(), key=lambda x: -len(x[1]))[:15]:
        count = len(urls)
        ratio = count / user_count if user_count > 0 else float("inf")
        velocity_comparison.append({
            "domain": domain,
            "unique_ranking_pages": count,
            "vs_user_ratio": round(ratio, 2),
            "assessment": (
                "significantly_more" if ratio > 2.0 else
                "moderately_more" if ratio > 1.3 else
                "comparable" if ratio > 0.7 else
                "fewer"
            ),
        })

    return {
        "user_unique_pages": user_count,
        "competitor_velocity": velocity_comparison,
        "user_content_gap": sum(
            1 for v in velocity_comparison
            if v["assessment"] in ("significantly_more", "moderately_more")
        ),
    }


# ---------------------------------------------------------------------------
# 5. Competitive Pressure by Keyword Cluster
# ---------------------------------------------------------------------------

def _analyze_competitive_pressure(
    serp_data: List[Dict[str, Any]], user_domain: str
) -> List[Dict[str, Any]]:
    """
    Group keywords by cluster and score competitive pressure in each.

    Pressure factors:
      - Number of unique competitors in the cluster
      - Average competitor position vs user position
      - Percentage of keywords where user is NOT in top 3
    """
    norm_user = _normalize_domain(user_domain)

    clusters: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
        "keywords": [],
        "user_positions": [],
        "competitor_domains": set(),
        "comp_positions": [],
        "user_not_top3": 0,
    })

    for serp in serp_data:
        kw = serp.get("keyword", "")
        cluster = _keyword_cluster(kw)
        c = clusters[cluster]
        c["keywords"].append(kw)

        user_result = _find_user_result(serp, user_domain)
        user_pos = user_result.get("position", 999) if user_result else 999
        c["user_positions"].append(user_pos)
        if user_pos > 3:
            c["user_not_top3"] += 1

        for result in serp.get("organic_results", [])[:10]:
            domain = _extract_domain(result.get("url", ""))
            if domain and domain != norm_user:
                c["competitor_domains"].add(domain)
                c["comp_positions"].append(result.get("position", 100))

    pressure_results: List[Dict[str, Any]] = []

    for cluster, data in clusters.items():
        n_kw = len(data["keywords"])
        if n_kw < 2:
            continue

        avg_user_pos = sum(data["user_positions"]) / n_kw
        avg_comp_pos = (
            sum(data["comp_positions"]) / len(data["comp_positions"])
            if data["comp_positions"] else 50
        )
        n_competitors = len(data["competitor_domains"])
        not_top3_pct = data["user_not_top3"] / n_kw * 100

        # Pressure score 0-100
        score = 0.0
        score += min(n_competitors / 15, 1.0) * 30         # Competitor density (max 30)
        score += min(not_top3_pct / 100, 1.0) * 30         # Weakness (max 30)
        score += max(0, avg_user_pos - avg_comp_pos) / 10 * 20  # Position deficit (max 20)
        score += min(n_kw / 10, 1.0) * 20                  # Cluster size relevance (max 20)

        pressure_results.append({
            "cluster": cluster,
            "keyword_count": n_kw,
            "sample_keywords": data["keywords"][:5],
            "avg_user_position": round(avg_user_pos, 1),
            "avg_competitor_position": round(avg_comp_pos, 1),
            "unique_competitors": n_competitors,
            "user_outside_top3_pct": round(not_top3_pct, 1),
            "pressure_score": round(score, 1),
            "pressure_level": (
                "critical" if score >= 70 else
                "high" if score >= 50 else
                "moderate" if score >= 30 else
                "low"
            ),
        })

    pressure_results.sort(key=lambda p: -p["pressure_score"])
    return pressure_results[:20]


# ---------------------------------------------------------------------------
# 6. Recommendations Generator
# ---------------------------------------------------------------------------

def _generate_recommendations(
    competitors: List[Dict[str, Any]],
    vulnerability: Dict[str, Any],
    emerging: List[Dict[str, Any]],
    content_velocity: Dict[str, Any],
    pressure: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Produce prioritised recommendations from all competitive analyses."""
    recs: List[Dict[str, Any]] = []

    # Critical competitors
    critical_comps = [c for c in competitors if c["threat_level"] == "critical"]
    if critical_comps:
        domains = ", ".join(c["domain"] for c in critical_comps[:3])
        recs.append({
            "priority": "critical",
            "category": "competitor_monitoring",
            "recommendation": f"Set up ongoing monitoring for critical competitors: {domains}. "
                              f"Track their content updates, new pages, and ranking changes weekly.",
        })

    # Vulnerable keywords
    vuln_critical = vulnerability.get("critical_count", 0)
    if vuln_critical > 0:
        recs.append({
            "priority": "critical",
            "category": "keyword_defence",
            "recommendation": f"{vuln_critical} keywords are critically vulnerable to competitor displacement. "
                              f"Prioritise content refresh and internal linking for these pages immediately.",
        })

    vuln_not_ranking = sum(
        1 for v in vulnerability.get("vulnerable_keywords", [])
        if v.get("vulnerability") == "not_ranking"
    )
    if vuln_not_ranking > 0:
        recs.append({
            "priority": "high",
            "category": "content_gap",
            "recommendation": f"You have no ranking page for {vuln_not_ranking} keywords where competitors are present. "
                              f"Create targeted content to capture this missing visibility.",
        })

    # Emerging threats
    high_velocity = [e for e in emerging if e.get("signal") == "high_content_velocity"]
    if high_velocity:
        domains = ", ".join(e["domain"] for e in high_velocity[:3])
        recs.append({
            "priority": "high",
            "category": "emerging_threats",
            "recommendation": f"Emerging competitors with high content velocity detected: {domains}. "
                              f"Monitor their growth and consider accelerating your publishing cadence.",
        })

    # Content velocity gap
    gap = content_velocity.get("user_content_gap", 0)
    if gap >= 3:
        recs.append({
            "priority": "high",
            "category": "content_velocity",
            "recommendation": f"{gap} competitors deploy significantly more unique pages than you across overlapping keywords. "
                              f"Consider increasing content production to close the coverage gap.",
        })

    # High-pressure clusters
    critical_clusters = [p for p in pressure if p["pressure_level"] == "critical"]
    if critical_clusters:
        clusters = ", ".join(f'"{p["cluster"]}"' for p in critical_clusters[:3])
        recs.append({
            "priority": "high",
            "category": "cluster_defence",
            "recommendation": f"Keyword clusters under critical competitive pressure: {clusters}. "
                              f"Build content hubs and strengthen internal linking in these topic areas.",
        })

    # Sort by priority
    priority_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    recs.sort(key=lambda r: priority_order.get(r["priority"], 9))

    return recs[:10]


# ---------------------------------------------------------------------------
# Public entry point  (imported by api.routes.modules)
# ---------------------------------------------------------------------------

def analyze_competitive_threats(
    serp_data, gsc_data=None, user_domain=None
) -> Dict[str, Any]:
    """
    Module 11: Competitive Threats — full Phase 2 implementation.

    Args:
        serp_data: list of SERP result dicts from DataForSEO / serp_helper.
        gsc_data: optional pandas DataFrame of GSC keyword metrics
                  (columns: query, clicks, impressions, position, ctr).
        user_domain: the user's domain string (e.g. "example.com").

    Returns:
        Dict with competitor_profiles, keyword_vulnerability, emerging_threats,
        content_velocity, competitive_pressure, recommendations, and summary.
    """
    logger.info("Running analyze_competitive_threats — Phase 2 full implementation")

    if not serp_data or not user_domain:
        logger.warning("Missing SERP data or user_domain; returning empty analysis")
        return _empty_result()

    try:
        # 1. Competitor profiling
        competitors = _profile_competitors(serp_data, user_domain)

        # 2. Keyword vulnerability
        vulnerability = _assess_keyword_vulnerability(serp_data, gsc_data, user_domain)

        # 3. Emerging threats
        emerging = _detect_emerging_threats(serp_data, user_domain)

        # 4. Content velocity
        content_velocity = _estimate_content_velocity(serp_data, user_domain)

        # 5. Competitive pressure by cluster
        pressure = _analyze_competitive_pressure(serp_data, user_domain)

        # 6. Recommendations
        recommendations = _generate_recommendations(
            competitors, vulnerability, emerging, content_velocity, pressure
        )

        # Build summary
        critical_comps = sum(1 for c in competitors if c["threat_level"] == "critical")
        high_comps = sum(1 for c in competitors if c["threat_level"] == "high")

        summary = (
            f"Analyzed {len(serp_data)} keywords across the competitive landscape. "
            f"Identified {len(competitors)} competitors "
            f"({critical_comps} critical, {high_comps} high threat). "
            f"{vulnerability['total_vulnerable']} keywords are vulnerable "
            f"({vulnerability['critical_count']} critical risk). "
            f"{len(emerging)} emerging threats detected. "
            f"{len([p for p in pressure if p['pressure_level'] in ('critical', 'high')])} "
            f"keyword clusters under significant competitive pressure."
        )

        result = {
            "keywords_analyzed": len(serp_data),
            "competitor_profiles": competitors,
            "keyword_vulnerability": vulnerability,
            "emerging_threats": emerging,
            "content_velocity": content_velocity,
            "competitive_pressure": pressure,
            "recommendations": recommendations,
            "summary": summary,
        }

        # Normalise field names so the frontend renders correctly
        _add_frontend_keys(result)

        logger.info(
            "Competitive threats analysis complete: %d keywords, %d competitors, "
            "%d vulnerable keywords, %d emerging threats",
            len(serp_data), len(competitors),
            vulnerability["total_vulnerable"], len(emerging),
        )
        return result

    except Exception as exc:
        logger.error("Error in competitive threats analysis: %s", exc, exc_info=True)
        return _empty_result()


def _add_frontend_keys(result: Dict[str, Any]) -> None:
    """
    Add frontend-compatible alias keys to the result dict so the React
    CompetitiveThreatsContent component renders all sections correctly.

    Canonical keys are preserved for PDF export, Gameplan consumption, and
    any downstream modules. This only ADDS aliases — never removes originals.

    Fixes 8 known data-shape mismatches between backend and frontend:
      1. vulnerable_keywords[].risk_level → severity / threat_level
      2. emerging_threats[].keywords_present → new_keywords
      3. emerging_threats[].avg_position → avg_entry_position
      4. emerging_threats[].unique_urls → unique_urls_seen
      5. emerging_threats[] missing threat_level → derived from score
      6. competitive_pressure[].keyword_count → keywords_in_cluster
      7. competitive_pressure[] missing avg_competitor_gap → computed
      8. content_velocity.competitor_velocity[].unique_ranking_pages → unique_pages
    """
    # 1. Vulnerable keywords: add severity / threat_level aliases for risk_level
    vuln = result.get("keyword_vulnerability", {})
    for v in vuln.get("vulnerable_keywords", []):
        rl = v.get("risk_level", "medium")
        v.setdefault("severity", rl)
        v.setdefault("threat_level", rl)

    # 2-5. Emerging threats: add frontend-expected field aliases
    for et in result.get("emerging_threats", []):
        # keywords_present → new_keywords
        et.setdefault("new_keywords", et.get("keywords_present", 0))
        # avg_position → avg_entry_position AND current_avg_position
        avg_pos = et.get("avg_position")
        if avg_pos is not None:
            et.setdefault("avg_entry_position", avg_pos)
            et.setdefault("current_avg_position", avg_pos)
        # unique_urls → unique_urls_seen
        et.setdefault("unique_urls_seen", et.get("unique_urls", 0))
        # Derive threat_level from emerging_threat_score
        score = et.get("emerging_threat_score", 0)
        if "threat_level" not in et:
            if score >= 60:
                et["threat_level"] = "critical"
            elif score >= 40:
                et["threat_level"] = "high"
            elif score >= 25:
                et["threat_level"] = "medium"
            else:
                et["threat_level"] = "low"

    # 6-7. Competitive pressure: add keywords_in_cluster and avg_competitor_gap
    for p in result.get("competitive_pressure", []):
        # keyword_count → keywords_in_cluster
        p.setdefault("keywords_in_cluster", p.get("keyword_count", 0))
        # Compute avg_competitor_gap = avg_user_position - avg_competitor_position
        avg_user = p.get("avg_user_position")
        avg_comp = p.get("avg_competitor_position")
        if "avg_competitor_gap" not in p and avg_user is not None and avg_comp is not None:
            p["avg_competitor_gap"] = round(avg_user - avg_comp, 1)

    # 8. Content velocity: unique_ranking_pages → unique_pages
    cv = result.get("content_velocity", {})
    for comp in cv.get("competitor_velocity", []):
        comp.setdefault("unique_pages", comp.get("unique_ranking_pages", 0))


def _empty_result() -> Dict[str, Any]:
    """Return a safe empty result dict matching the expected schema."""
    return {
        "keywords_analyzed": 0,
        "competitor_profiles": [],
        "keyword_vulnerability": {
            "vulnerable_keywords": [],
            "defended_keywords_count": 0,
            "total_vulnerable": 0,
            "critical_count": 0,
            "high_count": 0,
            "medium_count": 0,
        },
        "emerging_threats": [],
        "content_velocity": {
            "user_unique_pages": 0,
            "competitor_velocity": [],
            "user_content_gap": 0,
        },
        "competitive_pressure": [],
        "recommendations": [],
        "summary": "Competitive threats analysis requires SERP data and a user domain.",
    }
