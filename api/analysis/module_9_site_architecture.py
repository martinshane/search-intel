"""
Module 9: Site Architecture — internal linking analysis, link equity flow,
orphan page detection, hub/spoke evaluation, and conversion path optimization.

Phase 3 full implementation.  Consumes the crawl link graph, GSC page
performance data, sitemap URLs, and GSC query data to produce:
  1. Internal link graph metrics (in-degree, out-degree, depth)
  2. Simplified PageRank distribution
  3. Orphan page detection (in sitemap / GSC but unreachable via links)
  4. Hub & spoke cluster evaluation
  5. Link equity bottleneck identification
  6. Conversion path analysis
  7. Prioritised recommendations
"""

import logging
import math
from collections import defaultdict, deque
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DAMPING_FACTOR = 0.85
PAGERANK_ITERATIONS = 30
PAGERANK_CONVERGENCE = 1e-6

# Depth thresholds
DEPTH_SHALLOW = 2
DEPTH_DEEP = 4

# Link count thresholds
LOW_INTERNAL_LINKS = 3
HIGH_INTERNAL_LINKS = 50

# Hub detection thresholds
HUB_MIN_OUTLINKS = 8
SPOKE_MAX_OUTLINKS = 3


# ---------------------------------------------------------------------------
# Helper: normalise URLs for consistent comparison
# ---------------------------------------------------------------------------

def _normalise_url(url: str) -> str:
    """Strip fragments, trailing slashes, and lowercase for comparison."""
    if not url:
        return ""
    parsed = urlparse(url)
    path = parsed.path.rstrip("/") or "/"
    return f"{parsed.scheme}://{parsed.netloc}{path}".lower()


# ---------------------------------------------------------------------------
# Core class
# ---------------------------------------------------------------------------

class SiteArchitectureAnalyzer:
    """Comprehensive site architecture analysis engine."""

    def __init__(
        self,
        link_graph: Dict[str, Any],
        page_performance: Optional[List[Dict[str, Any]]] = None,
        sitemap_urls: Optional[List[str]] = None,
        query_data: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        # The link_graph from crawl_helper has structure:
        #   { "pages": [...], "link_graph": {src: [dst, ...]}, ... }
        if isinstance(link_graph, dict) and "link_graph" in link_graph:
            self.raw_link_graph: Dict[str, List[str]] = link_graph.get("link_graph", {})
            self.crawled_pages: List[Dict[str, Any]] = link_graph.get("pages", [])
        else:
            # Fallback: treat as direct {src: [dst]} mapping
            self.raw_link_graph = link_graph if isinstance(link_graph, dict) else {}
            self.crawled_pages = []

        self.page_performance = page_performance or []
        self.sitemap_urls = [_normalise_url(u) for u in (sitemap_urls or [])]
        self.query_data = query_data or []

        # Build normalised adjacency structures
        self.adj: Dict[str, Set[str]] = defaultdict(set)  # outgoing
        self.reverse_adj: Dict[str, Set[str]] = defaultdict(set)  # incoming
        self.all_nodes: Set[str] = set()

        self._build_graph()

    # ------------------------------------------------------------------
    # Graph construction
    # ------------------------------------------------------------------

    def _build_graph(self) -> None:
        """Build normalised adjacency lists from raw link graph."""
        for src, targets in self.raw_link_graph.items():
            src_n = _normalise_url(src)
            self.all_nodes.add(src_n)
            for dst in targets:
                dst_n = _normalise_url(dst)
                if dst_n and dst_n != src_n:
                    self.adj[src_n].add(dst_n)
                    self.reverse_adj[dst_n].add(src_n)
                    self.all_nodes.add(dst_n)

        # Also register pages from crawl that might not appear in adj
        for page in self.crawled_pages:
            self.all_nodes.add(_normalise_url(page.get("url", "")))

        # Register sitemap URLs
        for u in self.sitemap_urls:
            self.all_nodes.add(u)

        logger.info(
            "Graph built: %d nodes, %d edges",
            len(self.all_nodes),
            sum(len(v) for v in self.adj.values()),
        )

    # ------------------------------------------------------------------
    # 1. Link metrics (in-degree, out-degree)
    # ------------------------------------------------------------------

    def _compute_link_metrics(self) -> List[Dict[str, Any]]:
        """Per-page in-degree and out-degree stats."""
        metrics = []
        for url in self.all_nodes:
            in_deg = len(self.reverse_adj.get(url, set()))
            out_deg = len(self.adj.get(url, set()))
            metrics.append({
                "url": url,
                "in_degree": in_deg,
                "out_degree": out_deg,
                "total_links": in_deg + out_deg,
            })
        metrics.sort(key=lambda m: m["in_degree"], reverse=True)
        return metrics

    # ------------------------------------------------------------------
    # 2. Crawl depth via BFS from homepage
    # ------------------------------------------------------------------

    def _compute_crawl_depth(self) -> Dict[str, int]:
        """BFS from homepage to determine crawl depth of every page."""
        # Find homepage (shortest URL or explicit "/")
        homepage = None
        for url in self.all_nodes:
            parsed = urlparse(url)
            if parsed.path in ("/", ""):
                homepage = url
                break
        if homepage is None and self.all_nodes:
            homepage = min(self.all_nodes, key=len)

        if not homepage:
            return {}

        depth_map: Dict[str, int] = {homepage: 0}
        queue: deque = deque([homepage])

        while queue:
            current = queue.popleft()
            for neighbour in self.adj.get(current, set()):
                if neighbour not in depth_map:
                    depth_map[neighbour] = depth_map[current] + 1
                    queue.append(neighbour)

        return depth_map

    # ------------------------------------------------------------------
    # 3. Simplified PageRank
    # ------------------------------------------------------------------

    def _compute_pagerank(self) -> Dict[str, float]:
        """Iterative PageRank with damping factor."""
        n = len(self.all_nodes)
        if n == 0:
            return {}

        nodes = list(self.all_nodes)
        pr: Dict[str, float] = {u: 1.0 / n for u in nodes}

        for iteration in range(PAGERANK_ITERATIONS):
            new_pr: Dict[str, float] = {}
            diff = 0.0
            for u in nodes:
                rank_sum = 0.0
                for src in self.reverse_adj.get(u, set()):
                    out_count = len(self.adj.get(src, set()))
                    if out_count > 0:
                        rank_sum += pr[src] / out_count
                new_pr[u] = (1 - DAMPING_FACTOR) / n + DAMPING_FACTOR * rank_sum
                diff += abs(new_pr[u] - pr[u])

            pr = new_pr
            if diff < PAGERANK_CONVERGENCE:
                logger.info("PageRank converged at iteration %d", iteration)
                break

        # Normalise to percentages
        total = sum(pr.values()) or 1.0
        return {u: round((v / total) * 100, 4) for u, v in pr.items()}

    # ------------------------------------------------------------------
    # 4. Orphan page detection
    # ------------------------------------------------------------------

    def _detect_orphan_pages(self, depth_map: Dict[str, int]) -> List[Dict[str, Any]]:
        """
        Find pages that are in the sitemap or have GSC impressions
        but receive zero internal links (unreachable from link graph).
        """
        # Pages with GSC impressions
        gsc_pages: Dict[str, Dict[str, Any]] = {}
        for row in self.page_performance:
            url = _normalise_url(row.get("page", row.get("url", "")))
            if url:
                gsc_pages[url] = {
                    "clicks": row.get("clicks", 0),
                    "impressions": row.get("impressions", 0),
                    "position": row.get("position", 0),
                }

        orphans = []
        linked_pages = set(depth_map.keys())

        # Check sitemap URLs
        for url in self.sitemap_urls:
            if url and url not in linked_pages:
                perf = gsc_pages.get(url, {})
                orphans.append({
                    "url": url,
                    "source": "sitemap",
                    "in_degree": len(self.reverse_adj.get(url, set())),
                    "clicks": perf.get("clicks", 0),
                    "impressions": perf.get("impressions", 0),
                    "position": perf.get("position", 0),
                    "impact": "high" if perf.get("impressions", 0) > 100 else "medium",
                })

        # Check GSC pages not reached
        for url, perf in gsc_pages.items():
            if url not in linked_pages and url not in {o["url"] for o in orphans}:
                orphans.append({
                    "url": url,
                    "source": "gsc",
                    "in_degree": 0,
                    "clicks": perf.get("clicks", 0),
                    "impressions": perf.get("impressions", 0),
                    "position": perf.get("position", 0),
                    "impact": "high" if perf.get("impressions", 0) > 100 else "medium",
                })

        orphans.sort(key=lambda o: o["impressions"], reverse=True)
        return orphans[:50]

    # ------------------------------------------------------------------
    # 5. Hub & spoke analysis
    # ------------------------------------------------------------------

    def _analyze_hub_spoke(
        self, depth_map: Dict[str, int], pagerank: Dict[str, float]
    ) -> Dict[str, Any]:
        """
        Identify hub pages (high out-degree, link to many related pages)
        and spoke pages (low out-degree, linked from hubs).  Evaluate
        cluster quality.
        """
        hubs = []
        spokes = []
        unclassified = []

        for url in self.all_nodes:
            out_deg = len(self.adj.get(url, set()))
            in_deg = len(self.reverse_adj.get(url, set()))
            depth = depth_map.get(url, -1)
            pr = pagerank.get(url, 0)

            page_info = {
                "url": url,
                "out_degree": out_deg,
                "in_degree": in_deg,
                "depth": depth,
                "pagerank_pct": pr,
            }

            if out_deg >= HUB_MIN_OUTLINKS and depth <= DEPTH_SHALLOW:
                page_info["role"] = "hub"
                hubs.append(page_info)
            elif out_deg <= SPOKE_MAX_OUTLINKS and in_deg >= 1:
                page_info["role"] = "spoke"
                spokes.append(page_info)
            else:
                page_info["role"] = "intermediate"
                unclassified.append(page_info)

        hubs.sort(key=lambda h: h["out_degree"], reverse=True)
        spokes.sort(key=lambda s: s["in_degree"], reverse=True)

        # Evaluate clusters: for each hub, what spokes does it reach?
        clusters = []
        for hub in hubs[:20]:
            hub_targets = self.adj.get(hub["url"], set())
            spoke_targets = [
                s for s in spokes if s["url"] in hub_targets
            ]
            cluster_pr = hub["pagerank_pct"] + sum(
                s["pagerank_pct"] for s in spoke_targets
            )
            clusters.append({
                "hub_url": hub["url"],
                "hub_out_degree": hub["out_degree"],
                "spoke_count": len(spoke_targets),
                "cluster_pagerank_pct": round(cluster_pr, 4),
                "quality": (
                    "strong" if len(spoke_targets) >= 5
                    else "moderate" if len(spoke_targets) >= 2
                    else "weak"
                ),
            })

        return {
            "hubs": hubs[:20],
            "hub_count": len(hubs),
            "spoke_count": len(spokes),
            "intermediate_count": len(unclassified),
            "clusters": clusters,
        }

    # ------------------------------------------------------------------
    # 6. Link equity bottlenecks
    # ------------------------------------------------------------------

    def _find_equity_bottlenecks(
        self,
        pagerank: Dict[str, float],
        depth_map: Dict[str, int],
    ) -> List[Dict[str, Any]]:
        """
        Identify pages that hoard PageRank but distribute it poorly,
        and high-value pages buried too deep.
        """
        bottlenecks = []

        # Build GSC click lookup
        gsc_clicks: Dict[str, int] = {}
        for row in self.page_performance:
            url = _normalise_url(row.get("page", row.get("url", "")))
            gsc_clicks[url] = row.get("clicks", 0)

        for url in self.all_nodes:
            pr = pagerank.get(url, 0)
            in_deg = len(self.reverse_adj.get(url, set()))
            out_deg = len(self.adj.get(url, set()))
            depth = depth_map.get(url, -1)
            clicks = gsc_clicks.get(url, 0)

            issues = []

            # High PR but very few outgoing links → hoarding equity
            if pr > 1.0 and out_deg < 3:
                issues.append("high_pr_low_outlinks")

            # Deep page with significant traffic → should be closer to root
            if depth > DEPTH_DEEP and clicks > 50:
                issues.append("deep_high_traffic")

            # High impressions page with zero inlinks from within site
            if in_deg == 0 and clicks > 20:
                issues.append("no_inlinks_has_traffic")

            # Very high out-degree diluting equity
            if out_deg > HIGH_INTERNAL_LINKS:
                issues.append("excessive_outlinks_dilution")

            if issues:
                bottlenecks.append({
                    "url": url,
                    "pagerank_pct": pr,
                    "in_degree": in_deg,
                    "out_degree": out_deg,
                    "depth": depth,
                    "clicks": clicks,
                    "issues": issues,
                    "severity": "critical" if "high_pr_low_outlinks" in issues or "deep_high_traffic" in issues else "high",
                })

        bottlenecks.sort(
            key=lambda b: (0 if b["severity"] == "critical" else 1, -b["pagerank_pct"])
        )
        return bottlenecks[:40]

    # ------------------------------------------------------------------
    # 7. Conversion path analysis
    # ------------------------------------------------------------------

    def _analyze_conversion_paths(
        self, depth_map: Dict[str, int], pagerank: Dict[str, float]
    ) -> Dict[str, Any]:
        """
        Evaluate how well the internal link structure supports conversion
        paths from informational content → transactional pages.
        """
        # Classify pages by likely intent using URL patterns
        transactional_patterns = [
            "/pricing", "/plans", "/buy", "/checkout", "/signup",
            "/register", "/demo", "/trial", "/contact", "/quote",
            "/get-started", "/order", "/subscribe",
        ]
        informational_patterns = [
            "/blog", "/article", "/guide", "/learn", "/resource",
            "/help", "/faq", "/wiki", "/how-to", "/what-is",
            "/tutorial", "/tips",
        ]

        transactional_pages = set()
        informational_pages = set()

        for url in self.all_nodes:
            path = urlparse(url).path.lower()
            if any(p in path for p in transactional_patterns):
                transactional_pages.add(url)
            elif any(p in path for p in informational_patterns):
                informational_pages.add(url)

        # For each informational page, find shortest path to any transactional page
        paths_found = []
        no_path_pages = []

        for info_url in list(informational_pages)[:100]:
            shortest = self._bfs_shortest_path(info_url, transactional_pages)
            if shortest is not None:
                paths_found.append({
                    "from": info_url,
                    "to": shortest["target"],
                    "hops": shortest["distance"],
                    "path": shortest["path"][:6],  # cap display
                })
            else:
                no_path_pages.append(info_url)

        # Evaluate transactional page accessibility
        transactional_metrics = []
        for t_url in transactional_pages:
            depth = depth_map.get(t_url, -1)
            in_deg = len(self.reverse_adj.get(t_url, set()))
            pr = pagerank.get(t_url, 0)
            transactional_metrics.append({
                "url": t_url,
                "depth": depth,
                "in_degree": in_deg,
                "pagerank_pct": pr,
                "accessible": depth != -1 and depth <= DEPTH_DEEP,
            })

        avg_hops = (
            sum(p["hops"] for p in paths_found) / len(paths_found)
            if paths_found else 0
        )

        return {
            "transactional_page_count": len(transactional_pages),
            "informational_page_count": len(informational_pages),
            "paths_to_conversion": sorted(paths_found, key=lambda p: p["hops"])[:30],
            "avg_hops_to_conversion": round(avg_hops, 2),
            "info_pages_no_path_to_conversion": no_path_pages[:20],
            "no_path_count": len(no_path_pages),
            "transactional_page_metrics": sorted(
                transactional_metrics, key=lambda t: -t["in_degree"]
            ),
        }

    def _bfs_shortest_path(
        self, start: str, targets: Set[str]
    ) -> Optional[Dict[str, Any]]:
        """BFS to find shortest path from start to any target."""
        if start in targets:
            return {"target": start, "distance": 0, "path": [start]}

        visited = {start}
        queue: deque = deque([(start, [start])])

        while queue:
            current, path = queue.popleft()
            if len(path) > 8:
                break  # limit search depth

            for neighbour in self.adj.get(current, set()):
                if neighbour in targets:
                    return {
                        "target": neighbour,
                        "distance": len(path),
                        "path": path + [neighbour],
                    }
                if neighbour not in visited:
                    visited.add(neighbour)
                    queue.append((neighbour, path + [neighbour]))

        return None

    # ------------------------------------------------------------------
    # 8. Depth distribution
    # ------------------------------------------------------------------

    def _depth_distribution(self, depth_map: Dict[str, int]) -> Dict[str, Any]:
        """Summarise how pages are distributed across crawl depths."""
        if not depth_map:
            return {"distribution": {}, "avg_depth": 0, "max_depth": 0, "unreachable": 0}

        dist: Dict[int, int] = defaultdict(int)
        for d in depth_map.values():
            dist[d] += 1

        unreachable = len(self.all_nodes) - len(depth_map)
        depths = list(depth_map.values())
        avg_depth = sum(depths) / len(depths) if depths else 0

        return {
            "distribution": dict(sorted(dist.items())),
            "avg_depth": round(avg_depth, 2),
            "max_depth": max(depths) if depths else 0,
            "unreachable": unreachable,
            "pages_beyond_depth_4": sum(1 for d in depths if d > DEPTH_DEEP),
        }

    # ------------------------------------------------------------------
    # 9. Recommendations
    # ------------------------------------------------------------------

    def _generate_recommendations(
        self,
        orphans: List[Dict[str, Any]],
        bottlenecks: List[Dict[str, Any]],
        hub_spoke: Dict[str, Any],
        depth_info: Dict[str, Any],
        conversion: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """Generate prioritised, actionable architecture recommendations."""
        recs = []

        # Orphan pages
        high_imp_orphans = [o for o in orphans if o["impact"] == "high"]
        if high_imp_orphans:
            recs.append({
                "priority": 1,
                "category": "orphan_pages",
                "title": "Add internal links to high-value orphan pages",
                "description": (
                    f"{len(high_imp_orphans)} page(s) receive Google impressions but "
                    "have zero internal links pointing to them. Add contextual links "
                    "from related hub or category pages to pass link equity and help "
                    "crawlers discover this content."
                ),
                "affected_pages": [o["url"] for o in high_imp_orphans[:5]],
                "impact": "high",
            })

        # Equity bottlenecks
        critical_bottlenecks = [
            b for b in bottlenecks if b["severity"] == "critical"
        ]
        if critical_bottlenecks:
            hoarding = [b for b in critical_bottlenecks if "high_pr_low_outlinks" in b["issues"]]
            deep_traffic = [b for b in critical_bottlenecks if "deep_high_traffic" in b["issues"]]

            if hoarding:
                recs.append({
                    "priority": 2,
                    "category": "equity_distribution",
                    "title": "Redistribute link equity from hoarding pages",
                    "description": (
                        f"{len(hoarding)} page(s) accumulate significant PageRank "
                        "but have very few outgoing links. Add internal links from "
                        "these pages to important target pages to spread equity."
                    ),
                    "affected_pages": [b["url"] for b in hoarding[:5]],
                    "impact": "high",
                })

            if deep_traffic:
                recs.append({
                    "priority": 2,
                    "category": "depth_optimisation",
                    "title": "Move high-traffic pages closer to the homepage",
                    "description": (
                        f"{len(deep_traffic)} page(s) with meaningful organic traffic "
                        "are buried more than 4 clicks from the homepage. Reduce "
                        "crawl depth by linking from shallower hub pages."
                    ),
                    "affected_pages": [b["url"] for b in deep_traffic[:5]],
                    "impact": "high",
                })

        # Hub/spoke quality
        weak_clusters = [
            c for c in hub_spoke.get("clusters", []) if c["quality"] == "weak"
        ]
        if weak_clusters:
            recs.append({
                "priority": 3,
                "category": "hub_spoke_structure",
                "title": "Strengthen weak hub-spoke clusters",
                "description": (
                    f"{len(weak_clusters)} hub page(s) link to very few spoke pages. "
                    "Build out supporting content and interlink it with these hub "
                    "pages to create stronger topical clusters."
                ),
                "affected_pages": [c["hub_url"] for c in weak_clusters[:5]],
                "impact": "medium",
            })

        # Deep pages
        deep_count = depth_info.get("pages_beyond_depth_4", 0)
        if deep_count > 0:
            recs.append({
                "priority": 4,
                "category": "crawl_depth",
                "title": "Flatten site architecture for deep pages",
                "description": (
                    f"{deep_count} page(s) sit more than 4 clicks from the homepage. "
                    "Google may deprioritise crawling these. Add links from higher-level "
                    "pages or create an HTML sitemap to reduce average depth."
                ),
                "impact": "medium",
            })

        # Conversion paths
        if conversion.get("no_path_count", 0) > 0:
            recs.append({
                "priority": 3,
                "category": "conversion_paths",
                "title": "Create internal links from content pages to conversion pages",
                "description": (
                    f"{conversion['no_path_count']} informational page(s) have no internal "
                    "link path to any transactional page. Add in-content CTAs or sidebar "
                    "links to guide users toward conversion."
                ),
                "affected_pages": conversion.get("info_pages_no_path_to_conversion", [])[:5],
                "impact": "high",
            })

        if conversion.get("avg_hops_to_conversion", 0) > 3:
            recs.append({
                "priority": 4,
                "category": "conversion_paths",
                "title": "Shorten path from content to conversion",
                "description": (
                    f"Average path length from informational to transactional pages is "
                    f"{conversion['avg_hops_to_conversion']} hops. Aim for 2 or fewer "
                    "by adding direct links from high-traffic content pages."
                ),
                "impact": "medium",
            })

        # Excessive outlinks
        diluters = [b for b in bottlenecks if "excessive_outlinks_dilution" in b["issues"]]
        if diluters:
            recs.append({
                "priority": 5,
                "category": "link_dilution",
                "title": "Reduce excessive internal links on key pages",
                "description": (
                    f"{len(diluters)} page(s) have more than {HIGH_INTERNAL_LINKS} internal "
                    "links, which dilutes the PageRank passed to each target. "
                    "Audit these pages and remove or consolidate low-value links."
                ),
                "affected_pages": [b["url"] for b in diluters[:5]],
                "impact": "medium",
            })

        recs.sort(key=lambda r: r["priority"])
        return recs

    # ------------------------------------------------------------------
    # 10. Summary
    # ------------------------------------------------------------------

    def _build_summary(
        self,
        link_metrics: List[Dict[str, Any]],
        depth_info: Dict[str, Any],
        orphans: List[Dict[str, Any]],
        hub_spoke: Dict[str, Any],
        bottlenecks: List[Dict[str, Any]],
        conversion: Dict[str, Any],
        recommendations: List[Dict[str, Any]],
    ) -> str:
        """Build a human-readable narrative summary."""
        parts = []

        total_pages = len(self.all_nodes)
        total_edges = sum(len(v) for v in self.adj.values())
        avg_in = round(total_edges / total_pages, 1) if total_pages else 0

        parts.append(
            f"Site architecture analysis covers {total_pages} pages and "
            f"{total_edges} internal links (avg {avg_in} inlinks per page)."
        )

        parts.append(
            f"Average crawl depth is {depth_info['avg_depth']} "
            f"(max {depth_info['max_depth']}). "
            f"{depth_info.get('pages_beyond_depth_4', 0)} page(s) are deeper than 4 clicks."
        )

        if orphans:
            high_orphans = sum(1 for o in orphans if o["impact"] == "high")
            parts.append(
                f"{len(orphans)} orphan page(s) detected ({high_orphans} high-impact)."
            )

        parts.append(
            f"Hub/spoke analysis found {hub_spoke['hub_count']} hub(s), "
            f"{hub_spoke['spoke_count']} spoke(s), "
            f"and {hub_spoke['intermediate_count']} intermediate page(s)."
        )

        critical_bn = sum(1 for b in bottlenecks if b["severity"] == "critical")
        if critical_bn:
            parts.append(
                f"{critical_bn} critical link equity bottleneck(s) identified."
            )

        if conversion.get("transactional_page_count", 0) > 0:
            parts.append(
                f"Conversion path analysis: {conversion['transactional_page_count']} "
                f"transactional page(s), avg {conversion['avg_hops_to_conversion']} hops "
                f"from content. {conversion.get('no_path_count', 0)} content page(s) "
                f"have no link path to conversion."
            )

        parts.append(f"{len(recommendations)} recommendation(s) generated.")

        return " ".join(parts)

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def run(self) -> Dict[str, Any]:
        """Execute the full site architecture analysis pipeline."""
        logger.info("Starting site architecture analysis")

        # Core computations
        link_metrics = self._compute_link_metrics()
        depth_map = self._compute_crawl_depth()
        pagerank = self._compute_pagerank()
        depth_info = self._depth_distribution(depth_map)

        # Analyses
        orphans = self._detect_orphan_pages(depth_map)
        hub_spoke = self._analyze_hub_spoke(depth_map, pagerank)
        bottlenecks = self._find_equity_bottlenecks(pagerank, depth_map)
        conversion = self._analyze_conversion_paths(depth_map, pagerank)

        # Recommendations
        recommendations = self._generate_recommendations(
            orphans, bottlenecks, hub_spoke, depth_info, conversion
        )

        # Summary
        summary = self._build_summary(
            link_metrics, depth_info, orphans, hub_spoke,
            bottlenecks, conversion, recommendations,
        )

        # Top pages by PageRank
        top_pagerank = sorted(
            [{"url": u, "pagerank_pct": v} for u, v in pagerank.items()],
            key=lambda x: -x["pagerank_pct"],
        )[:30]

        result = {
            "summary": summary,
            "graph_stats": {
                "total_pages": len(self.all_nodes),
                "total_internal_links": sum(len(v) for v in self.adj.values()),
                "avg_in_degree": round(
                    sum(len(v) for v in self.reverse_adj.values()) / max(len(self.all_nodes), 1), 2
                ),
                "avg_out_degree": round(
                    sum(len(v) for v in self.adj.values()) / max(len(self.all_nodes), 1), 2
                ),
            },
            "depth_distribution": depth_info,
            "top_pages_by_pagerank": top_pagerank,
            "orphan_pages": orphans,
            "hub_spoke_analysis": hub_spoke,
            "link_equity_bottlenecks": bottlenecks,
            "conversion_paths": conversion,
            "recommendations": recommendations,
        }

        logger.info("Site architecture analysis complete: %s", summary)
        return result


# ---------------------------------------------------------------------------
# Public function (matches signature expected by routes/modules.py)
# ---------------------------------------------------------------------------

def analyze_site_architecture(
    link_graph: Dict[str, Any],
    page_performance: Optional[List[Dict[str, Any]]] = None,
    sitemap_urls: Optional[List[str]] = None,
    query_data: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    Module 9: Site Architecture — internal linking analysis, link equity
    flow, orphan page detection, hub/spoke evaluation, conversion paths.

    Args:
        link_graph: Crawl link graph from crawl_helper (dict with "link_graph",
                    "pages" keys) or direct {src_url: [dst_url, ...]} mapping.
        page_performance: GSC page-level data, list of dicts with keys
                          page/url, clicks, impressions, position.
        sitemap_urls: List of URLs from the XML sitemap.
        query_data: GSC query data (list of dicts) for intent mapping.

    Returns:
        Dict with summary, graph_stats, depth_distribution, top_pages_by_pagerank,
        orphan_pages, hub_spoke_analysis, link_equity_bottlenecks,
        conversion_paths, recommendations.
    """
    analyzer = SiteArchitectureAnalyzer(
        link_graph=link_graph,
        page_performance=page_performance,
        sitemap_urls=sitemap_urls,
        query_data=query_data,
    )
    return analyzer.run()
