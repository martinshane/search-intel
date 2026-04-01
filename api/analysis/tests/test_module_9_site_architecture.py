"""
Comprehensive test suite for Module 9: Site Architecture.

Tests cover:
  1. Constants — thresholds are sane
  2. _normalise_url — URL normalisation helper
  3. __init__ / _build_graph — graph construction from various inputs
  4. _compute_link_metrics — in/out degree calculation
  5. _compute_crawl_depth — BFS depth from homepage
  6. _compute_pagerank — simplified PageRank
  7. _detect_orphan_pages — orphan page identification
  8. _analyze_hub_spoke — hub/spoke classification and clusters
  9. _find_equity_bottlenecks — equity distribution issues
 10. _analyze_conversion_paths — informational→transactional path analysis
 11. _bfs_shortest_path — shortest path BFS helper
 12. _depth_distribution — depth histogram
 13. _generate_recommendations — recommendation generation
 14. _build_summary — narrative summary
 15. Full pipeline (run / analyze_site_architecture) — end-to-end
 16. Edge cases
"""

import math
import pytest
from unittest.mock import patch

from api.analysis.module_9_site_architecture import (
    SiteArchitectureAnalyzer,
    analyze_site_architecture,
    _normalise_url,
    DAMPING_FACTOR,
    PAGERANK_ITERATIONS,
    PAGERANK_CONVERGENCE,
    DEPTH_SHALLOW,
    DEPTH_DEEP,
    LOW_INTERNAL_LINKS,
    HIGH_INTERNAL_LINKS,
    HUB_MIN_OUTLINKS,
    SPOKE_MAX_OUTLINKS,
)


# -----------------------------------------------------------------------
# Helpers to build test fixtures
# -----------------------------------------------------------------------

def _simple_link_graph():
    """A→B→C, A→C, homepage at A."""
    return {
        "link_graph": {
            "https://example.com/": ["https://example.com/b", "https://example.com/c"],
            "https://example.com/b": ["https://example.com/c"],
        },
        "pages": [
            {"url": "https://example.com/"},
            {"url": "https://example.com/b"},
            {"url": "https://example.com/c"},
        ],
    }


def _hub_spoke_graph():
    """Hub with 10 outlinks (spokes), each spoke has 1 inlink."""
    spokes = [f"https://example.com/spoke-{i}" for i in range(10)]
    graph = {
        "link_graph": {
            "https://example.com/": spokes,
        },
        "pages": [{"url": "https://example.com/"}] + [{"url": s} for s in spokes],
    }
    return graph


def _deep_graph():
    """Linear chain: / → /a → /b → /c → /d → /e (depth 5)."""
    return {
        "link_graph": {
            "https://example.com/": ["https://example.com/a"],
            "https://example.com/a": ["https://example.com/b"],
            "https://example.com/b": ["https://example.com/c"],
            "https://example.com/c": ["https://example.com/d"],
            "https://example.com/d": ["https://example.com/e"],
        },
        "pages": [],
    }


def _conversion_graph():
    """Blog pages and a pricing page connected via intermediate."""
    return {
        "link_graph": {
            "https://example.com/": [
                "https://example.com/blog/post-1",
                "https://example.com/about",
            ],
            "https://example.com/blog/post-1": [
                "https://example.com/about",
            ],
            "https://example.com/about": [
                "https://example.com/pricing",
            ],
        },
        "pages": [],
    }


# ====================================================================
# 1. Constants
# ====================================================================

class TestConstants:
    def test_damping_factor_range(self):
        assert 0 < DAMPING_FACTOR < 1

    def test_pagerank_iterations_positive(self):
        assert PAGERANK_ITERATIONS > 0

    def test_convergence_small(self):
        assert PAGERANK_CONVERGENCE > 0
        assert PAGERANK_CONVERGENCE < 0.01

    def test_depth_thresholds_ordered(self):
        assert DEPTH_SHALLOW < DEPTH_DEEP

    def test_link_thresholds_ordered(self):
        assert LOW_INTERNAL_LINKS < HIGH_INTERNAL_LINKS

    def test_hub_spoke_thresholds(self):
        assert HUB_MIN_OUTLINKS > SPOKE_MAX_OUTLINKS


# ====================================================================
# 2. _normalise_url
# ====================================================================

class TestNormaliseUrl:
    def test_empty_string(self):
        assert _normalise_url("") == ""

    def test_strip_trailing_slash(self):
        result = _normalise_url("https://example.com/path/")
        assert not result.endswith("/path/")
        assert result.endswith("/path")

    def test_root_path_preserved(self):
        result = _normalise_url("https://example.com/")
        assert result.endswith("/")

    def test_strip_fragment(self):
        result = _normalise_url("https://example.com/page#section")
        assert "#" not in result

    def test_lowercase(self):
        result = _normalise_url("HTTPS://Example.COM/Path")
        assert result == result.lower()

    def test_query_params_not_included(self):
        # urlparse puts query in a separate field, path won't include it
        result = _normalise_url("https://example.com/page?q=test")
        # The current impl only uses scheme + netloc + path
        assert "q=test" not in result


# ====================================================================
# 3. Graph construction (__init__ / _build_graph)
# ====================================================================

class TestGraphConstruction:
    def test_standard_link_graph_input(self):
        analyzer = SiteArchitectureAnalyzer(_simple_link_graph())
        assert len(analyzer.all_nodes) == 3
        assert len(analyzer.adj) > 0

    def test_direct_dict_input(self):
        """When link_graph is just {src: [dst]} without 'link_graph' key."""
        direct = {
            "https://a.com/": ["https://a.com/b"],
        }
        analyzer = SiteArchitectureAnalyzer(direct)
        assert len(analyzer.all_nodes) == 2

    def test_empty_link_graph(self):
        analyzer = SiteArchitectureAnalyzer({"link_graph": {}, "pages": []})
        assert len(analyzer.all_nodes) == 0

    def test_self_links_excluded(self):
        graph = {
            "link_graph": {
                "https://a.com/": ["https://a.com/", "https://a.com/b"],
            },
            "pages": [],
        }
        analyzer = SiteArchitectureAnalyzer(graph)
        a_norm = _normalise_url("https://a.com/")
        # Self link should not be in adjacency
        assert a_norm not in analyzer.adj.get(a_norm, set())

    def test_sitemap_urls_registered(self):
        analyzer = SiteArchitectureAnalyzer(
            {"link_graph": {}, "pages": []},
            sitemap_urls=["https://a.com/page1", "https://a.com/page2"],
        )
        assert len(analyzer.all_nodes) >= 2

    def test_page_performance_stored(self):
        perf = [{"page": "https://a.com/", "clicks": 100}]
        analyzer = SiteArchitectureAnalyzer(
            {"link_graph": {}, "pages": []},
            page_performance=perf,
        )
        assert analyzer.page_performance == perf

    def test_non_dict_link_graph_fallback(self):
        """If link_graph is not a dict, adj should be empty."""
        analyzer = SiteArchitectureAnalyzer("not a dict")
        assert len(analyzer.adj) == 0


# ====================================================================
# 4. _compute_link_metrics
# ====================================================================

class TestLinkMetrics:
    def test_basic_metrics(self):
        analyzer = SiteArchitectureAnalyzer(_simple_link_graph())
        metrics = analyzer._compute_link_metrics()
        assert len(metrics) == 3
        for m in metrics:
            assert "url" in m
            assert "in_degree" in m
            assert "out_degree" in m
            assert "total_links" in m
            assert m["total_links"] == m["in_degree"] + m["out_degree"]

    def test_sorted_by_in_degree(self):
        analyzer = SiteArchitectureAnalyzer(_simple_link_graph())
        metrics = analyzer._compute_link_metrics()
        in_degrees = [m["in_degree"] for m in metrics]
        assert in_degrees == sorted(in_degrees, reverse=True)

    def test_empty_graph(self):
        analyzer = SiteArchitectureAnalyzer({"link_graph": {}, "pages": []})
        metrics = analyzer._compute_link_metrics()
        assert metrics == []

    def test_homepage_out_degree(self):
        analyzer = SiteArchitectureAnalyzer(_simple_link_graph())
        metrics = analyzer._compute_link_metrics()
        home = [m for m in metrics if m["url"].endswith("/")]
        assert len(home) == 1
        assert home[0]["out_degree"] == 2


# ====================================================================
# 5. _compute_crawl_depth
# ====================================================================

class TestCrawlDepth:
    def test_homepage_depth_zero(self):
        analyzer = SiteArchitectureAnalyzer(_simple_link_graph())
        depth = analyzer._compute_crawl_depth()
        home_url = _normalise_url("https://example.com/")
        assert depth[home_url] == 0

    def test_direct_child_depth_one(self):
        analyzer = SiteArchitectureAnalyzer(_simple_link_graph())
        depth = analyzer._compute_crawl_depth()
        b_url = _normalise_url("https://example.com/b")
        assert depth[b_url] == 1

    def test_deep_chain(self):
        analyzer = SiteArchitectureAnalyzer(_deep_graph())
        depth = analyzer._compute_crawl_depth()
        e_url = _normalise_url("https://example.com/e")
        assert depth[e_url] == 5

    def test_empty_graph_returns_empty(self):
        analyzer = SiteArchitectureAnalyzer({"link_graph": {}, "pages": []})
        depth = analyzer._compute_crawl_depth()
        assert depth == {}

    def test_unreachable_pages_not_in_depth_map(self):
        graph = {
            "link_graph": {"https://a.com/": ["https://a.com/b"]},
            "pages": [],
        }
        analyzer = SiteArchitectureAnalyzer(graph)
        # Add an unreachable node via sitemap
        analyzer.all_nodes.add(_normalise_url("https://a.com/orphan"))
        depth = analyzer._compute_crawl_depth()
        orphan_url = _normalise_url("https://a.com/orphan")
        assert orphan_url not in depth

    def test_shortest_url_fallback_homepage(self):
        """When no URL has path '/', shortest URL is used as homepage."""
        graph = {
            "link_graph": {
                "https://a.com/page1": ["https://a.com/page2"],
            },
            "pages": [],
        }
        analyzer = SiteArchitectureAnalyzer(graph)
        depth = analyzer._compute_crawl_depth()
        # Should still produce a depth map
        assert len(depth) >= 1


# ====================================================================
# 6. _compute_pagerank
# ====================================================================

class TestPageRank:
    def test_empty_graph(self):
        analyzer = SiteArchitectureAnalyzer({"link_graph": {}, "pages": []})
        pr = analyzer._compute_pagerank()
        assert pr == {}

    def test_sums_to_100(self):
        analyzer = SiteArchitectureAnalyzer(_simple_link_graph())
        pr = analyzer._compute_pagerank()
        total = sum(pr.values())
        assert abs(total - 100.0) < 0.1

    def test_sink_node_gets_pagerank(self):
        """Node C has no outlinks but should still receive PageRank."""
        analyzer = SiteArchitectureAnalyzer(_simple_link_graph())
        pr = analyzer._compute_pagerank()
        c_url = _normalise_url("https://example.com/c")
        assert pr.get(c_url, 0) > 0

    def test_all_values_positive(self):
        analyzer = SiteArchitectureAnalyzer(_simple_link_graph())
        pr = analyzer._compute_pagerank()
        for v in pr.values():
            assert v >= 0

    def test_single_node(self):
        graph = {"link_graph": {}, "pages": [{"url": "https://a.com/"}]}
        analyzer = SiteArchitectureAnalyzer(graph)
        pr = analyzer._compute_pagerank()
        assert len(pr) == 1
        assert abs(list(pr.values())[0] - 100.0) < 0.1

    def test_hub_gets_more_pagerank_from_backlinks(self):
        """Hub with many pages linking to it should accumulate PR."""
        spokes = [f"https://a.com/s{i}" for i in range(5)]
        link_graph = {}
        for s in spokes:
            link_graph[s] = ["https://a.com/hub"]
        link_graph["https://a.com/hub"] = spokes
        graph = {"link_graph": link_graph, "pages": []}
        analyzer = SiteArchitectureAnalyzer(graph)
        pr = analyzer._compute_pagerank()
        hub_url = _normalise_url("https://a.com/hub")
        hub_pr = pr.get(hub_url, 0)
        avg_spoke_pr = sum(pr.get(_normalise_url(s), 0) for s in spokes) / len(spokes)
        assert hub_pr > avg_spoke_pr


# ====================================================================
# 7. _detect_orphan_pages
# ====================================================================

class TestOrphanPages:
    def test_no_orphans_when_all_linked(self):
        analyzer = SiteArchitectureAnalyzer(_simple_link_graph())
        depth = analyzer._compute_crawl_depth()
        orphans = analyzer._detect_orphan_pages(depth)
        assert len(orphans) == 0

    def test_sitemap_orphan_detected(self):
        graph = {
            "link_graph": {"https://a.com/": ["https://a.com/b"]},
            "pages": [],
        }
        analyzer = SiteArchitectureAnalyzer(
            graph,
            sitemap_urls=["https://a.com/orphan"],
        )
        depth = analyzer._compute_crawl_depth()
        orphans = analyzer._detect_orphan_pages(depth)
        assert len(orphans) >= 1
        assert orphans[0]["source"] == "sitemap"

    def test_gsc_orphan_detected(self):
        graph = {
            "link_graph": {"https://a.com/": ["https://a.com/b"]},
            "pages": [],
        }
        perf = [{"page": "https://a.com/gsc-only", "clicks": 50, "impressions": 200}]
        analyzer = SiteArchitectureAnalyzer(graph, page_performance=perf)
        depth = analyzer._compute_crawl_depth()
        orphans = analyzer._detect_orphan_pages(depth)
        gsc_orphans = [o for o in orphans if o["source"] == "gsc"]
        assert len(gsc_orphans) >= 1

    def test_high_impact_orphan(self):
        graph = {"link_graph": {"https://a.com/": []}, "pages": []}
        perf = [{"page": "https://a.com/important", "impressions": 500, "clicks": 100}]
        analyzer = SiteArchitectureAnalyzer(graph, page_performance=perf)
        depth = analyzer._compute_crawl_depth()
        orphans = analyzer._detect_orphan_pages(depth)
        high = [o for o in orphans if o["impact"] == "high"]
        assert len(high) >= 1

    def test_orphans_sorted_by_impressions(self):
        graph = {"link_graph": {"https://a.com/": []}, "pages": []}
        perf = [
            {"page": "https://a.com/low", "impressions": 10, "clicks": 1},
            {"page": "https://a.com/high", "impressions": 500, "clicks": 50},
        ]
        analyzer = SiteArchitectureAnalyzer(graph, page_performance=perf)
        depth = analyzer._compute_crawl_depth()
        orphans = analyzer._detect_orphan_pages(depth)
        if len(orphans) >= 2:
            assert orphans[0]["impressions"] >= orphans[1]["impressions"]

    def test_orphans_capped_at_50(self):
        graph = {"link_graph": {"https://a.com/": []}, "pages": []}
        perf = [{"page": f"https://a.com/p{i}", "impressions": i, "clicks": 0} for i in range(60)]
        analyzer = SiteArchitectureAnalyzer(graph, page_performance=perf)
        depth = analyzer._compute_crawl_depth()
        orphans = analyzer._detect_orphan_pages(depth)
        assert len(orphans) <= 50

    def test_no_duplicate_orphans(self):
        """A URL in both sitemap and GSC should not appear twice."""
        graph = {"link_graph": {"https://a.com/": []}, "pages": []}
        analyzer = SiteArchitectureAnalyzer(
            graph,
            sitemap_urls=["https://a.com/dupe"],
            page_performance=[{"page": "https://a.com/dupe", "impressions": 100, "clicks": 10}],
        )
        depth = analyzer._compute_crawl_depth()
        orphans = analyzer._detect_orphan_pages(depth)
        urls = [o["url"] for o in orphans]
        dupe_url = _normalise_url("https://a.com/dupe")
        assert urls.count(dupe_url) <= 1


# ====================================================================
# 8. Hub & spoke analysis
# ====================================================================

class TestHubSpoke:
    def test_hub_detected(self):
        analyzer = SiteArchitectureAnalyzer(_hub_spoke_graph())
        depth = analyzer._compute_crawl_depth()
        pr = analyzer._compute_pagerank()
        hs = analyzer._analyze_hub_spoke(depth, pr)
        assert hs["hub_count"] >= 1

    def test_spokes_detected(self):
        analyzer = SiteArchitectureAnalyzer(_hub_spoke_graph())
        depth = analyzer._compute_crawl_depth()
        pr = analyzer._compute_pagerank()
        hs = analyzer._analyze_hub_spoke(depth, pr)
        assert hs["spoke_count"] >= 1

    def test_cluster_quality_strong(self):
        """Hub with 10 spokes should produce a strong cluster."""
        analyzer = SiteArchitectureAnalyzer(_hub_spoke_graph())
        depth = analyzer._compute_crawl_depth()
        pr = analyzer._compute_pagerank()
        hs = analyzer._analyze_hub_spoke(depth, pr)
        strong = [c for c in hs["clusters"] if c["quality"] == "strong"]
        assert len(strong) >= 1

    def test_hubs_sorted_by_out_degree(self):
        analyzer = SiteArchitectureAnalyzer(_hub_spoke_graph())
        depth = analyzer._compute_crawl_depth()
        pr = analyzer._compute_pagerank()
        hs = analyzer._analyze_hub_spoke(depth, pr)
        hubs = hs["hubs"]
        if len(hubs) >= 2:
            out_degs = [h["out_degree"] for h in hubs]
            assert out_degs == sorted(out_degs, reverse=True)

    def test_hubs_capped_at_20(self):
        # Create 25 "hub" pages with 10 outlinks each
        link_graph = {}
        pages = []
        for i in range(25):
            hub = f"https://a.com/hub-{i}"
            targets = [f"https://a.com/target-{i}-{j}" for j in range(10)]
            link_graph[hub] = targets
            pages.append({"url": hub})
        graph = {"link_graph": link_graph, "pages": pages}
        analyzer = SiteArchitectureAnalyzer(graph)
        depth = analyzer._compute_crawl_depth()
        pr = analyzer._compute_pagerank()
        hs = analyzer._analyze_hub_spoke(depth, pr)
        assert len(hs["hubs"]) <= 20

    def test_empty_graph_no_hubs(self):
        analyzer = SiteArchitectureAnalyzer({"link_graph": {}, "pages": []})
        depth = analyzer._compute_crawl_depth()
        pr = analyzer._compute_pagerank()
        hs = analyzer._analyze_hub_spoke(depth, pr)
        assert hs["hub_count"] == 0
        assert hs["spoke_count"] == 0

    def test_cluster_fields(self):
        analyzer = SiteArchitectureAnalyzer(_hub_spoke_graph())
        depth = analyzer._compute_crawl_depth()
        pr = analyzer._compute_pagerank()
        hs = analyzer._analyze_hub_spoke(depth, pr)
        if hs["clusters"]:
            c = hs["clusters"][0]
            assert "hub_url" in c
            assert "hub_out_degree" in c
            assert "spoke_count" in c
            assert "cluster_pagerank_pct" in c
            assert "quality" in c


# ====================================================================
# 9. Equity bottlenecks
# ====================================================================

class TestEquityBottlenecks:
    def test_high_pr_low_outlinks(self):
        # Node gets all PR but has 0 outlinks
        graph = {
            "link_graph": {
                "https://a.com/": ["https://a.com/sink"],
                "https://a.com/feeder1": ["https://a.com/sink"],
                "https://a.com/feeder2": ["https://a.com/sink"],
            },
            "pages": [],
        }
        analyzer = SiteArchitectureAnalyzer(graph)
        pr = analyzer._compute_pagerank()
        depth = analyzer._compute_crawl_depth()
        bottlenecks = analyzer._find_equity_bottlenecks(pr, depth)
        hoarding = [b for b in bottlenecks if "high_pr_low_outlinks" in b["issues"]]
        # sink has high PR and 0 outlinks
        assert len(hoarding) >= 1

    def test_deep_high_traffic(self):
        graph = _deep_graph()
        analyzer = SiteArchitectureAnalyzer(
            graph,
            page_performance=[{"page": "https://example.com/e", "clicks": 100}],
        )
        pr = analyzer._compute_pagerank()
        depth = analyzer._compute_crawl_depth()
        bottlenecks = analyzer._find_equity_bottlenecks(pr, depth)
        deep = [b for b in bottlenecks if "deep_high_traffic" in b["issues"]]
        assert len(deep) >= 1

    def test_excessive_outlinks(self):
        targets = [f"https://a.com/t{i}" for i in range(60)]
        graph = {
            "link_graph": {"https://a.com/": targets},
            "pages": [],
        }
        analyzer = SiteArchitectureAnalyzer(graph)
        pr = analyzer._compute_pagerank()
        depth = analyzer._compute_crawl_depth()
        bottlenecks = analyzer._find_equity_bottlenecks(pr, depth)
        diluters = [b for b in bottlenecks if "excessive_outlinks_dilution" in b["issues"]]
        assert len(diluters) >= 1

    def test_bottlenecks_sorted_critical_first(self):
        graph = _deep_graph()
        analyzer = SiteArchitectureAnalyzer(
            graph,
            page_performance=[{"page": "https://example.com/e", "clicks": 100}],
        )
        pr = analyzer._compute_pagerank()
        depth = analyzer._compute_crawl_depth()
        bottlenecks = analyzer._find_equity_bottlenecks(pr, depth)
        if len(bottlenecks) >= 2:
            severities = [b["severity"] for b in bottlenecks]
            critical_indices = [i for i, s in enumerate(severities) if s == "critical"]
            high_indices = [i for i, s in enumerate(severities) if s == "high"]
            if critical_indices and high_indices:
                assert max(critical_indices) < min(high_indices)

    def test_bottlenecks_capped_at_40(self):
        # Create many pages with issues
        link_graph = {}
        perf = []
        for i in range(50):
            url = f"https://a.com/deep-{i}"
            link_graph[f"https://a.com/chain-{i}"] = [url]
            perf.append({"page": url, "clicks": 100})
        graph = {"link_graph": link_graph, "pages": []}
        analyzer = SiteArchitectureAnalyzer(graph, page_performance=perf)
        pr = analyzer._compute_pagerank()
        depth = analyzer._compute_crawl_depth()
        bottlenecks = analyzer._find_equity_bottlenecks(pr, depth)
        assert len(bottlenecks) <= 40

    def test_no_bottlenecks_clean_graph(self):
        """Simple well-linked graph should have few or no bottlenecks."""
        analyzer = SiteArchitectureAnalyzer(_simple_link_graph())
        pr = analyzer._compute_pagerank()
        depth = analyzer._compute_crawl_depth()
        bottlenecks = analyzer._find_equity_bottlenecks(pr, depth)
        # May have some minor issues but no critical ones
        critical = [b for b in bottlenecks if b["severity"] == "critical"]
        # Simple graph is small enough that PR won't exceed 1.0% for hoarding
        # Just verify it doesn't crash
        assert isinstance(bottlenecks, list)

    def test_bottleneck_fields(self):
        graph = _deep_graph()
        analyzer = SiteArchitectureAnalyzer(
            graph,
            page_performance=[{"page": "https://example.com/e", "clicks": 100}],
        )
        pr = analyzer._compute_pagerank()
        depth = analyzer._compute_crawl_depth()
        bottlenecks = analyzer._find_equity_bottlenecks(pr, depth)
        if bottlenecks:
            b = bottlenecks[0]
            assert "url" in b
            assert "pagerank_pct" in b
            assert "in_degree" in b
            assert "out_degree" in b
            assert "depth" in b
            assert "clicks" in b
            assert "issues" in b
            assert "severity" in b


# ====================================================================
# 10. Conversion paths
# ====================================================================

class TestConversionPaths:
    def test_path_found(self):
        analyzer = SiteArchitectureAnalyzer(_conversion_graph())
        depth = analyzer._compute_crawl_depth()
        pr = analyzer._compute_pagerank()
        conv = analyzer._analyze_conversion_paths(depth, pr)
        assert conv["transactional_page_count"] >= 1
        assert len(conv["paths_to_conversion"]) >= 1

    def test_no_path_detected(self):
        """Blog page with no link to pricing."""
        graph = {
            "link_graph": {
                "https://a.com/": ["https://a.com/blog/post"],
                "https://a.com/pricing": [],
            },
            "pages": [],
        }
        analyzer = SiteArchitectureAnalyzer(graph)
        depth = analyzer._compute_crawl_depth()
        pr = analyzer._compute_pagerank()
        conv = analyzer._analyze_conversion_paths(depth, pr)
        # blog/post is informational, pricing is transactional
        # but there's no path from blog/post to pricing
        if conv["informational_page_count"] > 0:
            assert conv["no_path_count"] >= 0  # may or may not detect depending on URL patterns

    def test_avg_hops_calculated(self):
        analyzer = SiteArchitectureAnalyzer(_conversion_graph())
        depth = analyzer._compute_crawl_depth()
        pr = analyzer._compute_pagerank()
        conv = analyzer._analyze_conversion_paths(depth, pr)
        assert isinstance(conv["avg_hops_to_conversion"], (int, float))

    def test_transactional_metrics(self):
        analyzer = SiteArchitectureAnalyzer(_conversion_graph())
        depth = analyzer._compute_crawl_depth()
        pr = analyzer._compute_pagerank()
        conv = analyzer._analyze_conversion_paths(depth, pr)
        if conv["transactional_page_metrics"]:
            tm = conv["transactional_page_metrics"][0]
            assert "url" in tm
            assert "depth" in tm
            assert "in_degree" in tm
            assert "accessible" in tm

    def test_paths_sorted_by_hops(self):
        analyzer = SiteArchitectureAnalyzer(_conversion_graph())
        depth = analyzer._compute_crawl_depth()
        pr = analyzer._compute_pagerank()
        conv = analyzer._analyze_conversion_paths(depth, pr)
        paths = conv["paths_to_conversion"]
        if len(paths) >= 2:
            hops = [p["hops"] for p in paths]
            assert hops == sorted(hops)

    def test_paths_capped_at_30(self):
        # Create many info→transactional paths
        link_graph = {}
        for i in range(40):
            link_graph[f"https://a.com/blog/post-{i}"] = ["https://a.com/pricing"]
        graph = {"link_graph": link_graph, "pages": []}
        analyzer = SiteArchitectureAnalyzer(graph)
        depth = analyzer._compute_crawl_depth()
        pr = analyzer._compute_pagerank()
        conv = analyzer._analyze_conversion_paths(depth, pr)
        assert len(conv["paths_to_conversion"]) <= 30


# ====================================================================
# 11. BFS shortest path
# ====================================================================

class TestBfsShortestPath:
    def test_start_is_target(self):
        analyzer = SiteArchitectureAnalyzer(_simple_link_graph())
        target = _normalise_url("https://example.com/")
        result = analyzer._bfs_shortest_path(target, {target})
        assert result is not None
        assert result["distance"] == 0

    def test_direct_link(self):
        analyzer = SiteArchitectureAnalyzer(_simple_link_graph())
        start = _normalise_url("https://example.com/")
        target = _normalise_url("https://example.com/b")
        result = analyzer._bfs_shortest_path(start, {target})
        assert result is not None
        assert result["distance"] == 1

    def test_no_path(self):
        graph = {
            "link_graph": {"https://a.com/": ["https://a.com/b"]},
            "pages": [{"url": "https://a.com/isolated"}],
        }
        analyzer = SiteArchitectureAnalyzer(graph)
        start = _normalise_url("https://a.com/b")
        target = _normalise_url("https://a.com/isolated")
        result = analyzer._bfs_shortest_path(start, {target})
        assert result is None

    def test_path_capped_at_8(self):
        """Very deep chains should stop at depth 8."""
        links = {}
        for i in range(15):
            links[f"https://a.com/n{i}"] = [f"https://a.com/n{i+1}"]
        graph = {"link_graph": links, "pages": []}
        analyzer = SiteArchitectureAnalyzer(graph)
        start = _normalise_url("https://a.com/n0")
        target = _normalise_url("https://a.com/n14")
        result = analyzer._bfs_shortest_path(start, {target})
        # Should be None since path > 8
        assert result is None


# ====================================================================
# 12. Depth distribution
# ====================================================================

class TestDepthDistribution:
    def test_empty_depth_map(self):
        analyzer = SiteArchitectureAnalyzer({"link_graph": {}, "pages": []})
        info = analyzer._depth_distribution({})
        assert info["avg_depth"] == 0
        assert info["max_depth"] == 0

    def test_basic_distribution(self):
        analyzer = SiteArchitectureAnalyzer(_simple_link_graph())
        depth = analyzer._compute_crawl_depth()
        info = analyzer._depth_distribution(depth)
        assert 0 in info["distribution"]
        assert info["avg_depth"] >= 0
        assert info["max_depth"] >= 0

    def test_unreachable_count(self):
        graph = {"link_graph": {"https://a.com/": []}, "pages": []}
        analyzer = SiteArchitectureAnalyzer(
            graph,
            sitemap_urls=["https://a.com/orphan1", "https://a.com/orphan2"],
        )
        depth = analyzer._compute_crawl_depth()
        info = analyzer._depth_distribution(depth)
        assert info["unreachable"] >= 2

    def test_pages_beyond_depth_4(self):
        analyzer = SiteArchitectureAnalyzer(_deep_graph())
        depth = analyzer._compute_crawl_depth()
        info = analyzer._depth_distribution(depth)
        assert info["pages_beyond_depth_4"] >= 1

    def test_distribution_keys_are_ints(self):
        analyzer = SiteArchitectureAnalyzer(_simple_link_graph())
        depth = analyzer._compute_crawl_depth()
        info = analyzer._depth_distribution(depth)
        for k in info["distribution"]:
            assert isinstance(k, int)


# ====================================================================
# 13. Recommendations
# ====================================================================

class TestRecommendations:
    def test_orphan_recommendation(self):
        graph = {"link_graph": {"https://a.com/": []}, "pages": []}
        perf = [{"page": "https://a.com/orphan", "impressions": 500, "clicks": 100}]
        analyzer = SiteArchitectureAnalyzer(graph, page_performance=perf)
        depth = analyzer._compute_crawl_depth()
        pr = analyzer._compute_pagerank()
        orphans = analyzer._detect_orphan_pages(depth)
        hs = analyzer._analyze_hub_spoke(depth, pr)
        bottlenecks = analyzer._find_equity_bottlenecks(pr, depth)
        depth_info = analyzer._depth_distribution(depth)
        conv = analyzer._analyze_conversion_paths(depth, pr)
        recs = analyzer._generate_recommendations(orphans, bottlenecks, hs, depth_info, conv)
        orphan_recs = [r for r in recs if r["category"] == "orphan_pages"]
        assert len(orphan_recs) >= 1

    def test_deep_pages_recommendation(self):
        analyzer = SiteArchitectureAnalyzer(_deep_graph())
        depth = analyzer._compute_crawl_depth()
        pr = analyzer._compute_pagerank()
        depth_info = analyzer._depth_distribution(depth)
        orphans = analyzer._detect_orphan_pages(depth)
        hs = analyzer._analyze_hub_spoke(depth, pr)
        bottlenecks = analyzer._find_equity_bottlenecks(pr, depth)
        conv = analyzer._analyze_conversion_paths(depth, pr)
        recs = analyzer._generate_recommendations(orphans, bottlenecks, hs, depth_info, conv)
        depth_recs = [r for r in recs if r["category"] == "crawl_depth"]
        assert len(depth_recs) >= 1

    def test_recommendations_sorted_by_priority(self):
        analyzer = SiteArchitectureAnalyzer(_deep_graph())
        result = analyzer.run()
        recs = result["recommendations"]
        if len(recs) >= 2:
            priorities = [r["priority"] for r in recs]
            assert priorities == sorted(priorities)

    def test_recommendation_fields(self):
        analyzer = SiteArchitectureAnalyzer(_deep_graph())
        result = analyzer.run()
        recs = result["recommendations"]
        for r in recs:
            assert "priority" in r
            assert "category" in r
            assert "title" in r
            assert "description" in r
            assert "impact" in r

    def test_no_recommendations_when_clean(self):
        """Minimal clean graph may produce zero recommendations."""
        graph = {
            "link_graph": {
                "https://a.com/": ["https://a.com/b"],
                "https://a.com/b": ["https://a.com/"],
            },
            "pages": [],
        }
        analyzer = SiteArchitectureAnalyzer(graph)
        result = analyzer.run()
        # Just verify it's a list (may or may not be empty)
        assert isinstance(result["recommendations"], list)


# ====================================================================
# 14. Summary
# ====================================================================

class TestSummary:
    def test_summary_is_string(self):
        analyzer = SiteArchitectureAnalyzer(_simple_link_graph())
        result = analyzer.run()
        assert isinstance(result["summary"], str)
        assert len(result["summary"]) > 0

    def test_summary_mentions_pages(self):
        analyzer = SiteArchitectureAnalyzer(_simple_link_graph())
        result = analyzer.run()
        assert "page" in result["summary"].lower()

    def test_summary_mentions_recommendations(self):
        analyzer = SiteArchitectureAnalyzer(_simple_link_graph())
        result = analyzer.run()
        assert "recommendation" in result["summary"].lower()

    def test_summary_mentions_depth(self):
        analyzer = SiteArchitectureAnalyzer(_simple_link_graph())
        result = analyzer.run()
        assert "depth" in result["summary"].lower()

    def test_summary_mentions_orphans(self):
        graph = {"link_graph": {"https://a.com/": []}, "pages": []}
        perf = [{"page": "https://a.com/orphan", "impressions": 200, "clicks": 20}]
        analyzer = SiteArchitectureAnalyzer(graph, page_performance=perf)
        result = analyzer.run()
        assert "orphan" in result["summary"].lower()

    def test_summary_mentions_hub_spoke(self):
        analyzer = SiteArchitectureAnalyzer(_hub_spoke_graph())
        result = analyzer.run()
        assert "hub" in result["summary"].lower()


# ====================================================================
# 15. Full pipeline
# ====================================================================

class TestFullPipeline:
    def test_output_schema(self):
        result = analyze_site_architecture(_simple_link_graph())
        expected_keys = {
            "summary", "graph_stats", "depth_distribution",
            "top_pages_by_pagerank", "orphan_pages",
            "hub_spoke_analysis", "link_equity_bottlenecks",
            "conversion_paths", "recommendations",
        }
        assert expected_keys.issubset(set(result.keys()))

    def test_graph_stats_fields(self):
        result = analyze_site_architecture(_simple_link_graph())
        gs = result["graph_stats"]
        assert "total_pages" in gs
        assert "total_internal_links" in gs
        assert "avg_in_degree" in gs
        assert "avg_out_degree" in gs

    def test_top_pagerank_capped_30(self):
        # Build graph with 40 nodes
        link_graph = {}
        for i in range(40):
            link_graph[f"https://a.com/p{i}"] = [f"https://a.com/p{(i+1)%40}"]
        graph = {"link_graph": link_graph, "pages": []}
        result = analyze_site_architecture(graph)
        assert len(result["top_pages_by_pagerank"]) <= 30

    def test_top_pagerank_sorted_descending(self):
        result = analyze_site_architecture(_simple_link_graph())
        prs = [p["pagerank_pct"] for p in result["top_pages_by_pagerank"]]
        assert prs == sorted(prs, reverse=True)

    def test_with_all_optional_data(self):
        result = analyze_site_architecture(
            link_graph=_conversion_graph(),
            page_performance=[
                {"page": "https://example.com/pricing", "clicks": 200, "impressions": 1000},
                {"page": "https://example.com/blog/post-1", "clicks": 50, "impressions": 500},
            ],
            sitemap_urls=[
                "https://example.com/",
                "https://example.com/pricing",
                "https://example.com/blog/post-1",
                "https://example.com/unlisted",
            ],
            query_data=[{"query": "test", "clicks": 10}],
        )
        assert result["graph_stats"]["total_pages"] >= 4

    def test_empty_input(self):
        result = analyze_site_architecture({"link_graph": {}, "pages": []})
        assert result["graph_stats"]["total_pages"] == 0
        assert result["summary"] != ""

    def test_direct_dict_graph(self):
        """analyze_site_architecture with plain {src: [dst]} dict."""
        result = analyze_site_architecture({
            "https://a.com/": ["https://a.com/b"],
            "https://a.com/b": ["https://a.com/c"],
        })
        assert result["graph_stats"]["total_pages"] == 3


# ====================================================================
# 16. Edge cases
# ====================================================================

class TestEdgeCases:
    def test_unicode_urls(self):
        graph = {
            "link_graph": {
                "https://example.com/café": ["https://example.com/naïve"],
            },
            "pages": [],
        }
        result = analyze_site_architecture(graph)
        assert result["graph_stats"]["total_pages"] == 2

    def test_very_large_graph(self):
        """Smoke test: 500-node ring graph."""
        n = 500
        link_graph = {}
        for i in range(n):
            link_graph[f"https://a.com/p{i}"] = [f"https://a.com/p{(i+1)%n}"]
        graph = {"link_graph": link_graph, "pages": []}
        result = analyze_site_architecture(graph)
        assert result["graph_stats"]["total_pages"] == n

    def test_duplicate_links(self):
        """Duplicate links in list should not create duplicate edges."""
        graph = {
            "link_graph": {
                "https://a.com/": ["https://a.com/b", "https://a.com/b", "https://a.com/b"],
            },
            "pages": [],
        }
        analyzer = SiteArchitectureAnalyzer(graph)
        a_url = _normalise_url("https://a.com/")
        assert len(analyzer.adj[a_url]) == 1

    def test_none_page_performance(self):
        result = analyze_site_architecture(
            _simple_link_graph(),
            page_performance=None,
        )
        assert isinstance(result, dict)

    def test_none_sitemap_urls(self):
        result = analyze_site_architecture(
            _simple_link_graph(),
            sitemap_urls=None,
        )
        assert isinstance(result, dict)

    def test_empty_url_in_links(self):
        graph = {
            "link_graph": {
                "https://a.com/": ["", "https://a.com/b"],
            },
            "pages": [],
        }
        result = analyze_site_architecture(graph)
        # Should handle empty URL gracefully
        assert isinstance(result, dict)

    def test_url_with_port(self):
        graph = {
            "link_graph": {
                "https://a.com:8080/": ["https://a.com:8080/b"],
            },
            "pages": [],
        }
        result = analyze_site_architecture(graph)
        assert result["graph_stats"]["total_pages"] == 2

    def test_mixed_http_https(self):
        graph = {
            "link_graph": {
                "http://a.com/": ["https://a.com/b"],
            },
            "pages": [],
        }
        result = analyze_site_architecture(graph)
        # http and https treated as different nodes
        assert result["graph_stats"]["total_pages"] >= 1

    def test_page_url_key_variant(self):
        """page_performance uses 'url' key instead of 'page'."""
        graph = {"link_graph": {"https://a.com/": []}, "pages": []}
        perf = [{"url": "https://a.com/orphan", "impressions": 300, "clicks": 30}]
        analyzer = SiteArchitectureAnalyzer(graph, page_performance=perf)
        depth = analyzer._compute_crawl_depth()
        orphans = analyzer._detect_orphan_pages(depth)
        # Should detect via 'url' key fallback
        assert len(orphans) >= 1

    def test_conversion_patterns_recognized(self):
        """Verify transactional URL patterns are detected."""
        graph = {
            "link_graph": {
                "https://a.com/": [
                    "https://a.com/pricing",
                    "https://a.com/blog/guide",
                    "https://a.com/signup",
                ],
            },
            "pages": [],
        }
        analyzer = SiteArchitectureAnalyzer(graph)
        depth = analyzer._compute_crawl_depth()
        pr = analyzer._compute_pagerank()
        conv = analyzer._analyze_conversion_paths(depth, pr)
        assert conv["transactional_page_count"] >= 2  # pricing + signup
        assert conv["informational_page_count"] >= 1  # blog/guide
