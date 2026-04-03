"""
Demo report data — comprehensive sample data for all 12 analysis modules.

Serves the ``GET /api/reports/demo`` endpoint so visitors can preview what
the Search Intelligence Report looks like before connecting their own
GSC + GA4 data.  This is the front-door conversion mechanism for the
consulting business.

The data mirrors what the real pipeline produces (report_data.sections.*)
using a fictional "Acme Widgets" e-commerce site.
"""

from datetime import datetime, timedelta
import random


def _date(days_ago: int) -> str:
    """Return ISO date string for N days ago."""
    return (datetime.utcnow() - timedelta(days=days_ago)).strftime("%Y-%m-%d")


def _datetime(days_ago: int) -> str:
    return (datetime.utcnow() - timedelta(days=days_ago)).isoformat() + "Z"


def get_demo_report() -> dict:
    """Return a complete demo report matching the real pipeline output schema."""
    now = datetime.utcnow().isoformat() + "Z"

    # --- Module 1: Health & Trajectory ---
    daily_metrics = []
    base_clicks = 850
    for i in range(365, -1, -1):
        day_of_week = (365 - i) % 7
        seasonal = 1.0 + 0.08 * (1 if day_of_week in (1, 2) else -0.3 if day_of_week >= 5 else 0)
        trend = 1.0 - 0.002 * (365 - i) / 30  # slow decline
        noise = random.uniform(0.85, 1.15)
        clicks = int(base_clicks * seasonal * trend * noise)
        impressions = int(clicks / random.uniform(0.025, 0.045))
        daily_metrics.append({
            "date": _date(i),
            "clicks": max(clicks, 50),
            "impressions": impressions,
            "ctr": round(clicks / max(impressions, 1), 4),
            "position": round(random.uniform(8.5, 14.2), 1),
        })

    health_trajectory = {
        "overall_direction": "declining",
        "trend_slope_pct_per_month": -2.3,
        "change_points": [
            {"date": _date(180), "magnitude": -0.12, "direction": "drop",
             "attributed_to": "November 2025 Core Update"},
            {"date": _date(90), "magnitude": 0.08, "direction": "recovery",
             "attributed_to": None},
            {"date": _date(30), "magnitude": -0.06, "direction": "drop",
             "attributed_to": "March 2026 Spam Update"},
        ],
        "seasonality": {
            "best_day": "Tuesday",
            "worst_day": "Saturday",
            "monthly_cycle": True,
            "cycle_description": "15% traffic spike first week of each month",
            "weekly_pattern": {
                "Monday": 0.95, "Tuesday": 1.08, "Wednesday": 1.02,
                "Thursday": 0.98, "Friday": 0.92, "Saturday": 0.78, "Sunday": 0.82,
            },
        },
        "anomalies": [
            {"date": _date(100), "type": "discord", "magnitude": -0.45, "description": "Holiday drop"},
            {"date": _date(45), "type": "motif", "magnitude": 0.32, "description": "Recurring monthly spike"},
        ],
        "forecast": {
            "30d": {"clicks": 12400, "ci_low": 11200, "ci_high": 13600},
            "60d": {"clicks": 11800, "ci_low": 10100, "ci_high": 13500},
            "90d": {"clicks": 11200, "ci_low": 9000, "ci_high": 13400},
        },
        "daily_metrics": daily_metrics,
    }

    # --- Module 2: Page Triage ---
    page_triage = {
        "pages": [
            {"url": "/best-industrial-widgets", "current_clicks": 2800, "trend_slope": 0.03,
             "bucket": "growing", "engagement_rate": 0.72, "avg_ctr": 0.045,
             "avg_position": 4.2, "impressions": 62000, "priority_score": 92},
            {"url": "/widget-size-guide", "current_clicks": 1900, "trend_slope": -0.001,
             "bucket": "stable", "engagement_rate": 0.68, "avg_ctr": 0.038,
             "avg_position": 6.1, "impressions": 50000, "priority_score": 78},
            {"url": "/compare-widget-brands", "current_clicks": 1200, "trend_slope": -0.04,
             "bucket": "decaying", "engagement_rate": 0.45, "avg_ctr": 0.022,
             "avg_position": 11.3, "impressions": 55000, "priority_score": 85},
            {"url": "/widget-maintenance-tips", "current_clicks": 800, "trend_slope": -0.06,
             "bucket": "critical", "engagement_rate": 0.35, "avg_ctr": 0.015,
             "avg_position": 15.7, "impressions": 53000, "priority_score": 95},
            {"url": "/buy-widgets-online", "current_clicks": 3200, "trend_slope": 0.02,
             "bucket": "growing", "engagement_rate": 0.78, "avg_ctr": 0.052,
             "avg_position": 3.8, "impressions": 61500, "priority_score": 88},
            {"url": "/widget-installation-guide", "current_clicks": 600, "trend_slope": -0.08,
             "bucket": "critical", "engagement_rate": 0.30, "avg_ctr": 0.012,
             "avg_position": 18.2, "impressions": 50000, "priority_score": 97},
            {"url": "/widget-accessories", "current_clicks": 1500, "trend_slope": 0.01,
             "bucket": "stable", "engagement_rate": 0.62, "avg_ctr": 0.034,
             "avg_position": 7.5, "impressions": 44000, "priority_score": 65},
            {"url": "/widget-reviews-2026", "current_clicks": 2100, "trend_slope": 0.05,
             "bucket": "growing", "engagement_rate": 0.71, "avg_ctr": 0.041,
             "avg_position": 5.1, "impressions": 51000, "priority_score": 70},
        ],
        "ctr_anomaly_summary": {
            "underperforming": 12,
            "normal": 45,
            "overperforming": 8,
        },
        "category_distribution": {
            "growing": 3, "stable": 2, "decaying": 1, "critical": 2,
        },
    }

    # --- Module 3: SERP Landscape ---
    serp_landscape = {
        "competitor_map": [
            {"domain": "widgetworld.com", "overlap_queries": 45, "avg_position": 3.2,
             "visibility_score": 0.78, "threat_level": "high"},
            {"domain": "megawidgets.io", "overlap_queries": 38, "avg_position": 5.1,
             "visibility_score": 0.62, "threat_level": "medium"},
            {"domain": "widgetsdirect.com", "overlap_queries": 29, "avg_position": 7.3,
             "visibility_score": 0.45, "threat_level": "medium"},
            {"domain": "cheapwidgets.net", "overlap_queries": 22, "avg_position": 9.8,
             "visibility_score": 0.31, "threat_level": "low"},
        ],
        "serp_feature_displacement": [
            {"query": "best widgets 2026", "feature": "ai_overview",
             "position_before": 1, "position_after": 3, "estimated_ctr_impact": -0.10,
             "impressions": 12000},
            {"query": "widget size chart", "feature": "featured_snippet",
             "position_before": 2, "position_after": 4, "estimated_ctr_impact": -0.08,
             "impressions": 8500},
            {"query": "how to install widgets", "feature": "video_carousel",
             "position_before": 1, "position_after": 2, "estimated_ctr_impact": -0.05,
             "impressions": 6200},
        ],
        "serp_feature_summary": {
            "feature_prevalence": {
                "ai_overview": 0.42,
                "featured_snippet": 0.28,
                "people_also_ask": 0.65,
                "video_carousel": 0.18,
                "local_pack": 0.12,
                "shopping_results": 0.35,
                "knowledge_panel": 0.08,
                "top_stories": 0.05,
            },
        },
        "intent_analysis": {
            "intent_distribution": {
                "informational": 0.38,
                "commercial": 0.32,
                "transactional": 0.22,
                "navigational": 0.08,
            },
            "intent_mismatches": [
                {"query": "widget price comparison", "page_intent": "informational",
                 "serp_intent": "commercial", "impressions": 4500},
                {"query": "buy widgets near me", "page_intent": "informational",
                 "serp_intent": "transactional", "impressions": 3200},
            ],
        },
    }

    # --- Module 4: Content Intelligence ---
    content_intelligence = {
        "cannibalization_clusters": [
            {"cluster_name": "widget buying guides",
             "queries": ["best widgets to buy", "top rated widgets", "widget buying guide"],
             "competing_pages": ["/best-industrial-widgets", "/widget-reviews-2026", "/buy-widgets-online"],
             "impressions_lost": 4200, "severity": "high"},
            {"cluster_name": "widget sizing",
             "queries": ["widget size guide", "what size widget do i need"],
             "competing_pages": ["/widget-size-guide", "/widget-accessories"],
             "impressions_lost": 1800, "severity": "medium"},
        ],
        "striking_distance": [
            {"query": "industrial widget specifications", "page": "/widget-size-guide",
             "current_position": 11.2, "impressions": 8500, "potential_clicks": 680,
             "intent": "informational", "difficulty": "low"},
            {"query": "widget comparison chart", "page": "/compare-widget-brands",
             "current_position": 12.8, "impressions": 6200, "potential_clicks": 450,
             "intent": "commercial", "difficulty": "medium"},
            {"query": "affordable widgets online", "page": "/buy-widgets-online",
             "current_position": 13.5, "impressions": 5800, "potential_clicks": 390,
             "intent": "transactional", "difficulty": "medium"},
        ],
        "thin_content": [
            {"url": "/widget-faq", "word_count": 280, "clicks": 45, "impressions": 3200,
             "recommendation": "Expand to 1500+ words with detailed Q&A sections"},
            {"url": "/widget-glossary", "word_count": 190, "clicks": 12, "impressions": 1100,
             "recommendation": "Merge into widget-size-guide or expand significantly"},
        ],
        "content_age_matrix": {
            "fresh_growing": 3, "fresh_stable": 5, "fresh_declining": 2,
            "stale_growing": 1, "stale_stable": 8, "stale_declining": 6,
        },
    }

    # --- Module 5: Gameplan ---
    gameplan = {
        "critical": [
            {"title": "Fix cannibalization: widget buying guides cluster",
             "description": "Three pages compete for the same buying-intent keywords, splitting authority. Consolidate /best-industrial-widgets and /widget-reviews-2026 into a single definitive guide.",
             "impact_monthly_clicks": 4200, "effort": "medium", "timeframe": "this_week",
             "category": ["content_consolidation"],
             "specific_tasks": [
                 "301 redirect /widget-reviews-2026 to /best-industrial-widgets",
                 "Merge unique content from both pages",
                 "Update internal links site-wide",
                 "Request re-indexing via GSC",
             ]},
            {"title": "Rescue 2 critical-decay pages before they drop off page 2",
             "description": "/widget-maintenance-tips and /widget-installation-guide are losing 6-8% traffic per month. Both need content refreshes and internal link boosts.",
             "impact_monthly_clicks": 1400, "effort": "medium", "timeframe": "this_week",
             "category": ["content_refresh"],
             "specific_tasks": [
                 "Update both pages with 2026-relevant information",
                 "Add internal links from top-performing pages",
                 "Improve title tags and meta descriptions",
                 "Add FAQ schema markup",
             ]},
        ],
        "quick_wins": [
            {"title": "Push 3 striking-distance keywords to page 1",
             "description": "Three high-impression keywords sit at positions 11-14. Minor on-page optimization could push them into the top 10 for an estimated 1,520 additional monthly clicks.",
             "impact_monthly_clicks": 1520, "effort": "low", "timeframe": "this_week",
             "category": ["on_page_seo"],
             "specific_tasks": [
                 "Optimize title tags to include exact-match keywords",
                 "Add keyword variations to H2/H3 headings",
                 "Build 2-3 internal links from authoritative pages",
             ]},
            {"title": "Fix CTR underperformers with better title tags",
             "description": "12 keywords have CTR significantly below position-expected rates. Rewriting title tags and meta descriptions could recover 800+ monthly clicks.",
             "impact_monthly_clicks": 800, "effort": "low", "timeframe": "this_week",
             "category": ["ctr_optimization"],
             "specific_tasks": [
                 "A/B test new title tags for top 5 underperforming pages",
                 "Add power words and numbers to meta descriptions",
                 "Include current year in seasonal content titles",
             ]},
        ],
        "strategic": [
            {"title": "Counter AI Overview displacement on 8 high-value keywords",
             "description": "AI Overviews appear on 42% of your tracked keywords, pushing organic results down. Create concise, authoritative answer content to earn featured snippet placement above the AI Overview.",
             "impact_monthly_clicks": 3500, "effort": "high", "timeframe": "this_month",
             "category": ["ai_overview_defense"],
             "specific_tasks": [
                 "Add concise FAQ sections to top 8 affected pages",
                 "Implement HowTo and FAQ schema markup",
                 "Create video content for video carousel opportunities",
                 "Build topical authority clusters around core topics",
             ]},
        ],
        "structural": [
            {"title": "Build comparison tool to capture commercial intent traffic",
             "description": "Competitors rank for 'compare [widget A] vs [widget B]' queries with interactive tools. Building a comparison feature could capture 2,800 monthly clicks in an underserved segment.",
             "impact_monthly_clicks": 2800, "effort": "high", "timeframe": "this_quarter",
             "category": ["new_feature"],
             "specific_tasks": [
                 "Build interactive widget comparison tool",
                 "Target 'compare [brand] vs [brand]' queries",
                 "Add SoftwareApplication schema markup",
                 "Promote from existing blog content",
             ]},
        ],
    }

    # --- Module 6: Algorithm Impact ---
    algorithm_impact = {
        "algorithm_correlations": [
            {"update_name": "November 2025 Core Update", "update_date": _date(180),
             "update_type": "core", "traffic_change_pct": -12.3,
             "confidence": 0.87, "affected_pages": 23,
             "description": "Strong correlation between core update rollout and traffic decline. Pages with thin content and poor E-E-A-T signals were most affected."},
            {"update_name": "March 2026 Spam Update", "update_date": _date(30),
             "update_type": "spam", "traffic_change_pct": -6.1,
             "confidence": 0.72, "affected_pages": 8,
             "description": "Moderate correlation. Affiliate-heavy pages with external links saw the largest declines."},
        ],
        "uncorrelated_changes": [
            {"date": _date(90), "traffic_change_pct": 8.2, "direction": "increase",
             "possible_causes": ["Competitor site went down for 3 days", "Seasonal demand spike", "Internal link restructure took effect"]},
        ],
        "vulnerability_score": {
            "overall": 62,
            "core_update_risk": 68,
            "spam_risk": 35,
            "helpful_content_risk": 55,
            "description": "Moderate vulnerability. E-E-A-T improvements and content quality upgrades would reduce core update exposure.",
        },
        "timeline_data": [
            {"date": _date(i), "clicks": int(850 * (1.0 - 0.002 * (365 - i) / 30) * random.uniform(0.9, 1.1))}
            for i in range(365, -1, -30)
        ],
    }

    # --- Module 7: Intent Migration ---
    intent_migration = {
        "portfolio_distribution": {
            "dominant_intent": "informational",
            "recent_distribution": {
                "informational": 0.38, "commercial": 0.32,
                "transactional": 0.22, "navigational": 0.08,
            },
            "previous_distribution": {
                "informational": 0.45, "commercial": 0.28,
                "transactional": 0.18, "navigational": 0.09,
            },
            "changes_by_intent": {
                "informational": {"change_pct": -7, "direction": "decreasing"},
                "commercial": {"change_pct": 4, "direction": "increasing"},
                "transactional": {"change_pct": 4, "direction": "increasing"},
                "navigational": {"change_pct": -1, "direction": "stable"},
            },
        },
        "intent_shifts": [
            {"query": "widget reviews", "previous_intent": "informational",
             "current_intent": "commercial", "impressions": 5200,
             "description": "Google now shows product cards and shopping results for this query"},
            {"query": "how widgets work", "previous_intent": "informational",
             "current_intent": "informational", "impressions": 3800,
             "description": "AI Overview now handles this query — organic CTR dropped 40%"},
        ],
        "emerging_intents": [
            {"intent": "AI-assisted widget selection",
             "queries": ["ai widget recommender", "smart widget finder"],
             "growth_rate": 0.85, "current_volume": 2400},
            {"intent": "Sustainable widgets",
             "queries": ["eco-friendly widgets", "recyclable widget materials"],
             "growth_rate": 0.62, "current_volume": 1800},
        ],
        "content_alignment": [
            {"query": "best widget for small spaces", "page": "/widget-size-guide",
             "dominant_intent": "commercial", "page_intent": "informational",
             "recommendation": "Add product recommendations and comparison table"},
        ],
    }

    # --- Module 8: CTR Modeling ---
    ctr_modeling = {
        "ctr_model_accuracy": 0.78,
        "keyword_ctr_analysis": [
            {"query": "buy widgets online", "position": 3, "actual_ctr": 0.082,
             "expected_ctr_generic": 0.110, "expected_ctr_contextual": 0.065,
             "serp_features": ["shopping_results", "ai_overview"], "impressions": 12000},
            {"query": "best industrial widgets", "position": 1, "actual_ctr": 0.18,
             "expected_ctr_generic": 0.280, "expected_ctr_contextual": 0.155,
             "serp_features": ["featured_snippet", "people_also_ask"], "impressions": 8500},
            {"query": "widget size chart", "position": 4, "actual_ctr": 0.095,
             "expected_ctr_generic": 0.080, "expected_ctr_contextual": 0.072,
             "serp_features": ["people_also_ask"], "impressions": 6200},
            {"query": "widget installation guide", "position": 7, "actual_ctr": 0.015,
             "expected_ctr_generic": 0.038, "expected_ctr_contextual": 0.032,
             "serp_features": ["video_carousel", "featured_snippet"], "impressions": 5800},
            {"query": "widget maintenance tips", "position": 12, "actual_ctr": 0.008,
             "expected_ctr_generic": 0.010, "expected_ctr_contextual": 0.009,
             "serp_features": ["people_also_ask"], "impressions": 4200},
            {"query": "compare widget brands", "position": 6, "actual_ctr": 0.028,
             "expected_ctr_generic": 0.047, "expected_ctr_contextual": 0.035,
             "serp_features": ["shopping_results"], "impressions": 5500},
        ],
        "feature_opportunities": [
            {"feature": "featured_snippet", "query": "what is a widget",
             "current_position": 3, "snippet_opportunity_score": 0.85,
             "estimated_ctr_gain": 0.12, "impressions": 9200},
            {"feature": "video_carousel", "query": "how to install widgets",
             "current_position": 2, "snippet_opportunity_score": 0.72,
             "estimated_ctr_gain": 0.08, "impressions": 6200},
        ],
        "contextual_ctr_benchmarks": {
            "no_features": {"position_1": 0.310, "position_3": 0.125, "position_5": 0.075},
            "with_ai_overview": {"position_1": 0.180, "position_3": 0.065, "position_5": 0.040},
            "with_featured_snippet": {"position_1": 0.220, "position_3": 0.090, "position_5": 0.055},
        },
    }

    # --- Module 9: Site Architecture ---
    site_architecture = {
        "pagerank_scores": [
            {"url": "/", "pagerank": 1.0, "internal_links_in": 85, "internal_links_out": 24},
            {"url": "/best-industrial-widgets", "pagerank": 0.72, "internal_links_in": 18, "internal_links_out": 12},
            {"url": "/buy-widgets-online", "pagerank": 0.68, "internal_links_in": 15, "internal_links_out": 10},
            {"url": "/widget-size-guide", "pagerank": 0.45, "internal_links_in": 8, "internal_links_out": 7},
            {"url": "/compare-widget-brands", "pagerank": 0.38, "internal_links_in": 5, "internal_links_out": 9},
            {"url": "/widget-accessories", "pagerank": 0.32, "internal_links_in": 4, "internal_links_out": 6},
        ],
        "orphaned_pages": [
            {"url": "/widget-clearance-sale", "pagerank": 0.02, "internal_links_in": 0,
             "recommendation": "Add internal links from /buy-widgets-online and category pages"},
            {"url": "/old-widget-catalog-2024", "pagerank": 0.01, "internal_links_in": 0,
             "recommendation": "301 redirect to current catalog or add contextual links"},
        ],
        "hub_authority_scores": [
            {"url": "/best-industrial-widgets", "hub_score": 0.82, "authority_score": 0.91},
            {"url": "/", "hub_score": 0.95, "authority_score": 0.75},
            {"url": "/buy-widgets-online", "hub_score": 0.45, "authority_score": 0.88},
        ],
        "link_equity_flow": {
            "total_internal_links": 342,
            "avg_links_per_page": 8.2,
            "max_depth": 4,
            "pages_at_depth": {"1": 8, "2": 15, "3": 12, "4": 7},
        },
        "network_graph": {
            "nodes": [
                {"id": "/", "label": "Home", "pagerank": 1.0, "group": "hub"},
                {"id": "/best-industrial-widgets", "label": "Best Widgets", "pagerank": 0.72, "group": "content"},
                {"id": "/buy-widgets-online", "label": "Buy Widgets", "pagerank": 0.68, "group": "commercial"},
                {"id": "/widget-size-guide", "label": "Size Guide", "pagerank": 0.45, "group": "content"},
                {"id": "/compare-widget-brands", "label": "Compare", "pagerank": 0.38, "group": "content"},
                {"id": "/widget-accessories", "label": "Accessories", "pagerank": 0.32, "group": "commercial"},
            ],
            "edges": [
                {"source": "/", "target": "/best-industrial-widgets"},
                {"source": "/", "target": "/buy-widgets-online"},
                {"source": "/", "target": "/widget-size-guide"},
                {"source": "/best-industrial-widgets", "target": "/buy-widgets-online"},
                {"source": "/best-industrial-widgets", "target": "/compare-widget-brands"},
                {"source": "/widget-size-guide", "target": "/widget-accessories"},
                {"source": "/buy-widgets-online", "target": "/widget-accessories"},
                {"source": "/compare-widget-brands", "target": "/buy-widgets-online"},
            ],
        },
    }

    # --- Module 10: Branded Split ---
    branded_split = {
        "segments": {
            "branded": {"clicks": 4200, "impressions": 28000, "ctr": 0.15, "avg_position": 1.8},
            "non_branded": {"clicks": 8900, "impressions": 285000, "ctr": 0.031, "avg_position": 12.4},
        },
        "branded_pct": 32.1,
        "brand_dependency": {
            "branded_click_share_pct": 32.1,
            "risk_level": "moderate",
            "description": "Moderate brand dependency. Non-branded traffic is the primary growth driver.",
        },
        "trends": {
            "data_points": [
                {"date": _date(360), "branded_clicks": 380, "non_branded_clicks": 720, "branded_click_share_pct": 34.5},
                {"date": _date(300), "branded_clicks": 400, "non_branded_clicks": 780, "branded_click_share_pct": 33.9},
                {"date": _date(240), "branded_clicks": 390, "non_branded_clicks": 810, "branded_click_share_pct": 32.5},
                {"date": _date(180), "branded_clicks": 370, "non_branded_clicks": 760, "branded_click_share_pct": 32.7},
                {"date": _date(120), "branded_clicks": 360, "non_branded_clicks": 740, "branded_click_share_pct": 32.7},
                {"date": _date(60), "branded_clicks": 350, "non_branded_clicks": 730, "branded_click_share_pct": 32.4},
                {"date": _date(0), "branded_clicks": 340, "non_branded_clicks": 710, "branded_click_share_pct": 32.4},
            ],
            "trend": {
                "branded_share_change_pp": -2.1,
                "non_branded_click_growth_pct": -1.4,
            },
        },
        "top_branded_queries": [
            {"query": "acme widgets", "clicks": 1800, "impressions": 12000, "position": 1.0},
            {"query": "acme widget store", "clicks": 900, "impressions": 6500, "position": 1.2},
            {"query": "acmewidgets.com", "clicks": 650, "impressions": 4200, "position": 1.0},
        ],
        "top_non_branded_queries": [
            {"query": "best industrial widgets", "clicks": 2800, "impressions": 62000, "position": 4.2},
            {"query": "buy widgets online", "clicks": 1900, "impressions": 38000, "position": 3.8},
            {"query": "widget size chart", "clicks": 1200, "impressions": 31000, "position": 6.1},
        ],
        "non_branded_opportunities": [
            {"query": "affordable widgets 2026", "impressions": 9200, "current_position": 14.5,
             "potential_clicks": 420, "difficulty": "medium"},
            {"query": "eco-friendly widgets", "impressions": 7800, "current_position": 18.2,
             "potential_clicks": 310, "difficulty": "low"},
        ],
    }

    # --- Module 11: Competitive Threats ---
    competitive_threats = {
        "competitors": [
            {"domain": "widgetworld.com", "overlap_score": 0.78, "threat_level": "high",
             "overlap_queries": 45, "avg_position_delta": -2.8,
             "dimensions": {"content_depth": 85, "backlink_strength": 72,
                            "serp_features": 68, "content_freshness": 90, "keyword_coverage": 80}},
            {"domain": "megawidgets.io", "overlap_score": 0.62, "threat_level": "medium",
             "overlap_queries": 38, "avg_position_delta": -1.2,
             "dimensions": {"content_depth": 70, "backlink_strength": 65,
                            "serp_features": 55, "content_freshness": 75, "keyword_coverage": 68}},
            {"domain": "widgetsdirect.com", "overlap_score": 0.45, "threat_level": "medium",
             "overlap_queries": 29, "avg_position_delta": 1.5,
             "dimensions": {"content_depth": 55, "backlink_strength": 80,
                            "serp_features": 40, "content_freshness": 50, "keyword_coverage": 52}},
        ],
        "competitive_dimensions": [
            "content_depth", "backlink_strength", "serp_features",
            "content_freshness", "keyword_coverage",
        ],
        "your_scores": {
            "content_depth": 65, "backlink_strength": 58,
            "serp_features": 45, "content_freshness": 60, "keyword_coverage": 72,
        },
        "competitive_pressure": [
            {"cluster": "buying guides", "your_position": 4.2,
             "best_competitor_position": 1.8, "competitor": "widgetworld.com",
             "queries": 12, "pressure_level": "high"},
            {"cluster": "technical specs", "your_position": 6.1,
             "best_competitor_position": 3.5, "competitor": "megawidgets.io",
             "queries": 8, "pressure_level": "medium"},
        ],
    }

    # --- Module 12: Revenue Attribution ---
    revenue_attribution = {
        "summary": {
            "total_estimated_revenue": 28500,
            "total_potential_revenue": 42000,
            "revenue_gap": 13500,
        },
        "total_potential_value": 42000,
        "revenue_by_page": [
            {"page": "/buy-widgets-online", "estimated_revenue": 12500, "clicks": 3200,
             "conversion_rate": 0.035, "avg_order_value": 112},
            {"page": "/best-industrial-widgets", "estimated_revenue": 8200, "clicks": 2800,
             "conversion_rate": 0.028, "avg_order_value": 105},
            {"page": "/widget-accessories", "estimated_revenue": 4800, "clicks": 1500,
             "conversion_rate": 0.042, "avg_order_value": 76},
            {"page": "/compare-widget-brands", "estimated_revenue": 3000, "clicks": 1200,
             "conversion_rate": 0.025, "avg_order_value": 100},
        ],
        "top_converting_queries": [
            {"query": "buy widgets online", "revenue": 8500, "conversions": 78, "cpa": 0.45},
            {"query": "industrial widget kit", "revenue": 4200, "conversions": 35, "cpa": 0.62},
            {"query": "widget starter pack", "revenue": 2800, "conversions": 28, "cpa": 0.38},
        ],
        "revenue_at_risk": [
            {"page": "/widget-maintenance-tips", "revenue_at_risk": 2400,
             "decline_rate_pct": -6.2, "months_until_critical": 4},
            {"page": "/compare-widget-brands", "revenue_at_risk": 1800,
             "decline_rate_pct": -4.1, "months_until_critical": 6},
        ],
        "position_improvement_roi": [
            {"query": "best widgets 2026", "current_position": 4,
             "target_position": 1, "additional_revenue": 3200,
             "confidence": 0.72},
            {"query": "widget comparison", "current_position": 11,
             "target_position": 5, "additional_revenue": 1800,
             "confidence": 0.65},
        ],
        "conversion_funnel": {
            "total_impressions": 385000,
            "total_clicks": 13100,
            "total_conversions": 458,
            "total_revenue": 28500,
        },
        "revenue_concentration": {
            "top_5_pages_pct": 78.2,
            "top_10_pages_pct": 92.5,
        },
        "data_quality": {
            "pages_analyzed": 42,
            "queries_analyzed": 185,
            "confidence_level": "medium",
            "note": "Revenue estimates based on GSC click data and industry conversion benchmarks. Connect GA4 e-commerce tracking for precise attribution.",
        },
    }

    # -----------------------------------------------------------------------
    # Assemble full report
    # -----------------------------------------------------------------------
    return {
        "id": "demo",
        "user_id": "demo",
        "domain": "acmewidgets.com",
        "gsc_property": "sc-domain:acmewidgets.com",
        "ga4_property": "properties/123456789",
        "status": "complete",
        "created_at": _datetime(1),
        "completed_at": now,
        "current_module": 12,
        "progress": {},
        "error_message": None,
        "report_data": {
            "metadata": {
                "domain": "acmewidgets.com",
                "date_range": {"start": _date(365), "end": _date(0)},
                "total_queries": 2850,
                "total_pages": 42,
                "modules_completed": 12,
                "modules_failed": 0,
                "generation_time_seconds": 142.5,
            },
            "sections": {
                "health_trajectory": {
                    "status": "success",
                    "execution_time_seconds": 3.2,
                    "data": health_trajectory,
                },
                "page_triage": {
                    "status": "success",
                    "execution_time_seconds": 2.1,
                    "data": page_triage,
                },
                "serp_landscape": {
                    "status": "success",
                    "execution_time_seconds": 8.5,
                    "data": serp_landscape,
                },
                "content_intelligence": {
                    "status": "success",
                    "execution_time_seconds": 4.3,
                    "data": content_intelligence,
                },
                "gameplan": {
                    "status": "success",
                    "execution_time_seconds": 12.8,
                    "data": gameplan,
                },
                "algorithm_impact": {
                    "status": "success",
                    "execution_time_seconds": 2.8,
                    "data": algorithm_impact,
                },
                "intent_migration": {
                    "status": "success",
                    "execution_time_seconds": 3.5,
                    "data": intent_migration,
                },
                "technical_health": {
                    "status": "success",
                    "execution_time_seconds": 5.2,
                    "data": ctr_modeling,
                },
                "site_architecture": {
                    "status": "success",
                    "execution_time_seconds": 15.3,
                    "data": site_architecture,
                },
                "branded_split": {
                    "status": "success",
                    "execution_time_seconds": 1.8,
                    "data": branded_split,
                },
                "competitive_threats": {
                    "status": "success",
                    "execution_time_seconds": 7.2,
                    "data": competitive_threats,
                },
                "revenue_attribution": {
                    "status": "success",
                    "execution_time_seconds": 4.1,
                    "data": revenue_attribution,
                },
            },
            "errors": [],
        },
    }


def get_demo_modules() -> dict:
    """Return demo module results in the progressive-rendering format."""
    report = get_demo_report()
    sections = report["report_data"]["sections"]
    modules = {}
    for key, section in sections.items():
        modules[key] = {
            "status": "success",
            "data": section["data"],
        }
    return {
        "report_id": "demo",
        "status": "complete",
        "domain": "acmewidgets.com",
        "modules": modules,
    }
