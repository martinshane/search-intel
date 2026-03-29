# Search Intelligence Report — Technical Architecture Spec

## Overview

A free web-based tool that generates a comprehensive "Search Intelligence Report" for any site connected via GSC + GA4 OAuth. The report combines 12 integrated analysis sections that progressively build from raw data → statistical analysis → cross-dataset correlation → predictive modeling → prioritized action plan. Positioned as the front door to a search consulting business.

**Core thesis:** The moat is computational complexity. Every section requires orchestrating multiple APIs, applying real statistical/ML techniques, and synthesizing cross-dataset insights. This is not viably reproducible via vibe-coding.

---

## System Architecture

### Stack

| Layer | Technology | Notes |
|-------|-----------|-------|
| Frontend | React (Next.js) or standalone React app | Report UI with interactive charts, network graphs, expandable sections |
| Backend API | Python (FastAPI) on Railway | All heavy computation lives here |
| Data Store | Supabase (PostgreSQL) | Cache API responses, store generated reports, user OAuth tokens |
| Job Queue | Supabase + Railway cron or Celery/Redis | Report generation takes 2-5 min; must be async |
| Auth | Google OAuth 2.0 | GSC + GA4 read-only scopes |
| External APIs | Google Search Console API, GA4 Data API, DataForSEO API | Primary data sources |
| ML/Stats | Python: scikit-learn, PyOD, STUMPY, statsmodels, networkx, sentence-transformers | Analysis engines |

### Data Flow

```
User connects GSC + GA4 via OAuth
            │
            ▼
    ┌───────────────┐
    │  Data Ingestion │  ← Pull 12-16 months of GSC + GA4 data
    │  (async job)    │  ← Pull live SERP data for top keywords via DataForSEO
    │                 │  ← Crawl site for internal link graph (Scrapy or custom)
    └───────┬────────┘
            │
            ▼
    ┌───────────────┐
    │  Analysis      │  ← 12 analysis modules run in sequence
    │  Pipeline      │  ← Each module reads from shared data store
    │                │  ← Each writes structured results to report JSON
    └───────┬────────┘
            │
            ▼
    ┌───────────────┐
    │  Report        │  ← LLM synthesis pass for narrative sections
    │  Generation    │  ← Chart/graph generation
    │                │  ← Final JSON → rendered HTML/PDF report
    └───────────────┘
```

---

## Data Ingestion Layer

### GSC Data Pull

```python
# Endpoints needed:
# 1. Performance data (clicks, impressions, CTR, position)
#    - By query (all queries, 16 months, 25K row limit per request)
#    - By page (all pages, 16 months)
#    - By date (daily granularity, full range)
#    - By query+page (to build query-page mapping)
#    - By query+date (for per-keyword time series)
#    - By page+date (for per-page time series)
# 2. URL inspection (for indexing status on key pages)
# 3. Sitemaps list

# Pagination strategy: GSC caps at 25K rows per request.
# For large sites, paginate by date ranges (monthly chunks)
# then merge and deduplicate.

# Storage: Raw responses cached in Supabase with TTL of 24 hours
# so re-runs don't re-fetch within the same day.
```

**Estimated API calls per report:** 50-200 depending on site size (date-range pagination).

### GA4 Data Pull

```python
# Endpoints needed:
# 1. Traffic overview (sessions, users, pageviews, bounce, engagement)
# 2. Landing pages with engagement metrics
# 3. Traffic by channel group
# 4. Traffic by source/medium
# 5. Conversions (event-based)
# 6. Custom report: pagePath × date for per-page daily time series
# 7. Custom report: pagePath × sessionSource for source attribution per page
# 8. Device breakdown

# All with date ranges matching GSC pull (16 months)
```

**Estimated API calls per report:** 15-30.

### DataForSEO SERP Data

```python
# For the user's top N non-branded keywords (N = 50-100):
# 1. Live SERP pull — full SERP features, positions, URLs
#    Endpoint: /v3/serp/google/organic/live/advanced
#    Cost: ~$0.002/query (same as your SERP MCP)
#    Budget per report: $0.10-0.20
#
# 2. SERP history (if available via DataForSEO or cached from prior runs)
#    For change-point detection on SERP composition over time
#
# Keyword selection logic:
# - Pull all queries from GSC, filter out branded (fuzzy match on domain/brand name)
# - Sort by impressions DESC
# - Take top 50-100
# - Also include any queries where position changed by > 3 spots in last 30 days
```

### Site Crawl (Internal Link Graph)

```python
# Lightweight crawl focused on internal links only.
# Options:
# 1. Scrapy (Python) — full control, can run on Railway
# 2. Sitemap-based — if sitemap exists, fetch all URLs and
#    extract internal links from each page via requests + BeautifulSoup
# 3. Accept user-uploaded Screaming Frog export (CSV) as alternative
#
# Data extracted per page:
# - URL, title, meta description, h1
# - All internal links (href + anchor text)
# - Word count
# - Schema markup present (types)
# - Canonical URL
#
# Storage: adjacency list in Supabase (from_url, to_url, anchor_text)
# For sites > 5,000 pages, cap crawl and note coverage %
```

---

## Analysis Modules

Each module is a standalone Python function that reads from the shared data store and writes structured results. Modules run sequentially because some depend on outputs of earlier modules.

### Module 1: Health & Trajectory

**Input:** GSC daily time series (clicks, impressions), 16 months

**Libraries:** `statsmodels` (MSTL decomposition), `STUMPY` (matrix profile), `scipy` (curve fitting), `numpy`

```python
def analyze_health_trajectory(daily_data: pd.DataFrame) -> dict:
    """
    1. MSTL decomposition → trend, day-of-week seasonal, longer seasonal, residual
       - periods: [7, 30] (weekly and monthly cycles)
    2. Trend direction classification:
       - Fit linear regression on trend component
       - Classify: strong_growth (>5%/mo), growth (1-5%), flat (-1 to 1%), 
         decline (-5 to -1%), strong_decline (< -5%)
    3. Change point detection on trend component:
       - PELT algorithm (ruptures library) to find structural breaks
       - Each break = a "something changed" event with date and magnitude
    4. STUMPY matrix profile on residuals:
       - Motifs = recurring patterns (e.g., monthly traffic spikes)
       - Discords = one-off anomalies
    5. Forward projection:
       - Fit ARIMA or Prophet on the deseasonalized trend
       - Generate 30/60/90 day forecast with confidence intervals
       - Express as: "projected clicks in 30 days: X (±Y)"
    
    Returns:
        {
            "overall_direction": "declining",
            "trend_slope_pct_per_month": -2.3,
            "change_points": [
                {"date": "2025-11-08", "magnitude": -0.12, "direction": "drop"}
            ],
            "seasonality": {
                "best_day": "Tuesday",
                "worst_day": "Saturday",
                "monthly_cycle": true,
                "cycle_description": "15% traffic spike first week of each month"
            },
            "anomalies": [
                {"date": "2025-12-25", "type": "discord", "magnitude": -0.45}
            ],
            "forecast": {
                "30d": {"clicks": 12400, "ci_low": 11200, "ci_high": 13600},
                "60d": {"clicks": 11800, "ci_low": 10100, "ci_high": 13500},
                "90d": {"clicks": 11200, "ci_low": 9000, "ci_high": 13400}
            }
        }
    """
```

### Module 2: Page-Level Triage

**Input:** GSC per-page daily time series, GA4 landing page engagement data

**Libraries:** `PyOD` (Isolation Forest), `scipy` (linear regression per page), `sklearn` (clustering)

```python
def analyze_page_triage(page_daily_data, ga4_landing_data, gsc_page_summary) -> dict:
    """
    1. Per-page trend fitting:
       - For each page with > 30 days of data:
         - Fit linear regression on daily clicks
         - Calculate slope (clicks/day change rate)
         - Project "days until falls below threshold" (e.g., page 1 = position 10)
       - Bucket pages: Growing (slope > 0.1), Stable (±0.1), 
         Decaying (-0.1 to -0.5), Critical (< -0.5)
    
    2. CTR anomaly detection (existing PyOD approach):
       - Group pages by average position (rounded)
       - Within each position group, use Isolation Forest to find 
         pages with anomalously low CTR
       - Flag as "title/snippet problem" vs "SERP feature displacement"
         (determined in Module 5 when SERP data is available)
    
    3. Engagement cross-reference (GA4):
       - Match GSC pages to GA4 landing pages
       - Flag pages with high search traffic but low engagement 
         (bounce > 80%, avg session < 30s)
       - These are "content mismatch" candidates
    
    4. Priority scoring:
       - Score = (current_monthly_clicks × abs(decay_rate)) × recoverability_factor
       - recoverability_factor based on: how recently decay started,
         current position (easier to recover from #8 than #25),
         whether it's a CTR problem (easy fix) vs position problem (hard fix)
    
    Returns:
        {
            "pages": [
                {
                    "url": "/blog/best-widgets",
                    "bucket": "decaying",
                    "current_monthly_clicks": 340,
                    "trend_slope": -0.28,
                    "projected_page1_loss_date": "2026-05-15",
                    "ctr_anomaly": true,
                    "ctr_expected": 0.082,
                    "ctr_actual": 0.031,
                    "engagement_flag": "low_engagement",
                    "priority_score": 87.4,
                    "recommended_action": "title_rewrite"
                }
            ],
            "summary": {
                "total_pages_analyzed": 142,
                "growing": 23, "stable": 67, "decaying": 38, "critical": 14,
                "total_recoverable_clicks_monthly": 2840
            }
        }
    """
```

### Module 3: SERP Landscape Analysis

**Input:** DataForSEO live SERP results for top 50-100 keywords, GSC position data

**Libraries:** `pandas`, custom SERP feature parsing

```python
def analyze_serp_landscape(serp_data, gsc_keyword_data) -> dict:
    """
    1. SERP feature displacement analysis:
       - For each keyword, parse SERP features present:
         featured_snippet, people_also_ask (count), video_carousel,
         local_pack, knowledge_panel, ai_overview, reddit_threads,
         image_pack, shopping_results, top_stories
       - Calculate "visual position" = number of SERP elements above the
         user's organic listing (each PAA = 0.5 positions, featured snippet = 2, etc.)
       - Compare organic rank vs visual position
       - Flag keywords where visual_position > organic_rank + 3
         ("you rank #3 but you're visually #7")
    
    2. Competitor mapping:
       - For each keyword, extract all domains in top 10
       - Build competitor frequency matrix:
         which domains appear most across the user's keyword set
       - Identify "primary competitors" (appear in >20% of keywords)
       - For each competitor: average position, position trend if historical 
         data available
    
    3. Intent classification of SERPs:
       - Based on SERP composition, classify each keyword's current intent:
         informational (PAA heavy, knowledge panels),
         commercial (shopping, reviews),
         navigational (site links, knowledge panel for brand),
         transactional (shopping, ads heavy)
       - Compare against the user's page type for that keyword
       - Flag mismatches: "You have a blog post ranking for a transactional query"
    
    4. Click share estimation:
       - Using position-adjusted CTR curves (conditioned on SERP features present)
       - Estimate: of all available clicks for this keyword, what % does the user capture
       - Aggregate to total estimated click share across keyword portfolio
    
    Returns:
        {
            "keywords_analyzed": 87,
            "serp_feature_displacement": [
                {
                    "keyword": "best crm software",
                    "organic_position": 3,
                    "visual_position": 8,
                    "features_above": ["featured_snippet", "paa_x4", "ai_overview"],
                    "estimated_ctr_impact": -0.062
                }
            ],
            "competitors": [
                {
                    "domain": "competitor.com",
                    "keywords_shared": 34,
                    "avg_position": 4.2,
                    "threat_level": "high"
                }
            ],
            "intent_mismatches": [...],
            "total_click_share": 0.12,
            "click_share_opportunity": 0.31
        }
    """
```

### Module 4: Content Intelligence

**Input:** GSC query-page mapping, page crawl data, GA4 engagement

**Libraries:** `sklearn` (cosine similarity on TF-IDF or embeddings), `sentence-transformers` (optional for semantic analysis), `pandas`

```python
def analyze_content_intelligence(gsc_query_page, page_data, ga4_engagement) -> dict:
    """
    1. Cannibalization detection:
       - From GSC query+page data, find queries that drive impressions 
         to 2+ pages
       - For each cannibalizing pair:
         - Calculate query overlap % (Jaccard on shared queries)
         - Compare avg positions (is one clearly winning?)
         - Recommend: consolidate, differentiate, or canonical redirect
       - Severity = total impressions affected × position gap
    
    2. Striking distance opportunities:
       - Queries where avg position is 8-20 (page 1-2 boundary)
       - Filter: impressions > threshold (worth pursuing)
       - Classify by intent (using query patterns: "how to" = informational,
         "best" = commercial, "[brand] vs" = comparison, etc.)
       - Sort by: (impressions × estimated CTR gain from reaching top 5)
    
    3. Thin content flagging:
       - Cross-reference crawl data (word count) with GSC performance
       - Pages with high impressions but: word count < 500,
         or bounce rate > 85% (from GA4), or avg session < 20s
       - These need content expansion or rewrite
    
    4. Content age vs performance matrix:
       - If last-modified dates available from crawl (or infer from 
         sitemap lastmod), plot content age vs trajectory
       - Quadrants:
         Old + Decaying = URGENT UPDATE
         Old + Stable = LEAVE ALONE (evergreen)
         New + Decaying = STRUCTURAL PROBLEM (wrong intent, bad targeting)
         New + Growing = DOUBLE DOWN (more internal links, backlinks)
    
    Returns:
        {
            "cannibalization_clusters": [
                {
                    "query_group": "crm pricing comparison",
                    "pages": ["/blog/crm-pricing", "/crm-pricing-page"],
                    "shared_queries": 23,
                    "total_impressions_affected": 4500,
                    "recommendation": "consolidate",
                    "keep_page": "/crm-pricing-page"
                }
            ],
            "striking_distance": [
                {
                    "query": "best crm for small business",
                    "current_position": 11.3,
                    "impressions": 8900,
                    "estimated_click_gain_if_top5": 420,
                    "intent": "commercial",
                    "landing_page": "/blog/best-crm"
                }
            ],
            "thin_content": [...],
            "update_priority_matrix": {
                "urgent_update": [...],
                "leave_alone": [...],
                "structural_problem": [...],
                "double_down": [...]
            }
        }
    """
```

### Module 5: The Gameplan

**Input:** Outputs from Modules 1-4

**Libraries:** None (synthesis logic + LLM for narrative generation)

```python
def generate_gameplan(health, triage, serp, content) -> dict:
    """
    Synthesize all prior modules into a prioritized action list.
    
    1. Critical fixes (do this week):
       - Pages in "critical" decay bucket with > 100 clicks/month
       - CTR anomalies on high-impression keywords (title rewrites)
       - Cannibalization causing both pages to underperform
    
    2. Quick wins (do this month):
       - Striking distance keywords needing minor content updates
       - SERP feature optimization (add FAQ schema for PAA keywords,
         add video for video carousel keywords)
       - Internal link additions to boost decaying pages
    
    3. Strategic plays (this quarter):
       - Content gaps worth filling (new pages to create)
       - Consolidation projects (merge cannibalizing pages)
       - Content refreshes for "urgent update" quadrant pages
    
    4. Structural improvements (ongoing):
       - Internal link architecture changes
       - Seasonal content calendar based on identified cycles
       - Competitor monitoring priorities
    
    Each action item includes:
    - Specific page/keyword affected
    - What to do (concrete instruction)
    - Estimated traffic impact (clicks/month recoverable or gainable)
    - Effort level (low/medium/high)
    - Dependencies (e.g., "do after consolidating /blog/crm-pricing")
    
    LLM synthesis pass:
    - Feed structured data to Claude API
    - Generate human-readable narrative for each section
    - Tone: direct, consultant-grade, no fluff
    
    Returns:
        {
            "critical": [{"action": "...", "impact": 120, "effort": "low", ...}],
            "quick_wins": [...],
            "strategic": [...],
            "structural": [...],
            "total_estimated_monthly_click_recovery": 2840,
            "total_estimated_monthly_click_growth": 5200,
            "narrative": "Your site is currently declining at 2.3% per month..."
        }
    """
```

### Module 6: Algorithm Update Impact Analysis

**Input:** GSC daily time series, public algorithm update database

**Libraries:** `ruptures` (change point detection), `pandas`

```python
def analyze_algorithm_impacts(daily_data, change_points_from_module1) -> dict:
    """
    1. Maintain/fetch algorithm update database:
       - Source: scrape from Semrush Sensor, Moz, Search Engine Roundtable
       - Store in Supabase: date, name, type (core, spam, helpful content, etc.)
       - Update weekly via cron job
    
    2. For each change point detected in Module 1:
       - Find nearest algorithm update within ±7 days
       - If match found:
         - Pull per-page data for the 14-day window around the update
         - Identify which pages were most affected (largest position/click changes)
         - Look for common characteristics among affected pages
           (content type, word count, schema presence, backlink profile)
         - Generate attribution: "Core Update hit your thin blog posts hardest"
       - If no match found:
         - Flag as "unexplained change" — could be manual action,
           technical issue, or competitor movement
    
    3. Historical vulnerability assessment:
       - How many updates in the last 12 months affected the site?
       - What's the recovery pattern? (quick bounce-back vs sustained loss)
       - Which page types are most algorithmically vulnerable?
    
    Returns:
        {
            "updates_impacting_site": [
                {
                    "update_name": "November 2025 Core Update",
                    "date": "2025-11-08",
                    "site_impact": "negative",
                    "click_change_pct": -12.3,
                    "pages_most_affected": ["/blog/x", "/blog/y"],
                    "common_characteristics": ["thin_content", "no_schema"],
                    "recovery_status": "not_recovered"
                }
            ],
            "vulnerability_score": 0.72,  # 0-1, higher = more vulnerable
            "recommendation": "Focus on content depth for blog section"
        }
    """
```

### Module 7: Query Intent Migration Tracking

**Input:** GSC query data (16 months), with date dimension

**Libraries:** LLM API (Claude) for intent classification, `pandas`, `sklearn`

```python
def analyze_intent_migration(gsc_query_date_data) -> dict:
    """
    1. Intent classification:
       - Batch classify all unique queries via Claude API:
         informational, commercial, navigational, transactional
       - Cache classifications in Supabase (queries don't change intent often)
       - For queries that could shift (e.g., "[product] review" moves from
         informational to commercial), flag for re-classification
    
    2. Intent distribution over time:
       - Group queries by intent type
       - For each month: calculate % of total clicks from each intent type
       - Plot the migration: "6 months ago 60% informational, now 45%"
    
    3. AI Overview impact estimation:
       - Informational queries losing CTR (same impressions, fewer clicks)
         are likely being answered by AI Overviews
       - Flag these: "These 34 informational queries show stable impressions
         but declining CTR — likely AI Overview displacement"
       - Quantify: estimated clicks lost to AI Overviews per month
    
    4. Intent-based strategy:
       - If informational is declining: pivot to commercial/transactional content
       - If branded is dominant: non-branded growth plan
       - If navigational is majority: brand is strong but SEO isn't driving
         new discovery
    
    Returns:
        {
            "intent_distribution_current": {
                "informational": 0.45, "commercial": 0.30,
                "navigational": 0.15, "transactional": 0.10
            },
            "intent_distribution_6mo_ago": {
                "informational": 0.60, "commercial": 0.22,
                "navigational": 0.12, "transactional": 0.06
            },
            "ai_overview_impact": {
                "queries_affected": 34,
                "estimated_monthly_clicks_lost": 890,
                "affected_queries": [...]
            },
            "strategic_recommendation": "Shift content investment toward commercial intent..."
        }
    """
```

### Module 8: CTR Modeling by SERP Context

**Input:** DataForSEO SERP data, GSC position + CTR data

**Libraries:** `sklearn` (gradient boosting regressor), `pandas`

```python
def model_contextual_ctr(serp_data, gsc_data) -> dict:
    """
    1. Build SERP-context-aware CTR model:
       - Features per keyword:
         position, has_featured_snippet (above), paa_count_above,
         video_carousel_present, ai_overview_present, shopping_present,
         local_pack_present, number_of_ads_above, 
         number_of_organic_results_above_fold
       - Target: actual CTR from GSC
       - Train gradient boosting model on the user's own data
       - (Optionally supplement with benchmark data from aggregated users)
    
    2. Expected vs actual CTR per keyword:
       - For each keyword: model predicts expected CTR given SERP context
       - Compare to actual CTR
       - Overperformers: good titles/snippets (learn from these)
       - Underperformers: title/snippet problem OR user intent mismatch
    
    3. Adjusted position value:
       - "Your #3 ranking for [keyword] is worth 2.1% CTR in this SERP layout,
          not the 8% you'd expect from generic benchmarks"
       - Reframe all click estimates throughout the report using contextual CTR
    
    4. SERP feature opportunity scoring:
       - For keywords where a feature doesn't exist yet (no featured snippet):
         "Adding FAQ schema could capture featured snippet for [keyword],
          estimated +340 clicks/month"
       - For keywords where user could win video carousel:
         "Creating a video for [keyword] could capture video carousel position"
    
    Returns:
        {
            "ctr_model_accuracy": 0.84,  # R² on held-out data
            "keyword_ctr_analysis": [
                {
                    "keyword": "best crm software",
                    "position": 3,
                    "expected_ctr_generic": 0.082,
                    "expected_ctr_contextual": 0.021,
                    "actual_ctr": 0.018,
                    "performance": "in_line",  # not underperforming — SERP is just crowded
                    "serp_features_present": ["featured_snippet", "paa_x4", "shopping"]
                }
            ],
            "feature_opportunities": [
                {
                    "keyword": "crm implementation guide",
                    "feature": "featured_snippet",
                    "current_holder": "competitor.com",
                    "estimated_click_gain": 340,
                    "difficulty": "medium"
                }
            ]
        }
    """
```

### Module 9: Site Architecture & Authority Flow

**Input:** Internal link graph from crawl, GSC page performance data

**Libraries:** `networkx` (PageRank, graph analysis), `community` (Louvain clustering)

```python
def analyze_site_architecture(link_graph, page_performance) -> dict:
    """
    1. Build directed graph:
       - Nodes = pages, Edges = internal links (with anchor text)
       - Annotate nodes with GSC performance data (clicks, impressions, position)
    
    2. PageRank simulation:
       - Run NetworkX PageRank on internal link graph
       - Identify authority distribution:
         - Where is authority concentrated?
         - Which pages have high PageRank but low search traffic? (wasted authority)
         - Which pages have high traffic potential but low PageRank? (starved pages)
    
    3. Authority flow analysis:
       - For each conversion/money page: trace all paths from homepage
       - Calculate "link equity reaching conversion pages" as % of total
       - Flag if blog content is trapping authority (common pattern):
         "73% of internal links point to /blog/ but only 4% flow onward to /pricing/"
    
    4. Orphan page detection:
       - Pages in sitemap but with 0 internal links pointing to them
       - Pages with GSC impressions but no internal links (getting traffic
         only from external links or direct)
    
    5. Cluster analysis:
       - Louvain community detection on the link graph
       - Identify content silos (do they match intended site structure?)
       - Flag cross-silo linking opportunities
    
    6. Optimal link insertion recommendations:
       - For each "starved" high-potential page:
         find the highest-authority pages that are topically related
         (based on shared GSC queries) but don't currently link to it
       - Recommend specific link placements with suggested anchor text
    
    Returns:
        {
            "pagerank_distribution": {
                "top_authority_pages": [...],
                "starved_pages": [...],  # high potential, low PageRank
                "authority_sinks": [...]  # high PageRank, low traffic value
            },
            "authority_flow_to_conversion": 0.04,  # 4% reaches money pages
            "orphan_pages": [...],
            "content_silos": [
                {"name": "blog", "pages": 87, "internal_pagerank_share": 0.73},
                {"name": "product", "pages": 12, "internal_pagerank_share": 0.15}
            ],
            "link_recommendations": [
                {
                    "target_page": "/pricing",
                    "link_from": "/blog/crm-guide",
                    "suggested_anchor": "CRM pricing comparison",
                    "estimated_pagerank_boost": 0.023
                }
            ],
            "network_graph_data": {...}  # for D3/vis.js rendering
        }
    """
```

### Module 10: Branded vs Non-Branded Health

**Input:** GSC query data with brand-name filtering

**Libraries:** `fuzzywuzzy` or `rapidfuzz` (brand name matching), `pandas`, `scipy`

```python
def analyze_branded_split(gsc_query_data, brand_terms: list) -> dict:
    """
    1. Brand classification:
       - Fuzzy match all queries against brand name + common misspellings
       - Categories: branded, non-branded, competitor-branded
       - Allow user to review/adjust classification
    
    2. Independent trend analysis:
       - Run Module 1 trajectory analysis separately for branded and non-branded
       - Each gets its own trend, seasonality, forecast
       - "Your branded traffic is stable but non-branded is growing 8%/month"
    
    3. Dependency risk scoring:
       - branded_ratio = branded_clicks / total_clicks
       - > 0.90 = "critical dependency" — site would collapse without brand
       - 0.70-0.90 = "high dependency" — vulnerable
       - 0.50-0.70 = "balanced"
       - < 0.50 = "discovery-driven" — healthy SEO
    
    4. Non-branded opportunity sizing:
       - Total non-branded impressions (people are searching, you're showing up)
       - Total non-branded clicks (what you're actually capturing)
       - Gap = impressions × avg_ctr_if_top3 - current_clicks
       - "You're leaving ~X clicks/month on the table in non-branded search"
    
    5. Growth projection:
       - At current non-branded growth rate, when does non-branded become
         meaningful (>20% of traffic)?
       - "At current trajectory, non-branded won't become meaningful for 14 months.
          With recommended actions, this could accelerate to 6 months."
    
    Returns:
        {
            "branded_ratio": 0.94,
            "dependency_level": "critical",
            "branded_trend": {"direction": "stable", "slope": 0.001},
            "non_branded_trend": {"direction": "growing", "slope": 0.08},
            "non_branded_opportunity": {
                "current_monthly_clicks": 340,
                "potential_monthly_clicks": 2800,
                "gap": 2460,
                "months_to_meaningful_at_current_rate": 14,
                "months_to_meaningful_with_actions": 6
            }
        }
    """
```

### Module 11: Competitive Threat Radar

**Input:** DataForSEO SERP data (competitor positions), GSC query data

**Libraries:** `pandas`, `scipy` (trend detection)

```python
def analyze_competitive_threats(serp_data, gsc_data) -> dict:
    """
    1. Competitor frequency analysis:
       - Across all monitored keywords, count how often each domain appears
       - Rank by frequency → primary competitors
    
    2. Emerging threat detection:
       - Domains that appeared in the keyword set for the first time recently
         (within last 30-60 days based on SERP history or GSC position changes)
       - Domains rapidly climbing (appeared at position 15-20 and now at 5-10
         across multiple keywords)
       - Flag: "Domain X is new to your keyword set and already ranking for
         8 of your top 20 terms"
    
    3. Competitor content velocity:
       - For top 5 competitors: estimate publishing frequency from SERP
         appearances (new URLs entering rankings)
       - "Competitor Y is publishing 4x/week in your niche vs your 1x/week"
    
    4. Vulnerability assessment per keyword:
       - For each of the user's top keywords:
         how many competitors are within 3 positions?
         Is the gap narrowing or widening?
       - Flag keywords where competitors are closing in
    
    Returns:
        {
            "primary_competitors": [
                {"domain": "competitor.com", "keyword_overlap": 34, "avg_position": 4.2}
            ],
            "emerging_threats": [
                {
                    "domain": "newcomer.com",
                    "first_seen": "2026-02-15",
                    "keywords_entered": 8,
                    "avg_entry_position": 12.3,
                    "current_avg_position": 7.1,
                    "trajectory": "rapidly_improving",
                    "threat_level": "high"
                }
            ],
            "keyword_vulnerability": [
                {
                    "keyword": "best crm software",
                    "your_position": 5,
                    "competitors_within_3": 4,
                    "gap_trend": "narrowing"
                }
            ]
        }
    """
```

### Module 12: Revenue Attribution

**Input:** GSC click data, GA4 conversion data, GA4 ecommerce data (if available)

**Libraries:** `pandas`, `scipy`

```python
def estimate_revenue_attribution(gsc_data, ga4_conversions, ga4_engagement) -> dict:
    """
    1. Click-to-conversion mapping:
       - Match GSC landing pages to GA4 conversion rates
       - For each landing page: conversion_rate = conversions / sessions
       - If ecommerce data available: avg_order_value per landing page
    
    2. Position-to-revenue modeling:
       - For each keyword: estimate clicks at each position (1-10)
         using contextual CTR model from Module 8
       - Multiply by landing page conversion rate and AOV
       - "Moving [keyword] from position 6 to position 3 = 
          +340 clicks × 2.4% CVR × $150 AOV = $1,224/month"
    
    3. Total search revenue at risk:
       - For all decaying pages (from Module 2):
         calculate revenue lost if decay continues to forecast
       - "If you do nothing, you'll lose ~$X/month in search-attributed 
          revenue within 90 days"
    
    4. ROI of recommended actions:
       - For each action in the gameplan (Module 5):
         attach revenue estimate
       - "The 5 critical fixes have a combined estimated value of $X/month.
          The 12 quick wins are worth ~$Y/month."
       - This makes the consulting pitch trivial: 
         "My consulting fee is $Z, the identified opportunities are worth $X/month"
    
    Returns:
        {
            "total_search_attributed_revenue_monthly": 34000,
            "revenue_at_risk_90d": 4200,
            "top_revenue_keywords": [
                {
                    "keyword": "crm pricing",
                    "current_revenue_monthly": 2100,
                    "potential_revenue_if_top3": 5400,
                    "gap": 3300
                }
            ],
            "action_roi": {
                "critical_fixes_monthly_value": 2800,
                "quick_wins_monthly_value": 5200,
                "strategic_plays_monthly_value": 12000,
                "total_opportunity": 20000
            }
        }
    """
```

---

## Infrastructure & Deployment

### Railway Services

| Service | Purpose | Resources |
|---------|---------|-----------|
| `search-intel-api` | FastAPI backend, analysis pipeline | 2GB RAM, 2 vCPU (analysis is CPU-intensive) |
| `search-intel-worker` | Async job processor for report generation | 4GB RAM (ML models in memory) |
| `search-intel-web` | Next.js frontend | Standard |
| `search-intel-cron` | Weekly algorithm DB update, data cleanup | Minimal |

### Supabase Tables

```sql
-- User accounts and OAuth tokens
CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email TEXT UNIQUE NOT NULL,
    gsc_token JSONB,           -- encrypted OAuth token
    ga4_token JSONB,           -- encrypted OAuth token
    created_at TIMESTAMPTZ DEFAULT now()
);

-- Generated reports
CREATE TABLE reports (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID REFERENCES users(id),
    gsc_property TEXT NOT NULL,
    ga4_property TEXT,
    status TEXT DEFAULT 'pending',  -- pending, ingesting, analyzing, generating, complete, failed
    progress JSONB DEFAULT '{}',    -- {"module_1": "complete", "module_2": "running", ...}
    report_data JSONB,              -- complete report output
    created_at TIMESTAMPTZ DEFAULT now(),
    completed_at TIMESTAMPTZ
);

-- Cached API responses (avoid re-fetching within 24h)
CREATE TABLE api_cache (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID REFERENCES users(id),
    cache_key TEXT NOT NULL,       -- hash of API call params
    response JSONB NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    UNIQUE(user_id, cache_key)
);

-- Algorithm update database
CREATE TABLE algorithm_updates (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    name TEXT NOT NULL,
    type TEXT,                    -- core, spam, helpful_content, link, etc.
    source TEXT,                  -- where we sourced this info
    description TEXT
);

-- Query intent classification cache
CREATE TABLE query_intents (
    query_hash TEXT PRIMARY KEY,
    query TEXT NOT NULL,
    intent TEXT NOT NULL,         -- informational, commercial, navigational, transactional
    confidence FLOAT,
    classified_at TIMESTAMPTZ DEFAULT now()
);

-- SERP snapshots (for historical comparison on re-runs)
CREATE TABLE serp_snapshots (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    keyword TEXT NOT NULL,
    snapshot_date DATE NOT NULL,
    serp_data JSONB NOT NULL,
    UNIQUE(keyword, snapshot_date)
);
```

### Cost Estimation Per Report

| Component | Cost | Notes |
|-----------|------|-------|
| DataForSEO SERP queries | $0.10-0.20 | 50-100 keywords × $0.002 |
| Claude API (intent classification) | $0.05-0.15 | Batch classify ~500 queries |
| Claude API (narrative generation) | $0.02-0.05 | Gameplan synthesis |
| Railway compute | ~$0.01-0.02 | 3-5 min of worker time |
| **Total per report** | **~$0.20-0.40** | |

At scale: 1,000 reports/month = ~$200-400/month in variable costs. Very sustainable for a free tool → consulting funnel.

### Python Dependencies

```
# requirements.txt
fastapi
uvicorn
httpx                    # async HTTP client
pandas
numpy
scipy
scikit-learn
pyod                     # anomaly detection
stumpy                   # matrix profiles
statsmodels              # MSTL decomposition, ARIMA
ruptures                 # change point detection
networkx                 # graph analysis
python-louvain           # community detection
rapidfuzz                # fuzzy string matching
sentence-transformers    # optional: semantic content analysis
google-auth
google-auth-oauthlib
google-api-python-client # GSC + GA4 APIs
supabase                 # Supabase Python client
anthropic                # Claude API for LLM passes
```

---

## Frontend Report UI

### Sections map to modules 1:1

Each section is a collapsible card with:
- **TL;DR** — one sentence summary with key metric
- **Visualization** — interactive chart (Recharts or D3)
- **Detail table** — sortable, filterable data
- **Actions** — specific recommendations with estimated impact

### Key visualizations

| Section | Chart Type |
|---------|-----------|
| Health & Trajectory | Line chart with trend + forecast + confidence interval + change point markers |
| Page Triage | Scatter plot (current clicks vs decay rate), color-coded by bucket |
| SERP Landscape | Stacked bar chart showing SERP feature composition per keyword |
| Content Intelligence | 2×2 matrix (age vs decay) with page dots |
| Algorithm Impacts | Timeline with traffic overlay and update markers |
| Intent Migration | Stacked area chart showing intent % over time |
| CTR Modeling | Expected vs actual CTR scatter with diagonal reference line |
| Site Architecture | Force-directed network graph (D3) with node size = PageRank |
| Branded Split | Dual-axis line chart (branded vs non-branded over time) |
| Competitive Radar | Radar/spider chart per competitor across keyword overlap dimensions |
| Revenue Attribution | Waterfall chart showing current revenue → potential with actions |

### Consulting CTA placement

- After Section 5 (Gameplan): "Want help executing this plan? [Book a call]"
- After Section 12 (Revenue): "These opportunities total $X/month. [Let's capture them together]"
- Subtle footer on every section: "Generated by [Brand] — Search Intelligence Consulting"

---

## Build Priority & Timeline

### Phase 1 — MVP (Weeks 1-4)
- OAuth flow (GSC + GA4)
- Data ingestion layer
- Modules 1, 2, 5 (Health, Page Triage, Gameplan)
- Basic report UI
- **This alone is a useful free tool that drives consulting leads**

### Phase 2 — SERP Intelligence (Weeks 5-8)
- DataForSEO integration
- Modules 3, 8, 11 (SERP Landscape, CTR Modeling, Competitive Radar)
- Enhanced visualizations

### Phase 3 — Deep Analysis (Weeks 9-12)
- Modules 4, 6, 7 (Content Intelligence, Algorithm Impacts, Intent Migration)
- Site crawl infrastructure
- Module 9 (Site Architecture with network graph)

### Phase 4 — Revenue & Polish (Weeks 13-16)
- Module 10 (Branded/Non-Branded)
- Module 12 (Revenue Attribution)
- Report PDF export
- Email delivery (scheduled re-runs)
- Historical comparison (this month vs last month)

---

## Competitive Moat Analysis

**Why this can't be easily replicated:**

1. **12 interdependent modules** — each module feeds into others (Module 8 uses Module 3 data, Module 12 uses Modules 2+5+8)
2. **Multiple API integrations** — GSC, GA4, DataForSEO, Claude API, site crawler
3. **Real ML/stats** — not calling a "score" API, actually running PyOD, STUMPY, MSTL, PageRank, gradient boosting, change point detection
4. **LLM synthesis** — converting structured data into actionable narrative requires prompt engineering that produces consultant-grade output
5. **Domain expertise baked in** — the logic for what's "good" vs "bad", how to prioritize actions, what constitutes a real opportunity vs noise — that's 12+ years of SEO knowledge encoded into the system
6. **Ongoing data requirements** — algorithm update DB, SERP snapshots, intent classification cache — these compound over time

Even if someone understood every technique, wiring them together into a coherent product with a polished UI is 3-4 months of focused development. Nobody is doing that for free when they could just use your tool.
