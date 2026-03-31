# Search Intelligence Report — Development Program

## Project Status: Phase 1 Complete ✓

**Current Day:** 28 (Phase 1: Days 1-28 complete)  
**Next Phase:** Phase 2 — SERP Intelligence (Days 29-56)

---

## Overview

A free web-based tool that generates a comprehensive "Search Intelligence Report" for any site connected via GSC + GA4 OAuth. The report combines 12 integrated analysis sections that progressively build from raw data → statistical analysis → cross-dataset correlation → predictive modeling → prioritized action plan.

**Core thesis:** The moat is computational complexity. Every section requires orchestrating multiple APIs, applying real statistical/ML techniques, and synthesizing cross-dataset insights. This is not viably reproducible via vibe-coding.

---

## Phase 1: MVP — Complete ✓

**Duration:** Days 1-28  
**Goal:** Produce a working free tool that drives consulting leads  
**Status:** ✅ SHIPPED

### What Was Built

#### Infrastructure (Days 1-5) ✓
- [x] Railway deployment configured
- [x] Supabase database provisioned
- [x] FastAPI backend skeleton
- [x] React frontend skeleton
- [x] OAuth flow (GSC + GA4) fully functional
- [x] Environment variables and secrets management
- [x] Basic error handling and logging

#### Data Ingestion Layer (Days 6-10) ✓
- [x] GSC API integration with pagination
- [x] GA4 API integration
- [x] Supabase caching layer (24h TTL)
- [x] Data validation and normalization
- [x] Async job queue for report generation
- [x] Progress tracking (stored in reports.progress JSONB)

#### Core Analysis Modules (Days 11-22) ✓
- [x] **Module 1: Health & Trajectory**
  - MSTL decomposition (trend + seasonality)
  - Change point detection (PELT algorithm)
  - STUMPY matrix profiles (motifs + discords)
  - 30/60/90 day forecast (ARIMA/Prophet)
  
- [x] **Module 2: Page-Level Triage**
  - Per-page trend fitting (linear regression)
  - CTR anomaly detection (Isolation Forest)
  - Page bucketing (Growing/Stable/Decaying/Critical)
  - Priority scoring algorithm
  - GA4 engagement cross-reference
  
- [x] **Module 5: The Gameplan**
  - Action prioritization (Critical/Quick Wins/Strategic/Structural)
  - Impact estimation (clicks/month recoverable)
  - Effort scoring (low/medium/high)
  - Claude API integration for narrative synthesis
  - Dependency mapping

#### Frontend Report UI (Days 23-26) ✓
- [x] Report dashboard with collapsible sections
- [x] Health & Trajectory visualization (Recharts line chart with forecast)
- [x] Page Triage scatter plot (clicks vs decay rate)
- [x] Gameplan action list with filters
- [x] Consulting CTAs strategically placed
- [x] Mobile-responsive design
- [x] Export to PDF functionality

#### Testing & Polish (Days 27-28) ✓
- [x] End-to-end testing on 3 different sites:
  - Small blog (~200 pages, 500 queries)
  - Mid-size SaaS (~2,000 pages, 5K queries)
  - Large content site (~10,000 pages, 50K+ queries)
- [x] Performance optimization (pagination, caching)
- [x] Error handling hardening
- [x] User-facing error messages
- [x] Documentation (API docs, setup guide)

### Phase 1 Outcomes

✅ **Working Product:** Users can connect GSC + GA4, generate a report in 3-5 minutes, and receive actionable insights  
✅ **Consulting Funnel:** CTAs placed after Gameplan and throughout report  
✅ **Cost-Effective:** ~$0.05-0.10 per report (Claude API only, no DataForSEO yet)  
✅ **Scalable:** Async job queue handles concurrent reports  
✅ **Validated:** Tested on real sites, identifies genuine opportunities  

### Known Limitations (Phase 1)
- No SERP data yet (positions from GSC only, no SERP features)
- No internal link graph analysis
- No competitor intelligence
- CTR analysis uses generic benchmarks (not SERP-context-aware)
- No algorithm update attribution
- Intent classification basic (keyword pattern matching, not LLM)

**These are addressed in Phase 2.**

---

## Phase 2: SERP Intelligence — In Progress

**Duration:** Days 29-56 (4 weeks)  
**Goal:** Add competitive depth and SERP-context awareness  
**Start Date:** Day 29  
**Current Day:** 28 (Phase 2 starts tomorrow)

### Modules to Build

#### Module 3: SERP Landscape Analysis
- DataForSEO integration (live SERP pulls)
- SERP feature parsing (featured snippet, PAA, AI Overview, etc.)
- Visual position calculation
- Competitor frequency mapping
- Intent classification via SERP composition
- Click share estimation

#### Module 8: CTR Modeling by SERP Context
- Build gradient boosting CTR model
- Feature engineering (SERP features as inputs)
- Expected vs actual CTR comparison
- SERP feature opportunity scoring
- Position value adjustment based on SERP layout

#### Module 11: Competitive Threat Radar
- Competitor position tracking
- Emerging threat detection (new entrants, rapid climbers)
- Keyword vulnerability assessment
- Competitor content velocity estimation

#### Module 9: Site Architecture & Authority Flow (Stretch Goal)
- Internal link graph crawl (Scrapy or sitemap-based)
- PageRank simulation via NetworkX
- Authority flow analysis
- Orphan page detection
- Link placement recommendations

### Phase 2 Task Queue (Days 29-56)

#### Week 5: DataForSEO Integration & Module 3 (Days 29-35)

**Day 29: DataForSEO Setup**
- [ ] Create DataForSEO account and get API key
- [ ] Add DataForSEO client to backend (`services/dataforseo.py`)
- [ ] Implement SERP data fetching for keyword list
- [ ] Add SERP snapshots table to Supabase (for historical comparison)
- [ ] Test on 10 keywords, verify JSON structure

**Day 30: SERP Feature Parsing**
- [ ] Build SERP feature parser (featured_snippet, PAA, AI Overview, etc.)
- [ ] Extract competitor domains from organic results
- [ ] Calculate visual position (elements above organic listing)
- [ ] Unit tests for parser on diverse SERP types

**Day 31: Module 3 Core Logic**
- [ ] Implement `analyze_serp_landscape()` function
- [ ] SERP feature displacement analysis
- [ ] Competitor frequency mapping
- [ ] Intent classification based on SERP composition
- [ ] Output schema matching spec

**Day 32: Module 3 Integration**
- [ ] Add Module 3 to analysis pipeline (after Module 2)
- [ ] Update report data model in Supabase
- [ ] Wire keyword selection logic (top 50-100 non-branded)
- [ ] Handle API rate limits and retries

**Day 33: Module 3 Frontend**
- [ ] SERP Landscape section component
- [ ] Stacked bar chart (SERP feature composition)
- [ ] Competitor table (sortable by keyword overlap)
- [ ] Visual position vs organic rank comparison chart

**Day 34: Testing & Cost Optimization**
- [ ] Test Module 3 on all 3 test sites
- [ ] Verify DataForSEO costs (~$0.10-0.20 per report)
- [ ] Add caching for SERP data (24h TTL)
- [ ] Document any edge cases in build_log

**Day 35: Buffer & Documentation**
- [ ] Fix any issues from Day 34 testing
- [ ] Update API documentation
- [ ] Update program.md with Week 5 completion status

#### Week 6: Module 8 — CTR Modeling (Days 36-42)

**Day 36: Feature Engineering**
- [ ] Build feature extraction pipeline from SERP data
- [ ] Features: position, SERP elements above, counts of each type
- [ ] Combine with GSC actual CTR data
- [ ] Train/test split (80/20)

**Day 37: CTR Model Training**
- [ ] Implement gradient boosting regressor (sklearn)
- [ ] Hyperparameter tuning (grid search or Optuna)
- [ ] Model evaluation (R², MAE, feature importance)
- [ ] Persist model to disk (joblib)

**Day 38: Module 8 Core Logic**
- [ ] Implement `model_contextual_ctr()` function
- [ ] Expected vs actual CTR per keyword
- [ ] Identify overperformers and underperformers
- [ ] SERP feature opportunity scoring

**Day 39: Position Value Adjustment**
- [ ] Refactor all click estimates throughout report to use contextual CTR
- [ ] Update Module 2 priority scoring with adjusted CTR
- [ ] Update Module 5 impact estimates with adjusted CTR
- [ ] Update Module 12 (revenue) with adjusted CTR

**Day 40: Module 8 Frontend**
- [ ] CTR Modeling section component
- [ ] Expected vs actual scatter plot with diagonal reference line
- [ ] SERP feature opportunity table
- [ ] Model accuracy display (R², feature importance chart)

**Day 41: Integration Testing**
- [ ] Test on all 3 test sites
- [ ] Verify CTR model improves estimate accuracy vs generic benchmarks
- [ ] Check that adjusted CTR propagates through all modules
- [ ] Performance profiling (model inference should be <100ms)

**Day 42: Buffer & Polish**
- [ ] Address any issues from Day 41
- [ ] Add explanatory tooltips for CTR modeling concepts
- [ ] Update program.md with Week 6 status

#### Week 7: Module 11 — Competitive Radar (Days 43-49)

**Day 43: Competitor Analysis Foundation**
- [ ] Implement competitor frequency analysis
- [ ] Build competitor domain ranking logic
- [ ] Cross-reference with GSC query data

**Day 44: Emerging Threat Detection**
- [ ] Implement logic to detect new entrants (domains not seen 60 days ago)
- [ ] Identify rapid climbers (position improvement >5 spots)
- [ ] Threat level scoring algorithm

**Day 45: Module 11 Core Logic**
- [ ] Implement `analyze_competitive_threats()` function
- [ ] Keyword vulnerability assessment (competitors within 3 positions)
- [ ] Gap trend detection (narrowing vs widening)
- [ ] Output schema per spec

**Day 46: Historical Comparison**
- [ ] Use SERP snapshots table for historical competitor positions
- [ ] Implement content velocity estimation (new URLs from competitor)
- [ ] Track competitor position changes over time

**Day 47: Module 11 Frontend**
- [ ] Competitive Threat Radar section component
- [ ] Competitor table with threat levels
- [ ] Keyword vulnerability list (sortable by threat)
- [ ] Optional: Radar chart visualization per competitor

**Day 48: Integration & Testing**
- [ ] Add Module 11 to analysis pipeline
- [ ] Test on all 3 sites
- [ ] Verify threat detection accuracy
- [ ] Check performance (SERP data for 50-100 keywords)

**Day 49: Buffer & Documentation**
- [ ] Fix issues from Day 48
- [ ] Document competitive analysis methodology
- [ ] Update program.md with Week 7 status

#### Week 8: Module 9 — Site Architecture (Days 50-56)

**Day 50: Crawl Strategy Decision**
- [ ] Evaluate: Scrapy vs sitemap-based vs user-uploaded Screaming Frog
- [ ] Implement chosen approach
- [ ] Test on small site (<500 pages) first

**Day 51: Link Graph Extraction**
- [ ] Extract internal links from pages
- [ ] Build adjacency list in Supabase (from_url, to_url, anchor_text)
- [ ] Handle large sites (>5,000 pages) with sampling

**Day 52: Module 9 Core Logic — Part 1**
- [ ] Build directed graph with NetworkX
- [ ] Run PageRank simulation
- [ ] Identify authority distribution (high PR + low traffic = wasted authority)

**Day 53: Module 9 Core Logic — Part 2**
- [ ] Authority flow analysis (conversion page link equity)
- [ ] Orphan page detection
- [ ] Louvain community detection (content silos)

**Day 54: Link Recommendations**
- [ ] Optimal link insertion algorithm
- [ ] Match high-authority pages to starved pages via shared queries
- [ ] Generate anchor text suggestions
- [ ] Priority scoring for link placements

**Day 55: Module 9 Frontend**
- [ ] Site Architecture section component
- [ ] Network graph visualization (D3 force-directed or vis.js)
- [ ] PageRank distribution table
- [ ] Link recommendation list with estimated impact

**Day 56: Phase 2 Complete Review**
- [ ] Full pipeline test on all 3 sites
- [ ] Verify all 4 new modules integrate correctly
- [ ] Cost analysis (DataForSEO should still be <$0.25 per report)
- [ ] Document any issues in build_log
- [ ] Update program.md with Phase 2 completion and Phase 3 preview

---

## Phase 3: Intelligence Depth (Weeks 9-12, Days 57-84)

**Status:** Not Started  
**Goal:** Add historical analysis, intent tracking, algorithm impact

### Modules to Build

#### Module 6: Algorithm Update Impact Analysis
- Algorithm update database (scrape from Semrush Sensor, Moz, etc.)
- Change point attribution to known updates
- Per-page impact analysis
- Historical vulnerability scoring
- Recovery pattern analysis

#### Module 7: Query Intent Migration Tracking
- LLM-based intent classification (batch classify via Claude)
- Intent distribution over time (16 months)
- AI Overview impact estimation
- Strategic pivot recommendations

#### Module 10: Branded vs Non-Branded Health
- Fuzzy brand name matching
- Independent trajectory analysis for each
- Dependency risk scoring
- Non-branded opportunity sizing
- Growth projection to meaningful threshold

#### Module 4: Content Intelligence (Enhanced)
- Cannibalization detection (query overlap analysis)
- Striking distance opportunities
- Thin content flagging
- Content age vs performance matrix

### Phase 3 Task Queue Outline (Detailed on Day 56)

- Week 9: Module 6 (Algorithm Impact)
- Week 10: Module 7 (Intent Migration)
- Week 11: Module 10 (Branded Split) + Module 4 (Content Intelligence)
- Week 12: Integration, testing, polish

---

## Phase 4: Revenue Intelligence (Weeks 13-14, Days 85-98)

**Status:** Not Started  
**Goal:** Close the loop with revenue attribution

### Module 12: Revenue Attribution
- Click-to-conversion mapping (GSC → GA4)
- Position-to-revenue modeling
- Revenue at risk calculation
- ROI of recommended actions
- Make the consulting pitch trivial ("identified opportunities worth $X/month")

### Integration & Launch Polish
- Final end-to-end testing
- Performance optimization (target <3 min report generation)
- Error handling hardening
- User onboarding flow
- Marketing site copy
- Launch prep

---

## Cost Analysis (Current)

| Component | Phase 1 | Phase 2 (Target) | Phase 3 (Target) |
|-----------|---------|------------------|------------------|
| DataForSEO SERP | N/A | $0.10-0.20 | $0.10-0.20 |
| Claude API (intent) | N/A | N/A | $0.05-0.15 |
| Claude API (narrative) | $0.05-0.10 | $0.05-0.10 | $0.05-0.10 |
| Railway compute | $0.01 | $0.02 | $0.02 |
| **Total per report** | **$0.06-0.11** | **$0.18-0.32** | **$0.23-0.47** |

**At scale:** 1,000 reports/month = $230-470/month in variable costs.

---

## Success Metrics

### Phase 1 (Achieved) ✓
- [x] Report generation success rate >95%
- [x] Average report time <5 minutes
- [x] Zero manual intervention required per report
- [x] At least 1 consulting CTA per section
- [x] Validated on 3 diverse sites

### Phase 2 (Targets)
- [ ] SERP data enriches >80% of top keywords
- [ ] CTR model achieves R² >0.75
- [ ] Competitive analysis identifies threats on >50% of sites
- [ ] DataForSEO cost stays <$0.25 per report
- [ ] Report generation time stays <6 minutes with SERP data

### Phase 3 (Targets)
- [ ] Algorithm attribution accuracy >70% (vs known updates)
- [ ] Intent classification agreement with manual review >85%
- [ ] Branded/non-branded split accurate on >90% of sites

### Phase 4 (Targets)
- [ ] Revenue estimates within ±30% of user's internal tracking
- [ ] >50% of users who view report click a consulting CTA
- [ ] Average deal size >$5K/month (driven by revenue opportunity sizing)

---

## Tech Stack Reference

| Layer | Technology |
|-------|-----------|
| Frontend | React + Next.js |
| Backend | Python + FastAPI |
| Database | Supabase (PostgreSQL) |
| Hosting | Railway |
| Job Queue | Supabase + Railway async workers |
| Auth | Google OAuth 2.0 |
| APIs | GSC, GA4, DataForSEO, Claude |
| Analysis | scikit-learn, PyOD, STUMPY, statsmodels, networkx, ruptures |

---

## Current State (Day 28)

**Last completed:** Full Phase 1 testing on 3 sites  
**Next up:** Day 29 — DataForSEO setup and API integration  
**Blockers:** None  
**Notes:** Phase 1 exceeded expectations. Report quality is high enough to drive consulting leads as-is. Phase 2 will add significant competitive differentiation.

---

## Build Log Reference

Detailed daily notes, decisions, and issues tracked in `build_log.md`

---

**Last Updated:** Day 28  
**Next Review:** Day 56 (Phase 2 completion)