# Search Intelligence Report

> **A free, computational-moat SEO analysis tool that drives search consulting leads**

[![Deploy on Railway](https://railway.app/button.svg)](https://railway.app/template)

## Overview

Search Intelligence Report generates a comprehensive 12-section SEO analysis for any site connected via Google Search Console and GA4 OAuth. The report combines statistical analysis, machine learning, cross-dataset correlation, and predictive modeling to deliver actionable insights that go far beyond basic SEO tools.

**Core thesis:** The moat is computational complexity. Every section requires orchestrating multiple APIs, applying real statistical/ML techniques (MSTL decomposition, STUMPY matrix profiles, PyOD anomaly detection, PageRank simulation), and synthesizing cross-dataset insights. This is not viably reproducible via casual coding.

### What makes this different

- **Real statistics:** MSTL seasonal decomposition, change-point detection (PELT), matrix profiles for pattern discovery
- **Machine learning:** Isolation Forest for CTR anomalies, gradient boosting for contextual CTR modeling
- **Graph analysis:** PageRank simulation on internal link structure, Louvain clustering for content silos
- **Predictive modeling:** ARIMA/Prophet forecasting, decay trajectory projection with confidence intervals
- **Cross-dataset synthesis:** GSC + GA4 + live SERP data + site crawl + algorithm updates = insights impossible from any single source

## Architecture

```
┌─────────────┐
│  Frontend   │  React (Next.js) — interactive charts, network graphs
│  (Railway)  │  Report UI with collapsible sections
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   FastAPI   │  Python backend — all analysis logic
│  (Railway)  │  OAuth flow, data ingestion, 12 analysis modules
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  Supabase   │  PostgreSQL — cache, reports, user tokens
│             │  Job queue for async report generation
└─────────────┘
       │
       ▼
┌─────────────────────────────────────────────┐
│  External APIs                              │
│  • Google Search Console API                │
│  • GA4 Data API                             │
│  • DataForSEO (live SERP data)              │
│  • Claude API (LLM synthesis)               │
└─────────────────────────────────────────────┘
```

## Report Sections (12 modules)

1. **Health & Trajectory** — MSTL decomposition, change-point detection, STUMPY anomaly discovery, 90-day forecast
2. **Page-Level Triage** — Per-page trend analysis, CTR anomaly detection (Isolation Forest), decay projection
3. **SERP Landscape Analysis** — SERP feature displacement, competitor mapping, intent classification, click share estimation
4. **Content Intelligence** — Cannibalization detection, striking distance opportunities, thin content flagging
5. **The Gameplan** — Synthesized action list (critical fixes, quick wins, strategic plays) with estimated impact
6. **Algorithm Update Impact** — Attribution of traffic changes to known Google updates, vulnerability scoring
7. **Query Intent Migration** — LLM-powered intent classification, AI Overview impact estimation, strategic pivots
8. **CTR Modeling by SERP Context** — Gradient boosting model for expected CTR given SERP features, opportunity scoring
9. **Site Architecture & Authority Flow** — PageRank simulation, authority flow analysis, optimal link recommendations
10. **Branded vs Non-Branded Health** — Independent trajectory analysis, dependency risk scoring, growth projection
11. **Competitive Threat Radar** — Emerging threat detection, competitor velocity tracking, keyword vulnerability
12. **Revenue Attribution** — Position-to-revenue modeling, ROI estimation for recommended actions

## Quick Start

### Prerequisites

- Python 3.11+
- Node.js 18+
- Supabase account
- Railway account
- Google Cloud project (OAuth credentials)
- DataForSEO API key
- Anthropic API key (Claude)

### Environment Variables

```bash
# Backend (.env)
SUPABASE_URL=your_supabase_url
SUPABASE_ANON_KEY=your_anon_key
SUPABASE_SERVICE_KEY=your_service_key
GOOGLE_CLIENT_ID=your_google_client_id
GOOGLE_CLIENT_SECRET=your_google_client_secret
DATAFORSEO_LOGIN=your_dataforseo_login
DATAFORSEO_PASSWORD=your_dataforseo_password
ANTHROPIC_API_KEY=your_claude_api_key
FRONTEND_URL=http://localhost:3000
API_URL=http://localhost:8000
```

### Local Development

**Backend:**
```bash
cd api
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

**Frontend:**
```bash
cd web
npm install
npm run dev
```

Visit http://localhost:3000

### Deploy to Railway

1. **Fork this repository**

2. **Create Railway project:**
   - Connect your GitHub repo
   - Create three services:
     - `search-intel-api` (from `/api`)
     - `search-intel-worker` (from `/api`, different start command)
     - `search-intel-web` (from `/web`)

3. **Configure environment variables** in Railway dashboard for each service

4. **Provision Supabase:**
   - Run database migrations from `/supabase/migrations`
   - Set up Row Level Security policies
   - Configure OAuth redirect URLs

5. **Deploy:**
   - Railway will auto-deploy on push to main
   - Health check available at: `https://your-api.railway.app/health`

## Project Structure

```
search-intel-report/
├── api/                          # FastAPI backend
│   ├── main.py                   # FastAPI app, routes, OAuth flow
│   ├── models/                   # Pydantic schemas
│   ├── services/                 # Business logic
│   │   ├── ingestion/            # GSC, GA4, DataForSEO, crawl
│   │   ├── analysis/             # 12 analysis modules
│   │   └── generation/           # Report assembly, LLM synthesis
│   ├── core/                     # Config, database, utils
│   └── requirements.txt
│
├── web/                          # Next.js frontend
│   ├── src/
│   │   ├── app/                  # Pages (App Router)
│   │   ├── components/           # React components
│   │   │   ├── auth/             # OAuth flow UI
│   │   │   ├── report/           # Report sections
│   │   │   └── charts/           # Visualization components
│   │   └── lib/                  # API client, utils
│   └── package.json
│
├── supabase/                     # Database schema
│   ├── migrations/               # SQL migrations
│   └── seed.sql                  # Algorithm update seed data
│
├── scripts/                      # Utility scripts
│   ├── scrape_algorithm_updates.py
│   └── backfill_intent_cache.py
│
├── tests/                        # Test suites
│   ├── test_modules/             # Per-module tests
│   └── test_integration/         # End-to-end tests
│
└── docs/                         # Documentation
    ├── API.md                    # API reference
    ├── MODULES.md                # Analysis module specs
    └── DEPLOYMENT.md             # Deployment guide
```

## Key Technologies

**Backend:**
- FastAPI (async API framework)
- scikit-learn (ML models)
- PyOD (anomaly detection)
- STUMPY (time series matrix profiles)
- statsmodels (MSTL decomposition, ARIMA)
- ruptures (change-point detection)
- networkx (graph analysis)
- sentence-transformers (semantic analysis)

**Frontend:**
- Next.js 14 (App Router)
- Recharts (charting)
- D3.js (network graphs)
- TailwindCSS (styling)

**Infrastructure:**
- Railway (hosting)
- Supabase (PostgreSQL + auth)
- Google Cloud (OAuth)
- DataForSEO (SERP data)
- Anthropic Claude (LLM synthesis)

## Cost Model

**Per report:**
- DataForSEO: $0.10-0.20 (50-100 SERP queries)
- Claude API: $0.07-0.20 (intent classification + narrative)
- Railway compute: ~$0.02 (3-5 min processing)
- **Total: ~$0.20-0.40 per report**

At 1,000 reports/month = ~$200-400 in variable costs. Highly sustainable for a free tool → consulting funnel.

## Development Roadmap

### ✅ Phase 1: MVP (Weeks 1-4)
- [x] OAuth flow (GSC + GA4)
- [x] Basic data ingestion
- [x] Modules 1, 2, 5 (Health, Triage, Gameplan)
- [x] Simple report UI
- [x] Deploy to Railway

### 🚧 Phase 2: SERP Intelligence (Weeks 5-8)
- [ ] DataForSEO integration
- [ ] Modules 3, 8, 11 (SERP, CTR modeling, Competitive)
- [ ] Enhanced visualizations (network graphs)

### 📋 Phase 3: Advanced Analysis (Weeks 9-12)
- [ ] Modules 6, 7, 9, 10 (Algorithm, Intent, Architecture, Branded)
- [ ] Site crawl integration
- [ ] Historical comparison (re-run reports)

### 🎯 Phase 4: Revenue & Polish (Weeks 13-16)
- [ ] Module 12 (Revenue Attribution)
- [ ] PDF export
- [ ] White-label options
- [ ] API access tier

## Testing

```bash
# Backend tests
cd api
pytest

# Frontend tests
cd web
npm test

# Integration tests
pytest tests/test_integration/
```

## Contributing

This is a commercial project, but we welcome:
- Bug reports
- Performance optimizations
- Algorithm update data contributions
- Documentation improvements

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

Proprietary — All Rights Reserved

This code is provided for review purposes. Commercial use, modification, or distribution requires explicit permission.

## Support

- **Documentation:** [docs/](docs/)
- **Issues:** GitHub Issues
- **Email:** support@searchintel.report

---

**Built with 🔍 by [Your Consulting Brand]**

*Turning computational complexity into consulting leads since 2025*