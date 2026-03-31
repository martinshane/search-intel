# Search Intelligence Report — Phase 1 Integration Tests

This directory contains integration tests for the Phase 1 MVP of the Search Intelligence Report system.

## Overview

Phase 1 implements the core pipeline:
- OAuth flow (GSC + GA4)
- Data ingestion layer
- Analysis Modules 1, 2, 5 (Health & Trajectory, Page Triage, Gameplan)
- Report generation and storage
- Job status polling

## Test Files

- `test_phase1_e2e.py` — End-to-end integration test for the complete Phase 1 pipeline

## Running the Phase 1 Integration Test

### Prerequisites

1. **Real GSC + GA4 Properties**
   - You need access to a Google Search Console property with at least 3 months of data
   - You need access to a Google Analytics 4 property connected to the same domain
   - Both properties should have measurable traffic (at least 100 clicks/month)

2. **Google OAuth Credentials**
   - Create OAuth 2.0 credentials in Google Cloud Console
   - Enable Google Search Console API and Google Analytics Data API
   - Set up OAuth consent screen
   - Add your test account as a test user

3. **Supabase Instance**
   - Set up a Supabase project with the required tables (see main README)
   - Get your Supabase URL and service role key

4. **Environment Variables**
   - Create a `.env` file in the project root with:
     ```
     SUPABASE_URL=your_supabase_url
     SUPABASE_KEY=your_supabase_service_role_key
     GOOGLE_CLIENT_ID=your_google_client_id
     GOOGLE_CLIENT_SECRET=your_google_client_secret
     CLAUDE_API_KEY=your_claude_api_key
     ```

### Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Run database migrations (if needed)
# The test will check for required tables and create them if missing

# Ensure the API server is running
cd api
python -m uvicorn main:app --reload --port 8000
```

### Running the Test

```bash
# From the project root
cd api/tests
pytest test_phase1_e2e.py -v -s

# Or run with coverage
pytest test_phase1_e2e.py -v -s --cov=api --cov-report=html
```

### OAuth Flow During Testing

The test will:
1. Print an authorization URL to the console
2. Wait for you to visit the URL in a browser
3. Authenticate with your Google account
4. Authorize access to GSC and GA4
5. Receive the authorization code and exchange it for tokens
6. Continue with the test automatically

**Note:** You'll need to manually copy/paste the authorization code from the OAuth callback into the terminal when prompted.

### What the Test Does

The Phase 1 end-to-end test performs the following operations:

1. **User Creation**
   - Creates a test user in the database
   - Verifies user record is created with correct schema

2. **OAuth Token Storage**
   - Completes OAuth flow for GSC and GA4
   - Stores encrypted tokens in the database
   - Verifies tokens are retrievable and valid

3. **Report Job Creation**
   - Creates a new report generation job
   - Verifies job is created with `pending` status
   - Returns a job ID for polling

4. **Data Ingestion**
   - Triggers data fetch from GSC (queries, pages, date ranges)
   - Triggers data fetch from GA4 (landing pages, engagement metrics)
   - Verifies data is cached in the `api_cache` table
   - Checks that ingestion progress is updated in the job record

5. **Module 1: Health & Trajectory**
   - Runs MSTL decomposition on GSC daily time series
   - Performs change point detection
   - Identifies seasonality patterns
   - Generates traffic forecast (30/60/90 days)
   - Writes results to report JSON

6. **Module 2: Page-Level Triage**
   - Analyzes per-page trend lines
   - Classifies pages into buckets (growing, stable, decaying, critical)
   - Detects CTR anomalies using Isolation Forest
   - Cross-references with GA4 engagement data
   - Calculates priority scores for each page
   - Writes results to report JSON

7. **Module 5: The Gameplan**
   - Synthesizes outputs from Modules 1 and 2
   - Generates prioritized action items (critical, quick wins, strategic)
   - Calls Claude API for narrative generation
   - Estimates total recoverable/gainable traffic
   - Writes results to report JSON

8. **Report Storage**
   - Writes complete report JSON to Supabase `reports` table
   - Updates job status to `complete`
   - Records completion timestamp

9. **Job Status Polling**
   - Polls the job status endpoint multiple times
   - Verifies progress updates are reflected
   - Confirms final status is `complete`
   - Retrieves the complete report data

### Expected Test Duration

The full Phase 1 pipeline typically takes **2-5 minutes** depending on:
- Site size (number of pages and keywords)
- API response times (GSC and GA4 can be slow)
- Claude API response time for narrative generation

The test has a 10-minute timeout to accommodate larger properties.

### Expected Outcomes

A successful test run should show:

```
✓ User created successfully
✓ OAuth tokens stored and encrypted
✓ Report job created with ID: abc-123-def
✓ GSC data ingested: 1,234 queries, 567 pages
✓ GA4 data ingested: 543 landing pages
✓ Module 1 complete: trend=declining, change_points=2, forecast generated
✓ Module 2 complete: 45 pages analyzed, 12 decaying, 3 critical
✓ Module 5 complete: 8 actions generated, 2,340 clicks recoverable
✓ Report JSON written to database
✓ Job status polling successful
✓ Final report retrieved and validated

PASSED test_phase1_e2e.py::test_full_phase1_pipeline
```

### Verification Checklist

After the test completes, verify the following:

- [ ] **Database Records**
  - User record exists in `users` table
  - Report record exists in `reports` table with status `complete`
  - API cache entries exist in `api_cache` table
  - OAuth tokens are encrypted (not plain text)

- [ ] **Report Data Structure**
  - `report_data` JSON contains keys: `health_trajectory`, `page_triage`, `gameplan`
  - `health_trajectory` contains: `overall_direction`, `change_points`, `seasonality`, `forecast`
  - `page_triage` contains: `pages` array, `summary` object with bucket counts
  - `gameplan` contains: `critical`, `quick_wins`, `strategic` arrays with action items

- [ ] **Module Outputs**
  - Module 1: At least one change point detected OR a clear trend direction
  - Module 2: Pages correctly bucketed into growing/stable/decaying/critical
  - Module 2: Priority scores are numeric and > 0 for decaying pages
  - Module 5: Each action item has `action`, `impact`, `effort` fields
  - Module 5: Total estimated impact is calculated and > 0

- [ ] **Data Quality**
  - GSC data: Date range covers at least 12 months
  - GSC data: No duplicate query-page combinations
  - GA4 data: Landing pages match GSC pages (URL normalized)
  - Time series: No gaps > 7 days (weekends/holidays expected)

- [ ] **Performance**
  - Data ingestion completes in < 2 minutes
  - Each analysis module completes in < 1 minute
  - Total pipeline completes in < 5 minutes
  - API cache is used on repeat runs (no duplicate fetches)

### Troubleshooting

**OAuth errors:**
- Verify your OAuth credentials are correct in `.env`
- Check that GSC API and GA4 API are enabled in Google Cloud Console
- Ensure your test account has access to the properties
- Make sure redirect URI is configured correctly

**Data ingestion failures:**
- Check that the GSC property has sufficient historical data (3+ months)
- Verify the property URL format matches GSC exactly (http vs https, www vs non-www)
- For GA4, ensure the property ID is numeric (not "G-XXXXXXXXXX")
- Check API quotas in Google Cloud Console

**Module analysis failures:**
- Module 1 requires at least 60 days of daily data for MSTL decomposition
- Module 2 requires at least 10 pages with 30+ days of data
- If site has very low traffic, some statistical methods may fail gracefully

**Database errors:**
- Run migrations to ensure all tables exist
- Check Supabase connection string and credentials
- Verify service role key has write permissions

**Claude API errors:**
- Check API key is valid and has credits
- Module 5 will retry up to 3 times on rate limits
- Narrative generation is optional; test will pass even if it fails

### Manual Testing

You can also run parts of the pipeline manually:

```bash
# Test OAuth flow only
curl -X POST http://localhost:8000/api/v1/auth/google/authorize

# Test data ingestion
curl -X POST http://localhost:8000/api/v1/reports \
  -H "Content-Type: application/json" \
  -d '{"gsc_property": "https://example.com", "ga4_property": "123456789"}'

# Poll job status
curl http://localhost:8000/api/v1/reports/{job_id}/status

# Retrieve completed report
curl http://localhost:8000/api/v1/reports/{job_id}
```

## Next Steps After Phase 1

Once Phase 1 tests pass, the following are ready for Phase 2:

- **Frontend Integration:** Connect React UI to the report generation API
- **SERP Intelligence:** Add DataForSEO integration and Modules 3, 8, 11
- **Advanced Analysis:** Implement remaining modules (4, 6, 7, 9, 10, 12)
- **Visualization:** Build interactive charts for each report section
- **PDF Export:** Add PDF generation for downloadable reports

## CI/CD Integration

This test is designed to run in CI/CD pipelines:

```yaml
# .github/workflows/phase1-integration.yml
name: Phase 1 Integration Tests
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - uses: actions/setup-python@v2
        with:
          python-version: '3.11'
      - run: pip install -r requirements.txt
      - run: pytest api/tests/test_phase1_e2e.py -v
    env:
      SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
      SUPABASE_KEY: ${{ secrets.SUPABASE_KEY }}
      # Note: OAuth flow requires manual intervention,
      # so CI runs use pre-generated test tokens
      TEST_GSC_TOKEN: ${{ secrets.TEST_GSC_TOKEN }}
      TEST_GA4_TOKEN: ${{ secrets.TEST_GA4_TOKEN }}
```

## Contact

For questions about the tests or issues with Phase 1 integration, refer to the main project documentation or open an issue in the repository.