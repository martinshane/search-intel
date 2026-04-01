-- Search Intelligence Report — Supabase Schema
-- Created: 2026-03-29
-- Updated: 2026-03-31 — added report_modules table, fixed reports.status CHECK,
--   added domain + current_module columns to reports.

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- User accounts and OAuth tokens
CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email TEXT UNIQUE NOT NULL,
    gsc_token JSONB,           -- encrypted OAuth token for Google Search Console
    ga4_token JSONB,           -- encrypted OAuth token for Google Analytics 4
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_created_at ON users(created_at DESC);

-- Generated reports
CREATE TABLE reports (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID REFERENCES users(id) ON DELETE CASCADE,
    domain TEXT,                           -- target domain (e.g. "example.com")
    gsc_property TEXT NOT NULL,
    ga4_property TEXT,
    current_module INTEGER,                -- which module is currently running (1-12)
    status TEXT DEFAULT 'pending' CHECK (status IN (
        'pending', 'ingesting', 'running', 'analyzing',
        'generating', 'complete', 'completed', 'partial', 'failed'
    )),
    progress JSONB DEFAULT '{}'::JSONB,  -- {"module_1": "complete", "module_2": "running", ...}
    report_data JSONB,                    -- complete report output
    error_message TEXT,                   -- if status = failed, why
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    completed_at TIMESTAMPTZ
);

CREATE INDEX idx_reports_user_id ON reports(user_id);
CREATE INDEX idx_reports_status ON reports(status);
CREATE INDEX idx_reports_created_at ON reports(created_at DESC);
CREATE INDEX idx_reports_user_status ON reports(user_id, status);

-- Individual module results per report
-- Used by routes/modules.py _store_module_result() and module 5 (gameplan aggregation)
CREATE TABLE report_modules (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    report_id UUID REFERENCES reports(id) ON DELETE CASCADE,
    module_number INTEGER NOT NULL,
    module_name TEXT NOT NULL,
    results JSONB,
    status TEXT DEFAULT 'pending' CHECK (status IN ('pending', 'running', 'completed', 'failed')),
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(report_id, module_number)
);

CREATE INDEX idx_report_modules_report ON report_modules(report_id);
CREATE INDEX idx_report_modules_report_module ON report_modules(report_id, module_number);

-- Cached API responses (avoid re-fetching within 24h)
CREATE TABLE api_cache (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID REFERENCES users(id) ON DELETE CASCADE,
    cache_key TEXT NOT NULL,              -- hash of API call params (MD5 or SHA256)
    api_source TEXT NOT NULL,             -- 'gsc', 'ga4', 'dataforseo', 'crawler'
    response JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL,
    UNIQUE(user_id, cache_key)
);

CREATE INDEX idx_api_cache_user_key ON api_cache(user_id, cache_key);
CREATE INDEX idx_api_cache_expires ON api_cache(expires_at);
CREATE INDEX idx_api_cache_source ON api_cache(api_source);

-- Cleanup expired cache entries (run via cron)
CREATE OR REPLACE FUNCTION cleanup_expired_cache()
RETURNS void AS $$
BEGIN
    DELETE FROM api_cache WHERE expires_at < now();
END;
$$ LANGUAGE plpgsql;

-- Algorithm update database
CREATE TABLE algorithm_updates (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    name TEXT NOT NULL,
    type TEXT,                            -- 'core', 'spam', 'helpful_content', 'link', 'local', etc.
    source TEXT,                          -- 'semrush', 'moz', 'search_engine_roundtable', 'google_official'
    description TEXT,
    severity TEXT,                        -- 'major', 'minor', 'unknown'
    created_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(date, name)
);

CREATE INDEX idx_algorithm_updates_date ON algorithm_updates(date DESC);
CREATE INDEX idx_algorithm_updates_type ON algorithm_updates(type);
CREATE INDEX idx_algorithm_updates_name ON algorithm_updates(name);

-- Query intent classification cache
CREATE TABLE query_intents (
    query_hash TEXT PRIMARY KEY,          -- MD5 or SHA256 hash of normalized query
    query TEXT NOT NULL,
    intent TEXT NOT NULL CHECK (intent IN ('informational', 'commercial', 'navigational', 'transactional')),
    confidence FLOAT CHECK (confidence >= 0 AND confidence <= 1),
    model_version TEXT DEFAULT 'claude-3-opus-20240229',
    classified_at TIMESTAMPTZ DEFAULT now(),
    classification_prompt TEXT            -- store the prompt used for reproducibility
);

CREATE INDEX idx_query_intents_query ON query_intents(query);
CREATE INDEX idx_query_intents_intent ON query_intents(intent);
CREATE INDEX idx_query_intents_classified_at ON query_intents(classified_at DESC);

-- SERP snapshots (for historical comparison on re-runs)
CREATE TABLE serp_snapshots (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID REFERENCES users(id) ON DELETE CASCADE,
    keyword TEXT NOT NULL,
    snapshot_date DATE NOT NULL,
    serp_data JSONB NOT NULL,             -- full DataForSEO response
    organic_results JSONB,                -- extracted organic results for quick access
    serp_features JSONB,                  -- extracted SERP features list
    created_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(user_id, keyword, snapshot_date)
);

CREATE INDEX idx_serp_snapshots_user_keyword ON serp_snapshots(user_id, keyword);
CREATE INDEX idx_serp_snapshots_date ON serp_snapshots(snapshot_date DESC);
CREATE INDEX idx_serp_snapshots_keyword ON serp_snapshots(keyword);

-- Build log (autoresearch program tracking)
CREATE TABLE search_intel_build_log (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ DEFAULT now(),
    task TEXT NOT NULL,
    status TEXT NOT NULL,
    notes TEXT,
    score_before INTEGER,
    score_after INTEGER,
    commit_sha TEXT,
    commit_url TEXT
);

-- Row Level Security (RLS) policies
ALTER TABLE users ENABLE ROW LEVEL SECURITY;
ALTER TABLE reports ENABLE ROW LEVEL SECURITY;
ALTER TABLE report_modules ENABLE ROW LEVEL SECURITY;
ALTER TABLE api_cache ENABLE ROW LEVEL SECURITY;
ALTER TABLE serp_snapshots ENABLE ROW LEVEL SECURITY;
ALTER TABLE algorithm_updates ENABLE ROW LEVEL SECURITY;
ALTER TABLE query_intents ENABLE ROW LEVEL SECURITY;

-- Permissive policies (API uses service role key which bypasses RLS,
-- but these ensure anon/authenticated keys also work for the app).
CREATE POLICY users_all ON users FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY reports_all ON reports FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY report_modules_all ON report_modules FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY api_cache_all ON api_cache FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY serp_snapshots_all ON serp_snapshots FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY algorithm_updates_select_all ON algorithm_updates FOR SELECT USING (true);
CREATE POLICY query_intents_select_all ON query_intents FOR SELECT USING (true);
CREATE POLICY query_intents_all ON query_intents FOR ALL USING (true) WITH CHECK (true);

-- Seed algorithm updates with known 2024-2026 updates
INSERT INTO algorithm_updates (date, name, type, source, description, severity) VALUES
    ('2024-03-05', 'March 2024 Core Update', 'core', 'google_official', 'Broad core algorithm update rolling out over 2-3 weeks', 'major'),
    ('2024-04-16', 'April 2024 Reviews Update', 'helpful_content', 'google_official', 'Update targeting review content quality', 'minor'),
    ('2024-06-20', 'June 2024 Spam Update', 'spam', 'google_official', 'Spam detection improvements', 'minor'),
    ('2024-08-15', 'August 2024 Core Update', 'core', 'google_official', 'Broad core algorithm update', 'major'),
    ('2024-11-11', 'November 2024 Core Update', 'core', 'google_official', 'Broad core algorithm update with focus on content quality', 'major'),
    ('2025-03-04', 'March 2025 Core Update', 'core', 'google_official', 'Broad core algorithm update', 'major'),
    ('2025-06-10', 'June 2025 Helpful Content Update', 'helpful_content', 'google_official', 'Refinements to helpful content system', 'minor'),
    ('2025-11-08', 'November 2025 Core Update', 'core', 'google_official', 'Broad core algorithm update', 'major'),
    ('2026-02-12', 'February 2026 Link Spam Update', 'link', 'google_official', 'Link spam detection improvements', 'minor')
ON CONFLICT (date, name) DO NOTHING;

-- Utility function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Triggers for auto-updating updated_at
CREATE TRIGGER update_users_updated_at BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_reports_updated_at BEFORE UPDATE ON reports
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Comments for documentation
COMMENT ON TABLE users IS 'User accounts with encrypted OAuth tokens for GSC and GA4';
COMMENT ON TABLE reports IS 'Generated search intelligence reports with job status tracking';
COMMENT ON TABLE report_modules IS 'Individual module results per report — stores output from each of the 12 analysis modules';
COMMENT ON TABLE api_cache IS 'Cached API responses with 24h TTL to reduce API calls';
COMMENT ON TABLE algorithm_updates IS 'Google algorithm updates database for change point attribution';
COMMENT ON TABLE query_intents IS 'LLM-classified query intents cache to avoid repeated classification';
COMMENT ON TABLE serp_snapshots IS 'Historical SERP data for competitive tracking and change detection';
COMMENT ON TABLE search_intel_build_log IS 'Build agent execution log for automated codebase maintenance';
