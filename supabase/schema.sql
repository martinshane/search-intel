-- Search Intelligence Report — Supabase Schema
-- Created: 2026-03-29
-- DO NOT MODIFY after initial deployment — downstream modules depend on this structure

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
    gsc_property TEXT NOT NULL,
    ga4_property TEXT,
    status TEXT DEFAULT 'pending' CHECK (status IN ('pending', 'ingesting', 'analyzing', 'generating', 'complete', 'failed')),
    progress JSONB DEFAULT '{}'::JSONB,  -- {"module_1": "complete", "module_2": "running", ...}
    report_data JSONB,                    -- complete report output
    error_message TEXT,                   -- if status = failed, why
    created_at TIMESTAMPTZ DEFAULT now(),
    completed_at TIMESTAMPTZ
);

CREATE INDEX idx_reports_user_id ON reports(user_id);
CREATE INDEX idx_reports_status ON reports(status);
CREATE INDEX idx_reports_created_at ON reports(created_at DESC);
CREATE INDEX idx_reports_user_status ON reports(user_id, status);

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
CREATE TABLE build_log (
    id SERIAL PRIMARY KEY,
    day_number INTEGER NOT NULL,
    run_date DATE NOT NULL DEFAULT CURRENT_DATE,
    task_name TEXT NOT NULL,
    task_description TEXT,
    status TEXT NOT NULL CHECK (status IN ('pass', 'fail', 'skipped')),
    notes TEXT,                           -- detailed notes on what was built, decisions made
    failure_reason TEXT,                  -- if status = fail, exactly why
    shrunk_task TEXT,                     -- if status = fail, smaller scope to attempt tomorrow
    files_changed JSONB,                  -- array of file paths modified
    commit_hash TEXT,                     -- git commit SHA
    commit_message TEXT,
    railway_deploy_status TEXT,           -- 'success', 'failed', 'pending', 'not_applicable'
    railway_deploy_url TEXT,
    tests_passed BOOLEAN,
    tests_output TEXT,
    execution_time_seconds INTEGER,
    created_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(day_number)
);

CREATE INDEX idx_build_log_day ON build_log(day_number DESC);
CREATE INDEX idx_build_log_date ON build_log(run_date DESC);
CREATE INDEX idx_build_log_status ON build_log(status);

-- Row Level Security (RLS) policies
-- Enable RLS on all tables
ALTER TABLE users ENABLE ROW LEVEL SECURITY;
ALTER TABLE reports ENABLE ROW LEVEL SECURITY;
ALTER TABLE api_cache ENABLE ROW LEVEL SECURITY;
ALTER TABLE serp_snapshots ENABLE ROW LEVEL SECURITY;

-- Users can only see their own data
CREATE POLICY users_select_own ON users
    FOR SELECT USING (auth.uid() = id);

CREATE POLICY users_update_own ON users
    FOR UPDATE USING (auth.uid() = id);

-- Reports policies
CREATE POLICY reports_select_own ON reports
    FOR SELECT USING (user_id = auth.uid());

CREATE POLICY reports_insert_own ON reports
    FOR INSERT WITH CHECK (user_id = auth.uid());

CREATE POLICY reports_update_own ON reports
    FOR UPDATE USING (user_id = auth.uid());

-- API cache policies
CREATE POLICY api_cache_select_own ON api_cache
    FOR SELECT USING (user_id = auth.uid());

CREATE POLICY api_cache_insert_own ON api_cache
    FOR INSERT WITH CHECK (user_id = auth.uid());

CREATE POLICY api_cache_delete_own ON api_cache
    FOR DELETE USING (user_id = auth.uid());

-- SERP snapshots policies
CREATE POLICY serp_snapshots_select_own ON serp_snapshots
    FOR SELECT USING (user_id = auth.uid());

CREATE POLICY serp_snapshots_insert_own ON serp_snapshots
    FOR INSERT WITH CHECK (user_id = auth.uid());

-- Public read access for algorithm_updates, query_intents, build_log
-- (no user_id column, these are shared data)
ALTER TABLE algorithm_updates ENABLE ROW LEVEL SECURITY;
CREATE POLICY algorithm_updates_select_all ON algorithm_updates
    FOR SELECT USING (true);

ALTER TABLE query_intents ENABLE ROW LEVEL SECURITY;
CREATE POLICY query_intents_select_all ON query_intents
    FOR SELECT USING (true);

ALTER TABLE build_log ENABLE ROW LEVEL SECURITY;
CREATE POLICY build_log_select_all ON build_log
    FOR SELECT USING (true);

-- Service role can do anything (for backend API)
-- This is configured in Supabase dashboard, not SQL

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

-- Trigger to auto-update updated_at on users table
CREATE TRIGGER update_users_updated_at BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Comments for documentation
COMMENT ON TABLE users IS 'User accounts with encrypted OAuth tokens for GSC and GA4';
COMMENT ON TABLE reports IS 'Generated search intelligence reports with job status tracking';
COMMENT ON TABLE api_cache IS 'Cached API responses with 24h TTL to reduce API calls';
COMMENT ON TABLE algorithm_updates IS 'Google algorithm updates database for change point attribution';
COMMENT ON TABLE query_intents IS 'LLM-classified query intents cache to avoid repeated classification';
COMMENT ON TABLE serp_snapshots IS 'Historical SERP data for competitive tracking and change detection';
COMMENT ON TABLE build_log IS 'Autoresearch program execution log for nightly build tracking';

COMMENT ON COLUMN reports.progress IS 'JSON object tracking which modules have completed: {"module_1": "complete", "module_2": "running"}';
COMMENT ON COLUMN reports.report_data IS 'Complete structured report output from all 12 analysis modules';
COMMENT ON COLUMN api_cache.cache_key IS 'MD5/SHA256 hash of API call parameters for deduplication';
COMMENT ON COLUMN query_intents.query_hash IS 'Hash of normalized query (lowercase, trimmed) for fast lookup';
COMMENT ON COLUMN build_log.files_changed IS 'Array of relative file paths modified in this build: ["api/main.py", "web/pages/index.tsx"]';