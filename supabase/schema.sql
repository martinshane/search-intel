-- Search Intelligence Report — Supabase Schema
-- Run once. Never modify after first run.

-- User accounts and OAuth tokens
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email TEXT UNIQUE NOT NULL,
    gsc_token JSONB,
    ga4_token JSONB,
    created_at TIMESTAMPTZ DEFAULT now()
);

-- Generated reports
CREATE TABLE IF NOT EXISTS reports (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID REFERENCES users(id),
    gsc_property TEXT NOT NULL,
    ga4_property TEXT,
    status TEXT DEFAULT 'pending',
    progress JSONB DEFAULT '{}',
    report_data JSONB,
    created_at TIMESTAMPTZ DEFAULT now(),
    completed_at TIMESTAMPTZ
);

-- Cached API responses (24h TTL)
CREATE TABLE IF NOT EXISTS api_cache (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID REFERENCES users(id),
    cache_key TEXT NOT NULL,
    response JSONB NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    UNIQUE(user_id, cache_key)
);

-- Algorithm update database
CREATE TABLE IF NOT EXISTS algorithm_updates (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    name TEXT NOT NULL,
    type TEXT,
    source TEXT,
    description TEXT
);

-- Query intent classification cache
CREATE TABLE IF NOT EXISTS query_intents (
    query_hash TEXT PRIMARY KEY,
    query TEXT NOT NULL,
    intent TEXT NOT NULL,
    confidence FLOAT,
    classified_at TIMESTAMPTZ DEFAULT now()
);

-- SERP snapshots
CREATE TABLE IF NOT EXISTS serp_snapshots (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    keyword TEXT NOT NULL,
    snapshot_date DATE NOT NULL,
    serp_data JSONB NOT NULL,
    UNIQUE(keyword, snapshot_date)
);

-- Autoresearch nightly build log
CREATE TABLE IF NOT EXISTS build_log (
    id SERIAL PRIMARY KEY,
    day INTEGER,
    run_date DATE NOT NULL DEFAULT CURRENT_DATE,
    task TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pass', 'fail', 'skip')),
    notes TEXT,
    commit_url TEXT,
    duration_seconds INTEGER,
    created_at TIMESTAMPTZ DEFAULT now()
);
