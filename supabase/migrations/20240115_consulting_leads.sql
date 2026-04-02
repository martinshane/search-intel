-- consulting_leads table migration
-- Created: 2024-01-15
-- Purpose: Store email leads captured from consulting CTA on report page

CREATE TABLE IF NOT EXISTS consulting_leads (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email TEXT NOT NULL,
    report_id UUID REFERENCES reports(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Index for faster lookups by report
CREATE INDEX IF NOT EXISTS idx_consulting_leads_report_id ON consulting_leads(report_id);

-- Index for faster lookups by email
CREATE INDEX IF NOT EXISTS idx_consulting_leads_email ON consulting_leads(email);

-- Enable Row Level Security
ALTER TABLE consulting_leads ENABLE ROW LEVEL SECURITY;

-- Policy: Allow authenticated users to insert consulting leads
CREATE POLICY "Allow authenticated users to insert consulting leads"
    ON consulting_leads
    FOR INSERT
    TO authenticated
    WITH CHECK (true);

-- Policy: Users can only read their own consulting leads
-- (based on reports they own)
CREATE POLICY "Users can read their own consulting leads"
    ON consulting_leads
    FOR SELECT
    TO authenticated
    USING (
        report_id IN (
            SELECT id FROM reports WHERE user_id = auth.uid()
        )
    );

-- Policy: Allow public inserts for unauthenticated users who are submitting leads
-- This is safe because we're just collecting emails, not exposing data
CREATE POLICY "Allow public to insert consulting leads"
    ON consulting_leads
    FOR INSERT
    TO anon
    WITH CHECK (true);

-- Add comment to table
COMMENT ON TABLE consulting_leads IS 'Stores email leads captured from consulting CTA on report pages';

-- Add comments to columns
COMMENT ON COLUMN consulting_leads.id IS 'Unique identifier for the consulting lead';
COMMENT ON COLUMN consulting_leads.email IS 'Email address of the potential consulting client';
COMMENT ON COLUMN consulting_leads.report_id IS 'Reference to the report that generated this lead';
COMMENT ON COLUMN consulting_leads.created_at IS 'Timestamp when the lead was captured';