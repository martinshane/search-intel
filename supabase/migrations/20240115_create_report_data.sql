-- Create report_data table to store individual module results for each report
CREATE TABLE IF NOT EXISTS report_data (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    report_id UUID NOT NULL REFERENCES reports(id) ON DELETE CASCADE,
    module_number INTEGER NOT NULL CHECK (module_number BETWEEN 1 AND 12),
    module_name TEXT NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    CONSTRAINT unique_report_module UNIQUE (report_id, module_number)
);

-- Index on report_id for fast lookups of all modules for a given report
CREATE INDEX idx_report_data_report_id ON report_data(report_id);

-- Index on module_number for filtering by specific modules
CREATE INDEX idx_report_data_module_number ON report_data(module_number);

-- Composite index for report_id + module_number lookups
CREATE INDEX idx_report_data_report_module ON report_data(report_id, module_number);

-- Enable Row Level Security
ALTER TABLE report_data ENABLE ROW LEVEL SECURITY;

-- Policy: Users can only access report_data for their own reports
CREATE POLICY report_data_access_policy ON report_data
    FOR ALL
    USING (
        report_id IN (
            SELECT id FROM reports WHERE user_id = auth.uid()
        )
    );

-- Trigger to automatically update updated_at timestamp
CREATE OR REPLACE FUNCTION update_report_data_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_report_data_updated_at
    BEFORE UPDATE ON report_data
    FOR EACH ROW
    EXECUTE FUNCTION update_report_data_updated_at();

-- Add comment to table for documentation
COMMENT ON TABLE report_data IS 'Stores individual module results for each report. Each module (1-12) stores its analysis results as JSONB data.';
COMMENT ON COLUMN report_data.module_number IS 'Module number (1-12): 1=Health&Trajectory, 2=PageTriage, 3=SERPLandscape, 4=ContentIntelligence, 5=Gameplan, 6=AlgorithmImpact, 7=QueryIntentMigration, 8=InternalLinkGraph, 9=CompetitorGapAnalysis, 10=SeasonalityForecast, 11=ConversionPathways, 12=TechnicalSEOAudit';
COMMENT ON COLUMN report_data.data IS 'JSONB object containing the structured results from the module analysis';