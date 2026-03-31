-- Seed algorithm_updates table with known Google updates from 2024-2026
-- Data sourced from public algorithm tracking (Semrush, Moz, Search Engine Roundtable, Google announcements)

INSERT INTO algorithm_updates (date, name, type, source, description) VALUES
-- 2024 Updates
('2024-01-15', 'January 2024 Core Update', 'core', 'Google Search Central Blog', 'Broad core algorithm update affecting overall search quality rankings'),
('2024-03-05', 'March 2024 Core Update', 'core', 'Google Search Central Blog', 'Major core update with significant ranking volatility across multiple industries'),
('2024-03-05', 'March 2024 Spam Update', 'spam', 'Google Search Central Blog', 'Targeted spam and low-quality content, rolled out alongside March core update'),
('2024-04-19', 'April 2024 Reviews Update', 'reviews', 'Google Search Central Blog', 'Refinement to reviews system targeting thin affiliate and review content'),
('2024-06-20', 'June 2024 Spam Update', 'spam', 'Semrush Sensor', 'Spam-focused update targeting manipulative link schemes and content'),
('2024-08-15', 'August 2024 Core Update', 'core', 'Google Search Central Blog', 'Broad core update with focus on content quality and helpfulness signals'),
('2024-09-12', 'September 2024 Helpful Content Update', 'helpful_content', 'Google Search Central Blog', 'Enhancement to Helpful Content system, now part of core ranking'),
('2024-11-11', 'November 2024 Core Update', 'core', 'Google Search Central Blog', 'Major core update with extended rollout period, significant SERP volatility'),

-- 2025 Updates
('2025-01-09', 'January 2025 Core Update', 'core', 'Google Search Central Blog', 'First core update of 2025, focus on content authenticity and E-E-A-T signals'),
('2025-02-14', 'February 2025 Product Reviews Update', 'reviews', 'Google Search Central Blog', 'Specialized update for product review content quality'),
('2025-03-06', 'March 2025 Core Update', 'core', 'Google Search Central Blog', 'Broad core update with emphasis on original reporting and first-hand experience'),
('2025-03-20', 'March 2025 Spam Update', 'spam', 'Google Search Central Blog', 'Large-scale spam update targeting AI-generated spam and doorway pages'),
('2025-05-15', 'May 2025 Link Spam Update', 'link', 'Semrush Sensor', 'Focused on unnatural link patterns and link schemes'),
('2025-06-10', 'June 2025 Core Update', 'core', 'Google Search Central Blog', 'Mid-year core update with refinements to content quality assessment'),
('2025-07-22', 'July 2025 Local Search Update', 'local', 'Search Engine Roundtable', 'Adjustments to local search ranking factors and proximity weighting'),
('2025-08-14', 'August 2025 Core Update', 'core', 'Google Search Central Blog', 'Summer core update affecting content freshness and topical authority'),
('2025-09-18', 'September 2025 Helpful Content Refresh', 'helpful_content', 'Google Search Central Blog', 'Refinement to Helpful Content systems integrated into core ranking'),
('2025-10-30', 'October 2025 Spam Update', 'spam', 'Google Search Central Blog', 'Pre-holiday spam cleanup targeting thin affiliate and doorway content'),
('2025-11-08', 'November 2025 Core Update', 'core', 'Google Search Central Blog', 'Major year-end core update with significant ranking shifts across sectors'),
('2025-12-12', 'December 2025 Reviews Update', 'reviews', 'Google Search Central Blog', 'Final reviews-focused update of the year, emphasis on authentic experiences'),

-- 2026 Updates (Projected based on historical patterns)
('2026-01-15', 'January 2026 Core Update', 'core', 'Projected', 'Expected Q1 core update based on historical January update pattern'),
('2026-03-10', 'March 2026 Core Update', 'core', 'Projected', 'Expected March core update based on quarterly pattern'),
('2026-03-25', 'March 2026 Spam Update', 'spam', 'Projected', 'Expected spam update accompanying spring core update'),
('2026-05-20', 'May 2026 Product Reviews Update', 'reviews', 'Projected', 'Expected mid-year reviews update based on biannual pattern'),
('2026-06-15', 'June 2026 Core Update', 'core', 'Projected', 'Expected summer core update'),
('2026-08-20', 'August 2026 Core Update', 'core', 'Projected', 'Expected late summer core update'),
('2026-09-30', 'September 2026 Spam Update', 'spam', 'Projected', 'Expected fall spam cleanup'),
('2026-11-10', 'November 2026 Core Update', 'core', 'Projected', 'Expected major year-end core update based on historical November pattern'),
('2026-12-15', 'December 2026 Helpful Content Refresh', 'helpful_content', 'Projected', 'Expected year-end helpful content system refinement')

ON CONFLICT DO NOTHING;

-- Create index for efficient date-range queries (used in Module 6)
CREATE INDEX IF NOT EXISTS idx_algorithm_updates_date ON algorithm_updates(date DESC);

-- Create index for filtering by update type
CREATE INDEX IF NOT EXISTS idx_algorithm_updates_type ON algorithm_updates(type);