import { useEffect, useState, useCallback, useRef } from 'react';
import { useRouter } from 'next/router';
import Head from 'next/head';
import dynamic from 'next/dynamic';

// D3 force-directed network graph — client-only (SSR-disabled)
const NetworkGraph = dynamic(() => import('../../components/NetworkGraph'), {
  ssr: false,
  loading: () => (
    <div className="flex items-center justify-center h-48 bg-gray-50 rounded-lg border border-dashed border-gray-300 text-sm text-gray-500">
      Loading network graph...
    </div>
  ),
});
import {
  LineChart,
  Line,
  ScatterChart,
  Scatter,
  BarChart,
  Bar,
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  ReferenceLine,
  Cell,
} from 'recharts';
import { Menu, X, ChevronDown, ChevronUp, ExternalLink, Download, Calendar, TrendingUp, TrendingDown, Minus, AlertTriangle, CheckCircle, Target, Zap, RefreshCw, Clock, BarChart2, Shield, Globe, DollarSign, Search, FileText, Activity, Layers, Users } from 'lucide-react';
import NavHeader from '../../components/NavHeader';
import Module5IndexingCoverage from '../../components/Module5IndexingCoverage';

// ---------------------------------------------------------------------------
// Config — API base URL from env, falls back to relative path
// ---------------------------------------------------------------------------
const API_BASE = process.env.NEXT_PUBLIC_API_URL || '';
const POLL_INTERVAL_MS = 5000;
const TERMINAL_STATUSES = new Set(['completed', 'complete', 'partial', 'failed']);

// ---------------------------------------------------------------------------
// Module metadata for rendering section headers
// ---------------------------------------------------------------------------
const MODULE_META: Record<string, { title: string; icon: string; number: number }> = {
  health_trajectory:    { title: 'Health & Trajectory',     icon: 'activity',    number: 1  },
  page_triage:          { title: 'Page-Level Triage',       icon: 'target',      number: 2  },
  serp_landscape:       { title: 'SERP Landscape',          icon: 'search',      number: 3  },
  content_intelligence: { title: 'Content Intelligence',    icon: 'file-text',   number: 4  },
  indexing_coverage:    { title: 'Indexing & Coverage',     icon: 'layers',      number: 5  },
  gameplan:             { title: 'The Gameplan',            icon: 'zap',         number: 6  },
  algorithm_impact:     { title: 'Algorithm Impact',        icon: 'shield',      number: 7  },
  intent_migration:     { title: 'Intent Migration',        icon: 'layers',      number: 8  },
  technical_health:     { title: 'CTR Modeling',            icon: 'bar-chart',   number: 9  },
  site_architecture:    { title: 'Site Architecture',       icon: 'globe',       number: 10 },
  branded_split:        { title: 'Branded vs Non-Branded',  icon: 'users',       number: 11 },
  competitive_threats:  { title: 'Competitive Radar',       icon: 'users',       number: 12 },
  revenue_attribution:  { title: 'Revenue Attribution',     icon: 'dollar',      number: 13 },
};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------
interface ReportData {
  report_id: string;
  domain: string;
  status: string;
  generated_at?: string;
  summary?: {
    monthly_clicks?: number;
    trend?: string;
    critical_pages?: number;
    total_estimated_recovery?: number;
  };
  modules?: Record<string, any>;
}

// ---------------------------------------------------------------------------
// Main Report Page Component
// ---------------------------------------------------------------------------
export default function ReportPage() {
  const router = useRouter();
  const { id } = router.query;

  const [report, setReport] = useState<ReportData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [expandedSections, setExpandedSections] = useState<Set<string>>(new Set());
  const pollTimeoutRef = useRef<NodeJS.Timeout | null>(null);

  // ---------------------------------------------------------------------------
  // Fetch report data from API
  // ---------------------------------------------------------------------------
  const fetchReport = useCallback(async () => {
    if (!id) return;

    try {
      const response = await fetch(`${API_BASE}/api/report/${id}`);
      if (!response.ok) {
        if (response.status === 404) {
          setError('Report not found');
          setLoading(false);
          return;
        }
        throw new Error(`Failed to fetch report: ${response.statusText}`);
      }

      const data: ReportData = await response.json();
      setReport(data);
      setError(null);

      // If report is still processing, continue polling
      if (!TERMINAL_STATUSES.has(data.status)) {
        pollTimeoutRef.current = setTimeout(fetchReport, POLL_INTERVAL_MS);
      } else {
        setLoading(false);
      }
    } catch (err) {
      console.error('Error fetching report:', err);
      setError(err instanceof Error ? err.message : 'An error occurred');
      setLoading(false);
    }
  }, [id]);

  // ---------------------------------------------------------------------------
  // Initial fetch + polling setup
  // ---------------------------------------------------------------------------
  useEffect(() => {
    fetchReport();
    return () => {
      if (pollTimeoutRef.current) {
        clearTimeout(pollTimeoutRef.current);
      }
    };
  }, [fetchReport]);

  // ---------------------------------------------------------------------------
  // Section toggle handler
  // ---------------------------------------------------------------------------
  const toggleSection = (sectionKey: string) => {
    setExpandedSections((prev) => {
      const next = new Set(prev);
      if (next.has(sectionKey)) {
        next.delete(sectionKey);
      } else {
        next.add(sectionKey);
      }
      return next;
    });
  };

  // ---------------------------------------------------------------------------
  // Loading State
  // ---------------------------------------------------------------------------
  if (loading) {
    return (
      <>
        <Head>
          <title>Generating Report | Search Intelligence</title>
        </Head>
        <NavHeader />
        <main className="min-h-screen bg-gray-50 py-12 px-4">
          <div className="max-w-4xl mx-auto text-center">
            <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-12">
              <RefreshCw className="w-16 h-16 text-blue-600 mx-auto mb-6 animate-spin" />
              <h1 className="text-2xl font-bold text-gray-900 mb-3">
                Generating Your Report
              </h1>
              <p className="text-gray-600 mb-6">
                {report?.status === 'processing'
                  ? 'Analyzing your site data. This typically takes 2-5 minutes...'
                  : 'Initializing report generation...'}
              </p>
              {report?.domain && (
                <p className="text-sm text-gray-500">
                  Domain: <span className="font-mono">{report.domain}</span>
                </p>
              )}
            </div>
          </div>
        </main>
      </>
    );
  }

  // ---------------------------------------------------------------------------
  // Error State
  // ---------------------------------------------------------------------------
  if (error || !report) {
    return (
      <>
        <Head>
          <title>Error | Search Intelligence</title>
        </Head>
        <NavHeader />
        <main className="min-h-screen bg-gray-50 py-12 px-4">
          <div className="max-w-4xl mx-auto text-center">
            <div className="bg-white rounded-lg shadow-sm border border-red-200 p-12">
              <AlertTriangle className="w-16 h-16 text-red-600 mx-auto mb-6" />
              <h1 className="text-2xl font-bold text-gray-900 mb-3">
                Unable to Load Report
              </h1>
              <p className="text-gray-600 mb-6">
                {error || 'Report data could not be retrieved'}
              </p>
              <button
                onClick={() => router.push('/connect')}
                className="px-6 py-3 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition"
              >
                Generate New Report
              </button>
            </div>
          </div>
        </main>
      </>
    );
  }

  // ---------------------------------------------------------------------------
  // Report Failed State
  // ---------------------------------------------------------------------------
  if (report.status === 'failed') {
    return (
      <>
        <Head>
          <title>Report Failed | Search Intelligence</title>
        </Head>
        <NavHeader />
        <main className="min-h-screen bg-gray-50 py-12 px-4">
          <div className="max-w-4xl mx-auto text-center">
            <div className="bg-white rounded-lg shadow-sm border border-yellow-200 p-12">
              <AlertTriangle className="w-16 h-16 text-yellow-600 mx-auto mb-6" />
              <h1 className="text-2xl font-bold text-gray-900 mb-3">
                Report Generation Failed
              </h1>
              <p className="text-gray-600 mb-6">
                We encountered an error while generating your report for{' '}
                <span className="font-mono">{report.domain}</span>
              </p>
              <button
                onClick={() => router.push('/connect')}
                className="px-6 py-3 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition"
              >
                Try Again
              </button>
            </div>
          </div>
        </main>
      </>
    );
  }

  // ---------------------------------------------------------------------------
  // Main Report View
  // ---------------------------------------------------------------------------
  const modules = report.modules || {};
  const summary = report.summary || {};

  return (
    <>
      <Head>
        <title>{report.domain} | Search Intelligence Report</title>
        <meta
          name="description"
          content={`Comprehensive search intelligence analysis for ${report.domain}`}
        />
      </Head>

      <NavHeader />

      <main className="min-h-screen bg-gray-50">
        {/* Hero Header */}
        <div className="bg-gradient-to-br from-blue-600 via-blue-700 to-indigo-800 text-white">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
            <div className="flex items-center justify-between mb-6">
              <div className="flex items-center space-x-3">
                <div className="w-12 h-12 bg-white/20 rounded-lg flex items-center justify-center backdrop-blur-sm">
                  <BarChart2 className="w-7 h-7" />
                </div>
                <div>
                  <h1 className="text-3xl font-bold">{report.domain}</h1>
                  <p className="text-blue-100 text-sm mt-1">
                    Search Intelligence Report
                  </p>
                </div>
              </div>
              <button
                onClick={() => window.print()}
                className="flex items-center space-x-2 px-4 py-2 bg-white/10 hover:bg-white/20 rounded-lg transition backdrop-blur-sm"
              >
                <Download className="w-4 h-4" />
                <span className="text-sm font-medium">Export</span>
              </button>
            </div>

            {/* Summary Cards */}
            <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
              <SummaryCard
                icon={<Activity className="w-5 h-5" />}
                label="Monthly Clicks"
                value={summary.monthly_clicks?.toLocaleString() || 'N/A'}
                trend={summary.trend}
              />
              <SummaryCard
                icon={<TrendingUp className="w-5 h-5" />}
                label="Trend"
                value={summary.trend || 'Unknown'}
                className="capitalize"
              />
              <SummaryCard
                icon={<AlertTriangle className="w-5 h-5" />}
                label="Critical Pages"
                value={summary.critical_pages?.toString() || '0'}
              />
              <SummaryCard
                icon={<Target className="w-5 h-5" />}
                label="Recovery Potential"
                value={
                  summary.total_estimated_recovery
                    ? `${summary.total_estimated_recovery.toLocaleString()} clicks/mo`
                    : 'N/A'
                }
              />
            </div>
          </div>
        </div>

        {/* Module Sections */}
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8 space-y-6">
          {/* Module 1: Health & Trajectory */}
          {modules.health_trajectory && (
            <ModuleSection
              moduleKey="health_trajectory"
              expanded={expandedSections.has('health_trajectory')}
              onToggle={() => toggleSection('health_trajectory')}
            >
              <HealthTrajectoryContent data={modules.health_trajectory} />
            </ModuleSection>
          )}

          {/* Module 2: Page-Level Triage */}
          {modules.page_triage && (
            <ModuleSection
              moduleKey="page_triage"
              expanded={expandedSections.has('page_triage')}
              onToggle={() => toggleSection('page_triage')}
            >
              <PageTriageContent data={modules.page_triage} />
            </ModuleSection>
          )}

          {/* Module 3: SERP Landscape */}
          {modules.serp_landscape && (
            <ModuleSection
              moduleKey="serp_landscape"
              expanded={expandedSections.has('serp_landscape')}
              onToggle={() => toggleSection('serp_landscape')}
            >
              <SerpLandscapeContent data={modules.serp_landscape} />
            </ModuleSection>
          )}

          {/* Module 4: Content Intelligence */}
          {modules.content_intelligence && (
            <ModuleSection
              moduleKey="content_intelligence"
              expanded={expandedSections.has('content_intelligence')}
              onToggle={() => toggleSection('content_intelligence')}
            >
              <ContentIntelligenceContent data={modules.content_intelligence} />
            </ModuleSection>
          )}

          {/* Module 5: Indexing & Coverage */}
          {modules.indexing_coverage && (
            <ModuleSection
              moduleKey="indexing_coverage"
              expanded={expandedSections.has('indexing_coverage')}
              onToggle={() => toggleSection('indexing_coverage')}
            >
              <Module5IndexingCoverage data={modules.indexing_coverage} />
            </ModuleSection>
          )}

          {/* Module 6: The Gameplan */}
          {modules.gameplan && (
            <ModuleSection
              moduleKey="gameplan"
              expanded={expandedSections.has('gameplan')}
              onToggle={() => toggleSection('gameplan')}
            >
              <GameplanContent data={modules.gameplan} />
            </ModuleSection>
          )}

          {/* Module 7: Algorithm Impact */}
          {modules.algorithm_impact && (
            <ModuleSection
              moduleKey="algorithm_impact"
              expanded={expandedSections.has('algorithm_impact')}
              onToggle={() => toggleSection('algorithm_impact')}
            >
              <AlgorithmImpactContent data={modules.algorithm_impact} />
            </ModuleSection>
          )}

          {/* Module 8: Intent Migration */}
          {modules.intent_migration && (
            <ModuleSection
              moduleKey="intent_migration"
              expanded={expandedSections.has('intent_migration')}
              onToggle={() => toggleSection('intent_migration')}
            >
              <IntentMigrationContent data={modules.intent_migration} />
            </ModuleSection>
          )}

          {/* Module 9: Technical Health */}
          {modules.technical_health && (
            <ModuleSection
              moduleKey="technical_health"
              expanded={expandedSections.has('technical_health')}
              onToggle={() => toggleSection('technical_health')}
            >
              <TechnicalHealthContent data={modules.technical_health} />
            </ModuleSection>
          )}

          {/* Module 10: Site Architecture */}
          {modules.site_architecture && (
            <ModuleSection
              moduleKey="site_architecture"
              expanded={expandedSections.has('site_architecture')}
              onToggle={() => toggleSection('site_architecture')}
            >
              <SiteArchitectureContent data={modules.site_architecture} />
            </ModuleSection>
          )}

          {/* Module 11: Branded Split */}
          {modules.branded_split && (
            <ModuleSection
              moduleKey="branded_split"
              expanded={expandedSections.has('branded_split')}
              onToggle={() => toggleSection('branded_split')}
            >
              <BrandedSplitContent data={modules.branded_split} />
            </ModuleSection>
          )}

          {/* Module 12: Competitive Threats */}
          {modules.competitive_threats && (
            <ModuleSection
              moduleKey="competitive_threats"
              expanded={expandedSections.has('competitive_threats')}
              onToggle={() => toggleSection('competitive_threats')}
            >
              <CompetitiveThreatsContent data={modules.competitive_threats} />
            </ModuleSection>
          )}

          {/* Module 13: Revenue Attribution */}
          {modules.revenue_attribution && (
            <ModuleSection
              moduleKey="revenue_attribution"
              expanded={expandedSections.has('revenue_attribution')}
              onToggle={() => toggleSection('revenue_attribution')}
            >
              <RevenueAttributionContent data={modules.revenue_attribution} />
            </ModuleSection>
          )}
        </div>

        {/* Footer CTA */}
        <div className="bg-gradient-to-r from-blue-600 to-indigo-700 text-white">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-12 text-center">
            <h2 className="text-3xl font-bold mb-4">
              Ready to Execute This Strategy?
            </h2>
            <p className="text-blue-100 mb-8 text-lg max-w-2xl mx-auto">
              This report identifies opportunities worth{' '}
              <span className="font-bold text-white">
                {summary.total_estimated_recovery?.toLocaleString() || 'thousands of'}
              </span>{' '}
              clicks per month. Let's make it happen.
            </p>
            <button
              onClick={() => router.push('/contact')}
              className="px-8 py-4 bg-white text-blue-600 rounded-lg font-semibold hover:bg-blue-50 transition text-lg shadow-lg"
            >
              Work With Us
            </button>
          </div>
        </div>
      </main>
    </>
  );
}

// ---------------------------------------------------------------------------
// Component: Summary Card
// ---------------------------------------------------------------------------
interface SummaryCardProps {
  icon: React.ReactNode;
  label: string;
  value: string;
  trend?: string;
  className?: string;
}

function SummaryCard({ icon, label, value, trend, className = '' }: SummaryCardProps) {
  return (
    <div className="bg-white/10 backdrop-blur-sm rounded-lg p-4 border border-white/20">
      <div className="flex items-center space-x-3 mb-2">
        <div className="text-blue-100">{icon}</div>
        <span className="text-sm text-blue-100 font-medium">{label}</span>
      </div>
      <div className={`text-2xl font-bold ${className}`}>{value}</div>
      {trend && (
        <div className="flex items-center space-x-1 mt-1">
          {trend === 'growing' || trend === 'growth' ? (
            <TrendingUp className="w-3 h-3 text-green-300" />
          ) : trend === 'declining' || trend === 'decline' ? (
            <TrendingDown className="w-3 h-3 text-red-300" />
          ) : (
            <Minus className="w-3 h-3 text-blue-300" />
          )}
          <span className="text-xs text-blue-100 capitalize">{trend}</span>
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Component: Module Section Wrapper
// ---------------------------------------------------------------------------
interface ModuleSectionProps {
  moduleKey: string;
  expanded: boolean;
  onToggle: () => void;
  children: React.ReactNode;
}

function ModuleSection({ moduleKey, expanded, onToggle, children }: ModuleSectionProps) {
  const meta = MODULE_META[moduleKey];
  if (!meta) return null;

  const IconComponent = getIconComponent(meta.icon);

  return (
    <div className="bg-white rounded-lg shadow-sm border border-gray-200 overflow-hidden">
      <button
        onClick={onToggle}
        className="w-full flex items-center justify-between p-6 hover:bg-gray-50 transition text-left"
      >
        <div className="flex items-center space-x-4">
          <div className="w-10 h-10 bg-blue-100 rounded-lg flex items-center justify-center text-blue-600 flex-shrink-0">
            <IconComponent className="w-5 h-5" />
          </div>
          <div>
            <div className="flex items-center space-x-2">
              <span className="text-xs font-semibold text-blue-600 uppercase tracking-wide">
                Module {meta.number}
              </span>
            </div>
            <h2 className="text-xl font-bold text-gray-900 mt-1">{meta.title}</h2>
          </div>
        </div>
        <div className="text-gray-400">
          {expanded ? (
            <ChevronUp className="w-6 h-6" />
          ) : (
            <ChevronDown className="w-6 h-6" />
          )}
        </div>
      </button>

      {expanded && (
        <div className="border-t border-gray-200 p-6 bg-gray-50">{children}</div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Helper: Get Icon Component by Name
// ---------------------------------------------------------------------------
function getIconComponent(iconName: string) {
  const icons: Record<string, any> = {
    activity: Activity,
    target: Target,
    search: Search,
    'file-text': FileText,
    layers: Layers,
    zap: Zap,
    shield: Shield,
    'bar-chart': BarChart2,
    globe: Globe,
    users: Users,
    dollar: DollarSign,
  };
  return icons[iconName] || Activity;
}

// ---------------------------------------------------------------------------
// Module Content Components
// ---------------------------------------------------------------------------

// Module 1: Health & Trajectory
function HealthTrajectoryContent({ data }: { data: any }) {
  if (!data) return <div className="text-gray-500">No data available</div>;

  const forecastData = data.forecast_chart_data || [];
  const trendDirection = data.overall_direction || 'unknown';
  const trendSlope = data.trend_slope_pct_per_month || 0;

  return (
    <div className="space-y-6">
      {/* Overview */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <MetricCard
          label="Trend Direction"
          value={trendDirection}
          className="capitalize"
          icon={
            trendDirection === 'growing' || trendDirection === 'growth' ? (
              <TrendingUp className="w-5 h-5 text-green-600" />
            ) : trendDirection === 'declining' || trendDirection === 'decline' ? (
              <TrendingDown className="w-5 h-5 text-red-600" />
            ) : (
              <Minus className="w-5 h-5 text-gray-600" />
            )
          }
        />
        <MetricCard
          label="Monthly Change"
          value={`${trendSlope >= 0 ? '+' : ''}${trendSlope.toFixed(1)}%`}
          className={trendSlope >= 0 ? 'text-green-600' : 'text-red-600'}
        />
        <MetricCard
          label="90-Day Forecast"
          value={
            data.forecast?.['90d']?.clicks
              ? data.forecast['90d'].clicks.toLocaleString()
              : 'N/A'
          }
        />
      </div>

      {/* Forecast Chart */}
      {forecastData.length > 0 && (
        <div className="bg-white p-4 rounded-lg border border-gray-200">
          <h3 className="text-sm font-semibold text-gray-700 mb-4">
            Traffic Forecast (90 Days)
          </h3>
          <ResponsiveContainer width="100%" height={300}>
            <LineChart data={forecastData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="date" tick={{ fontSize: 12 }} />
              <YAxis tick={{ fontSize: 12 }} />
              <Tooltip />
              <Legend />
              <Line
                type="monotone"
                dataKey="actual"
                stroke="#3b82f6"
                strokeWidth={2}
                dot={false}
                name="Actual"
              />
              <Line
                type="monotone"
                dataKey="forecast"
                stroke="#10b981"
                strokeWidth={2}
                strokeDasharray="5 5"
                dot={false}
                name="Forecast"
              />
              <Line
                type="monotone"
                dataKey="ci_low"
                stroke="#e5e7eb"
                strokeWidth={1}
                dot={false}
                name="Lower Bound"
              />
              <Line
                type="monotone"
                dataKey="ci_high"
                stroke="#e5e7eb"
                strokeWidth={1}
                dot={false}
                name="Upper Bound"
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* Change Points */}
      {data.change_points && data.change_points.length > 0 && (
        <div className="bg-white p-4 rounded-lg border border-gray-200">
          <h3 className="text-sm font-semibold text-gray-700 mb-4">
            Detected Change Points
          </h3>
          <div className="space-y-2">
            {data.change_points.map((cp: any, idx: number) => (
              <div
                key={idx}
                className="flex items-center justify-between p-3 bg-gray-50 rounded"
              >
                <div>
                  <div className="font-medium text-gray-900">{cp.date}</div>
                  <div className="text-sm text-gray-600 capitalize">
                    {cp.direction} — {Math.abs(cp.magnitude * 100).toFixed(1)}% change
                  </div>
                </div>
                {cp.direction === 'drop' ? (
                  <TrendingDown className="w-5 h-5 text-red-500" />
                ) : (
                  <TrendingUp className="w-5 h-5 text-green-500" />
                )}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Seasonality */}
      {data.seasonality && (
        <div className="bg-white p-4 rounded-lg border border-gray-200">
          <h3 className="text-sm font-semibold text-gray-700 mb-4">
            Seasonality Patterns
          </h3>
          <div className="grid grid-cols-2 gap-4">
            <div>
              <div className="text-sm text-gray-600">Best Day</div>
              <div className="text-lg font-semibold text-gray-900">
                {data.seasonality.best_day || 'N/A'}
              </div>
            </div>
            <div>
              <div className="text-sm text-gray-600">Worst Day</div>
              <div className="text-lg font-semibold text-gray-900">
                {data.seasonality.worst_day || 'N/A'}
              </div>
            </div>
          </div>
          {data.seasonality.cycle_description && (
            <p className="text-sm text-gray-600 mt-3">
              {data.seasonality.cycle_description}
            </p>
          )}
        </div>
      )}
    </div>
  );
}

// Module 2: Page-Level Triage
function PageTriageContent({ data }: { data: any }) {
  if (!data) return <div className="text-gray-500">No data available</div>;

  const pages = data.pages || [];
  const summary = data.summary || {};

  return (
    <div className="space-y-6">
      {/* Summary */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <MetricCard label="Growing" value={summary.growing || 0} className="text-green-600" />
        <MetricCard label="Stable" value={summary.stable || 0} className="text-gray-600" />
        <MetricCard label="Decaying" value={summary.decaying || 0} className="text-yellow-600" />
        <MetricCard label="Critical" value={summary.critical || 0} className="text-red-600" />
      </div>

      {/* Pages Table */}
      <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead className="bg-gray-50 border-b border-gray-200">
              <tr>
                <th className="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase">
                  Page
                </th>
                <th className="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase">
                  Status
                </th>
                <th className="px-4 py-3 text-right text-xs font-semibold text-gray-700 uppercase">
                  Monthly Clicks
                </th>
                <th className="px-4 py-3 text-right text-xs font-semibold text-gray-700 uppercase">
                  Trend
                </th>
                <th className="px-4 py-3 text-right text-xs font-semibold text-gray-700 uppercase">
                  Priority
                </th>
                <th className="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase">
                  Action
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200">
              {pages.slice(0, 20).map((page: any, idx: number) => (
                <tr key={idx} className="hover:bg-gray-50">
                  <td className="px-4 py-3">
                    <a
                      href={page.url}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="text-sm text-blue-600 hover:underline flex items-center"
                    >
                      <span className="truncate max-w-xs">{page.url}</span>
                      <ExternalLink className="w-3 h-3 ml-1 flex-shrink-0" />
                    </a>
                  </td>
                  <td className="px-4 py-3">
                    <StatusBadge status={page.bucket} />
                  </td>
                  <td className="px-4 py-3 text-right text-sm text-gray-900">
                    {page.current_monthly_clicks?.toLocaleString() || 0}
                  </td>
                  <td className="px-4 py-3 text-right text-sm">
                    <span
                      className={
                        page.trend_slope >= 0 ? 'text-green-600' : 'text-red-600'
                      }
                    >
                      {page.trend_slope >= 0 ? '+' : ''}
                      {page.trend_slope?.toFixed(2) || 0}
                    </span>
                  </td>
                  <td className="px-4 py-3 text-right">
                    <PriorityBadge score={page.priority_score} />
                  </td>
                  <td className="px-4 py-3 text-sm text-gray-700">
                    {page.recommended_action || 'Monitor'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

// Module 3: SERP Landscape
function SerpLandscapeContent({ data }: { data: any }) {
  if (!data) return <div className="text-gray-500">No data available</div>;

  const competitors = data.competitors || [];
  const displacements = data.serp_feature_displacement || [];

  return (
    <div className="space-y-6">
      {/* Summary */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <MetricCard
          label="Keywords Analyzed"
          value={data.keywords_analyzed || 0}
        />
        <MetricCard
          label="Click Share"
          value={`${((data.total_click_share || 0) * 100).toFixed(1)}%`}
        />
        <MetricCard
          label="Opportunity"
          value={`${((data.click_share_opportunity || 0) * 100).toFixed(1)}%`}
        />
      </div>

      {/* SERP Feature Displacement */}
      {displacements.length > 0 && (
        <div className="bg-white p-4 rounded-lg border border-gray-200">
          <h3 className="text-sm font-semibold text-gray-700 mb-4">
            SERP Feature Displacement
          </h3>
          <div className="space-y-3">
            {displacements.slice(0, 10).map((disp: any, idx: number) => (
              <div
                key={idx}
                className="flex items-start justify-between p-3 bg-gray-50 rounded"
              >
                <div className="flex-1">
                  <div className="font-medium text-gray-900">{disp.keyword}</div>
                  <div className="text-sm text-gray-600 mt-1">
                    Organic #{disp.organic_position} → Visual #{disp.visual_position}
                  </div>
                  <div className="text-xs text-gray-500 mt-1">
                    {disp.features_above?.join(', ')}
                  </div>
                </div>
                <div className="text-right ml-4">
                  <div className="text-sm font-medium text-red-600">
                    {((disp.estimated_ctr_impact || 0) * 100).toFixed(1)}%
                  </div>
                  <div className="text-xs text-gray-500">CTR impact</div>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Competitors */}
      {competitors.length > 0 && (
        <div className="bg-white p-4 rounded-lg border border-gray-200">
          <h3 className="text-sm font-semibold text-gray-700 mb-4">
            Top Competitors
          </h3>
          <div className="space-y-2">
            {competitors.slice(0, 10).map((comp: any, idx: number) => (
              <div
                key={idx}
                className="flex items-center justify-between p-3 bg-gray-50 rounded"
              >
                <div>
                  <div className="font-medium text-gray-900">{comp.domain}</div>
                  <div className="text-sm text-gray-600">
                    {comp.keywords_shared} shared keywords
                  </div>
                </div>
                <div className="text-right">
                  <div className="text-sm font-medium text-gray-900">
                    Avg pos: {comp.avg_position?.toFixed(1)}
                  </div>
                  <ThreatBadge level={comp.threat_level} />
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

// Module 4: Content Intelligence
function ContentIntelligenceContent({ data }: { data: any }) {
  if (!data) return <div className="text-gray-500">No data available</div>;

  const cannibalization = data.cannibalization_clusters || [];
  const strikingDistance = data.striking_distance || [];

  return (
    <div className="space-y-6">
      {/* Cannibalization */}
      {cannibalization.length > 0 && (
        <div className="bg-white p-4 rounded-lg border border-gray-200">
          <h3 className="text-sm font-semibold text-gray-700 mb-4">
            Keyword Cannibalization
          </h3>
          <div className="space-y-3">
            {cannibalization.map((cluster: any, idx: number) => (
              <div key={idx} className="p-3 bg-yellow-50 border border-yellow-200 rounded">
                <div className="font-medium text-gray-900 mb-2">
                  {cluster.query_group}
                </div>
                <div className="text-sm text-gray-700 space-y-1">
                  <div>Pages: {cluster.pages?.join(' vs ')}</div>
                  <div>Shared queries: {cluster.shared_queries}</div>
                  <div>Impressions affected: {cluster.total_impressions_affected?.toLocaleString()}</div>
                  <div className="font-medium text-yellow-800 mt-2">
                    → {cluster.recommendation}: {cluster.keep_page}
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Striking Distance */}
      {strikingDistance.length > 0 && (
        <div className="bg-white p-4 rounded-lg border border-gray-200">
          <h3 className="text-sm font-semibold text-gray-700 mb-4">
            Striking Distance Opportunities
          </h3>
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead className="bg-gray-50 border-b border-gray-200">
                <tr>
                  <th className="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase">
                    Query
                  </th>
                  <th className="px-4 py-3 text-right text-xs font-semibold text-gray-700 uppercase">
                    Position
                  </th>
                  <th className="px-4 py-3 text-right text-xs font-semibold text-gray-700 uppercase">
                    Impressions
                  </th>
                  <th className="px-4 py-3 text-right text-xs font-semibold text-gray-700 uppercase">
                    Click Gain
                  </th>
                  <th className="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase">
                    Intent
                  </th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-200">
                {strikingDistance.slice(0, 15).map((opp: any, idx: number) => (
                  <tr key={idx} className="hover:bg-gray-50">
                    <td className="px-4 py-3 text-sm text-gray-900">{opp.query}</td>
                    <td className="px-4 py-3 text-right text-sm text-gray-900">
                      {opp.current_position?.toFixed(1)}
                    </td>
                    <td className="px-4 py-3 text-right text-sm text-gray-900">
                      {opp.impressions?.toLocaleString()}
                    </td>
                    <td className="px-4 py-3 text-right text-sm text-green-600 font-medium">
                      +{opp.estimated_click_gain_if_top5?.toLocaleString()}
                    </td>
                    <td className="px-4 py-3 text-sm text-gray-700 capitalize">
                      {opp.intent}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}

// Module 6: The Gameplan
function GameplanContent({ data }: { data: any }) {
  if (!data) return <div className="text-gray-500">No data available</div>;

  return (
    <div className="space-y-6">
      {/* Summary */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <MetricCard
          label="Recovery Potential"
          value={`${data.total_estimated_monthly_click_recovery?.toLocaleString() || 0} clicks/mo`}
          className="text-green-600"
        />
        <MetricCard
          label="Growth Opportunity"
          value={`${data.total_estimated_monthly_click_growth?.toLocaleString() || 0} clicks/mo`}
          className="text-blue-600"
        />
      </div>

      {/* Narrative */}
      {data.narrative && (
        <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
          <p className="text-sm text-gray-800 leading-relaxed whitespace-pre-wrap">
            {data.narrative}
          </p>
        </div>
      )}

      {/* Action Lists */}
      {data.critical && data.critical.length > 0 && (
        <ActionSection
          title="Critical Fixes (This Week)"
          actions={data.critical}
          color="red"
        />
      )}
      {data.quick_wins && data.quick_wins.length > 0 && (
        <ActionSection
          title="Quick Wins (This Month)"
          actions={data.quick_wins}
          color="yellow"
        />
      )}
      {data.strategic && data.strategic.length > 0 && (
        <ActionSection
          title="Strategic Plays (This Quarter)"
          actions={data.strategic}
          color="blue"
        />
      )}
      {data.structural && data.structural.length > 0 && (
        <ActionSection
          title="Structural Improvements (Ongoing)"
          actions={data.structural}
          color="gray"
        />
      )}
    </div>
  );
}

function ActionSection({
  title,
  actions,
  color,
}: {
  title: string;
  actions: any[];
  color: string;
}) {
  const colorClasses = {
    red: 'bg-red-50 border-red-200',
    yellow: 'bg-yellow-50 border-yellow-200',
    blue: 'bg-blue-50 border-blue-200',
    gray: 'bg-gray-50 border-gray-200',
  };

  return (
    <div className={`rounded-lg border p-4 ${colorClasses[color as keyof typeof colorClasses]}`}>
      <h3 className="text-sm font-semibold text-gray-900 mb-3">{title}</h3>
      <div className="space-y-2">
        {actions.map((action: any, idx: number) => (
          <div key={idx} className="bg-white p-3 rounded border border-gray-200">
            <div className="font-medium text-gray-900">{action.action}</div>
            <div className="grid grid-cols-3 gap-2 mt-2 text-xs text-gray-600">
              <div>Impact: +{action.impact} clicks/mo</div>
              <div>Effort: {action.effort}</div>
              {action.page && (
                <div className="truncate" title={action.page}>
                  Page: {action.page}
                </div>
              )}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

// Module 7: Algorithm Impact
function AlgorithmImpactContent({ data }: { data: any }) {
  if (!data) return <div className="text-gray-500">No data available</div>;

  const updates = data.updates_impacting_site || [];

  return (
    <div className="space-y-6">
      <MetricCard
        label="Vulnerability Score"
        value={`${((data.vulnerability_score || 0) * 100).toFixed(0)}%`}
        className={
          (data.vulnerability_score || 0) > 0.7
            ? 'text-red-600'
            : (data.vulnerability_score || 0) > 0.4
            ? 'text-yellow-600'
            : 'text-green-600'
        }
      />

      {updates.length > 0 && (
        <div className="bg-white p-4 rounded-lg border border-gray-200">
          <h3 className="text-sm font-semibold text-gray-700 mb-4">
            Algorithm Updates Impact
          </h3>
          <div className="space-y-3">
            {updates.map((update: any, idx: number) => (
              <div
                key={idx}
                className={`p-3 rounded border ${
                  update.site_impact === 'negative'
                    ? 'bg-red-50 border-red-200'
                    : update.site_impact === 'positive'
                    ? 'bg-green-50 border-green-200'
                    : 'bg-gray-50 border-gray-200'
                }`}
              >
                <div className="flex items-start justify-between">
                  <div>
                    <div className="font-medium text-gray-900">
                      {update.update_name}
                    </div>
                    <div className="text-sm text-gray-600 mt-1">{update.date}</div>
                    {update.common_characteristics && (
                      <div className="text-xs text-gray-500 mt-2">
                        Affected: {update.common_characteristics.join(', ')}
                      </div>
                    )}
                  </div>
                  <div className="text-right ml-4">
                    <div
                      className={`text-sm font-medium ${
                        update.click_change_pct >= 0
                          ? 'text-green-600'
                          : 'text-red-600'
                      }`}
                    >
                      {update.click_change_pct >= 0 ? '+' : ''}
                      {update.click_change_pct?.toFixed(1)}%
                    </div>
                    <div className="text-xs text-gray-500 mt-1">
                      {update.recovery_status || 'monitoring'}
                    </div>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {data.recommendation && (
        <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
          <p className="text-sm text-gray-800">{data.recommendation}</p>
        </div>
      )}
    </div>
  );
}

// Module 8: Intent Migration
function IntentMigrationContent({ data }: { data: any }) {
  if (!data) return <div className="text-gray-500">No data available</div>;

  const migrations = data.migrations || [];

  return (
    <div className="space-y-6">
      {migrations.length > 0 && (
        <div className="bg-white p-4 rounded-lg border border-gray-200">
          <h3 className="text-sm font-semibold text-gray-700 mb-4">
            Detected Intent Shifts
          </h3>
          <div className="space-y-3">
            {migrations.map((migration: any, idx: number) => (
              <div
                key={idx}
                className="p-3 bg-purple-50 border border-purple-200 rounded"
              >
                <div className="font-medium text-gray-900">{migration.query}</div>
                <div className="text-sm text-gray-700 mt-2">
                  {migration.previous_intent} → {migration.current_intent}
                </div>
                <div className="text-xs text-gray-600 mt-1">
                  Confidence: {((migration.confidence || 0) * 100).toFixed(0)}%
                </div>
                {migration.recommendation && (
                  <div className="text-sm text-purple-800 mt-2 font-medium">
                    → {migration.recommendation}
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

// Module 9: Technical Health (CTR Modeling)
function TechnicalHealthContent({ data }: { data: any }) {
  if (!data) return <div className="text-gray-500">No data available</div>;

  return (
    <div className="space-y-6">
      <div className="bg-white p-4 rounded-lg border border-gray-200">
        <h3 className="text-sm font-semibold text-gray-700 mb-4">
          CTR Performance Analysis
        </h3>
        <p className="text-sm text-gray-600">
          Detailed CTR modeling and anomaly detection coming soon.
        </p>
      </div>
    </div>
  );
}

// Module 10: Site Architecture
function SiteArchitectureContent({ data }: { data: any }) {
  if (!data) return <div className="text-gray-500">No data available</div>;

  const hubs = data.hub_pages || [];
  const orphans = data.orphan_pages || [];

  return (
    <div className="space-y-6">
      {/* Network Graph */}
      {data.network_graph && (
        <div className="bg-white p-4 rounded-lg border border-gray-200">
          <h3 className="text-sm font-semibold text-gray-700 mb-4">
            Internal Link Network
          </h3>
          <NetworkGraph data={data.network_graph} />
        </div>
      )}

      {/* Hub Pages */}
      {hubs.length > 0 && (
        <div className="bg-white p-4 rounded-lg border border-gray-200">
          <h3 className="text-sm font-semibold text-gray-700 mb-4">
            Hub Pages (High PageRank)
          </h3>
          <div className="space-y-2">
            {hubs.slice(0, 10).map((hub: any, idx: number) => (
              <div
                key={idx}
                className="flex items-center justify-between p-3 bg-gray-50 rounded"
              >
                <a
                  href={hub.url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-sm text-blue-600 hover:underline flex items-center truncate flex-1"
                >
                  <span className="truncate">{hub.url}</span>
                  <ExternalLink className="w-3 h-3 ml-1 flex-shrink-0" />
                </a>
                <div className="ml-4 text-right">
                  <div className="text-sm font-medium text-gray-900">
                    PR: {hub.pagerank?.toFixed(3)}
                  </div>
                  <div className="text-xs text-gray-500">
                    {hub.inbound_links} links
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Orphan Pages */}
      {orphans.length > 0 && (
        <div className="bg-white p-4 rounded-lg border border-gray-200">
          <h3 className="text-sm font-semibold text-gray-700 mb-4">
            Orphan Pages (No Internal Links)
          </h3>
          <div className="space-y-2">
            {orphans.slice(0, 10).map((orphan: any, idx: number) => (
              <div key={idx} className="p-3 bg-yellow-50 border border-yellow-200 rounded">
                <a
                  href={orphan}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-sm text-blue-600 hover:underline flex items-center"
                >
                  <span className="truncate">{orphan}</span>
                  <ExternalLink className="w-3 h-3 ml-1 flex-shrink-0" />
                </a>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

// Module 11: Branded Split
function BrandedSplitContent({ data }: { data: any }) {
  if (!data) return <div className="text-gray-500">No data available</div>;

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <div className="bg-white p-4 rounded-lg border border-gray-200">
          <h3 className="text-sm font-semibold text-gray-700 mb-2">
            Branded Traffic
          </h3>
          <div className="text-3xl font-bold text-gray-900">
            {data.branded_clicks?.toLocaleString() || 0}
          </div>
          <div className="text-sm text-gray-600 mt-1">clicks/month</div>
        </div>
        <div className="bg-white p-4 rounded-lg border border-gray-200">
          <h3 className="text-sm font-semibold text-gray-700 mb-2">
            Non-Branded Traffic
          </h3>
          <div className="text-3xl font-bold text-gray-900">
            {data.non_branded_clicks?.toLocaleString() || 0}
          </div>
          <div className="text-sm text-gray-600 mt-1">clicks/month</div>
        </div>
      </div>

      {data.brand_dependency_score !== undefined && (
        <div className="bg-white p-4 rounded-lg border border-gray-200">
          <h3 className="text-sm font-semibold text-gray-700 mb-2">
            Brand Dependency
          </h3>
          <div className="text-2xl font-bold text-gray-900">
            {(data.brand_dependency_score * 100).toFixed(1)}%
          </div>
          <p className="text-sm text-gray-600 mt-2">
            {data.brand_dependency_score > 0.7
              ? 'High dependency on branded traffic — diversify with non-branded content'
              : data.brand_dependency_score > 0.4
              ? 'Moderate brand dependency — healthy balance'
              : 'Strong non-branded presence — good diversification'}
          </p>
        </div>
      )}
    </div>
  );
}

// Module 12: Competitive Threats
function CompetitiveThreatsContent({ data }: { data: any }) {
  if (!data) return <div className="text-gray-500">No data available</div>;

  const threats = data.threats || [];

  return (
    <div className="space-y-6">
      {threats.length > 0 && (
        <div className="bg-white p-4 rounded-lg border border-gray-200">
          <h3 className="text-sm font-semibold text-gray-700 mb-4">
            Competitive Threats
          </h3>
          <div className="space-y-3">
            {threats.map((threat: any, idx: number) => (
              <div
                key={idx}
                className="p-3 bg-red-50 border border-red-200 rounded"
              >
                <div className="flex items-start justify-between">
                  <div className="flex-1">
                    <div className="font-medium text-gray-900">
                      {threat.competitor}
                    </div>
                    <div className="text-sm text-gray-700 mt-1">
                      {threat.keywords_at_risk} keywords at risk
                    </div>
                    <div className="text-xs text-gray-600 mt-1">
                      Estimated loss: {threat.estimated_click_loss?.toLocaleString()} clicks/mo
                    </div>
                  </div>
                  <ThreatBadge level={threat.threat_level} />
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

// Module 13: Revenue Attribution
function RevenueAttributionContent({ data }: { data: any }) {
  if (!data) return <div className="text-gray-500">No data available</div>;

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <MetricCard
          label="Attributed Revenue"
          value={`$${data.total_attributed_revenue?.toLocaleString() || 0}`}
          className="text-green-600"
        />
        <MetricCard
          label="Revenue per Session"
          value={`$${data.revenue_per_session?.toFixed(2) || 0}`}
        />
        <MetricCard
          label="Top Converting Page"
          value={data.top_converting_page || 'N/A'}
          className="text-sm truncate"
        />
      </div>

      {data.revenue_by_channel && (
        <div className="bg-white p-4 rounded-lg border border-gray-200">
          <h3 className="text-sm font-semibold text-gray-700 mb-4">
            Revenue by Channel
          </h3>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={data.revenue_by_channel}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="channel" tick={{ fontSize: 12 }} />
              <YAxis tick={{ fontSize: 12 }} />
              <Tooltip />
              <Bar dataKey="revenue" fill="#3b82f6" />
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Utility Components
// ---------------------------------------------------------------------------

function MetricCard({
  label,
  value,
  icon,
  className = '',
}: {
  label: string;
  value: string | number;
  icon?: React.ReactNode;
  className?: string;
}) {
  return (
    <div className="bg-white p-4 rounded-lg border border-gray-200">
      {icon && <div className="mb-2">{icon}</div>}
      <div className="text-sm text-gray-600 mb-1">{label}</div>
      <div className={`text-2xl font-bold ${className}`}>{value}</div>
    </div>
  );
}

function StatusBadge({ status }: { status: string }) {
  const colors: Record<string, string> = {
    growing: 'bg-green-100 text-green-800',
    stable: 'bg-gray-100 text-gray-800',
    decaying: 'bg-yellow-100 text-yellow-800',
    critical: 'bg-red-100 text-red-800',
  };

  return (
    <span
      className={`px-2 py-1 text-xs font-medium rounded ${
        colors[status] || 'bg-gray-100 text-gray-800'
      }`}
    >
      {status}
    </span>
  );
}

function PriorityBadge({ score }: {